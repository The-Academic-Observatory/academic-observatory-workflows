# Copyright 2020 Curtin University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Author: James Diprose, Aniek Roelofs, Tuan Chien

import logging
import os
import shutil
import unittest
from pathlib import Path
from unittest.mock import MagicMock, PropertyMock, patch

import pendulum
from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.utils.state import State
from click.testing import CliRunner

from academic_observatory_workflows.config import test_fixtures_folder
from academic_observatory_workflows.workflows.grid_telescope import (
    GridRelease,
    GridTelescope,
    list_grid_records,
)
from observatory.api.client import ApiClient, Configuration
from observatory.api.client.api.observatory_api import ObservatoryApi  # noqa: E501
from observatory.api.client.model.dataset import Dataset
from observatory.api.client.model.dataset_type import DatasetType
from observatory.api.client.model.organisation import Organisation
from observatory.api.client.model.table_type import TableType
from observatory.api.client.model.workflow import Workflow
from observatory.api.client.model.workflow_type import WorkflowType
from observatory.api.testing import ObservatoryApiEnvironment
from observatory.platform.utils.airflow_utils import AirflowConns
from observatory.platform.utils.file_utils import get_file_hash, gzip_file_crc
from observatory.platform.utils.release_utils import get_dataset_releases
from observatory.platform.utils.test_utils import (
    HttpServer,
    ObservatoryEnvironment,
    ObservatoryTestCase,
    module_file_path,
    find_free_port,
)
from observatory.platform.utils.workflow_utils import blob_name
from observatory.platform.utils.workflow_utils import table_ids_from_path


class MockResponse:
    def __init__(self):
        self.text = '[{"published_date": "20210101", "id":12345, "title":"no date in here"}]'


class MockSession:
    def get(self, *args, **kwargs):
        return MockResponse()


class MockTaskInstance:
    def __init__(self, records):
        """Construct a MockTaskInstance. This mocks the airflow TaskInstance and is passed as a keyword arg to the
        make_release function.
        :param records: List of record info, returned as value during xcom_pull
        """
        self.records = records

    def xcom_pull(self, key: str, task_ids: str, include_prior_dates: bool):
        """Mock xcom_pull method of airflow TaskInstance.
        :param key: -
        :param task_ids: -
        :param include_prior_dates: -
        :return: Records list
        """
        return self.records


def side_effect(arg):
    values = {
        "project_id": "project",
        "download_bucket_name": "download-bucket",
        "transform_bucket_name": "transform-bucket",
        "data_path": "data",
        "data_location": "US",
    }
    return values[arg]


def copy_download_fixtures(*, mock, fixtures):
    _, call_args = mock.call_args
    src_filename = os.path.basename(call_args["url"])
    src = os.path.join(fixtures, "files", src_filename)
    dst = call_args["filename"]
    shutil.copyfile(src, dst)


@patch("observatory.platform.utils.workflow_utils.Variable.get")
class TestGridTelescope(unittest.TestCase):
    """Tests for the functions used by the GRID telescope"""

    def __init__(
        self,
        *args,
        **kwargs,
    ):
        """Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        self.fixtures = test_fixtures_folder("grid")
        self.httpserver = HttpServer(directory=self.fixtures)
        self.httpserver.start()

        super(TestGridTelescope, self).__init__(*args, **kwargs)

        # Telescope instance
        self.grid = GridTelescope()

        # Paths
        # Contains GRID releases 2015-09-22 and 2015-10-09 (format for both is .csv and .json files)
        with patch.object(
            GridTelescope,
            "GRID_FILE_URL",
            f"http://{self.httpserver.host}:{self.httpserver.port}" + "/v2/articles/{article_id}/files",
        ):
            with patch("observatory.platform.utils.workflow_utils.Variable.get") as mock_variable_get:
                mock_variable_get.side_effect = side_effect
                self.grid_run_2015_10_18 = {
                    "start_date": pendulum.datetime(2015, 10, 11),
                    "end_date": pendulum.datetime(2015, 10, 18),
                    "records": [
                        {"article_ids": [1570967, 1570968], "release_date": "2015-10-09"},
                        {"article_ids": [1553267, 1553266], "release_date": "2015-09-22"},
                    ],
                    # there are 2 releases in this run, but use only 1 for testing
                    "release": GridRelease(
                        self.grid.dag_id, ["1553266", "1553267"], pendulum.parse("2015-09-22T00:00:00+00:00")
                    ),
                    "download_hash": "c6fd33fd31b6699a2f19622f0283f4f1",
                    "extract_hash": "c6fd33fd31b6699a2f19622f0283f4f1",
                    "transform_crc": "eb66ae78",
                }

                # Contains GRID release 2020-03-15 (format is a .zip file, which is more common)
                self.grid_run_2020_03_27 = {
                    "start_date": pendulum.datetime(2020, 3, 20),
                    "end_date": pendulum.datetime(2020, 3, 27),
                    "records": [{"article_ids": [12022722], "release_date": "2020-03-15T00:00:00+00:00"}],
                    "release": GridRelease(self.grid.dag_id, ["12022722"], pendulum.parse("2020-03-15T00:00:00+00:00")),
                    "download_hash": "3d300affce1666ac50b8d945c6ca4c5a",
                    "extract_hash": "5aff68e9bf72e846a867e91c1fa206a0",
                    "transform_crc": "77bc8585",
                }

        self.grid_runs = [self.grid_run_2015_10_18, self.grid_run_2020_03_27]

        # Turn logging to warning because vcr prints too much at info level
        logging.basicConfig()
        logging.getLogger().setLevel(logging.WARNING)

    def __del__(self):
        self.httpserver.stop()

    def test_ctor(self, mock_variable_get):
        """Cover case where airflow_vars is given."""
        telescope = GridTelescope(airflow_vars=[])
        self.assertEqual(telescope.airflow_vars, list(["transform_bucket"]))

    def test_list_grid_records(self, mock_variable_get):
        """Check that list grid records returns a list of dictionaries with records in the correct format.

        :param mock_variable_get: Mock result of airflow's Variable.get() function
        :return: None.
        """
        with patch.object(
            GridTelescope,
            "GRID_DATASET_URL",
            f"http://{self.httpserver.host}:{self.httpserver.port}/list_grid_releases",
        ):
            start_date = self.grid_run_2015_10_18["start_date"]
            end_date = self.grid_run_2015_10_18["end_date"]
            records = list_grid_records(start_date, end_date, GridTelescope.GRID_DATASET_URL)
            self.assertEqual(self.grid_run_2015_10_18["records"], records)

    def test_list_grid_records_bad_title(self, mock_variable_get):
        """Check exception raised when invalid title given."""

        with patch(
            "academic_observatory_workflows.workflows.grid_telescope.retry_session", return_value=MockSession()
        ) as _:
            start_date = pendulum.datetime(2020, 1, 1)
            end_date = pendulum.datetime(2022, 1, 1)
            self.assertRaises(ValueError, list_grid_records, start_date, end_date, "")

    def test_list_releases(self, mock_variable_get):
        """Test list_releases."""

        ti = MagicMock()
        with patch("academic_observatory_workflows.workflows.grid_telescope.list_grid_records") as m_list_grid_records:
            m_list_grid_records.return_value = []
            telescope = GridTelescope()
            result = telescope.list_releases(execution_date=pendulum.now(), next_execution_date=pendulum.now())
            self.assertEqual(result, False)

            m_list_grid_records.return_value = [1]
            telescope = GridTelescope()
            result = telescope.list_releases(execution_date=pendulum.now(), next_execution_date=pendulum.now(), ti=ti)
            self.assertEqual(result, True)

    def test_make_release(self, mock_variable_get):
        """Check that make_release returns a list of GridRelease instances.

        :param mock_variable_get: Mock result of airflow's Variable.get() function
        :return: None.
        """
        mock_variable_get.side_effect = side_effect

        for run in self.grid_runs:
            records = run["records"]

            releases = self.grid.make_release(ti=MockTaskInstance(records))
            self.assertIsInstance(releases, list)
            for release in releases:
                self.assertIsInstance(release, GridRelease)

    @patch("academic_observatory_workflows.workflows.grid_telescope.download_file")
    def test_download_release(self, m_download, mock_variable_get):
        """Download two specific GRID releases and check they have the expected md5 sum.

        :param mock_variable_get: Mock result of airflow's Variable.get() function
        :return:
        """
        mock_variable_get.side_effect = side_effect

        with CliRunner().isolated_filesystem():
            for run in self.grid_runs:
                release = run["release"]
                downloads = release.download()
                # Check that returned downloads has correct length
                self.assertEqual(1, len(downloads))

            self.assertEqual(m_download.call_count, 2)

            _, call_args = m_download.call_args_list[0]
            self.assertEqual(call_args["url"], "https://ndownloader.figshare.com/files/2284777")
            self.assertEqual(call_args["filename"], "data/telescopes/download/grid/grid_2015_09_22/grid.json")
            self.assertEqual(call_args["hash"], "c6fd33fd31b6699a2f19622f0283f4f1")

            _, call_args = m_download.call_args_list[1]
            self.assertEqual(call_args["url"], "https://ndownloader.figshare.com/files/22091379")
            self.assertEqual(call_args["filename"], "data/telescopes/download/grid/grid_2020_03_15/grid.zip")
            self.assertEqual(call_args["hash"], "3d300affce1666ac50b8d945c6ca4c5a")

    @patch("academic_observatory_workflows.workflows.grid_telescope.download_file")
    def test_extract_release(self, m_download, mock_variable_get):
        """Test that the GRID releases are extracted as expected, both for an unzipped json file and a zip file.

        :param mock_variable_get: Mock result of airflow's Variable.get() function
        :return: None.
        """
        mock_variable_get.side_effect = side_effect

        with CliRunner().isolated_filesystem():
            for run in self.grid_runs:
                release = run["release"]

                release.download()

                # Copy the file in rather than download
                copy_download_fixtures(mock=m_download, fixtures=self.fixtures)

                release.extract()

                self.assertEqual(1, len(release.extract_files))
                self.assertEqual(
                    run["extract_hash"], get_file_hash(file_path=release.extract_files[0], algorithm="md5")
                )

    @patch("academic_observatory_workflows.workflows.grid_telescope.download_file")
    def test_transform_release(self, m_download, mock_variable_get):
        """Test that the GRID releases are transformed as expected.

        :param mock_variable_get: Mock result of airflow's Variable.get() function
        :return: None.
        """
        mock_variable_get.side_effect = side_effect

        with CliRunner().isolated_filesystem():
            for run in self.grid_runs:
                release = run["release"]

                release.download()

                # Copy the file in rather than download
                copy_download_fixtures(mock=m_download, fixtures=self.fixtures)

                release.extract()
                release.transform()

                self.assertEqual(1, len(release.transform_files))
                self.assertEqual(run["transform_crc"], gzip_file_crc(release.transform_files[0]))


class TestGridRelease(unittest.TestCase):
    @patch(
        "academic_observatory_workflows.workflows.grid_telescope.GridRelease.extract_folder", new_callable=PropertyMock
    )
    @patch(
        "academic_observatory_workflows.workflows.grid_telescope.GridRelease.download_files", new_callable=PropertyMock
    )
    def test_extract_not_zip_file(self, m_download_files, m_extract_folder):
        with CliRunner().isolated_filesystem():
            Path("file.zip").touch()

            release = GridRelease(dag_id="dag", article_ids=[], release_date=pendulum.now())
            m_download_files.return_value = ["file.zip"]
            m_extract_folder.return_value = "."
            release.extract()

    @patch(
        "academic_observatory_workflows.workflows.grid_telescope.GridRelease.extract_files", new_callable=PropertyMock
    )
    def test_transform_multiple_extract(self, m_extract_files):
        m_extract_files.return_value = ["1", "2"]

        with CliRunner().isolated_filesystem():
            release = GridRelease(dag_id="dag", article_ids=[], release_date=pendulum.now())
            self.assertRaises(AirflowException, release.transform)


class TestGridTelescopeDag(ObservatoryTestCase):
    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """
        super().__init__(*args, **kwargs)

        self.fixtures = test_fixtures_folder("grid")
        self.project_id = os.environ["TEST_GCP_PROJECT_ID"]
        self.data_location = os.environ["TEST_GCP_DATA_LOCATION"]

        # Paths
        self.fixtures = test_fixtures_folder("grid")
        self.httpserver = HttpServer(directory=self.fixtures)
        self.httpserver.start()

        # GridTelescope.GRID_FILE_URL = (
        #     f"http://{self.httpserver.host}:{self.httpserver.port}" + "/v2/articles/{article_id}/files"
        # )

        # API environment
        self.host = "localhost"
        self.port = find_free_port()
        configuration = Configuration(host=f"http://{self.host}:{self.port}")
        api_client = ApiClient(configuration)
        self.api = ObservatoryApi(api_client=api_client)  # noqa: E501
        self.env = ObservatoryApiEnvironment(host=self.host, port=self.port)
        self.org_name = "Curtin University"

    def setup_api(self):
        dt = pendulum.now("UTC")

        name = "GRID Telescope"
        workflow_type = WorkflowType(name=name, type_id=GridTelescope.DAG_ID)
        self.api.put_workflow_type(workflow_type)

        organisation = Organisation(
            name="Curtin University",
            project_id="project",
            download_bucket="download_bucket",
            transform_bucket="transform_bucket",
        )
        self.api.put_organisation(organisation)

        telescope = Workflow(
            name=name,
            workflow_type=WorkflowType(id=1),
            organisation=Organisation(id=1),
            extra={},
        )
        self.api.put_workflow(telescope)

        table_type = TableType(
            type_id="partitioned",
            name="partitioned bq table",
        )
        self.api.put_table_type(table_type)

        dataset_type = DatasetType(
            type_id="grid",
            name="ds type",
            extra={},
            table_type=TableType(id=1),
        )
        self.api.put_dataset_type(dataset_type)

        dataset = Dataset(
            name="GRID Dataset",
            address="project.dataset.table",
            service="bigquery",
            workflow=Workflow(id=1),
            dataset_type=DatasetType(id=1),
        )
        self.api.put_dataset(dataset)

    def setup_connections(self, env):
        # Add Observatory API connection
        conn = Connection(conn_id=AirflowConns.OBSERVATORY_API, uri=f"http://:password@{self.host}:{self.port}")
        env.add_connection(conn)

    def __del__(self):
        self.httpserver.stop()

    def setup_observatory_environment(self):
        env = ObservatoryEnvironment(self.project_id, self.data_location, api_host=self.host, api_port=self.port)
        self.dataset_id = env.add_dataset()
        return env

    def test_dag_structure(self):
        """Test that the GRID DAG has the correct structure.

        :return: None
        """
        telescope = GridTelescope()
        dag = telescope.make_dag()
        self.assert_dag_structure(
            {
                "check_dependencies": ["list_releases"],
                "list_releases": ["download"],
                "download": ["upload_downloaded"],
                "upload_downloaded": ["extract"],
                "extract": ["transform"],
                "transform": ["upload_transformed"],
                "upload_transformed": ["bq_load"],
                "bq_load": ["cleanup"],
                "cleanup": ["add_new_dataset_releases"],
                "add_new_dataset_releases": [],
            },
            dag,
        )

    def test_dag_load(self):
        """Test that the GRID DAG can be loaded from a DAG bag.

        :return: None
        """

        env = ObservatoryEnvironment(self.project_id, self.data_location, api_host=self.host, api_port=self.port)

        with env.create():
            self.setup_connections(env)
            self.setup_api()
            dag_file = os.path.join(module_file_path("academic_observatory_workflows.dags"), "grid_telescope.py")
            self.assert_dag_load("grid", dag_file)

    @patch("academic_observatory_workflows.workflows.grid_telescope.download_file")
    def test_telescope(self, m_download):
        """Test running the telescope. Functional test."""

        env = self.setup_observatory_environment()
        telescope = GridTelescope(dag_id="grid", dataset_id=self.dataset_id, workflow_id=1)
        dag = telescope.make_dag()
        execution_date = pendulum.datetime(year=2015, month=9, day=22)

        # Create the Observatory environment and run tests
        with env.create():
            self.setup_connections(env)
            self.setup_api()
            with env.create_dag_run(dag, execution_date):
                with patch.object(
                    GridTelescope,
                    "GRID_FILE_URL",
                    f"http://{self.httpserver.host}:{self.httpserver.port}" + "/v2/articles/{article_id}/files",
                ):
                    # Check dependencies
                    env.run_task(telescope.check_dependencies.__name__)

                    # List releases
                    with patch(
                        "academic_observatory_workflows.workflows.grid_telescope.list_grid_records"
                    ) as m_list_grid_records:
                        m_list_grid_records.return_value = [
                            {"article_ids": [1553266, 1553267], "release_date": "2015-10-09"},
                        ]
                        ti = env.run_task(telescope.list_releases.__name__)

                        # Test list releases
                        available_releases = ti.xcom_pull(
                            key=GridTelescope.RELEASE_INFO,
                            task_ids=telescope.list_releases.__name__,
                            include_prior_dates=False,
                        )
                        self.assertEqual(len(available_releases), 1)

                        # Download
                        env.run_task(telescope.download.__name__)
                        copy_download_fixtures(mock=m_download, fixtures=self.fixtures)

                        # Test download
                        release = GridRelease(
                            dag_id="grid",
                            article_ids=[1553266, 1553267],
                            release_date=pendulum.datetime(2015, 10, 9),
                        )
                        self.assertEqual(len(release.download_files), 1)

                        # upload_downloaded
                        env.run_task(telescope.upload_downloaded.__name__)

                        # Test upload_downloaded
                        for file in release.download_files:
                            self.assert_blob_integrity(env.download_bucket, blob_name(file), file)

                        # extract
                        env.run_task(telescope.extract.__name__)

                        # Test extract
                        self.assertEqual(len(release.extract_files), 1)

                        # transform
                        env.run_task(telescope.transform.__name__)

                        # Test transform
                        self.assertEqual(len(release.transform_files), 1)

                        # upload_transformed
                        env.run_task(telescope.upload_transformed.__name__)

                        # Test upload_transformed
                        for file in release.transform_files:
                            self.assert_blob_integrity(env.transform_bucket, blob_name(file), file)

                        # bq_load
                        env.run_task(telescope.bq_load.__name__)

                        # Test bq_load
                        # Will only check table exists rather than validate data.
                        for file in release.transform_files:
                            table_id, _ = table_ids_from_path(file)
                            suffix = release.release_date.format("YYYYMMDD")
                            table_id = f"{self.project_id}.{self.dataset_id}.{table_id}{suffix}"
                            expected_rows = 48987
                            self.assert_table_integrity(table_id, expected_rows)

                        # cleanup
                        env.run_task(telescope.cleanup.__name__)

                        # Test cleanup
                        # Test that all telescope data deleted
                        download_folder, extract_folder, transform_folder = (
                            release.download_folder,
                            release.extract_folder,
                            release.transform_folder,
                        )
                        env.run_task(telescope.cleanup.__name__)
                        self.assert_cleanup(download_folder, extract_folder, transform_folder)

                        # add_dataset_release_task
                        dataset_releases = get_dataset_releases(dataset_id=1)
                        self.assertEqual(len(dataset_releases), 0)
                        ti = env.run_task("add_new_dataset_releases")
                        self.assertEqual(ti.state, State.SUCCESS)
                        dataset_releases = get_dataset_releases(dataset_id=1)
                        self.assertEqual(len(dataset_releases), 1)
