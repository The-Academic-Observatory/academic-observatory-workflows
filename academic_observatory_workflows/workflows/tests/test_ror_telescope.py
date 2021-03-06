# Copyright 2021 Curtin University
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

# Author: Aniek Roelofs

import json
import os
from unittest.mock import patch

import httpretty
import jsonlines
import pendulum
from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.utils.state import State
from click.testing import CliRunner

from academic_observatory_workflows.config import test_fixtures_folder
from academic_observatory_workflows.workflows.ror_telescope import (
    RorRelease,
    RorTelescope,
    list_ror_records,
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
from observatory.platform.utils.gc_utils import bigquery_sharded_table_id
from observatory.platform.utils.release_utils import get_dataset_releases
from observatory.platform.utils.test_utils import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    module_file_path,
    find_free_port,
)
from observatory.platform.utils.workflow_utils import (
    blob_name,
)


class TestRorTelescope(ObservatoryTestCase):
    """Tests for the ROR telescope"""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        super(TestRorTelescope, self).__init__(*args, **kwargs)
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")

        # Get list of dictionaries with expected BQ table content
        table_content = []
        with jsonlines.open(test_fixtures_folder("ror", "table_content.jsonl"), "r") as reader:
            for row in reader:
                table_content.append(row)

        self.releases = {
            "https://zenodo.org/api/files/6b2024bb-b37f-4a01-a78a-6d90f9d0cb90/2021-09-20-ror-data.zip": {
                "path": test_fixtures_folder("ror", "2021-09-20-ror-data.zip"),
                "download_hash": "60620675937e6513104275931331f68f",
                "extract_hash": "17931b9f766387d10778f121725c0fa1",
                "transform_hash": "2e6c12a9",
                "table_content": table_content,
            },
            "https://zenodo.org/api/files/ee5e3ae8-81a1-4f49-88ea-6feb09d4d0ac/2021-09-23-ror-data.zip": {
                "path": test_fixtures_folder("ror", "2021-09-23-ror-data.zip"),
                "download_hash": "0cac8705fba6df755648472356b7cb83",
                "extract_hash": "17931b9f766387d10778f121725c0fa1",
                "transform_hash": "2e6c12a9",
                "table_content": table_content,
            },
        }
        self.release = RorRelease("ror", pendulum.datetime(2021, 1, 1), "https://myurl")

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

        name = "RoR Telescope"
        workflow_type = WorkflowType(name=name, type_id=RorTelescope.DAG_ID)
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
            type_id="ror",
            name="ds type",
            extra={},
            table_type=TableType(id=1),
        )
        self.api.put_dataset_type(dataset_type)

        dataset = Dataset(
            name="RoR Dataset",
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

    def test_dag_structure(self):
        """Test that the ROR DAG has the correct structure.

        :return: None
        """

        dag = RorTelescope().make_dag()
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

    @patch("observatory.platform.utils.release_utils.make_observatory_api")
    def test_dag_load(self, m_makeapi):
        """Test that the ROR DAG can be loaded from a DAG bag.

        :return: None
        """

        m_makeapi.return_value = self.api
        env = ObservatoryEnvironment(self.project_id, self.data_location, api_host=self.host, api_port=self.port)

        with env.create():
            self.setup_connections(env)
            self.setup_api()
            dag_file = os.path.join(module_file_path("academic_observatory_workflows.dags"), "ror_telescope.py")
            self.assert_dag_load("ror", dag_file)

    @patch("observatory.platform.utils.release_utils.make_observatory_api")
    def test_telescope(self, m_makeapi):
        """Test the ROR telescope end to end.

        :return: None.
        """

        m_makeapi.return_value = self.api

        # Setup Observatory environment
        env = ObservatoryEnvironment(self.project_id, self.data_location, api_host=self.host, api_port=self.port)
        dataset_id = env.add_dataset()

        # Setup Telescope
        execution_date = pendulum.datetime(year=2021, month=9, day=19)
        telescope = RorTelescope(dataset_id=dataset_id, workflow_id=1)
        dag = telescope.make_dag()

        # Create the Observatory environment and run tests
        with env.create():
            self.setup_api()
            with env.create_dag_run(dag, execution_date):
                # Test that all dependencies are specified: no error should be thrown
                env.run_task(telescope.check_dependencies.__name__)

                # Test list releases task with files available
                with httpretty.enabled():
                    records_path = test_fixtures_folder("ror", "zenodo_records.json")
                    self.setup_mock_file_download(telescope.ROR_DATASET_URL, records_path)
                    ti = env.run_task(telescope.list_releases.__name__)

                records = ti.xcom_pull(
                    key=RorTelescope.RELEASE_INFO,
                    task_ids=telescope.list_releases.__name__,
                    include_prior_dates=False,
                )
                self.assertListEqual(
                    [
                        {
                            "release_date": "20210923",
                            "url": "https://zenodo.org/api/files/ee5e3ae8-81a1-4f49-88ea-6feb09d4d0ac/2021-09-23-ror-data.zip",
                        },
                        {
                            "release_date": "20210920",
                            "url": "https://zenodo.org/api/files/6b2024bb-b37f-4a01-a78a-6d90f9d0cb90/2021-09"
                            "-20-ror-data.zip",
                        },
                    ],
                    records,
                )

                # Use release info for other tasks
                releases = []
                for record in records:
                    release_date = record["release_date"]
                    url = record["url"]
                    releases.append(RorRelease(telescope.dag_id, pendulum.parse(release_date), url))

                # Test download task
                with httpretty.enabled():
                    for release in releases:
                        download_path = self.releases[release.url]["path"]
                        self.setup_mock_file_download(release.url, download_path)
                    env.run_task(telescope.download.__name__)
                for release in releases:
                    self.assertEqual(1, len(release.download_files))
                    download_hash = self.releases[release.url]["download_hash"]
                    self.assert_file_integrity(release.download_path, download_hash, "md5")

                # Test that file uploaded
                env.run_task(telescope.upload_downloaded.__name__)
                for release in releases:
                    self.assert_blob_integrity(
                        env.download_bucket, blob_name(release.download_path), release.download_path
                    )

                # Test that file extracted
                env.run_task(telescope.extract.__name__)
                for release in releases:
                    self.assertEqual(1, len(release.extract_files))
                    extract_hash = self.releases[release.url]["extract_hash"]
                    self.assert_file_integrity(release.extract_files[0], extract_hash, "md5")

                # Test that file transformed
                env.run_task(telescope.transform.__name__)
                for release in releases:
                    self.assertEqual(1, len(release.extract_files))
                    transform_hash = self.releases[release.url]["transform_hash"]
                    self.assert_file_integrity(release.transform_path, transform_hash, "gzip_crc")

                # Test that transformed file uploaded
                env.run_task(telescope.upload_transformed.__name__)
                for release in releases:
                    self.assert_blob_integrity(
                        env.transform_bucket, blob_name(release.transform_path), release.transform_path
                    )

                # Test that data loaded into BigQuery
                env.run_task(telescope.bq_load.__name__)
                for release in releases:
                    table_id = (
                        f"{self.project_id}.{dataset_id}."
                        f"{bigquery_sharded_table_id(telescope.dag_id, release.release_date)}"
                    )
                    expected_content = self.releases[release.url]["table_content"]
                    self.assert_table_content(table_id, expected_content)

                # Test that all telescope data deleted
                download_folders, extract_folders, transform_folders = (
                    [releases[0].download_folder, releases[1].download_folder],
                    [releases[0].extract_folder, releases[1].extract_folder],
                    [releases[0].transform_folder, releases[1].transform_folder],
                )
                env.run_task(telescope.cleanup.__name__)
                for i, release in enumerate(releases):
                    self.assert_cleanup(download_folders[i], extract_folders[i], transform_folders[i])

                # add_dataset_release_task
                dataset_releases = get_dataset_releases(dataset_id=1)
                self.assertEqual(len(dataset_releases), 0)
                ti = env.run_task("add_new_dataset_releases")
                self.assertEqual(ti.state, State.SUCCESS)
                dataset_releases = get_dataset_releases(dataset_id=1)
                self.assertEqual(len(dataset_releases), 2)

    @patch("academic_observatory_workflows.workflows.ror_telescope.list_ror_records")
    def test_list_releases(self, mock_list_records):
        """Test the list_releases method of the ROR telescope when there are no records

        :return: None
        """
        mock_list_records.return_value = []

        execution_date = pendulum.datetime(2020, 1, 1)
        next_execution_date = pendulum.date(2020, 2, 1)
        telescope = RorTelescope()

        continue_dag = telescope.list_releases(execution_date=execution_date, next_execution_date=next_execution_date)
        self.assertFalse(continue_dag)

    @patch("airflow.models.variable.Variable.get")
    def test_release_extract(self, mock_variable_get):
        """Test exceptions are raised for the extract method of the ROR release

        :return: None
        """
        mock_variable_get.return_value = "data_path"
        with CliRunner().isolated_filesystem():
            # Create file at download path that is not a zip file
            with open(self.release.download_path, "w") as f:
                f.write("test")

            # Test that exception is raised
            with self.assertRaises(AirflowException):
                self.release.extract()

    @patch("airflow.models.variable.Variable.get")
    def test_release_transform(self, mock_variable_get):
        """Test exceptions are raised for the transform method of the ROR release

        :return: None
        """
        mock_variable_get.return_value = "data_path"
        with CliRunner().isolated_filesystem():
            # Test exception is raised when there is more than one file
            file_path1 = os.path.join(self.release.extract_folder, "2020-01-01-ror-data.json")
            file_path2 = os.path.join(self.release.extract_folder, "2021-01-01-ror-data.json")
            for file in [file_path1, file_path2]:
                with open(file, "w") as f:
                    f.write("test")
            with self.assertRaises(AirflowException):
                self.release.transform()

        with CliRunner().isolated_filesystem():
            # Test exception is raised when there is no file (does not match regex pattern)
            file_path1 = os.path.join(self.release.extract_folder, "ror-data.json")
            with open(file_path1, "w") as f:
                f.write("test")
            with self.assertRaises(AirflowException):
                self.release.transform()

    def test_list_ror_records(self):
        """Test the list_ror_records function

        :return: None
        """
        start_date = pendulum.datetime(2020, 1, 1)
        end_date = pendulum.datetime(2020, 2, 1)

        # Test list records when there are no hits
        with httpretty.enabled():
            body = {
                "hits": {"hits": [], "total": 2},
                "links": {
                    "self": "https://zenodo.org/api/records/?sort=mostrecent&communities=ror-data&page=1&size=10"
                },
            }
            httpretty.register_uri(httpretty.GET, RorTelescope.ROR_DATASET_URL, body=json.dumps(body))
            records = list_ror_records(start_date, end_date)
            self.assertEqual([], records)

        # Test list records with a response code that is not 200
        with httpretty.enabled():
            httpretty.register_uri(httpretty.GET, RorTelescope.ROR_DATASET_URL, status=400)
            with self.assertRaises(AirflowException):
                list_ror_records(start_date, end_date)
