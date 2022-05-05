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

# Author: Tuan Chien

from gc import freeze
import os
import shutil
import unittest
from unittest.mock import patch

import pendulum
from academic_observatory_workflows.config import test_fixtures_folder
from academic_observatory_workflows.workflows.unpaywall_telescope import (
    UnpaywallRelease,
    UnpaywallTelescope,
)
from airflow.models.connection import Connection
from airflow.utils.state import State
from click.testing import CliRunner
from google.cloud import bigquery
from observatory.platform.utils.file_utils import validate_file_hash
from observatory.platform.utils.jinja2_utils import render_template
from observatory.platform.utils.release_utils import get_dataset_releases, get_latest_dataset_release
from observatory.platform.utils.test_utils import (
    HttpServer,
    ObservatoryEnvironment,
    ObservatoryTestCase,
    module_file_path,
)
from observatory.platform.utils.workflow_utils import blob_name, create_date_table_id
from observatory.api.testing import ObservatoryApiEnvironment
from observatory.api.client import ApiClient, Configuration
from observatory.api.client.api.observatory_api import ObservatoryApi  # noqa: E501
from observatory.api.client.model.organisation import Organisation
from observatory.api.client.model.workflow import Workflow
from observatory.api.client.model.workflow_type import WorkflowType
from observatory.api.client.model.dataset import Dataset
from observatory.api.client.model.dataset_release import DatasetRelease
from observatory.api.client.model.dataset_type import DatasetType
from observatory.api.client.model.table_type import TableType
from observatory.platform.utils.airflow_utils import AirflowConns
from airflow.models import Connection


class TestUnpaywallRelease(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.fixture_dir = test_fixtures_folder("unpaywall")
        self.snapshot_file = "unpaywall_2021-07-02T151134.jsonl.gz"
        self.snapshot_path = os.path.join(self.fixture_dir, self.snapshot_file)
        self.snapshot_hash = "0f1ac32355c4582d82ae4bc76db17c26"  # md5

        # API environment
        self.host = "localhost"
        self.port = 5000
        configuration = Configuration(host=f"http://{self.host}:{self.port}")
        api_client = ApiClient(configuration)
        self.api = ObservatoryApi(api_client=api_client)  # noqa: E501
        self.env = ObservatoryApiEnvironment(host=self.host, port=self.port)

    def setup_api(self):
        dt = pendulum.now("UTC")

        name = "Unpaywall Telescope"
        workflow_type = WorkflowType(name=name, type_id=UnpaywallTelescope.DAG_ID)
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
            type_id="dataset_type_id",
            name="ds type",
            extra={},
            table_type=TableType(id=1),
        )
        self.api.put_dataset_type(dataset_type)

        dataset = Dataset(
            name="Unpaywall Dataset",
            address="project.dataset.table",
            service="bigquery",
            workflow=Workflow(id=1),
            dataset_type=DatasetType(id=1),
        )
        self.api.put_dataset(dataset)

    @patch("academic_observatory_workflows.workflows.unpaywall_telescope.get_airflow_connection_password")
    def test_api_key(self, m_pass):
        m_pass.return_value = "testpass"
        self.assertEqual(UnpaywallRelease.api_key(), "testpass")

    @patch("academic_observatory_workflows.workflows.unpaywall_telescope.get_airflow_connection_password")
    def test_snapshot_url(self, m_pass):
        m_pass.return_value = "testpass"
        url = "https://api.unpaywall.org/feed/snapshot?api_key=testpass"
        self.assertEqual(UnpaywallRelease.snapshot_url(), url)

    @patch("academic_observatory_workflows.workflows.unpaywall_telescope.get_airflow_connection_password")
    def test_data_feed_url(self, m_pass):
        m_pass.return_value = "testpass"
        url = "https://api.unpaywall.org/feed/changefiles?interval=day&api_key=testpass"
        self.assertEqual(UnpaywallRelease.data_feed_url(), url)

    @patch("academic_observatory_workflows.workflows.unpaywall_telescope.UnpaywallRelease.get_diff_releases")
    @patch("academic_observatory_workflows.workflows.unpaywall_telescope.get_airflow_connection_password")
    @patch("academic_observatory_workflows.workflows.unpaywall_telescope.download_file")
    @patch("academic_observatory_workflows.workflows.unpaywall_telescope.get_observatory_http_header")
    @patch("airflow.models.variable.Variable.get")
    def test_download_data_feed(self, m_get, m_header, m_download, m_pass, m_diff_releases):
        m_get.return_value = "data"
        m_pass.return_value = "testpass"
        m_header.return_value = {"User-Agent": "custom"}
        m_diff_releases.return_value = [
            {
                "url": "http://url1",
                "filename": "changed_dois_with_versions_2021-07-02T080001.jsonl.gz",
            }
        ]

        release = UnpaywallRelease(
            dag_id="dag",
            start_date=pendulum.datetime(2021, 7, 4),
            end_date=pendulum.datetime(2021, 7, 4),
            first_release=False,
            workflow_id=1,
        )

        release.download()
        _, call_args = m_download.call_args
        self.assertEqual(call_args["url"], "http://url1")
        self.assertEqual(
            call_args["filename"],
            "data/telescopes/download/dag/2021_07_04-2021_07_04/changed_dois_with_versions_2021-07-02T080001.jsonl.gz",
        )

    @patch("academic_observatory_workflows.workflows.unpaywall_telescope.get_observatory_http_header")
    @patch("academic_observatory_workflows.workflows.unpaywall_telescope.get_airflow_connection_password")
    @patch("academic_observatory_workflows.workflows.unpaywall_telescope.download_file")
    @patch("airflow.models.variable.Variable.get")
    def test_download_snapshot(self, m_get, m_download, m_pass, m_header):
        m_get.return_value = "data"
        m_pass.return_value = "testpass"
        m_header.return_value = {"User-Agent": "custom"}

        with CliRunner().isolated_filesystem():
            release = UnpaywallRelease(
                dag_id="dag",
                start_date=pendulum.datetime(2021, 7, 2),
                end_date=pendulum.datetime(2021, 7, 3),
                first_release=True,
                workflow_id=1,
            )

            src = self.snapshot_path
            dst = os.path.join(release.download_folder, self.snapshot_file)
            shutil.copyfile(src, dst)

            release.download()

    @patch("academic_observatory_workflows.workflows.unpaywall_telescope.get_datasets")
    @patch("academic_observatory_workflows.workflows.unpaywall_telescope.UnpaywallRelease.get_unpaywall_daily_feeds")
    @patch("academic_observatory_workflows.workflows.unpaywall_telescope.get_dataset_releases")
    def test_get_diff_releases(self, m_ds_releases, m_dailyfeeds, m_ds):
        # First release
        m_ds_releases.return_value = [
            DatasetRelease(
                dataset=Dataset(id=1),
                start_date=pendulum.datetime(2021, 1, 20),
                end_date=pendulum.datetime(2021, 1, 20),
            )
        ]
        m_ds.return_value = [
            Dataset(
                name="dataset",
                service="bigquery",
                address="project.dataset.table",
                dataset_type=DatasetType(id=1),
            )
        ]
        m_dailyfeeds.return_value = [
            {"release_date": pendulum.datetime(2021, 1, 19), "url": "url1", "filename": "file1"},
            {"release_date": pendulum.datetime(2021, 1, 20), "url": "url2", "filename": "file2"},
            {"release_date": pendulum.datetime(2021, 1, 21), "url": "url3", "filename": "file3"},
            {"release_date": pendulum.datetime(2021, 1, 22), "url": "url4", "filename": "file4"},
        ]

        end_date = pendulum.datetime(2021, 1, 21)
        result = UnpaywallRelease.get_diff_releases(end_date=end_date, workflow_id=1)

        self.assertEqual(len(result), 3)
        self.assertEqual(result[0]["release_date"], pendulum.datetime(2021, 1, 19))
        self.assertEqual(result[1]["release_date"], pendulum.datetime(2021, 1, 20))
        self.assertEqual(result[2]["release_date"], pendulum.datetime(2021, 1, 21))

        # Subsequent release
        m_ds_releases.return_value = [
            DatasetRelease(
                dataset=Dataset(id=1),
                start_date=pendulum.datetime(2021, 1, 17),
                end_date=pendulum.datetime(2021, 1, 18),
            ),
            DatasetRelease(
                dataset=Dataset(id=1),
                start_date=pendulum.datetime(2021, 1, 18),
                end_date=pendulum.datetime(2021, 1, 19),
            ),
        ]
        end_date = pendulum.datetime(2021, 1, 21)
        result = UnpaywallRelease.get_diff_releases(end_date=end_date, workflow_id=1)

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["release_date"], pendulum.datetime(2021, 1, 20))
        self.assertEqual(result[1]["release_date"], pendulum.datetime(2021, 1, 21))

    @patch("airflow.models.variable.Variable.get")
    def test_extract(self, m_get):
        m_get.return_value = "data"
        with CliRunner().isolated_filesystem():
            release = UnpaywallRelease(
                dag_id="dag",
                start_date=pendulum.datetime(2021, 7, 4),
                end_date=pendulum.datetime(2021, 7, 4),
                first_release=True,
                workflow_id=1,
            )
            src = self.snapshot_path
            dst = os.path.join(release.download_folder, self.snapshot_file)
            shutil.copyfile(src, dst)
            self.assertEqual(len(release.download_files), 1)
            release.extract()
            self.assertEqual(len(release.extract_files), 1)

    @patch("airflow.models.variable.Variable.get")
    def test_transform(self, m_get):
        m_get.return_value = "data"
        with CliRunner().isolated_filesystem():
            release = UnpaywallRelease(
                dag_id="dag",
                start_date=pendulum.datetime(2021, 7, 4),
                end_date=pendulum.datetime(2021, 7, 4),
                first_release=True,
                workflow_id=1,
            )
            src = self.snapshot_path
            dst = os.path.join(release.download_folder, self.snapshot_file)
            shutil.copyfile(src, dst)
            release.extract()
            release.transform()
            self.assertEqual(len(release.transform_files), 1)

            json_transformed_hash = "62cbb5af5a78d2e0769a28d976971cba"
            json_transformed = os.path.join(release.transform_folder, self.snapshot_file[:-3])
            self.assertTrue(validate_file_hash(file_path=json_transformed, expected_hash=json_transformed_hash))

    @patch("observatory.platform.utils.release_utils.make_observatory_api")
    @patch("academic_observatory_workflows.workflows.unpaywall_telescope.get_dataset_releases")
    def test_is_second_run(self, m_get_ds_release, m_makeapi):
        m_makeapi.return_value = self.api

        with self.env.create():
            self.setup_api()
            m_get_ds_release.return_value = [
                DatasetRelease(
                    dataset=Dataset(id=1),
                    start_date=pendulum.datetime(2020, 1, 1),
                    end_date=pendulum.datetime(2020, 1, 1),
                )
            ]

            self.assertTrue(UnpaywallRelease.is_second_run(1))

    @patch("academic_observatory_workflows.workflows.unpaywall_telescope.get_http_response_json")
    @patch("academic_observatory_workflows.workflows.unpaywall_telescope.UnpaywallRelease.data_feed_url")
    def test_get_unpaywall_daily_feeds(self, m_url, m_response):
        m_url.return_value = "url"
        m_response.return_value = {"list": [{"filename": "unpaywall_2021-07-02T151134.jsonl.gz", "url": "url1"}]}

        feeds = UnpaywallRelease.get_unpaywall_daily_feeds()
        self.assertEqual(len(feeds), 1)
        self.assertEqual(feeds[0]["release_date"], pendulum.datetime(2021, 7, 2, 15, 11, 34))


class TestUnpaywallTelescope(ObservatoryTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")

        self.fixture_dir = test_fixtures_folder("unpaywall")
        self.snapshot_file = "unpaywall_2021-07-02T151134.jsonl.gz"
        self.snapshot_path = os.path.join(self.fixture_dir, self.snapshot_file)
        self.snapshot_hash = "0f1ac32355c4582d82ae4bc76db17c26"  # md5

        # API environment
        self.host = "localhost"
        self.port = 5001
        configuration = Configuration(host=f"http://{self.host}:{self.port}")
        api_client = ApiClient(configuration)
        self.api = ObservatoryApi(api_client=api_client)  # noqa: E501
        self.env = ObservatoryApiEnvironment(host=self.host, port=self.port)
        self.org_name = "Curtin University"

    def setup_api(self):
        org = Organisation(name=self.org_name)
        result = self.api.put_organisation(org)
        self.assertIsInstance(result, Organisation)

        tele_type = WorkflowType(type_id=UnpaywallTelescope.DAG_ID, name="Unpaywall")
        result = self.api.put_workflow_type(tele_type)
        self.assertIsInstance(result, WorkflowType)

        telescope = Workflow(organisation=Organisation(id=1), workflow_type=WorkflowType(id=1))
        result = self.api.put_workflow(telescope)
        self.assertIsInstance(result, Workflow)

        table_type = TableType(
            type_id="partitioned",
            name="partitioned bq table",
        )
        self.api.put_table_type(table_type)

        dataset_type = DatasetType(
            type_id="dataset_type_id",
            name="ds type",
            extra={},
            table_type=TableType(id=1),
        )
        self.api.put_dataset_type(dataset_type)

        dataset = Dataset(
            name="Unpaywall Dataset",
            address="project.dataset.table",
            service="bigquery",
            workflow=Workflow(id=1),
            dataset_type=DatasetType(id=1),
        )
        result = self.api.put_dataset(dataset)
        self.assertIsInstance(result, Dataset)

    def setup_connections(self, env):
        # Add Observatory API connection
        conn = Connection(conn_id=AirflowConns.OBSERVATORY_API, uri=f"http://:password@{self.host}:{self.port}")
        env.add_connection(conn)

    @patch("observatory.platform.utils.release_utils.make_observatory_api")
    def test_ctor(self, m_makeapi):
        m_makeapi.return_value = self.api

        with self.env.create():
            self.setup_api()

            telescope = UnpaywallTelescope(airflow_vars=[], workflow_id=1)
            self.assertEqual(telescope.airflow_vars, ["transform_bucket"])

    @patch("observatory.platform.utils.release_utils.make_observatory_api")
    def test_dag_structure(self, m_makeapi):
        """Test that the Crossref Events DAG has the correct structure."""

        m_makeapi.return_value = self.api
        with self.env.create():
            self.setup_api()
            dag = UnpaywallTelescope().make_dag()
            self.assert_dag_structure(
                {
                    "check_dependencies": ["check_releases"],
                    "check_releases": ["download"],
                    "download": ["upload_downloaded"],
                    "upload_downloaded": ["extract"],
                    "extract": ["transform"],
                    "transform": ["upload_transformed"],
                    "upload_transformed": ["bq_load_partition"],
                    "bq_load_partition": ["bq_delete_old"],
                    "bq_delete_old": ["bq_append_new"],
                    "bq_append_new": ["cleanup"],
                    "cleanup": ["add_new_dataset_releases"],
                    "add_new_dataset_releases": [],
                },
                dag,
            )

    @patch("observatory.platform.utils.release_utils.make_observatory_api")
    def test_dag_load(self, m_makeapi):
        """Test that the DAG can be loaded from a DAG bag."""

        m_makeapi.return_value = self.api
        env = ObservatoryEnvironment(self.project_id, self.data_location, api_host=self.host, api_port=self.port)

        with env.create():
            self.setup_connections(env)
            self.setup_api()
            dag_file = os.path.join(module_file_path("academic_observatory_workflows.dags"), "unpaywall_telescope.py")
            self.assert_dag_load("unpaywall", dag_file)

    def setup_observatory_environment(self):
        env = ObservatoryEnvironment(self.project_id, self.data_location, api_host=self.host, api_port=self.port)
        self.dataset_id = env.add_dataset()
        return env

    def create_changefiles(self, host, port):
        # Daily
        template_path = os.path.join(self.fixture_dir, "daily-feed", "changefiles.jinja2")
        changefiles = render_template(template_path, host=host, port=port)
        dst = os.path.join(self.fixture_dir, "daily-feed", "changefiles")
        with open(dst, "w") as f:
            f.write(changefiles)

    def remove_changefiles(self):
        dst = os.path.join(self.fixture_dir, "daily-feed", "changefiles")
        os.remove(dst)

    @patch("academic_observatory_workflows.workflows.unpaywall_telescope.get_filename_from_http_header")
    @patch("academic_observatory_workflows.workflows.unpaywall_telescope.UnpaywallRelease.snapshot_url")
    def test_get_snapshot_date(self, m_url, m_get_filename):
        m_url.return_value = "url"
        m_get_filename.return_value = "unpaywall_2021-07-02T151134.jsonl.gz"
        dt = UnpaywallTelescope._get_snapshot_date()
        self.assertEqual(dt, pendulum.datetime(2021, 7, 2, 15, 11, 34))

    # We want to do 3 dag runs.  First is to load snapshot.
    # Second run brings us up to date and applies the overlapping diffs from before snapshot.
    # Third checks the "all other situations" case
    @patch("academic_observatory_workflows.workflows.unpaywall_telescope.get_filename_from_http_header")
    @patch("observatory.platform.utils.release_utils.make_observatory_api")
    def test_telescope_day(self, m_makeapi, m_http_response):
        m_makeapi.return_value = self.api
        m_http_response.return_value = self.snapshot_file

        env = self.setup_observatory_environment()

        first_execution_date = pendulum.datetime(2021, 7, 3)  # Snapshot
        second_execution_date = pendulum.datetime(2021, 7, 4)  # Not enough updates found
        third_execution_date = pendulum.datetime(2021, 7, 5)  # Updates found
        fourth_execution_date = pendulum.datetime(2021, 7, 6)  # No updates found

        with env.create(task_logging=True):
            self.setup_api()
            server = HttpServer(directory=self.fixture_dir)
            with server.create():
                with patch.object(
                    UnpaywallRelease, "SNAPSHOT_URL", f"http://{server.host}:{server.port}/{self.snapshot_file}"
                ):
                    with patch.object(
                        UnpaywallRelease,
                        "CHANGEFILES_URL",
                        f"http://{server.host}:{server.port}/daily-feed/changefiles",
                    ):
                        self.create_changefiles(server.host, server.port)

                        conn = Connection(
                            conn_id=UnpaywallRelease.AIRFLOW_CONNECTION, uri="http://:YOUR_API_KEY@localhost"
                        )

                        env.add_connection(conn)

                        telescope = UnpaywallTelescope(dataset_id=self.dataset_id, workflow_id=1)
                        dag = telescope.make_dag()

                        # First run
                        with env.create_dag_run(dag, first_execution_date):
                            # Check dependencies are met
                            ti = env.run_task(telescope.check_dependencies.__name__)
                            self.assertEqual(ti.state, State.SUCCESS)

                            # Check releases
                            ti = env.run_task(telescope.check_releases.__name__)
                            self.assertEqual(ti.state, State.SUCCESS)

                            release = telescope.make_release(**{"ti": ti})

                            # Download data
                            ti = env.run_task(telescope.download.__name__)
                            self.assertEqual(ti.state, State.SUCCESS)

                            # Upload downloaded data
                            ti = env.run_task(telescope.upload_downloaded.__name__)
                            self.assertEqual(ti.state, State.SUCCESS)
                            for file in release.download_files:
                                self.assert_blob_integrity(env.download_bucket, blob_name(file), file)

                            # Extract data
                            ti = env.run_task(telescope.extract.__name__)
                            self.assertEqual(ti.state, State.SUCCESS)

                            # Transform data
                            ti = env.run_task(telescope.transform.__name__)
                            self.assertEqual(ti.state, State.SUCCESS)
                            self.assertEqual(len(release.transform_files), 1)

                            # Upload transformed data
                            ti = env.run_task(telescope.upload_transformed.__name__)
                            self.assertEqual(ti.state, State.SUCCESS)
                            for file in release.transform_files:
                                self.assert_blob_integrity(env.transform_bucket, blob_name(file), file)

                            # Load bq table partitions
                            ti = env.run_task(telescope.bq_load_partition.__name__)
                            self.assertEqual(ti.state, State.SUCCESS)

                            # Delete changed data from main table
                            with patch("observatory.platform.utils.gc_utils.bq_query_bytes_daily_limit_check"):
                                ti = env.run_task(telescope.bq_delete_old.__name__)
                            self.assertEqual(ti.state, State.SUCCESS)

                            # Add new changes
                            ti = env.run_task(telescope.bq_append_new.__name__)
                            self.assertEqual(ti.state, State.SUCCESS)
                            main_table_id, partition_table_id = release.dag_id, f"{release.dag_id}_partitions"
                            table_id = f"{self.project_id}.{telescope.dataset_id}.{main_table_id}"
                            expected_rows = 100
                            self.assert_table_integrity(table_id, expected_rows)

                            # Cleanup files
                            download_folder, extract_folder, transform_folder = (
                                release.download_folder,
                                release.extract_folder,
                                release.transform_folder,
                            )
                            ti = env.run_task(telescope.cleanup.__name__)
                            self.assertEqual(ti.state, State.SUCCESS)
                            self.assert_cleanup(download_folder, extract_folder, transform_folder)

                            # Load update_dataset_release_task
                            dataset_releases = get_dataset_releases(dataset_id=1)
                            self.assertEqual(len(dataset_releases), 0)
                            ti = env.run_task(telescope.add_new_dataset_releases.__name__)
                            self.assertEqual(ti.state, State.SUCCESS)
                            dataset_releases = get_dataset_releases(dataset_id=1)
                            self.assertEqual(len(dataset_releases), 1)
                            self.assertEqual(dataset_releases[0].start_date, release.start_date)
                            self.assertEqual(dataset_releases[0].end_date, release.end_date)

                        # Second run (skips)  Use dailies
                        with env.create_dag_run(dag, second_execution_date):
                            # Check dependencies are met
                            ti = env.run_task(telescope.check_dependencies.__name__)
                            self.assertEqual(ti.state, State.SUCCESS)

                            # Check releases
                            ti = env.run_task(telescope.check_releases.__name__)
                            self.assertEqual(ti.state, State.SUCCESS)

                            # Download data
                            ti = env.run_task(telescope.download.__name__)
                            self.assertEqual(ti.state, State.SKIPPED)

                            # Upload downloaded data
                            ti = env.run_task(telescope.upload_downloaded.__name__)
                            self.assertEqual(ti.state, State.SKIPPED)

                            # Extract data
                            ti = env.run_task(telescope.extract.__name__)
                            self.assertEqual(ti.state, State.SKIPPED)

                            # Transform data
                            ti = env.run_task(telescope.transform.__name__)
                            self.assertEqual(ti.state, State.SKIPPED)
                            self.assertEqual(len(release.transform_files), 0)

                            # Upload transformed data
                            ti = env.run_task(telescope.upload_transformed.__name__)
                            self.assertEqual(ti.state, State.SKIPPED)

                            # Load bq table partitions
                            ti = env.run_task(telescope.bq_load_partition.__name__)
                            self.assertEqual(ti.state, State.SKIPPED)

                            # Delete changed data from main table
                            with patch("observatory.platform.utils.gc_utils.bq_query_bytes_daily_limit_check"):
                                ti = env.run_task(telescope.bq_delete_old.__name__)
                            self.assertEqual(ti.state, State.SKIPPED)

                            # Add new changes
                            ti = env.run_task(telescope.bq_append_new.__name__)
                            self.assertEqual(ti.state, State.SKIPPED)

                            # Cleanup files
                            ti = env.run_task(telescope.cleanup.__name__)
                            self.assertEqual(ti.state, State.SKIPPED)

                            # Load add_new_dataset_releases
                            ti = env.run_task(telescope.add_new_dataset_releases.__name__)
                            self.assertEqual(ti.state, State.SKIPPED)

                        # Third run (downloads)
                        with env.create_dag_run(dag, third_execution_date):
                            # Check dependencies are met
                            ti = env.run_task(telescope.check_dependencies.__name__)
                            self.assertEqual(ti.state, State.SUCCESS)

                            # Check releases
                            ti = env.run_task(telescope.check_releases.__name__)
                            self.assertEqual(ti.state, State.SUCCESS)

                            release = telescope.make_release(**{"ti": ti})

                            # Download data
                            ti = env.run_task(telescope.download.__name__)
                            self.assertEqual(ti.state, State.SUCCESS)

                            # Upload downloaded data
                            ti = env.run_task(telescope.upload_downloaded.__name__)
                            self.assertEqual(ti.state, State.SUCCESS)
                            for file in release.download_files:
                                self.assert_blob_integrity(env.download_bucket, blob_name(file), file)

                            # Extract data
                            ti = env.run_task(telescope.extract.__name__)
                            self.assertEqual(ti.state, State.SUCCESS)

                            # Transform data
                            ti = env.run_task(telescope.transform.__name__)
                            self.assertEqual(ti.state, State.SUCCESS)
                            self.assertEqual(len(release.transform_files), 3)

                            # Upload transformed data
                            ti = env.run_task(telescope.upload_transformed.__name__)
                            self.assertEqual(ti.state, State.SUCCESS)
                            for file in release.transform_files:
                                self.assert_blob_integrity(env.transform_bucket, blob_name(file), file)

                            # Load bq table partitions
                            ti = env.run_task(telescope.bq_load_partition.__name__)
                            self.assertEqual(ti.state, State.SUCCESS)
                            main_table_id, partition_table_id = release.dag_id, f"{release.dag_id}_partitions"
                            table_id = create_date_table_id(
                                partition_table_id, release.end_date, bigquery.TimePartitioningType.DAY
                            )
                            table_id = f"{self.project_id}.{telescope.dataset_id}.{table_id}"
                            expected_rows = 6
                            self.assert_table_integrity(table_id, expected_rows)

                            # Delete changed data from main table
                            with patch("observatory.platform.utils.gc_utils.bq_query_bytes_daily_limit_check"):
                                ti = env.run_task(telescope.bq_delete_old.__name__)
                            self.assertEqual(ti.state, State.SUCCESS)
                            table_id = f"{self.project_id}.{telescope.dataset_id}.{main_table_id}"
                            expected_rows = 99
                            self.assert_table_integrity(table_id, expected_rows)

                            # Add new changes
                            ti = env.run_task(telescope.bq_append_new.__name__)
                            self.assertEqual(ti.state, State.SUCCESS)
                            table_id = f"{self.project_id}.{telescope.dataset_id}.{main_table_id}"
                            expected_rows = 105
                            self.assert_table_integrity(table_id, expected_rows)

                            # Cleanup files
                            download_folder, extract_folder, transform_folder = (
                                release.download_folder,
                                release.extract_folder,
                                release.transform_folder,
                            )
                            ti = env.run_task(telescope.cleanup.__name__)
                            self.assertEqual(ti.state, State.SUCCESS)
                            self.assert_cleanup(download_folder, extract_folder, transform_folder)

                            # Load update_dataset_release_task
                            dataset_releases = get_dataset_releases(dataset_id=1)
                            self.assertEqual(len(dataset_releases), 1)
                            ti = env.run_task(telescope.add_new_dataset_releases.__name__)
                            self.assertEqual(ti.state, State.SUCCESS)
                            dataset_releases = get_dataset_releases(dataset_id=1)
                            latest_release = get_latest_dataset_release(dataset_releases)
                            self.assertEqual(len(dataset_releases), 2)
                            self.assertEqual(latest_release.start_date, release.start_date)
                            self.assertEqual(latest_release.end_date, release.end_date)

                        # Fourth run. No new data
                        with env.create_dag_run(dag, fourth_execution_date):
                            # Check dependencies are met
                            ti = env.run_task(telescope.check_dependencies.__name__)
                            self.assertEqual(ti.state, State.SUCCESS)

                            # Check releases
                            with patch(
                                "academic_observatory_workflows.workflows.unpaywall_telescope.UnpaywallRelease.get_diff_releases"
                            ) as m_diff_releases:
                                m_diff_releases.return_value = []
                                ti = env.run_task(telescope.check_releases.__name__)
                                self.assertEqual(ti.state, State.SUCCESS)

                            # Download data
                            ti = env.run_task(telescope.download.__name__)
                            self.assertEqual(ti.state, State.SKIPPED)

                        # Clean up template
                        self.remove_changefiles()
