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

import os
import shutil
import unittest
from unittest.mock import patch

import pendulum
from academic_observatory_workflows.config import test_fixtures_folder
from academic_observatory_workflows.workflows.unpaywall_data_feed_telescope import (
    UnpaywallDataFeedRelease,
    UnpaywallDataFeedTelescope,
)
from airflow.exceptions import AirflowException
from airflow.models.connection import Connection
from airflow.utils.state import State
from click.testing import CliRunner
from observatory.platform.utils.file_utils import validate_file_hash
from observatory.platform.utils.test_utils import (
    HttpServer,
    ObservatoryEnvironment,
    ObservatoryTestCase,
    module_file_path,
)
from observatory.platform.utils.workflow_utils import (
    bigquery_sharded_table_id,
    blob_name,
)


class TestUnpaywallDataFeedRelease(unittest.TestCase):
    def test_ctor(self):
        # OK
        release = UnpaywallDataFeedRelease(
            dag_id="dag",
            start_date=pendulum.now(),
            end_date=pendulum.now(),
            first_release=True,
            max_download_connections=1,
            update_interval="day",
        )

        # Invalid update interval
        self.assertRaises(
            AirflowException,
            UnpaywallDataFeedRelease,
            dag_id="dag",
            start_date=pendulum.now(),
            end_date=pendulum.now(),
            first_release=True,
            max_download_connections=1,
            update_interval="invalid",
        )

    @patch("academic_observatory_workflows.workflows.unpaywall_data_feed_telescope.get_airflow_connection_password")
    def test_api_key(self, m_pass):
        m_pass.return_value = "testpass"
        release = UnpaywallDataFeedRelease(
            dag_id="dag",
            start_date=pendulum.now(),
            end_date=pendulum.now(),
            first_release=True,
            max_download_connections=1,
            update_interval="day",
        )
        self.assertEqual(release.api_key, "testpass")

    @patch("academic_observatory_workflows.workflows.unpaywall_data_feed_telescope.get_airflow_connection_password")
    def test_snapshot_url(self, m_pass):
        m_pass.return_value = "testpass"
        release = UnpaywallDataFeedRelease(
            dag_id="dag",
            start_date=pendulum.now(),
            end_date=pendulum.now(),
            first_release=True,
            max_download_connections=1,
            update_interval="day",
        )
        url = "https://api.unpaywall.org/feed/snapshot?api_key=testpass"
        self.assertEqual(release.snapshot_url, url)

    @patch("academic_observatory_workflows.workflows.unpaywall_data_feed_telescope.get_airflow_connection_password")
    def test_data_feed_url(self, m_pass):
        m_pass.return_value = "testpass"
        release = UnpaywallDataFeedRelease(
            dag_id="dag",
            start_date=pendulum.now(),
            end_date=pendulum.now(),
            first_release=True,
            max_download_connections=1,
            update_interval="day",
        )
        url = "https://api.unpaywall.org/feed/changefiles?interval=day&api_key=testpass"
        self.assertEqual(release.data_feed_url, url)

    @patch("academic_observatory_workflows.workflows.unpaywall_data_feed_telescope.get_airflow_connection_password")
    @patch("academic_observatory_workflows.workflows.unpaywall_data_feed_telescope.download_files")
    @patch("academic_observatory_workflows.workflows.unpaywall_data_feed_telescope.get_observatory_http_header")
    @patch("academic_observatory_workflows.workflows.unpaywall_data_feed_telescope.get_http_response_json")
    @patch("airflow.models.variable.Variable.get")
    def test_download_data_feed(self, m_get, m_get_response, m_header, m_download, m_pass):
        m_get.return_value = "data"
        m_pass.return_value = "testpass"
        m_header.return_value = {"User-Agent": "custom"}

        # Day
        m_get_response.return_value = [
            {"date": "2021-01-01", "url": "http://url1", "filename": "file1"},
            {"date": "2020-01-01", "url": "http://url2", "filename": "file2"},
        ]

        release = UnpaywallDataFeedRelease(
            dag_id="dag",
            start_date=pendulum.datetime(2021, 1, 1),
            end_date=pendulum.datetime(2021, 1, 3),
            first_release=True,
            max_download_connections=1,
            update_interval="day",
        )

        release.download_data_feed_()
        _, call_args = m_download.call_args
        download_list = call_args["download_list"]
        self.assertEqual(len(download_list), 1)
        self.assertEqual(download_list[0]["url"], "http://url1")
        self.assertEqual(download_list[0]["filename"], "data/telescopes/download/dag/2021_01_01-2021_01_03/file1")

        # Week
        m_get_response.return_value = [
            {"to_date": "2021-01-01", "url": "http://url1", "filename": "file1"},
            {"to_date": "2020-01-01", "url": "http://url2", "filename": "file2"},
        ]

        release = UnpaywallDataFeedRelease(
            dag_id="dag",
            start_date=pendulum.datetime(2021, 1, 1),
            end_date=pendulum.datetime(2021, 1, 3),
            first_release=True,
            max_download_connections=1,
            update_interval="week",
        )

        release.download_data_feed_()
        _, call_args = m_download.call_args
        download_list = call_args["download_list"]
        self.assertEqual(len(download_list), 1)
        self.assertEqual(download_list[0]["url"], "http://url1")
        self.assertEqual(download_list[0]["filename"], "data/telescopes/download/dag/2021_01_01-2021_01_03/file1")

    def test_download_snapshot(self):
        pass

    @patch("airflow.models.variable.Variable.get")
    def test_extract(self, m_get):
        m_get.return_value = "data"
        fixture_dir = test_fixtures_folder("unpaywall_data_feed")
        with CliRunner().isolated_filesystem():
            release = UnpaywallDataFeedRelease(
                dag_id="dag",
                start_date=pendulum.datetime(2021, 1, 1),
                end_date=pendulum.datetime(2021, 1, 3),
                first_release=True,
                max_download_connections=1,
                update_interval="week",
            )
            src = os.path.join(fixture_dir, "unpaywall.jsonl.gz")
            dst = os.path.join(release.download_folder, "unpaywall.jsonl.gz")
            shutil.copyfile(src, dst)
            self.assertEqual(len(release.download_files), 1)
            release.extract()
            self.assertEqual(len(release.extract_files), 1)

    @patch("airflow.models.variable.Variable.get")
    def test_transform(self, m_get):
        m_get.return_value = "data"
        fixture_dir = test_fixtures_folder("unpaywall_data_feed")
        with CliRunner().isolated_filesystem():
            release = UnpaywallDataFeedRelease(
                dag_id="dag",
                start_date=pendulum.datetime(2021, 1, 1),
                end_date=pendulum.datetime(2021, 1, 3),
                first_release=True,
                max_download_connections=1,
                update_interval="week",
            )
            src = os.path.join(fixture_dir, "unpaywall.jsonl.gz")
            dst = os.path.join(release.download_folder, "unpaywall.jsonl.gz")
            shutil.copyfile(src, dst)
            src = os.path.join(fixture_dir, "unpaywall1.csv.gz")
            dst = os.path.join(release.download_folder, "unpaywall1.csv.gz")
            shutil.copyfile(src, dst)
            release.extract()
            release.transform()
            self.assertEqual(len(release.transform_files), 2)

            csv_transformed_hash = "5ec6414a16ce7af18855c146534f80c7"
            json_transformed_hash = "62cbb5af5a78d2e0769a28d976971cba"

            json_transformed = os.path.join(release.transform_folder, "unpaywall.jsonl")
            csv_transformed = os.path.join(release.transform_folder, "unpaywall1.csv.jsonl")
            self.assertTrue(validate_file_hash(file_path=csv_transformed, expected_hash=csv_transformed_hash))
            self.assertTrue(validate_file_hash(file_path=json_transformed, expected_hash=json_transformed_hash))


class TestUnpaywallDataFeedTelescope(ObservatoryTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")
        self.first_execution_date = pendulum.datetime(2021, 7, 18)
        self.second_execution_date = pendulum.datetime(2021, 2, 19)
        self.fixture_dir = test_fixtures_folder("unpaywall_data_feed")

    def test_ctor(self):
        telescope = UnpaywallDataFeedTelescope(airflow_vars=[])
        self.assertEqual(telescope.airflow_vars, ["transform_bucket"])

    def test_dag_structure(self):
        """Test that the Crossref Events DAG has the correct structure."""

        dag = UnpaywallDataFeedTelescope().make_dag()
        self.assert_dag_structure(
            {
                "check_dependencies": ["get_release_info"],
                "get_release_info": ["download"],
                "download": ["upload_downloaded"],
                "upload_downloaded": ["extract"],
                "extract": ["transform"],
                "transform": ["upload_transformed"],
                "upload_transformed": ["bq_load_partition"],
                "bq_load_partition": ["bq_delete_old"],
                "bq_delete_old": ["bq_append_new"],
                "bq_append_new": ["cleanup"],
                "cleanup": [],
            },
            dag,
        )

    def test_dag_load(self):
        """Test that the DAG can be loaded from a DAG bag."""

        with ObservatoryEnvironment().create():
            dag_file = os.path.join(
                module_file_path("academic_observatory_workflows.dags"), "unpaywall_data_feed_telescope.py"
            )
            self.assert_dag_load("unpaywall_data_feed", dag_file)

    def setup_observatory_environment(self):
        env = ObservatoryEnvironment(self.project_id, self.data_location)
        self.dataset_id = env.add_dataset()
        return env

    def test_telescope(self):
        env = self.setup_observatory_environment()

        with env.create():
            server = HttpServer(directory=self.fixture_dir)
            with server.create():
                with patch.object(
                    UnpaywallDataFeedRelease, "SNAPSHOT_URL", f"http://{server.host}:{server.port}/feed/snapshot.json"
                ):
                    conn = Connection(
                        conn_id=UnpaywallDataFeedRelease.AIRFLOW_CONNECTION, uri="http://:YOUR_API_KEY@localhost"
                    )

                    env.add_connection(conn)

                    telescope = UnpaywallDataFeedTelescope(dataset_id=self.dataset_id)
                    dag = telescope.make_dag()

                    # First run
                    with env.create_dag_run(dag, self.first_execution_date):
                        release = UnpaywallDataFeedRelease(
                            dag_id=UnpaywallDataFeedTelescope.DAG_ID,
                            start_date=pendulum.datetime(2021, 7, 2),
                            end_date=pendulum.datetime(2021, 7, 18),
                            first_release=True,
                            max_download_connections=1,
                            update_interval="day",
                        )

                        # Check dependencies are met
                        ti = env.run_task(telescope.check_dependencies.__name__)
                        self.assertEqual(ti.state, State.SUCCESS)

                        # Get release information
                        ti = env.run_task(telescope.get_release_info.__name__)
                        self.assertEqual(ti.state, State.SUCCESS)

                        # Download data
                        ti = env.run_task(telescope.download.__name__)
                        self.assertEqual(ti.state, State.SUCCESS)

                        # Upload downloaded data
                        ti = env.run_task(telescope.upload_downloaded.__name__)
                        self.assertEqual(ti.state, State.SUCCESS)

                        # Extract data
                        ti = env.run_task(telescope.extract.__name__)
                        self.assertEqual(ti.state, State.SUCCESS)

                        # Transform data
                        ti = env.run_task(telescope.transform.__name__)
                        self.assertEqual(ti.state, State.SUCCESS)

                        # Upload transformed data
                        ti = env.run_task(telescope.upload_transformed.__name__)
                        self.assertEqual(ti.state, State.SUCCESS)

                        # Load bq table partitions
                        ti = env.run_task(telescope.bq_load_partition.__name__)
                        self.assertEqual(ti.state, State.SKIPPED)

                        # Delete changed data from main table
                        ti = env.run_task(telescope.bq_delete_old.__name__)
                        self.assertEqual(ti.state, State.SUCCESS)

                        # Add new changes
                        ti = env.run_task(telescope.bq_append_new.__name__)
                        self.assertEqual(ti.state, State.SUCCESS)

                        # Cleanup files
                        download_folder, extract_folder, transform_folder = (
                            release.download_folder,
                            release.extract_folder,
                            release.transform_folder,
                        )
                        ti = env.run_task(telescope.cleanup.__name__)
                        self.assertEqual(ti.state, State.SUCCESS)
                        self.assert_cleanup(download_folder, extract_folder, transform_folder)

                    # # Second run
                    with env.create_dag_run(dag, self.second_execution_date):
                        release = UnpaywallDataFeedRelease(
                            dag_id=UnpaywallDataFeedTelescope.DAG_ID,
                            start_date=pendulum.datetime(2021, 7, 19),
                            end_date=pendulum.datetime(2021, 7, 19),
                            first_release=False,
                            max_download_connections=1,
                            update_interval="day",
                        )

                        # Check dependencies are met
                        ti = env.run_task(telescope.check_dependencies.__name__)
                        self.assertEqual(ti.state, State.SUCCESS)

                        # Get release information
                        ti = env.run_task(telescope.get_release_info.__name__)
                        self.assertEqual(ti.state, State.SUCCESS)

                        # Download data
                        ti = env.run_task(telescope.download.__name__)
                        self.assertEqual(ti.state, State.SUCCESS)

                        # Upload downloaded data
                        ti = env.run_task(telescope.upload_downloaded.__name__)
                        self.assertEqual(ti.state, State.SUCCESS)

                        # Extract data
                        ti = env.run_task(telescope.extract.__name__)
                        self.assertEqual(ti.state, State.SUCCESS)

                        # Transform data
                        ti = env.run_task(telescope.transform.__name__)
                        self.assertEqual(ti.state, State.SUCCESS)

                        # Upload transformed data
                        ti = env.run_task(telescope.upload_transformed.__name__)
                        self.assertEqual(ti.state, State.SUCCESS)

                        # Load bq table partitions
                        ti = env.run_task(telescope.bq_load_partition.__name__)
                        self.assertEqual(ti.state, State.SUCCESS)

                        # Delete changed data from main table
                        ti = env.run_task(telescope.bq_delete_old.__name__)
                        self.assertEqual(ti.state, State.SUCCESS)

                        # Add new changes
                        ti = env.run_task(telescope.bq_append_new.__name__)
                        self.assertEqual(ti.state, State.SUCCESS)

                        # Cleanup files
                        download_folder, extract_folder, transform_folder = (
                            release.download_folder,
                            release.extract_folder,
                            release.transform_folder,
                        )
                        ti = env.run_task(telescope.cleanup.__name__)
                        self.assertEqual(ti.state, State.SUCCESS)
                        self.assert_cleanup(download_folder, extract_folder, transform_folder)
