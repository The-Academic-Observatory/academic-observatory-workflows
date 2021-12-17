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
from datetime import timedelta
from unittest.mock import patch

import pendulum
from academic_observatory_workflows.config import test_fixtures_folder
from academic_observatory_workflows.workflows.unpaywall_telescope import (
    UnpaywallRelease,
    UnpaywallTelescope,
)
from airflow.exceptions import AirflowException
from airflow.models.connection import Connection
from airflow.utils.state import State
from click.testing import CliRunner
from google.cloud import bigquery
from observatory.platform.utils.file_utils import validate_file_hash
from observatory.platform.utils.jinja2_utils import render_template
from observatory.platform.utils.test_utils import (
    HttpServer,
    ObservatoryEnvironment,
    ObservatoryTestCase,
    module_file_path,
)
from observatory.platform.utils.workflow_utils import blob_name, create_date_table_id


class TestUnpaywallRelease(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.fixture_dir = test_fixtures_folder("unpaywall")
        self.snapshot_file = "unpaywall_2021-07-02T151134.jsonl.gz"
        self.snapshot_path = os.path.join(self.fixture_dir, self.snapshot_file)
        self.snapshot_hash = "0f1ac32355c4582d82ae4bc76db17c26"  # md5

    @patch("academic_observatory_workflows.workflows.unpaywall_telescope.get_airflow_connection_password")
    def test_api_key(self, m_pass):
        m_pass.return_value = "testpass"
        release = UnpaywallRelease(
            dag_id="dag",
            start_date=pendulum.now(),
            end_date=pendulum.now(),
            first_release=True,
        )
        self.assertEqual(release.api_key, "testpass")

    @patch("academic_observatory_workflows.workflows.unpaywall_telescope.get_airflow_connection_password")
    def test_snapshot_url(self, m_pass):
        m_pass.return_value = "testpass"
        release = UnpaywallRelease(
            dag_id="dag",
            start_date=pendulum.now(),
            end_date=pendulum.now(),
            first_release=True,
        )
        url = "https://api.unpaywall.org/feed/snapshot?api_key=testpass"
        self.assertEqual(release.snapshot_url, url)

    @patch("academic_observatory_workflows.workflows.unpaywall_telescope.get_airflow_connection_password")
    def test_data_feed_url(self, m_pass):
        m_pass.return_value = "testpass"
        release = UnpaywallRelease(
            dag_id="dag",
            start_date=pendulum.now(),
            end_date=pendulum.now(),
            first_release=True,
        )
        url = "https://api.unpaywall.org/feed/changefiles?interval=day&api_key=testpass"
        self.assertEqual(release.data_feed_url, url)

    @patch("academic_observatory_workflows.workflows.unpaywall_telescope.get_airflow_connection_password")
    @patch("academic_observatory_workflows.workflows.unpaywall_telescope.download_file")
    @patch("academic_observatory_workflows.workflows.unpaywall_telescope.get_observatory_http_header")
    @patch("academic_observatory_workflows.workflows.unpaywall_telescope.get_http_response_json")
    @patch("airflow.models.variable.Variable.get")
    def test_download_data_feed(self, m_get, m_get_response, m_header, m_download, m_pass):
        m_get.return_value = "data"
        m_pass.return_value = "testpass"
        m_header.return_value = {"User-Agent": "custom"}

        # Day
        m_get_response.return_value = {
            "list": [
                {
                    "url": "http://url1",
                    "filename": "changed_dois_with_versions_2021-07-02T080001.jsonl.gz",
                },
                {
                    "url": "http://url2",
                    "filename": "changed_dois_with_versions_2021-07-02T080001.jsonl.gz",
                },
            ]
        }

        release = UnpaywallRelease(
            dag_id="dag",
            start_date=pendulum.datetime(2021, 7, 4),
            end_date=pendulum.datetime(2021, 7, 4),
            first_release=False,
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

        fixture_dir = test_fixtures_folder("unpaywall")

        with CliRunner().isolated_filesystem():
            release = UnpaywallRelease(
                dag_id="dag",
                start_date=pendulum.datetime(2021, 7, 2),
                end_date=pendulum.datetime(2021, 7, 3),
                first_release=True,
            )

            src = self.snapshot_path
            dst = os.path.join(release.download_folder, self.snapshot_file)
            shutil.copyfile(src, dst)

            release.download()

        # Bad dates
        with CliRunner().isolated_filesystem():
            release = UnpaywallRelease(
                dag_id="dag",
                start_date=pendulum.datetime(2021, 9, 22),
                end_date=pendulum.datetime(2021, 1, 3),
                first_release=True,
            )

            src = self.snapshot_path
            dst = os.path.join(release.download_folder, self.snapshot_file)
            shutil.copyfile(src, dst)
            self.assertRaises(AirflowException, release.download)

    @patch("academic_observatory_workflows.workflows.unpaywall_telescope.get_http_response_json")
    def test_get_diff_release(self, m_get_json):
        # No release info
        m_get_json.return_value = {"list": []}

        result = UnpaywallRelease.get_diff_release(feed_url=None, start_date=None)

        self.assertEqual(result, (None, None))

        m_get_json.return_value = {
            "list": [
                {"url": "url", "filename": "changed_dois_with_versions_2021-07-02T080001.jsonl.gz"},
                {"url": "url", "filename": "changed_dois_with_versions_2021-07-02T080001.jsonl.gz"},
                {"url": "url", "filename": "changed_dois_with_versions_2021-07-02T080001.jsonl.gz"},
            ]
        }

        url, filename = UnpaywallRelease.get_diff_release(
            feed_url=None,
            start_date=pendulum.datetime(2021, 7, 4),
        )

        self.assertEqual(filename, "changed_dois_with_versions_2021-07-02T080001.jsonl.gz")

    @patch("airflow.models.variable.Variable.get")
    def test_extract(self, m_get):
        m_get.return_value = "data"
        fixture_dir = test_fixtures_folder("unpaywall")
        with CliRunner().isolated_filesystem():
            release = UnpaywallRelease(
                dag_id="dag",
                start_date=pendulum.datetime(2021, 7, 4),
                end_date=pendulum.datetime(2021, 7, 4),
                first_release=True,
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
        fixture_dir = test_fixtures_folder("unpaywall")
        with CliRunner().isolated_filesystem():
            release = UnpaywallRelease(
                dag_id="dag",
                start_date=pendulum.datetime(2021, 7, 4),
                end_date=pendulum.datetime(2021, 7, 4),
                first_release=True,
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


class TestUnpaywallTelescope(ObservatoryTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")

        self.fixture_dir = test_fixtures_folder("unpaywall")
        self.snapshot_file = "unpaywall_2021-07-02T151134.jsonl.gz"
        self.snapshot_path = os.path.join(self.fixture_dir, self.snapshot_file)
        self.snapshot_hash = "0f1ac32355c4582d82ae4bc76db17c26"  # md5

    def test_ctor(self):
        telescope = UnpaywallTelescope(airflow_vars=[])
        self.assertEqual(telescope.airflow_vars, ["transform_bucket"])

        self.assertRaises(AirflowException, UnpaywallTelescope, schedule_interval="@monthly")

    def test_schedule_days_apart(self):
        start_date = pendulum.datetime(2021, 1, 9)
        schedule_interval = timedelta(days=2)
        days_apart_gen = UnpaywallTelescope._schedule_days_apart(
            start_date=start_date, schedule_interval=schedule_interval
        )

        diff = next(days_apart_gen)
        self.assertEqual(diff, 2)
        diff = next(days_apart_gen)
        self.assertEqual(diff, 2)

        schedule_interval = "@weekly"
        days_apart_gen = UnpaywallTelescope._schedule_days_apart(
            start_date=start_date, schedule_interval=schedule_interval
        )

        diff = next(days_apart_gen)
        self.assertEqual(diff, 1)
        diff = next(days_apart_gen)
        self.assertEqual(diff, 7)

    def test_dag_structure(self):
        """Test that the Crossref Events DAG has the correct structure."""

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
                "cleanup": [],
            },
            dag,
        )

    def test_dag_load(self):
        """Test that the DAG can be loaded from a DAG bag."""

        with ObservatoryEnvironment().create():
            dag_file = os.path.join(module_file_path("academic_observatory_workflows.dags"), "unpaywall_telescope.py")
            self.assert_dag_load("unpaywall", dag_file)

    def setup_observatory_environment(self):
        env = ObservatoryEnvironment(self.project_id, self.data_location)
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

    # We want to do 3 dag runs.  First is to load snapshot.
    # Second is to load day diff 1 day before snapshot date (won't exist, so skip).
    # Third loads a daily diff on day of snapshot (exists).
    # Demonstrates that we are looking 2 days back with diff updates.
    def test_telescope_day(self):
        env = self.setup_observatory_environment()

        first_execution_date = pendulum.datetime(2021, 7, 2)  # Snapshot
        second_execution_date = pendulum.datetime(2021, 7, 3)  # No update found
        third_execution_date = pendulum.datetime(2021, 7, 4)  # Update found

        with env.create(task_logging=True):
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

                        telescope = UnpaywallTelescope(dataset_id=self.dataset_id)
                        dag = telescope.make_dag()

                        # First run
                        with env.create_dag_run(dag, first_execution_date):
                            release = UnpaywallRelease(
                                dag_id=UnpaywallTelescope.DAG_ID,
                                start_date=pendulum.datetime(2021, 7, 2),
                                end_date=pendulum.datetime(2021, 7, 2),
                                first_release=True,
                            )

                            # Check dependencies are met
                            ti = env.run_task(telescope.check_dependencies.__name__)
                            self.assertEqual(ti.state, State.SUCCESS)

                            # Check releases
                            ti = env.run_task(telescope.check_releases.__name__)
                            self.assertEqual(ti.state, State.SUCCESS)

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
                            self.assertEqual(ti.state, State.SKIPPED)

                            # Delete changed data from main table
                            with patch("observatory.platform.utils.gc_utils.bq_query_bytes_daily_limit_check"):
                                ti = env.run_task(telescope.bq_delete_old.__name__)
                            self.assertEqual(ti.state, State.SKIPPED)

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

                        # Second run (skips)  Use dailies
                        with env.create_dag_run(dag, second_execution_date):
                            release = UnpaywallRelease(
                                dag_id=UnpaywallTelescope.DAG_ID,
                                start_date=pendulum.datetime(2021, 7, 3),
                                end_date=pendulum.datetime(2021, 7, 3),
                                first_release=False,
                            )

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

                        # Third run (downloads)
                        with env.create_dag_run(dag, third_execution_date):
                            release = UnpaywallRelease(
                                dag_id=UnpaywallTelescope.DAG_ID,
                                start_date=pendulum.datetime(2021, 7, 4),
                                end_date=pendulum.datetime(2021, 7, 4),
                                first_release=True,
                            )

                            # Check dependencies are met
                            ti = env.run_task(telescope.check_dependencies.__name__)
                            self.assertEqual(ti.state, State.SUCCESS)

                            # Check releases
                            ti = env.run_task(telescope.check_releases.__name__)
                            self.assertEqual(ti.state, State.SUCCESS)

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
                            main_table_id, partition_table_id = release.dag_id, f"{release.dag_id}_partitions"
                            table_id = create_date_table_id(
                                partition_table_id, release.end_date, bigquery.TimePartitioningType.DAY
                            )
                            table_id = f"{self.project_id}.{telescope.dataset_id}.{table_id}"
                            expected_rows = 2
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
                            expected_rows = 101
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

                        # Clean up template
                        self.remove_changefiles()
