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

# Author: Aniek Roelofs, Tuan Chien

import datetime
import logging
import os
import shutil
from typing import List
from unittest.mock import patch

import pendulum
import vcr
from academic_observatory_workflows.config import test_fixtures_folder
from academic_observatory_workflows.workflows.unpaywall_snapshot_telescope import (
    UnpaywallSnapshotRelease,
    UnpaywallSnapshotTelescope,
)
from airflow.utils.state import State
from click.testing import CliRunner
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


class TestUnpaywallSnapshotRelease(ObservatoryTestCase):
    """Tests for the functions used by the unpaywall telescope"""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        super().__init__(*args, **kwargs)

        # Unpaywall test release
        self.unpaywall_test_path = test_fixtures_folder("unpaywall_snapshot", "unpaywall_snapshot.jsonl.gz")
        self.unpaywall_test_file = "unpaywall_3000-01-27T153236.jsonl.gz"
        self.unpaywall_test_url = "http://localhost/unpaywall_3000-01-27T153236.jsonl.gz"
        self.unpaywall_test_date = pendulum.datetime(year=3000, month=1, day=27)
        self.unpaywall_test_decompress_hash = "fe4e72ce54c4bb236802ddbb3dbee905"
        self.unpaywall_test_transform_hash = "62cbb5af5a78d2e0769a28d976971cba"

        # Turn logging to warning because vcr prints too much at info level
        logging.basicConfig()
        logging.getLogger().setLevel(logging.WARNING)

    def test_parse_release_date(self):
        """Test that date obtained from url is string and in correct format.

        :return: None.
        """

        release_date = UnpaywallSnapshotRelease.parse_release_date(self.unpaywall_test_file)
        self.assertEqual(self.unpaywall_test_date, release_date)

    @patch("academic_observatory_workflows.workflows.unpaywall_snapshot_telescope.Variable.get")
    def test_extract_release(self, mock_variable_get):
        """Test that the release is decompressed as expected.

        :return: None.
        """

        # Create data path and mock getting data path
        data_path = "data"
        mock_variable_get.return_value = data_path

        with CliRunner().isolated_filesystem():
            release = UnpaywallSnapshotRelease(
                dag_id="test", release_date=self.unpaywall_test_date, file_name=self.unpaywall_test_file
            )

            # 'download' release
            shutil.copyfile(self.unpaywall_test_path, release.download_path)

            release.extract()
            self.assertEqual(len(release.extract_files), 1)
            self.assert_file_integrity(release.extract_path, self.unpaywall_test_decompress_hash, "md5")

    @patch("academic_observatory_workflows.workflows.unpaywall_snapshot_telescope.get_airflow_connection_url")
    @patch("observatory.platform.utils.workflow_utils.Variable.get")
    def test_transform_release(self, mock_variable_get, m_get_conn):
        """Test that the release is transformed as expected.

        :return: None.
        """

        m_get_conn.return_value = "http://localhost/"

        # Create data path and mock getting data path
        data_path = "data"
        mock_variable_get.return_value = data_path

        with CliRunner().isolated_filesystem():
            release = UnpaywallSnapshotRelease(
                dag_id="test", release_date=self.unpaywall_test_date, file_name=self.unpaywall_test_file
            )
            shutil.copyfile(self.unpaywall_test_path, release.download_path)

            release.extract()
            release.transform()
            self.assertEqual(len(release.transform_files), 1)
            self.assert_file_integrity(release.transform_path, self.unpaywall_test_transform_hash, "md5")

    @patch("academic_observatory_workflows.workflows.unpaywall_snapshot_telescope.get_airflow_connection_url")
    @patch("academic_observatory_workflows.workflows.unpaywall_snapshot_telescope.Variable.get")
    @patch("academic_observatory_workflows.workflows.unpaywall_snapshot_telescope.download_file")
    def test_download(self, m_download_files, m_varget, m_get_conn):
        release = UnpaywallSnapshotRelease(
            dag_id="test", release_date=self.unpaywall_test_date, file_name=self.unpaywall_test_file
        )

        # Setup mocks
        data_path = "data"
        m_varget.return_value = data_path
        m_get_conn.return_value = "http://localhost/"

        release.download()
        _, call_args = m_download_files.call_args

        self.assertEqual(
            call_args["url"],
            "http://localhost/unpaywall_3000-01-27T153236.jsonl.gz",
        )
        self.assertEqual(
            call_args["filename"], "data/telescopes/download/test/test_3000_01_27/unpaywall_snapshot.jsonl.gz"
        )

    @patch("academic_observatory_workflows.workflows.unpaywall_snapshot_telescope.get_airflow_connection_url")
    @patch("academic_observatory_workflows.workflows.unpaywall_snapshot_telescope.Variable.get")
    def test_extract_outputs(self, m_variable_get, m_get_conn):
        # Create data path and mock getting data path
        data_path = "data"
        m_variable_get.return_value = data_path
        m_get_conn.return_value = "http://localhost/"

        with CliRunner().isolated_filesystem():
            release = UnpaywallSnapshotRelease(
                dag_id="test", release_date=self.unpaywall_test_date, file_name=self.unpaywall_test_file
            )
            shutil.copyfile(self.unpaywall_test_path, release.download_path)
            release.extract()
            self.assertEqual(len(release.extract_files), 1)

    @patch("academic_observatory_workflows.workflows.unpaywall_snapshot_telescope.get_airflow_connection_url")
    @patch("academic_observatory_workflows.workflows.unpaywall_snapshot_telescope.Variable.get")
    def test_transform_outputs(self, m_variable_get, m_get_conn):
        # Create data path and mock getting data path
        data_path = "data"
        m_variable_get.return_value = data_path
        m_get_conn.return_value = "http://localhost/"

        with CliRunner().isolated_filesystem():
            release = UnpaywallSnapshotRelease(
                dag_id="test", release_date=self.unpaywall_test_date, file_name=self.unpaywall_test_file
            )
            shutil.copyfile(self.unpaywall_test_path, release.download_path)
            release.extract()
            release.transform()
            self.assertEqual(len(release.transform_files), 1)


class TestUnpaywallSnapshotTelescope(ObservatoryTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Unpaywall releases list
        self.list_unpaywall_releases_path = test_fixtures_folder("unpaywall_snapshot", "list_unpaywall_releases.yaml")
        self.list_unpaywall_releases_hash = "78d1a129cb0aba072ca49e2599f60c10"

        self.start_date = pendulum.datetime(year=2018, month=3, day=29)
        self.end_date = pendulum.datetime(year=2020, month=4, day=29)

        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")
        self.unpaywall_test_path = test_fixtures_folder("unpaywall_snapshot", "unpaywall_snapshot.jsonl.gz")

    def test_ctor(self):
        # set table description
        telescope = UnpaywallSnapshotTelescope(table_descriptions="something")
        self.assertEqual(telescope.table_descriptions, "something")

        # set airflow_vars
        telescope = UnpaywallSnapshotTelescope(airflow_vars=[])
        self.assertEqual(telescope.airflow_vars, ["transform_bucket"])

    @patch("academic_observatory_workflows.workflows.unpaywall_snapshot_telescope.get_airflow_connection_url")
    @patch("observatory.platform.utils.workflow_utils.Variable.get")
    def test_list_releases(self, mock_variable_get, m_get_conn):
        """Test that list releases returns a list of string with urls.

        :return: None.
        """

        data_path = "data"
        mock_variable_get.return_value = data_path
        m_get_conn.return_value = "http://localhost/"

        telescope = UnpaywallSnapshotTelescope()

        with CliRunner().isolated_filesystem():
            with vcr.use_cassette(self.list_unpaywall_releases_path):
                releases = UnpaywallSnapshotTelescope.list_releases(self.start_date, self.end_date)
                self.assertIsInstance(releases, List)
                for release in releases:
                    self.assertIsInstance(release, dict)
                self.assertEqual(13, len(releases))

    @patch("academic_observatory_workflows.workflows.unpaywall_snapshot_telescope.get_http_response_xml_to_dict")
    @patch("academic_observatory_workflows.workflows.unpaywall_snapshot_telescope.get_airflow_connection_url")
    @patch("observatory.platform.utils.workflow_utils.Variable.get")
    def test_list_releases_fail(self, m_get, m_get_conn, m_get_xml_dict):
        data_path = "data"
        m_get.return_value = data_path
        m_get_conn.return_value = "http://localhost/"
        m_get_xml_dict.side_effect = ConnectionError("Test")
        telescope = UnpaywallSnapshotTelescope()

        # Fetch error
        self.assertRaises(ConnectionError, UnpaywallSnapshotTelescope.list_releases, self.start_date, self.end_date)

    @patch("academic_observatory_workflows.workflows.unpaywall_snapshot_telescope.get_http_response_xml_to_dict")
    @patch("academic_observatory_workflows.workflows.unpaywall_snapshot_telescope.get_airflow_connection_url")
    @patch("observatory.platform.utils.workflow_utils.Variable.get")
    def test_list_releases_date_out_of_range(self, m_get, m_get_conn, m_get_xmldict):
        data_path = "data"
        m_get.return_value = data_path
        m_get_conn.return_value = "http://localhost/"

        telescope = UnpaywallSnapshotTelescope()

        m_get_xmldict.return_value = {
            "ListBucketResult": {
                "Contents": [
                    {"Key": "unpaywall_2018-03-29T113154.jsonl.gz", "LastModified": "2000-04-28T17:28:55.000Z"}
                ]
            }  # Outside range
        }

        releases = UnpaywallSnapshotTelescope.list_releases(self.start_date, self.end_date)
        self.assertEqual(len(releases), 0)

    class MockTI:
        def xcom_push(self, *args):
            pass

    @patch("academic_observatory_workflows.workflows.unpaywall_snapshot_telescope.bigquery_table_exists")
    @patch(
        "academic_observatory_workflows.workflows.unpaywall_snapshot_telescope.UnpaywallSnapshotTelescope.list_releases"
    )
    @patch("observatory.platform.utils.workflow_utils.Variable.get")
    def test_get_release_info(self, m_get, m_releases, m_bq_table_exist):
        m_get.return_value = "projectid"

        # No release
        m_releases.return_value = []
        m_bq_table_exist.return_value = True
        telescope = UnpaywallSnapshotTelescope()
        continue_dag = telescope.get_release_info(
            **{
                "ti": TestUnpaywallSnapshotTelescope.MockTI(),
                "execution_date": datetime.datetime(2021, 1, 1),
                "next_execution_date": datetime.datetime(2021, 2, 1),
            }
        )

        self.assertEqual(continue_dag, False)

        # Single release exists
        m_releases.return_value = [{"date": "20210101", "file_name": "some file"}]
        m_bq_table_exist.return_value = True
        continue_dag = telescope.get_release_info(
            **{
                "ti": TestUnpaywallSnapshotTelescope.MockTI(),
                "execution_date": datetime.datetime(2021, 1, 1),
                "next_execution_date": datetime.datetime(2021, 2, 1),
            }
        )
        self.assertEqual(continue_dag, False)

        # Single release, not exist
        m_releases.return_value = [{"date": "20210101", "file_name": "some file"}]
        m_bq_table_exist.return_value = False
        continue_dag = telescope.get_release_info(
            **{
                "ti": TestUnpaywallSnapshotTelescope.MockTI(),
                "execution_date": datetime.datetime(2021, 1, 1),
                "next_execution_date": datetime.datetime(2021, 2, 1),
            }
        )
        self.assertEqual(continue_dag, True)

    def test_dag_structure(self):
        """Test that the Crossref Events DAG has the correct structure."""

        dag = UnpaywallSnapshotTelescope().make_dag()
        self.assert_dag_structure(
            {
                "check_dependencies": ["get_release_info"],
                "get_release_info": ["download"],
                "download": ["upload_downloaded"],
                "upload_downloaded": ["extract"],
                "extract": ["transform"],
                "transform": ["upload_transformed"],
                "upload_transformed": ["bq_load"],
                "bq_load": ["cleanup"],
                "cleanup": [],
            },
            dag,
        )

    def test_dag_load(self):
        """Test that the DAG can be loaded from a DAG bag."""

        with ObservatoryEnvironment().create():
            dag_file = os.path.join(
                module_file_path("academic_observatory_workflows.dags"), "unpaywall_snapshot_telescope.py"
            )
            self.assert_dag_load("unpaywall_snapshot", dag_file)

    def setup_observatory_env(self):
        env = ObservatoryEnvironment(self.project_id, self.data_location)
        self.dataset_id = env.add_dataset()
        return env

    @patch("airflow.hooks.base.BaseHook.get_connection")
    def test_telescope(self, m_base_get_con):
        """Test the Telescope end to end."""

        m_base_get_con.return_value = "http://localhost"

        # Setup http server to serve files
        httpserver = HttpServer(directory=test_fixtures_folder("unpaywall_snapshot"))
        with httpserver.create():
            with patch(
                "academic_observatory_workflows.workflows.unpaywall_snapshot_telescope.get_airflow_connection_url"
            ) as m_get_conns:
                # Mock out unpaywall connection url
                mock_url = f"http://{httpserver.host}:{httpserver.port}/"
                m_get_conns.return_value = mock_url

                env = self.setup_observatory_env()

                execution_date = pendulum.datetime(2021, 6, 1)
                release_date_str = "20210101"
                release_date = pendulum.parse(release_date_str)
                file_name = "unpaywall_snapshot.jsonl.gz"

                with env.create():
                    telescope = UnpaywallSnapshotTelescope(dataset_id=self.dataset_id)
                    dag = telescope.make_dag()

                    release = UnpaywallSnapshotRelease(
                        dag_id=dag.dag_id, release_date=release_date, file_name=file_name
                    )

                    with env.create_dag_run(dag, execution_date):
                        # check dependencies
                        ti = env.run_task(telescope.check_dependencies.__name__)
                        self.assertEqual(ti.state, State.SUCCESS)

                        # get release info
                        with patch(
                            "academic_observatory_workflows.workflows.unpaywall_snapshot_telescope.UnpaywallSnapshotTelescope.list_releases"
                        ) as m_list_releases:

                            m_list_releases.return_value = [
                                {
                                    "date": release_date_str,
                                    "file_name": file_name,
                                }
                            ]
                            ti = env.run_task(telescope.get_release_info.__name__)
                        self.assertEqual(ti.state, State.SUCCESS)

                        # download
                        ti = env.run_task(telescope.download.__name__)
                        self.assertEqual(ti.state, State.SUCCESS)

                        # Check file was downloaded
                        self.assertEqual(len(release.download_files), 1)

                        # upload_downloaded
                        ti = env.run_task(telescope.upload_downloaded.__name__)
                        self.assertEqual(ti.state, State.SUCCESS)
                        self.assert_blob_integrity(
                            env.download_bucket, blob_name(release.download_path), release.download_path
                        )

                        # extract
                        ti = env.run_task(telescope.extract.__name__)
                        self.assertEqual(ti.state, State.SUCCESS)

                        # transform
                        ti = env.run_task(telescope.transform.__name__)
                        self.assertEqual(ti.state, State.SUCCESS)

                        # upload_transformed
                        ti = env.run_task(telescope.upload_transformed.__name__)
                        self.assertEqual(ti.state, State.SUCCESS)
                        self.assert_blob_integrity(
                            env.transform_bucket, blob_name(release.transform_path), release.transform_path
                        )

                        # bq_load
                        ti = env.run_task(telescope.bq_load.__name__)
                        self.assertEqual(ti.state, State.SUCCESS)

                        table_id = (
                            f"{self.project_id}.{self.dataset_id}."
                            f"{bigquery_sharded_table_id(telescope.dag_id, release.release_date)}"
                        )
                        expected_rows = 100
                        self.assert_table_integrity(table_id, expected_rows)

                        # cleanup
                        download_folder, extract_folder, transform_folder = (
                            release.download_folder,
                            release.extract_folder,
                            release.transform_folder,
                        )
                        env.run_task(telescope.cleanup.__name__)
                        self.assertEqual(ti.state, State.SUCCESS)
                        self.assert_cleanup(download_folder, extract_folder, transform_folder)
