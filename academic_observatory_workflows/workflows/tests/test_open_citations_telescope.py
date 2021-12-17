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

# Author: James Diprose, Tuan Chien

import os
import unittest
from re import template
from unittest.mock import MagicMock, patch

import pendulum
from academic_observatory_workflows.config import test_fixtures_folder
from academic_observatory_workflows.workflows.open_citations_telescope import (
    OpenCitationsRelease,
    OpenCitationsTelescope,
)
from airflow.utils.state import State
from observatory.platform.utils.gc_utils import run_bigquery_query
from observatory.platform.utils.http_download import DownloadInfo
from observatory.platform.utils.jinja2_utils import render_template
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


class TestOpenCitationsTelescope(ObservatoryTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")
        self.fixture_dir = test_fixtures_folder("open_citations")
        self.release_list_file = "list_open_citation_releases.json"
        self.version_1_file = "1.json"

    def test_ctor(self):
        table_descriptions = {"open_citations": "Custom description"}
        telescope = OpenCitationsTelescope(table_descriptions=table_descriptions)
        self.assertEqual(telescope.table_descriptions, table_descriptions)

        telescope = OpenCitationsTelescope(airflow_vars=[])
        self.assertEqual(telescope.airflow_vars, ["transform_bucket"])

    @patch("academic_observatory_workflows.workflows.open_citations_telescope.get_http_response_json")
    def test_list_releases_skip(self, m_get_response):
        telescope = OpenCitationsTelescope()
        m_get_response.side_effect = [
            [{"url": "something"}],
            {"created_date": "2018-11-13T12:03:08Z", "files": [1, 2]},
        ]
        start_date = pendulum.datetime(2019, 1, 1)
        end_date = pendulum.datetime(2019, 2, 1)
        releases = telescope._list_releases(start_date=start_date, end_date=end_date)
        self.assertEqual(len(releases), 0)

    @patch("academic_observatory_workflows.workflows.open_citations_telescope.bigquery_table_exists")
    @patch("academic_observatory_workflows.workflows.open_citations_telescope.bigquery_sharded_table_id")
    @patch("academic_observatory_workflows.workflows.open_citations_telescope.Variable.get")
    def test_process_release_no_files(self, m_get, m_bq_table_id, m_bq_table_exists):
        m_get.return_value = "project_id"
        m_bq_table_id.return_value = "1"
        m_bq_table_exists.return_value = False
        telescope = OpenCitationsTelescope()
        releases = [
            {"files": [], "date": "20210101"},
            {"files": [1], "date": "20210101"},
            {"files": [2], "date": "20210101"},
        ]

        filtered_releases = list(filter(telescope._process_release, releases))
        self.assertEqual(len(filtered_releases), 2)

    @patch("academic_observatory_workflows.workflows.open_citations_telescope.bigquery_table_exists")
    @patch("academic_observatory_workflows.workflows.open_citations_telescope.bigquery_sharded_table_id")
    @patch("academic_observatory_workflows.workflows.open_citations_telescope.Variable.get")
    def test_process_release_table_exists(self, m_get, m_bq_table_id, m_bq_table_exists):
        m_get.return_value = "project_id"
        m_bq_table_id.return_value = "1"
        m_bq_table_exists.side_effect = [False, True, False]

        telescope = OpenCitationsTelescope()
        releases = [
            {"files": [0], "date": "20210101"},
            {"files": [1], "date": "20210101"},
            {"files": [2], "date": "20210101"},
        ]

        filtered_releases = list(filter(telescope._process_release, releases))
        self.assertEqual(len(filtered_releases), 2)

    @patch("academic_observatory_workflows.workflows.open_citations_telescope.OpenCitationsTelescope._process_release")
    @patch("academic_observatory_workflows.workflows.open_citations_telescope.OpenCitationsTelescope._list_releases")
    def test_get_release_info_continue(self, m_list_releases, m_process_release):
        m_list_releases.return_value = [1, 2, 3]
        m_process_release.return_value = True

        telescope = OpenCitationsTelescope()
        execution_date = pendulum.datetime(2021, 1, 1)
        next_execution_date = pendulum.datetime(2021, 1, 8)
        ti = MagicMock()
        continue_dag = telescope.get_release_info(
            execution_date=execution_date, next_execution_date=next_execution_date, ti=ti
        )
        self.assertTrue(continue_dag)
        self.assertEqual(len(ti.method_calls), 1)

    @patch("academic_observatory_workflows.workflows.open_citations_telescope.OpenCitationsTelescope._process_release")
    @patch("academic_observatory_workflows.workflows.open_citations_telescope.OpenCitationsTelescope._list_releases")
    def test_get_release_info_skip(self, m_list_releases, m_process_release):
        m_list_releases.return_value = []
        m_process_release.return_value = True

        telescope = OpenCitationsTelescope()
        execution_date = pendulum.datetime(2021, 1, 1)
        next_execution_date = pendulum.datetime(2021, 1, 8)
        ti = MagicMock()
        continue_dag = telescope.get_release_info(
            execution_date=execution_date, next_execution_date=next_execution_date, ti=ti
        )
        self.assertFalse(continue_dag)
        self.assertEqual(len(ti.method_calls), 0)

    def create_templates(self, *, host, port):
        # list open citation releases
        template_path = os.path.join(self.fixture_dir, self.release_list_file + ".jinja2")
        rendered = render_template(template_path, host=host, port=port)
        dst = os.path.join(self.fixture_dir, self.release_list_file)
        with open(dst, "w") as f:
            f.write(rendered)

        # version 1
        template_path = os.path.join(self.fixture_dir, self.version_1_file + ".jinja2")
        rendered = render_template(template_path, host=host, port=port)
        dst = os.path.join(self.fixture_dir, self.version_1_file)
        with open(dst, "w") as f:
            f.write(rendered)

    def remove_templates(self):
        dst = os.path.join(self.fixture_dir, self.release_list_file)
        os.remove(dst)

        dst = os.path.join(self.fixture_dir, self.version_1_file)
        os.remove(dst)

    def test_dag_structure(self):
        """Test that the OpenCitationsTelescope DAG has the correct structure.

        :return: None
        """

        dag = OpenCitationsTelescope().make_dag()
        self.assert_dag_structure(
            {
                "check_dependencies": ["get_release_info"],
                "get_release_info": ["download"],
                "download": ["upload_downloaded"],
                "upload_downloaded": ["extract"],
                "extract": ["upload_transformed"],
                "upload_transformed": ["bq_load"],
                "bq_load": ["cleanup"],
                "cleanup": [],
            },
            dag,
        )

    def test_dag_load(self):
        """Test that the OpenCitationsTelescope DAG can be loaded from a DAG bag.

        :return: None
        """

        with ObservatoryEnvironment().create():
            dag_file = os.path.join(
                module_file_path("academic_observatory_workflows.dags"), "open_citations_telescope.py"
            )
            self.assert_dag_load("open_citations", dag_file)

    def test_telescope(self):
        """Test the OpenCitationsTelescope telescope end to end."""

        # Setup Observatory environment
        env = ObservatoryEnvironment(self.project_id, self.data_location)
        dataset_id = env.add_dataset()

        with env.create():
            execution_date = pendulum.datetime(year=2018, month=11, day=12)
            telescope = OpenCitationsTelescope(dataset_id=dataset_id)
            dag = telescope.make_dag()

            with env.create_dag_run(dag, execution_date):
                server = HttpServer(directory=self.fixture_dir)
                with patch.object(
                    OpenCitationsTelescope,
                    "VERSION_URL",
                    f"http://{server.host}:{server.port}/{self.release_list_file}",
                ):
                    download_url = f"http://{server.host}:{server.port}/data.csv.zip"
                    download_url2 = f"http://{server.host}:{server.port}/data2.csv.zip"

                    download_file_hash = "f06dfd0bee323a95861f0ba490e786c9"
                    download_file_hash2 = "6d90805d99b65b107b17907432aa8534"

                    release = OpenCitationsRelease(
                        telescope.dag_id,
                        release_date=pendulum.datetime(2018, 11, 13),
                        files=[
                            DownloadInfo(
                                url=download_url,
                                filename="data.csv.zip",
                                hash=download_file_hash,
                                hash_algorithm="md5",
                            ),
                            DownloadInfo(
                                url=download_url2,
                                filename="data2.csv.zip",
                                hash=download_file_hash2,
                                hash_algorithm="md5",
                            ),
                        ],
                    )

                    self.create_templates(host=server.host, port=server.port)
                    with server.create():
                        # Check dependencies
                        ti = env.run_task(telescope.check_dependencies.__name__)
                        self.assertEqual(ti.state, State.SUCCESS)

                        # Get release info
                        ti = env.run_task(telescope.get_release_info.__name__)
                        self.assertEqual(ti.state, State.SUCCESS)

                        actual_release_info = ti.xcom_pull(
                            key=OpenCitationsTelescope.RELEASE_INFO,
                            task_ids=telescope.get_release_info.__name__,
                            include_prior_dates=False,
                        )
                        self.assertEqual(len(actual_release_info), 1)
                        self.assertEqual(actual_release_info[0]["date"], "20181113")
                        self.assertEqual(len(actual_release_info[0]["files"]), 2)
                        self.assertEqual(actual_release_info[0]["files"][0]["download_url"], download_url)
                        self.assertEqual(actual_release_info[0]["files"][1]["download_url"], download_url2)

                        # Download
                        ti = env.run_task(telescope.download.__name__)
                        self.assertEqual(ti.state, State.SUCCESS)
                        self.assertEqual(len(release.download_files), 2)

                        self.remove_templates()

                    # Upload downloaded
                    ti = env.run_task(telescope.upload_downloaded.__name__)
                    self.assertEqual(ti.state, State.SUCCESS)
                    self.assert_blob_integrity(
                        env.download_bucket, blob_name(release.download_files[0]), release.download_files[0]
                    )

                    # Extract
                    ti = env.run_task(telescope.extract.__name__)
                    self.assertEqual(ti.state, State.SUCCESS)

                    # Upload transformed
                    ti = env.run_task(telescope.upload_transformed.__name__)
                    self.assertEqual(ti.state, State.SUCCESS)
                    self.assert_blob_integrity(
                        env.transform_bucket, blob_name(release.transform_files[0]), release.transform_files[0]
                    )

                    print(release.transform_files)

                    # BQ load
                    ti = env.run_task(telescope.bq_load.__name__)
                    self.assertEqual(ti.state, State.SUCCESS)

                    table_id = (
                        f"{self.project_id}.{dataset_id}."
                        f"{bigquery_sharded_table_id(telescope.dag_id, release.release_date)}"
                    )
                    expected_rows = 4
                    self.assert_table_integrity(table_id, expected_rows)

                    sql = f"SELECT * from {self.project_id}.{dataset_id}.open_citations20181113"
                    with patch("observatory.platform.utils.gc_utils.bq_query_bytes_daily_limit_check"):
                        records = run_bigquery_query(sql)

                    self.assertEqual(
                        records[0]["oci"],
                        "020010100093631183015370109090737060203090304-020020102030636101310000103090309",
                    )
                    self.assertEqual(records[0]["citing"], "10.1109/viuf.1997.623934")
                    self.assertEqual(records[0]["cited"], "10.21236/ada013939")
                    self.assertEqual(records[0]["creation"], "1997")
                    self.assertEqual(records[0]["timespan"], "P22Y")
                    self.assertEqual(records[0]["journal_sc"], False)
                    self.assertEqual(records[0]["author_sc"], False)

                    self.assertEqual(
                        records[1]["oci"],
                        "02001010009363118353702000009370100-0200100010636280009020563020301025800025900000601036306",
                    )
                    self.assertEqual(records[1]["citing"], "10.1109/viz.2009.10")
                    self.assertEqual(records[1]["cited"], "10.1016/s0925-2312(02)00613-6")
                    self.assertEqual(records[1]["creation"], "2009-07")
                    self.assertEqual(records[1]["timespan"], "P6Y3M")
                    self.assertEqual(records[1]["journal_sc"], False)
                    self.assertEqual(records[1]["author_sc"], False)

                    self.assertEqual(records[2]["citing"], "10.1109/viuf.1997.623934")
                    self.assertEqual(records[2]["cited"], "10.21236/ada013939")
                    self.assertEqual(records[2]["creation"], "1997")
                    self.assertEqual(records[2]["timespan"], "P22Y")
                    self.assertEqual(records[2]["journal_sc"], False)
                    self.assertEqual(records[2]["author_sc"], False)

                    self.assertEqual(
                        records[1]["oci"],
                        "02001010009363118353702000009370100-0200100010636280009020563020301025800025900000601036306",
                    )
                    self.assertEqual(records[3]["citing"], "10.1109/viz.2009.10")
                    self.assertEqual(records[3]["cited"], "10.1016/s0925-2312(02)00613-6")
                    self.assertEqual(records[3]["creation"], "2009-07")
                    self.assertEqual(records[3]["timespan"], "P6Y3M")
                    self.assertEqual(records[3]["journal_sc"], False)
                    self.assertEqual(records[3]["author_sc"], False)

                    # Cleanup
                    download_folder, extract_folder, transform_folder = (
                        release.download_folder,
                        release.extract_folder,
                        release.transform_folder,
                    )
                    env.run_task(telescope.cleanup.__name__)
                    self.assertEqual(ti.state, State.SUCCESS)
                    self.assert_cleanup(download_folder, extract_folder, transform_folder)
