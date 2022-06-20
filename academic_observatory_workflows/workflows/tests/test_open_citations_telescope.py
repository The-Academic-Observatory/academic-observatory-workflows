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
from unittest.mock import MagicMock, patch

import pendulum
from airflow.models import Connection
from airflow.utils.state import State

from academic_observatory_workflows.config import test_fixtures_folder
from academic_observatory_workflows.workflows.open_citations_telescope import (
    OpenCitationsRelease,
    OpenCitationsTelescope,
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
from observatory.platform.utils.gc_utils import run_bigquery_query
from observatory.platform.utils.http_download import DownloadInfo
from observatory.platform.utils.jinja2_utils import render_template
from observatory.platform.utils.release_utils import get_dataset_releases
from observatory.platform.utils.test_utils import (
    HttpServer,
    ObservatoryEnvironment,
    ObservatoryTestCase,
    module_file_path,
    find_free_port,
)
from observatory.platform.utils.workflow_utils import (
    bigquery_sharded_table_id,
)
from observatory.platform.utils.workflow_utils import blob_name


class TestOpenCitationsTelescope(ObservatoryTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")
        self.fixture_dir = test_fixtures_folder("open_citations")
        self.release_list_file = "list_open_citation_releases.json"
        self.version_1_file = "1.json"

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

        name = "Open Citations Telescope"
        workflow_type = WorkflowType(name=name, type_id=OpenCitationsTelescope.DAG_ID)
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
            type_id="open_citations",
            name="ds type",
            extra={},
            table_type=TableType(id=1),
        )
        self.api.put_dataset_type(dataset_type)

        dataset = Dataset(
            name="Open Citations Dataset",
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

    @patch("academic_observatory_workflows.workflows.open_citations_telescope.OpenCitationsTelescope._list_releases")
    def test_get_release_info_continue(self, m_list_releases):
        releases = [
            {"date": "20200101", "files": "file1.txt"},
            {"date": "20200102", "files": "file2.txt"},
            {"date": "20200103", "files": "file3.txt"},
        ]
        m_list_releases.return_value = releases
        env = ObservatoryEnvironment(self.project_id, self.data_location, api_host=self.host, api_port=self.port)

        with env.create():
            self.setup_connections(env)
            self.setup_api()

            telescope = OpenCitationsTelescope(workflow_id=1)
            execution_date = pendulum.datetime(2021, 1, 1)
            next_execution_date = pendulum.datetime(2021, 1, 8)
            ti = MagicMock()
            continue_dag = telescope.get_release_info(
                execution_date=execution_date, next_execution_date=next_execution_date, ti=ti
            )
            self.assertTrue(continue_dag)
            self.assertEqual(len(ti.method_calls), 1)

    @patch("academic_observatory_workflows.workflows.open_citations_telescope.OpenCitationsTelescope._list_releases")
    def test_get_release_info_skip(self, m_list_releases):
        m_list_releases.return_value = []

        env = ObservatoryEnvironment(self.project_id, self.data_location, api_host=self.host, api_port=self.port)

        with env.create():
            self.setup_connections(env)
            self.setup_api()

            telescope = OpenCitationsTelescope(workflow_id=1)
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
                "cleanup": ["add_new_dataset_releases"],
                "add_new_dataset_releases": [],
            },
            dag,
        )

    def test_dag_load(self):
        """Test that the OpenCitationsTelescope DAG can be loaded from a DAG bag.

        :return: None
        """

        env = ObservatoryEnvironment(self.project_id, self.data_location, api_host=self.host, api_port=self.port)

        with env.create():
            self.setup_connections(env)
            self.setup_api()
            dag_file = os.path.join(
                module_file_path("academic_observatory_workflows.dags"), "open_citations_telescope.py"
            )
            self.assert_dag_load("open_citations", dag_file)

    def test_telescope(self):
        """Test the OpenCitationsTelescope telescope end to end."""

        # Setup Observatory environment
        env = ObservatoryEnvironment(self.project_id, self.data_location, api_host=self.host, api_port=self.port)
        dataset_id = env.add_dataset()

        with env.create():
            self.setup_connections(env)
            self.setup_api()
            execution_date = pendulum.datetime(year=2018, month=11, day=12)
            telescope = OpenCitationsTelescope(dataset_id=dataset_id, workflow_id=1)
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

                    # add_dataset_release_task
                    dataset_releases = get_dataset_releases(dataset_id=1)
                    self.assertEqual(len(dataset_releases), 0)
                    ti = env.run_task("add_new_dataset_releases")
                    self.assertEqual(ti.state, State.SUCCESS)
                    dataset_releases = get_dataset_releases(dataset_id=1)
                    self.assertEqual(len(dataset_releases), 1)
