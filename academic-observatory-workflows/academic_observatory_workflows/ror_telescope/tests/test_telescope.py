# Copyright 2021-2024 Curtin University
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

# Author: Aniek Roelofs, James Diprose

import os
from unittest.mock import patch

import pendulum

from academic_observatory_workflows.config import project_path
from academic_observatory_workflows.ror_telescope.telescope import create_dag, DagParams
from observatory_platform.dataset_api import DatasetAPI
from observatory_platform.google.bigquery import bq_sharded_table_id
from observatory_platform.files import load_jsonl
from observatory_platform.airflow.workflow import Workflow
from observatory_platform.sandbox.http_server import HttpServer
from observatory_platform.sandbox.sandbox_environment import SandboxEnvironment
from observatory_platform.sandbox.test_utils import SandboxTestCase, find_free_port

FIXTURES_FOLDER = project_path("ror_telescope", "tests", "fixtures")


class TestRorTelescope(SandboxTestCase):
    """Tests for the ROR telescope"""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        super(TestRorTelescope, self).__init__(*args, **kwargs)
        self.dag_id = "ror"
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")
        self.ror_conceptrecid = 6347574
        self.server_port = find_free_port()
        self.mocked_files = [
            os.path.join(FIXTURES_FOLDER, "v1.11-2022-10-20-ror-data.zip"),
            os.path.join(FIXTURES_FOLDER, "v1.10-2022-10-17-ror-data.zip"),
        ]

    def test_dag_structure(self):
        """Test that the DAG has the correct structure."""

        dag_params = DagParams(dag_id=self.dag_id, cloud_workspace=self.fake_cloud_workspace)
        dag = create_dag(dag_params)

        self.assert_dag_structure(
            {
                "check_dependencies": ["fetch_releases"],
                # fetch_release passes an XCom to all of these tasks
                "fetch_releases": [
                    "process_release.download",
                    "process_release.transform",
                    "process_release.bq_load",
                    "process_release.add_dataset_releases",
                    "process_release.cleanup_workflow",
                ],
                "process_release.download": ["process_release.transform"],
                "process_release.transform": ["process_release.bq_load"],
                "process_release.bq_load": ["process_release.add_dataset_releases"],
                "process_release.add_dataset_releases": ["process_release.cleanup_workflow"],
                "process_release.cleanup_workflow": [],
            },
            dag,
        )

    def test_dag_load(self):
        """Test that workflow can be loaded from a DAG bag."""

        env = SandboxEnvironment(
            workflows=[
                Workflow(
                    dag_id=self.dag_id,
                    name="ROR Telescope",
                    class_name="academic_observatory_workflows.ror_telescope.telescope",
                    cloud_workspace=self.fake_cloud_workspace,
                )
            ]
        )

        with env.create():
            dag_file = os.path.join(project_path(), "..", "..", "dags", "load_dags.py")
            self.assert_dag_load(self.dag_id, dag_file)

    @patch("academic_observatory_workflows.ror_telescope.tasks.list_ror_records")
    def test_telescope(self, m_list_ror_records):
        """Test the ROR workflow end to end."""

        # Get mocked releases, list_ror_records is tested in its own function
        ror_records = [
            {
                "snapshot_date": "2022-10-20",
                "url": f"http://localhost:{self.server_port}/v1.11-2022-10-20-ror-data.zip",
                "checksum": "md5:0cac8705fba6df755648472356b7cb83",
            },
            {
                "snapshot_date": "2022-10-17",
                "url": f"http://localhost:{self.server_port}/v1.10-2022-10-17-ror-data.zip",
                "checksum": "md5:60620675937e6513104275931331f68f",
            },
        ]
        m_list_ror_records.return_value = ror_records

        env = SandboxEnvironment(self.project_id, self.data_location)
        bq_dataset_id = env.add_dataset(prefix="ror")
        api_bq_dataset_id = env.add_dataset(prefix="ror_api")
        server = HttpServer(FIXTURES_FOLDER, port=self.server_port)

        # Create the Observatory environment and run tests
        with env.create(), server.create():
            logical_date = pendulum.datetime(year=2022, month=10, day=16)
            bq_table_name = "ror"
            test_params = DagParams(
                dag_id=self.dag_id,
                cloud_workspace=env.cloud_workspace,
                bq_dataset_id=bq_dataset_id,
                retries=0,
                bq_table_name=bq_table_name,
                api_bq_dataset_id=api_bq_dataset_id,
            )
            dag = create_dag(test_params)
            dagrun = dag.test(execution_date=logical_date)

            # Make assertions
            if not dagrun.state == "success":
                raise RuntimeError("Frist Dagrun did not complete successfully")

            # Two tables - both with identical content
            expected_content = load_jsonl(os.path.join(FIXTURES_FOLDER, "table_content.jsonl"))
            table_id = bq_sharded_table_id(
                test_params.cloud_workspace.project_id,
                test_params.bq_dataset_id,
                test_params.bq_table_name,
                pendulum.date(2022, 10, 17),
            )
            self.assert_table_content(table_id, expected_content, "id")
            table_id = bq_sharded_table_id(
                test_params.cloud_workspace.project_id,
                test_params.bq_dataset_id,
                test_params.bq_table_name,
                pendulum.date(2022, 10, 20),
            )
            self.assert_table_content(table_id, expected_content, "id")

            # Assert that the dataset has been added to the observatory-api
            api = DatasetAPI(
                bq_project_id=test_params.cloud_workspace.project_id, bq_dataset_id=test_params.api_bq_dataset_id
            )
            api.seed_db()
            dataset_releases = api.get_dataset_releases(dag_id=test_params.dag_id, entity_id="ror")
            self.assertEqual(len(dataset_releases), 2)
