# Copyright 2020-2024 Curtin University
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

# Author: Aniek Roelofs, James Diprose, Keegan Smith

import os
from unittest.mock import patch

import pendulum

from academic_observatory_workflows.config import project_path, TestConfig
from academic_observatory_workflows.crossref_metadata_telescope.telescope import create_dag, DagParams
from observatory_platform.airflow.workflow import Workflow
from observatory_platform.airflow.airflow import upsert_airflow_connection, clear_airflow_connections
from observatory_platform.dataset_api import DatasetAPI
from observatory_platform.google.bigquery import bq_sharded_table_id
from observatory_platform.sandbox.sandbox_environment import SandboxEnvironment
from observatory_platform.sandbox.test_utils import SandboxTestCase

FIXTURES_FOLDER = project_path("crossref_metadata_telescope", "tests", "fixtures")


class TestCrossrefMetadataTelescope(SandboxTestCase):
    """Tests for the Crossref Metadata telescope"""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        super(TestCrossrefMetadataTelescope, self).__init__(*args, **kwargs)
        self.dag_id = "crossref_metadata"
        self.download_path = os.path.join(FIXTURES_FOLDER, "crossref_metadata.json.tar.gz")

    def test_dag_structure(self):
        """Test that the DAG has the correct structure."""

        dag = create_dag(DagParams(dag_id=self.dag_id, cloud_workspace=self.fake_cloud_workspace))
        self.assert_dag_structure(
            {
                "check_dependencies": ["fetch_release"],
                # fetch_release passes an XCom to all of these tasks
                "fetch_release": [
                    "gke_create_storage",
                    "download",
                    "upload_downloaded",
                    "extract",
                    "transform",
                    "upload_transformed",
                    "bq_load",
                    "add_dataset_release",
                    "cleanup_workflow",
                ],
                "gke_create_storage": ["download"],
                "download": ["upload_downloaded"],
                "upload_downloaded": ["extract"],
                "extract": ["transform"],
                "transform": ["upload_transformed"],
                "upload_transformed": ["bq_load"],
                "bq_load": ["gke_delete_storage"],
                "gke_delete_storage": ["add_dataset_release"],
                "add_dataset_release": ["cleanup_workflow"],
                "cleanup_workflow": [],
            },
            dag,
        )

    def test_dag_load(self):
        """Test that the DAG can be loaded from a DAG bag."""

        env = SandboxEnvironment(
            workflows=[
                Workflow(
                    dag_id=self.dag_id,
                    name="Crossref Metadata Telescope",
                    class_name="academic_observatory_workflows.crossref_metadata_telescope.telescope",
                    cloud_workspace=self.fake_cloud_workspace,
                )
            ]
        )

        with env.create():
            dag_file = os.path.join(project_path(), "..", "..", "dags", "load_dags.py")
            self.assert_dag_load(self.dag_id, dag_file)

    def test_telescope(self):
        """Test the telescope end to end"""

        env = SandboxEnvironment(project_id=TestConfig.gcp_project_id, data_location=TestConfig.gcp_data_location)
        api_bq_dataset_id = env.add_dataset("crossref_metadata_api")
        bq_dataset_id = env.add_dataset("crossref_metadata")

        with env.create(task_logging=True), patch(
            "academic_observatory_workflows.crossref_metadata_telescope.tasks.check_release_exists"
        ) as mock_cre:

            mock_cre.return_value = True
            clear_airflow_connections()
            upsert_airflow_connection(conn_id="crossref_metadata", conn_type="http")
            upsert_airflow_connection(**TestConfig.gke_cluster_connection)

            task_resources = {
                "download": {"memory": "2G", "cpu": "2"},
                "upload_downloaded": {"memory": "2G", "cpu": "2"},
                "extract": {"memory": "2G", "cpu": "2"},
                "transform": {"memory": "2G", "cpu": "2"},
                "upload_transformed": {"memory": "2G", "cpu": "2"},
            }
            test_params = DagParams(
                dag_id="test_crossref_metadata",
                cloud_workspace=env.cloud_workspace,
                crossref_base_url=TestConfig.flask_service_url,
                retries=0,
                bq_dataset_id=bq_dataset_id,
                api_bq_dataset_id=api_bq_dataset_id,
                gke_image=TestConfig.gke_image,
                gke_namespace=TestConfig.gke_namespace,
                gke_volume_name=TestConfig.gke_volume_name,
                gke_volume_path=TestConfig.gke_volume_path,
                gke_resource_overrides=task_resources,
                test_run=True,
            )

            dagrun = create_dag(dag_params=test_params).test(
                execution_date=pendulum.datetime(year=2023, month=1, day=7)
            )
            if not dagrun.state == "success":
                raise RuntimeError("Dagrun did not complete successfully")

            # Make Assertions
            table_id = bq_sharded_table_id(
                project_id=env.cloud_workspace.project_id,
                dataset_id=bq_dataset_id,
                table_name="crossref_metadata",
                date=pendulum.date(2022, 12, 31),
            )
            self.assert_table_integrity(table_id, 20)

            # Should be one release in the API
            api = DatasetAPI(bq_project_id=env.cloud_workspace.project_id, bq_dataset_id=test_params.api_bq_dataset_id)
            api.seed_db()
            api_releases = api.get_dataset_releases(dag_id=test_params.dag_id, entity_id="crossref_metadata")
            self.assertEqual(len(api_releases), 1)
