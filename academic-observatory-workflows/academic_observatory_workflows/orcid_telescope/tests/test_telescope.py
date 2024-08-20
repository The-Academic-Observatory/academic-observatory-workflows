# Copyright 2023 Curtin University
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

# Author: Keegan Smith

from __future__ import annotations

from dataclasses import dataclass
import os
from unittest.mock import patch

import pendulum

from academic_observatory_workflows.config import project_path, TestConfig
from academic_observatory_workflows.orcid_telescope.telescope import create_dag, DagParams
from observatory_platform.airflow.airflow import clear_airflow_connections, upsert_airflow_connection
from observatory_platform.airflow.workflow import Workflow
from observatory_platform.config import module_file_path
from observatory_platform.dataset_api import DatasetAPI
from observatory_platform.google.bigquery import bq_table_id, bq_sharded_table_id
from observatory_platform.google.gcs import gcs_upload_files
from observatory_platform.sandbox.test_utils import SandboxTestCase, load_and_parse_json
from observatory_platform.sandbox.sandbox_environment import SandboxEnvironment

FIXTURES_FOLDER = project_path("orcid_telescope", "tests", "fixtures")


@dataclass
class OrcidTestRecords:
    # First run
    first_run_folder = os.path.join(FIXTURES_FOLDER, "first_run")
    first_run_records = [
        {
            "orcid": "0000-0001-5000-5000",
            "path": os.path.join(first_run_folder, "0000-0001-5000-5000.xml"),
            "batch": "000",
        },
        {
            "orcid": "0000-0001-5001-3000",
            "path": os.path.join(first_run_folder, "0000-0001-5001-3000.xml"),
            "batch": "001",
        },
        {
            "orcid": "0000-0001-5002-1000",
            "path": os.path.join(first_run_folder, "0000-0001-5002-1000.xml"),
            "batch": "00X",
        },
        {
            "orcid": "0000-0001-5007-2000",
            "path": os.path.join(first_run_folder, "0000-0001-5007-2000.xml"),
            "batch": "000",
        },
    ]
    first_run_main_table = os.path.join(first_run_folder, "main_table.json")

    # Second run
    second_run_folder = os.path.join(FIXTURES_FOLDER, "second_run")
    second_run_records = [
        {
            "orcid": "0000-0001-5000-5000",
            "path": os.path.join(second_run_folder, "0000-0001-5000-5000.xml"),
            "batch": "000",
        },
        # This record has an "error" key - but still valid for parsing:
        {
            "orcid": "0000-0001-5007-2000",
            "path": os.path.join(second_run_folder, "0000-0001-5007-2000.xml"),
            "batch": "000",
        },
    ]
    second_run_main_table = os.path.join(second_run_folder, "main_table.json")
    upsert_table = os.path.join(second_run_folder, "upsert_table.json")
    delete_table = os.path.join(second_run_folder, "delete_table.json")

    # Invalid Key
    invalid_key_orcid = {
        "orcid": "0000-0001-5010-1000",
        "path": os.path.join(FIXTURES_FOLDER, "0000-0001-5010-1000.xml"),
    }
    # ORICD doesn't match path
    mismatched_orcid = {
        "orcid": "0000-0001-5011-1000",
        "path": os.path.join(FIXTURES_FOLDER, "0000-0001-5011-1000.xml"),
    }

    # Table date fields
    timestamp_fields = ["submission_date", "last_modified_date", "created_date"]


class TestOrcidTelescope(SandboxTestCase):
    """Tests for the ORCID telescope"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dag_id = "orcid"
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")
        self.aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        self.aws_region_name = os.getenv("AWS_DEFAULT_REGION")

    def test_dag_structure(self):
        """Test that the DAG has the correct structure."""

        dag = create_dag(dag_id=self.dag_id, cloud_workspace=self.fake_cloud_workspace)
        self.assert_dag_structure(
            {
                "wait_for_prev_dag_run": ["check_dependencies"],
                "check_dependencies": ["fetch_release"],
                "fetch_release": [
                    "create_dataset",
                    "transfer_orcid",
                    "bq_create_main_table_snapshot",
                    "create_storage",
                    "create_manifests",
                    "download",
                    "transform",
                    "upload_transformed",
                    "delete_storage",
                    "bq_load_main_table",
                    "bq_load_upsert_table",
                    "bq_load_delete_table",
                    "bq_upsert_records",
                    "bq_delete_records",
                    "add_dataset_release",
                    "cleanup_workflow",
                ],
                "create_dataset": ["transfer_orcid"],
                "transfer_orcid": ["bq_create_main_table_snapshot"],
                "bq_create_main_table_snapshot": ["create_storage"],
                "create_storage": ["create_manifests"],
                "create_manifests": ["download"],
                "download": ["transform"],
                "transform": ["upload_transformed"],
                "upload_transformed": ["delete_storage"],
                "delete_storage": ["bq_load_main_table"],
                "bq_load_main_table": ["bq_load_upsert_table"],
                "bq_load_upsert_table": ["bq_load_delete_table"],
                "bq_load_delete_table": ["bq_upsert_records"],
                "bq_upsert_records": ["bq_delete_records"],
                "bq_delete_records": ["add_dataset_release"],
                "add_dataset_release": ["cleanup_workflow"],
                "cleanup_workflow": ["dag_run_complete"],
                "dag_run_complete": [],
            },
            dag,
        )

    def test_dag_load(self):
        """Test that workflow can be loaded from a DAG bag."""

        env = SandboxEnvironment(
            workflows=[
                Workflow(
                    dag_id=self.dag_id,
                    name="Orcid Telescope",
                    class_name="academic_observatory_workflows.orcid_telescope.telesope",
                    cloud_workspace=self.fake_cloud_workspace,
                )
            ]
        )

        with env.create():
            dag_file = os.path.join(module_file_path("observatory.platform.dags"), "load_dags.py")
            self.assert_dag_load(self.dag_id, dag_file)

    def test_telescope(self):
        """Test the ORCID workflow end to end."""

        env = SandboxEnvironment(project_id=TestConfig.gcp_project_id, data_location=TestConfig.gcp_data_location)
        api_bq_dataset_id = env.add_dataset("orcid_api")
        bq_dataset_id = env.add_dataset("orcid")
        orcid_bucket = env.add_bucket(prefix="orcid")

        with env.create(task_logging=True):

            clear_airflow_connections()
            upsert_airflow_connection(conn_id="aws_orcid", conn_type="http")
            upsert_airflow_connection(**TestConfig.gke_cluster_connection)

            # Make an http server to serve the test files
            task_resources = {
                "create_manifests": {"memory": "2G", "cpu": "2"},
                "latest_modified_record_date": {"memory": "2G", "cpu": "2"},
                "download": {"memory": "2G", "cpu": "2"},
                "transform": {"memory": "2G", "cpu": "2"},
                "upload_transformed": {"memory": "2G", "cpu": "2"},
                "bq_load_main_table": {"memory": "2G", "cpu": "2"},
                "bq_load_upsert_table": {"memory": "2G", "cpu": "2"},
                "bq_load_delete_table": {"memory": "2G", "cpu": "2"},
            }
            test_params = DagParams(
                dag_id="test_orcid",
                cloud_workspace=env.cloud_workspace,
                orcid_bucket=orcid_bucket,
                retries=0,
                bq_dataset_id=bq_dataset_id,
                api_bq_dataset_id=api_bq_dataset_id,
                max_workers=2,
                gke_image=TestConfig.gke_image,
                gke_namespace=TestConfig.gke_namespace,
                gke_volume_name=TestConfig.gke_volume_name,
                gke_volume_path=TestConfig.gke_volume_path,
                gke_resource_overrides=task_resources,
                test_run=True,
            )
            api = DatasetAPI(bq_project_id=env.cloud_workspace.project_id, bq_dataset_id=test_params.api_bq_dataset_id)
            api.seed_db()

            # First execution
            # Upload the test files to the test bucket
            blob_names = []
            file_paths = []
            for record in OrcidTestRecords.first_run_records:
                blob_names.append(f"{test_params.orcid_summaries_prefix}/{record['batch']}/{record['orcid']}.xml")
                file_paths.append(record["path"])
            success = gcs_upload_files(
                bucket_name=test_params.orcid_bucket, file_paths=file_paths, blob_names=blob_names
            )
            self.assertTrue(success)

            # Begin the run
            first_execution_date = pendulum.datetime(year=2023, month=6, day=1)
            with patch("academic_observatory_workflows.orcid_telescope.tasks.gcs_create_aws_transfer") as mock_transfer:
                mock_transfer.return_value = (True, 1)  # Fake transfer success
                dagrun = create_dag(dag_params=test_params).test(execution_date=first_execution_date)

            # Make assertions
            if not dagrun.state == "success":
                raise RuntimeError("Frist Dagrun did not complete successfully")

            # Main table
            main_table_id = bq_table_id(
                project_id=TestConfig.gcp_project_id, dataset_id=test_params.bq_dataset_id, table_id="orcid"
            )
            expected = load_and_parse_json(
                OrcidTestRecords.first_run_main_table,
                timestamp_fields=OrcidTestRecords.timestamp_fields,
            )
            self.assert_table_content(main_table_id, expected, primary_key="path")

            # Should be one release in the API
            api_releases = api.get_dataset_releases(dag_id=test_params.dag_id, entity_id="orcid")
            self.assertEqual(len(api_releases), 1)

            # Second execution
            # Upload the second run test files to the bucket
            blob_names = []
            file_paths = []
            for record in OrcidTestRecords.second_run_records:
                blob_names.append(f"{test_params.orcid_summaries_prefix}/{record['batch']}/{record['orcid']}.xml")
                file_paths.append(record["path"])
            success = gcs_upload_files(
                bucket_name=test_params.orcid_bucket, file_paths=file_paths, blob_names=blob_names
            )
            self.assertTrue(success)

            # Begin the run
            second_execution_date = pendulum.datetime(year=2023, month=6, day=8)
            with patch("academic_observatory_workflows.orcid_telescope.tasks.gcs_create_aws_transfer") as mock_transfer:
                mock_transfer.return_value = (True, 1)  # Fake transfer success
                dagrun = create_dag(dag_params=test_params).test(execution_date=second_execution_date)

            # Make assertions
            if not dagrun.state == "success":
                raise RuntimeError("Second Dagrun did not complete successfully")

            # Snapshotted table
            expected = load_and_parse_json(
                OrcidTestRecords.first_run_main_table,
                timestamp_fields=OrcidTestRecords.timestamp_fields,
            )
            snapshot_table_id = bq_sharded_table_id(
                project_id=TestConfig.gcp_project_id,
                dataset_id=test_params.bq_dataset_id,
                table_name="orcid_snapshot",
                date=pendulum.date(2023, 5, 28),
            )
            self.assert_table_content(snapshot_table_id, expected, primary_key="path")

            # Main table
            expected = load_and_parse_json(
                OrcidTestRecords.second_run_main_table,
                timestamp_fields=OrcidTestRecords.timestamp_fields,
            )
            self.assert_table_content(main_table_id, expected, primary_key="path")

            # Delete table
            table_id = bq_table_id(
                project_id=TestConfig.gcp_project_id, dataset_id=test_params.bq_dataset_id, table_id="orcid_delete"
            )
            expected = load_and_parse_json(
                OrcidTestRecords.delete_table,
                timestamp_fields=OrcidTestRecords.timestamp_fields,
            )
            self.assert_table_content(table_id, expected, primary_key="id")

            # Upsert table
            table_id = bq_table_id(
                project_id=TestConfig.gcp_project_id, dataset_id=test_params.bq_dataset_id, table_id="orcid_upsert"
            )
            expected = load_and_parse_json(
                OrcidTestRecords.upsert_table,
                timestamp_fields=OrcidTestRecords.timestamp_fields,
            )
            self.assert_table_content(table_id, expected, primary_key="path")

            # Should be two releases in the API
            api_releases = api.get_dataset_releases(dag_id=test_params.dag_id, entity_id="orcid")
            self.assertEqual(len(api_releases), 2)
