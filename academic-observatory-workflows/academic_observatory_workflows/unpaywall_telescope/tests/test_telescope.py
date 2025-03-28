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

# Author: Tuan Chien, James Diprose

import os
from unittest.mock import patch

import pendulum

from observatory_platform.dataset_api import DatasetAPI
from observatory_platform.airflow.airflow import clear_airflow_connections, upsert_airflow_connection
from academic_observatory_workflows.config import project_path
from academic_observatory_workflows.unpaywall_telescope.telescope import create_dag, DagParams

from academic_observatory_workflows.config import project_path, TestConfig
from observatory_platform.google.bigquery import bq_table_id, bq_sharded_table_id
from observatory_platform.airflow.workflow import Workflow
from observatory_platform.sandbox.test_utils import load_and_parse_json, SandboxTestCase
from observatory_platform.sandbox.sandbox_environment import SandboxEnvironment


FIXTURES_FOLDER = project_path("unpaywall_telescope", "tests", "fixtures")


class TestUnpaywallTelescope(SandboxTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.dag_id = "unpaywall"
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")

    def test_dag_structure(self):
        """Test that the DAG has the correct structure."""

        dag = create_dag(DagParams(dag_id=self.dag_id, cloud_workspace=self.fake_cloud_workspace))
        self.assert_dag_structure(
            {
                "wait_for_prev_dag_run": {"check_dependencies"},
                "check_dependencies": {"fetch_release"},
                "fetch_release": {
                    "load_changefiles.bq_upsert",
                    "load_snapshot.bq_load",
                    "load_snapshot.download",
                    "load_snapshot.upload_main_table_files",
                    "load_snapshot.split_main_table_file",
                    "load_snapshot.transform",
                    "load_changefiles.download",
                    "branch",
                    "cleanup_workflow",
                    "short_circuit",
                    "load_changefiles.transform",
                    "add_dataset_release",
                    "load_changefiles.extract",
                    "load_changefiles.upload",
                    "bq_create_main_table_snapshot",
                    "load_changefiles.bq_load",
                    "load_snapshot.extract",
                },
                "short_circuit": {"create_dataset"},
                "create_dataset": {"bq_create_main_table_snapshot"},
                "bq_create_main_table_snapshot": {"gke_create_storage"},
                "gke_create_storage": {"branch"},
                "branch": {"load_snapshot.download", "load_changefiles.download"},
                "load_snapshot.download": {"load_snapshot.extract"},
                "load_snapshot.extract": {"load_snapshot.transform"},
                "load_snapshot.transform": {"load_snapshot.split_main_table_file"},
                "load_snapshot.split_main_table_file": {"load_snapshot.upload_main_table_files"},
                "load_snapshot.upload_main_table_files": {"load_snapshot.bq_load"},
                "load_snapshot.bq_load": {"load_changefiles.download"},
                "load_changefiles.download": {"load_changefiles.extract"},
                "load_changefiles.extract": {"load_changefiles.transform"},
                "load_changefiles.transform": {"load_changefiles.upload"},
                "load_changefiles.upload": {"load_changefiles.bq_load"},
                "load_changefiles.bq_load": {"load_changefiles.bq_upsert"},
                "load_changefiles.bq_upsert": {"merge_branches"},
                "merge_branches": {"gke_delete_storage"},
                "gke_delete_storage": {"add_dataset_release"},
                "add_dataset_release": {"cleanup_workflow"},
                "cleanup_workflow": {"dag_run_complete"},
                "dag_run_complete": {},
            },
            dag,
        )

    def test_dag_load(self):
        """Test that workflow can be loaded from a DAG bag."""

        env = SandboxEnvironment(
            workflows=[
                Workflow(
                    dag_id=self.dag_id,
                    name="Unpaywall Telescope",
                    class_name="academic_observatory_workflows.unpaywall_telescope.telescope",
                    cloud_workspace=self.fake_cloud_workspace,
                )
            ]
        )

        with env.create():
            dag_file = os.path.join(project_path(), "..", "..", "dags", "load_dags.py")
            self.assert_dag_load(self.dag_id, dag_file)

    def test_telescope(self):
        """Test workflow end to end.

        The test files in fixtures/unpaywall have been carefully crafted to make sure that the data is loaded
        into BigQuery correctly.
        """

        env = SandboxEnvironment(project_id=TestConfig.gcp_project_id, data_location=TestConfig.gcp_data_location)
        api_bq_dataset_id = env.add_dataset("unpaywall_api")
        bq_dataset_id = env.add_dataset("unpaywall")

        with env.create(task_logging=True):

            task_resources = {
                "load_snapshot_download": {"memory": "2G", "cpu": "2"},
                "load_snapshot_extract": {"memory": "2G", "cpu": "2"},
                "load_snapshot_transform": {"memory": "2G", "cpu": "2"},
                "load_snapshot_split_main_table_file": {"memory": "2G", "cpu": "2"},
                "load_snapshot_upload_main_table_files": {"memory": "2G", "cpu": "2"},
                "load_changefiles_download": {"memory": "2G", "cpu": "2"},
                "load_changefiles_extract": {"memory": "2G", "cpu": "2"},
                "load_changefiles_transform": {"memory": "2G", "cpu": "2"},
                "load_changefiles_upload": {"memory": "2G", "cpu": "2"},
            }
            start_date = pendulum.datetime(2021, 7, 2)
            test_params = DagParams(
                dag_id="test_unpaywall",
                cloud_workspace=env.cloud_workspace,
                retries=0,
                bq_dataset_id=bq_dataset_id,
                api_bq_dataset_id=api_bq_dataset_id,
                start_date=start_date,
                unpaywall_base_url=f"http://{TestConfig.http_host_url}:{TestConfig.http_port}/unpaywall/1",  # Add /1/ for proper routing in the http-server
                gke_image=TestConfig.gke_image,
                gke_namespace=TestConfig.gke_namespace,
                gke_volume_name=TestConfig.gke_volume_name,
                gke_volume_path=TestConfig.gke_volume_path,
                gke_resource_overrides=task_resources,
                test_run=True,
            )
            api = DatasetAPI(bq_project_id=env.cloud_workspace.project_id, bq_dataset_id=test_params.api_bq_dataset_id)
            main_table_id = bq_table_id(
                test_params.cloud_workspace.project_id, test_params.bq_dataset_id, test_params.bq_table_name
            )
            upsert_table_id = f"{main_table_id}_upsert"

            ### First run: snapshot and initial changefiles
            data_interval_start = pendulum.datetime(2023, 4, 25)
            data_interval_end = data_interval_start.end_of("day")
            snapshot_date = pendulum.datetime(2023, 4, 25, 8, 30, 2)

            clear_airflow_connections()
            upsert_airflow_connection(conn_id="unpaywall", conn_type="http", password="secret")
            upsert_airflow_connection(**TestConfig.gke_cluster_connection)
            with patch("academic_observatory_workflows.unpaywall_telescope.tasks.get_http_response_json") as cfs, patch(
                "academic_observatory_workflows.unpaywall_telescope.tasks.get_filename_from_http_header"
            ) as ss:
                cfs.return_value = {
                    "list": [{"filename": "changed_dois_with_versions_2023-04-25T080001.jsonl.gz", "filetype": "jsonl"}]
                }
                ss.return_value = f"unpaywall_snapshot_{snapshot_date.format('YYYY-MM-DDTHHmmss')}.jsonl.gz"
                dagrun = create_dag(dag_params=test_params).test(execution_date=data_interval_end)

            # Make assertions
            if not dagrun.state == "success":
                raise RuntimeError("First Dagrun did not complete successfully")

            self.assert_table_integrity(upsert_table_id, expected_rows=2)
            expected_content = load_and_parse_json(
                os.path.join(FIXTURES_FOLDER, "expected", "run1_bq_upsert_records.json"),
                date_fields={"oa_date", "published_date"},
                timestamp_fields={"updated"},
            )
            self.assert_table_content(main_table_id, expected_content, "doi")
            api_releases = api.get_dataset_releases(dag_id=test_params.dag_id, entity_id="unpaywall")
            self.assertEqual(len(api_releases), 1)

            ### Second Run: No new changefiles
            data_interval_start = pendulum.datetime(2023, 4, 26)
            data_interval_end = data_interval_start.end_of("day")
            with patch("academic_observatory_workflows.unpaywall_telescope.tasks.get_http_response_json") as cfs, patch(
                "academic_observatory_workflows.unpaywall_telescope.tasks.get_filename_from_http_header"
            ) as ss:
                cfs.return_value = {"list": []}
                ss.return_value = "filename"
                dagrun = create_dag(dag_params=test_params).test(execution_date=data_interval_end)
            # Make assertions

            if not dagrun.state == "success":
                raise RuntimeError("Second Dagrun did not complete successfully")
            # Check that only 1 dataset release exists
            api_releases = api.get_dataset_releases(dag_id=test_params.dag_id, entity_id="unpaywall")
            self.assertEqual(len(api_releases), 1)

            ## Third run: waiting a couple of days and applying multiple changefiles
            prev_end_date = pendulum.datetime(2023, 4, 25, 8, 0, 1)
            data_interval_start = pendulum.datetime(2023, 4, 27)
            data_interval_end = data_interval_start.end_of("day")
            with patch("academic_observatory_workflows.unpaywall_telescope.tasks.get_http_response_json") as cfs, patch(
                "academic_observatory_workflows.unpaywall_telescope.tasks.get_filename_from_http_header"
            ) as ss:
                cfs.return_value = {
                    "list": [
                        {"filename": "changed_dois_with_versions_2023-04-27T080001.jsonl.gz", "filetype": "jsonl"},
                        {"filename": "changed_dois_with_versions_2023-04-26T080001.jsonl.gz", "filetype": "jsonl"},
                    ]
                }
                ss.return_value = f"unpaywall_snapshot_{snapshot_date.format('YYYY-MM-DDTHHmmss')}.jsonl.gz"
                dagrun = create_dag(dag_params=test_params).test(execution_date=data_interval_end)

            # Make assertions
            if not dagrun.state == "success":
                raise RuntimeError("Third Dagrun did not complete successfully")

            dst_table_id = bq_sharded_table_id(
                test_params.cloud_workspace.project_id,
                test_params.bq_dataset_id,
                "unpaywall_snapshot",
                prev_end_date,
            )
            self.assert_table_integrity(dst_table_id, expected_rows=10)
            self.assert_table_integrity(upsert_table_id, expected_rows=4)
            self.assert_table_integrity(main_table_id, expected_rows=12)
            expected_content = load_and_parse_json(
                os.path.join(FIXTURES_FOLDER, "expected", "run3_bq_upsert_records.json"),
                date_fields={"oa_date", "published_date"},
                timestamp_fields={"updated"},
            )
            self.assert_table_content(main_table_id, expected_content, "doi")
            api_releases = api.get_dataset_releases(dag_id=test_params.dag_id, entity_id="unpaywall")
            self.assertEqual(len(api_releases), 2)
