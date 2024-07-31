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

# Author: Aniek Roelofs, James Diprose

import os
import unittest

import httpretty
import pendulum
from airflow.models import Connection
from airflow.utils.state import State

from academic_observatory_workflows.config import project_path
from academic_observatory_workflows.crossref_metadata_telescope.telescope import create_dag, DagParams
from academic_observatory_workflows.crossref_metadata_telescope.release import CrossrefMetadataRelease
from academic_observatory_workflows.crossref_metadata_telescope.tasks import make_snapshot_url
from observatory_platform.google.bigquery import bq_sharded_table_id
from observatory_platform.files import is_gzip, list_files
from observatory_platform.google.gcs import gcs_blob_name_from_path
from observatory_platform.airflow.workflow import Workflow
from observatory_platform.sandbox.sandbox_environment import SandboxEnvironment
from observatory_platform.sandbox.test_utils import find_free_port, SandboxTestCase

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
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")
        self.download_path = os.path.join(FIXTURES_FOLDER, "crossref_metadata.json.tar.gz")

    def test_dag_structure(self):
        """Test that the DAG has the correct structure."""

        dag = create_dag(DagParams(dag_id=self.dag_id, cloud_workspace=self.fake_cloud_workspace))
        self.assert_dag_structure(
            {
                "check_dependencies": ["_fetch_release"],
                # fetch_release passes an XCom to all of these tasks
                "_fetch_release": [
                    "gke_create_storage",
                    "_download",
                    "_upload_downloaded",
                    "_extract",
                    "_transform",
                    "_upload_transformed",
                    "_bq_load",
                    "_add_dataset_release",
                    "_cleanup_workflow",
                ],
                "gke_create_storage": ["_download"],
                "_download": ["_upload_downloaded"],
                "_upload_downloaded": ["_extract"],
                "_extract": ["_transform"],
                "_transform": ["_upload_transformed"],
                "_upload_transformed": ["_bq_load"],
                "_bq_load": ["gke_delete_storage"],
                "gke_delete_storage": ["_add_dataset_release"],
                "_add_dataset_release": ["_cleanup_workflow"],
                "_cleanup_workflow": [],
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

    @unittest.skip
    def test_telescope(self):
        """Test the Crossref Metadata telescope end to end."""

        env = SandboxEnvironment(self.project_id, self.data_location, api_port=find_free_port())
        bq_dataset_id = env.add_dataset()
        crossref_metadata_conn_id = "crossref_metadata"
        bq_table_name = "crossref_metadata"
        api_dataset_id = "crossref_metadata"
        batch_size = 20

        with env.create():
            # Setup Workflow
            # Execution date is always the 7th of the month and the start of the data interval
            # The DAG run for execution date of 2023-01-07 actually runs on 2023-02-07
            dag = create_dag(
                dag_id=self.dag_id,
                cloud_workspace=env.cloud_workspace,
                bq_dataset_id=bq_dataset_id,
                crossref_metadata_conn_id=crossref_metadata_conn_id,
                bq_table_name=bq_table_name,
                api_dataset_id=api_dataset_id,
                batch_size=batch_size,
            )
            logical_date = pendulum.datetime(year=2023, month=1, day=7)

            # Add Crossref Metadata connection
            env.add_connection(Connection(conn_id=crossref_metadata_conn_id, uri="http://:crossref-token@"))

            with env.create_dag_run(dag, logical_date) as dag_run:
                # Mocked and expected data
                # Snapshot date is the end of the execution date month
                snapshot_date = logical_date.end_of("month")
                release = CrossrefMetadataRelease(
                    dag_id=self.dag_id,
                    run_id=dag_run.run_id,
                    snapshot_date=snapshot_date,
                    cloud_workspace=env.cloud_workspace,
                    batch_size=batch_size,
                )

                # Test that all dependencies are specified: no error should be thrown
                ti = env.run_task("check_dependencies")
                self.assertEqual(State.SUCCESS, ti.state)

                # Test check release exists task, next tasks should not be skipped
                url = make_snapshot_url(logical_date)
                with httpretty.enabled():
                    httpretty.register_uri(httpretty.HEAD, url, body="", status=302)
                    ti = env.run_task("fetch_release")
                    self.assertEqual(State.SUCCESS, ti.state)

                # Test download task
                with httpretty.enabled():
                    self.setup_mock_file_download(url, self.download_path)
                    ti = env.run_task("download")
                    self.assertEqual(State.SUCCESS, ti.state)
                expected_file_hash = "047770ae386f3376c08e3975d7f06016"
                self.assert_file_integrity(release.download_file_path, expected_file_hash, "md5")
                self.assertTrue(is_gzip(release.download_file_path))

                # Test that file uploaded
                ti = env.run_task("upload_downloaded")
                self.assertEqual(State.SUCCESS, ti.state)
                self.assert_blob_integrity(
                    env.download_bucket, gcs_blob_name_from_path(release.download_file_path), release.download_file_path
                )

                # Test that file extracted
                ti = env.run_task("extract")
                self.assertEqual(State.SUCCESS, ti.state)
                file_paths = list_files(release.extract_folder, release.extract_files_regex)
                self.assertEqual(5, len(file_paths))
                for file_path in file_paths:
                    self.assertTrue(os.path.isfile(file_path))
                    self.assertFalse(is_gzip(file_path))

                # Test that files transformed
                ti = env.run_task("transform")
                self.assertEqual(State.SUCCESS, ti.state)
                file_paths = list_files(release.transform_folder, release.transform_files_regex)
                self.assertEqual(5, len(file_paths))
                for file_path in file_paths:
                    self.assertTrue(os.path.isfile(file_path))

                # Test that transformed files uploaded
                ti = env.run_task("upload_transformed")
                self.assertEqual(State.SUCCESS, ti.state)
                for file_path in list_files(release.transform_folder, release.transform_files_regex):
                    self.assert_blob_integrity(env.transform_bucket, gcs_blob_name_from_path(file_path), file_path)

                # Test that data loaded into BigQuery
                ti = env.run_task("bq_load")
                self.assertEqual(State.SUCCESS, ti.state)
                table_id = bq_sharded_table_id(self.project_id, bq_dataset_id, bq_table_name, release.snapshot_date)
                expected_rows = 20
                self.assert_table_integrity(table_id, expected_rows)

                # Test that DatasetRelease is added to database
                dataset_releases = get_dataset_releases(dag_id=self.dag_id, dataset_id=api_dataset_id)
                self.assertEqual(len(dataset_releases), 0)
                ti = env.run_task("add_dataset_release")
                self.assertEqual(State.SUCCESS, ti.state)
                dataset_releases = get_dataset_releases(dag_id=self.dag_id, dataset_id=api_dataset_id)
                self.assertEqual(len(dataset_releases), 1)

                # Test that all workflow data deleted
                ti = env.run_task("cleanup_workflow")
                self.assertEqual(State.SUCCESS, ti.state)
                self.assert_cleanup(release.workflow_folder)
