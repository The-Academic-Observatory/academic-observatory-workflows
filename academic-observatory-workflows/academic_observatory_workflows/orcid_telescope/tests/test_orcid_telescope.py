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

import csv
import datetime
import os
import re
import shutil
import tempfile
import unittest
from dataclasses import dataclass
from unittest.mock import MagicMock, patch

import pendulum
from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.utils.state import State
from google.cloud import storage

from academic_observatory_workflows.config import project_path
from academic_observatory_workflows.orcid_telescope.orcid_telescope import (
    BATCH_REGEX,
    create_dag,
    create_orcid_batch_manifest,
    latest_modified_record_date,
    MANIFEST_HEADER,
    orcid_batch_names,
    OrcidBatch,
    OrcidRelease,
    transform_orcid_record,
)
from observatory_platform.dataset_api import get_dataset_releases
from observatory_platform.config import module_file_path
from observatory_platform.google.gcs import gcs_blob_name_from_path, gcs_blob_uri, gcs_upload_files
from observatory_platform.airflow.workflow import Workflow
from observatory_platform.sandbox.sandbox_environment import (
    find_free_port,
    load_and_parse_json,
    ObservatoryEnvironment,
    ObservatoryTestCase,
    random_id,
)

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


class TestOrcidTelescope(ObservatoryTestCase):
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
                    "create_manifests",
                    "download",
                    "transform",
                    "upload_transformed",
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
                "bq_create_main_table_snapshot": ["create_manifests"],
                "create_manifests": ["download"],
                "download": ["transform"],
                "transform": ["upload_transformed"],
                "upload_transformed": ["bq_load_main_table"],
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

        env = ObservatoryEnvironment(
            workflows=[
                Workflow(
                    dag_id=self.dag_id,
                    name="Orcid Telescope",
                    class_name="academic_observatory_workflows.orcid_telescope.orcid_telescope.create_dag",
                    cloud_workspace=self.fake_cloud_workspace,
                )
            ]
        )

        with env.create():
            dag_file = os.path.join(module_file_path("observatory.platform.dags"), "load_dags.py")
            self.assert_dag_load(self.dag_id, dag_file)

    def test_telescope(self):
        """Test the ORCID workflow end to end."""

        # Create the Observatory environment and run tests for first run
        env = ObservatoryEnvironment(self.project_id, self.data_location, api_port=find_free_port())
        orcid_bucket = env.add_bucket(prefix="orcid")
        bq_dataset_id = env.add_dataset(prefix="orcid")
        with env.create(task_logging=True):
            aws_orcid_conn_id = "aws_orcid"
            bq_main_table_name = "orcid"
            bq_upsert_table_name = "orcid_upsert"
            bq_delete_table_name = "orcid_delete"
            transfer_attempts = 3
            orcid_summaries_prefix = "orcid_summaries"
            api_dataset_id = "orcid"
            dag = create_dag(
                dag_id=self.dag_id,
                cloud_workspace=env.cloud_workspace,
                orcid_bucket=orcid_bucket,
                bq_dataset_id=bq_dataset_id,
                bq_main_table_name=bq_main_table_name,
                bq_upsert_table_name=bq_upsert_table_name,
                bq_delete_table_name=bq_delete_table_name,
                transfer_attempts=transfer_attempts,
                orcid_summaries_prefix=orcid_summaries_prefix,
                aws_orcid_conn_id=aws_orcid_conn_id,
                api_dataset_id=api_dataset_id,
            )

            # Add connections
            env.add_connection(Connection(conn_id=aws_orcid_conn_id, uri="http://:aws-orcid-token@"))

            #################
            ### First Run ###
            #################

            first_execution_date = pendulum.datetime(year=2023, month=6, day=1)
            with env.create_dag_run(dag, first_execution_date) as dag_run:
                expected_release = OrcidRelease(
                    dag_id=self.dag_id,
                    run_id=dag_run.run_id,
                    cloud_workspace=env.cloud_workspace,
                    bq_dataset_id=bq_dataset_id,
                    bq_main_table_name=bq_main_table_name,
                    bq_upsert_table_name=bq_upsert_table_name,
                    bq_delete_table_name=bq_delete_table_name,
                    prev_release_end=pendulum.instance(datetime.datetime.min),  # For fist run
                    prev_latest_modified_record=pendulum.instance(datetime.datetime.min),  # For fist run
                    start_date=dag_run.data_interval_start,
                    end_date=dag_run.data_interval_end,
                    is_first_run=True,
                )

                # Wait for the previous DAG run to finish
                ti = env.run_task("wait_for_prev_dag_run")
                self.assertEqual(State.SUCCESS, ti.state)

                # Check dependencies are met
                ti = env.run_task("check_dependencies")
                self.assertEqual(State.SUCCESS, ti.state)

                # Fetch release
                ti = env.run_task("fetch_release")
                self.assertEqual(State.SUCCESS, ti.state)

                # Check that the expected release is the same as the created release
                first_release = OrcidRelease.from_dict(
                    ti.xcom_pull(
                        key="return_value",
                        task_ids="fetch_release",
                        include_prior_dates=False,
                    )
                )
                self.assertTrue(expected_release.__dict__, first_release.__dict__)

                # Create datasets
                ti = env.run_task("create_dataset")
                self.assertEqual(State.SUCCESS, ti.state)

                # Transfer ORCID records to bucket
                with patch(
                    "academic_observatory_workflows.orcid_telescope.orcid_telescope.gcs_create_aws_transfer"
                ) as mock_transfer:
                    # Transfer failures should raise an error
                    mock_transfer.side_effect = ([False, 0] for _ in range(transfer_attempts))
                    with self.assertRaises(AirflowException):
                        env.run_task("transfer_orcid")
                    dag.clear(task_ids=["transfer_orcid"])
                    # Transfer success:
                    mock_transfer.side_effect = ([False, 0], [True, 1])
                    ti = env.run_task("transfer_orcid")
                    self.assertEqual(State.SUCCESS, ti.state)
                # Upload the fixtures to the bucket to be picked up by the download
                blob_names = []
                file_paths = []
                for record in OrcidTestRecords.first_run_records:
                    blob_names.append(f"{orcid_summaries_prefix}/{record['batch']}/{record['orcid']}.xml")
                    file_paths.append(record["path"])
                success = gcs_upload_files(bucket_name=orcid_bucket, file_paths=file_paths, blob_names=blob_names)
                self.assertTrue(success)

                # Create snapshot - nothing should happen for first run
                ti = env.run_task("bq_create_main_table_snapshot")
                self.assertEqual(State.SKIPPED, ti.state)

                # Create manifests
                ti = env.run_task("create_manifests")
                self.assertEqual(State.SUCCESS, ti.state)
                self.assertTrue(os.path.exists(first_release.master_manifest_file))
                for batch in first_release.orcid_batches():
                    self.assertTrue(os.path.exists(batch.manifest_file))
                with open(first_release.master_manifest_file, "r") as f:
                    content = list(csv.DictReader(f))
                self.assertEqual(len(content), len(OrcidTestRecords.first_run_records))

                # Download the files from the transfer bucket
                # s5cmd fails for any reason:
                with patch(
                    "academic_observatory_workflows.orcid_telescope.orcid_telescope.S5Cmd.download_from_bucket"
                ) as mock_download:
                    mock_download.return_value = [1]
                    with self.assertRaisesRegex(RuntimeError, "returned non-zero exit code:"):
                        env.run_task("download")
                dag.clear(task_ids=["download"])
                for _file in first_release.downloaded_records:  # Any downloaded file needs to be removed before rerun
                    os.remove(_file)
                # Any file is missing post-download of batch
                with patch(
                    "academic_observatory_workflows.orcid_telescope.orcid_telescope.OrcidBatch.missing_records"
                ) as mock_missing_records:
                    mock_missing_records.return_value = ["some_missing_file.xml"]
                    with self.assertRaisesRegex(FileNotFoundError, "All files were not downloaded"):
                        env.run_task("download")
                dag.clear(task_ids=["download"])
                for _file in first_release.downloaded_records:
                    os.remove(_file)
                # Any file is missing post-download of all batches
                with patch(
                    "academic_observatory_workflows.orcid_telescope.orcid_telescope.OrcidRelease.downloaded_records"
                ) as mock_downloaded_records:
                    mock_downloaded_records.return_value = []
                    with self.assertRaisesRegex(FileNotFoundError, "found 0 records on disk."):
                        env.run_task("download")
                dag.clear(task_ids=["download"])
                for _file in first_release.downloaded_records:
                    os.remove(_file)
                # Successful download (clean up the directory first):
                ti = env.run_task("download")
                self.assertEqual(State.SUCCESS, ti.state)
                for batch in first_release.orcid_batches():
                    self.assertIs(len(batch.missing_records), 0)
                self.assertEqual(len(OrcidTestRecords.first_run_records), len(first_release.downloaded_records))

                # Transform the files
                # Number of batch files processed does not match number of records
                with patch(
                    "academic_observatory_workflows.orcid_telescope.orcid_telescope.OrcidBatch.expected_records"
                ) as mock_expected_records:
                    mock_expected_records.return_value = [0]
                    with self.assertRaisesRegex(ValueError, "Expected 0 records but got"):
                        env.run_task("transform")
                dag.clear(task_ids=["transform"])
                # Total number of files processed does not equal number of files downloaded
                with patch(
                    "academic_observatory_workflows.orcid_telescope.orcid_telescope.OrcidRelease.downloaded_records"
                ) as mock_downloaded_records:
                    mock_downloaded_records.return_value = []
                    with self.assertRaisesRegex(ValueError, "Expected 0 total records processed"):
                        env.run_task("transform")
                dag.clear(task_ids=["transform"])
                ti = env.run_task("transform")
                self.assertEqual(State.SUCCESS, ti.state)
                self.assertEqual(len(first_release.upsert_files), 3)  # Number of unique 'batches'
                self.assertEqual(len(first_release.delete_files), 0)  # No "error" records

                # Upload the transformed files
                ti = env.run_task("upload_transformed")
                self.assertEqual(State.SUCCESS, ti.state)
                for file_path in first_release.upsert_files + first_release.delete_files:
                    self.assert_blob_integrity(env.transform_bucket, gcs_blob_name_from_path(file_path), file_path)

                # BQ load the main table
                # Number of blobs in storage not equal to local transformed file count
                with patch(
                    "academic_observatory_workflows.orcid_telescope.orcid_telescope.OrcidRelease.upsert_files"
                ) as mock_upsert_files:
                    mock_upsert_files.return_value = []
                    with self.assertRaisesRegex(ValueError, "Number of blobs"):
                        env.run_task("bq_load_main_table")
                dag.clear(task_ids=["bq_load_main_table"])
                ti = env.run_task("bq_load_main_table")
                self.assertEqual(State.SUCCESS, ti.state)

                # BQ load the upsert table - should do nothing
                ti = env.run_task("bq_load_upsert_table")
                self.assertEqual(State.SKIPPED, ti.state)

                # BQ load the delete table - should do nothing
                ti = env.run_task("bq_load_delete_table")
                self.assertEqual(State.SKIPPED, ti.state)

                # BQ upsert records - should do nothing
                ti = env.run_task("bq_upsert_records")
                self.assertEqual(State.SKIPPED, ti.state)

                # BQ delete records - should do nothing
                ti = env.run_task("bq_delete_records")
                self.assertEqual(State.SKIPPED, ti.state)

                # Check that table load is as expected
                self.assert_table_integrity(first_release.bq_main_table_id, 4)
                expected_content = load_and_parse_json(
                    OrcidTestRecords.first_run_main_table, timestamp_fields=OrcidTestRecords.timestamp_fields
                )
                self.assert_table_content(first_release.bq_main_table_id, expected_content, primary_key="path")
                first_run_last_modified = latest_modified_record_date(first_release.master_manifest_file)

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
                self.assert_cleanup(first_release.workflow_folder)

            ##################
            ### Second Run ###
            ##################

            second_execution_date = pendulum.datetime(year=2023, month=6, day=8)
            with env.create_dag_run(dag, second_execution_date) as dag_run:
                expected_release = OrcidRelease(
                    dag_id=self.dag_id,
                    run_id=dag_run.run_id,
                    cloud_workspace=env.cloud_workspace,
                    bq_dataset_id=bq_dataset_id,
                    bq_main_table_name=bq_main_table_name,
                    bq_upsert_table_name=bq_upsert_table_name,
                    bq_delete_table_name=bq_delete_table_name,
                    prev_release_end=first_release.end_date,  # For second run
                    prev_latest_modified_record=first_run_last_modified,  # For second run
                    start_date=dag_run.data_interval_start,
                    end_date=dag_run.data_interval_end,
                    is_first_run=False,
                )

                dag_run.dag.set_task_instance_state(
                    task_id="wait_for_prev_dag_run", state=State.SUCCESS, run_id=dag_run.run_id
                )
                # ti = env.run_task("wait_for_prev_dag_run")
                # self.assertEqual(State.SUCCESS, ti.state)
                ti = env.run_task("check_dependencies")
                self.assertEqual(State.SUCCESS, ti.state)

                # Fetch release
                ti = env.run_task("fetch_release")
                self.assertEqual(State.SUCCESS, ti.state)

                # Check that the expected release is the same as the created release
                second_release = OrcidRelease.from_dict(
                    ti.xcom_pull(
                        key="return_value",
                        task_ids="fetch_release",
                        include_prior_dates=False,
                    )
                )
                self.assertTrue(expected_release.__dict__, second_release.__dict__)

                ti = env.run_task("create_dataset")
                self.assertEqual(State.SUCCESS, ti.state)

                # Transfer ORCID records to bucket
                with patch(
                    "academic_observatory_workflows.orcid_telescope.orcid_telescope.gcs_create_aws_transfer"
                ) as mock_transfer:
                    mock_transfer.return_value = [True, 1]
                    ti = env.run_task("transfer_orcid")
                    self.assertEqual(State.SUCCESS, ti.state)
                # Upload the fixtures to the bucket to be picked up by the download
                blob_names = []
                file_paths = []
                for record in OrcidTestRecords.second_run_records:
                    blob_names.append(f"{orcid_summaries_prefix}/{record['batch']}/{record['orcid']}.xml")
                    file_paths.append(record["path"])
                success = gcs_upload_files(bucket_name=orcid_bucket, file_paths=file_paths, blob_names=blob_names)
                self.assertTrue(success)

                # Create snapshot
                ti = env.run_task("bq_create_main_table_snapshot")
                self.assertEqual(State.SUCCESS, ti.state)
                # Check that table load is as expected
                self.assert_table_integrity(second_release.bq_snapshot_table_id, 4)
                self.assert_table_integrity(first_release.bq_main_table_id, 4)
                expected_content = load_and_parse_json(
                    OrcidTestRecords.first_run_main_table, timestamp_fields=OrcidTestRecords.timestamp_fields
                )
                self.assert_table_content(first_release.bq_main_table_id, expected_content, primary_key="path")

                # Create manifests
                ti = env.run_task("create_manifests")
                self.assertEqual(State.SUCCESS, ti.state)
                self.assertTrue(os.path.exists(second_release.master_manifest_file))
                with open(second_release.master_manifest_file, "r") as f:
                    content = list(csv.DictReader(f))
                self.assertEqual(len(content), len(OrcidTestRecords.second_run_records))
                for batch in second_release.orcid_batches():
                    self.assertTrue(os.path.exists(batch.manifest_file))

                # Download the files from the transfer bucket
                ti = env.run_task("download")
                self.assertEqual(State.SUCCESS, ti.state)
                for batch in second_release.orcid_batches():
                    self.assertIs(len(batch.missing_records), 0)
                self.assertEqual(len(OrcidTestRecords.second_run_records), len(second_release.downloaded_records))

                # Transform the files
                ti = env.run_task("transform")
                self.assertEqual(State.SUCCESS, ti.state)
                self.assertEqual(len(second_release.upsert_files), 1)  # One upsert record - 000 batch
                self.assertEqual(len(second_release.delete_files), 1)  # One "error" record - 000 batch

                # Upload the transformed files
                ti = env.run_task("upload_transformed")
                self.assertEqual(State.SUCCESS, ti.state)
                for file_path in second_release.upsert_files + second_release.delete_files:
                    self.assert_blob_integrity(env.transform_bucket, gcs_blob_name_from_path(file_path), file_path)

                # BQ load the main table - Should do nothing
                ti = env.run_task("bq_load_main_table")
                self.assertEqual(State.SKIPPED, ti.state)

                # BQ load the upsert table
                ti = env.run_task("bq_load_upsert_table")
                self.assertEqual(State.SUCCESS, ti.state)
                self.assert_table_integrity(second_release.bq_upsert_table_id, 1)
                expected_content = load_and_parse_json(
                    OrcidTestRecords.upsert_table, timestamp_fields=OrcidTestRecords.timestamp_fields
                )
                self.assert_table_content(second_release.bq_upsert_table_id, expected_content, primary_key="path")

                # BQ load the delete table
                ti = env.run_task("bq_load_delete_table")
                self.assertEqual(State.SUCCESS, ti.state)
                self.assert_table_integrity(second_release.bq_delete_table_id, 1)
                expected_content = load_and_parse_json(
                    OrcidTestRecords.delete_table, timestamp_fields=OrcidTestRecords.timestamp_fields
                )
                self.assert_table_content(second_release.bq_delete_table_id, expected_content, primary_key="id")

                # BQ upsert records
                ti = env.run_task("bq_upsert_records")
                self.assertEqual(State.SUCCESS, ti.state)

                # BQ delete records
                ti = env.run_task("bq_delete_records")
                self.assertEqual(State.SUCCESS, ti.state)

                # Check the main table now
                self.assert_table_integrity(second_release.bq_main_table_id, 3)
                expected_content = load_and_parse_json(
                    OrcidTestRecords.second_run_main_table, timestamp_fields=OrcidTestRecords.timestamp_fields
                )
                self.assert_table_content(second_release.bq_main_table_id, expected_content, primary_key="path")

                # Test that DatasetRelease is added to database
                dataset_releases = get_dataset_releases(dag_id=self.dag_id, dataset_id=api_dataset_id)
                self.assertEqual(len(dataset_releases), 1)
                ti = env.run_task("add_dataset_release")
                self.assertEqual(State.SUCCESS, ti.state)
                dataset_releases = get_dataset_releases(dag_id=self.dag_id, dataset_id=api_dataset_id)
                self.assertEqual(len(dataset_releases), 2)

                # Test that all workflow data deleted
                ti = env.run_task("cleanup_workflow")
                self.assertEqual(State.SUCCESS, ti.state)
                self.assert_cleanup(first_release.workflow_folder)


class TestOrcidBatch(unittest.TestCase):
    def test_orcid_batch(self):
        """Test that the orcid batches are correctly constructed"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            download_dir = os.path.join(tmp_dir, "download")
            transform_dir = os.path.join(tmp_dir, "transform")
            test_batch_str = "12X"

            # Download/transform dirs don't exist
            with self.assertRaises(NotADirectoryError):
                OrcidBatch(download_dir, transform_dir, test_batch_str)
            os.makedirs(download_dir)
            with self.assertRaises(NotADirectoryError):
                OrcidBatch(download_dir, transform_dir, test_batch_str)
            os.makedirs(transform_dir)

            # Invalid batch string
            for batch_str in ["0000", "12C", "99", "XXX"]:
                with self.assertRaises(ValueError):
                    OrcidBatch(download_dir, transform_dir, batch_str)

            # Create a batch for testing
            test_batch = OrcidBatch(download_dir, transform_dir, test_batch_str)

            # Check that expected folders exist
            self.assertTrue(os.path.isdir(test_batch.download_dir))

            # Check that file names are as expected
            self.assertEqual(test_batch.download_batch_dir, os.path.join(download_dir, test_batch_str))
            self.assertEqual(test_batch.download_log_file, os.path.join(download_dir, f"{test_batch_str}_log.txt"))
            self.assertEqual(test_batch.download_error_file, os.path.join(download_dir, f"{test_batch_str}_error.txt"))
            self.assertEqual(test_batch.manifest_file, os.path.join(download_dir, f"{test_batch_str}_manifest.csv"))
            self.assertEqual(
                test_batch.transform_upsert_file, os.path.join(transform_dir, f"{test_batch_str}_upsert.jsonl.gz")
            )
            self.assertEqual(
                test_batch.transform_delete_file, os.path.join(transform_dir, f"{test_batch_str}_delete.jsonl.gz")
            )

            # Make the manifest file
            shutil.copy(os.path.join(FIXTURES_FOLDER, "test_manifest.csv"), test_batch.manifest_file)

            # Check that missing, expected and existing records are correctly identified
            records = [os.path.basename(record["path"]) for record in OrcidTestRecords.first_run_records]
            self.assertEqual(set(test_batch.expected_records), set(records))
            self.assertEqual(test_batch.existing_records, [])
            self.assertEqual(set(test_batch.missing_records), set(records))
            for record in OrcidTestRecords.first_run_records:
                path = record["path"]
                shutil.copy(path, test_batch.download_batch_dir)
            self.assertEqual(set(test_batch.expected_records), set(records))
            self.assertEqual(set(test_batch.existing_records), set(records))
            self.assertEqual(test_batch.missing_records, [])

            # Check that the blob uris are correctly generated
            expected_blob_uris = []
            for record in OrcidTestRecords.first_run_records:
                expected_blob_uris.append(gcs_blob_uri("orcid-testing", f"orcid_summaries/000/{record['orcid']}.xml"))
            self.assertEqual(set(test_batch.blob_uris), set(expected_blob_uris))


class TestCreateOrcidBatchManifest(ObservatoryTestCase):
    dag_id = "orcid"
    aws_key = (os.getenv("AWS_ACCESS_KEY_ID"), os.getenv("AWS_SECRET_ACCESS_KEY"))
    aws_region_name = os.getenv("AWS_DEFAULT_REGION")

    def test_create_orcid_batch_manifest(self):
        """Tests the create_orcid_batch_manifest function"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            download_dir = os.path.join(tmp_dir, "download")
            transform_dir = os.path.join(tmp_dir, "transform")
            test_batch_str = "12X"
            # Create a batch for testing
            test_batch = OrcidBatch(download_dir, transform_dir, test_batch_str)

            # Upload the .xml files to the test bucket
            client = storage.Client()
            bucket_id = f"orcid_test_{random_id()}"
            bucket = client.create_bucket(bucket_id)

            blob1 = storage.Blob(f"{test_batch_str}/0000-0001-5000-1000.xml", bucket)
            blob1.upload_from_string("Test data 1")
            # Make now the reference time - blob1 should be ignored
            reference_time = pendulum.now()
            blob2 = storage.Blob(f"{test_batch_str}/0000-0001-5000-2000.xml", bucket)
            blob2.upload_from_string("Test data 2")
            blob3 = storage.Blob(f"{test_batch_str}/0000-0001-5000-3000.xml", bucket)
            blob3.upload_from_string("Test data 3")
            # Put a blob in a different folder - should be ignored
            blob4 = storage.Blob(f"somewhere_else/{test_batch_str}/0000-0001-5000-4000.xml", bucket)
            blob4.upload_from_string("Test data 4")

            create_orcid_batch_manifest(orcid_batch=test_batch, reference_time=reference_time, bucket=bucket_id)
            with open(test_batch.manifest_file, "w", newline="") as csvfile:
                reader = csv.reader(csvfile)
                manifest_rows = [row for row in reader]
            bucket = [row[0] for row in manifest_rows]
            blobs = [row[1] for row in manifest_rows]
            orcid = [row[2] for row in manifest_rows]
            modification_times = [row[3] for row in manifest_rows]
            self.assertEqual(len(manifest_rows), 2)
            self.assertEqual(set(blobs), set([blob2.name, blob3.name]))
            self.assertEqual(set(orcid), set(["0000-0001-5000-2000", "0000-0001-5000-3000"]))
            self.assertEqual(set(modification_times), set([blob2.updated.isoformat(), blob3.updated.isoformat()]))


class TestCreateOrcidBatchManifest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.list_blobs_path = "academic_observatory_workflows.orcid_telescope.orcid_telescope.gcs_list_blobs"
        self.test_batch_str = "12X"
        self.bucket_name = "test-bucket"

    def test_create_orcid_batch_manifest(self):
        """Tests that the manifest file is created with the correct header and contains the correct blob names and
        modification dates"""

        updated_dates = [
            datetime.datetime(2022, 12, 31),
            datetime.datetime(2023, 1, 1),
            datetime.datetime(2023, 1, 1, 1),
            datetime.datetime(2023, 1, 2),
        ]
        blobs = []
        for i, updated in enumerate(updated_dates):
            blob = MagicMock()
            blob.name = f"{self.test_batch_str}/blob{i+1}"
            blob.bucket.name = self.bucket_name
            blob.updated = updated
            blobs.append(blob)

        reference_date = pendulum.datetime(2023, 1, 1)
        with tempfile.TemporaryDirectory() as tmp_dir:
            transform_dir = os.path.join(tmp_dir, "transform")
            os.mkdir(transform_dir)
            test_batch = OrcidBatch(tmp_dir, transform_dir, self.test_batch_str)
            with patch(self.list_blobs_path, return_value=blobs):
                create_orcid_batch_manifest(test_batch, reference_date, self.bucket_name)

            # Assert manifest file is created with correct header and content
            with open(test_batch.manifest_file, "r") as f:
                reader = csv.DictReader(f)
                rows = list(reader)

            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["blob_name"], blobs[-2].name)
            self.assertEqual(rows[0]["updated"], str(blobs[-2].updated))
            self.assertEqual(rows[1]["blob_name"], blobs[-1].name)
            self.assertEqual(rows[1]["updated"], str(blobs[-1].updated))

    def test_no_results(self):
        """Tests that the manifest file is not created if there are no blobs modified after the reference date"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            transform_dir = os.path.join(tmp_dir, "transform")
            os.mkdir(transform_dir)
            test_batch = OrcidBatch(tmp_dir, transform_dir, self.test_batch_str)

            # Mock gcs_list_blobs
            blob = MagicMock()
            blob.name = f"{self.test_batch_str}/blob1"
            blob.bucket.name = self.bucket_name
            blob.updated = datetime.datetime(2022, 6, 1)
            with patch(self.list_blobs_path, return_value=[blob]):
                create_orcid_batch_manifest(test_batch, pendulum.datetime(2023, 1, 1), self.bucket_name)

            # Assert manifest file is created
            self.assertTrue(os.path.exists(test_batch.manifest_file))
            with open(test_batch.manifest_file, "r") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
            self.assertEqual(len(rows), 0)


class TestTransformOrcidRecord(unittest.TestCase):
    def test_valid_record(self):
        """Tests that a valid ORCID record with 'record' section is transformed correctly"""
        for asset in OrcidTestRecords.first_run_records:
            orcid = asset["orcid"]
            path = asset["path"]
            transformed_record = transform_orcid_record(path)
            self.assertIsInstance(transformed_record, dict)
            self.assertEqual(transformed_record["orcid_identifier"]["path"], orcid)

    def test_error_record(self):
        """Tests that an ORCID record with 'error' section is transformed correctly"""
        error_record = OrcidTestRecords.second_run_records[1]
        orcid = error_record["orcid"]
        path = error_record["path"]
        transformed_record = transform_orcid_record(path)
        self.assertIsInstance(transformed_record, str)
        self.assertEqual(transformed_record, orcid)

    def test_invalid_key_record(self):
        """Tests that an ORCID record with no 'error' or 'record' section raises a Key Error"""
        invaid_key_record = OrcidTestRecords.invalid_key_orcid
        path = invaid_key_record["path"]
        with self.assertRaises(KeyError):
            transform_orcid_record(path)

    def test_mismatched_orcid(self):
        """Tests that a ValueError is raised if the ORCID in the file name does not match the ORCID in the record"""
        mismatched_orcid = OrcidTestRecords.mismatched_orcid
        path = mismatched_orcid["path"]
        with self.assertRaisesRegex(ValueError, "does not match ORCID in record"):
            transform_orcid_record(path)


class TestExtras(unittest.TestCase):
    def test_latest_modified_record_date(self):
        """Tests that the latest_modified_record_date function returns the correct date"""
        # Create a temporary manifest file for the test
        with tempfile.NamedTemporaryFile() as temp_file:
            with open(temp_file.name, "w") as f:
                f.write(",".join(MANIFEST_HEADER))
                f.write("\n")
                f.write("gs://test-bucket,folder/0000-0000-0000-0001.xml,2023-06-03T00:00:00Z\n")
                f.write("gs://test-bucket,folder/0000-0000-0000-0002.xml,2023-06-03T00:00:00Z\n")
                f.write("gs://test-bucket,folder/0000-0000-0000-0003.xml,2023-06-02T00:00:00Z\n")
                f.write("gs://test-bucket,folder/0000-0000-0000-0004.xml,2023-06-01T00:00:00Z\n")

            # Call the function and assert the result
            expected_date = pendulum.parse("2023-06-03T00:00:00Z")
            actual_date = latest_modified_record_date(temp_file.name)
            self.assertEqual(actual_date, expected_date)

    def test_orcid_batch_names(self):
        """Tests that the orcid_batch_names function returns the expected results"""
        batch_names = orcid_batch_names()

        # Test that the function returns a list
        self.assertIsInstance(batch_names, list)
        self.assertEqual(len(batch_names), 1100)
        self.assertTrue(all(isinstance(element, str) for element in batch_names))
        self.assertEqual(len(set(batch_names)), len(batch_names))
        # Test that the batch names match the OrcidBatch regex
        for batch_name in batch_names:
            self.assertTrue(re.match(BATCH_REGEX, batch_name))
