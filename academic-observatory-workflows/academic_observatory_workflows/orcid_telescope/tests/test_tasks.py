from __future__ import annotations

import csv
import datetime
from dataclasses import dataclass
import os
import re
import shutil
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from airflow.exceptions import AirflowException, AirflowSkipException
import pendulum

from academic_observatory_workflows.config import project_path, TestConfig
from academic_observatory_workflows.orcid_telescope import tasks
from academic_observatory_workflows.orcid_telescope.batch import OrcidBatch, BATCH_REGEX
from academic_observatory_workflows.orcid_telescope.release import OrcidRelease, orcid_batch_names
from observatory_platform.airflow.workflow import CloudWorkspace
from observatory_platform.airflow.airflow import upsert_airflow_connection, clear_airflow_connections
from observatory_platform.dataset_api import DatasetAPI, DatasetRelease
from observatory_platform.date_utils import datetime_normalise
from observatory_platform.files import load_jsonl
from observatory_platform.google.gcs import gcs_upload_file
from observatory_platform.google import bigquery as bq
from observatory_platform.sandbox.sandbox_environment import SandboxEnvironment
from observatory_platform.sandbox.test_utils import SandboxTestCase

FIXTURES_FOLDER = project_path("orcid_telescope", "tests", "fixtures")


def dummy_release():
    """Create a non-functional release object"""

    cloud_workspace = CloudWorkspace(
        project_id="",
        data_location="",
        download_bucket="",
        transform_bucket="",
    )
    return OrcidRelease(
        dag_id="test_orcid",
        run_id="test_orcid_run",
        bq_dataset_id="test_orcid",
        bq_main_table_name="test_orcid",
        bq_upsert_table_name="test_orcid_upsert",
        bq_delete_table_name="test_orcid_delete",
        cloud_workspace=cloud_workspace,
        start_date=pendulum.datetime(2024, 1, 1),
        end_date=pendulum.datetime(2024, 1, 1),
        prev_release_end=pendulum.now(),
        prev_latest_modified_record=pendulum.now(),
        is_first_run=True,
    )


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


class TestFetchRelease(unittest.TestCase):

    dag_id = "test_orcid"
    run_id = "test_orcid_run"
    bq_main_table_name = "test_orcid"
    bq_upsert_table_name = "test_orcid_upsert"
    bq_delete_table_name = "test_orcid_delete"

    def test_fetch_release(self):
        """Tests the fetch_release function. Runs the function once for first run functionality, then again."""
        data_interval_start = pendulum.datetime(2023, 6, 1)
        data_interval_end = pendulum.datetime(2023, 6, 8)
        env = SandboxEnvironment(project_id=TestConfig.gcp_project_id, data_location=TestConfig.gcp_data_location)
        bq_dataset_id = env.add_dataset()
        api_bq_dataset_id = env.add_dataset()
        with env.create():
            with patch("academic_observatory_workflows.orcid_telescope.tasks.is_first_dag_run") as mock_ifdr:
                mock_ifdr.return_value = True
                actual_release = tasks.fetch_release(
                    dag_id=self.dag_id,
                    run_id=self.run_id,
                    dag_run=MagicMock(),
                    data_interval_start=data_interval_start,
                    data_interval_end=data_interval_end,
                    cloud_workspace=env.cloud_workspace,
                    api_bq_dataset_id=api_bq_dataset_id,
                    bq_dataset_id=bq_dataset_id,
                    bq_main_table_name=self.bq_main_table_name,
                    bq_upsert_table_name=self.bq_upsert_table_name,
                    bq_delete_table_name=self.bq_delete_table_name,
                )

            expected_release = {
                "dag_id": "test_orcid",
                "run_id": "test_orcid_run",
                "cloud_workspace": env.cloud_workspace.to_dict(),
                "bq_dataset_id": bq_dataset_id,
                "bq_main_table_name": "test_orcid",
                "bq_upsert_table_name": "test_orcid_upsert",
                "bq_delete_table_name": "test_orcid_delete",
                "start_date": data_interval_start.timestamp(),
                "end_date": data_interval_end.timestamp(),
                "prev_release_end": pendulum.instance(datetime.datetime.min).timestamp(),
                "prev_latest_modified_record": pendulum.instance(datetime.datetime.min).timestamp(),
                "is_first_run": True,
            }
            self.assertEqual(expected_release, actual_release)

            # Populate the API with a release
            api = DatasetAPI(bq_project_id=env.cloud_workspace.project_id, bq_dataset_id=api_bq_dataset_id)
            api.seed_db()
            release = OrcidRelease.from_dict(actual_release)
            dataset_release = DatasetRelease(
                dag_id=self.dag_id,
                entity_id="orcid",
                dag_run_id=self.run_id,
                created=pendulum.now(),
                modified=pendulum.now(),
                changefile_start_date=release.start_date,
                changefile_end_date=release.end_date,
                extra={"latest_modified_record_date": data_interval_end.to_iso8601_string()},
            )
            api.add_dataset_release(dataset_release)

            # Check that fetch_release behaves differently now
            data_interval_start = pendulum.datetime(2023, 6, 8)
            data_interval_end = pendulum.datetime(2023, 6, 15)
            actual_release = tasks.fetch_release(
                dag_id=self.dag_id,
                run_id=self.run_id,
                dag_run=MagicMock(),
                data_interval_start=data_interval_start,
                data_interval_end=data_interval_end,
                cloud_workspace=env.cloud_workspace,
                api_bq_dataset_id=api_bq_dataset_id,
                bq_dataset_id=bq_dataset_id,
                bq_main_table_name=self.bq_main_table_name,
                bq_upsert_table_name=self.bq_upsert_table_name,
                bq_delete_table_name=self.bq_delete_table_name,
            )

            expected_release = {
                "dag_id": "test_orcid",
                "run_id": "test_orcid_run",
                "cloud_workspace": env.cloud_workspace.to_dict(),
                "bq_dataset_id": bq_dataset_id,
                "bq_main_table_name": "test_orcid",
                "bq_upsert_table_name": "test_orcid_upsert",
                "bq_delete_table_name": "test_orcid_delete",
                "start_date": data_interval_start.timestamp(),
                "end_date": data_interval_end.timestamp(),
                "prev_release_end": pendulum.instance(data_interval_start).timestamp(),
                "prev_latest_modified_record": pendulum.instance(data_interval_start).timestamp(),
                "is_first_run": False,
            }
            self.assertEqual(expected_release, actual_release)


class TestTransferOrcid(unittest.TestCase):

    transfer_attempts = 3
    aws_conn_id = "aws_test_transfer_orcid"

    def test_transfer_orcid(self):
        """Test that the transfer_orcid_function succeeds when a successful return is handed back"""
        with SandboxEnvironment().create(), patch(
            "academic_observatory_workflows.orcid_telescope.tasks.gcs_create_aws_transfer"
        ) as mock_transfer:
            clear_airflow_connections()
            upsert_airflow_connection(conn_id=self.aws_conn_id, conn_type="http", login="", password="")
            # One failure, but 3 total attempts so it should pass
            mock_transfer.side_effect = ([False, 0], [True, 1])
            tasks.transfer_orcid(
                release=dummy_release().to_dict(),
                aws_orcid_conn_id=self.aws_conn_id,
                transfer_attempts=self.transfer_attempts,
                orcid_bucket="orcid_bucket",
                orcid_summaries_prefix="orcid_summaries_prefix",
            )

    def test_transfer_orcid_fails(self):
        """Test that the transfer_orcid_function fails when an unsuccessful return is handed back"""
        with SandboxEnvironment().create(), patch(
            "academic_observatory_workflows.orcid_telescope.tasks.gcs_create_aws_transfer"
        ) as mock_transfer:
            clear_airflow_connections()
            upsert_airflow_connection(conn_id=self.aws_conn_id, conn_type="http", login="", password="")
            # Transfer failures should raise an error
            mock_transfer.side_effect = ([False, 0] for _ in range(self.transfer_attempts))
            with self.assertRaises(AirflowException):
                tasks.transfer_orcid(
                    release=dummy_release().to_dict(),
                    aws_orcid_conn_id=self.aws_conn_id,
                    transfer_attempts=self.transfer_attempts,
                    orcid_bucket="orcid_bucket",
                    orcid_summaries_prefix="orcid_summaries_prefix",
                )


class TestCreateMainTableSnapshot(SandboxTestCase):

    def test_create_main_table_snapshot_first_run(self):
        """Test that the main table snapshot is not created for the first run"""
        with SandboxEnvironment().create():
            release = dummy_release()
            release.is_first_run = True
            with self.assertRaises(AirflowSkipException):
                tasks.bq_create_main_table_snapshot(release.to_dict(), snapshot_expiry_days=31)

    def test_create_main_table_snapshot_second_run(self):
        """Test that the main table snapshot is created for subsequent runs"""
        env = SandboxEnvironment(project_id=TestConfig.gcp_project_id, data_location=TestConfig.gcp_data_location)
        bq_dataset_id = env.add_dataset()

        with env.create():
            release = OrcidRelease(
                dag_id="test_orcid",
                run_id="test_orcid_run",
                bq_dataset_id=bq_dataset_id,
                bq_main_table_name="test_orcid",
                bq_upsert_table_name="test_orcid_upsert",
                bq_delete_table_name="test_orcid_delete",
                cloud_workspace=env.cloud_workspace,
                start_date=pendulum.now(),
                end_date=pendulum.now(),
                prev_release_end=pendulum.now(),
                prev_latest_modified_record=pendulum.now(),
                is_first_run=False,
            )

            # Create the main table to be snapshotted
            content = [{"test": "test_1"}]
            bq.bq_load_from_memory(release.bq_main_table_id, records=content)

            # Run the task and check output
            tasks.bq_create_main_table_snapshot(release.to_dict(), snapshot_expiry_days=31)
            self.assertTrue(bq.bq_table_exists(release.bq_snapshot_table_id))
            self.assert_table_content(
                table_id=release.bq_snapshot_table_id, expected_content=content, primary_key="test"
            )


class TestCreateOrcidManifest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.list_blobs_path = "academic_observatory_workflows.orcid_telescope.tasks.gcs_list_blobs"
        self.test_batch_str = "12X"
        self.bucket_name = "test-bucket"

    def test_create_manifest(self):
        """Tests the create_manifest function"""

        env = SandboxEnvironment(project_id=TestConfig.gcp_project_id, data_location=TestConfig.gcp_data_location)
        bq_dataset_id = env.add_dataset()
        with env.create():
            release = OrcidRelease(
                dag_id="test_orcid",
                run_id="test_orcid_run",
                bq_dataset_id=bq_dataset_id,
                bq_main_table_name="test_orcid",
                bq_upsert_table_name="test_orcid_upsert",
                bq_delete_table_name="test_orcid_delete",
                cloud_workspace=env.cloud_workspace,
                start_date=pendulum.datetime(2024, 1, 1),
                end_date=pendulum.datetime(2024, 1, 1),
                prev_release_end=pendulum.now(),
                prev_latest_modified_record=pendulum.now(),
                is_first_run=False,
            )
            summaries_prefix = "summaries"

            # Upload the .xml files to the test bucket
            records = OrcidTestRecords.first_run_records
            file_path = records[0]["path"]
            orcid = records[0]["orcid"]
            batch = records[0]["batch"]
            gcs_upload_file(
                bucket_name=env.download_bucket, blob_name=f"{summaries_prefix}/{batch}/{orcid}", file_path=file_path
            )

            # Set the last modified to now - blob0 should be ignored
            release.prev_latest_modified_record = pendulum.now()

            # These two should not be ignored
            file_path = records[1]["path"]
            orcid = records[1]["orcid"]
            batch = records[1]["batch"]
            blob_1_name = f"{summaries_prefix}/{batch}/{orcid}"
            gcs_upload_file(bucket_name=env.download_bucket, blob_name=blob_1_name, file_path=file_path)
            file_path = records[2]["path"]
            orcid = records[2]["orcid"]
            batch = records[2]["batch"]
            blob_2_name = f"{summaries_prefix}/{batch}/{orcid}"
            gcs_upload_file(bucket_name=env.download_bucket, blob_name=blob_2_name, file_path=file_path)

            # Put a blob in a different folder - blob3 should be ignored
            file_path = records[3]["path"]
            orcid = records[3]["orcid"]
            batch = records[3]["batch"]
            gcs_upload_file(
                bucket_name=env.download_bucket, blob_name=f"somewhere_else/{batch}/{orcid}", file_path=file_path
            )

            # 1100 tasks taskes too long, only submit the ones we need
            with patch(
                "academic_observatory_workflows.orcid_telescope.tasks.OrcidRelease.orcid_batches"
            ) as mock_batches:
                batches = [OrcidBatch(release.download_folder, release.transform_folder, r["batch"]) for r in records]
                mock_batches.return_value = list(set(batches))

                # Run the task
                tasks.create_manifests(
                    release.to_dict(),
                    orcid_bucket=env.download_bucket,
                    orcid_summaries_prefix=summaries_prefix,
                    max_workers=2,
                )

            # Make asserstions
            self.assertTrue(os.path.exists(release.master_manifest_file))
            with open(release.master_manifest_file, "r", newline="") as csvfile:
                reader = csv.reader(csvfile)
                rows = list(reader)
            manifest_records = rows[1:]
            blobs = [row[1] for row in manifest_records]
            self.assertEqual(len(manifest_records), 2)
            self.assertEqual(set(blobs), set([blob_1_name, blob_2_name]))

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
                tasks.create_orcid_batch_manifest(test_batch, reference_date, self.bucket_name)

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
                tasks.create_orcid_batch_manifest(test_batch, pendulum.datetime(2023, 1, 1), self.bucket_name)

            # Assert manifest file is created
            self.assertTrue(os.path.exists(test_batch.manifest_file))
            with open(test_batch.manifest_file, "r") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
            self.assertEqual(len(rows), 0)


class TestDownload(unittest.TestCase):
    def test_download(self):
        """Tests the download function works as intended"""

        env = SandboxEnvironment(project_id=TestConfig.gcp_project_id, data_location=TestConfig.gcp_data_location)
        with env.create():
            release = OrcidRelease(
                dag_id="test_orcid",
                run_id="test_orcid_run",
                bq_dataset_id="test_dataset",
                bq_main_table_name="test_orcid",
                bq_upsert_table_name="test_orcid_upsert",
                bq_delete_table_name="test_orcid_delete",
                cloud_workspace=env.cloud_workspace,
                start_date=pendulum.datetime(2024, 1, 1),
                end_date=pendulum.datetime(2024, 1, 1),
                prev_release_end=pendulum.now(),
                prev_latest_modified_record=pendulum.now(),
                is_first_run=False,
            )

            # Upload the files to the bucket
            records = OrcidTestRecords.first_run_records[:-1]
            for record in records:
                orcid = record["orcid"]
                batch_name = record["batch"]
                blob_name = f"summaries/{batch_name}/{orcid}.xml"
                gcs_upload_file(bucket_name=env.download_bucket, blob_name=blob_name, file_path=record["path"])

                # Create the manifest file for the batch
                batch = OrcidBatch(release.download_folder, release.transform_folder, batch_name)
                with open(batch.manifest_file, "w", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=tasks.MANIFEST_HEADER)
                    writer.writeheader()
                    writer.writerow(
                        {
                            tasks.MANIFEST_HEADER[0]: env.download_bucket,
                            tasks.MANIFEST_HEADER[1]: blob_name,
                            tasks.MANIFEST_HEADER[2]: datetime.datetime.min,
                        }
                    )

            # Run the task
            with patch(
                "academic_observatory_workflows.orcid_telescope.tasks.OrcidRelease.orcid_batches"
            ) as mock_batches:
                mock_batches.return_value = [
                    OrcidBatch(release.download_folder, release.transform_folder, r["batch"]) for r in records
                ]
                tasks.download(release.to_dict())

            # Make assertions
            self.assertEqual(len(release.downloaded_records), 3)


class TestTransform(unittest.TestCase):
    def test_transform(self):
        """Tests that a set of files is transformed as expected"""
        env = SandboxEnvironment(project_id=TestConfig.gcp_project_id, data_location=TestConfig.gcp_data_location)
        with env.create():
            release = dummy_release()

            # Set up the data
            records = OrcidTestRecords.first_run_records[:-1]
            for record in records:
                orcid = record["orcid"]
                batch_name = record["batch"]
                blob_name = f"summaries/{batch_name}/{orcid}.xml"
                batch = OrcidBatch(release.download_folder, release.transform_folder, batch_name)

                # Copy the test file
                shutil.copy(record["path"], batch.download_batch_dir)

                # Create the manifest file
                with open(batch.manifest_file, "w", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=tasks.MANIFEST_HEADER)
                    writer.writeheader()
                    writer.writerow(
                        {
                            tasks.MANIFEST_HEADER[0]: env.download_bucket,
                            tasks.MANIFEST_HEADER[1]: blob_name,
                            tasks.MANIFEST_HEADER[2]: datetime.datetime.min,
                        }
                    )

            # Perform the transform task
            relevant_batches = [
                OrcidBatch(release.download_folder, release.transform_folder, r["batch"]) for r in records
            ]
            with patch(
                "academic_observatory_workflows.orcid_telescope.tasks.OrcidRelease.orcid_batches"
            ) as mock_batches:
                mock_batches.return_value = relevant_batches
                tasks.transform(release.to_dict(), max_workers=2)

            # Make assertions
            self.assertEqual(len(release.upsert_files), 3)
            for b in relevant_batches:
                self.assertEqual(os.path.exists(b.transform_upsert_file), True)
                actual_transformed_data = sorted(load_jsonl(b.transform_upsert_file), key=lambda i: i["path"])
                expected_transformed_data = sorted(
                    load_jsonl(os.path.join(FIXTURES_FOLDER, "first_run", f"{b.batch_str}_upsert.jsonl")),
                    key=lambda i: i["path"],
                )
                self.assertEqual(expected_transformed_data, actual_transformed_data)

    def test_transform_orcid_record_valid_record(self):
        """Tests that a valid ORCID record with 'record' section is transformed correctly"""
        for asset in OrcidTestRecords.first_run_records:
            orcid = asset["orcid"]
            path = asset["path"]
            transformed_record = tasks.transform_orcid_record(path)
            self.assertIsInstance(transformed_record, dict)
            self.assertEqual(transformed_record["orcid_identifier"]["path"], orcid)

    def test_transform_orcid_record_error_record(self):
        """Tests that an ORCID record with 'error' section is transformed correctly"""
        error_record = OrcidTestRecords.second_run_records[1]
        orcid = error_record["orcid"]
        path = error_record["path"]
        transformed_record = tasks.transform_orcid_record(path)
        self.assertIsInstance(transformed_record, str)
        self.assertEqual(transformed_record, orcid)

    def test_transform_orcid_record_invalid_key_record(self):
        """Tests that an ORCID record with no 'error' or 'record' section raises a Key Error"""
        invaid_key_record = OrcidTestRecords.invalid_key_orcid
        path = invaid_key_record["path"]
        with self.assertRaises(KeyError):
            tasks.transform_orcid_record(path)

    def test_transform_orcid_record_mismatched_orcid(self):
        """Tests that a ValueError is raised if the ORCID in the file name does not match the ORCID in the record"""
        mismatched_orcid = OrcidTestRecords.mismatched_orcid
        path = mismatched_orcid["path"]
        with self.assertRaisesRegex(ValueError, "does not match ORCID in record"):
            tasks.transform_orcid_record(path)


class TestAddDatasetRelease(unittest.TestCase):

    snapshot_date = pendulum.datetime(2024, 1, 1)

    def test_add_dataset_release(self):
        env = SandboxEnvironment(project_id=TestConfig.gcp_project_id, data_location=TestConfig.gcp_data_location)
        api_dataset_id = env.add_dataset(prefix="orcid_test_api")
        now = pendulum.now()

        with env.create():
            release = OrcidRelease(
                dag_id="test_orcid",
                run_id="test_orcid_run",
                bq_dataset_id="test_dataset",
                bq_main_table_name="test_orcid",
                bq_upsert_table_name="test_orcid_upsert",
                bq_delete_table_name="test_orcid_delete",
                cloud_workspace=env.cloud_workspace,
                start_date=pendulum.datetime(2024, 1, 1),
                end_date=pendulum.datetime(2024, 1, 1),
                prev_release_end=pendulum.now(),
                prev_latest_modified_record=pendulum.now(),
                is_first_run=False,
            )
            expected_api_release = {
                "dag_id": "test_orcid",
                "entity_id": "orcid",
                "dag_run_id": "test_orcid_run",
                "created": datetime_normalise(now),
                "modified": datetime_normalise(now),
                "data_interval_start": None,
                "data_interval_end": None,
                "snapshot_date": None,
                "partition_date": None,
                "changefile_start_date": "2024-01-01T00:00:00+00:00",
                "changefile_end_date": "2024-01-01T00:00:00+00:00",
                "sequence_start": None,
                "sequence_end": None,
                "extra": {"latest_modified_record_date": datetime_normalise(now)},
            }
            api = DatasetAPI(bq_project_id=release.cloud_workspace.project_id, bq_dataset_id=api_dataset_id)
            api.seed_db()

            # Should not be any releases in the API before the task is run
            self.assertEqual(len(api.get_dataset_releases(dag_id=release.dag_id, entity_id="orcid")), 0)
            with patch("academic_observatory_workflows.orcid_telescope.tasks.pendulum.now") as mock_now, patch(
                "academic_observatory_workflows.orcid_telescope.tasks.latest_modified_record_date"
            ) as mock_last_date:
                mock_now.return_value = now
                mock_last_date.return_value = datetime_normalise(now)
                tasks.add_dataset_release(release.to_dict(), api_bq_dataset_id=api_dataset_id)

            # Should be one release in the API
            api_releases = api.get_dataset_releases(dag_id=release.dag_id, entity_id="orcid")
        self.assertEqual(len(api_releases), 1)
        self.assertEqual(expected_api_release, api_releases[0].to_dict())


class TestLatestModifiedRecordDate(unittest.TestCase):
    def test_latest_modified_record_date(self):
        """Tests that the latest_modified_record_date function returns the correct date"""
        # Create a temporary manifest file for the test
        with SandboxEnvironment(
            project_id=TestConfig.gcp_project_id, data_location=TestConfig.gcp_data_location
        ).create():
            release = dummy_release()
            with open(release.master_manifest_file, "w") as f:
                f.write(",".join(tasks.MANIFEST_HEADER))
                f.write("\n")
                f.write("gs://test-bucket,folder/0000-0000-0000-0001.xml,2023-06-03T00:00:00Z\n")
                f.write("gs://test-bucket,folder/0000-0000-0000-0002.xml,2023-06-03T00:00:00Z\n")
                f.write("gs://test-bucket,folder/0000-0000-0000-0003.xml,2023-06-02T00:00:00Z\n")
                f.write("gs://test-bucket,folder/0000-0000-0000-0004.xml,2023-06-01T00:00:00Z\n")

            # Call the function and assert the result
            expected_date = datetime_normalise("2023-06-03T00:00:00Z")
            actual_date = tasks.latest_modified_record_date(release.to_dict())
            self.assertEqual(actual_date, expected_date)


class TestOrcidBatchNames(unittest.TestCase):
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


class TestCleanupWorkflow(SandboxTestCase):
    def test_cleanup_workflow(self):
        """Test the cleanup_workflow function"""

        env = SandboxEnvironment(project_id=TestConfig.gcp_project_id, data_location=TestConfig.gcp_data_location)
        with env.create():
            release = dummy_release()

            # Create the folders
            workflow_folder = release.workflow_folder
            release.download_folder
            release.transform_folder
            release.extract_folder

            # Run cleanup and make sure the folders are gone
            tasks.cleanup_workflow(release.to_dict())
            self.assert_cleanup(workflow_folder)
