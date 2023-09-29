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

from functools import cached_property
import datetime
import time
import logging
import os
from os import PathLike
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from datetime import timedelta
from typing import List, Dict, Tuple, Union
import itertools
import csv
import re

import pendulum
import xmltodict
from airflow.hooks.base import BaseHook
from airflow.operators.dummy import DummyOperator
from google.auth import default
from google.cloud import bigquery
from google.cloud.bigquery import SourceFormat


from academic_observatory_workflows.config import schema_folder as default_schema_folder, Tag
from academic_observatory_workflows.s5cmd import S5Cmd
from observatory.api.client.model.dataset_release import DatasetRelease
from observatory.platform.airflow import PreviousDagRunSensor, is_first_dag_run
from observatory.platform.api import get_dataset_releases, get_latest_dataset_release
from observatory.platform.api import make_observatory_api
from observatory.platform.bigquery import (
    bq_table_id,
    bq_load_table,
    bq_upsert_records,
    bq_delete_records,
    bq_sharded_table_id,
    bq_create_dataset,
    bq_snapshot,
)
from observatory.platform.config import AirflowConns
from observatory.platform.files import list_files, save_jsonl_gz, change_keys
from observatory.platform.gcs import (
    gcs_upload_files,
    gcs_blob_uri,
    gcs_blob_name_from_path,
    gcs_create_aws_transfer,
    gcs_list_blobs,
    gcs_hmac_key,
)
from observatory.platform.observatory_config import CloudWorkspace
from observatory.platform.workflows.workflow import (
    Workflow,
    ChangefileRelease,
    cleanup,
    set_task_state,
)

ORCID_AWS_SUMMARIES_BUCKET = "v2.0-summaries"
ORCID_REGEX = r"\d{4}-\d{4}-\d{4}-\d{3}(\d|X)\b"
ORCID_RECORD_REGEX = r"\d{4}-\d{4}-\d{4}-\d{3}(\d|X)\.xml$"
MANIFEST_HEADER = ["bucket_name", "blob_name", "updated"]
BATCH_REGEX = r"^\d{2}(\d|X)$"


class OrcidBatch:
    """Describes a single ORCID batch and its related files/folders"""

    def __init__(self, download_dir: str, transform_dir: str, batch_str: str):
        self.download_dir = download_dir
        self.transform_dir = transform_dir
        self.batch_str = batch_str
        self.download_batch_dir = os.path.join(self.download_dir, batch_str)
        self.download_log_file = os.path.join(self.download_dir, f"{self.batch_str}_log.txt")
        self.download_error_file = os.path.join(self.download_dir, f"{self.batch_str}_error.txt")
        self.manifest_file = os.path.join(self.download_dir, f"{self.batch_str}_manifest.csv")
        self.transform_upsert_file = os.path.join(self.transform_dir, f"{self.batch_str}_upsert.jsonl.gz")
        self.transform_delete_file = os.path.join(self.transform_dir, f"{self.batch_str}_delete.jsonl.gz")

        if not os.path.exists(self.download_dir):
            raise NotADirectoryError(f"Directory {self.download_dir} does not exist.")
        if not os.path.exists(self.transform_dir):
            raise NotADirectoryError(f"Directory {self.transform_dir} does not exist.")
        if not re.match(BATCH_REGEX, self.batch_str):
            raise ValueError(f"Batch string {self.batch_str} is not valid.")

        os.makedirs(self.download_batch_dir, exist_ok=True)

    @property
    def existing_records(self) -> List[str]:
        """List of existing ORCID records on disk for this ORCID directory."""
        return [os.path.basename(path) for path in list_files(self.download_batch_dir, ORCID_RECORD_REGEX)]

    @property
    def missing_records(self) -> List[str]:
        """List of missing ORCID records on disk for this ORCID directory."""
        return list(set(self.expected_records) - set(self.existing_records))

    @cached_property
    def expected_records(self) -> List[str]:
        """List of expected ORCID records for this ORCID directory. Derived from the manifest file"""
        with open(self.manifest_file, "r") as f:
            reader = csv.DictReader(f)
            return [os.path.basename(row["blob_name"]) for row in reader]

    @cached_property
    def blob_uris(self) -> List[str]:
        """List of blob URIs from the manifest this ORCID directory."""
        with open(self.manifest_file, "r") as f:
            reader = csv.DictReader(f)
            return [gcs_blob_uri(bucket_name=row["bucket_name"], blob_name=row["blob_name"]) for row in reader]


class OrcidRelease(ChangefileRelease):
    def __init__(
        self,
        *,
        dag_id: str,
        run_id: str,
        cloud_workspace: CloudWorkspace,
        bq_dataset_id: str,
        bq_main_table_name: str,
        bq_upsert_table_name: str,
        bq_delete_table_name: str,
        start_date: pendulum.DateTime,
        end_date: pendulum.DateTime,
        prev_release_end: pendulum.DateTime,
        prev_latest_modified_record: pendulum.DateTime,
        is_first_run: bool,
    ):
        """Construct a CrossrefEventsRelease instance

        :param dag_id: DAG ID.
        :param run_id: DAG run ID.
        :param cloud_workspace: Cloud workspace object for this release.
        :param bq_dataset_id: BigQuery dataset ID.
        :param bq_main_table_name: BigQuery main table name for the ORCID table.
        :param bq_upsert_table_name: BigQuery table name for the ORCID upsert table.
        :param bq_delete_table_name: BigQuery table name for the ORCID delete table.
        :param start_date: Start date for the release.
        :param end_date: End date for the release.
        :param prev_end_date: End date for the previous release. Used for making the snapshot table date.
        :param prev_latest_modified_record: Latest modified record for the previous release. Used to decide which records to update.
        :param is_first_run: Whether this is the first run of the DAG.
        """
        super().__init__(
            dag_id=dag_id,
            run_id=run_id,
            start_date=start_date,
            end_date=end_date,
        )
        self.cloud_workspace = cloud_workspace
        self.bq_dataset_id = bq_dataset_id
        self.bq_main_table_name = bq_main_table_name
        self.bq_upsert_table_name = bq_upsert_table_name
        self.bq_delete_table_name = bq_delete_table_name
        self.prev_release_end = prev_release_end
        self.prev_latest_modified_record = prev_latest_modified_record
        self.is_first_run = is_first_run

        # Files/folders
        self.master_manifest_file = os.path.join(self.workflow_folder, "manifest.csv")

        # Table names and URIs
        self.upsert_blob_glob = f"{gcs_blob_name_from_path(self.transform_folder)}/*_upsert.jsonl.gz"
        self.upsert_table_uri = gcs_blob_uri(self.cloud_workspace.transform_bucket, self.upsert_blob_glob)
        self.delete_blob_glob = f"{gcs_blob_name_from_path(self.transform_folder)}/*_delete.jsonl.gz"
        self.delete_table_uri = gcs_blob_uri(self.cloud_workspace.transform_bucket, self.delete_blob_glob)
        self.bq_main_table_id = bq_table_id(cloud_workspace.project_id, bq_dataset_id, bq_main_table_name)
        self.bq_upsert_table_id = bq_table_id(cloud_workspace.project_id, bq_dataset_id, bq_upsert_table_name)
        self.bq_delete_table_id = bq_table_id(cloud_workspace.project_id, bq_dataset_id, bq_delete_table_name)
        self.bq_snapshot_table_id = bq_sharded_table_id(
            cloud_workspace.project_id, bq_dataset_id, f"{bq_main_table_name}_snapshot", prev_release_end
        )

    @property
    def upsert_files(self):  # The 'upsert' files in the transform_folder
        return list_files(self.transform_folder, r"^.*\d{2}(\d|X)_upsert\.jsonl\.gz$")

    @property
    def delete_files(self):  # The 'delete' files in the transform_folder
        return list_files(self.transform_folder, r"^.*\d{2}(\d|X)_delete\.jsonl\.gz$")

    @property
    def downloaded_records(self):  # Every downloaded record
        return list_files(self.download_folder, ORCID_RECORD_REGEX)

    @property
    def orcid_directory_paths(self) -> List[str]:
        """Generates the paths to the orcid directories in the download folder"""
        return [os.path.join(self.download_folder, folder) for folder in orcid_batch_names()]

    def orcid_batches(self) -> List[OrcidBatch]:
        """Creates the orcid directories in the download folder if they don't exist and returns them"""
        return [OrcidBatch(self.download_folder, self.transform_folder, batch) for batch in orcid_batch_names()]


class OrcidTelescope(Workflow):
    """ORCID telescope"""

    def __init__(
        self,
        dag_id: str,
        cloud_workspace: CloudWorkspace,
        orcid_bucket: str = "ao-orcid",
        orcid_summaries_prefix: str = "orcid_summaries",
        bq_dataset_id: str = "orcid",
        bq_main_table_name: str = "orcid",
        bq_upsert_table_name: str = "orcid_upsert",
        bq_delete_table_name: str = "orcid_delete",
        dataset_description: str = "The ORCID dataset and supporting tables",
        table_description: str = "The ORCID dataset",
        snapshot_expiry_days: int = 31,
        schema_file_path: str = os.path.join(default_schema_folder(), "orcid", "orcid.json"),
        delete_schema_file_path: str = os.path.join(default_schema_folder(), "orcid", "orcid_delete.json"),
        transfer_attempts: int = 5,
        batch_size: int = 25000,
        max_workers: int = os.cpu_count() * 2,
        api_dataset_id: str = "orcid",
        observatory_api_conn_id: str = AirflowConns.OBSERVATORY_API,
        aws_orcid_conn_id: str = "aws_orcid",
        start_date: pendulum.DateTime = pendulum.datetime(2023, 6, 1),
        schedule: str = "0 0 * * 0",  # Midnight UTC every Sunday
        queue: str = "remote",
    ):
        """Construct an ORCID telescope instance"""

        super().__init__(
            dag_id=dag_id,
            start_date=start_date,
            schedule=schedule,
            catchup=False,
            airflow_conns=[observatory_api_conn_id, aws_orcid_conn_id],
            tags=[Tag.academic_observatory],
            queue=queue,
        )

        self.cloud_workspace = cloud_workspace
        self.orcid_bucket = orcid_bucket
        self.orcid_summaries_prefix = orcid_summaries_prefix
        self.bq_dataset_id = bq_dataset_id
        self.bq_main_table_name = bq_main_table_name
        self.bq_upsert_table_name = bq_upsert_table_name
        self.bq_delete_table_name = bq_delete_table_name
        self.api_dataset_id = api_dataset_id
        self.schema_file_path = schema_file_path
        self.delete_schema_file_path = delete_schema_file_path
        self.transfer_attempts = transfer_attempts
        self.batch_size = batch_size
        self.dataset_description = dataset_description
        self.table_description = table_description
        self.snapshot_expiry_days = snapshot_expiry_days
        self.max_workers = max_workers
        self.observatory_api_conn_id = observatory_api_conn_id
        self.aws_orcid_conn_id = aws_orcid_conn_id

        external_task_id = "dag_run_complete"
        self.add_operator(
            PreviousDagRunSensor(
                dag_id=self.dag_id,
                external_task_id=external_task_id,
                execution_delta=timedelta(days=7),  # To match the @weekly schedule
            )
        )
        self.add_setup_task(self.check_dependencies)
        self.add_task(self.create_datasets)

        # Download the data
        self.add_task(self.transfer_orcid)

        # Create snapshots of main table in case we mess up
        self.add_task(self.bq_create_main_table_snapshot)

        # Scour the data for updates
        self.add_task(self.create_manifests)

        # Download and transform updated files
        self.add_task(self.download)
        self.add_task(self.transform)

        # Load the data to table
        self.add_task(self.upload_transformed)
        self.add_task(self.bq_load_main_table)
        self.add_task(self.bq_load_upsert_table)
        self.add_task(self.bq_load_delete_table)
        self.add_task(self.bq_upsert_records)
        self.add_task(self.bq_delete_records)

        # Finish
        self.add_task(self.add_new_dataset_release)
        self.add_task(self.cleanup)

        # The last task that the next DAG run's ExternalTaskSensor waits for.
        self.add_operator(DummyOperator(task_id=external_task_id))

    @property
    def aws_orcid_key(self) -> Tuple[str, str]:
        """Return API login and password"""
        connection = BaseHook.get_connection(self.aws_orcid_conn_id)
        return connection.login, connection.password

    def make_release(self, **kwargs) -> OrcidRelease:
        """Generates the OrcidRelease object."""
        dag_run = kwargs["dag_run"]
        is_first_run = is_first_dag_run(dag_run)
        releases = get_dataset_releases(dag_id=self.dag_id, dataset_id=self.api_dataset_id)

        # Determine the modication cutoff for the new release
        if is_first_run:
            if not len(releases) == 0 and kwargs["task"].task_id != self.cleanup.__name__:
                raise ValueError(
                    "fetch_releases: there should be no DatasetReleases stored in the Observatory API on the first DAG run."
                )
            prev_latest_modified_record = pendulum.instance(datetime.datetime.min)
            prev_release_end = pendulum.instance(datetime.datetime.min)
        else:
            if not len(releases) >= 1:
                raise ValueError(
                    "fetch_releases: there should be at least 1 DatasetRelease in the Observatory API after the first DAG run"
                )
            prev_release = get_latest_dataset_release(releases, "changefile_end_date")
            prev_release_end = prev_release.changefile_end_date
            prev_latest_modified_record = pendulum.parse(prev_release.extra["latest_modified_record_date"])

        return OrcidRelease(
            dag_id=self.dag_id,
            run_id=kwargs["run_id"],
            cloud_workspace=self.cloud_workspace,
            bq_dataset_id=self.bq_dataset_id,
            bq_main_table_name=self.bq_main_table_name,
            bq_upsert_table_name=self.bq_upsert_table_name,
            bq_delete_table_name=self.bq_delete_table_name,
            prev_release_end=prev_release_end,
            start_date=kwargs["data_interval_start"],
            end_date=kwargs["data_interval_end"],
            prev_latest_modified_record=prev_latest_modified_record,
            is_first_run=is_first_run,
        )

    def create_datasets(self, release: OrcidRelease, **kwargs) -> None:
        """Create datasets"""

        bq_create_dataset(
            project_id=self.cloud_workspace.project_id,
            dataset_id=self.bq_dataset_id,
            location=self.cloud_workspace.data_location,
            description=self.dataset_description,
        )

    def transfer_orcid(self, release: OrcidRelease, **kwargs):
        """Sync files from AWS bucket to Google Cloud bucket."""
        success = False
        for i in range(self.transfer_attempts):
            logging.info(f"Beginning AWS to GCP transfer attempt no. {i+1}")
            success, objects_count = gcs_create_aws_transfer(
                aws_key=self.aws_orcid_key,
                aws_bucket=ORCID_AWS_SUMMARIES_BUCKET,
                include_prefixes=[],
                gc_project_id=self.cloud_workspace.project_id,
                gc_bucket_dst_uri=gcs_blob_uri(self.orcid_bucket, f"{self.orcid_summaries_prefix}/"),
                description="Transfer ORCID data from AWS to GCP",
            )
            logging.info(f"Attempt {i+1}: Total number of objects transferred: {objects_count}")
            logging.info(f"Attempt {i+1}: Success? {success}")
            if success:
                break
        set_task_state(success, self.transfer_orcid.__name__, release)

    def bq_create_main_table_snapshot(self, release: OrcidRelease, **kwargs):
        """Create a snapshot of each main table. The purpose of this table is to be able to rollback the table
        if something goes wrong. The snapshot expires after self.snapshot_expiry_days."""

        if release.is_first_run:
            logging.info(f"bq_create_main_table_snapshots: skipping as snapshots are not created on the first run")
            return

        expiry_date = pendulum.now().add(days=self.snapshot_expiry_days)
        success = bq_snapshot(
            src_table_id=release.bq_main_table_id, dst_table_id=release.bq_snapshot_table_id, expiry_date=expiry_date
        )
        set_task_state(success, self.bq_create_main_table_snapshot.__name__, release)

    def create_manifests(self, release: OrcidRelease, **kwargs):
        """Create a manifest of all the modified files in the orcid bucket."""
        logging.info("Creating manifest")
        orcid_batches = release.orcid_batches()

        with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            futures = []
            for orcid_batch in orcid_batches:
                futures.append(
                    executor.submit(
                        create_orcid_batch_manifest,
                        orcid_batch=orcid_batch,
                        reference_date=release.prev_latest_modified_record,
                        bucket=self.orcid_bucket,
                        bucket_prefix=self.orcid_summaries_prefix,
                    )
                )
            for future in as_completed(futures):
                future.result()

        # Open and write each batch manifest to the master manifest file
        logging.info("Joining manifest files")
        with open(release.master_manifest_file, "w") as f:
            writer = csv.DictWriter(f, fieldnames=MANIFEST_HEADER)
            writer.writeheader()
            for orcid_batch in orcid_batches:
                with open(orcid_batch.manifest_file, "r") as df:
                    reader = csv.DictReader(df)
                    writer.writerows(reader)

    def download(self, release: OrcidRelease, **kwargs):
        """Reads each batch's manifest and downloads the files from the gcs bucket."""
        gcs_creds, project_id = default()
        with gcs_hmac_key(project_id, gcs_creds.service_account_email) as (key, secret):
            total_files = 0
            start_time = time.time()
            for orcid_batch in release.orcid_batches():
                if not orcid_batch.missing_records:
                    logging.info(f"All files present for {orcid_batch.batch_str}. Skipping download.")
                    continue

                logging.info(f"Downloading files for ORCID directory: {orcid_batch.batch_str}")
                with open(orcid_batch.download_log_file, "w") as f:
                    s5cmd = S5Cmd(access_credentials=(key.access_id, secret), out_stream=f)
                    returncode = s5cmd.download_from_bucket(
                        uris=orcid_batch.blob_uris, local_path=orcid_batch.download_batch_dir
                    )[-1]

                # Check for errors
                if returncode != 0:
                    raise RuntimeError(
                        f"Download attempt '{orcid_batch.batch_str}': returned non-zero exit code: {returncode}. See log file: {orcid_batch.download_log_file}"
                    )
                if orcid_batch.missing_records:
                    raise FileNotFoundError(f"All files were not downloaded for {orcid_batch.batch_str}. Aborting.")

                # Keep track of files downloaded
                total_files += len(orcid_batch.expected_records)
                logging.info(
                    f"Downloaded {len(orcid_batch.expected_records)} records for batch '{orcid_batch.batch_str}'"
                )
            total_time = time.time() - start_time

        # Check all files exist
        if total_files != len(release.downloaded_records):
            raise FileNotFoundError(
                f"Downloaded {total_files} files but found {len(release.downloaded_records)} records on disk."
            )
        logging.info(f"Completed download for {total_files} files in {str(datetime.timedelta(seconds=total_time))}")

    def transform(self, release: OrcidRelease, **kwargs):
        """Transforms the downloaded files into serveral bigquery-compatible .jsonl files"""
        total_upsert_records = 0
        total_delete_records = 0
        start_time = time.time()
        for orcid_batch in release.orcid_batches():
            logging.info(f"Transforming ORCID batch {orcid_batch.batch_str}")
            transformed_data = []
            with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
                futures = []
                for record in orcid_batch.existing_records:
                    future = executor.submit(
                        transform_orcid_record, os.path.join(orcid_batch.download_batch_dir, record)
                    )
                    futures.append(future)
                for future in futures:
                    transformed_data.append(future.result())

            # Save records to upsert
            batch_upserts = [record for record in transformed_data if isinstance(record, dict)]
            n_batch_upserts = len(batch_upserts)
            if n_batch_upserts > 0:
                save_jsonl_gz(orcid_batch.transform_upsert_file, batch_upserts)

            # Save records to delete
            batch_deletes = [{"id": record} for record in transformed_data if isinstance(record, str)]
            n_batch_deletes = len(batch_deletes)
            if n_batch_deletes > 0:
                save_jsonl_gz(orcid_batch.transform_delete_file, batch_deletes)

            # Check that the number of records processed matches the expected number of records for this batch
            batch_records = n_batch_upserts + n_batch_deletes
            if not batch_records == len(orcid_batch.expected_records):
                raise ValueError(
                    f"Expected {len(orcid_batch.expected_records)} records but got {batch_records} records ({n_batch_upserts} upserts | {n_batch_deletes} deletes)"
                )

            # Record keeping
            total_upsert_records += n_batch_upserts
            total_delete_records += n_batch_deletes
            logging.info(
                f"Transformed {n_batch_upserts} upserts and {n_batch_deletes} deletes for batch {orcid_batch.batch_str}"
            )
        total_time = time.time() - start_time
        logging.info(
            f"Transformed {total_upsert_records} upserts and {total_delete_records} deletes in {str(datetime.timedelta(seconds=total_time))}"
        )

        # Sanity checks
        if total_upsert_records + total_delete_records != len(release.downloaded_records):
            raise ValueError(
                f"Expected {len(release.downloaded_records)} total records processed but got {total_upsert_records + total_delete_records}"
            )

    def upload_transformed(self, release: OrcidRelease, **kwargs):
        """Uploads the upsert and delete files to the transform bucket."""
        file_paths = release.upsert_files + release.delete_files
        success = gcs_upload_files(bucket_name=self.cloud_workspace.transform_bucket, file_paths=file_paths)
        set_task_state(success, self.upload_transformed.__name__, release)

    def bq_load_main_table(self, release: OrcidRelease, **kwargs):
        """Load the main table."""
        if not release.is_first_run:
            logging.info(f"bq_load_main_table: skipping as the main table is only created on the first run")
            return

        # Check that the number of files matches the number of blobs
        blobs = gcs_list_blobs(self.cloud_workspace.transform_bucket, match_glob=release.upsert_blob_glob)
        if not len(blobs) == len(release.upsert_files):
            raise ValueError(
                f"Number of blobs ({len(blobs)}) does not match number of files ({len(release.upsert_files)})"
            )

        success = bq_load_table(
            uri=[gcs_blob_uri(blob.bucket.name, blob.name) for blob in blobs],
            table_id=release.bq_main_table_id,
            schema_file_path=self.schema_file_path,
            source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_EMPTY,
            ignore_unknown_values=False,
        )
        set_task_state(success, self.bq_load_main_table.__name__, release)

    def bq_load_upsert_table(self, release: OrcidRelease, **kwargs):
        """Load the upsert table into bigquery"""
        if release.is_first_run:
            logging.info(f"bq_load_upsert_table: skipping as no records are upserted on the first run")
            return

        # Check that the number of files matches the number of blobs
        blobs = gcs_list_blobs(self.cloud_workspace.transform_bucket, match_glob=release.upsert_blob_glob)
        if not len(blobs) == len(release.upsert_files):
            raise ValueError(
                f"Number of blobs ({len(blobs)}) does not match number of files ({len(release.upsert_files)})"
            )

        success = bq_load_table(
            uri=[gcs_blob_uri(blob.bucket.name, blob.name) for blob in blobs],
            table_id=release.bq_upsert_table_id,
            schema_file_path=self.schema_file_path,
            source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            ignore_unknown_values=True,
        )
        set_task_state(success, self.bq_load_upsert_table.__name__, release)

    def bq_load_delete_table(self, release: OrcidRelease, **kwargs):
        """Load the delete table into bigquery"""
        if release.is_first_run:
            logging.info(f"bq_load_delete_table: skipping as no records are deleted on the first run")
            return
        if not len(release.delete_files):
            logging.info(f"bq_load_delete_table: skipping as no records require deleting")
            return

        # Check that the number of files matches the number of blobs
        blobs = gcs_list_blobs(self.cloud_workspace.transform_bucket, match_glob=release.delete_blob_glob)
        if not len(blobs) == len(release.delete_files):
            raise ValueError(
                f"Number of blobs ({len(blobs)}) does not match number of files ({len(release.delete_files)})"
            )

        success = bq_load_table(
            uri=[gcs_blob_uri(blob.bucket.name, blob.name) for blob in blobs],
            table_id=release.bq_delete_table_id,
            schema_file_path=self.delete_schema_file_path,
            source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            ignore_unknown_values=True,
        )
        set_task_state(success, self.bq_load_delete_table.__name__, release)

    def bq_upsert_records(self, release: OrcidRelease, **kwargs):
        """Upsert the records from the upserts table into the main table."""
        if release.is_first_run:
            logging.info("bq_upsert_records: skipping as no records are upserted on the first run")
            return

        bq_upsert_records(
            main_table_id=release.bq_main_table_id,
            upsert_table_id=release.bq_upsert_table_id,
            primary_key="orcid_identifier",
        )

    def bq_delete_records(self, release: OrcidRelease, **kwargs):
        """Delete the records in the delete table from the main table."""
        if release.is_first_run:
            logging.info("bq_delete_records: skipping as no records are deleted on the first run")
            return
        if not len(release.delete_files):
            logging.info(f"bq_load_delete_table: skipping as no records require deleting")
            return

        bq_delete_records(
            main_table_id=release.bq_main_table_id,
            delete_table_id=release.bq_delete_table_id,
            main_table_primary_key="orcid_identifier.path",
            delete_table_primary_key="id",
        )

    def add_new_dataset_release(self, release: OrcidRelease, **kwargs) -> None:
        """Adds release information to API."""
        dataset_release = DatasetRelease(
            dag_id=self.dag_id,
            dataset_id=self.api_dataset_id,
            dag_run_id=release.run_id,
            changefile_start_date=release.start_date,
            changefile_end_date=release.end_date,
            extra={"latest_modified_record_date": latest_modified_record_date(release.master_manifest_file)},
        )
        api = make_observatory_api(observatory_api_conn_id=self.observatory_api_conn_id)
        api.post_dataset_release(dataset_release)

    def cleanup(self, release: OrcidRelease, **kwargs) -> None:
        """Delete all files, folders and XComs associated with this release."""
        cleanup(dag_id=self.dag_id, execution_date=kwargs["logical_date"], workflow_folder=release.workflow_folder)


def create_orcid_batch_manifest(
    orcid_batch: OrcidBatch, reference_date: pendulum.DateTime, bucket: str, bucket_prefix: str = None
) -> None:
    """Create a manifest (.csv) for each orcid batch containing blob names and modification dates of all the
    modified files in the bucket's orcid directory. Only blobs modified after the reference date are included.

    :param orcid_batch: The OrcidBatch instance for this orcid directory
    :param reference_date: The date to use as a reference for the manifest
    :param bucket: The name of the bucket
    :param bucket_prefix: The prefix to use when listing blobs in the bucket. i.e. where the orcid directories are located
    :return: The path to the date manifest, the path to the uri manifest
    """
    prefix = f"{bucket_prefix}/{orcid_batch.batch_str}/" if bucket_prefix else f"{orcid_batch.batch_str}/"

    logging.info(f"Creating manifests for {orcid_batch.batch_str}")
    blobs = gcs_list_blobs(bucket, prefix=prefix)
    manifest = []
    for blob in blobs:
        if pendulum.instance(blob.updated) >= reference_date:
            manifest.append(
                {
                    MANIFEST_HEADER[0]: blob.bucket.name,
                    MANIFEST_HEADER[1]: blob.name,
                    MANIFEST_HEADER[2]: blob.updated,
                }
            )

    with open(orcid_batch.manifest_file, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=MANIFEST_HEADER)
        writer.writeheader()
        writer.writerows(manifest)

    logging.info(f"Manifest saved to {orcid_batch.manifest_file}")


def latest_modified_record_date(manifest_file_path: Union[str, PathLike]) -> pendulum.DateTime:
    """Reads the manifest file and finds the most recent date of modification for the records

    :param manifest_file_path: the path to the manifest file
    :return: the most recent date of modification for the records
    """
    with open(manifest_file_path, "r") as f:
        reader = csv.DictReader(f)
        modified_dates = sorted([pendulum.parse(row["updated"]) for row in reader])
    return modified_dates[-1]


def orcid_batch_names() -> List[str]:
    """Create a list of all the possible ORCID directories

    :return: A list of all the possible ORCID directories
    """
    n_1_2 = [str(i) for i in range(10)]  # 0, 1, 2, 3, 4, 5, 6, 7, 8, 9
    n_3 = n_1_2.copy() + ["X"]  # 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, X
    combinations = list(itertools.product(n_1_2, n_1_2, n_3))  # Creates the 000 to 99X directory structure
    return ["".join(i) for i in combinations]


def transform_orcid_record(record_path: str) -> Union[Dict, str]:
    """Transform a single ORCID file/record.
    Streams the content from the blob URI and turns this into a dictionary.
    The xml file is turned into a dictionary, a record should have either a valid 'record' section or an 'error'section.
    The keys of the dictionary are slightly changed so they are valid BigQuery fields.
    If the record is valid, it is returned. If it is an error, the ORCID ID is returned.

    :param download_path: The path to the file with the ORCID record.
    :param transform_folder: The path where transformed files will be saved.
    :return: The transformed ORCID record. If the record is an error, the ORCID ID is returned.
    :raises KeyError: If the record is not valid - no 'error' or 'record' section found
    :raises ValueError: If the ORCID ID does not match the file name's ID
    """
    # Get the orcid from the record path
    expected_orcid = re.search(ORCID_REGEX, record_path).group(0)

    with open(record_path, "rb") as f:
        orcid_dict = xmltodict.parse(f)

    try:
        orcid_record = orcid_dict["record:record"]
    # Some records do not have a 'record', but only 'error'. We return the path to the file in this case.
    except KeyError:
        orcid_record = orcid_dict["error:error"]
        return expected_orcid

    # Check that the ORCID in the file name matches the ORCID in the record
    if not orcid_record["common:orcid-identifier"]["common:path"] == expected_orcid:
        raise ValueError(
            f"Expected ORCID {expected_orcid} does not match ORCID in record {orcid_record['common:orcid-identifier']['common:path']}"
        )

    # Transform the keys of the dictionary so they are valid BigQuery fields
    orcid_record = {k: v for k, v in orcid_record.items() if not k.startswith("@xmlns")}
    convert_key = lambda k: k.split(":")[-1].lstrip("@#").replace("-", "_")
    orcid_record = change_keys(orcid_record, convert_key)

    return orcid_record
