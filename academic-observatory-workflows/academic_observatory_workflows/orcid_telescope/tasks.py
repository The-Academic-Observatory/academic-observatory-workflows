# Copyright 2023-2024 Curtin University
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
import logging
import os
import re
import time
from concurrent.futures import as_completed, ProcessPoolExecutor, ThreadPoolExecutor
import datetime
from os import PathLike
from typing import Dict, Optional, Tuple, Union

import pendulum
import xmltodict
from airflow import AirflowException
from airflow.exceptions import AirflowSkipException
from airflow.hooks.base import BaseHook
from airflow.models import DagRun
from google.auth import default
from google.cloud import bigquery
from google.cloud.bigquery import SourceFormat

from academic_observatory_workflows.s5cmd import S5Cmd
from academic_observatory_workflows.orcid_telescope.release import OrcidRelease
from academic_observatory_workflows.orcid_telescope.batch import OrcidBatch
from observatory_platform.airflow.airflow import is_first_dag_run
from observatory_platform.airflow.workflow import CloudWorkspace, cleanup
from observatory_platform.dataset_api import DatasetRelease, DatasetAPI
from observatory_platform.files import change_keys, save_jsonl_gz
from observatory_platform.google import bigquery as bq
from observatory_platform.google.gcs import (
    gcs_blob_uri,
    gcs_create_aws_transfer,
    gcs_hmac_key,
    gcs_list_blobs,
    gcs_upload_files,
)

MANIFEST_HEADER = ["bucket_name", "blob_name", "updated"]
ORCID_AWS_SUMMARIES_BUCKET = "v2.0-summaries"
ORCID_REGEX = r"\d{4}-\d{4}-\d{4}-\d{3}(\d|X)\b"


def fetch_release(
    dag_id: str,
    run_id: str,
    dag_run: DagRun,
    data_interval_start: pendulum.DateTime,
    data_interval_end: pendulum.DateTime,
    cloud_workspace: CloudWorkspace,
    api_bq_dataset_id: str,
    bq_dataset_id: str,
    bq_main_table_name: str,
    bq_upsert_table_name: str,
    bq_delete_table_name: str,
) -> dict:
    """Generates the OrcidRelease object.

    :param dag_id: The ID of the dag
    :param run_id: The ID of this dag run
    :param dag_run: This dag run's DagRun object
    :param data_interval_start: The start of the data interval
    :param data_interval_end: The end of the data interval
    :param cloud_workspace: The CloudWorkspace object
    :param api_bq_dataset_id: The bigquery dataset ID for the API
    :param bq_dataset_id: The bigquery dataset ID for the telescope
    :param bq_main_table_name: The name of the main bigquery table
    :param bq_upsert_table_name: The name of the upsert table
    :param bq_delete_table_name: The name of the delete table
    """

    api = DatasetAPI(bq_project_id=cloud_workspace.project_id, bq_dataset_id=api_bq_dataset_id)
    api.seed_db()
    releases = api.get_dataset_releases(dag_id=dag_id, entity_id="orcid", date_key="changefile_end_date")
    is_first_run = is_first_dag_run(dag_run)

    # Determine the modication cutoff for the new release
    if is_first_run:
        if not len(releases) == 0:
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
        prev_release = api.get_latest_dataset_release(dag_id=dag_id, entity_id="orcid", date_key="changefile_end_date")
        prev_release_end = prev_release.changefile_end_date
        prev_latest_modified_record = pendulum.parse(prev_release.extra["latest_modified_record_date"])

    return OrcidRelease(
        dag_id=dag_id,
        run_id=run_id,
        cloud_workspace=cloud_workspace,
        bq_dataset_id=bq_dataset_id,
        bq_main_table_name=bq_main_table_name,
        bq_upsert_table_name=bq_upsert_table_name,
        bq_delete_table_name=bq_delete_table_name,
        prev_release_end=prev_release_end,
        start_date=data_interval_start,
        end_date=data_interval_end,
        prev_latest_modified_record=prev_latest_modified_record,
        is_first_run=is_first_run,
    ).to_dict()


def create_dataset(release: dict, dataset_description: str) -> None:
    """Create datasets

    :param release: The Orcid Release object
    :param dataset_description: The description to give the Orcid dataset
    """

    release = OrcidRelease.from_dict(release)
    bq.bq_create_dataset(
        project_id=release.cloud_workspace.project_id,
        dataset_id=release.bq_dataset_id,
        location=release.cloud_workspace.data_location,
        description=dataset_description,
    )


def transfer_orcid(
    release: dict, aws_orcid_conn_id: str, transfer_attempts: int, orcid_bucket: str, orcid_summaries_prefix: str
):
    """Sync files from AWS bucket to Google Cloud bucket.

    :param aws_orcid_conn_id: The airflow connection ID for the AWS ORCID bucket
    :param transfer_attempts: The number of times to attempt the transfer job before giving up
    :param orcid_bucket: The name of the gcs bucket to store the Orcid data
    :param orcid_summaries_prefix: The prefix in which to store the summaries in the bucket
    """

    release = OrcidRelease.from_dict(release)
    success = False
    aws_key = aws_orcid_key(aws_orcid_conn_id)
    for i in range(transfer_attempts):
        logging.info(f"Beginning AWS to GCP transfer attempt no. {i+1}")
        success, objects_count = gcs_create_aws_transfer(
            aws_key=aws_key,
            aws_bucket=ORCID_AWS_SUMMARIES_BUCKET,
            include_prefixes=[],
            gc_project_id=release.cloud_workspace.project_id,
            gc_bucket_dst_uri=gcs_blob_uri(orcid_bucket, f"{orcid_summaries_prefix}/"),
            description="Transfer ORCID data from AWS to GCP",
        )
        logging.info(f"Attempt {i+1}: Total number of objects transferred: {objects_count}")
        logging.info(f"Attempt {i+1}: Success? {success}")
        if success:
            break

    if not success:
        raise AirflowException("")


def bq_create_main_table_snapshot(release: dict, snapshot_expiry_days: int):
    """Create a snapshot of each main table. The purpose of this table is to be able to rollback the table
    if something goes wrong. The snapshot expires after snapshot_expiry_days.

    :param snapshot_expiry_days: The number of days to keep the snapshot
    """

    release = OrcidRelease.from_dict(release)
    if release.is_first_run:
        msg = f"bq_create_main_table_snapshots: skipping as snapshots are not created on the first run"
        logging.info(msg)
        raise AirflowSkipException(msg)

    expiry_date = pendulum.now().add(days=snapshot_expiry_days)
    success = bq.bq_snapshot(
        src_table_id=release.bq_main_table_id,
        dst_table_id=release.bq_snapshot_table_id,
        expiry_date=expiry_date,
    )
    if not success:
        raise AirflowException("")


def create_manifests(release: dict, orcid_bucket: str, orcid_summaries_prefix: str, max_workers: Optional[int] = None):
    """Create a manifest of all the modified files in the orcid bucket.

    :param orcid_bucket: The name of the gcs bucket to store the Orcid data
    :param orcid_summaries_prefix: The prefix in which to store the summaries in the bucket
    :param max_workers: Number of process pools to use. If None, uses the cpu count
    """

    release = OrcidRelease.from_dict(release)
    logging.info("Creating manifest")

    if not max_workers:
        max_workers = os.cpu_count() * 2
    logging.info(f"Using {max_workers} threads for manifest creation")

    orcid_batches = release.orcid_batches()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for orcid_batch in orcid_batches:
            futures.append(
                executor.submit(
                    create_orcid_batch_manifest,
                    orcid_batch=orcid_batch,
                    reference_date=release.prev_latest_modified_record,
                    bucket=orcid_bucket,
                    bucket_prefix=orcid_summaries_prefix,
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


def download(release: dict):
    """Reads each batch's manifest and downloads the files from the gcs bucket."""

    release = OrcidRelease.from_dict(release)
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
            logging.info(f"Downloaded {len(orcid_batch.expected_records)} records for batch '{orcid_batch.batch_str}'")
        total_time = time.time() - start_time

    # Check all files exist
    if total_files != len(release.downloaded_records):
        raise FileNotFoundError(
            f"Downloaded {total_files} files but found {len(release.downloaded_records)} records on disk."
        )
    logging.info(f"Completed download for {total_files} files in {str(datetime.timedelta(seconds=total_time))}")


def transform(release: dict, max_workers: Optional[int] = None):
    """Transforms the downloaded files into serveral bigquery-compatible .jsonl files

    :param max_workers: Number of process pools to use. If None, uses the cpu count
    """

    release = OrcidRelease.from_dict(release)
    if not max_workers:
        max_workers = os.cpu_count() * 2
    logging.info(f"Using {max_workers} processes for transform operation")

    total_upsert_records = 0
    total_delete_records = 0
    start_time = time.time()
    for orcid_batch in release.orcid_batches():
        logging.info(f"Transforming ORCID batch {orcid_batch.batch_str}")
        transformed_data = []
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for record in orcid_batch.existing_records:
                future = executor.submit(transform_orcid_record, os.path.join(orcid_batch.download_batch_dir, record))
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


def upload_transformed(release: dict):
    """Uploads the upsert and delete files to the transform bucket."""

    release = OrcidRelease.from_dict(release)
    file_paths = release.upsert_files + release.delete_files
    success = gcs_upload_files(bucket_name=release.cloud_workspace.transform_bucket, file_paths=file_paths)
    if not success:
        raise AirflowException("Error occurred uploading transformed filed to bigquery")


def bq_load_main_table(release: dict, schema_file_path: str):
    """Load the main table.

    :param schema_file_path: The path to the schema file to use for table load
    """

    release = OrcidRelease.from_dict(release)
    if not release.is_first_run:
        msg = f"bq_load_main_table: skipping as the main table is only created on the first run"
        logging.info(msg)
        raise AirflowSkipException(msg)

    # Check that the number of files matches the number of blobs
    blobs = gcs_list_blobs(release.cloud_workspace.transform_bucket, match_glob=release.upsert_blob_glob)
    if not len(blobs) == len(release.upsert_files):
        raise ValueError(f"Number of blobs ({len(blobs)}) does not match number of files ({len(release.upsert_files)})")

    success = bq.bq_load_table(
        uri=[gcs_blob_uri(blob.bucket.name, blob.name) for blob in blobs],
        table_id=release.bq_main_table_id,
        schema_file_path=schema_file_path,
        source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_EMPTY,
        ignore_unknown_values=False,
    )
    if not success:
        raise AirflowException("Error occurred loading data to main table")


def bq_load_upsert_table(release: dict, schema_file_path: str):
    """Load the upsert table into bigquery

    :param schema_file_path: The path to the schema file to use for table load
    """

    release = OrcidRelease.from_dict(release)
    if release.is_first_run:
        msg = f"bq_load_upsert_table: skipping as no records are upserted on the first run"
        logging.info(msg)
        raise AirflowSkipException(msg)

    # Check that the number of files matches the number of blobs
    blobs = gcs_list_blobs(release.cloud_workspace.transform_bucket, match_glob=release.upsert_blob_glob)
    if not len(blobs) == len(release.upsert_files):
        raise ValueError(f"Number of blobs ({len(blobs)}) does not match number of files ({len(release.upsert_files)})")

    success = bq.bq_load_table(
        uri=[gcs_blob_uri(blob.bucket.name, blob.name) for blob in blobs],
        table_id=release.bq_upsert_table_id,
        schema_file_path=schema_file_path,
        source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        ignore_unknown_values=True,
    )
    if not success:
        raise AirflowException("Error occurred loading upsert table")


def bq_load_delete_table(release: dict, delete_schema_file_path: str):
    """Load the delete table into bigquery

    :param schema_file_path: The path to the 'delete' schema file to use for table load
    """

    release = OrcidRelease.from_dict(release)
    if release.is_first_run:
        msg = "bq_load_delete_table: skipping as no records are deleted on the first run"
        logging.info(msg)
        raise AirflowSkipException(msg)
    if not len(release.delete_files):
        msg = "bq_load_delete_table: skipping as no records require deleting"
        logging.info(msg)
        raise AirflowSkipException(msg)

    # Check that the number of files matches the number of blobs
    blobs = gcs_list_blobs(release.cloud_workspace.transform_bucket, match_glob=release.delete_blob_glob)
    if not len(blobs) == len(release.delete_files):
        raise ValueError(f"Number of blobs ({len(blobs)}) does not match number of files ({len(release.delete_files)})")

    success = bq.bq_load_table(
        uri=[gcs_blob_uri(blob.bucket.name, blob.name) for blob in blobs],
        table_id=release.bq_delete_table_id,
        schema_file_path=delete_schema_file_path,
        source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        ignore_unknown_values=True,
    )
    if not success:
        raise AirflowException("Error occurred loading delete table")


def bq_upsert_records(release: dict):
    """Upsert the records from the upserts table into the main table."""

    release = OrcidRelease.from_dict(release)
    if release.is_first_run:
        msg = "bq_upsert_records: skipping as no records are upserted on the first run"
        logging.info(msg)
        raise AirflowSkipException(msg)

    bq.bq_upsert_records(
        main_table_id=release.bq_main_table_id,
        upsert_table_id=release.bq_upsert_table_id,
        primary_key="orcid_identifier",
    )


def bq_delete_records(release: dict):
    """Delete the records in the delete table from the main table."""

    release = OrcidRelease.from_dict(release)
    if release.is_first_run:
        msg = "bq_delete_records: skipping as no records are deleted on the first run"
        logging.info(msg)
        raise AirflowSkipException(msg)
    if not len(release.delete_files):
        msg = f"bq_load_delete_table: skipping as no records require deleting"
        logging.info(msg)
        raise AirflowSkipException(msg)

    bq.bq_delete_records(
        main_table_id=release.bq_main_table_id,
        delete_table_id=release.bq_delete_table_id,
        main_table_primary_key="orcid_identifier.path",
        delete_table_primary_key="id",
    )


def add_dataset_release(release: dict) -> None:
    """Adds release information to API."""

    release = OrcidRelease.from_dict(release)
    api = DatasetAPI(bq_project_id=release.cloud_workspace.project_id, bq_dataset_id=release.api_bq_dataset_id)
    api.seed_db()
    now = pendulum.now()
    dataset_release = DatasetRelease(
        dag_id=release.dag_id,
        entity_id="orcid",
        dag_run_id=release.run_id,
        created=now,
        modified=now,
        changefile_start_date=release.start_date,
        changefile_end_date=release.end_date,
        extra={"latest_modified_record_date": latest_modified_record_date(release.master_manifest_file)},
    )
    api.add_dataset_release(dataset_release)


def cleanup_workflow(release: dict) -> None:
    """Delete all files, folders and XComs associated with this release."""

    release = OrcidRelease.from_dict(release)
    cleanup(dag_id=release.dag_id, workflow_folder=release.workflow_folder)


def aws_orcid_key(conn_id: str) -> Tuple[str, str]:
    """Return API login and password"""

    connection = BaseHook.get_connection(conn_id)
    return connection.login, connection.password


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
        if pendulum.instance(blob.updated) > reference_date:
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
