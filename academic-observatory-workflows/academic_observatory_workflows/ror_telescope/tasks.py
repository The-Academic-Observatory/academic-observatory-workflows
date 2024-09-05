from __future__ import annotations

import json
import logging
import math
import os
import shutil
import urllib.parse
from typing import Any, Dict, List
from zipfile import BadZipFile, ZipFile

import pendulum
from airflow.exceptions import AirflowException
from google.cloud.bigquery import SourceFormat

from academic_observatory_workflows.ror_telescope.release import RorRelease
from observatory_platform.dataset_api import DatasetRelease, DatasetAPI
from observatory_platform.google.bigquery import bq_create_dataset, bq_find_schema, bq_load_table, bq_sharded_table_id
from observatory_platform.files import clean_dir, list_files, save_jsonl_gz
from observatory_platform.google.gcs import gcs_download_blob, gcs_upload_files
from observatory_platform.airflow.workflow import CloudWorkspace
from observatory_platform.http_download import download_file
from observatory_platform.url_utils import retry_get_url
from observatory_platform.airflow.workflow import cleanup
from observatory_platform.airflow.release import set_task_state


def fetch_releases(
    dag_id: str,
    run_id: str,
    cloud_workspace: CloudWorkspace,
    data_interval_start: pendulum.Datetime,
    data_interval_end: pendulum.Datetime,
    ror_conceptrecid: int,
) -> List[dict]:
    """Lists all ROR records and publishes their url, snapshot_date and checksum as an XCom.

    :param dag_id: The ID of the dag
    :param run_id: The ID of this dagrun
    :param cloud_workspace: The CoudWorkspace object
    :param data_interval_start: The start of the data interval for this dagrun
    :param data_interval_end: The end of the data interval for this dagrun
    :param ror_conceptrecid: the Zenodo conceptrecid for the ROR dataset.
    :return: The list of releases
    """

    records = list_ror_records(ror_conceptrecid, data_interval_start, data_interval_end)
    releases = []
    for record in records:
        releases.append(
            dict(
                dag_id=dag_id,
                run_id=run_id,
                snapshot_date=record["snapshot_date"],
                url=record["url"],
                checksum=record["checksum"],
                cloud_workspace=cloud_workspace.to_dict(),
                data_interval_start=data_interval_start,
                data_interval_end=data_interval_end,
            )
        )

    return releases


def download(release: dict):
    """Task to download the ROR releases."""

    release = RorRelease.from_dict(release)
    clean_dir(release.download_folder)

    # Download file from Zenodo
    hash_algorithm, hash_checksum = release.checksum.split(":")
    success, _ = download_file(
        url=release.url,
        filename=release.download_file_path,
        hash=hash_checksum,
        hash_algorithm=hash_algorithm,
    )
    if not success:
        raise AirflowException(f"Error downloading file from Zenodo: {release.url}")
    logging.info(f"Downloaded file from {release.url} to: {release.download_file_path}")

    # Upload file to GCS
    success = gcs_upload_files(
        bucket_name=release.cloud_workspace.download_bucket,
        file_paths=[release.download_file_path],
    )
    if not success:
        raise AirflowException(f"Error uploading file {release.download_file_path} to bucket: {release.download_uri}")


def transform(release: dict):
    """Task to transform the ROR releases."""

    release = RorRelease.from_dict(release)

    # Download file
    success = gcs_download_blob(
        bucket_name=release.cloud_workspace.download_bucket,
        blob_name=release.download_blob_name,
        file_path=release.download_file_path,
    )
    if not success:
        raise AirflowException(f"Error downloading file: {release.download_uri}")

    # Extract files
    logging.info(f"Extracting file: {release.download_file_path}")
    clean_dir(release.extract_folder)
    try:
        with ZipFile(release.download_file_path) as zip_file:
            zip_file.extractall(release.extract_folder)
    except BadZipFile:
        raise AirflowException(f"Not a zip file: {release.download_file_path}")
    logging.info(f"File extracted to: {release.extract_folder}")

    # Remove dud __MACOSX folder that shouldn't be there
    try:
        shutil.rmtree(os.path.join(release.extract_folder, "__MACOSX"))
    except FileNotFoundError:
        pass
    logging.info(f"Files extracted to: {release.extract_folder}")

    # Transform files
    logging.info(f"Transforming files")
    clean_dir(release.transform_folder)
    extract_files = list_files(release.extract_folder, release.extract_file_regex)

    # Check there is only one JSON file
    if len(extract_files) == 1:
        release_json_file = extract_files[0]
        logging.info(f"Transforming file: {release_json_file}")
    else:
        raise AirflowException(f"{len(extract_files)} extracted files found: {extract_files}")

    with open(release_json_file, "r") as f:
        records = json.load(f)
        records = transform_ror(records)
    save_jsonl_gz(release.transform_file_path, records)
    logging.info(f"Saved transformed file to: {release.transform_file_path}")

    # Upload to Bucket
    logging.info(f"Uploading file to bucket: {release.cloud_workspace.transform_bucket}")
    success = gcs_upload_files(
        bucket_name=release.cloud_workspace.transform_bucket,
        file_paths=[release.transform_file_path],
    )
    if not success:
        raise AirflowException(f"Error uploading file {release.transform_file_path} to bucket: {release.transform_uri}")


def bq_load(
    release: dict,
    bq_dataset_id: str,
    dataset_description: str,
    bq_table_name: str,
    table_description: str,
    schema_folder: str,
) -> None:
    """Load the data into BigQuery.

    :param bq_dataset_id: The bigquery dataset ID to load the data to
    :param dataset_description: The description to give the bigquery dataset
    :param bq_table_name: The table name to load into
    :param table_description: The description to give the bigquery table
    :param schema_folder: the folder containing the schema
    """

    release = RorRelease.from_dict(release)
    bq_create_dataset(
        project_id=release.cloud_workspace.output_project_id,
        dataset_id=bq_dataset_id,
        location=release.cloud_workspace.data_location,
        description=dataset_description,
    )
    schema_file_path = bq_find_schema(path=schema_folder, table_name=bq_table_name, release_date=release.snapshot_date)
    table_id = bq_sharded_table_id(
        release.cloud_workspace.output_project_id, bq_dataset_id, bq_table_name, release.snapshot_date
    )

    success = bq_load_table(
        uri=release.transform_uri,
        table_id=table_id,
        schema_file_path=schema_file_path,
        source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
        table_description=table_description,
        ignore_unknown_values=True,
    )
    set_task_state(success, "bq_load", release)


def add_dataset_releases(release: dict, api_bq_dataset_id: str) -> None:
    """Adds release information to API.

    :param api_bq_dataset_id: The dataset containing the api table"
    """

    release = RorRelease.from_dict(release)
    api = DatasetAPI(bq_project_id=release.cloud_workspace.project_id, bq_dataset_id=api_bq_dataset_id)
    api.seed_db()
    now = pendulum.now()
    dataset_release = DatasetRelease(
        dag_id=release.dag_id,
        entity_id="ror",
        dag_run_id=release.run_id,
        created=now,
        modified=now,
        snapshot_date=release.snapshot_date,
        data_interval_start=release.data_interval_start,
        data_interval_end=release.data_interval_end,
    )
    api.add_dataset_release(dataset_release)


def cleanup_workflow(release: dict) -> None:
    """Delete all files, folders and XComs associated with this release."""

    release = RorRelease.from_dict(release)
    cleanup(dag_id=release.dag_id, workflow_folder=release.workflow_folder)


def list_ror_records(
    conceptrecid: int,
    start_date: pendulum.DateTime,
    end_date: pendulum.DateTime,
    page_size: int = 10,
    timeout: float = 30.0,
) -> List[dict]:
    """List all ROR Zenodo versions available between two dates.

    :param conceptrecid: the Zendodo conceptrecid for ROR.
    :param start_date: Start date of period to look into
    :param end_date: End date of period to look into
    :param page_size: the page size for the query.
    :param timeout: the number of seconds to wait until timing out.
    :return: the list of ROR Zenodo records with required variables stored as a dictionary.
    """
    logging.info(f"Getting info on available records from Zenodo")

    records: List[dict] = []
    q = urllib.parse.quote_plus(f"conceptrecid:{conceptrecid}")
    page = 1
    fetch = True
    while fetch:
        url = f"https://zenodo.org/api/records/?q={q}&all_versions=1&sort=mostrecent&size={page_size}&page={page}"
        response = retry_get_url(url, timeout=timeout, headers={"Accept-encoding": "gzip"})
        response = json.loads(response.text)

        # Get release date and url of records that are created between two dates
        hits = response.get("hits", {}).get("hits", [])
        logging.info(f"Looking for records between dates {start_date} and {end_date}")
        for hit in hits:
            publication_date: pendulum.DateTime = pendulum.parse(hit["metadata"]["publication_date"])
            if start_date <= publication_date < end_date:
                # Get ROR file checking that there is only one
                files = hit["files"]
                if len(files) != 1:
                    raise AirflowException(f"list_zenodo_records: there should only be one file present: hit={hit}")
                file = files[0]

                # Get file metadata also checking that the file type is zip
                link = file["links"]["self"]
                checksum = file["checksum"]
                filename = file["key"]
                file_type = os.path.splitext(filename)[1][1:]
                if file_type != "zip":
                    raise AirflowException(f"list_zenodo_records: file is not .zip: hit={hit}")

                records.append(
                    {"snapshot_date": publication_date.format("YYYYMMDD"), "url": link, "checksum": checksum}
                )
                logging.info(f"Found record created on '{publication_date}', url: {link}")

            if publication_date < start_date:
                fetch = False
                break

        # Get the URL for the next page
        if len(hits) == 0:
            fetch = False

        # Go to next page
        page += 1

    return records


def is_lat_lng_valid(lat: Any, lng: Any) -> bool:
    """Validate whether a lat and lng are valid.

    :param lat: the latitude.
    :param lng: the longitude.
    :return: whether the lat/long combination is valid
    """

    return math.fabs(lat) <= 90 and math.fabs(lng) <= 180


def transform_ror(ror: List[Dict]) -> List[Dict]:
    """Transform a ROR release.

    :param ror: the ROR records.
    :return: the transfromed records.
    """

    records = []
    for record in ror:
        ror_id = record["id"]
        # Check that address coordinates are correct
        for address in record["addresses"]:
            lat = address["lat"]
            lng = address["lng"]
            if lat is not None and lng is not None and not is_lat_lng_valid(lat, lng):
                logging.warning(f"{ror_id} has invalid lat or lng: {lat}, {lng}. Setting both to None.")
                address["lat"] = None
                address["lng"] = None
        records.append(record)
    return records
