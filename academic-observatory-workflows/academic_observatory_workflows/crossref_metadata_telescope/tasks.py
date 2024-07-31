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

from concurrent.futures import as_completed, ProcessPoolExecutor
from datetime import datetime
import functools
import json
import logging
import os
import requests
import shutil

from bs4 import BeautifulSoup
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from google.cloud.bigquery import SourceFormat
import jsonlines
from natsort import natsorted
import pendulum

from academic_observatory_workflows.crossref_metadata_telescope.release import CrossrefMetadataRelease
from observatory_platform.airflow.release import set_task_state
from observatory_platform.airflow.workflow import cleanup, CloudWorkspace
from observatory_platform.dataset_api import DatasetRelease, DatasetAPI
from observatory_platform.files import clean_dir, get_chunks, list_files
from observatory_platform.google.bigquery import bq_create_dataset, bq_find_schema, bq_load_table, bq_sharded_table_id
from observatory_platform.google.gcs import gcs_blob_name_from_path, gcs_blob_uri, gcs_upload_files
from observatory_platform.url_utils import retry_get_url, retry_session


SNAPSHOT_URL = "https://api.crossref.org/snapshots/monthly/{year}/{month:02d}/all.json.tar.gz"


def fetch_release(
    *,
    cloud_workspace: CloudWorkspace,
    crossref_metadata_conn_id: str,
    dag_id: str,
    run_id: str,
    data_interval_start: pendulum.DateTime,
    data_interval_end: pendulum.DateTime,
) -> dict:
    """Task to retrieve the release for the given start date

    :param cloud_workspace: The cloud workspace object for the dag run
    :param crossref_metadata_conn_id: The connection ID for crossref metadata
    :param dag_id: The dag ID for the dag run
    :param run_id: The run ID for the dag run
    :param start_date: The earliest date to retrieve a release for
    :param data_interval_start: The start of the data interval for this dag run
    :param data_interval_end: The end of the data interval for this dag run
    :return: The release object in dictionary form
    """

    # Construct the release for the execution date and check if it exists.
    # The release for a given logical_date is added on the 5th day of the following month.
    # E.g. the 2020-05 release is added to the website on 2020-06-05.
    exists = check_release_exists(data_interval_start, get_api_key(crossref_metadata_conn_id))
    if not exists:
        raise AirflowException(
            f"Release doesn't exist for month {data_interval_start.year}-{data_interval_start.month}, something is wrong and needs investigating."
        )

    # The release date is always the end of the logical_date month
    snapshot_date = data_interval_start.end_of("month")
    return CrossrefMetadataRelease(
        dag_id=dag_id,
        run_id=run_id,
        snapshot_date=snapshot_date,
        data_interval_start=data_interval_start,
        data_interval_end=data_interval_end,
        cloud_workspace=cloud_workspace,
    ).to_dict()


def download(release: dict) -> None:
    """Task to Download the crossref metadata dataset.
    Expects the api key to be set as an environment variable named CROSSREF_METADATA_API_KEY
    """
    release = CrossrefMetadataRelease.from_dict(release)
    clean_dir(release.download_folder)

    url = make_snapshot_url(release.snapshot_date)
    logging.info(f"Downloading from url: {url}")

    # Set API token header
    api_key = os.environ.get("CROSSREF_METADATA_API_KEY")
    if api_key is None:
        raise AirflowException(
            f"The CROSSREF_METADATA_API_KEY environment variable is not set, please set it with a Kubernetes Secret."
        )
    header = {"Crossref-Plus-API-Token": f"Bearer {api_key}"}

    # Download release
    with retry_get_url(url, headers=header, stream=True) as response:
        with open(release.download_file_path, "wb") as file:
            response.raw.read = functools.partial(response.raw.read, decode_content=True)
            shutil.copyfileobj(response.raw, file)

    logging.info(f"Successfully download url to {release.download_file_path}")


def upload_downloaded(release: dict) -> None:
    """Task to upload downloaded data to Cloud Storage."""

    release = CrossrefMetadataRelease.from_dict(release)
    success = gcs_upload_files(
        bucket_name=release.cloud_workspace.download_bucket, file_paths=[release.download_file_path]
    )
    set_task_state(success, "upload_downloaded", release)


def extract(release, **context) -> None:
    """Task to extract the downloaded metadata."""

    release = CrossrefMetadataRelease.from_dict(release)
    op = BashOperator(
        task_id="extract",
        bash_command=f'tar -xv -I "pigz -d" -f { release.download_file_path } -C { release.extract_folder }',
        do_xcom_push=False,
    )
    op.execute(context)


def transform(release: dict, *, max_processes: int, batch_size: int) -> None:
    """Task to transform the CrossrefMetadataRelease release for a given month.
    Each extracted file is transformmed.

    :param max_processes: the number of processes used with ProcessPoolExecutor to transform files in parallel.
    :param batch_size: the number of files to send to ProcessPoolExecutor at one time.
    """

    release = CrossrefMetadataRelease.from_dict(release)
    logging.info(f"Transform input folder: {release.extract_folder}, output folder: {release.transform_folder}")
    clean_dir(release.transform_folder)
    finished = 0

    # List files and sort so that they are processed in ascending order
    input_file_paths = natsorted(list_files(release.extract_folder, release.extract_files_regex))

    # Process files in batches so that ProcessPoolExecutor doesn't deplete the system of memory
    for chunk in get_chunks(input_list=input_file_paths, chunk_size=batch_size):
        with ProcessPoolExecutor(max_workers=max_processes) as executor:
            futures = []

            # Create tasks for each file
            for input_file in chunk:
                output_file = os.path.join(release.transform_folder, os.path.basename(input_file) + "l")
                future = executor.submit(transform_file, input_file, output_file)
                futures.append(future)

            # Wait for completed tasks
            for future in as_completed(futures):
                future.result()
                finished += 1
                if finished % 1000 == 0:
                    logging.info(f"Transformed {finished} files")


def upload_transformed(release: dict) -> None:
    """Task to upload the transformed data to Cloud Storage."""

    release = CrossrefMetadataRelease.from_dict(release)
    files_list = list_files(release.transform_folder, release.transform_files_regex)
    success = gcs_upload_files(bucket_name=release.cloud_workspace.transform_bucket, file_paths=files_list)
    set_task_state(success, "upload_transformed", release)


def bq_load(
    release: dict,
    *,
    bq_dataset_id: str,
    bq_table_name: str,
    dataset_description: str,
    table_description: str,
    schema_folder: str,
):
    """Task to load each transformed release to BigQuery.
    The table_id is set to the file name without the extension.

    :param bq_dataset_id: The bigquery dataset ID
    :param bq_table_name: The bigqiery table name
    :param dataset_description: The description to use when creating the dataset
    :param table_description: The description to use when creating the table
    :param schema_folder: The path to the schema folder
    """

    release = CrossrefMetadataRelease.from_dict(release)
    bq_create_dataset(
        project_id=release.cloud_workspace.output_project_id,
        dataset_id=bq_dataset_id,
        location=release.cloud_workspace.data_location,
        description=dataset_description,
    )

    # Selects all jsonl.gz files in the releases transform folder on the Google Cloud Storage bucket and all of its
    # subfolders: https://cloud.google.com/bigquery/docs/batch-loading-data#load-wildcards
    uri = gcs_blob_uri(
        release.cloud_workspace.transform_bucket,
        f"{gcs_blob_name_from_path(release.transform_folder)}/*.jsonl",
    )
    table_id = bq_sharded_table_id(
        release.cloud_workspace.output_project_id, bq_dataset_id, bq_table_name, release.snapshot_date
    )
    schema_file_path = bq_find_schema(path=schema_folder, table_name=bq_table_name, release_date=release.snapshot_date)
    success = bq_load_table(
        uri=uri,
        table_id=table_id,
        schema_file_path=schema_file_path,
        source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
        table_description=table_description,
        ignore_unknown_values=True,
    )
    set_task_state(success, "bq_load", release)


def add_dataset_release(release: dict, *, api_bq_dataset_id: str) -> None:
    """Task to add release information to API.

    :param dag_id: The DAG ID
    :param api_bq_dataset_id: The bigquery dataset ID for the API
    """

    release = CrossrefMetadataRelease.from_dict(release)

    api = DatasetAPI(bq_project_id=release.cloud_workspace.project_id, bq_dataset_id=api_bq_dataset_id)
    api.seed_db()
    now = pendulum.now()
    dataset_release = DatasetRelease(
        dag_id=release.dag_id,
        entity_id="crossref_metadata",
        dag_run_id=release.run_id,
        created=now,
        modified=now,
        data_interval_start=release.data_interval_start,
        data_interval_end=release.data_interval_end,
        snapshot_date=release.snapshot_date,
    )
    api.add_dataset_release(dataset_release)


def cleanup_workflow(release: dict, *, dag_id: str) -> None:
    """Task to delete all files, folders and XComs associated with this release.

    :param dag_id: The ID of the DAG
    :param logical_date: The DAG run's logical/execution date
    """

    release = CrossrefMetadataRelease.from_dict(release)
    cleanup(dag_id=dag_id, workflow_folder=release.workflow_folder)


def make_snapshot_url(snapshot_date: pendulum.DateTime) -> str:
    """Creates the url to the snapshot

    :param snashot_date: The date of the snapshot
    :return: The snapshot url
    """

    return SNAPSHOT_URL.format(year=snapshot_date.year, month=snapshot_date.month)


def get_api_key(crossref_metadata_conn_id: str) -> str:
    """Return Crossref Metadata API token from Airflow connections

    :param crossref_metadata_id: The airflow connection ID for crossref metadata
    :return: The crossref metadata api key
    """

    connection = BaseHook.get_connection(crossref_metadata_conn_id)
    return connection.password


def check_release_exists(month: pendulum.DateTime, api_key: str) -> bool:
    """Check if a release exists.

    :param month: the month of the release given as a datetime.
    :param api_key: the Crossref Metadata API key.
    :return: if release exists or not.
    """

    url = make_snapshot_url(month)
    logging.info(f"Checking if available release exists for {month.year}-{month.month}")

    # Get API key: it is required to check the head now
    response = retry_session().head(url, headers={"Crossref-Plus-API-Token": f"Bearer {api_key}"})
    if response.status_code == 302:
        logging.info(f"Snapshot exists at url: {url}, response code: {response.status_code}")
        return True
    else:
        logging.info(
            f"Snapshot does not exist at url: {url}, response code: {response.status_code}, "
            f"reason: {response.reason}"
        )
        return False


def transform_file(input_file_path: str, output_file_path: str):
    """Transform a single Crossref Metadata json file.
    The json file is converted to a jsonl file and field names are transformed so they are accepted by BigQuery.

    :param input_file_path: the path of the file to transform.
    :param output_file_path: where to save the transformed file.
    :return: None.
    """

    # Open json
    with open(input_file_path, mode="r") as in_file:
        input_data = json.load(in_file)

    # Transform and write
    with jsonlines.open(output_file_path, mode="w", compact=True) as out_file:
        for item in input_data["items"]:
            out_file.write(transform_item(item))


def transform_item(item):
    """Transform a single Crossref Metadata JSON value.

    :param item: a JSON value.
    :return: the transformed item.
    """

    if isinstance(item, dict):
        new = {}
        for k, v in item.items():
            # Replace hyphens with underscores for BigQuery compatibility
            k = k.replace("-", "_")

            # Get inner array for date parts
            if k == "date_parts":
                v = v[0]
                if None in v:
                    # "date-parts" : [ [ null ] ]
                    v = []
            elif k == "award":
                if isinstance(v, str):
                    v = [v]
            elif k == "date_time":
                try:
                    datetime.strptime(v, "%Y-%m-%dT%H:%M:%SZ")
                except ValueError:
                    v = ""

            new[k] = transform_item(v)
        return new
    elif isinstance(item, list):
        return [transform_item(i) for i in item]
    else:
        return item
