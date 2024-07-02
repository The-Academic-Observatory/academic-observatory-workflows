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


from __future__ import annotations

import functools
import json
import logging
import os
import shutil
from concurrent.futures import as_completed, ProcessPoolExecutor
from datetime import datetime

import jsonlines
import pendulum
import requests
from airflow import DAG
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.operators.bash import BashOperator
from bs4 import BeautifulSoup
from google.cloud.bigquery import SourceFormat
from natsort import natsorted

from academic_observatory_workflows.config import project_path
from observatory.api.client.model.dataset_release import DatasetRelease
from observatory_platform.airflow import on_failure_callback
from observatory_platform.dataset_api import make_observatory_api
from observatory_platform.google.bigquery import bq_create_dataset, bq_find_schema, bq_load_table, bq_sharded_table_id
from observatory_platform.config import AirflowConns
from observatory_platform.files import clean_dir, get_chunks, list_files
from observatory_platform.google.gcs import gcs_blob_name_from_path, gcs_blob_uri, gcs_upload_files
from observatory_platform.airflow.workflow import CloudWorkspace
from observatory_platform.refactor.tasks import check_dependencies
from observatory_platform.url_utils import retry_get_url, retry_session
from observatory_platform.workflows.workflow import cleanup, set_task_state, SnapshotRelease

SNAPSHOT_URL = "https://api.crossref.org/snapshots/monthly/{year}/{month:02d}/all.json.tar.gz"


class CrossrefMetadataRelease(SnapshotRelease):
    def __init__(
        self,
        *,
        dag_id: str,
        run_id: str,
        snapshot_date: pendulum.DateTime,
        cloud_workspace: CloudWorkspace,
        batch_size: int,
    ):
        """Construct a RorRelease.

        :param dag_id: the DAG id.
        :param run_id: the DAG run id.
        :param snapshot_date: the release date.
        :param cloud_workspace: the cloud workspace settings.
        :param batch_size: the number of files to send to ProcessPoolExecutor at one time.
        """

        super().__init__(dag_id=dag_id, run_id=run_id, snapshot_date=snapshot_date)
        self.cloud_workspace = cloud_workspace
        self.batch_size = batch_size
        self.download_file_name = "crossref_metadata.json.tar.gz"
        self.download_file_path = os.path.join(self.download_folder, self.download_file_name)
        self.extract_files_regex = r".*\.json$"
        self.transform_files_regex = r".*\.jsonl$"

    @staticmethod
    def from_dict(dict_: dict):
        dag_id = dict_["dag_id"]
        run_id = dict_["run_id"]
        snapshot_date = pendulum.parse(dict_["snapshot_date"])
        cloud_workspace = CloudWorkspace.from_dict(dict_["cloud_workspace"])
        batch_size = dict_["batch_size"]
        return CrossrefMetadataRelease(
            dag_id=dag_id,
            run_id=run_id,
            snapshot_date=snapshot_date,
            cloud_workspace=cloud_workspace,
            batch_size=batch_size,
        )

    def to_dict(self) -> dict:
        return dict(
            dag_id=self.dag_id,
            run_id=self.run_id,
            snapshot_date=self.snapshot_date.to_datetime_string(),
            cloud_workspace=self.cloud_workspace.to_dict(),
            batch_size=self.batch_size,
        )


def create_dag(
    *,
    dag_id: str,
    cloud_workspace: CloudWorkspace,
    bq_dataset_id: str = "crossref_metadata",
    bq_table_name: str = "crossref_metadata",
    api_dataset_id: str = "crossref_metadata",
    schema_folder: str = project_path("crossref_metadata_telescope", "schema"),
    dataset_description: str = "The Crossref Metadata Plus dataset: https://www.crossref.org/services/metadata-retrieval/metadata-plus/",
    table_description: str = "The Crossref Metadata Plus dataset: https://www.crossref.org/services/metadata-retrieval/metadata-plus/",
    crossref_metadata_conn_id: str = "crossref_metadata",
    observatory_api_conn_id: str = AirflowConns.OBSERVATORY_API,
    max_processes: int = os.cpu_count(),
    batch_size: int = 20,
    start_date: pendulum.DateTime = pendulum.datetime(2020, 6, 7),
    schedule: str = "0 0 7 * *",
    catchup: bool = True,
    queue: str = "remote_queue",
    max_active_runs: int = 1,
    retries: int = 3,
) -> DAG:
    """The Crossref Metadata telescope

    :param dag_id: the id of the DAG.
    :param cloud_workspace: the cloud workspace settings.
    :param bq_dataset_id: the BigQuery dataset id.
    :param bq_table_name: the BigQuery table name.
    :param api_dataset_id: the Dataset ID to use when storing releases.
    :param schema_folder: the SQL schema path.
    :param dataset_description: description for the BigQuery dataset.
    :param table_description: description for the BigQuery table.
    :param crossref_metadata_conn_id: the Crossref Metadata Airflow connection key.
    :param observatory_api_conn_id: the Observatory API connection key.
    :param max_processes: the number of processes used with ProcessPoolExecutor to transform files in parallel.
    :param batch_size: the number of files to send to ProcessPoolExecutor at one time.
    :param start_date: the start date of the DAG.
    :param schedule: the schedule interval of the DAG.
    :param catchup: whether to catchup the DAG or not.
    :param queue: what Airflow queue this job runs on.
    :param max_active_runs: the maximum number of DAG runs that can be run at once.
    :param retries: the number of times to retry a task.
    """

    @dag(
        dag_id=dag_id,
        start_date=start_date,
        schedule=schedule,
        catchup=catchup,
        max_active_runs=max_active_runs,
        tags=["academic-observatory"],
        default_args={
            "owner": "airflow",
            "on_failure_callback": on_failure_callback,
            "retries": retries,
            "queue": queue,
        },
    )
    def crossref_metadata():
        @task
        def fetch_release(**context):
            """Fetch the release for this month, making sure that it exists."""

            # List all available releases for logging and debugging purposes
            # These values are not used to actually check if the release is available
            logging.info(f"Listing available releases since start date ({start_date}):")
            for dt in pendulum.period(pendulum.instance(start_date), pendulum.today("UTC")).range("years"):
                response = requests.get(f"https://api.crossref.org/snapshots/monthly/{dt.year}")
                soup = BeautifulSoup(response.text)
                hrefs = soup.find_all("a", href=True)
                for href in hrefs:
                    logging.info(href["href"])

            # Construct the release for the execution date and check if it exists.
            # The release for a given logical_date is added on the 5th day of the following month.
            # E.g. the 2020-05 release is added to the website on 2020-06-05.
            data_interval_start = context["data_interval_start"]
            exists = check_release_exists(data_interval_start, get_api_key(crossref_metadata_conn_id))
            if not exists:
                raise AirflowException(
                    f"check_release_exists: release doesn't exist for month {data_interval_start.year}-{data_interval_start.month}, something is wrong and needs investigating."
                )

            # The release date is always the end of the logical_date month
            snapshot_date = context["data_interval_start"].end_of("month")
            run_id = context["run_id"]
            return CrossrefMetadataRelease(
                dag_id=dag_id,
                run_id=run_id,
                snapshot_date=snapshot_date,
                cloud_workspace=cloud_workspace,
                batch_size=batch_size,
            ).to_dict()

        @task
        def download(release: dict, **context):
            """Task to download the CrossrefMetadataRelease release for a given month."""

            release = CrossrefMetadataRelease.from_dict(release)
            clean_dir(release.download_folder)

            url = make_snapshot_url(release.snapshot_date)
            logging.info(f"Downloading from url: {url}")

            # Set API token header
            api_key = get_api_key(crossref_metadata_conn_id)
            header = {"Crossref-Plus-API-Token": f"Bearer {api_key}"}

            # Download release
            with retry_get_url(url, headers=header, stream=True) as response:
                with open(release.download_file_path, "wb") as file:
                    response.raw.read = functools.partial(response.raw.read, decode_content=True)
                    shutil.copyfileobj(response.raw, file)

            logging.info(f"Successfully download url to {release.download_file_path}")

        @task
        def upload_downloaded(release: dict, **context):
            """Upload data to Cloud Storage."""

            release = CrossrefMetadataRelease.from_dict(release)
            success = gcs_upload_files(
                bucket_name=cloud_workspace.download_bucket, file_paths=[release.download_file_path]
            )
            set_task_state(success, "upload_downloaded", release)

        @task
        def extract(release: dict, **context):
            release = CrossrefMetadataRelease.from_dict(release)
            op = BashOperator(
                task_id="extract",
                bash_command=f'tar -xv -I "pigz -d" -f { release.download_file_path } -C { release.extract_folder }',
                do_xcom_push=False,
            )
            op.execute(context)

        @task
        def transform(release: dict, **context):
            """Task to transform the CrossrefMetadataRelease release for a given month.
            Each extracted file is transformed."""

            release = CrossrefMetadataRelease.from_dict(release)
            logging.info(f"Transform input folder: {release.extract_folder}, output folder: {release.transform_folder}")
            clean_dir(release.transform_folder)
            finished = 0

            # List files and sort so that they are processed in ascending order
            input_file_paths = natsorted(list_files(release.extract_folder, release.extract_files_regex))

            # Process files in batches so that ProcessPoolExecutor doesn't deplete the system of memory
            for i, chunk in enumerate(get_chunks(input_list=input_file_paths, chunk_size=batch_size)):
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

        @task
        def upload_transformed(release: dict, **context) -> None:
            """Upload the transformed data to Cloud Storage."""

            release = CrossrefMetadataRelease.from_dict(release)
            files_list = list_files(release.transform_folder, release.transform_files_regex)
            success = gcs_upload_files(bucket_name=cloud_workspace.transform_bucket, file_paths=files_list)
            set_task_state(success, "upload_transformed", release)

        @task
        def bq_load(release: dict, **context):
            """Task to load each transformed release to BigQuery.
            The table_id is set to the file name without the extension."""

            release = CrossrefMetadataRelease.from_dict(release)
            bq_create_dataset(
                project_id=cloud_workspace.output_project_id,
                dataset_id=bq_dataset_id,
                location=cloud_workspace.data_location,
                description=dataset_description,
            )

            # Selects all jsonl.gz files in the releases transform folder on the Google Cloud Storage bucket and all of its
            # subfolders: https://cloud.google.com/bigquery/docs/batch-loading-data#load-wildcards
            uri = gcs_blob_uri(
                cloud_workspace.transform_bucket,
                f"{gcs_blob_name_from_path(release.transform_folder)}/*.jsonl",
            )
            table_id = bq_sharded_table_id(
                cloud_workspace.output_project_id, bq_dataset_id, bq_table_name, release.snapshot_date
            )
            schema_file_path = bq_find_schema(
                path=schema_folder, table_name=bq_table_name, release_date=release.snapshot_date
            )
            success = bq_load_table(
                uri=uri,
                table_id=table_id,
                schema_file_path=schema_file_path,
                source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
                table_description=table_description,
                ignore_unknown_values=True,
            )
            set_task_state(success, "bq_load", release)

        @task
        def add_dataset_release(release: dict, **context) -> None:
            """Adds release information to API."""

            release = CrossrefMetadataRelease.from_dict(release)
            dataset_release = DatasetRelease(
                dag_id=dag_id,
                dataset_id=api_dataset_id,
                dag_run_id=release.run_id,
                snapshot_date=release.snapshot_date,
                data_interval_start=context["data_interval_start"],
                data_interval_end=context["data_interval_end"],
            )
            api = make_observatory_api(observatory_api_conn_id=observatory_api_conn_id)
            api.post_dataset_release(dataset_release)

        @task
        def cleanup_workflow(release: dict, **context) -> None:
            """Delete all files, folders and XComs associated with this release."""

            release = CrossrefMetadataRelease.from_dict(release)
            cleanup(dag_id=dag_id, execution_date=context["logical_date"], workflow_folder=release.workflow_folder)

        # Define task connections
        task_check_dependencies = check_dependencies(airflow_conns=[observatory_api_conn_id, crossref_metadata_conn_id])
        xcom_release = fetch_release()
        task_download = download(xcom_release)
        task_upload_downloaded = upload_downloaded(xcom_release)
        task_extract = extract(xcom_release)
        task_transform = transform(xcom_release)
        task_upload_transformed = upload_transformed(xcom_release)
        task_bq_load = bq_load(xcom_release)
        task_add_dataset_release = add_dataset_release(xcom_release)
        task_cleanup_workflow = cleanup_workflow(xcom_release)

        (
            task_check_dependencies
            >> xcom_release
            >> task_download
            >> task_upload_downloaded
            >> task_extract
            >> task_transform
            >> task_upload_transformed
            >> task_bq_load
            >> task_add_dataset_release
            >> task_cleanup_workflow
        )

    return crossref_metadata()


def make_snapshot_url(snapshot_date: pendulum.DateTime) -> str:
    return SNAPSHOT_URL.format(year=snapshot_date.year, month=snapshot_date.month)


def get_api_key(crossref_metadata_conn_id: str):
    """Return API token"""
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
