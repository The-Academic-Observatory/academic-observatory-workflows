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
from airflow.hooks.base import BaseHook
from bs4 import BeautifulSoup
from google.cloud.bigquery import SourceFormat
from natsort import natsorted
from observatory.api.client.model.dataset_release import DatasetRelease
from observatory.platform.api import make_observatory_api
from observatory.platform.bigquery import bq_create_dataset, bq_find_schema, bq_load_table, bq_sharded_table_id
from observatory.platform.config import AirflowConns
from observatory.platform.files import clean_dir, get_chunks, list_files
from observatory.platform.gcs import gcs_blob_name_from_path, gcs_blob_uri, gcs_upload_files
from observatory.platform.observatory_config import CloudWorkspace
from observatory.platform.utils.url_utils import retry_get_url, retry_session
from observatory.platform.workflows.workflow import (
    cleanup,
    set_task_state,
    SnapshotRelease,
    Workflow,
    WorkflowBashOperator,
)

from academic_observatory_workflows.config import project_path, Tag

SNAPSHOT_URL = "https://api.crossref.org/snapshots/monthly/{year}/{month:02d}/all.json.tar.gz"


def make_snapshot_url(snapshot_date: pendulum.DateTime) -> str:
    return SNAPSHOT_URL.format(year=snapshot_date.year, month=snapshot_date.month)


class CrossrefMetadataRelease(SnapshotRelease):
    def __init__(self, *, dag_id: str, run_id: str, snapshot_date: pendulum.DateTime):
        """Construct a RorRelease.

        :param dag_id: the DAG id.
        :param run_id: the DAG run id.
        :param snapshot_date: the release date.
        """

        super().__init__(dag_id=dag_id, run_id=run_id, snapshot_date=snapshot_date)
        self.download_file_name = "crossref_metadata.json.tar.gz"
        self.download_file_path = os.path.join(self.download_folder, self.download_file_name)
        self.extract_files_regex = r".*\.json$"
        self.transform_files_regex = r".*\.jsonl$"


class CrossrefMetadataTelescope(Workflow):
    """
    The Crossref Metadata Telescope

    Saved to the BigQuery table: <project_id>.crossref.crossref_metadataYYYYMMDD
    """

    def __init__(
        self,
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
    ):
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
        """

        super().__init__(
            dag_id=dag_id,
            start_date=start_date,
            schedule=schedule,
            catchup=catchup,
            airflow_conns=[observatory_api_conn_id, crossref_metadata_conn_id],
            tags=[Tag.academic_observatory],
            max_active_runs=max_active_runs,
            queue=queue,
        )
        self.cloud_workspace = cloud_workspace
        self.bq_dataset_id = bq_dataset_id
        self.bq_table_name = bq_table_name
        self.api_dataset_id = api_dataset_id
        self.schema_folder = schema_folder
        self.dataset_description = dataset_description
        self.table_description = table_description
        self.crossref_metadata_conn_id = crossref_metadata_conn_id
        self.observatory_api_conn_id = observatory_api_conn_id
        self.max_processes = max_processes
        self.batch_size = batch_size

        self.add_setup_task(self.check_dependencies)
        self.add_setup_task(self.check_release_exists)
        self.add_task(self.download)
        self.add_task(self.upload_downloaded)
        self.add_operator(
            WorkflowBashOperator(
                workflow=self,
                task_id="extract",
                bash_command='tar -xv -I "pigz -d" -f {{ release.download_file_path }} -C {{ release.extract_folder }}',
            )
        )
        self.add_task(self.transform)
        self.add_task(self.upload_transformed)
        self.add_task(self.bq_load)
        self.add_task(self.add_new_dataset_releases)
        self.add_task(self.cleanup)

    @property
    def api_key(self):
        """Return API token"""
        connection = BaseHook.get_connection(self.crossref_metadata_conn_id)
        return connection.password

    def make_release(self, **kwargs) -> CrossrefMetadataRelease:
        """Make release instances. The release is passed as an argument to the function (TelescopeFunction) that is
        called in 'task_callable'.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are
        passed to this argument.
        :return: a list of CrossrefMetadataRelease instances.
        """

        # The release date is always the end of the execution_date month
        snapshot_date = kwargs["data_interval_start"].end_of("month")
        run_id = kwargs["run_id"]
        return CrossrefMetadataRelease(dag_id=self.dag_id, run_id=run_id, snapshot_date=snapshot_date)

    def check_release_exists(self, **kwargs):
        """Check that the release for this month exists."""

        # List all available releases for logging and debugging purposes
        # These values are not used to actually check if the release is available
        logging.info(f"Listing available releases since start date ({self.start_date}):")
        for dt in pendulum.period(pendulum.instance(self.start_date), pendulum.today("UTC")).range("years"):
            response = requests.get(f"https://api.crossref.org/snapshots/monthly/{dt.year}")
            soup = BeautifulSoup(response.text)
            hrefs = soup.find_all("a", href=True)
            for href in hrefs:
                logging.info(href["href"])

        # Construct the release for the execution date and check if it exists.
        # The release for a given execution_date is added on the 5th day of the following month.
        # E.g. the 2020-05 release is added to the website on 2020-06-05.
        data_interval_start = kwargs["data_interval_start"]
        exists = check_release_exists(data_interval_start, self.api_key)
        assert (
            exists
        ), f"check_release_exists: release doesn't exist for month {data_interval_start.year}-{data_interval_start.month}, something is wrong and needs investigating."

        return True

    def download(self, release: CrossrefMetadataRelease, **kwargs):
        """Task to download the CrossrefMetadataRelease release for a given month."""

        clean_dir(release.download_folder)

        url = make_snapshot_url(release.snapshot_date)
        logging.info(f"Downloading from url: {url}")

        # Set API token header
        header = {"Crossref-Plus-API-Token": f"Bearer {self.api_key}"}

        # Download release
        with retry_get_url(url, headers=header, stream=True) as response:
            with open(release.download_file_path, "wb") as file:
                response.raw.read = functools.partial(response.raw.read, decode_content=True)
                shutil.copyfileobj(response.raw, file)

        logging.info(f"Successfully download url to {release.download_file_path}")

    def upload_downloaded(self, release: CrossrefMetadataRelease, **kwargs):
        """Upload data to Cloud Storage."""

        success = gcs_upload_files(
            bucket_name=self.cloud_workspace.download_bucket, file_paths=[release.download_file_path]
        )
        set_task_state(success, self.upload_transformed.__name__, release)

    def transform(self, release: CrossrefMetadataRelease, **kwargs):
        """Task to transform the CrossrefMetadataRelease release for a given month.
        Each extracted file is transformed."""

        logging.info(f"Transform input folder: {release.extract_folder}, output folder: {release.transform_folder}")
        clean_dir(release.transform_folder)
        finished = 0

        # List files and sort so that they are processed in ascending order
        input_file_paths = natsorted(list_files(release.extract_folder, release.extract_files_regex))

        # Process files in batches so that ProcessPoolExecutor doesn't deplete the system of memory
        for i, chunk in enumerate(get_chunks(input_list=input_file_paths, chunk_size=self.batch_size)):
            with ProcessPoolExecutor(max_workers=self.max_processes) as executor:
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

    def upload_transformed(self, release: CrossrefMetadataRelease, **kwargs) -> None:
        """Upload the transformed data to Cloud Storage."""

        files_list = list_files(release.transform_folder, release.transform_files_regex)
        success = gcs_upload_files(bucket_name=self.cloud_workspace.transform_bucket, file_paths=files_list)
        set_task_state(success, self.upload_transformed.__name__, release)

    def bq_load(self, release: CrossrefMetadataRelease, **kwargs):
        """Task to load each transformed release to BigQuery.
        The table_id is set to the file name without the extension."""

        bq_create_dataset(
            project_id=self.cloud_workspace.output_project_id,
            dataset_id=self.bq_dataset_id,
            location=self.cloud_workspace.data_location,
            description=self.dataset_description,
        )

        # Selects all jsonl.gz files in the releases transform folder on the Google Cloud Storage bucket and all of its
        # subfolders: https://cloud.google.com/bigquery/docs/batch-loading-data#load-wildcards
        uri = gcs_blob_uri(
            self.cloud_workspace.transform_bucket,
            f"{gcs_blob_name_from_path(release.transform_folder)}/*.jsonl",
        )
        table_id = bq_sharded_table_id(
            self.cloud_workspace.output_project_id, self.bq_dataset_id, self.bq_table_name, release.snapshot_date
        )
        schema_file_path = bq_find_schema(
            path=self.schema_folder, table_name=self.bq_table_name, release_date=release.snapshot_date
        )
        success = bq_load_table(
            uri=uri,
            table_id=table_id,
            schema_file_path=schema_file_path,
            source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
            table_description=self.table_description,
            ignore_unknown_values=True,
        )
        set_task_state(success, self.bq_load.__name__, release)

    def add_new_dataset_releases(self, release: CrossrefMetadataRelease, **kwargs) -> None:
        """Adds release information to API."""

        dataset_release = DatasetRelease(
            dag_id=self.dag_id,
            dataset_id=self.api_dataset_id,
            dag_run_id=release.run_id,
            snapshot_date=release.snapshot_date,
            data_interval_start=kwargs["data_interval_start"],
            data_interval_end=kwargs["data_interval_end"],
        )
        api = make_observatory_api(observatory_api_conn_id=self.observatory_api_conn_id)
        api.post_dataset_release(dataset_release)

    def cleanup(self, release: CrossrefMetadataRelease, **kwargs) -> None:
        """Delete all files, folders and XComs associated with this release.

        :param release: the release instance.
        :param kwargs: the context passed from the Airflow Operator.
        See https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return: None.
        """

        cleanup(dag_id=self.dag_id, execution_date=kwargs["execution_date"], workflow_folder=release.workflow_folder)


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
