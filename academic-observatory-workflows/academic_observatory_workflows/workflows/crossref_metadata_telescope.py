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

import json
import logging
import os
from datetime import datetime
from typing import Dict

import jsonlines
import pendulum
import requests
# import __editable___academic_observatory_workflows_1_0_0_finder
from academic_observatory_workflows.config import Tag
from academic_observatory_workflows.config import schema_folder as default_schema_folder
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.providers.cncf.kubernetes.secret import Secret
from bs4 import BeautifulSoup
from google.cloud.bigquery import SourceFormat
from kubernetes.client import models as k8s
from kubernetes.client.models import V1ResourceRequirements

from observatory.platform.airflow import on_failure_callback
from observatory.platform.bigquery import (
    bq_find_schema,
    bq_sharded_table_id,
    bq_load_table,
    bq_create_dataset,
)
from observatory.platform.config import AirflowConns
from observatory.platform.gcs import gcs_blob_name_from_path, gcs_blob_uri
from observatory.platform.observatory_config import CloudWorkspace
from observatory.platform.tasks import gke_create_storage, gke_delete_storage, check_dependencies
from observatory.platform.utils.url_utils import retry_session
from observatory.platform.workflows.workflow import (
    SnapshotRelease,
    set_task_state,
)

SNAPSHOT_URL = "https://api.crossref.org/snapshots/monthly/{year}/{month:02d}/all.json.tar.gz"


def make_snapshot_url(snapshot_date: pendulum.DateTime) -> str:
    return SNAPSHOT_URL.format(year=snapshot_date.year, month=snapshot_date.month)


def get_api_key(crossref_metadata_conn_id: str):
    """Return API token"""
    connection = BaseHook.get_connection(crossref_metadata_conn_id)
    return connection.password


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
        batch_size = dict_["run_id"]
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
            snapshot_date=self.snapshot_date.to_date_string(),
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
    schema_folder: str = os.path.join(default_schema_folder(), "crossref_metadata"),
    dataset_description: str = "The Crossref Metadata Plus dataset: https://www.crossref.org/services/metadata-retrieval/metadata-plus/",
    table_description: str = "The Crossref Metadata Plus dataset: https://www.crossref.org/services/metadata-retrieval/metadata-plus/",
    crossref_metadata_conn_id: str = "crossref_metadata",
    observatory_api_conn_id: str = AirflowConns.OBSERVATORY_API,
    batch_size: int = 20,
    start_date: pendulum.DateTime = pendulum.datetime(2020, 6, 7),
    schedule: str = "0 0 7 * *",
    catchup: bool = False,
    queue: str = "default",
    retries: int = 3,
    max_active_runs: int = 1,
    gke_image="your.private.registry.com/your-image:latest",
    gke_image_pull_secrets="",
    gke_namespace: str = "coki-astro",
    gke_startup_timeout_seconds: int = 300,
    gke_volume_name: str = "crossref_metadata",
    gke_volume_path: str = "/data",
    gke_zone: str = "us-central1-a",
    gke_volume_size: int = 1000,
    kubernetes_conn_id: str = "gke_cluster",
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
    :param batch_size: the number of files to send to ProcessPoolExecutor at one time.
    :param start_date: the start date of the DAG.
    :param schedule: the schedule interval of the DAG.
    :param catchup: whether to catchup the DAG or not.
    :param queue: what Airflow queue this job runs on.
    :param max_active_runs: the maximum number of DAG runs that can be run at once.
    """

    # Common @task.kubernetes params
    kubernetes_task_params = dict(
        image=gke_image,
        image_pull_secrets="myregistrykey",
        do_xcom_push=True,
        get_logs=True,
        in_cluster=False,
        kubernetes_conn_id=kubernetes_conn_id,
        log_events_on_failure=True,
        namespace=gke_namespace,
        startup_timeout_seconds=gke_startup_timeout_seconds,
        volumes=[
            k8s.V1Volume(
                name=gke_volume_name,
                persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name=gke_volume_name),
            )
        ],
        volume_mounts=[k8s.V1VolumeMount(mount_path=gke_volume_path, name=gke_volume_name)],
    )

    @dag(
        dag_id=dag_id,
        start_date=start_date,
        schedule=schedule,
        catchup=catchup,
        max_active_runs=max_active_runs,
        tags=[Tag.academic_observatory],
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
            # The release for a given execution_date is added on the 5th day of the following month.
            # E.g. the 2020-05 release is added to the website on 2020-06-05.
            data_interval_start = context["data_interval_start"]
            exists = check_release_exists(data_interval_start, get_api_key(crossref_metadata_conn_id))
            if not exists:
                raise AirflowException(
                    f"check_release_exists: release doesn't exist for month {data_interval_start.year}-{data_interval_start.month}, something is wrong and needs investigating."
                )

            # The release date is always the end of the execution_date month
            snapshot_date = context["data_interval_start"].end_of("month")
            run_id = context["run_id"]
            return CrossrefMetadataRelease(
                dag_id=dag_id,
                run_id=run_id,
                snapshot_date=snapshot_date,
                cloud_workspace=cloud_workspace,
                batch_size=batch_size,
            ).to_dict()

        @task.kubernetes(
            name="download",
            resources=V1ResourceRequirements(
                requests={"memory": "4Gi", "cpu": "4"}, limits={"memory": "4Gi", "cpu": "4"}
            ),
            secrets=[Secret("env", "CROSSREF_METADATA_API_KEY", "crossref-metadata", "api-key")],
            **kubernetes_task_params,
        )
        def download(release: Dict, **context):
            """Task to download the CrossrefMetadataRelease release for a given month."""

            import functools
            import logging
            import shutil

            from observatory.platform.files import clean_dir
            from observatory.platform.utils.url_utils import retry_get_url
            from academic_observatory_workflows.workflows.crossref_metadata_telescope import CrossrefMetadataRelease

            release = CrossrefMetadataRelease.from_dict(release)
            clean_dir(release.download_folder)

            url = make_snapshot_url(release.snapshot_date)
            logging.info(f"Downloading from url: {url}")

            # Set API token header
            api_key = os.environ.get("CROSSREF_METADATA_API_KEY")
            if api_key is None:
                raise AirflowException(
                    f"download: the CROSSREF_METADATA_API_KEY environment variable is not set, please set it with a Kubernetes Secret."
                )
            header = {"Crossref-Plus-API-Token": f"Bearer {api_key}"}

            # Download release
            with retry_get_url(url, headers=header, stream=True) as response:
                with open(release.download_file_path, "wb") as file:
                    response.raw.read = functools.partial(response.raw.read, decode_content=True)
                    shutil.copyfileobj(response.raw, file)

            logging.info(f"Successfully download url to {release.download_file_path}")

        @task.kubernetes(
            name="upload_downloaded",
            resources=V1ResourceRequirements(
                requests={"memory": "4Gi", "cpu": "4"}, limits={"memory": "4Gi", "cpu": "4"}
            ),
            **kubernetes_task_params,
        )
        def upload_downloaded(release: Dict, **context):
            """Upload data to Cloud Storage."""

            from observatory.platform.gcs import gcs_upload_files
            from observatory.platform.workflows.workflow import (
                set_task_state,
            )
            from academic_observatory_workflows.workflows.crossref_metadata_telescope import CrossrefMetadataRelease

            release = CrossrefMetadataRelease.from_dict(release)
            success = gcs_upload_files(
                bucket_name=release.cloud_workspace.download_bucket, file_paths=[release.download_file_path]
            )
            set_task_state(success, context["ti"].task_id, release)

        @task.kubernetes(
            name="extract",
            resources=V1ResourceRequirements(
                requests={"memory": "30Gi", "cpu": "8"}, limits={"memory": "30Gi", "cpu": "8"}
            ),
            **kubernetes_task_params,
        )
        def extract(release: Dict, **context):
            import subprocess
            from observatory.platform.utils.proc_utils import stream_process
            from academic_observatory_workflows.workflows.crossref_metadata_telescope import CrossrefMetadataRelease

            release = CrossrefMetadataRelease.from_dict(release)
            logging.info(f"Extract {release.download_file_path} to folder: {release.extract_folder}")
            process = subprocess.Popen(
                ["tar", "-xv", "-I", '"pigz -d"', "-f", release.download_file_path, "-C", release.extract_folder],
                shell=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            success = stream_process(process)
            if not success:
                raise AirflowException(f"Error extracting {release.download_file_path}")

        @task.kubernetes(
            name="transform",
            resources=V1ResourceRequirements(
                requests={"memory": "30Gi", "cpu": "8"}, limits={"memory": "30Gi", "cpu": "8"}
            ),
            **kubernetes_task_params,
        )
        def transform(release: Dict, **context):
            """Task to transform the CrossrefMetadataRelease release for a given month.
            Each extracted file is transformed."""

            import logging
            import os
            from concurrent.futures import ProcessPoolExecutor, as_completed
            from natsort import natsorted
            from observatory.platform.files import list_files, get_chunks, clean_dir
            from academic_observatory_workflows.workflows.crossref_metadata_telescope import CrossrefMetadataRelease

            release = CrossrefMetadataRelease.from_dict(release)
            logging.info(f"Transform input folder: {release.extract_folder}, output folder: {release.transform_folder}")
            clean_dir(release.transform_folder)
            finished = 0

            # List files and sort so that they are processed in ascending order
            input_file_paths = natsorted(list_files(release.extract_folder, release.extract_files_regex))

            # Process files in batches so that ProcessPoolExecutor doesn't deplete the system of memory
            for i, chunk in enumerate(get_chunks(input_list=input_file_paths, chunk_size=release.batch_size)):
                with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
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

        @task.kubernetes(
            name="upload_transformed",
            resources=V1ResourceRequirements(
                requests={"memory": "8Gi", "cpu": "8"}, limits={"memory": "8Gi", "cpu": "8"}
            ),
            **kubernetes_task_params,
        )
        def upload_transformed(release: Dict, **context):
            """Upload the transformed data to Cloud Storage."""

            from observatory.platform.files import list_files
            from observatory.platform.gcs import gcs_upload_files
            from observatory.platform.workflows.workflow import (
                set_task_state,
            )
            from academic_observatory_workflows.workflows.crossref_metadata_telescope import CrossrefMetadataRelease

            release = CrossrefMetadataRelease.from_dict(release)
            files_list = list_files(release.transform_folder, release.transform_files_regex)
            success = gcs_upload_files(bucket_name=release.cloud_workspace.transform_bucket, file_paths=files_list)
            set_task_state(success, context["ti"].task_id, release)

        @task
        def bq_load(release: Dict, **context):
            """Task to load each transformed release to BigQuery.
            The table_id is set to the file name without the extension."""

            bq_create_dataset(
                project_id=cloud_workspace.output_project_id,
                dataset_id=bq_dataset_id,
                location=cloud_workspace.data_location,
                description=dataset_description,
            )

            # Selects all jsonl.gz files in the releases transform folder on the Google Cloud Storage bucket and all of its
            # subfolders: https://cloud.google.com/bigquery/docs/batch-loading-data#load-wildcards
            release = CrossrefMetadataRelease.from_dict(release)
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
            set_task_state(success, context["ti"].task_id, release)

        # Define task connections
        task_check = check_dependencies(
            airflow_conns=[observatory_api_conn_id, crossref_metadata_conn_id, kubernetes_conn_id]
        )
        xcom_release = fetch_release()
        task_create_storage = gke_create_storage(
            cloud_workspace.project_id, gke_zone, gke_volume_name, gke_volume_size, kubernetes_conn_id
        )
        task_download = download(xcom_release)
        task_upload_downloaded = upload_downloaded(xcom_release)
        task_extract = extract(xcom_release)
        task_transform = transform(xcom_release)
        task_upload_transformed = upload_transformed(xcom_release)
        task_bq_load = bq_load(xcom_release)
        task_delete_storage = gke_delete_storage(
            cloud_workspace.project_id, gke_zone, gke_volume_name, kubernetes_conn_id
        )

        (
            task_check
            >> xcom_release
            >> task_create_storage
            >> task_download
            >> task_upload_downloaded
            >> task_extract
            >> task_transform
            >> task_upload_transformed
            >> task_bq_load
            >> task_delete_storage
        )


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
