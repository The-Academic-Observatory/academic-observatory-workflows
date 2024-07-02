# Copyright 2021-2024 Curtin University
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
import math
import os
import shutil
import urllib.parse
from typing import Any, Dict, List
from zipfile import BadZipFile, ZipFile

import pendulum
from airflow import DAG
from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowException
from google.cloud.bigquery import SourceFormat

from academic_observatory_workflows.config import project_path
from observatory.api.client.model.dataset_release import DatasetRelease
from observatory_platform.airflow import on_failure_callback
from observatory_platform.dataset_api import make_observatory_api
from observatory_platform.google.bigquery import bq_create_dataset, bq_find_schema, bq_load_table, bq_sharded_table_id
from observatory_platform.config import AirflowConns
from observatory_platform.files import clean_dir, list_files, save_jsonl_gz
from observatory_platform.google.gcs import gcs_blob_name_from_path, gcs_blob_uri, gcs_download_blob, gcs_upload_files
from observatory_platform.airflow.workflow import CloudWorkspace
from observatory_platform.refactor.tasks import check_dependencies
from observatory_platform.utils.http_download import download_file
from observatory_platform.url_utils import retry_get_url
from observatory_platform.workflows.workflow import cleanup, set_task_state, SnapshotRelease


class RorRelease(SnapshotRelease):
    def __init__(
        self,
        *,
        dag_id: str,
        run_id: str,
        snapshot_date: pendulum.DateTime,
        url: str,
        checksum: str,
        cloud_workspace: CloudWorkspace,
    ):
        """Construct a RorRelease.

        :param dag_id: the DAG id.
        :param run_id: the DAG id.
        :param snapshot_date: the release date.
        :param url: The url to the ror snapshot.
        :param checksum: the file checksum.
        :param cloud_workspace: the cloud workspace settings.
        """

        super().__init__(dag_id=dag_id, run_id=run_id, snapshot_date=snapshot_date)
        self.url = url
        self.checksum = checksum
        self.download_file_name = "ror.zip"
        self.extract_file_regex = r"^\S+-ror-data\.json$"
        self.transform_file_name = "ror.jsonl.gz"
        self.download_file_path = os.path.join(self.download_folder, self.download_file_name)
        self.transform_file_path = os.path.join(self.transform_folder, self.transform_file_name)
        self.cloud_workspace = cloud_workspace

    @property
    def download_blob_name(self):
        return gcs_blob_name_from_path(self.download_file_path)

    @property
    def transform_blob_name(self):
        return gcs_blob_name_from_path(self.transform_file_path)

    @property
    def download_uri(self):
        return gcs_blob_uri(self.cloud_workspace.download_bucket, self.download_blob_name)

    @property
    def transform_uri(self):
        return gcs_blob_uri(self.cloud_workspace.transform_bucket, self.transform_blob_name)

    def to_dict(self) -> Dict:
        return dict(
            dag_id=self.dag_id,
            run_id=self.run_id,
            snapshot_date=self.snapshot_date.to_date_string(),
            url=self.url,
            checksum=self.checksum,
            cloud_workspace=self.cloud_workspace.to_dict(),
        )

    @staticmethod
    def from_dict(dict_: Dict) -> RorRelease:
        dag_id = dict_["dag_id"]
        snapshot_date = dict_["snapshot_date"]
        url = dict_["url"]
        checksum = dict_["checksum"]
        cloud_workspace = dict_["cloud_workspace"]

        return RorRelease(
            dag_id=dag_id,
            run_id=dict_["run_id"],
            snapshot_date=pendulum.parse(snapshot_date),
            url=url,
            checksum=checksum,
            cloud_workspace=CloudWorkspace.from_dict(cloud_workspace),
        )


def create_dag(
    *,
    dag_id: str,
    cloud_workspace: CloudWorkspace,
    bq_dataset_id: str = "ror",
    bq_table_name: str = "ror",
    api_dataset_id: str = "ror",
    schema_folder: str = project_path("ror_telescope", "schema"),
    dataset_description: str = "The Research Organization Registry (ROR) database: https://ror.org/",
    table_description: str = "The Research Organization Registry (ROR) database: https://ror.org/",
    observatory_api_conn_id: str = AirflowConns.OBSERVATORY_API,
    ror_conceptrecid: int = 6347574,
    start_date: pendulum.DateTime = pendulum.datetime(2021, 9, 1),
    schedule: str = "@weekly",
    catchup: bool = True,
    max_active_runs: int = 1,
    retries: int = 3,
) -> DAG:
    """Construct a RorTelescope instance.

    :param dag_id: the id of the DAG.
    :param cloud_workspace: the cloud workspace settings.
    :param bq_dataset_id: the BigQuery dataset id.
    :param bq_table_name: the BigQuery table name.
    :param api_dataset_id: the Dataset ID to use when storing releases.
    :param schema_folder: the SQL schema path.
    :param dataset_description: description for the BigQuery dataset.
    :param table_description: description for the BigQuery table.
    :param observatory_api_conn_id: the Observatory API connection key.
    :param ror_conceptrecid: the Zenodo conceptrecid for the ROR dataset.
    :param start_date: the start date of the DAG.
    :param schedule: the schedule interval of the DAG.
    :param catchup: whether to catchup the DAG or not.
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
        },
    )
    def ror():
        @task
        def fetch_releases(**context) -> List[dict]:
            """Lists all ROR records and publishes their url, snapshot_date and checksum as an XCom."""

            data_interval_start = context["data_interval_start"]
            data_interval_end = context["data_interval_end"]
            records = list_ror_records(ror_conceptrecid, data_interval_start, data_interval_end)
            releases = []
            for record in records:
                releases.append(
                    dict(
                        dag_id=dag_id,
                        run_id=context["run_id"],
                        snapshot_date=record["snapshot_date"],
                        url=record["url"],
                        checksum=record["checksum"],
                        cloud_workspace=cloud_workspace.to_dict(),
                    )
                )

            return releases

        @task_group(group_id="process_release")
        def process_release(data):
            @task
            def download(release: dict, **context):
                """Task to download the ROR releases."""

                # Download file from Zenodo
                release = RorRelease.from_dict(release)
                clean_dir(release.download_folder)
                hash_algorithm, hash_checksum = release.checksum.split(":")
                success, info = download_file(
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
                    bucket_name=cloud_workspace.download_bucket,
                    file_paths=[release.download_file_path],
                )
                if not success:
                    raise AirflowException(
                        f"Error uploading file {release.download_file_path} to bucket: {release.download_uri}"
                    )

            @task
            def transform(release: dict, **context):
                """Task to transform the ROR releases."""

                release = RorRelease.from_dict(release)

                # Download file
                success = gcs_download_blob(
                    bucket_name=cloud_workspace.download_bucket,
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
                logging.info(f"Uploading file to bucket: {cloud_workspace.transform_bucket}")
                success = gcs_upload_files(
                    bucket_name=cloud_workspace.transform_bucket,
                    file_paths=[release.transform_file_path],
                )
                if not success:
                    raise AirflowException(
                        f"Error uploading file {release.transform_file_path} to bucket: {release.transform_uri}"
                    )

            @task
            def bq_load(release: dict, **context) -> None:
                """Load the data into BigQuery."""

                release = RorRelease.from_dict(release)
                bq_create_dataset(
                    project_id=cloud_workspace.output_project_id,
                    dataset_id=bq_dataset_id,
                    location=cloud_workspace.data_location,
                    description=dataset_description,
                )
                schema_file_path = bq_find_schema(
                    path=schema_folder, table_name=bq_table_name, release_date=release.snapshot_date
                )
                table_id = bq_sharded_table_id(
                    cloud_workspace.output_project_id, bq_dataset_id, bq_table_name, release.snapshot_date
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

            @task
            def add_dataset_releases(release: dict, **context) -> None:
                """Adds release information to API."""

                release = RorRelease.from_dict(release)
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

                release = RorRelease.from_dict(release)
                cleanup(dag_id=dag_id, execution_date=context["logical_date"], workflow_folder=release.workflow_folder)

            (download(data) >> transform(data) >> bq_load(data) >> add_dataset_releases(data) >> cleanup_workflow(data))

        check_task = check_dependencies(airflow_conns=[observatory_api_conn_id])
        xcom_releases = fetch_releases()
        process_release_task_group = process_release.expand(data=xcom_releases)

        (check_task >> xcom_releases >> process_release_task_group)

    return ror()


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
    :return:
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
