# Copyright 2021 Curtin University
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
from typing import List, Any, Dict
from zipfile import BadZipFile, ZipFile

import pendulum
from airflow.exceptions import AirflowException
from airflow.models.taskinstance import TaskInstance
from google.cloud.bigquery import SourceFormat

from academic_observatory_workflows.config import schema_folder as default_schema_folder, Tag
from observatory.api.client.model.dataset_release import DatasetRelease
from observatory.platform.api import make_observatory_api
from observatory.platform.bigquery import (
    bq_find_schema,
    bq_load_table,
    bq_sharded_table_id,
    bq_create_dataset,
)
from observatory.platform.config import AirflowConns
from observatory.platform.files import list_files, save_jsonl_gz, clean_dir
from observatory.platform.gcs import gcs_blob_name_from_path, gcs_upload_files, gcs_blob_uri
from observatory.platform.observatory_config import CloudWorkspace
from observatory.platform.utils.http_download import download_file
from observatory.platform.utils.url_utils import retry_get_url
from observatory.platform.workflows.workflow import Workflow, SnapshotRelease, cleanup, set_task_state


class RorRelease(SnapshotRelease):
    def __init__(self, *, dag_id: str, run_id: str, snapshot_date: pendulum.DateTime, url: str, checksum: str):
        """Construct a RorRelease.

        :param dag_id: the DAG id.
        :param run_id: the DAG id.
        :param snapshot_date: the release date.
        :param url: The url to the ror snapshot.
        :param checksum: the file checksum.
        """

        super().__init__(dag_id=dag_id, run_id=run_id, snapshot_date=snapshot_date)
        self.url = url
        self.checksum = checksum
        self.download_file_name = "ror.zip"
        self.extract_file_regex = r".*\.json$"
        self.transform_file_name = "ror.jsonl.gz"
        self.download_file_path = os.path.join(self.download_folder, self.download_file_name)
        self.transform_file_path = os.path.join(self.transform_folder, self.transform_file_name)


class RorTelescope(Workflow):
    """
    The Research Organization Registry (ROR): https://ror.readme.io/

    Saved to the BigQuery table: <project_id>.ror.rorYYYYMMDD
    """

    def __init__(
        self,
        *,
        dag_id: str,
        cloud_workspace: CloudWorkspace,
        bq_dataset_id: str = "ror",
        bq_table_name: str = "ror",
        api_dataset_id: str = "ror",
        schema_folder: str = os.path.join(default_schema_folder(), "ror"),
        dataset_description: str = "The Research Organization Registry (ROR) database: https://ror.org/",
        table_description: str = "The Research Organization Registry (ROR) database: https://ror.org/",
        observatory_api_conn_id: str = AirflowConns.OBSERVATORY_API,
        ror_conceptrecid: int = 6347574,
        start_date: pendulum.DateTime = pendulum.datetime(2021, 9, 1),
        schedule: str = "@weekly",
        catchup: bool = True,
    ):
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
        """

        super().__init__(
            dag_id=dag_id,
            start_date=start_date,
            schedule=schedule,
            catchup=catchup,
            airflow_conns=[observatory_api_conn_id],
            tags=[Tag.academic_observatory],
        )

        self.cloud_workspace = cloud_workspace
        self.bq_dataset_id = bq_dataset_id
        self.bq_table_name = bq_table_name
        self.api_dataset_id = api_dataset_id
        self.schema_folder = schema_folder
        self.dataset_description = dataset_description
        self.table_description = table_description
        self.ror_conceptrecid = ror_conceptrecid
        self.observatory_api_conn_id = observatory_api_conn_id

        self.add_setup_task(self.check_dependencies)
        self.add_setup_task(self.list_releases)
        self.add_task(self.download)
        self.add_task(self.upload_downloaded)
        self.add_task(self.extract)
        self.add_task(self.transform)
        self.add_task(self.upload_transformed)
        self.add_task(self.bq_load)
        self.add_task(self.add_new_dataset_releases)
        self.add_task(self.cleanup)

    def make_release(self, **kwargs) -> List[RorRelease]:
        """Make release instances. The release is passed as an argument to the function (TelescopeFunction) that is
        called in 'task_callable'.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are
        passed to this argument.
        :return: A list of ROR release instances
        """

        ti: TaskInstance = kwargs["ti"]
        records = ti.xcom_pull(
            key=RorTelescope.RELEASE_INFO, task_ids=self.list_releases.__name__, include_prior_dates=False
        )
        releases = []
        for record in records:
            snapshot_date = record["snapshot_date"]
            url = record["url"]
            checksum = record["checksum"]
            releases.append(
                RorRelease(
                    dag_id=self.dag_id,
                    run_id=kwargs["run_id"],
                    snapshot_date=pendulum.parse(snapshot_date),
                    url=url,
                    checksum=checksum,
                )
            )

        return releases

    def list_releases(self, **kwargs):
        """Lists all ROR records and publishes their url, snapshot_date and checksum as an XCom."""

        start_date = kwargs["data_interval_start"]
        end_date = kwargs["data_interval_end"]
        records = list_ror_records(self.ror_conceptrecid, start_date, end_date)

        continue_dag = len(records)
        if continue_dag:
            # Push messages
            ti: TaskInstance = kwargs["ti"]
            ti.xcom_push(RorTelescope.RELEASE_INFO, records, kwargs["logical_date"])
        else:
            logging.info(f"Found no available records.")

        return continue_dag

    def download(self, releases: List[RorRelease], **kwargs):
        """Task to download the ROR releases."""

        for release in releases:
            clean_dir(release.download_folder)
            hash_algorithm, hash_checksum = release.checksum.split(":")
            success, info = download_file(
                url=release.url, filename=release.download_file_path, hash=hash_checksum, hash_algorithm=hash_algorithm
            )
            set_task_state(success, self.download.__name__, release)
            logging.info(f"Downloaded file from {release.url} to: {release.download_file_path}")

    def upload_downloaded(self, releases: List[RorRelease], **kwargs):
        """Upload the Geonames data to Cloud Storage."""

        for release in releases:
            success = gcs_upload_files(
                bucket_name=self.cloud_workspace.download_bucket,
                file_paths=[release.download_file_path],
            )
            set_task_state(success, self.upload_downloaded.__name__, release)

    def extract(self, releases: List[RorRelease], **kwargs):
        """Task to extract the ROR releases."""

        for release in releases:
            logging.info(f"Extracting file: {release.download_file_path}")
            clean_dir(release.extract_folder)
            try:
                with ZipFile(release.download_file_path) as zip_file:
                    zip_file.extractall(release.extract_folder)
            except BadZipFile:
                raise AirflowException("Not a zip file")
            logging.info(f"File extracted to: {release.extract_folder}")

            # Remove dud __MACOSX folder that shouldn't be there
            try:
                shutil.rmtree(os.path.join(release.extract_folder, "__MACOSX"))
            except FileNotFoundError:
                pass

    def transform(self, releases: List[RorRelease], **kwargs):
        """Task to transform the ROR releases."""

        for release in releases:
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

    def upload_transformed(self, releases: List[RorRelease], **kwargs) -> None:
        """Upload the transformed data to Cloud Storage."""

        for release in releases:
            success = gcs_upload_files(
                bucket_name=self.cloud_workspace.transform_bucket,
                file_paths=[release.transform_file_path],
            )
            set_task_state(success, self.upload_transformed.__name__, release)

    def bq_load(self, releases: List[RorRelease], **kwargs) -> None:
        """Load the data into BigQuery."""

        bq_create_dataset(
            project_id=self.cloud_workspace.output_project_id,
            dataset_id=self.bq_dataset_id,
            location=self.cloud_workspace.data_location,
            description=self.dataset_description,
        )

        for release in releases:
            uri = gcs_blob_uri(
                self.cloud_workspace.transform_bucket, gcs_blob_name_from_path(release.transform_file_path)
            )
            schema_file_path = bq_find_schema(
                path=self.schema_folder, table_name=self.bq_table_name, release_date=release.snapshot_date
            )
            table_id = bq_sharded_table_id(
                self.cloud_workspace.output_project_id, self.bq_dataset_id, self.bq_table_name, release.snapshot_date
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

    def add_new_dataset_releases(self, releases: List[RorRelease], **kwargs) -> None:
        """Adds release information to API."""

        for release in releases:
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

    def cleanup(self, releases: List[RorRelease], **kwargs) -> None:
        """Delete all files, folders and XComs associated with this release."""

        for release in releases:
            cleanup(
                dag_id=self.dag_id, execution_date=kwargs["execution_date"], workflow_folder=release.workflow_folder
            )


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
