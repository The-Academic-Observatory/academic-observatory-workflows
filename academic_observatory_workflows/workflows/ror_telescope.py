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

# Author: Aniek Roelofs

from __future__ import annotations

import json
import logging
import math
import os
import shutil
from typing import List, Dict, Any
from zipfile import BadZipFile, ZipFile

import pendulum
import requests
from airflow.exceptions import AirflowException
from airflow.models.taskinstance import TaskInstance
from google.cloud.bigquery import SourceFormat

from academic_observatory_workflows.config import schema_folder as default_schema_folder
from academic_observatory_workflows.dag_tag import Tag
from observatory.platform.utils.airflow_utils import AirflowVars
from observatory.platform.utils.file_utils import list_to_jsonl_gz
from observatory.platform.utils.url_utils import (
    retry_session,
)
from observatory.platform.workflows.snapshot_telescope import (
    SnapshotRelease,
    SnapshotTelescope,
)


class RorRelease(SnapshotRelease):
    def __init__(self, dag_id: str, release_date: pendulum.DateTime, url: str):
        """Construct a RorRelease.

        :param release_date: the release date.
        :param url: The url to the ror snapshot
        """

        download_files_regex = f"{dag_id}.zip"
        extract_files_regex = r"^(v\d+.\d+-)?\d{4}-\d{2}-\d{2}-ror-data.json$"
        transform_files_regex = f"{dag_id}.jsonl.gz"

        super().__init__(dag_id, release_date, download_files_regex, extract_files_regex, transform_files_regex)
        self.url = url

    @property
    def download_path(self) -> str:
        """Get the path to the downloaded file.

        :return: the file path.
        """
        return os.path.join(self.download_folder, f"{self.dag_id}.zip")

    @property
    def transform_path(self) -> str:
        """Get the path to the transformed file.

        :return: the file path.
        """
        return os.path.join(self.transform_folder, f"{self.dag_id}.jsonl.gz")


class RorTelescope(SnapshotTelescope):
    """
    The Research Organization Registry (ROR): https://ror.readme.io/

    Saved to the BigQuery table: <project_id>.ror.rorYYYYMMDD
    """

    DAG_ID = "ror"
    DATASET_ID = "ror"
    ROR_DATASET_URL = "https://zenodo.org/api/records/?communities=ror-data&sort=mostrecent"

    def __init__(
        self,
        dag_id: str = DAG_ID,
        start_date: pendulum.DateTime = pendulum.datetime(2021, 9, 1),
        schedule_interval: str = "@weekly",
        dataset_id: str = DATASET_ID,
        schema_folder: str = default_schema_folder(),
        load_bigquery_table_kwargs: Dict = None,
        source_format: str = SourceFormat.NEWLINE_DELIMITED_JSON,
        dataset_description: str = "",
        catchup: bool = True,
        airflow_vars: List = None,
        workflow_id: int = None,
    ):
        """Construct a RorTelescope instance.

        :param dag_id: the id of the DAG.
        :param start_date: the start date of the DAG.
        :param schedule_interval: the schedule interval of the DAG.
        :param dataset_id: the BigQuery dataset id.
        :param schema_folder: the SQL schema path.
        :param load_bigquery_table_kwargs: the customisation parameters for loading data into a BigQuery table.
        :param source_format: the format of the data to load into BigQuery.
        :param dataset_description: description for the BigQuery dataset.
        :param catchup: whether to catchup the DAG or not.
        :param airflow_vars: list of airflow variable keys, for each variable it is checked if it exists in airflow
        :param workflow_id: api workflow id.
        """

        if airflow_vars is None:
            airflow_vars = [
                AirflowVars.DATA_PATH,
                AirflowVars.PROJECT_ID,
                AirflowVars.DATA_LOCATION,
                AirflowVars.DOWNLOAD_BUCKET,
                AirflowVars.TRANSFORM_BUCKET,
            ]

        if load_bigquery_table_kwargs is None:
            load_bigquery_table_kwargs = {"ignore_unknown_values": True}

        super().__init__(
            dag_id,
            start_date,
            schedule_interval,
            dataset_id,
            schema_folder,
            source_format=source_format,
            load_bigquery_table_kwargs=load_bigquery_table_kwargs,
            dataset_description=dataset_description,
            catchup=catchup,
            airflow_vars=airflow_vars,
            workflow_id=workflow_id,
            tags=[Tag.academic_observatory],
        )

        self.add_setup_task_chain([self.check_dependencies, self.list_releases])
        self.add_task_chain(
            [
                self.download,
                self.upload_downloaded,
                self.extract,
                self.transform,
                self.upload_transformed,
                self.bq_load,
                self.cleanup,
                self.add_new_dataset_releases,
            ]
        )

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
            release_date = record["release_date"]
            url = record["url"]

            releases.append(RorRelease(self.dag_id, pendulum.parse(release_date), url))
        return releases

    def list_releases(self, **kwargs):
        """Lists all ROR records for a given month and publishes their url and release_date as an XCom.

        :param kwargs: the context passed from the BranchPythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: the identifier of the task to execute next.
        """

        execution_date = kwargs["execution_date"]
        next_execution_date = kwargs["next_execution_date"]

        records = list_ror_records(execution_date, next_execution_date)

        continue_dag = len(records)
        if continue_dag:
            # Push messages
            ti: TaskInstance = kwargs["ti"]
            ti.xcom_push(RorTelescope.RELEASE_INFO, records, execution_date)
        else:
            logging.info(f"Found no available records.")

        return continue_dag

    def download(self, releases: List[RorRelease], **kwargs):
        """Task to download the ROR releases for a given month.

        :param releases: a list of ROR releases.
        :return: None.
        """
        for release in releases:
            with requests.get(release.url, stream=True) as r:
                with open(release.download_path, "wb") as f:
                    shutil.copyfileobj(r.raw, f)
            logging.info(f"Downloaded file from {release.url} to: {release.download_path}")

    def extract(self, releases: List[RorRelease], **kwargs):
        """Task to extract the ROR releases for a given month.

        :param releases: a list of ROR releases.
        :return: None.
        """
        for release in releases:
            logging.info(f"Extracting file: {release.download_path}")
            try:
                with ZipFile(release.download_path) as zip_file:
                    zip_file.extractall(release.extract_folder)
            except BadZipFile:
                raise AirflowException("Not a zip file")
            logging.info(f"File extracted to: {release.extract_folder}")

    def transform(self, releases: List[RorRelease], **kwargs):
        """Task to transform the ROR releases for a given month.

        :param releases: a list of ROR releases.
        :return: None.
        """
        for release in releases:
            extract_files = release.extract_files

            # Check there is only one JSON file
            if len(extract_files) == 1:
                release_json_file = extract_files[0]
                logging.info(f"Transforming file: {release_json_file}")
            else:
                raise AirflowException(f"{len(extract_files)} extracted files found: {extract_files}")

            with open(release_json_file, "r") as f:
                records = json.load(f)
                records = transform_ror(records)
            list_to_jsonl_gz(release.transform_path, records)


def list_ror_records(start_date: pendulum.DateTime, end_date: pendulum.DateTime, timeout: float = 30.0) -> List[dict]:
    """List all ROR records available on Zenodo between two dates.

    :param start_date: Start date of period to look into
    :param end_date: End date of period to look into
    :param timeout: the number of seconds to wait until timing out.
    :return: the list of ROR records with required variables stored as a dictionary.
    """
    logging.info(f"Getting info on available ROR records from Zenodo, from url: {RorTelescope.ROR_DATASET_URL}")
    response = retry_session().get(RorTelescope.ROR_DATASET_URL, timeout=timeout, headers={"Accept-encoding": "gzip"})
    if response.status_code != 200:
        raise AirflowException(
            f"Request to get available records on Zenodo unsuccessful, url: {RorTelescope.ROR_DATASET_URL}, "
            f"status code: {response.status_code}, response: {response.text}, reason: {response.reason}"
        )
    response_json = json.loads(response.text)

    # Get release date and url of records that are created between two dates
    records: List[dict] = []
    hits = response_json.get("hits", {}).get("hits", [])
    logging.info(f"Looking for records between dates {start_date} and {end_date}")
    for hit in hits:
        release_date: pendulum.DateTime = pendulum.parse(hit["created"])
        if start_date <= release_date < end_date:
            link = hit["files"][0]["links"]["self"]
            records.append({"release_date": release_date.format("YYYYMMDD"), "url": link})
            logging.info(f"Found record created on '{release_date}', url: {link}")

        if release_date < start_date:
            break

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
