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

import gzip
import logging
import os
import shutil
from zipfile import ZipFile

import pendulum
import requests
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
from observatory.platform.files import clean_dir
from observatory.platform.gcs import gcs_blob_name_from_path, gcs_upload_files, gcs_blob_uri
from observatory.platform.observatory_config import CloudWorkspace
from observatory.platform.utils.http_download import download_file
from observatory.platform.workflows.workflow import Workflow, SnapshotRelease, cleanup, set_task_state

DOWNLOAD_URL = "https://download.geonames.org/export/dump/allCountries.zip"


class GeonamesRelease(SnapshotRelease):
    def __init__(
        self,
        *,
        dag_id: str,
        run_id: str,
        snapshot_date: pendulum.DateTime,
    ):
        """Construct a GeonamesRelease.

        :param dag_id: the DAG id.
        :param run_id: the DAG run id.
        :param snapshot_date: the release date.
        """

        super().__init__(dag_id=dag_id, run_id=run_id, snapshot_date=snapshot_date)
        self.download_file_name = "geonames.zip"
        self.extract_file_name = "allCountries.txt"
        self.transform_file_name = "geonames.csv.gz"
        self.download_file_path = os.path.join(self.download_folder, self.download_file_name)
        self.extract_file_path = os.path.join(self.extract_folder, self.extract_file_name)
        self.transform_file_path = os.path.join(self.transform_folder, self.transform_file_name)


def fetch_snapshot_date() -> pendulum.DateTime:
    """Fetch the Geonames release date.

    :return: the release date.
    """

    response = requests.head(DOWNLOAD_URL)
    date_str = response.headers["Last-Modified"]
    date: pendulum.DateTime = pendulum.from_format(date_str, "ddd, DD MMM YYYY HH:mm:ss z")
    return date


class GeonamesTelescope(Workflow):
    """
    A Telescope that harvests the GeoNames geographical database: https://www.geonames.org/

    Saved to the BigQuery table: <project_id>.geonames.geonamesYYYYMMDD
    """

    def __init__(
        self,
        *,
        dag_id: str,
        cloud_workspace: CloudWorkspace,
        bq_dataset_id: str = "geonames",
        bq_table_name: str = "geonames",
        api_dataset_id: str = "geonames",
        schema_folder: str = os.path.join(default_schema_folder(), "geonames"),
        dataset_description: str = "The GeoNames geographical database: https://www.geonames.org/",
        table_description: str = "The GeoNames geographical database: https://www.geonames.org/",
        observatory_api_conn_id: str = AirflowConns.OBSERVATORY_API,
        start_date: pendulum.DateTime = pendulum.datetime(2020, 9, 1),
        schedule_interval: str = "@monthly",
    ):
        """The Geonames telescope.

        :param dag_id: the id of the DAG.
        :param cloud_workspace: the cloud workspace settings.
        :param bq_dataset_id: the BigQuery dataset id.
        :param bq_table_name: the BigQuery table name.
        :param api_dataset_id: the Dataset ID to use when storing releases.
        :param schema_folder: the SQL schema path.
        :param dataset_description: description for the BigQuery dataset.
        :param table_description: description for the BigQuery table.
        :param observatory_api_conn_id: the Observatory API connection key.
        :param start_date: the start date of the DAG.
        :param schedule_interval: the schedule interval of the DAG.
        """

        super().__init__(
            dag_id=dag_id,
            start_date=start_date,
            schedule_interval=schedule_interval,
            catchup=False,
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
        self.observatory_api_conn_id = observatory_api_conn_id

        self.add_setup_task(self.check_dependencies)
        self.add_setup_task(self.fetch_snapshot_date)
        self.add_task(self.download)
        self.add_task(self.upload_downloaded)
        self.add_task(self.extract)
        self.add_task(self.transform)
        self.add_task(self.upload_transformed)
        self.add_task(self.bq_load)
        self.add_task(self.add_new_dataset_releases)
        self.add_task(self.cleanup)

    def fetch_snapshot_date(self, **kwargs):
        """Get the Geonames release for a given month and publishes the snapshot_date as an XCom."""

        snapshot_date = fetch_snapshot_date()
        ti: TaskInstance = kwargs["ti"]
        execution_date = kwargs["execution_date"]
        ti.xcom_push(GeonamesTelescope.RELEASE_INFO, snapshot_date.format("YYYYMMDD"), execution_date)

        return True

    def make_release(self, **kwargs) -> GeonamesRelease:
        """Creates a new GeonamesRelease instance

        :param kwargs: the context passed from the BranchPythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: GeonamesRelease
        """

        ti: TaskInstance = kwargs["ti"]
        snapshot_date = ti.xcom_pull(
            key=GeonamesTelescope.RELEASE_INFO, task_ids=self.fetch_snapshot_date.__name__, include_prior_dates=False
        )
        snapshot_date = pendulum.parse(snapshot_date)
        return GeonamesRelease(dag_id=self.dag_id, run_id=kwargs["run_id"], snapshot_date=snapshot_date)

    def download(self, release: GeonamesRelease, **kwargs):
        """Downloads geonames dump file containing country data. The file is in zip format and will be extracted
        after downloading, saving the unzipped content."""

        clean_dir(release.download_folder)
        download_file(url=DOWNLOAD_URL, filename=release.download_file_path)
        logging.info(f"Downloaded file: {release.download_file_path}")

    def upload_downloaded(self, release: GeonamesRelease, **kwargs):
        """Upload the Geonames data to Cloud Storage."""

        success = gcs_upload_files(
            bucket_name=self.cloud_workspace.download_bucket, file_paths=[release.download_file_path]
        )
        set_task_state(success, self.upload_downloaded.__name__, release)

    def extract(self, release: GeonamesRelease, **kwargs):
        """Task to extract the Release release for a given month."""

        clean_dir(release.extract_folder)
        with ZipFile(release.download_file_path) as zip_file:
            zip_file.extractall(release.extract_folder)

    def transform(self, release: GeonamesRelease, **kwargs):
        """Transforms release by storing file content in gzipped csv format."""

        clean_dir(release.transform_folder)
        with open(release.extract_file_path, "rb") as file_in:
            with gzip.open(release.transform_file_path, "wb") as file_out:
                shutil.copyfileobj(file_in, file_out)

    def upload_transformed(self, release: GeonamesRelease, **kwargs) -> None:
        """Upload the transformed data to Cloud Storage."""

        success = gcs_upload_files(
            bucket_name=self.cloud_workspace.transform_bucket, file_paths=[release.transform_file_path]
        )
        set_task_state(success, self.upload_downloaded.__name__, release)

    def bq_load(self, release: GeonamesRelease, **kwargs) -> None:
        """Load the data into BigQuery."""

        bq_create_dataset(
            project_id=self.cloud_workspace.output_project_id,
            dataset_id=self.bq_dataset_id,
            location=self.cloud_workspace.data_location,
            description=self.dataset_description,
        )

        uri = gcs_blob_uri(self.cloud_workspace.transform_bucket, gcs_blob_name_from_path(release.transform_file_path))
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
            source_format=SourceFormat.CSV,
            table_description=self.table_description,
            csv_field_delimiter="\t",
            csv_quote_character="",
            ignore_unknown_values=True,
        )
        set_task_state(success, self.bq_load.__name__, release)

    def add_new_dataset_releases(self, release: GeonamesRelease, **kwargs) -> None:
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

    def cleanup(self, release: GeonamesRelease, **kwargs) -> None:
        """Delete all files, folders and XComs associated with this release."""

        cleanup(dag_id=self.dag_id, execution_date=kwargs["execution_date"], workflow_folder=release.workflow_folder)
