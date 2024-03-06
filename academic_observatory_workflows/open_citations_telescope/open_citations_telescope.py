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

# Author: James Diprose, Tuan Chien

import os.path
import zipfile
from typing import Dict, List

import pendulum
from airflow.models.taskinstance import TaskInstance
from google.cloud.bigquery import SourceFormat
from observatory.api.client.model.dataset_release import DatasetRelease
from observatory.platform.api import make_observatory_api
from observatory.platform.bigquery import (
    bq_create_dataset,
    bq_find_schema,
    bq_load_table,
    bq_sharded_table_id,
    bq_table_exists,
)
from observatory.platform.config import AirflowConns
from observatory.platform.files import clean_dir, list_files
from observatory.platform.gcs import gcs_blob_name_from_path, gcs_blob_uri, gcs_upload_files
from observatory.platform.observatory_config import CloudWorkspace
from observatory.platform.utils.http_download import download_files, DownloadInfo
from observatory.platform.utils.url_utils import get_http_response_json, get_observatory_http_header
from observatory.platform.workflows.workflow import cleanup, set_task_state, SnapshotRelease, Workflow

from academic_observatory_workflows.config import project_path, Tag

VERSION_URL = "https://api.figshare.com/v2/articles/6741422/versions"


class OpenCitationsRelease(SnapshotRelease):
    def __init__(self, *, dag_id: str, run_id: str, snapshot_date: pendulum.DateTime, files: List[DownloadInfo]):
        """Construct a RorRelease.

        :param dag_id: the DAG id.
        :param dag_id: the DAG run id.
        :param snapshot_date: the release date.
        :param files: list of files to download.
        """

        super().__init__(dag_id=dag_id, run_id=run_id, snapshot_date=snapshot_date)
        self.files = files
        self.download_file_regex = r".*\.csv\.zip$"
        self.transform_file_regex = r".*\.csv$"


class OpenCitationsTelescope(Workflow):
    """A telescope that harvests the Open Citations COCI CSV dataset . http://opencitations.net/index/coci"""

    def __init__(
        self,
        *,
        dag_id: str,
        cloud_workspace: CloudWorkspace,
        bq_dataset_id: str = "open_citations",
        bq_table_name: str = "open_citations",
        api_dataset_id: str = "open_citations",
        schema_folder: str = project_path("open_citations_telescope", "schema"),
        dataset_description: str = "The OpenCitations Indexes: http://opencitations.net/",
        table_description: str = "The OpenCitations COCI CSV table: http://opencitations.net/",
        observatory_api_conn_id: str = AirflowConns.OBSERVATORY_API,
        start_date: pendulum.DateTime = pendulum.datetime(2018, 7, 1),
        schedule: str = "@weekly",
        catchup: bool = True,
        queue: str = "remote_queue",
    ):
        """Construct a OpenCitationsTelescope instance.

        :param dag_id: the id of the DAG.
        :param cloud_workspace: the cloud workspace settings.
        :param bq_dataset_id: the BigQuery dataset id.
        :param bq_table_name: the BigQuery table name.
        :param api_dataset_id: the Dataset ID to use when storing releases.
        :param schema_folder: the SQL schema path.
        :param dataset_description: description for the BigQuery dataset.
        :param table_description: description for the BigQuery table.
        :param observatory_api_conn_id: the Observatory API connection key.
        :param catchup: whether to catchup the DAG or not.
        :param start_date: the start date of the DAG.
        :param schedule: the schedule interval of the DAG.
        :param queue: what Airflow queue to use.
        """

        super().__init__(
            dag_id=dag_id,
            start_date=start_date,
            schedule=schedule,
            catchup=catchup,
            airflow_conns=[observatory_api_conn_id],
            queue=queue,
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
        self.add_setup_task(self.get_release_info)
        self.add_task(self.download)
        self.add_task(self.upload_downloaded)
        self.add_task(self.extract)
        self.add_task(self.upload_transformed)
        self.add_task(self.bq_load)
        self.add_task(self.add_new_dataset_releases)
        self.add_task(self.cleanup)

    def process_release(self, release: Dict[str, str]) -> bool:
        """Indicates whether we should process this release. If there are no files, or if the BigQuery table exists, we will not process this release.

        :param release: the release instance.
        :return: Whether to process the release.
        """

        if len(release["files"]) == 0:
            return False

        table_id = bq_sharded_table_id(
            self.cloud_workspace.output_project_id,
            self.bq_dataset_id,
            self.bq_table_name,
            pendulum.parse(release["date"]),
        )
        if bq_table_exists(table_id):
            return False

        return True

    def get_release_info(self, **kwargs):
        """Calculate which releases require processing, and push the info to an XCom."""

        start_date = kwargs["data_interval_start"]
        end_date = kwargs["data_interval_end"]
        releases = list_releases(start_date, end_date)
        filtered_releases = list(filter(self.process_release, releases))

        continue_dag = len(filtered_releases) > 0
        if continue_dag:
            ti = kwargs["ti"]
            ti.xcom_push(OpenCitationsTelescope.RELEASE_INFO, filtered_releases, start_date)
        return continue_dag

    def make_release(self, **kwargs) -> List[OpenCitationsRelease]:
        """Make release instances. The release is passed as an argument to the function (TelescopeFunction) that is
        called in 'task_callable'.

        :param kwargs: the context passed from the BranchPythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: list of OpenCitationsRelease instances.
        """

        ti: TaskInstance = kwargs["ti"]
        release_info = ti.xcom_pull(
            key=OpenCitationsTelescope.RELEASE_INFO, task_ids=self.get_release_info.__name__, include_prior_dates=False
        )

        releases = []
        for data in release_info:
            files = []
            for file in data["files"]:
                info = DownloadInfo(
                    url=file["download_url"], filename=file["name"], hash=file["computed_md5"], hash_algorithm="md5"
                )
                files.append(info)

            release = OpenCitationsRelease(
                dag_id=self.dag_id, run_id=kwargs["run_id"], snapshot_date=pendulum.parse(data["date"]), files=files
            )
            releases.append(release)

        return releases

    def download(self, releases: List[OpenCitationsRelease], **kwargs):
        """Task to download the data."""

        for release in releases:
            clean_dir(release.download_folder)
            headers = get_observatory_http_header(package_name="academic_observatory_workflows")
            success = download_files(download_list=release.files, headers=headers, prefix_dir=release.download_folder)
            set_task_state(success, self.download.__name__, release)

    def upload_downloaded(self, releases: List[OpenCitationsRelease], **kwargs):
        """Upload the data to Cloud Storage."""

        for release in releases:
            # List all files in download folder and upload them
            files_list = list_files(release.download_folder)
            success = gcs_upload_files(bucket_name=self.cloud_workspace.download_bucket, file_paths=files_list)
            set_task_state(success, self.upload_transformed.__name__, release)

    def extract(self, releases: List[OpenCitationsRelease], **kwargs):
        """Task to extract the data."""

        for release in releases:
            clean_dir(release.transform_folder)

            # List all files in download folder and extract them into transform folder
            files_list = list_files(release.download_folder, release.download_file_regex)
            for file in files_list:
                with zipfile.ZipFile(file, "r") as zf:
                    zf.extractall(release.transform_folder)

    def upload_transformed(self, releases: List[OpenCitationsRelease], **kwargs) -> None:
        """Upload the transformed data to Cloud Storage."""

        for release in releases:
            # List all extracted files in transform folder and upload them to transform bucket
            files_list = list_files(release.transform_folder, release.transform_file_regex)
            success = gcs_upload_files(bucket_name=self.cloud_workspace.transform_bucket, file_paths=files_list)
            set_task_state(success, self.upload_transformed.__name__, release)

    def bq_load(self, releases: List[OpenCitationsRelease], **kwargs) -> None:
        """Load the data into BigQuery."""

        bq_create_dataset(
            project_id=self.cloud_workspace.output_project_id,
            dataset_id=self.bq_dataset_id,
            location=self.cloud_workspace.data_location,
            description=self.dataset_description,
        )

        for release in releases:
            # Selects all CSV files in the releases transform folder on the Google Cloud Storage bucket and all of its
            # subfolders: https://cloud.google.com/bigquery/docs/batch-loading-data#load-wildcards
            uri = gcs_blob_uri(
                self.cloud_workspace.transform_bucket, f"{gcs_blob_name_from_path(release.transform_folder)}/*.csv"
            )

            # Find schema and load data for release
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
                csv_field_delimiter=",",
                csv_quote_character='"',
                csv_skip_leading_rows=1,
                csv_allow_quoted_newlines=True,
                ignore_unknown_values=True,
            )
            set_task_state(success, self.bq_load.__name__, release)

    def add_new_dataset_releases(self, releases: List[OpenCitationsRelease], **kwargs) -> None:
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

    def cleanup(self, releases: List[OpenCitationsRelease], **kwargs) -> None:
        """Delete all files, folders and XComs associated with this release."""

        for release in releases:
            cleanup(
                dag_id=self.dag_id, execution_date=kwargs["execution_date"], workflow_folder=release.workflow_folder
            )


def list_releases(
    start_date: pendulum.DateTime,
    end_date: pendulum.DateTime,
) -> List[Dict[str, str]]:
    """List available releases from figshare between the start and end date. Semi-open interval [start, end).

    :param start_date: Start date.
    :param end_date: End date.
    :return: List of dictionaries containing release info.
    """

    versions = get_http_response_json(VERSION_URL)
    releases = []
    for version in versions:
        article = get_http_response_json(version["url"])
        snapshot_date = pendulum.parse(article["created_date"])

        if start_date <= snapshot_date < end_date:
            releases.append({"date": snapshot_date.format("YYYYMMDD"), "files": article["files"]})

    return releases
