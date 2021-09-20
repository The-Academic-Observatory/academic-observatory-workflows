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

# Author: Tuan Chien

import os
from typing import List

import pendulum
from academic_observatory_workflows.config import schema_folder as default_schema_folder
from airflow.exceptions import AirflowException
from airflow.models.taskinstance import TaskInstance
from observatory.platform.utils.airflow_utils import (
    AirflowVars,
    get_airflow_connection_password,
)
from observatory.platform.utils.file_utils import find_replace_file, gunzip_files
from observatory.platform.utils.http_download import download_files
from observatory.platform.utils.json_util import csv_to_jsonlines
from observatory.platform.utils.url_utils import (
    get_http_response_json,
    get_observatory_http_header,
)
from observatory.platform.workflows.stream_telescope import (
    StreamRelease,
    StreamTelescope,
)


class UnpaywallDataFeedRelease(StreamRelease):
    """Unpaywall Data Feed Release"""

    AIRFLOW_CONNECTION = "unpaywall_data_feed"  # Contains API key

    # These links are publicly listed on Unpaywall's website. See https://unpaywall.org/products/data-feed
    SNAPSHOT_URL = "https://api.unpaywall.org/feed/snapshot"
    CHANGEFILES_URL = "https://api.unpaywall.org/feed/changefiles"

    def __init__(
        self,
        *,
        dag_id: str,
        start_date: pendulum.DateTime,
        end_date: pendulum.DateTime,
        first_release: bool,
        max_download_connections: int = 1,
        update_interval: str = "day",  # Can be "week"
    ):
        """Construct an UnpaywallDataFeedRelease instance

        :param dag_id: the id of the DAG.
        :param start_date: the start_date of the release.
        :param end_date: the end_date of the release.
        :param first_release: whether this is the first release that is processed for this DAG
        :param max_download_connections: max download connections.
        :param update_interval: Whether to use the daily or weekly diffs.
        """

        super().__init__(
            dag_id,
            start_date,
            end_date,
            first_release,
        )

        if update_interval != "day" and update_interval != "week":
            raise AirflowException("update_interval can only be day or week")

        self.update_interval = update_interval
        self.max_download_connections = max_download_connections

    @property
    def api_key(self) -> str:
        """The API key for accessing Unpaywall."""

        return get_airflow_connection_password(UnpaywallDataFeedRelease.AIRFLOW_CONNECTION)

    @property
    def snapshot_url(self) -> str:
        """Snapshot URL"""

        return f"{UnpaywallDataFeedRelease.SNAPSHOT_URL}?api_key={self.api_key}"

    @property
    def data_feed_url(self) -> str:
        """Data Feed URL"""

        return f"{UnpaywallDataFeedRelease.CHANGEFILES_URL}?interval={self.update_interval}&api_key={self.api_key}"

    def download(self):
        """Download the release."""

        # May need to refactor depending on how snapshot looks.
        if self.first_release:
            self.download_snapshot_()
        else:
            self.download_data_feed_()

    def download_snapshot_(self):
        """Download the most recent Unpaywall snapshot on or before the start date."""

        # Query list of available snapshots.
        # Find the most recent one.
        # Download most recent one.
        response = get_http_response_json(self.snapshot_url)

    def download_data_feed_(self):
        """Download data feed updates (diff) that can be applied to the base snapshot."""

        download_list = list()

        release_info = get_http_response_json(self.data_feed_url)
        for release in release_info["list"]:
            release_date = release["date"] if self.update_interval == "day" else release["to_date"]
            release_date = pendulum.parse(release_date)

            if self.start_date <= release_date <= self.end_date:
                filename = os.path.join(self.download_folder, release["filename"])
                download_list.append({"url": release["url"], "filename": filename})

        headers = get_observatory_http_header(package_name="academic_observatory_workflows")
        download_files(download_list=download_list, headers=headers)

    def extract(self):
        """Unzip the downloaded files."""

        gunzip_files(file_list=self.download_files, output_dir=self.extract_folder)

    def transform(self):
        """Convert any CSV to JSONL as needed.  It's unclear if this is still necessary, but there are examples of CSV
        in the data feed. The latest being in 2021-01.

        Find and replace the 'authenticated-orcid' string in the jsonl to 'authenticated_orcid'
        """

        # Convert csv to jsonl and put it in the extract dir
        self.convert_csv_to_jsonl_()

        # Replace authenticated-orcid string with authenticated_orcid in files
        self.find_and_replace_strings_()

    def convert_csv_to_jsonl_(self):
        """Convert CSV to JSONL files as needed."""

        files = filter(lambda file: file[-3:] == "csv", self.extract_files)

        for src_path in files:
            file = os.path.basename(src_path)
            dst_file = file + ".jsonl"  # Add json suffix
            dst_path = os.path.join(self.extract_folder, dst_file)
            csv_to_jsonlines(csv_file=src_path, jsonl_file=dst_path)

    def find_and_replace_strings_(self):
        """Find and replace the 'authenticated-orcid' string in the jsonl to 'authenticated_orcid'"""

        files = list(filter(lambda file: file[-5:] == "jsonl", self.extract_files))
        print(f"FIND AND REPLACE ON: {files}.  extract files: {self.extract_files}")

        for src in files:
            print(f"Find and replacing {src}")
            filename = os.path.basename(src)
            dst = os.path.join(self.transform_folder, filename)
            find_replace_file(src=src, dst=dst, pattern="authenticated-orcid", replacement="authenticated_orcid")


class UnpaywallDataFeedTelescope(StreamTelescope):
    DAG_ID = "unpaywall_data_feed"
    DATAFEED_URL = "https://unpaywall.org/products/data-feed"

    def __init__(
        self,
        dag_id: str = DAG_ID,
        start_date: pendulum.DateTime = pendulum.datetime(2021, 7, 2),
        schedule_interval: str = "@daily",
        dataset_id: str = "unpaywall_data_feed",
        dataset_description: str = f"Unpaywall Data Feed: {DATAFEED_URL}",
        queue: str = "default",
        merge_partition_field: str = "doi",
        bq_merge_days: int = 7,
        schema_folder: str = default_schema_folder(),
        airflow_vars: List = None,
        max_download_connections: int = 8,
    ):
        """Unpaywall Data Feed telescope.

        :param dag_id: the id of the DAG.
        :param start_date: the start date of the DAG.
        :param schedule_interval: the schedule interval of the DAG.
        :param dataset_id: the dataset id.
        :param dataset_description: the dataset description.
        :param queue: the queue that the tasks should run on.
        :param merge_partition_field: the BigQuery field used to match partitions for a merge
        :param bq_merge_days: how often partitions should be merged (every x days)
        :param schema_folder: the SQL schema path.
        :param airflow_vars: list of airflow variable keys, for each variable it is checked if it exists in airflow
        :param max_download_connections: Maximum number of connections allowed for simultaneous downloading of files.
        """

        if airflow_vars is None:
            airflow_vars = [
                AirflowVars.DATA_PATH,
                AirflowVars.PROJECT_ID,
                AirflowVars.DATA_LOCATION,
                AirflowVars.DOWNLOAD_BUCKET,
                AirflowVars.TRANSFORM_BUCKET,
            ]

        airflow_conns = ["unpaywall_data_feed"]

        super().__init__(
            dag_id,
            start_date,
            schedule_interval,
            dataset_id,
            merge_partition_field,
            bq_merge_days,
            schema_folder,
            dataset_description=dataset_description,
            queue=queue,
            batch_load=True,
            airflow_vars=airflow_vars,
            airflow_conns=airflow_conns,
        )

        self.max_download_connections = max_download_connections

        self.add_setup_task_chain([self.check_dependencies, self.get_release_info])

        self.add_task(self.download)
        self.add_task(self.upload_downloaded)
        self.add_task(self.extract)
        self.add_task(self.transform)
        self.add_task(self.upload_transformed)
        self.add_task(self.bq_load_partition)

        self.add_task_chain([self.bq_delete_old, self.bq_append_new, self.cleanup], trigger_rule="none_failed")

    def make_release(self, **kwargs) -> UnpaywallDataFeedRelease:
        """Make a Release instance

        :param kwargs: The context passed from the PythonOperator.
        :return: UnpaywallDataFeedRelease
        """

        ti: TaskInstance = kwargs["ti"]
        start_date, end_date, first_release = ti.xcom_pull(key=self.RELEASE_INFO, include_prior_dates=True)

        start_date = pendulum.parse(start_date)
        end_date = pendulum.parse(end_date)

        update_interval = "week" if self.schedule_interval == "@weekly" else "day"

        release = UnpaywallDataFeedRelease(
            dag_id=self.dag_id,
            start_date=start_date,
            end_date=end_date,
            first_release=first_release,
            update_interval=update_interval,
            max_download_connections=self.max_download_connections,
        )
        return release
