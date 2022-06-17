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
from airflow.models.taskinstance import TaskInstance

from academic_observatory_workflows.api_type_ids import DatasetTypeId
from academic_observatory_workflows.config import schema_folder as default_schema_folder
from academic_observatory_workflows.workflows.unpaywall_snapshot_telescope import UnpaywallSnapshotRelease
from observatory.platform.utils.airflow_utils import (
    AirflowVars,
    get_airflow_connection_password,
)
from observatory.platform.utils.file_utils import find_replace_file, gunzip_files
from observatory.platform.utils.http_download import download_file
from observatory.platform.utils.release_utils import (
    get_dataset_releases,
    is_first_release,
    get_datasets,
    get_latest_dataset_release,
)
from observatory.platform.utils.url_utils import (
    get_http_response_json,
    get_observatory_http_header,
    get_filename_from_http_header,
)
from observatory.platform.workflows.stream_telescope import (
    StreamRelease,
    StreamTelescope,
)
from academic_observatory_workflows.dag_tag import Tag


class UnpaywallRelease(StreamRelease):
    """Unpaywall Data Feed Release"""

    AIRFLOW_CONNECTION = "unpaywall"  # Contains API key

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
        workflow_id: int,
    ):
        """Construct an UnpaywallRelease instance

        :param dag_id: the id of the DAG.
        :param start_date: the start_date of the release.
        :param end_date: the end_date of the release.
        :param first_release: whether this is the first release that is processed for this DAG
        :param workflow_id: api workflow id
        """

        super().__init__(
            dag_id,
            start_date,
            end_date,
            first_release,
        )

        self.workflow_id = workflow_id
        self.http_header = get_observatory_http_header(package_name="academic_observatory_workflows")

    @staticmethod
    def api_key() -> str:
        """The API key for accessing Unpaywall."""

        return get_airflow_connection_password(UnpaywallRelease.AIRFLOW_CONNECTION)

    @staticmethod
    def snapshot_url() -> str:
        """Snapshot URL"""

        return f"{UnpaywallRelease.SNAPSHOT_URL}?api_key={UnpaywallRelease.api_key()}"

    @staticmethod
    def data_feed_url() -> str:
        """Data Feed URL"""

        return f"{UnpaywallRelease.CHANGEFILES_URL}?interval=day&api_key={UnpaywallRelease.api_key()}"

    @staticmethod
    def is_second_run(workflow_id: int) -> bool:
        """Check if this is the second data ingestion run.

        :param workflow_id: API workflow id.
        :return: Whether this is the second run.
        """

        datasets = get_datasets(workflow_id=workflow_id)
        releases = get_dataset_releases(dataset_id=datasets[0].id)
        return len(releases) == 1

    def download(self):
        """Download the release."""
        if self.first_release:
            self._download_snapshot()
        else:
            self._download_data_feed()

    def _download_snapshot(self):
        """Download the most recent Unpaywall snapshot."""

        download_file(url=UnpaywallRelease.snapshot_url(), headers=self.http_header, prefix_dir=self.download_folder)

        # Rename to unpaywall.jsonl.gz. Need for current method of schema detection.
        for file in self.download_files:
            dst = os.path.join(self.download_folder, "unpaywall.jsonl.gz")
            os.rename(file, dst)

    @staticmethod
    def get_unpaywall_daily_feeds():
        url = UnpaywallRelease.data_feed_url()
        release_info = get_http_response_json(url)

        feeds = []
        for release in release_info["list"]:
            release_date = UnpaywallSnapshotRelease.parse_release_date(release["filename"])
            feeds.append({"release_date": release_date, "url": release["url"], "filename": release["filename"]})

        return feeds

    @staticmethod
    def get_diff_releases(*, end_date: pendulum.DateTime, workflow_id: int) -> List[dict]:
        """Get a list of differential releases available between the last release and a target end date.

        :param end_date: Latest date to consider (inclusive).
        :return: List of releases available.
        """

        dataset = get_datasets(workflow_id=workflow_id)[0]
        api_releases = get_dataset_releases(dataset_id=dataset.id)
        latest_release = get_latest_dataset_release(api_releases)

        # Need to get snapshot_date-1, snapshot_date, ..., end_date worth of updates.
        if UnpaywallRelease.is_second_run(workflow_id):
            target_start_date = pendulum.instance(latest_release.start_date).subtract(days=1).start_of("day")
        # Day after recorded end date
        else:
            target_start_date = pendulum.instance(latest_release.end_date).add(seconds=1)

        feeds = UnpaywallRelease.get_unpaywall_daily_feeds()
        filtered_releases = list(
            filter(lambda x: x["release_date"] >= target_start_date and x["release_date"] <= end_date, feeds)
        )

        return filtered_releases

    def _download_data_feed(self):
        """Download data feed update (diff) that can be applied to the base snapshot.  Can only handle a single download."""

        feeds = UnpaywallRelease.get_diff_releases(end_date=self.end_date, workflow_id=self.workflow_id)
        for feed in feeds:
            url = feed["url"]
            filename = os.path.join(self.download_folder, feed["filename"])
            download_file(url=url, filename=filename, headers=self.http_header)

    def extract(self):
        """Unzip the downloaded files."""

        gunzip_files(file_list=self.download_files, output_dir=self.extract_folder)

    def transform(self):
        """Find and replace the 'authenticated-orcid' string in the jsonl to 'authenticated_orcid'"""

        files = list(filter(lambda file: file[-5:] == "jsonl", self.extract_files))

        for src in files:
            filename = os.path.basename(src)
            dst = os.path.join(self.transform_folder, filename)
            find_replace_file(src=src, dst=dst, pattern="authenticated-orcid", replacement="authenticated_orcid")


class UnpaywallTelescope(StreamTelescope):
    DAG_ID = "unpaywall"
    DATAFEED_URL = "https://unpaywall.org/products/data-feed"
    AIRFLOW_CONNECTION = "unpaywall"

    def __init__(
        self,
        workflow_id: int,
        dag_id: str = DAG_ID,
        start_date: pendulum.DateTime = pendulum.datetime(2021, 7, 2),
        schedule_interval: str = "@daily",
        dataset_id: str = "our_research",
        dataset_description: str = f"Unpaywall Data Feed: {DATAFEED_URL}",
        merge_partition_field: str = "doi",
        schema_folder: str = default_schema_folder(),
        airflow_vars: List = None,
        dataset_type_id: str = DatasetTypeId.unpaywall,
    ):
        """Unpaywall Data Feed telescope.

        :param dag_id: the id of the DAG.
        :param start_date: the start date of the DAG.
        :param schedule_interval: the schedule interval of the DAG.
        :param dataset_id: the dataset id.
        :param dataset_description: the dataset description.
        :param merge_partition_field: the BigQuery field used to match partitions for a merge
        :param schema_folder: the SQL schema path.
        :param airflow_vars: list of airflow variable keys, for each variable it is checked if it exists in airflow
        :param workflow_id: API workflow id.
        """

        if airflow_vars is None:
            airflow_vars = [
                AirflowVars.DATA_PATH,
                AirflowVars.PROJECT_ID,
                AirflowVars.DATA_LOCATION,
                AirflowVars.DOWNLOAD_BUCKET,
                AirflowVars.TRANSFORM_BUCKET,
            ]

        super().__init__(
            dag_id,
            start_date,
            schedule_interval,
            dataset_id,
            merge_partition_field,
            schema_folder,
            dataset_description=dataset_description,
            batch_load=False,
            airflow_vars=airflow_vars,
            airflow_conns=[UnpaywallTelescope.AIRFLOW_CONNECTION],
            load_bigquery_table_kwargs={"ignore_unknown_values": True},
            workflow_id=workflow_id,
            dataset_type_id=dataset_type_id,
            tags=[Tag.academic_observatory],
        )

        self.add_setup_task(self.check_dependencies)
        self.add_setup_task(self.check_releases)

        self.add_task(self.download)
        self.add_task(self.upload_downloaded)
        self.add_task(self.extract)
        self.add_task(self.transform)
        self.add_task(self.upload_transformed)
        self.add_task(self.merge_update_files)
        self.add_task(self.bq_load_partition)
        self.add_task(self.bq_delete_old)
        self.add_task(self.bq_append_new)
        self.add_task(self.cleanup)
        self.add_task(self.add_new_dataset_releases)

    @staticmethod
    def _get_snapshot_date() -> pendulum.DateTime:
        """Get the Unpaywall snapshot release date from download file.

        :return: Snapshot file date.
        """

        filename = get_filename_from_http_header(UnpaywallRelease.snapshot_url())
        dt = UnpaywallSnapshotRelease.parse_release_date(filename)
        return dt

    def check_releases(self, **kwargs) -> bool:
        """Check to see if diff releases are available. If not, and it's not the first release, then skip doing work.
        Snapshot releases are checked on first release at download stage. Publish any available releases as an XCOM
        to avoid re-querying Unpaywall servers.

        :param kwargs: The context passed from the PythonOperator.
        :return: True to continue, False to skip.
        """

        # Calculate start/end dates
        if is_first_release(self.workflow_id):
            start_date = UnpaywallTelescope._get_snapshot_date().isoformat()
            end_date = start_date
        else:
            dataset = get_datasets(workflow_id=self.workflow_id)[0]
            api_releases = get_dataset_releases(dataset_id=dataset.id)
            latest_release = get_latest_dataset_release(api_releases)
            start_date = latest_release.end_date.isoformat()
            end_date = pendulum.instance(kwargs["data_interval_end"]).start_of("day")
            releases = UnpaywallRelease.get_diff_releases(end_date=end_date, workflow_id=self.workflow_id)

            if len(releases) == 0:
                return False

            # Not enough data for second run. Need snapshot date-1, snapshot date, snapshot date + 1 (days)
            if UnpaywallRelease.is_second_run(self.workflow_id) and len(releases) < 3:
                return False

            # Use most recent daily-feed date as end date
            releases.sort(key=lambda a: a["release_date"])
            end_date = releases[-1]["release_date"].isoformat()

        # Publish start/end dates
        ti: TaskInstance = kwargs["ti"]
        ti.xcom_push(UnpaywallTelescope.RELEASE_INFO, (start_date, end_date), kwargs["execution_date"])

        # Return status
        return True

    def make_release(self, **kwargs) -> UnpaywallRelease:
        """Make a Release instance. Gets the list of releases available from the release check (setup task).

        :param kwargs: The context passed from the PythonOperator.
        :return: UnpaywallRelease
        """

        ti: TaskInstance = kwargs["ti"]
        start_date_str, end_date_str = ti.xcom_pull(
            key=UnpaywallTelescope.RELEASE_INFO, task_ids=self.check_releases.__name__, include_prior_dates=False
        )

        start_date = pendulum.parse(start_date_str)
        end_date = pendulum.parse(end_date_str)
        first_release = start_date == end_date

        release = UnpaywallRelease(
            dag_id=self.dag_id,
            start_date=start_date,
            end_date=end_date,
            first_release=first_release,
            workflow_id=self.workflow_id,
        )

        return release
