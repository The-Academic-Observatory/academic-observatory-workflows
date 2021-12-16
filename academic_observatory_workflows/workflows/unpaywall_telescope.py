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
from datetime import datetime, timedelta
from typing import Generator, List, Optional, Tuple, Union

import pendulum
from academic_observatory_workflows.config import schema_folder as default_schema_folder
from academic_observatory_workflows.workflows.unpaywall_snapshot_telescope import (
    UnpaywallSnapshotRelease,
)
from airflow.exceptions import AirflowException
from airflow.models.dagrun import DagRun
from croniter import croniter
from dateutil.relativedelta import relativedelta
from observatory.platform.utils.airflow_utils import (
    AirflowVars,
    get_airflow_connection_password,
)
from observatory.platform.utils.file_utils import find_replace_file, gunzip_files
from observatory.platform.utils.http_download import download_file
from observatory.platform.utils.url_utils import (
    get_http_response_json,
    get_observatory_http_header,
)
from observatory.platform.utils.workflow_utils import is_first_dag_run
from observatory.platform.workflows.stream_telescope import (
    StreamRelease,
    StreamTelescope,
)


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
    ):
        """Construct an UnpaywallRelease instance

        :param dag_id: the id of the DAG.
        :param start_date: the start_date of the release.
        :param end_date: the end_date of the release.
        :param first_release: whether this is the first release that is processed for this DAG
        """

        super().__init__(
            dag_id,
            start_date,
            end_date,
            first_release,
        )

        self.http_header = get_observatory_http_header(package_name="academic_observatory_workflows")

    @property
    def api_key(self) -> str:
        """The API key for accessing Unpaywall."""

        return get_airflow_connection_password(UnpaywallRelease.AIRFLOW_CONNECTION)

    @property
    def snapshot_url(self) -> str:
        """Snapshot URL"""

        return f"{UnpaywallRelease.SNAPSHOT_URL}?api_key={self.api_key}"

    @property
    def data_feed_url(self) -> str:
        """Data Feed URL"""

        return f"{UnpaywallRelease.CHANGEFILES_URL}?interval=day&api_key={self.api_key}"

    def download(self):
        """Download the release."""
        if self.first_release:
            self._download_snapshot()
        else:
            self._download_data_feed()

    def _download_snapshot(self):
        """Download the most recent Unpaywall snapshot on or before the start date."""

        download_file(url=self.snapshot_url, headers=self.http_header, prefix_dir=self.download_folder)

        download_date = UnpaywallSnapshotRelease.parse_release_date(self.download_files[0]).date()
        start_date = self.start_date.date()

        if start_date != download_date:
            raise AirflowException(
                f"The telescope start date {start_date} and the downloaded snapshot date {download_date} do not match.  Please set the telescope's start date to {download_date}."
            )

    @staticmethod
    def get_diff_release(*, feed_url: str, start_date: pendulum.DateTime) -> Tuple[Optional[str], Optional[str]]:
        """Get the differential release url and filename.

        :param feed_url: The URL to query for releases.
        :param start_date: Earliest date to consider.
        :return: (None,None) if nothing found, otherwise (url, filename).
        """

        release_info = get_http_response_json(feed_url)
        for release in release_info["list"]:
            # Have been advised by Unpaywall to parse timestamp from filename instead of relying on the json fields.
            release_date = UnpaywallSnapshotRelease.parse_release_date(release["filename"]).date()

            # Apply diffs from 2 days ago.  This is so we start applying diffs 1 day before the snapshot date to
            # guarantee no gaps with the snapshot.
            target_date = (start_date - pendulum.Duration(days=2)).date()

            if release_date == target_date:
                return release["url"], release["filename"]

        return (None, None)

    def _download_data_feed(self):
        """Download data feed update (diff) that can be applied to the base snapshot.  Can only handle a single download."""

        url, filename = self.get_diff_release(
            feed_url=self.data_feed_url,
            start_date=self.start_date,
        )
        filename = os.path.join(self.download_folder, filename)
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
        dag_id: str = DAG_ID,
        start_date: pendulum.DateTime = pendulum.datetime(2021, 7, 2),
        schedule_interval: str = "@daily",
        dataset_id: str = "our_research",
        dataset_description: str = f"Unpaywall Data Feed: {DATAFEED_URL}",
        merge_partition_field: str = "doi",
        schema_folder: str = default_schema_folder(),
        airflow_vars: List = None,
        catchup=True,
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
        :param catchup: Whether to perform catchup on old releases.
        """

        self._validate_schedule_interval(start_date=start_date, schedule_interval=schedule_interval)

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
            batch_load=True,
            catchup=catchup,
            airflow_vars=airflow_vars,
            airflow_conns=[UnpaywallTelescope.AIRFLOW_CONNECTION],
            load_bigquery_table_kwargs={"ignore_unknown_values": True},
        )

        self.add_setup_task(self.check_dependencies)
        self.add_setup_task(self.check_releases)

        self.add_task(self.download)
        self.add_task(self.upload_downloaded)
        self.add_task(self.extract)
        self.add_task(self.transform)
        self.add_task(self.upload_transformed)
        self.add_task(self.bq_load_partition)

        self.add_task_chain([self.bq_delete_old, self.bq_append_new, self.cleanup], trigger_rule="none_failed")

    @staticmethod
    def _schedule_days_apart(
        *, start_date: pendulum.DateTime, schedule_interval: Union[str, timedelta, relativedelta]
    ) -> Generator:
        """Calculate the scheduled days apart.

        :param start_date: DAG start date.
        :param schedule_interval: DAG schedule interval.
        :return: A generator that gives back the days apart for each execution.
        """

        if isinstance(schedule_interval, (timedelta, relativedelta)):
            while True:
                yield schedule_interval.days

        a = start_date
        it = croniter(schedule_interval, start_date)

        while True:
            b = it.next(datetime)
            diff = (b - a).days
            a = b
            yield diff

    def _validate_schedule_interval(self, *, start_date: pendulum.DateTime, schedule_interval: str):
        """Check that the schedule interval gives us 1 or 7 day differences.
        Throws exception on failure.

        :param start_date: DAG start date.
        :param schedule_interval: DAG schedule interval.
        """

        days_apart = UnpaywallTelescope._schedule_days_apart(start_date=start_date, schedule_interval=schedule_interval)

        diffs = [next(days_apart) for i in range(2)]
        if diffs[0] != diffs[1] or diffs[0] != 1:
            raise AirflowException(f"Schedule interval must trigger executions 1 days apart.")

    def check_releases(self, **kwargs) -> bool:
        """Check to see if diff releases are available. If not, and it's not the first release, then skip doing work.
        Snapshot releases are checked on first release at download stage.

        :param kwargs: The context passed from the PythonOperator.
        :return: True to continue, False to skip.
        """

        start_date, first_release = self._get_release_info(**kwargs)

        # No checks on first release
        if first_release:
            return True

        # Check for diffs
        api_key = get_airflow_connection_password(UnpaywallRelease.AIRFLOW_CONNECTION)
        url = f"{UnpaywallRelease.CHANGEFILES_URL}?interval=day&api_key={api_key}"
        _, filename = UnpaywallRelease.get_diff_release(feed_url=url, start_date=start_date)

        # No release within our target date.
        if filename is None:
            return False

        # Release found
        return True

    def make_release(self, **kwargs) -> UnpaywallRelease:
        """Make a Release instance

        :param kwargs: The context passed from the PythonOperator.
        :return: UnpaywallRelease
        """

        start_date, first_release = self._get_release_info(**kwargs)
        release = UnpaywallRelease(
            dag_id=self.dag_id,
            start_date=start_date,
            end_date=start_date,
            first_release=first_release,
        )
        return release

    def _get_release_info(self, **kwargs) -> Tuple[pendulum.DateTime, bool]:
        """Get the start, end dates, and whether this is a first release.

        :param kwargs: The context passed from the PythonOperator.
        :return start date, whether first release.
        """
        dag_run: DagRun = kwargs["dag_run"]
        first_release = is_first_dag_run(dag_run)
        start_date = pendulum.instance(kwargs["execution_date"])
        return start_date, first_release
