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

# Author: Aniek Roelofs, Tuan Chien

import logging
import os
import re
from typing import Dict, List, Union

import pendulum
from airflow.models import Variable
from airflow.models.taskinstance import TaskInstance
from observatory.platform.utils.airflow_utils import (
    AirflowVars,
    get_airflow_connection_url,
)
from observatory.platform.utils.file_utils import find_replace_file, gunzip_files
from observatory.platform.utils.gc_utils import (
    bigquery_sharded_table_id,
    bigquery_table_exists,
)
from observatory.platform.utils.http_download import download_file
from observatory.platform.utils.url_utils import (
    get_http_response_xml_to_dict,
    get_observatory_http_header,
)
from observatory.platform.workflows.snapshot_telescope import (
    SnapshotRelease,
    SnapshotTelescope,
)

from academic_observatory_workflows.config import schema_folder as default_schema_folder


class UnpaywallSnapshotRelease(SnapshotRelease):
    """Unpaywall Snapshot Release instance."""

    AIRFLOW_CONNECTION = "unpaywall_snapshot"

    def __init__(
        self,
        dag_id: str,
        release_date: pendulum.DateTime,
        file_name: str = None,
    ):
        """Construct an UnpaywallSnapshotRelease instance.

        :param dag_id: The DAG ID.
        :param release_date: Release date.
        :param file_name: Filename to download.
        """

        super().__init__(
            dag_id=dag_id,
            release_date=release_date,
        )

        self.file_name = file_name

    @property
    def url(self):
        """Download url."""

        dataset_url = get_airflow_connection_url(UnpaywallSnapshotRelease.AIRFLOW_CONNECTION)
        return f"{dataset_url}{self.file_name}"

    @property
    def download_path(self) -> str:
        """Get the path to the downloaded file.

        :return: the file path.
        """

        return os.path.join(self.download_folder, "unpaywall_snapshot.jsonl.gz")

    @property
    def extract_path(self) -> str:
        """Get the path to the extracted file.

        :return: the file path.
        """

        return os.path.join(self.extract_folder, "unpaywall_snapshot.jsonl")

    @property
    def transform_path(self) -> str:
        """Get the path to the transformed file.

        :return: the file path.
        """

        return os.path.join(self.transform_folder, "unpaywall_snapshot.jsonl")

    @staticmethod
    def parse_release_date(file_name: str) -> pendulum.DateTime:
        """Parses a release date from a file name.

        :param file_name: Unpaywall release file name (contains date string).
        :return: date.
        """

        date = re.search(r"\d{4}-\d{2}-\d{2}", file_name).group()
        return pendulum.parse(date)

    def download(self):
        """Download an Unpaywall release.  Either from the snapshot or data freed."""

        headers = get_observatory_http_header(package_name="academic_observatory_workflows")
        download_file(url=self.url, filename=self.download_path, headers=headers)

    def extract(self):
        """Extract release from gzipped file."""

        gunzip_files(file_list=[self.download_path], output_dir=self.extract_folder)

    def transform(self):
        """Transforms release by replacing a specific '-' with '_'."""

        pattern = "authenticated-orcid"
        replacement = "authenticated_orcid"
        find_replace_file(src=self.extract_path, dst=self.transform_path, pattern=pattern, replacement=replacement)


class UnpaywallSnapshotTelescope(SnapshotTelescope):
    """A container for holding the constants and static functions for the Unpaywall telescope."""

    DAG_ID = "unpaywall_snapshot"

    def __init__(
        self,
        *,
        dag_id: str = DAG_ID,
        start_date: pendulum.DateTime = pendulum.datetime(2018, 5, 14),
        schedule_interval: str = "@weekly",
        dataset_id: str = "our_research",
        queue: str = "remote_queue",
        schema_folder: str = default_schema_folder(),
        load_bigquery_table_kwargs: Dict = None,
        table_descriptions: Dict = None,
        catchup: bool = True,
        airflow_vars: Union[List[AirflowVars], None] = None,
    ):
        """Initialise the telescope.

        :param dag_id: DAG ID.
        :param start_date: Airflow start date for running the DAG.
        :param schedule_interval: Airflow schedule interval for running the DAG.
        :param dataset_id: GCP dataset ID.
        :param queue: Airflow worker queue to use.
        :param schema_folder: Folder containing the database schemas.
        :param load_bigquery_table_kwargs: the customisation parameters for loading data into a BigQuery table.
        :param table_descriptions: Descriptions of the tables.
        :param catchup: Whether Airflow should catch up past dag runs.
        :param airflow_vars: List of Airflow variables to use.
        """

        if table_descriptions is None:
            table_descriptions = {dag_id: "The Unpaywall database: https://unpaywall.org/"}

        if airflow_vars is None:
            airflow_vars = [
                AirflowVars.DATA_PATH,
                AirflowVars.PROJECT_ID,
                AirflowVars.DATA_LOCATION,
                AirflowVars.DOWNLOAD_BUCKET,
                AirflowVars.TRANSFORM_BUCKET,
            ]

        airflow_conns = [UnpaywallSnapshotRelease.AIRFLOW_CONNECTION]

        if load_bigquery_table_kwargs is None:
            load_bigquery_table_kwargs = {"ignore_unknown_values": True}

        super().__init__(
            dag_id,
            start_date,
            schedule_interval,
            dataset_id,
            schema_folder,
            load_bigquery_table_kwargs=load_bigquery_table_kwargs,
            table_descriptions=table_descriptions,
            catchup=catchup,
            airflow_vars=airflow_vars,
            airflow_conns=airflow_conns,
            queue=queue,
        )

        self.add_setup_task(self.check_dependencies)
        self.add_setup_task(self.get_release_info)
        self.add_task(self.download)
        self.add_task(self.upload_downloaded)
        self.add_task(self.extract)
        self.add_task(self.transform)
        self.add_task(self.upload_transformed)
        self.add_task(self.bq_load)
        self.add_task(self.cleanup)

    @staticmethod
    def list_releases(start_date: pendulum.DateTime, end_date: pendulum.DateTime) -> List[Dict]:
        """Parses xml string retrieved from GET request to create list of urls for
        different releases.

        :param start_date:
        :param end_date:
        :return: a list of UnpaywallSnapshotRelease instances.
        """

        releases_list = list()

        # Request releases page
        dataset_url = get_airflow_connection_url(UnpaywallSnapshotRelease.AIRFLOW_CONNECTION)
        response = get_http_response_xml_to_dict(dataset_url)

        items = response["ListBucketResult"]["Contents"]
        for item in items:
            # Get filename and parse dates
            file_name = item["Key"]
            last_modified = pendulum.parse(item["LastModified"])
            release_date = UnpaywallSnapshotRelease.parse_release_date(file_name)

            # Only include release if last modified date is within start and end date.
            # Last modified date is used rather than release date because if release date is used then releases will
            # be missed.
            if start_date <= last_modified < end_date:
                release = {
                    "date": release_date.format("YYYYMMDD"),
                    "file_name": file_name,
                }
                releases_list.append(release)

        return releases_list

    def get_release_info(self, **kwargs) -> bool:
        """Based on a list of all releases, checks which ones were released between this and the next execution date
        of the DAG. If the release falls within the time period mentioned above, checks if a bigquery table doesn't
        exist yet for the release. A list of releases that passed both checks is passed to the next tasks. If the list
        is empty the workflow will stop.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: Whether the DAG should continue.
        """

        # Get variables
        project_id = Variable.get(AirflowVars.PROJECT_ID)

        # List releases between a start and end date
        execution_date = pendulum.instance(kwargs["execution_date"])
        next_execution_date = pendulum.instance(kwargs["next_execution_date"])
        releases_list = UnpaywallSnapshotTelescope.list_releases(execution_date, next_execution_date)
        logging.info(f"Releases between {execution_date} and {next_execution_date}:\n{releases_list}\n")

        # Check if the BigQuery table exists for each release to see if the workflow needs to process
        releases_list_out = []
        for release in releases_list:
            table_id = bigquery_sharded_table_id(UnpaywallSnapshotTelescope.DAG_ID, pendulum.parse(release["date"]))

            file = release["file_name"]
            if bigquery_table_exists(project_id, self.dataset_id, table_id):
                logging.info(f"Skipping as table exists for {file}: " f"{project_id}.{self.dataset_id}.{table_id}")
            else:
                logging.info(f"Table doesn't exist yet, processing {file} in this workflow")
                releases_list_out.append(release)

        # If releases_list_out contains items then the DAG will continue (return True) otherwise it will
        # stop (return False)
        continue_dag = len(releases_list_out) > 0
        if continue_dag:
            ti: TaskInstance = kwargs["ti"]
            ti.xcom_push(UnpaywallSnapshotTelescope.RELEASE_INFO, releases_list_out, execution_date)
        return continue_dag

    def make_release(self, **kwargs) -> List[UnpaywallSnapshotRelease]:
        """Make a list of UnpaywallSnapshotRelease instances.

        :param kwargs: The context passed from the PythonOperator.
        :return: UnpaywallSnapshotRelease instance.
        """

        ti: TaskInstance = kwargs["ti"]
        release_info = ti.xcom_pull(
            key=UnpaywallSnapshotTelescope.RELEASE_INFO,
            task_ids=self.get_release_info.__name__,
            include_prior_dates=False,
        )
        releases = list()
        for release in release_info:
            release_date = pendulum.parse(release["date"])
            file_name = release["file_name"]
            release = UnpaywallSnapshotRelease(dag_id=self.dag_id, release_date=release_date, file_name=file_name)
            releases.append(release)

        return releases
