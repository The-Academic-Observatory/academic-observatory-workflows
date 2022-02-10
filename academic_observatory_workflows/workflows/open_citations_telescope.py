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

import os
import zipfile
from typing import Dict, List

import pendulum
from academic_observatory_workflows.config import schema_folder as default_schema_folder
from airflow.models import Variable
from airflow.models.taskinstance import TaskInstance
from google.cloud import bigquery
from google.cloud.bigquery import SourceFormat
from observatory.platform.utils.airflow_utils import AirflowVars
from observatory.platform.utils.gc_utils import (
    bigquery_sharded_table_id,
    bigquery_table_exists,
)
from observatory.platform.utils.http_download import DownloadInfo, download_files
from observatory.platform.utils.url_utils import (
    get_http_response_json,
    get_observatory_http_header,
)
from observatory.platform.workflows.snapshot_telescope import (
    SnapshotRelease,
    SnapshotTelescope,
)


class OpenCitationsRelease(SnapshotRelease):
    """Open Citations COCI dataset release info."""

    def __init__(
        self,
        dag_id: str,
        release_date: pendulum.DateTime,
        files: List[DownloadInfo],
    ):
        """Create a OpenCitationsRelease instance.

        :param dag_id: the DAG id.
        :param release_date: the date of the release.
        :param files: List of files to download.
        """

        super().__init__(dag_id, release_date)
        self.files = files

    def download(self):
        """Download the release."""

        headers = get_observatory_http_header(package_name="academic_observatory_workflows")
        download_files(download_list=self.files, headers=headers, prefix_dir=self.download_folder)

    def extract(self):
        """Extract the release to the transform folder."""

        for file in self.download_files:
            with zipfile.ZipFile(file, "r") as zf:
                zf.extractall(self.transform_folder)

        # Need to rename files to make the schema finding mechanism work
        for file in self.transform_files:
            filename = os.path.basename(file)
            dir = os.path.dirname(file)
            new_name = os.path.join(dir, f"open_citations.{filename}")
            os.rename(file, new_name)


class OpenCitationsTelescope(SnapshotTelescope):
    """A telescope that harvests the Open Citations COCI CSV dataset . http://opencitations.net/index/coci"""

    DAG_ID = "open_citations"
    VERSION_URL = "https://api.figshare.com/v2/articles/6741422/versions"

    def __init__(
        self,
        dag_id: str = DAG_ID,
        start_date: pendulum.DateTime = pendulum.datetime(2018, 7, 1),
        schedule_interval: str = "@weekly",
        dataset_id: str = DAG_ID,
        schema_folder: str = default_schema_folder(),
        queue: str = "remote_queue",
        dataset_description: str = "The OpenCitations Indexes: http://opencitations.net/",
        table_descriptions: Dict = None,
        catchup: bool = False,
        airflow_vars: List = None,
    ):
        """
        :param dag_id: the id of the DAG.
        :param start_date: the start date of the DAG.
        :param schedule_interval: the schedule interval of the DAG.
        :param dataset_id: the BigQuery dataset id.
        :param schema_folder: the SQL schema path.
        :param queue: Queue to run tasks on.
        :param dataset_description: description for the BigQuery dataset.
        :param table_descriptions: a dictionary with table ids and corresponding table descriptions.
        :param catchup:  whether to catchup the DAG or not.
        :param airflow_vars: list of airflow variable keys, for each variable it is checked if it exists in airflow.
        """

        load_bigquery_table_kwargs = {
            "csv_field_delimiter": ",",
            "csv_quote_character": '"',
            "csv_skip_leading_rows": 1,
            "csv_allow_quoted_newlines": True,
            "write_disposition": bigquery.WriteDisposition.WRITE_APPEND,
            "ignore_unknown_values": True
        }

        if table_descriptions is None:
            table_descriptions = {dag_id: "The Open Citations COCI CSV table."}

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
            schema_folder,
            queue=queue,
            source_format=SourceFormat.CSV,
            load_bigquery_table_kwargs=load_bigquery_table_kwargs,
            dataset_description=dataset_description,
            table_descriptions=table_descriptions,
            catchup=catchup,
            airflow_vars=airflow_vars,
        )

        self.add_setup_task(self.check_dependencies)
        self.add_setup_task(self.get_release_info)
        self.add_task(self.download)
        self.add_task(self.upload_downloaded)
        self.add_task(self.extract)
        self.add_task(self.upload_transformed)
        self.add_task(self.bq_load)
        self.add_task(self.cleanup)

    def _list_releases(
        self,
        *,
        start_date: pendulum.DateTime,
        end_date: pendulum.DateTime,
    ) -> List[Dict[str, str]]:
        """List available releases from figshare between the start and end date (inclusive).

        :param start_date: Start date.
        :param end_date: End date.
        :return: List of dictionaries containing release info.
        """

        versions = get_http_response_json(OpenCitationsTelescope.VERSION_URL)
        releases = []
        for version in versions:
            article = get_http_response_json(version["url"])
            release_date = pendulum.parse(article["created_date"])

            if (start_date is None or start_date <= release_date) and (end_date is None or release_date <= end_date):
                releases.append({"date": release_date.format("YYYYMMDD"), "files": article["files"]})

        return releases

    def _process_release(self, release: Dict[str, str]) -> bool:
        """Indicates whether we should process this release. If there are no files, or if the BigQuery table exists, we will not process this release.

        :param release: Release to consider.
        :return: Whether to process the release.
        """

        if len(release["files"]) == 0:
            return False

        project_id = Variable.get(AirflowVars.PROJECT_ID)
        table_id = bigquery_sharded_table_id(self.dag_id, pendulum.parse(release["date"]))

        if bigquery_table_exists(project_id, self.dataset_id, table_id):
            return False

        return True

    def get_release_info(self, **kwargs):
        """Calculate which releases require processing, and push the info to an XCom.

        :param kwargs: the context passed from the BranchPythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: whether to keep executing the DAG.
        """

        start_date = kwargs["execution_date"]
        end_date = kwargs["next_execution_date"].subtract(microseconds=1)
        releases = self._list_releases(start_date=start_date, end_date=end_date)
        filtered_releases = list(filter(self._process_release, releases))

        continue_dag = len(filtered_releases) > 0
        if continue_dag:
            ti = kwargs["ti"]
            ti.xcom_push(OpenCitationsTelescope.RELEASE_INFO, filtered_releases, start_date)
        return continue_dag

    def make_release(self, **kwargs) -> List[OpenCitationsRelease]:
        """Make release instances. The release is passed as an argument to the function (TelescopeFunction) that is
        called in 'task_callable'.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are
        passed to this argument.
        :return: a list of OpenCitationsRelease instances.
        """

        ti: TaskInstance = kwargs["ti"]
        releases_dict = ti.xcom_pull(
            key=OpenCitationsTelescope.RELEASE_INFO, task_ids=self.get_release_info.__name__, include_prior_dates=False
        )

        releases = []
        for rel_info in releases_dict:
            files = []
            for file in rel_info["files"]:
                info = DownloadInfo(
                    url=file["download_url"], filename=file["name"], hash=file["computed_md5"], hash_algorithm="md5"
                )
                files.append(info)

            release = OpenCitationsRelease(self.dag_id, release_date=pendulum.parse(rel_info["date"]), files=files)

            releases.append(release)

        return releases
