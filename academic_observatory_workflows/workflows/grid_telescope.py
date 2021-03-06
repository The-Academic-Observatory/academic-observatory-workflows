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

# Author: James Diprose, Aniek Roelofs

from __future__ import annotations

import json
import logging
import os
import re
from shutil import copyfile
from typing import List
from zipfile import BadZipFile, ZipFile

import pendulum
from airflow.exceptions import AirflowException
from airflow.models.taskinstance import TaskInstance
from google.cloud.bigquery import SourceFormat

from academic_observatory_workflows.config import schema_folder as default_schema_folder
from observatory.platform.utils.airflow_utils import AirflowVars
from observatory.platform.utils.file_utils import list_to_jsonl_gz
from observatory.platform.utils.http_download import download_file
from observatory.platform.utils.url_utils import (
    get_observatory_http_header,
    retry_session,
)
from observatory.platform.workflows.snapshot_telescope import (
    SnapshotRelease,
    SnapshotTelescope,
)
from academic_observatory_workflows.dag_tag import Tag


class GridRelease(SnapshotRelease):
    def __init__(self, dag_id: str, article_ids: List[str], release_date: pendulum.DateTime):
        """Construct a GridRelease.

        :param article_ids: the titles of the Figshare articles.
        :param release_date: the release date.
        """

        self.article_ids = article_ids
        download_files_regex = dag_id + "\.[a-zA-Z]+"
        extract_files_regex = "grid.json"
        transform_files_regex = f"{dag_id}.jsonl.gz"

        super().__init__(dag_id, release_date, download_files_regex, extract_files_regex, transform_files_regex)

    @property
    def transform_path(self) -> str:
        return os.path.join(self.transform_folder, f"{self.dag_id}.jsonl.gz")

    def download(self, timeout: float = 30.0) -> List[str]:
        """Downloads an individual GRID release from Figshare.

        :param timeout: the timeout in seconds when calling the Figshare API.
        :return: the paths on the system of the downloaded files.
        """

        downloads = []
        for article_id in self.article_ids:
            response = retry_session().get(
                GridTelescope.GRID_FILE_URL.format(article_id=article_id),
                timeout=timeout,
                headers={"Accept-encoding": "gzip"},
            )
            article_files = json.loads(response.text)

            for i, article_file in enumerate(article_files):
                real_file_name = article_file["name"]
                supplied_md5 = article_file["supplied_md5"]
                download_url = article_file["download_url"]
                file_type = os.path.splitext(real_file_name)[1]

                if file_type == ".csv":
                    continue

                # Download
                logging.info(f"Downloading file: {real_file_name}, md5: {supplied_md5}, url: {download_url}")
                file_path = os.path.join(self.download_folder, f"{self.dag_id}{file_type}")
                logging.info(f"Saving to {file_path}")

                headers = get_observatory_http_header(package_name="academic_observatory_workflows")
                download_file(
                    url=download_url, filename=file_path, hash=supplied_md5, hash_algorithm="md5", headers=headers
                )

                downloads.append(file_path)

        return downloads

    def extract(self) -> None:
        """Extract a single GRID release to a given extraction path. The release will be extracted into the following
        directory structure: extraction_path/file_name (without extension).

        If the release is a .zip file, it will be extracted, otherwise it will be copied to a directory within the
        extraction path.
        :return: None.
        """

        logging.info(f"Download files {self.download_files}")
        # Extract files
        for file_path in self.download_files:
            # Extract zip files
            if file_path.endswith(".zip"):
                unzip_folder_path = self.extract_folder
                logging.info(f"Extracting file: {file_path}")
                try:
                    with ZipFile(file_path) as zip_file:
                        zip_file.extractall(unzip_folder_path)
                except BadZipFile:
                    logging.error("Not a zip file")
                logging.info(f"File extracted to: {unzip_folder_path}")
            else:
                # File is already uncompressed (.json or .csv), so make a directory and copy it into it
                output_file_path = os.path.join(self.extract_folder, os.path.basename(file_path))
                copyfile(file_path, output_file_path)
                logging.info(f"File saved to: {output_file_path}")

    def transform(self) -> str:
        """Transform an extracted GRID release .json file into json lines format and gzip the result.

        :return: the GRID version, the file name and the file path.
        """

        extract_files = self.extract_files

        # Only process one JSON file
        if len(extract_files) == 1:
            release_json_file = extract_files[0]
            logging.info(f"Transforming file: {release_json_file}")

        else:
            raise AirflowException(f"{len(extract_files)} extracted grid.json file found: {extract_files}")

        with open(release_json_file) as json_file:
            # Load GRID release JSON file
            data = json.load(json_file)
            version = data["version"]
            institutes = data["institutes"]

            # Transform GRID release into JSON Lines format saving in memory buffer
            # Save in memory buffer to gzipped file
            list_to_jsonl_gz(self.transform_path, institutes)

        return version


def list_grid_records(
    start_date: pendulum.DateTime, end_date: pendulum.DateTime, grid_dataset_url: str, timeout: float = 30.0
) -> List[dict]:
    """List all GRID records available on Figshare between two dates.

    :param timeout: the number of seconds to wait until timing out.
    :return: the list of GRID releases with required variables stored as a dictionary.
    """

    response = retry_session().get(grid_dataset_url, timeout=timeout, headers={"Accept-encoding": "gzip"})
    response_json = json.loads(response.text)

    records: List[dict] = []
    release_articles = {}
    for item in response_json:
        published_date: pendulum.DateTime = pendulum.parse(item["published_date"])

        if start_date <= published_date < end_date:
            article_id = item["id"]
            title = item["title"]

            # Parse date:
            # The publish date is not used as the release date because the dataset is often
            # published after the release date
            date_matches = re.search("([0-9]{4}\-[0-9]{2}\-[0-9]{2})", title)
            if date_matches is None:
                raise ValueError(f"No release date found in GRID title: {title}")
            release_date = date_matches[0]

            try:
                release_articles[release_date].append(article_id)
            except KeyError:
                release_articles[release_date] = [article_id]

    for release_date in release_articles:
        article_ids = release_articles[release_date]
        records.append({"article_ids": article_ids, "release_date": release_date})
    return records


class GridTelescope(SnapshotTelescope):
    """
    The Global Research Identifier Database (GRID): https://grid.ac/

    Saved to the BigQuery table: <project_id>.digital_science.gridYYYYMMDD
    """

    DAG_ID = "grid"
    DATASET_ID = "digital_science"
    GRID_FILE_URL = "https://api.figshare.com/v2/articles/{article_id}/files"
    GRID_DATASET_URL = "https://api.figshare.com/v2/collections/3812929/articles?page_size=1000"

    def __init__(
        self,
        dag_id: str = DAG_ID,
        start_date: pendulum.DateTime = pendulum.datetime(2015, 9, 1),
        schedule_interval: str = "@weekly",
        dataset_id: str = DATASET_ID,
        schema_folder: str = default_schema_folder(),
        source_format: str = SourceFormat.NEWLINE_DELIMITED_JSON,
        dataset_description: str = "Datasets provided by Digital Science: https://www.digital-science.com/",
        catchup: bool = True,
        airflow_vars: List = None,
        workflow_id: int = None,
    ):
        """Construct a GridTelescope instance.

        :param dag_id: the id of the DAG.
        :param start_date: the start date of the DAG.
        :param schedule_interval: the schedule interval of the DAG.
        :param dataset_id: the BigQuery dataset id.
        :param schema_folder: the SQL schema path.
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
        super().__init__(
            dag_id,
            start_date,
            schedule_interval,
            dataset_id,
            schema_folder,
            source_format=source_format,
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

    def make_release(self, **kwargs) -> List[GridRelease]:
        """Make release instances. The release is passed as an argument to the function (TelescopeFunction) that is
        called in 'task_callable'.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are
        passed to this argument.
        :return: A list of grid release instances
        """

        ti: TaskInstance = kwargs["ti"]
        records = ti.xcom_pull(
            key=GridTelescope.RELEASE_INFO, task_ids=self.list_releases.__name__, include_prior_dates=False
        )
        releases = []
        for record in records:
            article_ids = record["article_ids"]
            release_date = record["release_date"]

            releases.append(GridRelease(self.dag_id, article_ids, pendulum.parse(release_date)))
        return releases

    def list_releases(self, **kwargs):
        """Lists all GRID releases for a given month and publishes their article_id's and
        release_date's as an XCom.

        :param kwargs: the context passed from the BranchPythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: the identifier of the task to execute next.
        """

        execution_date = kwargs["execution_date"]
        next_execution_date = kwargs["next_execution_date"]
        records = list_grid_records(execution_date, next_execution_date, GridTelescope.GRID_DATASET_URL)

        continue_dag = len(records)
        if continue_dag:
            # Push messages
            ti: TaskInstance = kwargs["ti"]
            ti.xcom_push(GridTelescope.RELEASE_INFO, records, execution_date)

        return continue_dag

    def download(self, releases: List[GridRelease], **kwargs):
        """Task to download the GRID releases for a given month.

        :param releases: a list of GRID releases.
        :return: None.
        """

        # Download each release
        for release in releases:
            release.download()

    def extract(self, releases: List[GridRelease], **kwargs):
        """Task to extract the GRID releases for a given month.

        :param releases: a list of GRID releases.
        :return: None.
        """

        # Extract each release
        for release in releases:
            release.extract()

    def transform(self, releases: List[GridRelease], **kwargs):
        """Task to transform the GRID releases for a given month.

        :param releases: a list of GRID releases.
        :return: None.
        """

        # Transform each release
        for release in releases:
            release.transform()
