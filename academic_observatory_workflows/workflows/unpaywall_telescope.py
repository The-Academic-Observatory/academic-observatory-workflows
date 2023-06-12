# Copyright 2020-2023 Curtin University
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

# Author: Tuan Chien, James Diprose

from __future__ import annotations

import datetime
import logging
import os
import re
from datetime import timedelta
from typing import List, Dict

import pendulum
from airflow.models.taskinstance import TaskInstance
from airflow.operators.dummy import DummyOperator
from google.cloud import bigquery
from google.cloud.bigquery import SourceFormat

from academic_observatory_workflows.config import schema_folder as default_schema_folder, Tag
from observatory.api.client.model.dataset_release import DatasetRelease
from observatory.platform.airflow import get_airflow_connection_password, PreviousDagRunSensor, is_first_dag_run
from observatory.platform.api import get_dataset_releases, get_latest_dataset_release, make_observatory_api
from observatory.platform.bigquery import (
    bq_table_id,
    bq_find_schema,
    bq_load_table,
    bq_create_dataset,
    bq_upsert_records,
    bq_snapshot,
    bq_sharded_table_id,
)
from observatory.platform.config import AirflowConns
from observatory.platform.files import (
    list_files,
    gunzip_files,
    clean_dir,
    find_replace_file,
    merge_update_files,
)
from observatory.platform.gcs import gcs_blob_name_from_path, gcs_blob_uri, gcs_upload_files
from observatory.platform.observatory_config import CloudWorkspace
from observatory.platform.utils.http_download import download_file, download_files, DownloadInfo
from observatory.platform.utils.url_utils import (
    get_http_response_json,
    get_observatory_http_header,
    get_filename_from_http_header,
)
from observatory.platform.workflows.workflow import (
    Workflow,
    cleanup,
    Release,
    SnapshotRelease,
    ChangefileRelease,
    set_task_state,
    WorkflowBashOperator,
)

# See https://unpaywall.org/products/data-feed for details of available APIs
SNAPSHOT_URL = "https://api.unpaywall.org/feed/snapshot"
CHANGEFILES_URL = "https://api.unpaywall.org/feed/changefiles"
CHANGEFILES_DOWNLOAD_URL = "https://api.unpaywall.org/daily-feed/changefile"


class Changefile:
    def __init__(self, filename: str, changefile_date: pendulum.DateTime, changefile_release: ChangefileRelease = None):
        """Holds the metadata about a single Unpaywall changefile.

        :param filename: the name of the changefile.
        :param changefile_date: the date of the changefile.
        :param changefile_release: the ChangefileRelease object.
        """

        self.filename = filename
        self.changefile_date = changefile_date
        self.changefile_release = changefile_release

    def __eq__(self, other):
        if isinstance(other, Changefile):
            return self.filename == other.filename and self.changefile_date == other.changefile_date
        return False

    @staticmethod
    def from_dict(dict_: Dict) -> Changefile:
        filename = dict_["filename"]
        changefile_date = pendulum.parse(dict_["changefile_date"])
        return Changefile(filename, changefile_date)

    def to_dict(self) -> Dict:
        return dict(filename=self.filename, changefile_date=self.changefile_date.isoformat())

    @property
    def download_file_path(self):
        assert self.changefile_release is not None, "Changefile.download_file_path: self.changefile_release is None"
        return os.path.join(self.changefile_release.download_folder, self.filename)

    @property
    def extract_file_path(self):
        assert self.changefile_release is not None, "Changefile.extract_file_path: self.changefile_release is None"
        # Remove .gz
        return os.path.join(self.changefile_release.extract_folder, f"{self.filename[:-3]}")

    @property
    def transform_file_path(self):
        assert self.changefile_release is not None, "Changefile.transform_file_path: self.changefile_release is None"
        # Remove .gz
        return os.path.join(self.changefile_release.transform_folder, self.filename[:-3])


class UnpaywallRelease(Release):
    def __init__(
        self,
        *,
        dag_id: str,
        run_id: str,
        cloud_workspace: CloudWorkspace,
        bq_dataset_id: str,
        bq_table_name: str,
        is_first_run: bool,
        snapshot_date: pendulum.DateTime,
        start_date: pendulum.DateTime,
        end_date: pendulum.DateTime,
        changefiles: List[Changefile],
        prev_end_date: pendulum.DateTime,
    ):
        """Construct an UnpaywallRelease instance

        :param dag_id: the id of the DAG.
        :param run_id: the run id of the DAG.
        :param cloud_workspace: the CloudWorkspace instance.
        :param bq_dataset_id: the BigQuery dataset id.
        :param bq_table_name: the BigQuery table name.
        :param is_first_run: whether this is the first DAG run.
        :param snapshot_date: the date of the Unpaywall snapshot.
        :param start_date: the start date of the Unpaywall changefiles processed in this release.
        :param end_date: the end date of the Unpaywall changefiles processed in this release.
        :param changefiles: changefiles.
        :param prev_end_date: the previous end date.
        """

        super().__init__(
            dag_id=dag_id,
            run_id=run_id,
        )
        self.is_first_run = is_first_run
        self.snapshot_release = SnapshotRelease(dag_id=dag_id, run_id=run_id, snapshot_date=snapshot_date)
        self.changefile_release = ChangefileRelease(
            dag_id=dag_id,
            run_id=run_id,
            start_date=start_date,
            end_date=end_date,
        )
        self.changefiles = changefiles
        for changefile in changefiles:
            changefile.changefile_release = self.changefile_release
        self.prev_end_date = prev_end_date

        # Paths used during processing
        self.snapshot_download_file_path = os.path.join(
            self.snapshot_release.download_folder, "unpaywall_snapshot.jsonl.gz"
        )
        self.snapshot_extract_file_path = os.path.join(self.snapshot_release.extract_folder, "unpaywall_snapshot.jsonl")
        self.main_table_file_path = os.path.join(self.snapshot_release.transform_folder, "main_table.jsonl")
        self.upsert_table_file_path = os.path.join(self.changefile_release.transform_folder, "upsert_table.jsonl")

        # The final split files
        self.main_table_files_regex = r"^main_table\d{12}\.jsonl$"

        self.main_table_uri = gcs_blob_uri(
            cloud_workspace.transform_bucket,
            f"{gcs_blob_name_from_path(self.snapshot_release.transform_folder)}/main_table*.jsonl",
        )
        self.upsert_table_uri = gcs_blob_uri(
            cloud_workspace.transform_bucket, gcs_blob_name_from_path(self.upsert_table_file_path)
        )
        self.bq_main_table_id = bq_table_id(cloud_workspace.output_project_id, bq_dataset_id, bq_table_name)
        self.bq_upsert_table_id = bq_table_id(
            cloud_workspace.output_project_id, bq_dataset_id, f"{bq_table_name}_upsert"
        )
        self.bq_snapshot_table_id = bq_sharded_table_id(
            cloud_workspace.output_project_id,
            bq_dataset_id,
            f"{bq_table_name}_snapshot",
            self.prev_end_date,
        )


class UnpaywallTelescope(Workflow):
    def __init__(
        self,
        *,
        dag_id: str,
        cloud_workspace: CloudWorkspace,
        bq_dataset_id: str = "unpaywall",
        bq_table_name: str = "unpaywall",
        api_dataset_id: str = "unpaywall",
        schema_folder: str = os.path.join(default_schema_folder(), "unpaywall"),
        dataset_description: str = "Unpaywall Data Feed: https://unpaywall.org/products/data-feed",
        table_description: str = "Unpaywall Data Feed: https://unpaywall.org/products/data-feed",
        primary_key: str = "doi",
        snapshot_expiry_days: int = 7,
        http_header: str = None,
        unpaywall_conn_id: str = "unpaywall",
        observatory_api_conn_id: str = AirflowConns.OBSERVATORY_API,
        start_date: pendulum.DateTime = pendulum.datetime(2021, 7, 2),
        schedule_interval: str = "@daily",
    ):
        """The Unpaywall Data Feed Telescope.

        :param dag_id: the id of the DAG.
        :param cloud_workspace: the cloud workspace settings.
        :param bq_dataset_id: the BigQuery dataset id.
        :param bq_table_name: the BigQuery table name.
        :param api_dataset_id: the API dataset id.
        :param schema_folder: the schema folder.
        :param dataset_description: a description for the BigQuery dataset.
        :param table_description: a description for the table.
        :param primary_key: the primary key to use for merging changefiles.
        :param snapshot_expiry_days: the number of days to keep snapshots.
        :param http_header: the http header to use when making requests to Unpaywall.
        :param unpaywall_conn_id: Unpaywall connection key.
        :param observatory_api_conn_id: the Observatory API connection key.
        :param start_date: the start date of the DAG.
        :param schedule_interval: the schedule interval of the DAG.
        """

        super().__init__(
            dag_id=dag_id,
            start_date=start_date,
            schedule_interval=schedule_interval,
            catchup=False,
            airflow_conns=[observatory_api_conn_id, unpaywall_conn_id],
            tags=[Tag.academic_observatory],
        )

        if http_header is None:
            http_header = get_observatory_http_header(package_name="academic_observatory_workflows")

        self.cloud_workspace = cloud_workspace
        self.bq_dataset_id = bq_dataset_id
        self.bq_table_name = bq_table_name
        self.api_dataset_id = api_dataset_id
        self.schema_folder = schema_folder
        self.schema_file_path = bq_find_schema(path=self.schema_folder, table_name=self.bq_table_name)
        self.dataset_description = dataset_description
        self.table_description = table_description
        self.primary_key = primary_key
        self.snapshot_expiry_days = snapshot_expiry_days
        self.http_header = http_header
        self.unpaywall_conn_id = unpaywall_conn_id
        self.observatory_api_conn_id = observatory_api_conn_id

        # Wait for the previous DAG run to finish to make sure that
        # changefiles are processed in the correct order
        external_task_id = "dag_run_complete"
        self.add_operator(
            PreviousDagRunSensor(
                dag_id=self.dag_id,
                external_task_id=external_task_id,
                execution_delta=timedelta(days=1),  # To match the @daily schedule_interval
            )
        )
        self.add_setup_task(self.check_dependencies)
        self.add_setup_task(self.fetch_releases)
        self.add_task(self.create_datasets)
        self.add_task(self.bq_create_main_table_snapshot)

        # Download and process snapshot
        self.add_task(self.download_snapshot)
        self.add_task(self.upload_downloaded_snapshot)
        self.add_task(self.extract_snapshot)
        self.add_task(self.transform_snapshot)
        self.add_operator(
            WorkflowBashOperator(
                workflow=self,
                task_id="split_main_table_file",
                bash_command="{% if release.is_first_run %}cd {{ release.snapshot_release.transform_folder }} && split -C 4G --numeric-suffixes=1 --suffix-length=12 --additional-suffix=.jsonl main_table.jsonl main_table{% else %}echo 'Skipping split command because release.is_first_run is false'{% endif %}",
            )
        )
        self.add_task(self.upload_main_table_files)
        self.add_task(self.bq_load_main_table)

        # Download and process changefiles
        self.add_task(self.download_change_files)
        self.add_task(self.upload_downloaded_change_files)
        self.add_task(self.extract_change_files)
        self.add_task(self.transform_change_files)
        self.add_task(self.upload_upsert_files)
        self.add_task(self.bq_load_upsert_table)
        self.add_task(self.bq_upsert_records)

        # Add release info to API and cleanup
        self.add_task(self.add_new_dataset_releases)
        self.add_task(self.cleanup)

        # The last task that the next DAG run's ExternalTaskSensor waits for.
        self.add_operator(
            DummyOperator(
                task_id=external_task_id,
            )
        )

    @property
    def api_key(self) -> str:
        """The API key for accessing Unpaywall."""

        return get_airflow_connection_password(self.unpaywall_conn_id)

    def fetch_releases(self, **kwargs) -> bool:
        """Fetches the release information. On the first DAG run gets the latest snapshot and the necessary changefiles
        required to get the dataset up to date. On subsequent runs it fetches unseen changefiles. It is possible
        for no changefiles to be found after the first run, in which case the rest of the tasks are skipped. Publish
        any available releases as an XCOM to avoid re-querying Unpaywall servers.

        :param kwargs: the context passed from the Airflow Operator.
        See https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return: True to continue, False to skip.
        """

        dag_run = kwargs["dag_run"]
        releases = get_dataset_releases(dag_id=self.dag_id, dataset_id=self.api_dataset_id)

        # Get Unpaywall changefiles and sort from newest to oldest
        all_changefiles = get_unpaywall_changefiles(self.api_key)
        all_changefiles.sort(key=lambda c: c.changefile_date, reverse=True)

        logging.info(f"fetch_releases: {len(all_changefiles)} JSONL changefiles discovered")
        changefiles = []
        is_first_run = is_first_dag_run(dag_run)
        prev_end_date = pendulum.instance(datetime.datetime.min)

        if is_first_run:
            assert (
                len(releases) == 0
            ), "fetch_releases: there should be no DatasetReleases stored in the Observatory API on the first DAG run."

            # Get snapshot date as this is used to determine what changefile to get
            snapshot_file_name = get_snapshot_file_name(self.api_key)
            snapshot_date = unpaywall_filename_to_datetime(snapshot_file_name)

            # On first run, add changefiles from present until the changefile before the snapshot_date
            # As per Unpaywall changefiles documentation: https://unpaywall.org/products/data-feed/changefiles
            for changefile in all_changefiles:
                changefiles.append(changefile)
                if changefile.changefile_date < snapshot_date:
                    break

            # Assert that there is at least 1 changefile
            assert (
                len(changefiles) >= 1
            ), f"fetch_releases: there should be at least 1 changefile when loading a snapshot"
        else:
            assert (
                len(releases) >= 1
            ), f"fetch_releases: there should be at least 1 DatasetRelease in the Observatory API after the first DAG run"

            # On subsequent runs, fetch changefiles from after the previous changefile date
            prev_release = get_latest_dataset_release(releases, "changefile_end_date")
            snapshot_date = prev_release.snapshot_date  # so that we can easily see what snapshot is being used
            prev_end_date = prev_release.changefile_end_date
            for changefile in all_changefiles:
                if prev_end_date < changefile.changefile_date:
                    changefiles.append(changefile)

            # Sort from oldest to newest
            changefiles.sort(key=lambda c: c.changefile_date, reverse=False)

            if len(changefiles) == 0:
                logging.info(f"fetch_releases: no changefiles found, skipping")
                return False

        # Convert to JSON before printing and XComs
        changefiles = [changefile.to_dict() for changefile in changefiles]

        # Print summary information
        logging.info(f"is_first_run: {is_first_run}")
        logging.info(f"snapshot_date: {snapshot_date}")
        logging.info(f"changefiles: {changefiles}")
        logging.info(f"prev_end_date: {prev_end_date}")

        # Publish release information
        ti: TaskInstance = kwargs["ti"]
        msg = dict(
            snapshot_date=snapshot_date.isoformat(),
            changefiles=changefiles,
            is_first_run=is_first_run,
            prev_end_date=prev_end_date.isoformat(),
        )
        ti.xcom_push(self.RELEASE_INFO, msg, kwargs["logical_date"])

        return True

    def make_release(self, **kwargs) -> UnpaywallRelease:
        """Make a Release instance. Gets the list of releases available from the release check (setup task)."""

        ti: TaskInstance = kwargs["ti"]
        msg = ti.xcom_pull(key=self.RELEASE_INFO, task_ids=self.fetch_releases.__name__, include_prior_dates=False)
        snapshot_date, changefiles, is_first_run, prev_end_date = parse_release_msg(msg)
        run_id = kwargs["run_id"]

        # The first changefile is the oldest and the last one is the newest
        start_date = changefiles[0].changefile_date
        end_date = changefiles[-1].changefile_date

        release = UnpaywallRelease(
            dag_id=self.dag_id,
            run_id=run_id,
            cloud_workspace=self.cloud_workspace,
            bq_dataset_id=self.bq_dataset_id,
            bq_table_name=self.bq_table_name,
            is_first_run=is_first_run,
            snapshot_date=snapshot_date,
            start_date=start_date,
            end_date=end_date,
            changefiles=changefiles,
            prev_end_date=prev_end_date,
        )

        # Set changefile_release
        for changefile in changefiles:
            changefile.changefile_release = release.changefile_release

        return release

    def create_datasets(self, release: UnpaywallRelease, **kwargs) -> None:
        """Create datasets."""

        bq_create_dataset(
            project_id=self.cloud_workspace.output_project_id,
            dataset_id=self.bq_dataset_id,
            location=self.cloud_workspace.data_location,
            description=self.dataset_description,
        )

    def bq_create_main_table_snapshot(self, release: UnpaywallRelease, **kwargs) -> None:
        """Create a snapshot of the main table. The purpose of this table is to be able to rollback the table
        if something goes wrong. The snapshot expires after self.snapshot_expiry_days."""

        if release.is_first_run:
            logging.info(f"bq_create_main_table_snapshots: skipping as snapshots are not created on the first run")
            return

        expiry_date = pendulum.now().add(days=self.snapshot_expiry_days)
        success = bq_snapshot(
            src_table_id=release.bq_main_table_id, dst_table_id=release.bq_snapshot_table_id, expiry_date=expiry_date
        )
        set_task_state(success, self.bq_create_main_table_snapshot.__name__, release)

    ###############################################
    # Download and process snapshot on first run
    ###############################################

    def download_snapshot(self, release: UnpaywallRelease, **kwargs):
        """Downlaod the most recent Unpaywall snapshot."""

        if not release.is_first_run:
            logging.info("download_snapshot: skipping as this is only run on the first release")
            return

        # Clean all files
        clean_dir(release.snapshot_release.download_folder)

        # Download the most recent Unpaywall snapshot
        # Use a read buffer size of 8MiB as we are downloading a large file
        url = snapshot_url(self.api_key)
        success, download_info = download_file(
            url=url,
            headers=self.http_header,
            prefix_dir=release.snapshot_release.download_folder,
            read_buffer_size=2**23,
        )
        set_task_state(success, self.download_snapshot.__name__, release)

        # Assert that the date on the filename matches the snapshot date stored in the release object as there is a
        # small chance that the snapshot changed between when we collated the releases and when we downloaded the snapshot
        file_path = download_info.file_path
        file_name = os.path.basename(file_path)
        snapshot_date = unpaywall_filename_to_datetime(file_name)
        assert (
            release.snapshot_release.snapshot_date == snapshot_date
        ), f"download_snapshot: release snapshot_date {release.snapshot_release.snapshot_date} != snapshot_date of downloaded file {file_path}"

        # Rename file so that it is easier to deal with
        os.rename(file_path, release.snapshot_download_file_path)

    def upload_downloaded_snapshot(self, release: UnpaywallRelease, **kwargs):
        """Upload the downloaded snapshot for the given release."""

        if not release.is_first_run:
            logging.info("upload_downloaded_snapshot: skipping as this is only run on the first release")
            return

        success = gcs_upload_files(
            bucket_name=self.cloud_workspace.download_bucket, file_paths=[release.snapshot_download_file_path]
        )
        set_task_state(success, self.upload_downloaded_snapshot.__name__, release)

    def extract_snapshot(self, release: UnpaywallRelease, **kwargs):
        """Gunzip the downloaded Unpaywall snapshot."""

        if not release.is_first_run:
            logging.info("extract_snapshot: skipping as this is only run on the first release")
            return

        clean_dir(release.snapshot_release.extract_folder)

        gunzip_files(
            file_list=[release.snapshot_download_file_path], output_dir=release.snapshot_release.extract_folder
        )

    def transform_snapshot(self, release: UnpaywallRelease, **kwargs):
        """Transform the snapshot into the main table file. Find and replace the 'authenticated-orcid' string in the
        jsonl to 'authenticated_orcid'."""

        if not release.is_first_run:
            logging.info("transform_snapshot: skipping as this is only run on the first release")
            return

        clean_dir(release.snapshot_release.transform_folder)

        # Transform data
        logging.info(f"transform_snapshot: find_replace_file")
        find_replace_file(
            src=release.snapshot_extract_file_path,
            dst=release.main_table_file_path,
            pattern="authenticated-orcid",
            replacement="authenticated_orcid",
        )

    def upload_main_table_files(self, release: UnpaywallRelease, **kwargs) -> None:
        """Upload the main table files to Cloud Storage."""

        if not release.is_first_run:
            logging.info("upload_main_table_file: skipping as this is only run on the first release")
            return

        files_list = list_files(release.snapshot_release.transform_folder, release.main_table_files_regex)
        success = gcs_upload_files(bucket_name=self.cloud_workspace.transform_bucket, file_paths=files_list)
        set_task_state(success, self.upload_main_table_files.__name__, release)

    def bq_load_main_table(self, release: UnpaywallRelease, **kwargs) -> None:
        """Load the main table."""

        if not release.is_first_run:
            logging.info("bq_load_main_table: skipping as this is only run on the first release")
            return

        success = bq_load_table(
            uri=release.main_table_uri,
            table_id=release.bq_main_table_id,
            schema_file_path=self.schema_file_path,
            source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
            table_description=self.table_description,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            ignore_unknown_values=True,
        )
        set_task_state(success, self.bq_load_upsert_table.__name__, release)

    ################################################
    # Download and process change files on each run
    ################################################

    def download_change_files(self, release: UnpaywallRelease, **kwargs):
        """Download the Unpaywall change files that are required for this release."""

        clean_dir(release.changefile_release.download_folder)

        download_list = []
        for changefile in release.changefiles:
            url = changefile_download_url(changefile.filename, self.api_key)
            # TODO: it is a bit confusing that you have to set prefix_dir and filename, but can't just directly set filepath
            download_list.append(
                DownloadInfo(
                    url=url,
                    filename=changefile.filename,
                    prefix_dir=release.changefile_release.download_folder,
                    retry=True,
                )
            )
        download_files(download_list=download_list, headers=self.http_header)

    def upload_downloaded_change_files(self, release: UnpaywallRelease, **kwargs):
        """Upload the downloaded changefiles for the given release."""

        files_list = [changefile.download_file_path for changefile in release.changefiles]
        success = gcs_upload_files(bucket_name=self.cloud_workspace.download_bucket, file_paths=files_list)
        set_task_state(success, self.upload_downloaded_change_files.__name__, release)

    def extract_change_files(self, release: UnpaywallRelease, **kwargs):
        """Task to gunzip the downloaded Unpaywall changefiles."""

        clean_dir(release.changefile_release.extract_folder)

        files_list = [changefile.download_file_path for changefile in release.changefiles]
        logging.info(f"extracting changefiles: {files_list}")
        gunzip_files(file_list=files_list, output_dir=release.changefile_release.extract_folder)

    def transform_change_files(self, release: UnpaywallRelease, **kwargs):
        """Task to transform the Unpaywall changefiles merging them into the upsert file.
        Find and replace the 'authenticated-orcid' string in the jsonl to 'authenticated_orcid'."""

        clean_dir(release.changefile_release.transform_folder)

        logging.info(
            "transform_change_files: find and replace the 'authenticated-orcid' string in the jsonl to 'authenticated_orcid'"
        )
        for changefile in release.changefiles:
            find_replace_file(
                src=changefile.extract_file_path,
                dst=changefile.transform_file_path,
                pattern="authenticated-orcid",
                replacement="authenticated_orcid",
            )

        logging.info(
            "transform_change_files: Merge change files, make sure that we process them from the oldest changefile to the newest"
        )
        # Make sure changefiles are sorted from oldest to newest, just in case they were not sorted for some reason
        changefiles = sorted(release.changefiles, key=lambda c: c.changefile_date, reverse=False)
        transform_files = [changefile.transform_file_path for changefile in changefiles]
        merge_update_files(
            primary_key=self.primary_key, input_files=transform_files, output_file=release.upsert_table_file_path
        )

    def upload_upsert_files(self, release: UnpaywallRelease, **kwargs) -> None:
        """Upload the transformed data to Cloud Storage.
        :raises AirflowException: Raised if the files to be uploaded are not found."""

        success = gcs_upload_files(
            bucket_name=self.cloud_workspace.transform_bucket, file_paths=[release.upsert_table_file_path]
        )
        set_task_state(success, self.upload_upsert_files.__name__, release)

    def bq_load_upsert_table(self, release: UnpaywallRelease, **kwargs) -> None:
        """Load the upsert table."""

        # Will overwrite any existing upsert table
        success = bq_load_table(
            uri=release.upsert_table_uri,
            table_id=release.bq_upsert_table_id,
            schema_file_path=self.schema_file_path,
            source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
            table_description=self.table_description,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            ignore_unknown_values=True,
        )
        set_task_state(success, self.bq_load_upsert_table.__name__, release)

    def bq_upsert_records(self, release: UnpaywallRelease, **kwargs) -> None:
        """Upsert the records from the upserts table into the main table."""

        bq_upsert_records(
            main_table_id=release.bq_main_table_id,
            upsert_table_id=release.bq_upsert_table_id,
            primary_key=self.primary_key,
        )

    def add_new_dataset_releases(self, release: UnpaywallRelease, **kwargs) -> None:
        """Adds release information to API."""

        dataset_release = DatasetRelease(
            dag_id=self.dag_id,
            dataset_id=self.api_dataset_id,
            dag_run_id=release.run_id,
            snapshot_date=release.snapshot_release.snapshot_date,
            changefile_start_date=release.changefile_release.start_date,
            changefile_end_date=release.changefile_release.end_date,
        )
        api = make_observatory_api(observatory_api_conn_id=self.observatory_api_conn_id)
        api.post_dataset_release(dataset_release)

    def cleanup(self, release: UnpaywallRelease, **kwargs) -> None:
        """Delete all files, folders and XComs associated with this release."""

        cleanup(dag_id=self.dag_id, execution_date=kwargs["logical_date"], workflow_folder=release.workflow_folder)


def parse_release_msg(msg: Dict):
    snapshot_date = pendulum.parse(msg["snapshot_date"])
    changefiles = [Changefile.from_dict(changefile) for changefile in msg["changefiles"]]
    is_first_run = msg["is_first_run"]
    prev_end_date = pendulum.parse(msg["prev_end_date"])

    return snapshot_date, changefiles, is_first_run, prev_end_date


def snapshot_url(api_key: str) -> str:
    """Snapshot URL"""

    return f"{SNAPSHOT_URL}?api_key={api_key}"


def get_snapshot_file_name(api_key: str) -> str:
    """Get the Unpaywall snapshot filename.

    :return: Snapshot file date.
    """

    url = snapshot_url(api_key)
    return get_filename_from_http_header(url)


def changefiles_url(api_key: str) -> str:
    """Data Feed URL"""

    return f"{CHANGEFILES_URL}?interval=day&api_key={api_key}"


def changefile_download_url(filename: str, api_key: str):
    return f"{CHANGEFILES_DOWNLOAD_URL}/{filename}?api_key={api_key}"


def get_unpaywall_changefiles(api_key: str) -> List[Changefile]:
    url = changefiles_url(api_key)
    response = get_http_response_json(url)
    changefiles = []

    # Only include jsonl files, parse date and strip out api key
    for changefile in response["list"]:
        filetype = changefile["filetype"]
        if filetype == "jsonl":
            filename = changefile["filename"]
            changefiles.append(Changefile(filename, unpaywall_filename_to_datetime(filename)))

    # Make sure sorted from oldest to newest
    changefiles.sort(key=lambda c: c.changefile_date, reverse=False)

    return changefiles


def unpaywall_filename_to_datetime(file_name: str) -> pendulum.DateTime:
    """Parses a release date from a file name.

    :param file_name: Unpaywall release file name (contains date string).
    :return: date.
    """

    date = re.search(r"\d{4}-\d{2}-\d{2}(T\d{6})?", file_name).group()
    return pendulum.parse(date)
