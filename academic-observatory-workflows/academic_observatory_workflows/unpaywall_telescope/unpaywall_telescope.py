# Copyright 2020-2024 Curtin University
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
from typing import Dict, List

import pendulum
from airflow import DAG
from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from google.cloud import bigquery
from google.cloud.bigquery import SourceFormat

from academic_observatory_workflows.config import project_path
from observatory.api.client.model.dataset_release import DatasetRelease
from observatory.platform.airflow import (
    get_airflow_connection_password,
    is_first_dag_run,
    on_failure_callback,
    PreviousDagRunSensor,
)
from observatory.platform.api import get_dataset_releases, get_latest_dataset_release, make_observatory_api
from observatory.platform.bigquery import (
    bq_create_dataset,
    bq_find_schema,
    bq_load_table,
    bq_sharded_table_id,
    bq_snapshot,
    bq_table_id,
    bq_upsert_records,
)
from observatory.platform.config import AirflowConns
from observatory.platform.files import clean_dir, find_replace_file, gunzip_files, list_files, merge_update_files
from observatory.platform.gcs import gcs_blob_name_from_path, gcs_blob_uri, gcs_upload_files
from observatory.platform.observatory_config import CloudWorkspace
from observatory.platform.refactor.tasks import check_dependencies
from observatory.platform.utils.http_download import download_file, download_files, DownloadInfo
from observatory.platform.utils.url_utils import (
    get_filename_from_http_header,
    get_http_response_json,
    get_observatory_http_header,
)
from observatory.platform.workflows.workflow import (
    ChangefileRelease,
    cleanup,
    Release,
    SnapshotRelease,
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
        :param changefiles: a list of Changefile objects.
        :param prev_end_date: the previous end date.
        """

        super().__init__(
            dag_id=dag_id,
            run_id=run_id,
        )
        self.cloud_workspace = cloud_workspace
        self.bq_dataset_id = bq_dataset_id
        self.bq_table_name = bq_table_name
        self.is_first_run = is_first_run
        self.snapshot_date = snapshot_date
        self.snapshot_release = SnapshotRelease(dag_id=dag_id, run_id=run_id, snapshot_date=snapshot_date)

        # The first changefile is the oldest and the last one is the newest
        # the start and end date of the Unpaywall changefiles processed in this release.
        changefiles = sorted(changefiles, key=lambda c: c.changefile_date, reverse=False)
        start_date = changefiles[0].changefile_date
        end_date = changefiles[-1].changefile_date

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

    @staticmethod
    def from_dict(dict_: dict) -> UnpaywallRelease:
        return UnpaywallRelease(
            dag_id=dict_["dag_id"],
            run_id=dict_["run_id"],
            cloud_workspace=CloudWorkspace.from_dict(dict_["cloud_workspace"]),
            bq_dataset_id=dict_["bq_dataset_id"],
            bq_table_name=dict_["bq_table_name"],
            is_first_run=dict_["is_first_run"],
            snapshot_date=pendulum.parse(dict_["snapshot_date"]),
            changefiles=[Changefile.from_dict(changefile) for changefile in dict_["changefiles"]],
            prev_end_date=pendulum.parse(dict_["prev_end_date"]),
        )

    def to_dict(self) -> dict:
        return dict(
            dag_id=self.dag_id,
            run_id=self.run_id,
            cloud_workspace=self.cloud_workspace.to_dict(),
            bq_dataset_id=self.bq_dataset_id,
            bq_table_name=self.bq_table_name,
            is_first_run=self.is_first_run,
            snapshot_date=self.snapshot_date.isoformat(),
            changefiles=[changefile.to_dict() for changefile in self.changefiles],
            prev_end_date=self.prev_end_date.isoformat(),
        )


def create_dag(
    *,
    dag_id: str,
    cloud_workspace: CloudWorkspace,
    bq_dataset_id: str = "unpaywall",
    bq_table_name: str = "unpaywall",
    api_dataset_id: str = "unpaywall",
    schema_folder: str = project_path("unpaywall_telescope", "schema"),
    dataset_description: str = "Unpaywall Data Feed: https://unpaywall.org/products/data-feed",
    table_description: str = "Unpaywall Data Feed: https://unpaywall.org/products/data-feed",
    primary_key: str = "doi",
    snapshot_expiry_days: int = 7,
    http_header: str = None,
    unpaywall_conn_id: str = "unpaywall",
    observatory_api_conn_id: str = AirflowConns.OBSERVATORY_API,
    start_date: pendulum.DateTime = pendulum.datetime(2021, 7, 2),
    schedule: str = "@daily",
    max_active_runs: int = 1,
    retries: int = 3,
) -> DAG:
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
    :param schedule: the schedule interval of the DAG.
    :param max_active_runs: the maximum number of DAG runs that can be run at once.
    :param retries: the number of times to retry a task.
    """

    if http_header is None:
        http_header = get_observatory_http_header(package_name="academic_observatory_workflows")

    schema_file_path = bq_find_schema(path=schema_folder, table_name=bq_table_name)

    @dag(
        dag_id=dag_id,
        start_date=start_date,
        schedule=schedule,
        catchup=False,
        max_active_runs=max_active_runs,
        tags=["academic-observatory"],
        default_args={
            "owner": "airflow",
            "on_failure_callback": on_failure_callback,
            "retries": retries,
        },
    )
    def unpaywall():
        @task
        def fetch_release(**context) -> dict | None:
            """Fetches the release information. On the first DAG run gets the latest snapshot and the necessary changefiles
            required to get the dataset up to date. On subsequent runs it fetches unseen changefiles. It is possible
            for no changefiles to be found after the first run, in which case the rest of the tasks are skipped. Publish
            any available releases as an XCOM to avoid re-querying Unpaywall servers."""

            dag_run = context["dag_run"]
            releases = get_dataset_releases(dag_id=dag_id, dataset_id=api_dataset_id)

            # Get Unpaywall changefiles and sort from newest to oldest
            api_key = get_airflow_connection_password(unpaywall_conn_id)
            all_changefiles = get_unpaywall_changefiles(api_key)
            all_changefiles.sort(key=lambda c: c.changefile_date, reverse=True)

            logging.info(f"fetch_release: {len(all_changefiles)} JSONL changefiles discovered")
            changefiles = []
            is_first_run = is_first_dag_run(dag_run)
            prev_end_date = pendulum.instance(datetime.datetime.min)

            if is_first_run:
                assert (
                    len(releases) == 0
                ), "fetch_release: there should be no DatasetReleases stored in the Observatory API on the first DAG run."

                # Get snapshot date as this is used to determine what changefile to get
                snapshot_file_name = get_snapshot_file_name(api_key)
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
                ), f"fetch_release: there should be at least 1 changefile when loading a snapshot"
            else:
                assert (
                    len(releases) >= 1
                ), f"fetch_release: there should be at least 1 DatasetRelease in the Observatory API after the first DAG run"

                # On subsequent runs, fetch changefiles from after the previous changefile date
                prev_release = get_latest_dataset_release(releases, "changefile_end_date")
                snapshot_date = pendulum.instance(
                    prev_release.snapshot_date
                )  # so that we can easily see what snapshot is being used
                prev_end_date = pendulum.instance(prev_release.changefile_end_date)
                for changefile in all_changefiles:
                    if prev_end_date < changefile.changefile_date:
                        changefiles.append(changefile)

                # Sort from oldest to newest
                changefiles.sort(key=lambda c: c.changefile_date, reverse=False)

                if len(changefiles) == 0:
                    msg = "fetch_release: no changefiles found, skipping"
                    logging.info(msg)
                    return

            # Print summary information
            logging.info(f"is_first_run: {is_first_run}")
            logging.info(f"snapshot_date: {snapshot_date}")
            logging.info(f"changefiles: {changefiles}")
            logging.info(f"prev_end_date: {prev_end_date}")

            return UnpaywallRelease(
                dag_id=dag_id,
                run_id=context["run_id"],
                cloud_workspace=cloud_workspace,
                bq_dataset_id=bq_dataset_id,
                bq_table_name=bq_table_name,
                is_first_run=is_first_run,
                snapshot_date=snapshot_date,
                changefiles=changefiles,
                prev_end_date=prev_end_date,
            ).to_dict()

        @task.short_circuit
        def short_circuit(release: dict | None, **context) -> bool:
            return release is not None

        @task
        def create_dataset(release: dict, **context) -> None:
            """Create datasets."""

            bq_create_dataset(
                project_id=cloud_workspace.output_project_id,
                dataset_id=bq_dataset_id,
                location=cloud_workspace.data_location,
                description=dataset_description,
            )

        @task
        def bq_create_main_table_snapshot(release: dict, **context) -> None:
            """Create a snapshot of the main table. The purpose of this table is to be able to rollback the table
            if something goes wrong. The snapshot expires after snapshot_expiry_days."""

            release = UnpaywallRelease.from_dict(release)
            if release.is_first_run:
                msg = f"bq_create_main_table_snapshots: skipping as snapshots are not created on the first run"
                logging.info(msg)
                return
                # raise AirflowSkipException(msg)

            expiry_date = pendulum.now().add(days=snapshot_expiry_days)
            success = bq_snapshot(
                src_table_id=release.bq_main_table_id,
                dst_table_id=release.bq_snapshot_table_id,
                expiry_date=expiry_date,
            )
            if not success:
                raise AirflowException("bq_create_main_table_snapshot: failed to create BigQuery snapshot")

        @task.branch
        def branch(release: dict, **context):
            release = UnpaywallRelease.from_dict(release)
            if release.is_first_run:
                return "load_snapshot.download"
            else:
                return "load_changefiles.download"

        @task_group
        def load_snapshot(data: dict):
            """Download and process snapshot on first run"""

            @task
            def download(release: dict, **context):
                """Downlaod the most recent Unpaywall snapshot."""

                # Clean all files
                release = UnpaywallRelease.from_dict(release)
                clean_dir(release.snapshot_release.download_folder)

                # Download the most recent Unpaywall snapshot
                # Use a read buffer size of 8MiB as we are downloading a large file
                api_key = get_airflow_connection_password(unpaywall_conn_id)
                url = snapshot_url(api_key)
                success, download_info = download_file(
                    url=url,
                    headers=http_header,
                    prefix_dir=release.snapshot_release.download_folder,
                    read_buffer_size=2**23,
                )
                if not success:
                    raise AirflowException("download: failed to download snapshot")

                # Assert that the date on the filename matches the snapshot date stored in the release object as there is a
                # small chance that the snapshot changed between when we collated the releases and when we downloaded the snapshot
                file_path = download_info.file_path
                file_name = os.path.basename(file_path)
                snapshot_date = unpaywall_filename_to_datetime(file_name)
                assert (
                    release.snapshot_release.snapshot_date == snapshot_date
                ), f"download: release snapshot_date {release.snapshot_release.snapshot_date} != snapshot_date of downloaded file {file_path}"

                # Rename file so that it is easier to deal with
                os.rename(file_path, release.snapshot_download_file_path)

            @task
            def upload_downloaded(release: dict, **context):
                """Upload the downloaded snapshot for the given release."""

                release = UnpaywallRelease.from_dict(release)
                success = gcs_upload_files(
                    bucket_name=cloud_workspace.download_bucket, file_paths=[release.snapshot_download_file_path]
                )
                if not success:
                    raise AirflowException("gcs_upload_files: failed to upload snapshot")

            @task
            def extract(release: dict, **context):
                """Gunzip the downloaded Unpaywall snapshot."""

                release = UnpaywallRelease.from_dict(release)
                clean_dir(release.snapshot_release.extract_folder)
                gunzip_files(
                    file_list=[release.snapshot_download_file_path], output_dir=release.snapshot_release.extract_folder
                )

            @task
            def transform(release: dict, **context):
                """Transform the snapshot into the main table file. Find and replace the 'authenticated-orcid' string in the
                jsonl to 'authenticated_orcid'."""

                release = UnpaywallRelease.from_dict(release)
                clean_dir(release.snapshot_release.transform_folder)

                # Transform data
                logging.info(f"transform: find_replace_file")
                find_replace_file(
                    src=release.snapshot_extract_file_path,
                    dst=release.main_table_file_path,
                    pattern="authenticated-orcid",
                    replacement="authenticated_orcid",
                )

            @task
            def split_main_table_file(release: dict, **context):
                """Split main table into multiple smaller files"""

                release = UnpaywallRelease.from_dict(release)
                op = BashOperator(
                    task_id="split_main_table_file",
                    bash_command=f"cd { release.snapshot_release.transform_folder } && split -C 4G --numeric-suffixes=1 --suffix-length=12 --additional-suffix=.jsonl main_table.jsonl main_table",
                    do_xcom_push=False,
                )
                op.execute(context)

            @task
            def upload_main_table_files(release: dict, **context) -> None:
                """Upload the main table files to Cloud Storage."""

                release = UnpaywallRelease.from_dict(release)
                files_list = list_files(release.snapshot_release.transform_folder, release.main_table_files_regex)
                success = gcs_upload_files(bucket_name=cloud_workspace.transform_bucket, file_paths=files_list)
                if not success:
                    raise AirflowException(f"upload_main_table_files: failed to upload main table files")

            @task
            def bq_load(release: dict, **context) -> None:
                """Load the main table."""

                release = UnpaywallRelease.from_dict(release)
                success = bq_load_table(
                    uri=release.main_table_uri,
                    table_id=release.bq_main_table_id,
                    schema_file_path=schema_file_path,
                    source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
                    table_description=table_description,
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                    ignore_unknown_values=True,
                )
                if not success:
                    raise AirflowException("bq_load: failed to load main table")

            task_download = download(data)
            task_upload_downloaded = upload_downloaded(data)
            task_extract = extract(data)
            task_transform = transform(data)
            task_split_main_table_file = split_main_table_file(data)
            task_upload_main_table_files = upload_main_table_files(data)
            task_bq_load = bq_load(data)

            (
                task_download
                >> task_upload_downloaded
                >> task_extract
                >> task_transform
                >> task_split_main_table_file
                >> task_upload_main_table_files
                >> task_bq_load
            )

        @task_group
        def load_changefiles(data: dict):
            """Download and process change files on each run"""

            @task(trigger_rule=TriggerRule.ALL_DONE)
            def download(release: dict, **context):
                """Download the Unpaywall change files that are required for this release."""

                release = UnpaywallRelease.from_dict(release)
                clean_dir(release.changefile_release.download_folder)

                download_list = []
                api_key = get_airflow_connection_password(unpaywall_conn_id)
                for changefile in release.changefiles:
                    url = changefile_download_url(changefile.filename, api_key)
                    # TODO: it is a bit confusing that you have to set prefix_dir and filename, but can't just directly set filepath
                    download_list.append(
                        DownloadInfo(
                            url=url,
                            filename=changefile.filename,
                            prefix_dir=release.changefile_release.download_folder,
                            retry=True,
                        )
                    )
                download_files(download_list=download_list, headers=http_header)

            @task
            def upload_downloaded(release: dict, **context):
                """Upload the downloaded changefiles for the given release."""

                release = UnpaywallRelease.from_dict(release)
                files_list = [changefile.download_file_path for changefile in release.changefiles]
                success = gcs_upload_files(bucket_name=cloud_workspace.download_bucket, file_paths=files_list)
                if not success:
                    raise AirflowException("upload_downloaded: failed to upload downloaded changefiles")

            @task
            def extract(release: dict, **context):
                """Task to gunzip the downloaded Unpaywall changefiles."""

                release = UnpaywallRelease.from_dict(release)
                clean_dir(release.changefile_release.extract_folder)
                files_list = [changefile.download_file_path for changefile in release.changefiles]
                logging.info(f"extracting changefiles: {files_list}")
                gunzip_files(file_list=files_list, output_dir=release.changefile_release.extract_folder)

            @task
            def transform(release: dict, **context):
                """Task to transform the Unpaywall changefiles merging them into the upsert file.
                Find and replace the 'authenticated-orcid' string in the jsonl to 'authenticated_orcid'."""

                release = UnpaywallRelease.from_dict(release)
                clean_dir(release.changefile_release.transform_folder)

                logging.info(
                    "transform: find and replace the 'authenticated-orcid' string in the jsonl to 'authenticated_orcid'"
                )
                for changefile in release.changefiles:
                    with open(changefile.extract_file_path, "r") as f_in, open(
                        changefile.transform_file_path, "w"
                    ) as f_out:
                        for line in f_in:
                            if line.strip() != "null":
                                output = re.sub(pattern="authenticated-orcid", repl="authenticated_orcid", string=line)
                                f_out.write(output)

                logging.info(
                    "transform: Merge change files, make sure that we process them from the oldest changefile to the newest"
                )
                # Make sure changefiles are sorted from oldest to newest, just in case they were not sorted for some reason
                changefiles = sorted(release.changefiles, key=lambda c: c.changefile_date, reverse=False)
                transform_files = [changefile.transform_file_path for changefile in changefiles]
                merge_update_files(
                    primary_key=primary_key, input_files=transform_files, output_file=release.upsert_table_file_path
                )

            @task
            def upload(release: dict, **context) -> None:
                """Upload the transformed data to Cloud Storage.
                :raises AirflowException: Raised if the files to be uploaded are not found."""

                release = UnpaywallRelease.from_dict(release)
                success = gcs_upload_files(
                    bucket_name=cloud_workspace.transform_bucket, file_paths=[release.upsert_table_file_path]
                )
                if not success:
                    raise AirflowException("upload: failed to upload upsert files")

            @task
            def bq_load(release: dict, **context) -> None:
                """Load the upsert table."""

                # Will overwrite any existing upsert table
                release = UnpaywallRelease.from_dict(release)
                success = bq_load_table(
                    uri=release.upsert_table_uri,
                    table_id=release.bq_upsert_table_id,
                    schema_file_path=schema_file_path,
                    source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
                    table_description=table_description,
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                    ignore_unknown_values=True,
                )
                if not success:
                    raise AirflowException("bq_load: failed to load upsert table")

            @task
            def bq_upsert(release: dict, **context) -> None:
                """Upsert the records from the upserts table into the main table."""

                release = UnpaywallRelease.from_dict(release)
                bq_upsert_records(
                    main_table_id=release.bq_main_table_id,
                    upsert_table_id=release.bq_upsert_table_id,
                    primary_key=primary_key,
                )

            task_download = download(data)
            task_upload_downloaded = upload_downloaded(data)
            task_extract = extract(data)
            task_transform = transform(data)
            task_upload = upload(data)
            task_bq_load = bq_load(data)
            task_bq_upsert = bq_upsert(data)

            (
                task_download
                >> task_upload_downloaded
                >> task_extract
                >> task_transform
                >> task_upload
                >> task_bq_load
                >> task_bq_upsert
            )

        @task
        def add_dataset_release(release: dict, **context) -> None:
            """Adds release information to API."""

            release = UnpaywallRelease.from_dict(release)
            dataset_release = DatasetRelease(
                dag_id=dag_id,
                dataset_id=api_dataset_id,
                dag_run_id=release.run_id,
                snapshot_date=release.snapshot_release.snapshot_date,
                changefile_start_date=release.changefile_release.start_date,
                changefile_end_date=release.changefile_release.end_date,
            )
            api = make_observatory_api(observatory_api_conn_id=observatory_api_conn_id)
            api.post_dataset_release(dataset_release)

        @task
        def cleanup_workflow(release: dict, **context) -> None:
            """Delete all files, folders and XComs associated with this release."""

            release = UnpaywallRelease.from_dict(release)
            cleanup(dag_id=dag_id, execution_date=context["logical_date"], workflow_folder=release.workflow_folder)

        # Wait for the previous DAG run to finish to make sure that
        # changefiles are processed in the correct order
        external_task_id = "dag_run_complete"
        sensor = PreviousDagRunSensor(
            dag_id=dag_id,
            external_task_id=external_task_id,
        )
        task_check_dependencies = check_dependencies(airflow_conns=[observatory_api_conn_id, unpaywall_conn_id])
        xcom_release = fetch_release()
        task_short_circuit = short_circuit(xcom_release)
        task_create_dataset = create_dataset(xcom_release)
        task_bq_create_main_table_snapshot = bq_create_main_table_snapshot(xcom_release)
        task_branch = branch(xcom_release)
        task_group_load_snapshot = load_snapshot(xcom_release)
        task_group_load_changefiles = load_changefiles(xcom_release)
        task_add_dataset_release = add_dataset_release(xcom_release)
        task_cleanup_workflow = cleanup_workflow(xcom_release)
        # The last task that the next DAG run's ExternalTaskSensor waits for.
        task_dag_run_complete = EmptyOperator(
            task_id=external_task_id,
        )

        (
            sensor
            >> task_check_dependencies
            >> xcom_release
            >> task_short_circuit
            >> task_create_dataset
            >> task_bq_create_main_table_snapshot
            >> task_branch
            >> task_group_load_snapshot
            >> task_group_load_changefiles
            >> task_add_dataset_release
            >> task_cleanup_workflow
            >> task_dag_run_complete
        )

        task_branch >> task_group_load_changefiles

    return unpaywall()


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
