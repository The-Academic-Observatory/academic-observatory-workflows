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
import requests
from typing import List
from urllib.parse import urlparse

import pendulum
from airflow.exceptions import AirflowException
from airflow.models import DagRun
from airflow.operators.bash import BashOperator
from google.cloud import bigquery
from google.cloud.bigquery import SourceFormat

from academic_observatory_workflows.unpaywall_telescope.release import Changefile, UnpaywallRelease
from observatory_platform.airflow.airflow import get_airflow_connection_password, is_first_dag_run
from observatory_platform.airflow.release import release_from_bucket, release_to_bucket
from observatory_platform.airflow.workflow import cleanup, CloudWorkspace
from observatory_platform.dataset_api import DatasetAPI, DatasetRelease
from observatory_platform.files import clean_dir, find_replace_file, gunzip_files, list_files, merge_update_files
from observatory_platform.google.bigquery import bq_load_table, bq_snapshot, bq_upsert_records
from observatory_platform.google.gcs import gcs_upload_files
from observatory_platform.http_download import download_file, download_files, DownloadInfo
from observatory_platform.url_utils import get_http_response_json

# See https://unpaywall.org/products/data-feed for details of available APIs
UNPAYWALL_BASE_URL = "https://api.unpaywall.org"
SNAPSHOT_URL = "https://api.unpaywall.org/feed/snapshot"
CHANGEFILES_URL = "https://api.unpaywall.org/feed/changefiles"
CHANGEFILES_DOWNLOAD_URL = "https://api.unpaywall.org/daily-feed/changefile"


def fetch_release(
    dag_id: str,
    run_id: str,
    dag_run: DagRun,
    cloud_workspace: CloudWorkspace,
    bq_dataset_id: str,
    bq_table_name: str,
    api_bq_dataset_id: str,
    unpaywall_conn_id: str,
    base_url: str,
) -> str | None:
    """Fetches the release information. On the first DAG run gets the latest snapshot and the necessary changefiles
    required to get the dataset up to date. On subsequent runs it fetches unseen changefiles. It is possible
    for no changefiles to be found after the first run, in which case the rest of the tasks are skipped.

    Release will be published to bigquery with a unique identifier.

    :param dag_id: The ID of the dag running
    :param run_id: The ID of the dag run
    :param dag_run: The DagRun object
    :param cloud_workspace: The CloudWorkspace Object
    :param bq_dataset_id: The bigquery dataset id
    :param bq_table_name: The bigquery table name
    :param api_bq_dataset_id: The name of the api dataset
    :param unpaywall_conn_id: The airflow connection ID for unpaywall
    :param base_url: The unpaywall base url
    :return: None if there are no release files to be processed, otherwise the release ID
    """

    api = DatasetAPI(bq_project_id=cloud_workspace.project_id, bq_dataset_id=api_bq_dataset_id)
    prev_release = api.get_dataset_releases(dag_id=dag_id, entity_id="unpaywall", limit=1)

    # Get Unpaywall changefiles and sort from newest to oldest
    api_key = get_airflow_connection_password(unpaywall_conn_id)

    all_changefiles = get_unpaywall_changefiles(changefiles_url(base_url, api_key))
    all_changefiles.sort(key=lambda c: c.changefile_date, reverse=True)

    logging.info(f"fetch_release: {len(all_changefiles)} JSONL changefiles discovered")
    changefiles = []
    is_first_run = is_first_dag_run(dag_run)
    prev_end_date = pendulum.instance(datetime.datetime.min)

    if is_first_run:
        assert (
            len(prev_release) == 0
        ), "fetch_release: there should be no DatasetReleases stored in the Observatory API on the first DAG run."

        # Get snapshot date as this is used to determine what changefile to get
        snapshot_file_name = get_snapshot_file_name(base_url, api_key)
        snapshot_date = unpaywall_filename_to_datetime(snapshot_file_name)

        # On first run, add changefiles from present until the changefile before the snapshot_date
        # As per Unpaywall changefiles documentation: https://unpaywall.org/products/data-feed/changefiles
        for changefile in all_changefiles:
            changefiles.append(changefile)
            if changefile.changefile_date < snapshot_date:
                break

        # Assert that there is at least 1 changefile
        assert len(changefiles) >= 1, f"fetch_release: there should be at least 1 changefile when loading a snapshot"
    else:
        assert (
            len(prev_release) >= 1
        ), f"fetch_release: there should be at least 1 DatasetRelease in the Observatory API after the first DAG run"

        # On subsequent runs, fetch changefiles from after the previous changefile date
        snapshot_date = pendulum.instance(
            prev_release[0].snapshot_date
        )  # so that we can easily see what snapshot is being used
        prev_end_date = pendulum.instance(prev_release[0].changefile_end_date)
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

    id = release_to_bucket(
        UnpaywallRelease(
            dag_id=dag_id,
            run_id=run_id,
            cloud_workspace=cloud_workspace,
            bq_dataset_id=bq_dataset_id,
            bq_table_name=bq_table_name,
            is_first_run=is_first_run,
            snapshot_date=snapshot_date,
            changefiles=changefiles,
            prev_end_date=prev_end_date,
        ).to_dict(),
        cloud_workspace.download_bucket,
    )
    return id


def bq_create_main_table_snapshot(release_id: str, cloud_workspace: CloudWorkspace, snapshot_expiry_days: int) -> None:
    """Create a snapshot of the main table. The purpose of this table is to be able to rollback the table
    if something goes wrong. The snapshot expires after snapshot_expiry_days."""

    release = release_from_bucket(cloud_workspace.download_bucket, release_id)
    release = UnpaywallRelease.from_dict(release)
    if release.is_first_run:
        msg = f"bq_create_main_table_snapshots: skipping as snapshots are not created on the first run"
        logging.info(msg)
        return

    expiry_date = pendulum.now().add(days=snapshot_expiry_days)
    success = bq_snapshot(
        src_table_id=release.bq_main_table_id,
        dst_table_id=release.bq_snapshot_table_id,
        expiry_date=expiry_date,
    )
    if not success:
        raise AirflowException("bq_create_main_table_snapshot: failed to create BigQuery snapshot")


def load_snapshot_download(release_id: str, cloud_workspace: CloudWorkspace, http_header: str, base_url: str):
    # Clean all files
    release = release_from_bucket(cloud_workspace.download_bucket, release_id)
    release = UnpaywallRelease.from_dict(release)
    clean_dir(release.snapshot_release.download_folder)

    # Download the most recent Unpaywall snapshot
    # Use a read buffer size of 8MiB as we are downloading a large file
    api_key = os.environ.get("UNPAYWALL_API_KEY")
    if not api_key:
        raise AirflowException("API key 'UNPAYWALL_API_KEY' not found")

    # Assert that the date on the filename matches the snapshot date stored in the release object as there is a
    # small chance that the snapshot changed between when we collated the releases and when we downloaded the snapshot
    snapshot_date = unpaywall_filename_to_datetime(get_snapshot_file_name(base_url, api_key))
    if not release.snapshot_release.snapshot_date == snapshot_date:
        raise AirflowException(
            f"download: release snapshot_date {release.snapshot_release.snapshot_date} != snapshot_date of current snapshot file {snapshot_date}. This can happen because the snapshot was updated between fetch_release() and download()"
        )

    success, download_info = download_file(
        url=snapshot_url(base_url, api_key),
        headers=http_header,
        prefix_dir=release.snapshot_release.download_folder,
        read_buffer_size=2**23,
    )
    if not success:
        raise AirflowException("download: failed to download snapshot")

    # Rename file so that it is easier to deal with
    os.rename(download_info.file_path, release.snapshot_download_file_path)


def load_snapshot_upload_downloaded(release_id: str, cloud_workspace: CloudWorkspace):
    release = release_from_bucket(cloud_workspace.download_bucket, release_id)
    release = UnpaywallRelease.from_dict(release)
    success = gcs_upload_files(
        bucket_name=cloud_workspace.download_bucket,
        file_paths=[release.snapshot_download_file_path],
    )
    if not success:
        raise AirflowException("gcs_upload_files: failed to upload snapshot")


def load_snapshot_extract(release_id: str, cloud_workspace: CloudWorkspace):
    release = release_from_bucket(cloud_workspace.download_bucket, release_id)
    release = UnpaywallRelease.from_dict(release)
    clean_dir(release.snapshot_release.extract_folder)
    gunzip_files(file_list=[release.snapshot_download_file_path], output_dir=release.snapshot_release.extract_folder)


def load_snapshot_transform(release_id: str, cloud_workspace: CloudWorkspace):
    release = release_from_bucket(cloud_workspace.download_bucket, release_id)
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


def load_snapshot_split_main_table_file(release_id: str, cloud_workspace: CloudWorkspace, **context):
    release = release_from_bucket(cloud_workspace.download_bucket, release_id)
    release = UnpaywallRelease.from_dict(release)
    op = BashOperator(
        task_id="split_main_table_file",
        bash_command=f"cd { release.snapshot_release.transform_folder } && split -C 4G --numeric-suffixes=1 --suffix-length=12 --additional-suffix=.jsonl main_table.jsonl main_table",
        do_xcom_push=False,
    )
    op.execute(context)


def load_snapshot_upload_main_table_files(release_id: str, cloud_workspace: CloudWorkspace):
    release = release_from_bucket(cloud_workspace.download_bucket, release_id)
    release = UnpaywallRelease.from_dict(release)
    files_list = list_files(release.snapshot_release.transform_folder, release.main_table_files_regex)
    success = gcs_upload_files(bucket_name=cloud_workspace.transform_bucket, file_paths=files_list)
    if not success:
        raise AirflowException(f"upload_main_table_files: failed to upload main table files")


def load_snapshot_bq_load(
    release_id: str, cloud_workspace: CloudWorkspace, schema_file_path: str, table_description: str
):
    release = release_from_bucket(cloud_workspace.download_bucket, release_id)
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


def changefile_download_url(base_url: str, changefile: str, api_key: str):
    """Generate the changefile download url"""
    return f"{base_url}/daily-feed/changefile/{changefile}?api_key={api_key}"


def load_changefiles_download(release_id: str, cloud_workspace: CloudWorkspace, http_header: str, base_url: str):
    release = release_from_bucket(cloud_workspace.download_bucket, release_id)
    release = UnpaywallRelease.from_dict(release)
    clean_dir(release.changefile_release.download_folder)
    api_key = os.environ.get("UNPAYWALL_API_KEY")
    if not api_key:
        raise AirflowException("API key 'UNPAYWALL_API_KEY' not found")

    download_list = []
    for changefile in release.changefiles:
        url = changefile_download_url(base_url, changefile.filename, api_key)
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


def load_changefiles_upload_downloaded(release_id: str, cloud_workspace: CloudWorkspace):
    release = release_from_bucket(cloud_workspace.download_bucket, release_id)
    release = UnpaywallRelease.from_dict(release)
    files_list = [changefile.download_file_path for changefile in release.changefiles]
    success = gcs_upload_files(bucket_name=cloud_workspace.download_bucket, file_paths=files_list)
    if not success:
        raise AirflowException("upload_downloaded: failed to upload downloaded changefiles")


def load_changefiles_extract(release_id: str, cloud_workspace: CloudWorkspace):
    release = release_from_bucket(cloud_workspace.download_bucket, release_id)
    release = UnpaywallRelease.from_dict(release)
    clean_dir(release.changefile_release.extract_folder)
    files_list = [changefile.download_file_path for changefile in release.changefiles]
    logging.info(f"extracting changefiles: {files_list}")
    gunzip_files(file_list=files_list, output_dir=release.changefile_release.extract_folder)


def load_changefiles_transform(release_id: str, cloud_workspace: CloudWorkspace, primary_key: str):
    release = release_from_bucket(cloud_workspace.download_bucket, release_id)
    release = UnpaywallRelease.from_dict(release)
    clean_dir(release.changefile_release.transform_folder)

    logging.info("transform: find and replace the 'authenticated-orcid' string in the jsonl to 'authenticated_orcid'")
    for changefile in release.changefiles:
        with open(changefile.extract_file_path, "r") as f_in, open(changefile.transform_file_path, "w") as f_out:
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
    merge_update_files(primary_key=primary_key, input_files=transform_files, output_file=release.upsert_table_file_path)


def load_changefiles_upload(release_id: str, cloud_workspace: CloudWorkspace):
    release = release_from_bucket(cloud_workspace.download_bucket, release_id)
    release = UnpaywallRelease.from_dict(release)
    success = gcs_upload_files(
        bucket_name=cloud_workspace.transform_bucket, file_paths=[release.upsert_table_file_path]
    )
    if not success:
        raise AirflowException("upload: failed to upload upsert files")


def load_changefiles_bq_load(
    release_id: str, cloud_workspace: CloudWorkspace, schema_file_path: str, table_description: str
):
    # Will overwrite any existing upsert table
    release = release_from_bucket(cloud_workspace.download_bucket, release_id)
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


def load_changefiles_bq_upsert(release_id: str, cloud_workspace: CloudWorkspace, primary_key: str):
    release = release_from_bucket(cloud_workspace.download_bucket, release_id)
    release = UnpaywallRelease.from_dict(release)
    bq_upsert_records(
        main_table_id=release.bq_main_table_id,
        upsert_table_id=release.bq_upsert_table_id,
        primary_key=primary_key,
    )


def add_dataset_release(release_id: str, cloud_workspace: CloudWorkspace, api_bq_dataset_id: str):
    release = release_from_bucket(cloud_workspace.download_bucket, release_id)
    release = UnpaywallRelease.from_dict(release)

    api = DatasetAPI(bq_project_id=release.cloud_workspace.project_id, bq_dataset_id=api_bq_dataset_id)
    now = pendulum.now()
    dataset_release = DatasetRelease(
        dag_id=release.dag_id,
        entity_id="unpaywall",
        dag_run_id=release.run_id,
        created=now,
        modified=now,
        snapshot_date=release.snapshot_date,
        changefile_start_date=release.changefile_release.start_date,
        changefile_end_date=release.changefile_release.end_date,
    )

    api.add_dataset_release(dataset_release)


def cleanup_workflow(release_id: str, cloud_workspace: CloudWorkspace):
    release = release_from_bucket(cloud_workspace.download_bucket, release_id)
    release = UnpaywallRelease.from_dict(release)
    cleanup(dag_id=release.dag_id, workflow_folder=release.workflow_folder)


def snapshot_url(base_url: str, api_key: str) -> str:
    """Snapshot URL"""

    return f"{base_url}/feed/snapshot?api_key={api_key}"


def get_snapshot_file_name(base_url: str, api_key: str) -> str:
    """Get the Unpaywall snapshot filename. Raises errors if reponses from unpaywall are unexpected.

    :param base_url: The Unpaywall feed base url
    :param api_key: The Unpaywall feed api key
    :return: Snapshot file date.
    """

    url = snapshot_url(base_url, api_key)

    response = requests.head(url)
    if response.status_code != 302:  # Expect a redirect attempt
        raise AirflowException(f"Unexpected status code: url={response.url}, status_code={response.status_code}")
    if not response.headers.get("location"):  # The filename is in the "location" header item that points to AWS
        raise AirflowException(
            f'Could not determine filename. Missing "location" in header: {response.headers.items()}'
        )

    parsed_url = urlparse(response.headers["location"])
    fname = parsed_url.path.split("/")[-1]
    logging.info(f"Found snapshot filename: {fname}")
    return fname


def changefiles_url(base_url: str, api_key: str):
    return f"{base_url}/feed/changefiles?interval=day&api_key={api_key}"


def get_unpaywall_changefiles(url: str) -> List[Changefile]:
    """Get all changefiles from unpaywall"""

    response = get_http_response_json(url)

    # Only include jsonl files, parse date and strip out api key
    changefiles = []
    for changefile in response["list"]:
        filetype = changefile["filetype"]
        if filetype == "jsonl":
            filename = changefile["filename"]
            changefiles.append(Changefile(filename, unpaywall_filename_to_datetime(filename)))

    # Make sure sorted from oldest to newest
    changefiles.sort(key=lambda c: c.changefile_date, reverse=False)

    return changefiles


def unpaywall_filename_to_datetime(file_name: str) -> pendulum.DateTime:
    """Parses a release date from a file name, e.g. 2023-04-25T080001

    :param file_name: Unpaywall release file name (contains date string).
    :return: date.
    """

    date_string_re = re.search(r"\d{4}-\d{2}-\d{2}(T\d{6})?", file_name)
    if not date_string_re:
        raise AirflowException(f"Could not find date in file name: {file_name}")
    date_string = date_string_re.group()

    try:
        # Try to parse full date time
        return pendulum.from_format(date_string, "YYYY-MM-DDTHHmmss")
    except ValueError:
        # Try to just parse date
        return pendulum.from_format(date_string, "YYYY-MM-DD")
