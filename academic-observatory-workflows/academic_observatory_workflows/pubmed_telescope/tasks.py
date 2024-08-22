from __future__ import annotations

import gzip
import hashlib
import json
import logging
import os
import random
import re
import time
from concurrent.futures import as_completed, ProcessPoolExecutor
from dataclasses import dataclass
from ftplib import error_reply, FTP
from typing import Dict, List, Set, Tuple, Union

import pendulum
from airflow import AirflowException
from airflow.models import DagRun
from airflow.models.taskinstance import TaskInstance
from Bio import Entrez
from Bio.Entrez.Parser import (
    DictionaryElement,
    ListElement,
    OrderedListElement,
    StringElement,
    ValidationError,
)
from google.cloud import bigquery
from google.cloud.bigquery import SourceFormat

import observatory_platform.google.bigquery as bq
from academic_observatory_workflows.pubmed_telescope.datafile import Datafile
from academic_observatory_workflows.pubmed_telescope.release import PubMedRelease
from observatory_platform.dataset_api import DatasetRelease, DatasetAPI
from observatory_platform.airflow.airflow import is_first_dag_run
from observatory_platform.files import get_chunks, save_jsonl_gz, yield_jsonl
from observatory_platform.google.gcs import gcs_upload_files
from observatory_platform.airflow.workflow import CloudWorkspace, cleanup


@dataclass
class PMID:
    """Object to identify a particular PMID record by the PMID value and Version.

    :param value: The PMID value.
    :param Version: The Version of the PMID record.
    """

    value: int
    Version: int

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return self.value == other.value and self.Version == other.Version

    def __hash__(self):
        return hash(self.to_str())

    def to_dict(self) -> Dict:
        return dict(value=self.value, Version=self.Version)

    def from_dict(dict_: Dict) -> PMID:
        return PMID(value=dict_["value"], Version=dict_["Version"])

    def to_str(self) -> str:
        return str(self.to_dict())


@dataclass
class PubmedUpdatefile:
    """Object to hold the list of upserts and deletes for a single Pubmed updatefile.

    :param name: Filename of the updatefile.
    :param upserts: List of PMID objects that are to be upserted.
    :param deletes: List of PMID objects that are to be deleted.
    """

    name: str
    upserts: List[PMID]
    deletes: List[PMID]

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return self.name == other.name and set(self.upserts) == set(self.deletes)

    def to_dict(self) -> Dict:
        return dict(
            name=self.name,
            upserts=[upsert.to_dict() for upsert in self.upserts],
            deletes=[delete.to_dict() for delete in self.deletes],
        )

    def from_dict(dict_) -> PubmedUpdatefile:
        return PubmedUpdatefile(
            name=dict_["name"],
            upserts=[PMID.from_dict(upsert) for upsert in dict_["upserts"]],
            deletes=[PMID.from_dict(delete) for delete in dict_["deletes"]],
        )


def fetch_release(
    dag_id: str,
    cloud_workspace: CloudWorkspace,
    run_id: str,
    dag_run: DagRun,
    data_interval_end: pendulum.DateTime,
    bq_dataset_id: str,
    ftp_server_url: str,
    ftp_port: int,
    reset_ftp_counter: int,
) -> Dict:
    """Get a list of all files to process for this release.

    Determine if workflow needs to redownload the baseline files again because of a new yearly release.

    :param dag_id: The ID of the dag
    :param cloud_workspace: The cloud workspace object
    :param run_id: The run ID of this dagrun
    :param dag_run: The DagRun object
    :param data_interval_end: The end of the data interval for this run
    :param bq_dataset_id: The bigquery datset ID
    :param ftp_server_url: The host of the pubmed data
    :param ftp_port: The port to access the ftp server
    :param reset_ftp_counter: After this many files, reset the ftp connection
    """

    # Get list of all baseline files from Pubmed FTP server
    ftp_conn = login_to_ftp(host=ftp_server_url, port=ftp_port)
    baseline_path = "/pubmed/baseline/"
    ftp_conn.cwd(baseline_path)
    baseline_list_ftp = ftp_conn.nlst()

    # Get upload date of the first baseline file.
    baseline_list_ftp.sort()
    baseline_first_file = [file for file in baseline_list_ftp if file.endswith("0001.xml.gz")][0]
    baseline_upload = ftp_conn.sendcmd("MDTM {}".format(baseline_first_file))[4:]
    baseline_upload_date = pendulum.from_format(baseline_upload, "YYYYMMDDHHmmss")
    logging.info(f"baseline upload time: {baseline_first_file}, {baseline_upload_date}")

    # Make workflow re-download the baseline yearly data if the upload date does not match the date from the last release.
    dataset_releases = get_dataset_releases(dag_id=dag_id, dataset_id=bq_dataset_id)
    prev_release = get_latest_dataset_release(dataset_releases, "changefile_end_date")
    is_first_run = is_first_dag_run(dag_run)
    if is_first_run:
        year_first_run = True
    else:
        prev_release_extra = prev_release.extra
        last_baseline_upload_date = pendulum.from_timestamp(prev_release_extra["baseline_upload_date"])

        logging.info(f"prev_release: {prev_release}")
        logging.info(f"pre_release_extra: {prev_release_extra}")
        logging.info(f"Last baseline upload date: {last_baseline_upload_date}")

        if last_baseline_upload_date == baseline_upload_date:
            year_first_run = False
        else:
            year_first_run = True

    logging.info(f"Setting year_first_run to {year_first_run}")

    # Grab list of baseline files from FTP server.
    files_to_download = []
    if year_first_run:
        logging.info(f"This is the first run for the year for Pubmed. Grabbing list of 'baseline' files.")

        # Grab metadata and path of the file.
        for file in baseline_list_ftp:
            if file.endswith(".xml.gz"):  # Find all the xml.gz files available from the server.
                filename = file
                file_index = int(re.findall("\d{4}", file)[0])
                path_on_ftp = baseline_path + file
                datafile = Datafile(
                    filename=filename,
                    file_index=file_index,
                    baseline=True,
                    path_on_ftp=path_on_ftp,
                    datafile_date=baseline_upload_date,
                )
                files_to_download.append(datafile)

    # Grab list of updatefiles from FTP server.
    # Reset FTP connection, as it could have timed out.
    ftp_conn.close()
    ftp_conn = login_to_ftp(host=ftp_server_url, port=ftp_port)
    updatefiles_path = "/pubmed/updatefiles/"
    ftp_conn.cwd(updatefiles_path)

    # Find all the .xml.gz files available
    updatefiles_list = ftp_conn.nlst()
    updatefiles_xml_gz = [file for file in updatefiles_list if file.endswith(".xml.gz")]

    # Determine start date for the new data interval period for this run.
    if baseline_upload_date > data_interval_end:
        raise AirflowException(
            f"Baseline_upload_date: {baseline_upload_date} is after the data_interval_end: {data_interval_end}!"
        )

    logging.info(f"data_interval_end from workflow: {data_interval_end}")
    if is_first_run:
        if not len(dataset_releases) == 0:
            raise ValueError(
                "fetch_releases: there should be no DatasetReleases stored in the Observatory API on the first DAG run."
            )
        release_interval_start = baseline_upload_date
    elif year_first_run:
        if not len(dataset_releases) >= 1:
            raise ValueError(
                "fetch_releases: there should be at least 1 DatasetRelease in the Observatory API after the first DAG run"
            )
        release_interval_start = baseline_upload_date
    else:
        if not len(dataset_releases) >= 1:
            raise ValueError(
                "fetch_releases: there should be at least 1 DatasetRelease in the Observatory API after the first DAG run"
            )
        release_interval_start = pendulum.instance(prev_release.changefile_end_date)

    logging.info(f"Grabbing list of 'updatefiles' for this release: {release_interval_start} to {data_interval_end}")

    for i, file in enumerate(updatefiles_xml_gz):
        # Every reset_ftp_counter number of files that are found, reinistialise the FTP connection.
        if i % reset_ftp_counter == 0 and i != 0:
            # Close exisiting FTP connection.
            ftp_conn.close()

            # Sleep for short time so that the server doesn't refuse the connection.
            time.sleep(random.randint(5, 10))

            # Open a new FTP connection
            ftp_conn = login_to_ftp(host=ftp_server_url, port=ftp_port)
            ftp_conn.cwd(updatefiles_path)

            logging.info(
                f"FTP connection to Pubmed's servers has been reset after {reset_ftp_counter} files to avoid issues."
            )

        # Only return list of updatefiles that are within the required release period.
        file_upload_ftp = ftp_conn.sendcmd("MDTM {}".format(file))[4:]
        file_upload_date = pendulum.from_format(file_upload_ftp, "YYYYMMDDHHmmss")
        if file_upload_date in pendulum.period(release_interval_start, data_interval_end):
            # Grab metadata and path of the file.
            filename = file
            file_index = int(re.findall("\d{4}", file)[0])
            path_on_ftp = updatefiles_path + file
            datafile_date = file_upload_date

            datafile = Datafile(
                filename=filename,
                file_index=file_index,
                baseline=False,
                path_on_ftp=path_on_ftp,
                datafile_date=datafile_date,
            )
            files_to_download.append(datafile)

    if not files_to_download:
        raise RuntimeError(
            "List of files to download is empty. There should be at leaast 7 datafiles to download from Pubmed's FTP server."
        )

    # Sort from oldest to newest using the file index
    files_to_download.sort(key=lambda c: c.file_index, reverse=False)

    # Check that all baseline/updatefiles pulled from the FTP server are not missing any between start_index and end_index.
    # e.g. 10, 11, 13, 14 - will throw an error.
    file_index_prev = files_to_download[0].file_index
    logging.info(f"Starting datafile file_index: {file_index_prev}")
    for datafile in files_to_download[1:]:
        if datafile.file_index == file_index_prev + 1:
            file_index_prev = datafile.file_index
        else:
            raise AirflowException(
                f"The updatefiles are not going to be sequential. Please investigate download {datafile.file_index} and {file_index_prev+1}"
            )

    # Make sure the first datafile.file_index for this release is only +1 ahead of the last release.
    if not year_first_run:
        if not files_to_download[0].file_index == prev_release.sequence_end + 1:
            raise ValueError(
                f"Last updatefile index is not n+1 from the previous release end. Latest release update index: {prev_release.sequence_end} vs current start: {files_to_download[0].file_index}"
            )
    else:
        logging.info(
            f"First run of the Pubmed telescope for the year. No previous releases.file_index to check against."
        )

    logging.info(f"List of files to download from the PubMed FTP server for release:")
    for datafile in files_to_download:
        logging.info(f"Filename: {datafile.filename}, is a baseline file: {datafile.baseline}")

    # Close the connection to the FTP server.
    ftp_conn.close()

    return PubMedRelease(
        dag_id=dag_id,
        run_id=run_id,
        cloud_workspace=cloud_workspace,
        bq_dataset_id=bq_dataset_id,
        start_date=release_interval_start,
        end_date=data_interval_end,
        year_first_run=year_first_run,
        datafile_list=files_to_download,
        baseline_upload_date=baseline_upload_date,
    ).to_dict()


def short_circuit(release: dict) -> bool:
    """Don't skip this DAG run if:
    a) We are in the first run of the year
    b) We are not in the first run of the year and there are updatefiles

    :return: Whether to skip this dag run or not
    """
    release = PubMedRelease.from_dict(release)
    return release.year_first_run or ((not release.year_first_run) and len(release.updatefiles) > 0)


def branch_baseline_or_updatefiles(release: dict) -> None:
    release = PubMedRelease.from_dict(release)
    if release.year_first_run:
        return "baseline.download"
    return "updatefiles.download"


def branch_updatefiles_or_dataset_release(release: dict) -> None:
    release = PubMedRelease.from_dict(release)
    if release.updatefiles:
        return "updatefiles.download"
    return "add_dataset_releases"


def baseline_download(
    release: dict, ftp_server_url: str, ftp_port: str, reset_ftp_counter: int, max_download_retry: int
) -> None:
    """Download files from PubMed's FTP server for this release.
    Unable to do this in parallel because their FTP server is not able to handle many requests at once.

    :param ftp_server_url: The host of the pubmed data
    :param ftp_port: The port to access the ftp server
    :param max_download_retry: Fail after this many unsuccessful download attempts
    """

    release = PubMedRelease.from_dict(release)
    success = download_datafiles(
        datafile_list=release.baseline_files,
        ftp_server_url=ftp_server_url,
        ftp_port=ftp_port,
        reset_ftp_counter=reset_ftp_counter,
        max_download_retry=max_download_retry,
    )
    if not success:
        raise AirflowException("baseline.download: failed to download datafiles")


def updatefiles_download(
    release: dict, ftp_server_url: str, ftp_port: int, reset_ftp_counter: int, max_download_retry: int
) -> None:
    """Download the updatefiles from PubMed's FTP server for this release.
    Unable to do this in parallel due to limitations of their FTP server.

    :param ftp_server_url: The host of the pubmed data
    :param ftp_port: The port to access the ftp server
    :param max_download_retry: Fail after this many unsuccessful download attempts
    """

    release = PubMedRelease.from_dict(release)
    success = download_datafiles(
        datafile_list=release.updatefiles,
        ftp_server_url=ftp_server_url,
        ftp_port=ftp_port,
        reset_ftp_counter=reset_ftp_counter,
        max_download_retry=max_download_retry,
    )
    if not success:
        raise AirflowException("updatefiles.download: failed to download datafiles")


def baseline_upload_downloaded(release: dict) -> None:
    """Upload downloaded baseline files to GCS."""

    # Grab list of files to upload.
    release = PubMedRelease.from_dict(release)
    file_paths = [datafile.download_file_path for datafile in release.baseline_files]
    success = gcs_upload_files(
        bucket_name=release.cloud_workspace.download_bucket,
        file_paths=file_paths,
    )
    if not success:
        raise AirflowException("baseline.upload_downloaded: failed to upload files to cloud storage bucket")


def updatefiles_upload_downloaded(release: dict) -> None:
    """Upload downloaded updatefiles files to GCS."""

    release = PubMedRelease.from_dict(release)
    datafiles_to_upload = [datafile.download_file_path for datafile in release.updatefiles]

    success = gcs_upload_files(
        bucket_name=release.cloud_workspace.download_bucket,
        file_paths=datafiles_to_upload,
    )
    if not success:
        raise AirflowException("updatefiles.upload_downloaded: failed to upload files to cloud storage bucket")


def baseline_transform(release: dict, max_processes: int) -> None:
    """
    Transform the *.xml.gz files downloaded from PubMed into usable json files for BigQuery import.

    :param max_proceses: The max number of processes to use for multiprocessing
    """

    release = PubMedRelease.from_dict(release)

    # Process files in batches so that ProcessPoolExecutor doesn't deplete the system of memory
    for i, chunk in enumerate(get_chunks(input_list=release.baseline_files, chunk_size=max_processes)):
        with ProcessPoolExecutor(max_workers=max_processes) as executor:
            logging.info(f"In chunk {i} and processing files: {chunk}")

            futures = []
            datafile: Datafile
            for datafile in chunk:
                input_path = datafile.download_file_path
                upsert_path = datafile.transform_baseline_file_path
                futures.append(executor.submit(transform_pubmed, input_path, upsert_path))

            # Make sure that all datafiles have been properly transformed.
            for future in as_completed(futures):
                filename = future.result()

                assert filename, f"Unable to transform baseline file: {filename}"


def updatefiles_transform(release: dict, max_processes: int) -> None:
    """
    Transform the *.xml.gz files downloaded from PubMed's FTP server into usable json-like files for BigQuery import.
    This is a multithreaded and pulls the PubmedArticle records from the downloaded XML files.

    :param max_processes: The number of processes to use for multithreading
    """

    release = PubMedRelease.from_dict(release)

    # Object to store all of the upserts and delete keys that are present in each file.
    updatefiles: List[PubmedUpdatefile] = []

    # Process files in batches so that ProcessPoolExecutor doesn't deplete the system of memory
    for i, chunk in enumerate(get_chunks(input_list=release.updatefiles, chunk_size=max_processes)):
        with ProcessPoolExecutor(max_workers=max_processes) as executor:
            logging.info(f"In chunk {i} and processing files: {chunk}")

            futures = []
            datafile: Datafile
            for datafile in chunk:
                input_path = datafile.download_file_path
                upsert_path = datafile.transform_upsert_file_path

                futures.append(executor.submit(transform_pubmed, input_path, upsert_path))

            for future in as_completed(futures):
                updatefiles.append(future.result())

    assert len(release.updatefiles) == len(
        updatefiles
    ), f"Number of updatefiles does not match the number of non baseline datafiles: {len(release.updatefiles)} vs {len(updatefiles)}"

    ti: TaskInstance["ti"]
    updatefile_list = [updatefile.to_dict() for updatefile in updatefiles]
    ti.xcom_push(key="updatefile_list", value=updatefile_list)


def baseline_upload_transformed(release: dict) -> None:
    """Upload transformed baseline files to GCS."""

    release = PubMedRelease.from_dict(release)
    file_paths = [datafile.transform_baseline_file_path for datafile in release.baseline_files]

    success = gcs_upload_files(
        bucket_name=release.cloud_workspace.transform_bucket,
        file_paths=file_paths,
    )
    if not success:
        raise AirflowException("baseline.upload_transformed: failed to upload files to cloud storage bucket")


def updatefiles_merge_upserts_deletes(release: dict, max_processes: int) -> None:
    """Merge the upserts and deletes for this release period.

    :param max_processes: The number of processes to use for multithreading
    """

    release = PubMedRelease.from_dict(release)
    ti: TaskInstance["ti"]
    updatefile_list: list = ti.xcom_pull(key="updatefile_list")
    updatefiles = [PubmedUpdatefile.from_dict(updatefile) for updatefile in updatefile_list]

    # Merge records and return a list of what upserts to pull from the transformed updatefiles.
    upsert_index, deletes = merge_upserts_and_deletes(updatefiles)

    logging.info(f"Number of records for this release: {len(upsert_index.keys())} upserts and {len(deletes)} deletes.")

    # Save deletes
    data = [delete.to_dict() for delete in deletes]
    save_jsonl_gz(release.merged_delete_file_path, data)

    # Save final upserts
    for i, chunk in enumerate(get_chunks(input_list=release.updatefiles, chunk_size=max_processes)):
        with ProcessPoolExecutor(max_workers=max_processes) as executor:
            logging.info(f"In chunk {i} and processing files: {chunk}")

            futures = []
            datafile: Datafile
            for datafile in chunk:
                filename = os.path.basename(datafile.download_file_path)

                futures.append(
                    executor.submit(
                        save_pubmed_merged_upserts,
                        filename,
                        upsert_index,
                        datafile.transform_upsert_file_path,
                        datafile.merged_upsert_file_path,
                    )
                )
            for future in as_completed(futures):
                logging.info(f"Finished writing out upserts to file: {future.result()}")


def updatefiles_upload_merged_upsert_records(release: dict) -> None:
    """Upload the merged upsert records to GCS."""

    release = PubMedRelease.from_dict(release)
    file_paths = [datafile.merged_upsert_file_path for datafile in release.updatefiles]
    success = gcs_upload_files(
        bucket_name=release.cloud_workspace.transform_bucket,
        file_paths=file_paths,
    )
    if not success:
        raise AirflowException(
            "updatefiles.upload_merged_upsert_records: failed to upload files to cloud storage bucket"
        )


def baseline_bq_load(
    release: dict,
    bq_dataset_description: str,
    main_table_name: str,
    baseline_table_description: str,
) -> None:
    """Ingest the baseline table from GCS to BQ using a file pattern.

    :param bq_dataset_description: The description to give the bigquery dataset
    :param main_table_name: The name of the table
    baseline_table_description: The description to give the table
    """

    release = PubMedRelease.from_dict(release)

    # Create the dataset if not already present.
    bq.bq_create_dataset(
        project_id=release.cloud_workspace.project_id,
        dataset_id=release.bq_dataset_id,
        location=release.cloud_workspace.data_location,
        description=bq_dataset_description,
    )

    baseline_transform_blob_pattern = release.transfer_blob_pattern("baseline")

    logging.info(f"Creating a load job for all of the baseline files with pattern: {baseline_transform_blob_pattern} ")

    main_table_id = bq.bq_table_id(
        project_id=release.cloud_workspace.project_id, dataset_id=release.bq_dataset_id, table_id=main_table_name
    )
    success = bq.bq_load_table(
        uri=baseline_transform_blob_pattern,
        table_id=main_table_id,
        source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
        schema_file_path=release.schema_file_path(record_type="pubmed"),
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        table_description=baseline_table_description,
        ignore_unknown_values=True,
    )
    if not success:
        raise AirflowException("baseline.bq_load: failed to load data into BigQuery")


def updatefiles_bq_load_upsert_table(release: dict, upsert_table_name: str, upsert_table_description: str) -> None:
    """Ingest the upsert records from GCS to BQ using a glob pattern.

    :param upsert_table_name: The name of the upsert table to upload
    :param upsert_table_description: The description to give the upsert table
    """

    release = PubMedRelease.from_dict(release)
    upsert_table_id = bq.bq_table_id(
        project_id=release.cloud_workspace.project_id, dataset_id=release.bq_dataset_id, table_id=upsert_table_name
    )
    logging.info(f"Uploading to table - {upsert_table_id}")

    success = bq.bq_load_table(
        uri=release.merged_upsert_uri_blob_pattern,
        table_id=upsert_table_id,
        source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
        schema_file_path=release.schema_file_path(record_type="pubmed"),
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        table_description=upsert_table_description,
        ignore_unknown_values=True,
    )

    assert success, f"Unable to tranfer to table - {upsert_table_id}"
    if not success:
        raise AirflowException("updatefiles.bq_load_upsert_table: failed to load upsert table")

    expiry_date = pendulum.now().add(days=7)
    bq_update_table_expiration(upsert_table_id, expiration_date=expiry_date)


def updatefiles_bq_upsert_records(release: dict, main_table_name: str, upsert_table_name: str) -> None:
    """
    Upsert records into the main table.
    Has to match on both the PMID value and the Version number, as there could be multiple different versions in
    the main table.

    :param main_table_name: The name of the main table to upsert to
    :param upsert_table_name: The name of the table containing the records to upsert
    """

    release = PubMedRelease.from_dict(release)
    keys_to_match_on = [
        "MedlineCitation.PMID.value",
        "MedlineCitation.PMID.Version",
    ]
    main_table_id = bq.bq_table_id(
        project_id=release.cloud_workspace.project_id, dataset_id=release.bq_dataset_id, table_id=main_table_name
    )
    upsert_table_id = bq.bq_table_id(
        project_id=release.cloud_workspace.project_id, dataset_id=release.bq_dataset_id, table_id=upsert_table_name
    )
    bq.bq_upsert_records(main_table_id=main_table_id, upsert_table_id=upsert_table_id, primary_key=keys_to_match_on)


def updatefiles_upload_merged_delete_records(release: dict) -> None:
    """Upload the merged delete records to GCS."""

    release = PubMedRelease.from_dict(release)
    file_paths = [release.merged_delete_file_path]

    success = gcs_upload_files(
        bucket_name=release.cloud_workspace.transform_bucket,
        file_paths=file_paths,
    )
    if not success:
        raise AirflowException("Upload of merged delete records failed")


def updatefiles_bq_load_delete_table(release: dict, delete_table_name: str, delete_table_description: str) -> None:
    """Ingest delete records from GCS to BQ.

    :param delete_table_name: The name of the delete table
    :param delete_table_description: The description to give the delete table
    """

    release = PubMedRelease.from_dict(release)
    logging.info(f"Uploading to table - {delete_table_id}")

    delete_table_id = bq.bq_table_id(
        project_id=release.cloud_workspace.project_id, dataset_id=release.bq_dataset_id, table_id=delete_table_name
    )
    success = bq.bq_load_table(
        uri=release.merged_delete_transfer_uri,
        table_id=delete_table_id,
        source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
        schema_file_path=release.schema_file_path(record_type="delete"),
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        table_description=delete_table_description,
        ignore_unknown_values=True,
    )

    assert success, f"Unable to tranfer to table - {delete_table_id}"
    if not success:
        raise AirflowException("")

    expiry_date = pendulum.now().add(days=7)
    bq_update_table_expiration(delete_table_id, expiration_date=expiry_date)


def updatefiles_bq_delete_records(release: dict, main_table_name: str, delete_table_name: str) -> None:
    """
    Removed records from the main table that are specified in delete table.
    Has to match on both the PMID value and the Version number, as there could be multiple different versions in
    the main table.

    :param main_table_name: The name of the table to delete records from
    :param delete_table_name: The name of the table containing the records to delete
    """

    release = PubMedRelease.from_dict(release)
    main_table_keys_to_match_on = [
        "MedlineCitation.PMID.value",
        "MedlineCitation.PMID.Version",
    ]
    delete_table_keys_to_match_on = ["value", "Version"]
    main_table_id = bq.bq_table_id(
        project_id=release.cloud_workspace.project_id, dataset_id=release.bq_dataset_id, table_id=main_table_name
    )
    delete_table_id = bq.bq_table_id(
        project_id=release.cloud_workspace.project_id, dataset_id=release.bq_dataset_id, table_id=delete_table_name
    )

    bq.bq_delete_records(
        main_table_id=main_table_id,
        delete_table_id=delete_table_id,
        main_table_primary_key=main_table_keys_to_match_on,
        delete_table_primary_key=delete_table_keys_to_match_on,
    )


def add_dataset_releases(release: dict, api_bq_dataset_id: str) -> None:
    """Adds release information to the API.

    :param api_bq_datastet_id: bigquery dataset ID of the API
    """

    release = PubMedRelease.from_dict(release)
    logging.info(f"add_dataset_releases: creating dataset release for Pubmed Articles.")
    api = DatasetAPI(bq_project_id=release.cloud_workspace.project_id, bq_dataset_id=api_bq_dataset_id)
    api.seed_db()
    now = pendulum.now()
    dataset_release = DatasetRelease(
        dag_id=release.dag_id,
        entity_id="pubmed",
        dag_run_id=release.run_id,
        created=now,
        modified=now,
        data_interval_start=release.start_date,
        data_interval_end=release.end_date,
        sequence_start=release.datafile_list[0].file_index,
        sequence_end=release.datafile_list[-1].file_index,
        changefile_start_date=release.start_date,
        changefile_end_date=release.end_date,
        extra={"baseline_upload_date": release.baseline_upload_date.timestamp()},
    )
    logging.info(f"add_dataset_releases: dataset_release={dataset_release}")
    api.post_dataset_release(dataset_release)


def cleanup_workflow(release: dict) -> None:
    """
    Cleanup files from this workflow run.

    Delete local download files, tranform files and current task instance.
    """

    release = PubMedRelease.from_dict(release)
    logging.info(f"Deleting local files from - {release.workflow_folder}")
    cleanup(dag_id=release.dag_id, workflow_folder=release.workflow_folder)


def login_to_ftp(host: str, port: int) -> FTP:
    ftp_conn = FTP()
    ftp_conn.connect(host=host, port=port)
    ftp_conn.login()  # Anonymous login (publicly available data)
    return ftp_conn


def create_snapshot(release: dict, bq_dataset_id: str, bq_main_table_name: str, snapshot_expiry_days: int) -> None:
    """Create a snapshot of main table as a backup just in case something happens when applying the upserts and deletes."""

    release = PubMedRelease.from_dict(release)
    if not release.year_first_run:
        snapshot_table_id = bq.bq_sharded_table_id(
            project_id=release.cloud_workspace.project_id,
            dataset_id=bq_dataset_id,
            table_name=f"{bq_main_table_name}_snapshot",
            date=release.start_date,
        )
        main_table_id = bq.bq_table_id(
            project_id=release.cloud_workspace.project_id, dataset_id=bq_dataset_id, table_name=bq_main_table_name
        )
        expiry_date = pendulum.now().add(days=snapshot_expiry_days)
        success = bq.bq_snapshot(
            src_table_id=main_table_id,
            dst_table_id=snapshot_table_id,
            expiry_date=expiry_date,
        )
        if not success:
            raise AirflowException("create_snapshot: failed to create snapshot")
        logging.info(f"Created snapshot table: {snapshot_table_id}")
    else:
        logging.info("Not required to create a snapshot of the table for this run.")


def download_datafiles(
    datafile_list: List[Datafile],
    ftp_server_url: str,
    ftp_port: int,
    reset_ftp_counter: int,
    max_download_retry: int,
) -> bool:
    """Download a list of Pubmed datafiles from their FTP server.

    :param datafile_list: List of datafiles to download from their FTP server.
    :param ftp_server_url: FTP server URL.
    :param ftp_port: Port for the FTP connection.
    :param reset_ftp_counter: After this number of files, reset the FTP connection
    to make sure that the connect is not reset by the host.
    :param max_download_retry: Maximum number of retries for downloading one datafile before throwing an error.

    :return download_success: True if downloading all of the datafiles were successful.
    """

    # Open FTP connection

    for i, datafile in enumerate(datafile_list):
        # Pubmed's FTP server disconnects after an artbitary length of time.
        # Connection needs to be reset every so often so we can reliably download it.
        # After reset_ftp_counter number of downloads, it is re-established.

        # Every reset_ftp_counter number of files that are downloaded, reinitialise the FTP connection.
        if i % reset_ftp_counter == 0 and i != 0:
            # Close exisiting FTP connection.
            ftp_conn.close()

            # Sleep for short time so that the server doesn't refuse the connection.
            time.sleep(random.randint(5, 10))

            # Open a new FTP connection
            ftp_conn = login_to_ftp(host=ftp_server_url, port=ftp_port)

            logging.info(
                f"FTP connection to Pubmed's servers has been reset after {reset_ftp_counter} downloads to avoid issues."
            )

        download_attempt_count = 1
        download_success = False
        while download_attempt_count <= max_download_retry and not download_success:
            logging.info(f"Downloading: {datafile.filename} Attempt: {download_attempt_count}")
            try:
                # Download file
                with open(datafile.download_file_path, "wb") as f:
                    ftp_conn.retrbinary(f"RETR {datafile.path_on_ftp}", f.write)
                logging.info(f"File downloaded to - {datafile.download_file_path}")
            except error_reply:
                logging.info(f"Unable to download {datafile.path_on_ftp} from PubMed's FTP server {ftp_server_url}.")

            # Create the hash from the above downloaded file.
            with open(datafile.download_file_path, "rb") as f_hash:
                data = f_hash.read()
                md5hash_from_download = hashlib.md5(data).hexdigest()

            logging.info(f"MD5 hash: {md5hash_from_download} for downloaded file: {datafile.filename}")

            # Need to have a download catch for the hash file as well, otherwise it can break the download loop.
            try:
                # Download corresponding md5 hash.
                with open(f"{datafile.download_file_path}.md5", "wb") as f:
                    ftp_conn.retrbinary(f"RETR {datafile.path_on_ftp}.md5", f.write)
            except error_reply:
                logging.info(
                    f"Unable to download {datafile.path_on_ftp}.md5 from PubMed's FTP server {ftp_server_url}."
                )

            with open(f"{datafile.download_file_path}.md5", "r") as f_md5:
                md5_from_pubmed_ftp = f_md5.read()

            # If md5 does not match, retry download.
            if md5hash_from_download in md5_from_pubmed_ftp:
                download_success = True
            else:
                logging.info(
                    f"MD5 hash does not match the given checksum from server: {datafile.download_file_path}\
                            - Retrying download ..."
                )

            download_attempt_count += 1

        assert (
            download_success
        ), f"Unable to download {datafile.download_file_path} from PubMed's FTP server \
                    {ftp_server_url} after {max_download_retry} tries."

    # Close the FTP connection after downloading the required files.
    ftp_conn.close()

    return download_success


def load_datafile(input_path: str) -> List[Dict]:
    """Read in a Pubmed XML file and return it in a well-defined dictionary/json object.

    :param input_path: Path to the Pubmed xml.gz file.
    :return data: A list of Pubmed records.
    """

    logging.info(f"Reading in file - {input_path}")

    with gzip.open(input_path, "rb") as f_in:
        # Use the BioPython package for reading in the Pubmed XML files.
        # This package also checks against its own DTD schema defined in the XML header.
        data = Entrez.read(f_in, validate=True)

    # Need pull out XML attributes from the Biopython data classes.
    data = add_attributes(data)

    #  Remove unwanted nested list structure from the Pubmed dictionary.
    data = change_pubmed_list_structure(data)

    return data


def save_pubmed_jsonl(output_path: str, data: List[Dict]):
    """Save a Pubmed jsonl to file using the custom encoder.

    :param output_path: Path of the output file.
    :param data: The data to write to file.
    :return: None.
    """

    if output_path.endswith(".gz"):
        with gzip.open(output_path, "w") as f_out:
            for line in data:
                f_out.write(str.encode(json.dumps(line, cls=PubMedCustomEncoder) + "\n"))
    else:
        with open(output_path, "w") as f_out:
            for line in data:
                f_out.write(json.dumps(line, cls=PubMedCustomEncoder) + "\n")


def parse_articles(data: Dict) -> Union[List, List[Dict]]:
    try:
        return data["PubmedArticle"]
    except KeyError:
        logging.info(f"No PubmedArticle records in file")
        return []


def parse_deletes(data: Dict) -> Union[List, List[Dict]]:
    try:
        return data["DeleteCitation"]["PMID"]
    except KeyError:
        logging.info(f"No DeleteCitation.PMID records in file")
        return []


def transform_pubmed(input_path: str, upsert_path: str) -> Union[bool, str, PubmedUpdatefile]:
    """
    Convert a single Pubmed XML file to JSONL, pulling out any of the Pubmed the upserts and or deletes.
    Used in parallelised transform task.

    :param input_path: Path to the donwloaded xml.gz file.
    :param upsert_path: Output file path for the upserts.
    :return: filename of the baseline file or a PubmedUpdatefile object if it is an updatefile.
    """

    try:
        data = load_datafile(input_path)
    except ValidationError:
        logging.info(f"Fields in XML are not valid against it's own DTD file - {input_path}")
        return False

    upserts = parse_articles(data)
    logging.info(f"Pulled out {len(upserts)} upserts from file, writing to file: {upsert_path}")

    # Save upserts to file.
    save_pubmed_jsonl(upsert_path, upserts)

    # Pull out keys of upserts and deletes from an updatefile. This is not required for baseline files.
    if "upsert" in upsert_path:
        delete_keys = parse_deletes(data)
        logging.info(f"Pulled out {len(delete_keys)} deletes from file.")

        upsert_keys = [record["MedlineCitation"]["PMID"] for record in upserts]

        return PubmedUpdatefile(
            name=os.path.basename(input_path),
            upserts=[PMID.from_dict(record) for record in upsert_keys],
            deletes=[PMID.from_dict(record) for record in delete_keys],
        )

    return os.path.basename(input_path)


def save_pubmed_merged_upserts(
    filename: str,
    upsert_index: Dict[PMID, str],
    input_path: str,
    output_path: str,
) -> str:
    """Used in parallel by save_merged_upserts_and_deletes to write out the merged upsert records using the custom
    Pubmed encoder.

    :param filename: Name of the original updatefile used by the upsert_index.
    :param upserts: A list of upsert keys to pull out and write to file.
    :param input_path: Where the original upsert records are stored.
    :param output_path: Destination of where to write the merged upsert records.
    :return output_path: For logging purposes and ensuring the multithreaded task completed.
    """
    data = []
    for record in yield_jsonl(input_path):
        pmid = PMID.from_dict(record["MedlineCitation"]["PMID"])
        # If the PMID is in the upsert_index to write to file and
        # then if the record is from the correct file. Could be replaced by newer record.
        if pmid in upsert_index:
            if upsert_index[pmid] == filename:
                data.append(record)
    save_pubmed_jsonl(output_path, data)
    return output_path


def merge_upserts_and_deletes(
    updatefiles: List[PubmedUpdatefile],
) -> Tuple[Dict[PMID, str], Set[PMID]]:
    """Merge the Pubmed upserts and deletes and return them as a list of PMID value-Version pairs so that they can be
    easily pulled from file.

    :param updatefiles: List of PubmedUpdatefile objects to merge together.
    :return: The merged upserts and deletes."""

    upsert_index = dict()
    deletes = set()

    upsert_count = 0
    delete_count = 0

    updatefiles.sort(key=lambda x: x.name)
    for file in updatefiles:
        logging.info(
            f"In file {file.name} - trying to merge {len(file.upserts)} upserts and {len(file.deletes)} deletes."
        )
        upsert_count += len(file.upserts)
        delete_count += len(file.deletes)

        for upsert in file.upserts:
            # Check if item was previously deleted and remove this delete if so
            if upsert in deletes:
                deletes.remove(upsert)

            # Update the file where the upsert comes from
            upsert_index[upsert] = file.name

        for delete in file.deletes:
            # Delete any previous upserts
            if delete in upsert_index:
                del upsert_index[delete]

            # Add to delete set, as the record could still be in the main table
            deletes.add(delete)

    logging.info(f"Removed {upsert_count - len(upsert_index.keys())} upserts and {delete_count-len(deletes)} deletes.")

    return upsert_index, deletes


def add_attributes(obj: Union[StringElement, DictionaryElement, ListElement, OrderedListElement, list, str]):
    """
    Recursively travel down the Pubmed data tree to add attributes from Biopython classes as key-value pairs.

    Only pulling data from StringElements, DictionaryElements, ListElements and OrderedListElements.

    :param obj: Input object, being one of the Biopython data classes.
    :return new: Object with attributes added as keys to the dictionary.
    """

    if isinstance(obj, StringElement):
        if len(list(obj.attributes.keys())) > 0:
            new = {}
            new["value"] = str(obj)
            for key in list(obj.attributes.keys()):
                new[key] = add_attributes(obj.attributes[key])
        else:
            new = str(obj)

        return new

    elif isinstance(obj, DictionaryElement):
        new = {}
        for key in list(obj.attributes.keys()):
            new[key] = add_attributes(obj.attributes[key])

        for k, v in list(obj.items()):
            new[k] = add_attributes(v)

        return new

    elif isinstance(obj, (ListElement, OrderedListElement)):
        new = {}
        if len(obj) > 0:
            new[obj[0].tag] = [add_attributes(v) for v in obj]
        try:
            for key in list(obj.attributes.keys()):
                new[key] = add_attributes(obj.attributes[key])
        except:
            pass

        return new

    elif isinstance(obj, list):
        new = [add_attributes(v) for v in obj]
        return new

    else:
        return obj


# List of problematic fields that have nested lists.
# Elements in the list are extra fields to append to the same level as the *List field.
# Only for the 2023 schema. May change with a new revision.
bad_list_fields = {
    "AuthorList": [],
    "ArticleIdList": [],
    "AuthorList": ["CompleteYN", "Type"],
    "GrantList": ["CompleteYN"],
    "ChemicalList": [],
    "CommentsCorrectionsList": [],
    "GeneSymbolList": [],
    "MeshHeadingList": [],
    "PersonalNameSubjectList": [],
    "InvestigatorList": [],
    "PublicationTypeList": [],
    "ObjectList": [],
    # The following are taken care of with if statements as they are special cases:
    # KeywordList
    # SupplMeshList
    # DataBankList
    # AccessionNumberList
}


def change_pubmed_list_structure(
    obj: Union[dict, list, str, DictionaryElement, ListElement, StringElement]
) -> Union[dict, list, str, DictionaryElement, ListElement, StringElement]:
    """Recursively travel down the Pubmed data tree to move the specified fields
    up one level to make it easier to query in Bigquery.

    For example, the original data can look something like

    {
        "AuthorList": {
            "CompleteYN": "Y",
            "Author": [{ "First": "Foo", "Last": "Bar" },
                       { "First": "James", "Last": "Bond" }]
        }
    }

    The "Author" field will be removed and the data from it will be moved up
    to the "List" level, along with any data specified in the "bad_list_fields" dictionary:

    {
        "AuthorListCompleteYN": "Y",
        "AuthorListType": None,
        "AuthorList":  [{ "First": "Foo", "Last": "Bar" },
                       { "First": "James", "Last": "Bond" }]
    }

    :param obj: Incoming data object.
    :return: Any type as it could be a Pubmed dataclass.
    """

    if isinstance(obj, dict):
        new_obj = {}

        for key, value in obj.items():
            # If the key is listed in the above
            if key in bad_list_fields:
                if isinstance(value, dict):
                    # Remove "List" from the key
                    try:
                        new_obj[key] = value[key[:-4]]
                    except KeyError:
                        logging.info(
                            f"No data under the key {key}/{key[:-4]} and value {value}. Leaving the field blank."
                        )
                        pass

                    # Add fields onto new data object, e.g. CompleteYN, or Type
                    for to_add in bad_list_fields[key]:
                        try:
                            new_obj[f"{key}{to_add}"] = value[to_add]
                        except KeyError:
                            # In this instance the field does not exist in the data.
                            # Add it to the dictionary as a null and move on.
                            new_obj[f"{key}{to_add}"] = None
                            pass

            # Special cases below where field names are changed and some are nested.
            elif key == "DataBankList":
                new_list = []

                for nested_value in value["DataBank"]:
                    new_dict = {}
                    new_dict["DataBankName"] = nested_value["DataBankName"]
                    new_dict["AccessionNumberList"] = nested_value["AccessionNumberList"]["AccessionNumber"]

                    new_list.append(new_dict)

                new_obj[f"{key}CompleteYN"] = value["CompleteYN"]
                new_obj[key] = new_list

            elif key == "SupplMeshList":
                new_obj[key] = value["SupplMeshName"]

            elif key == "KeywordList":
                if isinstance(value, dict):
                    new_obj[key] = value["Keyword"]
                    new_obj[f"{key}Owner"] = []
                elif len(value) > 0:
                    new_obj[key] = []
                    new_obj[f"{key}Owner"] = []
                    for sub in value:
                        new_obj[key].extend(sub["Keyword"])
                        new_obj[f"{key}Owner"].append(sub["Owner"])

            # Go down to the next level of the data tree to find more fields to change.
            else:
                new_obj[key] = change_pubmed_list_structure(value)

        return new_obj

    elif isinstance(obj, list):
        return [change_pubmed_list_structure(value) for value in obj]

    elif isinstance(obj, str):
        return obj


class PubMedCustomEncoder(json.JSONEncoder):
    """Custom encoder for json dump for it to write a dictionary field as a string of text for a
    select number of key values in the Pubmed data.

    For example, the AbstractText field can be a string or an array containing background, methods, etc,
    but Bigquery requires it to be well defined and it can't be both an array and a string in the
    schema. This encoder forces the below fields to be a string when written to a json file.
    """

    write_as_single_field = [
        "AbstractText",
        "Affiliation",
        "ArticleTitle",
        "b",
        "BookTitle",
        "Citation",
        "CoiStatement",
        "CollectionTitle",
        "CollectiveName",
        "i",
        "Param",
        "PublisherName",
        "SectionTitle",
        "sub",
        "Suffix",
        "sup",
        "u",
        "VernacularTitle",
        "VolumeTitle",
    ]

    def _transform_obj_data(self, obj):
        if isinstance(obj, str):
            return obj
        elif isinstance(obj, dict):
            new = {}
            # Loop through field names for the match fields to change to text.
            for k, v in list(obj.items()):
                if k in self.write_as_single_field:
                    new[k] = str(v)
                else:
                    new[k] = self._transform_obj_data(v)

            return new
        elif isinstance(obj, list):
            return [self._transform_obj_data(elem) for elem in obj]
        else:
            return obj

    def encode(self, obj):
        transformed_obj = self._transform_obj_data(obj)
        return super(PubMedCustomEncoder, self).encode(transformed_obj)


def bq_update_table_expiration(full_table_id: str, expiration_date: pendulum.datetime) -> None:
    """Update a Bigquery table expiration date.

    :param full_table_id: Full qualified table id.
    :param expiration_date: Expiration date of the table.
    """

    client = bigquery.Client()
    table = client.get_table(full_table_id)

    # Update the expiration time
    table.expires = expiration_date

    # Update the table metadata
    client.update_table(table, ["expires"])
