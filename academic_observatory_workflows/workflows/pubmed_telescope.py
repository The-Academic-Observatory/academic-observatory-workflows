# Copyright 2023 Curtin University
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

# Author: Alex Massen-Hane

# Common libraries
import os
from pathlib import Path
import subprocess
from typing import Dict, List
import pendulum
from datetime import timedelta
import logging
import re
import gzip
import json

# To download the files from the FTP server
import ftplib

# For checking files have been downloaded correctly
import hashlib
from psutil import Popen

# For Injesting the XML using Biopython library.
from Bio import Entrez
from Bio.Entrez.Parser import (
    StringElement,
    ListElement,
    DictionaryElement,
    OrderedListElement,
)

# Multithreading libraries
from concurrent.futures import ProcessPoolExecutor, as_completed

# Airflow modules
from airflow import AirflowException
from airflow.models.taskinstance import TaskInstance
from airflow.models.variable import Variable

from academic_observatory_workflows.config import schema_folder as default_schema_folder
from observatory.platform.utils.airflow_utils import AirflowVars, make_workflow_folder
from observatory.platform.workflows.workflow import Workflow, Release
from observatory.platform.utils.workflow_utils import get_chunks

from observatory.platform.utils.gc_utils import (
    bigquery_table_exists,
    upload_file_to_cloud_storage,
    bigquery_sharded_table_id,
    create_bigquery_dataset,
    load_bigquery_table,
    run_bigquery_query,
    delete_old_datasets_with_prefix,
)

from observatory.platform.utils.release_utils import (
    get_dataset_releases,
    get_datasets,
    get_latest_dataset_release,
    is_first_release,
)

from observatory.platform.utils.workflow_utils import (
    SubFolder,
    make_release_date,
)

from google.cloud import bigquery


class PubMedRelease(Release):
    def __init__(
        self,
        *,
        dag_id: str,
        release_id: str,
    ):
        """Construct a PubmedRelease.

        :param dag_id: the DAG id.
        :param start_date:
        :param end_date:
        :param first_release:
        """

        download_files_regex = r"*.xml.gz"
        transform_files_regex = r"*.jsonl"

        super().__init__(
            dag_id=dag_id,
            release_id=release_id,
            download_files_regex=download_files_regex,
            transform_files_regex=transform_files_regex,
        )

    # @property
    #


class PubMedTelescope(Workflow):
    DAG_ID_PREFIX = "pubmed"

    """

    PubMed Telescope

    Please visit:
    https://pubmed.ncbi.nlm.nih.gov/

    """

    def __init__(
        self,
        *,
        dag_id: str,
        workflow_id: int,
        start_date: str = pendulum.datetime(year=2022, month=11, day=27),  # When the baseline data became available.
        schedule_interval: str = "0 0 * * 0",  # weekly
        catchup: bool = True,
        ftp_server_url: str,
        reset_ftp_counter: int = 20,
        check_md5_hash: bool,
        max_download_retry: int = 5,
        airflow_vars: List[str] = None,
        queue: str = "default",
        max_processes: int = 4,  # Limited to 4 due to RAM
        batch_size: int = 4,
        dataset_description: str = "PubMed Citation Lists",
        dataset_id: str = None,
        data_location: str = Variable.get(AirflowVars.DATA_LOCATION),
        source_format: str,
        **kwargs,
    ):
        """Construct an PubMed Telescope instance.

        # TODO: Go through descriptions of classes and functions.

        :param dag_id: the id of the DAG.
        :param start_date: the start date of the DAG.
        :param schedule_interval: the schedule interval of the DAG.
        :param dataset_id: the dataset id.
        :param dataset_description: the dataset description.
        :param queue: the queue that the tasks should run on.
        :param schema_folder: the SQL schema path.
        :param airflow_vars: list of airflow variable keys, for each variable it is checked if it exists in airflow
        :param workflow_id: api workflow id.
        """

        if airflow_vars is None:
            airflow_vars = [
                AirflowVars.DATA_PATH,
                AirflowVars.PROJECT_ID,
                AirflowVars.DOWNLOAD_BUCKET,
                AirflowVars.TRANSFORM_BUCKET,
            ]

        # TODO: Go throguh these params. Are they needed?
        super().__init__(
            dag_id=dag_id,
            start_date=start_date,
            schedule_interval=schedule_interval,
            catchup=catchup,
            airflow_vars=airflow_vars,
            workflow_id=workflow_id,
            queue=queue,
            **kwargs,
        )

        # Databse settings
        self.project_id = Variable.get(AirflowVars.PROJECT_ID)
        self.download_bucket = Variable.get(AirflowVars.DOWNLOAD_BUCKET)
        self.transform_bucket = Variable.get(AirflowVars.TRANSFORM_BUCKET)

        self.data_location = data_location
        self.dataset_description = dataset_description
        self.dataset_id = dataset_id
        self.source_format = source_format
        self.schema_folder = default_schema_folder()
        self.main_table = "articles"
        self.temp_dataset = "temporary_working_space"

        # Workflow parameters
        self.workflow_id = workflow_id
        self.schedule_interval = schedule_interval

        # PubMed download parameters
        self.ftp_server_url = ftp_server_url  # FTP server URL
        self.baseline_path = "/pubmed/baseline/"
        self.updatefiles_path = "/pubmed/updatefiles/"

        self.check_md5_hash = check_md5_hash
        self.max_download_retry = max_download_retry
        self.max_processes = max_processes
        self.batch_size = batch_size

        # After how many downloads to reset the connection to Pubmed's FTP server.
        self.reset_ftp_counter = reset_ftp_counter

        # If this is the first ever run of the telescope, download the "baseline" database and build on this.
        ## Use if first dag run later instead of this.

        # TODO: Make condition for when the new baseline is out - after the release date for it each year.
        # if present day after the new baseline date then download baseline or
        # this is the very first release for the telescope

        self.add_setup_task(self.check_dependencies)
        self.add_task(self.check_releases)
        self.add_task(self.download)
        self.add_task(self.upload_downloaded)
        self.add_task(self.transform)
        self.add_task(self.upload_transformed)
        self.add_task(self.bq_ingest_update_tables)
        self.add_task(self.bq_add_updates_to_main_table)
        # self.add_task(self.add_release)
        self.add_task(self.cleanup)

        # self.add_task(self.add_release)

    def make_release(self, **kwargs) -> PubMedRelease:
        """Creates a new Pubmed release instance

        :return: The Pubmed release instance
        """
        release_date = kwargs["data_interval_end"]

        self.data_interval_start = kwargs["data_interval_start"]
        self.data_interval_end = kwargs["data_interval_end"]
        self.first_release = is_first_release(self.workflow_id)
        # Check for if the initial baseline table exists and what pubmed IDs are in he description??????????????????????????/

        logging.info(
            f"Start data date: {self.data_interval_start} End date: {self.data_interval_end} First release: {self.first_release}"
        )

        self.release_id = (
            self.data_interval_start.strftime("%Y_%m_%d") + "-" + self.data_interval_end.strftime("%Y_%m_%d")
        )
        logging.info(f"Release date interval: {self.release_id}")

        release = PubMedRelease(dag_id=self.dag_id, release_id=self.release_id)

        # TODO: Make this properties on the release.
        self.workflow_folder = make_workflow_folder(self.dag_id, release_date)
        self.download_folder = make_workflow_folder(self.dag_id, release_date, SubFolder.downloaded.value)
        self.transform_folder = make_workflow_folder(self.dag_id, release_date, SubFolder.transformed.value)

        if self.data_interval_start < self.start_date + timedelta(days=7) and not bigquery_table_exists(
            project_id=self.project_id, dataset_id=self.dataset_id, table_name=self.main_table
        ):
            self.download_baseline = True
        else:
            self.download_baseline = False

        return release

    def check_releases(self, release: PubMedRelease, **kwargs):
        """TODO: Update this doc string"""

        # Open FTP connection to PubMed servers.
        ftp_conn = ftplib.FTP(self.ftp_server_url, timeout=1000000.0)
        ftp_conn.login()  # anonymous login (publicly available data)

        files_to_download = []

        if self.download_baseline:
            logging.info(f"This is the first release of the PubMed Telescope. Grabbing list of 'baseline' files. ")

            # Change to the baseline directory.
            ftp_conn.cwd(self.baseline_path)

            # Find all the xml.gz files available from the server.
            baseline_list = ftp_conn.nlst()
            files_to_download = [
                self.baseline_path + file for file in baseline_list if (file.split(".")[-1] == "gz" in file)
            ]
            files_to_download.sort()

            logging.info(f"List of files to download from the PubMed FTP server for 'baseline': {files_to_download}")

        else:
            logging.info(
                f"Grabbing list of 'updatefiles' for this release: {self.data_interval_start} to {self.data_interval_end}"
            )

            # Change to updatefiles directory
            ftp_conn.cwd(self.updatefiles_path)

            # Find all the xml.gz files available from the server.
            updatefiles_list = ftp_conn.nlst()
            updatefiles_xml_gz = [file for file in updatefiles_list if (file.split(".")[-1] == "gz" in file)]
            updatefiles_xml_gz.sort()

            # Only return list of update files that are within the release period.
            updatefiles_to_download = [
                self.updatefiles_path + file
                for file in updatefiles_xml_gz
                if pendulum.from_format(ftp_conn.sendcmd("MDTM {}".format(file))[4:], "YYYYMMDDHHmmss")
                in pendulum.period(self.data_interval_start, self.data_interval_end)
            ]

            # Check that all update files that will be used are sequential.
            updatefiles_to_download.sort()
            file_index_last = int(re.findall(r"\d{4}", updatefiles_to_download[0].split("/")[-1].split(".")[0])[0])
            logging.info(f"file_index_last: {file_index_last}")
            for i in range(1, len(updatefiles_to_download), 1):
                file_index = int(re.findall(r"\d{4}", updatefiles_to_download[i].split("/")[-1].split(".")[0])[0])
                if file_index != file_index_last + 1:
                    raise AirflowException(
                        f"The update files are not going to be sequential. Please investigate download {file_index} and {file_index_last+1}"
                    )
                else:
                    file_index_last = file_index

            # TODO: Check that last release index is file_index_last + 1

            logging.info(
                f"List of files to download from the PubMed FTP server for 'updatefiles': {updatefiles_to_download}"
            )

            files_to_download.extend(updatefiles_to_download)

        # Close the connection to the FTP server.
        ftp_conn.close()

        # Push list of files to download into the xcom.
        ti: TaskInstance = kwargs["ti"]
        ti.xcom_push(key="files_to_download", value=files_to_download)

    def download(self, release: PubMedRelease, **kwargs):
        """Download files from PubMed's FTP server for this release.

        Unable to do this in parallel because their FTP server is not able to handle too many requests.
        """

        # Grab list of files to download from xcom
        ti: TaskInstance = kwargs["ti"]
        files_to_download = ti.xcom_pull(key="files_to_download")

        # For testing
        if self.download_baseline:
            if len(files_to_download) < 3:
                num_to_download = len(files_to_download)
            else:
                num_to_download = 3
        else:
            num_to_download = len(files_to_download)

        logging.info(
            f"Files to download from PubMed for this release ({len(files_to_download)}): {files_to_download[:num_to_download]}"
        )

        # Open FTP connection
        ftp_conn = ftplib.FTP(self.ftp_server_url, timeout=1000000000.0)
        ftp_conn.login()  # anonymous login (publicly available data)

        downloaded_files_for_release = []

        # for file_on_ftp in files_to_download:
        for i in range(0, num_to_download, 1):  # -  for testing
            file_on_ftp = files_to_download[i]

            # The FTP server disconnects the connection for the download after some time.
            # Need to have it refreshed every so often so we can reliably download it.

            # Every self.reset_ftp_counter number of files that are downloaded, reinitialise the FTP connection.
            if i % self.reset_ftp_counter == 0 and i != 0:
                # Close exisiting FTP connection.
                ftp_conn.close()

                # Open a new FTP connection
                ftp_conn = ftplib.FTP(self.ftp_server_url, timeout=1000000000.0)
                ftp_conn.login()

                logging.info(
                    f"FTP connection to Pubmed's servers has been reset after {self.reset_ftp_counter} downloads to avoid issues."
                )

            file = file_on_ftp.split("/")[-1]

            # Save file to correct download path for the workflow
            file_download_location = os.path.join(self.download_folder, file)

            if self.check_md5_hash:
                download_attemp_count = 1
                download_success = False
                while download_attemp_count <= self.max_download_retry and not download_success:
                    logging.info(f"Downloading: {file} Attempt: {download_attemp_count}")
                    try:
                        # Download file
                        with open(file_download_location, "wb") as f:
                            ftp_conn.retrbinary(f"RETR {file_on_ftp}", f.write)
                    except:
                        logging.info(
                            f"Unable to download {file_on_ftp} from PubMed's FTP server {self.ftp_server_url}."
                        )

                    # Create the hash from the above downloaded file.
                    with open(file_download_location, "rb") as f_in:
                        data = f_in.read()
                        md5hash_from_download = hashlib.md5(data).hexdigest()

                    # Need to have a download catch for the hash file as well, otherwise it can break the download loop.
                    try:
                        # Download corresponding md5 hash.
                        with open(f"{file_download_location}.md5", "wb") as f:
                            ftp_conn.retrbinary(f"RETR {file_on_ftp}.md5", f.write)
                    except:
                        logging.info(
                            f"Unable to download {file_on_ftp}.md5 from PubMed's FTP server {self.ftp_server_url}."
                        )

                    # Peep into md5 file.
                    with open(f"{file_download_location}.md5", "r") as f_md5:
                        md5_from_pubmed_ftp = f_md5.read()

                    # If md5 does not match, raise an Airflow exception.
                    if md5hash_from_download in md5_from_pubmed_ftp:
                        download_success = True
                        downloaded_files_for_release.append(file_download_location)
                    else:
                        logging.info(f"MD5 hash does not match the given MD5 checksum from server: {file}")

                    download_attemp_count += 1

                if not download_success:
                    raise AirflowException(
                        f"Unable to download {file_on_ftp} from PubMed's FTP server {self.ftp_server_url} after {self.max_download_retry}"
                    )

            else:
                logging.info(f"Downloading: {file}")
                try:
                    # Download file
                    with open(file_download_location, "wb") as f:
                        ftp_conn.retrbinary(f"RETR {file_on_ftp}", f.write)
                except:
                    raise AirflowException(
                        f"Unable to download {file_on_ftp} from PubMed's FTP server {self.ftp_server_url}"
                    )

                downloaded_files_for_release.append(file_download_location)

        # Close the FTP connection after downloading the required files.
        ftp_conn.close()

        # Push list of downloaded files into the xcom
        ti.xcom_push(key="downloaded_files_for_release", value=downloaded_files_for_release)

    def upload_downloaded(self, release: PubMedRelease, **kwargs):
        """Put all downloaded files onto GCS."""

        # Pull list of downloaded files from xcom
        ti: TaskInstance = kwargs["ti"]
        downloaded_files_for_release = ti.xcom_pull(key="downloaded_files_for_release")

        uploaded_download_files = []
        for file_to_upload in downloaded_files_for_release:
            file = file_to_upload.split("/")[-1]

            try:
                # TODO: how to get proper path of the GCS blobs?
                blob_name = f"telescopes/{self.dag_id}/{self.release_id}/{file}"
                logging.info(f"Uploading file {file} to GCS bucket {self.download_bucket} and {blob_name}")
                success = upload_file_to_cloud_storage(
                    bucket_name=self.download_bucket,
                    blob_name=blob_name,
                    file_path=file_to_upload,
                    check_blob_hash=False,
                )

                uploaded_download_files.append(file_to_upload)

            except:
                raise AirflowException(f"Unable to upload file: {file} to GCS bucket {self.download_bucket}")

        # Push to xcom for keeping track of the release files.
        ti.xcom_push(key="uploaded_download_files", value=uploaded_download_files)

    def transform(self, release: PubMedRelease, **kwargs):
        """Transform the *.xml.gz files downloaded from PubMed's FTP server into usable jsonl files for BigQuery.

        This task pulls all of the PubmedArticle, PubmedBookArticle and BookDocument entires from the Pubmed files.
        It runs the transform task in parallel is max_processes. Limited by the amount of RAM on the system that it is run on.

        Requires ~30gb of RAM to run.

        :return: None.
        """

        # Grab list of files downloaded from previous step.
        ti: TaskInstance = kwargs["ti"]
        downloaded_files_for_release = ti.xcom_pull(key="downloaded_files_for_release")

        # List to hold metadata and file lists about how to pull the data from Pubmed.

        # TODO: Move this to the init?
        # Only the article additions and deletions are populated in Pubmed.
        entity_list = {
            "article_additions": {
                "data_type": "additions",
                "sub_key": "PubmedArticle",
                "set_key": "PubmedArticleSet",
                "pmid_key_loc": "MedlineCitation",
                "output_file_base": f"pubmed_article_additions_{self.release_id}",
                "transform_files": [],
                "merged_transform_files": [],
                "uploaded_to_gcs": [],
                "schema_file_path": os.path.join(default_schema_folder(), "pubmed_articles_2023-01-01.json"),
                "table_list": [],
                "table_row_count": None,
            },
            # "pubmed_book_article_additions": {
            #     "data_type": "additions",
            #     "sub_key": "PubmedBookArticle",
            #     "set_key": "PubmedBookArticleSet",
            #     "pmid_key_loc": "MedlineCitation",
            #     "output_file_base": f"pubmed_book_article_additions_{self.release_id}",
            #     "transform_files": [],
            #     "merged_transform_files": [],
            #     "uploaded_to_gcs": [],
            #     "schema_file_path": os.path.join(default_schema_folder(), "pubmed_articles_2023-01-01.json"),
            #     "table_list": [],
            #     "table_row_count": None,
            # },
            # "book_document_additions": {
            #     "data_type": "additions",
            #     "sub_key": "BookDocument",
            #     "set_key": "BookDocumentSet",
            #     "pmid_key_loc": "BookDocument",
            #     "output_file_base": f"book_document_additions_{self.release_id}",
            #     "transform_files": [],
            #     "merged_transform_files": [],
            #     "uploaded_to_gcs": [],
            #     "schema_file_path": os.path.join(default_schema_folder(), "pubmed_articles_2023-01-01.json"),
            #     "table_list": [],
            #     "table_row_count": None,
            # },
            "article_deletions": {
                "data_type": "deletions",
                "sub_key": "PMID",
                "set_key": "DeleteCitation",
                "output_file_base": f"pubmed_article_deletions_{self.release_id}",
                "transform_files": [],
                "merged_transform_files": [],
                "uploaded_to_gcs": [],
                "schema_file_path": os.path.join(default_schema_folder(), "pubmed_deletions_2023-01-01.json"),
                "table_list": [],
                "table_row_count": None,
            },
            # "book_document_deletions": {
            #     "data_type": "deletions",
            #     "sub_key": "PMID",
            #     "set_key": "DeleteDocument",
            #     "output_file_base": f"book_document_deletions_{self.release_id}",
            #     "transform_files": [],
            #     "merged_transform_files": [],
            #     "uploaded_to_gcs": [],
            #     "schema_file_path": os.path.join(default_schema_folder(), "pubmed_deletions_2023-01-01.json"),
            #     "table_list": [],
            #     "table_row_count": None,
            # },
        }

        # Process files in batches so that ProcessPoolExecutor doesn't deplete the system of memory
        for i, chunk in enumerate(get_chunks(input_list=downloaded_files_for_release, chunk_size=4)):
            with ProcessPoolExecutor(max_workers=4) as executor:
                logging.info(f"In chunk {i} and processing files: {chunk}")

                futures = [
                    executor.submit(transform_pubmed_xml_file_to_jsonl, input_file, entity_list, self.transform_folder)
                    for input_file in chunk
                ]

                # Gather list of files transformed by the above.
                for future in as_completed(futures):
                    returned_entity_list = future.result()

                    # Merge the list of transform files from mulitprocessing step
                    for name, entity in returned_entity_list.items():
                        entity_list[name]["transform_files"].extend(entity["transform_files"])

        # Push updated entity metadata into the Xcom
        ti.xcom_push(key="entity_list", value=entity_list)

    def upload_transformed(self, release: PubMedRelease, **kwargs):
        """Upload the transformed files to GCS.

        :return: None
        """

        # Pull entity_list from xcom
        ti: TaskInstance = kwargs["ti"]
        entity_list = ti.xcom_pull(key="entity_list")

        for name, entity in entity_list.items():
            entity["transform_files"].sort()

            for file_to_upload in entity["transform_files"]:
                file = file_to_upload.split("/")[-1]

                try:
                    blob_name = f"telescopes/{self.dag_id}/{self.release_id}/{file}"
                    logging.info(f"Uploading file {file} to GCS bucket {self.transform_bucket} and {blob_name}")
                    upload_file_to_cloud_storage(
                        bucket_name=self.transform_bucket,
                        blob_name=blob_name,
                        file_path=file_to_upload,
                        check_blob_hash=False,
                    )

                    # Add uri to entity data for transferring to BQ
                    entity["uploaded_to_gcs"].append(f"gs://{self.transform_bucket}/{blob_name}")

                except:
                    raise AirflowException(f"Unable to upload file: {file} to GCS bucket {self.transform_bucket}")

            # Sort list for later
            entity["uploaded_to_gcs"].sort()

            # Update entity_list with the new list of GCS blobs.
            entity_list[name] = entity

        # Push updated entity_lsit into the xcom.
        ti.xcom_push(key="entity_list", value=entity_list)

    def bq_ingest_update_tables(self, release: PubMedRelease, **kwargs):
        """
        Ingest data into tables for this particular release.

        If first run of the workflow, it will only upload the baseline table.

        :return: None.
        """

        # Pull lsit of files to transfer from GCS to BQ
        ti: TaskInstance = kwargs["ti"]
        entity_list = ti.xcom_pull(key="entity_list")

        # If its the first run of the workflow, only upload the baseline table to the main table.
        if self.download_baseline:
            # Create the dataset if it doesn't exist already.
            try:
                create_bigquery_dataset(
                    self.project_id,
                    self.dataset_id,
                    self.data_location,
                    description="Pubmed dataset for Medline Citation records.",
                )
                logging.info(f"Created dataset: {self.project_id}.{self.dataset_id}")
            except:
                logging.info(f"Dataset {self.project_id}.{self.dataset_id} already exists.")

            # Delete old baseline table just in case there was a bad previous run.
            bq_delete_table(
                project_id=self.project_id, dataset_id=self.dataset_id, table_id=self.main_table, not_found_okay=True
            )

            logging.info(f"Uploading to table - {self.main_table}")

            # Create a transfer job into BQ from GCS for all of the transformed baseline files.
            transform_blob_pattern = f"gs://{self.transform_bucket}/telescopes/{self.dag_id}/{self.release_id}/*"

            logging.info(
                f"Creating a load job for all of the smaller baseline files with pattern: {transform_blob_pattern} "
            )

            success = load_bigquery_table(
                transform_blob_pattern,
                self.dataset_id,
                self.data_location,
                self.main_table,
                entity_list["article_additions"]["schema_file_path"],
                self.source_format,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                project_id=self.project_id,
                table_description="Baseline table of the Pubmed database.",
                partition=False,
            )

            if success:
                entity_list["article_additions"]["table_list"].append(
                    f"{self.project_id}.{self.dataset_id}.{self.main_table}"
                )
            else:
                raise AirflowException(f"Unable to upload table {self.main_table}")

        # Not the first workflow run, only ingest update tables into BQ.
        else:
            # Create a temporary dataset for injesting the tables and not make main dataset messy with small update tables.
            # Unable to partition on dates or indicies - could look into another method later.
            try:
                create_bigquery_dataset(
                    self.project_id,
                    self.temp_dataset,
                    self.data_location,
                    description="Temporary dataset for Pubmed workflow for the update tables for current release.",
                )
                logging.info(f"Created working dataset: {self.project_id}.{self.temp_dataset}")
            except:
                raise AirflowException(f"Working dataset {self.project_id}.{self.temp_dataset} already exists.")
                # Should be deleted at the end of each run. If it still exists it means that the cleanup step didn't run properly.

            # Upload release tables for this data interval.
            for name, entity in entity_list.items():
                entity["table_list"] = []

                for transform_blob in entity["uploaded_to_gcs"]:
                    # Create a table for each of the pubmed updates.
                    file_index = transform_blob.split("_")[-1].split(".")[0]
                    table_id = f"{name}_{file_index}"
                    table_description = f"Update table from file {file_index}"

                    # Delete old tables with table_id just in case there was a bad previous run.
                    # We don't want to append or add any dupliactes to the tables.
                    bq_delete_table(
                        project_id=self.project_id, dataset_id=self.temp_dataset, table_id=table_id, not_found_okay=True
                    )

                    logging.info(f"Uploading to table - {table_id}")

                    success = load_bigquery_table(
                        transform_blob,
                        self.temp_dataset,
                        self.data_location,
                        table_id,
                        entity["schema_file_path"],
                        self.source_format,
                        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                        project_id=self.project_id,
                        table_description=table_description,
                        partition=False,
                    )

                    if success:
                        if not table_id in entity["table_list"]:
                            entity["table_list"].append(f"{self.project_id}.{self.temp_dataset}.{table_id}")
                    else:
                        raise AirflowException(
                            f"Unable to upload table {self.project_id}.{self.temp_dataset}.{table_id}"
                        )

                if success:
                    logging.info(
                        f"""Successfully uploaded to table - {name} with {len(entity['table_list'])} tables. """
                    )

                    # Update the entity after a successful table upload.
                    entity_list[name] = entity

        #  Update workflow metadata of tables.
        entity_list = ti.xcom_push(key="entity_list", value=entity_list)

    def bq_add_updates_to_main_table(self, release: PubMedRelease, **kwargs):
        """
        Updates the main table with the additions and deletions from the update files.

        :param release: Pubmed release.
        :return: None.
        """

        # TODO: Add a dependant check on if previous release was done. Can only run if the last data_interval_period was done.

        # Pull workflow entity data
        ti: TaskInstance = kwargs["ti"]
        entity_list = ti.xcom_pull(key="entity_list")

        # Use the table from this workflow run to make the first snapshot.
        # This should be just the baseline dataset, so there should be no deletions to the snapshot.
        if self.download_baseline:
            # TODO: Asssert that the main table exists.

            logging.info(f"Table {self.main_table} has already been created and injested in the previous step.")

        else:
            if len(entity_list["article_additions"]["table_list"]) != len(
                entity_list["article_deletions"]["table_list"]
            ):
                # Stop workflow as something has gone wrong in the upload table step.
                raise AirflowException("Number of addition and deletion record tables are not the same.")

            num_update_tables = len(entity_list["article_additions"]["table_list"])

            # Create a snapshot of main table just in case something happens with the below updates and deletes
            # and we can revert if necessary.
            # This will be replaced with a function in the new version of the workflow.
            backup_table_id = bigquery_sharded_table_id(f"{self.main_table}_backup", self.data_interval_start)
            create_backup_table = f"""
            CREATE SNAPSHOT TABLE `{self.project_id}.{self.dataset_id}.{backup_table_id}`
            CLONE `{self.project_id}.{self.dataset_id}.{self.main_table}`
            """
            bq_output = run_bigquery_query(create_backup_table)
            logging.info(f"Output from 'create_backup_table' query: {bq_output}")

            entity_list["article_additions"]["table_list"].sort()
            entity_list["article_deletions"]["table_list"].sort()

            logging.info(
                f'Addition table list to apply to main_table: {entity_list["article_additions"]["table_list"]}'
            )
            logging.info(
                f'Deletion table list to apply to main_table: {entity_list["article_deletions"]["table_list"]}'
            )

            for i in range(num_update_tables):
                logging.info(f"Currently doing - {entity_list['article_additions']['table_list'][i]}")

                # Ensure that the file_index is the same for the addition and deletion tables.
                add_file_index = entity_list["article_additions"]["table_list"][i].split(".").split("_")[-1]
                del_file_index = entity_list["article_deletions"]["table_list"][i].split(".").split("_")[-1]

                if add_file_index != del_file_index:
                    raise AirflowException(
                        f"File indices do not match for these additions and deletions! Additions: {add_file_index} Deletions: {del_file_index}"
                    )

                logging.info(f"Currently doing additions from - {entity_list['article_additions']['table_list'][i]}")

                # Remove the records that have updates from the incoming additions table.
                # This will be done with an upsert merge with the next version of the workflows.
                delete_records_from_main_table_from_incoming_additions = f"""
                DELETE FROM `{self.project_id}.{self.dataset_id}.{self.main_table}`
                WHERE MedlineCitation.PMID.value IN (
                SELECT MedlineCitation.PMID.value
                FROM `{entity_list['article_additions']['table_list'][i]}`)
                """
                bq_output = run_bigquery_query(delete_records_from_main_table_from_incoming_additions)
                logging.info(f"Output from 'delete_records_from_main_table_from_incoming_additions' query: {bq_output}")

                # Insert the additions in to the main table.
                insert_records_into_main_table_from_incoming_additions = f"""
                INSERT INTO `{self.project_id}.{self.dataset_id}.{self.main_table}` 
                SELECT * 
                FROM `{entity_list['article_additions']['table_list'][i]}`
                """
                bq_output = run_bigquery_query(insert_records_into_main_table_from_incoming_additions)
                logging.info(f"Output from 'insert_records_into_main_table_from_incoming_additions' query: {bq_output}")

                logging.info(f"Currently doing deletions from - {entity_list['article_deletions']['table_list'][i]}")

                # Delete records from the main table using the incoming deletions, matching on the PMID and Version.
                delete_records_from_main_table_from_incoming_deletions = f"""
                DELETE FROM `{self.project_id}.{self.dataset_id}.{self.main_table}`
                WHERE (MedlineCitation.PMID.value, MedlineCitation.PMID.Version) IN (
                SELECT (value, Version) 
                FROM `{entity_list['article_deletions']['table_list'][i]}`)
                """
                bq_output = run_bigquery_query(delete_records_from_main_table_from_incoming_deletions)
                logging.info(f"Output from 'delete_records_from_main_table_from_incoming_deletions' query: {bq_output}")

                # TODO: Update the main table description with the new update file index.
                # description = f"""Pubmed Article table with baseline files pubmed23n0001-pubmed23n1166 and updatefiles from pubmed23n1167-pubmed23nxxxx"""

                # TODO: Do some sort of check step on the main table to ensure that it's been done right?
                # Re result from the deletions and see if the length of the

    def add_release(self, release: PubMedRelease, **kwargs):
        """Add the release info to the observatory API.

        :return: None.
        """

        # Add the PubMed release into to the API
        # Include the following into the release info

        # Index of pubmed files from their FTP server.
        # data_interval_start and data_interval_end
        # Names of tables made for this release
        # Name of Snapshot (partition date)
        # Create

    def cleanup(self, release: PubMedRelease, **kwargs):
        """Cleanup files from this workflow run.

        Delete local download files, tranform files and left over tables in BQ.

        :return: None.
        """

        # Pull from Xcom
        ti: TaskInstance = kwargs["ti"]
        downloaded_files_for_release = ti.xcom_pull(key="downloaded_files_for_release")
        entity_list = ti.xcom_pull(key="entity_list")

        logging.info(f"Deleting the locally downloaded files for this release.")
        for file in downloaded_files_for_release:
            logging.info(f"Deleting file = {file}")
            try:
                os.remove(file)
            except:
                logging.info(f"File {file} does not exist.")

        # For each entity, delete the local transform files
        for name, entity in entity_list.items():
            logging.info(f"Deleting local transform files for - {name}")
            for transform_file in entity["transform_files"]:
                logging.info(f"Deleting file = {file}")
                try:
                    os.remove(transform_file)
                except:
                    logging.info(f"File {transform_file} does not exist.")

        if not self.download_baseline:
            logging.info(
                f"Deleting the dataset {self.project_id}.{self.temp_dataset} and all update tables inside it for this release."
            )
            try:
                # Delete the update tables from BQ by deleting the whole temporary dataset.
                delete_old_datasets_with_prefix(prefix=self.temp_dataset, age_to_delete=0)
            except:
                raise AirflowException(f"Unable to delete dataset: {self.project_id}.{self.temp_dataset}")

        # TODO: Remove all task instance data from this workflow run.


def transform_pubmed_xml_file_to_jsonl(input_file: str, entity_list: Dict, transform_folder: str) -> Dict:
    """
    Convert a single Pubmed XML file to JSONL, pulling out any of the Pubmed additions and or deletions.

    Used in parallelised section "transform". Each file is given to an individual process
    rather than multple files read in and transformed at once.

    :param input_file: Absolute path to the Pubmed xml.gz file.
    :param download_folder: Absolute path to the download folder set for the release.
    :param entity_list: Dictionary object with details of how to pull out the Pubmed data.
    :return entity_list: Update dictionary object with updated list of transform files.
    """

    pubmed_file_id = Path(input_file).name.split(".")[0]

    logging.info(f"Reading in file - {input_file}")

    with gzip.open(input_file, "rb") as f_in:
        # Use the BioPython library for reading in the Pubmed XML files.
        data_dict_dirty = Entrez.read(f_in, validate=True)

        # Need to have the XML attributes pulled out from the Biopython data classes.
        data_dict = add_attributes_to_data_from_biopython_classes(data_dict_dirty)

    # now a key, value - loop through entities
    for name, entity in entity_list.items():
        # Have to set this to empty here because python multi processing is dumb.
        entity_list[name]["transform_files"] = []

        # Pull out each record type from the data.
        data_type, set_key, sub_key = (
            entity["data_type"],
            entity["set_key"],
            entity["sub_key"],
        )

        logging.info(
            f"Extracting {set_key if data_type == 'deletions' else sub_key} records from file - {pubmed_file_id}"
        )

        try:
            try:
                data_part = [retrieve for retrieve in data_dict[set_key][sub_key]]
            except:
                data_part = [retrieve for retrieve in data_dict[sub_key]]

            logging.info(
                f"Pulled out {len(data_part)} {f'{set_key} records' if data_type == 'deletions' else f'{sub_key} {data_type}'} from file - {pubmed_file_id}"
            )

        except:
            logging.info(
                f"No {f'{set_key} records' if data_type == 'deletions' else f'{sub_key} {data_type}'} records in file - {pubmed_file_id}"
            )
            data_part = []

        output_file = os.path.join(transform_folder, entity["output_file_base"] + "_" + pubmed_file_id + ".jsonl.gz")

        logging.info(f"Writing {name} {data_type} to file - {output_file}")

        # Write to compressed *.jsonl.gz
        with gzip.open(output_file, "w") as f_out:
            for line in data_part:
                f_out.write(str.encode(json.dumps(line, cls=CustomEncoder) + "\n"))

        entity_list[name]["transform_files"].append(output_file)

    return entity_list


# TODO: Clean up this function if possible. Too many messy if statements and is slow.
def add_attributes_to_data_from_biopython_classes(obj):
    """
    Recursively travel down the data tree and add attributes from the biopython data classes as dictionary keys.

    :param obj: Input object, any type.
    :return new: Object with attributes added as keys.
    """

    if isinstance(obj, StringElement):
        if len(list(obj.attributes.keys())) > 0:
            # New object to hold the string data.
            new = {}
            new["value"] = obj

            # Loop through attributes and add as needed.
            for key in list(obj.attributes.keys()):
                new[key] = add_attributes_to_data_from_biopython_classes(obj.attributes[key])
        else:
            new = obj

        return new

    if isinstance(obj, DictionaryElement):
        # New object to hold the string data.
        new = {}
        # Loop through attributes and add as needed.
        for key in list(obj.attributes.keys()):
            new[key] = add_attributes_to_data_from_biopython_classes(obj.attributes[key])

        # loop through values as needed
        for k, v in list(obj.items()):
            new[k] = add_attributes_to_data_from_biopython_classes(v)

        return new

    if isinstance(obj, (ListElement, OrderedListElement)):
        # New object to hold the string data.
        new = {}
        if len(obj) > 0:
            new[obj[0].tag] = [add_attributes_to_data_from_biopython_classes(v) for v in obj]
        try:
            # Loop through attributes and add as needed.
            for key in list(obj.attributes.keys()):
                new[key] = add_attributes_to_data_from_biopython_classes(obj.attributes[key])
        except:
            pass

        return new

    if isinstance(obj, list):
        new = [add_attributes_to_data_from_biopython_classes(v) for v in obj]
        return new

    else:
        return obj


# TODO: Clean up the below if possible and remove unnecessary comments about attributes
class CustomEncoder(json.JSONEncoder):

    """Custom encoder for JSON dump for it to write a dictionary field as a string of text for a
    number of select key values in the Pubmed data"""

    write_as_single_field = [
        "AbstractText",  # ? also has attributes
        "Affiliation",
        "ArticleTitle",  # ? also has attributes
        "b",
        "BookTitle",  # ? also has attributes
        "Citation",
        "CoiStatement",
        "CollectionTitle",  # ? also has attributes
        "CollectiveName",
        "i",
        "Param",  # ? also has attributes
        "PublisherName",
        "SectionTitle",  # ? also has attributes
        "sub",
        "Suffix",
        "sup",
        "u",
        "VernacularTitle",  # ? also has attributes
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

                # TODO: Reconsider if this is needed after review.
                # # To remove a list of a list e.g. "KeywordList": [["a","b"]] to "KeywordList": ["a","b"]
                # if k == "KeywordList" and len(v) > 0:
                #     new[k] = v[0]
                # else:
                #     new[k] = self._transform_obj_data(v)

            return new
        elif isinstance(obj, list):
            return [self._transform_obj_data(elem) for elem in obj]
        else:
            return obj

    def encode(self, obj):
        transformed_obj = self._transform_obj_data(obj)
        return super(CustomEncoder, self).encode(transformed_obj)


def bq_delete_table(
    project_id: str, dataset_id: str, table_id: str, partition_decorator: str = None, not_found_okay: bool = True
):
    # TODO: To add to gcs utils after the workflow restructure.

    """Delete a single table in Bigquery."""

    bq_client = bigquery.Client(project=project_id)
    dataset = bigquery.DatasetReference(project_id, dataset_id)
    table = bigquery.Table(dataset.table(table_id))

    try:
        # bq_client.get_table(table)
        if partition_decorator:
            bq_client.delete_table(table, tableId=partition_decorator, not_found_ok=not_found_okay)
        else:
            bq_client.delete_table(table, not_found_ok=not_found_okay)

        logging.info(f"Deleted exising bigquery table: {table_id} {partition_decorator}")
    except:
        raise AirflowException(f"An error occured deleting table {table_id}")
