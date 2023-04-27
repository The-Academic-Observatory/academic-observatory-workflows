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
import subprocess
from typing import Dict, List, OrderedDict, Tuple
import pendulum
import datetime
import logging
import re

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
    IntegerElement,
    NoneElement,
)

# Alternative method for injest
import xmlschema

import gzip
import json
import tarfile

# Parallelising libraries
import concurrent.futures

# Airflow modules
from airflow import AirflowException
from airflow.models.taskinstance import TaskInstance
from airflow.models.variable import Variable

from google.cloud.bigquery import SourceFormat
from academic_observatory_workflows.config import schema_folder as default_schema_folder
from observatory.platform.utils.airflow_utils import AirflowVars, make_workflow_folder
from observatory.platform.workflows.workflow import Workflow, Release

from observatory.platform.utils.gc_utils import (
    bigquery_table_exists,
    create_bigquery_snapshot,
    create_bigquery_dataset,
    create_empty_bigquery_table,
    run_bigquery_query,
)

from observatory.platform.utils.proc_utils import wait_for_process

from observatory.platform.utils.release_utils import (
    get_dataset_releases,
    get_datasets,
    get_latest_dataset_release,
    is_first_release,
)

from observatory.platform.utils.gc_utils import (
    bigquery_sharded_table_id,
    select_table_shard_dates,
    upload_file_to_cloud_storage,
)

from observatory.platform.utils.workflow_utils import (
    SubFolder,
    make_release_date,
)


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
        transform_files_regex = r"*.jsonl.gz"

        super().__init__(
            dag_id=dag_id,
            release_id=release_id,
            download_files_regex=download_files_regex,
            transform_files_regex=transform_files_regex,
        )


class PubMedTelescope(Workflow):
    DAG_ID_PREFIX = "pubmed"

    """PubMed Telescope

    Please visit:
    https://pubmed.ncbi.nlm.nih.gov/

    """

    def __init__(
        self,
        *,
        dag_id: str,
        workflow_id: int,
        start_date: str = pendulum.datetime(year=2022, month=12, day=4),
        # data_interval_start: pendulum.DateTime = pendulum.datetime(2022, 12, 8),
        schedule_interval: str = "0 0 * * 0",  # weekly
        catchup: bool = True,
        dataset_id: str = None,
        merge_partition_field: str = None,
        schema_folder: str = default_schema_folder(),
        dataset_type_id: str = None,
        # queue: str = "default",
        table_id: str,
        ftp_server_url: str,
        check_md5_hash: bool,
        max_download_retry: int = 5,
        dataset_description: str,
        source_format: str,
        airflow_vars: List[str] = None,
        max_processes: int,
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

        super().__init__(
            dag_id=dag_id,
            start_date=start_date,
            schedule_interval=schedule_interval,
            airflow_vars=airflow_vars,
            workflow_id=workflow_id,
            # queue=queue,
            # data_interval_start=data_interval_start,
            # dataset_id=dataset_id,
            # dataset_type_id=dataset_type_id,
            # merge_partition_field=merge_partition_field,
            # schema_folder=schema_folder,
            **kwargs,
        )

        # Databse settings
        self.project_id = Variable.get(AirflowVars.PROJECT_ID)
        self.download_bucket = Variable.get(AirflowVars.DOWNLOAD_BUCKET)
        self.transform_bucket = Variable.get(AirflowVars.TRANSFORM_BUCKET)

        self.dataset_description = dataset_description
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.full_table_id = f"{self.project_id}.{self.dataset_id}.{self.table_id}"
        self.source_format = source_format
        self.schema_folder = default_schema_folder()  # schema_folder

        # Workflow parameters
        self.workflow_id = workflow_id
        self.schedule_interval = schedule_interval

        # PubMed settings
        self.ftp_server_url = ftp_server_url  # FTP server URL
        self.baseline_path = "/pubmed/baseline/"
        self.updatefiles_path = "/pubmed/updatefiles/"

        self.check_md5_hash = check_md5_hash
        self.max_download_retry = max_download_retry
        self.max_processes = max_processes

        # If this is the first ever run of the telescope, download the "baseline" database and build on this.
        ## Use if first dag run later instead of this.
        self.download_baseline = False

        self.add_setup_task(self.check_dependencies)
        self.add_task(self.check_releases)  # check releases and get list of files to download
        self.add_task(self.download)  # download the xml files from the FTP server, shove into gzip
        # self.add_task(self.upload_downloaded)  # upload raw files from pubmed servers for specific releases
        self.add_task(self.transform)
        self.add_task(self.upload_transformed)  # upload additions and deletions files from transform/cleaning step

        # self.add_task(transform) # convert xml files into *.jsonl.gz, validate if neccesary using their API

        # TODO: All BQ steps, snapshot and other
        # self.add_task(self.bq_append_new)
        # self.add_task(self.bq_delete_old)
        # self.add_task(self.bq_create_snapshot)

        # add release to api to keep track.

    def make_release(self, **kwargs) -> PubMedRelease:
        """Creates a new Pubmed release instance

        :return: The Pubmed release instance
        """
        release_date = make_release_date(**kwargs)
        self.start_date, self.end_date, self.first_release = self.get_release_info(**kwargs)

        kwargs["data_interval_start"] = self.start_date

        # TODO: Figure out why the end date for the release is so wrong - should be + 7 days but its well over 2 months after the start date.
        # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! dates are cooked. Need to fix !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

        logging.info(f"Start date: {self.start_date} End date: {self.end_date} First release: {self.first_release}")
        self.release_id = self.start_date.strftime("%Y_%m_%d") + "-" + self.end_date.strftime("%Y_%m_%d")
        logging.info(f"Release date interval: {self.release_id}")

        release = PubMedRelease(dag_id=self.dag_id, release_id=self.release_id)

        self.workflow_folder = make_workflow_folder(self.dag_id, release_date)
        self.download_folder = make_workflow_folder(self.dag_id, release_date, SubFolder.downloaded.value)
        self.transform_folder = make_workflow_folder(self.dag_id, release_date, SubFolder.transformed.value)
        return release

    def get_release_info(self, **kwargs) -> Tuple[pendulum.DateTime, pendulum.DateTime, bool]:
        """Return release information with the start and end date. [start date, end date) Includes start date, excludes end date.
        :param kwargs: The context passed from the PythonOperator.
        :return: None.
        """

        # Check if first release or not
        first_release = is_first_release(self.workflow_id)
        if first_release:
            # When first release, set start date to the start of the DAG
            # Can we get away with using data_interval_start instead?
            start_date = pendulum.instance(kwargs["dag"].default_args["start_date"]).start_of("day")
        else:
            datasets = get_datasets(workflow_id=self.workflow_id)
            try:
                top_level_dataset = [
                    dataset for dataset in datasets if dataset.dataset_type.type_id == self.dataset_type_id
                ][0]
            except IndexError:
                raise AirflowException(
                    f"No top level dataset type set, expecting dataset type with type_id: {self.dataset_type_id}"
                )
            releases = get_dataset_releases(dataset_id=top_level_dataset.id)
            latest_release = get_latest_dataset_release(releases=releases)
            start_date = pendulum.instance(latest_release.end_date).start_of("day")

        # end_date = pendulum.instance(kwargs["dag"].default_args["start_date"]).start_of("day")
        end_date = pendulum.instance(kwargs["data_interval_end"])
        logging.info(f"Start date: {start_date}, end date: {end_date}, first release: {first_release}")

        return start_date, end_date, first_release

    def check_releases(self, release: PubMedRelease, **kwargs):
        """Use previous releases of the Pubmed telescope to determine which files are needed from the FTP server for the new release.

        :param workflow_id:
        :param ftp_server_url: Pubmed FTP server address.
        :param start_date: start of release period.
        :param start_date: end of release period.
        :param baseline_path: Path on the Pubmed FTP server to the 'baseline' files.
        :param updatefiles_path: Path on the Pubmed FTP server to the 'updatefiles_path' files.
        :return: List of files to download from the PubMed FTP server.
        """

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

        logging.info(f"Grabbing list of 'updatefiles' for this release: {self.start_date} to {self.end_date}")

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
            in pendulum.period(self.start_date, self.end_date)
        ]

        logging.info(
            f"List of files to download from the PubMed FTP server for 'updatefiles': {updatefiles_to_download}"
        )

        files_to_download.extend(updatefiles_to_download)

        # TODO: Check release api for last downloaded files, need to look at "extra" param.
        # Get list of all releases, check that sequence of files from FTP are are consecutive from beginning to end.
        # throw an error if there's a file missing in the sequence.

        # Close the connection to the FTP server.
        ftp_conn.close()

        # Push list of files to download into the xcom.
        ti: TaskInstance = kwargs["ti"]
        ti.xcom_push(key="files_to_download", value=files_to_download)

    def download(self, release: PubMedRelease, **kwargs):
        """Download files from PubMed's FTP server for this release.

        Unable to do this in parallel because their FTP server is not able to handle too many requests.
        """

        # TODO: Consider parallelising this section if files to download > 100

        # Grab list of files to download from xcom
        ti: TaskInstance = kwargs["ti"]
        files_to_download = ti.xcom_pull(key="files_to_download")

        logging.info(f"Files to download from PubMed for this release ({len(files_to_download)}): {files_to_download}")

        # Open FTP connection
        ftp_conn = ftplib.FTP(self.ftp_server_url, timeout=1000000.0)
        ftp_conn.login()  # anonymous login (publicly available data)

        # Having to do this in serial because their FTP server chucks errors when downloading in parallel.
        downloaded_files_for_release = []

        # for file_on_ftp in files_to_download:
        # For testing
        for i in range(0, 4, 1):  # -  for testing
            file_on_ftp = files_to_download[i]

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

                    # Download corresponding md5 hash.
                    with open(f"{file_download_location}.md5", "wb") as f:
                        ftp_conn.retrbinary(f"RETR {file_on_ftp}.md5", f.write)

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

        # Close the FTP connection to prevent errors.
        ftp_conn.close()

        # Push list of downloaded files into the xcom
        ti.xcom_push(key="downloaded_files_for_release", value=downloaded_files_for_release)

    def upload_downloaded(self, release: PubMedRelease, **kwargs):
        """Put all files into a tar ball and upload to GCS."""

        # Pull list of transform files from xcom
        ti: TaskInstance = kwargs["ti"]
        downloaded_files_for_release = ti.xcom_pull(key="downloaded_files_for_release")

        # put the files into a tarball first before uploading?
        # open tarbal
        # for each file
        # add to tarball

        uploaded_download_files = []
        for file_to_upload in downloaded_files_for_release:  # upload tarball of release files
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

                # ??? gcs uri ???
                uploaded_download_files.append(file_to_upload)

                # get gcs uri from the upload of the blob
            except:
                raise AirflowException(f"Unable to upload file: {file} to GCS bucket {self.download_bucket}")

        # Push to xcom for keeping track of the release files.
        ti.xcom_push(key="uploaded_download_files", value=uploaded_download_files)

    def transform(self, release: PubMedRelease, **kwargs):
        """Transform the *.xml.gz files from the FTP server into usable jsonl like files.

        This task pulls all of the PubmedArticle, PubmedBookArticle and BookDocument entires from the Pubmed files.

        compiles all the additions and deletions from the update files into separate tables.
        Loops through and checks if there are any deletions to apply before uploading them to Bigquery, to save cost.

        Matches on both the version of the publication and the PMID number.
        """

        # Read master dictionary from file.
        # Not all of Pubmed has the fields in the XML files.

        # If instance in dictionary exists, add to the matching master dict copy.
        # read in the schema and make sure that they're the same

        # Push details of the files into one dictionary for ease of use later, datafiles: { filename: blah, path: blah, files_included: (list of pubmed indexes) }

        # Grab list of files downloaded.
        ti: TaskInstance = kwargs["ti"]
        downloaded_files_for_release = ti.xcom_pull(key="downloaded_files_for_release")

        # Make paths for the additions and deletions files.
        pubmed_article_additions_file_path = os.path.join(
            self.transform_folder, f"pubmed_article_additions_{self.release_id}.jsonl"
        )
        pubmed_article_deletions_file_path = os.path.join(
            self.transform_folder, f"pubmed_article_deletions_{self.release_id}.jsonl"
        )

        pubmed_book_article_additions_file_path = os.path.join(
            self.transform_folder, f"pubmed_book_article_additions_{self.release_id}.jsonl"
        )

        book_document_additions_file_path = os.path.join(
            self.transform_folder, f"book_document_additions_{self.release_id}.jsonl"
        )
        book_document_deletions_file_path = os.path.join(
            self.transform_folder, f"book_document_deletions_{self.release_id}.jsonl"
        )

        pubmed_article_additions_for_release = []
        pubmed_article_deletions_for_release = []

        pubmed_book_article_additions_for_release = []
        # TODO: Check that pubmed_book_article_deletions are included in the pubmed_article_deletions or book_document_deletions ????????

        book_document_additions_for_release = []
        book_document_deletions_for_release = []

        # TODO: Consider parallelising this section.

        # dictionary for a list of chunks to keep the number of files limited to < 4gb, to make parallelising easier and uploading to BQ faster.

        # Loop through each of the PubMed database files and gather additions and deletions.
        # for file in downloaded_files_for_release:
        for i in range(0, 4, 1):  # -  for testing
            file = downloaded_files_for_release[i]  # -  for testing

            logging.info(f"Running through file - {file}")

            with gzip.open(file, "rb") as f_in:
                # Use the BioPython library for reading in the Pubmed xml files and putting into a usable format.
                data_dict_dirty = Entrez.read(f_in, validate=True)

                # Need to have the XML attributes pulled out from the special Biopython classes.
                data_dict = add_attributes_to_data_from_biopython_classes(data_dict_dirty)

                # Using the XMLSchema library - Slowest option of the two by 10x but more robust as it checks against the XSD schema during the import.
                # Please not that this parsing methoud does not work currently and the CustomStrEncoder below would need to be modified for it.
                # Path to the XSD schemas needed for this method.
                # pubmed_schema = xmlschema.XMLSchema(
                #     os.path.join(self.schema_folder, "pubmed_xsd_schemas", "pubmed_230101.xsd")
                # )
                # data_dict_dirty_keys = pubmed_schema.to_dict(f_in)
                # data_dict = change_keys(data_dict_dirty_keys, convert)

                # Grab additions from file.
                try:
                    logging.info(f"Extracting PubmedArticle records from file - {file}")

                    try:
                        additions = [addition for addition in data_dict["PubmedArticleSet"]["PubmedArticle"]]
                    except:
                        additions = [addition for addition in data_dict["PubmedArticle"]]

                    pubmed_article_additions_for_release.extend(additions)
                    logging.info(f"Pulled out {len(additions)} PubmedArticle additions from file - {file}")

                except:
                    logging.info(f"No PubmedArticle additions in file - {file}")

                try:
                    logging.info(f"Extracting PubmedBookArticle records from file - {file}")

                    try:
                        additions = [
                            addition for addition in data_dict["PubmedBookBookArticleSet"]["PubmedBookArticle"]
                        ]
                    except:
                        additions = [addition for addition in data_dict["PubmedBookArticle"]]
                    pubmed_book_article_additions_for_release.extend(additions)

                    logging.info(f"Pulled out {len(additions)} PubmedBookArticle additions from file - {file}")
                except:
                    logging.info(f"No PubmedBookArticle additions in file - {file}")

                try:
                    logging.info(f"Extracting BookDocument records from file - {file}")

                    try:
                        additions = [addition for addition in data_dict["BookDocumentSet"]["BookDocument"]]
                    except:
                        additions = [addition for addition in data_dict["BookDocument"]]
                    book_document_additions_for_release.extend(additions)

                    logging.info(f"Pulled out {len(additions)} BookDocument additions from file - {file}")
                except:
                    logging.info(f"No BookDocument additions in file - {file}")

                # Grab deletions from file.

                try:
                    logging.info(f"Extracting DeleteCitation records from file - {file}")
                    deletions = [deletion for deletion in data_dict["DeleteCitation"]]
                    pubmed_article_deletions_for_release.extend(deletions)
                    logging.info(f"Pulled out {len(deletions)} DeleteCitation records from file - {file}")
                except:
                    logging.info(f"No DeleteCitation records in file - {file}")

                try:
                    logging.info(f"Extracting DeleteDocument records from file - {file}")
                    deletions = [deletion for deletion in data_dict["DeleteDocument"]]
                    book_document_deletions_for_release.extend(deletions)
                    logging.info(f"Pulled out {len(deletions)} DeleteDocument records from file - {file}")
                except:
                    logging.info(f"No DeleteDocument records in file - {file}")

        logging.info(
            "Checking through the additions for this release to see if any deletions can be done before going onto BigQuery."
        )

        # TODO: Do a check step to make sure there are no doubles of the deletes?? Each deletion *should* be unique in the DB.

        # TODO: Consider parallelising this section, and make it a function to clean up this section - initial baseline additions will be ~ 35 million entries and needs to be quicker for checking deletions.

        ### Pubmed Article pre-check for deletions
        try:
            logging.info("Running though PubmedArticle record for deletions.")

            pubmed_article_additions_with_dels_checked = pubmed_article_additions_for_release.copy()
            pubmed_article_deletions_with_dels_checked = pubmed_article_deletions_for_release.copy()

            # print(pubmed_article_additions_with_dels_checked[0])
            for record_to_delete in pubmed_article_deletions_for_release:
                for article_addition_to_check in pubmed_article_additions_for_release:
                    to_check = article_addition_to_check["MedlineCitation"]["PMID"]
                    if record_to_delete == to_check:
                        pubmed_article_deletions_with_dels_checked.remove(record_to_delete)
                        pubmed_article_additions_with_dels_checked.remove(article_addition_to_check)

                        # logging.info(f"Removed the following record from additions list - {record_to_delete}")

            logging.info(
                f"There are {len(pubmed_article_deletions_with_dels_checked)} article deletions left to do on BQ and {len(pubmed_article_additions_with_dels_checked)} article additions to add to the snapshot for this release."
            )

        except:
            logging.info("No PubMedArticle records to check against for deletion.")

        ### Pubmed Book Article pre-check for deletions
        try:
            logging.info("Running though PubmedBookArticle record for deletions.")

            # PubmedBookArticle uses the PMID from the DeleteCitation records.
            pubmed_book_article_deletions_for_release = pubmed_article_deletions_with_dels_checked.copy()

            pubmed_book_article_additions_with_dels_checked = pubmed_book_article_additions_for_release.copy()
            pubmed_book_article_deletions_with_dels_checked = pubmed_book_article_deletions_for_release.copy()

            # print(pubmed_book_article_additions_with_dels_checked[0])
            for record_to_delete in pubmed_book_article_deletions_for_release:
                for book_article_addition_to_check in pubmed_book_article_additions_for_release:
                    to_check = book_article_addition_to_check["BookDocument"]["PMID"]
                    if record_to_delete == to_check:
                        pubmed_book_article_deletions_with_dels_checked.remove(record_to_delete)
                        pubmed_book_article_additions_with_dels_checked.remove(article_addition_to_check)

                        # logging.info(f"Removed the following record from additions list - {record_to_delete}")

            logging.info(
                f"There are {len(pubmed_book_article_deletions_with_dels_checked)} book article deletions left to do on BQ and {len(pubmed_book_article_additions_with_dels_checked)} book article additions to add to the snapshot for this release."
            )

        except:
            logging.info("No PubMedArticle records to check against for deletion.")

        ### Book Document pre-check for deletions
        try:
            logging.info("Running though BookDocument for deletions.")

            book_document_additions_with_dels_checked = book_document_additions_for_release.copy()
            book_document_deletions_with_dels_checked = book_document_deletions_for_release.copy()

            # print(book_document_additions_with_dels_checked[0])
            for record_to_delete in book_document_deletions_for_release:
                for book_addition_to_check in book_document_additions_for_release:
                    to_check = book_addition_to_check["BookDocument"]["PMID"]
                    if record_to_delete == to_check:
                        book_document_deletions_with_dels_checked.remove(record_to_delete)
                        book_document_additions_with_dels_checked.remove(book_addition_to_check)

                        # logging.info(f"Removed the following record from additions list - {record_to_delete}")

            logging.info(
                f"There are {len(book_document_deletions_with_dels_checked)} BookDocument deletions left to do on BQ and {len(book_document_additions_with_dels_checked)} additions to add to the snapshot for this release."
            )
        except:
            logging.info("No BookDocument records to check against for deletion.")

        ################################################################################################

        # # Working on additions file
        # logging.info(f"Removing bad characters from keys from additions file  data.")

        # # Give each thread a chunk of the work to do.
        # chunk_size = len(additions_with_dels_checked) // self.max_processes
        # chunks = [additions_with_dels_checked[i : i + chunk_size] for i in range(0, len(additions_with_dels_checked), chunk_size)]

        # additions_cleaned_fieldnames = []
        # with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_processes) as executor:
        #     # send off chunks to each of the threads
        #     futures = [ executor.submit(rename_bad_keys_in_dict_list, chunk) for chunk in chunks]

        #     for future in concurrent.futures.as_completed(futures):
        #         fields_renamed = future.result()
        #         additions_cleaned_fieldnames.extend(fields_renamed)

        # # Working on deletions file
        # logging.info(f"Removing bad characters from keys from deletions file data.")

        # # Give each thread a chunk of the work to do.
        # chunk_size = len(deletions_with_dels_checked) // self.max_processes
        # chunks = [deletions_with_dels_checked[i : i + chunk_size] for i in range(0, len(deletions_with_dels_checked), chunk_size)]

        # deletions_cleaned_fieldnames = []
        # with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_processes) as executor:
        #     # send off chunks to each of the threads
        #     futures = [ executor.submit(rename_bad_keys_in_dict_list, chunk) for chunk in chunks]

        #     for future in concurrent.futures.as_completed(futures):
        #         fields_renamed = future.result()
        #         deletions_cleaned_fieldnames.extend(fields_renamed)

        ################################################################################################

        # Write out the processed additions and deletions to file.
        with open(pubmed_article_additions_file_path, "wb") as f_add_out, open(
            pubmed_article_deletions_file_path, "wb"
        ) as f_del_out:
            logging.info(f"Writing article additions to file - {pubmed_article_additions_file_path}")
            for line in pubmed_article_additions_with_dels_checked:
                f_add_out.write(str.encode(json.dumps(line, cls=CustomEncoder) + "\n"))
                # Custom encoder used here so that the abstracts (AbstractText) are written as strings, not dictionary objects

            logging.info(f"Writing article deletions to file - {pubmed_article_deletions_file_path}")
            for line in pubmed_article_deletions_with_dels_checked:
                f_del_out.write(str.encode(json.dumps(line, cls=CustomEncoder) + "\n"))

        with open(pubmed_book_article_additions_file_path, "wb") as f_add_out:
            logging.info(f"Writing book additions to file - {pubmed_book_article_additions_file_path}")
            for line in pubmed_book_article_additions_with_dels_checked:
                f_add_out.write(str.encode(json.dumps(line, cls=CustomEncoder) + "\n"))
                # Custom encoder used here so that the abstracts (AbstractText) are written as strings, not dictionary objects

        with open(book_document_additions_file_path, "wb") as f_add_out, open(
            book_document_deletions_file_path, "wb"
        ) as f_del_out:
            logging.info(f"Writing book additions to file - {book_document_additions_file_path}")
            for line in book_document_additions_with_dels_checked:
                f_add_out.write(str.encode(json.dumps(line, cls=CustomEncoder) + "\n"))
                # Custom encoder used here so that the abstracts (AbstractText) are written as strings, not dictionary objects

            logging.info(f"Writing book article deletions to file - {book_document_deletions_file_path}")
            for line in book_document_deletions_with_dels_checked:
                f_del_out.write(str.encode(json.dumps(line, cls=CustomEncoder) + "\n"))

        # Push list of transform files into the xcom

        # TODO: Push these values into a list of strings for the file paths so the upload_transform function can just be a for loop.

        transformed_files_to_updload_to_gcs = [
            pubmed_article_additions_file_path,
            pubmed_article_deletions_file_path,
            pubmed_book_article_additions_file_path,
            book_document_additions_file_path,
            book_document_deletions_file_path,
        ]

        ti.xcom_push(key="transformed_files_to_updload_to_gcs", value=transformed_files_to_updload_to_gcs)

        # ti.xcom_push(key="transform_pubmed_article_additions_for_release", value=pubmed_article_additions_file_path)
        # ti.xcom_push(key="transform_pubmed_article_deletions_for_release", value=pubmed_article_deletions_file_path)

        # ti.xcom_push(key="transform_pubmed_book_article_additions_for_release", value=pubmed_book_article_additions_file_path)

        # ti.xcom_push(key="transform_book_document_additions_for_release", value=book_document_additions_file_path)
        # ti.xcom_push(key="transform_book_document_deletions_for_release", value=book_document_deletions_file_path)

    def upload_transformed(self, release: PubMedRelease, **kwargs):
        """Upload the transformed and combined release files to GCS."""

        # Pull list of transform files from xcom
        ti: TaskInstance = kwargs["ti"]
        transformed_files_to_updload_to_gcs = ti.xcom_pull(key="transformed_files_to_updload_to_gcs")

        uploaded_transform_files = []
        for file_to_upload in transformed_files_to_updload_to_gcs:
            file = file_to_upload.split("/")[-1]

            try:
                # TODO: how to get proper path of the GCS blobs?
                # TODO: Add a retry loop ??
                blob_name = f"telescopes/{self.dag_id}/{self.release_id}/{file}"
                logging.info(f"Uploading file {file} to GCS bucket {self.transform_bucket} and {blob_name}")
                success = upload_file_to_cloud_storage(
                    bucket_name=self.transform_bucket,
                    blob_name=blob_name,
                    file_path=file_to_upload,
                    check_blob_hash=False,
                )

                uploaded_transform_files.append(file_to_upload)

                # get gcs uri from the upload of the blob??
            except:
                raise AirflowException(f"Unable to upload file: {file} to GCS bucket {self.download_bucket}")

        # save name of additons file that was uploaded into the task instance for
        # Push list of uploaded transform files into the xcom, which will be imported from GCS to Bigquery.
        ti.xcom_push(key="uploaded_transform_files", value=uploaded_transform_files)

    def bq_create_snapshot(self, release: PubMedRelease, **kwargs):
        """Make a new snapshot of the PubMed table using the previous release and applying the current release additions and deletions."""

        # Pull files to transfer from GCS to BQ
        ti: TaskInstance = kwargs["ti]"]
        uploaded_transform_files = ti.xcom_pull(key="uploaded_transform_files")

        ### MAJOR
        ### TODO: Make BQ json schema based on the pubmed_230101.xsd schema that includes all possible fields.
        ###

        if self.download_baseline or not bigquery_table_exists(self.project_id, self.dataset_id, self.table_id):
            logging.info("Create new Pubmed BQ table.")
            create_bigquery_dataset(self.project_id, self.dataset_id, self.table_id)
            create_empty_bigquery_table(
                self.project_id, self.dataset_id, self.table_id, os.path.join(self.schema_folder, self.schema_file)
            )

            # Apply additions with gcloud command from the additions file on GCS as this is the initial table

        with open(self.deletions_file, "r", encoding="utf8") as f_del_out:
            logging.info("Openning del file.")
            # read both add and del file
            deletions_for_release = []

        # Make a new snapshot from the last version of the talbe. This essentially copes the last table and lets us modify the current 'release' one.
        create_bigquery_snapshot(self.project_id, self.dataset_id, self.table_id, self.dataset_id, self.table_id)

        # create list of PMIDs to delete from the just new snapshot table.
        publications_to_delete = [PMID_to_delete["PMID"]["text"] for PMID_to_delete in deletions_for_release]

        # apply deletions with query
        # WHERE column_name IN ('value1', 'value2', 'value3');;""" TODO: figure out the formatting.

        bq_deletion_query = (
            f""" DELETE FROM `my_dataset.my_table` \nWHERE column_name IN ({publications_to_delete});;"""
        )

        run_bigquery_query(bq_deletion_query)  # TODO: figure out what the result is.

        ## Apply updates to last table using gcloud command from gcs

        # Authenticate gcloud with service account
        args = ["gcloud", "transfer from GCS TO BQ - import job from additons file"]
        proc: Popen = subprocess.Popen(
            args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=dict(os.environ, CLOUDSDK_PYTHON="python3")
        )
        run_subprocess_cmd(proc, args)

        ############## OLD ####################

        ## check that the last snapshot table was actually the last release, using sequence of file numbers.

        # dels need to be done with a query to find the PMID - unique ID that are from the deletion file.

        # if is_first_release(self.workflow_id):  ## Use if first dag run later instead of this.
        #     self.table_id = "pubmed"  # just one large table as it is a list of publications only.

        #     # can pull out authors and other data from the main table for later DOI inclusion.

        #     full_table_id = f"{self.project_id}.{self.dataset_id}.{self.table_id}"

        #     logging.info(f"Creating table {full_table_id}")

        #     # if this is the first release, make a new initial table.

        # else:
        #     # TODO: All this
        #     previous_release_date = ""
        #     previous_release_snapshot_table_id = ""

        #     logging.info(f"Creating PubMed snapshot based on previous release {previous_release_date}")
        # make description of this snapshot - "Based off of the previous release - {executuon_date_of_telescope}"
        ## In each snapshot, include the sequence number of the from the files that were used to make the table (add/del)
        # bq_create_snapshot - from stream telescope

        # Use exisitnig table to make a new snapshot of the database with the additions and deletions.

        # loop through all exisiting snapshots on BQ to make sure one isnt missing??

        # if this is the next squential release to the last, then append additions and deletion to the last table and make a new snapshot.

        # adds can be done in bulk with an append to the new pubmed snapshot copy.

        # apply deletions with the custom query. # figure out how to do this without large bigquery costs.
        # Find rows with the matching PMIDs and remove them.
        # Construct the BQ query
        # QUERY TEMPLATE - FROM (the snapshot table) SELECT (column of PMIDs)
        # DELETE the entire row with that PMID (list_of_deletions_with_PMID)

        # Apply BQ query on the table
        #
        # Make the new snapshot of the table after doing adds and dels.

    def cleanup(self, release: PubMedRelease, **kwargs):
        """Cleanup files and task instances from this release."""

        ## os. remove file and entire tree of this release.

        ## remove task instances and xcoms used


# Move this to a utils file later
def run_subprocess_cmd(proc: Popen, args: list):
    """Execute and wait for subprocess to finish, also handle stdout & stderr from process.

    :param proc: subprocess proc
    :param args: args list that was passed on to subprocess
    :return: None.
    """
    logging.info(f"Executing bash command: {subprocess.list2cmdline(args)}")
    out, err = wait_for_process(proc)
    if out:
        logging.info(out)
    if err:
        logging.info(err)
    if proc.returncode != 0:
        # Don't raise exception if the only error is because blobs could not be found in bucket
        err_lines = err.split("\n")
        if err_lines:
            raise AirflowException("bash command failed")
    logging.info("Finished cmd successfully")


def convert(k: str) -> str:
    """Convert a key name.
    BigQuery specification for field names: Fields must contain only letters, numbers, and underscores, start with a
    letter or underscore, and be at most 128 characters long.
    :param k: Key.
    :return: Converted key.
    """
    # Trim special characters at start:
    k = re.sub("^[^A-Za-z0-9]+", "", k)
    # Replace other special characters (except '_') in remaining string:
    k = re.sub(r"\W+", "_", k)

    # Incase the key are only special characters. Need to be something for BQ to injest.
    if len(k) == 0:
        k = "value"

    return k


def change_keys(obj, convert):
    """Recursively goes through the dictionary obj and replaces keys with the convert function.
    :param obj: Dictionary object.
    :param convert: Convert function.
    :return: Updated dictionary object.
    """
    if isinstance(obj, (str, int, float)):
        return obj
    if isinstance(obj, dict):
        new = obj.__class__()
        for k, v in list(obj.items()):
            new[convert(k)] = change_keys(v, convert)
    elif isinstance(obj, (list, set, tuple)):
        new = obj.__class__(change_keys(v, convert) for v in obj)
    else:
        return obj
    return new


# TODO: Clean up this function if possible.
def add_attributes_to_data_from_biopython_classes(obj):
    """Recursively travel down the data tree and add attributes from the biopython data classes as dictionary keys.

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


class CustomEncoder(json.JSONEncoder):

    """Custom encoder for JSON dump for it to write a dictionary field as a string of text for a
    number of select key values in the Pubmed data"""

    def _transform_obj_data(self, obj):
        if isinstance(obj, str):
            return obj
        elif isinstance(obj, dict):
            new = {}
            # Loop through field names for the match fields to change to text.
            for k, v in list(obj.items()):
                # TODO: Change this to an input of a List or Set for multiple fields if needed - hard coding bad.
                if k == "AbstractText":  # or k == "GeneralNote":
                    new[k] = str(v)
                else:
                    new[k] = self._transform_obj_data(v)

                # # To remove a list of a list e.g. "KeywordList": [["a","b"]] to "KeywordList": ["a","b"]
                # if k == "KeywordList" and len(v) > 0:
                #     new[k] = v[0]
                # else:
                #     new[k] = self._transform_obj_data(v)

            return new
        elif isinstance(obj, list):
            # # If the data is something like "key": [] needs to be "key": [""] for BQ
            # if len(obj) == 0:
            #     return [""]
            # else:
            #     return [self._transform_obj_to_str(elem) for elem in obj]

            return [self._transform_obj_data(elem) for elem in obj]
        else:
            return obj

    def encode(self, obj):
        transformed_obj = self._transform_obj_data(obj)
        return super(CustomEncoder, self).encode(transformed_obj)


# Not used because FTP server can't download multiple files using multiple threads.
# def download_file_from_ftp(files_to_download: List[str], ftp_server_url: str, check_md5_hash: bool) -> List[str]:
#     """Function for allowing parallisation of downloading the PubMed files"""

#     # Open FTP connection
#     ftp_conn = ftplib.FTP(ftp_server_url, timeout=1000000.0)
#     ftp_conn.login()  # anonymous login (publicly available data)

#     downloaded_files = []
#     for file_on_ftp in files_to_download:
#         file = file_on_ftp.split("/")[-1]

#         logging.info(f"Downloading: {file}")

#         try:
#             # Download file
#             with open(file, "wb") as f:
#                 ftp_conn.retrbinary(f"RETR {file_on_ftp}", f.write)
#         except:
#             raise AirflowException(f"Unable to download {file_on_ftp} from PubMed's FTP server {ftp_server_url}")

#         if check_md5_hash:
#             # Create the hash from the above downloaded file.
#             with open(file, "rb") as f_in:
#                 data = f_in.read()
#                 md5hash_from_download = hashlib.md5(data).hexdigest()

#             # Download corresponding md5 hash.
#             with open(f"{file}.md5", "wb") as f:
#                 ftp_conn.retrbinary(f"RETR {file_on_ftp}.md5", f.write)

#             # Peep into md5 file.
#             with open(f"{file}.md5", "r") as f_md5:
#                 md5_from_pubmed_ftp = f_md5.read()

#             # If md5 does not match, raise an Airflow exception.
#             if md5hash_from_download in md5_from_pubmed_ftp:
#                 downloaded_files.append(file)
#             else:
#                 raise AirflowException(f"MD5 hash does not match the given MD5 checksum from server: {file}")
#         else:
#             downloaded_files.append(file)

#     # Close the FTP connection to prevent errors.
#     ftp_conn.close()

#     return downloaded_files

# # Download in parallel
# downloaded_files_for_release = []
# # Give each thread a chunck of the download list
# chunk_size = len(files_to_download) // self.max_processes
# chunks = [files_to_download[i : i + chunk_size] for i in range(0, len(files_to_download), chunk_size)]

# with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
#     # send off each chunk of the download list to each of the threads
#     futures = [
#         executor.submit(download_file_from_ftp, chunk, self.ftp_server_url, self.check_md5_hash)
#         for chunk in chunks
#     ]

#     # Wait for all futures to complete
#     concurrent.futures.wait(futures)

#     # Put the successfully downloaded list of files together after everything is complete.
#     for future in concurrent.futures.as_completed(futures):
#         chunk_downloaded_files = future.result()
#         downloaded_files_for_release.extend(chunk_downloaded_files)

#   for file_on_ftp in files_to_download:
#         futures.append(
#             executor.submit(
#                 download_file_from_ftp, ftp_conn, file_on_ftp, self.ftp_server_url, self.check_md5_hash
#             )
#         )
#     for future in concurrent.futures.as_completed(futures):
#         file = future.result()
#         downloaded_files_for_release.append(file)

# Download files in parallel - from dumb chatGPT
# logging.info(f"Downloading files in parallel with {self.max_processes}")
# downloaded_files_for_release = []
# with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_processes) as executor:
#     futures = []
#     for file_on_ftp in files_to_download:
#         futures.append(
#             executor.submit(
#                 download_file_from_ftp, ftp_conn, file_on_ftp, self.ftp_server_url, self.check_md5_hash
#             )
#         )
#     for future in concurrent.futures.as_completed(futures):
#         file = future.result()
#         downloaded_files_for_release.append(file)


### OLD BAD - DOESN'T WORK ###

# def rename_bad_keys_in_dict_list(obj_list_input: List[Dict]) -> List[Dict]:
#     """Remove bad characters from a keys in a dictionary for Bigquery import."""

#     obj_list_output = []
#     for obj in obj_list_input:
#         obj_list_output.append(change_keys(obj, convert))

#     return obj_list_output

# def update_dict_with_missing_keys(input_dict, schema_dict):
#     if isinstance(schema_dict, dict):
#         if not input_dict:
#             input_dict = schema_dict
#         else:
#             for key in schema_dict.keys():
#                 # print(f"key: {key} in schema_dict")

#                 if key not in input_dict:
#                     # Push schema branch onto data branch so that format is the same
#                     input_dict[key] = schema_dict[key]
#             else:
#                 update_dict_with_missing_keys(input_dict[key], schema_dict[key])

#     elif isinstance(schema_dict, list):
#         if len(input_dict):
#             print("Help: ", schema_dict, input_dict)
#             for i in range(len(input_dict)):
#                 update_dict_with_missing_keys(input_dict[i], schema_dict[0])
#         else:
#             input_dict = schema_dict
#             # logging.info(f"Empty list", schema_dict)

# def fix_xml_structure(path, key, value):
#     #print(f"In postprocessing function with key: {key}")

#         #if isinstance(value, str) or isinstance(value, OrderedDict):.
#     if isinstance(value, str):
#         value = [value]

#     # All possible lists in pubmed
#     list_to_change = ['QualifierName', 'DescriptorName', "Grant", "PublicationType", "Author", "PubMedPubDate", "ArticleIdList", "Reference"]
#     if isinstance(value, dict) and key in list_to_change:
#         value = [value]

#     return key, value

# The optional argument `postprocessor` is a function that takes `path`,
# `key` and `value` as positional arguments and returns a new `(key, value)`
# pair where both `key` and `value` may have changed. Usage example::

#     >>> def postprocessor(path, key, value):
#     ...     try:
#     ...         return key + ':int', int(value)
#     ...     except (ValueError, TypeError):
#     ...         return key, value
