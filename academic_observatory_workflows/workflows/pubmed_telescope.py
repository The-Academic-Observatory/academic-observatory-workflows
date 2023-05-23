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

# To determine the size of an object in memory
from pympler import asizeof

# Airflow modules
from airflow import AirflowException
from airflow.models.taskinstance import TaskInstance
from airflow.models.variable import Variable

from google.cloud.bigquery import SourceFormat
from academic_observatory_workflows.config import schema_folder as default_schema_folder
from observatory.platform.utils.airflow_utils import AirflowVars, make_workflow_folder
from observatory.platform.workflows.workflow import Workflow, Release
from observatory.platform.utils.workflow_utils import get_chunks

from observatory.platform.utils.config_utils import find_schema

from google.cloud import bigquery

from observatory.platform.utils.gc_utils import (
    bigquery_table_exists,
    create_bigquery_snapshot,
    load_bigquery_table,
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
        transform_file_size: float = 4096,  # 4 Gb, in Megabytes
        dataset_description: str = "PubMed Medline Citaition Lists",
        dataset_id: str = None,
        data_location: str = Variable.get(AirflowVars.DATA_LOCATION),
        merge_partition_field: str = None,
        source_format: str,
        schema_folder: str = default_schema_folder(),
        dataset_type_id: str = None,
        table_id: str,
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
        self.table_id = table_id
        self.full_table_id = f"{self.project_id}.{self.dataset_id}.{self.table_id}"
        self.source_format = source_format
        self.schema_folder = default_schema_folder()

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

        # Rough size of what the output file size should be for the merged files.
        # 1 Gb = 1024 Megabytes = 1024^2 Kilobytes = 1024^3 Bytes = 1073741824 Bytes
        self.transform_file_size = transform_file_size  #  Megabytes

        # After how many downloads to reset the connection to Pubmed's FTP server.
        self.reset_ftp_counter = reset_ftp_counter

        # If this is the first ever run of the telescope, download the "baseline" database and build on this.
        ## Use if first dag run later instead of this.

        # TODO: Make condition for when the new baseline is out - after the release date for it each year.
        # if present day after the new baseline date then download baseline or
        # this is the very first release for the telescope
        # self.download_baseline = is_first_release(self.workflow_id)

        self.add_setup_task(self.check_dependencies)
        self.add_task(self.check_releases)
        self.add_task(self.download)
        self.add_task(self.upload_downloaded)
        self.add_task(self.transform)
        # self.add_task(self.merge_transform_files)
        self.add_task(self.upload_transformed)
        self.add_task(self.bq_ingest_release_tables)
        # self.add_task(self.bq_create_snapshot)
        # self.add_task(self.add_release)
        # self.add_task(self.cleanup)

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

        # TODO: Make this baseline check more robust.
        if self.data_interval_start < self.start_date + timedelta(days=7):
            self.download_baseline = True
        else:
            self.download_baseline = False

        return release

    def check_releases(self, release: PubMedRelease, **kwargs):
        """Use previous releases of the Pubmed telescope to determine which files are needed from the FTP server for the new release.

            and uses the data_interval_start and data_interval_end to determine what files are to be downloaded for this release.

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

        logging.info(
            f"List of files to download from the PubMed FTP server for 'updatefiles': {updatefiles_to_download}"
        )

        files_to_download.extend(updatefiles_to_download)

        # NEEDED ???????????????????????????????????????????????
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
                    f"FTP connection to Pubmed's servers has been reset after {self.reset_ftp_counter} to avoid issues."
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

        # Pull list of transform files from xcom
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
        entity_list = {
            "pubmed_article_additions": {
                "data_type": "additions",
                "sub_key": "PubmedArticle",
                "set_key": "PubmedArticleSet",
                "pmid_key_loc": "MedlineCitation",
                "output_file_base": f"pubmed_article_additions_{self.release_id}",
                "transform_files": [],
                "merged_transform_files": [],
                "uploaded_to_gcs": [],
                "schema_file_path": os.path.join(default_schema_folder(), "pubmed_articles_2023-01-01.json"),
                "table_id": "",
                "table_row_count": None,
                "table_description": "List citation records to be added to the main Pubmed snapshot.",
            },
            "pubmed_book_article_additions": {
                "data_type": "additions",
                "sub_key": "PubmedBookArticle",
                "set_key": "PubmedBookArticleSet",
                "pmid_key_loc": "MedlineCitation",
                "output_file_base": f"pubmed_book_article_additions_{self.release_id}",
                "transform_files": [],
                "merged_transform_files": [],
                "uploaded_to_gcs": [],
                "schema_file_path": os.path.join(default_schema_folder(), "pubmed_articles_2023-01-01.json"),
                "table_id": "",
                "table_row_count": None,
                "table_description": "List citation records to be added to the main Pubmed snapshot.",
            },
            "book_document_additions": {
                "data_type": "additions",
                "sub_key": "BookDocument",
                "set_key": "BookDocumentSet",
                "pmid_key_loc": "BookDocument",
                "output_file_base": f"book_document_additions_{self.release_id}",
                "transform_files": [],
                "merged_transform_files": [],
                "uploaded_to_gcs": [],
                "schema_file_path": os.path.join(default_schema_folder(), "pubmed_articles_2023-01-01.json"),
                "table_id": "",
                "table_row_count": None,
                "table_description": "List citation records to be added to the main Pubmed snapshot.",
            },
            "pubmed_article_deletions": {
                "data_type": "deletions",
                "sub_key": "PMID",
                "set_key": "DeleteCitation",
                "output_file_base": f"pubmed_article_deletions_{self.release_id}",
                "transform_files": [],
                "merged_transform_files": [],
                "uploaded_to_gcs": [],
                "schema_file_path": os.path.join(default_schema_folder(), "pubmed_deletions_2023-01-01.json"),
                "table_id": "",
                "table_row_count": None,
                "table_description": "List of PMIDs and version of the citation to delete from the main Pubmed snapshot.",
            },
            "book_document_deletions": {
                "data_type": "deletions",
                "sub_key": "PMID",
                "set_key": "DeleteDocument",
                "output_file_base": f"book_document_deletions_{self.release_id}",
                "transform_files": [],
                "merged_transform_files": [],
                "uploaded_to_gcs": [],
                "schema_file_path": os.path.join(default_schema_folder(), "pubmed_deletions_2023-01-01.json"),
                "table_id": "",
                "table_row_count": None,
                "table_description": "List of PMIDs and version of the citation to delete from the main Pubmed snapshot.",
            },
        }

        # Process files in batches so that ProcessPoolExecutor doesn't deplete the system of memory
        for i, chunk in enumerate(get_chunks(input_list=downloaded_files_for_release, chunk_size=2)):
            with ProcessPoolExecutor(max_workers=2) as executor:
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

    def merge_transform_files(self, release: PubMedRelease, **kwargs):
        """
        Merge the transformed Pubmed files into appropriately sized chunks for upload GCS and import to BQ.

        :return: None.
        """

        # TODO: Rewrite this section for size on disk vs number of entries in files and avg size per row
        # Amount of ram used is silly.

        # Pull entity list from Xcom
        ti: TaskInstance = kwargs["ti"]
        entity_list = ti.xcom_pull(key="entity_list")

        logging.info(f"Merging Pubmed transform files into {self.transform_file_size} Megabyte sized files.")

        for name, entity in entity_list.items():
            # Initialise list
            merged_files = []

            # Part number of the larger file chunks.
            part_num = 1

            entity["transform_files"].sort()

            logging.info(f"List of transformed files from {name} {entity['transform_files']}")

            data = []
            for file in entity["transform_files"]:
                logging.info(f"Reading file into memory - {file}")

                # Open file in memory
                with open(file, "r") as f_in:
                    new_data = [json.loads(line) for line in f_in]

                # Append it onto existing data object.
                data.extend(new_data)

                current_size = asizeof.asizeof(data) / (8.0 * 1024.0**2)

                logging.info(f"Current data size: {current_size} Mb")

                # If the size of the data is greater than this, write out to file and clear variable from memeory
                # OR if the file is the last one, write it out to file regardless.

                if current_size >= self.transform_file_size or file == entity["transform_files"][-1]:
                    # Make proper name for larger file of chunk.
                    merged_file = os.path.join(
                        self.transform_folder,
                        f"{name}_{self.release_id}_part_{part_num}.jsonl",
                    )

                    logging.info(f"Writing out to file: {merged_file}")

                    # Write out to file as compressed *.jsonl.gz
                    with open(merged_file, "wb") as f_out:
                        for line in data:
                            f_out.write(str.encode(json.dumps(line, cls=CustomEncoder) + "\n"))

                    # Clear object in memeory (necessary??)
                    data = []

                    # Save file name to be put into entity list.
                    merged_files.append(merged_file)

                    # Increase part number for next file
                    part_num += 1

            # Export list of merged files to transform list
            entity_list[name]["merged_files"] = merged_files

        # Push updated entity_list with merged files list into the xcom.
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

            # Update entity_list with the new list of GCS blobs.
            entity_list[name] = entity

        # Push updated entity_lsit into the xcom.
        ti.xcom_push(key="entity_list", value=entity_list)

    def bq_ingest_release_tables(self, release: PubMedRelease, **kwargs):
        """
        Ingest data into tables for this particular release.

        Use the list of files uploaded to Google Cloud Storage to import to Bigquery.

        :return: None.
        """

        # Pull files to transfer from GCS to BQ
        ti: TaskInstance = kwargs["ti"]
        entity_list = ti.xcom_pull(key="entity_list")

        # Upload release tables for this data interval for snapshot step later.
        for name, entity in entity_list.items():
            # Create table with shard date.
            entity["table_id"] = bigquery_sharded_table_id(name, self.data_interval_end)

            # TODO: Delete old tables with exact table_id just in case there was a bad previous run.

            logging.info(f"Creating and adding to table: {entity['table_id']} ")

            for tranform_blob in entity["uploaded_to_gcs"]:
                # Append each entity parts to it's own new table with a shard datetime.

                # TODO: Add table partition expiration date when uploading.
                success = load_bigquery_table(
                    tranform_blob,
                    self.dataset_id,
                    self.data_location,
                    entity["table_id"],
                    entity["schema_file_path"],
                    self.source_format,
                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                    project_id=self.project_id,
                    table_description=entity["table_description"],
                    partition=False,
                )
                if not success:
                    raise AirflowException(f"Unable to upload table {name}")

            # TODO: Get number of rows of table from BQ for an assert?

            if success:
                logging.info(f"Successfully added to table - {entity['table_id']} ")

                # Update the entity after a successful table upload.
                entity_list[name] = entity

        #  Update workflow metadata of tables.
        entity_list = ti.xcom_push(key="entity_list", value=entity_list)

    def bq_create_snapshot_from_tables(self, release: PubMedRelease, **kwargs):
        """
        Create snapshot from the release tables transfered in previous section.

        :param release: Pubmed release.
        :return: None.
        """

        # Pull workflow entity data
        ti: TaskInstance = kwargs["ti"]
        entity_list = ti.xcom_pull(key="entity_list")

        # Use the table from this workflow run to make the first snapshot.
        # This should be just the baseline dataset, so there should be no deletions to the snapshot.
        if self.download_baseline:
            # Copy pubmed_article_addtions to snapshot table.
            snapshot_table_id = bigquery_sharded_table_id("pubmed_article_snapshot", self.data_interval_end)

            bq_query_create_baseline_snapshot = f"""
            CREATE SNAPSHOT TABLE {self.project_id}.{self.dataset_id}.{snapshot_table_id}
            CLONE {self.project_id}.{self.dataset_id}.{entity_list['pubmed_article_additions']['table_id']}
            """

            # Run bq query
            bq_output = run_bigquery_query(bq_query_create_baseline_snapshot)

            logging.info(f"Output from create snapshot query: {bq_output}")

        else:
            # TODO: Add this to the init
            working_table = "pubmed_snapshot"

            # Get last snapshot table name.
            snapshot_table_old = bq_list_tables_in_dataset_with_prefix(self.project_id, self.dataset_id, working_table)[
                0
            ]

            logging.info(f"Copy to working table - {snapshot_table_old}")

            # Delete old working table due to possible old bad run.
            working_table_id = "pubmed_snapshot_working"
            bq_delete_table(self.project_id, self.dataset_id, working_table_id, not_found_okay=True)

            # Copy old snapshot to a normal table to work on it. (snapshots are read-only)
            copy_old_snapshot_to_regular = f"""
            CREATE TABLE {self.project_id}.{self.dataset_id}.{working_table_id}
            CLONE {self.project_id}.{self.dataset_id}.{snapshot_table_old}
            """
            bq_output = run_bigquery_query(copy_old_snapshot_to_regular)
            logging.info(f"Output from copy snapshot query: {bq_output}")

            # Remove the records that have updates from the incoming additions table
            delete_records_from_working_from_additions = f"""
            DELETE FROM `{self.project_id}.{self.dataset_id}.{working_table_id}`
            WHERE MedlineCitation.PMID.value IN (
            SELECT MedlineCitation.PMID.value
            FROM `{self.project_id}.{self.dataset_id}.{entity_list['pubmed_article_additions']['table_id']}`
            );
            """
            bq_output = run_bigquery_query(delete_records_from_working_from_additions)
            logging.info(f"Output from 'delete records to be updated from additions table' query: {bq_output}")

            # Insert the additions in to the working table.
            insert_records_into_working_from_additions = f"""
            INSERT INTO `{self.project_id}.{self.dataset_id}.{working_table_id}` 
            SELECT * 
            FROM `{self.project_id}.{self.dataset_id}.{entity_list['pubmed_article_additions']['table_id']}`
            """
            bq_output = run_bigquery_query(insert_records_into_working_from_additions)
            logging.info(f"Output from 'insert records ' query: {bq_output}")

            # Delete records using the deletions table, matching on the Version of the citation and the PMID.
            delete_records_from_working_from_deletions = f"""
            DELETE FROM `{self.project_id}.{self.dataset_id}.{working_table_id}`
            WHERE (MedlineCitation.PMID.value, MedlineCitation.PMID.Vserion) IN (
            SELECT (value, Version) 
            FROM `{self.project_id}.{self.dataset_id}.{entity_list['pubmed_article_deletions']['table_id']}`
            );
            """
            bq_output = run_bigquery_query(delete_records_from_working_from_deletions)
            logging.info(f"Output from 'delete records from working table from deletions' query: {bq_output}")

            # TODO: Update the working table description.

            # Create new snapshot table from working
            copy_working_into_new_snapshot = f"""
            CREATE SNAPSHOT TABLE {self.project_id}.{self.dataset_id}.{working_table_id}
            CLONE {self.project_id}.{self.dataset_id}.{snapshot_table_old}
            """
            bq_output = run_bigquery_query(copy_working_into_new_snapshot)
            logging.info(f"Output from 'copy working into new snapshot' query: {bq_output}")

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
        """Cleanup files and task instances from this release.

        :return: None.
        """

        # Loop through all of the download and transform files and confirm that data has been deleted from this workflow run.

        # Remove the old snapshot table.

        # Remove all task instance data from this workflow run.


def transform_pubmed_xml_file_to_jsonl(input_file: str, entity_list: Dict, transform_folder: str) -> Dict:
    """
    Convert a single Pubmed XML file to JSONL, pulling out any of the PubmedArticle,
    PubmedBookArticle, BookDocument, DeleteCitation, DeleteDocument.

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

        output_file = os.path.join(transform_folder, entity["output_file_base"] + "_" + pubmed_file_id + ".jsonl")

        logging.info(f"Writing {name} {data_type} to file - {output_file}")

        with open(output_file, "wb") as f_out:
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


def bq_update_table_expiration_date(
    project_id: str, dataset_id: str, table_id: str, expiration_date: pendulum.datetime
):
    # TODO: To add to gcs utils after the workflow restructure.

    """
    Update a BigQuery table expiration date.

    :project_id:
    :dataset_id:
    :table_id:
    :expiration_date: Date when the table with expire.

    :return: None.
    """

    bq_client = bigquery.Client(project=project_id)
    dataset_ref = bigquery.DatasetReference(project_id, dataset_id)

    table_ref = dataset_ref.table(table_id)
    table = bq_client.get_table(table_ref)

    table.expires = expiration_date

    try:
        table = bq_client.update_table(table, ["expires"])
    except:
        raise AirflowException(f"Unable to set expiration date of table {table_id} to {expiration_date}")


def bq_delete_table(
    project_id: str, dataset_id: str, table_id: str, partition_decorator: str = None, not_found_okay: bool = True
):
    """Delete a single table in Bigquery."""

    # TODO: To add to gcs utils after the workflow restructure.

    bq_client = bigquery.Client(project=project_id)
    dataset = bigquery.DatasetReference(project_id, dataset_id)
    table = bigquery.Table(dataset.table(table_id))

    try:
        bq_client.get_table(table)
        if partition_decorator:
            bq_client.delete_table(table, not_found_ok=False)
        else:
            bq_client.delete_table(table, tableId=partition_decorator, not_found_ok=not_found_okay)

        logging.info(f"Deleted exising bigquery table: {table_id} {partition_decorator}")
    except:
        raise AirflowException(f"An error occured deleting table {table_id}")


def bq_list_tables_in_dataset_with_prefix(project_id: str, dataset_id: str, prefix: str = "") -> List[str]:
    # TODO: Add prefix or suffix for this function.

    """List tables under a dataset in Bigquery.

    :param project_id:
    :param dataset_id:
    :param prefix:

    """

    # TODO: To add to gcs utils after the workflow restructure.

    bq_client = bigquery.Client(project=project_id)
    tables = bq_client.list_tables(dataset_id)

    return [
        f"{table.project}.{table.dataset_id}.{table.table_id}" for table in tables if table.table_id.startswith(prefix)
    ]
