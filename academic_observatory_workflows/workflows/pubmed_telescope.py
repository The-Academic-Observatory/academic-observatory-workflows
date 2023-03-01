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
from typing import List
import pendulum
import logging

# To download the files from the FTP server
import ftplib

# For checking files have been downloaded correctly
import hashlib

# Unpacking and parse XML to json files.
import xmltodict
import gzip
import json
import tarfile

# Airflow modules
from airflow import AirflowException
from academic_observatory_workflows.dag_tag import Tag


from google.cloud.bigquery import SourceFormat
from observatory.platform.utils.airflow_utils import AirflowVars, make_workflow_folder
from observatory.platform.workflows.workflow import Workflow, Release

from observatory.platform.utils.release_utils import (
    get_dataset_releases,
    is_first_release,
    get_datasets,
    get_latest_dataset_release,
)

from observatory.platform.utils.workflow_utils import (
    SubFolder,
    bq_load_shard,
    make_release_date,
    table_ids_from_path,
    delete_old_xcoms,
)


class PubMedRelease(Release):
    def __init__(
        self,
        *,
        dag_id: str,
        release_date: pendulum.DateTime,
    ):
        """Construct a ThothRelease.

        :param dag_id: the DAG id.
        :param release_date: the release date.
        """
        self.release_date = release_date
        release_id = f'{dag_id}_{self.release_date.format("YYYY_MM_DD")}'
        super().__init__(dag_id, release_id)


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
        project_id: str,
        download_bucket: str,
        transform_bucket: str,
        data_location: str,
        workflow_id: int,
        start_date: pendulum.DateTime,
        schedule_interval: str,
        catchup: bool,
        dataset_id: str = "pubmed",
        ftp_server_url: str,
        check_md5_hash: bool,
        dataset_description: str,
        schema_folder: str = None,
        source_format: str,
        airflow_vars: List[str] = None,
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
                AirflowVars.DATA_LOCATION,
                AirflowVars.DOWNLOAD_BUCKET,
                AirflowVars.TRANSFORM_BUCKET,
            ]

        # Workflow parameters
        self.workflow_id = workflow_id
        self.schedule_interval = schedule_interval
        self.start_date = start_date

        # Cloud workspace settings
        self.project_id = project_id
        self.download_bucket = download_bucket
        self.transform_bucket = transform_bucket

        # Databse settings
        self.dataset_description = dataset_description
        self.dataset_id = dataset_id
        self.data_location = data_location
        self.source_format = source_format
        self.schema_folder = schema_folder

        # PubMed settings
        self.ftp_server_url = ftp_server_url  # FTP server URL
        self.baseline_path = "/pubmed/baseline/"
        self.updatefiles_path = "/pubmed/updatefiles/"
        self.files_to_download_for_release = []
        self.download_file_name_pattern = "*.xml.gz"
        self.transform_file_name_pattern = "*.json.gz"
        self.check_md5_hash = check_md5_hash

        # If this is the first ever run of the telescope, download the "baseline" database and build on this.
        self.download_baseline = is_first_release(workflow_id)

        # Initialise folders
        self.download_folder = None
        self.transform_folder = None

        super().__init__(
            dag_id=dag_id,
            start_date=start_date,
            schedule_interval=schedule_interval,
            catchup=catchup,
            airflow_vars=airflow_vars,
            workflow_id=workflow_id,
        )

        self.add_setup_task(self.check_dependencies)
        self.add_task(self.check_releases)  # check releases and get list of files to download
        # self.add_task(self.download_files) # download the xml files from the FTP server, shove into gzip
        # self.add_task(upload_downloaded) # upload to GCS
        # self.add_task(transform) # convert xml files into *.jsonl.gz, validate if neccesary using their API
        # self.add_task(upload_trasformed) # upload to GCS

        # TODO: All BQ steps, snapshot and other
        # self.add_task(self.bq_append_new)
        # self.add_task(self.bq_delete_old)
        # self.add_task(self.bq_create_snapshot)

        ## make baseline table from

        # Add to release API
        # self.add_task(self.add_new_dataset_releases)

    def make_release(self, **kwargs) -> PubMedRelease:
        """Creates a new Pubmed release instance

        :return: The Pubmed release instance
        """
        release_date = make_release_date(**kwargs)
        release = PubMedRelease(dag_id=self.dag_id, release_date=release_date)
        self.workflow_folder = make_workflow_folder(self.dag_id, release_date)
        self.download_folder = make_workflow_folder(self.dag_id, release_date, SubFolder.downloaded.value)
        self.transform_folder = make_workflow_folder(self.dag_id, release_date, SubFolder.transformed.value)
        return release

    def check_releases(self, release: PubMedRelease, **kwargs):
        """Check previous releases and get a list files required to download/make this release."""

        end_date = kwargs["data_interval_end"].subtract(days=1).end_of("day")

        self.files_to_download_for_release = check_pubmed_releases(
            self.workflow_id,
            self.ftp_server_url,
            self.start_date,
            end_date,
            self.baseline_path,
            self.updatefiles_path,
        )

        logging.info(f"Files to download for this release: {self.files_to_download_for_release}")

    # def download(self, **kwargs):
    #     """Task to download the PubMed data from the FTP server.

    #     :param release: an OpenAlexRelease instance.
    #     :param kwargs: The context passed from the PythonOperator.
    #     :return: None.
    #     """

    # return download_pubmed(self.files_to_download_for_release)

    # def upload_downloaded(self, release: PubMedRelease, **kwargs):
    #     """Task to upload the data for the PubMed release.

    #     :param release: an PubMedRelease instance.
    #     :param kwargs: The context passed from the PythonOperator.
    #     :return: None.
    #     """

    #     release.upload_downloaded()

    # def transform(self, release: PubMedRelease, **kwargs):
    #     """Task to transform the PubMedRelease data.

    #     :param release: an PubMedRelease instance.
    #     :param kwargs: The context passed from the PythonOperator.
    #     :return: None.
    #     """
    #     release.transform()

    # def upload_transformed():
    #     """Upload the part Pubmed files"""

    # def bq_load(self, release: PubMedRelease, **kwargs):
    #     return release.load_part_files_into_bq()


def check_pubmed_releases(
    workflow_id: int,
    ftp_server_url: str,
    start_date: pendulum.datetime,
    end_date: pendulum.datetime,
    baseline_path: str,
    updatefiles_path: str,
) -> List[str]:
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
    ftp_conn = ftplib.FTP(ftp_server_url, timeout=1000000.0)
    ftp_conn.login()  # anonymous login (publicly available data)

    if is_first_release(workflow_id):
        logging.info(f"This is the first release of the PubMed Telescope. ")

        # Change to the baseline directory.
        ftp_conn.cwd(baseline_path)

        # Find all the xml.gz files available from the server.
        baseline_list = ftp_conn.nlst()
        files_to_download = [file for file in baseline_list if (file.split(".")[-1] == "gz" in file)]
        files_to_download.sort()

        logging.info(f"List of files to download from the PubMed FTP server for 'baseline': {files_to_download}")

        # Change to updatefiles directory
        ftp_conn.cwd(updatefiles_path)

        # Find all the xml.gz files available from the server.
        updatefiles_list = ftp_conn.nlst()
        updatefiles_xml_gz = [file for file in updatefiles_list if (file.split(".")[-1] == "gz" in file)]
        updatefiles_xml_gz.sort()

        # Find the files in the date range for the release (given from previous airflow step)
        updatefiles_to_download = [
            file
            for file in updatefiles_xml_gz
            if pendulum.datetime(ftp_conn.sendcmd("MDTM {}".format(file))[4:]) in pendulum.period(start_date, end_date)
        ]

        logging.info(
            f"List of files to download from the PubMed FTP server for 'updatefiles': {updatefiles_to_download}"
        )

        # Merge the two list to for the initial release.
        files_to_download.extend(files_to_download)

    else:
        logging.info(f"This is not the first run of this telescope. For this release, {start_date} to {end_date}")

        # Change to updatefiles directory
        ftp_conn.cwd(updatefiles_path)

        # Find all the xml.gz files available from the server.
        updatefiles_list = ftp_conn.nlst()
        updatefiles_xml_gz = [file for file in updatefiles_list if (file.split(".")[-1] == "gz" in file)]
        updatefiles_xml_gz.sort()

        # Find the files in the date range for the release (given from previous airflow step)
        files_to_download = [
            file
            for file in updatefiles_xml_gz
            if pendulum.datetime(ftp_conn.sendcmd("MDTM {}".format(file))[4:]) in pendulum.period(start_date, end_date)
        ]

        # TODO: Check release api for last downloaded files for the release, need use of "extra" param in release.

    # Close the connection to the FTP server.
    ftp_conn.close()

    return files_to_download


#### Not used, in dev
# def download_pubmed(
#     check_md5_hash: bool,
#     list_of_files_to_download: List[str],
#     database_url: str,
#     start_date: pendulum.datetime,
#     release_date_interval: pendulum.period,
# ):
#     """Download and compress the PubMed database files from the FTP server.

#     Downloaded files:
#     pubmed_telescope/{year_of_release}/{pubmed_files}.xml.gz
#     pubmed_telescope/{year_of_release}/updatedfiles-{release.start_date}-{release.end_date}/{update_files}.xml.gz

#     GZip'ed:
#     pubmed_telescope/{year_of_release}/baseline.gz
#     pubmed_telescope/{year_of_release}/updatedfiles-{release.start_date}-{release.end_date}/{end_date}.gz
#     """

#     download_dst_path_year = os.path.join(self.download_folder, f"{self.start_date.year}")
#     download_dst_path_baseline = os.path.join(download_dst_path_year, "baseline")
#     download_dst_path_release = os.path.join(
#         download_dst_path_year,
#     )

#     # Open FTP connection
#     ftp_conn = ftplib.FTP(database_url, timeout=1000000.0)
#     ftp_conn.login()  # anonymous login (publicly available data)

#     download_baseline = True
#     if download_baseline:
#         # If required to download all of the year baseline files again.

#         # Change to correct directory.
#         ftp_conn.cwd(f"{database_path}/baseline/")

#         # Find all the xml.gz files available from the server.
#         file_list = ftp_conn.nlst()
#         file_list_xml_gz = [file for file in file_list if (file.split(".")[-1] == "gz" in file)].sort()

#         logging.info(f"Downloading PubMed baseline files from FTP server.\nFiles to download: {file_list_xml_gz}")

#         downloaded_baseline = []
#         for file in file_list_xml_gz:
#             # Download the file
#             with open(file, "wb") as f:
#                 ftp_conn.retrbinary(f"RETR {file}", f.write)

#             # Download the hash the above downloaded file.
#             with open(file, "rb") as f_in:
#                 data = f_in.read()
#                 md5hash_from_download = hashlib.md5(data).hexdigest()

#             # Download corresponding md5 hash.
#             with open(f"{file}.md5", "wb") as f:
#                 ftp_conn.retrbinary(f"RETR {file}.md5", f.write)

#             # Peep md5 file.
#             with open(f"{file}.md5", "r") as f_md5:
#                 md5_from_pubmed_ftp = f_md5.read()

#             # If md5 does not match, raise an Airflow exception.
#             if md5hash_from_download in md5_from_pubmed_ftp:
#                 downloaded_updatefiles.append(file)
#             else:
#                 raise AirflowException(f"MD5 hash does not match the given MD5 checksum from server: {file}")

#         # with open( os.join.path(download_dst_path, 'baseline.gz')):

#     download_updatedfiles = False
#     if download_updatedfiles:
#         updatefile_dst_path = os.join.path(download_dst_path_release, "updatefiles")

#         # Change to correct directory.
#         ftp_conn.cwd(updatefiles_path)

#         # Find all the xml.gz files available on the FTP server.
#         file_list = ftp_conn.nlst()
#         file_list_xml_gz = [file for file in file_list if (file.split(".")[-1] == "gz" in file)].sort()

#         # Find the files in the date range for the release (given from previous airflow step)
#         files_to_download = [
#             file for file in file_list_xml_gz if ftp_conn.sendcmd("MDTM {}".format(file))[4:] in release_date_interval
#         ]

#         downloaded_updatefiles = []
#         for file in files_to_download:
#             # Download the file
#             with open(file, "wb") as f:
#                 ftp_conn.retrbinary(f"RETR {file}", f.write)

#             # Download the hash the above downloaded file.
#             with open(file, "rb") as f_in:
#                 data = f_in.read()
#                 md5hash_from_download = hashlib.md5(data).hexdigest()

#             # Download corresponding md5 hash.
#             with open(f"{file}.md5", "wb") as f:
#                 ftp_conn.retrbinary(f"RETR {file}.md5", f.write)

#             if check_md5_hash:
#                 # Peep md5 file.
#                 with open(f"{file}.md5", "r") as f_md5:
#                     md5_from_pubmed_ftp = f_md5.read()

#                 # If md5 does not match, raise an Airflow exception.
#                 if md5hash_from_download in md5_from_pubmed_ftp:
#                     downloaded_updatefiles.append(file)
#                 else:
#                     raise AirflowException(f"MD5 hash does not match the given MD5 checksum from server: {file}")

#         # with open( os.join.path(updatefile_dst_path, '{release_date}.gz') ):
#         #     for file in downloaded_updatefiles:
#         #         with open( os.join.path())

#     # Close the FTP connection to prevent errors.
#     ftp_conn.close()

# if downloaded_baseline:
#     # Gzip the baseline XML files into a GZIP for the upload task
# if downloaded_updatefiles:
# Gzip the updatefiles for the release period and name them the period

## Add list of updated files to the context of the telescope for next steps?


#### OLD - to review
# class PubMedRelease(Release):
#     def __init__(
#         self,
#         dag_id: str,
#         workflow_id: int,
#         dataset_type_id: str,
#         start_date: pendulum.DateTime,
#         end_date: pendulum.DateTime,
#         first_release: bool,
#     ):
#         """Construct a PubMedRelease

#         :param dag_id: the id of the DAG.
#         :param workflow_id: Observatory API workflow id.
#         :param start_date: the start_date of the release.
#         :param end_date: the end_date of the release.

#         :param first_release: whether this is the first release that is processed for this DAG
#         :param extra: Extra information added to the release api
#         """
#         super().__init__(
#             dag_id, start_date, end_date, first_release, download_files_regex=".*.gz", transform_files_regex=".*.gz"
#         )

#     def upload_downloaded(
#         upload_baseline: bool,
#         upload_updatefiles: bool,
#     ):
#         """Upload downloaded PubMed database files to GCS in a gzip.

#         The baseline files will be separate from the update files.

#         Baseline files are only updated each year
#         """

#         gcs_uri = ""

#         if upload_baseline:
#             logging.info(f"Uploading the Gzipped baseline files to GCS: {gcs_uri}")

#             # gcp_upload_file function

#         if upload_updatefiles:
#             release_period = pendulum.period(
#                 self.start_date,
#             )
#             logging.info(f"Uploading the Gzipped updatefiles for release period {release_period}: {gcs_uri}")

#             # upload the files to GCS just for this release.

#         # gcs_connector = something

#         # list of raw files to upload

#         ### Upload to download bucket

#     def transform(self, downloaded_files: List[str]):
#         """Transform the downloaded files for the release, baseline"""

#         # get list of files to tranfsorm from the download task

#         transformed_files = []
#         for file in downloaded_files:
#             with gzip.open(file, "rb") as f_in, open(f"{file}.json", "w") as f_out:
#                 data_dict = xmltodict.parse(f_in.read())
#                 json.dump(data_dict, f_out)
#             transformed_files.append(file)

#         ## Combine the release adds and deletions into one file or keep separate?

#     def upload_transformed(self, tranformed_files: List[str]):
#         """Upload the transformed files (unnested XMLs) from the transform task.


#         transform_bucket/pubmed_telescope/{year_of_release}/baseline.jsonl.gz
#         transform_bucket/pubmed_telescope/{year_of_release}/updatedfiles-{release.start_date}-{release.end_date}/{end_date}.jsonl.gz
#         """

#     # use gcs connector

#     def apply_udates_to_table():  ### If it already exists - to get check bool from the
#         """If the direct previous release exists, the new schedule interval data to create a new table"""

#         # TODO: Find a column that the matching could be done for a deletion.

#         # Where to do it - here in the telescope and make a copy of the table first, keep it in GCS then BQ
#         # OR
#         # only do it in BQ. copy the original table first and then apply the new updates.
#         # s
#         # delete row from tableid where 'column' = something
#         #

#         # do an injest first then additions from

#         # * to ask Jamie
