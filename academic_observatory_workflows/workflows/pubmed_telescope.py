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
from airflow.models.taskinstance import TaskInstance

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
        self.add_task(self.download)  # download the xml files from the FTP server, shove into gzip

        # self.add_task(upload_downloaded) # upload to GCS
        # self.add_task(transform) # convert xml files into *.jsonl.gz, validate if neccesary using their API
        # self.add_task(upload_trasformed) # upload to GCS

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
        release = PubMedRelease(dag_id=self.dag_id, release_date=release_date)
        self.workflow_folder = make_workflow_folder(self.dag_id, release_date)
        self.download_folder = make_workflow_folder(self.dag_id, release_date, SubFolder.downloaded.value)
        self.transform_folder = make_workflow_folder(self.dag_id, release_date, SubFolder.transformed.value)
        return release

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

        end_date = kwargs["data_interval_end"].subtract(days=1).end_of("day")

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

        logging.info(f"Grabbing list of 'updatefiles' for this release: {self.start_date} to {end_date}")

        # Change to updatefiles directory
        ftp_conn.cwd(self.updatefiles_path)

        # Find all the xml.gz files available from the server.
        updatefiles_list = ftp_conn.nlst()
        updatefiles_xml_gz = [file for file in updatefiles_list if (file.split(".")[-1] == "gz" in file)]
        updatefiles_xml_gz.sort()

        updatefiles_to_download = [
            self.updatefiles_path + file
            for file in updatefiles_xml_gz
            if pendulum.from_format(ftp_conn.sendcmd("MDTM {}".format(file))[4:], "YYYYMMDDHHmmss")
            in pendulum.period(self.start_date, end_date)
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

        List contains their os path as well.


        ??? Need to do this ???
        Downloaded files:
        pubmed_telescope/{year_of_release}/{pubmed_files}.xml.gz
        pubmed_telescope/{year_of_release}/updatedfiles-{release.start_date}-{release.end_date}/{update_files}.xml.gz

        GZip'ed:
        pubmed_telescope/{year_of_release}/baseline.gz
        pubmed_telescope/{year_of_release}/updatedfiles-{release.start_date}-{release.end_date}/{end_date}.gz
        """

        # Grab list of files to download from xcom
        ti: TaskInstance = kwargs["ti"]
        files_to_download = ti.xcom_pull(key="files_to_download")

        logging.info(f"Files to download from PubMed: {files_to_download}")

        # Open FTP connection
        ftp_conn = ftplib.FTP(self.ftp_server_url, timeout=1000000.0)
        ftp_conn.login()  # anonymous login (publicly available data)

        downloaded_files_for_release = []
        for file_on_ftp in files_to_download:
            file = file_on_ftp.split("/")[-1]

            print(f"Downloading: {file}")

            try:
                # Download file
                with open(file, "wb") as f:
                    ftp_conn.retrbinary(f"RETR {file_on_ftp}", f.write)
            except:
                raise AirflowException(
                    f"Unable to download {file_on_ftp} from PubMed's FTP server {self.ftp_server_url}"
                )

            if self.check_md5_hash:
                # Create the hash from the above downloaded file.
                with open(file, "rb") as f_in:
                    data = f_in.read()
                    md5hash_from_download = hashlib.md5(data).hexdigest()

                # Download corresponding md5 hash.
                with open(f"{file}.md5", "wb") as f:
                    ftp_conn.retrbinary(f"RETR {file_on_ftp}.md5", f.write)

                # Peep into md5 file.
                with open(f"{file}.md5", "r") as f_md5:
                    md5_from_pubmed_ftp = f_md5.read()

                # If md5 does not match, raise an Airflow exception.
                if md5hash_from_download in md5_from_pubmed_ftp:
                    downloaded_files_for_release.append(file)
                else:
                    raise AirflowException(f"MD5 hash does not match the given MD5 checksum from server: {file}")
            else:
                downloaded_files_for_release.append(file)

        # Close the FTP connection to prevent errors.
        ftp_conn.close()

        # Push list of downloaded files into the xcom
        ti.xcom_push(key="downloaded_files_for_release", value=downloaded_files_for_release)

    def upload_downloaded(self, release: PubMedRelease, **kwargs):
        """Put all files into a tar ball and upload to GCS."""

        # Grab list of files downloaded from xcom.
        ti: TaskInstance = kwargs["ti"]
        downloaded_files_for_release = ti.xcom_pull(key="downloaded_files_for_release")

        # Make tar gz ball of downloaded files.

        # Upload to GCS.

    def transform(self, release: PubMedRelease, **kwargs):
        """Transform the *.xml.gz files from the FTP server into usable jsonl like files."""

        # Loop through all of the files and add them together, compiling into a larger table

        # Grab list of files downloaded.
        ti: TaskInstance = kwargs["ti"]
        downloaded_files_for_release = ti.xcom_pull(key="downloaded_files_for_release")

        files_to_squash = []
        for file in downloaded_files_for_release:
            # with gzip.open(file, "rb") as f_in, open(f"{file}.json", "w") as f_out:
            #     data_dict = xmltodict.parse(f_in.read())
            #     json.dump(data_dict, f_out)
            # transformed_files.append(file)

            with gzip.open(file, "rb") as f_in:
                data_dict = xmltodict.parse(f_in.read())
                files_to_squash.append(data_dict)

        ## Combine the release adds and deletions into one file or keep separate?

        # Dump output as a jsonl file

    def upload_transformed(self, release: PubMedRelease, **kwargs):
        """Upload the transformed and combined release files from PubMed to GCS."""

        # figure out pubmed telescope bucket / directory

        # Compress the combined file into a jsonl.gz file

        # use gcs connector to upload file

    def do_clever_snapshot_things(self, release: PubMedRelease, **kwargs):
        ## Apply updates to last table.

        ## use uploaded transform file into change the latest snapshot

        ## check that the last snapshot table was actually the last release.

        """Make new snapsohts of the tables, using the last."""

        # if this is the first release, make a new initial table.

        # if this is the next squential release to the last, then append additions and deletion to the last table and make a new snapshot.

        # figure out how to do this without large bigquery costs.

    def cleanup(self, release: PubMedRelease, **kwargs):
        """Cleanup files and task instances from this release."""

        ## os. remove file tree of this release.

        ## remove task instances.
