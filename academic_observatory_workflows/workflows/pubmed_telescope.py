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
from typing import List, Tuple
import pendulum
import datetime
import logging

# To download the files from the FTP server
import ftplib

# For checking files have been downloaded correctly
import hashlib
from psutil import Popen

# Unpacking and parse XML to json files.
import xmltodict
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
    download_blobs_from_cloud_storage,
    select_table_shard_dates,
    upload_file_to_cloud_storage,
    download_blob_from_cloud_storage,
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
        start_date: str = pendulum.datetime(year=2022, month=12, day=8),
        # data_interval_start: pendulum.DateTime = pendulum.datetime(2022, 12, 8),
        schedule_interval: str = "0 0 * * 0",  # weekly
        catchup: bool = True,
        dataset_id: str = None,
        merge_partition_field: str = None,
        schema_folder: str = None,
        dataset_type_id: str = None,
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
        self.schema_folder = schema_folder

        # Workflow parameters
        self.workflow_id = workflow_id
        self.schedule_interval = schedule_interval
        self.schema_file_path = os.path.join(self.schema_folder)

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
        self.add_task(self.upload_downloaded)  # upload raw files from pubmed servers for specific releases
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
        for i in range(0, 3, 1):  # -  for testing
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
                        download_attemp_count += 1
                        raise logging.info(f"MD5 hash does not match the given MD5 checksum from server: {file}")

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
                logging.info(f"Uplaoding file {file} to GCS bucket {self.download_bucket} and {blob_name}")
                success = upload_file_to_cloud_storage(
                    bucket_name=self.download_bucket,
                    blob_name=blob_name,
                    file_path=file_to_upload,
                    check_blob_hash=False,
                )

                # get gcs uri from the upload of the blob
            except:
                raise AirflowException(f"Unable to upload file: {file} to GCS bucket {self.download_bucket}")

        # Push to xcom for keeping track of the release files.
        ti.xcom_push(key="uploaded_download_files", value=uploaded_download_files)

    def transform(self, release: PubMedRelease, **kwargs):
        """Transform the *.xml.gz files from the FTP server into usable jsonl like files.

        This step compiles all the additions and deletions from the update files into separate tables.
        Loops through and checks if there are any deletions to apply before uploading them to Bigquery, to save cost.

        Matches on both the version of the publication and the PMID number.

        Outputs additions.jsonl.gz and deletions.jsonl.gz files for the release.
        """

        # Grab list of files downloaded.
        ti: TaskInstance = kwargs["ti"]
        downloaded_files_for_release = ti.xcom_pull(key="downloaded_files_for_release")

        # Make paths for the additions and deletions files.
        additions_file_path = os.path.join(self.transform_folder, f"additions_{self.release_id}.jsonl.gz")
        deletions_file_path = os.path.join(self.transform_folder, f"deletions_{self.release_id}.jsonl.gz")

        additions_for_release = []
        deletions_for_release = []
        with gzip.open(additions_file_path, "w") as f_add_out, gzip.open(deletions_file_path, "w") as f_del_out:
            # TODO: Consider parallelising this section.

            # Loop through each of the PubMed database files and gather additions and deletions.
            # for file in downloaded_files_for_release:
            for i in range(0, 3, 1):  # -  for testing
                file = downloaded_files_for_release[i]

                logging.info(f"Running through file - {file}")

                with gzip.open(file, "rb") as f_in:
                    data_dict = xmltodict.parse(f_in.read())

                    try:
                        logging.info(f"Extracting additions to the PubMed dataset - {file}")
                        additions_for_release.extend(data_dict["PubmedArticleSet"]["PubmedArticle"])
                    except:
                        logging.info(f"No additions in this file - {file}")

                    try:
                        logging.info(f"Extracting deletions from the PubMed dataset - {file}")
                        deletions_for_release.extend(data_dict["PubmedArticleSet"]["DeleteCitation"]["PMID"])
                    except:
                        logging.info(f"No deletions in this file - {file}")

            logging.info(
                "Checking through the additions for this release to see if any deletions can be done before going onto BigQuery."
            )

            # TODO: Do a check step to make sure there are no doubles of the deletes?? Each deletion *should* be unique in the DB.

            # TODO: Consider parallelising this section - initial baseline additions will be ~ 35 million entries and needs to be quicker for checking deletions.
            additions_with_dels_checked = additions_for_release.copy()
            deletions_with_dels_checked = deletions_for_release.copy()
            for record_to_delete in deletions_for_release:
                for addition_to_check in additions_for_release:
                    if record_to_delete == addition_to_check["MedlineCitation"]["PMID"]:
                        deletions_with_dels_checked.remove(record_to_delete)
                        additions_with_dels_checked.remove(addition_to_check)

                        # logging.info(f"Removed the following record from additions list - {count} {record_to_delete}")

            # Do some sort of assert check to make sure all of the deletions are done properly and none missing

            logging.info(
                f"There are {len(deletions_with_dels_checked)} deletions left to do on BQ and {len(additions_with_dels_checked)} additions to add to the snapshot for this release."
            )

            # TODO: Rename the field of the data? Some are pretty bad, such as "#text" and some have other strange characters in them.

            # Write out the processed additions and deletions to file.
            logging.info(f"Writing additions to file - {additions_file_path}")
            for line in additions_with_dels_checked:
                f_add_out.write(str.encode(json.dumps(line) + "\n"))

            logging.info(f"Writing deletions to file - {deletions_file_path}")
            for line in deletions_with_dels_checked:
                f_del_out.write(str.encode(json.dumps(line) + "\n"))

        # Push list of transform files into the xcom
        ti.xcom_push(key="transform_additions_for_release", value=additions_file_path)
        ti.xcom_push(key="transform_deletions_for_release", value=deletions_file_path)

    def upload_transformed(self, release: PubMedRelease, **kwargs):
        """Upload the transformed and combined release files to GCS."""

        # Pull list of transform files from xcom
        ti: TaskInstance = kwargs["ti"]
        transformed_additions = ti.xcom_pull(key="transform_additions_for_release")
        transformed_deletions = ti.xcom_pull(key="transform_deletions_for_release")

        uploaded_transform_files = []
        for file_to_upload in [transformed_additions, transformed_deletions]:
            file = file_to_upload.split("/")[-1]

            try:
                # TODO: how to get proper path of the GCS blobs?
                blob_name = f"telescopes/{self.dag_id}/{self.release_id}/{file}"
                logging.info(f"Uplaoding file {file} to GCS bucket {self.transform_bucket} and {blob_name}")
                success = upload_file_to_cloud_storage(
                    bucket_name=self.transform_bucket,
                    blob_name=blob_name,
                    file_path=file_to_upload,
                    check_blob_hash=False,
                )

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

        #

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
        publications_to_delete = [PMID_to_delete["PMID"]["#text"] for PMID_to_delete in deletions_for_release]

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
