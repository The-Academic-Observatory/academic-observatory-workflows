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

# Common
import os
import gzip
import json
import math
import logging
import pendulum
from datetime import timedelta
from typing import Union, Dict, List

# Google Biguery
from google.cloud import bigquery
from google.cloud.bigquery import SourceFormat

# To download the files from the Pubmed FTP server
from ftplib import FTP, error_reply
import hashlib

# For reading in the XML files using Biopython library.
from Bio import Entrez
from Bio.Entrez.Parser import StringElement, ListElement, DictionaryElement, OrderedListElement, ValidationError

# Multithreading libraries
from concurrent.futures import ProcessPoolExecutor, as_completed

# Airflow
from airflow import AirflowException
from airflow.operators.dummy import DummyOperator
from airflow.models.taskinstance import TaskInstance
from observatory.platform.airflow import PreviousDagRunSensor, is_first_dag_run

# Observatory Platform
from observatory.platform.files import get_chunks
from observatory.api.client.model.dataset_release import DatasetRelease
from observatory.platform.api import get_latest_dataset_release, get_dataset_releases, make_observatory_api
from observatory.platform.gcs import gcs_upload_files, gcs_blob_name_from_path
from observatory.platform.observatory_config import CloudWorkspace
from observatory.platform.config import AirflowConns
from observatory.platform.workflows.workflow import (
    Workflow,
    ChangefileRelease,
    set_task_state,
    cleanup,
)
from observatory.platform.bigquery import (
    bq_table_exists,
    bq_sharded_table_id,
    bq_create_dataset,
    bq_load_table,
    bq_snapshot,
    bq_run_query,
)

# Academic observatory workflows
from academic_observatory_workflows.config import schema_folder as default_schema_folder, Tag


class Changefile:
    def __init__(
        self,
        filename: str,
        file_index: str,
        path_on_ftp: str,
        is_first_run: bool,
        changefile_date: pendulum.DateTime,
        changefile_release: ChangefileRelease = None,
    ):
        """Holds the metadata about a single Pubmed updatefile.

        :param filename: the name of the changefile.
        :param file_index: Index of the updatefile, effectively a count of the n-th updatefile.
        :param path_on_ftp: Path of the
        :param is_first_run: Identifies if this changefile is a baseline (true) or an updatefile (false).
        :param changefile_date: The date that the changefile was added to PubMed's FTP server.
        :param ChangefileRelease: Changefile release.
        """

        self.filename = filename
        self.file_index = file_index
        self.path_on_ftp = path_on_ftp
        self.is_first_run = is_first_run
        self.changefile_date = changefile_date
        self.changefile_release: ChangefileRelease = changefile_release

        self.file_type = "jsonl.gz"

    def __eq__(self, other):
        if isinstance(other, Changefile):
            return (
                self.filename == other.filename
                and self.file_index == other.file_index
                and self.path_on_ftp == other.path_on_ftp
                and self.is_first_run == other.is_first_run
                and self.changefile_date == other.changefile_date
                and self.changefile_release == other.changefile_release
            )
        return False

    @staticmethod
    def from_dict(dict_: Dict):  # Unable to do a -> Changefile . Airflow does not like it and says it is undefined.
        filename = dict_["filename"]
        file_index = dict_["file_index"]
        path_on_ftp = dict_["path_on_ftp"]
        is_first_run = dict_["is_first_run"]
        changefile_date = pendulum.parse(dict_["changefile_date"])
        changefile_release = dict_["changefile_release"]

        return Changefile(
            filename=filename,
            file_index=file_index,
            path_on_ftp=path_on_ftp,
            is_first_run=is_first_run,
            changefile_date=changefile_date,
            changefile_release=changefile_release,
        )

    def to_dict(self) -> Dict:
        return dict(
            filename=self.filename,
            file_index=self.file_index,
            path_on_ftp=self.path_on_ftp,
            is_first_run=self.is_first_run,
            changefile_date=self.changefile_date.isoformat(),
            changefile_release=self.changefile_release,
        )

    @property
    def download_file_path(self):
        assert self.changefile_release is not None, "Changefile.download_folder: self.changefile_release is None"
        return os.path.join(self.changefile_release.download_folder, self.filename)

    @property
    def transform_path(self):
        assert self.changefile_release is not None, "Changefile.transform_path: self.changefile_release is None"
        return self.changefile_release.transform_folder

    def transform_file_path(self, entity_type: str) -> str:
        """
        Give path to the transform file depending on entity type.

        :param entity_type: Type of the record, either "additions" or "deletions".
        :return: Retuns the path to the transformed file.
        """

        assert self.changefile_release is not None, "Changefile.transform_file_path: self.changefile_release is None"
        return os.path.join(
            self.changefile_release.transform_folder, f"{entity_type}_{self.filename[:-7]}.{self.file_type}"
        )

    def transform_file_pattern(self, entity_type: str) -> str:
        """Return a glob pattern for the transform files.

        :param entity_type: Type of the record, either "additions" or "deletions".
        :return: Retuns the path to the transformed file.
        """

        assert self.changefile_release is not None, "Changefile.transform_file_path: self.changefile_release is None"
        return os.path.join(self.changefile_release.transform_folder, f"{entity_type}_*.{self.file_type}")

    def merged_transform_file_path(
        self, entity_type: str, first_file_index: int, last_file_index: int, part_num: int = None
    ) -> str:
        """
        Give path to the merged transform file.

        :param entity_type: Type of the record, either "additions" or "deletions".
        :return: Retuns the path to the transformed file.
        """

        assert self.changefile_release is not None, "Changefile.transform_file_path: self.changefile_release is None"
        if part_num:
            return os.path.join(
                self.changefile_release.transform_folder,
                f"merged_{entity_type}_{first_file_index}-{last_file_index}_part_{part_num}.{self.file_type}",
            )
        else:
            return os.path.join(
                self.changefile_release.transform_folder,
                f"merged_{entity_type}_{first_file_index}-{last_file_index}.{self.file_type}",
            )

    def merged_tranform_file_pattern(self, entity_type: str) -> str:
        """
        Return a glob pattern for the merged transform files.

        :param entity_type: Type of the record, either "additions" or "deletions".
        :return: Retuns the path to the transformed file.
        """
        assert self.changefile_release is not None, "Changefile.transform_file_path: self.changefile_release is None"
        return os.path.join(
            self.changefile_release.transform_folder,
            f"merged_{entity_type}_*-*_part_*.{self.file_type}",
        )


class PubmedEntity:
    def __init__(
        self,
        name: str,
        type: str,
        sub_key: str,
        set_key: str,
        pmid_location: str,
        table_description: str,
    ):
        """
        Holds the metadata about the records stored in Pubmed.

        :param name: Name of the enitty, articles, book articles or book documents.
        :param type: Either a set of 'addition' or 'deletion' records.
        :param sub_key: Sub key location of Pubmed data.
        :param set_key: Key location of Pubmed.
        :param pmid_location: Location of PMID values
        :param table_description: Description for the table on Bigquery.
        """

        self.name = name
        self.type = type
        self.sub_key = sub_key
        self.set_key = set_key
        self.pmid_location = pmid_location
        self.table_description = table_description

        self.schema_file_path = os.path.join(default_schema_folder(), "pubmed", f"{name}_2023-01-01.json")


class PubMedRelease(ChangefileRelease):
    def __init__(
        self,
        *,
        dag_id: str,
        run_id: str,
        cloud_workspace: CloudWorkspace,
        start_date: pendulum.DateTime,
        end_date: pendulum.DateTime,
        is_first_run: bool,
        changefile_list: List[Changefile],
    ):
        """
        Construct a PubmedRelease.

        :param dag_id: the DAG id.
        :param run_id: Run id of this workflow.
        :param cloud_workspace: Holds cloud location details.
        :param start_date: Start date of the release.
        :param end_date: End date of the release.
        :param is_first_run: If it's the first run of the workflow, if this
        release is to download the baseline files of Pubmed.
        :param changefile_list: List of changefiles for this release.
        """

        super().__init__(
            dag_id=dag_id,
            run_id=run_id,
            start_date=start_date,
            end_date=end_date,
            sequence_start=changefile_list[0].file_index,
            sequence_end=changefile_list[-1].file_index,
        )
        self.cloud_workspace = cloud_workspace
        self.is_first_run = is_first_run
        self.changefile_list = changefile_list

        self.changefile_release = ChangefileRelease(
            dag_id=dag_id,
            run_id=run_id,
            start_date=start_date,
            end_date=end_date,
            sequence_start=changefile_list[0].file_index,
            sequence_end=changefile_list[-1].file_index,
        )

        self.changefile_list = changefile_list
        for changefile in changefile_list:
            changefile.changefile_release = self.changefile_release

    def merged_transfer_blob_pattern(self, entity_type: str, first_index: int = None, last_index: int = None) -> str:
        """
        Create a gcs blob pattern url for transfering the merged updatefiles into Bigquery.

        :param entity_type: Either "additions" or "deletions".
        :return: GCS blob uri for merged transform files.
        """

        if first_index:
            return f"gs://{self.cloud_workspace.transform_bucket}/{gcs_blob_name_from_path(self.changefile_release.transform_folder)}/merged_{entity_type}_{first_index}-{last_index}_*.{self.changefile_list[0].file_type}"

        else:
            return f"gs://{self.cloud_workspace.transform_bucket}/{gcs_blob_name_from_path(self.changefile_release.transform_folder)}/merged_{entity_type}_*.{self.changefile_list[0].file_type}"


class PubMedTelescope(Workflow):

    """
    PubMed Telescope

    Please visit:
    https://pubmed.ncbi.nlm.nih.gov/

    """

    def __init__(
        self,
        *,
        dag_id: str,
        cloud_workspace: CloudWorkspace,
        bq_dataset_id: str = "pubmed",
        bq_table_id: str = "articles",
        bq_dataset_description: str = "Pubmed Medline database. https://pubmed.ncbi.nlm.nih.gov/about/ ",
        start_date: str = pendulum.datetime(year=2022, month=11, day=27),  # When the baseline data became available.
        schedule_interval: str = "0 0 * * 0",  # weekly
        catchup: bool = True,
        ftp_server_url: str = "ftp.ncbi.nlm.nih.gov",
        ftp_port: str = 21,
        reset_ftp_counter: int = 20,
        check_md5_hash: bool = True,
        max_download_retry: int = 5,
        observatory_api_conn_id: str = AirflowConns.OBSERVATORY_API,
        queue: str = "remote_queue",
        snapshot_expiry_days: int = 7,
        max_processes: int = 4,  # Limited to 4 due to RAM usage when transforming files
        batch_size: int = 4,
    ):
        """Construct an PubMed Telescope instance.

        :param dag_id: the id of the DAG.
        :param cloud_workspace: Cloud settings.
        :param bq_dataset_id: Dataset name for final tables.
        :param start_date: the start date of the DAG.
        :param schedule_interval: the schedule interval of the DAG.
        :param catchup: Turn catch up on or not.
        :param ftp_server_url: Server address of Pubmed's FTP server.
        :param ftp_port: FTP port.
        :param reset_ftp_counter: Resets FTP connection after downloading x number of files.
        :param check_md5_hash: Verify the MD5 checksum of the downloaded changefile.
        :param queue: the queue that the tasks should run on.
        :param snapshot_expiry_days: How long until the snapshot expires.
        :param max_processes: Max number of parallel processors.
        :param batch_size: Number of changefiles per batch.
        :param bq_table_id: Name of Pubmed table.
        :param table_description: Description of the main table.
        """

        self.observatory_api_conn_id = observatory_api_conn_id
        self.start_date = start_date

        super().__init__(
            dag_id=dag_id,
            start_date=start_date,
            schedule_interval=schedule_interval,
            catchup=catchup,
            airflow_conns=[observatory_api_conn_id],
            queue=queue,
            tags=[Tag.academic_observatory],
        )

        # Databse settings
        self.cloud_workspace = cloud_workspace
        self.bq_dataset_id = bq_dataset_id
        self.bq_table_id = bq_table_id
        self.bq_dataset_description = bq_dataset_description
        self.snapshot_expiry_days = snapshot_expiry_days

        # Workflow parameters
        self.dag_id = dag_id
        self.schedule_interval = schedule_interval

        # Metadata of how to pull out data from the XML files.
        self.entity_list = [
            PubmedEntity(
                name="article_additions",
                type="additions",
                sub_key="PubmedArticle",
                set_key="PubmedArticleSet",
                pmid_location="MedlineCitation",
                table_description="""PubmedArticle - Includes all the metadata associated with a journal article citation, both the
                metadata to describe the published article, i.e. <MedlineCitation>, and additional metadata often
                pertaining to the publication's history or processing at NLM, i.e. <PubMedData>.""",
            ),
            PubmedEntity(
                name="article_deletions",
                type="deletions",
                sub_key="PMID",
                set_key="DeleteCitation",
                pmid_location=None,
                table_description="""DeleteCitation - Indicates one or more <PubmedArticle> or <PubmedBookArticle> that have been deleted. 
                PMIDs in DeleteCitation will typically have been found to be duplicate citations, or citations to content 
                that was determined to be out-of-scope for PubMed. It is possible that a PMID would appear in DeleteCitation 
                without having been distributed in a previous file. This would happen if the creation and deletion of the 
                record take place on the same day.""",
            ),
        ]

        # PubMed download parameters
        self.ftp_server_url = ftp_server_url
        self.ftp_port = ftp_port
        self.baseline_path = "/pubmed/baseline/"
        self.updatefiles_path = "/pubmed/updatefiles/"

        self.check_md5_hash = check_md5_hash
        self.max_download_retry = max_download_retry
        self.max_processes = max_processes
        self.batch_size = batch_size

        # Required file size of the update files.
        self.merged_file_size = 3.8  # Gb

        # After how many downloads to reset the connection to Pubmed's FTP server.
        self.reset_ftp_counter = reset_ftp_counter

        # Wait for the previous DAG run to finish to make sure that
        # changefiles are processed in the correct order
        external_task_id = "dag_run_complete"
        self.add_operator(
            PreviousDagRunSensor(
                dag_id=self.dag_id,
                external_task_id=external_task_id,
                execution_delta=timedelta(days=7),  # To match the @weekly schedule_interval
            )
        )

        self.add_setup_task(self.check_dependencies)
        self.add_setup_task(self.list_changefiles_for_release)
        self.add_task(self.download)
        self.add_task(self.upload_downloaded)
        self.add_task(self.transform)
        self.add_task(self.merge_updatefiles)
        self.add_task(self.upload_transformed)
        self.add_task(self.bq_ingest_update_tables)
        self.add_task(self.bq_add_updates_to_table)
        self.add_task(self.add_new_dataset_release)
        self.add_task(self.cleanup)

        # The last task that the next DAG run's ExternalTaskSensor waits for.
        self.add_operator(
            DummyOperator(
                task_id=external_task_id,
            )
        )

    def list_changefiles_for_release(self, **kwargs) -> bool:
        """
        Get a list of all files to download for this data interval run / release period.
        """

        dag_run = kwargs["dag_run"]
        is_first_run = is_first_dag_run(dag_run)
        data_interval_start = kwargs["data_interval_start"]
        data_interval_end = kwargs["data_interval_end"]

        # Open FTP connection to PubMed servers.
        ftp_conn = FTP()
        ftp_conn.connect(host=self.ftp_server_url, port=self.ftp_port)
        ftp_conn.login()  # anonymous login (publicly available data)

        files_to_download = []
        if is_first_run:
            logging.info(f"This is the first release of the PubMed Telescope. Grabbing list of 'baseline' files. ")

            # Change to the baseline directory.
            ftp_conn.cwd(self.baseline_path)

            # Grab list of all baseline files to be downloaded.
            baseline_list_ftp = ftp_conn.nlst()

            # Grab metadata and path of the file.
            for file in baseline_list_ftp:
                if file.endswith(".xml.gz"):  # Find all the xml.gz files available from the server.
                    filename = file
                    file_index = int(file[9:13])
                    path_on_ftp = self.baseline_path + file
                    changefile = Changefile(
                        filename=filename,
                        file_index=file_index,
                        path_on_ftp=path_on_ftp,
                        is_first_run=is_first_run,
                        changefile_date=self.start_date,
                    )
                    files_to_download.append(changefile)
        else:
            logging.info(
                f"Grabbing list of 'updatefiles' for this release: {data_interval_start} to {data_interval_end}"
            )

            # Change to updatefiles directory
            ftp_conn.cwd(self.updatefiles_path)
            ftp_conn.sendcmd("PASV")

            # Find all the .xml.gz files available
            updatefiles_list = ftp_conn.nlst()
            updatefiles_xml_gz = [file for file in updatefiles_list if file.endswith(".xml.gz")]

            for file in updatefiles_xml_gz:
                # Only return list of updatefiles that are within the required release period.
                file_upload_time = ftp_conn.sendcmd("MDTM {}".format(file))[4:]
                file_upload_date = pendulum.from_format(file_upload_time, "YYYYMMDDHHmmss")
                if file_upload_date in pendulum.period(data_interval_start, data_interval_end):
                    # Grab name and metadata for this release file.
                    filename = file
                    file_index = int(file[9:13])
                    path_on_ftp = self.updatefiles_path + file
                    changefile_date = file_upload_date

                    changefile = Changefile(
                        filename=filename,
                        file_index=file_index,
                        path_on_ftp=path_on_ftp,
                        changefile_date=changefile_date,
                        is_first_run=is_first_run,
                    )
                    files_to_download.append(changefile)

            # Check that all updatefiles pulled from the FTP server are not missing any between start_index and end_index.
            # e.g. 10, 11, 13, 14 - will throw an error.

            # Sort from oldest to newest using the file index
            files_to_download.sort(key=lambda c: c.file_index, reverse=False)
            file_index_last = files_to_download[0].file_index
            logging.info(f"file_index_last: {file_index_last}")
            for changefile in files_to_download[1:]:
                logging.info(f"{changefile.file_index} {file_index_last + 1}")
                if changefile.file_index == file_index_last + 1:
                    logging.info(
                        f"changefile.file_index = {changefile.file_index} - file_index_last + 1 = {file_index_last + 1} "
                    )
                    file_index_last = changefile.file_index
                else:
                    raise AirflowException(
                        f"The update files are not going to be sequential. Please investigate download {changefile.file_index} and {file_index_last+1}"
                    )

            # Make sure the first changefile file index for this release is n + 1 ahead of the last release.
            dataset_releases = get_dataset_releases(dag_id=self.dag_id, dataset_id=self.bq_dataset_id)
            latest_release = get_latest_dataset_release(dataset_releases, "changefile_end_date")
            if files_to_download[0].file_index != latest_release.sequence_end + 1:
                raise AirflowException(
                    f"Last updatefile index is not n+1 from the previous release. Latest release update index: {latest_release.sequence_end + 1} vs current start: {files_to_download[0].file_index}"
                )

            logging.info(f"List of files to download from the PubMed FTP server for 'updatefiles':")
            for changefile in files_to_download:
                logging.info(f"{changefile.filename}")

        # Close the connection to the FTP server.
        ftp_conn.close()

        # Push list of changefile objects to download to the Xcom for next step.
        ti: TaskInstance = kwargs["ti"]
        files_to_download_list = [changefile.to_dict() for changefile in files_to_download]
        ti.xcom_push(key="files_to_download", value=files_to_download_list)

        # Return True so downstream tasks aren't skipped.
        return True

    def make_release(self, **kwargs) -> PubMedRelease:
        """
        Make a Release instance. Gets the list of releases available from the release check (setup task).

        :param kwargs: the context passed from the Airflow Operator.
        See https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return PubMedRelease.
        """

        # Pass the list of files to download to the release.

        ti: TaskInstance = kwargs["ti"]
        files_to_download_list = ti.xcom_pull(
            key="files_to_download", task_ids=self.list_changefiles_for_release.__name__, include_prior_dates=False
        )
        changefile_list = [Changefile.from_dict(changefile) for changefile in files_to_download_list]

        # Sort the incoming list.
        changefile_list.sort(key=lambda c: c.file_index, reverse=False)

        # limit to the first 20 files for testing
        changefile_list = changefile_list[:100]

        run_id = kwargs["run_id"]
        dag_run = kwargs["dag_run"]
        is_first_run = is_first_dag_run(dag_run)
        start_date = kwargs["data_interval_start"]
        end_date = kwargs["data_interval_end"]

        release = PubMedRelease(
            dag_id=self.dag_id,
            run_id=run_id,
            cloud_workspace=self.cloud_workspace,
            start_date=start_date,
            end_date=end_date,
            is_first_run=is_first_run,
            changefile_list=changefile_list,
        )

        logging.info(f"Release info: {release}")

        return release

    def download(self, release: PubMedRelease, **kwargs):
        """
        Download files from PubMed's FTP server for this release.

        Unable to do this in parallel because their FTP server is not able to handle too many requests.
        """

        # Open FTP connection
        ftp_conn = FTP()
        ftp_conn.connect(host=self.ftp_server_url, port=self.ftp_port)
        ftp_conn.login()  # anonymous login (publicly available data)

        num_to_download = len(release.changefile_list)
        for i in range(0, num_to_download, 1):
            changefile = release.changefile_list[i]

            # The FTP server disconnects the connection for the download after some time.
            # Need to have it refreshed every so often so we can reliably download it.

            # Every self.reset_ftp_counter number of files that are downloaded, reinitialise the FTP connection.
            if i % self.reset_ftp_counter == 0 and i != 0:
                # Close exisiting FTP connection.
                ftp_conn.close()

                # Open a new FTP connection
                ftp_conn = FTP()
                ftp_conn.connect(host=self.ftp_server_url, port=self.ftp_port)
                ftp_conn.login()  # anonymous login (publicly available data)

                logging.info(
                    f"FTP connection to Pubmed's servers has been reset after {self.reset_ftp_counter} downloads to avoid issues."
                )

            if self.check_md5_hash:
                download_attempt_count = 1
                download_success = False
                while download_attempt_count <= self.max_download_retry and not download_success:
                    logging.info(f"Downloading: {changefile.filename} Attempt: {download_attempt_count}")
                    try:
                        download_location = changefile.download_file_path
                        # Download file
                        with open(download_location, "wb") as f:
                            ftp_conn.retrbinary(f"RETR {changefile.path_on_ftp}", f.write)
                        logging.info(f"File downloaded to - {download_location}")
                    except error_reply:
                        logging.info(
                            f"Unable to download {changefile.path_on_ftp} from PubMed's FTP server {self.ftp_server_url}."
                        )

                    # Create the hash from the above downloaded file.
                    with open(download_location, "rb") as f_hash:
                        data = f_hash.read()
                        md5hash_from_download = hashlib.md5(data).hexdigest()

                    logging.info(f"MD5 has from downloaded file: {md5hash_from_download}")

                    # Need to have a download catch for the hash file as well, otherwise it can break the download loop.
                    try:
                        # Download corresponding md5 hash.
                        with open(f"{download_location}.md5", "wb") as f:
                            ftp_conn.retrbinary(f"RETR {changefile.path_on_ftp}.md5", f.write)
                    except error_reply:
                        logging.info(
                            f"Unable to download {changefile.path_on_ftp}.md5 from PubMed's FTP server {self.ftp_server_url}."
                        )

                    with open(f"{download_location}.md5", "r") as f_md5:
                        md5_from_pubmed_ftp = f_md5.read()

                    # If md5 does not match, retry download
                    if md5hash_from_download in md5_from_pubmed_ftp:
                        download_success = True
                    else:
                        logging.info(
                            f"MD5 hash does not match the given MD5 checksum from server: {changefile.download_file_path} - Retrying download ..."
                        )

                    download_attempt_count += 1

                if not download_success:
                    raise AirflowException(
                        f"Unable to download {download_location} from PubMed's FTP server {self.ftp_server_url} after {self.max_download_retry}"
                    )

            else:
                logging.info(f"Downloading: {changefile.download_file_path}")
                try:
                    # Download file
                    with open(changefile.download_file_path, "wb") as f:
                        ftp_conn.retrbinary(f"RETR {changefile.path_on_ftp}", f.write)
                except error_reply:
                    raise AirflowException(
                        f"Unable to download {changefile.path_on_ftp} from PubMed's FTP server {self.ftp_server_url}"
                    )

        # Close the FTP connection after downloading the required files.
        ftp_conn.close()

    def upload_downloaded(self, release: PubMedRelease, **kwargs):
        """
        Push all downloaded changefiles from Pubmed to Google Cloud Storage.
        """

        # Grab list of files to upload.
        changefiles_to_upload = [changefile.download_file_path for changefile in release.changefile_list]

        success = gcs_upload_files(
            bucket_name=self.cloud_workspace.download_bucket,
            file_paths=changefiles_to_upload,
        )

        set_task_state(success, kwargs["ti"].task_id, release=release)

    def transform(self, release: PubMedRelease, **kwargs):
        """
        Transform the *.xml.gz files downloaded from PubMed's FTP server into usable *.jsonl.gz files for BigQuery import.

        This is a multithreaded task that pulls the information from the files using the keys defined in self.entity_list.
        """

        # Grab list of changefiles for this release
        files_to_transform = release.changefile_list

        # Process files in batches so that ProcessPoolExecutor doesn't deplete the system of memory
        for i, chunk in enumerate(get_chunks(input_list=files_to_transform, chunk_size=self.max_processes)):
            with ProcessPoolExecutor(max_workers=self.max_processes) as executor:
                logging.info(f"In chunk {i} and processing files: {chunk}")

                futures = [
                    executor.submit(transform_pubmed_xml_file_to_jsonl, changefile, self.entity_list)
                    for changefile in chunk
                ]

                # Make sure that all changefiles have been properly transformed.
                for future in as_completed(futures):
                    transformed_changefile = future.result()

                    logging.info(f"Successfully transformed file - {transformed_changefile.filename}")

    def merge_updatefiles(self, release: PubMedRelease, **kwargs):
        """
        Check through the changefiles and only grab the newest records for a particular PMID and version number of the article.
        This is to reduce the number of quiries done on the main table in BQ.

        If it is the first run of the workflow, it will merge the baseline changefiles into more appropriate sized chunks.
        """

        # # Grab list of changefiles that were transformed in previous task.
        files_to_merge = release.changefile_list

        # Make sure that the incoming list of changefiles are sorted on file_index, oldest to newest.
        files_to_merge.sort(key=lambda c: c.file_index, reverse=False)

        merged_updatefiles = {}

        # Merge the baseline files so that they are easier for importing from GCS to BQ
        if release.is_first_run:
            logging.info(f"Merging the baseline files from the transform step.")

            # Loop through additions or deletions
            for entity in self.entity_list:
                merged_updatefiles[entity.name] = []

                # Get the size of all the updatefiles
                file_size_sum = 0
                for changefile in files_to_merge:
                    transform_file = changefile.transform_file_path(entity.type)
                    transform_file_stats = os.stat(transform_file)
                    transform_file_size = transform_file_stats.st_size / 1024.0**3
                    file_size_sum += transform_file_size

                logging.info(f"Total size of updatefiles for {entity.type} for this release: {file_size_sum} ")

                num_chunks = math.ceil(file_size_sum / self.merged_file_size)

                logging.info(f"Aproximate size of each merged: {file_size_sum/num_chunks} Gb")

                if num_chunks == 1:
                    logging.info(f"There were will be 1 part for the merged updatefiles.")

                    # Only one chunk to process for required file size, do in serial.
                    merged_updatefile_path = merge_changefiles_together(files_to_merge, 1, entity.type)
                    merged_updatefiles[entity.name].append(merged_updatefile_path)
                    logging.info(f"Successfully merged updatefiles to - {merged_updatefile_path}")

                else:
                    chunk_size = math.floor(len(files_to_merge) / num_chunks)
                    chunks = [
                        chunk for i, chunk in enumerate(get_chunks(input_list=files_to_merge, chunk_size=chunk_size))
                    ]

                    if num_chunks > self.max_processes:
                        processes_to_use = self.max_processes

                    else:
                        processes_to_use = num_chunks

                    logging.info(f"There were will be {len(chunks)} parts for the merged updatefiles.")

                    # Multiple output for merged files, do in parallel.
                    for j, sub_chunks in enumerate(get_chunks(input_list=chunks, chunk_size=processes_to_use)):
                        # Pass off each chunk to a process for them to merge files in parallel.
                        with ProcessPoolExecutor(max_workers=processes_to_use) as executor:
                            futures = []
                            for i, chunk in enumerate(sub_chunks):
                                futures.append(
                                    executor.submit(
                                        merge_changefiles_together, chunk, i + 1 + j * processes_to_use, entity.type
                                    )
                                )

                            for future in as_completed(futures):
                                merged_updatefile_path = future.result()
                                merged_updatefiles[entity.name].append(merged_updatefile_path)

                                logging.info(f"Successfully merged updatefiles to - {merged_updatefile_path}")

        # For each updatefile, only keep the newest record for a PMID and version.
        else:
            # Separating the updatefiles into smaller chunks is not necessary as they are small to begin with.
            # Max number of updatefiles per week is about 10 to 15 and will be less than 4gb when compressed.

            logging.info(f"Running through each of the updatefiles and checking if there are updated records.")

            for entity in self.entity_list:
                # Find newest records from the updatefiles
                # Clear last entity
                merged_data = []
                merged_updatefiles[entity.name] = []
                update_counter = 0

                first_file_index = files_to_merge[0].file_index
                for changefile in files_to_merge:
                    transform_file = changefile.transform_file_path(entity.type)

                    logging.info(f"Reading in file - {transform_file}")

                    # Read in the updatefile into memory
                    with gzip.open(transform_file, "r") as f_in:
                        incoming_data = [json.loads(line) for line in f_in]

                    if entity.type == "additions":
                        # Make the merged data a dictionary for faster lookup.
                        merged_dict = {str(record[entity.pmid_location]["PMID"]): record for record in merged_data}

                        for incoming_record in incoming_data:
                            key = str(incoming_record[entity.pmid_location]["PMID"])
                            if key in merged_dict:
                                logging.info(
                                    f"Found a duplicate of PMID and version number {key} - {incoming_record[entity.pmid_location]['PMID']}"
                                )

                                # Remove the old record from merged_data
                                del merged_dict[key]

                                update_counter += 1

                        # Convert back to a list.
                        merged_data = list(merged_dict.values())

                    # Add on the newer records.
                    # This will also merge all of the deletion records into one file so that can be removed from the main table.
                    merged_data.extend(incoming_data)

                logging.info(f"Found the following number of records that were updated - {update_counter}")

                merged_output_file = changefile.merged_transform_file_path(
                    entity.type, first_file_index, changefile.file_index
                )

                merged_updatefiles[entity.name].append(merged_output_file)

                logging.info(f"Writing to file - {merged_output_file}")

                # Write to compressed *.jsonl.gz
                with gzip.open(merged_output_file, "w") as f_out:
                    for line in merged_data:
                        f_out.write(str.encode(json.dumps(line, cls=PubMedCustomEncoder) + "\n"))

        # Push list of merged transformed files to Xcom for upload step.
        ti: TaskInstance = kwargs["ti"]
        ti.xcom_push(key="merged_updatefiles", value=merged_updatefiles)

    def upload_transformed(self, release: PubMedRelease, **kwargs):
        """
        Upload the merged transformed files to GCS.
        """

        ti: TaskInstance = kwargs["ti"]
        merged_updatefiles = ti.xcom_pull(key="merged_updatefiles")
        files_to_upload = []
        for entity in self.entity_list:
            files_to_upload.extend(merged_updatefiles[entity.name])

        logging.info(f"files_to_upload - {files_to_upload}")

        success = gcs_upload_files(
            bucket_name=self.cloud_workspace.transform_bucket,
            file_paths=files_to_upload,
        )

        set_task_state(success, kwargs["ti"].task_id, release=release)

    def bq_ingest_update_tables(self, release: PubMedRelease, **kwargs):
        """
        Ingest data into tables for this particular release.

        If first run of the workflow, it will only trasnfer the baseline table in BQ, otherwise it will
        ingest the addition and deletion tables as shards.
        """

        ti: TaskInstance = kwargs["ti"]
        merged_updatefiles = ti.xcom_pull(key="merged_updatefiles")

        # If its the first run of the workflow, only upload the baseline table to the main table.
        if release.is_first_run:
            # Create the dataset if it doesn't exist already.
            bq_create_dataset(
                project_id=self.cloud_workspace.project_id,
                dataset_id=self.bq_dataset_id,
                location=self.cloud_workspace.data_location,
                description=self.bq_dataset_description,
            )

            # Delete old baseline table just in case there was a bad previous run.
            bq_delete_table(
                project_id=self.cloud_workspace.project_id,
                dataset_id=self.bq_dataset_id,
                table_id=self.bq_table_id,
                not_found_okay=True,
            )

            logging.info(f"Uploading to table - {self.bq_table_id}")

            merged_transform_blob_pattern = release.merged_transfer_blob_pattern("additions")

            logging.info(
                f"Creating a load job for all of the smaller baseline files with pattern: {merged_transform_blob_pattern} "
            )

            success = bq_load_table(
                uri=merged_transform_blob_pattern,
                table_id=f"{self.cloud_workspace.project_id}.{self.bq_dataset_id}.{self.bq_table_id}",
                source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
                schema_file_path=self.entity_list[0].schema_file_path,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                table_description=self.entity_list[0].table_description,
                partition=False,
                ignore_unknown_values=False,
            )

            if not success:
                raise AirflowException(f"Unable to upload table {self.bq_table_id}")

        # Not the first workflow run, only ingest update tables into BQ.
        else:
            # Keep a list of update tables for this release.
            update_tables = {}

            # Upload release tables for this data interval - store on a sharded table and have it expire after a month.
            for entity in self.entity_list:
                table_id = bq_sharded_table_id(
                    self.cloud_workspace.project_id, self.bq_dataset_id, entity.name, date=release.end_date
                )
                merged_transform_blob_pattern = release.merged_transfer_blob_pattern(entity_type=entity.type)

                # Delete old table just in case there was a bad previous run.
                # We don't want to append or add any dupliactes to the tables.
                bq_delete_table(
                    project_id=self.cloud_workspace.project_id,
                    dataset_id=self.bq_dataset_id,
                    table_id=table_id.split(".")[2],
                    not_found_okay=True,
                )

                logging.info(f"Uploading to table - {table_id}")

                success = bq_load_table(
                    uri=merged_transform_blob_pattern,
                    table_id=table_id,
                    source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
                    schema_file_path=entity.schema_file_path,
                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                    table_description=f"Update table for {entity.type} for Pumbed update files {release.changefile_list[0].filename} to {release.changefile_list[-1].filename}. {entity.table_description}",
                    partition=False,
                    ignore_unknown_values=False,
                )

                if success:
                    logging.info(f"Successfully tranferred to table - {table_id}")
                    update_tables[entity.name] = table_id

                else:
                    raise AirflowException(f"Unable to tranfer to table - {table_id}")

                # Update the table expiration so we don't have lots of old sharded tables hanging around in the dataset.
                expiry_date = pendulum.now().add(days=7)
                bq_update_table_expiration(
                    self.cloud_workspace.project_id,
                    self.bq_dataset_id,
                    table_id.split(".")[2],
                    expiration_date=expiry_date,
                )

            # Push list of update tables into the xcom
            ti.xcom_push(key="update_tables", value=update_tables)

    def bq_add_updates_to_table(self, release: PubMedRelease, **kwargs):
        """
        Updates the main table with the additions and deletions from the update files.
        """

        # Use the table from this workflow run to make the first snapshot.
        # This should be just the baseline dataset, so there should be no deletions to the snapshot.
        if release.is_first_run:
            table_exists = bq_table_exists(
                table_id=f"{self.cloud_workspace.project_id}.{self.bq_dataset_id}.{self.bq_table_id}"
            )
            if not table_exists:
                raise AirflowException(
                    f"Main table {self.bq_table_id} does not exist. Something went wrong in the previous transfer step."
                )
            else:
                logging.info(
                    f"""First run of the workflow does not require more updates to be added to the table. \n
                        The table {self.bq_table_id} has already been created and injested in the previous step."""
                )

        else:
            # Pull list of update tables from xcom
            ti: TaskInstance = kwargs["ti"]
            update_tables = ti.xcom_pull(key="update_tables")

            # Create a snapshot of main table as a backup just in case something happens with the below updates and deletes
            # and we can revert if necessary.

            prev_end_date = kwargs["data_interval_end"]
            full_table_id = f"{self.cloud_workspace.project_id}.{self.bq_dataset_id}.{self.bq_table_id}"
            backup_table_id = bq_sharded_table_id(
                self.cloud_workspace.project_id, self.bq_dataset_id, f"{self.bq_table_id}_backup", prev_end_date
            )
            expiry_date = pendulum.now().add(days=31)
            success = bq_snapshot(src_table_id=full_table_id, dst_table_id=backup_table_id, expiry_date=expiry_date)

            set_task_state(success, kwargs["ti"].task_id, release=release)

            # Need to use custom queries for the update and delete since they rely on both the PMID value and Version.

            # Delete old records in the main table that are to be updated.
            delete_records_to_be_updated = f"""
            DELETE FROM `{full_table_id}`
            WHERE (MedlineCitation.PMID.value, MedlineCitation.PMID.Version) IN (
            SELECT (MedlineCitation.PMID.value, MedlineCitation.PMID.Version)
            FROM `{update_tables['article_additions']}`)
            """
            result = bq_run_query(delete_records_to_be_updated)
            logging.info(f"Result from deleting old records from {self.bq_table_id} - {result}")

            # Insert the new updated records.
            insert_updated_records = f"""
            INSERT INTO `{full_table_id}`
            SELECT *
            FROM `{update_tables['article_additions']}`
            """
            result = bq_run_query(insert_updated_records)
            logging.info(f"Result from inserting the updated records to {self.bq_table_id} - {result}")

            # Delete the records from the "deletion" table using both PMID and version number.
            delete_records_from_table = f"""
            DELETE FROM `{full_table_id}`
            WHERE (MedlineCitation.PMID.value, MedlineCitation.PMID.Version) IN (
            SELECT (value, Version) 
            FROM `{update_tables['article_deletions']}`)
            """
            result = bq_run_query(delete_records_from_table)
            logging.info(f"Result from inserting the updated records to {self.bq_table_id} - {result}")

    def add_new_dataset_release(self, release: PubMedRelease, **kwargs):
        """
        Adds release information to API.
        """

        logging.info(f"add_new_dataset_releases: creating dataset release for Pubmed Articles.")
        dataset_release = DatasetRelease(
            dag_id=self.dag_id,
            dataset_id=self.bq_dataset_id,
            dag_run_id=release.run_id,
            data_interval_start=release.start_date,
            data_interval_end=release.end_date,
            sequence_start=release.changefile_list[0].file_index,
            sequence_end=release.changefile_list[-1].file_index,
            changefile_start_date=release.start_date,
            changefile_end_date=release.end_date,
        )
        logging.info(f"add_new_dataset_releases: dataset_release={dataset_release}")
        api = make_observatory_api(observatory_api_conn_id=self.observatory_api_conn_id)
        api.post_dataset_release(dataset_release)

    def cleanup(self, release: PubMedRelease, **kwargs):
        """
        Cleanup files from this workflow run.

        Delete local download files, tranform files and current task instance.
        """

        logging.info(f"Deleting local files from - {release.workflow_folder}")
        cleanup(dag_id=self.dag_id, execution_date=kwargs["logical_date"], workflow_folder=release.workflow_folder)


def transform_pubmed_xml_file_to_jsonl(
    changefile: Changefile, entity_list: List[PubmedEntity]
) -> Union[Changefile, bool]:
    """
    Convert a single Pubmed XML file to JSONL, pulling out any of the Pubmed entities and their additions
    and or deletions.

    Used in parallelised section "transform". Each file is given to an individual process
    rather than multple files read in and transformed at once.

    :param changefile: Incoming changefile to transform.
    :param entity_list" List of entities to pull out from the changefile.

    :return: Changefile object that's been transformed.
    """

    logging.info(f"Reading in file - {changefile.filename}")

    with gzip.open(changefile.download_file_path, "rb") as f_in:
        # Use the BioPython library for reading in the Pubmed XML files.
        try:
            data_dict_dirty = Entrez.read(f_in, validate=True)
        except ValidationError:
            logging.info(f"Fields in XML are not valid against it's own DTD file - {changefile.filename}")
            return False

        # Need to have the XML attributes pulled out from the Biopython data classes.
        data_dict = add_attributes_to_data_from_biopython_classes(data_dict_dirty)
        data_dict_dirty = []

    # Loop through the Pubmed record metadata, pulling out additions or deletions.
    for entity in entity_list:
        logging.info(
            f"Extracting {entity.set_key if entity.type == 'deletions' else entity.sub_key} records from file - {changefile.filename}"
        )

        try:
            try:
                data_part = [retrieve for retrieve in data_dict[entity.set_key][entity.sub_key]]
            except KeyError:
                data_part = [retrieve for retrieve in data_dict[entity.sub_key]]

            logging.info(
                f"Pulled out {len(data_part)} {f'{entity.set_key} records' if entity.type == 'deletions' else f'{entity.sub_key} {entity.type}'} from file - {changefile.filename}"
            )

        except KeyError:
            logging.info(
                f"No {f'{entity.set_key} records' if entity.type == 'deletions' else f'{entity.sub_key} {entity.type}'} records in file - {changefile.filename}"
            )
            data_part = []

        output_file = changefile.transform_file_path(entity.type)

        logging.info(f"Writing {entity.name} {entity.type} to file - {output_file}")

        # Write to compressed *.jsonl.gz
        with gzip.open(output_file, "w") as f_out:
            for line in data_part:
                f_out.write(str.encode(json.dumps(line, cls=PubMedCustomEncoder) + "\n"))

    return changefile


def merge_changefiles_together(changefile_list: List[Changefile], part_num: int, entity_type: str) -> str:
    """Merge a given list of changefiles together.

    :param changefile_list: List of changefiles to merge together.
    :param part_num: Part number of the merge file from the greater list of changefiles.
    :param entity_type: Type of changefiles given, for helping with name of the output file.
    :return merged_output_file: Path to the resultant output file.
    """

    merged_output_file = changefile_list[0].merged_transform_file_path(
        entity_type, changefile_list[0].file_index, changefile_list[-1].file_index, part_num
    )

    logging.info(f"Writing data to file - {merged_output_file}")

    # Read in data and directly write it out to file to avoid holding too much
    # data in memory and crashing the workflow.
    with gzip.open(merged_output_file, "w") as f_out:
        # Loop through the set of given changefiles
        for changefile_sub in changefile_list:
            transform_file_sub = changefile_sub.transform_file_path(entity_type)

            # Read in file
            with gzip.open(transform_file_sub, "rb") as f_in:
                data = [json.loads(line) for line in f_in]

            # Write directly to output file.
            for line in data:
                f_out.write(str.encode(json.dumps(line, cls=PubMedCustomEncoder) + "\n"))

    return merged_output_file


def add_attributes_to_data_from_biopython_classes(
    obj: Union[StringElement, DictionaryElement, ListElement, OrderedListElement, list]
):
    """
    Recursively travel down the data tree and add attributes from the Biopython classes as dictionary keys.

    :param obj: Input object, being one of the Biopython data classes.
    :return new: Object with attributes added as keys to the dictionary.
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


class PubMedCustomEncoder(json.JSONEncoder):

    """
    Custom encoder for JSON dump for it to write a dictionary field as a string of text for a
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


def bq_delete_table(
    project_id: str, dataset_id: str, table_id: str, partition_decorator: str = None, not_found_okay: bool = True
):
    """
    Delete a single table in Bigquery.

    # TODO: Add to bq utils later

    :param project_id: Google project id.
    :param dataset_id: Name of the dataset.
    :param table_id: Table name.
    :param partition_decorator: Partition decorator.
    :param not_found_okay: Is okay if the table does not exist.

    :return: None.
    """

    bq_client = bigquery.Client(project=project_id)
    dataset = bigquery.DatasetReference(project_id, dataset_id)
    table = bigquery.Table(dataset.table(table_id))

    try:
        if partition_decorator:
            bq_client.delete_table(table, tableId=partition_decorator, not_found_ok=not_found_okay)
        else:
            bq_client.delete_table(table, not_found_ok=not_found_okay)

        logging.info(f"Deleted exising bigquery table: {table_id} {partition_decorator}")
    except:
        raise AirflowException(f"An error occured deleting table {table_id}")


def bq_update_table_expiration(project_id: str, dataset_id: str, table_id: str, expiration_date: pendulum.datetime):
    """
    Update a Bigquery table expiration date.

    # TODO: Add to bq utils later

    :param project_id: Google project id.
    :param dataset_id: Name of the dataset.
    :param table_id: Table name.
    :param expiration_date: Expiration date of the table.

    :return: None.
    """

    bq_client = bigquery.Client(project=project_id)
    table_ref = bq_client.dataset(dataset_id).table(table_id)
    table = bq_client.get_table(table_ref)

    # Update the expiration time
    table.expires = expiration_date

    # Update the table metadata
    bq_client.update_table(table, ["expires"])
