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

from __future__ import annotations

import os
import re
import gzip
import json
import time
import random
import hashlib
import logging
import pendulum
from Bio import Entrez
from datetime import timedelta
from google.cloud import bigquery
from ftplib import FTP, error_reply
from typing import Any, Union, Dict, List, Optional
from google.cloud.bigquery import SourceFormat
from concurrent.futures import ProcessPoolExecutor, as_completed
from Bio.Entrez.Parser import StringElement, ListElement, DictionaryElement, OrderedListElement, ValidationError

from airflow import AirflowException
from airflow.operators.dummy import DummyOperator
from airflow.models.taskinstance import TaskInstance

from observatory.platform.airflow import PreviousDagRunSensor, is_first_dag_run
from observatory.platform.files import get_chunks
from observatory.api.client.model.dataset_release import DatasetRelease
from observatory.platform.api import get_latest_dataset_release, get_dataset_releases, make_observatory_api
from observatory.platform.gcs import gcs_upload_files, gcs_blob_name_from_path
from observatory.platform.observatory_config import CloudWorkspace
from observatory.platform.config import AirflowConns
from observatory.platform.workflows.workflow import (
    Workflow,
    set_task_state,
    cleanup,
)

from observatory.platform.workflows.workflow import ChangefileRelease as DatafileRelease
from observatory.platform.bigquery import (
    bq_sharded_table_id,
    bq_create_dataset,
    bq_load_table,
    bq_snapshot,
    bq_upsert_records,
    bq_delete_records,
)
from academic_observatory_workflows.config import schema_folder as default_schema_folder, Tag


class Datafile:
    def __init__(
        self,
        filename: str,
        file_index: int,
        baseline: bool,
        path_on_ftp: str,
        datafile_date: pendulum.DateTime,
        datafile_release: DatafileRelease = None,
    ):
        """Holds the metadata about a single Pubmed datafile.

        Pubmed is organised into a yearly "baseline" snapshots and then "updatefiles" are released daily afterwards
        to modify and or add to the table. To avoid confusion, we call both the baseline and updatefiles a "datafile".
        Each datafile could hold an upsert or delete record and are extracted in later parts of the workflow.

        :param filename: the name of the datafile.
        :param file_index: Index of the datafile_date, effectively a count of the n-th datafile.
        :param baseline: Boolean for if this is from the bsaeline set of files. False is it is from the updatefiles set.
        :param path_on_ftp: Path of the file on Pubmed's FTP server.
        :param datafile_date: The date that the datafile_date was added to PubMed's FTP server.
        :param DatafileRelease: The Datafile release, helps give download and transform paths to files.
        """

        self.filename = filename
        self.file_index = file_index
        self.baseline = baseline
        self.path_on_ftp = path_on_ftp
        self.datafile_date = datafile_date
        self.datafile_release: DatafileRelease = datafile_release

        self.file_type = "jsonl.gz"

    def __eq__(self, other):
        if isinstance(other, Datafile):
            return (
                self.filename == other.filename
                and self.file_index == other.file_index
                and self.baseline == other.baseline
                and self.path_on_ftp == other.path_on_ftp
                and self.datafile_date == other.datafile_date
                and self.datafile_release == other.datafile_release
            )
        return False

    @staticmethod
    def from_dict(dict_: Dict) -> Datafile:
        filename = dict_["filename"]
        file_index = dict_["file_index"]
        baseline = dict_["baseline"]
        path_on_ftp = dict_["path_on_ftp"]
        datafile_date = pendulum.parse(dict_["datafile_date"])
        datafile_release = dict_["datafile_release"]

        return Datafile(
            filename=filename,
            file_index=file_index,
            baseline=baseline,
            path_on_ftp=path_on_ftp,
            datafile_date=datafile_date,
            datafile_release=datafile_release,
        )

    def to_dict(self) -> Dict:
        return dict(
            filename=self.filename,
            file_index=self.file_index,
            baseline=self.baseline,
            path_on_ftp=self.path_on_ftp,
            datafile_date=self.datafile_date.isoformat(),
            datafile_release=self.datafile_release,
        )

    @property
    def download_file_path(self):
        assert self.datafile_release is not None, "Datafile.download_folder: self.datafile_release is None"
        return os.path.join(self.datafile_release.download_folder, self.filename)

    @property
    def transform_path(self):
        assert self.datafile_release is not None, "Datafile.transform_path: self.datafile_release is None"
        return self.datafile_release.transform_folder

    def transform_file_path(self, entity_type: str) -> str:
        """
        Give path to the transform file depending on entity type.

        :param entity_type: Type of the record, either "upsert" or "delete".
        :return: Retuns the path to the transformed file.
        """

        assert self.datafile_release is not None, "Datafile.transform_file_path: self.datafile_release is None"

        # Remove the .gz compression if it's not a baseline file.
        if self.baseline:
            file_type = self.file_type
        else:
            file_type = self.file_type[:-3]

        return os.path.join(
            self.datafile_release.transform_folder,
            f"{entity_type}_{self.filename[:-7]}.{file_type}",
        )

    def transform_file_pattern(self, entity_type: str) -> str:
        """Return a glob pattern for the transform files.

        :param entity_type: Type of the record, either "upsert" or "delete".
        :return: Retuns the path to the transformed file.
        """

        assert self.datafile_release is not None, "Datafile.transform_file_path: self.datafile_release is None"
        return os.path.join(
            self.datafile_release.transform_folder,
            f"{entity_type}_*.{self.file_type}",
        )

    def merged_transform_file_path(
        self, entity_type: str, first_file_index: int, last_file_index: int, part_num: int
    ) -> str:
        """
        Give path to the merged transform file.

        :param entity_type: Type of the record, either "upsert" or "delete".
        :return: Retuns the path to the transformed file.
        """

        assert self.datafile_release is not None, "Datafile.transform_file_path: self.datafile_release is None"
        return os.path.join(
            self.datafile_release.transform_folder,
            f"merged_{entity_type}_{first_file_index}-{last_file_index}_part_{part_num}.{self.file_type}",
        )

    def merged_tranform_file_pattern(self, entity_type: str) -> str:
        """
        Return a glob pattern for the merged transform files.

        :param entity_type: Type of the record, either either "upsert" or "delete".
        :return: Retuns the path to the transformed file.
        """
        assert self.datafile_release is not None, "Datafile.transform_file_path: self.datafile_release is None"
        return os.path.join(
            self.datafile_release.transform_folder,
            f"merged_{entity_type}_*-*_part_*.{self.file_type}",
        )


class PubmedEntity:
    def __init__(
        self,
        type: str,
        sub_key: str,
        set_key: str,
        pmid_location: Optional[str],
        table_name: Optional[str],
        table_description: str,
    ):
        """
        Holds the metadata about the records stored in Pubmed.
        :param type: Either a set of 'upsert' or 'deletion' records.
        :param sub_key: Sub key location of Pubmed data.
        :param set_key: Key location of Pubmed.
        :param pmid_location: Location of PMID values
        :param table_description: Description for the table on Bigquery.
        """

        self.type = type
        self.sub_key = sub_key
        self.set_key = set_key
        self.pmid_location = pmid_location
        self.table_description = table_description

        self.schema_file_path = os.path.join(default_schema_folder(), "pubmed", f"{table_name}.json")


class PubMedRelease(DatafileRelease):
    def __init__(
        self,
        *,
        dag_id: str,
        run_id: str,
        cloud_workspace: CloudWorkspace,
        start_date: pendulum.DateTime,
        end_date: pendulum.DateTime,
        year_first_run: bool,
        datafile_list: List[Datafile],
    ):
        """
        Construct a PubmedRelease.

        :param dag_id: the DAG id.
        :param run_id: Run id of this workflow.
        :param cloud_workspace: Holds cloud location details.
        :param start_date: Start date of the release.
        :param end_date: End date of the release.
        :param year_first_run: True if it's the first run of the workflow for the year, if this
        release is to download the baseline files of Pubmed.
        :param datafile_list: List of datafiles for this release.
        """

        super().__init__(
            dag_id=dag_id,
            run_id=run_id,
            start_date=start_date,
            end_date=end_date,
            sequence_start=datafile_list[0].file_index,
            sequence_end=datafile_list[-1].file_index,
        )
        self.cloud_workspace = cloud_workspace
        self.year_first_run = year_first_run
        self.datafile_list = datafile_list

        self.datafile_release = DatafileRelease(
            dag_id=dag_id,
            run_id=run_id,
            start_date=start_date,
            end_date=end_date,
            sequence_start=datafile_list[0].file_index,
            sequence_end=datafile_list[-1].file_index,
        )

        for datafile in datafile_list:
            datafile.datafile_release = self.datafile_release

    def transfer_blob_pattern(self, entity_type: str) -> str:
        """
        Create a blob pattern for importing the transformed unmerged records from GCS into Bigquery.

        :param entity_type: Type of the entity.
        :return: Uri pattern for transformed files.
        """

        return f"gs://{self.cloud_workspace.transform_bucket}/{gcs_blob_name_from_path(self.datafile_release.transform_folder)}/{entity_type}_*.{self.datafile_list[0].file_type}"

    def merged_transfer_blob_pattern(self, entity_type: str) -> str:
        """
        Create a blob pattern for importing the transformed merged records from GCS into Bigquery.

        Only the upserts and delete records are merged and will use this glob pattern.

        :param entity_type: Type of the entity.
        :return: Uri pattern for merged transform files.
        """

        return f"gs://{self.cloud_workspace.transform_bucket}/{gcs_blob_name_from_path(self.datafile_release.transform_folder)}/merged_{entity_type}_*.{self.datafile_list[0].file_type}"


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
        observatory_api_conn_id: str = AirflowConns.OBSERVATORY_API,
        bq_dataset_id: str = "pubmed",
        bq_table_id: str = "pubmed",
        bq_dataset_description: str = "Pubmed Medline database, only PubmedArticle records: https://pubmed.ncbi.nlm.nih.gov/about/",
        start_date: pendulum.DateTime = pendulum.datetime(year=2022, month=12, day=8),
        schedule_interval: str = "@weekly",
        catchup: bool = False,
        ftp_server_url: str = "ftp.ncbi.nlm.nih.gov",
        ftp_port: int = 21,
        reset_ftp_counter: int = 40,
        max_download_retry: int = 5,
        num_merged_records: int = 30000,
        queue: str = "remote_queue",
        snapshot_expiry_days: int = 31,
        max_processes: int = 4,  # Limited to 4 due to RAM usage.
    ):
        """Construct an PubMed Telescope instance.

        :param dag_id: the id of the DAG.
        :param cloud_workspace: Cloud settings.
        :param observatory_api_conn_id: Observatory API connection for Airflow.
        :param bq_dataset_id: Dataset name for final tables.
        :param bq_table_id: Table name of the final Pubmed table.
        :param bq_dataset_description: Description of the Pubmed dataset.
        :param start_date: The start date of the DAG.
        :param schedule_interval: tTe schedule interval of the DAG, how often it should run.
        :param catchup: Turn catch up on or not.
        :param ftp_server_url: Server address of Pubmed's FTP server.
        :param ftp_port: Port for connectiong to Pubmed's FTP server.
        :param reset_ftp_counter: Resets FTP connection after downloading x number of files.
        :param max_download_retry: Maximum number of retries of a single Pubmed file from the FTP server before throwing an error.
        :param num_merged_records: Number of records to write to file when merging the upsert and delete records, before import into BQ.
        :param queue: The queue that the tasks should run on, "default" or "remote_queue".
        :param snapshot_expiry_days: How long until the backup snapshot (before this release's upserts and deletes) of the Pubmed table exist in BQ.
        :param max_processes: Max number of parallel processors.
        """

        self.observatory_api_conn_id = observatory_api_conn_id

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
        self.main_table_id = f"{cloud_workspace.project_id}.{bq_dataset_id}.{bq_table_id}"
        self.bq_dataset_description = bq_dataset_description
        self.snapshot_expiry_days = snapshot_expiry_days

        # Metadata on how to pull out and transform the Pubmed data.
        self.entity_list = {
            "baseline": PubmedEntity(
                type="baseline",
                sub_key="PubmedArticle",
                set_key="PubmedArticleSet",
                pmid_location="MedlineCitation",
                table_name="upserts",
                table_description="""Pubmed's PubmedArticle table - Includes all the metadata associated with a journal article citation, 
                both the metadata to describe the published article, i.e. <MedlineCitation>, and additional metadata often pertaining to 
                the publication's history or processing at NLM, i.e. <PubMedData>.""",
            ),
            "upsert": PubmedEntity(
                type="upsert",
                sub_key="PubmedArticle",
                set_key="PubmedArticleSet",
                pmid_location="MedlineCitation",
                table_name="upserts",
                table_description="""PubmedArticle - Includes all the metadata associated with a journal article citation, both the
                metadata to describe the published article, i.e. <MedlineCitation>, and additional metadata often
                pertaining to the publication's history or processing at NLM, i.e. <PubMedData>.""",
            ),
            "delete": PubmedEntity(
                type="delete",
                sub_key="PMID",
                set_key="DeleteCitation",
                pmid_location=None,
                table_name="deletes",
                table_description="""DeleteCitation - Indicates one or more <PubmedArticle> or <PubmedBookArticle> that have been deleted. 
                PMIDs in DeleteCitation will typically have been found to be duplicate citations, or citations to content 
                that was determined to be out-of-scope for PubMed. It is possible that a PMID would appear in DeleteCitation 
                without having been distributed in a previous file. This would happen if the creation and deletion of the 
                record take place on the same day.""",
            ),
        }

        # Workflow specific parameters.
        self.ftp_server_url = ftp_server_url
        self.ftp_port = ftp_port
        self.baseline_path = "/pubmed/baseline/"
        self.updatefiles_path = "/pubmed/updatefiles/"
        self.max_download_retry = max_download_retry
        self.reset_ftp_counter = reset_ftp_counter
        self.num_merged_records = num_merged_records
        self.max_processes = max_processes

        # Wait for the previous DAG run to finish to make sure that
        # datafiles are processed in the correct order
        external_task_id = "dag_run_complete"
        self.add_operator(
            PreviousDagRunSensor(
                dag_id=self.dag_id,
                external_task_id=external_task_id,
                execution_delta=timedelta(days=7),  # To match the @weekly schedule_interval
            )
        )

        self.add_setup_task(self.check_dependencies)
        self.add_setup_task(self.list_datafiles_for_release)

        ### BASELINE ###

        # Download and process the baseline files - skipped if no new baseline files are available.
        self.add_task(self.download_baseline)
        self.add_task(self.upload_downloaded_baseline)
        self.add_task(self.transform_baseline)
        self.add_task(self.upload_transformed_baseline)
        self.add_task(self.bq_load_main_table)

        # Snapshot the table before making changes using the updatefiles.
        self.add_task(self.create_snapshot)

        ### UPDATEFILES ###

        # Download and pull out the upsert and delete records from the updatefiles.
        self.add_task(self.download_updatefiles)
        self.add_task(self.upload_downloaded_updatefiles)
        self.add_task(self.transform_updatefiles)

        # Merge and apply upsert records.
        self.add_task(self.merge_upsert_records)
        self.add_task(self.upload_merged_upsert_records)
        self.add_task(self.bq_load_upsert_table)
        self.add_task(self.bq_upsert_records)

        # Merge and apply delete records.
        self.add_task(self.merge_delete_records)
        self.add_task(self.upload_merged_delete_records)
        self.add_task(self.bq_load_delete_table)
        self.add_task(self.bq_delete_records)

        self.add_task(self.add_new_dataset_release)
        self.add_task(self.cleanup)

        # The last task that the next DAG run's ExternalTaskSensor waits for.
        self.add_operator(
            DummyOperator(
                task_id=external_task_id,
            )
        )

    def list_datafiles_for_release(self, **kwargs) -> bool:
        """
        Get a list of all files to process for this release.

        Determine if workflow needs to redownload the baseline files again because of a new yearly release
        """

        dag_run = kwargs["dag_run"]
        is_first_run = is_first_dag_run(dag_run)
        dataset_releases = get_dataset_releases(dag_id=self.dag_id, dataset_id=self.bq_dataset_id)
        prev_release = get_latest_dataset_release(dataset_releases, "changefile_end_date")

        # Open FTP connection to PubMed servers.
        ftp_conn = FTP()
        ftp_conn.connect(host=self.ftp_server_url, port=self.ftp_port)
        ftp_conn.login()  # anonymous login (publicly available data)

        # Change to the baseline directory.
        ftp_conn.cwd(self.baseline_path)

        # Get list of all baseline files.
        baseline_list_ftp = ftp_conn.nlst()

        # Get upload date of the first baseline file.
        baseline_list_ftp.sort()
        baseline_first_file = [file for file in baseline_list_ftp if file.endswith("0001.xml.gz")][0]
        baseline_upload = ftp_conn.sendcmd("MDTM {}".format(baseline_first_file))[4:]
        baseline_upload_date = pendulum.from_format(baseline_upload, "YYYYMMDDHHmmss")
        logging.info(f"baselinev upload time: {baseline_first_file}, {baseline_upload_date}")

        # Make workflow re-download the baseline yearly data if the upload date does not match from the last release.
        if is_first_run:
            year_first_run = True
        else:
            prev_release_extra = prev_release.extra
            logging.info(f"prev_release: {prev_release}")
            logging.info(f"pre_release_extra: {prev_release_extra}")
            last_baseline_upload_date = pendulum.from_timestamp(prev_release_extra["baseline_upload_date"])
            if last_baseline_upload_date == baseline_upload_date:
                year_first_run = False
            else:
                year_first_run = True

        # Grab list of baseline files from FTP server.
        files_to_download = []
        if year_first_run:
            logging.info(f"This is the first run for the year for Pubmed. Grabbing list of 'baseline' files.")

            # Grab metadata and path of the file.
            for file in baseline_list_ftp:
                if file.endswith(".xml.gz"):  # Find all the xml.gz files available from the server.
                    filename = file
                    file_index = int(re.findall("\d{4}", file)[0])
                    path_on_ftp = self.baseline_path + file
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

        ftp_conn = FTP()
        ftp_conn.connect(host=self.ftp_server_url, port=self.ftp_port)
        ftp_conn.login()  # anonymous login (publicly available data)

        # Change to updatefiles directory
        ftp_conn.cwd(self.updatefiles_path)

        # Find all the .xml.gz files available
        updatefiles_list = ftp_conn.nlst()
        updatefiles_xml_gz = [file for file in updatefiles_list if file.endswith(".xml.gz")]

        # Determine start date for the new data interval period for this run.
        release_interval_end = pendulum.instance(kwargs["execution_date"])
        logging.info(f"release_interval_end from workflow: {release_interval_end}")
        if is_first_run:
            assert (
                len(dataset_releases) == 0
            ), "fetch_releases: there should be no DatasetReleases stored in the Observatory API on the first DAG run."

            release_interval_start = baseline_upload_date

        elif year_first_run:
            assert (
                len(dataset_releases) >= 1
            ), f"fetch_releases: there should be at least 1 DatasetRelease in the Observatory API after the first DAG run"

            release_interval_start = baseline_upload_date

        else:
            assert (
                len(dataset_releases) >= 1
            ), f"fetch_releases: there should be at least 1 DatasetRelease in the Observatory API after the first DAG run"

            release_interval_start = pendulum.instance(prev_release.changefile_end_date)

        logging.info(
            f"Grabbing list of 'updatefiles' for this release: {release_interval_start} to {release_interval_end}"
        )

        for i, file in enumerate(updatefiles_xml_gz):
            # Every self.reset_ftp_counter number of files that are found, reinistialise the FTP connection.
            if i % self.reset_ftp_counter == 0 and i != 0:
                # Close exisiting FTP connection.
                ftp_conn.close()

                # Sleep for short time so that the server doesn't refuse the connection.
                time.sleep(random.randint(5, 10))

                # Open a new FTP connection
                ftp_conn = FTP()
                ftp_conn.connect(host=self.ftp_server_url, port=self.ftp_port)
                ftp_conn.login()  # anonymous login (publicly available data)
                ftp_conn.cwd(self.updatefiles_path)

                logging.info(
                    f"FTP connection to Pubmed's servers has been reset after {self.reset_ftp_counter} files to avoid issues."
                )
            # for file in updatefiles_xml_gz:
            # Only return list of updatefiles that are within the required release period.
            file_upload_ftp = ftp_conn.sendcmd("MDTM {}".format(file))[4:]
            file_upload_date = pendulum.from_format(file_upload_ftp, "YYYYMMDDHHmmss")
            if file_upload_date in pendulum.period(release_interval_start, release_interval_end):
                # Grab metadata and path of the file.
                filename = file
                file_index = int(re.findall("\d{4}", file)[0])
                path_on_ftp = self.updatefiles_path + file
                datafile_date = file_upload_date

                datafile = Datafile(
                    filename=filename,
                    file_index=file_index,
                    baseline=False,
                    path_on_ftp=path_on_ftp,
                    datafile_date=datafile_date,
                )
                files_to_download.append(datafile)

        assert (
            files_to_download
        ), f"List of files to download is empty. There should be at leaast 7 datafiles to download from Pubmed's FTP server."

        # Sort from oldest to newest using the file index
        files_to_download.sort(key=lambda c: c.file_index, reverse=False)

        # Check that all updatefiles pulled from the FTP server are not missing any between start_index and end_index.
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
            assert (
                files_to_download[0].file_index == prev_release.sequence_end + 1
            ), f"Last updatefile index is not n+1 from the previous release end. Latest release update index: {prev_release.sequence_end} vs current start: {files_to_download[0].file_index}"
        else:
            logging.info(
                f"First run of the Pubmed telescope for the year. No previous releases.file_index to check against."
            )

        logging.info(f"List of files to download from the PubMed FTP server for release:")
        for datafile in files_to_download:
            logging.info(f"Filename: {datafile.filename}, is a baseline file: {datafile.baseline}")

        # Close the connection to the FTP server.
        ftp_conn.close()

        # Push release data to Xcom for next step.
        release_metadata = {
            "files_to_download": [datafile.to_dict() for datafile in files_to_download],
            "release_interval_start": release_interval_start.timestamp(),
            "release_interval_end": release_interval_end.timestamp(),
            "baseline_upload_date": baseline_upload_date.timestamp(),
            "year_first_run": year_first_run,
        }
        ti: TaskInstance = kwargs["ti"]
        ti.xcom_push(key="release_metadata", value=release_metadata)

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

        ti: TaskInstance = kwargs["ti"]
        release_metadata = ti.xcom_pull(
            key="release_metadata", task_ids=self.list_datafiles_for_release.__name__, include_prior_dates=False
        )

        run_id = kwargs["run_id"]
        release = PubMedRelease(
            dag_id=self.dag_id,
            run_id=run_id,
            cloud_workspace=self.cloud_workspace,
            start_date=pendulum.from_timestamp(release_metadata["release_interval_start"]),
            end_date=pendulum.from_timestamp(release_metadata["release_interval_end"]),
            year_first_run=release_metadata["year_first_run"],
            datafile_list=[Datafile.from_dict(datafile) for datafile in release_metadata["files_to_download"]],
        )

        return release

    def download_baseline(self, release: PubMedRelease, **kwargs):
        """
        Download files from PubMed's FTP server for this release.

        Unable to do this in parallel because their FTP server is not able to handle too many requests.
        """

        if not release.year_first_run:
            logging.info("download_baseline: skipping as this is only run when there is a new baseline dump available.")
            return

        baseline_datafiles = [datafile for datafile in release.datafile_list if datafile.baseline]

        success = download_datafiles_from_ftp_server(
            datafile_list=baseline_datafiles,
            ftp_server_url=self.ftp_server_url,
            ftp_port=self.ftp_port,
            reset_ftp_counter=self.reset_ftp_counter,
            max_download_retry=self.max_download_retry,
        )

        set_task_state(success, kwargs["ti"].task_id, release=release)

    def upload_downloaded_baseline(self, release: PubMedRelease, **kwargs):
        """Upload downloaded baseline files to GCS."""

        if not release.year_first_run:
            logging.info(
                "upload_downloaded_baseline: skipping as this is only run when there is a new baseline dump available."
            )
            return

        # Grab list of files to upload.
        datafiles_to_upload = [datafile.download_file_path for datafile in release.datafile_list if datafile.baseline]

        success = gcs_upload_files(
            bucket_name=self.cloud_workspace.download_bucket,
            file_paths=datafiles_to_upload,
        )

        set_task_state(success, kwargs["ti"].task_id, release=release)

    def transform_baseline(self, release: PubMedRelease, **kwargs):
        """
        Transform the *.xml.gz files downloaded from PubMed into usable json files for BigQuery import.

        This is a multithreaded task that pulls the information from the files using the keys defined in self.entity_list.
        """

        if not release.year_first_run:
            logging.info(
                "transform_baseline: skipping as this is only run when there is a new baseline dump available."
            )
            return

        # Get list of datafiles for the baseline to trasform.
        datafiles_to_transform = [datafile for datafile in release.datafile_list if datafile.baseline]

        # Process files in batches so that ProcessPoolExecutor doesn't deplete the system of memory
        for i, chunk in enumerate(get_chunks(input_list=datafiles_to_transform, chunk_size=self.max_processes)):
            with ProcessPoolExecutor(max_workers=self.max_processes) as executor:
                logging.info(f"In chunk {i} and processing files: {chunk}")

                futures = [
                    executor.submit(transform_pubmed_xml_file_to_jsonl, datafile, [self.entity_list["baseline"]])
                    for datafile in chunk
                ]

                # Make sure that all datafiles have been properly transformed.
                for future in as_completed(futures):
                    transformed_datafile = future.result()

                    logging.info(f"Successfully transformed file - {transformed_datafile.filename}")

    def upload_transformed_baseline(self, release: PubMedRelease, **kwargs):
        """Upload transformed baseline files to GCS."""

        if not release.year_first_run:
            logging.info(
                "upload_transformed_baseline: skipping as this is only run when there is a new baseline dump available."
            )
            return

        files_to_upload = [
            datafile.transform_file_path("baseline") for datafile in release.datafile_list if datafile.baseline
        ]

        success = gcs_upload_files(
            bucket_name=self.cloud_workspace.transform_bucket,
            file_paths=files_to_upload,
        )

        set_task_state(success, kwargs["ti"].task_id, release=release)

    def bq_load_main_table(self, release: PubMedRelease, **kwargs):
        """Ingest the baseline table from GCS to BQ using a file pattern."""

        if not release.year_first_run:
            logging.info(
                "bq_load_main_table: skipping as this is only run when there is a new baseline dump available."
            )
            return

        # Create the dataset if not already present.
        bq_create_dataset(
            project_id=self.cloud_workspace.project_id,
            dataset_id=self.bq_dataset_id,
            location=self.cloud_workspace.data_location,
            description=self.bq_dataset_description,
        )

        baseline_transform_blob_pattern = release.transfer_blob_pattern("baseline")

        logging.info(
            f"Creating a load job for all of the baseline files with pattern: {baseline_transform_blob_pattern} "
        )

        success = bq_load_table(
            uri=baseline_transform_blob_pattern,
            table_id=self.main_table_id,
            source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
            schema_file_path=self.entity_list["baseline"].schema_file_path,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            table_description=self.entity_list["baseline"].table_description,
            partition=False,
            ignore_unknown_values=False,
        )

        set_task_state(success, kwargs["ti"].task_id, release=release)

    def create_snapshot(self, release: PubMedRelease, **kwargs):
        """Create a snapshot of main table as a backup just in case something happens with the below upserts and
        deletes."""

        snapshot_table_id = bq_sharded_table_id(
            self.cloud_workspace.project_id, self.bq_dataset_id, f"{self.bq_table_id}_backup", release.start_date
        )
        expiry_date = pendulum.now().add(days=self.snapshot_expiry_days)
        success = bq_snapshot(src_table_id=self.main_table_id, dst_table_id=snapshot_table_id, expiry_date=expiry_date)

        set_task_state(success, kwargs["ti"].task_id, release=release)

    def download_updatefiles(self, release: PubMedRelease, **kwargs):
        """
        Download the updatefiles from PubMed's FTP server for this release.

        Unable to do this in parallel due to limitations of their FTP server.
        """

        updatefiles_datafiles = [datafile for datafile in release.datafile_list if not datafile.baseline]

        success = download_datafiles_from_ftp_server(
            datafile_list=updatefiles_datafiles,
            ftp_server_url=self.ftp_server_url,
            ftp_port=self.ftp_port,
            reset_ftp_counter=self.reset_ftp_counter,
            max_download_retry=self.max_download_retry,
        )

        set_task_state(success, kwargs["ti"].task_id, release=release)

    def upload_downloaded_updatefiles(self, release: PubMedRelease, **kwargs):
        """Upload downloaded updatefiles files to GCS."""

        # Grab list of files to upload.
        datafiles_to_upload = [
            datafile.download_file_path for datafile in release.datafile_list if not datafile.baseline
        ]

        success = gcs_upload_files(
            bucket_name=self.cloud_workspace.download_bucket,
            file_paths=datafiles_to_upload,
        )

        set_task_state(success, kwargs["ti"].task_id, release=release)

    def transform_updatefiles(self, release: PubMedRelease, **kwargs):
        """
        Transform the *.xml.gz files downloaded from PubMed's FTP server into usable json-like files for BigQuery import.

        This is a multithreaded task that pulls the information from the files using the keys defined in self.entity_list.
        """

        # Get list of datafiles for the baseline to trasform.
        datafiles_to_transform = [datafile for datafile in release.datafile_list if not datafile.baseline]

        # Process files in batches so that ProcessPoolExecutor doesn't deplete the system of memory
        for i, chunk in enumerate(get_chunks(input_list=datafiles_to_transform, chunk_size=self.max_processes)):
            with ProcessPoolExecutor(max_workers=self.max_processes) as executor:
                logging.info(f"In chunk {i} and processing files: {chunk}")

                futures = [
                    executor.submit(
                        transform_pubmed_xml_file_to_jsonl,
                        datafile,
                        [self.entity_list["upsert"], self.entity_list["delete"]],
                    )
                    for datafile in chunk
                ]

                # Make sure that all datafiles have been properly transformed.
                for future in as_completed(futures):
                    transformed_datafile = future.result()

                    logging.info(f"Successfully transformed file - {transformed_datafile.filename}")

    ########## UPSERT RECORDS #############

    def merge_upsert_records(self, release: PubMedRelease, **kwargs):
        """
        Merge the upsert records that will be applied to the main table.

        Only keep the newest upsert record from the updatefiles.
        """

        upsert_files_to_merge = [datafile for datafile in release.datafile_list if not datafile.baseline]

        upsert_files = {}
        for datafile in upsert_files_to_merge:
            transform_file = datafile.transform_file_path(entity_type=self.entity_list["upsert"].type)

            with open(transform_file, "r") as f_in:
                incoming_data = [json.loads(line) for line in f_in]

            # Get list of 'primary keys' for each record.
            records = [str(record[self.entity_list["upsert"].pmid_location]["PMID"]) for record in incoming_data]

            set_new_records_to_check = set(records)
            for key_file_path, records_in_file in upsert_files.items():
                # Find the primary keys that are the same in the old updatefile.
                set_old = set(records_in_file)
                same_records = set_old.intersection(set_new_records_to_check)

                # Remove primary keys in the old upsert file set if it is present in a newer upsert file.
                set_old.difference_update(same_records)

                if same_records:
                    logging.info(
                        (
                            f"Found {len(same_records)} newer records in file: {transform_file.split('/')[-1]} vs {key_file_path.split('/')[-1]}."
                            f" {len(set_old)} records left in {key_file_path.split('/')[-1]}"
                        )
                    )

                # Relace with list of primary keys with old keys removed.
                upsert_files[key_file_path] = list(set_old)

            # Add newest updatefile to list of updatefiles and it's records.
            upsert_files[transform_file] = records

        merged_upsert_files = []
        first_file_index = upsert_files_to_merge[0].file_index
        last_file_index = upsert_files_to_merge[-1].file_index
        for file_count, (transform_file, records_to_write) in enumerate(upsert_files.items()):
            logging.info(f"Reading in file to pull out records - {transform_file}")
            with open(transform_file) as f_in:
                incoming_data = [json.loads(line) for line in f_in]

            # Turn list of records to a dictionary for faster lookup.
            records = {
                str(record[self.entity_list["upsert"].pmid_location]["PMID"]): record for record in incoming_data
            }

            merged_output_file = datafile.merged_transform_file_path(
                self.entity_list["upsert"].type, first_file_index, last_file_index, part_num=file_count + 1
            )

            logging.info(f"Writing {len(records_to_write)} records to file - {merged_output_file}")

            # Write to compressed *.jsonl.gz as this will be uploaded to GCS and imported into BQ.
            with gzip.open(merged_output_file, "w") as f_out:
                for record_key in records_to_write:
                    f_out.write(str.encode(json.dumps(records[record_key], cls=PubMedCustomEncoder) + "\n"))

            merged_upsert_files.append(merged_output_file)

        # Send list of merged upsert record files to the Xcom.
        ti: TaskInstance = kwargs["ti"]
        ti.xcom_push(key="merged_upsert_files", value=merged_upsert_files)

    def upload_merged_upsert_records(self, release: PubMedRelease, **kwargs):
        """Upload the merged upsert records to GCS."""

        ti: TaskInstance = kwargs["ti"]
        merged_upsert_files = ti.xcom_pull(key="merged_upsert_files")

        success = gcs_upload_files(
            bucket_name=self.cloud_workspace.transform_bucket,
            file_paths=merged_upsert_files,
        )

        set_task_state(success, kwargs["ti"].task_id, release=release)

    def bq_load_upsert_table(self, release: PubMedRelease, **kwargs):
        """Ingest the upsert records from GCS to BQ using a file pattern."""

        upsert_table_id = bq_sharded_table_id(
            self.cloud_workspace.project_id,
            self.bq_dataset_id,
            table_name=self.entity_list["upsert"].type,
            date=release.end_date,
        )
        merged_transform_blob_pattern = release.merged_transfer_blob_pattern(
            entity_type=self.entity_list["upsert"].type
        )

        logging.info(f"Uploading to table - {upsert_table_id}")

        success = bq_load_table(
            uri=merged_transform_blob_pattern,
            table_id=upsert_table_id,
            source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
            schema_file_path=self.entity_list["upsert"].schema_file_path,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            table_description=self.entity_list["upsert"].table_description,
            partition=False,
            ignore_unknown_values=False,
        )

        assert success, f"Unable to tranfer to table - {upsert_table_id}"

        expiry_date = pendulum.now().add(days=7)
        bq_update_table_expiration(upsert_table_id, expiration_date=expiry_date)

        ti: TaskInstance = kwargs["ti"]
        ti.xcom_push(key="upsert_table_id", value=upsert_table_id)

    def bq_upsert_records(self, release: PubMedRelease, **kwargs):
        """
        Upsert records into the main table.

        Has to match on both the PMID value and the Version number, as there could be multiple different versions in
        the main table.
        """

        ti: TaskInstance = kwargs["ti"]
        upsert_table_id = ti.xcom_pull(key="upsert_table_id")

        keys_to_match_on = ["MedlineCitation.PMID.value", "MedlineCitation.PMID.Version"]

        bq_upsert_records(
            main_table_id=self.main_table_id,
            upsert_table_id=upsert_table_id,
            primary_key=keys_to_match_on,
        )

    ########## DELETE RECORDS ##########

    def merge_delete_records(self, release: PubMedRelease, **kwargs):
        """
        Merge the delete records that will be applied to the main table.

        Remove duplicates and write file.
        """

        delete_files_to_merge = [datafile for datafile in release.datafile_list if not datafile.baseline]

        merged = []
        first_file_index = delete_files_to_merge[0].file_index
        for datafile in delete_files_to_merge:
            transform_file = datafile.transform_file_path(entity_type=self.entity_list["delete"].type)

            logging.info(f"Reading in file - {transform_file}")

            # Read in delete file
            with open(transform_file, "r") as f_in:
                incoming_data = [json.loads(line) for line in f_in]

            merged.extend(incoming_data)

        # Remove duplicate records
        merged_unique = [dict(tup) for tup in {tuple(delete.items()) for delete in merged}]
        logging.info(f"{len(merged)} in merged but number of unique entries are: {len(merged_unique)}")

        # Write out to file with num_merged_records in each file.
        merged_delete_files = []
        for i, chunk in enumerate(get_chunks(input_list=merged_unique, chunk_size=self.num_merged_records)):
            merged_output_file = datafile.merged_transform_file_path(
                self.entity_list["delete"].type, first_file_index, datafile.file_index, part_num=i + 1
            )

            logging.info(f"Writing to file - {merged_output_file}")

            # Write to compressed *.jsonl.gz as this will be uploaded to GCS and imported into BQ.
            with gzip.open(merged_output_file, "w") as f_out:
                for line in chunk:
                    f_out.write(str.encode(json.dumps(line, cls=PubMedCustomEncoder) + "\n"))

            merged_delete_files.append(merged_output_file)

        # Send list of merged delete record files to the Xcom.
        ti: TaskInstance = kwargs["ti"]
        ti.xcom_push(key="merged_delete_files", value=merged_delete_files)

    def upload_merged_delete_records(self, release: PubMedRelease, **kwargs):
        """Upload the merged delete records to GCS."""

        ti: TaskInstance = kwargs["ti"]
        merged_delete_files = ti.xcom_pull(key="merged_delete_files")

        success = gcs_upload_files(
            bucket_name=self.cloud_workspace.transform_bucket,
            file_paths=merged_delete_files,
        )

        set_task_state(success, kwargs["ti"].task_id, release=release)

    def bq_load_delete_table(self, release: PubMedRelease, **kwargs):
        """Ingest delete records from GCS to BQ."""

        delete_table_id = bq_sharded_table_id(
            self.cloud_workspace.project_id,
            self.bq_dataset_id,
            table_name=self.entity_list["delete"].type,
            date=release.end_date,
        )
        merged_transform_blob_pattern = release.merged_transfer_blob_pattern(
            entity_type=self.entity_list["delete"].type
        )

        logging.info(f"Uploading to table - {delete_table_id}")

        success = bq_load_table(
            uri=merged_transform_blob_pattern,
            table_id=delete_table_id,
            source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
            schema_file_path=self.entity_list["delete"].schema_file_path,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            table_description=self.entity_list["delete"].table_description,
            partition=False,
            ignore_unknown_values=False,
        )

        assert success, f"Unable to tranfer to table - {delete_table_id}"

        expiry_date = pendulum.now().add(days=7)
        bq_update_table_expiration(delete_table_id, expiration_date=expiry_date)

        # Send list of merged delete record files to the Xcom.
        ti: TaskInstance = kwargs["ti"]
        ti.xcom_push(key="delete_table_id", value=delete_table_id)

    def bq_delete_records(self, release: PubMedRelease, **kwargs):
        """
        Removed records from the main table that are specified in delete table.

        Has to match on both the PMID value and the Version number, as there could be multiple different versions in
        the main table.
        """

        # Pull list of update tables from xcom
        ti: TaskInstance = kwargs["ti"]
        delete_table_id = ti.xcom_pull(key="delete_table_id")
        main_table_keys_to_match_on = ["MedlineCitation.PMID.value", "MedlineCitation.PMID.Version"]
        delete_table_keys_to_match_on = ["value", "Version"]

        bq_delete_records(
            main_table_id=self.main_table_id,
            delete_table_id=delete_table_id,
            main_table_primary_key=main_table_keys_to_match_on,
            delete_table_primary_key=delete_table_keys_to_match_on,
        )

    def add_new_dataset_release(self, release: PubMedRelease, **kwargs):
        """Adds release information to the API."""

        ti: TaskInstance = kwargs["ti"]
        release_metadata = ti.xcom_pull(
            key="release_metadata", task_ids=self.list_datafiles_for_release.__name__, include_prior_dates=False
        )

        logging.info(f"add_new_dataset_releases: creating dataset release for Pubmed Articles.")
        dataset_release = DatasetRelease(
            dag_id=self.dag_id,
            dataset_id=self.bq_dataset_id,
            dag_run_id=release.run_id,
            data_interval_start=release.start_date,
            data_interval_end=release.end_date,
            sequence_start=release.datafile_list[0].file_index,
            sequence_end=release.datafile_list[-1].file_index,
            changefile_start_date=release.start_date,
            changefile_end_date=release.end_date,
            extra={"baseline_upload_date": release_metadata["baseline_upload_date"]},
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


def download_datafiles_from_ftp_server(
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

    :return download_success: If downloading all of the datafiles were successful.
    """

    # Open FTP connection
    ftp_conn = FTP()
    ftp_conn.connect(host=ftp_server_url, port=ftp_port)
    ftp_conn.login()  # Anonymous login (publicly available data)

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
            ftp_conn = FTP()
            ftp_conn.connect(host=ftp_server_url, port=ftp_port)
            ftp_conn.login()  # anonymous login (publicly available data)

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


def transform_pubmed_xml_file_to_jsonl(datafile: Datafile, entity_list: List[PubmedEntity]) -> Union[Datafile, bool]:
    """
    Convert a single Pubmed XML file to JSONL, pulling out any of the Pubmed entities and their upserts and or deletes.
    Used in parallelised transform section.

    :param datafile: Incoming datafile to transform.
    :param entity_list: List of entities to pull out from the datafile.
    :return: Datafile that's been transformed.
    """

    logging.info(f"Reading in file - {datafile.filename}")

    with gzip.open(datafile.download_file_path, "rb") as f_in:
        # Use the BioPython package for reading in the Pubmed XML files.
        # This package also checks against it's own DTD schema defined in the XML header.
        try:
            data_dirty1 = Entrez.read(f_in, validate=True)
        except ValidationError:
            logging.info(f"Fields in XML are not valid against it's own DTD file - {datafile.filename}")
            return False

        # Need pull out XML attributes from the Biopython data classes.
        data_dirty2 = add_attributes_to_data_from_biopython_classes(data_dirty1)
        del data_dirty1

        #  Remove unwanted nested list structure from the Pubmed dictionary.
        data = change_pubmed_list_structure(data_dirty2)
        del data_dirty2

    for entity in entity_list:
        try:
            try:
                data_part = [retrieve for retrieve in data[entity.set_key][entity.sub_key]]
            except KeyError:
                data_part = [retrieve for retrieve in data[entity.sub_key]]

            logging.info(
                f"Pulled out {len(data_part)} {f'{entity.sub_key} {entity.type}'} from file - {datafile.filename}"
            )

        except KeyError:
            logging.info(f"No {f'{entity.sub_key} {entity.type}'} records in file - {datafile.filename}")
            data_part = []

        output_file = datafile.transform_file_path(entity.type)

        logging.info(f"Writing {entity.type} {entity.type} to file - {output_file}")

        # Baseline files are not read in later, can be dumped straight to compressed file for import to GCS and BQ.
        if datafile.baseline:
            with gzip.open(output_file, "w") as f_out:
                for line in data_part:
                    f_out.write(str.encode(json.dumps(line, cls=PubMedCustomEncoder) + "\n"))
        else:
            with open(output_file, "w") as f_out:
                for line in data_part:
                    f_out.write(json.dumps(line, cls=PubMedCustomEncoder) + "\n")

    return datafile


def add_attributes_to_data_from_biopython_classes(
    obj: Union[StringElement, DictionaryElement, ListElement, OrderedListElement, list]
):
    """
    Recursively travel down the Pubmed data tree to add attributes from Biopython classes as key-value pairs.

    Only pulling data from StringElements, DictionaryElements, ListElements and OrderedListElements.

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


def bq_update_table_expiration(full_table_id: str, expiration_date: pendulum.datetime):
    """
    Update a Bigquery table expiration date.

    :param full_table_id: Full qualified table id.
    :param expiration_date: Expiration date of the table.

    :return: None.
    """

    client = bigquery.Client()
    table = client.get_table(full_table_id)

    # Update the expiration time
    table.expires = expiration_date

    # Update the table metadata
    client.update_table(table, ["expires"])
