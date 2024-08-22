from __future__ import annotations

import os
from typing import List

import pendulum

from academic_observatory_workflows.pubmed_telescope.datafile import Datafile
from observatory_platform.google.gcs import gcs_blob_name_from_path
from observatory_platform.airflow.workflow import CloudWorkspace
from observatory_platform.airflow.release import ChangefileRelease as DatafileRelease


class PubMedRelease(DatafileRelease):
    def __init__(
        self,
        *,
        dag_id: str,
        run_id: str,
        cloud_workspace: CloudWorkspace,
        bq_dataset_id: str,
        start_date: pendulum.DateTime,
        end_date: pendulum.DateTime,
        year_first_run: bool,
        datafile_list: List[Datafile],
        baseline_upload_date: pendulum.DateTime,
    ):
        """
        Construct a PubmedRelease.

        :param dag_id: the DAG id.
        :param run_id: Run id of this workflow.
        :param cloud_workspace: Holds cloud location details.
        :param bq_dataset_id: The ID of the working bigquery dataset
        :param start_date: Start date of the release.
        :param end_date: End date of the release.
        :param year_first_run: True if it's the first run of the workflow for the year, if this
        release is to download the baseline files of Pubmed.
        :param datafile_list: List of datafiles for this release.
        :param baseline_upload_date: the modification date of the first file in the baseline upload, retrieved via FTP with the MDTM command.
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
        self.bq_dataset_id = bq_dataset_id
        self.year_first_run = year_first_run
        self.datafile_list = datafile_list
        self.baseline_upload_date = baseline_upload_date

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

    def schema_file_path(self, record_type: str) -> str:
        return project_path("pubmed_telescope", "schema", f"{record_type}.json")

    def transfer_blob_pattern(self, table_type: str) -> str:
        """
        Create a blob pattern for importing the transformed unmerged records from GCS into Bigquery.

        :param table_type: Type of the record.
        :return: Uri pattern for transformed files.
        """

        return f"gs://{self.cloud_workspace.transform_bucket}/{gcs_blob_name_from_path(self.datafile_release.transform_folder)}/{table_type}_*.{self.datafile_list[0].file_type}"

    @property
    def baseline_files(self) -> List[Datafile]:
        """Return a list of the "baseline" datafiles files for this release."""
        return [datafile for datafile in self.datafile_list if datafile.baseline]

    @property
    def updatefiles(self) -> List[Datafile]:
        """Return a list of "updatefile" datafiles for this release."""
        return [datafile for datafile in self.datafile_list if not datafile.baseline]

    @property
    def merged_upsert_uri_blob_pattern(self) -> str:
        """
        Create a uri blob pattern for importing the transformed merged upserts from GCS into Bigquery.

        :return: Uri pattern for merged transform files.
        """

        return f"gs://{self.cloud_workspace.transform_bucket}/{gcs_blob_name_from_path(self.datafile_release.transform_folder)}/upsert_merged_*.{self.datafile_list[0].file_type}"

    @property
    def merged_delete_transfer_uri(self) -> str:
        """
        Create a uri for importing the transformed merged deletes from GCS into Bigquery.

        :return: uri for merged transform files.
        """

        return f"gs://{self.cloud_workspace.transform_bucket}/{gcs_blob_name_from_path(self.datafile_release.transform_folder)}/delete_merged.{self.datafile_list[0].file_type}"

    @property
    def merged_delete_file_path(self):
        # This is just a singular file, not multiple part files from each updatefile.
        assert self.datafile_release is not None, "release.merged_delete_file_path: self.datafile_release is None"
        return os.path.join(
            self.datafile_release.transform_folder,
            f"delete_merged.{self.datafile_list[0].file_type}",
        )

    @staticmethod
    def from_dict(dict_: dict) -> PubMedRelease:
        return PubMedRelease(
            dag_id=dict_["dag_id"],
            run_id=dict_["run_id"],
            cloud_workspace=CloudWorkspace.from_dict(dict_["cloud_workspace"]),
            start_date=pendulum.from_timestamp(dict_["start_date"]),
            end_date=pendulum.from_timestamp(dict_["end_date"]),
            year_first_run=dict_["year_first_run"],
            datafile_list=[Datafile.from_dict(datafile) for datafile in dict_["datafile_list"]],
            baseline_upload_date=pendulum.from_timestamp(dict_["baseline_upload_date"]),
        )

    def to_dict(self) -> dict:
        return dict(
            dag_id=self.dag_id,
            run_id=self.run_id,
            cloud_workspace=self.cloud_workspace.to_dict(),
            start_date=self.start_date.timestamp(),
            end_date=self.end_date.timestamp(),
            year_first_run=self.year_first_run,
            datafile_list=[datafile.to_dict() for datafile in self.datafile_list],
            baseline_upload_date=self.baseline_upload_date.timestamp(),
        )
