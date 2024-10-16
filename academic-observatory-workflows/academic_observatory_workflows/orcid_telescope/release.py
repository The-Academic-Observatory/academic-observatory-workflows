import itertools
import os
from typing import Dict, List

import pendulum

from academic_observatory_workflows.orcid_telescope.batch import OrcidBatch, ORCID_RECORD_REGEX
from observatory_platform.google.gcs import (
    gcs_blob_name_from_path,
    gcs_blob_uri,
)
from observatory_platform.airflow.release import ChangefileRelease
from observatory_platform.airflow.workflow import CloudWorkspace
from observatory_platform.google import bigquery as bq
from observatory_platform.files import list_files


class OrcidRelease(ChangefileRelease):
    def __init__(
        self,
        *,
        dag_id: str,
        run_id: str,
        cloud_workspace: CloudWorkspace,
        bq_dataset_id: str,
        bq_main_table_name: str,
        bq_upsert_table_name: str,
        bq_delete_table_name: str,
        start_date: pendulum.DateTime,
        end_date: pendulum.DateTime,
        prev_release_end: pendulum.DateTime,
        prev_latest_modified_record: pendulum.DateTime,
        is_first_run: bool,
    ):
        """Construct a Orcid Release instance

        :param dag_id: DAG ID.
        :param run_id: DAG run ID.
        :param cloud_workspace: Cloud workspace object for this release.
        :param bq_dataset_id: BigQuery dataset ID.
        :param bq_main_table_name: BigQuery main table name for the ORCID table.
        :param bq_upsert_table_name: BigQuery table name for the ORCID upsert table.
        :param bq_delete_table_name: BigQuery table name for the ORCID delete table.
        :param start_date: Start date for the release.
        :param end_date: End date for the release.
        :param prev_release_end: End date for the previous release. Used for making the snapshot table date.
        :param prev_latest_modified_record: Latest modified record for the previous release. Used to decide which records to update.
        :param is_first_run: Whether this is the first run of the DAG.
        """
        super().__init__(
            dag_id=dag_id,
            run_id=run_id,
            start_date=start_date,
            end_date=end_date,
        )
        self.cloud_workspace = cloud_workspace
        self.bq_dataset_id = bq_dataset_id
        self.bq_main_table_name = bq_main_table_name
        self.bq_upsert_table_name = bq_upsert_table_name
        self.bq_delete_table_name = bq_delete_table_name
        self.prev_release_end = prev_release_end
        self.prev_latest_modified_record = prev_latest_modified_record
        self.is_first_run = is_first_run

        # Files/folders
        self.master_manifest_file = os.path.join(self.release_folder, "manifest.csv")

        # Table names and URIs
        self.upsert_blob_glob = f"{gcs_blob_name_from_path(self.transform_folder)}/*_upsert.jsonl.gz"
        self.upsert_table_uri = gcs_blob_uri(self.cloud_workspace.transform_bucket, self.upsert_blob_glob)
        self.delete_blob_glob = f"{gcs_blob_name_from_path(self.transform_folder)}/*_delete.jsonl.gz"
        self.delete_table_uri = gcs_blob_uri(self.cloud_workspace.transform_bucket, self.delete_blob_glob)
        self.bq_main_table_id = bq.bq_table_id(cloud_workspace.project_id, bq_dataset_id, bq_main_table_name)
        self.bq_upsert_table_id = bq.bq_table_id(cloud_workspace.project_id, bq_dataset_id, bq_upsert_table_name)
        self.bq_delete_table_id = bq.bq_table_id(cloud_workspace.project_id, bq_dataset_id, bq_delete_table_name)
        self.bq_snapshot_table_id = bq.bq_sharded_table_id(
            cloud_workspace.project_id, bq_dataset_id, f"{bq_main_table_name}_snapshot", prev_release_end
        )

    @property
    def upsert_files(self):  # The 'upsert' files in the transform_folder
        return list_files(self.transform_folder, r"^.*\d{2}(\d|X)_upsert\.jsonl\.gz$")

    @property
    def delete_files(self):  # The 'delete' files in the transform_folder
        return list_files(self.transform_folder, r"^.*\d{2}(\d|X)_delete\.jsonl\.gz$")

    @property
    def downloaded_records(self):  # Every downloaded record
        return list_files(self.download_folder, ORCID_RECORD_REGEX)

    @property
    def orcid_directory_paths(self) -> List[str]:
        """Generates the paths to the orcid directories in the download folder"""
        return [os.path.join(self.download_folder, folder) for folder in orcid_batch_names()]

    def orcid_batches(self) -> List[OrcidBatch]:
        """Creates the orcid directories in the download folder if they don't exist and returns them"""
        return [OrcidBatch(self.download_folder, self.transform_folder, batch) for batch in orcid_batch_names()]

    def to_dict(self) -> Dict:
        return dict(
            dag_id=self.dag_id,
            run_id=self.run_id,
            cloud_workspace=self.cloud_workspace.to_dict(),
            bq_dataset_id=self.bq_dataset_id,
            bq_main_table_name=self.bq_main_table_name,
            bq_upsert_table_name=self.bq_upsert_table_name,
            bq_delete_table_name=self.bq_delete_table_name,
            start_date=self.start_date.timestamp(),
            end_date=self.end_date.timestamp(),
            prev_release_end=self.prev_release_end.timestamp(),
            prev_latest_modified_record=self.prev_latest_modified_record.timestamp(),
            is_first_run=self.is_first_run,
        )

    @staticmethod
    def from_dict(dict_: Dict):
        return OrcidRelease(
            dag_id=dict_["dag_id"],
            run_id=dict_["run_id"],
            cloud_workspace=CloudWorkspace.from_dict(dict_["cloud_workspace"]),
            bq_dataset_id=dict_["bq_dataset_id"],
            bq_main_table_name=dict_["bq_main_table_name"],
            bq_upsert_table_name=dict_["bq_upsert_table_name"],
            bq_delete_table_name=dict_["bq_delete_table_name"],
            start_date=pendulum.from_timestamp(dict_["start_date"]),
            end_date=pendulum.from_timestamp(dict_["end_date"]),
            prev_release_end=pendulum.from_timestamp(dict_["prev_release_end"]),
            prev_latest_modified_record=pendulum.from_timestamp(dict_["prev_latest_modified_record"]),
            is_first_run=dict_["is_first_run"],
        )


def orcid_batch_names() -> List[str]:
    """Create a list of all the possible ORCID directories

    :return: A list of all the possible ORCID directories
    """
    n_1_2 = [str(i) for i in range(10)]  # 0, 1, 2, 3, 4, 5, 6, 7, 8, 9
    n_3 = n_1_2.copy() + ["X"]  # 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, X
    combinations = list(itertools.product(n_1_2, n_1_2, n_3))  # Creates the 000 to 99X directory structure
    return ["".join(i) for i in combinations]
