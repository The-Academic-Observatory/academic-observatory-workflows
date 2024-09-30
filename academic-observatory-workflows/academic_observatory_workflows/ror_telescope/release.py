from __future__ import annotations

import os
from typing import Dict

import pendulum

from observatory_platform.google.gcs import gcs_blob_name_from_path, gcs_blob_uri
from observatory_platform.airflow.workflow import CloudWorkspace
from observatory_platform.airflow.release import SnapshotRelease


class RorRelease(SnapshotRelease):
    def __init__(
        self,
        *,
        dag_id: str,
        run_id: str,
        snapshot_date: pendulum.DateTime,
        url: str,
        checksum: str,
        cloud_workspace: CloudWorkspace,
        data_interval_start: pendulum.Datetime,
        data_interval_end: pendulum.Datetime,
    ):
        """Construct a RorRelease.

        :param dag_id: the DAG id.
        :param run_id: the DAG id.
        :param snapshot_date: the release date.
        :param url: The url to the ror snapshot.
        :param checksum: the file checksum.
        :param cloud_workspace: the cloud workspace settings.
        :param data_interval_start: The beginning of this release's data interval
        :param data_interval_end: The end of this release's data interval
        """

        super().__init__(dag_id=dag_id, run_id=run_id, snapshot_date=snapshot_date)
        self.url = url
        self.checksum = checksum
        self.download_file_name = "ror.zip"
        self.extract_file_regex = r"^\S+-ror-data\.json$"
        self.transform_file_name = "ror.jsonl.gz"
        self.download_file_path = os.path.join(self.download_folder, self.download_file_name)
        self.transform_file_path = os.path.join(self.transform_folder, self.transform_file_name)
        self.cloud_workspace = cloud_workspace
        self.data_interval_start = data_interval_start
        self.data_interval_end = data_interval_end

    @property
    def download_blob_name(self):
        return gcs_blob_name_from_path(self.download_file_path)

    @property
    def transform_blob_name(self):
        return gcs_blob_name_from_path(self.transform_file_path)

    @property
    def download_uri(self):
        return gcs_blob_uri(self.cloud_workspace.download_bucket, self.download_blob_name)

    @property
    def transform_uri(self):
        return gcs_blob_uri(self.cloud_workspace.transform_bucket, self.transform_blob_name)

    def to_dict(self) -> Dict:
        return dict(
            dag_id=self.dag_id,
            run_id=self.run_id,
            snapshot_date=self.snapshot_date.to_date_string(),
            url=self.url,
            checksum=self.checksum,
            cloud_workspace=self.cloud_workspace.to_dict(),
        )

    @staticmethod
    def from_dict(dict_: Dict) -> RorRelease:
        dag_id = dict_["dag_id"]
        snapshot_date = dict_["snapshot_date"]
        url = dict_["url"]
        checksum = dict_["checksum"]
        cloud_workspace = dict_["cloud_workspace"]
        data_interval_start = dict_["data_interval_start"]
        data_interval_end = dict_["data_interval_end"]

        return RorRelease(
            dag_id=dag_id,
            run_id=dict_["run_id"],
            snapshot_date=pendulum.parse(snapshot_date),
            url=url,
            checksum=checksum,
            cloud_workspace=CloudWorkspace.from_dict(cloud_workspace),
            data_interval_start=data_interval_start,
            data_interval_end=data_interval_end,
        )
