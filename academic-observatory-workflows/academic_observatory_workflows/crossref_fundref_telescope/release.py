from __future__ import annotations

import os
from typing import Dict

import pendulum

from observatory_platform.google.gcs import gcs_blob_name_from_path, gcs_blob_uri
from observatory_platform.airflow.workflow import CloudWorkspace
from observatory_platform.airflow.workflow import SnapshotRelease


class CrossrefFundrefRelease(SnapshotRelease):
    def __init__(
        self,
        *,
        dag_id: str,
        run_id: str,
        snapshot_date: pendulum.DateTime,
        url: str,
        cloud_workspace: CloudWorkspace,
    ):
        """Construct a RorRelease.

        :param dag_id: the DAG id.
        :param run_id: the DAG run id.
        :param snapshot_date: the release date.
        :param url: The url corresponding with this release date.
        :param cloud_workspace: the cloud workspace settings.
        """

        super().__init__(dag_id=dag_id, run_id=run_id, snapshot_date=snapshot_date)
        self.url = url
        self.download_file_name = "crossref_fundref.tar.gz"
        self.extract_file_name = "crossref_fundref.rdf"
        self.transform_file_name = "crossref_fundref.jsonl.gz"
        self.cloud_workspace = cloud_workspace

    @property
    def download_file_path(self):
        return os.path.join(self.download_folder, self.download_file_name)

    @property
    def extract_file_path(self):
        return os.path.join(self.extract_folder, self.extract_file_name)

    @property
    def transform_file_path(self):
        return os.path.join(self.transform_folder, self.transform_file_name)

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
            cloud_workspace=self.cloud_workspace.to_dict(),
        )

    @staticmethod
    def from_dict(dict_: Dict) -> CrossrefFundrefRelease:
        return CrossrefFundrefRelease(
            dag_id=dict_["dag_id"],
            run_id=dict_["run_id"],
            snapshot_date=pendulum.parse(dict_["snapshot_date"]),
            url=dict_["url"],
            cloud_workspace=CloudWorkspace.from_dict(dict_["cloud_workspace"]),
        )
