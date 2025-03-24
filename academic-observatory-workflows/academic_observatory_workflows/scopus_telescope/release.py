import os

import pendulum

from observatory_platform.airflow.release import SnapshotRelease
from observatory_platform.google.gcs import gcs_blob_name_from_path
from observatory_platform.airflow.workflow import CloudWorkspace


class ScopusRelease(SnapshotRelease):
    def __init__(
        self,
        *,
        dag_id: str,
        cloud_workspace: CloudWorkspace,
        run_id: str,
        snapshot_date: pendulum.DateTime,
        data_interval_end: pendulum.DateTime,
        data_interval_start: pendulum.DateTime,
    ):
        """Construct a WebOfScienceRelease instance.

        :param dag_id: The DAG ID.
        :param cloud_workspace: The CloudWorkspace object
        :param run_id: The DAG run ID.
        :param snapshot_date: Release date.
        :param data_interval_end: The end of the data interval for this release
        :param data_interval_start: The start of the data interval for this release
        """

        super().__init__(
            dag_id=dag_id,
            run_id=run_id,
            snapshot_date=snapshot_date,
        )
        self.cloud_workspace = cloud_workspace
        self.data_interval_end = data_interval_end
        self.data_interval_start = data_interval_start
        self.download_file_regex = r".*\.json"
        self.transform_file_name = "scopus.jsonl.gz"
        self.transform_file_path = os.path.join(self.transform_folder, self.transform_file_name)

    @property
    def transform_blob_name(self):
        return gcs_blob_name_from_path(self.transform_file_path)

    @staticmethod
    def from_dict(dict_: dict):
        dag_id = dict_["dag_id"]
        cloud_workspace = CloudWorkspace.from_dict(dict_["cloud_workspace"])
        run_id = dict_["run_id"]
        snapshot_date = pendulum.parse(dict_["snapshot_date"])
        data_interval_end = pendulum.parse(dict_["data_interval_end"])
        data_interval_start = pendulum.parse(dict_["data_interval_start"])
        return ScopusRelease(
            dag_id=dag_id,
            cloud_workspace=cloud_workspace,
            run_id=run_id,
            snapshot_date=snapshot_date,
            data_interval_end=data_interval_end,
            data_interval_start=data_interval_start,
        )

    def to_dict(self) -> dict:
        return dict(
            dag_id=self.dag_id,
            cloud_workspace=self.cloud_workspace.to_dict(),
            run_id=self.run_id,
            snapshot_date=self.snapshot_date.to_datetime_string(),
            data_interval_end=self.data_interval_end.to_datetime_string(),
            data_interval_start=self.data_interval_start.to_datetime_string(),
        )
