import os
import pendulum
from observatory_platform.airflow.release import SnapshotRelease
from observatory_platform.airflow.workflow import CloudWorkspace


class CrossrefMetadataRelease(SnapshotRelease):
    def __init__(
        self,
        *,
        dag_id: str,
        run_id: str,
        snapshot_date: pendulum.DateTime,
        data_interval_start: pendulum.DateTime,
        data_interval_end: pendulum.DateTime,
        cloud_workspace: CloudWorkspace,
    ):
        """Construct a Crossref Metadata Release Object.

        :param dag_id: the DAG id.
        :param run_id: the DAG run id.
        :param data_interval_start: The start of the data interval
        :param data_interval_end: The end of the data interval
        :param snapshot_date: the release date.
        :param cloud_workspace: the cloud workspace settings.
        """

        super().__init__(dag_id=dag_id, run_id=run_id, snapshot_date=snapshot_date)
        self.cloud_workspace = cloud_workspace
        self.download_file_name = "crossref_metadata.json.tar.gz"
        self.extract_files_regex = r".*\.json$"
        self.transform_files_regex = r".*\.jsonl$"
        self.data_interval_start = data_interval_start
        self.data_interval_end = data_interval_end

    @property
    def download_file_path(self):
        return os.path.join(self.download_folder, self.download_file_name)

    @staticmethod
    def from_dict(dict_: dict):
        dag_id = dict_["dag_id"]
        run_id = dict_["run_id"]
        snapshot_date = pendulum.parse(dict_["snapshot_date"])
        data_interval_start = pendulum.parse(dict_["data_interval_start"])
        data_interval_end = pendulum.parse(dict_["data_interval_end"])
        cloud_workspace = CloudWorkspace.from_dict(dict_["cloud_workspace"])
        return CrossrefMetadataRelease(
            dag_id=dag_id,
            run_id=run_id,
            snapshot_date=snapshot_date,
            data_interval_start=data_interval_start,
            data_interval_end=data_interval_end,
            cloud_workspace=cloud_workspace,
        )

    def to_dict(self) -> dict:
        return dict(
            dag_id=self.dag_id,
            run_id=self.run_id,
            snapshot_date=self.snapshot_date.to_datetime_string(),
            data_interval_start=self.data_interval_start.to_datetime_string(),
            data_interval_end=self.data_interval_end.to_datetime_string(),
            cloud_workspace=self.cloud_workspace.to_dict(),
        )
