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
        cloud_workspace: CloudWorkspace,
        batch_size: int,
    ):
        """Construct a RorRelease.

        :param dag_id: the DAG id.
        :param run_id: the DAG run id.
        :param snapshot_date: the release date.
        :param cloud_workspace: the cloud workspace settings.
        :param batch_size: the number of files to send to ProcessPoolExecutor at one time.
        """

        super().__init__(dag_id=dag_id, run_id=run_id, snapshot_date=snapshot_date)
        self.cloud_workspace = cloud_workspace
        self.batch_size = batch_size
        self.download_file_name = "crossref_metadata.json.tar.gz"
        self.download_file_path = os.path.join(self.download_folder, self.download_file_name)
        self.extract_files_regex = r".*\.json$"
        self.transform_files_regex = r".*\.jsonl$"

    @staticmethod
    def from_dict(dict_: dict):
        dag_id = dict_["dag_id"]
        run_id = dict_["run_id"]
        snapshot_date = pendulum.parse(dict_["snapshot_date"])
        cloud_workspace = CloudWorkspace.from_dict(dict_["cloud_workspace"])
        batch_size = dict_["batch_size"]
        return CrossrefMetadataRelease(
            dag_id=dag_id,
            run_id=run_id,
            snapshot_date=snapshot_date,
            cloud_workspace=cloud_workspace,
            batch_size=batch_size,
        )

    def to_dict(self) -> dict:
        return dict(
            dag_id=self.dag_id,
            run_id=self.run_id,
            snapshot_date=self.snapshot_date.to_datetime_string(),
            cloud_workspace=self.cloud_workspace.to_dict(),
            batch_size=self.batch_size,
        )
