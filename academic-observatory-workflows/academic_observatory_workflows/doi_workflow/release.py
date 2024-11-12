import pendulum

from observatory_platform.airflow.release import SnapshotRelease


class DOIRelease(SnapshotRelease):
    def __init__(
        self,
        *,
        dag_id: str,
        run_id: str,
        snapshot_date: pendulum.DateTime,
        data_interval_start: pendulum.DateTime,
        data_interval_end: pendulum.DateTime,
    ):
        """Construct a DOIRelease instance.

        :param dag_id: The DAG ID.
        :param run_id: The DAG run ID.
        :param snapshot_date: Release date.
        """

        super().__init__(
            dag_id=dag_id,
            run_id=run_id,
            snapshot_date=snapshot_date,
        )
        self.data_interval_start = data_interval_start
        self.data_interval_end = data_interval_end

    @staticmethod
    def from_dict(dict_: dict):
        dag_id = dict_["dag_id"]
        run_id = dict_["run_id"]
        snapshot_date = pendulum.parse(dict_["snapshot_date"])
        data_interval_start = pendulum.parse(dict_["data_interval_start"])
        data_interval_end = pendulum.parse(dict_["data_interval_end"])
        return DOIRelease(
            dag_id=dag_id,
            run_id=run_id,
            snapshot_date=snapshot_date,
            data_interval_start=data_interval_start,
            data_interval_end=data_interval_end,
        )

    def to_dict(self) -> dict:
        return dict(
            dag_id=self.dag_id,
            run_id=self.run_id,
            snapshot_date=self.snapshot_date.to_datetime_string(),
            data_interval_start=self.data_interval_start.to_datetime_string(),
            data_interval_end=self.data_interval_end.to_datetime_string(),
        )
