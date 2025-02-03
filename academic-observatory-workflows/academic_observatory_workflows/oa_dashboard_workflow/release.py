# Copyright 2021-2024 Curtin University
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


from __future__ import annotations

import functools
import os
import os.path

import pendulum

from observatory_platform.airflow.release import SnapshotRelease
from observatory_platform.google.bigquery import bq_select_latest_table, bq_sharded_table_id, bq_table_id


class OaDashboardRelease(SnapshotRelease):
    def __init__(
        self,
        *,
        dag_id: str,
        run_id: str,
        snapshot_date: pendulum.DateTime,
        input_project_id: str,
        output_project_id: str,
        bq_agg_dataset_id: str,
        bq_ror_dataset_id: str,
        bq_settings_dataset_id: str,
        bq_oa_dashboard_dataset_id: str,
    ):
        """Create an OaDashboardRelease instance.

        :param dag_id: the dag id.
        :param run_id: the DAG run id.
        :param snapshot_date: the release date.
        :param input_project_id: the ID of the Google Cloud project where data will be pulled from.
        :param output_project_id: the ID of the Google Cloud project where data will be written to.
        :param bq_agg_dataset_id: the id of the BigQuery dataset where the Academic Observatory aggregated data lives.
        :param bq_ror_dataset_id: the id of the BigQuery dataset containing the ROR table.
        :param bq_settings_dataset_id: the id of the BigQuery settings dataset, which contains the country table.
        :param bq_oa_dashboard_dataset_id: the id of the BigQuery dataset where the tables produced by this workflow will be created.
        """

        super().__init__(dag_id=dag_id, run_id=run_id, snapshot_date=snapshot_date)
        self.input_project_id = input_project_id
        self.output_project_id = output_project_id
        self.bq_ror_dataset_id = bq_ror_dataset_id
        self.bq_settings_dataset_id = bq_settings_dataset_id
        self.bq_agg_dataset_id = bq_agg_dataset_id
        self.bq_oa_dashboard_dataset_id = bq_oa_dashboard_dataset_id

    @property
    def build_path(self):
        path = os.path.join(self.transform_folder, "build")
        os.makedirs(path, exist_ok=True)
        return path

    @property
    def intermediate_path(self):
        path = os.path.join(self.transform_folder, "intermediate")
        os.makedirs(path, exist_ok=True)
        return path

    @property
    def out_path(self):
        path = os.path.join(self.transform_folder, "out")
        os.makedirs(path, exist_ok=True)
        return path

    @functools.cached_property
    def ror_table_id(self):
        return bq_select_latest_table(
            table_id=bq_table_id(self.input_project_id, self.bq_ror_dataset_id, "ror"),
            end_date=self.snapshot_date,
            sharded=True,
        )

    @functools.cached_property
    def country_table_id(self):
        return bq_table_id(self.input_project_id, self.bq_settings_dataset_id, "country")

    def observatory_agg_table_id(self, table_name: str):
        return bq_select_latest_table(
            table_id=bq_table_id(self.input_project_id, self.bq_agg_dataset_id, table_name),
            end_date=self.snapshot_date,
            sharded=True,
        )

    @functools.cached_property
    def institution_ids_table_id(self):
        return bq_sharded_table_id(
            self.output_project_id, self.bq_oa_dashboard_dataset_id, "institution_ids", self.snapshot_date
        )

    def oa_dashboard_table_id(self, table_name: str):
        return bq_sharded_table_id(
            self.output_project_id, self.bq_oa_dashboard_dataset_id, table_name, self.snapshot_date
        )

    def descriptions_table_id(self, table_name: str):
        return bq_sharded_table_id(
            self.output_project_id, self.bq_oa_dashboard_dataset_id, f"{table_name}_descriptions", self.snapshot_date
        )

    def logos_table_id(self, table_name: str):
        return bq_sharded_table_id(
            self.output_project_id, self.bq_oa_dashboard_dataset_id, f"{table_name}_logos", self.snapshot_date
        )

    @staticmethod
    def from_dict(dict_: dict):
        return OaDashboardRelease(
            dag_id=dict_["dag_id"],
            run_id=dict_["run_id"],
            snapshot_date=pendulum.parse(dict_["snapshot_date"]),
            input_project_id=dict_["input_project_id"],
            output_project_id=dict_["output_project_id"],
            bq_agg_dataset_id=dict_["bq_agg_dataset_id"],
            bq_ror_dataset_id=dict_["bq_ror_dataset_id"],
            bq_settings_dataset_id=dict_["bq_settings_dataset_id"],
            bq_oa_dashboard_dataset_id=dict_["bq_oa_dashboard_dataset_id"],
        )

    def to_dict(self) -> dict:
        return dict(
            dag_id=self.dag_id,
            run_id=self.run_id,
            snapshot_date=self.snapshot_date.to_datetime_string(),
            input_project_id=self.input_project_id,
            output_project_id=self.output_project_id,
            bq_agg_dataset_id=self.bq_agg_dataset_id,
            bq_ror_dataset_id=self.bq_ror_dataset_id,
            bq_settings_dataset_id=self.bq_settings_dataset_id,
            bq_oa_dashboard_dataset_id=self.bq_oa_dashboard_dataset_id,
        )
