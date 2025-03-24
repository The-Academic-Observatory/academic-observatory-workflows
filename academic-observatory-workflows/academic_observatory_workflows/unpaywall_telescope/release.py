# Copyright 2020-2024 Curtin University
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

# Author: Tuan Chien, James Diprose
from __future__ import annotations

import os
from typing import Dict, List

import pendulum

from observatory_platform.google.bigquery import bq_sharded_table_id, bq_table_id
from observatory_platform.google.gcs import gcs_blob_name_from_path, gcs_blob_uri
from observatory_platform.airflow.workflow import CloudWorkspace
from observatory_platform.airflow.release import ChangefileRelease, Release, SnapshotRelease


class Changefile:
    def __init__(self, filename: str, changefile_date: pendulum.DateTime, changefile_release: ChangefileRelease = None):
        """Holds the metadata about a single Unpaywall changefile.

        :param filename: the name of the changefile.
        :param changefile_date: the date of the changefile.
        :param changefile_release: the ChangefileRelease object.
        """

        self.filename = filename
        self.changefile_date = changefile_date
        self.changefile_release = changefile_release

    def __eq__(self, other):
        if isinstance(other, Changefile):
            return self.filename == other.filename and self.changefile_date == other.changefile_date
        return False

    @staticmethod
    def from_dict(dict_: Dict) -> Changefile:
        filename = dict_["filename"]
        changefile_date = pendulum.parse(dict_["changefile_date"])
        return Changefile(filename, changefile_date)

    def to_dict(self) -> Dict:
        return dict(filename=self.filename, changefile_date=self.changefile_date.isoformat())

    @property
    def download_file_path(self):
        assert self.changefile_release is not None, "Changefile.download_file_path: self.changefile_release is None"
        return os.path.join(self.changefile_release.download_folder, self.filename)

    @property
    def extract_file_path(self):
        assert self.changefile_release is not None, "Changefile.extract_file_path: self.changefile_release is None"
        # Remove .gz
        return os.path.join(self.changefile_release.extract_folder, f"{self.filename[:-3]}")

    @property
    def transform_file_path(self):
        assert self.changefile_release is not None, "Changefile.transform_file_path: self.changefile_release is None"
        # Remove .gz
        return os.path.join(self.changefile_release.transform_folder, self.filename[:-3])


class UnpaywallRelease(Release):
    def __init__(
        self,
        *,
        dag_id: str,
        run_id: str,
        cloud_workspace: CloudWorkspace,
        bq_dataset_id: str,
        bq_table_name: str,
        is_first_run: bool,
        snapshot_date: pendulum.DateTime,
        changefiles: List[Changefile],
        prev_end_date: pendulum.DateTime,
    ):
        """Construct an UnpaywallRelease instance

        :param dag_id: the id of the DAG.
        :param run_id: the run id of the DAG.
        :param cloud_workspace: the CloudWorkspace instance.
        :param bq_dataset_id: the BigQuery dataset id.
        :param bq_table_name: the BigQuery table name.
        :param is_first_run: whether this is the first DAG run.
        :param snapshot_date: the date of the Unpaywall snapshot.
        :param changefiles: a list of Changefile objects.
        :param prev_end_date: the previous end date.
        """

        super().__init__(
            dag_id=dag_id,
            run_id=run_id,
        )
        self.cloud_workspace = cloud_workspace
        self.bq_dataset_id = bq_dataset_id
        self.bq_table_name = bq_table_name
        self.is_first_run = is_first_run
        self.snapshot_date = snapshot_date
        self.snapshot_release = SnapshotRelease(dag_id=dag_id, run_id=run_id, snapshot_date=snapshot_date)

        # The first changefile is the oldest and the last one is the newest
        # the start and end date of the Unpaywall changefiles processed in this release.
        changefiles = sorted(changefiles, key=lambda c: c.changefile_date, reverse=False)
        start_date = changefiles[0].changefile_date
        end_date = changefiles[-1].changefile_date

        self.changefile_release = ChangefileRelease(
            dag_id=dag_id,
            run_id=run_id,
            start_date=start_date,
            end_date=end_date,
        )

        self.changefiles = changefiles
        for changefile in changefiles:
            changefile.changefile_release = self.changefile_release
        self.prev_end_date = prev_end_date

        # Paths used during processing
        self.snapshot_download_file_path = os.path.join(
            self.snapshot_release.download_folder, "unpaywall_snapshot.jsonl.gz"
        )
        self.snapshot_extract_file_path = os.path.join(self.snapshot_release.extract_folder, "unpaywall_snapshot.jsonl")
        self.main_table_file_path = os.path.join(self.snapshot_release.transform_folder, "main_table.jsonl")
        self.upsert_table_file_path = os.path.join(self.changefile_release.transform_folder, "upsert_table.jsonl")

        # The final split files
        self.main_table_files_regex = r"^main_table\d{12}\.jsonl$"

        self.main_table_uri = gcs_blob_uri(
            cloud_workspace.transform_bucket,
            f"{gcs_blob_name_from_path(self.snapshot_release.transform_folder)}/main_table*.jsonl",
        )
        self.upsert_table_uri = gcs_blob_uri(
            cloud_workspace.transform_bucket, gcs_blob_name_from_path(self.upsert_table_file_path)
        )
        self.bq_main_table_id = bq_table_id(cloud_workspace.output_project_id, bq_dataset_id, bq_table_name)
        self.bq_upsert_table_id = bq_table_id(
            cloud_workspace.output_project_id, bq_dataset_id, f"{bq_table_name}_upsert"
        )
        self.bq_snapshot_table_id = bq_sharded_table_id(
            cloud_workspace.output_project_id,
            bq_dataset_id,
            f"{bq_table_name}_snapshot",
            self.prev_end_date,
        )

    @staticmethod
    def from_dict(dict_: dict) -> UnpaywallRelease:
        return UnpaywallRelease(
            dag_id=dict_["dag_id"],
            run_id=dict_["run_id"],
            cloud_workspace=CloudWorkspace.from_dict(dict_["cloud_workspace"]),
            bq_dataset_id=dict_["bq_dataset_id"],
            bq_table_name=dict_["bq_table_name"],
            is_first_run=dict_["is_first_run"],
            snapshot_date=pendulum.parse(dict_["snapshot_date"]),
            changefiles=[Changefile.from_dict(changefile) for changefile in dict_["changefiles"]],
            prev_end_date=pendulum.parse(dict_["prev_end_date"]),
        )

    def to_dict(self) -> dict:
        return dict(
            dag_id=self.dag_id,
            run_id=self.run_id,
            cloud_workspace=self.cloud_workspace.to_dict(),
            bq_dataset_id=self.bq_dataset_id,
            bq_table_name=self.bq_table_name,
            is_first_run=self.is_first_run,
            snapshot_date=self.snapshot_date.isoformat(),
            changefiles=[changefile.to_dict() for changefile in self.changefiles],
            prev_end_date=self.prev_end_date.isoformat(),
        )
