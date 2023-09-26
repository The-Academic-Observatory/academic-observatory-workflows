# Copyright 2022 Curtin University
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

# Author: Aniek Roelofs, James Diprose

from __future__ import annotations

import copy
import datetime
import gzip
import json
import logging
import os
import re
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import timedelta
from functools import partial
from typing import List, Dict, Tuple, Optional, Callable

import boto3
import jsonlines
import pendulum
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.hooks.base import BaseHook
from airflow.models.taskinstance import TaskInstance
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import ShortCircuitOperator, PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from google.cloud import bigquery
from google.cloud.bigquery import SourceFormat

from academic_observatory_workflows.config import schema_folder as default_schema_folder, Tag
from observatory.api.client.model.dataset_release import DatasetRelease
from observatory.platform.airflow import PreviousDagRunSensor, is_first_dag_run
from observatory.platform.api import get_dataset_releases, get_latest_dataset_release, make_observatory_api
from observatory.platform.bigquery import (
    bq_table_id,
    bq_load_table,
    bq_delete_records,
    bq_create_dataset,
    bq_snapshot,
    bq_sharded_table_id,
    bq_create_empty_table,
    bq_table_exists,
    bq_upsert_records,
    bq_update_table_description,
)
from observatory.platform.config import AirflowConns
from observatory.platform.files import clean_dir
from observatory.platform.gcs import (
    gcs_create_aws_transfer,
    gcs_upload_transfer_manifest,
    gcs_blob_uri,
    gcs_blob_name_from_path,
    gcs_upload_files,
)
from observatory.platform.observatory_config import CloudWorkspace
from observatory.platform.workflows.workflow import (
    Workflow,
    WorkflowBashOperator,
    ChangefileRelease,
    set_task_state,
    cleanup,
)


class OpenAlexEntity:
    def __init__(
        self,
        entity_name: str,
        start_date: pendulum.DateTime,
        end_date: pendulum.DateTime,
        manifest: Manifest,
        merged_ids: List[MergedId],
        is_first_run: bool,
        prev_end_date: Optional[pendulum.DateTime],
        release: OpenAlexRelease = None,
    ):
        """This class represents the data and settings related to an OpenAlex entity or table.

        :param entity_name: the name of the entity, e.g. authors, institutions etc.
        :param start_date: the start date of the files covered by this release (inclusive).
        :param end_date: the end date of the files covered by this release (inclusive).
        :param manifest: the Redshift manifest provided by OpenAlex for this entity.
        :param merged_ids: the MergedIds provided by OpenAlex for this entity.
        :param is_first_run: whether this is the first run or not.
        :param prev_end_date: the previous end date.
        :param release: the release object.
        """

        self.entity_name = entity_name
        self.start_date = start_date
        self.end_date = end_date
        self.manifest = manifest
        self.merged_ids = merged_ids
        self.is_first_run = is_first_run
        self.prev_end_date = prev_end_date
        self.release = release

    def __eq__(self, other):
        if isinstance(other, OpenAlexEntity):
            return (
                self.entity_name == other.entity_name
                and self.start_date == other.start_date
                and self.end_date == other.end_date
                and self.manifest == other.manifest
                and self.merged_ids == other.merged_ids
                and self.is_first_run == other.is_first_run
                and self.prev_end_date == other.prev_end_date
            )
        return False

    @property
    def table_description(self):
        return f"OpenAlex {self.entity_name} table: https://docs.openalex.org/api-entities/{self.entity_name}"

    @property
    def schema_file_path(self):
        return os.path.join(self.release.schema_folder, f"{self.entity_name}.json")

    @property
    def upsert_uri(self):
        return gcs_blob_uri(
            self.release.cloud_workspace.transform_bucket,
            f"{gcs_blob_name_from_path(self.release.transform_folder)}/data/{self.entity_name}/*",
        )

    @property
    def merged_ids_uri(self):
        return gcs_blob_uri(
            self.release.cloud_workspace.download_bucket,
            f"{gcs_blob_name_from_path(self.release.download_folder)}/data/merged_ids/{self.entity_name}/*",
        )

    @property
    def merged_ids_schema_file_path(self):
        return os.path.join(self.release.schema_folder, "merged_ids.json")

    @property
    def bq_main_table_id(self):
        return bq_table_id(self.release.cloud_workspace.output_project_id, self.release.bq_dataset_id, self.entity_name)

    @property
    def bq_upsert_table_id(self):
        return bq_table_id(
            self.release.cloud_workspace.output_project_id, self.release.bq_dataset_id, f"{self.entity_name}_upsert"
        )

    @property
    def bq_delete_table_id(self):
        return bq_table_id(
            self.release.cloud_workspace.output_project_id, self.release.bq_dataset_id, f"{self.entity_name}_delete"
        )

    @property
    def bq_snapshot_table_id(self):
        return bq_sharded_table_id(
            self.release.cloud_workspace.output_project_id,
            self.release.bq_dataset_id,
            f"{self.entity_name}_snapshot",
            self.prev_end_date,
        )

    @property
    def current_entries(self):
        # We always only sync and process files that have an updated_date >= self.start_date
        # See https://docs.openalex.org/download-all-data/snapshot-data-format#downloading-updated-entities
        return [entry for entry in self.manifest.entries if entry.updated_date >= self.start_date]

    @property
    def has_merged_ids(self):
        return len(self.current_merged_ids) >= 1

    @property
    def current_merged_ids(self):
        # On the first run no merged_ids are processed
        # On subsequent runs merged_ids >= self.start_date are processed
        # See https://docs.openalex.org/download-all-data/snapshot-data-format#merged-entities
        return [
            merged_id
            for merged_id in self.merged_ids
            if merged_id.updated_date >= self.start_date and not self.is_first_run
        ]

    @staticmethod
    def from_dict(dict_: Dict) -> OpenAlexEntity:
        entity_name = dict_["entity_name"]
        start_date = pendulum.parse(dict_["start_date"])
        end_date = pendulum.parse(dict_["end_date"])
        manifest = Manifest.from_dict(dict_["manifest"])
        merged_ids = [MergedId.from_dict(merged_id) for merged_id in dict_["merged_ids"]]
        is_first_run = dict_["is_first_run"]
        prev_end_date = pendulum.parse(dict_["prev_end_date"])
        return OpenAlexEntity(entity_name, start_date, end_date, manifest, merged_ids, is_first_run, prev_end_date)

    def to_dict(self) -> Dict:
        return dict(
            entity_name=self.entity_name,
            start_date=self.start_date.isoformat(),
            end_date=self.end_date.isoformat(),
            manifest=self.manifest.to_dict(),
            merged_ids=[merged_id.to_dict() for merged_id in self.merged_ids],
            is_first_run=self.is_first_run,
            prev_end_date=self.prev_end_date.isoformat(),
        )


class OpenAlexRelease(ChangefileRelease):
    def __init__(
        self,
        *,
        dag_id: str,
        run_id: str,
        cloud_workspace: CloudWorkspace,
        bq_dataset_id: str,
        entities: List[OpenAlexEntity],
        start_date: pendulum.DateTime,
        end_date: pendulum.DateTime,
        is_first_run: bool,
        schema_folder: str,
    ):
        """Construct a OpenAlexRelease instance

        :param dag_id: the DAG ID.
        :param run_id: the DAG's run ID.
        :param cloud_workspace: the CloudWorkspace instance.
        :param bq_dataset_id: the BigQuery dataset id.
        :param start_date: the date of the first changefile processed in this release.
        :param end_date: the date of the last changefile processed in this release.
        :param schema_folder: the path to the schema folder.
        """
        super().__init__(
            dag_id=dag_id,
            run_id=run_id,
            start_date=start_date,
            end_date=end_date,
        )
        self.cloud_workspace = cloud_workspace
        self.bq_dataset_id = bq_dataset_id
        self.entities = entities
        self.is_first_run = is_first_run
        self.schema_folder = schema_folder

        # Set release object for each entity
        for entity in entities:
            entity.release = self

        self.entity_index = {entity.entity_name: entity for entity in self.entities}
        self.log_path = os.path.join(self.download_folder, "gsutil.log")
        self.transfer_manifest_uri = gcs_blob_uri(
            cloud_workspace.download_bucket, f"{gcs_blob_name_from_path(self.download_folder)}/manifest.csv"
        )
        self.gcs_openalex_data_uri = (
            f"gs://{cloud_workspace.download_bucket}/{gcs_blob_name_from_path(self.download_folder)}/"
        )

    def get_entity(self, entity_name: str) -> OpenAlexEntity | None:
        if entity_name in self.entity_index:
            return self.entity_index[entity_name]
        return None


def get_task_id(**kwargs):
    return kwargs["ti"].task_id


def make_no_updated_data_msg(task_id: str, entity_name: str) -> str:
    return (
        f"{task_id}: skipping this task, as there is no updated data for OpenAlexEntity({entity_name}) in this release"
    )


def make_first_run_message(task_id: str):
    return f"{task_id}: skipping this task, as it is not executed on the first run"


def make_no_merged_ids_msg(task_id: str, entity_name: str) -> str:
    return (
        f"{task_id}: skipping this task, as there are no merged_ids for OpenAlexEntity({entity_name}) in this release"
    )


class OpenAlexTelescope(Workflow):
    """OpenAlex telescope"""

    def __init__(
        self,
        *,
        dag_id: str,
        cloud_workspace: CloudWorkspace,
        bq_dataset_id: str = "openalex",
        entity_names: List[str] = None,
        schema_folder: str = os.path.join(default_schema_folder(), "openalex"),
        dataset_description: str = "The OpenAlex dataset: https://docs.openalex.org/",
        snapshot_expiry_days: int = 31,
        n_transfer_trys: int = 3,
        max_processes: int = os.cpu_count(),
        primary_key: str = "id",
        aws_conn_id: str = "aws_openalex",
        aws_openalex_bucket: str = "openalex",
        observatory_api_conn_id: str = AirflowConns.OBSERVATORY_API,
        start_date: pendulum.DateTime = pendulum.datetime(2021, 12, 1),
        schedule: str = "@weekly",
        queue: str = "remote_queue",
    ):
        """Construct an OpenAlexTelescope instance.

        :param dag_id: the id of the DAG.
        :param cloud_workspace: the cloud workspace settings.
        :param bq_dataset_id: the BigQuery dataset id.
        :param entity_names: the names of the OpenAlex entities to process.
        :param schema_folder: the SQL schema path.
        :param dataset_description: description for the BigQuery dataset.
        :param snapshot_expiry_days: the number of days that a snapshot of each entity's main table will take to expire,
        which is set to 31 days so there is some time to rollback after an update.
        :param n_transfer_trys: how to many times to transfer data from AWS to GCS.
        :param max_processes: the maximum number of processes to use when transforming data.
        :param aws_conn_id: the AWS Airflow Connection ID.
        :param aws_openalex_bucket: the OpenAlex AWS bucket name.
        :param observatory_api_conn_id: the Observatory API Airflow Connection ID.
        :param start_date: the Apache Airflow DAG start date.
        :param schedule: the Apache Airflow schedule interval. Whilst OpenAlex snapshots are released monthly,
        they are not released on any particular day of the month, so we instead simply run the workflow weekly on a
        Sunday as this will pickup new updates regularly. See here for past release dates: https://openalex.s3.amazonaws.com/RELEASE_NOTES.txt
        :param queue:
        """

        if entity_names is None:
            entity_names = ["authors", "concepts", "funders", "institutions", "publishers", "sources", "works"]

        super().__init__(
            dag_id=dag_id,
            start_date=start_date,
            schedule=schedule,
            catchup=False,
            airflow_conns=[observatory_api_conn_id, aws_conn_id],
            queue=queue,
            tags=[Tag.academic_observatory],
        )
        self.cloud_workspace = cloud_workspace
        self.bq_dataset_id = bq_dataset_id
        self.entity_names = entity_names
        self.schema_folder = schema_folder
        self.dataset_description = dataset_description
        self.snapshot_expiry_days = snapshot_expiry_days
        self.n_transfer_trys = n_transfer_trys
        self.max_processes = max_processes
        self.primary_key = primary_key
        self.aws_conn_id = aws_conn_id
        self.aws_openalex_bucket = aws_openalex_bucket
        self.observatory_api_conn_id = observatory_api_conn_id

    def make_python_operator(
        self,
        func: Callable,
        task_id: str,
        **kwargs,
    ):
        # TODO: move into Workflow class
        kwargs_ = copy.copy(kwargs)
        kwargs_["task_id"] = task_id
        return PythonOperator(python_callable=partial(self.task_callable, func), **kwargs_)

    def make_dag(self) -> DAG:
        def create_taskgroup(func, group_id, merge: bool = True):
            tasks = []
            with TaskGroup(group_id=group_id):
                for entity_name_ in self.entity_names:
                    task_id_ = entity_name_
                    tasks.append(
                        self.make_python_operator(
                            func,
                            task_id_,
                            op_kwargs={"entity_name": entity_name_},
                        )
                    )

            if merge:
                return tasks, EmptyOperator(
                    task_id=f"merge_{group_id}", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
                )

            return tasks, None

        with self.dag:
            # fmt: off
            # Wait for the previous DAG run to finish to make sure that
            # changefiles are processed in the correct order
            external_task_id = "dag_run_complete"
            task_sensor = PreviousDagRunSensor(
                dag_id=self.dag_id,
                external_task_id=external_task_id,
                execution_delta=timedelta(days=7),  # To match the @weekly schedule
            )
            task_check_dependencies = PythonOperator(python_callable=self.check_dependencies, task_id="check_dependencies")
            task_fetch_releases = ShortCircuitOperator(python_callable=self.fetch_releases, task_id="fetch_releases")
            task_create_datasets = PythonOperator(python_callable=self.create_datasets, task_id="create_datasets")
            tasks_snapshot, _ = create_taskgroup(self.bq_create_snapshots, "bq_create_snapshots", merge=False)
            task_aws_to_gcs_transfer = PythonOperator(python_callable=partial(self.task_callable, self.aws_to_gcs_transfer), task_id="aws_to_gcs_transfer", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS) #self.make_python_operator(self.aws_to_gcs_transfer, "aws_to_gcs_transfer", )

            # Download tasks
            tasks_download = []
            with TaskGroup(group_id="download"):
                for entity_name in self.entity_names:
                    tasks_download.append(make_download_bash_op(self, entity_name))
            task_merge_download = EmptyOperator(task_id="merge_download", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

            # Transform and upload
            tasks_transform, _ = create_taskgroup(self.transform, "transform", merge=False)
            task_upload = self.make_python_operator(self.upload_upsert_files, "upload_upsert_files", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

            # Upsert records
            tasks_bq_load_upserts, task_merge_bq_load_upserts = create_taskgroup(self.bq_load_upsert_tables, "bq_load_upserts")
            tasks_bq_upsert_records, task_merge_bq_upsert_records = create_taskgroup(self.bq_upsert_records, "bq_upsert_records")

            # Delete records
            tasks_bq_load_deletes, task_merge_bq_load_deletes = create_taskgroup(self.bq_load_delete_tables, "bq_load_deletes")
            tasks_bq_delete_records, task_merge_bq_delete_records = create_taskgroup(self.bq_delete_records, "bq_delete_records")

            # Add release info to API and cleanup
            tasks_add_dataset_releases, _ = create_taskgroup(self.add_dataset_releases, "add_dataset_releases", merge=False)
            task_cleanup = self.make_python_operator(self.cleanup, "cleanup", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
            task_wait = EmptyOperator(task_id=external_task_id)
            # fmt: on

            # Link tasks together
            (
                task_sensor
                >> task_check_dependencies
                >> task_fetch_releases
                >> task_create_datasets
                >> tasks_snapshot
                >> task_aws_to_gcs_transfer
                >> tasks_download
                >> task_merge_download
                >> tasks_transform
                >> task_upload
                >> tasks_bq_load_upserts
                >> task_merge_bq_load_upserts
                >> tasks_bq_upsert_records
                >> task_merge_bq_upsert_records
                >> tasks_bq_load_deletes
                >> task_merge_bq_load_deletes
                >> tasks_bq_delete_records
                >> task_merge_bq_delete_records
                >> tasks_add_dataset_releases
                >> task_cleanup
                >> task_wait
            )

        return self.dag

    @property
    def aws_key(self) -> Tuple[str, str]:
        """Get the AWS access key id and secret access key from the aws_conn_id airflow connection.

        :return: access key id and secret access key
        """

        conn = BaseHook.get_connection(self.aws_conn_id)
        access_key_id = conn.login
        secret_key = conn.password

        assert access_key_id is not None, f"OpenAlexTelescope.aws_key: {self.aws_conn_id} login is None"
        assert secret_key is not None, f"OpenAlexTelescope.aws_key: {self.aws_conn_id} password is None"

        return access_key_id, secret_key

    def fetch_releases(self, **kwargs) -> bool:
        """Fetch OpenAlex releases.

        :return: True to continue, False to skip.
        """

        dag_run = kwargs["dag_run"]
        is_first_run = is_first_dag_run(dag_run)

        # Build up information about what files we will be downloading for this release
        entities = []
        for entity_name in self.entity_names:
            releases = get_dataset_releases(dag_id=self.dag_id, dataset_id=entity_name)
            manifest = fetch_manifest(bucket=self.aws_openalex_bucket, aws_key=self.aws_key, entity_name=entity_name)
            merged_ids = fetch_merged_ids(
                bucket=self.aws_openalex_bucket, aws_key=self.aws_key, entity_name=entity_name
            )
            if is_first_run:
                assert (
                    len(releases) == 0
                ), f"fetch_releases: there should be no DatasetReleases for dag_id={self.dag_id}, dataset_id={entity_name} stored in the Observatory API on the first DAG run."

            # Calculate start and end dates of files for this release
            prev_release = get_latest_dataset_release(releases, "changefile_end_date")
            prev_end_date = pendulum.instance(datetime.datetime.min)
            if prev_release is not None:
                prev_end_date = prev_release.changefile_end_date
            new_entry_dates = [entry.updated_date for entry in manifest.entries if entry.updated_date > prev_end_date]
            start_date = min(new_entry_dates) if new_entry_dates else None
            end_date = max(new_entry_dates) if new_entry_dates else None

            # Don't add this entity because it has no updates
            if start_date is None or end_date is None:
                logging.info(f"fetch_releases: skipping OpenAlexEntity({entity_name}) as it has no updated data")
                continue

            logging.info(
                f"fetch_releases: adding OpenAlexEntity({entity_name}), start_date={start_date}, end_date={end_date})"
            )

            # Save metadata
            entity = OpenAlexEntity(
                entity_name, start_date, end_date, manifest, merged_ids, is_first_run, prev_end_date
            )
            entities.append(entity)

        # If no entities created then skip
        if not entities:
            logging.info(f"fetch_releases: no updates found, skipping")
            return False

        # Print summary information
        entities = [entity.to_dict() for entity in entities]
        logging.info(f"is_first_run: {is_first_run}")
        logging.info(f"entities: {entities}")

        # Publish release information
        ti: TaskInstance = kwargs["ti"]
        msg = dict(
            entities=entities,
        )
        ti.xcom_push(OpenAlexTelescope.RELEASE_INFO, msg, kwargs["logical_date"])

        return True

    def make_release(self, **kwargs) -> OpenAlexRelease:
        """Make a Release instance. Gets the list of releases available from the release check (setup task).

        :param kwargs: the context passed from the Airflow Operator.
        See https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return OpenAlexRelease.
        """

        ti: TaskInstance = kwargs["ti"]
        msg = ti.xcom_pull(key=self.RELEASE_INFO, task_ids=self.fetch_releases.__name__, include_prior_dates=False)

        run_id = kwargs["run_id"]
        dag_run = kwargs["dag_run"]
        is_first_run = is_first_dag_run(dag_run)
        entities = parse_release_msg(msg)
        start_date = min([entity.start_date for entity in entities])
        end_date = max([entity.start_date for entity in entities])

        release = OpenAlexRelease(
            dag_id=self.dag_id,
            run_id=run_id,
            cloud_workspace=self.cloud_workspace,
            bq_dataset_id=self.bq_dataset_id,
            entities=entities,
            start_date=start_date,
            end_date=end_date,
            is_first_run=is_first_run,
            schema_folder=self.schema_folder,
        )

        return release

    def create_datasets(self, **kwargs) -> None:
        """Create datasets."""

        bq_create_dataset(
            project_id=self.cloud_workspace.output_project_id,
            dataset_id=self.bq_dataset_id,
            location=self.cloud_workspace.data_location,
            description=self.dataset_description,
        )

    def bq_create_snapshots(self, release: OpenAlexRelease, entity_name: str = None, **kwargs):
        """Create a snapshot of each main table. The purpose of this table is to be able to rollback the table
        if something goes wrong. The snapshot expires after self.snapshot_expiry_days."""

        task_id = get_task_id(**kwargs)
        if release.is_first_run:
            raise AirflowSkipException(make_first_run_message(task_id))

        entity = release.get_entity(entity_name)
        if entity is None:
            raise AirflowSkipException(make_no_updated_data_msg(get_task_id(**kwargs), entity_name))

        expiry_date = pendulum.now().add(days=self.snapshot_expiry_days)
        logging.info(
            f"{task_id}: creating backup snapshot for {entity.bq_main_table_id} as {entity.bq_snapshot_table_id} expiring on {expiry_date}"
        )
        success = bq_snapshot(
            src_table_id=entity.bq_main_table_id, dst_table_id=entity.bq_snapshot_table_id, expiry_date=expiry_date
        )
        assert (
            success
        ), f"{task_id}: error creating backup snapshot for {entity.bq_main_table_id} as {entity.bq_snapshot_table_id} expiring on {expiry_date}"

    def aws_to_gcs_transfer(self, release: OpenAlexRelease, **kwargs):
        """Transfer files from AWS bucket to Google Cloud bucket"""

        # Make GCS Transfer Manifest for files that we need for this release
        object_paths = []
        for entity in release.entities:
            for entry in entity.current_entries:
                object_paths.append(entry.object_key)
            for merged_id in entity.current_merged_ids:
                object_paths.append(merged_id.object_key)
        gcs_upload_transfer_manifest(object_paths, release.transfer_manifest_uri)

        # Transfer files
        count = 0
        success = False
        for i in range(self.n_transfer_trys):
            success, objects_count = gcs_create_aws_transfer(
                aws_key=self.aws_key,
                aws_bucket=self.aws_openalex_bucket,
                include_prefixes=[],
                gc_project_id=self.cloud_workspace.input_project_id,
                gc_bucket_dst_uri=release.gcs_openalex_data_uri,
                description="Transfer OpenAlex from AWS to GCS",
                transfer_manifest=release.transfer_manifest_uri,
            )
            logging.info(
                f"gcs_create_aws_transfer: try={i + 1}/{self.n_transfer_trys}, success={success}, objects_count={objects_count}"
            )
            count += objects_count
            if success:
                break

        logging.info(f"gcs_create_aws_transfer: success={success}, total_object_count={count}")
        assert success, "Google Storage Transfer unsuccessful"

        # After the transfer, verify the manifests and merged_ids are the same as when we fetched them during
        # the fetch_releases task. If they are the same, the data did not change during transfer. If the
        # manifests do not match then the data has changed, and we need to restart the DAG run manually.
        # See step 3 : https://docs.openalex.org/download-all-data/snapshot-data-format#the-manifest-file
        success = True
        for entity in release.entities:
            current_manifest = fetch_manifest(
                bucket=self.aws_openalex_bucket, aws_key=self.aws_key, entity_name=entity.entity_name
            )
            current_merged_ids = fetch_merged_ids(
                bucket=self.aws_openalex_bucket, aws_key=self.aws_key, entity_name=entity.entity_name
            )
            if entity.manifest != current_manifest:
                logging.error(f"aws_to_gcs_transfer: OpenAlexEntity({entity.entity_name}) manifests have changed")
                success = False
            if entity.merged_ids != current_merged_ids:
                logging.error(f"aws_to_gcs_transfer: OpenAlexEntity({entity.entity_name}) merged_ids have changed")
                success = False

        if success:
            logging.info(f"aws_to_gcs_transfer: manifests and merged_ids the same")
        set_task_state(success, self.aws_to_gcs_transfer.__name__, release)

    def transform(self, release: OpenAlexRelease, entity_name: str = None, **kwargs):
        """Transform all files for the Work, Concept and Institution entities. Transforms one file per process."""

        task_id = get_task_id(**kwargs)
        entity = release.get_entity(entity_name)
        if entity is None:
            raise AirflowSkipException(make_no_updated_data_msg(task_id, entity_name))

        # Cleanup in case we re-run task
        output_folder = os.path.join(release.transform_folder, "data", entity_name)
        logging.info(f"{task_id}: cleaning path: {output_folder}")
        if os.path.exists(output_folder):
            clean_dir(output_folder)

        # These will all get executed as different tasks, so only use many processes for works which is the largest
        max_processes = 1
        if entity.entity_name == "works":
            max_processes = self.max_processes
        logging.info(f"{task_id}: transforming files for OpenAlexEntity({entity_name}), no. workers: {max_processes}")

        with ProcessPoolExecutor(max_workers=max_processes) as executor:
            futures = []
            for entry in entity.current_entries:
                input_path = os.path.join(release.download_folder, entry.object_key)
                output_path = os.path.join(release.transform_folder, entry.object_key)
                futures.append(executor.submit(transform_file, input_path, output_path))
            for future in as_completed(futures):
                future.result()

    def upload_upsert_files(self, release: OpenAlexRelease, **kwargs):
        """Upload the transformed data to Cloud Storage.
        :raises AirflowException: Raised if the files to be uploaded are not found."""

        # Make files to upload
        file_paths = []
        for entity in release.entities:
            for entry in entity.current_entries:
                file_path = os.path.join(release.transform_folder, entry.object_key)
                file_paths.append(file_path)

        # Upload files
        success = gcs_upload_files(bucket_name=self.cloud_workspace.transform_bucket, file_paths=file_paths)
        set_task_state(success, self.upload_upsert_files.__name__, release)

    def bq_load_upsert_tables(self, release: OpenAlexRelease, entity_name: str = None, **kwargs):
        """Load the upsert table for each entity."""

        task_id = get_task_id(**kwargs)
        entity = release.get_entity(entity_name)
        if entity is None:
            raise AirflowSkipException(make_no_updated_data_msg(task_id, entity_name))

        logging.info(f"{task_id}: loading OpenAlexEntity({entity_name}) upsert table {entity.bq_upsert_table_id}")
        success = bq_load_table(
            uri=entity.upsert_uri,
            table_id=entity.bq_upsert_table_id,
            schema_file_path=entity.schema_file_path,
            source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            ignore_unknown_values=True,
        )
        assert (
            success
        ), f"{task_id}: error loading OpenAlexEntity({entity_name}) upsert table {entity.bq_upsert_table_id}"

    def bq_upsert_records(self, release: OpenAlexRelease, entity_name: str = None, **kwargs):
        """Upsert the records from each upserts table into the main table."""

        task_id = get_task_id(**kwargs)
        entity = release.get_entity(entity_name)
        if entity is None:
            raise AirflowSkipException(make_no_updated_data_msg(task_id, entity_name))

        # Create main table if it doesn't exist
        if not bq_table_exists(entity.bq_main_table_id):
            logging.info(
                f"{task_id}: creating empty OpenAlexEntity({entity_name}) main table {entity.bq_main_table_id}"
            )
            bq_create_empty_table(table_id=entity.bq_main_table_id, schema_file_path=entity.schema_file_path)
            bq_update_table_description(table_id=entity.bq_main_table_id, description=entity.table_description)

        # Upsert records from upsert table to main table
        logging.info(
            f"{task_id}: upserting OpenAlexEntity({entity_name}) records from {entity.bq_upsert_table_id} to {entity.bq_main_table_id}"
        )
        bq_upsert_records(
            main_table_id=entity.bq_main_table_id,
            upsert_table_id=entity.bq_upsert_table_id,
            primary_key=self.primary_key,
        )

    def bq_load_delete_tables(self, release: OpenAlexRelease, entity_name: str = None, **kwargs):
        """Load the delete tables."""

        task_id = get_task_id(**kwargs)
        entity = release.get_entity(entity_name)
        if entity is None:
            raise AirflowSkipException(make_no_updated_data_msg(task_id, entity_name))
        elif not entity.has_merged_ids:
            raise AirflowSkipException(make_no_merged_ids_msg(task_id, entity_name))

        logging.info(f"{task_id}: loading OpenAlexEntity({entity_name}) delete table {entity.bq_delete_table_id}")
        success = bq_load_table(
            uri=entity.merged_ids_uri,
            table_id=entity.bq_delete_table_id,
            schema_file_path=entity.merged_ids_schema_file_path,
            source_format=SourceFormat.CSV,
            csv_skip_leading_rows=1,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            ignore_unknown_values=True,
        )
        assert (
            success
        ), f"{task_id}: error loading OpenAlexEntity({entity_name}) delete table {entity.bq_delete_table_id}"

    def bq_delete_records(self, release: OpenAlexRelease, entity_name: str = None, **kwargs):
        """Delete records from main tables that are in delete tables."""

        task_id = get_task_id(**kwargs)
        entity = release.get_entity(entity_name)
        if entity is None:
            raise AirflowSkipException(make_no_updated_data_msg(task_id, entity_name))
        elif not entity.has_merged_ids:
            raise AirflowSkipException(make_no_merged_ids_msg(task_id, entity_name))

        logging.info(
            f"{task_id}: deleting OpenAlexEntity({entity_name}) records in {entity.bq_main_table_id} from {entity.bq_delete_table_id}"
        )
        bq_delete_records(
            main_table_id=entity.bq_main_table_id,
            delete_table_id=entity.bq_delete_table_id,
            main_table_primary_key=self.primary_key,
            delete_table_primary_key=self.primary_key,
            delete_table_primary_key_prefix="https://openalex.org/",
        )

    def add_dataset_releases(self, release: OpenAlexRelease, entity_name: str = None, **kwargs) -> None:
        """Adds release information to API."""

        task_id = get_task_id(**kwargs)
        entity = release.get_entity(entity_name)
        if entity is None:
            raise AirflowSkipException(make_no_updated_data_msg(task_id, entity_name))

        logging.info(f"{task_id}: creating dataset release for OpenAlexEntity({entity_name})")
        dataset_release = DatasetRelease(
            dag_id=self.dag_id,
            dataset_id=entity.entity_name,
            dag_run_id=release.run_id,
            changefile_start_date=entity.start_date,
            changefile_end_date=entity.end_date,
        )
        logging.info(f"{task_id}: dataset_release={dataset_release}")
        api = make_observatory_api(observatory_api_conn_id=self.observatory_api_conn_id)
        api.post_dataset_release(dataset_release)

    def cleanup(self, release: OpenAlexRelease, **kwargs) -> None:
        """Delete all files, folders and XComs associated with this release."""

        cleanup(dag_id=self.dag_id, execution_date=kwargs["logical_date"], workflow_folder=release.workflow_folder)


def make_download_bash_op(workflow: Workflow, entity_name: str) -> WorkflowBashOperator:
    """Download files for an entity from the bucket.

    Gsutil is used instead of the standard Google Cloud Python library, because it is faster at downloading files
    than the Google Cloud Python library.

    :param workflow: the workflow.
    :param entity_name: the name of the OpenAlex entity, e.g. authors, institutions etc.
    :return: a WorkflowBashOperator instance.
    """

    output_folder = "{{ release.download_folder }}/data/" + entity_name + "/"
    bucket_path = "{{ release.gcs_openalex_data_uri }}data/" + entity_name + "/*"
    return WorkflowBashOperator(
        workflow=workflow,
        task_id=entity_name,
        bash_command="mkdir -p "
        + output_folder
        + " && gcloud auth activate-service-account --key-file=${GOOGLE_APPLICATION_CREDENTIALS}"
        + " && gsutil -m -q cp -L {{ release.log_path }} -r "
        + bucket_path
        + " "
        + output_folder,
    )


def parse_release_msg(msg: Dict):
    return [OpenAlexEntity.from_dict(entity) for entity in msg["entities"]]


def s3_uri_parts(s3_uri: str) -> Tuple[str, str]:
    """Extracts the S3 bucket name and object key from the given S3 URI.

    :param s3_uri: str, S3 URI in format s3://mybucketname/path/to/object
    :return: tuple, (bucket_name, object_key)
    """

    if not s3_uri.startswith("s3://"):
        raise ValueError(f"Invalid S3 URI. URI should start with 's3://' - {s3_uri}")

    parts = s3_uri[5:].split("/", 1)  # Remove 's3://' and split the remaining string
    bucket_name = parts[0]
    object_key = parts[1] if len(parts) > 1 else None

    return bucket_name, object_key


class Meta:
    def __init__(self, content_length, record_count):
        self.content_length = content_length
        self.record_count = record_count

    def __eq__(self, other):
        if isinstance(other, Meta):
            return self.content_length == other.content_length and self.record_count == other.record_count
        return False

    @staticmethod
    def from_dict(dict_: Dict) -> Meta:
        content_length = dict_["content_length"]
        record_count = dict_["record_count"]
        return Meta(content_length, record_count)

    def to_dict(self) -> Dict:
        return dict(content_length=self.content_length, record_count=self.record_count)


class ManifestEntry:
    def __init__(self, url: str, meta: Meta):
        # URLs given from OpenAlex may not be given with the 's3://' prefix.
        if not url.startswith("s3://"):
            self.url = f"s3://{url}"
        else:
            self.url = url
        self.meta = meta

    def __eq__(self, other):
        if isinstance(other, ManifestEntry):
            return self.url == other.url and self.meta == other.meta
        return False

    @property
    def object_key(self):
        bucket_name, object_key = s3_uri_parts(self.url)
        assert object_key is not None, f"object_key for url={self.url} is None"
        return object_key

    @property
    def updated_date(self) -> pendulum.DateTime:
        return pendulum.parse(re.search(r"updated_date=(\d{4}-\d{2}-\d{2})", self.url).group(1))

    @property
    def file_name(self):
        return re.search(r"part_\d+\.gz", self.url).group(0)

    @staticmethod
    def from_dict(dict_: Dict) -> ManifestEntry:
        url = dict_["url"]
        meta = Meta.from_dict(dict_["meta"])
        return ManifestEntry(url, meta)

    def to_dict(self) -> Dict:
        return dict(url=self.url, meta=self.meta.to_dict())


class Manifest:
    def __init__(self, entries: List[ManifestEntry], meta: Meta):
        self.entries = entries
        self.meta = meta

    def __eq__(self, other):
        if isinstance(other, Manifest):
            return self.meta == other.meta and len(self.entries) == len(other.entries) and self.entries == other.entries
        return False

    @staticmethod
    def from_dict(dict_: Dict) -> Manifest:
        entries = [ManifestEntry.from_dict(entry) for entry in dict_["entries"]]
        meta = Meta.from_dict(dict_["meta"])
        return Manifest(entries, meta)

    def to_dict(self) -> Dict:
        return dict(entries=[entry.to_dict() for entry in self.entries], meta=self.meta.to_dict())


class MergedId:
    def __init__(self, url: str, content_length: int):
        self.url = url
        self.content_length = content_length

    def __eq__(self, other):
        if isinstance(other, MergedId):
            return self.url == other.url and self.content_length == other.content_length
        return False

    @property
    def object_key(self):
        bucket_name, object_key = s3_uri_parts(self.url)
        assert object_key is not None, f"object_key for url={self.url} is None"
        return object_key

    @property
    def updated_date(self) -> pendulum.DateTime:
        return pendulum.parse(re.search(r"\d{4}-\d{2}-\d{2}", self.url).group(0))

    @property
    def file_name(self):
        return re.search(r"[^/]+\.csv\.gz$", self.url).group(0)

    @staticmethod
    def from_dict(dict_: Dict) -> MergedId:
        url = dict_["url"]
        content_length = dict_["content_length"]
        return MergedId(url, content_length)

    def to_dict(self) -> Dict:
        return dict(url=self.url, content_length=self.content_length)


def fetch_manifest(
    *,
    bucket: str,
    aws_key: Tuple[str, str],
    entity_name: str,
) -> Manifest:
    """Fetch OpenAlex manifests for a range of entity types.

    :param bucket: the OpenAlex AWS bucket.
    :param aws_key: the aws_access_key_id and aws_secret_key as a tuple.
    :param entity_name: the entity type.
    :return: None
    """

    aws_access_key_id, aws_secret_key = aws_key
    client = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_key,
    ).client("s3")
    obj = client.get_object(Bucket=bucket, Key=f"data/{entity_name}/manifest")
    data = json.loads(obj["Body"].read().decode())

    # Add s3:// as necessary

    return Manifest.from_dict(data)


def fetch_merged_ids(
    *, bucket: str, aws_key: Tuple[str, str], entity_name: str, prefix: str = "data/merged_ids"
) -> List[MergedId]:
    aws_access_key_id, aws_secret_key = aws_key
    client = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_key,
    ).client("s3")
    paginator = client.get_paginator("list_objects_v2")

    results = []
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{prefix}/{entity_name}"):
        for content in page.get("Contents", []):
            obj_key = content["Key"]
            # There is a dud file in data/merged_ids/sources/
            if obj_key != "data/merged_ids/sources/.csv":
                url = f"s3://{bucket}/{obj_key}"
                content_length = content["Size"]
                results.append(MergedId(url, content_length))

    # Sort from oldest to newest
    results.sort(key=lambda m: m.updated_date, reverse=False)

    return results


def transform_file(download_path: str, transform_path: str):
    """Transforms a single file.
    Each entry/object in the gzip input file is transformed and the transformed object is immediately written out to
    a gzip file. For each entity only one field has to be transformed.

    :param download_path: The path to the file with the OpenAlex entries.
    :param transform_path: The path where transformed data will be saved
    :return: None.
    """

    if not os.path.isdir(os.path.dirname(transform_path)):
        os.makedirs(os.path.dirname(transform_path))

    logging.info(f"Transforming {download_path}")
    with gzip.open(download_path, "rb") as f_in, gzip.open(transform_path, "wt", encoding="ascii") as f_out:
        reader = jsonlines.Reader(f_in)
        for obj in reader.iter(skip_empty=True):
            transform_object(obj)
            json.dump(obj, f_out)
            f_out.write("\n")
    logging.info(f"Finished transform, saved to {transform_path}")


def transform_object(obj: dict):
    """Transform an entry/object for one of the OpenAlex entities.
    For the Work entity only the "abstract_inverted_index" field is transformed.
    For the Concept and Institution entities only the "international" field is transformed.

    :param obj: Single object with entity information
    :param field: The field of interested that is transformed.
    :return: None.
    """

    # Remove nulls from arrays
    # And handle null value
    field = "corresponding_institution_ids"
    if field in obj:
        value = obj.get(field, [])
        if value is None:
            value = []
        obj[field] = [x for x in value if x is not None]

    # Remove nulls from arrays
    # And handle null value
    field = "corresponding_author_ids"
    if field in obj:
        value = obj.get(field, [])
        if value is None:
            value = []
        obj[field] = [x for x in value if x is not None]

    field = "abstract_inverted_index"
    if field in obj:
        if not isinstance(obj.get(field), dict):
            return
        keys = list(obj[field].keys())
        values = [str(value)[1:-1] for value in obj[field].values()]

        obj[field] = {"keys": keys, "values": values}

    field = "international"
    if field in obj:
        for nested_field in obj.get(field, {}).keys():
            if not isinstance(obj[field][nested_field], dict):
                continue
            keys = list(obj[field][nested_field].keys())
            values = list(obj[field][nested_field].values())

            obj[field][nested_field] = {"keys": keys, "values": values}

    # Transform updated_date from a date into a datetime
    field = "updated_date"
    if field in obj:
        obj[field] = pendulum.parse(obj[field]).to_iso8601_string()
