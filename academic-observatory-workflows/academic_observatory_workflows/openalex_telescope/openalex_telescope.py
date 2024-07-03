# Copyright 2022-2024 Curtin University
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

# Author: Aniek Roelofs, James Diprose, Alex Massen-Hane

from __future__ import annotations

import datetime
import gzip
import json
import logging
import os
import re
from collections import OrderedDict
from concurrent.futures import as_completed, ProcessPoolExecutor
from json.encoder import JSONEncoder
from typing import Any, Dict, List, Optional, Tuple

import boto3
import jsonlines
import pendulum
from airflow import DAG
from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.hooks.base import BaseHook
from airflow.models.taskinstance import TaskInstance
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from bigquery_schema_generator.generate_schema import flatten_schema_map, SchemaGenerator
from deepdiff import DeepDiff
from google.cloud import bigquery
from google.cloud.bigquery import SourceFormat

import observatory.platform.bigquery as bq
from academic_observatory_workflows.config import project_path
from observatory.api.client.model.dataset_release import DatasetRelease
from observatory_platform.airflow import is_first_dag_run, on_failure_callback, PreviousDagRunSensor, send_slack_msg
from observatory_platform.dataset_api import get_dataset_releases, get_latest_dataset_release, make_observatory_api
from observatory_platform.config import AirflowConns
from observatory_platform.files import clean_dir
from observatory_platform.google.gcs import (
    gcs_blob_name_from_path,
    gcs_blob_uri,
    gcs_create_aws_transfer,
    gcs_upload_files,
    gcs_upload_transfer_manifest,
)
from observatory_platform.airflow.workflow import CloudWorkspace
from observatory_platform.sandbox.sandbox_environment import log_diff
from observatory_platform.airflow.tasks import check_dependencies
from observatory_platform.refactor.workflow import make_workflow_folder
from observatory_platform.airflow.workflow import (
    ChangefileRelease,
    cleanup,
    DATE_TIME_FORMAT,
)

TEMP_TABLE_DESCRIPTION = "Temporary table for internal use. Do not use."
UPSERT_BYTE_LIMIT = int(3 * 2**40)


class OpenAlexEntity(ChangefileRelease):
    def __init__(
        self,
        *,
        dag_id: str,
        run_id: str,
        cloud_workspace: CloudWorkspace,
        entity_name: str,
        bq_dataset_id: str,
        schema_folder: str,
        start_date: pendulum.DateTime,
        end_date: pendulum.DateTime,
        manifest: Manifest,
        merged_ids: List[MergedId],
        is_first_run: bool,
        prev_end_date: Optional[pendulum.DateTime],
    ):
        """This class represents the data and settings related to an OpenAlex entity or table.

        :param dag_id: the DAG ID.
        :param run_id: the DAG's run ID.
        :param cloud_workspace: the CloudWorkspace instance.
        :param entity_name: the name of the entity, e.g. authors, institutions etc.
        :param bq_dataset_id: the BigQuery dataset id.
        :param schema_folder: the path to the schema folder.
        :param start_date: the start date of the files covered by this release (inclusive).
        :param end_date: the end date of the files covered by this release (inclusive).
        :param manifest: the Redshift manifest provided by OpenAlex for this entity.
        :param merged_ids: the MergedIds provided by OpenAlex for this entity.
        :param is_first_run: whether this is the first run or not.
        :param prev_end_date: the previous end date.
        """

        super().__init__(
            dag_id=dag_id,
            run_id=run_id,
            start_date=start_date,
            end_date=end_date,
        )
        self.cloud_workspace = cloud_workspace
        self.entity_name = entity_name
        self.bq_dataset_id = bq_dataset_id
        self.schema_folder = schema_folder
        self.start_date = start_date
        self.end_date = end_date
        self.manifest = manifest
        self.merged_ids = merged_ids
        self.is_first_run = is_first_run
        self.prev_end_date = prev_end_date
        self.transfer_manifest_uri = gcs_blob_uri(
            cloud_workspace.download_bucket, f"{gcs_blob_name_from_path(self.download_folder)}/manifest.csv"
        )
        self.gcs_openalex_data_uri = (
            f"gs://{cloud_workspace.download_bucket}/{gcs_blob_name_from_path(self.download_folder)}/"
        )
        self.log_path = os.path.join(self.download_folder, "gsutil.log")

    @property
    def release_folder(self):
        """Get the path to the release folder, which resides inside the workflow folder.

        :return: path to folder.
        """

        return make_workflow_folder(
            self.dag_id,
            self.run_id,
            f"{self.entity_name}_changefile_{self.start_date.format(DATE_TIME_FORMAT)}_to_{self.end_date.format(DATE_TIME_FORMAT)}",
        )

    @property
    def table_description(self):
        return f"OpenAlex {self.entity_name} table: https://docs.openalex.org/api-entities/{self.entity_name}"

    @property
    def schema_file_path(self):
        return os.path.join(self.schema_folder, f"{self.entity_name}.json")

    @property
    def generated_schema_path(self):
        return os.path.join(self.transform_folder, f"generated_schema_{self.entity_name}.json")

    @property
    def data_uri(self):
        return gcs_blob_uri(
            self.cloud_workspace.transform_bucket,
            f"{gcs_blob_name_from_path(self.transform_folder)}/data/{self.entity_name}/*",
        )

    @property
    def merged_ids_uri(self):
        return gcs_blob_uri(
            self.cloud_workspace.download_bucket,
            f"{gcs_blob_name_from_path(self.download_folder)}/data/merged_ids/{self.entity_name}/*",
        )

    @property
    def merged_ids_schema_file_path(self):
        return os.path.join(self.schema_folder, "merged_ids.json")

    @property
    def bq_main_table_id(self):
        return bq.bq_table_id(self.cloud_workspace.output_project_id, self.bq_dataset_id, self.entity_name)

    @property
    def bq_upsert_table_id(self):
        return bq.bq_table_id(self.cloud_workspace.output_project_id, self.bq_dataset_id, f"{self.entity_name}_upsert")

    @property
    def bq_delete_table_id(self):
        return bq.bq_table_id(self.cloud_workspace.output_project_id, self.bq_dataset_id, f"{self.entity_name}_delete")

    @property
    def bq_snapshot_table_id(self):
        return bq.bq_sharded_table_id(
            self.cloud_workspace.output_project_id,
            self.bq_dataset_id,
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
        return OpenAlexEntity(
            dag_id=dict_["dag_id"],
            run_id=dict_["run_id"],
            cloud_workspace=CloudWorkspace.from_dict(dict_["cloud_workspace"]),
            entity_name=dict_["entity_name"],
            bq_dataset_id=dict_["bq_dataset_id"],
            schema_folder=dict_["schema_folder"],
            start_date=pendulum.parse(dict_["start_date"]),
            end_date=pendulum.parse(dict_["end_date"]),
            manifest=Manifest.from_dict(dict_["manifest"]),
            merged_ids=[MergedId.from_dict(merged_id) for merged_id in dict_["merged_ids"]],
            is_first_run=dict_["is_first_run"],
            prev_end_date=pendulum.parse(dict_["prev_end_date"]),
        )

    def to_dict(self) -> Dict:
        return dict(
            dag_id=self.dag_id,
            run_id=self.run_id,
            cloud_workspace=self.cloud_workspace.to_dict(),
            entity_name=self.entity_name,
            bq_dataset_id=self.bq_dataset_id,
            schema_folder=self.schema_folder,
            start_date=self.start_date.isoformat(),
            end_date=self.end_date.isoformat(),
            manifest=self.manifest.to_dict(),
            merged_ids=[merged_id.to_dict() for merged_id in self.merged_ids],
            is_first_run=self.is_first_run,
            prev_end_date=self.prev_end_date.isoformat(),
        )


def create_dag(
    *,
    dag_id: str,
    cloud_workspace: CloudWorkspace,
    bq_dataset_id: str = "openalex",
    bq_upsert_byte_limit: int = UPSERT_BYTE_LIMIT,
    entity_names: List[str] = None,
    schema_folder: str = project_path("openalex_telescope", "schema"),
    dataset_description: str = "The OpenAlex dataset: https://docs.openalex.org/",
    temp_table_expiry_days: int = 7,
    snapshot_expiry_days: int = 31,
    n_transfer_trys: int = 3,
    max_processes: int = os.cpu_count(),
    primary_key: str = "id",
    aws_conn_id: str = "aws_openalex",
    aws_openalex_bucket: str = "openalex",
    observatory_api_conn_id: str = AirflowConns.OBSERVATORY_API,
    slack_conn_id: Optional[str] = AirflowConns.SLACK,
    start_date: pendulum.DateTime = pendulum.datetime(2021, 12, 1),
    schedule: str = "@weekly",
    queue: str = "remote_queue",
    max_active_runs: int = 1,
    retries: int = 3,
) -> DAG:
    """Construct an OpenAlexTelescope instance.

    :param dag_id: the id of the DAG.
    :param cloud_workspace: the cloud workspace settings.
    :param bq_dataset_id: the BigQuery dataset id.
    :param bq_upsert_byte_limit: the BigQuery byte limit for a single query when upserting data, in bytes.
    :param entity_names: the names of the OpenAlex entities to process.
    :param schema_folder: the SQL schema path.
    :param dataset_description: description for the BigQuery dataset.
    :param temp_table_expiry_days: the number of days until the upsert or delete tables will expire.
    :param snapshot_expiry_days: the number of days that a snapshot of each entity's main table will take to expire,
    which is set to 31 days so there is some time to rollback after an update.
    :param n_transfer_trys: how to many times to transfer data from AWS to GCS.
    :param max_processes: the maximum number of processes to use when transforming data.
    :param aws_conn_id: the AWS Airflow Connection ID.
    :param aws_openalex_bucket: the OpenAlex AWS bucket name.
    :param observatory_api_conn_id: the Observatory API Airflow Connection ID.
    :param slack_conn_id: the Slack Connection ID.
    :param start_date: the Apache Airflow DAG start date.
    :param schedule: the Apache Airflow schedule interval. Whilst OpenAlex snapshots are released monthly,
    they are not released on any particular day of the month, so we instead simply run the workflow weekly on a
    Sunday as this will pickup new updates regularly. See here for past release dates: https://openalex.s3.amazonaws.com/RELEASE_NOTES.txt
    :param queue: what Airflow queue this job runs on.
    :param max_active_runs: the maximum number of DAG runs that can be run at once.
    :param retries: the number of times to retry a task.
    """

    if entity_names is None:
        entity_names = [
            "authors",
            "concepts",
            "funders",
            "institutions",
            "publishers",
            "sources",
            "works",
            "domains",
            "fields",
            "subfields",
            "topics",
        ]

    @dag(
        dag_id=dag_id,
        start_date=start_date,
        schedule=schedule,
        catchup=False,
        max_active_runs=max_active_runs,
        tags=["academic-observatory"],
        default_args={
            "owner": "airflow",
            "on_failure_callback": on_failure_callback,
            "retries": retries,
            "queue": queue,
        },
    )
    def openalex():
        @task
        def fetch_entities(**context) -> dict:
            """Fetch OpenAlex releases.

            :return: True to continue, False to skip.
            """

            dag_run = context["dag_run"]
            is_first_run = is_first_dag_run(dag_run)

            # Build up information about what files we will be downloading for this release
            entity_index = {}
            aws_key = get_aws_key(aws_conn_id)
            for entity_name in entity_names:
                releases = get_dataset_releases(dag_id=dag_id, dataset_id=entity_name)
                manifest = fetch_manifest(bucket=aws_openalex_bucket, aws_key=aws_key, entity_name=entity_name)
                merged_ids = fetch_merged_ids(bucket=aws_openalex_bucket, aws_key=aws_key, entity_name=entity_name)
                if is_first_run and len(releases) != 0:
                    raise AirflowException(
                        f"fetch_releases: there should be no DatasetReleases for dag_id={dag_id}, dataset_id={entity_name} stored in the Observatory API on the first DAG run."
                    )

                # Calculate start and end dates of files for this release
                prev_release = get_latest_dataset_release(releases, "changefile_end_date")
                prev_end_date = pendulum.instance(datetime.datetime.min)
                if prev_release is not None:
                    prev_end_date = prev_release.changefile_end_date
                new_entry_dates = [
                    entry.updated_date for entry in manifest.entries if entry.updated_date > prev_end_date
                ]
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
                    dag_id=dag_id,
                    run_id=context["run_id"],
                    cloud_workspace=cloud_workspace,
                    entity_name=entity_name,
                    bq_dataset_id=bq_dataset_id,
                    schema_folder=schema_folder,
                    start_date=start_date,
                    end_date=end_date,
                    manifest=manifest,
                    merged_ids=merged_ids,
                    is_first_run=is_first_run,
                    prev_end_date=prev_end_date,
                )
                entity_index[entity_name] = entity.to_dict()

            # If no entities created then skip
            if len(entity_index) == 0:
                logging.info(f"fetch_releases: no updates found")

            # Print summary information
            logging.info(f"is_first_run: {is_first_run}")
            logging.info(f"entities: {entity_index}")

            return entity_index

        @task.short_circuit
        def short_circuit(entity_index: dict, **context):
            return len(entity_index) > 0

        @task
        def create_dataset(*context) -> None:
            """Create datasets."""

            bq.bq_create_dataset(
                project_id=cloud_workspace.output_project_id,
                dataset_id=bq_dataset_id,
                location=cloud_workspace.data_location,
                description=dataset_description,
            )

        @task_group
        def process_entity(entity_index: dict, entity_name: str):
            @task
            def bq_snapshot(entity_index: dict, entity_name: str, **context):
                """Create a snapshot of each main table. The purpose of this table is to be able to rollback the table
                if something goes wrong. The snapshot expires after snapshot_expiry_days."""

                task_id = get_task_id(**context)
                entity = get_entity(entity_index, entity_name)

                if entity is None:
                    raise AirflowSkipException(make_no_updated_data_msg(task_id, entity_name))

                elif entity.is_first_run:
                    raise AirflowSkipException(make_first_run_message(task_id))

                expiry_date = pendulum.now().add(days=snapshot_expiry_days)
                logging.info(
                    f"{task_id}: creating backup snapshot for {entity.bq_main_table_id} as {entity.bq_snapshot_table_id} expiring on {expiry_date}"
                )
                success = bq.bq_snapshot(
                    src_table_id=entity.bq_main_table_id,
                    dst_table_id=entity.bq_snapshot_table_id,
                    expiry_date=expiry_date,
                )
                if not success:
                    raise AirflowException(
                        f"{task_id}: error creating backup snapshot for {entity.bq_main_table_id} as {entity.bq_snapshot_table_id} expiring on {expiry_date}"
                    )

                # Add a description to the snapshot table
                bq.bq_update_table_description(table_id=entity.bq_snapshot_table_id, description=TEMP_TABLE_DESCRIPTION)

            @task(trigger_rule=TriggerRule.NONE_FAILED)
            def aws_to_gcs_transfer(entity_index: dict, entity_name: str, **context):
                """Transfer files from AWS bucket to Google Cloud bucket"""

                # Make GCS Transfer Manifest for files that we need for this release
                task_id = get_task_id(**context)
                entity = get_entity(entity_index, entity_name)

                if entity is None:
                    raise AirflowSkipException(make_no_updated_data_msg(task_id, entity_name))

                object_paths = []
                for entry in entity.current_entries:
                    object_paths.append(entry.object_key)
                for merged_id in entity.current_merged_ids:
                    object_paths.append(merged_id.object_key)
                gcs_upload_transfer_manifest(object_paths, entity.transfer_manifest_uri)

                # Transfer files
                count = 0
                success = False
                aws_key = get_aws_key(aws_conn_id)
                for i in range(n_transfer_trys):
                    success, objects_count = gcs_create_aws_transfer(
                        aws_key=aws_key,
                        aws_bucket=aws_openalex_bucket,
                        include_prefixes=[],
                        gc_project_id=cloud_workspace.input_project_id,
                        gc_bucket_dst_uri=entity.gcs_openalex_data_uri,
                        description=f"Transfer OpenAlex {entity.entity_name} from AWS to GCS",
                        transfer_manifest=entity.transfer_manifest_uri,
                    )
                    logging.info(
                        f"gcs_create_aws_transfer: try={i + 1}/{n_transfer_trys}, success={success}, objects_count={objects_count}"
                    )
                    count += objects_count
                    if success:
                        break

                logging.info(f"gcs_create_aws_transfer: success={success}, total_object_count={count}")
                if not success:
                    raise AirflowException("Google Storage Transfer unsuccessful")

                # After the transfer, verify the manifests and merged_ids are the same as when we fetched them during
                # the fetch_releases task. If they are the same, the data did not change during transfer. If the
                # manifests do not match then the data has changed, and we need to restart the DAG run manually.
                # See step 3 : https://docs.openalex.org/download-all-data/snapshot-data-format#the-manifest-file
                current_manifest = fetch_manifest(
                    bucket=aws_openalex_bucket, aws_key=aws_key, entity_name=entity.entity_name
                )
                current_merged_ids = fetch_merged_ids(
                    bucket=aws_openalex_bucket, aws_key=aws_key, entity_name=entity.entity_name
                )

                msgs = []
                manifest_changed = entity.manifest != current_manifest
                merged_ids_changed = entity.merged_ids != current_merged_ids

                if manifest_changed:
                    msg = f"OpenAlexEntity({entity.entity_name}) manifests have changed"
                    logging.error(f"aws_to_gcs_transfer: {msg}")
                    msgs.append(msg)

                if merged_ids_changed:
                    msg = "OpenAlexEntity({entity.entity_name}) merged_ids have changed"
                    logging.error(f"aws_to_gcs_transfer: {msg}")
                    msgs.append(msg)

                if not manifest_changed and not merged_ids_changed:
                    logging.info(f"aws_to_gcs_transfer: manifests and merged_ids the same")
                else:
                    raise AirflowException(f"aws_to_gcs_transfer: {' ,'.join(msgs)}")

            @task(trigger_rule=TriggerRule.NONE_FAILED)
            def download(entity_index: dict, entity_name: str, **context):
                """Download files for an entity from the bucket.

                Gsutil is used instead of the standard Google Cloud Python library, because it is faster at downloading files
                than the Google Cloud Python library.
                """

                task_id = get_task_id(**context)
                entity = get_entity(entity_index, entity_name)

                if entity is None:
                    raise AirflowSkipException(make_no_updated_data_msg(task_id, entity_name))

                output_folder = f"{entity.download_folder}/data/{entity.entity_name}/"
                bucket_path = f"{entity.gcs_openalex_data_uri}data/{entity.entity_name}/*"
                op = BashOperator(
                    task_id="process_entity.download",
                    bash_command="mkdir -p "
                    + output_folder
                    + " && gcloud auth activate-service-account --key-file=${GOOGLE_APPLICATION_CREDENTIALS}"
                    + f" && gsutil -m -q cp -L {entity.log_path} -r "
                    + bucket_path
                    + " "
                    + output_folder,
                    do_xcom_push=False,
                )
                op.execute(context)

            @task(trigger_rule=TriggerRule.NONE_FAILED)
            def transform(entity_index: dict, entity_name: str, **context):
                """Transform all files for the Work, Concept and Institution entities. Transforms one file per process.

                This step also scans through each file and generates a Biguqery style schema from the incoming data."""

                task_id = get_task_id(**context)
                entity = get_entity(entity_index, entity_name)

                if entity is None:
                    raise AirflowSkipException(make_no_updated_data_msg(task_id, entity_name))

                # Cleanup in case we re-run task
                output_folder = os.path.join(entity.transform_folder, "data", entity.entity_name)
                logging.info(f"{task_id}: cleaning path: {output_folder}")
                if os.path.exists(output_folder):
                    clean_dir(output_folder)

                # These will all get executed as different tasks, so only use many processes for works which is the largest
                mp = 1
                if entity.entity_name == "authors":
                    mp = 4
                elif entity.entity_name == "works":
                    mp = max(1, max_processes - 4)
                logging.info(
                    f"{task_id}: transforming files for OpenAlexEntity({entity.entity_name}), no. workers: {mp}"
                )

                # Initialise schema generator
                merged_schema_map = OrderedDict()

                with ProcessPoolExecutor(max_workers=mp) as executor:
                    futures = []
                    for entry in entity.current_entries:
                        input_path = os.path.join(entity.download_folder, entry.object_key)
                        output_path = os.path.join(entity.transform_folder, entry.object_key)
                        futures.append(executor.submit(transform_file, input_path, output_path))
                    for future in as_completed(futures):
                        input_path, schema_map, schema_error = future.result()

                        if schema_error:
                            logging.info(f"Error generating schema for file {input_path}: {schema_error}")

                        # Merge the schemas from each process. Each data file could have more fields than others.
                        merged_schema_map = merge_schema_maps(to_add=schema_map, old=merged_schema_map)

                # Flatten schema from nested OrderedDicts to a regular Bigquery schema.
                merged_schema = flatten_schema(schema_map=merged_schema_map)

                # Save schema to file
                with open(entity.generated_schema_path, mode="w") as f_out:
                    json.dump(merged_schema, f_out, indent=2)

            @task(trigger_rule=TriggerRule.NONE_FAILED)
            def upload_schema(entity_index: dict, entity_name: str, **context):
                """Upload the generated schema from the transform step to GCS."""

                task_id = get_task_id(**context)
                entity = get_entity(entity_index, entity_name)

                if entity is None:
                    raise AirflowSkipException(make_no_updated_data_msg(task_id, entity_name))

                success = gcs_upload_files(
                    bucket_name=cloud_workspace.transform_bucket, file_paths=[entity.generated_schema_path]
                )
                if not success:
                    raise AirflowException("upload_schema: error uploading schema")

            @task(trigger_rule=TriggerRule.NONE_FAILED)
            def compare_schemas(entity_index: dict, entity_name: str, **context):
                """Compare the generated schema against the expected schema for each entity."""

                task_id = get_task_id(**context)
                entity = get_entity(entity_index, entity_name)

                if entity is None:
                    raise AirflowSkipException(make_no_updated_data_msg(task_id, entity_name))

                logging.info(f"Loading schemas from file: generated: {entity.generated_schema_path}")
                logging.info(f"Expected: {entity.schema_file_path}")

                # Read in the expected schema for the entity.
                merged_schema = load_json(entity.generated_schema_path)
                expected_schema = load_json(entity.schema_file_path)

                try:
                    match = bq_compare_schemas(expected_schema, merged_schema, check_types_match=False)
                except:
                    match = False

                if not match:
                    logging.info("Generated schema and expected do not match! - Sending a notification via Slack")
                    slack_msg = f"Found differences in the OpenAlex entity {entity.entity_name} data structure for the data dump vs pre-defined Bigquery schema. Please investigate."
                    ti: TaskInstance = context["ti"]
                    execution_date = context["execution_date"]
                    send_slack_msg(
                        ti=ti, execution_date=execution_date, comments=slack_msg, slack_conn_id=slack_conn_id
                    )

            @task(trigger_rule=TriggerRule.NONE_FAILED)
            def upload_files(entity_index: dict, entity_name: str, **context):
                """Upload the transformed data to Cloud Storage.
                :raises AirflowException: Raised if the files to be uploaded are not found."""

                task_id = get_task_id(**context)
                entity = get_entity(entity_index, entity_name)

                if entity is None:
                    raise AirflowSkipException(make_no_updated_data_msg(task_id, entity_name))

                # Make files to upload
                file_paths = []
                for entry in entity.current_entries:
                    file_path = os.path.join(entity.transform_folder, entry.object_key)
                    file_paths.append(file_path)

                # Upload files
                success = gcs_upload_files(bucket_name=cloud_workspace.transform_bucket, file_paths=file_paths)
                if not success:
                    raise AirflowException("upload_files: error uploading files to cloud storage")

            @task(trigger_rule=TriggerRule.NONE_FAILED)
            def bq_load_table(entity_index: dict, entity_name: str, **context):
                """Load the main or upsert table for an entity."""

                task_id = get_task_id(**context)
                entity = get_entity(entity_index, entity_name)

                if entity is None:
                    raise AirflowSkipException(make_no_updated_data_msg(task_id, entity_name))

                table_name = "main"
                table_id = entity.bq_main_table_id
                description = entity.table_description
                if not entity.is_first_run:
                    table_name = "upsert"
                    table_id = entity.bq_upsert_table_id
                    description = TEMP_TABLE_DESCRIPTION

                if entity.is_first_run and bq.bq_table_exists(table_id):
                    raise AirflowException(
                        f"{task_id}: main table {entity.bq_main_table_id} for OpenAlexEntity({entity.entity_name}) should not exists"
                    )

                logging.info(f"{task_id}: loading OpenAlexEntity({entity.entity_name}) {table_name} table {table_id}")
                success = bq.bq_load_table(
                    uri=entity.data_uri,
                    table_id=table_id,
                    schema_file_path=entity.schema_file_path,
                    source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                    ignore_unknown_values=True,
                    table_description=description,
                )
                if not success:
                    raise AirflowException(
                        f"{task_id}: error loading OpenAlexEntity({entity.entity_name}) {table_name} table {table_id}"
                    )

                if not entity.is_first_run:
                    logging.info(f"Setting expiry time of {temp_table_expiry_days} days on {table_id}")
                    bq_set_table_expiry(table_id=table_id, days=temp_table_expiry_days)

            @task(trigger_rule=TriggerRule.NONE_FAILED)
            def bq_upsert_records(entity_index: dict, entity_name: str, **context):
                """Upsert the records from each upserts table into the main table."""

                task_id = get_task_id(**context)
                entity = get_entity(entity_index, entity_name)

                if entity is None:
                    raise AirflowSkipException(make_no_updated_data_msg(task_id, entity_name))

                elif entity.is_first_run:
                    raise AirflowSkipException(make_first_run_message(task_id))

                # Upsert records from upsert table to main table
                logging.info(
                    f"{task_id}: upserting OpenAlexEntity({entity.entity_name}) records from {entity.bq_upsert_table_id} to {entity.bq_main_table_id}"
                )
                bq.bq_upsert_records(
                    main_table_id=entity.bq_main_table_id,
                    upsert_table_id=entity.bq_upsert_table_id,
                    primary_key=primary_key,
                    bytes_budget=bq_upsert_byte_limit,
                )

            @task(trigger_rule=TriggerRule.NONE_FAILED)
            def bq_load_delete_table(entity_index: dict, entity_name: str, **context):
                """Load the delete tables."""

                task_id = get_task_id(**context)
                entity = get_entity(entity_index, entity_name)

                if entity is None:
                    raise AirflowSkipException(make_no_updated_data_msg(task_id, entity_name))

                elif entity.is_first_run:
                    raise AirflowSkipException(make_first_run_message(task_id))

                elif not entity.has_merged_ids:
                    raise AirflowSkipException(make_no_merged_ids_msg(task_id, entity.entity_name))

                logging.info(
                    f"{task_id}: loading OpenAlexEntity({entity.entity_name}) delete table {entity.bq_delete_table_id}"
                )
                success = bq.bq_load_table(
                    uri=entity.merged_ids_uri,
                    table_id=entity.bq_delete_table_id,
                    schema_file_path=entity.merged_ids_schema_file_path,
                    source_format=SourceFormat.CSV,
                    csv_skip_leading_rows=1,
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                    ignore_unknown_values=True,
                    table_description=TEMP_TABLE_DESCRIPTION,
                )
                if not success:
                    raise AirflowException(
                        f"{task_id}: error loading OpenAlexEntity({entity.entity_name}) delete table {entity.bq_delete_table_id}"
                    )

                if not entity.is_first_run:
                    logging.info(f"Setting expiry time of {temp_table_expiry_days} days on {entity.bq_delete_table_id}")
                    bq_set_table_expiry(table_id=entity.bq_delete_table_id, days=temp_table_expiry_days)

            @task(trigger_rule=TriggerRule.NONE_FAILED)
            def bq_delete_records(entity_index: dict, entity_name: str, **context):
                """Delete records from main tables that are in delete tables."""

                task_id = get_task_id(**context)
                entity = get_entity(entity_index, entity_name)

                if entity is None:
                    raise AirflowSkipException(make_no_updated_data_msg(task_id, entity_name))

                elif entity.is_first_run:
                    raise AirflowSkipException(make_first_run_message(task_id))

                elif not entity.has_merged_ids:
                    raise AirflowSkipException(make_no_merged_ids_msg(task_id, entity.entity_name))

                logging.info(
                    f"{task_id}: deleting OpenAlexEntity({entity.entity_name}) records in {entity.bq_main_table_id} from {entity.bq_delete_table_id}"
                )
                bq.bq_delete_records(
                    main_table_id=entity.bq_main_table_id,
                    delete_table_id=entity.bq_delete_table_id,
                    main_table_primary_key=primary_key,
                    delete_table_primary_key=primary_key,
                    delete_table_primary_key_prefix="https://openalex.org/",
                )

            @task(trigger_rule=TriggerRule.NONE_FAILED)
            def add_dataset_release(entity_index: dict, entity_name: str, **context) -> None:
                """Adds release information to API."""

                task_id = get_task_id(**context)
                entity = get_entity(entity_index, entity_name)

                if entity is None:
                    raise AirflowSkipException(make_no_updated_data_msg(task_id, entity_name))

                logging.info(f"{task_id}: creating dataset release for OpenAlexEntity({entity.entity_name})")
                dataset_release = DatasetRelease(
                    dag_id=dag_id,
                    dataset_id=entity.entity_name,
                    dag_run_id=entity.run_id,
                    changefile_start_date=entity.start_date,
                    changefile_end_date=entity.end_date,
                )
                logging.info(f"{task_id}: dataset_release={dataset_release}")
                api = make_observatory_api(observatory_api_conn_id=observatory_api_conn_id)
                api.post_dataset_release(dataset_release)

            task_bq_snapshot = bq_snapshot(entity_index, entity_name)
            task_aws_to_gcs_transfer = aws_to_gcs_transfer(entity_index, entity_name)
            task_download = download(entity_index, entity_name)
            task_transform = transform(entity_index, entity_name)
            task_upload_schema = upload_schema(entity_index, entity_name)
            task_compare_schemas = compare_schemas(entity_index, entity_name)
            task_upload_files = upload_files(entity_index, entity_name)
            task_bq_load_table = bq_load_table(entity_index, entity_name)
            task_bq_upsert_records = bq_upsert_records(entity_index, entity_name)
            task_bq_load_delete_table = bq_load_delete_table(entity_index, entity_name)
            task_bq_delete_records = bq_delete_records(entity_index, entity_name)
            task_add_dataset_release = add_dataset_release(entity_index, entity_name)

            (
                task_bq_snapshot
                >> task_aws_to_gcs_transfer
                >> task_download
                >> task_transform
                >> task_upload_schema
                >> task_compare_schemas
                >> task_upload_files
                >> task_bq_load_table
                >> task_bq_upsert_records
                >> task_bq_load_delete_table
                >> task_bq_delete_records
                >> task_add_dataset_release
            )

        @task(trigger_rule=TriggerRule.NONE_FAILED)
        def cleanup_workflow(**context) -> None:
            """Delete all files, folders and XComs associated with this release."""

            workflow_folder = make_workflow_folder(dag_id, context["run_id"])
            cleanup(dag_id=dag_id, execution_date=context["logical_date"], workflow_folder=workflow_folder)

        external_task_id = "dag_run_complete"
        sensor = PreviousDagRunSensor(
            dag_id=dag_id,
            external_task_id=external_task_id,
        )
        task_check_dependencies = check_dependencies(airflow_conns=[observatory_api_conn_id, aws_conn_id])
        xcom_entity_index = fetch_entities()
        task_short_circuit = short_circuit(xcom_entity_index)
        task_create_dataset = create_dataset()

        # Process each entity
        # We don't use .expand because we want each entity to be a first class citizen in the graph UI
        # Additionally, string based entity names for mapped dynamic tasks are only set once each task has been
        # run, which could be several days for certain OpenAlex tables
        process_entities = []
        for group_id in entity_names:
            t = process_entity.override(group_id=group_id)(entity_index=xcom_entity_index, entity_name=group_id)
            process_entities.append(t)

        task_cleanup_workflow = cleanup_workflow()
        task_dag_run_complete = EmptyOperator(
            task_id=external_task_id,
        )

        (
            sensor
            >> task_check_dependencies
            >> xcom_entity_index
            >> task_short_circuit
            >> task_create_dataset
            >> process_entities
            >> task_cleanup_workflow
            >> task_dag_run_complete
        )

    return openalex()


def get_entity(entity_index: dict, entity_name: str) -> Optional[OpenAlexEntity]:
    if entity_name not in entity_index:
        return None

    return OpenAlexEntity.from_dict(entity_index[entity_name])


def get_task_id(**context):
    return context["ti"].task_id


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


def get_aws_key(aws_conn_id: str) -> Tuple[str, str]:
    """Get the AWS access key id and secret access key from the aws_conn_id airflow connection.

    :return: access key id and secret access key
    """

    conn = BaseHook.get_connection(aws_conn_id)
    access_key_id = conn.login
    secret_key = conn.password

    if access_key_id is None:
        raise ValueError(f"OpenAlexTelescope.aws_key: {aws_conn_id} login is None")

    if secret_key is None:
        raise ValueError(f"OpenAlexTelescope.aws_key: {aws_conn_id} password is None")

    return access_key_id, secret_key


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
        if object_key is None:
            raise ValueError(f"object_key for url={self.url} is None")
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
        if object_key is None:
            raise ValueError(f"object_key for url={self.url} is None")
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


def transform_file(download_path: str, transform_path: str) -> Tuple[OrderedDict, list]:
    """Transforms a single file.
    Each entry/object in the gzip input file is transformed and the transformed object is immediately written out to
    a gzip file. For each entity only one field has to be transformed.

    This function generates and returnms a Bigquery style schema from the transformed object,
    using the ScehmaGenerator from the 'bigquery_schema_generator' package.

    :param download_path: The path to the file with the OpenAlex entries.
    :param transform_path: The path where transformed data will be saved.
    :return: schema_map. A nested OrderedDict object produced by the SchemaGenertaor.
    :return: schema_generator.error_logs: Possible error logs produced by the SchemaGenerator.
    """

    # Make base folder, e.g. authors/updated_date=2023-09-17
    base_folder = os.path.dirname(transform_path)
    os.makedirs(base_folder, exist_ok=True)

    # Initialise the schema generator.
    schema_map = OrderedDict()
    schema_generator = SchemaGenerator(input_format="dict")

    logging.info(f"Transforming {download_path}")
    with gzip.open(download_path, "rb") as f_in, gzip.open(transform_path, "wt", encoding="ascii") as f_out:
        reader = jsonlines.Reader(f_in)
        for obj in reader.iter(skip_empty=True):
            transform_object(obj)

            # Wrap this in a try and pass so that it doesn't
            # cause the transform step to fail unexpectedly.
            try:
                schema_generator.deduce_schema_for_record(obj, schema_map)
            except Exception:
                pass

            json.dump(obj, f_out)
            f_out.write("\n")

    logging.info(f"Finished transform, saved to {transform_path}")

    return download_path, schema_map, schema_generator.error_logs


def clean_array_field(obj: dict, field: str):
    if field in obj:
        value = obj.get(field) or []
        obj[field] = [x for x in value if x is not None]


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
    array_fields = ["corresponding_institution_ids", "corresponding_author_ids", "societies", "alternate_titles"]
    for field in array_fields:
        clean_array_field(obj, field)

    # Remove nulls from authors affiliations[].years
    for affiliation in obj.get("affiliations", []):
        if "years" in affiliation:
            affiliation["years"] = [x for x in affiliation["years"] if x is not None]

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


def bq_compare_schemas(expected: List[dict], actual: List[dict], check_types_match: Optional[bool] = False) -> bool:
    """Compare two Bigquery style schemas for if they have the same fields and/or data types.

    :param expected: the expected schema.
    :param actual: the actual schema.
    :check_types_match: Optional, if checking data types of fields is required.
    :return: whether the expected and actual match.
    """

    expected.sort(key=lambda c: c["name"], reverse=False)
    actual.sort(key=lambda c: c["name"], reverse=False)

    exp_names = [field_def["name"] for field_def in expected]
    act_names = [field_def["name"] for field_def in actual]

    if len(exp_names) != len(act_names):
        logging.info("Fields do not match:")
        logging.info(f"Only in expected: {set(exp_names) - set(act_names)}")
        logging.info(f"Only in actual: {set(act_names) - set(exp_names)}")
        return False

    # Check data types of fields
    if check_types_match:
        for exp_field, act_field in zip(expected, actual):
            if exp_field["type"] != act_field["type"]:
                logging.info(
                    f"Field types do not match for field  '{exp_field['name']}' ! Actual: {act_field['type']} vs Expected: {exp_field['type']}"
                )
            all_matched = False

    # Check for sub-fields within the schema.
    all_matched = True
    for exp_field, act_field in zip(expected, actual):
        # Ignore the "mode" and "description" definitions in fields as they are not required for check.
        diff = DeepDiff(exp_field, act_field, ignore_order=True, exclude_regex_paths=r"\s*(description|mode)")
        for diff_type, changes in diff.items():
            all_matched = False
            log_diff(diff_type, changes)

        if "fields" in exp_field and not "fields" in act_field:
            logging.info(f"Fields are present under expected but not in actual! Field name: {exp_field['name']}")
            all_mathced = False
        elif not "fields" in exp_field and "fields" in act_field:
            logging.info(f"Fields are present under actual but not in expected! Field name: {act_field['name']}")
            all_matched = False
        elif "fields" in exp_field and "fields" in act_field:
            all_matched = bq_compare_schemas(exp_field["fields"], act_field["fields"], check_types_match)

    return all_matched


def merge_schema_maps(to_add: OrderedDict, old: OrderedDict) -> OrderedDict:
    """Using the SchemaGenerator from the bigquery_schema_generator library, merge the schemas found
    when from scanning through files into one large nested OrderedDict.

    :param to_add: The incoming schema to add to the existing "old" schema.
    :param old: The existing old schema with previously populated values.
    :return: The old schema with newly added fields.
    """

    schema_generator = SchemaGenerator()

    if old:
        # Loop through the fields to add to the schema
        for key, value in to_add.items():
            if key in old:
                # Merge existing fields together.
                old[key] = schema_generator.merge_schema_entry(old_schema_entry=old[key], new_schema_entry=value)
            else:
                # New top level field is added.
                old[key] = value
    else:
        # Initialise it with first result if it is empty
        old = to_add.copy()

    return old


def flatten_schema(schema_map: OrderedDict) -> dict:
    """A quick trick using the JSON encoder and load string function to convert from a nested
    OrderedDict object to a regular dictionary.

    :param schema_map: The generated schema from SchemaGenerator.
    :return schema: A Bigquery style schema."""

    encoded_schema = JSONEncoder().encode(
        flatten_schema_map(
            schema_map,
            keep_nulls=False,
            sorted_schema=True,
            infer_mode=True,
            input_format="json",
        )
    )

    return json.loads(encoded_schema)


def load_json(file_path: str) -> Any:
    """Read in a *.json file."""

    with open(file_path, "r") as f_in:
        data = json.load(f_in)

    return data


def bq_set_table_expiry(*, table_id: str, days: int):
    """Set the expiry time for a BigQuery table.

    :param table_id: the fully qualified BigQuery table identifier.
    :param days: the number of days from now until the table expires.
    :return:
    """

    client = bigquery.Client()
    table = bigquery.Table(table_id)
    table.expires = pendulum.now().add(days=days)
    client.update_table(table, ["expires"])
