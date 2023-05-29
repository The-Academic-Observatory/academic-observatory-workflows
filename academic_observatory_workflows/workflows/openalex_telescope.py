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

import datetime
import gzip
import json
import logging
import os
import re
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import timedelta
from typing import List, Dict, Tuple, Optional

import boto3
import jsonlines
import pendulum
from airflow.hooks.base import BaseHook
from airflow.models.taskinstance import TaskInstance
from airflow.operators.dummy import DummyOperator
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
        transform: bool,
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
        :param transform: whether the data for the entity needs to be downloaded and transformed locally, or whether
        it can be loaded straight into BigQuery.
        :param start_date: the start date of the files covered by this release (inclusive).
        :param end_date: the end date of the files covered by this release (inclusive).
        :param manifest: the Redshift manifest provided by OpenAlex for this entity.
        :param merged_ids: the MergedIds provided by OpenAlex for this entity.
        :param is_first_run: whether this is the first run or not.
        :param prev_end_date: the previous end date.
        :param release: the release object.
        """

        self.entity_name = entity_name
        self.transform = transform
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
                and self.transform == other.transform
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
        if self.transform:
            return gcs_blob_uri(
                self.release.cloud_workspace.transform_bucket,
                f"{gcs_blob_name_from_path(self.release.transform_folder)}/data/{self.entity_name}/*",
            )
        else:
            return gcs_blob_uri(
                self.release.cloud_workspace.download_bucket,
                f"{gcs_blob_name_from_path(self.release.download_folder)}/data/{self.entity_name}/*",
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
        transform = dict_["transform"]
        start_date = pendulum.parse(dict_["start_date"])
        end_date = pendulum.parse(dict_["end_date"])
        manifest = Manifest.from_dict(dict_["manifest"])
        merged_ids = [MergedId.from_dict(merged_id) for merged_id in dict_["merged_ids"]]
        is_first_run = dict_["is_first_run"]
        prev_end_date = pendulum.parse(dict_["prev_end_date"])
        return OpenAlexEntity(
            entity_name, transform, start_date, end_date, manifest, merged_ids, is_first_run, prev_end_date
        )

    def to_dict(self) -> Dict:
        return dict(
            entity_name=self.entity_name,
            transform=self.transform,
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
        self.gsutil_transfer_wildcard = (
            "{" + ",".join([entity.entity_name for entity in entities if entity.transform]) + "}"
        )


class OpenAlexTelescope(Workflow):
    """OpenAlex telescope"""

    def __init__(
        self,
        *,
        dag_id: str,
        cloud_workspace: CloudWorkspace,
        bq_dataset_id: str = "openalex",
        entities: List[Tuple[str, bool]] = None,
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
        schedule_interval: str = "@weekly",
        queue: str = "remote_queue",
    ):
        """Construct an OpenAlexTelescope instance.

        :param dag_id: the id of the DAG.
        :param cloud_workspace: the cloud workspace settings.
        :param bq_dataset_id: the BigQuery dataset id.
        :param entities: these are tuples of entity names and whether the given entity needs to be transformed or not,
        which affects whether the entity data needs to be downloaded and transformed and what storage bucket the data
        will be loaded into BigQuery from.
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
        :param schedule_interval: the Apache Airflow schedule interval. Whilst OpenAlex snapshots are released monthly,
        they are not released on any particular day of the month, so we instead simply run the workflow weekly on a
        Sunday as this will pickup new updates regularly. See here for past release dates: https://openalex.s3.amazonaws.com/RELEASE_NOTES.txt
        :param queue:
        """

        if entities is None:
            entities = [
                ("concepts", True),
                ("institutions", True),
                ("works", True),
                ("authors", False),
                ("publishers", False),
                ("sources", False),
                ("funders", False),
            ]

        super().__init__(
            dag_id=dag_id,
            start_date=start_date,
            schedule_interval=schedule_interval,
            catchup=False,
            airflow_conns=[observatory_api_conn_id, aws_conn_id],
            queue=queue,
            tags=[Tag.academic_observatory],
        )
        self.cloud_workspace = cloud_workspace
        self.bq_dataset_id = bq_dataset_id
        self.entities = entities
        self.schema_folder = schema_folder
        self.dataset_description = dataset_description
        self.snapshot_expiry_days = snapshot_expiry_days
        self.n_transfer_trys = n_transfer_trys
        self.max_processes = max_processes
        self.primary_key = primary_key
        self.aws_conn_id = aws_conn_id
        self.aws_openalex_bucket = aws_openalex_bucket
        self.observatory_api_conn_id = observatory_api_conn_id

        # Wait for the previous DAG run to finish to make sure that
        # changefiles are processed in the correct order
        external_task_id = "dag_run_complete"
        self.add_operator(
            PreviousDagRunSensor(
                dag_id=self.dag_id,
                external_task_id=external_task_id,
                execution_delta=timedelta(days=7),  # To match the @weekly schedule_interval
            )
        )
        self.add_setup_task(self.check_dependencies)
        self.add_setup_task(self.fetch_releases)
        self.add_task(self.create_datasets)

        # Create snapshots of main tables in case we mess up
        # This is done before updating the tables to make sure that the snapshots haven't expired before the tables
        # are updated
        self.add_task(self.bq_create_main_table_snapshots)

        # Transfer, download and transform data
        self.add_task(self.aws_to_gcs_transfer)

        # Download concepts, institutions and works which need to be pre-processed
        for entity_name, transform in self.entities:
            if transform:
                self.add_operator(make_download_bash_op(self, entity_name))
        self.add_task(self.transform)

        # Upsert records
        self.add_task(self.upload_upsert_files)
        self.add_task(self.bq_load_upsert_tables)
        self.add_task(self.bq_upsert_records)

        # Delete records
        self.add_task(self.bq_load_delete_tables)
        self.add_task(self.bq_delete_records)

        # Add release info to API and cleanup
        self.add_task(self.add_new_dataset_releases)
        self.add_task(self.cleanup)

        # The last task that the next DAG run's ExternalTaskSensor waits for.
        self.add_operator(
            DummyOperator(
                task_id=external_task_id,
            )
        )

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
        for entity_name, transform in self.entities:
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
                logging.info(
                    f"fetch_releases: skipping OpenAlexEntity(entity_name={entity_name}) as it has no updated data"
                )
                continue

            logging.info(
                f"fetch_releases: adding OpenAlexEntity(entity_name={entity_name}, transform={transform}, start_date={start_date}, end_date={end_date})"
            )

            # Save metadata
            entity = OpenAlexEntity(
                entity_name, transform, start_date, end_date, manifest, merged_ids, is_first_run, prev_end_date
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

    def create_datasets(self, release: OpenAlexRelease, **kwargs) -> None:
        """Create datasets."""

        bq_create_dataset(
            project_id=self.cloud_workspace.output_project_id,
            dataset_id=self.bq_dataset_id,
            location=self.cloud_workspace.data_location,
            description=self.dataset_description,
        )

    def bq_create_main_table_snapshots(self, release: OpenAlexRelease, **kwargs):
        """Create a snapshot of each main table. The purpose of this table is to be able to rollback the table
        if something goes wrong. The snapshot expires after self.snapshot_expiry_days."""

        if release.is_first_run:
            logging.info(f"bq_create_main_table_snapshots: skipping as snapshots are not created on the first run")
            return

        for entity in release.entities:
            expiry_date = pendulum.now().add(days=self.snapshot_expiry_days)
            logging.info(
                f"bq_create_main_table_snapshots: creating backup snapshot for {entity.bq_main_table_id} as {entity.bq_snapshot_table_id} expiring on {expiry_date}"
            )
            success = bq_snapshot(
                src_table_id=entity.bq_main_table_id, dst_table_id=entity.bq_snapshot_table_id, expiry_date=expiry_date
            )
            assert (
                success
            ), f"bq_create_main_table_snapshots: error creating backup snapshot for {entity.bq_main_table_id} as {entity.bq_snapshot_table_id} expiring on {expiry_date}"

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
                logging.error(f"aws_to_gcs_transfer: entity {entity.entity_name} manifests have changed")
                success = False
            if entity.merged_ids != current_merged_ids:
                logging.error(f"aws_to_gcs_transfer: entity {entity.entity_name} merged_ids have changed")
                success = False

        if success:
            logging.info(f"aws_to_gcs_transfer: manifests and merged_ids the same")
        set_task_state(success, self.aws_to_gcs_transfer.__name__, release)

    def transform(self, release: OpenAlexRelease, **kwargs):
        """Transform all files for the Work, Concept and Institution entities. Transforms one file per process."""
        logging.info(f"Transforming files, no. workers: {self.max_processes}")

        clean_dir(release.transform_folder)

        with ProcessPoolExecutor(max_workers=self.max_processes) as executor:
            futures = []
            for entity in release.entities:
                if entity.transform:
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
            if entity.transform:
                for entry in entity.current_entries:
                    file_path = os.path.join(release.transform_folder, entry.object_key)
                    file_paths.append(file_path)

        # Upload files
        success = gcs_upload_files(bucket_name=self.cloud_workspace.transform_bucket, file_paths=file_paths)
        set_task_state(success, self.upload_upsert_files.__name__, release)

    def bq_load_upsert_tables(self, release: OpenAlexRelease, **kwargs):
        """Load the upsert table for each entity."""

        for entity in release.entities:
            logging.info(
                f"bq_load_upsert_tables: loading {entity.entity_name} upsert table {entity.bq_upsert_table_id}"
            )
            success = bq_load_table(
                uri=entity.upsert_uri,
                table_id=entity.bq_upsert_table_id,
                schema_file_path=entity.schema_file_path,
                source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                ignore_unknown_values=False,
            )
            assert (
                success
            ), f"bq_load_upsert_tables: error loading {entity.entity_name} upsert table {entity.bq_upsert_table_id}"

    def bq_upsert_records(self, release: OpenAlexRelease, **kwargs):
        """Upsert the records from each upserts table into the main table."""

        for entity in release.entities:
            # Create main table if it doesn't exist
            if not bq_table_exists(entity.bq_main_table_id):
                logging.info(
                    f"bq_upsert_records: creating empty {entity.entity_name} main table {entity.bq_main_table_id}"
                )
                bq_create_empty_table(table_id=entity.bq_main_table_id, schema_file_path=entity.schema_file_path)
                bq_update_table_description(table_id=entity.bq_main_table_id, description=entity.table_description)

            # Upsert records from upsert table to main table
            logging.info(
                f"bq_upsert_records: upserting {entity.entity_name} records from {entity.bq_upsert_table_id} to {entity.bq_main_table_id}"
            )
            bq_upsert_records(
                main_table_id=entity.bq_main_table_id,
                upsert_table_id=entity.bq_upsert_table_id,
                primary_key=self.primary_key,
            )

    def bq_load_delete_tables(self, release: OpenAlexRelease, **kwargs):
        """Load the delete tables."""

        for entity in release.entities:
            if entity.has_merged_ids:
                logging.info(
                    f"bq_load_delete_tables: loading {entity.entity_name} delete table {entity.bq_delete_table_id}"
                )
                success = bq_load_table(
                    uri=entity.merged_ids_uri,
                    table_id=entity.bq_delete_table_id,
                    schema_file_path=entity.merged_ids_schema_file_path,
                    source_format=SourceFormat.CSV,
                    csv_skip_leading_rows=1,
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                    ignore_unknown_values=False,
                )
                assert (
                    success
                ), f"bq_load_delete_tables: error loading {entity.entity_name} delete table {entity.bq_delete_table_id}"
            else:
                logging.info(
                    f"bq_load_delete_tables: skipping loading delete table for {entity.entity_name} as there are no merged_ids for this entity in this release"
                )

    def bq_delete_records(self, release: OpenAlexRelease, **kwargs):
        """Delete records from main tables that are in delete tables."""

        for entity in release.entities:
            if entity.has_merged_ids:
                logging.info(
                    f"bq_delete_records: deleting {entity.entity_name} records in {entity.bq_main_table_id} from {entity.bq_delete_table_id}"
                )
                bq_delete_records(
                    main_table_id=entity.bq_main_table_id,
                    delete_table_id=entity.bq_delete_table_id,
                    main_table_primary_key=self.primary_key,
                    delete_table_primary_key=self.primary_key,
                    delete_table_primary_key_prefix="https://openalex.org/",
                )
            else:
                logging.info(
                    f"bq_load_delete_tables: skipping delete records for {entity.entity_name} as there are no merged_ids for this entity in this release"
                )

    def add_new_dataset_releases(self, release: OpenAlexRelease, **kwargs) -> None:
        """Adds release information to API."""

        for entity in release.entities:
            logging.info(f"add_new_dataset_releases: creating dataset release for {entity.entity_name}")
            dataset_release = DatasetRelease(
                dag_id=self.dag_id,
                dataset_id=entity.entity_name,
                dag_run_id=release.run_id,
                changefile_start_date=entity.start_date,
                changefile_end_date=entity.end_date,
            )
            logging.info(f"add_new_dataset_releases: dataset_release={dataset_release}")
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
        task_id=f"download_{entity_name}",
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
        raise ValueError("Invalid S3 URI. URI should start with 's3://'")

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
            obj_key = content['Key']
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
            if "works" in download_path:
                transform_object(obj, "abstract_inverted_index")
            else:
                transform_object(obj, "international")
            json.dump(obj, f_out)
            f_out.write("\n")
    logging.info(f"Finished transform, saved to {transform_path}")


def transform_object(obj: dict, field: str):
    """Transform an entry/object for one of the OpenAlex entities.
    For the Work entity only the "abstract_inverted_index" field is transformed.
    For the Concept and Institution entities only the "international" field is transformed.

    :param obj: Single object with entity information
    :param field: The field of interested that is transformed.
    :return: None.
    """
    if field == "international":
        for nested_field in obj.get(field, {}).keys():
            if not isinstance(obj[field][nested_field], dict):
                continue
            keys = list(obj[field][nested_field].keys())
            values = list(obj[field][nested_field].values())

            obj[field][nested_field] = {"keys": keys, "values": values}
    elif field == "abstract_inverted_index":
        if not isinstance(obj.get(field), dict):
            return
        keys = list(obj[field].keys())
        values = [str(value)[1:-1] for value in obj[field].values()]

        obj[field] = {"keys": keys, "values": values}
