# Copyright 2020 Curtin University
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
import logging
import os
import pathlib
import time
from concurrent.futures import ProcessPoolExecutor, as_completed, ThreadPoolExecutor
from datetime import timedelta
from typing import List, Dict, Tuple

import jsonlines
import pendulum
import requests
from airflow.exceptions import AirflowSkipException
from airflow.models.taskinstance import TaskInstance
from airflow.operators.dummy import DummyOperator
from google.cloud import bigquery
from google.cloud.bigquery import SourceFormat
from importlib_metadata import metadata
from limits import RateLimitItemPerSecond
from limits.storage import storage_from_string
from limits.strategies import FixedWindowElasticExpiryRateLimiter

from academic_observatory_workflows.config import schema_folder as default_schema_folder, Tag
from observatory.api.client.model.dataset_release import DatasetRelease
from observatory.platform.airflow import PreviousDagRunSensor, is_first_dag_run
from observatory.platform.api import get_dataset_releases, get_latest_dataset_release
from observatory.platform.api import make_observatory_api
from observatory.platform.bigquery import (
    bq_table_id,
    bq_find_schema,
    bq_load_table,
    bq_upsert_records,
    bq_snapshot,
    bq_sharded_table_id,
    bq_create_dataset,
    bq_delete_records,
)
from observatory.platform.config import AirflowConns
from observatory.platform.files import list_files, yield_jsonl, merge_update_files, save_jsonl
from observatory.platform.gcs import gcs_upload_files, gcs_blob_uri, gcs_blob_name_from_path
from observatory.platform.observatory_config import CloudWorkspace
from observatory.platform.utils.url_utils import get_user_agent, retry_get_url
from observatory.platform.workflows.workflow import Workflow, ChangefileRelease, cleanup, set_task_state

CROSSREF_EVENTS_HOST = "https://api.eventdata.crossref.org/v1/events"
DATE_FORMAT = "YYYY-MM-DD"

BACKEND = storage_from_string("memory://")
MOVING_WINDOW = FixedWindowElasticExpiryRateLimiter(BACKEND)


class CrossrefEventsRelease(ChangefileRelease):
    def __init__(
        self,
        *,
        dag_id: str,
        run_id: str,
        cloud_workspace: CloudWorkspace,
        bq_dataset_id: str,
        bq_table_name: str,
        start_date: pendulum.DateTime,
        end_date: pendulum.DateTime,
        prev_end_date: pendulum.DateTime,
        is_first_run: bool,
        mailto: str,
    ):
        """Construct a CrossrefEventsRelease instance

        :param dag_id: the id of the DAG.
        :param cloud_workspace: the CloudWorkspace instance.
        :param bq_dataset_id: the BigQuery dataset id.
        :param bq_table_name: the BigQuery table name.
        :param start_date: the start_date of the release. Inclusive.
        :param end_date: the end_date of the release. Exclusive.
        :param is_first_run: whether this is the first DAG run.
        """

        super().__init__(
            dag_id=dag_id,
            run_id=run_id,
            start_date=start_date,
            end_date=end_date,
        )
        self.prev_end_date = prev_end_date
        self.is_first_run = is_first_run
        self.download_files_regex = r".*\.jsonl$"
        self.created_files_regex = r"^created-\d{4}-\d{2}-\d{2}\.jsonl$"
        self.edited_files_regex = r"^edited-\d{4}-\d{2}-\d{2}\.jsonl$"
        self.deleted_files_regex = r"^deleted-\d{4}-\d{2}-\d{2}\.jsonl$"
        self.transform_files_regex = r".*\.jsonl$"
        self.day_requests = make_day_requests(self.start_date, self.end_date, mailto)

        self.upsert_table_file_path = os.path.join(self.transform_folder, "upsert_table.jsonl")
        self.delete_table_file_path = os.path.join(self.transform_folder, "delete_table.jsonl")

        self.main_table_uri = gcs_blob_uri(
            cloud_workspace.transform_bucket, f"{gcs_blob_name_from_path(self.transform_folder)}/*.jsonl"
        )
        self.upsert_table_uri = gcs_blob_uri(
            cloud_workspace.transform_bucket, gcs_blob_name_from_path(self.upsert_table_file_path)
        )
        self.delete_table_uri = gcs_blob_uri(
            cloud_workspace.transform_bucket, gcs_blob_name_from_path(self.delete_table_file_path)
        )

        self.bq_main_table_id = bq_table_id(cloud_workspace.output_project_id, bq_dataset_id, bq_table_name)
        self.bq_upsert_table_id = bq_table_id(
            cloud_workspace.output_project_id, bq_dataset_id, f"{bq_table_name}_upsert"
        )
        self.bq_delete_table_id = bq_table_id(
            cloud_workspace.output_project_id, bq_dataset_id, f"{bq_table_name}_delete"
        )
        self.bq_snapshot_table_id = bq_sharded_table_id(
            cloud_workspace.output_project_id, bq_dataset_id, bq_table_name, prev_end_date
        )

    @property
    def created_files(self):
        return list_files(self.download_folder, self.created_files_regex)

    @property
    def edited_files(self):
        return list_files(self.download_folder, self.edited_files_regex)

    @property
    def deleted_files(self):
        return list_files(self.download_folder, self.deleted_files_regex)

    @property
    def has_created(self):
        return len(self.created_files) > 0

    @property
    def has_edited(self):
        return len(self.edited_files) > 0

    @property
    def has_deleted(self):
        return len(self.deleted_files) > 0


class CrossrefEventsTelescope(Workflow):
    """Crossref Events telescope"""

    def __init__(
        self,
        dag_id: str,
        cloud_workspace: CloudWorkspace,
        events_start_date: pendulum.DateTime = pendulum.datetime(2017, 2, 17),
        bq_dataset_id: str = "crossref_events",
        bq_table_name: str = "crossref_events",
        api_dataset_id: str = "crossref_events",
        schema_folder: str = os.path.join(default_schema_folder(), "crossref_events"),
        dataset_description: str = "The Crossref Events dataset: https://www.eventdata.crossref.org/guide/",
        table_description: str = "The Crossref Events dataset: https://www.eventdata.crossref.org/guide/",
        snapshot_expiry_days: int = 31,
        n_rows: int = 1000,
        max_threads: int = min(32, os.cpu_count() + 4),
        max_processes: int = os.cpu_count(),
        mailto: str = metadata("academic_observatory_workflows").get("Author-email"),
        primary_key: str = "id",
        observatory_api_conn_id: str = AirflowConns.OBSERVATORY_API,
        start_date: pendulum.DateTime = pendulum.datetime(2017, 2, 12),
        schedule: str = "@weekly",
        queue: str = "remote_queue",
    ):
        """Construct a CrossrefEventsTelescope instance.

        :param dag_id: the id of the DAG.
        :param cloud_workspace: the cloud workspace settings.
        :param events_start_date: the date to start querying Crossref Events from.
        :param bq_dataset_id: the BigQuery dataset id.
        :param bq_table_name: the BigQuery table name.
        :param api_dataset_id: the Dataset ID to use when storing releases.
        :param schema_folder: the SQL schema path.
        :param dataset_description: description for the BigQuery dataset.
        :param table_description: description for the BigQuery table.
        :param snapshot_expiry_days: the number of days that a snapshot of the main table will take to expire,
        which is set to 31 days so there is some time to rollback after an update.
        :param n_rows: the number of rows to fetch from Crossref Events for each page.
        :param max_threads: Max processes used for parallel downloading, default is based on 7 days x 3 url categories
        :param max_processes: max processes for transforming files.
        :param mailto: Email address used in the download url
        :param primary_key: the primary key to use when merging files.
        :param observatory_api_conn_id: the Observatory API connection key.
        :param start_date: the start date of the DAG.
        :param schedule: the schedule interval of the DAG.
        :param queue: what queue to run the DAG on.
        """

        super().__init__(
            dag_id=dag_id,
            start_date=start_date,
            schedule=schedule,
            catchup=False,
            airflow_conns=[observatory_api_conn_id],
            tags=[Tag.academic_observatory],
            queue=queue,
        )

        self.cloud_workspace = cloud_workspace
        self.events_start_date = events_start_date
        self.bq_dataset_id = bq_dataset_id
        self.bq_table_name = bq_table_name
        self.api_dataset_id = api_dataset_id
        self.schema_file_path = bq_find_schema(path=schema_folder, table_name=self.bq_table_name)
        self.delete_schema_file_path = os.path.join(schema_folder, "delete_table.json")
        self.dataset_description = dataset_description
        self.table_description = table_description
        self.snapshot_expiry_days = snapshot_expiry_days
        self.n_rows = n_rows
        self.max_threads = max_threads
        self.max_processes = max_processes
        self.mailto = mailto
        self.primary_key = primary_key
        self.observatory_api_conn_id = observatory_api_conn_id

        # Wait for the previous DAG run to finish to make sure that
        # changefiles are processed in the correct order
        external_task_id = "dag_run_complete"
        self.add_operator(
            PreviousDagRunSensor(
                dag_id=self.dag_id,
                external_task_id=external_task_id,
                execution_delta=timedelta(days=7),  # To match the @weekly schedule
            )
        )

        self.add_setup_task(self.check_dependencies)
        self.add_setup_task(self.fetch_releases)
        self.add_task(self.create_datasets)

        # Create snapshots of main table in case we mess up
        # This is done before updating the tables to make sure that the snapshots haven't expired before the tables
        # are updated
        self.add_task(self.bq_create_main_table_snapshot)

        # Download and transform data
        self.add_task(self.download)
        self.add_task(self.upload_downloaded)
        self.add_task(self.transform_snapshot)
        self.add_task(self.transform_created_edited)
        self.add_task(self.transform_deleted)
        self.add_task(self.upload_transformed)

        # Upsert records
        self.add_task(self.bq_load_main_table)
        self.add_task(self.bq_load_upsert_table)
        self.add_task(self.bq_upsert_records)

        # Delete records
        self.add_task(self.bq_load_delete_table)
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

    def fetch_releases(self, **kwargs):
        """Return release information with the start and end date. [start date, end date) Includes start date, excludes end date.

         :param kwargs: the context passed from the Airflow Operator.
        See https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return: True to continue, False to skip.
        """

        dag_run = kwargs["dag_run"]
        is_first_run = is_first_dag_run(dag_run)
        releases = get_dataset_releases(dag_id=self.dag_id, dataset_id=self.api_dataset_id)
        prev_end_date = pendulum.instance(datetime.datetime.min)

        # Get start date
        if is_first_run:
            assert (
                len(releases) == 0
            ), "fetch_releases: there should be no DatasetReleases stored in the Observatory API on the first DAG run."

            # The events start being collected around beginning of 2017
            start_date = self.events_start_date
        else:
            assert (
                len(releases) >= 1
            ), f"fetch_releases: there should be at least 1 DatasetRelease in the Observatory API after the first DAG run"

            # The start date is the last end_date
            prev_release = get_latest_dataset_release(releases, "changefile_end_date")
            start_date = prev_release.changefile_end_date
            prev_end_date = prev_release.changefile_end_date

        # End date is always the data_interval_end, although it is not inclusive
        end_date = kwargs["data_interval_end"]

        # Print summary information
        logging.info(f"is_first_run: {is_first_run}")
        logging.info(f"start_date: {start_date}")
        logging.info(f"end_date: {end_date}")

        # Publish release information
        ti: TaskInstance = kwargs["ti"]
        msg = dict(
            is_first_run=is_first_run,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            prev_end_date=prev_end_date.isoformat(),
        )
        ti.xcom_push(self.RELEASE_INFO, msg, kwargs["logical_date"])

        return True

    def make_release(self, **kwargs) -> CrossrefEventsRelease:
        """Make a Release instance"""

        ti: TaskInstance = kwargs["ti"]
        msg = ti.xcom_pull(key=self.RELEASE_INFO, task_ids=self.fetch_releases.__name__, include_prior_dates=False)
        start_date, end_date, is_first_run, prev_end_date = parse_release_msg(msg)
        run_id = kwargs["run_id"]

        release = CrossrefEventsRelease(
            dag_id=self.dag_id,
            run_id=run_id,
            cloud_workspace=self.cloud_workspace,
            bq_dataset_id=self.bq_dataset_id,
            bq_table_name=self.bq_table_name,
            start_date=start_date,
            end_date=end_date,
            prev_end_date=prev_end_date,
            is_first_run=is_first_run,
            mailto=self.mailto,
        )
        return release

    def create_datasets(self, release: CrossrefEventsRelease, **kwargs) -> None:
        """Create datasets."""

        bq_create_dataset(
            project_id=self.cloud_workspace.output_project_id,
            dataset_id=self.bq_dataset_id,
            location=self.cloud_workspace.data_location,
            description=self.dataset_description,
        )

    def bq_create_main_table_snapshot(self, release: CrossrefEventsRelease, **kwargs):
        """Create a snapshot of each main table. The purpose of this table is to be able to rollback the table
        if something goes wrong. The snapshot expires after self.snapshot_expiry_days."""

        if release.is_first_run:
            logging.info(f"bq_create_main_table_snapshots: skipping as snapshots are not created on the first run")
            return

        expiry_date = pendulum.now().add(days=self.snapshot_expiry_days)
        success = bq_snapshot(
            src_table_id=release.bq_main_table_id, dst_table_id=release.bq_snapshot_table_id, expiry_date=expiry_date
        )
        set_task_state(success, self.bq_create_main_table_snapshot.__name__, release)

    def download(self, release: CrossrefEventsRelease, **kwargs):
        """Task to download the CrossrefEventsRelease release."""

        events = []
        for day in release.day_requests:
            events.append(day.created)
            if not release.is_first_run:
                events.append(day.edited)
                events.append(day.deleted)
        logging.info(f"Downloading events: no. workers: {self.max_threads}")
        logging.info(f"Total event requests: {len(events)}")

        with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            futures = []
            for event in events:
                futures.append(executor.submit(download_events, event, release.download_folder, self.n_rows))
            for future in as_completed(futures):
                future.result()

        if not (release.has_created or release.has_edited or release.has_deleted):
            raise AirflowSkipException("No events found")

    def upload_downloaded(self, release: CrossrefEventsRelease, **kwargs):
        """Upload the downloaded files for the given release."""

        files_list = list_files(release.download_folder, release.download_files_regex)
        success = gcs_upload_files(bucket_name=self.cloud_workspace.download_bucket, file_paths=files_list)
        set_task_state(success, self.upload_downloaded.__name__, release)

    def transform_snapshot(self, release: CrossrefEventsRelease, **kwargs):
        """Transforms the create files on the first run."""

        if not release.is_first_run:
            logging.info(f"transform_snapshot: skipping as this is only done on the first run")
            return

        logging.info(f"Transforming events, no. workers: {self.max_processes}")
        with ProcessPoolExecutor(max_workers=self.max_processes) as executor:
            futures = []
            for file_path in list_files(release.download_folder, release.download_files_regex):
                futures.append(executor.submit(transform_events, file_path, release.transform_folder))
            for future in as_completed(futures):
                future.result()

    def transform_created_edited(self, release: CrossrefEventsRelease, **kwargs):
        """Transforms the created and edited files on subsequent runs"""

        if release.is_first_run:
            logging.info(f"transform_created_edited: skipping as first run")
            return

        if not (release.has_created or release.has_edited):
            logging.info(f"transform_created_edited: skipping as no created or edited files were downloaded")
            return

        # Merge
        input_files = []
        for day in release.day_requests:
            create_file_path = os.path.join(release.download_folder, day.created.data_file_name)
            edited_file_path = os.path.join(release.download_folder, day.edited.data_file_name)

            # Transform all events saving into events array
            data = []
            if os.path.isfile(create_file_path):
                for event in yield_jsonl(create_file_path):
                    data.append(transform_event(event))

            if os.path.isfile(edited_file_path):
                for event in yield_jsonl(edited_file_path):
                    data.append(transform_event(event))

            # Remove any duplicates between created and edited, taking most recent event
            # Sort events by date from newest to oldest
            data.sort(key=get_event_date, reverse=True)
            data_unique = []
            seen = set()
            for event in data:
                event_id = event[self.primary_key]
                if event_id not in seen:
                    data_unique.append(event)
                    seen.add(event_id)
            # Sort events by date from oldest to newest
            data_unique.sort(key=get_event_date, reverse=False)

            # Save events
            file_path = os.path.join(release.transform_folder, f"upsert_table-{day.date.format(DATE_FORMAT)}.jsonl")
            input_files.append(file_path)
            save_jsonl(file_path, data_unique)

        # Merge multiple days together
        merge_update_files(
            primary_key=self.primary_key, input_files=input_files, output_file=release.upsert_table_file_path
        )

    def transform_deleted(self, release: CrossrefEventsRelease, **kwargs):
        """Transform the deleted files into a file that just contains the primary key of each entry"""

        if release.is_first_run or not release.has_deleted:
            logging.info(f"transform_deleted: skipping as first run")
            return

        if not release.has_deleted:
            logging.info(f"transform_deleted: skipping as no deleted files downloaded")
            return

        # Create delete table file
        data = []
        for day in release.day_requests:
            delete_file_path = os.path.join(release.download_folder, day.deleted.data_file_name)
            if os.path.isfile(delete_file_path):
                for row in yield_jsonl(delete_file_path):
                    primary_key = row[self.primary_key]
                    data.append(dict(id=primary_key))

        save_jsonl(release.delete_table_file_path, data)

    def upload_transformed(self, release: CrossrefEventsRelease, **kwargs) -> None:
        """Upload the transformed data to Cloud Storage."""

        files_list = list_files(release.transform_folder, release.transform_files_regex)
        success = gcs_upload_files(bucket_name=self.cloud_workspace.transform_bucket, file_paths=files_list)
        set_task_state(success, self.upload_transformed.__name__, release)

    def bq_load_main_table(self, release: CrossrefEventsRelease, **kwargs):
        """Load the main table."""

        if not release.is_first_run:
            logging.info(f"bq_load_main_table: skipping as the main table is only created on the first run")
            return

        success = bq_load_table(
            uri=release.main_table_uri,
            table_id=release.bq_main_table_id,
            schema_file_path=self.schema_file_path,
            source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
            ignore_unknown_values=True,
        )
        set_task_state(success, self.bq_load_main_table.__name__, release)

    def bq_load_upsert_table(self, release: CrossrefEventsRelease, **kwargs):
        """Load the upsert table."""

        if release.is_first_run:
            logging.info(f"bq_load_upsert_table: skipping as no records are upserted on the first run")
            return

        if not (release.has_created or release.has_edited):
            logging.info(f"bq_load_upsert_table: skipping as no created or edited files were downloaded")
            return

        success = bq_load_table(
            uri=release.upsert_table_uri,
            table_id=release.bq_upsert_table_id,
            schema_file_path=self.schema_file_path,
            source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            ignore_unknown_values=True,
        )
        set_task_state(success, self.bq_load_upsert_table.__name__, release)

    def bq_upsert_records(self, release: CrossrefEventsRelease, **kwargs):
        """Upsert the records from the upserts table into the main table."""

        if release.is_first_run:
            logging.info(f"transform_deleted: skipping as no records are deleted on the first run")
            return

        if not (release.has_created or release.has_edited):
            logging.info(f"bq_upsert_records: skipping as no created or edited files were downloaded")
            return

        bq_upsert_records(
            main_table_id=release.bq_main_table_id,
            upsert_table_id=release.bq_upsert_table_id,
            primary_key=self.primary_key,
        )

    def bq_load_delete_table(self, release: CrossrefEventsRelease, **kwargs):
        """Load the delete table."""

        if release.is_first_run:
            logging.info(f"bq_load_delete_table: skipping as no records are deleted on the first run")
            return

        if not release.has_deleted:
            logging.info(f"bq_load_delete_table: skipping as no deleted files downloaded")
            return

        success = bq_load_table(
            uri=release.delete_table_uri,
            table_id=release.bq_delete_table_id,
            schema_file_path=self.delete_schema_file_path,
            source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            ignore_unknown_values=True,
        )
        set_task_state(success, self.bq_load_delete_table.__name__, release)

    def bq_delete_records(self, release: CrossrefEventsRelease, **kwargs):
        """Delete records from main table that are in delete table."""

        if release.is_first_run:
            logging.info(f"bq_delete_records: skipping as no records are deleted on the first run")
            return

        if not release.has_deleted:
            logging.info(f"bq_delete_records: skipping as no deleted files downloaded")
            return

        bq_delete_records(
            main_table_id=release.bq_main_table_id,
            delete_table_id=release.bq_delete_table_id,
            main_table_primary_key=self.primary_key,
            delete_table_primary_key=self.primary_key,
        )

    def add_new_dataset_releases(self, release: CrossrefEventsRelease, **kwargs) -> None:
        """Adds release information to API."""

        dataset_release = DatasetRelease(
            dag_id=self.dag_id,
            dataset_id=self.api_dataset_id,
            dag_run_id=release.run_id,
            changefile_start_date=release.start_date,
            changefile_end_date=release.end_date,
        )
        api = make_observatory_api(observatory_api_conn_id=self.observatory_api_conn_id)
        api.post_dataset_release(dataset_release)

    def cleanup(self, release: CrossrefEventsRelease, **kwargs) -> None:
        """Delete all files, folders and XComs associated with this release."""

        cleanup(dag_id=self.dag_id, execution_date=kwargs["logical_date"], workflow_folder=release.workflow_folder)


def parse_release_msg(msg: Dict) -> Tuple[pendulum.DateTime, pendulum.DateTime, bool, pendulum.DateTime]:
    start_date = pendulum.parse(msg["start_date"])
    end_date = pendulum.parse(msg["end_date"])
    is_first_run = msg["is_first_run"]
    prev_end_date = pendulum.parse(msg["prev_end_date"])

    return start_date, end_date, is_first_run, prev_end_date


def get_event_date(event) -> pendulum.DateTime:
    if "updated_date" in event:
        return pendulum.parse(event["updated_date"])
    else:
        return pendulum.parse(event["timestamp"])


class Action:
    create = "created"
    edit = "edited"
    delete = "deleted"


class DayRequest:
    def __init__(self, day: pendulum.DateTime, mailto: str):
        self.date = day
        self.mailto = mailto
        self.created = EventRequest(Action.create, day, mailto)
        self.edited = EventRequest(Action.edit, day, mailto)
        self.deleted = EventRequest(Action.delete, day, mailto)


def make_crossref_events_url(
    *, action: str, start_date: pendulum.DateTime, end_date: pendulum.DateTime, mailto: str, rows: int, cursor: str
):
    assert action in {
        "created",
        "edited",
        "deleted",
    }, f"make_crossref_events_url: unknown action={action}, must be one of created, edited or deleted"

    # Set query params and set path in URL
    url = CROSSREF_EVENTS_HOST
    query_params = dict()

    if action == Action.create:
        query_params["from-collected-date"] = start_date.format(DATE_FORMAT)
        query_params["until-collected-date"] = start_date.format(DATE_FORMAT)
    else:
        url += f"/{action}"
        query_params["from-updated-date"] = end_date.format(DATE_FORMAT)
        query_params["until-updated-date"] = end_date.format(DATE_FORMAT)

    if mailto is not None:
        query_params["mailto"] = mailto
    if rows is not None:
        query_params["rows"] = rows
    if cursor is not None:
        query_params["cursor"] = cursor

    req = requests.models.PreparedRequest()
    req.prepare_url(url, query_params)
    return req.url


class EventRequest:
    def __init__(self, action: str, day: pendulum.DateTime, mailto: str):
        self.action = action
        self.day = day
        self.mailto = mailto

    @property
    def cursor_file_name(self):
        return f"{self.action}-{self.day.format(DATE_FORMAT)}-cursor.txt"

    @property
    def data_file_name(self):
        return f"{self.action}-{self.day.format(DATE_FORMAT)}.jsonl"

    def make_url(self, rows: int = 1000, cursor: str = None) -> str:
        """Make a URL for this EventsRequest. See documentation for more details:

        Example queries: https://www.eventdata.crossref.org/guide/service/query-api/#example-queries
        Edited events: See documentation for more details: https://www.eventdata.crossref.org/guide/service/query-api/#edited-events
        Deleted events: https://www.eventdata.crossref.org/guide/service/query-api/#deleted-events

        :param rows: the number of rows.
        :param cursor: the cursor.
        :return: the URL.
        """

        return make_crossref_events_url(
            action=self.action, start_date=self.day, end_date=self.day, mailto=self.mailto, rows=rows, cursor=cursor
        )

    def __str__(self):
        return f"EventsRequest('{self.action}, {self.day.format(DATE_FORMAT)}, {self.mailto}')"

    def __repr__(self):
        return f"EventsRequest('{self.action}, {self.day.format(DATE_FORMAT), {self.mailto}}')"


def make_day_requests(start_date: pendulum.DateTime, end_date: pendulum.DateTime, mailto: str) -> List[DayRequest]:
    """Create the upsert and delete URLs. The interval for dates is [start_date, end_date). On the first run only
    create events are returned in the upsert_events list, on subsequent runs, create and edit event URLs are populated
    into upsert_events and delete events in delete_events.

    :param start_date: the start date. Inclusive.
    :param end_date: the end date. Exclusive.
    :param mailto: the mailto email address.
    :return: the URLs.
    """

    events = []
    period = pendulum.period(start_date, end_date)
    for day in period.range("days"):
        if day != end_date:
            events.append(DayRequest(day, mailto))
    return events


def download_events(request: EventRequest, download_folder: str, n_rows: int):
    """Download one day of events. When the download finished successfully, the generated cursor file is deleted.
    If there is a cursor file available at the start, it means that a previous download attempt failed. If there
    is an events file available and no cursor file, it means that a previous download attempt was successful,
    so these events will not be downloaded again.

    :param request: the event we will fetch.
    :param download_folder: the folder we will download the data into.
    :param n_rows: the number of rows per page.
    :return: None.
    """

    data_path = os.path.join(download_folder, request.data_file_name)
    cursor_path = os.path.join(download_folder, request.cursor_file_name)

    # If data file exists but no cursor file, previous request has finished & successful
    if os.path.isfile(data_path) and not os.path.isfile(cursor_path):
        logging.info(f"{request}: skipped, already finished")
        return

    # If cursor exists then the previous request must have failed
    # Remove data file and cursor and start again
    if os.path.isfile(cursor_path):
        logging.warning(f"{request}: deleting data and trying again")
        logging.warning(f"{request}: deleting {data_path}")
        try:
            os.remove(data_path)
        except FileNotFoundError:
            pass
        logging.warning(f"{request}: deleting {cursor_path}")
        try:
            os.remove(cursor_path)
        except FileNotFoundError:
            pass

    # Create empty cursor file before doing anything else
    pathlib.Path(cursor_path).touch()

    logging.info(f"{request}: downloading")
    next_cursor = None
    total_events = 0
    while True:
        # Fetch a page of events
        events, next_cursor = fetch_events(request, cursor=next_cursor, n_rows=n_rows)

        # Write cursor to file
        # this is done just so that we know that the task is still running
        # we don't actually use this data
        if next_cursor:
            with open(cursor_path, "a") as f:
                f.write(next_cursor + "\n")

        # Write events to file
        if events:
            total_events += len(events)
            with open(data_path, "a") as f:
                with jsonlines.Writer(f) as writer:
                    writer.write_all(events)

        # If next_cursor is None exit loop
        if not next_cursor:
            break

    # Remove cursor as we have finished
    if os.path.isfile(cursor_path):
        os.remove(cursor_path)

    logging.info(f"{request}: successfully downloaded {total_events} events")


def fetch_events(request: EventRequest, cursor: str = None, n_rows: int = 1000) -> Tuple[Dict, str]:
    """Fetch the events for an EventsRequest from the given url until no new cursor is returned or a RetryError occurs.
    The extracted events are appended to a jsonl file and the cursors are written to a text file.

    :param request: the url.
    :param cursor: the cursor.
    :param n_rows: number of rows per page.
    :return: the fetched events and the next_cursor.
    """

    headers = {"User-Agent": get_user_agent(package_name="academic_observatory_workflows")}
    try:
        crossref_events_limiter()
        url = request.make_url(rows=n_rows, cursor=cursor)
        response = retry_get_url(url, headers=headers)
    except requests.exceptions.RequestException:
        crossref_events_limiter()
        url = request.make_url(rows=100, cursor=cursor)
        response = retry_get_url(url, headers=headers)

    response_json = response.json()
    events = response_json["message"]["events"]
    next_cursor = response_json["message"]["next-cursor"]

    return events, next_cursor


def crossref_events_limiter(calls_per_second: int = 10):
    """Function to throttle the calls to the Crossref Events API"""

    identifier = "crossref_events_limiter"
    item = RateLimitItemPerSecond(calls_per_second)  # 10 per second

    while True:
        if not MOVING_WINDOW.test(item, identifier):
            time.sleep(0.01)
        else:
            break

    MOVING_WINDOW.hit(item, identifier)


def transform_events(download_path: str, transform_folder: str):
    """Transform one day of events.

    :param download_path: The path to the downloaded file.
    :param transform_folder: the transform folder.
    :return: None.
    """

    file_name = os.path.basename(download_path)
    transform_path = os.path.join(transform_folder, file_name)

    logging.info(f"Transforming file: {download_path}")
    logging.info(f"Saving to: {transform_path}")
    with jsonlines.open(download_path, "r") as reader:
        with jsonlines.open(transform_path, "w") as writer:
            for event in reader:
                event = transform_event(event)
                writer.write(event)

    logging.info(f"Finished: {file_name}")


def transform_event(event: Dict):
    """Transform the dictionary with event data by replacing '-' with '_' in key names, converting all int values to
    string except for the 'total' field and parsing datetime columns for a valid datetime.

    :param event: The event dictionary.
    :return: The updated event dictionary.
    """
    if isinstance(event, (str, int, float)):
        return event
    if isinstance(event, dict):
        new = event.__class__()
        for k, v in event.items():
            if isinstance(v, int) and k != "total":
                v = str(v)
            if k in ["timestamp", "occurred_at", "issued", "dateModified", "updated_date"]:
                try:
                    v = str(pendulum.parse(v))
                except ValueError:
                    v = "0001-01-01T00:00:00Z"

            # Replace hyphens with underscores for BigQuery compatibility
            k = k.replace("-", "_")

            # Replace @ symbol in keys left by DataCite between the 15 and 22 March 2019
            k = k.replace("@", "")

            new[k] = transform_event(v)

        return new
