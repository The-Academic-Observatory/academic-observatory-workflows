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


import calendar
import json
import logging
import os
import urllib.request
from concurrent.futures import as_completed, ThreadPoolExecutor
from queue import Empty, Queue
from threading import Event
from time import sleep
from typing import Any, Dict, List, Tuple, Type, Union
from urllib.parse import quote_plus

import pendulum
from airflow import AirflowException
from airflow.decorators import dag, task
from google.cloud.bigquery import SourceFormat, WriteDisposition
from ratelimit import limits, sleep_and_retry

from academic_observatory_workflows.config import project_path
from observatory.api.client.model.dataset_release import DatasetRelease
from observatory_platform.airflow import get_airflow_connection_password, on_failure_callback
from observatory_platform.dataset_api import build_schedule, make_observatory_api
from observatory_platform.google.bigquery import bq_create_dataset, bq_find_schema, bq_load_table, bq_sharded_table_id
from observatory_platform.config import AirflowConns
from observatory_platform.files import (
    clean_dir,
    get_as_list,
    get_entry_or_none,
    list_files,
    load_file,
    save_jsonl_gz,
    write_to_file,
)
from observatory_platform.google.gcs import gcs_blob_name_from_path, gcs_blob_uri, gcs_download_blobs, gcs_upload_files
from observatory_platform.airflow.workflow import CloudWorkspace
from observatory_platform.refactor.tasks import check_dependencies
from observatory_platform.url_utils import get_user_agent
from observatory_platform.workflows.workflow import (
    cleanup,
    make_snapshot_date,
    SnapshotRelease,
)


class ScopusRelease(SnapshotRelease):
    def __init__(
        self,
        *,
        dag_id: str,
        run_id: str,
        snapshot_date: pendulum.DateTime,
    ):
        """Construct a WebOfScienceRelease instance.

        :param dag_id: The DAG ID.
        :param run_id: The DAG run ID.
        :param snapshot_date: Release date.
        """

        super().__init__(
            dag_id=dag_id,
            run_id=run_id,
            snapshot_date=snapshot_date,
        )
        self.download_file_regex = r".*\.json"
        self.transform_file_name = "scopus.jsonl.gz"
        self.transform_file_path = os.path.join(self.transform_folder, self.transform_file_name)

    @property
    def transform_blob_name(self):
        return gcs_blob_name_from_path(self.transform_file_path)

    @staticmethod
    def from_dict(dict_: dict):
        dag_id = dict_["dag_id"]
        run_id = dict_["run_id"]
        snapshot_date = pendulum.parse(dict_["snapshot_date"])
        return ScopusRelease(
            dag_id=dag_id,
            run_id=run_id,
            snapshot_date=snapshot_date,
        )

    def to_dict(self) -> dict:
        return dict(
            dag_id=self.dag_id,
            run_id=self.run_id,
            snapshot_date=self.snapshot_date.to_datetime_string(),
        )


def create_dag(
    *,
    dag_id: str,
    cloud_workspace: CloudWorkspace,
    institution_ids: List[str],
    scopus_conn_ids: List[str],
    view: str = "STANDARD",
    earliest_date: pendulum.DateTime = pendulum.datetime(1800, 1, 1),
    bq_dataset_id: str = "scopus",
    bq_table_name: str = "scopus",
    api_dataset_id: str = "scopus",
    schema_folder: str = project_path("scopus_telescope", "schema"),
    dataset_description: str = "The Scopus citation database: https://www.scopus.com",
    table_description: str = "The Scopus citation database: https://www.scopus.com",
    observatory_api_conn_id: str = AirflowConns.OBSERVATORY_API,
    start_date: pendulum.DateTime = pendulum.datetime(2018, 5, 14),
    schedule: str = "@monthly",
    max_active_runs: int = 1,
    retries: int = 3,
):
    """Scopus telescope.
    :param dag_id: the id of the DAG.
    :param cloud_workspace: the cloud workspace settings.
    :param institution_ids: list of institution IDs to use for the Scopus search query.
    :param scopus_conn_ids: list of Scopus Airflow Connection IDs.
    :param view: The view type. Standard or complete. See https://dev.elsevier.com/sc_search_views.html
    :param earliest_date: earliest date to query for results.
    :param bq_dataset_id: the BigQuery dataset id.
    :param bq_table_name: the BigQuery table name.
    :param api_dataset_id: the Dataset ID to use when storing releases.
    :param schema_folder: the SQL schema path.
    :param dataset_description: description for the BigQuery dataset.
    :param table_description: description for the BigQuery table.
    :param observatory_api_conn_id: the Observatory API connection key.
    :param start_date: the start date of the DAG.
    :param schedule: the schedule interval of the DAG.
    :param max_active_runs: the maximum number of DAG runs that can be run at once.
    :param retries: the number of times to retry a task.
    """

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
        },
    )
    def scopus():
        @task
        def fetch_release(**context) -> dict:
            """Fetch the release"""

            snapshot_date = make_snapshot_date(**context)
            return ScopusRelease(
                dag_id=dag_id,
                run_id=context["run_id"],
                snapshot_date=snapshot_date,
            ).to_dict()

        @task
        def download(release: dict, **context):
            """Download snapshot from SCOPUS for the given institution."""

            release = ScopusRelease.from_dict(release)

            # Download data
            clean_dir(release.download_folder)
            scopus_schedule = build_schedule(earliest_date, release.snapshot_date)
            taskq = Queue()
            for period in scopus_schedule:
                taskq.put(period)

            workers = list()
            api_keys = [get_airflow_connection_password(conn) for conn in scopus_conn_ids]
            for i, key in enumerate(api_keys):
                worker = ScopusUtilWorker(
                    client_id=i,
                    client=ScopusClient(api_key=key, view=view),
                    quota_reset_date=release.snapshot_date,
                    quota_remaining=ScopusUtilWorker.DEFAULT_KEY_QUOTA,
                )
                workers.append(worker)

            ScopusUtility.download_parallel(
                workers=workers,
                taskq=taskq,
                conn=dag_id,
                institution_ids=institution_ids,
                download_dir=release.download_folder,
            )

            # Upload to Cloud Storage
            file_list = list_files(release.download_folder, release.download_file_regex)
            success = gcs_upload_files(bucket_name=cloud_workspace.download_bucket, file_paths=file_list)
            if not success:
                raise AirflowException(f"Error uploading files to Cloud Storage: {file_list}")

        @task
        def transform(release: dict, **context):
            """Transform the data into database format."""

            release = ScopusRelease.from_dict(release)

            # Download data
            bucket_name = cloud_workspace.download_bucket
            prefix = gcs_blob_name_from_path(release.download_folder)
            success = gcs_download_blobs(
                bucket_name=bucket_name,
                prefix=prefix,
                destination_path=release.download_folder,
            )
            if not success:
                raise AirflowException(f"Error downloading files from bucket: {bucket_name}/{prefix}")

            # Transform
            clean_dir(release.transform_folder)
            data = []
            file_list = list_files(release.download_folder, release.download_file_regex)
            for file in file_list:
                records = json.loads(load_file(file))
                data += transform_to_db_format(
                    records=records, snapshot_date=release.snapshot_date, institution_ids=institution_ids
                )
            save_jsonl_gz(release.transform_file_path, data)

            # Upload
            success = gcs_upload_files(
                bucket_name=cloud_workspace.transform_bucket, file_paths=[release.transform_file_path]
            )
            if not success:
                raise AirflowException("")

        @task
        def bq_load(release: dict, **context):
            """Task to load each transformed release to BigQuery.
            The table_id is set to the file name without the extension."""

            release = ScopusRelease.from_dict(release)
            bq_create_dataset(
                project_id=cloud_workspace.output_project_id,
                dataset_id=bq_dataset_id,
                location=cloud_workspace.data_location,
                description=dataset_description,
            )

            uri = gcs_blob_uri(cloud_workspace.transform_bucket, gcs_blob_name_from_path(release.transform_file_path))
            schema_file_path = bq_find_schema(
                path=schema_folder, table_name=bq_table_name, release_date=release.snapshot_date
            )
            table_id = bq_sharded_table_id(
                cloud_workspace.output_project_id, bq_dataset_id, bq_table_name, release.snapshot_date
            )
            success = bq_load_table(
                uri=uri,
                table_id=table_id,
                schema_file_path=schema_file_path,
                source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
                table_description=table_description,
                write_disposition=WriteDisposition.WRITE_APPEND,
                ignore_unknown_values=True,
            )
            if not success:
                raise AirflowException(f"Error loading BigQuery table")

        @task
        def add_dataset_release(release: dict, **context) -> None:
            """Adds release information to API."""

            release = ScopusRelease.from_dict(release)
            dataset_release = DatasetRelease(
                dag_id=dag_id,
                dataset_id=api_dataset_id,
                dag_run_id=release.run_id,
                snapshot_date=release.snapshot_date,
                data_interval_start=context["data_interval_start"],
                data_interval_end=context["data_interval_end"],
            )
            api = make_observatory_api(observatory_api_conn_id=observatory_api_conn_id)
            api.post_dataset_release(dataset_release)

        @task
        def cleanup_workflow(release: dict, **context) -> None:
            """Delete all files, folders and XComs associated with this release."""

            release = ScopusRelease.from_dict(release)
            cleanup(dag_id=dag_id, execution_date=context["logical_date"], workflow_folder=release.workflow_folder)

        # Define task connections
        task_check_dependencies = check_dependencies(airflow_conns=[observatory_api_conn_id] + scopus_conn_ids)
        xcom_release = fetch_release()
        task_download = download(xcom_release)
        task_transform = transform(xcom_release)
        task_bq_load = bq_load(xcom_release)
        task_add_dataset_release = add_dataset_release(xcom_release)
        task_cleanup_workflow = cleanup_workflow(xcom_release)

        (
            task_check_dependencies
            >> xcom_release
            >> task_download
            >> task_transform
            >> task_bq_load
            >> task_add_dataset_release
            >> task_cleanup_workflow
        )

    return scopus()


def transform_to_db_format(records: List[dict], snapshot_date: pendulum.Date, institution_ids: List[str]) -> List[dict]:
    """Convert the json response to the expected schema.
    :param records: List of the records as json.
    :param snapshot_date: release date.
    :param institution_ids: list of institutions.
    :return: List of transformed entries.
    """

    entries = []
    for data in records:
        entry = ScopusJsonParser.parse_json(
            data=data,
            snapshot_date=snapshot_date,
            institution_ids=institution_ids,
        )
        entries.append(entry)
    return entries


class ScopusClientThrottleLimits:
    """API throttling constants for ScopusClient."""

    CALL_LIMIT = 2  # SCOPUS allows 2 api calls / second.
    CALL_PERIOD = 1  # seconds


class ScopusClient:
    """Handles URL fetching of SCOPUS search."""

    RESULTS_PER_PAGE = 25
    MAX_RESULTS = 5000  # Upper limit on number of results returned
    QUOTA_EXCEED_ERROR_PREFIX = "QuotaExceeded. Resets at: "

    def __init__(self, *, api_key: str, view: str = "standard"):
        """Constructor.

        :param api_key: API key.
        :param view: The 'view' access level. Can be 'standard' or 'complete'.
        """

        self._headers = {
            "X-ELS-APIKey": api_key,
            "Accept": "application/json",
            "User-Agent": get_user_agent(package_name="academic_observatory_workflows"),
        }

        self._view = view

    def _url(self, query: str) -> str:
        """Get the query url.

        :param query: Query string.
        :return: Query url.
        """

        return f"https://api.elsevier.com/content/search/scopus?view={self._view}&query={quote_plus(query)}"

    @staticmethod
    def get_reset_date_from_error(msg: str) -> int:
        """Get the reset date timestamp in seconds from the exception message.
        According to https://dev.elsevier.com/api_key_settings.html it is meant to be seconds, but milliseconds were
        observed the last time it was checked in Oct 2020.

        :param msg: exception message.
        :return: Reset date timestamp in seconds.
        """

        ts_offset = len(ScopusClient.QUOTA_EXCEED_ERROR_PREFIX)
        return int(msg[ts_offset:]) / 1000  # Elsevier docs says reports seconds, but headers report milliseconds.

    @staticmethod
    def get_next_page_url(links: List[dict]) -> Union[None, str]:
        """Get the URL for the next result page.

        :param links: The list of links returned from the last query.
        :return None if next page not found, otherwise string to next page's url.
        """

        try:
            for link in links:
                if link["@ref"] == "next":
                    return link["@href"]
        except:
            return None

    @sleep_and_retry
    @limits(calls=ScopusClientThrottleLimits.CALL_LIMIT, period=ScopusClientThrottleLimits.CALL_PERIOD)
    def retrieve(self, query: str) -> Tuple[List[Dict[str, Any]], int, int]:
        """Execute the query.

        :param query: Query string.
        :return: (results of query, quota remaining, quota reset date timestamp in seconds)
        """

        http_ok = 200
        http_quota_exceeded = 429

        request = urllib.request.Request(self._url(query), headers=self._headers)
        results = list()

        while True:
            response = urllib.request.urlopen(request)
            quota_remaining = response.getheader("X-RateLimit-Remaining")
            quota_reset = response.getheader("X-RateLimit-Reset")

            request_code = response.getcode()
            if request_code == http_quota_exceeded:
                raise AirflowException(f"{ScopusClient.QUOTA_EXCEED_ERROR_PREFIX}{quota_reset}")

            response_dict = json.loads(response.read().decode("utf-8"))

            if request_code != http_ok:
                raise AirflowException(f"HTTP {request_code}:{response_dict}")

            if "search-results" not in response_dict:
                break

            results.extend(response_dict["search-results"]["entry"])
            total_results = int(response_dict["search-results"]["opensearch:totalResults"])

            if total_results > ScopusClient.MAX_RESULTS:
                raise AirflowException(
                    f"ScopusClient: query {query} has {total_results} results but the maximum is {ScopusClient.MAX_RESULTS}"
                )

            if len(results) == total_results:
                break

            if total_results == 0:
                results = list()
                break

            url = ScopusClient.get_next_page_url(response_dict["search-results"]["link"])
            if url is None:
                raise AirflowException(
                    f"ScopusClient: no next url found. Only have {len(results)} of {total_results} results."
                )

            request = urllib.request.Request(url, headers=self._headers)

        return results, quota_remaining, quota_reset


class ScopusUtilWorker:
    """Worker class"""

    DEFAULT_KEY_QUOTA = 20000  # API key query limit default per 7 days.
    QUEUE_WAIT_TIME = 20  # Wait time for Queue.get() call

    def __init__(
        self, *, client_id: int, client: ScopusClient, quota_reset_date: pendulum.DateTime, quota_remaining: int
    ):
        """Constructor.

        :param client_id: Client id to use for debug messages so we don't leak the API key.
        :param client: ElsClient object for an API key.
        :param quota_reset_date: Date at which the quota will reset.
        """

        self.client_id = client_id
        self.client = client
        self.quota_reset_date = quota_reset_date
        self.quota_remaining = quota_remaining


class ScopusUtility:
    """Handles the SCOPUS interactions."""

    @staticmethod
    def build_query(*, institution_ids: List[str], period: Type[pendulum.Period]) -> str:
        """Build a SCOPUS API query.

        :param institution_ids: List of Institutional ID to query, e.g, ["60031226"] (Curtin University)
        :param period: A schedule period.
        :return: Constructed web query.
        """

        tail_offset = -4  # To remove ' or ' and ' OR ' from tail of string

        organisations = str()
        for i, inst in enumerate(institution_ids):
            organisations += f"AF-ID({inst}) OR "
        organisations = organisations[:tail_offset]

        # Build publication date range
        search_months = str()

        for point in period.range("months"):
            month_name = calendar.month_name[point.month]
            search_months += f'"{month_name} {point.year}" or '
        search_months = search_months[:tail_offset]

        query = f"({organisations}) AND PUBDATETXT({search_months})"
        return query

    @staticmethod
    def download_period(
        *,
        worker: ScopusUtilWorker,
        conn: str,
        period: Type[pendulum.Period],
        institution_ids: List[str],
        download_dir: str,
    ):
        """Download records for a stated date range.
        The elsapy package currently has a cap of 5000 results per query. So in the unlikely event any institution has
        more than 5000 entries per month, this will present a problem.

        :param worker: Worker that will do the downloading.
        :param conn: Connection ID from Airflow (minus scopus_)
        :param period: Period to download.
        :param institution_ids: List of institutions to query concurrently.
        :param download_dir: Path to save downloaded files to.
        """

        timestamp = pendulum.now("UTC").isoformat()
        save_file = os.path.join(download_dir, f"{period.start}_{period.end}_{timestamp}.json")
        logging.info(f"{conn} worker {worker.client_id}: retrieving period {period.start} - {period.end}")
        query = ScopusUtility.build_query(institution_ids=institution_ids, period=period)
        result, num_results = ScopusUtility.make_query(worker=worker, query=query)
        logging.info(f"{conn}: {num_results} results retrieved")
        write_to_file(result, save_file)

    @staticmethod
    def sleep_if_needed(*, reset_date: pendulum.DateTime, conn: str):
        """Sleep until reset_date.

        :param reset_date: Date(time) to sleep to.
        :param conn: Connection id from Airflow.
        """

        now = pendulum.now("UTC")
        sleep_time = (reset_date - now).seconds
        if sleep_time > 0:
            logging.info(f"{conn}: Sleeping for {sleep_time} seconds until a worker is ready.")
            sleep(sleep_time)

    @staticmethod
    def update_reset_date(*, conn: str, error_msg: str, worker: ScopusUtilWorker):
        """Update the reset date to closest date that will make a worker available.

        :param conn: Airflow connection ID.
        :param error_msg: Error message from quota exceeded exception.
        :param worker: Worker that will do the downloading.
        """

        renews_ts = ScopusClient.get_reset_date_from_error(error_msg)
        worker.quota_reset_date = pendulum.from_timestamp(renews_ts)

        logging.warning(f"{conn} worker {worker.client_id}: quoted exceeded. New reset date: {worker.quota_reset_date}")

    @staticmethod
    def clear_task_queue(queue: Queue):
        """Clear a queue.

        :param queue: Queue to clear.
        """

        while not queue.empty():
            try:
                queue.get(False)
            except Empty:
                continue
            queue.task_done()

    @staticmethod
    def download_worker(
        *,
        worker: ScopusUtilWorker,
        exit_event: Event,
        taskq: Queue,
        conn: str,
        institution_ids: List[str],
        download_dir: str,
    ):
        """Download worker method used by parallel downloader.

        :param worker: worker to use.
        :param exit_event: exit event to monitor.
        :param taskq: tasks queue.
        :param conn: Airflow connection ID.
        :param institution_ids: List of institutions to query concurrently.
        :param download_dir: Path to save downloaded files to.
        """

        while True:
            try:
                logging.info(f"{conn} worker {worker.client_id}: attempting to get a task")
                task = taskq.get(block=True, timeout=ScopusUtilWorker.QUEUE_WAIT_TIME)
                logging.info(f"{conn} worker {worker.client_id}: received task {task}")
            except Empty:
                if exit_event.is_set():
                    logging.info(f"{conn} worker {worker.client_id}: received exit event. Returning results.")
                    break
                logging.info(f"{conn} worker {worker.client_id}: get task timeout. Retrying.")
                continue

            try:  # Got task. Try to download.
                logging.info(f"{conn} worker {worker.client_id}: downloading {task}")
                ScopusUtility.download_period(
                    worker=worker, conn=conn, period=task, institution_ids=institution_ids, download_dir=download_dir
                )
                taskq.task_done()
                logging.info(f"{conn} worker {worker.client_id}: download done for {task}")
            except Exception as e:
                logging.error(f"Received error: {e}")
                taskq.task_done()

                error_msg = str(e)
                if error_msg.startswith(ScopusClient.QUOTA_EXCEED_ERROR_PREFIX):
                    ScopusUtility.update_reset_date(conn=conn, error_msg=error_msg, worker=worker)
                    taskq.put(task)

                    ScopusUtility.sleep_if_needed(reset_date=worker.quota_reset_date, conn=conn)
                    continue

                # Need to clear the queue before we raise exception otherwise join blocks forever
                ScopusUtility.clear_task_queue(taskq)
                raise AirflowException(error_msg)

    @staticmethod
    def download_parallel(
        *, workers: List[ScopusUtilWorker], taskq: Queue, conn: str, institution_ids: List[str], download_dir: str
    ):
        """Download SCOPUS snapshot with parallel sessions. Tasks will be distributed in parallel to the available
        keys. Each key will independently fetch a task from the queue when it's free so there's no guarantee of load
        balance.

        :param workers: List of workers available.
        :param taskq: tasks queue.
        :param conn: Airflow connection ID.
        :param institution_ids: List of institutions to query concurrently.
        :param download_dir: Path to save downloaded files to.
        """

        sessions = len(workers)
        logging.info(f"Creating {sessions} concurrent sessions.")
        with ThreadPoolExecutor(max_workers=sessions) as executor:
            futures = list()
            thread_exit = Event()

            for worker in workers:
                futures.append(
                    executor.submit(
                        ScopusUtility.download_worker,
                        worker=worker,
                        exit_event=thread_exit,
                        taskq=taskq,
                        conn=conn,
                        institution_ids=institution_ids,
                        download_dir=download_dir,
                    )
                )

            taskq.join()  # Wait until all tasks done
            logging.info(f"{conn}: all tasks fetched. Signalling threads to exit.")
            thread_exit.set()

            for future in as_completed(futures):
                future.result()

    @staticmethod
    def make_query(*, worker: ScopusUtilWorker, query: str) -> Tuple[str, int]:
        """Throttling wrapper for the API call. This is a global limit for this API when called from a program on the
        same machine. Limits specified in ScopusUtilConst class.

        Throttle limits may or may not be enforced. Probably depends on how executors spin up tasks.

        :param worker: ScopusUtilWorker object.
        :param query: Query object.
        :returns: Query results.
        """

        results, _, _ = worker.client.retrieve(query)
        return json.dumps(results), len(results)


class ScopusJsonParser:
    """Helper methods to process the json from SCOPUS into desired structure."""

    @staticmethod
    def get_affiliations(data: Dict[str, Any]) -> Union[None, List[Dict[str, Any]]]:
        """Get the affiliation field.

        :param data: json response from SCOPUS.
        :return list of affiliation details.
        """

        affiliations = list()
        if "affiliation" not in data:
            return None

        for affiliation in data["affiliation"]:
            affil = dict()
            affil["name"] = get_entry_or_none(affiliation, "affilname")
            affil["city"] = get_entry_or_none(affiliation, "affiliation-city")
            affil["country"] = get_entry_or_none(affiliation, "affiliation-country")

            # Available in complete view
            affil["id"] = get_entry_or_none(affiliation, "afid")
            affil["name_variant"] = get_entry_or_none(affiliation, "name-variant")
            affiliations.append(affil)

        if len(affiliations) == 0:
            return None

        return affiliations

    @staticmethod
    def get_authors(data: Dict[str, Any]) -> Union[None, List[Dict[str, Any]]]:
        """Get the author field. Won't know if this parser is going to throw error unless we get access to api key
            with complete view access.

        :param data: json response from SCOPUS.
        :return list of authors' details.
        """

        author_list = list()
        if "author" not in data:
            return None

        # Assuming there's a list given the doc says complete author list
        authors = data["author"]
        for author in authors:
            ad = dict()
            ad["authid"] = get_entry_or_none(author, "authid")  # Not sure what this is or how it's diff to afid
            ad["orcid"] = get_entry_or_none(author, "orcid")
            ad["full_name"] = get_entry_or_none(author, "authname")  # Taking a guess that this is what it is
            ad["first_name"] = get_entry_or_none(author, "given-name")
            ad["last_name"] = get_entry_or_none(author, "surname")
            ad["initials"] = get_entry_or_none(author, "initials")
            ad["afid"] = get_entry_or_none(author, "afid")
            author_list.append(ad)

        if len(author_list) == 0:
            return None

        return author_list

    @staticmethod
    def get_identifier_list(data: dict, id_type: str) -> Union[None, List[str]]:
        """Get the list of document identifiers or null of it does not exist.  This string/list behaviour was observed
        for ISBNs so using it for other identifiers just in case.

        :param data: json response from SCOPUS.
        :param id_type: type of identifier, e.g., 'isbn'
        :return: List of identifiers.
        """

        identifier = list()
        if id_type not in data:
            return None

        id_data = data[id_type]
        if isinstance(id_data, str):
            identifier.append(id_data)
        else:  # Only other observed case is list
            for entry in id_data:
                identifier.append(entry["$"])  # This is what showed up in ISBN example in list situation

        if len(identifier) == 0:
            return None

        return identifier

    @staticmethod
    def parse_json(*, data: dict, snapshot_date: pendulum.DateTime, institution_ids: List[str]) -> dict:
        """Turn json data into db schema format.

        :param data: json response from SCOPUS.
        :param harvest_datetime: isoformat string of time the fetch took place.
        :param snapshot_date: DAG execution date.
        :param institution_ids: List of institution ids used in the query.
        :return: dict of data in right field format.
        """

        entry = dict()
        entry["snapshot_date"] = snapshot_date.date().isoformat()  # Release date (date string)
        entry["institution_ids"] = institution_ids

        entry["title"] = get_entry_or_none(data, "dc:title")  # Article title
        entry["identifier"] = get_entry_or_none(data, "dc:identifier")  # Scopus ID
        entry["creator"] = get_entry_or_none(data, "dc:creator")  # First author name
        entry["publication_name"] = get_entry_or_none(data, "prism:publicationName")  # Source title
        entry["cover_date"] = get_entry_or_none(data, "prism:coverDate")  # Publication date
        entry["doi"] = ScopusJsonParser.get_identifier_list(data, "prism:doi")  # DOI
        entry["eissn"] = ScopusJsonParser.get_identifier_list(data, "prism:eIssn")  # Electronic ISSN
        entry["issn"] = ScopusJsonParser.get_identifier_list(data, "prism:issn")  # ISSN
        entry["isbn"] = ScopusJsonParser.get_identifier_list(data, "prism:isbn")  # ISBN
        entry["aggregation_type"] = get_entry_or_none(data, "prism:aggregationType")  # Source type
        entry["pubmed_id"] = get_entry_or_none(data, "pubmed-id")  # MEDLINE identifier
        entry["pii"] = get_entry_or_none(data, "pii")  # PII Publisher item identifier
        entry["eid"] = get_entry_or_none(data, "eid")  # Electronic ID
        entry["subtype_description"] = get_entry_or_none(data, "subtypeDescription")  # Document Type description
        entry["open_access"] = get_entry_or_none(data, "openaccess", int)  # Open access status. (Integer)
        entry["open_access_flag"] = get_entry_or_none(data, "openaccessFlag")  # Open access status. (Boolean)
        entry["citedby_count"] = get_entry_or_none(data, "citedby-count", int)  # Cited by count (integer)
        entry["source_id"] = get_entry_or_none(data, "source-id", int)  # Source ID (integer)
        entry["affiliations"] = ScopusJsonParser.get_affiliations(data)  # Affiliations
        entry["orcid"] = get_entry_or_none(data, "orcid")  # ORCID

        # Available in complete view
        entry["authors"] = ScopusJsonParser.get_authors(data)  # List of authors
        entry["abstract"] = get_entry_or_none(data, "dc:description")  # Abstract
        entry["keywords"] = get_as_list(data, "authkeywords")  # Assuming it's a list of strings.
        entry["article_number"] = get_entry_or_none(data, "article-number")  # Article number (unclear if int or str)
        entry["fund_agency_ac"] = get_entry_or_none(data, "fund-acr")  # Funding agency acronym
        entry["fund_agency_id"] = get_entry_or_none(data, "fund-no")  # Funding agency identification
        entry["fund_agency_name"] = get_entry_or_none(data, "fund-sponsor")  # Funding agency name

        return entry
