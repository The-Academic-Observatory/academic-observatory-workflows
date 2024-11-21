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


import pendulum
from airflow import DAG
from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.cncf.kubernetes.secret import Secret

from academic_observatory_workflows.config import project_path
from academic_observatory_workflows.unpaywall_telescope import tasks
from academic_observatory_workflows.unpaywall_telescope.release import UnpaywallRelease
from observatory_platform.google.bigquery import bq_create_dataset, bq_find_schema
from observatory_platform.airflow.airflow import on_failure_callback
from observatory_platform.airflow.workflow import CloudWorkspace
from observatory_platform.airflow.sensors import PreviousDagRunSensor
from observatory_platform.airflow.tasks import check_dependencies, gke_create_storage, gke_delete_storage
from observatory_platform.url_utils import get_observatory_http_header
from observatory_platform.google.gke import GkeParams, gke_make_kubernetes_task_params, gke_make_container_resources

# See https://unpaywall.org/products/data-feed for details of available APIs
SNAPSHOT_URL = "https://api.unpaywall.org/feed/snapshot"
CHANGEFILES_URL = "https://api.unpaywall.org/feed/changefiles"
CHANGEFILES_DOWNLOAD_URL = "https://api.unpaywall.org/daily-feed/changefile"


class DagParams:
    """
    :param dag_id: the id of the DAG.
    :param cloud_workspace: the cloud workspace settings.
    :param bq_dataset_id: the BigQuery dataset id.
    :param bq_table_name: the BigQuery table name.
    :param api_bq_dataset_id: the API dataset id.
    :param schema_folder: the schema folder.
    :param dataset_description: a description for the BigQuery dataset.
    :param table_description: a description for the table.
    :param primary_key: the primary key to use for merging changefiles.
    :param unpaywall_base_url: The unpaywall api base url.
    :param snapshot_expiry_days: the number of days to keep snapshots.
    :param http_header: the http header to use when making requests to Unpaywall.
    :param unpaywall_conn_id: Unpaywall connection key.
    :param observatory_api_conn_id: the Observatory API connection key.
    :param start_date: the start date of the DAG.
    :param schedule: the schedule interval of the DAG.
    :param max_active_runs: the maximum number of DAG runs that can be run at once.
    :param retries: the number of times to retry a task.
    :param gke_namespace: The cluster namespace to use.
    :param gke_volume_name: The name of the persistent volume to create
    :param gke_volume_size: The amount of storage to request for the persistent volume in GiB
    :param kwargs: Takes kwargs for building a GkeParams object.
    """

    def __init__(
        self,
        dag_id: str,
        cloud_workspace: CloudWorkspace,
        bq_dataset_id: str = "unpaywall",
        bq_table_name: str = "unpaywall",
        api_bq_dataset_id: str = "unpaywall",
        schema_folder: str = project_path("unpaywall_telescope", "schema"),
        dataset_description: str = "Unpaywall Data Feed: https://unpaywall.org/products/data-feed",
        table_description: str = "Unpaywall Data Feed: https://unpaywall.org/products/data-feed",
        primary_key: str = "doi",
        unpaywall_base_url: str = "https://api.unpaywall.org",
        snapshot_expiry_days: int = 7,
        http_header: str = None,
        unpaywall_conn_id: str = "unpaywall",
        start_date: pendulum.DateTime = pendulum.datetime(2021, 7, 2),
        schedule: str = "@daily",
        max_active_runs: int = 1,
        retries: int = 3,
        test_run: bool = False,
        gke_volume_size: int = 500,
        gke_namespace: str = "coki-astro",
        gke_volume_name: str = "unpaywall",
        **kwargs,
    ):
        if http_header is None:
            http_header = get_observatory_http_header(package_name="academic_observatory_workflows")
        schema_file_path = bq_find_schema(path=schema_folder, table_name=bq_table_name)

        self.dag_id = dag_id
        self.cloud_workspace = cloud_workspace
        self.bq_dataset_id = bq_dataset_id
        self.bq_table_name = bq_table_name
        self.api_bq_dataset_id = api_bq_dataset_id
        self.schema_folder = schema_folder
        self.schema_file_path = schema_file_path
        self.dataset_description = dataset_description
        self.table_description = table_description
        self.primary_key = primary_key
        self.unpaywall_base_url = unpaywall_base_url
        self.snapshot_expiry_days = snapshot_expiry_days
        self.http_header = http_header
        self.unpaywall_conn_id = unpaywall_conn_id
        self.start_date = start_date
        self.schedule = schedule
        self.max_active_runs = max_active_runs
        self.retries = retries
        self.test_run = test_run
        self.gke_volume_size = gke_volume_size
        self.gke_namespace = gke_namespace
        self.gke_volume_name = gke_volume_name
        self.gke_params = GkeParams(
            gke_volume_size=gke_volume_size, gke_namespace=gke_namespace, gke_volume_name=gke_volume_name, **kwargs
        )


def create_dag(dag_params: DagParams) -> DAG:
    """The Unpaywall Data Feed Telescope."""

    kubernetes_task_params = gke_make_kubernetes_task_params(dag_params.gke_params)
    kubernetes_task_params["log_events_on_failure"] = False

    @dag(
        dag_id=dag_params.dag_id,
        start_date=dag_params.start_date,
        schedule=dag_params.schedule,
        catchup=False,
        max_active_runs=dag_params.max_active_runs,
        tags=["academic-observatory"],
        default_args={
            "owner": "airflow",
            "on_failure_callback": on_failure_callback,
            "retries": dag_params.retries,
        },
    )
    def unpaywall():
        @task
        def fetch_release(**context) -> dict | None:
            """Fetches the release information. On the first DAG run gets the latest snapshot and the necessary changefiles
            required to get the dataset up to date. On subsequent runs it fetches unseen changefiles. It is possible
            for no changefiles to be found after the first run, in which case the rest of the tasks are skipped. Publish
            any available releases as an XCOM to avoid re-querying Unpaywall servers."""

            return tasks.fetch_release(
                dag_id=dag_params.dag_id,
                run_id=context["run_id"],
                dag_run=context["dag_run"],
                cloud_workspace=dag_params.cloud_workspace,
                bq_dataset_id=dag_params.bq_dataset_id,
                bq_table_name=dag_params.bq_table_name,
                api_bq_dataset_id=dag_params.api_bq_dataset_id,
                unpaywall_conn_id=dag_params.unpaywall_conn_id,
                base_url=dag_params.unpaywall_base_url,
            )

        @task.short_circuit
        def short_circuit(release: dict | None, **context) -> bool:
            return release is not None

        @task
        def create_dataset(release: dict, **context) -> None:
            """Create datasets."""

            bq_create_dataset(
                project_id=dag_params.cloud_workspace.output_project_id,
                dataset_id=dag_params.bq_dataset_id,
                location=dag_params.cloud_workspace.data_location,
                description=dag_params.dataset_description,
            )

        @task
        def bq_create_main_table_snapshot(release: dict, dag_params: DagParams, **context) -> None:
            """Create a snapshot of the main table. The purpose of this table is to be able to rollback the table
            if something goes wrong. The snapshot expires after snapshot_expiry_days."""

            tasks.bq_create_main_table_snapshot(release, snapshot_expiry_days=dag_params.snapshot_expiry_days)

        @task.branch
        def branch(release: dict, **context):
            release = UnpaywallRelease.from_dict(release)
            if release.is_first_run:
                return "load_snapshot.load_snapshot_download"
            else:
                return "load_changefiles.load_snapshot_download"

        @task_group
        def load_snapshot(data: dict):
            """Download and process snapshot on first run"""

            @task.kubernetes(
                trigger_rule=TriggerRule.ALL_SUCCESS,
                name="load_snapshot_download",
                container_resources=gke_make_container_resources(
                    {"memory": "8G", "cpu": "8"},
                    dag_params.gke_params.gke_resource_overrides.get("load_snapshot_download"),
                ),
                secrets=[Secret("env", "UNPAYWALL_API_KEY", "unpaywall", "api-key")],
                **kubernetes_task_params,
            )
            def load_snapshot_download(release: dict, dag_params, **context):
                """Downlaod the most recent Unpaywall snapshot."""
                from academic_observatory_workflows.unpaywall_telescope import tasks

                tasks.load_snapshot_download(
                    release, http_header=dag_params.http_header, base_url=dag_params.unpaywall_base_url
                )

            @task.kubernetes(
                trigger_rule=TriggerRule.ALL_SUCCESS,
                name="load_snapshot_upload_downloaded",
                container_resources=gke_make_container_resources(
                    {"memory": "4G", "cpu": "4"},
                    dag_params.gke_params.gke_resource_overrides.get("load_snapshot_upload_downloaded"),
                ),
                **kubernetes_task_params,
            )
            def load_snapshot_upload_downloaded(release: dict, dag_params, **context):
                """Upload the downloaded snapshot for the given release."""
                from academic_observatory_workflows.unpaywall_telescope import tasks

                tasks.load_snapshot_upload_downloaded(release, cloud_workspace=dag_params.cloud_workspace)

            @task.kubernetes(
                trigger_rule=TriggerRule.ALL_SUCCESS,
                name="load_snapshot_extract",
                container_resources=gke_make_container_resources(
                    {"memory": "16G", "cpu": "16"},
                    dag_params.gke_params.gke_resource_overrides.get("load_snapshot_extract"),
                ),
                **kubernetes_task_params,
            )
            def load_snapshot_extract(release: dict, **context):
                """Gunzip the downloaded Unpaywall snapshot."""
                from academic_observatory_workflows.unpaywall_telescope import tasks

                tasks.load_snapshot_extract(release)

            @task.kubernetes(
                trigger_rule=TriggerRule.ALL_SUCCESS,
                name="load_snapshot_transform",
                container_resources=gke_make_container_resources(
                    {"memory": "16G", "cpu": "16"},
                    dag_params.gke_params.gke_resource_overrides.get("load_snapshot_transform"),
                ),
                **kubernetes_task_params,
            )
            def load_snapshot_transform(release: dict, **context):
                """Transform the snapshot into the main table file. Find and replace the 'authenticated-orcid' string in the
                jsonl to 'authenticated_orcid'."""
                from academic_observatory_workflows.unpaywall_telescope import tasks

                tasks.load_snapshot_transform(release)

            @task.kubernetes(
                trigger_rule=TriggerRule.ALL_SUCCESS,
                name="load_snapshot_split_main_table_file",
                container_resources=gke_make_container_resources(
                    {"memory": "4G", "cpu": "4"},
                    dag_params.gke_params.gke_resource_overrides.get("load_snapshot_split_main_table_file"),
                ),
                **kubernetes_task_params,
            )
            def load_snapshot_split_main_table_file(release: dict, **context):
                """Split main table into multiple smaller files"""
                from academic_observatory_workflows.unpaywall_telescope import tasks

                tasks.load_snapshot_split_main_table_file(release=release, **context)

            @task.kubernetes(
                trigger_rule=TriggerRule.ALL_SUCCESS,
                name="load_snapshot_upload_main_table_files",
                container_resources=gke_make_container_resources(
                    {"memory": "4G", "cpu": "4"},
                    dag_params.gke_params.gke_resource_overrides.get("load_snapshot_upload_main_table_files"),
                ),
                **kubernetes_task_params,
            )
            def load_snapshot_upload_main_table_files(release: dict, dag_params, **context) -> None:
                """Upload the main table files to Cloud Storage."""
                from academic_observatory_workflows.unpaywall_telescope import tasks

                tasks.load_snapshot_upload_main_table_files(release=release, cloud_workspace=dag_params.cloud_workspace)

            @task
            def load_snapshot_bq_load(release: dict, dag_params: DagParams, **context) -> None:
                """Load the main table."""

                tasks.load_snapshot_bq_load(
                    release=release,
                    schema_file_path=dag_params.schema_file_path,
                    table_description=dag_params.table_description,
                )

            task_download = load_snapshot_download(data, dag_params)
            task_upload_downloaded = load_snapshot_upload_downloaded(data, dag_params)
            task_extract = load_snapshot_extract(data)
            task_transform = load_snapshot_transform(data)
            task_split_main_table_file = load_snapshot_split_main_table_file(data)
            task_upload_main_table_files = load_snapshot_upload_main_table_files(data, dag_params)
            task_bq_load = load_snapshot_bq_load(data, dag_params)

            (
                task_download
                >> task_upload_downloaded
                >> task_extract
                >> task_transform
                >> task_split_main_table_file
                >> task_upload_main_table_files
                >> task_bq_load
            )

        @task_group
        def load_changefiles(data: dict):
            """Download and process change files on each run"""

            @task.kubernetes(
                trigger_rule=TriggerRule.ALL_SUCCESS,
                name="load_changefiles_download",
                container_resources=gke_make_container_resources(
                    {"memory": "8G", "cpu": "8"},
                    dag_params.gke_params.gke_resource_overrides.get("load_changefiles_download"),
                ),
                secrets=[Secret("env", "UNPAYWALL_API_KEY", "unpaywall", "api-key")],
                **kubernetes_task_params,
            )
            def load_changefiles_download(release: dict, dag_params, **context):
                """Download the Unpaywall change files that are required for this release."""
                from academic_observatory_workflows.unpaywall_telescope import tasks

                tasks.load_changefiles_download(
                    release=release,
                    http_header=dag_params.http_header,
                    base_url=dag_params.unpaywall_base_url,
                )

            @task.kubernetes(
                trigger_rule=TriggerRule.ALL_SUCCESS,
                name="load_changefiles_upload_downloaded",
                container_resources=gke_make_container_resources(
                    {"memory": "4G", "cpu": "4"},
                    dag_params.gke_params.gke_resource_overrides.get("load_changefiles_upload_downloaded"),
                ),
                **kubernetes_task_params,
            )
            def load_changefiles_upload_downloaded(release: dict, dag_params, **context):
                """Upload the downloaded changefiles for the given release."""
                from academic_observatory_workflows.unpaywall_telescope import tasks

                tasks.load_changefiles_upload_downloaded(release=release, cloud_workspace=dag_params.cloud_workspace)

            @task.kubernetes(
                trigger_rule=TriggerRule.ALL_SUCCESS,
                name="load_changefiles_extract",
                container_resources=gke_make_container_resources(
                    {"memory": "8G", "cpu": "8"},
                    dag_params.gke_params.gke_resource_overrides.get("load_changefiles_extract"),
                ),
                **kubernetes_task_params,
            )
            def load_changefiles_extract(release: dict, **context):
                """Task to gunzip the downloaded Unpaywall changefiles."""
                from academic_observatory_workflows.unpaywall_telescope import tasks

                tasks.load_changefiles_extract(release)

            @task.kubernetes(
                trigger_rule=TriggerRule.ALL_SUCCESS,
                name="load_changefiles_transform",
                container_resources=gke_make_container_resources(
                    {"memory": "8G", "cpu": "8"},
                    dag_params.gke_params.gke_resource_overrides.get("load_changefiles_transform"),
                ),
                **kubernetes_task_params,
            )
            def load_changefiles_transform(release: dict, dag_params, **context):
                """Task to transform the Unpaywall changefiles merging them into the upsert file.
                Find and replace the 'authenticated-orcid' string in the jsonl to 'authenticated_orcid'."""
                from academic_observatory_workflows.unpaywall_telescope import tasks

                tasks.load_changefiles_transform(release=release, primary_key=dag_params.primary_key)

            @task.kubernetes(
                trigger_rule=TriggerRule.ALL_SUCCESS,
                name="load_changefiles_upload",
                container_resources=gke_make_container_resources(
                    {"memory": "4G", "cpu": "4"},
                    dag_params.gke_params.gke_resource_overrides.get("load_changefiles_upload"),
                ),
                **kubernetes_task_params,
            )
            def load_changefiles_upload(release: dict, dag_params, **context) -> None:
                """Upload the transformed data to Cloud Storage.
                :raises AirflowException: Raised if the files to be uploaded are not found."""
                from academic_observatory_workflows.unpaywall_telescope import tasks

                tasks.load_changefiles_upload(release=release, cloud_workspace=dag_params.cloud_workspace)

            @task
            def load_changefiles_bq_load(release: dict, dag_params, **context) -> None:
                """Load the upsert table."""

                tasks.load_changefiles_bq_load(
                    release=release,
                    schema_file_path=dag_params.schema_file_path,
                    table_description=dag_params.table_description,
                )

            @task
            def load_changefiles_bq_upsert(release: dict, dag_params: DagParams, **context) -> None:
                """Upsert the records from the upserts table into the main table."""

                tasks.load_changefiles_bq_upsert(release=release, primary_key=dag_params.primary_key)

            task_download = load_changefiles_download(data, dag_params)
            task_upload_downloaded = load_changefiles_upload_downloaded(data, dag_params)
            task_extract = load_changefiles_extract(data)
            task_transform = load_changefiles_transform(data, dag_params)
            task_upload = load_changefiles_upload(data, dag_params)
            task_bq_load = load_changefiles_bq_load(data, dag_params)
            task_bq_upsert = load_changefiles_bq_upsert(data, dag_params)

            (
                task_download
                >> task_upload_downloaded
                >> task_extract
                >> task_transform
                >> task_upload
                >> task_bq_load
                >> task_bq_upsert
            )

        @task
        def add_dataset_release(release: dict, dag_params: DagParams, **context) -> None:
            """Adds release information to API."""

            tasks.add_dataset_release(release, dag_params.api_bq_dataset_id)

        @task
        def cleanup_workflow(release: dict, **context) -> None:
            """Delete all files, folders and XComs associated with this release."""

            tasks.cleanup_workflow(release)

        # Wait for the previous DAG run to finish to make sure that
        # changefiles are processed in the correct order
        external_task_id = "dag_run_complete"
        sensor = PreviousDagRunSensor(
            dag_id=dag_params.dag_id,
            external_task_id=external_task_id,
        )
        task_check_dependencies = check_dependencies(airflow_conns=[dag_params.unpaywall_conn_id])
        xcom_release = fetch_release()
        task_short_circuit = short_circuit(xcom_release)
        task_create_dataset = create_dataset(xcom_release)
        task_bq_create_main_table_snapshot = bq_create_main_table_snapshot(xcom_release, dag_params)
        task_branch = branch(xcom_release)
        task_group_load_snapshot = load_snapshot(xcom_release)
        task_group_load_changefiles = load_changefiles(xcom_release)
        task_add_dataset_release = add_dataset_release(xcom_release, dag_params)
        task_cleanup_workflow = cleanup_workflow(xcom_release)
        # The last task that the next DAG run's ExternalTaskSensor waits for.
        task_dag_run_complete = EmptyOperator(task_id=external_task_id)
        if dag_params.test_run:
            task_create_storage = EmptyOperator(task_id="gke_create_storage")
            task_delete_storage = EmptyOperator(task_id="gke_delete_storage")
        else:
            task_create_storage = gke_create_storage(
                volume_name=dag_params.gke_params.gke_volume_name,
                volume_size=dag_params.gke_params.gke_volume_size,
                kubernetes_conn_id=dag_params.gke_params.gke_conn_id,
            )
            task_delete_storage = gke_delete_storage(
                volume_name=dag_params.gke_params.gke_volume_name,
                kubernetes_conn_id=dag_params.gke_params.gke_conn_id,
            )

        (
            sensor
            >> task_check_dependencies
            >> xcom_release
            >> task_short_circuit
            >> task_create_dataset
            >> task_bq_create_main_table_snapshot
            >> task_create_storage
            >> task_branch
            >> task_group_load_snapshot
            >> task_group_load_changefiles
            >> task_delete_storage
            >> task_add_dataset_release
            >> task_cleanup_workflow
            >> task_dag_run_complete
        )

        task_branch >> task_group_load_changefiles

    return unpaywall()
