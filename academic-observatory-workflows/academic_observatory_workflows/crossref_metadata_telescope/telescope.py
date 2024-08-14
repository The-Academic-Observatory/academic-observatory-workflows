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

# Author: Aniek Roelofs, James Diprose, Keegan Smith


from __future__ import annotations

from dataclasses import dataclass
import os

import pendulum
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.cncf.kubernetes.secret import Secret

from academic_observatory_workflows.config import project_path
from observatory_platform.airflow.airflow import on_failure_callback
from observatory_platform.airflow.tasks import check_dependencies, gke_create_storage, gke_delete_storage
from observatory_platform.airflow.workflow import CloudWorkspace
from observatory_platform.google.gke import GkeParams, gke_make_kubernetes_task_params, gke_make_container_resources


@dataclass
class DagParams:
    """Parameters for the Crossref Metadata Telescope

    :param dag_id: the id of the DAG.
    :param cloud_workspace: the cloud workspace settings.
    :param bq_dataset_id: the BigQuery dataset id.
    :param bq_table_name: the BigQuery table name.
    :param api_dataset_id: the Dataset ID to use when storing releases.
    :param schema_folder: the SQL schema path.
    :param dataset_description: description for the BigQuery dataset.
    :param table_description: description for the BigQuery table.
    :param crossref_metadata_conn_id: the Crossref Metadata Airflow connection key.
    :param crossref_base_url: The crossref metadata api base url.
    :param observatory_api_conn_id: the Observatory API connection key.
    :param max_processes: the number of processes used with ProcessPoolExecutor to transform files in parallel.
    :param batch_size: the number of files to send to ProcessPoolExecutor at one time.
    :param start_date: the start date of the DAG.
    :param schedule: the schedule interval of the DAG.
    :param catchup: whether to catchup the DAG or not.
    :param queue: what Airflow queue this job runs on.
    :param max_active_runs: the maximum number of DAG runs that can be run at once.
    :param retries: the number of times to retry a task.
    :param test_run: Whether this is a test run or not.
    :param gke_namespace: The cluster namespace to use.
    :param gke_volume_name: The name of the persistent volume to create
    :param gke_volume_size: The amount of storage to request for the persistent volume in GiB
    :param kwargs: Takes kwargs for building a GkeParams object.
    """

    def __init__(
        self,
        dag_id: str,
        cloud_workspace: CloudWorkspace,
        bq_dataset_id: str = "crossref_metadata",
        bq_table_name: str = "crossref_metadata",
        api_bq_dataset_id: str = "crossref_metadata",
        schema_folder: str = project_path("crossref_metadata_telescope", "schema"),
        dataset_description: str = (
            "The Crossref Metadata Plus dataset: https://www.crossref.org/services/metadata-retrieval/metadata-plus/"
        ),
        table_description: str = (
            "The Crossref Metadata Plus dataset: https://www.crossref.org/services/metadata-retrieval/metadata-plus/"
        ),
        crossref_metadata_conn_id: str = "crossref_metadata",
        crossref_base_url: str = "https://api.crossref.org",
        max_processes: int = os.cpu_count(),
        batch_size: int = 20,
        start_date: pendulum.DateTime = pendulum.datetime(2020, 6, 7),
        schedule: str = "0 0 7 * *",
        catchup: bool = True,
        queue: str = "default",
        max_active_runs: int = 1,
        retries: int = 3,
        test_run: bool = False,
        gke_volume_size: int = 2500,
        gke_namespace: str = "coki-astro",
        gke_volume_name: str = "crossref-metadata",
        **kwargs,
    ):

        self.dag_id = dag_id
        self.cloud_workspace = cloud_workspace
        self.bq_dataset_id = bq_dataset_id
        self.bq_table_name = bq_table_name
        self.api_bq_dataset_id = api_bq_dataset_id
        self.schema_folder = schema_folder
        self.dataset_description = dataset_description
        self.table_description = table_description
        self.crossref_metadata_conn_id = crossref_metadata_conn_id
        self.crossref_base_url = crossref_base_url
        self.max_processes = max_processes
        self.batch_size = batch_size
        self.start_date = start_date
        self.schedule = schedule
        self.catchup = catchup
        self.queue = queue
        self.max_active_runs = max_active_runs
        self.retries = retries
        self.test_run = test_run
        self.gke_params = GkeParams(
            gke_volume_size=gke_volume_size, gke_namespace=gke_namespace, gke_volume_name=gke_volume_name, **kwargs
        )


def create_dag(dag_params: DagParams) -> DAG:
    """The Crossref Metadata DAG"""

    kubernetes_task_params = gke_make_kubernetes_task_params(dag_params.gke_params)

    @dag(
        dag_id=dag_params.dag_id,
        start_date=dag_params.start_date,
        schedule=dag_params.schedule,
        catchup=dag_params.catchup,
        max_active_runs=dag_params.max_active_runs,
        tags=["academic-observatory"],
        default_args={
            "owner": "airflow",
            "on_failure_callback": on_failure_callback,
            "retries": dag_params.retries,
            "queue": dag_params.queue,
        },
    )
    def crossref_metadata():
        @task
        def fetch_release(dag_params: DagParams, **context) -> dict:
            """Fetch the release for this month, making sure that it exists."""
            from academic_observatory_workflows.crossref_metadata_telescope import tasks

            return tasks.fetch_release(
                run_id=context["run_id"],
                data_interval_start=context["data_interval_start"],
                data_interval_end=context["data_interval_end"],
                cloud_workspace=dag_params.cloud_workspace,
                crossref_metadata_conn_id=dag_params.crossref_metadata_conn_id,
                dag_id=dag_params.dag_id,
            )

        @task.kubernetes(
            name="download",
            container_resources=gke_make_container_resources(
                {"memory": "2G", "cpu": "2"}, dag_params.gke_params.gke_resource_overrides.get("download")
            ),
            secrets=[Secret("env", "CROSSREF_METADATA_API_KEY", "crossref-metadata", "api-key")],
            **kubernetes_task_params,
        )
        def download(release: dict, dag_params, **context):
            """Downloads the data"""
            from academic_observatory_workflows.crossref_metadata_telescope import tasks

            tasks.download(release, base_url=dag_params.crossref_base_url)

        @task.kubernetes(
            name="upload_download",
            container_resources=gke_make_container_resources(
                {"memory": "2Gi", "cpu": "2"}, dag_params.gke_params.gke_resource_overrides.get("upload_downloaded")
            ),
            **kubernetes_task_params,
        )
        def upload_downloaded(release: dict, **context):
            """Upload downloaded data to Cloud Storage."""
            from academic_observatory_workflows.crossref_metadata_telescope import tasks

            tasks.upload_downloaded(release)

        @task.kubernetes(
            name="extract",
            container_resources=gke_make_container_resources(
                {"memory": "2G", "cpu": "2"}, dag_params.gke_params.gke_resource_overrides.get("extract")
            ),
            **kubernetes_task_params,
        )
        def extract(release: dict, **context):
            """Extracts the compressed data"""
            from academic_observatory_workflows.crossref_metadata_telescope import tasks

            tasks.extract(release, **context)

        @task.kubernetes(
            name="transform",
            container_resources=gke_make_container_resources(
                {"memory": "2G", "cpu": "2"}, dag_params.gke_params.gke_resource_overrides.get("transform")
            ),
            **kubernetes_task_params,
        )
        def transform(release: dict, dag_params, **context):
            """Task to transform the CrossrefMetadataRelease release for a given month.
            Each extracted file is transformed."""
            from academic_observatory_workflows.crossref_metadata_telescope import tasks

            tasks.transform(release, max_processes=dag_params.max_processes, batch_size=dag_params.batch_size)

        @task.kubernetes(
            name="upload_transformed",
            container_resources=gke_make_container_resources(
                {"memory": "2G", "cpu": "2"}, dag_params.gke_params.gke_resource_overrides.get("upload_transformed")
            ),
            **kubernetes_task_params,
        )
        def upload_transformed(release: dict, dag_params, **context) -> None:
            """Uploads the transformed data to cloud storage"""
            from academic_observatory_workflows.crossref_metadata_telescope import tasks

            tasks.upload_transformed(release)

        @task
        def bq_load(release: dict, dag_params: DagParams, **context):
            """Loads the data into a bigquery table"""
            from academic_observatory_workflows.crossref_metadata_telescope import tasks

            tasks.bq_load(
                release,
                bq_dataset_id=dag_params.bq_dataset_id,
                bq_table_name=dag_params.bq_table_name,
                dataset_description=dag_params.dataset_description,
                table_description=dag_params.table_description,
                schema_folder=dag_params.schema_folder,
            )

        @task
        def add_dataset_release(release: dict, dag_params: DagParams, **context) -> None:
            """Adds a release to the dataset API"""
            from academic_observatory_workflows.crossref_metadata_telescope import tasks

            tasks.add_dataset_release(release, api_bq_dataset_id=dag_params.api_bq_dataset_id)

        @task
        def cleanup_workflow(release: dict, **context) -> None:
            """Performs cleanup actions"""
            from academic_observatory_workflows.crossref_metadata_telescope import tasks

            tasks.cleanup_workflow(release)

        # Define task connections
        task_check_dependencies = check_dependencies(
            airflow_conns=[dag_params.crossref_metadata_conn_id, dag_params.gke_params.gke_conn_id]
        )
        xcom_release = fetch_release(dag_params)
        task_download = download(xcom_release, dag_params)
        task_upload_downloaded = upload_downloaded(xcom_release)
        task_extract = extract(xcom_release)
        task_transform = transform(xcom_release, dag_params)
        task_upload_transformed = upload_transformed(xcom_release, dag_params)
        task_bq_load = bq_load(xcom_release, dag_params)
        task_add_dataset_release = add_dataset_release(xcom_release, dag_params)
        task_cleanup_workflow = cleanup_workflow(xcom_release)
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
            task_check_dependencies
            >> xcom_release
            >> task_create_storage
            >> task_download
            >> task_upload_downloaded
            >> task_extract
            >> task_transform
            >> task_upload_transformed
            >> task_bq_load
            >> task_delete_storage
            >> task_add_dataset_release
            >> task_cleanup_workflow
        )

    return crossref_metadata()
