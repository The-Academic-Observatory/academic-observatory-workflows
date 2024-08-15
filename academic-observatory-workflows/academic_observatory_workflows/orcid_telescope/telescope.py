# Copyright 2023-2024 Curtin University
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


# Author: Keegan Smith

from __future__ import annotations

from datetime import timedelta
from typing import Optional

import pendulum
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

from academic_observatory_workflows.config import project_path
from observatory_platform.airflow.airflow import on_failure_callback
from observatory_platform.airflow.sensors import PreviousDagRunSensor
from observatory_platform.airflow.tasks import check_dependencies, gke_create_storage, gke_delete_storage
from observatory_platform.airflow.workflow import CloudWorkspace
from observatory_platform.config import AirflowConns
from observatory_platform.google.gke import GkeParams, gke_make_kubernetes_task_params, gke_make_container_resources


class DagParams:
    """
    :param dag_id: the id of the DAG.
    :param cloud_workspace: the cloud workspace settings.
    :param orcid_bucket: the Google Cloud Storage bucket where the ORCID files are stored.
    :param orcid_summaries_prefix: the base folder containing the ORCID summaries.
    :param bq_dataset_id: BigQuery dataset ID.
    :param bq_main_table_name: BigQuery main table name for the ORCID table.
    :param bq_upsert_table_name: BigQuery table name for the ORCID upsert table.
    :param bq_delete_table_name: BigQuery table name for the ORCID delete table.
    :param dataset_description: BigQuery dataset description.
    :param snapshot_expiry_days: the number of days that a snapshot of each entity's main table will take to expire,
    which is set to 31 days so there is some time to rollback after an update.
    :param schema_file_path: the path to the schema file for the records produced by this workflow.
    :param delete_schema_file_path: the path to the delete schema file for the records produced by this workflow.
    :param transfer_attempts: the number of AWS to GCP transfer attempts.
    :param max_workers: maximum processes to use when transforming files.
    :param api_dataset_id: the Dataset ID to use when storing releases.
    :param observatory_api_conn_id: the Observatory API Airflow Connection ID.
    :param aws_orcid_conn_id: Airflow Connection ID for the AWS ORCID bucket.
    :param start_date: the Apache Airflow DAG start date.
    :param schedule: the Apache Airflow schedule interval.
    :param max_active_runs: the maximum number of DAG runs that can be run at once.
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
        orcid_bucket: str = "ao-orcid",
        orcid_summaries_prefix: str = "orcid_summaries",
        bq_dataset_id: str = "orcid",
        api_bq_dataset_id: str = "orcid",
        bq_main_table_name: str = "orcid",
        bq_upsert_table_name: str = "orcid_upsert",
        bq_delete_table_name: str = "orcid_delete",
        dataset_description: str = "The ORCID dataset and supporting tables",
        snapshot_expiry_days: int = 31,
        schema_file_path: str = project_path("orcid_telescope", "schema", "orcid.json"),
        delete_schema_file_path: str = project_path("orcid_telescope", "schema", "orcid_delete.json"),
        transfer_attempts: int = 5,
        max_workers: Optional[int] = None,
        api_dataset_id: str = "orcid",
        observatory_api_conn_id: str = AirflowConns.OBSERVATORY_API,
        aws_orcid_conn_id: str = "aws_orcid",
        start_date: pendulum.DateTime = pendulum.datetime(2023, 6, 1),
        schedule: str = "0 0 * * 0",  # Midnight UTC every Sunday
        max_active_runs: int = 1,
        retries: int = 3,
        test_run: bool = False,
        gke_volume_size: int = 1000,
        gke_namespace: str = "coki-astro",
        gke_volume_name: str = "orcid",
        **kwargs,
    ):
        self.dag_id = dag_id
        self.cloud_workspace = cloud_workspace
        self.orcid_bucket = orcid_bucket
        self.orcid_summaries_prefix = orcid_summaries_prefix
        self.bq_dataset_id = bq_dataset_id
        self.api_bq_dataset_id = api_bq_dataset_id
        self.bq_main_table_name = bq_main_table_name
        self.bq_upsert_table_name = bq_upsert_table_name
        self.bq_delete_table_name = bq_delete_table_name
        self.dataset_description = dataset_description
        self.snapshot_expiry_days = snapshot_expiry_days
        self.schema_file_path = schema_file_path
        self.delete_schema_file_path = delete_schema_file_path
        self.transfer_attempts = transfer_attempts
        self.max_workers = max_workers
        self.api_dataset_id = api_dataset_id
        self.observatory_api_conn_id = observatory_api_conn_id
        self.aws_orcid_conn_id = aws_orcid_conn_id
        self.start_date = start_date
        self.schedule = schedule
        self.max_active_runs = max_active_runs
        self.retries = retries
        self.test_run = test_run
        self.gke_params = GkeParams(
            gke_volume_size=gke_volume_size, gke_namespace=gke_namespace, gke_volume_name=gke_volume_name, **kwargs
        )


def create_dag(dag_params: DagParams) -> DAG:
    """Construct an ORCID telescope instance."""

    kubernetes_task_params = gke_make_kubernetes_task_params(dag_params.gke_params)

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
    def orcid():
        @task
        def fetch_release(**context) -> dict:
            """Generates the OrcidRelease object."""

            from academic_observatory_workflows.orcid_telescope import tasks

            return tasks.fetch_release(
                dag_id=dag_params.dag_id,
                run_id=context["run_id"],
                dag_run=context["dag_run"],
                data_interval_start=context["data_interval_start"],
                data_interval_end=context["data_interval_end"],
                cloud_workspace=dag_params.cloud_workspace,
                api_bq_dataset_id=dag_params.api_bq_dataset_id,
                bq_dataset_id=dag_params.bq_dataset_id,
                bq_main_table_name=dag_params.bq_main_table_name,
                bq_upsert_table_name=dag_params.bq_upsert_table_name,
                bq_delete_table_name=dag_params.bq_delete_table_name,
            )

        @task
        def create_dataset(release: dict, dag_params, **context) -> None:
            """Create datasets"""

            from academic_observatory_workflows.orcid_telescope import tasks

            tasks.create_dataset(release, dataset_description=dag_params.dataset_description)

        @task
        def transfer_orcid(release: dict, dag_params, **context):
            """Sync files from AWS bucket to Google Cloud bucket."""

            from academic_observatory_workflows.orcid_telescope import tasks

            tasks.transfer_orcid(
                release,
                aws_orcid_conn_id=dag_params.aws_orcid_conn_id,
                transfer_attempts=dag_params.transfer_attempts,
                orcid_bucket=dag_params.orcid_bucket,
                orcid_summaries_prefix=dag_params.orcid_summaries_prefix,
            )

        @task
        def bq_create_main_table_snapshot(release: dict, dag_params, **context):
            """Create a snapshot of each main table. The purpose of this table is to be able to rollback the table
            if something goes wrong. The snapshot expires after snapshot_expiry_days."""
            from academic_observatory_workflows.orcid_telescope import tasks

            tasks.bq_create_main_table_snapshot(release, snapshot_expiry_days=dag_params.snapshot_expiry_days)

        @task.kubernetes(
            trigger_rule=TriggerRule.ALL_DONE,
            name="create_manifests",
            container_resources=gke_make_container_resources(
                {"memory": "16G", "cpu": "16"}, dag_params.gke_params.gke_resource_overrides.get("create_manifests")
            ),
            **kubernetes_task_params,
        )
        def create_manifests(release: dict, dag_params, **context):
            """Create a manifest of all the modified files in the orcid bucket."""
            from academic_observatory_workflows.orcid_telescope import tasks

            tasks.create_manifests(
                release,
                orcid_bucket=dag_params.orcid_bucket,
                orcid_summaries_prefix=dag_params.orcid_summaries_prefix,
                max_workers=dag_params.max_workers,
            )

        @task.kubernetes(
            name="download",
            container_resources=gke_make_container_resources(
                {"memory": "8G", "cpu": "8"}, dag_params.gke_params.gke_resource_overrides.get("download")
            ),
            **kubernetes_task_params,
        )
        def download(release: dict, **context):
            """Reads each batch's manifest and downloads the files from the gcs bucket."""
            from academic_observatory_workflows.orcid_telescope import tasks

            tasks.download(release)

        @task.kubernetes(
            name="transform",
            container_resources=gke_make_container_resources(
                {"memory": "16G", "cpu": "16"}, dag_params.gke_params.gke_resource_overrides.get("tranform")
            ),
            **kubernetes_task_params,
        )
        def transform(release: dict, dag_parms, **context):
            """Transforms the downloaded files into serveral bigquery-compatible .jsonl files"""
            from academic_observatory_workflows.orcid_telescope import tasks

            tasks.transform(release, max_workers=dag_params.max_workers)

        @task.kubernetes(
            name="upload_transformed",
            container_resources=gke_make_container_resources(
                {"memory": "8G", "cpu": "8"}, dag_params.gke_params.gke_resource_overrides.get("upload_tranformed")
            ),
            **kubernetes_task_params,
        )
        def upload_transformed(release: dict, **context):
            """Uploads the upsert and delete files to the transform bucket."""
            from academic_observatory_workflows.orcid_telescope import tasks

            tasks.upload_transformed(release)

        @task
        def bq_load_main_table(release: dict, dag_params, **context):
            """Load the main table."""
            from academic_observatory_workflows.orcid_telescope import tasks

            tasks.bq_load_main_table(release, schema_file_path=dag_params.schema_file_path)

        @task(trigger_rule=TriggerRule.ALL_DONE)
        def bq_load_upsert_table(release: dict, dag_params, **context):
            """Load the upsert table into bigquery"""
            from academic_observatory_workflows.orcid_telescope import tasks

            tasks.bq_load_upsert_table(release, schema_file_path=dag_params.schema_file_path)

        @task(trigger_rule=TriggerRule.ALL_DONE)
        def bq_load_delete_table(release: dict, dag_params, **context):
            """Load the delete table into bigquery"""
            from academic_observatory_workflows.orcid_telescope import tasks

            tasks.bq_load_delete_table(release, delete_schema_file_path=dag_params.delete_schema_file_path)

        @task(trigger_rule=TriggerRule.ALL_DONE)
        def bq_upsert_records(release: dict, **context):
            """Upsert the records from the upserts table into the main table."""
            from academic_observatory_workflows.orcid_telescope import tasks

            tasks.bq_upsert_records(release)

        @task(trigger_rule=TriggerRule.ALL_DONE)
        def bq_delete_records(release: dict, **context):
            """Delete the records in the delete table from the main table."""
            from academic_observatory_workflows.orcid_telescope import tasks

            tasks.bq_delete_records(release)

        @task(trigger_rule=TriggerRule.ALL_DONE)
        def add_dataset_release(release: dict, **context) -> None:
            """Adds release information to API."""
            from academic_observatory_workflows.orcid_telescope import tasks

            tasks.add_dataset_release(release)

        @task
        def cleanup_workflow(release: dict, **context) -> None:
            """Delete all files, folders and XComs associated with this release."""
            from academic_observatory_workflows.orcid_telescope import tasks

            tasks.cleanup_workflow(release)

        external_task_id = "dag_run_complete"
        sensor = PreviousDagRunSensor(
            dag_id=dag_params.dag_id,
            external_task_id=external_task_id,
            execution_delta=timedelta(days=7),  # To match the @weekly schedule_interval
        )
        task_check_dependencies = check_dependencies(airflow_conns=[dag_params.aws_orcid_conn_id])
        xcom_release = fetch_release()
        task_create_dataset = create_dataset(xcom_release, dag_params)
        task_transfer_orcid = transfer_orcid(xcom_release, dag_params)
        task_bq_create_main_table_snapshot = bq_create_main_table_snapshot(xcom_release, dag_params)
        task_create_manifests = create_manifests(xcom_release, dag_params)
        task_download = download(xcom_release)
        task_transform = transform(xcom_release, dag_params)
        task_upload_transformed = upload_transformed(xcom_release)
        task_bq_load_main_table = bq_load_main_table(xcom_release, dag_params)
        task_bq_load_upsert_table = bq_load_upsert_table(xcom_release, dag_params)
        task_bq_load_delete_table = bq_load_delete_table(xcom_release, dag_params)
        task_bq_upsert_records = bq_upsert_records(xcom_release)
        task_bq_delete_records = bq_delete_records(xcom_release)
        task_add_dataset_release = add_dataset_release(xcom_release)
        task_cleanup_workflow = cleanup_workflow(xcom_release)
        task_dag_run_complete = EmptyOperator(
            task_id=external_task_id,
        )
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
            >> task_create_dataset
            >> task_transfer_orcid
            >> task_bq_create_main_table_snapshot
            >> task_create_storage
            >> task_create_manifests
            >> task_download
            >> task_transform
            >> task_upload_transformed
            >> task_delete_storage
            >> task_bq_load_main_table
            >> task_bq_load_upsert_table
            >> task_bq_load_delete_table
            >> task_bq_upsert_records
            >> task_bq_delete_records
            >> task_add_dataset_release
            >> task_cleanup_workflow
            >> task_dag_run_complete
        )

    return orcid()
