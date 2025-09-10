# Copyright 2022-2025 Curtin University
# Copyright 2024-2025 UC Curation Center (California Digital Library)
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
from datetime import datetime
from dateutil import relativedelta
import logging
from typing import List, Optional

import pendulum
from airflow import DAG
from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from kubernetes.client import models as k8s

from academic_observatory_workflows.config import project_path
from academic_observatory_workflows.openalex_telescope.release import OpenAlexEntity
from observatory_platform.airflow.airflow import is_first_dag_run, on_failure_callback
from observatory_platform.airflow.release import release_to_bucket
from observatory_platform.airflow.sensors import PreviousDagRunSensor
from observatory_platform.airflow.tasks import check_dependencies, gke_create_storage, gke_delete_storage
from observatory_platform.airflow.workflow import cleanup, CloudWorkspace, make_workflow_folder
from observatory_platform.config import AirflowConns
from observatory_platform.google.bigquery import bq_create_dataset
from observatory_platform.google.gke import DEFAULT_GKE_IMAGE, GkeParams


def previous_month_fn(execution_date: datetime.datetime) -> datetime.datetime:
    return execution_date - relativedelta(months=1)


class DagParams:
    def __init__(
        self,
        *,
        dag_id: str,
        cloud_workspace: CloudWorkspace,
        bq_dataset_id: str = "openalex",
        api_bq_dataset_id: str = "dataset_api",
        entity_names: List[str] = None,
        schema_folder: str = project_path("openalex_telescope", "schema"),
        dataset_description: str = "The OpenAlex dataset: https://docs.openalex.org/",
        non_concurrent_table_expiry_days: int = 62,
        n_transfer_trys: int = 3,
        primary_key: str = "id",
        aws_conn_id: str = "aws_openalex",
        aws_openalex_bucket: str = "openalex",
        slack_conn_id: Optional[str] = AirflowConns.SLACK,
        start_date: pendulum.DateTime = pendulum.datetime(2021, 12, 1),
        schedule: str = "@weekly",
        max_active_runs: int = 1,
        retries: int = 3,
        gke_image: str = DEFAULT_GKE_IMAGE,
        gke_namespace: str = "coki-astro",
        gke_volume_path: str = "/data",
        gke_resource_map: dict = None,
        gke_volume_map: dict = None,
        gke_conn_id: str = "gke_cluster",
        **kwargs,
    ):
        """Construct an OpenAlexTelescope instance.

        :param dag_id: the id of the DAG.
        :param cloud_workspace: the cloud workspace settings.
        :param bq_dataset_id: the BigQuery dataset id.
        :param api_bq_dataset_id: the name of the Bigquery dataset to store the API release(s).
        :param entity_names: the names of the OpenAlex entities to process.
        :param schema_folder: the SQL schema path.
        :param dataset_description: description for the BigQuery dataset.
        :param non_concurrent_table_expiry_days: the number of days after creation that non-concurrent tables will expire.
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
        :param max_active_runs: the maximum number of DAG runs that can be run at once.
        :param retries: the number of times to retry a task.
        :param gke_image: the image location to pull from.
        :param gke_namespace: the cluster namespace to use.
        :param gke_volume_path: where to mount the persistent volume.
        :param gke_resource_map: a dictionary defining resource allocations for different workload sizes. Each size category ("small", "medium", "large")
        contains workload types (e.g., "download", "transform", "upload_schema", "upload_files") with their corresponding
        resource requirements and node scheduling constraints.

            The gke_resource_map dictionary structure:
            - **Key (str):** Workload size category ("small", "medium", "large").
            - **Value (dict):** Mapping of workload types to resource configurations.

            Example structure:

            .. code-block:: python
                {
                    "small": {
                        "download": {
                            "container_resources": V1ResourceRequirements(requests={"memory": "2G", "cpu": "2"},
                                                                           limits={"memory": "2G", "cpu": "2"}),
                            "node_selector": {"cloud.google.com/compute-class": "Balanced"}
                        },
                        "transform": {
                            "container_resources": V1ResourceRequirements(requests={"memory": "4G", "cpu": "4"},
                                                                           limits={"memory": "4G", "cpu": "4"}),
                            "node_selector": {"cloud.google.com/compute-class": "Balanced"}
                        }
                    }
                }

            **Entities and Their Resource Categories:**
            - `"authors"` → medium_resources
            - `"concepts"` → small_resources
            - `"funders"` → small_resources
            - `"institutions"` → small_resources
            - `"publishers"` → small_resources
            - `"sources"` → small_resources
            - `"works"` → large_resources
            - `"domains"` → small_resources
            - `"fields"` → small_resources
            - `"subfields"` → small_resources
            - `"topics"` → small_resources

        :param gke_volume_map: a dict containing the entity name (OpenAlex table name) as a key and a dictionary
        containing the size and storage class for the GKE PVC.

            Example structure:

                .. code-block:: python

                    gke_volume_map = {
                        "authors": {"size": "250Gi", "storage_class": "standard"},
                        "concepts": {"size": "250Gi", "storage_class": "premium-rwo"},
                        ...
                    }

        :param gke_conn_id: the name of the airlfow connection storing the gke cluster information.
        """

        self.dag_id = dag_id
        self.cloud_workspace = cloud_workspace
        self.bq_dataset_id = bq_dataset_id
        self.api_bq_dataset_id = api_bq_dataset_id

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
        self.entity_names = entity_names
        self.schema_folder = schema_folder
        self.dataset_description = dataset_description
        self.non_concurrent_table_expiry_days = non_concurrent_table_expiry_days
        self.n_transfer_trys = n_transfer_trys
        self.primary_key = primary_key
        self.aws_conn_id = aws_conn_id
        self.aws_openalex_bucket = aws_openalex_bucket
        self.slack_conn_id = slack_conn_id
        self.start_date = start_date
        self.schedule = schedule
        self.max_active_runs = max_active_runs
        self.retries = retries
        self.gke_conn_id = gke_conn_id

        # Construct GKE parameters
        # TODO: assert that resource map correct schema
        if gke_resource_map is None:
            # Unless you use the Performance compute class, CPU:memory ratios must be at least 1:1
            # However, Performance requires spot VMs: https://www.googlecloudcommunity.com/gc/Google-Kubernetes-Engine-GKE/Use-Performance-Compute-Class/m-p/755214
            resource_cpu2 = k8s.V1ResourceRequirements(
                requests={"memory": "2G", "cpu": "2"}, limits={"memory": "2G", "cpu": "2"}
            )
            resource_cpu4 = k8s.V1ResourceRequirements(
                requests={"memory": "4G", "cpu": "4"}, limits={"memory": "4G", "cpu": "4"}
            )
            resource_cpu8 = k8s.V1ResourceRequirements(
                requests={"memory": "8G", "cpu": "8"}, limits={"memory": "8G", "cpu": "8"}
            )
            resource_cpu16 = k8s.V1ResourceRequirements(
                requests={"memory": "16G", "cpu": "16"}, limits={"memory": "16G", "cpu": "16"}
            )
            resource_cpu32 = k8s.V1ResourceRequirements(
                requests={"memory": "32G", "cpu": "32"}, limits={"memory": "32G", "cpu": "32"}
            )
            balanced_node_selector = {
                "cloud.google.com/compute-class": "Balanced",
                "supported-cpu-platform.cloud.google.com/AMD_Rome": "true",
            }
            gke_resource_map = {
                "small": {
                    "download": {"container_resources": resource_cpu2, "node_selector": balanced_node_selector},
                    "transform": {"container_resources": resource_cpu4, "node_selector": balanced_node_selector},
                    "upload_schema": {
                        "container_resources": resource_cpu2,
                        "node_selector": balanced_node_selector,
                    },
                    "upload_files": {
                        "container_resources": resource_cpu2,
                        "node_selector": balanced_node_selector,
                    },
                },
                "medium": {
                    "download": {
                        "container_resources": resource_cpu8,
                        "node_selector": balanced_node_selector,
                    },
                    "transform": {
                        "container_resources": resource_cpu16,
                        "node_selector": balanced_node_selector,
                    },
                    "upload_schema": {
                        "container_resources": resource_cpu2,
                        "node_selector": balanced_node_selector,
                    },
                    "upload_files": {
                        "container_resources": resource_cpu8,
                        "node_selector": balanced_node_selector,
                    },
                },
                "large": {
                    "download": {
                        "container_resources": resource_cpu16,
                        "node_selector": balanced_node_selector,
                    },
                    "transform": {
                        "container_resources": resource_cpu32,
                        "node_selector": balanced_node_selector,
                    },
                    "upload_schema": {
                        "container_resources": resource_cpu2,
                        "node_selector": balanced_node_selector,
                    },
                    "upload_files": {
                        "container_resources": resource_cpu16,
                        "node_selector": balanced_node_selector,
                    },
                },
            }
        small_resources = gke_resource_map["small"]
        medium_resources = gke_resource_map["medium"]
        large_resources = gke_resource_map["large"]
        # TODO: assert that volume size map correct schema
        if gke_volume_map is None:
            # Space used by each OpenAlex entity: https://docs.google.com/spreadsheets/d/13RcrHkAUbGE0XWTv-12UkkcL3AkTSAagQ3Gs7vmvTaQ/edit?usp=sharing
            gke_volume_map = {
                "authors": {"size": "250Gi", "storage_class": "standard"},
                "concepts": {"size": "1Gi", "storage_class": "standard"},
                "funders": {"size": "1Gi", "storage_class": "standard"},
                "institutions": {"size": "2500Mi", "storage_class": "standard"},
                "publishers": {"size": "1Gi", "storage_class": "standard"},
                "sources": {"size": "2500Mi", "storage_class": "standard"},
                "works": {"size": "1000Gi", "storage_class": "premium-rwo"},
                "domains": {"size": "1Gi", "storage_class": "standard"},
                "fields": {"size": "1Gi", "storage_class": "standard"},
                "subfields": {"size": "1Gi", "storage_class": "standard"},
                "topics": {"size": "1Gi", "storage_class": "standard"},
            }
        gke_resource_overrides = {
            "authors": medium_resources,
            "concepts": small_resources,
            "funders": small_resources,
            "institutions": small_resources,
            "publishers": small_resources,
            "sources": small_resources,
            "works": large_resources,
            "domains": small_resources,
            "fields": small_resources,
            "subfields": small_resources,
            "topics": small_resources,
        }
        self.gke_params_map = {
            key: GkeParams(
                gke_image=gke_image,
                gke_namespace=gke_namespace,
                gke_volume_size=gke_volume_map[key].get("size", "1Gi"),
                gke_volume_name=f"openalex-{key}",
                gke_volume_storage_class=gke_volume_map[key].get("storage_class", "standard"),
                gke_resource_overrides=gke_resource_overrides[key],
                gke_volume_path=gke_volume_path,
                gke_conn_id=gke_conn_id,
            )
            for key in gke_volume_map
        }


def create_dag(dag_params: DagParams) -> DAG:
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
    def openalex():
        @task(multiple_outputs=True)
        def fetch_entities(dag_params: DagParams, **context) -> dict:
            """Fetch OpenAlex releases.

            :return: True to continue, False to skip.
            """

            import academic_observatory_workflows.openalex_telescope.tasks as tasks

            dag_run = context["dag_run"]
            is_first_run = is_first_dag_run(dag_run)
            entity_index = tasks.fetch_entities(
                dag_id=dag_params.dag_id,
                run_id=context["run_id"],
                is_first_run=is_first_run,
                entity_names=dag_params.entity_names,
                cloud_workspace=dag_params.cloud_workspace,
                schema_folder=dag_params.schema_folder,
                bq_dataset_id=dag_params.bq_dataset_id,
                api_bq_dataset_id=dag_params.api_bq_dataset_id,
                aws_conn_id=dag_params.aws_conn_id,
                aws_openalex_bucket=dag_params.aws_openalex_bucket,
            )
            entity_index_id = release_to_bucket(entity_index, dag_params.cloud_workspace.transform_bucket)

            return {"entity_index_id": entity_index_id, "entity_index": entity_index}

        @task.short_circuit
        def short_circuit(entity_index: dict, **context):
            return len(entity_index) > 0

        @task
        def create_dataset(dag_params: DagParams, *context) -> None:
            """Create datasets."""

            bq_create_dataset(
                project_id=dag_params.cloud_workspace.output_project_id,
                dataset_id=dag_params.bq_dataset_id,
                location=dag_params.cloud_workspace.data_location,
                description=dag_params.dataset_description,
            )

        @task_group
        def process_entity(
            entity_index_id: str,
            entity_index: dict,
            entity_name: str,
            dag_params: DagParams,
            gke_params: GkeParams,
        ):
            @task()
            def aws_to_gcs_transfer(entity_index: dict, entity_name: str, dag_params: DagParams, **context):
                """Transfer files from AWS bucket to Google Cloud bucket"""

                import academic_observatory_workflows.openalex_telescope.tasks as tasks

                entity = tasks.get_entity(entity_index, entity_name)
                tasks.aws_to_gcs_transfer(
                    entity=entity,
                    gc_project_id=dag_params.cloud_workspace.input_project_id,
                    aws_conn_id=dag_params.aws_conn_id,
                    n_transfer_trys=dag_params.n_transfer_trys,
                    aws_openalex_bucket=dag_params.aws_openalex_bucket,
                )

            @task.kubernetes(
                name=f"{dag_params.dag_id}-download",
                **{
                    **gke_params.kubernetes_task_params,
                    **gke_params.gke_resource_overrides.get("download"),
                },
            )
            def download(entity_index_id: str, entity_name: str, dag_params, **context):
                """Download files for an entity from the bucket.

                Gsutil is used instead of the standard Google Cloud Python library, because it is faster at downloading files
                than the Google Cloud Python library.
                """

                import academic_observatory_workflows.openalex_telescope.tasks as tasks
                from observatory_platform.airflow.release import release_from_bucket

                entity_index = release_from_bucket(dag_params.cloud_workspace.transform_bucket, entity_index_id)
                entity = tasks.get_entity(entity_index, entity_name)

                tasks.download(entity=entity, **context)

            @task.kubernetes(
                name=f"{dag_params.dag_id}-transform",
                # Don't retry transform step as these tasks take a long time
                **{
                    **gke_params.kubernetes_task_params,
                    **gke_params.gke_resource_overrides.get("transform"),
                    "retries": 0,
                },
            )
            def transform(entity_index_id: str, entity_name: str, dag_params, **context):
                """Transform all files for the Work, Concept and Institution entities. Transforms one file per process.

                This step also scans through each file and generates a Biguqery style schema from the incoming data."""

                import academic_observatory_workflows.openalex_telescope.tasks as tasks
                from observatory_platform.airflow.release import release_from_bucket

                entity_index = release_from_bucket(dag_params.cloud_workspace.transform_bucket, entity_index_id)
                entity = tasks.get_entity(entity_index, entity_name)

                tasks.transform(entity=entity)

            @task.kubernetes(
                name=f"{dag_params.dag_id}-upload-schema",
                **{
                    **gke_params.kubernetes_task_params,
                    **gke_params.gke_resource_overrides.get("upload_schema"),
                },
            )
            def upload_schema(entity_index_id: str, entity_name: str, dag_params, **context):
                """Upload the generated schema from the transform step to GCS."""

                import academic_observatory_workflows.openalex_telescope.tasks as tasks
                from observatory_platform.airflow.release import release_from_bucket

                entity_index = release_from_bucket(dag_params.cloud_workspace.transform_bucket, entity_index_id)
                entity = tasks.get_entity(entity_index, entity_name)

                tasks.upload_schema(entity=entity, transform_bucket=dag_params.cloud_workspace.transform_bucket)

            @task
            def compare_schemas(entity_index: dict, entity_name: str, dag_params: DagParams, **context):
                """Compare the generated schema against the expected schema for each entity."""

                import academic_observatory_workflows.openalex_telescope.tasks as tasks

                entity = tasks.get_entity(entity_index, entity_name)
                tasks.compare_schemas(
                    entity=entity,
                    transform_bucket=dag_params.cloud_workspace.transform_bucket,
                    slack_conn_id=dag_params.slack_conn_id,
                    **context,
                )

            @task.kubernetes(
                name=f"{dag_params.dag_id}-upload-files",
                **{
                    **gke_params.kubernetes_task_params,
                    **gke_params.gke_resource_overrides.get("upload_files"),
                },
            )
            def upload_files(entity_index_id: str, entity_name: str, dag_params, **context):
                """Upload the transformed data to Cloud Storage.
                :raises AirflowException: Raised if the files to be uploaded are not found."""

                import academic_observatory_workflows.openalex_telescope.tasks as tasks
                from observatory_platform.airflow.release import release_from_bucket

                entity_index = release_from_bucket(dag_params.cloud_workspace.transform_bucket, entity_index_id)
                entity = tasks.get_entity(entity_index, entity_name)

                tasks.upload_files(entity=entity, transform_bucket=dag_params.cloud_workspace.transform_bucket)

            @task()
            def bq_load_table(entity_index: dict, entity_name: str, dag_params: DagParams, **context):
                """Load the main or upsert table for an entity."""

                import academic_observatory_workflows.openalex_telescope.tasks as tasks

                entity = tasks.get_entity(entity_index, entity_name)
                tasks.bq_load_table(entity=entity)

            @task()
            def expire_previous_version(entity_index: dict, entity_name: str, dag_params: DagParams, **context) -> None:
                """Adds release information to API."""

                import academic_observatory_workflows.openalex_telescope.tasks as tasks

                dag_run = context["dag_run"]
                is_first_run = is_first_dag_run(dag_run)

                if is_first_run:
                    logging.info(
                        f"expire_previous_version: there are no previous versions to expire as it is the first run"
                    )
                    return

                entity = tasks.get_entity(entity_index, entity_name)
                tasks.expire_previous_version(
                    dag_id=dag_params.dag_id,
                    project_id=dag_params.cloud_workspace.output_project_id,
                    dataset_id=dag_params.bq_dataset_id,
                    table_id=entity_name,
                    snapshot_date=entity.snapshot_date,
                    expiry_days=dag_params.non_concurrent_table_expiry_days,
                    api_bq_dataset_id=dag_params.api_bq_dataset_id,
                )

            task_create_storage = gke_create_storage(
                volume_name=gke_params.gke_volume_name,
                volume_size=gke_params.gke_volume_size,
                storage_class=gke_params.gke_volume_storage_class,
                kubernetes_conn_id=gke_params.gke_conn_id,
            )
            task_aws_to_gcs_transfer = aws_to_gcs_transfer(entity_index, entity_name, dag_params)
            task_download = download(entity_index_id, entity_name, dag_params)
            task_transform = transform(entity_index_id, entity_name, dag_params)
            task_upload_schema = upload_schema(entity_index_id, entity_name, dag_params)
            task_compare_schemas = compare_schemas(entity_index, entity_name, dag_params)
            task_upload_files = upload_files(entity_index_id, entity_name, dag_params)
            task_bq_load_table = bq_load_table(entity_index, entity_name, dag_params)
            task_expire_previous_version = expire_previous_version(entity_index, entity_name, dag_params)
            task_delete_storage = gke_delete_storage(
                volume_name=gke_params.gke_volume_name,
                kubernetes_conn_id=gke_params.gke_conn_id,
            )

            (
                task_create_storage
                >> task_aws_to_gcs_transfer
                >> task_download
                >> task_transform
                >> task_upload_schema
                >> task_compare_schemas
                >> task_upload_files
                >> task_bq_load_table
                >> task_expire_previous_version
                >> task_delete_storage
            )

        @task()
        def add_dataset_release(entity_index: dict, dag_params: DagParams, **context) -> None:
            """Adds release information to API."""

            import academic_observatory_workflows.openalex_telescope.tasks as tasks

            snapshot_date = OpenAlexEntity.from_dict(entity_index[next(iter(entity_index))]).snapshot_date
            tasks.add_dataset_release(
                dag_id=dag_params.dag_id,
                run_id=context["run_id"],
                snapshot_date=snapshot_date,
                bq_project_id=dag_params.cloud_workspace.output_project_id,
                api_bq_dataset_id=dag_params.api_bq_dataset_id,
            )

        @task()
        def cleanup_workflow(dag_params: DagParams, **context) -> None:
            """Delete all files, folders and XComs associated with this release."""

            workflow_folder = make_workflow_folder(dag_params.dag_id, context["run_id"])
            cleanup(dag_id=dag_params.dag_id, workflow_folder=workflow_folder)

        external_task_id = "dag_run_complete"
        sensor = PreviousDagRunSensor(
            dag_id=dag_params.dag_id, external_task_id=external_task_id, execution_date_fn=previous_month_fn
        )

        task_check_dependencies = check_dependencies(
            airflow_conns=[dag_params.gke_conn_id, dag_params.aws_conn_id, dag_params.slack_conn_id]
        )
        xcom = fetch_entities(dag_params)
        xcom_entity_index_id = xcom["entity_index_id"]
        xcom_entity_index = xcom["entity_index"]
        task_short_circuit = short_circuit(xcom_entity_index)
        task_create_dataset = create_dataset(dag_params)

        # Process each entity
        # We don't use .expand because we want each entity to be a first class citizen in the graph UI
        # Additionally, string based entity names for mapped dynamic tasks are only set once each task has been
        # run, which could be several days for certain OpenAlex tables
        process_entities = []
        for group_id in dag_params.entity_names:
            t = process_entity.override(group_id=group_id)(
                entity_index_id=xcom_entity_index_id,
                entity_index=xcom_entity_index,
                entity_name=group_id,
                dag_params=dag_params,
                gke_params=dag_params.gke_params_map[group_id],
            )
            process_entities.append(t)

        task_add_dataset_release = add_dataset_release(xcom_entity_index, dag_params)
        task_cleanup_workflow = cleanup_workflow(dag_params)
        task_dag_run_complete = EmptyOperator(task_id=external_task_id)

        (
            sensor
            >> task_check_dependencies
            >> xcom_entity_index
            >> task_short_circuit
            >> task_create_dataset
            >> process_entities
            >> task_add_dataset_release
            >> task_cleanup_workflow
            >> task_dag_run_complete
        )

    return openalex()
