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

# Author: Aniek Roelofs, James Diprose


from __future__ import annotations

from dataclasses import dataclass
import os

import pendulum
from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.cncf.kubernetes.secret import Secret
from kubernetes.client import models as k8s
from kubernetes.client.models import V1ResourceRequirements

from academic_observatory_workflows.config import project_path
from observatory_platform.airflow.airflow import on_failure_callback
from observatory_platform.airflow.tasks import check_dependencies
from observatory_platform.airflow.workflow import CloudWorkspace


@dataclass
class Params:
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
    :param observatory_api_conn_id: the Observatory API connection key.
    :param max_processes: the number of processes used with ProcessPoolExecutor to transform files in parallel.
    :param batch_size: the number of files to send to ProcessPoolExecutor at one time.
    :param start_date: the start date of the DAG.
    :param schedule: the schedule interval of the DAG.
    :param catchup: whether to catchup the DAG or not.
    :param queue: what Airflow queue this job runs on.
    :param max_active_runs: the maximum number of DAG runs that can be run at once.
    :param retries: the number of times to retry a task.
    """

    dag_id: str
    cloud_workspace: CloudWorkspace
    bq_dataset_id: str = "crossref_metadata"
    bq_table_name: str = "crossref_metadata"
    api_bq_dataset_id: str = "crossref_metadata"
    schema_folder: str = project_path("crossref_metadata_telescope", "schema")
    dataset_description: str = (
        "The Crossref Metadata Plus dataset: https://www.crossref.org/services/metadata-retrieval/metadata-plus/"
    )
    table_description: str = (
        "The Crossref Metadata Plus dataset: https://www.crossref.org/services/metadata-retrieval/metadata-plus/"
    )
    crossref_metadata_conn_id: str = "crossref_metadata"
    max_processes: int = os.cpu_count()
    batch_size: int = 20
    start_date: pendulum.DateTime = pendulum.datetime(2020, 6, 7)
    schedule: str = "0 0 7 * *"
    catchup: bool = True
    queue: str = "default"
    max_active_runs: int = 1
    retries: int = 3
    gke_image = "us-docker.pkg.dev/keegan-dev/academic-observatory/academic-observatory:latest"  # TODO: change this
    gke_namespace: str = "coki-astro"
    gke_startup_timeout_seconds: int = 60
    gke_volume_name: str = "crossref-metadata"
    gke_volume_path: str = "/data"
    gke_zone: str = "us-central1"
    gke_volume_size: int = 1000
    kubernetes_conn_id: str = "gke_cluster"
    docker_astro_uid: int = 50000


def create_dag(*, params: Params) -> DAG:
    """The Crossref Metadata DAG"""

    # Common @task.kubernetes params
    volume_mounts = [k8s.V1VolumeMount(mount_path=params.gke_volume_path, name=params.gke_volume_name)]
    kubernetes_task_params = dict(
        image=params.gke_image,
        # init-container is used to apply the astro:astro owner to the /data directory
        init_containers=[
            k8s.V1Container(
                name="init-container",
                image="ubuntu",
                command=[
                    "sh",
                    "-c",
                    f"useradd -u {params.docker_astro_uid} astro && chown -R astro:astro /data",
                ],
                volume_mounts=volume_mounts,
                security_context=k8s.V1PodSecurityContext(fs_group=0, run_as_group=0, run_as_user=0),
            )
        ],
        # TODO: supposed to make makes pod run as astro user so that it has access to /data volume
        # It doesn't seem to work
        security_context=k8s.V1PodSecurityContext(
            # fs_user=docker_astro_uid,
            fs_group=params.docker_astro_uid,
            fs_group_change_policy="OnRootMismatch",
            run_as_group=params.docker_astro_uid,
            run_as_user=params.docker_astro_uid,
        ),
        do_xcom_push=True,
        get_logs=True,
        in_cluster=False,
        kubernetes_conn_id=params.kubernetes_conn_id,
        log_events_on_failure=True,
        namespace=params.gke_namespace,
        startup_timeout_seconds=params.gke_startup_timeout_seconds,
        env_vars={"DATA_PATH": params.gke_volume_path},
        volumes=[
            k8s.V1Volume(
                name=params.gke_volume_name,
                persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name=params.gke_volume_name),
            )
        ],
        volume_mounts=volume_mounts,
    )

    @dag(
        dag_id=params.dag_id,
        start_date=params.start_date,
        schedule=params.schedule,
        catchup=params.catchup,
        max_active_runs=params.max_active_runs,
        tags=["academic-observatory"],
        default_args={
            "owner": "airflow",
            "on_failure_callback": on_failure_callback,
            "retries": params.retries,
            "queue": params.queue,
        },
    )
    def crossref_metadata():
        @task
        def _fetch_release(params: Params, **context):
            """Fetch the release for this month, making sure that it exists."""
            from academic_observatory_workflows.crossref_metadata_telescope.tasks import fetch_release

            fetch_release()

        @task
        def _create_volume(params: Params, **context):
            """Create the Kubernetes persistent volume"""
            from academic_observatory_workflows.crossref_metadata_telescope.tasks import create_volume

            create_volume(
                kubernetes_conn_id=params.kubernetes_conn_id,
                gke_volume_name=params.gke_volume_name,
                gke_volume_size=params.gke_volume_size,
            )

        @task.kubernetes(
            name="download",
            container_resources=V1ResourceRequirements(
                requests={"memory": "2Gi", "cpu": "2"}, limits={"memory": "2Gi", "cpu": "2"}
            ),
            secrets=[Secret("env", "CROSSREF_METADATA_API_KEY", "crossref-metadata", "API_TOKEN")],
            **kubernetes_task_params,
        )
        def _download(release: dict, **context):
            """Task to download the CrossrefMetadataRelease release for a given month."""
            from academic_observatory_workflows.crossref_metadata_telescope.tasks import download

            download(release)

        @task.kubernetes(
            name="upload_download",
            container_resources=V1ResourceRequirements(
                requests={"memory": "2Gi", "cpu": "2"}, limits={"memory": "2Gi", "cpu": "2"}
            ),
            secrets=[Secret("env", "CROSSREF_METADATA_API_KEY", "crossref-metadata", "api-key")],
            **kubernetes_task_params,
        )
        def _upload_downloaded(release: dict, params: Params, **context):
            """Upload data to Cloud Storage."""
            from academic_observatory_workflows.crossref_metadata_telescope.tasks import upload_downloaded

            upload_downloaded(release, cloud_workspace=params.cloud_workspace)

        @task.kubernetes(
            name="extract",
            container_resources=V1ResourceRequirements(
                requests={"memory": "2Gi", "cpu": "2"}, limits={"memory": "2Gi", "cpu": "2"}
            ),
            secrets=[Secret("env", "CROSSREF_METADATA_API_KEY", "crossref-metadata", "api-key")],
            **kubernetes_task_params,
        )
        def _extract(release: dict, **context):
            from academic_observatory_workflows.crossref_metadata_telescope.tasks import extract

            extract(release, **context)

        @task.kubernetes(
            name="transform",
            container_resources=V1ResourceRequirements(
                requests={"memory": "8Gi", "cpu": "8"}, limits={"memory": "8Gi", "cpu": "8"}
            ),
            **kubernetes_task_params,
        )
        def _transform(release: dict, params: Params, **context):
            """Task to transform the CrossrefMetadataRelease release for a given month.
            Each extracted file is transformmed."""
            from academic_observatory_workflows.crossref_metadata_telescope.tasks import transform

            transform(release, max_processes=params.max_processes, batch_size=params.batch_size)

        @task.kubernetes(
            name="upload_transformed",
            container_resources=V1ResourceRequirements(
                requests={"memory": "4Gi", "cpu": "4"}, limits={"memory": "4Gi", "cpu": "4"}
            ),
            **kubernetes_task_params,
        )
        def _upload_transformed(release: dict, params: Params, **context) -> None:
            """Upload the transformed data to Cloud Storage."""
            from academic_observatory_workflows.crossref_metadata_telescope.tasks import upload_transformed

            upload_transformed(release, cloud_workspace=params.cloud_workspace)

        @task
        def _bq_load(release: dict, params: Params, **context):
            """Task to load each transformed release to BigQuery.
            The table_id is set to the file name without the extension."""
            from academic_observatory_workflows.crossref_metadata_telescope.tasks import bq_load

            bq_load(
                release,
                cloud_workspace=params.cloud_workspace,
                bq_dataset_id=params.bq_dataset_id,
                bq_table_name=params.bq_table_name,
                dataset_description=params.dataset_description,
                table_description=params.table_description,
                schema_folder=params.schema_folder,
            )

        @task
        def _delete_volume(params: Params, **context) -> None:
            """Delete the persistent Kubernetes volume"""
            from academic_observatory_workflows.crossref_metadata_telescope.tasks import delete_volume

            delete_volume(kubernetes_conn_id=params.kubernetes_conn_id, gke_volume_name=params.gke_volume_name)

        @task
        def _add_dataset_release(release: dict, params: Params, **context) -> None:
            """Adds release information to API."""
            from academic_observatory_workflows.crossref_metadata_telescope.tasks import add_dataset_release

            add_dataset_release(
                release,
                dag_id=params.dag_id,
                cloud_workspace=params.cloud_workspace,
                api_bq_dataset_id=params.api_bq_dataset_id,
            )

        @task
        def _cleanup_workflow(release: dict, params: Params, **context) -> None:
            """Delete all files, folders and XComs associated with this release."""
            from academic_observatory_workflows.crossref_metadata_telescope.tasks import cleanup_workflow

            cleanup_workflow(release, dag_id=params.dag_id, logical_date=context["logical_date"])

        # Define task connections
        task_check_dependencies = check_dependencies(airflow_conns=[params.crossref_metadata_conn_id])
        xcom_release = _fetch_release(params)
        task_create_volume = _create_volume(params)
        task_download = _download(xcom_release)
        task_upload_downloaded = _upload_downloaded(xcom_release, params)
        task_extract = _extract(xcom_release, params)
        task_transform = _transform(xcom_release, params)
        task_upload_transformed = _upload_transformed(xcom_release, params)
        task_delete_volume = _delete_volume(params)
        task_bq_load = _bq_load(xcom_release, params)
        task_add_dataset_release = _add_dataset_release(xcom_release, params)
        task_cleanup_workflow = _cleanup_workflow(xcom_release, params)

        (
            task_check_dependencies
            >> xcom_release
            >> task_create_volume
            >> task_download
            >> task_upload_downloaded
            >> task_extract
            >> task_transform
            >> task_upload_transformed
            >> task_bq_load
            >> task_delete_volume
            >> task_add_dataset_release
            >> task_cleanup_workflow
        )

    return crossref_metadata()
