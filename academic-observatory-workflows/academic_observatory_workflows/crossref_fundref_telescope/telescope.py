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

# Author: Aniek Roelofs, Richard Hosking, James Diprose

from __future__ import annotations

from dataclasses import dataclass

import pendulum
from airflow import DAG
from airflow.decorators import dag, task, task_group
from airflow.models import Pool

from academic_observatory_workflows.config import project_path
from observatory_platform.airflow import on_failure_callback
from observatory_platform.airflow.workflow import CloudWorkspace
from observatory_platform.airflow.tasks import check_dependencies
from observatory_platform.config import AirflowConns
from observatory_platform.google.gke import GkeParams, gke_make_kubernetes_task_params, gke_make_container_resources

RELEASES_URL = "https://gitlab.com/api/v4/projects/crossref%2Fopen_funder_registry/releases"


@dataclass
def DagParams():
    def __init__(
        self,
        dag_id: str,
        cloud_workspace: CloudWorkspace,
        bq_dataset_id: str = "crossref_fundref",
        bq_table_name: str = "crossref_fundref",
        api_dataset_id: str = "crossref_fundref",
        schema_folder: str = project_path("crossref_fundref_telescope", "schema"),
        dataset_description: str = "The Crossref Funder Registry dataset: https://www.crossref.org/services/funder-registry/",
        table_description: str = "The Crossref Funder Registry dataset: https://www.crossref.org/services/funder-registry/",
        observatory_api_conn_id: str = AirflowConns.OBSERVATORY_API,
        start_date: pendulum.DateTime = pendulum.datetime(2014, 2, 23),
        schedule: str = "@weekly",
        catchup: bool = True,
        gitlab_pool_name: str = "gitlab_pool",
        gitlab_pool_slots: int = 2,
        gitlab_pool_description: str = "A pool to limit the connections to Gitlab",
        retries: int = 3,
        test_run: bool = False,
        gke_volume_size: int = 1000,
        gke_namespace: str = "coki-astro",
        gke_volume_name: str = "crossref-fundref",
        **kwargs,
    ):
        self.dag_id = dag_id
        self.cloud_workspace = cloud_workspace
        self.bq_dataset_id = bq_dataset_id
        self.bq_table_name = bq_table_name
        self.api_dataset_id = api_dataset_id
        self.schema_folder = schema_folder
        self.dataset_description = dataset_description
        self.table_description = table_description
        self.observatory_api_conn_id = observatory_api_conn_id
        self.start_date = start_date
        self.schedule = schedule
        self.catchup = catchup
        self.gitlab_pool_name = gitlab_pool_name
        self.gitlab_pool_slots = gitlab_pool_slots
        self.gitlab_pool_description = gitlab_pool_description
        self.retries = retries
        self.test_run = (test_run,)
        self.gke_volume_size = gke_volume_size
        self.gke_namespace = gke_namespace
        self.gke_volume_name = gke_volume_name
        self.gke_params = GkeParams(
            gke_volume_size=gke_volume_size, gke_namespace=gke_namespace, gke_volume_name=gke_volume_name, **kwargs
        )


def create_dag(dag_params: DagParams) -> DAG:
    """Construct a CrossrefFundrefTelescope instance.

    :param dag_id: the id of the DAG.
    :param cloud_workspace: the cloud workspace settings.
    :param bq_dataset_id: the BigQuery dataset id.
    :param bq_table_name: the BigQuery table name.
    :param api_dataset_id: the Dataset ID to use when storing releases.
    :param schema_folder: the SQL schema path.
    :param dataset_description: description for the BigQuery dataset.
    :param table_description: description for the BigQuery table.
    :param observatory_api_conn_id: the Observatory API connection key.
    :param start_date: the start date of the DAG.
    :param schedule: the schedule interval of the DAG.
    :param catchup: whether to catchup the DAG or not.
    :param gitlab_pool_name: name of the Gitlab Pool.
    :param gitlab_pool_slots: number of slots for the Gitlab Pool.
    :param gitlab_pool_description: description for the Gitlab Pool.
    :param retries: the number of times to retry a task.
    """

    # Create Gitlab pool to limit the number of connections to Gitlab, which is very quick to block requests if
    # there are too many at once.
    Pool.create_or_update_pool(
        dag_params.gitlab_pool_name, dag_params.gitlab_pool_slots, dag_params.gitlab_pool_description, False
    )
    kubernetes_task_params = gke_make_kubernetes_task_params(dag_params.gke_params)

    @dag(
        dag_id=dag_params.dag_id,
        start_date=dag_params.start_date,
        schedule=dag_params.schedule,
        catchup=dag_params.catchup,
        tags=["academic-observatory"],
        default_args={
            "owner": "airflow",
            "on_failure_callback": on_failure_callback,
            "retries": dag_params.retries,
        },
    )
    def crossref_fundref():
        @task(pool=dag_params.gitlab_pool_name)
        def fetch_releases(**context):
            from academic_observatory_workflows.crossref_fundref_telescope.tasks import fetch_releases

            fetch_releases(
                dag_params.cloud_workspace,
                dag_id=dag_params.dag_id,
                run_id=context["run_id"],
                data_interval_start=context["data_interval_start"],
                data_interval_end=["data_interval_end"],
                bq_dataste_id=dag_params.bq_dataset_id,
                bq_table_name=dag_params.bq_table_name,
            )

        @task_group(group_id="process_release")
        def process_release(data):
            @task.kubernetes(
                name="download",
                container_resources=gke_make_container_resources(
                    {"memory": "2G", "cpu": "2"}, dag_params.gke_params.gke_resource_overrides.get("download")
                ),
                **kubernetes_task_params,
            )
            def download(release: dict, dag_params, **context):
                """Downloads release tar.gz file from url."""
                from academic_observatory_workflows.crossref_fundref_telescope.tasks import download

                download(release, dag_params.cloud_workspace)

            @task.kubernetes(
                name="transform",
                container_resources=gke_make_container_resources(
                    {"memory": "8G", "cpu": "8"}, dag_params.gke_params.gke_resource_overrides.get("download")
                ),
                **kubernetes_task_params,
            )
            def transform(release: dict, dag_params, **context):
                """Transforms release by storing file content in gzipped json format. Relationships between funders are added."""
                from academic_observatory_workflows.crossref_fundref_telescope.tasks import transform

                transform(release, cloud_workspace=dag_params.cloud_workspace)

            @task
            def bq_load(release: dict, dag_params, **context):
                """Load the data into BigQuery."""
                from academic_observatory_workflows.crossref_fundref_telescope.tasks import bq_load

                bq_load(
                    release,
                    cloud_workspace=dag_params.cloud_workspace,
                    bq_dataset_id=dag_params.bq_dataset_id,
                    dataset_description=dag_params.dataset_description,
                    bq_table_name=dag_params.bq_table_name,
                    table_description=dag_params.table_description,
                    schema_folder=dag_params.schema_folder,
                )

            @task
            def add_dataset_releases(release: dict, dag_params, **context):
                """Adds release information to API."""
                from academic_observatory_workflows.crossref_fundref_telescope.tasks import add_dataset_releases

                add_dataset_releases(
                    release,
                    dag_id=dag_params.dag_id,
                    api_dataset_id=dag_params.pi_dataset_id,
                    observatory_api_conn_id=dag_params.observatory_api_conn_id,
                )

            @task
            def cleanup_workflow(release: dict, **context):
                """Delete all files, folders and XComs associated with this release."""
                from academic_observatory_workflows.crossref_fundref_telescope.tasks import cleanup_workflow

                cleanup_workflow(release)

            download(data) >> transform(data) >> bq_load(data) >> add_dataset_releases(data) >> cleanup_workflow(data)

        check_task = check_dependencies(airflow_conns=[dag_params.observatory_api_conn_id])
        xcom_releases = fetch_releases()
        process_release_task_group = process_release.expand(data=xcom_releases)

        (check_task >> xcom_releases >> process_release_task_group)

    return crossref_fundref()
