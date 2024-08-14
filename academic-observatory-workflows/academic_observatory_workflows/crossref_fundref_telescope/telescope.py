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
from observatory_platform.airflow.airflow import on_failure_callback
from observatory_platform.airflow.workflow import CloudWorkspace


@dataclass
class DagParams:
    """Parameters for the Crossref Fundref Telescope

    :param dag_id: the id of the DAG.
    :param cloud_workspace: the cloud workspace settings.
    :param bq_dataset_id: the BigQuery dataset id.
    :param bq_table_name: the BigQuery table name.
    :param api_bq_dataset_id: the Dataset ID to use when storing releases.
    :param schema_folder: the SQL schema path.
    :param dataset_description: description for the BigQuery dataset.
    :param table_description: description for the BigQuery table.
    :param start_date: the start date of the DAG.
    :param schedule: the schedule interval of the DAG.
    :param catchup: whether to catchup the DAG or not.
    :param gitlab_pool_name: name of the Gitlab Pool.
    :param gitlab_pool_slots: number of slots for the Gitlab Pool.
    :param gitlab_pool_description: description for the Gitlab Pool.
    :param retries: the number of times to retry a task.
    """

    def __init__(
        self,
        dag_id: str,
        cloud_workspace: CloudWorkspace,
        bq_dataset_id: str = "crossref_fundref",
        bq_table_name: str = "crossref_fundref",
        api_bq_dataset_id: str = "crossref_fundref",
        schema_folder: str = project_path("crossref_fundref_telescope", "schema"),
        dataset_description: str = "The Crossref Funder Registry dataset: https://www.crossref.org/services/funder-registry/",
        table_description: str = "The Crossref Funder Registry dataset: https://www.crossref.org/services/funder-registry/",
        start_date: pendulum.DateTime = pendulum.datetime(2014, 2, 23),
        schedule: str = "@weekly",
        catchup: bool = True,
        gitlab_pool_name: str = "gitlab_pool",
        gitlab_pool_slots: int = 2,
        gitlab_pool_description: str = "A pool to limit the connections to Gitlab",
        retries: int = 3,
    ):
        self.dag_id = dag_id
        self.cloud_workspace = cloud_workspace
        self.bq_dataset_id = bq_dataset_id
        self.bq_table_name = bq_table_name
        self.api_bq_dataset_id = api_bq_dataset_id
        self.schema_folder = schema_folder
        self.dataset_description = dataset_description
        self.table_description = table_description
        self.start_date = start_date
        self.schedule = schedule
        self.catchup = catchup
        self.gitlab_pool_name = gitlab_pool_name
        self.gitlab_pool_slots = gitlab_pool_slots
        self.gitlab_pool_description = gitlab_pool_description
        self.retries = retries


def create_dag(dag_params: DagParams) -> DAG:
    """Construct a CrossrefFundrefTelescope instance."""

    # Create Gitlab pool to limit the number of connections to Gitlab, which is very quick to block requests if
    # there are too many at once.
    Pool.create_or_update_pool(
        name=dag_params.gitlab_pool_name,
        slots=dag_params.gitlab_pool_slots,
        description=dag_params.gitlab_pool_description,
        include_deferred=False,
    )

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
            from academic_observatory_workflows.crossref_fundref_telescope import tasks

            return tasks.fetch_releases(
                dag_params.cloud_workspace,
                dag_id=dag_params.dag_id,
                run_id=context["run_id"],
                data_interval_start=context["data_interval_start"],
                data_interval_end=context["data_interval_end"],
                bq_dataset_id=dag_params.bq_dataset_id,
                bq_table_name=dag_params.bq_table_name,
            )

        @task_group(group_id="process_release")
        def process_release(data):
            @task
            def download(release: dict, **context):
                from academic_observatory_workflows.crossref_fundref_telescope import tasks

                tasks.download(release)

            @task
            def upload_downloaded(release: dict, **context):
                from academic_observatory_workflows.crossref_fundref_telescope import tasks

                tasks.upload_downloaded(release)

            @task
            def extract(release: dict, **context):
                from academic_observatory_workflows.crossref_fundref_telescope import tasks

                tasks.extract(release)

            @task
            def transform(release: dict, **context):
                from academic_observatory_workflows.crossref_fundref_telescope import tasks

                tasks.transform(release)

            @task
            def upload_transformed(release: dict, **context):
                from academic_observatory_workflows.crossref_fundref_telescope import tasks

                tasks.upload_transformed(release)

            @task
            def bq_load(release: dict, dag_params, **context):
                from academic_observatory_workflows.crossref_fundref_telescope import tasks

                tasks.bq_load(
                    release,
                    bq_dataset_id=dag_params.bq_dataset_id,
                    dataset_description=dag_params.dataset_description,
                    bq_table_name=dag_params.bq_table_name,
                    table_description=dag_params.table_description,
                    schema_folder=dag_params.schema_folder,
                )

            @task
            def add_dataset_releases(release: dict, dag_params, **context):
                """Adds release information to API."""
                from academic_observatory_workflows.crossref_fundref_telescope import tasks

                tasks.add_dataset_releases(release, api_bq_dataset_id=dag_params.api_bq_dataset_id)

            @task
            def cleanup_workflow(release: dict, **context):
                from academic_observatory_workflows.crossref_fundref_telescope import tasks

                tasks.cleanup_workflow(release)

            (
                download(data)
                >> upload_downloaded(data)
                >> extract(data)
                >> transform(data)
                >> upload_transformed(data)
                >> bq_load(data, dag_params)
                >> add_dataset_releases(data, dag_params)
                >> cleanup_workflow(data)
            )

        xcom_releases = fetch_releases()
        process_release_task_group = process_release.expand(data=xcom_releases)

        (xcom_releases >> process_release_task_group)

    return crossref_fundref()
