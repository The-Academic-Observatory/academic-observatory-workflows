# Copyright 2021-2024 Curtin University
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

from typing import List

import pendulum
from airflow import DAG
from airflow.decorators import dag, task, task_group

from academic_observatory_workflows.config import project_path
from observatory_platform.airflow.airflow import on_failure_callback
from observatory_platform.airflow.workflow import CloudWorkspace
from observatory_platform.airflow.tasks import check_dependencies


class DagParams:
    """
    :param dag_id: the id of the DAG.
    :param cloud_workspace: the cloud workspace settings.
    :param bq_dataset_id: the BigQuery dataset id.
    :param bq_table_name: the BigQuery table name.
    :param api_dataset_id: the Dataset ID to use when storing releases.
    :param schema_folder: the SQL schema path.
    :param dataset_description: description for the BigQuery dataset.
    :param table_description: description for the BigQuery table.
    :param observatory_api_conn_id: the Observatory API connection key.
    :param ror_conceptrecid: the Zenodo conceptrecid for the ROR dataset.
    :param start_date: the start date of the DAG.
    :param schedule: the schedule interval of the DAG.
    :param catchup: whether to catchup the DAG or not.
    :param max_active_runs: the maximum number of DAG runs that can be run at once.
    :param retries: the number of times to retry a task.
    """

    def __init__(
        self,
        dag_id: str,
        cloud_workspace: CloudWorkspace,
        bq_dataset_id: str = "ror",
        bq_table_name: str = "ror",
        api_bq_dataset_id: str = "ror",
        schema_folder: str = project_path("ror_telescope", "schema"),
        dataset_description: str = "The Research Organization Registry (ROR) database: https://ror.org/",
        table_description: str = "The Research Organization Registry (ROR) database: https://ror.org/",
        ror_conceptrecid: int = 6347574,
        start_date: pendulum.DateTime = pendulum.datetime(2021, 9, 1),
        schedule: str = "@weekly",
        catchup: bool = True,
        max_active_runs: int = 1,
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
        self.ror_conceptrecid = ror_conceptrecid
        self.start_date = start_date
        self.schedule = schedule
        self.catchup = catchup
        self.max_active_runs = max_active_runs
        self.retries = retries


def create_dag(dag_params: DagParams) -> DAG:
    """Construct a RorTelescope instance."""

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
        },
    )
    def ror():
        @task
        def fetch_releases(dag_params: DagParams, **context) -> List[dict]:
            """Lists all ROR records and publishes their url, snapshot_date and checksum as an XCom."""

            from academic_observatory_workflows.ror_telescope import tasks

            return tasks.fetch_releases(
                dag_id=dag_params.dag_id,
                cloud_workspace=dag_params.cloud_workspace,
                run_id=context["run_id"],
                data_interval_start=context["data_interval_start"],
                data_interval_end=context["data_interval_end"],
                ror_conceptrecid=dag_params.ror_conceptrecid,
            )

        @task_group(group_id="process_release")
        def process_release(data):
            @task
            def download(release: dict, **context):
                """Task to download the ROR releases."""

                from academic_observatory_workflows.ror_telescope import tasks

                tasks.download(release)

            @task
            def transform(release: dict, **context):
                """Task to transform the ROR releases."""

                from academic_observatory_workflows.ror_telescope import tasks

                tasks.transform(release)

            @task
            def bq_load(release: dict, dag_params: DagParams, **context) -> None:
                """Load the data into BigQuery."""

                from academic_observatory_workflows.ror_telescope import tasks

                tasks.bq_load(
                    release,
                    bq_dataset_id=dag_params.bq_dataset_id,
                    dataset_description=dag_params.dataset_description,
                    bq_table_name=dag_params.bq_table_name,
                    table_description=dag_params.table_description,
                    schema_folder=dag_params.schema_folder,
                )

            @task
            def add_dataset_releases(release: dict, **context) -> None:
                """Adds release information to API."""

                from academic_observatory_workflows.ror_telescope import tasks

                tasks.add_dataset_releases(release, api_bq_dataset_id=dag_params.api_bq_dataset_id)

            @task
            def cleanup_workflow(release: dict, **context) -> None:
                """Delete all files, folders and XComs associated with this release."""

                from academic_observatory_workflows.ror_telescope import tasks

                tasks.cleanup_workflow(release)

            (
                download(data)
                >> transform(data)
                >> bq_load(data, dag_params)
                >> add_dataset_releases(data)
                >> cleanup_workflow(data)
            )

        check_task = check_dependencies()
        xcom_releases = fetch_releases(dag_params)
        process_release_task_group = process_release.expand(data=xcom_releases)

        (check_task >> xcom_releases >> process_release_task_group)

    return ror()
