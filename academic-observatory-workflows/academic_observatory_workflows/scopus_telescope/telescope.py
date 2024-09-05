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

from typing import List

import pendulum
from airflow.decorators import dag, task

from academic_observatory_workflows.config import project_path
from observatory_platform.airflow.airflow import on_failure_callback
from observatory_platform.airflow.workflow import CloudWorkspace
from observatory_platform.airflow.tasks import check_dependencies


class DagParams:
    """
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

    def __init__(
        self,
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
        start_date: pendulum.DateTime = pendulum.datetime(2018, 5, 14),
        schedule: str = "@monthly",
        max_active_runs: int = 1,
        retries: int = 3,
    ):
        self.dag_id = dag_id
        self.cloud_workspace = cloud_workspace
        self.institution_ids = institution_ids
        self.scopus_conn_ids = scopus_conn_ids
        self.view = view
        self.earliest_date = earliest_date
        self.bq_dataset_id = bq_dataset_id
        self.bq_table_name = bq_table_name
        self.api_dataset_id = api_dataset_id
        self.schema_folder = schema_folder
        self.dataset_description = dataset_description
        self.table_description = table_description
        self.start_date = start_date
        self.schedule = schedule
        self.max_active_runs = max_active_runs
        self.retries = retries


def create_dag(dag_params: DagParams):
    """Scopus telescope."""

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
    def scopus():
        @task
        def fetch_release(dag_params: DagParams, **context) -> dict:
            """Fetch the release"""

            from academic_observatory_workflows.scopus_telescope import tasks

            return tasks.fetch_release(
                dag_id=dag_params.dag_id,
                cloud_workspace=dag_params.cloud_workspace,
                run_id=context["run_id"],
                data_interval_start=context["data_interval_start"],
                data_interval_end=context["data_interval_end"],
            )

        @task
        def download(release: dict, dag_params: DagParams, **context):
            """Download snapshot from SCOPUS for the given institution."""

            from academic_observatory_workflows.scopus_telescope import tasks

            tasks.download(
                release,
                earliest_date=dag_params.earliest_date,
                scopus_conn_ids=dag_params.scopus_conn_ids,
                view=dag_params.view,
                institution_ids=dag_params.institution_ids,
            )

        @task
        def transform(release: dict, dag_params: DagParams, **context):
            """Transform the data into database format."""

            from academic_observatory_workflows.scopus_telescope import tasks

            tasks.transform(release, institution_ids=dag_params.institution_ids)

        @task
        def bq_load(release: dict, dag_params: DagParams, **context):
            """Task to load each transformed release to BigQuery.
            The table_id is set to the file name without the extension."""

            from academic_observatory_workflows.scopus_telescope import tasks

            tasks.bq_load(
                release=release,
                bq_dataset_id=dag_params.bq_dataset_id,
                dataset_description=dag_params.dataset_description,
                bq_table_name=dag_params.bq_table_name,
                table_description=dag_params.table_description,
                schema_folder=dag_params.schema_folder,
            )

        @task
        def add_dataset_release(release: dict, **context) -> None:
            """Adds release information to API."""

            from academic_observatory_workflows.scopus_telescope import tasks

            tasks.add_dataset_release(release, api_bq_dataset_id=dag_params.api_dataset_id)

        @task
        def cleanup_workflow(release: dict, **context) -> None:
            """Delete all files, folders and XComs associated with this release."""

            from academic_observatory_workflows.scopus_telescope import tasks

            tasks.cleanup_workflow(release)

        # Define task connections
        task_check_dependencies = check_dependencies(airflow_conns=dag_params.scopus_conn_ids)
        xcom_release = fetch_release(dag_params)
        task_download = download(xcom_release, dag_params)
        task_transform = transform(xcom_release, dag_params)
        task_bq_load = bq_load(xcom_release, dag_params)
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
