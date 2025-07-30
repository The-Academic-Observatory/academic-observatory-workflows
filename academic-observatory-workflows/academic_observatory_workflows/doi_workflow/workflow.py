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

# Author: Richard Hosking, James Diprose

from __future__ import annotations

from typing import List, Optional

import pendulum
from airflow import DAG
from airflow.decorators import dag, task, task_group
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator

import academic_observatory_workflows.doi_workflow.tasks as tasks
from academic_observatory_workflows.doi_workflow.queries import Aggregation, make_sql_queries, SQLQuery
from academic_observatory_workflows.doi_workflow.release import DOIRelease
from observatory_platform.airflow.airflow import on_failure_callback
from observatory_platform.airflow.release import make_snapshot_date
from observatory_platform.airflow.tasks import check_dependencies
from observatory_platform.airflow.workflow import CloudWorkspace

SENSOR_DAG_IDS = [
    "crossref_fundref",
    "crossref_metadata",
    "openalex",
    "ror",
    "unpaywall",
    "pubmed",
    "orcid",
]

AGGREGATIONS = [
    Aggregation(
        "country",
        "countries",
        relate_to_journals=True,
        relate_to_funders=True,
        relate_to_publishers=True,
    ),
    Aggregation(
        "funder",
        "funders",
        relate_to_institutions=True,
        relate_to_countries=True,
        relate_to_groups=True,
        relate_to_funders=True,
        relate_to_publishers=True,
    ),
    Aggregation(
        "group",
        "groupings",
        relate_to_institutions=True,
        relate_to_journals=True,
        relate_to_funders=True,
        relate_to_publishers=True,
    ),
    Aggregation(
        "institution",
        "institutions",
        relate_to_institutions=True,
        relate_to_countries=True,
        relate_to_journals=True,
        relate_to_funders=True,
        relate_to_publishers=True,
    ),
    Aggregation(
        "author",
        "authors",
        relate_to_institutions=True,
        relate_to_countries=True,
        relate_to_groups=True,
        relate_to_journals=True,
        relate_to_funders=True,
        relate_to_publishers=True,
    ),
    Aggregation(
        "journal",
        "journals",
        relate_to_institutions=True,
        relate_to_countries=True,
        relate_to_groups=True,
        relate_to_journals=True,
        relate_to_funders=True,
    ),
    Aggregation(
        "publisher",
        "publishers",
        relate_to_institutions=True,
        relate_to_countries=True,
        relate_to_groups=False,
        relate_to_funders=False,
    ),
    # Aggregation(
    #     "region",
    #     "regions",
    #     relate_to_funders=True,
    #     relate_to_publishers=True,
    # ),
    Aggregation(
        "subregion",
        "subregions",
        relate_to_funders=True,
        relate_to_publishers=True,
    ),
]


class DagParams:
    def __init__(
        self,
        dag_id: str,
        cloud_workspace: CloudWorkspace,
        bq_intermediate_dataset_id: str = "observatory_intermediate",
        bq_dashboards_dataset_id: str = "coki_dashboards",
        bq_observatory_dataset_id: str = "observatory",
        bq_unpaywall_dataset_id: str = "unpaywall",
        bq_ror_dataset_id: str = "ror",
        api_bq_dataset_id: str = "dataset_api",
        sql_queries: List[List[SQLQuery]] = None,
        max_fetch_threads: int = 4,
        start_date: Optional[pendulum.DateTime] = pendulum.datetime(2020, 8, 30),
        schedule: Optional[str] = "@weekly",
        sensor_dag_ids: List[str] = None,
        max_active_runs: int = 1,
        retries: int = 3,
        **kwargs,
    ):
        """Create the DoiWorkflow.

        :param dag_id: the DAG ID.
        :param cloud_workspace: the cloud workspace settings.
        :param bq_intermediate_dataset_id: the BigQuery intermediate dataset id.
        :param bq_dashboards_dataset_id: the BigQuery dashboards dataset id.
        :param bq_observatory_dataset_id: the BigQuery observatory dataset id.
        :param bq_unpaywall_dataset_id: the BigQuery Unpaywall dataset id.
        :param bq_ror_dataset_id: the BigQuery ROR dataset id.
        :param api_bq_dataset_id: the Dataset ID to use when storing releases.
        :param sql_queries: a list of batches of SQLQuery objects.
        :param max_fetch_threads: maximum number of threads to use when fetching.
        :param observatory_api_conn_id: the Observatory API connection key.
        :param start_date: the start date.
        :param schedule: the schedule interval.
        :param sensor_dag_ids: a list of DAG IDs to wait for with sensors.
        :param max_active_runs: the maximum number of DAG runs that can be run at once.
        :param retries: the number of times to retry a task.
        """

        self.dag_id = dag_id
        self.cloud_workspace = cloud_workspace
        self.bq_intermediate_dataset_id = bq_intermediate_dataset_id
        self.bq_dashboards_dataset_id = bq_dashboards_dataset_id
        self.bq_observatory_dataset_id = bq_observatory_dataset_id
        self.bq_unpaywall_dataset_id = bq_unpaywall_dataset_id
        self.bq_ror_dataset_id = bq_ror_dataset_id
        self.api_bq_dataset_id = api_bq_dataset_id

        self.sql_queries = sql_queries
        if sql_queries is None:
            self.sql_queries = make_sql_queries(
                self.cloud_workspace.input_project_id,
                self.cloud_workspace.output_project_id,
                dataset_id_observatory=bq_observatory_dataset_id,
            )

        self.max_fetch_threads = max_fetch_threads
        self.start_date = start_date
        self.schedule = schedule
        self.sensor_dag_ids = sensor_dag_ids if sensor_dag_ids is not None else SENSOR_DAG_IDS
        self.max_active_runs = max_active_runs
        self.retries = retries

        input_table_task_ids = []
        for batch in self.sql_queries:
            for sql_query in batch:
                task_id = sql_query.name
                input_table_task_ids.append(task_id)
        self.input_table_task_ids = input_table_task_ids


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
    def doi():
        @task_group(group_id="sensors")
        def make_sensors():
            """Create the sensor tasks for the DAG."""

            tasks.make_sensors(sensor_dag_ids=dag_params.sensor_dag_ids)

        @task
        def fetch_release(**context) -> dict:
            """Fetch a release instance."""

            snapshot_date = make_snapshot_date(**context)
            return DOIRelease(
                dag_id=dag_params.dag_id,
                run_id=context["run_id"],
                snapshot_date=snapshot_date,
                data_interval_start=context["data_interval_start"],
                data_interval_end=context["data_interval_end"],
            ).to_dict()

        @task
        def create_datasets(release: dict, **context):
            """Create required BigQuery datasets."""

            tasks.create_datasets(
                output_project_id=dag_params.cloud_workspace.output_project_id,
                bq_data_location=dag_params.cloud_workspace.data_location,
                bq_intermediate_dataset_id=dag_params.bq_intermediate_dataset_id,
                bq_dashboards_dataset_id=dag_params.bq_dashboards_dataset_id,
                bq_observatory_dataset_id=dag_params.bq_observatory_dataset_id,
            )

        @task
        def create_repo_institution_to_ror_table(release: dict, **context):
            """Create the repository_institution_to_ror_table."""

            release = DOIRelease.from_dict(release)
            tasks.create_repo_institution_to_ror_table(
                release=release,
                input_project_id=dag_params.cloud_workspace.input_project_id,
                output_project_id=dag_params.cloud_workspace.output_project_id,
                bq_unpaywall_dataset_id=dag_params.bq_unpaywall_dataset_id,
                bq_intermediate_dataset_id=dag_params.bq_intermediate_dataset_id,
                max_fetch_threads=dag_params.max_fetch_threads,
            )

        @task
        def create_ror_hierarchy_table(release: dict, **context):
            """Create the ROR hierarchy table."""

            release = DOIRelease.from_dict(release)
            tasks.create_ror_hierarchy_table(
                release=release,
                input_project_id=dag_params.cloud_workspace.input_project_id,
                output_project_id=dag_params.cloud_workspace.output_project_id,
                bq_ror_dataset_id=dag_params.bq_ror_dataset_id,
                bq_intermediate_dataset_id=dag_params.bq_intermediate_dataset_id,
            )

        @task_group(group_id="intermediate_tables")
        def create_intermediate_tables(release: dict, **context):
            """Create intermediate table tasks."""

            ts = []
            for i, batch in enumerate(dag_params.sql_queries):
                parallel = []
                for sql_query in batch:
                    task_id = sql_query.name
                    t = create_intermediate_table.override(task_id=task_id)(
                        release,
                        sql_query=sql_query,
                    )
                    parallel.append(t)

                merge = EmptyOperator(
                    task_id=f"merge_{i}",
                )
                ts.append(parallel)
                ts.append(merge)

            chain(*ts)

        @task
        def create_intermediate_table(release: dict, sql_query: SQLQuery, **context):
            """Create an intermediate table."""

            release = DOIRelease.from_dict(release)
            ti = context["ti"]
            tasks.create_intermediate_table(
                release=release,
                sql_query=sql_query,
                output_project_id=dag_params.cloud_workspace.output_project_id,
                ti=ti,
            )

        @task_group(group_id="aggregate_tables")
        def create_aggregate_tables(release: dict, **context):
            """Create aggregate table tasks."""

            ts = []
            for agg in AGGREGATIONS:
                task_id = agg.table_name
                t = create_aggregate_table.override(task_id=task_id)(
                    release,
                    aggregation=agg,
                )
                ts.append(t)
            chain(ts)

        @task
        def create_aggregate_table(release: dict, aggregation: Aggregation, **context):
            """Runs the aggregate table query."""

            release = DOIRelease.from_dict(release)
            tasks.create_aggregate_table(
                release=release,
                aggregation=aggregation,
                output_project_id=dag_params.cloud_workspace.output_project_id,
                bq_observatory_dataset_id=dag_params.bq_observatory_dataset_id,
            )

        @task
        def update_table_descriptions(release: dict, **context):
            """Update descriptions for tables."""

            release = DOIRelease.from_dict(release)
            ti = context["ti"]
            tasks.update_table_descriptions(
                release=release,
                aggregations=AGGREGATIONS,
                input_table_task_ids=dag_params.input_table_task_ids,
                output_project_id=dag_params.cloud_workspace.output_project_id,
                bq_observatory_dataset_id=dag_params.bq_observatory_dataset_id,
                ti=ti,
            )

        @task
        def add_dataset_release(release: dict, **context):
            """Adds release information to API."""

            release = DOIRelease.from_dict(release)
            tasks.add_dataset_release(
                release=release,
                api_bq_project_id=dag_params.cloud_workspace.project_id,
                api_bq_dataset_id=dag_params.api_bq_dataset_id,
            )

        sensor_task_group = make_sensors()
        task_check_dependencies = check_dependencies()
        xcom_release = fetch_release()
        create_datasets_task = create_datasets(xcom_release)
        create_repo_institution_to_ror_table_task = create_repo_institution_to_ror_table(xcom_release)
        create_ror_hierarchy_table_task = create_ror_hierarchy_table(xcom_release)
        create_intermediate_tables_task_group = create_intermediate_tables(xcom_release)
        create_aggregate_tables_task_group = create_aggregate_tables(xcom_release)
        update_table_descriptions_task = update_table_descriptions(xcom_release)
        add_dataset_release_task = add_dataset_release(xcom_release)

        (
            sensor_task_group
            >> task_check_dependencies
            >> xcom_release
            >> create_datasets_task
            >> create_repo_institution_to_ror_table_task
            >> create_ror_hierarchy_table_task
            >> create_intermediate_tables_task_group
            >> create_aggregate_tables_task_group
            >> update_table_descriptions_task
            >> add_dataset_release_task
        )

    return doi()
