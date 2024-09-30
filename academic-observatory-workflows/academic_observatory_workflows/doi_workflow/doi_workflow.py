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

import copy
import json
import logging
from concurrent.futures import as_completed, ThreadPoolExecutor
from dataclasses import dataclass
from typing import Dict, List, Optional, Set

import pendulum
import requests
from airflow import DAG
from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowException
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator

from academic_observatory_workflows.config import project_path
from observatory.api.client.model.dataset_release import DatasetRelease
from observatory_platform.airflow import on_failure_callback
from observatory_platform.dataset_api import make_observatory_api
from observatory_platform.google.bigquery import (
    bq_copy_table,
    bq_create_dataset,
    bq_create_table_from_query,
    bq_create_view,
    bq_load_from_memory,
    bq_run_query,
    bq_select_latest_table,
    bq_select_table_shard_dates,
    bq_sharded_table_id,
    bq_table_id,
    bq_update_table_description,
)
from observatory_platform.config import AirflowConns
from observatory_platform.airflow.workflow import CloudWorkspace
from observatory_platform.refactor.sensors import DagCompleteSensor
from observatory_platform.airflow.tasks import check_dependencies
from observatory_platform.utils.jinja2_utils import (
    render_template,
)
from observatory_platform.url_utils import retry_get_url
from observatory_platform.airflow.workflow import make_snapshot_date, set_task_state, SnapshotRelease

MAX_QUERIES = 100


@dataclass
class Table:
    project_id: str
    dataset_id: str
    table_name: str = None
    sharded: bool = False
    snapshot_date: pendulum.DateTime = None

    @property
    def table_id(self):
        """Generates the BigQuery table_id for both sharded and non-sharded tables.

        :return: BigQuery table_id.
        """

        if self.sharded:
            return f"{self.table_name}{self.snapshot_date.strftime('%Y%m%d')}"
        return self.table_name


@dataclass
class SQLQuery:
    name: str
    inputs: Dict = None
    output_table: Table = None
    output_clustering_fields: List = None


@dataclass
class Aggregation:
    table_name: str
    aggregation_field: str
    group_by_time_field: str = "published_year"
    relate_to_institutions: bool = False
    relate_to_countries: bool = False
    relate_to_groups: bool = False
    relate_to_members: bool = False
    relate_to_journals: bool = False
    relate_to_funders: bool = False
    relate_to_publishers: bool = False


SENSOR_DAG_IDS = [
    "crossref_metadata",
    "crossref_fundref",
    "geonames",
    "ror",
    "open_citations",
    "unpaywall",
    "orcid",
    "crossref_events",
    "openalex",
    "pubmed",
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


class DOIRelease(SnapshotRelease):
    def __init__(
        self,
        *,
        dag_id: str,
        run_id: str,
        snapshot_date: pendulum.DateTime,
    ):
        """Construct a DOIRelease instance.

        :param dag_id: The DAG ID.
        :param run_id: The DAG run ID.
        :param snapshot_date: Release date.
        """

        super().__init__(
            dag_id=dag_id,
            run_id=run_id,
            snapshot_date=snapshot_date,
        )

    @staticmethod
    def from_dict(dict_: dict):
        dag_id = dict_["dag_id"]
        run_id = dict_["run_id"]
        snapshot_date = pendulum.parse(dict_["snapshot_date"])
        return DOIRelease(
            dag_id=dag_id,
            run_id=run_id,
            snapshot_date=snapshot_date,
        )

    def to_dict(self) -> dict:
        return dict(
            dag_id=self.dag_id,
            run_id=self.run_id,
            snapshot_date=self.snapshot_date.to_datetime_string(),
        )


def create_dag(
    *,
    dag_id: str,
    cloud_workspace: CloudWorkspace,
    bq_intermediate_dataset_id: str = "observatory_intermediate",
    bq_dashboards_dataset_id: str = "coki_dashboards",
    bq_observatory_dataset_id: str = "observatory",
    bq_unpaywall_dataset_id: str = "unpaywall",
    bq_ror_dataset_id: str = "ror",
    api_dataset_id: str = "doi",
    sql_queries: List[List[SQLQuery]] = None,
    max_fetch_threads: int = 4,
    observatory_api_conn_id: str = AirflowConns.OBSERVATORY_API,
    start_date: Optional[pendulum.DateTime] = pendulum.datetime(2020, 8, 30),
    schedule: Optional[str] = "@weekly",
    sensor_dag_ids: List[str] = None,
    max_active_runs: int = 1,
    retries: int = 3,
) -> DAG:
    """Create the DoiWorkflow.
    :param dag_id: the DAG ID.
    :param cloud_workspace: the cloud workspace settings.
    :param bq_intermediate_dataset_id: the BigQuery intermediate dataset id.
    :param bq_dashboards_dataset_id: the BigQuery dashboards dataset id.
    :param bq_observatory_dataset_id: the BigQuery observatory dataset id.
    :param bq_unpaywall_dataset_id: the BigQuery Unpaywall dataset id.
    :param bq_ror_dataset_id: the BigQuery ROR dataset id.
    :param api_dataset_id: the DOI dataset id.
    :param sql_queries: a list of batches of SQLQuery objects.
    :param max_fetch_threads: maximum number of threads to use when fetching.
    :param observatory_api_conn_id: the Observatory API connection key.
    :param start_date: the start date.
    :param schedule: the schedule interval.
    :param sensor_dag_ids: a list of DAG IDs to wait for with sensors.
    :param max_active_runs: the maximum number of DAG runs that can be run at once.
    :param retries: the number of times to retry a task.
    """

    input_project_id = cloud_workspace.input_project_id
    output_project_id = cloud_workspace.output_project_id
    data_location = cloud_workspace.data_location

    if sensor_dag_ids is None:
        sensor_dag_ids = SENSOR_DAG_IDS

    if sql_queries is None:
        sql_queries = make_sql_queries(
            input_project_id, output_project_id, dataset_id_observatory=bq_observatory_dataset_id
        )

    input_table_task_ids = []
    for batch in sql_queries:
        for sql_query in batch:
            task_id = sql_query.name
            input_table_task_ids.append(task_id)

    @dag(
        dag_id=dag_id,
        start_date=start_date,
        schedule=schedule,
        catchup=False,
        max_active_runs=max_active_runs,
        tags=["academic-observatory"],
        default_args={
            "owner": "airflow",
            "on_failure_callback": on_failure_callback,
            "retries": retries,
        },
    )
    def doi():
        @task_group(group_id="sensors")
        def make_sensors():
            """Create the sensor tasks for the DAG."""

            tasks = []
            for ext_dag_id in sensor_dag_ids:
                sensor = DagCompleteSensor(
                    task_id=f"{ext_dag_id}_sensor",
                    external_dag_id=ext_dag_id,
                )
                tasks.append(sensor)
            chain(tasks)

        @task
        def fetch_release(**context) -> dict:
            """Fetch a release instance."""

            snapshot_date = make_snapshot_date(**context)
            return DOIRelease(
                dag_id=dag_id,
                run_id=context["run_id"],
                snapshot_date=snapshot_date,
            ).to_dict()

        @task
        def create_datasets(release: dict, **context):
            """Create required BigQuery datasets."""

            datasets = [
                (bq_intermediate_dataset_id, "Intermediate processing dataset for the Academic Observatory."),
                (bq_dashboards_dataset_id, "The latest data for display in the COKI dashboards."),
                (bq_observatory_dataset_id, "The Academic Observatory dataset."),
            ]

            for dataset_id, description in datasets:
                bq_create_dataset(
                    project_id=output_project_id,
                    dataset_id=dataset_id,
                    location=data_location,
                    description=description,
                )

        @task
        def create_repo_institution_to_ror_table(release: dict, **context):
            """Create the repository_institution_to_ror_table."""

            # Fetch unique Unpaywall repository institution names
            template_path = project_path("doi_workflow", "sql", "create_openaccess_repo_names.sql.jinja2")
            sql = render_template(template_path, project_id=input_project_id, dataset_id=bq_unpaywall_dataset_id)
            records = bq_run_query(sql)

            # Fetch affiliation strings
            results = []
            key = "repository_institution"
            repository_institutions = [dict(record)[key] for record in records]
            with ThreadPoolExecutor(max_workers=max_fetch_threads) as executor:
                futures = []
                for repo_inst in repository_institutions:
                    futures.append(executor.submit(fetch_ror_affiliations, repo_inst))
                for future in as_completed(futures):
                    data = future.result()
                    results.append(data)
            results.sort(key=lambda r: r[key])

            # Load the BigQuery table
            release = DOIRelease.from_dict(release)
            table_id = bq_sharded_table_id(
                output_project_id,
                bq_intermediate_dataset_id,
                "repository_institution_to_ror",
                release.snapshot_date,
            )
            success = bq_load_from_memory(table_id, results)
            set_task_state(success, "create_repo_institution_to_ror_table")

        @task
        def create_ror_hierarchy_table(release: dict, **context):
            """Create the ROR hierarchy table."""

            # Fetch latest ROR table
            release = DOIRelease.from_dict(release)
            ror_table_name = "ror"
            ror_table_id = bq_select_latest_table(
                table_id=bq_table_id(input_project_id, bq_ror_dataset_id, ror_table_name),
                end_date=release.snapshot_date,
                sharded=True,
            )
            ror = [dict(row) for row in bq_run_query(f"SELECT * FROM {ror_table_id}")]

            # Create ROR hierarchy table
            index = ror_to_ror_hierarchy_index(ror)

            # Convert to list of dictionaries
            records = []
            for child_id, ancestor_ids in index.items():
                records.append({"child_id": child_id, "ror_ids": [child_id] + list(ancestor_ids)})

            # Upload to intermediate table
            table_id = bq_sharded_table_id(
                output_project_id, bq_intermediate_dataset_id, "ror_hierarchy", release.snapshot_date
            )
            success = bq_load_from_memory(table_id, records)
            set_task_state(success, "create_ror_hierarchy_table")

        @task_group(group_id="intermediate_tables")
        def create_intermediate_tables(release: dict, **context):
            """Create intermediate table tasks."""

            tasks = []
            for i, batch in enumerate(sql_queries):
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
                tasks.append(parallel)
                tasks.append(merge)

            chain(*tasks)

        @task_group(group_id="aggregate_tables")
        def create_aggregate_tables(release: dict, **context):
            """Create aggregate table tasks."""

            tasks = []
            for agg in AGGREGATIONS:
                task_id = agg.table_name
                t = create_aggregate_table.override(task_id=task_id)(
                    release,
                    aggregation=agg,
                )
                tasks.append(t)
            chain(tasks)

        @task
        def create_intermediate_table(release: dict, **context):
            """Create an intermediate table."""

            release = DOIRelease.from_dict(release)
            sql_query: SQLQuery = context["sql_query"]
            task_id = sql_query.name
            print(f"create_intermediate_table: {task_id}")

            input_tables = []
            for k, table in sql_query.inputs.items():
                # Add release_date so that table_id can be computed
                if table.sharded:
                    table.snapshot_date = get_snapshot_date(
                        table.project_id, table.dataset_id, table.table_name, release.snapshot_date
                    )

                # Only add input tables when the table_name is not None as there is the odd Table instance
                # that is just used to specify the project id and dataset id in the SQL queries
                if table.table_name is not None:
                    input_tables.append(f"{table.project_id}.{table.dataset_id}.{table.table_id}")

            # Create processed table
            template_path = project_path("doi_workflow", "sql", f"{sql_query.name}.sql.jinja2")
            sql = render_template(
                template_path,
                snapshot_date=release.snapshot_date,
                **sql_query.inputs,
            )
            table_id = bq_sharded_table_id(
                output_project_id,
                sql_query.output_table.dataset_id,
                sql_query.output_table.table_name,
                release.snapshot_date,
            )
            success = bq_create_table_from_query(
                sql=sql,
                table_id=table_id,
                clustering_fields=sql_query.output_clustering_fields,
            )

            # Publish XComs with full table paths
            ti = context["ti"]
            ti.xcom_push(key="input_tables", value=json.dumps(input_tables))

            set_task_state(success, task_id)

        @task
        def create_aggregate_table(release: dict, **context):
            """Runs the aggregate table query."""

            release = DOIRelease.from_dict(release)
            agg: Aggregation = context["aggregation"]
            task_id = agg.table_name
            template_path = project_path("doi_workflow", "sql", "create_aggregate.sql.jinja2")
            sql = render_template(
                template_path,
                project_id=output_project_id,
                dataset_id=bq_observatory_dataset_id,
                snapshot_date=release.snapshot_date,
                aggregation_field=agg.aggregation_field,
                group_by_time_field=agg.group_by_time_field,
                relate_to_institutions=agg.relate_to_institutions,
                relate_to_countries=agg.relate_to_countries,
                relate_to_groups=agg.relate_to_groups,
                relate_to_members=agg.relate_to_members,
                relate_to_journals=agg.relate_to_journals,
                relate_to_funders=agg.relate_to_funders,
                relate_to_publishers=agg.relate_to_publishers,
            )

            table_id = bq_sharded_table_id(
                output_project_id, bq_observatory_dataset_id, agg.table_name, release.snapshot_date
            )
            success = bq_create_table_from_query(
                sql=sql,
                table_id=table_id,
                clustering_fields=["id"],
            )

            set_task_state(success, task_id)

        @task
        def update_table_descriptions(release: dict, **context):
            """Update descriptions for tables."""

            # Create list of input tables that were used to create our datasets
            release = DOIRelease.from_dict(release)
            ti = context["ti"]
            results = ti.xcom_pull(task_ids=input_table_task_ids, key="input_tables")
            input_tables = set()
            for task_input_tables in results:
                for input_table in json.loads(task_input_tables):
                    input_tables.add(input_table)
            input_tables = list(input_tables)
            input_tables.sort()

            # Update descriptions
            description = f"The DOI table.\n\nInput sources:\n"
            description += "".join([f"  - {input_table}\n" for input_table in input_tables])
            table_id = bq_sharded_table_id(output_project_id, bq_observatory_dataset_id, "doi", release.snapshot_date)
            bq_update_table_description(
                table_id=table_id,
                description=description,
            )

            table_names = [agg.table_name for agg in AGGREGATIONS]
            for table_name in table_names:
                description = f"The DOI table aggregated by {table_name}.\n\nInput sources:\n"
                description += "".join([f"  - {input_table}\n" for input_table in input_tables])
                table_id = bq_sharded_table_id(
                    output_project_id, bq_observatory_dataset_id, table_name, release.snapshot_date
                )
                bq_update_table_description(
                    table_id=table_id,
                    description=description,
                )

        @task
        def copy_to_dashboards(release: dict, **context):
            """Copy tables to dashboards dataset."""

            release = DOIRelease.from_dict(release)
            results = []
            table_names = [agg.table_name for agg in AGGREGATIONS]
            for table_id in table_names:
                src_table_id = bq_sharded_table_id(
                    output_project_id, bq_observatory_dataset_id, table_id, release.snapshot_date
                )
                dst_table_id = bq_table_id(output_project_id, bq_dashboards_dataset_id, table_id)
                success = bq_copy_table(src_table_id=src_table_id, dst_table_id=dst_table_id)
                if not success:
                    logging.error(f"Issue copying table: {src_table_id} to {dst_table_id}")

                results.append(success)

            success = all(results)
            set_task_state(success, "copy_to_dashboards")

        @task
        def create_dashboard_views(release: dict, **context):
            """Create views for dashboards dataset."""

            # Create processed dataset
            template_path = project_path("doi_workflow", "sql", "comparison_view.sql.jinja2")

            # Create views
            table_names = ["country", "funder", "group", "institution", "publisher", "subregion"]
            for table_name in table_names:
                view_name = f"{table_name}_comparison"
                query = render_template(
                    template_path,
                    project_id=output_project_id,
                    dataset_id=bq_dashboards_dataset_id,
                    table_id=table_name,
                )
                view_id = bq_table_id(output_project_id, bq_dashboards_dataset_id, view_name)
                bq_create_view(view_id=view_id, query=query)

        @task
        def add_dataset_release(release: dict, **context):
            """Adds release information to API."""

            release = DOIRelease.from_dict(release)
            dataset_release = DatasetRelease(
                dag_id=dag_id,
                dataset_id=api_dataset_id,
                dag_run_id=release.run_id,
                snapshot_date=release.snapshot_date,
                data_interval_start=context["data_interval_start"],
                data_interval_end=context["data_interval_end"],
            )
            api = make_observatory_api(observatory_api_conn_id=observatory_api_conn_id)
            api.post_dataset_release(dataset_release)

        sensor_task_group = make_sensors()
        task_check_dependencies = check_dependencies(airflow_conns=[observatory_api_conn_id])
        xcom_release = fetch_release()
        create_datasets_task = create_datasets(xcom_release)
        create_repo_institution_to_ror_table_task = create_repo_institution_to_ror_table(xcom_release)
        create_ror_hierarchy_table_task = create_ror_hierarchy_table(xcom_release)
        create_intermediate_tables_task_group = create_intermediate_tables(xcom_release)
        create_aggregate_tables_task_group = create_aggregate_tables(xcom_release)
        update_table_descriptions_task = update_table_descriptions(xcom_release)
        copy_to_dashboards_task = copy_to_dashboards(xcom_release)
        create_dashboard_views_task = create_dashboard_views(xcom_release)
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
            >> copy_to_dashboards_task
            >> create_dashboard_views_task
            >> add_dataset_release_task
        )

    return doi()


def make_sql_queries(
    input_project_id: str,
    output_project_id: str,
    dataset_id_crossref_events: str = "crossref_events",
    dataset_id_crossref_metadata: str = "crossref_metadata",
    dataset_id_crossref_fundref: str = "crossref_fundref",
    dataset_id_ror: str = "ror",
    dataset_id_orcid: str = "orcid",
    dataset_id_open_citations: str = "open_citations",
    dataset_id_unpaywall: str = "unpaywall",
    dataset_id_scihub: str = "scihub",
    dataset_id_openalex: str = "openalex",
    dataset_id_pubmed: str = "pubmed",
    dataset_id_settings: str = "settings",
    dataset_id_observatory: str = "observatory",
    dataset_id_observatory_intermediate: str = "observatory_intermediate",
) -> List[List[SQLQuery]]:
    return [
        [
            SQLQuery(
                "crossref_metadata",
                inputs={
                    "crossref_metadata": Table(
                        input_project_id, dataset_id_crossref_metadata, "crossref_metadata", sharded=True
                    )
                },
                output_table=Table(output_project_id, dataset_id_observatory_intermediate, "crossref_metadata"),
                output_clustering_fields=["doi"],
            )
        ],
        [
            SQLQuery(
                "crossref_events",
                inputs={"crossref_events": Table(input_project_id, dataset_id_crossref_events, "crossref_events")},
                output_table=Table(output_project_id, dataset_id_observatory_intermediate, "crossref_events"),
                output_clustering_fields=["doi"],
            ),
            SQLQuery(
                "crossref_fundref",
                inputs={
                    "crossref_fundref": Table(
                        input_project_id, dataset_id_crossref_fundref, "crossref_fundref", sharded=True
                    ),
                    "crossref_metadata": Table(
                        output_project_id, dataset_id_observatory_intermediate, "crossref_metadata", sharded=True
                    ),
                },
                output_table=Table(output_project_id, dataset_id_observatory_intermediate, "crossref_fundref"),
                output_clustering_fields=["doi"],
            ),
            SQLQuery(
                "ror",
                inputs={
                    "ror": Table(input_project_id, dataset_id_ror, "ror", sharded=True),
                    "country": Table(input_project_id, dataset_id_settings, "country"),
                },
                output_table=Table(output_project_id, dataset_id_observatory_intermediate, "ror"),
            ),
            SQLQuery(
                "orcid",
                inputs={"orcid": Table(input_project_id, dataset_id_orcid, "orcid")},
                output_table=Table(output_project_id, dataset_id_observatory_intermediate, "orcid"),
                output_clustering_fields=["doi"],
            ),
            SQLQuery(
                "open_citations",
                inputs={
                    "open_citations": Table(input_project_id, dataset_id_open_citations, "open_citations", sharded=True)
                },
                output_table=Table(output_project_id, dataset_id_observatory_intermediate, "open_citations"),
                output_clustering_fields=["doi"],
            ),
            SQLQuery(
                "openaccess",
                inputs={
                    "scihub": Table(input_project_id, dataset_id_scihub, "scihub", sharded=True),
                    "unpaywall": Table(input_project_id, dataset_id_unpaywall, "unpaywall", sharded=False),
                    "ror": Table(input_project_id, dataset_id_ror, "ror", sharded=True),
                    "repository": Table(input_project_id, dataset_id_settings, "repository"),
                    "repository_institution_to_ror": Table(
                        output_project_id,
                        dataset_id_observatory_intermediate,
                        "repository_institution_to_ror",
                        sharded=True,
                    ),
                    "crossref_metadata": Table(
                        output_project_id, dataset_id_observatory_intermediate, "crossref_metadata", sharded=True
                    ),
                },
                output_table=Table(output_project_id, dataset_id_observatory_intermediate, "openaccess"),
                output_clustering_fields=["doi"],
            ),
            SQLQuery(
                "openalex",
                inputs={"openalex": Table(input_project_id, dataset_id_openalex, "works", sharded=False)},
                output_table=Table(output_project_id, dataset_id_observatory_intermediate, "openalex"),
                output_clustering_fields=["doi"],
            ),
            SQLQuery(
                "pubmed",
                inputs={"pubmed": Table(input_project_id, dataset_id_pubmed, "pubmed", sharded=False)},
                output_table=Table(output_project_id, dataset_id_observatory_intermediate, "pubmed"),
                output_clustering_fields=["doi"],
            ),
        ],
        [
            SQLQuery(
                "doi",
                inputs={
                    "openalex": Table(output_project_id, dataset_id_observatory_intermediate, "openalex", sharded=True),
                    "ror_hierarchy": Table(
                        output_project_id, dataset_id_observatory_intermediate, "ror_hierarchy", sharded=True
                    ),
                    "openaccess": Table(
                        output_project_id, dataset_id_observatory_intermediate, "openaccess", sharded=True
                    ),
                    "unpaywall": Table(input_project_id, dataset_id_unpaywall, "unpaywall"),
                    "open_citations": Table(
                        output_project_id, dataset_id_observatory_intermediate, "open_citations", sharded=True
                    ),
                    "crossref_events": Table(
                        output_project_id, dataset_id_observatory_intermediate, "crossref_events", sharded=True
                    ),
                    "pubmed": Table(output_project_id, dataset_id_observatory_intermediate, "pubmed", sharded=True),
                    "crossref_metadata": Table(
                        output_project_id, dataset_id_observatory_intermediate, "crossref_metadata", sharded=True
                    ),
                    "ror": Table(output_project_id, dataset_id_observatory_intermediate, "ror", sharded=True),
                    "groupings": Table(input_project_id, dataset_id_settings, "groupings"),
                    "crossref_fundref": Table(
                        output_project_id, dataset_id_observatory_intermediate, "crossref_fundref", sharded=True
                    ),
                    "orcid": Table(output_project_id, dataset_id_observatory_intermediate, "orcid", sharded=True),
                },
                output_table=Table(output_project_id, dataset_id_observatory, "doi"),
                output_clustering_fields=["doi"],
            )
        ],
    ]


def fetch_ror_affiliations(repository_institution: str, num_retries: int = 3) -> Dict:
    """Fetch the ROR affiliations for a given affiliation string.

    :param repository_institution: the affiliation string to search with.
    :param num_retries: the number of retries.
    :return: the list of ROR affiliations.
    """

    print(f"fetch_ror_affiliations: {repository_institution}")
    rors = []
    try:
        response = retry_get_url(
            "https://api.ror.org/organizations", num_retries=num_retries, params={"affiliation": repository_institution}
        )
        items = response.json()["items"]
        for item in items:
            if item["chosen"]:
                org = item["organization"]
                rors.append({"id": org["id"], "name": org["name"]})
    except requests.exceptions.HTTPError as e:
        # If the repository_institution string causes a 500 error with the ROR affiliation matcher
        # Then catch the error and continue as if no ROR ids were matched for this entry.
        logging.error(f"requests.exceptions.HTTPError fetch_ror_affiliations error fetching: {e}")
        # TODO: it would be better to re-raise the error when it isn't a 500 error as something else is likely wrong

    return {"repository_institution": repository_institution, "rors": rors}


def get_snapshot_date(project_id: str, dataset_id: str, table_id: str, snapshot_date: pendulum.DateTime):
    # Get last table shard date before current end date
    logging.info(f"get_snapshot_date {project_id}.{dataset_id}.{table_id} {snapshot_date}")
    table_id = bq_table_id(project_id, dataset_id, table_id)
    table_shard_dates = bq_select_table_shard_dates(table_id=table_id, end_date=snapshot_date)

    if len(table_shard_dates):
        shard_date = table_shard_dates[0]
    else:
        raise AirflowException(f"{table_id} with a table shard date <= {snapshot_date} not found")

    return shard_date


def traverse_ancestors(index: Dict, child_ids: Set):
    """Traverse all of the ancestors of a set of child ROR ids.

    :param index: the index.
    :param child_ids: the child ids.
    :return: all of the ancestors of all child ids.
    """

    ancestors = child_ids.copy()
    for child_id in child_ids:
        parents = index[child_id]

        if not len(parents):
            continue

        child_ancestors = traverse_ancestors(index, parents)
        ancestors = ancestors.union(child_ancestors)

    return ancestors


def ror_to_ror_hierarchy_index(ror: List[Dict]) -> Dict:
    """Make an index of child to ancestor relationships.

    :param ror: the ROR dataset as a list of dicts.
    :return: the index.
    """

    index = {}
    names = {}
    # Add all items to index
    for row in ror:
        ror_id = row["id"]
        index[ror_id] = set()
        names[ror_id] = row["name"]

    # Add all child -> parent relationships to index
    for row in ror:
        ror_id = row["id"]
        name = row["name"]

        # Build index of parents and children
        parents = set()
        children = set()
        for rel in row["relationships"]:
            rel_id = rel["id"]
            rel_type = rel["type"]
            if rel_type == "Parent":
                parents.add(rel_id)
            elif rel_type == "Child":
                children.add(rel_id)

        for rel in row["relationships"]:
            rel_id = rel["id"]
            rel_type = rel["type"]
            rel_label = rel["label"]

            if rel_id in parents and rel_id in children:
                # Prevents infinite recursion
                logging.warning(
                    f"Skipping as: org({rel_id}, {rel_label}) is both a parent and a child of: org({ror_id}, {name})"
                )
                continue

            if rel_type == "Parent":
                if ror_id in index:
                    index[ror_id].add(rel_id)
                else:
                    logging.warning(
                        f"Parent does not exist in database for relationship: parent({rel_id}, {rel_label}), child({ror_id}, {name})"
                    )

            elif rel_type == "Child":
                if rel_id in index:
                    index[rel_id].add(ror_id)
                else:
                    logging.warning(
                        f"Child does not exist in database for relationship: parent({ror_id}, {name}), child({rel_id}, {rel_label})"
                    )

    # Convert parent sets to ancestor sets
    ancestor_index = copy.deepcopy(index)
    for ror_id in index.keys():
        parents = index[ror_id]
        ancestors = traverse_ancestors(index, parents)
        ancestor_index[ror_id] = ancestors

    return ancestor_index
