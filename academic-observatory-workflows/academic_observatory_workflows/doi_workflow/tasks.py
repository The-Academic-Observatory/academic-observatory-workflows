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

from __future__ import annotations

import json
from concurrent.futures import as_completed, ThreadPoolExecutor

import pendulum
from airflow.exceptions import AirflowException
from airflow.models.baseoperator import chain
from airflow.models.taskinstance import TaskInstance

from academic_observatory_workflows.config import project_path
from academic_observatory_workflows.doi_workflow.queries import (
    Aggregation,
    fetch_ror_affiliations,
    get_snapshot_date,
    ror_to_ror_hierarchy_index,
    SQLQuery,
)
from academic_observatory_workflows.doi_workflow.release import DOIRelease
from observatory_platform.airflow.sensors import DagCompleteSensor
from observatory_platform.dataset_api import DatasetAPI, DatasetRelease
from observatory_platform.google.bigquery import (
    bq_create_dataset,
    bq_create_table_from_query,
    bq_load_from_memory,
    bq_run_query,
    bq_select_latest_table,
    bq_sharded_table_id,
    bq_table_id,
    bq_update_table_description,
)
from observatory_platform.jinja2_utils import render_template


def make_sensors(*, sensor_dag_ids: list[str]):
    tasks = []
    for ext_dag_id in sensor_dag_ids:
        sensor = DagCompleteSensor(
            task_id=f"{ext_dag_id}_sensor",
            external_dag_id=ext_dag_id,
        )
        tasks.append(sensor)
    chain(tasks)


def create_datasets(
    *,
    output_project_id: str,
    bq_data_location: str,
    bq_intermediate_dataset_id: str,
    bq_dashboards_dataset_id: str,
    bq_observatory_dataset_id: str,
):
    # output_project_id
    datasets = [
        (bq_intermediate_dataset_id, "Intermediate processing dataset for the Academic Observatory."),
        (bq_dashboards_dataset_id, "The latest data for display in the COKI dashboards."),
        (bq_observatory_dataset_id, "The Academic Observatory dataset."),
    ]

    for dataset_id, description in datasets:
        bq_create_dataset(
            project_id=output_project_id,
            dataset_id=dataset_id,
            location=bq_data_location,
            description=description,
        )


def create_repo_institution_to_ror_table(
    *,
    release: DOIRelease,
    input_project_id: str,
    output_project_id: str,
    bq_unpaywall_dataset_id: str,
    bq_intermediate_dataset_id: str,
    max_fetch_threads: int,
):
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
    table_id = bq_sharded_table_id(
        output_project_id,
        bq_intermediate_dataset_id,
        "repository_institution_to_ror",
        release.snapshot_date,
    )
    success = bq_load_from_memory(table_id, results)
    if not success:
        raise AirflowException(
            f"create_repo_institution_to_ror_table: error loading repository_institution_to_ror table"
        )


def create_ror_hierarchy_table(
    *,
    release: DOIRelease,
    input_project_id: str,
    output_project_id: str,
    bq_ror_dataset_id: str,
    bq_intermediate_dataset_id: str,
):
    # Fetch latest ROR table
    # release = DOIRelease.from_dict(release)
    ror_table_name = "ror"
    print(
        f"create_ror_hierarchy_table: table_id={bq_table_id(input_project_id, bq_ror_dataset_id, ror_table_name)}, end_date={release.snapshot_date}"
    )
    ror_table_id = bq_select_latest_table(
        table_id=bq_table_id(input_project_id, bq_ror_dataset_id, ror_table_name),
        end_date=release.snapshot_date,
        sharded=True,
    )
    print("")
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
    if not success:
        raise AirflowException(f"create_ror_hierarchy_table: error loading ror_hierarchy table")


def create_intermediate_table(*, release: DOIRelease, sql_query: SQLQuery, output_project_id: str, ti: TaskInstance):
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
        bytes_budget=int(3 * 2**40),  # 3 TB
    )
    if not success:
        raise AirflowException(f"create_intermediate_table: error creating {table_id}")

    # Publish XComs with full table paths
    ti.xcom_push(key="input_tables", value=json.dumps(input_tables))


def create_aggregate_table(
    *, release: DOIRelease, aggregation: Aggregation, output_project_id: str, bq_observatory_dataset_id: str
):
    template_path = project_path("doi_workflow", "sql", "create_aggregate.sql.jinja2")
    sql = render_template(
        template_path,
        project_id=output_project_id,
        dataset_id=bq_observatory_dataset_id,
        snapshot_date=release.snapshot_date,
        aggregation_field=aggregation.aggregation_field,
        group_by_time_field=aggregation.group_by_time_field,
        relate_to_institutions=aggregation.relate_to_institutions,
        relate_to_countries=aggregation.relate_to_countries,
        relate_to_groups=aggregation.relate_to_groups,
        relate_to_members=aggregation.relate_to_members,
        relate_to_journals=aggregation.relate_to_journals,
        relate_to_funders=aggregation.relate_to_funders,
        relate_to_publishers=aggregation.relate_to_publishers,
    )

    table_id = bq_sharded_table_id(
        output_project_id, bq_observatory_dataset_id, aggregation.table_name, release.snapshot_date
    )
    success = bq_create_table_from_query(
        sql=sql,
        table_id=table_id,
        clustering_fields=["id"],
    )
    if not success:
        raise AirflowException(f"create_aggregate_table: error creating table {table_id}")


def update_table_descriptions(
    *,
    release: DOIRelease,
    aggregations: list[Aggregation],
    input_table_task_ids: list[str],
    output_project_id: str,
    bq_observatory_dataset_id: str,
    ti: TaskInstance,
):
    # Create list of input tables that were used to create our datasets
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

    table_names = [agg.table_name for agg in aggregations]
    for table_name in table_names:
        description = f"The DOI table aggregated by {table_name}.\n\nInput sources:\n"
        description += "".join([f"  - {input_table}\n" for input_table in input_tables])
        table_id = bq_sharded_table_id(output_project_id, bq_observatory_dataset_id, table_name, release.snapshot_date)
        bq_update_table_description(
            table_id=table_id,
            description=description,
        )


def add_dataset_release(*, release: DOIRelease, api_bq_project_id: str, api_bq_dataset_id: str):
    api = DatasetAPI(bq_project_id=api_bq_project_id, bq_dataset_id=api_bq_dataset_id)
    now = pendulum.now()
    dataset_release = DatasetRelease(
        dag_id=release.dag_id,
        entity_id="doi",
        dag_run_id=release.run_id,
        created=now,
        modified=now,
        snapshot_date=release.snapshot_date,
        data_interval_start=release.data_interval_start,
        data_interval_end=release.data_interval_end,
    )
    api.add_dataset_release(dataset_release)
