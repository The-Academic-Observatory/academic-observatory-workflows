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
import uuid

import pendulum
from airflow.exceptions import AirflowException
from airflow.models.baseoperator import chain
from airflow.models.taskinstance import TaskInstance
from google.cloud import bigquery

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


def _staging_table_id(project_id: str, dataset_id: str, suffix: str) -> str:
    """Return a unique staging table ID in the given dataset.

    A short random suffix prevents collisions between concurrent DAG runs.
    Tables are cleaned up explicitly in the finally block of
    create_aggregate_table, with the dataset's default expiry as a backstop.
    """
    run_id = uuid.uuid4().hex[:8]
    return f"{project_id}.{dataset_id}.agg_tmp_{suffix}_{run_id}"


def create_aggregate_table(
    *,
    release: DOIRelease,
    aggregation: Aggregation,
    output_project_id: str,
    bq_observatory_dataset_id: str,
    bq_staging_dataset_id: str = "observatory_staging",
):
    """Create an aggregate table from the DOI table.

    This implementation splits the work into four steps, each writing
    to a staging table, so no single query has to hold large arrays in memory:

      1a. Flat DOI table  — one scan of the raw DOI table, storing per-DOI
                            OA booleans, citations, concepts, and relation
                            arrays. All subsequent steps read from here.

      1b. Base table      — aggregated non-relation columns (citations, OA
                            summary, output types, events, disciplines).
                            Also reads from the raw DOI table; could be
                            moved to read from the flat table in future.

      2.  Relation tables — one per enabled relation type (institutions,
                            countries, etc.). Reads from the flat table.
                            Uses direct COUNTIF/SUM aggregations instead of
                            collecting large OA struct arrays, which was the
                            root cause of the original OOM.

      3.  Final assembly  — simple JOIN of base + relation tables into the
                            destination table.

    Staging tables are deleted in the finally block regardless of success or
    failure. Set a short default expiry on bq_staging_dataset_id as a
    backstop in case deletion fails.
    """
    client = bigquery.Client()

    relation_flags = {
        "institutions": aggregation.relate_to_institutions,
        "countries": aggregation.relate_to_countries,
        "groups": aggregation.relate_to_groups,
        "members": aggregation.relate_to_members,
        "funders": aggregation.relate_to_funders,
        "publishers": aggregation.relate_to_publishers,
        "journals": aggregation.relate_to_journals,
    }
    enabled_relations = [name for name, enabled in relation_flags.items() if enabled]

    common_vars = dict(
        project_id=output_project_id,
        dataset_id=bq_observatory_dataset_id,
        snapshot_date=release.snapshot_date,
        aggregation_field=aggregation.aggregation_field,
        group_by_time_field=aggregation.group_by_time_field,
    )

    staging_tables = []

    try:
        # ── Step 1a: Per-DOI flat table ───────────────────────────────────────
        # One scan of the raw DOI table, shared by all relation queries.
        # Stores OA booleans, citations, level-0 concepts, and pre-filtered
        # relation arrays so relation queries never touch the raw DOI table.
        flat_table_id = _staging_table_id(output_project_id, bq_staging_dataset_id, f"flat_{aggregation.table_name}")
        staging_tables.append(flat_table_id)
        print(f"create_aggregate_table: step 1a — flat table → {flat_table_id}")

        flat_sql = render_template(
            project_path("doi_workflow", "sql", "create_aggregate_doi_flat.sql.jinja2"),
            **common_vars,
            **{f"relate_to_{k}": v for k, v in relation_flags.items()},
        )
        success = bq_create_table_from_query(sql=flat_sql, table_id=flat_table_id, client=client)
        if not success:
            raise AirflowException(f"create_aggregate_table: failed creating flat table {flat_table_id}")

        # ── Step 1b: Base aggregated table ────────────────────────────────────
        base_table_id = _staging_table_id(output_project_id, bq_staging_dataset_id, f"base_{aggregation.table_name}")
        staging_tables.append(base_table_id)
        print(f"create_aggregate_table: step 1b — base table → {base_table_id}")

        base_sql = render_template(
            project_path("doi_workflow", "sql", "create_aggregate_base.sql.jinja2"),
            **common_vars,
        )
        success = bq_create_table_from_query(sql=base_sql, table_id=base_table_id, client=client)
        if not success:
            raise AirflowException(f"create_aggregate_table: failed creating base table {base_table_id}")

        # ── Step 2: One relation table per enabled relation type ──────────────
        relation_table_ids = {}
        for relation_type in enabled_relations:
            rel_table_id = _staging_table_id(
                output_project_id, bq_staging_dataset_id, f"{aggregation.table_name}_{relation_type}"
            )
            staging_tables.append(rel_table_id)
            print(f"create_aggregate_table: step 2 — {relation_type} → {rel_table_id}")

            rel_sql = render_template(
                project_path("doi_workflow", "sql", "create_aggregate_relation.sql.jinja2"),
                **common_vars,
                relation_type=relation_type,
                flat_table_id=flat_table_id,
                base_table_id=base_table_id,
            )
            success = bq_create_table_from_query(sql=rel_sql, table_id=rel_table_id, client=client)
            if not success:
                raise AirflowException(f"create_aggregate_table: failed creating relation table {rel_table_id}")
            relation_table_ids[relation_type] = rel_table_id

        # ── Step 3: Final assembly ────────────────────────────────────────────
        final_table_id = bq_sharded_table_id(
            output_project_id,
            bq_observatory_dataset_id,
            aggregation.table_name,
            release.snapshot_date,
        )
        print(f"create_aggregate_table: step 3 — final assembly → {final_table_id}")

        final_sql = render_template(
            project_path("doi_workflow", "sql", "create_aggregate_final.sql.jinja2"),
            base_table_id=base_table_id,
            relation_types=enabled_relations,
            relation_table_ids=relation_table_ids,
        )
        success = bq_create_table_from_query(
            sql=final_sql,
            table_id=final_table_id,
            clustering_fields=["id"],
            client=client,
        )
        if not success:
            raise AirflowException(f"create_aggregate_table: failed creating final table {final_table_id}")

    finally:
        # Clean up staging tables whether we succeeded or failed.
        for table_id in staging_tables:
            try:
                client.delete_table(table_id, not_found_ok=True)
                print(f"create_aggregate_table: deleted staging table {table_id}")
            except Exception as e:
                print(f"create_aggregate_table: could not delete staging table {table_id}: {e}")


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
