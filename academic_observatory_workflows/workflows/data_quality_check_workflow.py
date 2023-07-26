# Copyright 2023 Curtin University
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

# Author: Alex Massen-HSane


from __future__ import annotations

import gzip
import io
import jsonlines
import hashlib

import os
import re
import logging
import pendulum
from datetime import timedelta
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Set, Optional, Union
from airflow.exceptions import AirflowException

from google.cloud import bigquery
from google.cloud.bigquery import Table as BQTable
from google.cloud.bigquery import LoadJob, LoadJobConfig, SourceFormat
from google.api_core.exceptions import NotFound, Conflict, BadRequest

from observatory.platform.api import get_latest_dataset_release, get_dataset_releases, make_observatory_api
from observatory.platform.files import save_jsonl
from academic_observatory_workflows.config import schema_folder as default_schema_folder, sql_folder, Tag
from observatory.api.client.model.dataset_release import DatasetRelease
from observatory.platform.api import make_observatory_api
from observatory.platform.bigquery import (
    assert_table_id,
    sql_templates_path,
    bq_table_id_parts,
    bq_sharded_table_id,
    bq_create_dataset,
    bq_select_table_shard_dates,
    bq_run_query,
    bq_create_empty_table,
    bq_select_latest_table,
    bq_update_table_description,
)

from observatory.platform.config import AirflowConns
from observatory.platform.observatory_config import CloudWorkspace
from observatory.platform.utils.dag_run_sensor import DagRunSensor
from observatory.platform.utils.jinja2_utils import (
    make_sql_jinja2_filename,
    render_template,
)
from observatory.platform.workflows.workflow import Workflow, make_snapshot_date, set_task_state, Release


class DataQualityCheckRelease(Release):

    """Construct a QACheckRelease for the workflow."""

    def __init__(
        self,
        *,
        dag_id: str,
        run_id: str,
        workflow_list: List[str],
        schema_path: str,
    ):
        super().__init__(
            dag_id=dag_id,
            run_id=run_id,
        )
        self.workflow_list = workflow_list
        self.schema_path = schema_path


@dataclass
class Table:
    def __init__(
        self,
        *,
        project_id: str,
        dataset_id: str,
        name: str,
        sharded: bool,
        fields: Union[List[str], str],
    ):
        """Create a metadata class for each of the tables to be produced.

        There will be one one table for each dataset made.

        Each table will hold the QA information from each table, e.g.

        :param project_id: Which project where the table is located.
        :param dataset_id: The dataset that the table is under.
        :param name: The name of the table (not the full qualifed table name).
        :param source_dataset: Where the table is from.
        :param sharded: True if the table is shared or not.
        :param fields: Location of where the primary key is located in the table e.g. MedlineCiation.PMID.value, could be multiple different identifiers.

        """
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.name = name
        self.sharded = sharded
        self.fields = [fields] if isinstance(fields, str) else fields

    @property
    def full_table_id(self):
        return f"{self.project_id}.{self.dataset_id}.{self.name}"


class DataQualityCheckWorkflow(Workflow):
    # This workflow waits until all of these workflows have run successfully.
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
        "doi_workflow",
        "grid",
    ]

    def __init__(
        self,
        *,
        dag_id: str,
        cloud_workspace: CloudWorkspace,
        bq_dataset_id: str = "data_quality_checks",
        bq_dataset_description: str = "This dataset holds metadata about the tables that the Academic Observatory Worflows produce. If there are multiple shards tables, it will go back on the table and check if it hasn't done that table previously. These data checking workflow tables are produced after the last workflow is run.",
        bq_table_prefix: str = "",
        schema_path: str = os.path.join(default_schema_folder(), "data_quality_check", "data_quality_check.json"),
        observatory_api_conn_id: str = AirflowConns.OBSERVATORY_API,
        start_date: Optional[pendulum.DateTime] = pendulum.datetime(2020, 1, 1),
        schedule: Optional[str] = "@weekly",
        queue: str = "default",
        sensor_dag_ids: List[str] = None,
    ):
        """Create the DoiWorkflow.
        :param dag_id: the DAG ID.
        :param cloud_workspace: the cloud workspace settings.
        :param bq_dataset_id:
        :param bq_dataset_description:
        :param observatory_api_conn_id: COnnection ID for the observatory API.
        :param start_date: the start date.
        :param schedule: Schedule of how often the workflow runs.

        """

        self.observatory_api_conn_id = observatory_api_conn_id

        super().__init__(
            dag_id=dag_id,
            start_date=start_date,
            schedule=schedule,
            airflow_conns=[observatory_api_conn_id],
            tags=[Tag.academic_observatory],
            queue=queue,
        )

        self.cloud_workspace = cloud_workspace
        self.project_id = cloud_workspace.project_id
        self.bq_dataset_id = bq_dataset_id
        self.bq_table_prefix = bq_table_prefix

        self.data_location = cloud_workspace.data_location
        self.schema_path = schema_path

        self.data_quality_check_template_path = (
            "academic_observatory_workflows/database/sql/create_data_quality_table.sql.jinja2"
        )

        self.sensor_dag_ids = sensor_dag_ids
        if sensor_dag_ids is None:
            self.sensor_dag_ids = DataQualityCheckWorkflow.SENSOR_DAG_IDS

        # List of all datasets to go through and produce QA tables of
        self.dag_ids_to_check = {
            "pubmed": [
                Table(
                    project_id="alex-dev-356105",
                    dataset_id="pubmed",
                    name="pubmed",
                    sharded=False,
                    fields=["MedlineCitation.PMID.value", "MedlineCitation.PMID.Version"],
                )
            ],
            "openaire": [
                Table(
                    project_id="alex-dev-356105",
                    dataset_id="openaire",
                    name="publication",
                    sharded=True,
                    fields="id",
                )
            ],
            "openalex": [
                Table(
                    project_id="alex-dev-356105",
                    dataset_id="openalex",
                    name="works",
                    sharded=False,
                    fields="doi",
                ),
                Table(
                    project_id="alex-dev-356105",
                    dataset_id="openalex",
                    name="sources",
                    sharded=False,
                    fields="id",
                ),
            ],
            "doi_workflow": [
                Table(
                    project_id="alex-dev-356105",
                    dataset_id="observatory",
                    name="doi",
                    sharded=True,
                    fields="doi",
                ),
            ],
        }

        # "openalex": [
        #     Table(
        #         project_id=production_project,
        #         dataset_id="openalex",
        #         table_id="authors",
        #         sharded=False,
        #         primary_key_id_loc="ids.doi",
        #     ),
        #     Table(
        #         project_id=production_project,
        #         dataset_id="openalex",
        #         table_id="concepts",
        #         sharded=False,
        #         primary_key_id_loc="ids.doi",
        #     ),
        #     Table(
        #         project_id=production_project,
        #         dataset_id="openalex",
        #         table_id="funders",
        #         sharded=False,
        #         primary_key_id_loc="ids.doi",
        #     ),
        #     Table(
        #         project_id=production_project,
        #         dataset_id="openalex",
        #         table_id="institutions",
        #         sharded=False,
        #         primary_key_id_loc="ids.doi",
        #     ),
        #     Table(
        #         project_id=production_project,
        #         dataset_id="openalex",
        #         table_id="publishers",
        #         sharded=False,
        #         primary_key_id_loc="ids.doi",
        #     ),
        #     Table(
        #         project_id=production_project,
        #         dataset_id="openalex",
        #         table_id="sources",
        #         sharded=False,
        #         primary_key_id_loc="ids.doi",
        #     ),
        #     Table(
        #         project_id=production_project,
        #         dataset_id="openalex",
        #         table_id="works",
        #         sharded=False,
        #         primary_key_id_loc="ids.doi",
        #     ),
        # ],
        # "doi_workflow": [
        #     Table(
        #         project_id=production_project,
        #         dataset_id="observatory",
        #         table_id="author",
        #         sharded=True,
        #         primary_key_id_loc="id",
        #     ),
        #     Table(
        #         project_id=production_project,
        #         dataset_id="observatory",
        #         table_id="book",
        #         sharded=True,
        #         primary_key_id_loc="isbn",
        #     ),
        #     Table(
        #         project_id=production_project,
        #         dataset_id="observatory",
        #         table_id="country",
        #         sharded=True,
        #         primary_key_id_loc="id",
        #     ),
        #     Table(
        #         project_id=production_project,
        #         dataset_id="observatory",
        #         table_id="doi",
        #         sharded=True,
        #         primary_key_id_loc="id",
        #     ),
        #     Table(
        #         project_id=production_project,
        #         dataset_id="observatory",
        #         table_id="funder",
        #         sharded=True,
        #         primary_key_id_loc="id",
        #     ),
        #     Table(
        #         project_id=production_project,
        #         dataset_id="observatory",
        #         table_id="group",
        #         sharded=True,
        #         primary_key_id_loc="id",
        #     ),
        #     Table(
        #         project_id=production_project,
        #         dataset_id="observatory",
        #         table_id="institution",
        #         sharded=True,
        #         primary_key_id_loc="id",
        #     ),
        #     Table(
        #         project_id=production_project,
        #         dataset_id="observatory",
        #         table_id="journal",
        #         sharded=True,
        #         primary_key_id_loc="id",
        #     ),
        #     Table(
        #         project_id=production_project,
        #         dataset_id="observatory",
        #         table_id="publisher",
        #         sharded=True,
        #         primary_key_id_loc="id",
        #     ),
        #     Table(
        #         project_id=production_project,
        #         dataset_id="observatory",
        #         table_id="region",
        #         sharded=True,
        #         primary_key_id_loc="id",
        #     ),
        #     Table(
        #         project_id=production_project,
        #         dataset_id="observatory",
        #         table_id="subregion",
        #         sharded=True,
        #         primary_key_id_loc="id",
        #     ),
        # ],
        # "ror": [Table(table_id="ror", sharded=True, primary_key_id_loc="id")],
        # }

        # # Add sensors
        # with self.parallel_tasks():
        #     for ext_dag_id in self.sensor_dag_ids:
        #         sensor = DagRunSensor(
        #             task_id=f"{ext_dag_id}_sensor",
        #             external_dag_id=ext_dag_id,
        #             mode="reschedule",
        #             duration=timedelta(days=7),  # Look back up to 7 days from execution date
        #             poke_interval=int(timedelta(hours=1).total_seconds()),  # Check at this interval if dag run is ready
        #             timeout=int(timedelta(days=3).total_seconds()),  # Sensor will fail after 3 days of waiting
        #         )
        #         self.add_operator(sensor)

        self.add_setup_task(self.check_dependencies)
        self.add_task(self.create_dataset)

        # Create tasks creating the Metadata tables for each set of tables defined above.
        self.input_table_task_ids = []
        with self.parallel_tasks():
            for dag_id, tables in self.dag_ids_to_check.items():
                task_id = f"{dag_id}"
                self.add_task(
                    self.create_data_quality_check_table,
                    op_kwargs={"dag_id": dag_id, "task_id": task_id, "tables": tables},
                    task_id=task_id,
                )
                self.input_table_task_ids.append(task_id)

    def make_release(self, **kwargs) -> DataQualityCheckRelease:
        """Make a QA Check release instance. The release is passed as an argument to the function (TelescopeFunction) that is
        called in 'task_callable'.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return: A release instance or list of release instances
        """
        return DataQualityCheckRelease(
            dag_id=self.dag_id,
            run_id=kwargs["run_id"],
            workflow_list=self.dag_ids_to_check,
            schema_path=self.schema_path,
        )

    def create_dataset(self, release: DataQualityCheckRelease, **kwargs):
        """Create dataset for all the QA Check tables."""

        success = bq_create_dataset(
            project_id=self.project_id, dataset_id=self.bq_dataset_id, location=self.data_location
        )

        set_task_state(success, self.create_dataset.__name__, release)

    def create_data_quality_check_table(self, release: DataQualityCheckRelease, **kwargs):
        """
        For each dataset, create a table where the rows of the table hold the qa metadata of each of the tables.

        and append it onto the last row that exists."""

        dag_id = kwargs["dag_id"]  # TODO: Change to dataset id ???
        task_id = kwargs["task_id"]
        tables: List[Table] = kwargs["tables"]

        # Full table ID for this instance
        dqc_full_table_id = f"{self.project_id}.{self.bq_dataset_id}.{dag_id}"

        # Create the DQC table for the dataset that this instance of the workflow is checking.
        logging.info(f"Creating DQC table for workflow: {dag_id}: {dqc_full_table_id}")

        # Loop through each of the tables that are produced under this
        for table_to_check in tables:
            sub_tables: List[BQTable] = bq_list_tables_with_prefix(
                dataset_id=table_to_check.dataset_id, prefix=table_to_check.name
            )

            dqc_data = []

            table: BQTable
            for table in sub_tables:
                full_table_id = table.full_table_id.replace(":", ".")
                assert_table_id(full_table_id)

                hash_id = create_table_hash_id(
                    full_table_id=full_table_id,
                    num_bytes=table.num_bytes,
                    nrows=table.num_rows,
                    ncols=len(bq_select_columns(full_table_id)),
                )

                logging.info(f"Full table id: {full_table_id}, hash_id: {hash_id}, ")

                if not is_in_dqc_table(hash_id, dqc_full_table_id):
                    logging.info(f"Performing check on table {full_table_id} with hash {hash_id}")
                    dqc_data.append(
                        perform_dqc_check(
                            hash_id=hash_id,
                            full_table_id=full_table_id,
                            fields=table_to_check.fields,
                            table_in_bq=table,
                        )
                    )
                else:
                    logging.info(
                        f"Table {table_to_check.full_table_id} ( with hash {hash_id} ) has already been checked before. Not performing dqc check again."
                    )

            success = bq_load_from_memory(
                dqc_data,
                table_id=dqc_full_table_id,
                table_description=f"{dag_id}",
                schema_file_path=release.schema_path,
                source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            )

            assert success, f"Error uploading data quality check to Bigquery."

            set_task_state(success, task_id, release)


def perform_dqc_check(
    hash_id: str,
    full_table_id: str,
    fields: Union[str, List[str]],
    table_in_bq: BQTable,
) -> dict:
    """
    Perform novel data quality checks on a given table in Bigquery.

    :param full_table_id: The fully qualified table id, including the shard date suffix.
    :param table_in_bq: Google Bigquery table object from python API.
    :return: Dictionary of values from the data quality check.
    """

    sharded = bq_is_table_date_sharded(full_table_id)

    if sharded:
        shard_id = re.findall(r"\d{8}$", full_table_id)[-1]
        date_shard = pendulum.from_format(shard_id, "YYYYMMDD").format("YYYY-MM-DD")
        table_name = full_table_id.split(".")[-1][:-8]
    else:
        date_shard = None
        table_name = full_table_id.split(".")[-1]

    # Retrieve metadata on the table.
    date_created = pendulum.instance(table_in_bq.created).format("YYYY-MM-DD HH:mm:ss")
    date_checked = pendulum.now().format("YYYY-MM-DD HH:mm:ss")
    date_last_modified = pendulum.instance(table_in_bq.modified).format("YYYY-MM-DD HH:mm:ss")

    num_distinct_records = bq_count_distinct_records(full_table_id, fields=fields)
    num_null_records = bq_count_num_nulls_for_field(full_table_id, fields=fields)
    num_duplicates = bq_count_duplicate_records(full_table_id, fields=fields)

    num_all_fields = len(bq_select_columns(full_table_id))

    return dict(
        table_name=table_name,
        full_table_id=full_table_id,
        hash_id=hash_id,
        sharded=sharded,
        date_shard=date_shard,
        date_created=date_created,
        date_checked=date_checked,
        date_last_modified=date_last_modified,
        size_gb=float(table_in_bq.num_bytes) / (1024.0) ** 3,
        primary_key=fields,
        num_rows=table_in_bq.num_rows,
        num_distinct_records=num_distinct_records,
        num_null_records=num_null_records,
        num_duplicates=num_duplicates,
        num_all_fields=num_all_fields,
    )


def bq_table_exists(full_table_id: str) -> bool:
    """Check if a Bigquery table exists.

    :param table_id: Fully qualified table id.
    :return exists: True if the table exists, otherwise false."""

    bq_client = bigquery.Client()
    try:
        bq_client.get_table(full_table_id)
    except NotFound:
        return False

    return True


def bq_table_row_count(full_table_id: str) -> int:
    """Get the number of row that are present in table.

    :param table_id: Fully qualified table id.
    :return row_count: Number of rows in the Bigquery table."""

    bq_client = bigquery.Client()
    table = bq_client.get_table(full_table_id)
    row_count = table.num_rows

    return row_count


def bq_count_distinct_records(full_table_id: str, fields: Union[str, List[str]]) -> int:
    """
    Finds the distinct number of records that have these matching primary keys.

    :param table_id: The fully qualified table id.
    :param fields: Singular or list of fields to determine the distinct records.
    """

    keys = ", ".join(fields) if isinstance(fields, list) else fields
    sql = f""" 
    SELECT COUNT(*) as count
    FROM ( SELECT distinct {keys} FROM {full_table_id} ) 
    """
    print(sql)
    return [dict(row) for row in bq_run_query(sql)][0]["count"]


def bq_count_num_nulls_for_field(full_table_id: str, fields: Union[str, List[str]]) -> int:
    """Return the number of nulls for a singular field or number of fields.
       This is separated by an OR condition, thus will be counts if any of the fields listed are nulls/empty.

    :param full_table_id: The fully qualified table id.
    :param fields: A single string or list of strings to have the number of nulls checked.
    :return: The integer number of nulls present in the given fields."""

    fields_to_check = " IS NULL OR ".join(fields) if isinstance(fields, list) else fields
    sql = f"""
    SELECT COUNTIF( {fields_to_check} IS NULL) AS nullCount
    FROM `{full_table_id}`
    """
    print(sql)
    return [dict(row) for row in bq_run_query(sql)][0]["nullCount"]


def bq_list_tables_with_prefix(dataset_id: str, prefix: str) -> List[BQTable]:
    """Get a list of table obects that share the same prefix in a dataset.

    :param dataset_id: The name of the dataset.
    :param prefix: Prefix to search for.
    :return: List of table objects that share the specified prefix."""

    bq_client = bigquery.Client()
    tables = bq_client.list_tables(dataset_id)

    return [bq_client.get_table(table) for table in tables if table.table_id.startswith(prefix)]


def bq_is_table_date_sharded(full_table_id) -> bool:
    """Determine if the table is in a series of shards or not.

    :param full_table_id: Fully qualified table id.
    :return: True if the table is a date sharded table.
    """
    return bool(re.search(r"\d{8}$", full_table_id))


def bq_get_table_created_date(full_table_id: str) -> pendulum.datetime:
    """Retrive the date of when a table was created.

    :param full_table_id: Full qualified table id.
    :return: Datetime of when the table was created.
    """
    bq_client = bigquery.client()
    table = bq_client.get_table(full_table_id)

    return pendulum.instance(table.created)


def bq_count_duplicate_records(full_table_id: str, fields: Union[str, List[str]]) -> dict:
    """Query a table in Bigquery and return a dictionary of values holding the field/s
    and the number of duplicates for said key/s.

    :param fields: String or list of strings of the keys to query the table.
    :param full_table_id: Fully qualified table name.
    """

    fields_to_check = ", ".join(fields) if isinstance(fields, list) else fields
    sql = f"""
    SELECT 
        SUM(duplicate_count) AS total_duplicate_sum
    FROM (
        SELECT {fields_to_check}, COUNT(*) AS duplicate_count
        FROM `{full_table_id}`
        GROUP BY {fields_to_check}
        HAVING COUNT(*) > 1
    )
    """
    print(sql)

    result = [dict(row) for row in bq_run_query(sql)][0]["total_duplicate_sum"]
    num_duplicates = 0 if result is None else result

    return num_duplicates


# BigQuery single query byte limit.
# Daily limit is set in Terraform
BIGQUERY_SINGLE_QUERY_BYTE_LIMIT = int(2 * 2**40)  # 2 TiB


def bq_select_columns(
    table_id: str,
    bytes_budget: Optional[int] = BIGQUERY_SINGLE_QUERY_BYTE_LIMIT,
) -> List[Dict]:
    """Select columns from a BigQuery table.

    :param table_id: the fully qualified BigQuery table identifier.
    :param bytes_budget: the BigQuery bytes budget.
    :return: the columns, which includes column_name and data_type.
    """

    project_id, dataset_id, table_id, _, _ = bq_table_id_parts(table_id)
    template_path = os.path.join(sql_templates_path(), "select_columns.sql.jinja2")
    query = render_template(
        template_path,
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
    )
    rows = bq_run_query(query, bytes_budget=bytes_budget)
    return [dict(row) for row in rows]


def bq_load_from_memory(
    records: List[Dict],
    table_id: str,
    schema_file_path: str,
    source_format: str,
    csv_field_delimiter: str = ",",
    csv_quote_character: str = '"',
    csv_allow_quoted_newlines: bool = False,
    csv_skip_leading_rows: int = 0,
    partition: bool = False,
    partition_field: Union[None, str] = None,
    partition_type: bigquery.TimePartitioningType = bigquery.TimePartitioningType.DAY,
    require_partition_filter=False,
    write_disposition: str = bigquery.WriteDisposition.WRITE_EMPTY,
    table_description: str = "",
    cluster: bool = False,
    clustering_fields=None,
    ignore_unknown_values: bool = False,
) -> bool:
    """Load a BigQuery table from an object on Google Cloud Storage.

    :param file_path: the file_path(s) of the object to load from memory into BigQuery.
    :param table_id: the fully qualified BigQuery table identifier.
    :param schema_file_path: path on local file system to BigQuery table schema.
    :param source_format: the format of the data to load into BigQuery.
    :param csv_field_delimiter: the field delimiter character for data in CSV format.
    :param csv_quote_character: the quote character for data in CSV format.
    :param csv_allow_quoted_newlines: whether to allow quoted newlines for data in CSV format.
    :param csv_skip_leading_rows: the number of leading rows to skip for data in CSV format.
    :param partition: whether to partition the table.
    :param partition_field: the name of the partition field.
    :param partition_type: the type of partitioning.
    :param require_partition_filter: whether the partition filter is required or not when querying the table.
    :param write_disposition: whether to append, overwrite or throw an error when data already exists in the table.
    :param table_description: the description of the table.
    :param cluster: whether to cluster the table or not.
    :param clustering_fields: what fields to cluster on.
    Default is to overwrite.
    :param ignore_unknown_values: whether to ignore unknown values or not.
    :return: True if the load job was successful, False otherwise.
    """

    func_name = bq_load_from_memory.__name__

    assert_table_id(table_id)

    # Handle mutable default arguments
    if clustering_fields is None:
        clustering_fields = []

    # Create load job
    client = bigquery.Client()
    job_config = LoadJobConfig()

    # Set global options
    job_config.source_format = source_format
    job_config.schema = client.schema_from_json(schema_file_path)
    job_config.write_disposition = write_disposition
    job_config.destination_table_description = table_description
    job_config.ignore_unknown_values = ignore_unknown_values

    # Set CSV options
    if source_format == SourceFormat.CSV:
        job_config.field_delimiter = csv_field_delimiter
        job_config.quote_character = csv_quote_character
        job_config.allow_quoted_newlines = csv_allow_quoted_newlines
        job_config.skip_leading_rows = csv_skip_leading_rows

    # Set partitioning settings
    if partition:
        job_config.time_partitioning = bigquery.TimePartitioning(
            type_=partition_type, field=partition_field, require_partition_filter=require_partition_filter
        )
    # Set clustering settings
    if cluster:
        job_config.clustering_fields = clustering_fields

    load_job = None
    try:
        # Save as JSON Lines in memory
        with io.BytesIO() as bytes_io:
            with gzip.GzipFile(fileobj=bytes_io, mode="w") as gzip_file:
                with jsonlines.Writer(gzip_file) as writer:
                    writer.write_all(records)

            load_job: LoadJob = client.load_table_from_file(bytes_io, table_id, job_config=job_config, rewind=True)

        result = load_job.result()
        state = result.state == "DONE"

        logging.info(f"{func_name}: load bigquery table result.state={result.state}")
    except BadRequest as e:
        logging.error(f"{func_name}: load bigquery table failed: {e}.")
        if load_job:
            logging.error(f"Error collection:\n{load_job.errors}")
        state = False

    return state


def create_table_hash_id(full_table_id: str, num_bytes: int, nrows: int, ncols: int):
    """Create a unique table identifier based off of the the input parameters for a table in Biguqery.

    :param full_table_id: The fully qualified table name.
    :param num_bytes: Number of bytes stored in the table.
    :param num_rows: Number of rows/records in the table.
    :param num_cols: Number of columns/fields in the table.
    :return: A md5 hash based off of the given input parameters."""

    return hashlib.md5(f"{full_table_id}{num_bytes}{nrows}{ncols}".encode("utf-8")).hexdigest()


def query_table(table_id: str, select_columns: str, order_by_field: str) -> List[Dict]:
    """Query a BigQuery table, sorting the results and returning results as a list of dicts.

    :param table_id: the table id.
    :param select_columns: Columns to pull from the table.
    :param order_by_field: what field or fields to order by.
    :return: the table rows.
    """

    return [
        dict(row) for row in bq_run_query(f"SELECT {select_columns} FROM {table_id} ORDER BY {order_by_field} ASC;")
    ]


def is_in_dqc_table(
    hash_to_check: str,
    dqc_full_table_id: str,
):
    """
    :param dqc_full_table_id:
    :param hash_to_check: The md5 hash of the table to check if the data quality checks have been performed before.

    :return: True if the check has been done before, otherwise false.
    """

    if bq_table_exists(dqc_full_table_id):
        # List all entries in the DQC table
        dqc_table = query_table(dqc_full_table_id, "hash_id, date_created", order_by_field="date_created")
        hashes = [row["hash_id"] for row in dqc_table]
        return hash_to_check in hashes
    else:
        return False
