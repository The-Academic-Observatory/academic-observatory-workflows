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
import hashlib

import os
import re
import logging
import pendulum
from datetime import timedelta
from dataclasses import dataclass
from typing import Dict, List, Optional, Union

from google.cloud import bigquery
from google.cloud.bigquery import Table as BQTable

from academic_observatory_workflows.config import schema_folder as default_schema_folder, Tag
from observatory.platform.bigquery import (
    bq_load_from_memory,
    bq_create_dataset,
    bq_run_query,
    bq_table_exists,
    bq_select_columns,
)
from observatory.platform.observatory_config import CloudWorkspace
from observatory.platform.utils.dag_run_sensor import DagRunSensor
from observatory.platform.workflows.workflow import Workflow, set_task_state, Release


class DataQualityCheckRelease(Release):

    """Construct a DDQCRelease for the workflow."""

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
    # This workflow will wait until all of the below have finished before running the data quality check workflow.
    SENSOR_DAG_IDS = [
        "crossref_events",
        "crossref_fundref",
        "crossref_metadata",
        "doi_workflow",
        "geonames",
        "grid",
        "open_citations",
        "openalex",
        "orcid",
        "pubmed",
        "ror",
        "unpaywall",
    ]

    # These are a list of datasets from workflows or imported from other sources that are under the academic-observatory project.
    DATASETS = {
        "crossref_events": [
            Table(
                project_id="alex-dev-356105",
                dataset_id="crossref_fundref",
                name="crossref_events",
                sharded=False,
                fields="id",
            ),
        ],
        "crossref_fundref": [
            Table(
                project_id="alex-dev-356105",
                dataset_id="crossref_fundref",
                name="crossref_fundref",
                sharded=True,
                fields="funder",
            ),
        ],
        "crossref_metadata": [
            Table(
                project_id="alex-dev-356105",
                dataset_id="crossref_metadata",
                name="crossref_metadata",
                sharded=True,
                fields="doi",
            ),
        ],
        "geonames": [
            Table(
                project_id="alex-dev-356105",
                dataset_id="geonames",
                name="geonames",
                sharded=True,
                fields="oci",
            ),
        ],
        "grid": [
            Table(
                project_id="alex-dev-356105",
                dataset_id="grid",
                name="grid",
                sharded=True,
                fields="oci",
            ),
        ],
        "observatory": [
            Table(
                project_id="alex-dev-356105",
                dataset_id="observatory",
                name="author",
                sharded=True,
                fields="id",
            ),
            Table(
                project_id="alex-dev-356105",
                dataset_id="observatory",
                name="book",
                sharded=True,
                fields="isbn",
            ),
            Table(
                project_id="alex-dev-356105",
                dataset_id="observatory",
                name="country",
                sharded=True,
                fields="id",
            ),
            Table(
                project_id="alex-dev-356105",
                dataset_id="observatory",
                name="doi",
                sharded=True,
                fields="doi",
            ),
            Table(
                project_id="alex-dev-356105",
                dataset_id="observatory",
                name="funder",
                sharded=True,
                fields="id",
            ),
            Table(
                project_id="alex-dev-356105",
                dataset_id="observatory",
                name="group",
                sharded=True,
                fields="id",
            ),
            Table(
                project_id="alex-dev-356105",
                dataset_id="observatory",
                name="institution",
                sharded=True,
                fields="id",
            ),
            Table(
                project_id="alex-dev-356105",
                dataset_id="observatory",
                name="journal",
                sharded=True,
                fields="id",
            ),
            Table(
                project_id="alex-dev-356105",
                dataset_id="observatory",
                name="publisher",
                sharded=True,
                fields="id",
            ),
            Table(
                project_id="alex-dev-356105",
                dataset_id="observatory",
                name="region",
                sharded=True,
                fields="id",
            ),
            Table(
                project_id="alex-dev-356105",
                dataset_id="observatory",
                name="subregion",
                sharded=True,
                fields="id",
            ),
        ],
        "open_citations": [
            Table(
                project_id="alex-dev-356105",
                dataset_id="open_citations",
                name="open_citations",
                sharded=True,
                fields="oci",
            ),
        ],
        "openaire": [
            Table(
                project_id="alex-dev-356105",
                dataset_id="openaire",
                name="community_infrastructure",
                sharded=True,
                fields="id",
            ),
            Table(
                project_id="alex-dev-356105",
                dataset_id="openaire",
                name="dataset",
                sharded=True,
                fields="id",
            ),
            Table(
                project_id="alex-dev-356105",
                dataset_id="openaire",
                name="datasource",
                sharded=True,
                fields="id",
            ),
            Table(
                project_id="alex-dev-356105",
                dataset_id="openaire",
                name="organization",
                sharded=True,
                fields="id",
            ),
            Table(
                project_id="alex-dev-356105",
                dataset_id="openaire",
                name="otherresearchproduct",
                sharded=True,
                fields="id",
            ),
            Table(
                project_id="alex-dev-356105",
                dataset_id="openaire",
                name="publication",
                sharded=True,
                fields="id",
            ),
            Table(
                project_id="alex-dev-356105",
                dataset_id="openaire",
                name="relation",
                sharded=True,
                fields=["source", "target"],
            ),
            Table(
                project_id="alex-dev-356105",
                dataset_id="openaire",
                name="software",
                sharded=True,
                fields="id",
            ),
        ],
        "openalex": [
            Table(
                project_id="alex-dev-356105",
                dataset_id="openalex",
                name="authors",
                sharded=False,
                fields="id",
            ),
            Table(
                project_id="alex-dev-356105",
                dataset_id="openalex",
                name="concepts",
                sharded=False,
                fields="id",
            ),
            Table(
                project_id="alex-dev-356105",
                dataset_id="openalex",
                name="funders",
                sharded=False,
                fields="id",
            ),
            Table(
                project_id="alex-dev-356105",
                dataset_id="openalex",
                name="institutions",
                sharded=False,
                fields="id",
            ),
            Table(
                project_id="alex-dev-356105",
                dataset_id="openalex",
                name="publishers",
                sharded=False,
                fields="id",
            ),
            Table(
                project_id="alex-dev-356105",
                dataset_id="openalex",
                name="sources",
                sharded=False,
                fields="id",
            ),
            Table(
                project_id="alex-dev-356105",
                dataset_id="openalex",
                name="works",
                sharded=False,
                fields="id",
            ),
        ],
        "orcid": [
            Table(
                project_id="alex-dev-356105",
                dataset_id="orcid",
                name="orcid",
                sharded=False,
                fields="orcid_identifier.uri",
            ),
        ],
        "pubmed": [
            Table(
                project_id="alex-dev-356105",
                dataset_id="pubmed",
                name="pubmed",
                sharded=False,
                fields=["MedlineCitation.PMID.value", "MedlineCitation.PMID.Version"],
            ),
        ],
        "ror": [
            Table(
                project_id="alex-dev-356105",
                dataset_id="observatory",
                name="ror",
                sharded=True,
                fields="id",
            ),
        ],
        "scihub": [
            Table(
                project_id="alex-dev-356105",
                dataset_id="scihub",
                name="scihub",
                sharded=True,
                fields="doi",
            ),
        ],
        "unpaywall": [
            Table(
                project_id="alex-dev-356105",
                dataset_id="unpaywall",
                name="unpaywall",
                sharded=False,
                fields="doi",
            )
        ],
        "unpaywall_snapshot": [
            Table(
                project_id="alex-dev-356105",
                dataset_id="unpaywall_snapshot",
                name="unpaywall_snapshot",
                sharded=True,
                fields="doi",
            ),
        ],
    }

    def __init__(
        self,
        *,
        dag_id: str,
        cloud_workspace: CloudWorkspace,
        bq_dataset_id: str = "data_quality_checks",
        bq_dataset_description: str = "This dataset holds metadata about the tables that the Academic Observatory Worflows produce. If there are multiple shards tables, it will go back on the table and check if it hasn't done that table previously.",
        schema_path: str = os.path.join(default_schema_folder(), "data_quality_check", "data_quality_check.json"),
        start_date: Optional[pendulum.DateTime] = pendulum.datetime(2020, 1, 1),
        schedule: Optional[str] = "@weekly",
        queue: str = "default",
        sensor_dag_ids: List[str] = None,
        datasets: Dict[str, List[Table]] = None,
    ):
        # TODO: Fix the param details for all functions.
        """Create the DataQualityCheck Workflow.

        This workflow creates metadata for all

        :param dag_id: the DAG ID.
        :param cloud_workspace: the cloud workspace settings.
        :param bq_dataset_id:
        :param bq_dataset_description:
        :param start_date: the start date.
        :param schedule: Schedule of how often the workflow runs.
        """

        super().__init__(
            dag_id=dag_id,
            start_date=start_date,
            schedule=schedule,
            tags=[Tag.academic_observatory],
            queue=queue,
        )

        self.cloud_workspace = cloud_workspace
        self.project_id = cloud_workspace.project_id
        self.bq_dataset_id = bq_dataset_id
        self.bq_dataset_description = bq_dataset_description
        self.data_location = cloud_workspace.data_location
        self.schema_path = schema_path

        self.sensor_dag_ids = sensor_dag_ids
        if sensor_dag_ids is None:
            self.sensor_dag_ids = DataQualityCheckWorkflow.SENSOR_DAG_IDS

        self.datasets = datasets
        if datasets is None:
            self.datasets = DataQualityCheckWorkflow.DATASETS

        # Add sensors
        with self.parallel_tasks():
            for ext_dag_id in self.sensor_dag_ids:
                sensor = DagRunSensor(
                    task_id=f"{ext_dag_id}_sensor",
                    external_dag_id=ext_dag_id,
                    mode="reschedule",
                    duration=timedelta(days=7),  # Look back up to 7 days from execution date
                    poke_interval=int(timedelta(hours=1).total_seconds()),  # Check at this interval if dag run is ready
                    timeout=int(timedelta(days=3).total_seconds()),  # Sensor will fail after 3 days of waiting
                )
                self.add_operator(sensor)

        self.add_setup_task(self.check_dependencies)
        self.add_task(self.create_dataset)

        # Create parallel task for checking the workflows define above.
        with self.parallel_tasks():
            for task_id, tables in self.datasets.items():
                self.add_task(
                    self.perform_data_quality_check,
                    op_kwargs={"task_id": task_id, "tables": tables},
                    task_id=task_id,
                )

    def make_release(self, **kwargs) -> DataQualityCheckRelease:
        """Make a data quality check release instance.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return: A release instance or list of release instances
        """
        return DataQualityCheckRelease(
            dag_id=self.dag_id,
            run_id=kwargs["run_id"],
            workflow_list=self.datasets,
            schema_path=self.schema_path,
        )

    def create_dataset(self, release: DataQualityCheckRelease, **kwargs):
        """Create a dataset for all of the DQC tables for the workflows."""

        success = bq_create_dataset(
            project_id=self.project_id,
            dataset_id=self.bq_dataset_id,
            location=self.data_location,
            description=self.bq_dataset_description,
        )

        set_task_state(success, self.create_dataset.__name__, release)

    def perform_data_quality_check(self, release: DataQualityCheckRelease, **kwargs):
        """
        For each dataset, create a table where the rows of the table hold the qa metadata of each of the tables.

        and append it onto the last row that exists."""

        task_id = kwargs["task_id"]
        tables: List[Table] = kwargs["tables"]

        # Full table ID for this instance
        dqc_full_table_id = f"{self.project_id}.{self.bq_dataset_id}.{task_id}"

        # Create the DQC table for the dataset that this instance of the workflow is checking.
        logging.info(f"Creating DQC table for workflow/dataset: {task_id}: {dqc_full_table_id}")

        # Loop through each of the tables that are produced under this
        for table_to_check in tables:
            if table_to_check.sharded:
                sub_tables: List[BQTable] = bq_list_tables_shards(
                    dataset_id=table_to_check.dataset_id, base_name=table_to_check.name
                )
            else:
                print(f"table_to_check.full_table_id: {table_to_check.full_table_id}")
                sub_tables: List[BQTable] = [bq_get_table(table_to_check.full_table_id)]

            assert (
                len(sub_tables) > 0
            ), f"No table or sharded tables found in Bigquery for: {table_to_check.dataset_id}.{table_to_check.name}"

            print(f"Sub tables is {sub_tables}")

            records = []
            table: BQTable
            for table in sub_tables:
                full_table_id = table.full_table_id.replace(":", ".")

                hash_id = create_table_hash_id(
                    full_table_id=full_table_id,
                    num_bytes=table.num_bytes,
                    nrows=table.num_rows,
                    ncols=len(bq_select_columns(table_id=full_table_id)),
                )

                if not is_in_dqc_table(hash_id, dqc_full_table_id):
                    logging.info(f"Performing check on table {full_table_id} with hash {hash_id}")
                    dqc_check = create_dqc_record(
                        hash_id=hash_id,
                        full_table_id=full_table_id,
                        fields=table_to_check.fields,
                        is_sharded=table_to_check.sharded,
                        table_in_bq=table,
                    )
                    records.append(dqc_check)
                else:
                    logging.info(
                        f"Table {table_to_check.full_table_id} with hash {hash_id} has already been checked before. Not performing check again."
                    )

            success = bq_load_from_memory(
                table_id=dqc_full_table_id,
                records=records,
                schema_file_path=release.schema_path,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                table_description=f"Data quality check for tables in dataset/workflow: {task_id}",
            )

            assert success, f"Error uploading data quality check to Bigquery."

            set_task_state(success, task_id, release)


def create_dqc_record(
    hash_id: str,
    full_table_id: str,
    fields: Union[str, List[str]],
    is_sharded: bool,
    table_in_bq: BQTable,
) -> dict:
    """
    Perform novel data quality checks on a given table in Bigquery.

    :param table: Workflow table object.
    :param full_table_id: The fully qualified table id, including the shard date suffix.
    :param table_in_bq: Google Bigquery table object from python API.
    :return: Dictionary of values from the data quality check.
    """

    sharded = bool(re.search(r"\d{8}$", full_table_id))
    assert (
        sharded == is_sharded
    ), f"Workflow parameters for table {full_table_id} is/is not sharded is not the same. From Biguqery {sharded} vs from workflow {is_sharded}"

    if sharded:
        shard_id = re.findall(r"\d{8}$", full_table_id)[-1]
        date_shard = pendulum.from_format(shard_id, "YYYYMMDD").format("YYYY-MM-DD")
        table_name = full_table_id.split(".")[-1][:-8]
    else:
        date_shard = None
        table_name = full_table_id.split(".")[-1]

    # Retrieve metadata on the table.
    date_created = table_in_bq.created.strftime("%Y-%m-%d %H:%M:%S")
    date_checked = pendulum.now().format("YYYY-MM-DD HH:mm:ss")
    date_last_modified = table_in_bq.modified.strftime("%Y-%m-%d %H:%M:%S")

    expires = bool(table_in_bq.expires)
    date_expires = table_in_bq.expires.strftime("%Y-%m-%d %H:%M:%S") if expires else None

    num_distinct_records = bq_count_distinct_records(full_table_id, fields=fields)
    num_null_records = bq_count_nulls(full_table_id, fields=fields)
    num_duplicates = bq_count_duplicate_records(full_table_id, fields=fields)

    num_all_fields = len(bq_select_columns(table_id=full_table_id))

    return dict(
        table_name=table_name,
        full_table_id=full_table_id,
        hash_id=hash_id,
        sharded=sharded,
        date_shard=date_shard,
        date_created=date_created,
        expires=expires,
        date_expires=date_expires,
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


def bq_count_distinct_records(full_table_id: str, fields: Union[str, List[str]]) -> int:
    """
    Finds the distinct number of records that have these matching fields.

    :param table_id: The fully qualified table id.
    :param fields: Singular or list of fields to determine the distinct records.
    """

    fields_to_check = ", ".join(fields) if isinstance(fields, list) else fields
    sql = f""" 
    SELECT COUNT(*) as count
    FROM ( SELECT distinct {fields_to_check} FROM {full_table_id} ) 
    """
    print(sql)
    return [dict(row) for row in bq_run_query(sql)][0]["count"]


def bq_count_nulls(full_table_id: str, fields: Union[str, List[str]]) -> int:
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


def bq_get_table(full_table_id: str) -> BQTable:
    """Get a single Bigqury table object from the Google Bigquery API.

    :param full_table_id: Fully qualified table id.
    :return: The table obecjt from the Bigqury API."""

    bq_client = bigquery.Client()
    try:
        table = bq_client.get_table(full_table_id)
        return table
    except:
        print(f"Table is not found! {full_table_id}")
        return None


def bq_list_tables_shards(dataset_id: str, base_name: str) -> List[BQTable]:
    """Get a list of tables that are shards and share the same basename.

    :param dataset_id: The name of the dataset.
    :param table_name: Name of the sharded table.
    :return: List of table objects that share the same name."""

    bq_client = bigquery.Client()
    tables = bq_client.list_tables(dataset_id)
    return [bq_client.get_table(table) for table in tables if re.search(rf"{base_name}\d{{8}}$", table.table_id)]


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


def create_table_hash_id(full_table_id: str, num_bytes: int, nrows: int, ncols: int):
    """Create a unique table identifier based off of the the input parameters for a table in Biguqery.

    :param full_table_id: The fully qualified table name.
    :param num_bytes: Number of bytes stored in the table.
    :param num_rows: Number of rows/records in the table.
    :param num_cols: Number of columns/fields in the table.
    :return: A md5 hash based off of the given input parameters."""

    return hashlib.md5(f"{full_table_id}{num_bytes}{nrows}{ncols}".encode("utf-8")).hexdigest()


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
        sql = f"""
        SELECT hash_id, date_created
        FROM {dqc_full_table_id}
        ORDER BY date_created ASC;
        """
        dqc_table = [dict(row) for row in bq_run_query(sql)]
        hashes = [row["hash_id"] for row in dqc_table]
        return hash_to_check in hashes
    else:
        logging.info(f"DQC record table: {dqc_full_table_id} does not exist!")
        return False
