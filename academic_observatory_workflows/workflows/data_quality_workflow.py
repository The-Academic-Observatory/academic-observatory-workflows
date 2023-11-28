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

# Author: Alex Massen-Hane


from __future__ import annotations

import os
import hashlib
import logging
import pendulum
from google.cloud import bigquery
from dataclasses import dataclass
from typing import Dict, List, Optional, Union
from google.cloud.bigquery import Table as BQTable

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup

from academic_observatory_workflows.config import schema_folder as default_schema_folder, Tag

from observatory.platform.observatory_config import CloudWorkspace
from observatory.platform.workflows.workflow import Workflow, set_task_state, Release
from observatory.platform.bigquery import (
    bq_table_id_parts,
    bq_load_from_memory,
    bq_create_dataset,
    bq_run_query,
    bq_table_exists,
    bq_select_columns,
    bq_get_table,
    bq_select_table_shard_dates,
    bq_table_id as make_bq_table_id,
)


@dataclass
class Table:
    project_id: str
    dataset_id: str
    table_id: str
    primary_key: List[str]
    is_sharded: bool
    shard_limit: Optional[Union[int, bool]] = None

    """Create a metadata class for tables to be processed by this Workflow.

    :param project_id: The Google project_id of where the tables are located.
    :param dataset_id: The dataset that the table is under.
    :param table_id: The name of the table (not the full qualifed table name).
    :param primary_key: Location of where the primary key is located in the table e.g. ["doi"],
     could be multiple different identifiers like for Pubmed: ["MedlineCiation.PMID.value", "MedlineCiation.PMID.Version"]
    :param is_sharded: True if the table is shared or not.
    :param shard_limit: The number of shards to process for this series of table. 
    """

    # Set the default value for the shard limit to 5 just incase it's forgotten in the config file.
    def __post_init__(self):
        self.shard_limit = 5 if self.shard_limit is None and self.is_sharded else self.shard_limit

    @property
    def full_table_id(self):
        return f"{self.project_id}.{self.dataset_id}.{self.table_id}"


class DataQualityWorkflow(Workflow):
    def __init__(
        self,
        *,
        dag_id: str,
        cloud_workspace: CloudWorkspace,
        datasets: Dict,
        sensor_dag_ids: Optional[List[str]] = None,
        bq_dataset_id: str = "data_quality_checks",
        bq_dataset_description: str = "This dataset holds metadata about the tables that the Academic Observatory Worflows produce. If there are multiple shards tables, it will go back on the table and check if it hasn't done that table previously.",
        bq_table_id: str = "data_quality",
        bq_table_description: str = "Data quality check for all tables produced by the Academic Observatory workflows.",
        schema_path: str = os.path.join(default_schema_folder(), "data_quality", "data_quality.json"),
        start_date: Optional[pendulum.DateTime] = pendulum.datetime(2020, 1, 1),
        schedule: str = "@weekly",
        queue: str = "default",
    ):
        """Create the DataQualityCheck Workflow.

        This workflow creates metadata for all the tables defined in the "datasets" dictionary.
        If a table has already been checked before, it will no do it again. This based on if the
        number of columns, rows or bytesof data stored in the table changes. We cannot use the "date modified"
        from the Biguqery table object because it changes if any other metadata is modified, i.e. description, etc.

        :param dag_id: the DAG ID.
        :param cloud_workspace: the cloud workspace settings.
        :param datasets: A dictionary of datasets holding tables that will processed by this workflow.
        :param sensor_dag_ids: List of dags that this workflow will wait to finish before running.
        :param bq_dataset_id: The dataset_id of where the data quality records will be stored.
        :param bq_dataset_description: Description of the data quality check dataset.
        :param bq_table_id: The name of the table in Bigquery.
        :param bq_table_description: The description of the table in Biguqery.
        :param schema_path: The path to the schema file for the records produced by this workflow.
        :param start_date: The start date of the workflow.
        :param schedule: Schedule of how often the workflow runs.
        :param queue: Which queue this workflow will run.
        """

        super().__init__(
            dag_id=dag_id,
            start_date=start_date,
            schedule=schedule,
            tags=[Tag.data_quality],
            queue=queue,
        )

        self.cloud_workspace = cloud_workspace
        self.project_id = cloud_workspace.project_id
        self.bq_dataset_id = bq_dataset_id
        self.bq_table_id = bq_table_id
        self.bq_dataset_description = bq_dataset_description
        self.bq_table_description = bq_table_description
        self.data_location = cloud_workspace.data_location
        self.schema_path = schema_path
        self.datasets = datasets

        # Full table id for the data quality records.
        self.dqc_full_table_id = make_bq_table_id(self.project_id, self.bq_dataset_id, bq_table_id)

        # If no sensor workflow is given, then it will run on a regular scheduled basis.
        self.sensor_dag_ids = sensor_dag_ids if sensor_dag_ids is not None else []

        assert datasets, "No dataset or tables given for this DQC Workflow! Please revise the config file."

    def make_dag(self) -> DAG:
        """Create a DAG object for the workflow explicitly defining the tasks and task groups.

        :return: The DAG object for the workflow."""

        with self.dag:
            # Create a group of sensors for the workflow.
            task_sensor_group = []
            if self.sensor_dag_ids:
                with TaskGroup(group_id="dag_sensors") as tg_sensors:
                    for ext_dag_id in self.sensor_dag_ids:
                        ExternalTaskSensor(
                            task_id=f"{ext_dag_id}_sensor",
                            external_dag_id=ext_dag_id,
                            mode="reschedule",
                        )

                    task_sensor_group = tg_sensors

            # Add the standard tasks for the workflow
            # fmt: off
            task_check_dependencies = PythonOperator(python_callable=self.check_dependencies, task_id="check_dependencies")
            task_create_dataset = self.make_python_operator(self.create_dataset, "create_dataset")
            # fmt: on

            # Add each dataset as a task group and perform the checks on the tables in parallel.
            task_datasets_group = []
            for dataset_id in list(self.datasets.keys()):
                with TaskGroup(group_id=dataset_id) as tg_dataset:
                    table_objects = [
                        Table(
                            project_id=self.project_id,
                            dataset_id=dataset_id,
                            table_id=table["table_id"],
                            is_sharded=table["is_sharded"],
                            primary_key=table["primary_key"],
                            shard_limit=table.get("shard_limit"),
                        )
                        for table in self.datasets[dataset_id]["tables"]
                    ]

                    for table in table_objects:
                        self.make_python_operator(
                            self.perform_data_quality_check,
                            task_id=f"{table.table_id}",
                            op_kwargs={f"task_id": f"{table.table_id}", "table": table},
                        )

                    task_datasets_group.append(tg_dataset)

            # Link all tasks and task groups together.
            (task_sensor_group >> task_check_dependencies >> task_create_dataset >> task_datasets_group)

        return self.dag

    def make_release(self, **kwargs) -> Release:
        """Make a release instance.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return: A release instance.
        """
        return Release(dag_id=self.dag_id, run_id=kwargs["run_id"])

    def create_dataset(self, release: Release, **kwargs):
        """Create a dataset for the table."""

        bq_create_dataset(
            project_id=self.project_id,
            dataset_id=self.bq_dataset_id,
            location=self.data_location,
            description=self.bq_dataset_description,
        )

    def perform_data_quality_check(self, release: Release, **kwargs):
        """For each dataset, create a table that holds metadata about the dataset.

        Please refer to the output of the create_data_quality_record function for all the metadata included in this workflow.
        """

        task_id = kwargs["task_id"]
        table_to_check: Table = kwargs["table"]

        # Grab tables only if they are
        if table_to_check.is_sharded:
            dates = bq_select_table_shard_dates(
                table_id=table_to_check.full_table_id, end_date=pendulum.now(tz="UTC"), limit=1000
            )

            # Use limited number of table shards checked to reduce BQ costs
            shard_limit = len(dates) if len(dates) <= table_to_check.shard_limit else table_to_check.shard_limit

            tables = []
            for shard_date in dates[:shard_limit]:
                table_id = f'{table_to_check.full_table_id}{shard_date.strftime("%Y%m%d")}'
                assert bq_table_exists(table_id), f"Sharded table {table_id} does not exist!"
                table = bq_get_table(table_id)
                tables.append(table)
        else:
            tables = [bq_get_table(table_to_check.full_table_id)]

        assert (
            len(tables) > 0 and tables[0] is not None
        ), f"No table or sharded tables found in Bigquery for: {table_to_check.dataset_id}.{table_to_check.table_id}"

        records = []
        table: BQTable
        for table in tables:
            full_table_id = str(table.reference)

            hash_id = create_table_hash_id(
                full_table_id=full_table_id,
                num_bytes=table.num_bytes,
                nrows=table.num_rows,
                ncols=len(bq_select_columns(table_id=full_table_id)),
            )

            if not is_in_dqc_table(hash_id, self.dqc_full_table_id):
                logging.info(f"Performing check on table {full_table_id} with hash {hash_id}")
                check = create_data_quality_record(
                    hash_id=hash_id,
                    full_table_id=full_table_id,
                    primary_key=table_to_check.primary_key,
                    is_sharded=table_to_check.is_sharded,
                    table_in_bq=table,
                )
                records.append(check)
            else:
                logging.info(
                    f"Table {table_to_check.full_table_id} with hash {hash_id} has already been checked before. Not performing check again."
                )

        if records:
            logging.info(f"Uploading DQC records for table: {task_id}: {self.dqc_full_table_id}")
            success = bq_load_from_memory(
                table_id=self.dqc_full_table_id,
                records=records,
                schema_file_path=self.schema_path,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            )

            assert success, f"Error uploading data quality check to Bigquery."
        else:
            success = True

        set_task_state(success, task_id, release)


def create_data_quality_record(
    hash_id: str,
    full_table_id: str,
    primary_key: List[str],
    is_sharded: bool,
    table_in_bq: BQTable,
) -> Dict[str, Union[str, List[str], float, bool, int]]:
    """
    Perform novel data quality checks on a given table in Bigquery.

    :param hash_id: Unique md5 style identifier of the table.
    :param full_table_id: The fully qualified table id, including the shard date suffix.
    :param primary_key: The key identifier columns for the table.
    :param is_sharded: If the table is supposed to be sharded or not.
    :param table_in_bq: Table metadata object retrieved from the Bigquery API.
    :return: Dictionary of values from the data quality check.
    """

    project_id, dataset_id, table_id, shard_date = bq_table_id_parts(full_table_id)
    assert is_sharded == (shard_date is not None), f"Workflow config  of table {full_table_id} do not match."

    # Retrieve metadata on the table.
    date_created = table_in_bq.created.isoformat()
    date_checked = pendulum.now(tz="UTC").isoformat()
    date_last_modified = table_in_bq.modified.isoformat()

    expires = bool(table_in_bq.expires)
    date_expires = table_in_bq.expires.isoformat() if expires else None

    num_distinct_records = bq_count_distinct_records(full_table_id, fields=primary_key)
    num_null_records = bq_count_nulls(full_table_id, fields=primary_key)
    num_duplicates = bq_count_duplicate_records(full_table_id, fields=primary_key)

    num_all_fields = len(bq_select_columns(table_id=full_table_id))

    return dict(
        full_table_id=full_table_id,
        hash_id=hash_id,
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
        shard_date=shard_date.to_date_string() if shard_date is not None else shard_date,
        is_sharded=is_sharded,
        date_created=date_created,
        expires=expires,
        date_expires=date_expires,
        date_checked=date_checked,
        date_last_modified=date_last_modified,
        size_gb=float(table_in_bq.num_bytes) / (1024.0) ** 3,
        primary_key=primary_key,
        num_rows=table_in_bq.num_rows,
        num_distinct_records=num_distinct_records,
        num_null_records=num_null_records,
        num_duplicates=num_duplicates,
        num_all_fields=num_all_fields,
    )


def create_table_hash_id(full_table_id: str, num_bytes: int, nrows: int, ncols: int) -> str:
    """Create a unique table identifier based off of the the input parameters for a table in Biguqery.

    :param full_table_id: The fully qualified table name.
    :param num_bytes: Number of bytes stored in the table.
    :param nrows: Number of rows/records in the table.
    :param ncols: Number of columns/fields in the table.
    :return: A md5 hash based off of the given input parameters."""

    return hashlib.md5(f"{full_table_id}{num_bytes}{nrows}{ncols}".encode("utf-8")).hexdigest()


def is_in_dqc_table(hash_to_check: str, dqc_full_table_id: str) -> bool:
    """Checks if a table has already been processed before checking if the table's hash_id is in the main DQC table.

    :param hash_to_check: The hash of the table to check if the data quality checks have been performed before.
    :param dqc_full_table_id: The fully qualified name of the table that holds all of the DQC records in Bigquery.
    :return: True if the check has been done before, otherwise false.
    """

    if bq_table_exists(dqc_full_table_id):
        sql = f"""
        SELECT True
        FROM {dqc_full_table_id}
        WHERE hash_id = "{hash_to_check}"
        """
        return bool([dict(row) for row in bq_run_query(sql)])

    else:
        logging.info(f"DQC record table: {dqc_full_table_id} does not exist!")
        return False


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
    return int([dict(row) for row in bq_run_query(sql)][0]["count"])


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
    return int([dict(row) for row in bq_run_query(sql)][0]["nullCount"])


def bq_count_duplicate_records(full_table_id: str, fields: Union[str, List[str]]) -> int:
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
    result = [dict(row) for row in bq_run_query(sql)][0]["total_duplicate_sum"]
    num_duplicates = 0 if result is None else int(result)

    return num_duplicates
