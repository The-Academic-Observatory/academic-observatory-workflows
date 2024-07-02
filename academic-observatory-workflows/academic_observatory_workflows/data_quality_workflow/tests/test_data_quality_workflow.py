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

import os

import pendulum
from google.cloud import bigquery
from google.cloud.bigquery import Table as BQTable

from academic_observatory_workflows.config import project_path
from academic_observatory_workflows.data_quality_workflow.data_quality_workflow import (
    bq_count_distinct_records,
    bq_count_duplicate_records,
    bq_count_nulls,
    bq_get_table,
    create_dag,
    create_data_quality_record,
    create_table_hash_id,
    is_in_dqc_table,
    Table,
)
from observatory.platform.bigquery import (
    bq_load_from_memory,
    bq_select_columns,
    bq_table_id as make_bq_table_id,
    bq_upsert_records,
)
from observatory.platform.config import module_file_path
from observatory.platform.files import load_jsonl
from observatory.platform.observatory_config import Workflow
from observatory.platform.observatory_environment import (
    find_free_port,
    make_dummy_dag,
    ObservatoryEnvironment,
    ObservatoryTestCase,
    random_id,
)

FIXTURES_FOLDER = project_path("data_quality_workflow", "tests", "fixtures")


class TestDataQualityWorkflow(ObservatoryTestCase):
    """Tests for the Data Quality Check Workflow"""

    def __init__(self, *args, **kwargs):
        self.dag_id = "data_quality_workflow"
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")

        super(TestDataQualityWorkflow, self).__init__(*args, **kwargs)

    def test_dag_load(self):
        """Test that the DataQualityCheck DAG can be loaded from a DAG bag."""

        env = ObservatoryEnvironment(
            workflows=[
                Workflow(
                    dag_id=self.dag_id,
                    name="Data Quality Check Workflow",
                    class_name="academic_observatory_workflows.data_quality_workflow.data_quality_workflow.create_dag",
                    cloud_workspace=self.fake_cloud_workspace,
                    kwargs=dict(
                        sensor_dag_ids=["doi"],
                        datasets={
                            "observatory": {"tables": [{"table_id": "doi", "is_sharded": True, "primary_key": ["doi"]}]}
                        },
                    ),
                )
            ],
            api_port=find_free_port(),
        )

        with env.create():
            dag_file = os.path.join(module_file_path("observatory.platform.dags"), "load_dags.py")
            self.assert_dag_load(self.dag_id, dag_file)

    def test_dag_structure(self):
        """Test that the DAG has the correct structure."""

        dag = create_dag(
            dag_id=self.dag_id,
            cloud_workspace=self.fake_cloud_workspace,
            sensor_dag_ids=["dummy1", "dummy2"],
            datasets={
                "observatory": {
                    "tables": [
                        {
                            "table_id": "doi",
                            "primary_key": ["doi"],
                            "is_sharded": True,
                            "shard_limit": 5,
                        }
                    ],
                },
                "pubmed": {
                    "tables": [
                        {
                            "table_id": "pubmed",
                            "primary_key": ["MedlineCitation.PMID.value", "MedlineCitation.PMID.Version"],
                            "is_sharded": False,
                        }
                    ]
                },
            },
        )
        self.assert_dag_structure(
            {
                "sensors.dummy1_sensor": ["create_dataset"],
                "sensors.dummy2_sensor": ["create_dataset"],
                "create_dataset": ["observatory.doi", "pubmed.pubmed"],
                "observatory.doi": [],
                "pubmed.pubmed": [],
            },
            dag,
        )

    def test_workflow(self):
        """Test the Data Quality Check Workflow end to end

        Borrowing off of the doi test structure."""

        env = ObservatoryEnvironment(self.project_id, self.data_location, api_port=find_free_port())

        # Where the metadata generated for this workflow is going to be stored.
        bq_table_id = ""
        bq_dataset_id = env.add_dataset(prefix="data_quality_check")
        fake_dataset_id = env.add_dataset()

        test_tables = [
            {
                "full_table_id": make_bq_table_id(self.project_id, fake_dataset_id, "people"),
                "schema_path": os.path.join(FIXTURES_FOLDER, "people_schema.json"),
                "expected": load_jsonl(os.path.join(FIXTURES_FOLDER, "people20230101.jsonl")),
            },
            {
                "full_table_id": make_bq_table_id(self.project_id, fake_dataset_id, "people_shard20230101"),
                "schema_path": os.path.join(FIXTURES_FOLDER, "people_schema.json"),
                "expected": load_jsonl(os.path.join(FIXTURES_FOLDER, "people20230101.jsonl")),
            },
            {
                "full_table_id": make_bq_table_id(self.project_id, fake_dataset_id, "people_shard20230108"),
                "schema_path": os.path.join(FIXTURES_FOLDER, "people_schema.json"),
                "expected": load_jsonl(os.path.join(FIXTURES_FOLDER, "people20230108.jsonl")),
            },
            {
                "full_table_id": make_bq_table_id(self.project_id, fake_dataset_id, "people_shard20230115"),
                "schema_path": os.path.join(FIXTURES_FOLDER, "people_schema.json"),
                "expected": load_jsonl(os.path.join(FIXTURES_FOLDER, "people20230108.jsonl")),
            },
        ]

        with env.create(task_logging=True):
            # Upload the test tables to Bigquery
            for table in test_tables:
                bq_load_from_memory(
                    table_id=table["full_table_id"], records=table["expected"], schema_file_path=table["schema_path"]
                )

            start_date = pendulum.datetime(year=2021, month=10, day=10)
            sensor_dag_ids = ["doi", "pubmed"]
            datasets = {
                fake_dataset_id: {
                    "tables": [
                        {"table_id": "people", "primary_key": ["id"], "is_sharded": False},
                        {"table_id": "people_shard", "primary_key": ["id"], "is_sharded": True, "shard_limit": 2},
                    ],
                },
            }
            bq_table_id = "data_quality"
            data_quality_dag = create_dag(
                dag_id=self.dag_id,
                cloud_workspace=env.cloud_workspace,
                bq_dataset_id=bq_dataset_id,
                start_date=start_date,
                sensor_dag_ids=sensor_dag_ids,
                datasets=datasets,
                bq_table_id=bq_table_id,
            )

            # Run fake version of the dags that the workflow sensors are waiting for.
            logical_date = pendulum.datetime(year=2023, month=1, day=1)
            for dag_id in sensor_dag_ids:
                dag = make_dummy_dag(dag_id, logical_date)
                with env.create_dag_run(dag, logical_date):
                    # Running all of a DAGs tasks sets the DAG to finished
                    ti = env.run_task("dummy_task")
                    self.assertEqual("success", ti.state)

            ### FIRST RUN ###
            # First run of the workflow. Will produce the data_quality table and a record for
            # each of the test tables uploaded, but will miss the 20230101 shard due to the shard_limit parameter set.

            # Run end to end tests for DQC DAG
            with env.create_dag_run(data_quality_dag, logical_date):
                # Test that sensors go into 'success' state as the DAGs that they are waiting for have finished
                for task_id in sensor_dag_ids:
                    ti = env.run_task(f"sensors.{task_id}_sensor")
                    self.assertEqual("success", ti.state)

                # Create dataset
                ti = env.run_task("create_dataset")
                self.assertEqual("success", ti.state)

                # Perform data quality check
                for dataset_id, tables in datasets.items():
                    for table in tables["tables"]:
                        task_id = f"{dataset_id}.{table['table_id']}"
                        ti = env.run_task(task_id)
                        self.assertEqual("success", ti.state)

                # Check that DQC table has been created.
                table_id = make_bq_table_id(self.project_id, bq_dataset_id, bq_table_id)
                self.assert_table_integrity(table_id, expected_rows=3)  # stop and look at table on BQ

            ### SECOND RUN ###
            # For the sake of the test, we will change one of the tables by doing an upsert, so that the
            # hash_id of the first will be different.

            bq_upsert_records(
                main_table_id=test_tables[0]["full_table_id"],
                upsert_table_id=test_tables[2]["full_table_id"],
                primary_key="id",
            )
            self.assert_table_integrity(test_tables[0]["full_table_id"], 16)

            # Run Dummy Dags
            logical_date = pendulum.datetime(year=2023, month=2, day=1)
            for dag_id in sensor_dag_ids:
                dag = make_dummy_dag(dag_id, logical_date)
                with env.create_dag_run(dag, logical_date):
                    # Running all of a DAGs tasks sets the DAG to finished
                    ti = env.run_task("dummy_task")
                    self.assertEqual("success", ti.state)

            # Run end to end tests for DQC DAG
            with env.create_dag_run(data_quality_dag, logical_date):
                # Test that sensors go into 'success' state as the DAGs that they are waiting for have finished
                for task_id in sensor_dag_ids:
                    ti = env.run_task(f"sensors.{task_id}_sensor")
                    self.assertEqual("success", ti.state)

                # Create dataset
                ti = env.run_task("create_dataset")
                self.assertEqual("success", ti.state)
                # Perform data quality check
                for dataset_id, tables in datasets.items():
                    for table in tables["tables"]:
                        task_id = f"{dataset_id}.{table['table_id']}"
                        ti = env.run_task(task_id)
                        self.assertEqual("success", ti.state)

                # Check that DQC table has been created.
                table_id = make_bq_table_id(self.project_id, bq_dataset_id, bq_table_id)
                self.assert_table_integrity(table_id, expected_rows=4)

            ### THIRD RUN ###
            # For this third run, no tables should be updated or changed meaning that there should be
            # no data quality checks done.

            # Run Dummy Dags
            logical_date = pendulum.datetime(year=2023, month=3, day=1)
            for dag_id in sensor_dag_ids:
                dag = make_dummy_dag(dag_id, logical_date)
                with env.create_dag_run(dag, logical_date):
                    # Running all of a DAGs tasks sets the DAG to finished
                    ti = env.run_task("dummy_task")
                    self.assertEqual("success", ti.state)

            # Run end to end tests for Data Quality DAG
            with env.create_dag_run(data_quality_dag, logical_date):
                # Test that sensors go into 'success' state as the DAGs that they are waiting for have finished
                for task_id in sensor_dag_ids:
                    ti = env.run_task(f"sensors.{task_id}_sensor")
                    self.assertEqual("success", ti.state)

                # Create dataset
                ti = env.run_task("create_dataset")
                self.assertEqual("success", ti.state)

                # Perform data quality check
                for dataset_id, tables in datasets.items():
                    for table in tables["tables"]:
                        task_id = f"{dataset_id}.{table['table_id']}"
                        ti = env.run_task(task_id)
                        self.assertEqual("success", ti.state)

                # Check that the DQC table has no new records added for this third run.
                # Check that DQC table has been created.
                table_id = make_bq_table_id(self.project_id, bq_dataset_id, bq_table_id)
                self.assert_table_integrity(table_id, expected_rows=4)


class TestDataQualityUtils(ObservatoryTestCase):
    def __init__(self, *args, **kwargs):
        super(TestDataQualityUtils, self).__init__(*args, **kwargs)

        self.dag_id = "data_quality_checks"
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")
        self.schema_path = project_path("data_quality_workflow", "schema", "data_quality.json")

        # Can't use faker here because the number of bytes in a table is needed to be the same for each test run.
        self.test_table_hash = "771c9176e77c1b03f64b1b5fa4a39cdb"
        self.test_table = [
            dict(id="something", count="1", abstract_text="Hello"),
            dict(id="something", count="2", abstract_text="World"),
            dict(id="somethingelse", count="3", abstract_text="Science"),
            dict(id="somethingelse", count="4", abstract_text="Science"),
            dict(id=None, count="5", abstract_text="Maths"),
        ]

        self.expected_dqc_record = dict(
            table_id="create_dqc_record",
            project_id=self.project_id,
            is_sharded=False,
            shard_date=None,
            expires=False,
            date_expires=None,
            size_gb=1.2200325727462769e-07,
            primary_key=["id"],
            num_rows=5,
            num_distinct_records=3,
            num_null_records=1,
            num_duplicates=4,
            num_all_fields=3,
        )

    def test_create_table_hash_id(self):
        """Test if hash can be reliably created."""

        bq_table_id = "create_table_hash_id"
        result = create_table_hash_id(full_table_id=bq_table_id, num_bytes=131, nrows=5, ncols=3)
        self.assertEqual(result, self.test_table_hash)

    def test_create_dq_record(self):
        """Test if a data quality check record can be reliably created."""

        env = ObservatoryEnvironment(self.project_id, self.data_location, api_port=find_free_port())
        dataset_id = env.add_dataset()
        table_id = "create_dqc_record"

        table_to_check = Table(
            project_id=self.project_id,
            dataset_id=dataset_id,
            table_id=table_id,
            primary_key=["id"],
            is_sharded=False,
        )

        with env.create(task_logging=True):
            full_table_id = make_bq_table_id(self.project_id, dataset_id, table_id)

            # Load the test table from memory to Bigquery.
            success = bq_load_from_memory(table_id=full_table_id, records=self.test_table)
            self.assertTrue(success)

            # Grab the table from the Bigquery API
            table: BQTable = bq_get_table(full_table_id)

            # Need to add a DQC record into a temp table so that we can check if it's in there.
            hash_id = create_table_hash_id(
                full_table_id=full_table_id,
                num_bytes=table.num_bytes,
                nrows=table.num_rows,
                ncols=len(bq_select_columns(table_id=full_table_id)),
            )

            dqc_record = create_data_quality_record(
                hash_id=hash_id,
                full_table_id=full_table_id,
                primary_key=table_to_check.primary_key,
                is_sharded=table_to_check.is_sharded,
                table_in_bq=table,
            )

            # Loop through checking all of the values that do not change for each unittest run.
            keys_to_check = list(self.expected_dqc_record.keys())
            for key in keys_to_check:
                self.assertEqual(dqc_record[key], self.expected_dqc_record[key])

    def test_is_in_dqc_table(self):
        """Test if a data quality check has already been previously performed by checking the table hash that it creates."""

        env = ObservatoryEnvironment(self.project_id, self.data_location, api_port=find_free_port())
        dqc_dataset_id = env.add_dataset(prefix="data_quality_check")
        dataset_id = env.add_dataset()
        table_id = "is_in_dqc_table"
        dag_id = "test_dag"

        table_to_check = Table(
            project_id=self.project_id,
            dataset_id=dataset_id,
            table_id=table_id,
            primary_key=["id"],
            is_sharded=False,
        )

        with env.create(task_logging=True):
            full_table_id = make_bq_table_id(self.project_id, dataset_id, table_id)
            dqc_full_table_id = make_bq_table_id(self.project_id, dqc_dataset_id, dag_id)

            # Load the test table from memory to Bigquery.
            success = bq_load_from_memory(table_id=full_table_id, records=self.test_table)
            self.assertTrue(success)

            # Grab the table from the Bigquery API
            table: BQTable = bq_get_table(full_table_id)

            # Need to add a DQC record into a temp table so that we can check if it's in there.
            hash_id = create_table_hash_id(
                full_table_id=full_table_id,
                num_bytes=table.num_bytes,
                nrows=table.num_rows,
                ncols=len(bq_select_columns(table_id=full_table_id)),
            )

            dqc_record = [
                create_data_quality_record(
                    hash_id=hash_id,
                    full_table_id=full_table_id,
                    primary_key=table_to_check.primary_key,
                    is_sharded=table_to_check.is_sharded,
                    table_in_bq=table,
                )
            ]
            success = bq_load_from_memory(
                table_id=dqc_full_table_id,
                records=dqc_record,
                schema_file_path=self.schema_path,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                table_description=f"{dag_id}",
            )
            self.assertTrue(success)

            # Ensure that hash is in the data quality table.
            result = is_in_dqc_table(hash_to_check=hash_id, dqc_full_table_id=dqc_full_table_id)
            self.assertTrue(result)

            # A random hash that we know shouldn't be in the data quality table.
            random_hash = random_id()
            result = is_in_dqc_table(hash_to_check=random_hash, dqc_full_table_id=dqc_full_table_id)
            self.assertFalse(result)

    def test_bq_count_duplicate_records(self):
        """Test if duplicate records can be reliably found in a table."""

        env = ObservatoryEnvironment(self.project_id, self.data_location, api_port=find_free_port())
        bq_dataset_id = env.add_dataset()
        bq_table_id = "count_duplicate_records"

        with env.create(task_logging=True):
            full_table_id = f"{self.project_id}.{bq_dataset_id}.{bq_table_id}"

            success = bq_load_from_memory(table_id=full_table_id, records=self.test_table)
            self.assertTrue(success)

            num_distinct = bq_count_duplicate_records(full_table_id, "id")
            self.assertEqual(num_distinct, 4)

            num_distinct = bq_count_duplicate_records(full_table_id, ["id", "abstract_text"])
            self.assertEqual(num_distinct, 2)

    def test_bq_count_nulls(self):
        """Test if the number of nulls under a field can be correctly determined."""

        env = ObservatoryEnvironment(self.project_id, self.data_location, api_port=find_free_port())
        bq_dataset_id = env.add_dataset()
        bq_table_id = "count_num_nulls_for_field"

        with env.create(task_logging=True):
            full_table_id = f"{self.project_id}.{bq_dataset_id}.{bq_table_id}"

            success = bq_load_from_memory(table_id=full_table_id, records=self.test_table)
            self.assertTrue(success)

            num_distinct = bq_count_nulls(full_table_id, "id")
            self.assertEqual(num_distinct, 1)

            num_distinct = bq_count_nulls(full_table_id, ["id", "abstract_text"])
            self.assertEqual(num_distinct, 1)

    def test_bq_count_distinct_records(self):
        """Test that the number of distinct records can be reliably detmerined."""

        env = ObservatoryEnvironment(self.project_id, self.data_location, api_port=find_free_port())
        bq_dataset_id = env.add_dataset()
        bq_table_id = "distinct_records"

        with env.create(task_logging=True):
            full_table_id = f"{self.project_id}.{bq_dataset_id}.{bq_table_id}"

            success = bq_load_from_memory(table_id=full_table_id, records=self.test_table)
            self.assertTrue(success)

            num_distinct = bq_count_distinct_records(full_table_id, "id")
            self.assertEqual(num_distinct, 3)

            num_distinct = bq_count_distinct_records(full_table_id, ["id", "abstract_text"])
            self.assertEqual(num_distinct, 4)

    def test_table_object(self):
        """Test that a table's shard limit can be set properly."""

        table = Table(
            project_id=self.project_id,
            dataset_id="dataset_id",
            table_id="table_id",
            primary_key=["id"],
            is_sharded=False,
        )

        self.assertEqual(table.shard_limit, None)

        table = Table(
            project_id=self.project_id,
            dataset_id="dataset_id",
            table_id="table_id",
            primary_key=["id"],
            is_sharded=True,
        )

        self.assertEqual(table.shard_limit, 5)

        table = Table(
            project_id=self.project_id,
            dataset_id="dataset_id",
            table_id="table_id",
            primary_key=["id"],
            is_sharded=False,
            shard_limit=False,
        )

        self.assertEqual(table.shard_limit, False)
