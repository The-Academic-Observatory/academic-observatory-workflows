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
from typing import List
from datetime import timedelta
from google.cloud import bigquery
from google.cloud.bigquery import Table as BQTable

from academic_observatory_workflows.workflows.tests.test_doi_workflow import TestDoiWorkflow
from academic_observatory_workflows.model import bq_load_observatory_dataset, make_observatory_dataset
from academic_observatory_workflows.config import test_fixtures_folder, schema_folder as default_schema_folder
from academic_observatory_workflows.workflows.dqc_workflow import (
    DQCWorkflow,
    Table,
    create_dqc_record,
    bq_count_distinct_records,
    bq_count_nulls,
    bq_get_table,
    bq_list_tables_shards,
    bq_count_duplicate_records,
    create_table_hash_id,
    is_in_dqc_table,
)

from observatory.platform.files import load_jsonl
from observatory.platform.observatory_config import Workflow
from observatory.platform.observatory_environment import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    make_dummy_dag,
    find_free_port,
    random_id,
)
from observatory.platform.bigquery import (
    bq_table_id,
    bq_load_from_memory,
    bq_select_columns,
    bq_delete_old_datasets_with_prefix,
    bq_create_dataset,
)


class TestDQCWorkflow(ObservatoryTestCase):
    """Tests for the Data Quality Check Workflow"""

    def __init__(self, *args, **kwargs):
        self.dag_id = "dqc_workflow"
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")

        self.doi_fixtures = "doi"

        super(TestDQCWorkflow, self).__init__(*args, **kwargs)

    def test_dag_load(self):
        """Test that the DataQualityCheck DAG can be loaded from a DAG bag."""

        env = ObservatoryEnvironment(
            workflows=[
                Workflow(
                    dag_id=self.dag_id,
                    name="Data Quality Check Workflow",
                    class_name="academic_observatory_workflows.workflows.dqc_workflow.DQCWorkflow",
                    cloud_workspace=self.fake_cloud_workspace,
                )
            ]
        )

        with env.create():
            self.assert_dag_load_from_config(self.dag_id)

    def test_dag_structure(self):
        """Test that the DAG has the correct structure."""

        workflow = DQCWorkflow(
            dag_id=self.dag_id,
            cloud_workspace=self.fake_cloud_workspace,
        )
        dag = workflow.make_dag()
        self.assert_dag_structure(
            {
                "crossref_events_sensor": ["check_dependencies"],
                "crossref_fundref_sensor": ["check_dependencies"],
                "crossref_metadata_sensor": ["check_dependencies"],
                "doi_workflow_sensor": ["check_dependencies"],
                "geonames_sensor": ["check_dependencies"],
                "grid_sensor": ["check_dependencies"],
                "open_citations_sensor": ["check_dependencies"],
                "openalex_sensor": ["check_dependencies"],
                "orcid_sensor": ["check_dependencies"],
                "pubmed_sensor": ["check_dependencies"],
                "ror_sensor": ["check_dependencies"],
                "unpaywall_sensor": ["check_dependencies"],
                "check_dependencies": ["create_dataset"],
                "create_dataset": [
                    "crossref_events",
                    "crossref_fundref",
                    "crossref_metadata",
                    "geonames",
                    "grid",
                    "observatory",
                    "open_citations",
                    "openaire",
                    "openalex",
                    "orcid",
                    "pubmed",
                    "ror",
                    "scihub",
                    "unpaywall",
                    "unpaywall_snapshot",
                ],
                "crossref_events": [],
                "crossref_fundref": [],
                "crossref_metadata": [],
                "geonames": [],
                "grid": [],
                "observatory": [],
                "open_citations": [],
                "openaire": [],
                "openalex": [],
                "orcid": [],
                "pubmed": [],
                "ror": [],
                "scihub": [],
                "unpaywall": [],
                "unpaywall_snapshot": [],
            },
            dag,
        )

    def test_workflow(self):
        """Test the Data Quality Check Workflow end to end

        Borrowing off of the doi test structure."""

        env = ObservatoryEnvironment(self.project_id, self.data_location)

        # Where the metadata generated for this workflow is going to be stored.
        dqc_dataset_id = env.add_dataset(prefix="data_quality_check")
        fake_dataset_id = env.add_dataset()
        settings_dataset_id = env.add_dataset(prefix="settings")

        # Required to make the fake data for tests
        repositories = TestDoiWorkflow().repositories
        institutions = TestDoiWorkflow().institutions
        repository = load_jsonl(test_fixtures_folder(self.doi_fixtures, "repository.jsonl"))

        with env.create(task_logging=True):
            start_date = pendulum.datetime(year=2021, month=10, day=10)
            workflow = DQCWorkflow(
                dag_id=self.dag_id,
                cloud_workspace=env.cloud_workspace,
                bq_dataset_id=dqc_dataset_id,
                start_date=start_date,
                datasets={
                    "crossref_events": [
                        Table(
                            project_id=self.project_id,
                            dataset_id=fake_dataset_id,
                            name="crossref_events",
                            sharded=False,
                            fields="id",
                        ),
                    ],
                    "crossref_fundref": [
                        Table(
                            project_id=self.project_id,
                            dataset_id=fake_dataset_id,
                            name="crossref_fundref",
                            sharded=True,
                            fields="funder",
                        ),
                    ],
                    "crossref_metadata": [
                        Table(
                            project_id=self.project_id,
                            dataset_id=fake_dataset_id,
                            name="crossref_metadata",
                            sharded=True,
                            fields="doi",
                        ),
                    ],
                    "openalex": [
                        Table(
                            project_id=self.project_id,
                            dataset_id=fake_dataset_id,
                            name="works",
                            sharded=False,
                            fields="id",
                        ),
                    ],
                    "open_citations": [
                        Table(
                            project_id=self.project_id,
                            dataset_id=fake_dataset_id,
                            name="open_citations",
                            sharded=True,
                            fields="oci",
                        ),
                    ],
                    "pubmed": [
                        Table(
                            project_id=self.project_id,
                            dataset_id=fake_dataset_id,
                            name="pubmed",
                            sharded=False,
                            fields=["MedlineCitation.PMID.value", "MedlineCitation.PMID.Version"],
                        ),
                    ],
                    "ror": [
                        Table(
                            project_id=self.project_id,
                            dataset_id=fake_dataset_id,
                            name="ror",
                            sharded=True,
                            fields="id",
                        ),
                    ],
                    "scihub": [
                        Table(
                            project_id=self.project_id,
                            dataset_id=fake_dataset_id,
                            name="scihub",
                            sharded=True,
                            fields="doi",
                        ),
                    ],
                    "unpaywall": [
                        Table(
                            project_id=self.project_id,
                            dataset_id=fake_dataset_id,
                            name="unpaywall",
                            sharded=False,
                            fields="doi",
                        )
                    ],
                },
            )

            # Disable dag check on dag run sensor
            for sensor in workflow.operators[0]:
                sensor.check_exists = False
                sensor.grace_period = timedelta(seconds=1)

            dqc_dag = workflow.make_dag()

            # If there is no dag run for the DAG being monitored, the sensor will pass.  This is so we can
            # skip waiting on weeks when the DAG being waited on is not scheduled to run.
            with env.create_dag_run(dqc_dag, start_date):
                for task_id in DQCWorkflow.SENSOR_DAG_IDS:
                    ti = env.run_task(f"{task_id}_sensor")
                    self.assertEqual("success", ti.state)

            # Run Dummy Dags
            execution_date = pendulum.datetime(year=2023, month=1, day=1)
            snapshot_date = pendulum.datetime(year=2023, month=1, day=8)
            for dag_id in DQCWorkflow.SENSOR_DAG_IDS:
                dag = make_dummy_dag(dag_id, execution_date)
                with env.create_dag_run(dag, execution_date):
                    # Running all of a DAGs tasks sets the DAG to finished
                    ti = env.run_task("dummy_task")
                    self.assertEqual("success", ti.state)

            ### FIRST RUN ###

            # Creating the first instance of the observatory_dataset object means that there
            # should only be one record in the DQC table for each dataset

            # Run end to end tests for DQC DAG
            with env.create_dag_run(dqc_dag, execution_date):
                # Test that sensors go into 'success' state as the DAGs that they are waiting for have finished
                for task_id in DQCWorkflow.SENSOR_DAG_IDS:
                    ti = env.run_task(f"{task_id}_sensor")
                    self.assertEqual("success", ti.state)

                # Check dependencies
                ti = env.run_task(workflow.check_dependencies.__name__)
                self.assertEqual("success", ti.state)

                # Create dataset
                ti = env.run_task(workflow.create_dataset.__name__)
                self.assertEqual("success", ti.state)

                # Generate fake datasets for this test
                observatory_dataset = make_observatory_dataset(institutions, repositories)
                bq_load_observatory_dataset(
                    observatory_dataset,
                    repository,
                    env.download_bucket,
                    fake_dataset_id,
                    settings_dataset_id,
                    snapshot_date,
                    self.project_id,
                )

                # Check that at least one of the tables have been created
                expected_rows = len(observatory_dataset.papers)
                table_id = bq_table_id(self.project_id, fake_dataset_id, "works")
                self.assert_table_integrity(table_id, expected_rows=expected_rows)

                # Perform data quality check
                for task_id, _ in workflow.datasets.items():
                    ti = env.run_task(task_id)
                    self.assertEqual("success", ti.state)

                # Check that DQC tables have been created.
                for dataset, _ in workflow.datasets.items():
                    table_id = bq_table_id(self.project_id, dqc_dataset_id, dataset)
                    self.assert_table_integrity(table_id, expected_rows=1)

            ### SECOND RUN ###

            # Creating a new observatory_dataset object means that the faked data will be different than before.
            # There should be more DQC records added to the table.

            # Run Dummy Dags
            execution_date = pendulum.datetime(year=2023, month=2, day=1)
            snapshot_date = pendulum.datetime(year=2023, month=2, day=8)
            for dag_id in DQCWorkflow.SENSOR_DAG_IDS:
                dag = make_dummy_dag(dag_id, execution_date)
                with env.create_dag_run(dag, execution_date):
                    # Running all of a DAGs tasks sets the DAG to finished
                    ti = env.run_task("dummy_task")
                    self.assertEqual("success", ti.state)

            # Run end to end tests for DQC DAG
            with env.create_dag_run(dqc_dag, execution_date):
                # Test that sensors go into 'success' state as the DAGs that they are waiting for have finished
                for task_id in DQCWorkflow.SENSOR_DAG_IDS:
                    ti = env.run_task(f"{task_id}_sensor")
                    self.assertEqual("success", ti.state)

                # Check dependencies
                ti = env.run_task(workflow.check_dependencies.__name__)
                self.assertEqual("success", ti.state)

                # Create dataset
                ti = env.run_task(workflow.create_dataset.__name__)
                self.assertEqual("success", ti.state)

                # Need to delete tables that are being recreated by the bq_load_observatory_dataset function,
                # otherwise it complains that they already exist.
                for dataset_id in [fake_dataset_id, settings_dataset_id]:
                    bq_delete_old_datasets_with_prefix(prefix=dataset_id, age_to_delete=0)
                    bq_create_dataset(project_id=self.project_id, dataset_id=dataset_id, location=self.data_location)

                # Generate new fake datasets for this second run - should be different due to faker randomisation.
                observatory_dataset = make_observatory_dataset(institutions, repositories)
                bq_load_observatory_dataset(
                    observatory_dataset,
                    repository,
                    env.download_bucket,
                    fake_dataset_id,
                    settings_dataset_id,
                    snapshot_date,
                    self.project_id,
                )

                # Check that at least one of the tables have been created
                expected_rows = len(observatory_dataset.papers)
                table_id = bq_table_id(self.project_id, fake_dataset_id, "works")
                self.assert_table_integrity(table_id, expected_rows=expected_rows)

                # Perform data quality check
                for task_id, _ in workflow.datasets.items():
                    ti = env.run_task(task_id)
                    self.assertEqual("success", ti.state)

                # Check that DQC tables have been created.
                for dataset, _ in workflow.datasets.items():
                    table_id = bq_table_id(self.project_id, dqc_dataset_id, dataset)
                    print(f"Checking integrity of table: {table_id}")
                    self.assert_table_integrity(table_id, expected_rows=2)

            ### THIRD RUN ###

            # Using the same observatory_dataset object which will have the same faked table data.
            # No new records should be added to the DQC tables.

            # Run Dummy Dags
            execution_date = pendulum.datetime(year=2023, month=3, day=1)
            snapshot_date = pendulum.datetime(year=2023, month=3, day=8)
            for dag_id in DQCWorkflow.SENSOR_DAG_IDS:
                dag = make_dummy_dag(dag_id, execution_date)
                with env.create_dag_run(dag, execution_date):
                    # Running all of a DAGs tasks sets the DAG to finished
                    ti = env.run_task("dummy_task")
                    self.assertEqual("success", ti.state)

            # Run end to end tests for DQC DAG
            with env.create_dag_run(dqc_dag, execution_date):
                # Test that sensors go into 'success' state as the DAGs that they are waiting for have finished
                for task_id in DQCWorkflow.SENSOR_DAG_IDS:
                    ti = env.run_task(f"{task_id}_sensor")
                    self.assertEqual("success", ti.state)

                # Check dependencies
                ti = env.run_task(workflow.check_dependencies.__name__)
                self.assertEqual("success", ti.state)

                # Create dataset
                ti = env.run_task(workflow.create_dataset.__name__)
                self.assertEqual("success", ti.state)

                # Perform data quality check
                for task_id, _ in workflow.datasets.items():
                    ti = env.run_task(task_id)
                    self.assertEqual("success", ti.state)

                # Check that the DQC tables have no new records added for this third run.
                for dataset, _ in workflow.datasets.items():
                    table_id = bq_table_id(self.project_id, dqc_dataset_id, dataset)
                    self.assert_table_integrity(table_id, expected_rows=2)


class TestDataQualityCheckUtils(ObservatoryTestCase):
    def __init__(self, *args, **kwargs):
        super(TestDataQualityCheckUtils, self).__init__(*args, **kwargs)

        self.dag_id = "data_quality_checks"
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")

        self.schema_path = os.path.join(default_schema_folder(), "data_quality_check", "data_quality_check.json")

        # Can't use faker here because the number of bytes in a table is needed to be the same for each test run.
        self.test_table_hash = "771c9176e77c1b03f64b1b5fa4a39cdb"
        self.test_table = [
            dict(id="something", count="1", abstract_text="Hello"),
            dict(id="something", count="2", abstract_text="World"),
            dict(id="somethingelse", count="3", abstract_text="Science"),
            dict(id="somethingelse", count="4", abstract_text="Science"),
            dict(id=None, count="5", abstract_text="Maths"),
        ]
        self.test_table2 = [
            dict(id="something", count="1", abstract_text="Hello"),
            dict(id="other", count="2", abstract_text="World"),
            dict(id=None, count="3", abstract_text="History"),
        ]

        self.expected_dqc_record = dict(
            table_name="create_dqc_record",
            sharded=False,
            date_shard=None,
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

    def test_create_dqc_record(self):
        """Test if a dqc record can be reliably created."""

        env = ObservatoryEnvironment(self.project_id, self.data_location, api_port=find_free_port())
        bq_dataset_id = env.add_dataset()
        bq_table_id = "create_dqc_record"

        table_to_check = Table(
            project_id=self.project_id,
            dataset_id=bq_dataset_id,
            name=bq_table_id,
            sharded=False,
            fields="id",
        )

        with env.create(task_logging=True):
            full_table_id = f"{self.project_id}.{bq_dataset_id}.{bq_table_id}"

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

            dqc_record = create_dqc_record(
                hash_id=hash_id,
                full_table_id=full_table_id,
                fields=table_to_check.fields,
                is_sharded=table_to_check.sharded,
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
        bq_dataset_id = env.add_dataset()
        bq_table_id = "is_in_dqc_table"
        dag_id = "test_dag"

        table_to_check = Table(
            project_id=self.project_id,
            dataset_id=bq_dataset_id,
            name=bq_table_id,
            sharded=False,
            fields="id",
        )

        with env.create(task_logging=True):
            full_table_id = f"{self.project_id}.{bq_dataset_id}.{bq_table_id}"
            dqc_full_table_id = f"{self.project_id}.{dqc_dataset_id}.{dag_id}"

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
                create_dqc_record(
                    hash_id=hash_id,
                    full_table_id=full_table_id,
                    fields=table_to_check.fields,
                    is_sharded=table_to_check.sharded,
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

            # Ensure that hash is in the DQC table.
            result = is_in_dqc_table(hash_to_check=hash_id, dqc_full_table_id=dqc_full_table_id)
            self.assertTrue(result)

            # A random hash that we know shouldn't be in the DQC table.
            random_hash = random_id()
            result = is_in_dqc_table(hash_to_check=random_hash, dqc_full_table_id=dqc_full_table_id)
            self.assertFalse(result)

    def test_bq_get_table(self):
        """Test if a table can be reliably grabbed from the Bogquery API."""

        env = ObservatoryEnvironment(self.project_id, self.data_location, api_port=find_free_port())
        bq_dataset_id = env.add_dataset()
        bq_table_id = "get_table"

        with env.create(task_logging=True):
            full_table_id = f"{self.project_id}.{bq_dataset_id}.{bq_table_id}"

            # Load the test table from memory to Bigquery.
            success = bq_load_from_memory(table_id=full_table_id, records=self.test_table)
            self.assertTrue(success)

            # Get table object from Bigquery API
            table: BQTable = bq_get_table(full_table_id)

            # Make sure that metadata for the table is correct.
            self.assertTrue(table)
            self.assertEqual(table.num_rows, 5)
            self.assertEqual(table.table_id, bq_table_id)
            self.assertEqual(table.num_bytes, 131)

    def test_bq_list_tables_shards(self):
        """Test if a list of table shards can be reliably grabbed using the Bigquery API"""

        env = ObservatoryEnvironment(self.project_id, self.data_location, api_port=find_free_port())
        bq_dataset_id = env.add_dataset()
        base_name = "list_table_shards"
        bq_table_ids = ["list_table_shards20200101", "list_table_shards20200102"]

        with env.create(task_logging=True):
            # Load the test table from memory to Bigquery.
            for table_id, data in zip(bq_table_ids, [self.test_table, self.test_table2]):
                full_table_id = f"{self.project_id}.{bq_dataset_id}.{table_id}"
                success = bq_load_from_memory(table_id=full_table_id, records=data)
                self.assertTrue(success)

            # Get table object from Bigquery API
            tables: List[BQTable] = bq_list_tables_shards(dataset_id=bq_dataset_id, base_name=base_name)

            # Check metadata objects
            for table, data, bq_table_id in zip(tables, [self.test_table, self.test_table2], bq_table_ids):
                self.assertEqual(table.num_rows, len(data))
                self.assertEqual(table.table_id, bq_table_id)

    def test_bq_count_duplicate_records(self):
        """Test if a table can reliably check if duplucates exist in a table."""

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
