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
import json
import gzip
import shutil
import hashlib
import logging
import pendulum
from ftplib import FTP
from typing import List, Dict
from click.testing import CliRunner
from airflow.utils.state import State
from datetime import datetime, timedelta


from academic_observatory_workflows.model import (
    Institution,
    bq_load_observatory_dataset,
    make_aggregate_table,
    make_doi_table,
    make_observatory_dataset,
    sort_events,
    Repository,
)

from observatory.platform.files import load_jsonl
from observatory.platform.api import get_dataset_releases
from observatory.platform.observatory_config import Workflow
from observatory.platform.workflows.workflow import ChangefileRelease
from observatory.platform.gcs import gcs_blob_name_from_path
from observatory.platform.observatory_environment import ObservatoryEnvironment, ObservatoryTestCase
from observatory.platform.bigquery import bq_run_query, bq_sharded_table_id, bq_table_id
from observatory.platform.observatory_environment import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    make_dummy_dag,
    find_free_port,
)

from academic_observatory_workflows.config import test_fixtures_folder
from academic_observatory_workflows.workflows.data_quality_check_workflow import (
    DataQualityCheckWorkflow,
    Table as DQCTable,
)
from academic_observatory_workflows.workflows.tests.test_doi_workflow import TestDoiWorkflow


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


class TestDataQualityCheckWorkflow(ObservatoryTestCase):
    """Tests for the Data Quality Check Workflow"""

    def __init__(self, *args, **kwargs):
        self.dag_id = "data_quality_check_workflow"
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")

        self.doi_fixtures = "doi"

        super(TestDataQualityCheckWorkflow, self).__init__(*args, **kwargs)

    def test_dag_load(self):
        """Test that the DataQualityCheck DAG can be loaded from a DAG bag."""

        env = ObservatoryEnvironment(
            workflows=[
                Workflow(
                    dag_id=self.dag_id,
                    name="Data Quality Check Workflow",
                    class_name="academic_observatory_workflows.workflows.data_quality_check_workflow.DataQualityCheckWorkflow",
                    cloud_workspace=self.fake_cloud_workspace,
                )
            ]
        )

        with env.create():
            self.assert_dag_load_from_config(self.dag_id)

    def test_dag_structure(self):
        """Test that the DAG has the correct structure."""

        workflow = DataQualityCheckWorkflow(
            dag_id=self.dag_id,
            cloud_workspace=self.fake_cloud_workspace,
        )
        dag = workflow.make_dag()
        self.assert_dag_structure(
            {
                "check_dependencies": ["create_dataset"],
                "create_dataset": ["pubmed", "openaire", "openalex", "doi_workflow"],
                "pubmed": [],
                "openaire": [],
                "openalex": [],
                "doi_workflow": [],
            },
            dag,
        )

    def test_telescope(self):
        """Test the PubMed Telescope end to end

        Borrowing off of the doi test structure."""

        env = ObservatoryEnvironment(self.project_id, self.data_location, api_port=find_free_port())

        bq_dataset_id = env.add_dataset()
        fake_dataset_id = env.add_dataset(prefix="fake")
        bq_settings_dataset_id = env.add_dataset(prefix="settings")

        repositories = TestDoiWorkflow().repositories
        institutions = TestDoiWorkflow().institutions

        # Create range of fake datasets and tables

        with env.create(task_logging=True):
            start_date = pendulum.datetime(year=2021, month=10, day=10)
            workflow = DataQualityCheckWorkflow(
                dag_id=self.dag_id,
                cloud_workspace=env.cloud_workspace,
                start_date=start_date,
            )

            dag_ids_to_check: List[DQCTable] = workflow.dag_ids_to_check

            # Disable dag check on dag run sensor
            for sensor in workflow.operators[0]:
                sensor.check_exists = False
                sensor.grace_period = timedelta(seconds=1)

            doi_dag = workflow.make_dag()

            # If there is no dag run for the DAG being monitored, the sensor will pass.  This is so we can
            # skip waiting on weeks when the DAG being waited on is not scheduled to run.
            expected_state = "success"
            with env.create_dag_run(doi_dag, start_date):
                for task_id in DataQualityCheckWorkflow.SENSOR_DAG_IDS:
                    ti = env.run_task(f"{task_id}_sensor")
                    self.assertEqual(expected_state, ti.state)

            # Run Dummy Dags
            execution_date = pendulum.datetime(year=2023, month=1, day=1)
            snapshot_date = pendulum.datetime(year=2023, month=1, day=8)
            expected_state = "success"
            for dag_id in DataQualityCheckWorkflow.SENSOR_DAG_IDS:
                dag = make_dummy_dag(dag_id, execution_date)
                with env.create_dag_run(dag, execution_date):
                    # Running all of a DAGs tasks sets the DAG to finished
                    ti = env.run_task("dummy_task")
                    self.assertEqual(expected_state, ti.state)

            # Run end to end tests for DQC DAG
            with env.create_dag_run(doi_dag, execution_date):
                # Test that sensors go into 'success' state as the DAGs that they are waiting for have finished
                for task_id in DataQualityCheckWorkflow.SENSOR_DAG_IDS:
                    ti = env.run_task(f"{task_id}_sensor")
                    self.assertEqual(expected_state, ti.state)

                # Generate fake dataset
                repository = load_jsonl(test_fixtures_folder(self.doi_fixtures, "repository.jsonl"))
                observatory_dataset = make_observatory_dataset(institutions, repositories)
                bq_load_observatory_dataset(
                    observatory_dataset,
                    repository,
                    env.download_bucket,
                    fake_dataset_id,
                    bq_settings_dataset_id,
                    snapshot_date,
                    self.project_id,
                )

                # Check openalex table created
                expected_rows = len(observatory_dataset.papers)
                table_id = bq_table_id(self.project_id, fake_dataset_id, "works")
                self.assert_table_integrity(table_id, expected_rows=expected_rows)

        # assert x number of rows

        # future runs will be n new tables - olds done before
