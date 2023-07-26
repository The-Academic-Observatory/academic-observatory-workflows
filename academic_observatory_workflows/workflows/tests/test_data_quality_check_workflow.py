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

from observatory.platform.api import get_dataset_releases
from observatory.platform.observatory_config import Workflow
from observatory.platform.workflows.workflow import ChangefileRelease
from observatory.platform.gcs import gcs_blob_name_from_path
from observatory.platform.observatory_environment import ObservatoryEnvironment, ObservatoryTestCase
from observatory.platform.bigquery import bq_run_query, bq_sharded_table_id
from observatory.platform.observatory_environment import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    find_free_port,
)

from academic_observatory_workflows.config import test_fixtures_folder
from academic_observatory_workflows.workflows.data_quality_check_workflow import DataQualityCheckWorkflow


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

        # Create range of fake datasets and tables

        # Perform checks on them

        # assert x number of rows

        # future runs will be n new tables - olds done before
