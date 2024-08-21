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

# Author: Aniek Roelofs, James Diprose

import os
from unittest.mock import patch

import pendulum
import vcr

from academic_observatory_workflows.config import project_path, TestConfig
from academic_observatory_workflows.crossref_fundref_telescope.telescope import create_dag, DagParams
from observatory_platform.airflow.workflow import Workflow
from observatory_platform.sandbox.sandbox_environment import SandboxEnvironment
from observatory_platform.sandbox.test_utils import SandboxTestCase

FIXTURES_FOLDER = project_path("crossref_fundref_telescope", "tests", "fixtures")


class TestCrossrefFundrefTelescope(SandboxTestCase):
    """Tests for the CrossrefFundref workflow"""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        super(TestCrossrefFundrefTelescope, self).__init__(*args, **kwargs)
        self.dag_id = "crossref_fundref"

    def test_dag_structure(self):
        """Test that the DAG has the correct structure."""

        # Mock create_pool to prevent querying non-existing airflow db
        with patch("academic_observatory_workflows.crossref_fundref_telescope.telescope.Pool"):
            dag = create_dag(DagParams(dag_id=self.dag_id, cloud_workspace=self.fake_cloud_workspace))
            self.assert_dag_structure(
                {
                    # fetch_release passes an XCom to all of these tasks
                    "fetch_releases": [
                        "process_release.download",
                        "process_release.upload_downloaded",
                        "process_release.extract",
                        "process_release.transform",
                        "process_release.upload_transformed",
                        "process_release.bq_load",
                        "process_release.add_dataset_releases",
                        "process_release.cleanup_workflow",
                    ],
                    "process_release.download": ["process_release.upload_downloaded"],
                    "process_release.upload_downloaded": ["process_release.extract"],
                    "process_release.extract": ["process_release.transform"],
                    "process_release.transform": ["process_release.upload_transformed"],
                    "process_release.upload_transformed": ["process_release.bq_load"],
                    "process_release.bq_load": ["process_release.add_dataset_releases"],
                    "process_release.add_dataset_releases": ["process_release.cleanup_workflow"],
                    "process_release.cleanup_workflow": [],
                },
                dag,
            )

    def test_dag_load(self):
        """Test that workflow can be loaded from a DAG bag."""

        env = SandboxEnvironment(
            workflows=[
                Workflow(
                    dag_id=self.dag_id,
                    name="Crossref Fundref Telescope",
                    class_name="academic_observatory_workflows.crossref_fundref_telescope.telescope",
                    cloud_workspace=self.fake_cloud_workspace,
                )
            ]
        )

        with env.create():
            dag_file = os.path.join(project_path(), "..", "..", "dags", "load_dags.py")
            self.assert_dag_load(self.dag_id, dag_file)

    def test_telescope(self):
        """Test the telescope end to end"""

        env = SandboxEnvironment(project_id=TestConfig.gcp_project_id, data_location=TestConfig.gcp_data_location)
        api_bq_dataset_id = env.add_dataset("crossref_fundref_api")
        bq_dataset_id = env.add_dataset("crossref_fundref")
        logical_date = pendulum.datetime(year=2021, month=5, day=16)

        with env.create(task_logging=True):
            test_params = DagParams(
                dag_id="test_crossref_fundref",
                cloud_workspace=env.cloud_workspace,
                retries=0,
                bq_dataset_id=bq_dataset_id,
                api_bq_dataset_id=api_bq_dataset_id,
            )

            cassette = vcr.use_cassette(
                os.path.join(FIXTURES_FOLDER, "fundref_e2e.yaml"),
                ignore_hosts=["oauth2.googleapis.com", "bigquery.googleapis.com", "storage.googleapis.com"],
                ignore_localhost=True,
                match_on=["uri", "method"],
            )
            with cassette:
                dagrun = create_dag(dag_params=test_params).test(execution_date=logical_date)
            if not dagrun.state == "success":
                raise RuntimeError("Dagrun did not complete successfully")
