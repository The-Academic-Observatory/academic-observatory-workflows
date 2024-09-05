# Copyright 2020 Curtin University
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

# Author: Tuan Chien, James Diprose

import datetime
import os
from unittest.mock import patch

import pendulum
from airflow.models import Connection
from airflow.utils.state import State

from academic_observatory_workflows.config import project_path, TestConfig
from academic_observatory_workflows.scopus_telescope.telescope import create_dag, DagParams
from academic_observatory_workflows.scopus_telescope.release import ScopusRelease
from observatory_platform.dataset_api import DatasetAPI
from observatory_platform.google.bigquery import bq_sharded_table_id
from observatory_platform.files import list_files
from observatory_platform.google.gcs import gcs_blob_name_from_path
from observatory_platform.airflow.workflow import Workflow
from observatory_platform.airflow.airflow import upsert_airflow_connection, clear_airflow_connections
from observatory_platform.sandbox.sandbox_environment import SandboxEnvironment
from observatory_platform.sandbox.test_utils import SandboxTestCase

FIXTURES_FOLDER = project_path("scopus_telescope", "tests", "fixtures")


class TestScopusTelescope(SandboxTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.maxDiff = None  # so that entire diff from assertions are compared and returned
        self.dag_id = "scopus_curtin"

    def test_dag_structure(self):
        """Test that the DAG has the correct structure."""

        dag_params = DagParams(
            dag_id=self.dag_id,
            cloud_workspace=self.fake_cloud_workspace,
            institution_ids=["10"],
            scopus_conn_ids=["conn"],
        )
        dag = create_dag(dag_params)
        self.assert_dag_structure(
            {
                "check_dependencies": ["fetch_release"],
                "fetch_release": [
                    "download",
                    "transform",
                    "bq_load",
                    "add_dataset_release",
                    "cleanup_workflow",
                ],
                "download": ["transform"],
                "transform": ["bq_load"],
                "bq_load": ["add_dataset_release"],
                "add_dataset_release": ["cleanup_workflow"],
                "cleanup_workflow": [],
            },
            dag,
        )

    def test_dag_load(self):
        """Test that workflow can be loaded from a DAG bag."""

        # Success
        env = SandboxEnvironment(
            workflows=[
                Workflow(
                    dag_id=self.dag_id,
                    name="Scopus Telescope Curtin University",
                    class_name="academic_observatory_workflows.scopus_telescope.telescope",
                    cloud_workspace=self.fake_cloud_workspace,
                    kwargs=dict(
                        institution_ids=["10"],
                        scopus_conn_ids=["conn"],
                        earliest_date=pendulum.datetime(2021, 1, 1),
                    ),
                )
            ]
        )

        with env.create():
            dag_file = os.path.join(project_path(), "..", "..", "dags", "load_dags.py")
            self.assert_dag_load(self.dag_id, dag_file)

        # Failure to load caused by missing kwargs
        env = SandboxEnvironment(
            workflows=[
                Workflow(
                    dag_id=self.dag_id,
                    name="Scopus Telescope Curtin University",
                    class_name="academic_observatory_workflows.scopus_telescope.telescope",
                    cloud_workspace=self.fake_cloud_workspace,
                    kwargs=dict(),
                )
            ]
        )

        with env.create():
            with self.assertRaises(AssertionError) as cm:
                dag_file = os.path.join(project_path(), "..", "..", "dags", "load_dags.py")
                self.assert_dag_load(self.dag_id, dag_file)
            msg = cm.exception.args[0]
            self.assertTrue("missing 2 required keyword-only arguments" in msg)
            self.assertTrue("institution_ids" in msg)
            self.assertTrue("scopus_conn_ids" in msg)

    @patch("academic_observatory_workflows.scopus_telescope.tasks.ScopusUtility.make_query")
    def test_telescope(self, m_search):
        """Test workflow end to end"""

        env = SandboxEnvironment(project_id=TestConfig.gcp_project_id, data_location=TestConfig.gcp_data_location)
        bq_dataset_id = env.add_dataset("scopus")
        bq_table_name = "scopus"
        api_dataset_id = env.add_dataset("dataset_api")

        # Mock the download data
        fixture_file = project_path(FIXTURES_FOLDER, "test.json")
        with open(fixture_file, "r") as f:
            results_str = f.read()
        results_len = 1
        m_search.return_value = results_str, results_len

        with env.create():
            # Add login/pass connection
            clear_airflow_connections()
            conn_id = "scopus_curtin_university"
            upsert_airflow_connection(
                conn_id=conn_id, conn_type="http", host="http://login:password@localhost", password="foo"
            )

            logical_date = pendulum.datetime(2021, 1, 1)
            test_params = DagParams(
                dag_id=self.dag_id,
                cloud_workspace=env.cloud_workspace,
                institution_ids=["123"],
                scopus_conn_ids=[conn_id],
                bq_dataset_id=bq_dataset_id,
                bq_table_name=bq_table_name,
                api_dataset_id=api_dataset_id,
                earliest_date=logical_date,
                retries=0,
            )
            dag = create_dag(test_params)
            dagrun = dag.test(execution_date=logical_date)

            # Make assertions
            if not dagrun.state == "success":
                raise RuntimeError("Frist Dagrun did not complete successfully")

            # bq_load
            table_id = bq_sharded_table_id(
                test_params.cloud_workspace.project_id, bq_dataset_id, bq_table_name, pendulum.date(2021, 1, 1)
            )
            self.assert_table_integrity(table_id, 1)
            self.assert_table_content(
                table_id,
                [
                    {
                        "snapshot_date": datetime.date(2021, 2, 1),
                        "institution_ids": [123],
                        "title": "Article title",
                        "identifier": "SCOPUS_ID:000000",
                        "creator": "Name F.",
                        "publication_name": "Journal of Things",
                        "cover_date": datetime.date(2021, 10, 31),
                        "doi": ["10.0000/00"],
                        "eissn": [],
                        "issn": ["00000000"],
                        "isbn": [],
                        "aggregation_type": "Journal",
                        "pubmed_id": None,
                        "pii": "S00000",
                        "eid": "somedoi",
                        "subtype_description": "Article",
                        "open_access": 0,
                        "open_access_flag": False,
                        "citedby_count": 0,
                        "source_id": 1,
                        "affiliations": [
                            {
                                "name": "WA School of Things",
                                "city": "Kalgoorlie",
                                "country": "Australia",
                                "id": None,
                                "name_variant": None,
                            }
                        ],
                        "orcid": None,
                        "authors": [],
                        "abstract": None,
                        "keywords": [],
                        "article_number": "1",
                        "fund_agency_ac": None,
                        "fund_agency_id": None,
                        "fund_agency_name": None,
                    }
                ],
                "identifier",
            )

            # Test that DatasetRelease is added to database
            api = DatasetAPI(
                bq_project_id=test_params.cloud_workspace.project_id, bq_dataset_id=test_params.api_bq_dataset_id
            )
            api.seed_db()
            dataset_releases = api.get_dataset_releases(dag_id=test_params.dag_id, entity_id="scopus")
            self.assertEqual(len(dataset_releases), 1)

    def test_telescope_old(self):
        """Test workflow end to end"""

        env = SandboxEnvironment(self.project_id, self.data_location)
        bq_dataset_id = env.add_dataset("scopus")
        bq_table_name = "scopus"
        api_dataset_id = env.add_dataset("dataset_api")

        with env.create(task_logging=True):
            # Add login/pass connection
            conn_id = "scopus_curtin_university"
            conn = Connection(conn_id=conn_id, uri=f"http://login:password@localhost")
            env.add_connection(conn)

            logical_date = pendulum.datetime(2021, 1, 1)
            dag = create_dag(
                dag_id=self.dag_id,
                cloud_workspace=env.cloud_workspace,
                institution_ids=["123"],
                scopus_conn_ids=[conn_id],
                bq_dataset_id=bq_dataset_id,
                bq_table_name=bq_table_name,
                api_dataset_id=api_dataset_id,
                earliest_date=logical_date,
            )

            with env.create_dag_run(dag, logical_date) as dag_run:
                snapshot_date = pendulum.datetime(2021, 2, 1)
                release = ScopusRelease(
                    dag_id=self.dag_id,
                    run_id=dag_run.run_id,
                    snapshot_date=snapshot_date,
                )

                # Check dependencies
                ti = env.run_task("check_dependencies")
                self.assertEqual(State.SUCCESS, ti.state)

                # Fetch release
                ti = env.run_task("fetch_release")
                self.assertEqual(State.SUCCESS, ti.state)

                # Download
                with patch(
                    "academic_observatory_workflows.scopus_telescope.scopus_telescope.ScopusUtility.make_query"
                ) as m_search:
                    # Load mocked data
                    fixture_file = project_path(FIXTURES_FOLDER, "test.json")
                    with open(fixture_file, "r") as f:
                        results_str = f.read()
                    results_len = 1
                    m_search.return_value = results_str, results_len
                    ti = env.run_task("download")
                self.assertEqual(State.SUCCESS, ti.state)
                download_files = list_files(release.download_folder, release.download_file_regex)
                self.assertEqual(1, len(download_files))
                self.assertEqual(1, m_search.call_count)

                # Assert upload downloaded
                for file_path in download_files:
                    self.assert_blob_integrity(env.download_bucket, gcs_blob_name_from_path(file_path), file_path)

                # Transform
                ti = env.run_task("transform")
                self.assertEqual(State.SUCCESS, ti.state)
                self.assertTrue(os.path.isfile(release.transform_file_path))

                # Upload transformed
                self.assert_blob_integrity(
                    env.transform_bucket,
                    gcs_blob_name_from_path(release.transform_file_path),
                    release.transform_file_path,
                )

                # bq_load
                ti = env.run_task("bq_load")
                self.assertEqual(State.SUCCESS, ti.state)
                table_id = bq_sharded_table_id(self.project_id, bq_dataset_id, bq_table_name, release.snapshot_date)
                expected_rows = 1
                self.assert_table_integrity(table_id, expected_rows)
                self.assert_table_content(
                    table_id,
                    [
                        {
                            "snapshot_date": datetime.date(2021, 2, 1),
                            "institution_ids": [123],
                            "title": "Article title",
                            "identifier": "SCOPUS_ID:000000",
                            "creator": "Name F.",
                            "publication_name": "Journal of Things",
                            "cover_date": datetime.date(2021, 10, 31),
                            "doi": ["10.0000/00"],
                            "eissn": [],
                            "issn": ["00000000"],
                            "isbn": [],
                            "aggregation_type": "Journal",
                            "pubmed_id": None,
                            "pii": "S00000",
                            "eid": "somedoi",
                            "subtype_description": "Article",
                            "open_access": 0,
                            "open_access_flag": False,
                            "citedby_count": 0,
                            "source_id": 1,
                            "affiliations": [
                                {
                                    "name": "WA School of Things",
                                    "city": "Kalgoorlie",
                                    "country": "Australia",
                                    "id": None,
                                    "name_variant": None,
                                }
                            ],
                            "orcid": None,
                            "authors": [],
                            "abstract": None,
                            "keywords": [],
                            "article_number": "1",
                            "fund_agency_ac": None,
                            "fund_agency_id": None,
                            "fund_agency_name": None,
                        }
                    ],
                    "identifier",
                )

                # Test that DatasetRelease is added to database
                dataset_releases = get_dataset_releases(dag_id=self.dag_id, dataset_id=api_dataset_id)
                self.assertEqual(len(dataset_releases), 0)
                ti = env.run_task("add_dataset_release")
                self.assertEqual(State.SUCCESS, ti.state)
                dataset_releases = get_dataset_releases(dag_id=self.dag_id, dataset_id=api_dataset_id)
                self.assertEqual(len(dataset_releases), 1)

                # Test that all workflow data deleted
                ti = env.run_task("cleanup_workflow")
                self.assertEqual(State.SUCCESS, ti.state)
                self.assert_cleanup(release.workflow_folder)
