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
import re
from typing import List
from unittest.mock import patch

import pendulum
import responses
import vcr
from airflow.utils.state import State

from academic_observatory_workflows.config import project_path
from academic_observatory_workflows.crossref_fundref_telescope.crossref_fundref_telescope import (
    create_dag,
    CrossrefFundrefRelease,
    list_releases,
)
from observatory_platform.dataset_api import get_dataset_releases
from observatory_platform.google.bigquery import bq_sharded_table_id
from observatory_platform.config import module_file_path
from observatory_platform.google.gcs import gcs_blob_name_from_path
from observatory_platform.airflow.workflow import Workflow
from observatory_platform.sandbox.sandbox_environment import find_free_port, ObservatoryEnvironment, ObservatoryTestCase

FIXTURES_FOLDER = project_path("crossref_fundref_telescope", "tests", "fixtures")


class TestCrossrefFundrefTelescope(ObservatoryTestCase):
    """Tests for the CrossrefFundref workflow"""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        super(TestCrossrefFundrefTelescope, self).__init__(*args, **kwargs)
        self.dag_id = "crossref_fundref"
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")
        self.mock_download_hash = "0cd65042"
        self.mock_extract_hash = "559aa89d41a85ff84d705084c1caeb8d"
        self.mock_transform_hash = "632b453a"

    def test_dag_structure(self):
        """Test that the DAG has the correct structure."""

        # Mock create_pool to prevent querying non-existing airflow db
        with patch("academic_observatory_workflows.crossref_fundref_telescope.crossref_fundref_telescope.Pool"):
            dag = create_dag(
                dag_id=self.dag_id,
                cloud_workspace=self.fake_cloud_workspace,
            )
            self.assert_dag_structure(
                {
                    "check_dependencies": ["fetch_releases"],
                    # fetch_release passes an XCom to all of these tasks
                    "fetch_releases": [
                        "process_release.download",
                        "process_release.transform",
                        "process_release.bq_load",
                        "process_release.add_dataset_releases",
                        "process_release.cleanup",
                    ],
                    "process_release.download": ["process_release.transform"],
                    "process_release.transform": ["process_release.bq_load"],
                    "process_release.bq_load": ["process_release.add_dataset_releases"],
                    "process_release.add_dataset_releases": ["process_release.cleanup"],
                    "process_release.cleanup": [],
                },
                dag,
            )

    def test_dag_load(self):
        """Test that workflow can be loaded from a DAG bag."""

        env = ObservatoryEnvironment(
            workflows=[
                Workflow(
                    dag_id=self.dag_id,
                    name="Crossref Fundref Telescope",
                    class_name="academic_observatory_workflows.crossref_fundref_telescope.crossref_fundref_telescope.create_dag",
                    cloud_workspace=self.fake_cloud_workspace,
                )
            ]
        )

        with env.create():
            dag_file = os.path.join(module_file_path("observatory.platform.dags"), "load_dags.py")
            self.assert_dag_load(self.dag_id, dag_file)

    def test_telescope(self):
        """Test the CrossrefFundref workflow end to end."""

        env = ObservatoryEnvironment(self.project_id, self.data_location, api_port=find_free_port())
        bq_dataset_id = env.add_dataset()
        bq_table_name = "crossref_fundref"
        api_dataset_id = "crossref_fundref"

        with env.create():
            # Setup workflow inside env, so pool can be created
            dag = create_dag(
                dag_id=self.dag_id,
                cloud_workspace=env.cloud_workspace,
                bq_dataset_id=bq_dataset_id,
                bq_table_name=bq_table_name,
                api_dataset_id=api_dataset_id,
            )
            logical_date = pendulum.datetime(year=2021, month=5, day=16)

            with env.create_dag_run(dag, logical_date) as dag_run:
                # Mocked and expected data
                snapshot_date = pendulum.datetime(2021, 5, 19)
                url = (
                    "https://gitlab.com/crossref/open_funder_registry/-/archive/v1.34/open_funder_registry-v1.34.tar.gz"
                )
                release = CrossrefFundrefRelease(
                    dag_id=self.dag_id,
                    run_id=dag_run.run_id,
                    snapshot_date=snapshot_date,
                    url=url,
                    cloud_workspace=env.cloud_workspace,
                )

                # Test that all dependencies are specified: no error should be thrown
                ti = env.run_task("check_dependencies")
                self.assertEqual(State.SUCCESS, ti.state)

                # Test fetch releases task
                with vcr.use_cassette(
                    os.path.join(FIXTURES_FOLDER, "get_release_info.yaml"),
                    ignore_hosts=["oauth2.googleapis.com", "bigquery.googleapis.com"],
                    ignore_localhost=True,
                ):
                    ti = env.run_task("fetch_releases")
                    self.assertEqual(State.SUCCESS, ti.state)

                actual_release_info = ti.xcom_pull(
                    key="return_value",
                    task_ids="fetch_releases",
                    include_prior_dates=False,
                )
                self.assertEqual([release.to_dict()], actual_release_info)

                # Test download task
                with responses.RequestsMock() as rs:
                    mock_file_path = os.path.join(FIXTURES_FOLDER, "crossref_fundref_v1.34.tar.gz")
                    with open(mock_file_path, "rb") as f:
                        mock_data = f.read()
                    rs.add(responses.GET, url, body=mock_data, status=200)
                    rs.add_passthru(re.compile(r"https?://.*google(com|apis)\.com/.*"))
                    ti = env.run_task("process_release.download", map_index=0)

                self.assertEqual(State.SUCCESS, ti.state)
                self.assert_file_integrity(release.download_file_path, self.mock_download_hash, "gzip_crc")
                self.assert_blob_integrity(
                    env.download_bucket, gcs_blob_name_from_path(release.download_file_path), release.download_file_path
                )

                # Test that file extracted, transformed and uploaded
                ti = env.run_task("process_release.transform", map_index=0)
                self.assertEqual(State.SUCCESS, ti.state)
                self.assert_file_integrity(release.extract_file_path, self.mock_extract_hash, "md5")
                self.assert_file_integrity(release.transform_file_path, self.mock_transform_hash, "gzip_crc")
                self.assert_blob_integrity(
                    env.transform_bucket,
                    gcs_blob_name_from_path(release.transform_file_path),
                    release.transform_file_path,
                )

                # Test that data loaded into BigQuery
                ti = env.run_task("process_release.bq_load", map_index=0)
                self.assertEqual(State.SUCCESS, ti.state)
                table_id = bq_sharded_table_id(self.project_id, bq_dataset_id, bq_table_name, release.snapshot_date)
                expected_rows = 27949
                self.assert_table_integrity(table_id, expected_rows)

                # Test that DatasetRelease is added to database
                dataset_releases = get_dataset_releases(dag_id=self.dag_id, dataset_id=api_dataset_id)
                self.assertEqual(len(dataset_releases), 0)
                ti = env.run_task("process_release.add_dataset_releases", map_index=0)
                self.assertEqual(State.SUCCESS, ti.state)
                dataset_releases = get_dataset_releases(dag_id=self.dag_id, dataset_id=api_dataset_id)
                self.assertEqual(len(dataset_releases), 1)

                # Test that all workflow data deleted
                ti = env.run_task("process_release.cleanup", map_index=0)
                self.assertEqual(State.SUCCESS, ti.state)
                self.assert_cleanup(release.workflow_folder)

    def test_list_releases(self):
        """Test that list releases returns a list with dictionaries of release info.

        :return: None.
        """

        cassette_path = os.path.join(FIXTURES_FOLDER, "list_fundref_releases.yaml")
        with vcr.use_cassette(cassette_path):
            releases = list_releases(pendulum.datetime(2014, 3, 1), pendulum.datetime(2020, 6, 1))
            self.assertIsInstance(releases, List)
            self.assertEqual(39, len(releases))
            for release in releases:
                self.assertIsInstance(release, dict)
                self.assertIsInstance(release["url"], str)
                self.assertIsInstance(pendulum.parse(release["snapshot_date"]), pendulum.DateTime)
