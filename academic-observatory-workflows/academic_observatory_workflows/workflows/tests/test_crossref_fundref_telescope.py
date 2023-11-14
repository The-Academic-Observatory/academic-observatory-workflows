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

# Author: Aniek Roelofs, James Diprose

import os
from typing import List
from unittest.mock import patch

import httpretty
import pendulum
import vcr
from airflow.utils.state import State

from academic_observatory_workflows.config import test_fixtures_folder
from academic_observatory_workflows.workflows.crossref_fundref_telescope import (
    CrossrefFundrefRelease,
    CrossrefFundrefTelescope,
    list_releases,
)
from observatory.platform.api import get_dataset_releases
from observatory.platform.bigquery import bq_sharded_table_id
from observatory.platform.gcs import gcs_blob_name_from_path
from observatory.platform.observatory_config import Workflow
from observatory.platform.observatory_environment import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    find_free_port,
)


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
        with patch("academic_observatory_workflows.workflows.crossref_fundref_telescope.create_pool"):
            workflow = CrossrefFundrefTelescope(
                dag_id=self.dag_id,
                cloud_workspace=self.fake_cloud_workspace,
            )

            dag = workflow.make_dag()
            self.assert_dag_structure(
                {
                    "check_dependencies": ["get_release_info"],
                    "get_release_info": ["download"],
                    "download": ["upload_downloaded"],
                    "upload_downloaded": ["extract"],
                    "extract": ["transform"],
                    "transform": ["upload_transformed"],
                    "upload_transformed": ["bq_load"],
                    "bq_load": ["add_new_dataset_releases"],
                    "add_new_dataset_releases": ["cleanup"],
                    "cleanup": [],
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
                    class_name="academic_observatory_workflows.workflows.crossref_fundref_telescope.CrossrefFundrefTelescope",
                    cloud_workspace=self.fake_cloud_workspace,
                )
            ]
        )

        with env.create():
            self.assert_dag_load_from_config(self.dag_id)

    def test_telescope(self):
        """Test the CrossrefFundref workflow end to end."""

        env = ObservatoryEnvironment(self.project_id, self.data_location, api_port=find_free_port())
        bq_dataset_id = env.add_dataset()

        with env.create():
            # Setup workflow inside env, so pool can be created
            workflow = CrossrefFundrefTelescope(
                dag_id=self.dag_id,
                cloud_workspace=env.cloud_workspace,
                bq_dataset_id=bq_dataset_id,
            )
            dag = workflow.make_dag()
            execution_date = pendulum.datetime(year=2021, month=5, day=16)

            with env.create_dag_run(dag, execution_date) as dag_run:
                # Mocked and expected data
                snapshot_date = pendulum.datetime(2021, 5, 19)
                url = (
                    "https://gitlab.com/crossref/open_funder_registry/-/archive/v1.34/open_funder_registry-v1.34.tar.gz"
                )
                release = CrossrefFundrefRelease(
                    dag_id=self.dag_id, run_id=dag_run.run_id, snapshot_date=snapshot_date, url=url
                )
                expected_release_info = [{"date": "20210519", "url": url}]

                # Test that all dependencies are specified: no error should be thrown
                ti = env.run_task(workflow.check_dependencies.__name__)
                self.assertEqual(State.SUCCESS, ti.state)

                # Test list releases task
                with vcr.use_cassette(
                    test_fixtures_folder(self.dag_id, "get_release_info.yaml"),
                    ignore_hosts=["oauth2.googleapis.com", "bigquery.googleapis.com"],
                    ignore_localhost=True,
                ):
                    ti = env.run_task(workflow.get_release_info.__name__)
                    self.assertEqual(State.SUCCESS, ti.state)

                actual_release_info = ti.xcom_pull(
                    key=CrossrefFundrefTelescope.RELEASE_INFO,
                    task_ids=workflow.get_release_info.__name__,
                    include_prior_dates=False,
                )
                self.assertEqual(expected_release_info, actual_release_info)

                # Test download task
                with httpretty.enabled():
                    mock_file_path = test_fixtures_folder(self.dag_id, "crossref_fundref_v1.34.tar.gz")
                    self.setup_mock_file_download(url, mock_file_path)
                    ti = env.run_task(workflow.download.__name__)
                    self.assertEqual(State.SUCCESS, ti.state)
                self.assert_file_integrity(release.download_file_path, self.mock_download_hash, "gzip_crc")

                # Test that file uploaded
                ti = env.run_task(workflow.upload_downloaded.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                self.assert_blob_integrity(
                    env.download_bucket, gcs_blob_name_from_path(release.download_file_path), release.download_file_path
                )

                # Test that file extracted
                ti = env.run_task(workflow.extract.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                self.assert_file_integrity(release.extract_file_path, self.mock_extract_hash, "md5")

                # Test that file transformed
                ti = env.run_task(workflow.transform.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                self.assert_file_integrity(release.transform_file_path, self.mock_transform_hash, "gzip_crc")

                # Test that transformed file uploaded
                ti = env.run_task(workflow.upload_transformed.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                self.assert_blob_integrity(
                    env.transform_bucket,
                    gcs_blob_name_from_path(release.transform_file_path),
                    release.transform_file_path,
                )

                # Test that data loaded into BigQuery
                ti = env.run_task(workflow.bq_load.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                table_id = bq_sharded_table_id(
                    self.project_id, workflow.bq_dataset_id, workflow.bq_table_name, release.snapshot_date
                )
                expected_rows = 27949
                self.assert_table_integrity(table_id, expected_rows)

                # Test that DatasetRelease is added to database
                dataset_releases = get_dataset_releases(dag_id=self.dag_id, dataset_id=workflow.api_dataset_id)
                self.assertEqual(len(dataset_releases), 0)
                ti = env.run_task(workflow.add_new_dataset_releases.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                dataset_releases = get_dataset_releases(dag_id=self.dag_id, dataset_id=workflow.api_dataset_id)
                self.assertEqual(len(dataset_releases), 1)

                # Test that all workflow data deleted
                ti = env.run_task(workflow.cleanup.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                self.assert_cleanup(release.workflow_folder)

    def test_list_releases(self):
        """Test that list releases returns a list with dictionaries of release info.

        :return: None.
        """
        cassette_path = test_fixtures_folder("crossref_fundref", "list_fundref_releases.yaml")
        with vcr.use_cassette(cassette_path):
            releases = list_releases(pendulum.datetime(2014, 3, 1), pendulum.datetime(2020, 6, 1))
            self.assertIsInstance(releases, List)
            self.assertEqual(39, len(releases))
            for release in releases:
                self.assertIsInstance(release, dict)
                self.assertIsInstance(release["url"], str)
                self.assertIsInstance(pendulum.parse(release["date"]), pendulum.DateTime)
