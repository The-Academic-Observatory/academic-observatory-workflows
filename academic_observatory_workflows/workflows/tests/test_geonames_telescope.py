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
from unittest.mock import patch

import pendulum
import vcr
from airflow.utils.state import State

from academic_observatory_workflows.config import test_fixtures_folder
from academic_observatory_workflows.workflows.geonames_telescope import (
    GeonamesTelescope,
    fetch_snapshot_date,
    GeonamesRelease,
)
from observatory.platform.api import get_dataset_releases
from observatory.platform.bigquery import bq_sharded_table_id
from observatory.platform.files import get_file_hash
from observatory.platform.gcs import gcs_blob_name_from_path
from observatory.platform.observatory_config import Workflow
from observatory.platform.observatory_environment import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    find_free_port,
    HttpServer,
)


class MockResponse:
    def __init__(self, headers):
        self.headers = headers


class TestGeonamesTelescope(ObservatoryTestCase):
    """Tests for the Geonames telescope"""

    def __init__(self, *args, **kwargs):
        super(TestGeonamesTelescope, self).__init__(*args, **kwargs)
        self.dag_id = "geonames"
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")
        self.mock_all_countries = test_fixtures_folder("geonames", "allCountries.zip")

    def test_dag_structure(self):
        """Test that the Geonames DAG has the correct structure."""

        dag = GeonamesTelescope(
            dag_id=self.dag_id,
            cloud_workspace=self.fake_cloud_workspace,
        ).make_dag()
        self.assert_dag_structure(
            {
                "check_dependencies": ["fetch_snapshot_date"],
                "fetch_snapshot_date": ["download"],
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
                    name="Geonames Telescope",
                    class_name="academic_observatory_workflows.workflows.geonames_telescope.GeonamesTelescope",
                    cloud_workspace=self.fake_cloud_workspace,
                )
            ]
        )

        with env.create():
            self.assert_dag_load_from_config(self.dag_id)

    @patch("academic_observatory_workflows.workflows.geonames_telescope.requests.head")
    def test_fetch_snapshot_date(self, m_req):
        """Test fetch_snapshot_date function."""

        m_req.return_value = MockResponse({"Last-Modified": "Thu, 16 Jul 2020 01:22:15 GMT"})

        date = fetch_snapshot_date()
        self.assertEqual(date, pendulum.datetime(year=2020, month=7, day=16, hour=1, minute=22, second=15))

    def test_telescope(self):
        """Test the Geonames telescope end to end."""

        env = ObservatoryEnvironment(self.project_id, self.data_location, api_port=find_free_port())
        bq_dataset_id = env.add_dataset()

        with env.create():
            workflow = GeonamesTelescope(
                dag_id=self.dag_id, bq_dataset_id=bq_dataset_id, cloud_workspace=env.cloud_workspace
            )
            dag = workflow.make_dag()
            execution_date = pendulum.datetime(year=2023, month=4, day=1)
            with env.create_dag_run(dag, execution_date) as dag_run:
                # Mocked and expected data
                snapshot_date = pendulum.datetime(2023, 4, 18)
                release = GeonamesRelease(dag_id=self.dag_id, run_id=dag_run.run_id, snapshot_date=snapshot_date)

                # Test that all dependencies are specified: no error should be thrown
                ti = env.run_task(workflow.check_dependencies.__name__)
                self.assertEqual(State.SUCCESS, ti.state)

                # Test list releases task
                with vcr.use_cassette(
                    test_fixtures_folder(self.dag_id, "fetch_snapshot_date.yaml"),
                    ignore_hosts=["oauth2.googleapis.com", "bigquery.googleapis.com"],
                    ignore_localhost=True,
                ):
                    ti = env.run_task(workflow.fetch_snapshot_date.__name__)
                    self.assertEqual(State.SUCCESS, ti.state)
                actual_snapshot_date = ti.xcom_pull(
                    key=GeonamesTelescope.RELEASE_INFO,
                    task_ids=workflow.fetch_snapshot_date.__name__,
                    include_prior_dates=False,
                )
                self.assertEqual(snapshot_date, pendulum.parse(actual_snapshot_date))

                # Test download task
                # Uses HttpServer rather than httpretty as asyncio seems to have trouble with httpretty
                server = HttpServer(test_fixtures_folder(self.dag_id))
                with server.create():
                    with patch(
                        "academic_observatory_workflows.workflows.geonames_telescope.DOWNLOAD_URL",
                        f"http://{server.host}:{server.port}/allCountries.zip",
                    ):
                        ti = env.run_task(workflow.download.__name__)
                        self.assertEqual(State.SUCCESS, ti.state)
                expected_file_hash = get_file_hash(file_path=self.mock_all_countries, algorithm="md5")
                self.assert_file_integrity(release.download_file_path, expected_file_hash, "md5")

                # Test that file uploaded
                ti = env.run_task(workflow.upload_downloaded.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                self.assert_blob_integrity(
                    env.download_bucket, gcs_blob_name_from_path(release.download_file_path), release.download_file_path
                )

                # Test that file extracted
                ti = env.run_task(workflow.extract.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                expected_file_hash = "de1bf005df4840d16faf598999d72051"
                self.assert_file_integrity(release.extract_file_path, expected_file_hash, "md5")

                # Test that file transformed
                ti = env.run_task(workflow.transform.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                expected_file_hash = "26c14e16"
                self.assert_file_integrity(release.transform_file_path, expected_file_hash, "gzip_crc")

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
                expected_rows = 50
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
