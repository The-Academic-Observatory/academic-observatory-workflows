# Copyright 2021 Curtin University
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
from academic_observatory_workflows.workflows.ror_telescope import (
    RorRelease,
    RorTelescope,
    list_ror_records,
    transform_ror,
    is_lat_lng_valid,
)
from observatory.platform.api import get_dataset_releases
from observatory.platform.bigquery import bq_sharded_table_id
from observatory.platform.files import load_jsonl, list_files
from observatory.platform.gcs import gcs_blob_name_from_path
from observatory.platform.observatory_config import Workflow
from observatory.platform.observatory_environment import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    find_free_port,
    HttpServer,
)


class TestRorTelescope(ObservatoryTestCase):
    """Tests for the ROR telescope"""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        super(TestRorTelescope, self).__init__(*args, **kwargs)
        self.dag_id = "ror"
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")
        self.ror_conceptrecid = 6347574
        self.server_port = find_free_port()
        self.release_info = [
            {
                "snapshot_date": "20221020",
                "url": f"http://localhost:{self.server_port}/v1.11-2022-10-20-ror-data.zip",
                "checksum": "md5:0cac8705fba6df755648472356b7cb83",
            },
            {
                "snapshot_date": "20221017",
                "url": f"http://localhost:{self.server_port}/v1.10-2022-10-17-ror-data.zip",
                "checksum": "md5:60620675937e6513104275931331f68f",
            },
        ]
        self.mocked_files = [
            test_fixtures_folder(self.dag_id, "v1.11-2022-10-20-ror-data.zip"),
            test_fixtures_folder(self.dag_id, "v1.10-2022-10-17-ror-data.zip"),
        ]

    def test_dag_structure(self):
        """Test that the DAG has the correct structure."""

        workflow = RorTelescope(
            dag_id=self.dag_id,
            cloud_workspace=self.fake_cloud_workspace,
        )

        dag = workflow.make_dag()
        self.assert_dag_structure(
            {
                "check_dependencies": ["list_releases"],
                "list_releases": ["download"],
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
                    name="ROR Telescope",
                    class_name="academic_observatory_workflows.workflows.ror_telescope.RorTelescope",
                    cloud_workspace=self.fake_cloud_workspace,
                )
            ]
        )

        with env.create():
            self.assert_dag_load_from_config(self.dag_id)

    @patch("academic_observatory_workflows.workflows.ror_telescope.list_ror_records")
    def test_telescope(self, m_list_ror_records):
        """Test the ROR workflow end to end."""

        env = ObservatoryEnvironment(self.project_id, self.data_location, api_port=find_free_port())
        bq_dataset_id = env.add_dataset()

        # Create the Observatory environment and run tests
        with env.create():
            workflow = RorTelescope(
                dag_id=self.dag_id,
                cloud_workspace=env.cloud_workspace,
                bq_dataset_id=bq_dataset_id,
            )
            dag = workflow.make_dag()
            execution_date = pendulum.datetime(year=2022, month=10, day=16)

            with env.create_dag_run(dag, execution_date) as dag_run:
                # Mocked and expected data
                releases = [
                    RorRelease(
                        dag_id=self.dag_id,
                        run_id=dag_run.run_id,
                        snapshot_date=pendulum.parse(data["snapshot_date"]),
                        url=data["url"],
                        checksum=data["checksum"],
                    )
                    for data in self.release_info
                ]

                # Test that all dependencies are specified: no error should be thrown
                ti = env.run_task(workflow.check_dependencies.__name__)
                self.assertEqual(State.SUCCESS, ti.state)

                # Get mocked releases, list_ror_records is tested in its own function
                m_list_ror_records.return_value = self.release_info
                ti = env.run_task(workflow.list_releases.__name__)
                self.assertEqual(State.SUCCESS, ti.state)

                records = ti.xcom_pull(
                    key=RorTelescope.RELEASE_INFO,
                    task_ids=workflow.list_releases.__name__,
                    include_prior_dates=False,
                )
                self.assertListEqual(self.release_info, records)

                # Test download task
                server = HttpServer(test_fixtures_folder(self.dag_id), port=self.server_port)
                with server.create():
                    ti = env.run_task(workflow.download.__name__)
                    self.assertEqual(State.SUCCESS, ti.state)
                for release in releases:
                    md5 = release.checksum[4:]
                    self.assert_file_integrity(release.download_file_path, md5, "md5")

                # Test that file uploaded
                ti = env.run_task(workflow.upload_downloaded.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                for release in releases:
                    self.assert_blob_integrity(
                        env.download_bucket,
                        gcs_blob_name_from_path(release.download_file_path),
                        release.download_file_path,
                    )

                # Test that file extracted
                ti = env.run_task(workflow.extract.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                for release in releases:
                    self.assertEqual(1, len(list_files(release.extract_folder, release.extract_file_regex)))

                # Test that file transformed
                ti = env.run_task(workflow.transform.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                for release in releases:
                    self.assertTrue(os.path.isfile(release.transform_file_path))

                # Test that transformed file uploaded
                ti = env.run_task(workflow.upload_transformed.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                for release in releases:
                    self.assert_blob_integrity(
                        env.transform_bucket,
                        gcs_blob_name_from_path(release.transform_file_path),
                        release.transform_file_path,
                    )

                # Test that data loaded into BigQuery
                ti = env.run_task(workflow.bq_load.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                expected_content = load_jsonl(test_fixtures_folder("ror", "table_content.jsonl"))
                for release in releases:
                    table_id = bq_sharded_table_id(
                        self.project_id, workflow.bq_dataset_id, workflow.bq_table_name, release.snapshot_date
                    )
                    self.assert_table_content(table_id, expected_content, "id")

                # Test that DatasetRelease is added to database
                dataset_releases = get_dataset_releases(dag_id=self.dag_id, dataset_id=workflow.api_dataset_id)
                self.assertEqual(len(dataset_releases), 0)
                ti = env.run_task(workflow.add_new_dataset_releases.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                dataset_releases = get_dataset_releases(dag_id=self.dag_id, dataset_id=workflow.api_dataset_id)
                self.assertEqual(len(dataset_releases), 2)

                # Test that all workflow data deleted
                ti = env.run_task(workflow.cleanup.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                for release in releases:
                    self.assert_cleanup(release.workflow_folder)

    def test_list_ror_records(self):
        """Test that list_ror_records returns correct records"""

        # There are a number of releases near this range
        # 2023-04-12, 2023-03-30, 2023-03-16, 2023-02-28
        # It should just return 2023-03-30, 2023-03-16 as end_date is exclusive
        start_date = pendulum.datetime(2023, 3, 16)
        end_date = pendulum.datetime(2023, 4, 12)

        with vcr.use_cassette(test_fixtures_folder(self.dag_id, "list_ror_records.yaml")):
            records = list_ror_records(self.ror_conceptrecid, start_date, end_date)
            self.assertEqual(2, len(records))
            self.assertEqual(
                [
                    {
                        "snapshot_date": "20230330",
                        "url": "https://zenodo.org/api/records/7787102/files/v1.22-2023-03-30-ror-data.zip/content",
                        "checksum": "md5:019db485a7a88293851e91a0c4aa4206",
                    },
                    {
                        "snapshot_date": "20230316",
                        "url": "https://zenodo.org/api/records/7742581/files/v1.21-2023-03-16-ror-data.zip/content",
                        "checksum": "md5:2f24c6cf13186b4688ffa0f27abf2b5b",
                    },
                ],
                records,
            )

    def test_is_lat_lng_valid(self):
        """Test that lat lng valid"""

        self.assertTrue(is_lat_lng_valid(-90, -180))
        self.assertTrue(is_lat_lng_valid(90, 180))
        self.assertFalse(is_lat_lng_valid(90.1, 180.1))
        self.assertFalse(is_lat_lng_valid(-90.1, -180.1))

    def test_transform_ror(self):
        """Test that ROR transforms, i.e. invalid data is removed"""

        records = [
            {
                "id": "a",
                "addresses": [
                    {
                        "lat": 48.854692,
                        "lng": 2.33781,
                    }
                ],
            },
            {
                "id": "b",
                "addresses": [
                    {
                        "lat": 48.854692,
                        "lng": 233781,
                    }
                ],
            },
        ]
        records = transform_ror(records)
        self.assertEqual(
            records,
            [
                {
                    "id": "a",
                    "addresses": [
                        {
                            "lat": 48.854692,
                            "lng": 2.33781,
                        }
                    ],
                },
                {
                    "id": "b",
                    "addresses": [
                        {
                            "lat": None,
                            "lng": None,
                        }
                    ],
                },
            ],
        )
