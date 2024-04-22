# Copyright 2021-2024 Curtin University
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

import logging
import os
from unittest.mock import patch

import pendulum
import vcr
from airflow.utils.state import State

from academic_observatory_workflows.config import project_path
from academic_observatory_workflows.ror_telescope.ror_telescope import (
    create_dag,
    is_lat_lng_valid,
    list_ror_records,
    RorRelease,
    transform_ror,
)
from observatory.platform.api import get_dataset_releases
from observatory.platform.bigquery import bq_sharded_table_id
from observatory.platform.config import module_file_path
from observatory.platform.files import list_files, load_jsonl
from observatory.platform.gcs import gcs_blob_name_from_path
from observatory.platform.observatory_config import Workflow
from observatory.platform.observatory_environment import (
    find_free_port,
    HttpServer,
    ObservatoryEnvironment,
    ObservatoryTestCase,
)

FIXTURES_FOLDER = project_path("ror_telescope", "tests", "fixtures")


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
        self.mocked_files = [
            os.path.join(FIXTURES_FOLDER, "v1.11-2022-10-20-ror-data.zip"),
            os.path.join(FIXTURES_FOLDER, "v1.10-2022-10-17-ror-data.zip"),
        ]

    def test_dag_structure(self):
        """Test that the DAG has the correct structure."""

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
                    "process_release.cleanup_workflow",
                ],
                "process_release.download": ["process_release.transform"],
                "process_release.transform": ["process_release.bq_load"],
                "process_release.bq_load": ["process_release.add_dataset_releases"],
                "process_release.add_dataset_releases": ["process_release.cleanup_workflow"],
                "process_release.cleanup_workflow": [],
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
                    class_name="academic_observatory_workflows.ror_telescope.ror_telescope.create_dag",
                    cloud_workspace=self.fake_cloud_workspace,
                )
            ]
        )

        with env.create():
            dag_file = os.path.join(module_file_path("observatory.platform.dags"), "load_dags.py")
            self.assert_dag_load(self.dag_id, dag_file)

    @patch("academic_observatory_workflows.ror_telescope.ror_telescope.list_ror_records")
    def test_telescope(self, m_list_ror_records):
        """Test the ROR workflow end to end."""

        env = ObservatoryEnvironment(self.project_id, self.data_location, api_port=find_free_port())
        bq_dataset_id = env.add_dataset()

        # Create the Observatory environment and run tests
        with env.create():
            logical_date = pendulum.datetime(year=2022, month=10, day=16)
            bq_table_name = "ror"
            api_dataset_id = "ror"
            dag = create_dag(
                dag_id=self.dag_id,
                cloud_workspace=env.cloud_workspace,
                bq_dataset_id=bq_dataset_id,
                bq_table_name=bq_table_name,
                api_dataset_id=api_dataset_id,
            )

            with env.create_dag_run(dag, logical_date) as dag_run:
                # Test that all dependencies are specified: no error should be thrown
                ti = env.run_task("check_dependencies")
                self.assertEqual(State.SUCCESS, ti.state)

                # Get mocked releases, list_ror_records is tested in its own function
                ror_records = [
                    {
                        "snapshot_date": "2022-10-20",
                        "url": f"http://localhost:{self.server_port}/v1.11-2022-10-20-ror-data.zip",
                        "checksum": "md5:0cac8705fba6df755648472356b7cb83",
                    },
                    {
                        "snapshot_date": "2022-10-17",
                        "url": f"http://localhost:{self.server_port}/v1.10-2022-10-17-ror-data.zip",
                        "checksum": "md5:60620675937e6513104275931331f68f",
                    },
                ]
                releases = [
                    RorRelease.from_dict(
                        dict(
                            dag_id=self.dag_id,
                            run_id=dag_run.run_id,
                            cloud_workspace=env.cloud_workspace.to_dict(),
                            **data,
                        )
                    )
                    for data in ror_records
                ]
                m_list_ror_records.return_value = ror_records
                ti = env.run_task("fetch_releases")
                self.assertEqual(State.SUCCESS, ti.state)

                expected = [release.to_dict() for release in releases]
                actual = ti.xcom_pull(
                    key="return_value",
                    task_ids="fetch_releases",
                    include_prior_dates=False,
                )
                self.assertListEqual(expected, actual)

                # Test download task
                for map_index, release in enumerate(releases):
                    logging.info(f"Processing release: map_index={map_index}, {release.to_dict()}")

                    server = HttpServer(FIXTURES_FOLDER, port=self.server_port)
                    with server.create():
                        ti = env.run_task("process_release.download", map_index=map_index)
                        self.assertEqual(State.SUCCESS, ti.state)
                    md5 = release.checksum[4:]
                    self.assert_file_integrity(release.download_file_path, md5, "md5")

                    # Test that file uploaded
                    self.assert_blob_integrity(
                        env.download_bucket,
                        gcs_blob_name_from_path(release.download_file_path),
                        release.download_file_path,
                    )

                    # Transform data
                    ti = env.run_task("process_release.transform", map_index=map_index)
                    self.assertEqual(State.SUCCESS, ti.state)

                    # Test that file extracted
                    self.assertEqual(1, len(list_files(release.extract_folder, release.extract_file_regex)))

                    # Test that file transformed
                    self.assertTrue(os.path.isfile(release.transform_file_path))

                    # Test that transformed file uploaded
                    self.assert_blob_integrity(
                        env.transform_bucket,
                        gcs_blob_name_from_path(release.transform_file_path),
                        release.transform_file_path,
                    )

                    # Test that data loaded into BigQuery
                    ti = env.run_task("process_release.bq_load", map_index=map_index)
                    self.assertEqual(State.SUCCESS, ti.state)
                    expected_content = load_jsonl(os.path.join(FIXTURES_FOLDER, "table_content.jsonl"))
                    table_id = bq_sharded_table_id(self.project_id, bq_dataset_id, bq_table_name, release.snapshot_date)
                    self.assert_table_content(table_id, expected_content, "id")

                    # Test that DatasetRelease is added to database
                    dataset_releases = get_dataset_releases(dag_id=self.dag_id, dataset_id=api_dataset_id)
                    self.assertEqual(len(dataset_releases), map_index)
                    ti = env.run_task("process_release.add_dataset_releases", map_index=map_index)
                    self.assertEqual(State.SUCCESS, ti.state)
                    dataset_releases = get_dataset_releases(dag_id=self.dag_id, dataset_id=api_dataset_id)
                    self.assertEqual(len(dataset_releases), map_index + 1)

                    # Test that all workflow data deleted
                    ti = env.run_task("process_release.cleanup_workflow", map_index=map_index)
                    self.assertEqual(State.SUCCESS, ti.state)
                    self.assert_cleanup(release.workflow_folder)

    def test_list_ror_records(self):
        """Test that list_ror_records returns correct records"""

        # There are a number of releases near this range
        # 2023-04-12, 2023-03-30, 2023-03-16, 2023-02-28
        # It should just return 2023-03-30, 2023-03-16 as end_date is exclusive
        start_date = pendulum.datetime(2023, 3, 16)
        end_date = pendulum.datetime(2023, 4, 12)

        with vcr.use_cassette(os.path.join(FIXTURES_FOLDER, "list_ror_records.yaml")):
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
