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

# Author: James Diprose, Tuan Chien


import os
from unittest.mock import patch

import pendulum
import vcr
from airflow.utils.state import State

from academic_observatory_workflows.config import test_fixtures_folder
from academic_observatory_workflows.workflows.open_citations_telescope import (
    OpenCitationsRelease,
    OpenCitationsTelescope,
    list_releases,
)
from observatory.platform.api import get_dataset_releases
from observatory.platform.bigquery import bq_sharded_table_id
from observatory.platform.files import list_files
from observatory.platform.gcs import gcs_blob_name_from_path
from observatory.platform.observatory_config import Workflow
from observatory.platform.observatory_environment import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    find_free_port,
    HttpServer,
)
from observatory.platform.utils.http_download import DownloadInfo


class TestOpenCitationsTelescope(ObservatoryTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.dag_id = "open_citations"
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")
        self.server_port = find_free_port()

    def test_dag_structure(self):
        """Test that the DAG has the correct structure."""

        workflow = OpenCitationsTelescope(
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
                "extract": ["upload_transformed"],
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
                    name="Open Citations Telescope",
                    class_name="academic_observatory_workflows.workflows.open_citations_telescope.OpenCitationsTelescope",
                    cloud_workspace=self.fake_cloud_workspace,
                )
            ]
        )

        with env.create():
            self.assert_dag_load_from_config(self.dag_id)

    @patch("academic_observatory_workflows.workflows.open_citations_telescope.list_releases")
    def test_telescope(self, m_list_records):
        """Test the OpenCitationsTelescope telescope end to end."""

        # Setup Observatory environment
        env = ObservatoryEnvironment(self.project_id, self.data_location, api_port=find_free_port())
        bq_dataset_id = env.add_dataset()

        with env.create():
            execution_date = pendulum.datetime(year=2018, month=11, day=12)
            workflow = OpenCitationsTelescope(
                dag_id=self.dag_id,
                cloud_workspace=env.cloud_workspace,
                bq_dataset_id=bq_dataset_id,
            )
            dag = workflow.make_dag()

            with env.create_dag_run(dag, execution_date) as dag_run:
                # Make mocked data
                files = [
                    dict(
                        download_url=f"http://localhost:{self.server_port}/data.csv.zip",
                        name="data.csv.zip",
                        computed_md5="f06dfd0bee323a95861f0ba490e786c9",
                    ),
                    dict(
                        download_url=f"http://localhost:{self.server_port}/data2.csv.zip",
                        name="data2.csv.zip",
                        computed_md5="6d90805d99b65b107b17907432aa8534",
                    ),
                ]
                release_info = [dict(date="20181113", files=files)]
                release = OpenCitationsRelease(
                    dag_id=self.dag_id,
                    run_id=dag_run.run_id,
                    snapshot_date=pendulum.datetime(2018, 11, 13),
                    files=[
                        DownloadInfo(
                            url=file["download_url"],
                            filename=file["name"],
                            hash=file["computed_md5"],
                            hash_algorithm="md5",
                        )
                        for file in files
                    ],
                )

                # Check dependencies
                ti = env.run_task(workflow.check_dependencies.__name__)
                self.assertEqual(State.SUCCESS, ti.state)

                # Get release info
                # Mocked so that we can control what files are returned
                # list_release_info tested separately
                m_list_records.return_value = release_info
                ti = env.run_task(workflow.get_release_info.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                actual_release_info = ti.xcom_pull(
                    key=OpenCitationsTelescope.RELEASE_INFO,
                    task_ids=workflow.get_release_info.__name__,
                    include_prior_dates=False,
                )
                self.assertEqual(release_info, actual_release_info)

                # Download
                server = HttpServer(directory=test_fixtures_folder(self.dag_id), port=self.server_port)
                with server.create():
                    ti = env.run_task(workflow.download.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                download_files = list_files(release.download_folder, release.download_file_regex)
                self.assertEqual(2, len(download_files))
                for download_info in release.files:
                    self.assert_file_integrity(
                        os.path.join(release.download_folder, download_info.filename),
                        download_info.hash,
                        download_info.hash_algorithm,
                    )

                # Upload downloaded
                ti = env.run_task(workflow.upload_downloaded.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                download_files = list_files(release.download_folder, release.download_file_regex)
                for file_path in download_files:
                    self.assert_blob_integrity(env.download_bucket, gcs_blob_name_from_path(file_path), file_path)

                # Extract
                ti = env.run_task(workflow.extract.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                transform_files = list_files(release.transform_folder, release.transform_file_regex)
                self.assertEqual(2, len(transform_files))

                # Upload transformed
                ti = env.run_task(workflow.upload_transformed.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                transform_files = list_files(release.transform_folder, release.transform_file_regex)
                for file_path in transform_files:
                    self.assert_blob_integrity(env.transform_bucket, gcs_blob_name_from_path(file_path), file_path)

                # BQ load
                ti = env.run_task(workflow.bq_load.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                table_id = bq_sharded_table_id(
                    self.project_id, workflow.bq_dataset_id, workflow.bq_table_name, release.snapshot_date
                )
                expected_rows = 4
                self.assert_table_integrity(table_id, expected_rows)
                expected_content = [
                    dict(
                        oci="020010100093631183015370109090737060203090304-020020102030636101310000103090309",
                        citing="10.1109/viuf.1997.623934",
                        cited="10.21236/ada013939",
                        creation="1997",
                        timespan="P22Y",
                        journal_sc=False,
                        author_sc=False,
                    ),
                    dict(
                        oci="02001010009363118353702000009370100-0200100010636280009020563020301025800025900000601036306",
                        citing="10.1109/viz.2009.10",
                        cited="10.1016/s0925-2312(02)00613-6",
                        creation="2009-07",
                        timespan="P6Y3M",
                        journal_sc=False,
                        author_sc=False,
                    ),
                    dict(
                        oci="020010100093631183015370109090737060203090304-020020102030636101310000103090309",
                        citing="10.1109/viuf.1997.623934",
                        cited="10.21236/ada013939",
                        creation="1997",
                        timespan="P22Y",
                        journal_sc=False,
                        author_sc=False,
                    ),
                    dict(
                        oci="02001010009363118353702000009370100-0200100010636280009020563020301025800025900000601036306",
                        citing="10.1109/viz.2009.10",
                        cited="10.1016/s0925-2312(02)00613-6",
                        creation="2009-07",
                        timespan="P6Y3M",
                        journal_sc=False,
                        author_sc=False,
                    ),
                ]
                self.assert_table_content(table_id, expected_content, "oci")

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
        """Test that the list_releases function works correctly"""

        with vcr.use_cassette(
            test_fixtures_folder(self.dag_id, "list_releases.yaml"),
        ):
            # Test that 2 releases are returned
            # 2018-07-13T22:10:34Z, 2018-11-12T21:42:54Z, 2020-01-21T06:35:29Z
            # It should just return 2018-07-13, 2018-11-12 as end_date is exclusive
            start_date = pendulum.datetime(2018, 7, 13)
            end_date = pendulum.datetime(2020, 1, 21)
            records = list_releases(start_date, end_date)
            self.assertEqual(2, len(records))
            self.assertEqual(
                [
                    {
                        "date": "20180713",
                        "files": [
                            {
                                "id": 12302372,
                                "name": "data.csv.zip",
                                "size": 8730701380,
                                "is_link_only": False,
                                "download_url": "https://ndownloader.figshare.com/files/12302372",
                                "supplied_md5": "b4fe3f61981fd210651ef18c7bac8977",
                                "computed_md5": "b4fe3f61981fd210651ef18c7bac8977",
                            }
                        ],
                    },
                    {
                        "date": "20181112",
                        "files": [
                            {
                                "id": 13531112,
                                "name": "data.csv.zip",
                                "size": 11568165723,
                                "is_link_only": False,
                                "download_url": "https://ndownloader.figshare.com/files/13531112",
                                "supplied_md5": "9cbcd5b5b7df8d02a3d0d6f503fe79bc",
                                "computed_md5": "9cbcd5b5b7df8d02a3d0d6f503fe79bc",
                            }
                        ],
                    },
                ],
                records,
            )

            # Test that no releases are returned
            start_date = pendulum.datetime(2018, 7, 14)
            end_date = pendulum.datetime(2018, 11, 11)
            records = list_releases(start_date, end_date)
            self.assertEqual(0, len(records))
