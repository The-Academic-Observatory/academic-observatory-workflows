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
from typing import List
from unittest.mock import patch

import pendulum
import vcr
from airflow import AirflowException
from airflow.models import Connection
from airflow.utils.state import State
from observatory.platform.api import get_dataset_releases
from observatory.platform.bigquery import bq_sharded_table_id
from observatory.platform.files import list_files
from observatory.platform.gcs import gcs_blob_name_from_path
from observatory.platform.observatory_config import Workflow
from observatory.platform.observatory_environment import (
    find_free_port,
    HttpServer,
    load_and_parse_json,
    ObservatoryEnvironment,
    ObservatoryTestCase,
)

import academic_observatory_workflows
from academic_observatory_workflows.config import project_path
from academic_observatory_workflows.unpaywall_telescope.unpaywall_telescope import (
    Changefile,
    changefile_download_url,
    changefiles_url,
    get_snapshot_file_name,
    get_unpaywall_changefiles,
    parse_release_msg,
    snapshot_url,
    unpaywall_filename_to_datetime,
    UnpaywallRelease,
    UnpaywallTelescope,
)

FIXTURES_FOLDER = project_path("unpaywall_telescope", "tests", "fixtures")


class TestUnpaywallUtils(ObservatoryTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def test_changefile(self):
        class MockRelease:
            @property
            def download_folder(self):
                return "/path/to/download/"

            @property
            def extract_folder(self):
                return "/path/to/extract/"

            @property
            def transform_folder(self):
                return "/path/to/transform/"

        cf = Changefile(
            "changed_dois_with_versions_2020-03-11T005336.jsonl.gz",
            pendulum.datetime(2020, 3, 11, 0, 53, 36),
            changefile_release=MockRelease(),
        )

        # download_file_path
        self.assertEqual(
            "/path/to/download/changed_dois_with_versions_2020-03-11T005336.jsonl.gz", cf.download_file_path
        )

        # extract_file_path
        self.assertEqual("/path/to/extract/changed_dois_with_versions_2020-03-11T005336.jsonl", cf.extract_file_path)

        # transform_file_path
        self.assertEqual(
            "/path/to/transform/changed_dois_with_versions_2020-03-11T005336.jsonl", cf.transform_file_path
        )

        # from_dict
        dict_ = dict(filename=cf.filename, changefile_date=cf.changefile_date.isoformat())
        self.assertEqual(cf, Changefile.from_dict(dict_))

        # to_dict
        self.assertEqual(dict_, cf.to_dict())

    def test_snapshot_url(self):
        url = snapshot_url("my-api-key")
        self.assertEqual(f"https://api.unpaywall.org/feed/snapshot?api_key=my-api-key", url)

    def test_changefiles_url(self):
        url = changefiles_url("my-api-key")
        self.assertEqual(f"https://api.unpaywall.org/feed/changefiles?interval=day&api_key=my-api-key", url)

    def test_changefile_download_url(self):
        url = changefile_download_url("changed_dois_with_versions_2020-03-11T005336.jsonl.gz", "my-api-key")
        self.assertEqual(
            f"https://api.unpaywall.org/daily-feed/changefile/changed_dois_with_versions_2020-03-11T005336.jsonl.gz?api_key=my-api-key",
            url,
        )

    def test_unpaywall_filename_to_datetime(self):
        # Snapshot filename
        filename = "unpaywall_snapshot_2023-04-25T083002.jsonl.gz"
        dt = unpaywall_filename_to_datetime(filename)
        self.assertEqual(pendulum.datetime(2023, 4, 25, 8, 30, 2), dt)

        # Changefile filename
        filename = "changed_dois_with_versions_2020-03-11T005336.jsonl.gz"
        dt = unpaywall_filename_to_datetime(filename)
        self.assertEqual(pendulum.datetime(2020, 3, 11, 0, 53, 36), dt)

        # Filename without time component
        filename = "changed_dois_with_versions_2020-03-11.jsonl.gz"
        dt = unpaywall_filename_to_datetime(filename)
        self.assertEqual(pendulum.datetime(2020, 3, 11, 0, 0, 0), dt)

    @patch("observatory.platform.utils.url_utils.get_http_text_response")
    def test_get_unpaywall_changefiles(self, m_get_http_text_response):
        # Don't use vcr here because the actual returned data contains API keys and it is a lot of data
        m_get_http_text_response.return_value = '{"list":[{"date":"2023-04-25","filename":"changed_dois_with_versions_2023-04-25T080001.jsonl.gz","filetype":"jsonl","last_modified":"2023-04-25T08:03:12","lines":310346,"size":143840367,"url":"https://api.unpaywall.org/daily-feed/changefile/changed_dois_with_versions_2023-04-25T080001.jsonl.gz?api_key=my-api-key"},{"date":"2023-04-24","filename":"changed_dois_with_versions_2023-04-24T080001.jsonl.gz","filetype":"jsonl","last_modified":"2023-04-24T08:04:49","lines":220800,"size":112157260,"url":"https://api.unpaywall.org/daily-feed/changefile/changed_dois_with_versions_2023-04-24T080001.jsonl.gz?api_key=my-api-key"},{"date":"2023-04-23","filename":"changed_dois_with_versions_2023-04-23T080001.jsonl.gz","filetype":"jsonl","last_modified":"2023-04-23T08:03:54","lines":213140,"size":105247617,"url":"https://api.unpaywall.org/daily-feed/changefile/changed_dois_with_versions_2023-04-23T080001.jsonl.gz?api_key=my-api-key"},{"date":"2023-02-24","filename":"changed_dois_with_versions_2023-02-24.jsonl.gz","filetype":"jsonl","last_modified":"2023-03-21T01:51:18","lines":5,"size":6301,"url":"https://api.unpaywall.org/daily-feed/changefile/changed_dois_with_versions_2023-02-24.jsonl.gz?api_key=my-api-key"},{"date":"2020-03-11","filename":"changed_dois_with_versions_2020-03-11T005336.csv.gz","filetype":"csv","last_modified":"2020-03-11T01:27:04","lines":1806534,"size":195900034,"url":"https://api.unpaywall.org/daily-feed/changefile/changed_dois_with_versions_2020-03-11T005336.csv.gz?api_key=my-api-key"}]}'

        expected_changefiles = [
            Changefile("changed_dois_with_versions_2023-02-24.jsonl.gz", pendulum.datetime(2023, 2, 24)),
            Changefile(
                "changed_dois_with_versions_2023-04-23T080001.jsonl.gz", pendulum.datetime(2023, 4, 23, 8, 0, 1)
            ),
            Changefile(
                "changed_dois_with_versions_2023-04-24T080001.jsonl.gz", pendulum.datetime(2023, 4, 24, 8, 0, 1)
            ),
            Changefile(
                "changed_dois_with_versions_2023-04-25T080001.jsonl.gz", pendulum.datetime(2023, 4, 25, 8, 0, 1)
            ),
        ]
        changefiles = get_unpaywall_changefiles("my-api-key")
        self.assertEqual(expected_changefiles, changefiles)

    def test_get_snapshot_file_name(self):
        # This cassette was run with a valid api key, which is not saved into the cassette or code
        # Set UNPAYWALL_API_KEY to use the real api key
        with vcr.use_cassette(
            os.path.join(FIXTURES_FOLDER, "get_snapshot_file_name_success.yaml"),
            filter_query_parameters=["api_key"],
        ):
            filename = get_snapshot_file_name(os.getenv("UNPAYWALL_API_KEY", "my-api-key"))
            self.assertEqual("unpaywall_snapshot_2023-04-25T083002.jsonl.gz", filename)

        # An invalid API key
        with vcr.use_cassette(
            os.path.join(FIXTURES_FOLDER, "get_snapshot_file_name_failure.yaml"),
            filter_query_parameters=["api_key"],
        ):
            with self.assertRaises(AirflowException):
                get_snapshot_file_name("invalid-api-key")


def make_changefiles(start_date: pendulum.DateTime, end_date: pendulum.DateTime) -> List[Changefile]:
    changefiles = [
        Changefile(
            f"changed_dois_with_versions_{day.format('YYYY-MM-DD')}T080001.jsonl.gz",
            pendulum.datetime(day.year, day.month, day.day, 8, 0, 1),
        )
        for day in pendulum.period(start_date, end_date).range("days")
    ]

    # Make sure sorted
    changefiles.sort(key=lambda c: c.changefile_date, reverse=False)

    return changefiles


def make_snapshot_filename(snapshot_date: pendulum.DateTime):
    return f"unpaywall_snapshot_{snapshot_date.format('YYYY-MM-DDTHHmmss')}.jsonl.gz"


class TestUnpaywallTelescope(ObservatoryTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.dag_id = "unpaywall"
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")

    def test_dag_structure(self):
        """Test that the DAG has the correct structure."""

        workflow = UnpaywallTelescope(
            dag_id=self.dag_id,
            cloud_workspace=self.fake_cloud_workspace,
        )
        dag = workflow.make_dag()
        self.assert_dag_structure(
            {
                "wait_for_prev_dag_run": ["check_dependencies"],
                "check_dependencies": ["fetch_releases"],
                "fetch_releases": ["create_datasets"],
                "create_datasets": ["bq_create_main_table_snapshot"],
                "bq_create_main_table_snapshot": ["download_snapshot"],
                "download_snapshot": ["upload_downloaded_snapshot"],
                "upload_downloaded_snapshot": ["extract_snapshot"],
                "extract_snapshot": ["transform_snapshot"],
                "transform_snapshot": ["split_main_table_file"],
                "split_main_table_file": ["upload_main_table_files"],
                "upload_main_table_files": ["bq_load_main_table"],
                "bq_load_main_table": ["download_change_files"],
                "download_change_files": ["upload_downloaded_change_files"],
                "upload_downloaded_change_files": ["extract_change_files"],
                "extract_change_files": ["transform_change_files"],
                "transform_change_files": ["upload_upsert_files"],
                "upload_upsert_files": ["bq_load_upsert_table"],
                "bq_load_upsert_table": ["bq_upsert_records"],
                "bq_upsert_records": ["add_new_dataset_releases"],
                "add_new_dataset_releases": ["cleanup"],
                "cleanup": ["dag_run_complete"],
                "dag_run_complete": [],
            },
            dag,
        )

    def test_dag_load(self):
        """Test that workflow can be loaded from a DAG bag."""

        env = ObservatoryEnvironment(
            workflows=[
                Workflow(
                    dag_id=self.dag_id,
                    name="Unpaywall Telescope",
                    class_name="academic_observatory_workflows.unpaywall_telescope.unpaywall_telescope.UnpaywallTelescope",
                    cloud_workspace=self.fake_cloud_workspace,
                )
            ]
        )

        with env.create():
            self.assert_dag_load_from_config(self.dag_id)

    def test_telescope(self):
        """Test workflow end to end.

        The test files in fixtures/unpaywall have been carefully crafted to make sure that the data is loaded
        into BigQuery correctly.
        """

        env = ObservatoryEnvironment(self.project_id, self.data_location, api_port=find_free_port())
        bq_dataset_id = env.add_dataset()

        with env.create(task_logging=True):
            workflow = UnpaywallTelescope(
                dag_id=self.dag_id,
                cloud_workspace=env.cloud_workspace,
                bq_dataset_id=bq_dataset_id,
            )
            conn = Connection(conn_id=workflow.unpaywall_conn_id, uri="http://:YOUR_API_KEY@")
            env.add_connection(conn)
            dag = workflow.make_dag()

            # First run: snapshot and initial changefiles
            data_interval_start = pendulum.datetime(2023, 4, 25)
            snapshot_date = pendulum.datetime(2023, 4, 25, 8, 30, 2)
            changefile_date = pendulum.datetime(2023, 4, 25, 8, 0, 1)
            with env.create_dag_run(dag, data_interval_start) as dag_run:
                # Mocked and expected data
                release = UnpaywallRelease(
                    dag_id=self.dag_id,
                    run_id=dag_run.run_id,
                    cloud_workspace=workflow.cloud_workspace,
                    bq_dataset_id=workflow.bq_dataset_id,
                    bq_table_name=workflow.bq_table_name,
                    is_first_run=True,
                    snapshot_date=snapshot_date,
                    start_date=changefile_date,
                    end_date=changefile_date,
                    changefiles=[
                        Changefile(
                            "changed_dois_with_versions_2023-04-25T080001.jsonl.gz",
                            changefile_date,
                        )
                    ],
                    prev_end_date=pendulum.instance(datetime.datetime.min),
                )

                # Wait for the previous DAG run to finish
                ti = env.run_task("wait_for_prev_dag_run")
                self.assertEqual(State.SUCCESS, ti.state)

                # Check dependencies are met
                ti = env.run_task(workflow.check_dependencies.__name__)
                self.assertEqual(State.SUCCESS, ti.state)

                # Fetch releases and check that we have received the expected snapshot date and changefiles
                unpaywall_changefiles = make_changefiles(workflow.start_date, snapshot_date)
                snapshot_file_name = make_snapshot_filename(snapshot_date)
                with patch.multiple(
                    "academic_observatory_workflows.unpaywall_telescope.unpaywall_telescope",
                    get_unpaywall_changefiles=lambda api_key: unpaywall_changefiles,
                    get_snapshot_file_name=lambda api_key: snapshot_file_name,
                ):
                    task_id = workflow.fetch_releases.__name__
                    ti = env.run_task(task_id)
                    self.assertEqual(State.SUCCESS, ti.state)
                msg = ti.xcom_pull(
                    key=workflow.RELEASE_INFO,
                    task_ids=task_id,
                    include_prior_dates=False,
                )
                actual_snapshot_date, actual_changefiles, actual_is_first_run, actual_prev_end_date = parse_release_msg(
                    msg
                )
                self.assertEqual(snapshot_date, actual_snapshot_date)
                self.assertListEqual(
                    release.changefiles,
                    actual_changefiles,
                )
                self.assertTrue(actual_is_first_run)
                self.assertEqual(pendulum.instance(datetime.datetime.min), actual_prev_end_date)

                # Create datasets
                ti = env.run_task(workflow.create_datasets.__name__)
                self.assertEqual(State.SUCCESS, ti.state)

                # Create snapshot: no table created on this run
                ti = env.run_task(workflow.bq_create_main_table_snapshot.__name__)
                self.assertEqual(State.SUCCESS, ti.state)

                # Download and process snapshot
                server = HttpServer(directory=FIXTURES_FOLDER, port=find_free_port())
                with server.create():
                    with patch.object(
                        academic_observatory_workflows.unpaywall_telescope.unpaywall_telescope,
                        "SNAPSHOT_URL",
                        f"http://localhost:{server.port}/{snapshot_file_name}",
                    ):
                        ti = env.run_task(workflow.download_snapshot.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                self.assertTrue(os.path.exists(release.snapshot_download_file_path))

                ti = env.run_task(workflow.upload_downloaded_snapshot.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                self.assert_blob_integrity(
                    env.download_bucket,
                    gcs_blob_name_from_path(release.snapshot_download_file_path),
                    release.snapshot_download_file_path,
                )

                ti = env.run_task(workflow.extract_snapshot.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                self.assertTrue(os.path.isfile(release.snapshot_extract_file_path))

                ti = env.run_task(workflow.transform_snapshot.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                self.assertTrue(os.path.isfile(release.main_table_file_path))

                ti = env.run_task("split_main_table_file")
                self.assertEqual(State.SUCCESS, ti.state)
                file_paths = list_files(release.snapshot_release.transform_folder, release.main_table_files_regex)
                self.assertTrue(len(file_paths) >= 1)
                for file_path in file_paths:
                    self.assertTrue(os.path.isfile(file_path))

                ti = env.run_task(workflow.upload_main_table_files.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                for file_path in file_paths:
                    self.assert_blob_integrity(env.transform_bucket, gcs_blob_name_from_path(file_path), file_path)

                ti = env.run_task(workflow.bq_load_main_table.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                self.assert_table_integrity(release.bq_main_table_id, expected_rows=10)
                expected_content = load_and_parse_json(
                    os.path.join(FIXTURES_FOLDER, "expected", "run1_bq_load_main_table.json"),
                    date_fields={"oa_date", "published_date"},
                    timestamp_fields={"updated"},
                )
                self.assert_table_content(release.bq_main_table_id, expected_content, "doi")

                # Download and process changefiles
                server = HttpServer(directory=FIXTURES_FOLDER, port=find_free_port())
                with server.create():
                    with patch.object(
                        academic_observatory_workflows.unpaywall_telescope.unpaywall_telescope,
                        "CHANGEFILES_DOWNLOAD_URL",
                        f"http://localhost:{server.port}",
                    ):
                        ti = env.run_task(workflow.download_change_files.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                for changefile in release.changefiles:
                    self.assertTrue(os.path.isfile(changefile.download_file_path))

                ti = env.run_task(workflow.upload_downloaded_change_files.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                for changefile in release.changefiles:
                    self.assert_blob_integrity(
                        env.download_bucket,
                        gcs_blob_name_from_path(changefile.download_file_path),
                        changefile.download_file_path,
                    )

                ti = env.run_task(workflow.extract_change_files.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                for changefile in release.changefiles:
                    self.assertTrue(os.path.isfile(changefile.extract_file_path))

                ti = env.run_task(workflow.transform_change_files.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                # The transformed files are deleted
                for changefile in release.changefiles:
                    self.assertFalse(os.path.isfile(changefile.transform_file_path))
                # Upsert file should exist
                self.assertTrue(os.path.isfile(release.upsert_table_file_path))

                ti = env.run_task(workflow.upload_upsert_files.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                self.assert_blob_integrity(
                    env.transform_bucket,
                    gcs_blob_name_from_path(release.upsert_table_file_path),
                    release.upsert_table_file_path,
                )

                ti = env.run_task(workflow.bq_load_upsert_table.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                self.assert_table_integrity(release.bq_upsert_table_id, expected_rows=2)

                ti = env.run_task(workflow.bq_upsert_records.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                self.assert_table_integrity(release.bq_main_table_id, expected_rows=10)
                expected_content = load_and_parse_json(
                    os.path.join(FIXTURES_FOLDER, "expected", "run1_bq_upsert_records.json"),
                    date_fields={"oa_date", "published_date"},
                    timestamp_fields={"updated"},
                )
                self.assert_table_content(release.bq_main_table_id, expected_content, "doi")

                # Final tasks
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

                ti = env.run_task("dag_run_complete")
                self.assertEqual(State.SUCCESS, ti.state)

            # Second run: no new changefiles
            data_interval_start = pendulum.datetime(2023, 4, 26)
            with env.create_dag_run(dag, data_interval_start):
                # Fetch releases and check that we have received the expected snapshot date and changefiles
                task_ids = ["wait_for_prev_dag_run", "check_dependencies", "fetch_releases"]
                with patch.multiple(
                    "academic_observatory_workflows.unpaywall_telescope.unpaywall_telescope",
                    get_unpaywall_changefiles=lambda api_key: [],
                    get_snapshot_file_name=lambda api_key: "filename",
                ):
                    for task_id in task_ids:
                        ti = env.run_task(task_id)
                        self.assertEqual(State.SUCCESS, ti.state)

                # Check that all subsequent tasks are skipped
                task_ids = [
                    "create_datasets",
                    "bq_create_main_table_snapshot",
                    "download_snapshot",
                    "upload_downloaded_snapshot",
                    "extract_snapshot",
                    "transform_snapshot",
                    "split_main_table_file",
                    "upload_main_table_files",
                    "bq_load_main_table",
                    "download_change_files",
                    "upload_downloaded_change_files",
                    "extract_change_files",
                    "transform_change_files",
                    "upload_upsert_files",
                    "bq_load_upsert_table",
                    "bq_upsert_records",
                    "add_new_dataset_releases",
                    "cleanup",
                ]
                for task_id in task_ids:
                    ti = env.run_task(task_id)
                    self.assertEqual(State.SKIPPED, ti.state)

                # Check that only 1 dataset release exists
                dataset_releases = get_dataset_releases(dag_id=self.dag_id, dataset_id=workflow.api_dataset_id)
                self.assertEqual(len(dataset_releases), 1)

            # Third run: waiting a couple of days and applying multiple changefiles
            prev_end_date = pendulum.datetime(2023, 4, 25, 8, 0, 1)
            data_interval_start = pendulum.datetime(2023, 4, 27)
            start_date = pendulum.datetime(2023, 4, 26, 8, 0, 1)
            end_date = pendulum.datetime(2023, 4, 27, 8, 0, 1)
            with env.create_dag_run(dag, data_interval_start) as dag_run:
                # Mocked and expected data
                release = UnpaywallRelease(
                    dag_id=self.dag_id,
                    run_id=dag_run.run_id,
                    cloud_workspace=workflow.cloud_workspace,
                    bq_dataset_id=workflow.bq_dataset_id,
                    bq_table_name=workflow.bq_table_name,
                    is_first_run=False,
                    snapshot_date=snapshot_date,
                    start_date=start_date,
                    end_date=end_date,
                    changefiles=[
                        Changefile(
                            "changed_dois_with_versions_2023-04-27T080001.jsonl.gz",
                            end_date,
                        ),
                        Changefile(
                            "changed_dois_with_versions_2023-04-26T080001.jsonl.gz",
                            start_date,
                        ),
                    ],
                    prev_end_date=prev_end_date,
                )

                # Fetch releases and check that we have received the expected snapshot date and changefiles
                task_ids = [
                    "wait_for_prev_dag_run",
                    "check_dependencies",
                    "fetch_releases",
                    "create_datasets",
                    "bq_create_main_table_snapshot",
                ]
                with patch.multiple(
                    "academic_observatory_workflows.unpaywall_telescope.unpaywall_telescope",
                    get_unpaywall_changefiles=lambda api_key: release.changefiles,
                    get_snapshot_file_name=lambda api_key: make_snapshot_filename(snapshot_date),
                ):
                    for task_id in task_ids:
                        ti = env.run_task(task_id)
                        self.assertEqual(State.SUCCESS, ti.state)

                # Check that snapshot created
                dst_table_id = bq_sharded_table_id(
                    workflow.cloud_workspace.output_project_id,
                    workflow.bq_dataset_id,
                    f"{workflow.bq_table_name}_snapshot",
                    prev_end_date,
                )
                self.assert_table_integrity(dst_table_id, expected_rows=10)

                # Run snapshot tasks, these should all be successful, but actually just skip internally
                task_ids = [
                    "download_snapshot",
                    "upload_downloaded_snapshot",
                    "extract_snapshot",
                    "transform_snapshot",
                    "split_main_table_file",
                    "upload_main_table_files",
                    "bq_load_main_table",
                ]
                for task_id in task_ids:
                    ti = env.run_task(task_id)
                    self.assertEqual(State.SUCCESS, ti.state)

                # Run changefile tasks
                server = HttpServer(directory=FIXTURES_FOLDER, port=find_free_port())
                with server.create():
                    with patch.object(
                        academic_observatory_workflows.unpaywall_telescope.unpaywall_telescope,
                        "CHANGEFILES_DOWNLOAD_URL",
                        f"http://localhost:{server.port}",
                    ):
                        ti = env.run_task(workflow.download_change_files.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                for changefile in release.changefiles:
                    self.assertTrue(os.path.isfile(changefile.download_file_path))

                ti = env.run_task(workflow.upload_downloaded_change_files.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                for changefile in release.changefiles:
                    self.assert_blob_integrity(
                        env.download_bucket,
                        gcs_blob_name_from_path(changefile.download_file_path),
                        changefile.download_file_path,
                    )

                ti = env.run_task(workflow.extract_change_files.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                for changefile in release.changefiles:
                    self.assertTrue(os.path.isfile(changefile.extract_file_path))

                ti = env.run_task(workflow.transform_change_files.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                # The transformed files are deleted
                for changefile in release.changefiles:
                    self.assertFalse(os.path.isfile(changefile.transform_file_path))
                # Upsert file should exist
                self.assertTrue(os.path.isfile(release.upsert_table_file_path))

                ti = env.run_task(workflow.upload_upsert_files.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                self.assert_blob_integrity(
                    env.transform_bucket,
                    gcs_blob_name_from_path(release.upsert_table_file_path),
                    release.upsert_table_file_path,
                )

                ti = env.run_task(workflow.bq_load_upsert_table.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                self.assert_table_integrity(release.bq_upsert_table_id, expected_rows=4)

                ti = env.run_task(workflow.bq_upsert_records.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                self.assert_table_integrity(release.bq_main_table_id, expected_rows=12)
                expected_content = load_and_parse_json(
                    os.path.join(FIXTURES_FOLDER, "expected", "run3_bq_upsert_records.json"),
                    date_fields={"oa_date", "published_date"},
                    timestamp_fields={"updated"},
                )
                self.assert_table_content(release.bq_main_table_id, expected_content, "doi")

                # Final tasks
                dataset_releases = get_dataset_releases(dag_id=self.dag_id, dataset_id=workflow.api_dataset_id)
                self.assertEqual(len(dataset_releases), 1)
                ti = env.run_task(workflow.add_new_dataset_releases.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                dataset_releases = get_dataset_releases(dag_id=self.dag_id, dataset_id=workflow.api_dataset_id)
                self.assertEqual(len(dataset_releases), 2)

                # Test that all workflow data deleted
                ti = env.run_task(workflow.cleanup.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                self.assert_cleanup(release.workflow_folder)

                ti = env.run_task("dag_run_complete")
                self.assertEqual(State.SUCCESS, ti.state)
