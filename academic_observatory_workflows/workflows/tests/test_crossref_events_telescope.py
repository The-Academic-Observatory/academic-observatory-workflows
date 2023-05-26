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

import datetime
import json
import os
import pathlib
from concurrent.futures import as_completed, ThreadPoolExecutor
from unittest.mock import patch

import pendulum
import responses
import vcr
from airflow.utils.state import State
from click.testing import CliRunner
from google.cloud.exceptions import NotFound

from academic_observatory_workflows.config import test_fixtures_folder
from academic_observatory_workflows.workflows.crossref_events_telescope import (
    CrossrefEventsRelease,
    CrossrefEventsTelescope,
    Action,
    make_day_requests,
    parse_release_msg,
    EventRequest,
    crossref_events_limiter,
    download_events,
    fetch_events,
)
from observatory.platform.api import get_dataset_releases
from observatory.platform.files import list_files
from observatory.platform.gcs import gcs_blob_name_from_path
from observatory.platform.observatory_config import Workflow
from observatory.platform.observatory_environment import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    find_free_port,
    load_and_parse_json,
)


class TestCrossrefEventsTelescope(ObservatoryTestCase):
    def __init__(self, *args, **kwargs):
        super(TestCrossrefEventsTelescope, self).__init__(*args, **kwargs)
        self.dag_id = "crossref_events"
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")

    def test_dag_structure(self):
        """Test that the Crossref Events DAG has the correct structure."""

        workflow = CrossrefEventsTelescope(
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
                "bq_create_main_table_snapshot": ["download"],
                "download": ["upload_downloaded"],
                "upload_downloaded": ["transform_snapshot"],
                "transform_snapshot": ["transform_created_edited"],
                "transform_created_edited": ["transform_deleted"],
                "transform_deleted": ["upload_transformed"],
                "upload_transformed": ["bq_load_main_table"],
                "bq_load_main_table": ["bq_load_upsert_table"],
                "bq_load_upsert_table": ["bq_upsert_records"],
                "bq_upsert_records": ["bq_load_delete_table"],
                "bq_load_delete_table": ["bq_delete_records"],
                "bq_delete_records": ["add_new_dataset_releases"],
                "add_new_dataset_releases": ["cleanup"],
                "cleanup": ["dag_run_complete"],
                "dag_run_complete": [],
            },
            dag,
        )

    def test_dag_load(self):
        """Test that the Crossref Events DAG can be loaded from a DAG bag."""

        env = ObservatoryEnvironment(
            workflows=[
                Workflow(
                    dag_id=self.dag_id,
                    name="Crossref Events Telescope",
                    class_name="academic_observatory_workflows.workflows.crossref_events_telescope.CrossrefEventsTelescope",
                    cloud_workspace=self.fake_cloud_workspace,
                )
            ]
        )

        with env.create():
            self.assert_dag_load_from_config(self.dag_id)

    def test_telescope(self):
        """Test the Crossref Events telescope end to end."""

        env = ObservatoryEnvironment(self.project_id, self.data_location, api_port=find_free_port())
        bq_dataset_id = env.add_dataset()

        with env.create(task_logging=True):
            start_date = pendulum.datetime(2023, 4, 30)
            end_date = pendulum.datetime(2023, 5, 7)
            workflow = CrossrefEventsTelescope(
                dag_id=self.dag_id,
                cloud_workspace=env.cloud_workspace,
                bq_dataset_id=bq_dataset_id,
                max_threads=3,
                max_processes=3,
                events_start_date=start_date,
                n_rows=3,  # needs to be 3 because that is what the mocked data uses
            )
            dag = workflow.make_dag()

            # This run just calls create events
            with env.create_dag_run(dag, start_date) as dag_run:
                release = CrossrefEventsRelease(
                    dag_id=self.dag_id,
                    run_id=dag_run.run_id,
                    cloud_workspace=workflow.cloud_workspace,
                    bq_dataset_id=workflow.bq_dataset_id,
                    bq_table_name=workflow.bq_table_name,
                    start_date=start_date,
                    end_date=end_date,
                    prev_end_date=pendulum.instance(datetime.datetime.min),
                    is_first_run=True,
                    mailto=workflow.mailto,
                )

                # Wait for the previous DAG run to finish
                ti = env.run_task("wait_for_prev_dag_run")
                self.assertEqual(State.SUCCESS, ti.state)

                # Check dependencies are met
                ti = env.run_task(workflow.check_dependencies.__name__)
                self.assertEqual(State.SUCCESS, ti.state)

                # Fetch releases and check that we have received the expected information from the xcom
                task_id = workflow.fetch_releases.__name__
                ti = env.run_task(task_id)
                self.assertEqual(State.SUCCESS, ti.state)
                msg = ti.xcom_pull(
                    key=workflow.RELEASE_INFO,
                    task_ids=task_id,
                    include_prior_dates=False,
                )
                actual_start_date, actual_end_date, actual_is_first_run, actual_prev_end_date = parse_release_msg(msg)
                self.assertEqual(release.start_date, actual_start_date)
                self.assertEqual(release.end_date, actual_end_date)
                self.assertTrue(actual_is_first_run)
                self.assertEqual(release.prev_end_date, actual_prev_end_date)

                # Create datasets
                ti = env.run_task(workflow.create_datasets.__name__)
                self.assertEqual(State.SUCCESS, ti.state)

                # Create snapshot: no table created on this run
                ti = env.run_task(workflow.bq_create_main_table_snapshot.__name__)
                self.assertEqual(State.SUCCESS, ti.state)

                # Create mocked responses for run 1
                # Creates on this run
                with responses.RequestsMock(assert_all_requests_are_fired=True) as rsps:
                    with open(test_fixtures_folder(self.dag_id, "run1-responses.json"), mode="r") as f:
                        data = json.load(f)
                    for url, json_data in data.items():
                        rsps.add(
                            responses.GET,
                            url,
                            json=json_data,
                            status=200,
                        )
                    ti = env.run_task(workflow.download.__name__)
                    self.assertEqual(State.SUCCESS, ti.state)
                    for day in release.day_requests:
                        self.assertTrue(
                            os.path.isfile(os.path.join(release.download_folder, day.created.data_file_name))
                        )

                ti = env.run_task(workflow.upload_downloaded.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                for day in release.day_requests:
                    file_path = os.path.join(release.download_folder, day.created.data_file_name)
                    self.assert_blob_integrity(env.download_bucket, gcs_blob_name_from_path(file_path), file_path)

                ti = env.run_task(workflow.transform_snapshot.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                self.assertEqual(7, len(list_files(release.transform_folder, release.transform_files_regex)))

                ti = env.run_task(workflow.transform_created_edited.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                self.assertFalse(os.path.isfile(release.upsert_table_file_path))

                ti = env.run_task(workflow.transform_deleted.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                self.assertFalse(os.path.isfile(release.delete_table_file_path))

                ti = env.run_task(workflow.upload_transformed.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                for day in release.day_requests:
                    file_path = os.path.join(release.transform_folder, day.created.data_file_name)
                    self.assert_blob_integrity(env.transform_bucket, gcs_blob_name_from_path(file_path), file_path)

                ti = env.run_task(workflow.bq_load_main_table.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                self.assert_table_integrity(release.bq_main_table_id, expected_rows=42)

                ti = env.run_task(workflow.bq_load_upsert_table.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                with self.assertRaises(NotFound):
                    self.bigquery_client.get_table(release.bq_upsert_table_id)

                ti = env.run_task(workflow.bq_upsert_records.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                self.assert_table_integrity(release.bq_main_table_id, expected_rows=42)

                ti = env.run_task(workflow.bq_load_delete_table.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                with self.assertRaises(NotFound):
                    self.bigquery_client.get_table(release.bq_delete_table_id)

                ti = env.run_task(workflow.bq_delete_records.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                self.assert_table_integrity(release.bq_main_table_id, expected_rows=42)

                # Assert that we have correct dataset state
                expected_content = load_and_parse_json(
                    test_fixtures_folder(self.dag_id, "run1-expected.json"),
                    date_fields={"occurred_at", "timestamp", "updated_date"},
                )
                self.assert_table_content(release.bq_main_table_id, expected_content, workflow.primary_key)

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

            # Create, update and delete
            start_date = pendulum.datetime(2023, 5, 7)
            end_date = pendulum.datetime(2023, 5, 14)
            with env.create_dag_run(dag, start_date) as dag_run:
                release = CrossrefEventsRelease(
                    dag_id=self.dag_id,
                    run_id=dag_run.run_id,
                    cloud_workspace=workflow.cloud_workspace,
                    bq_dataset_id=workflow.bq_dataset_id,
                    bq_table_name=workflow.bq_table_name,
                    start_date=start_date,
                    end_date=end_date,
                    prev_end_date=start_date,
                    is_first_run=True,
                    mailto=workflow.mailto,
                )

                # Wait for the previous DAG run to finish
                ti = env.run_task("wait_for_prev_dag_run")
                self.assertEqual(State.SUCCESS, ti.state)

                # Check dependencies are met
                ti = env.run_task(workflow.check_dependencies.__name__)
                self.assertEqual(State.SUCCESS, ti.state)

                # Fetch releases and check that we have received the expected information from the xcom
                task_id = workflow.fetch_releases.__name__
                ti = env.run_task(task_id)
                self.assertEqual(State.SUCCESS, ti.state)
                msg = ti.xcom_pull(
                    key=workflow.RELEASE_INFO,
                    task_ids=task_id,
                    include_prior_dates=False,
                )
                actual_start_date, actual_end_date, actual_is_first_run, actual_prev_end_date = parse_release_msg(msg)
                self.assertEqual(release.start_date, actual_start_date)
                self.assertEqual(release.end_date, actual_end_date)
                self.assertFalse(actual_is_first_run)
                self.assertEqual(release.prev_end_date, actual_prev_end_date)

                # Create datasets
                ti = env.run_task(workflow.create_datasets.__name__)
                self.assertEqual(State.SUCCESS, ti.state)

                # Create snapshot
                ti = env.run_task(workflow.bq_create_main_table_snapshot.__name__)
                self.assertEqual(State.SUCCESS, ti.state)

                # Create mocked responses for run 2
                # Creates, updates and deletes on this run
                with responses.RequestsMock(assert_all_requests_are_fired=True) as rsps:
                    with open(test_fixtures_folder(self.dag_id, "run2-responses.json"), mode="r") as f:
                        data = json.load(f)
                    for url, json_data in data.items():
                        rsps.add(
                            responses.GET,
                            url,
                            json=json_data,
                            status=200,
                        )
                    ti = env.run_task(workflow.download.__name__)
                    self.assertEqual(State.SUCCESS, ti.state)
                    expected_file_names = [
                        "created-2023-05-07.jsonl",
                        "created-2023-05-08.jsonl",
                        "created-2023-05-09.jsonl",
                        "created-2023-05-10.jsonl",
                        "created-2023-05-11.jsonl",
                        "created-2023-05-12.jsonl",
                        "created-2023-05-13.jsonl",
                        "deleted-2023-05-07.jsonl",
                        "deleted-2023-05-13.jsonl",
                        "edited-2023-05-07.jsonl",
                        "edited-2023-05-13.jsonl",
                    ]
                    for file_name in expected_file_names:
                        self.assertTrue(os.path.isfile(os.path.join(release.download_folder, file_name)))
                    for file_name in [
                        "edited-2023-05-08.jsonl",
                        "edited-2023-05-09.jsonl",
                        "edited-2023-05-10.jsonl",
                        "edited-2023-05-11.jsonl",
                        "edited-2023-05-12.jsonl",
                        "deleted-2023-05-08.jsonl",
                        "deleted-2023-05-09.jsonl",
                        "deleted-2023-05-10.jsonl",
                        "deleted-2023-05-11.jsonl",
                        "deleted-2023-05-12.jsonl",
                    ]:
                        self.assertFalse(os.path.isfile(os.path.join(release.download_folder, file_name)))

                ti = env.run_task(workflow.upload_downloaded.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                for file_name in expected_file_names:
                    file_path = os.path.join(release.download_folder, file_name)
                    self.assert_blob_integrity(env.download_bucket, gcs_blob_name_from_path(file_path), file_path)

                ti = env.run_task(workflow.transform_snapshot.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                self.assertEqual(0, len(list_files(release.transform_folder, release.transform_files_regex)))

                ti = env.run_task(workflow.transform_created_edited.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                self.assertTrue(os.path.isfile(release.upsert_table_file_path))

                ti = env.run_task(workflow.transform_deleted.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                self.assertTrue(os.path.isfile(release.delete_table_file_path))

                ti = env.run_task(workflow.upload_transformed.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                self.assert_blob_integrity(
                    env.transform_bucket,
                    gcs_blob_name_from_path(release.upsert_table_file_path),
                    release.upsert_table_file_path,
                )
                self.assert_blob_integrity(
                    env.transform_bucket,
                    gcs_blob_name_from_path(release.delete_table_file_path),
                    release.delete_table_file_path,
                )

                ti = env.run_task(workflow.bq_load_main_table.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                self.assert_table_integrity(release.bq_main_table_id, expected_rows=42)

                ti = env.run_task(workflow.bq_load_upsert_table.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                self.assert_table_integrity(release.bq_upsert_table_id, expected_rows=42)

                ti = env.run_task(workflow.bq_upsert_records.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                self.assert_table_integrity(release.bq_main_table_id, expected_rows=84)

                ti = env.run_task(workflow.bq_load_delete_table.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                self.assert_table_integrity(release.bq_delete_table_id, expected_rows=2)

                ti = env.run_task(workflow.bq_delete_records.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                self.assert_table_integrity(release.bq_main_table_id, expected_rows=82)

                # Assert that we have correct dataset state
                expected_content = load_and_parse_json(
                    test_fixtures_folder(self.dag_id, "run2-expected.json"),
                    date_fields={"occurred_at", "timestamp", "updated_date"},
                )
                self.assert_table_content(release.bq_main_table_id, expected_content, workflow.primary_key)

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


class TestCrossrefEventsUtils(ObservatoryTestCase):
    @patch("academic_observatory_workflows.workflows.crossref_events_telescope.fetch_events")
    def test_download_events(self, m_fetch_events):
        m_fetch_events.return_value = ([], None)
        request = EventRequest(Action.create, pendulum.datetime(2023, 5, 1), "test@test.com")
        n_rows = 10

        # No files
        # Hasn't been run before
        with CliRunner().isolated_filesystem() as download_folder:
            download_events(request, download_folder, n_rows)
            assert m_fetch_events.call_count == 1

        # Data file and cursor file
        # The data file was party downloaded because cursor still exists
        m_fetch_events.reset_mock()
        with CliRunner().isolated_filesystem() as download_folder:
            pathlib.Path(pathlib.Path(download_folder) / "created-2023-05-01.jsonl").touch()
            pathlib.Path(pathlib.Path(download_folder) / "created-2023-05-01-cursor.txt").touch()
            download_events(request, download_folder, n_rows)
            assert m_fetch_events.call_count == 1

        # Data file and no cursor file
        # The data file was fully downloaded and cursor file removed
        m_fetch_events.reset_mock()
        with CliRunner().isolated_filesystem() as download_folder:
            pathlib.Path(pathlib.Path(download_folder) / "created-2023-05-01.jsonl").touch()
            download_events(request, download_folder, n_rows)
            assert m_fetch_events.call_count == 0

    def test_fetch_events(self):
        dt = pendulum.datetime(2023, 5, 1)
        mailto = "agent@observatory.academy"

        # Create events
        with vcr.use_cassette(test_fixtures_folder("crossref_events", "test_fetch_events_create.yaml")):
            request = EventRequest(Action.create, dt, mailto)
            events1, next_cursor1 = fetch_events(request, n_rows=10)
            self.assertEqual(10, len(events1))
            self.assertIsNotNone(next_cursor1)
            events2, next_cursor2 = fetch_events(request, next_cursor1, n_rows=10)
            self.assertEqual(10, len(events2))
            self.assertIsNotNone(next_cursor2)

        # Edit events
        with vcr.use_cassette(test_fixtures_folder("crossref_events", "test_fetch_events_edit.yaml")):
            request = EventRequest(Action.edit, dt, mailto)
            events, next_cursor = fetch_events(request, n_rows=10)
            self.assertEqual(0, len(events))
            self.assertIsNone(next_cursor)

        # Delete events
        with vcr.use_cassette(test_fixtures_folder("crossref_events", "test_fetch_events_delete.yaml")):
            request = EventRequest(Action.delete, dt, mailto)
            self.assertEqual(0, len(events))
            events, next_cursor = fetch_events(request, n_rows=10)
            self.assertIsNone(next_cursor)

    def test_crossref_events_limiter(self):
        n_per_second = 10

        def my_func():
            crossref_events_limiter(n_per_second)
            print("Called my_func")

        num_calls = 100
        max_workers = 10
        expected_wait = num_calls / n_per_second
        print(f"test_crossref_events_limiter: expected wait time {expected_wait}s")
        start = datetime.datetime.now()
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for event in range(num_calls):
                futures.append(executor.submit(my_func))
            for future in as_completed(futures):
                future.result()

        end = datetime.datetime.now()
        duration = (end - start).total_seconds()
        actual_n_per_second = 1 / (duration / num_calls)
        print(f"test_crossref_events_limiter: actual_n_per_second {actual_n_per_second}")
        self.assertAlmostEqual(float(n_per_second), actual_n_per_second, delta=6)

    def test_event_request(self):
        day = pendulum.datetime(2023, 1, 1)
        mailto = "test@test.com"
        request = EventRequest(Action.create, day, mailto)
        self.assertEqual("created-2023-01-01.jsonl", request.data_file_name)
        self.assertEqual("created-2023-01-01-cursor.txt", request.cursor_file_name)
        self.assertEqual(
            "https://api.eventdata.crossref.org/v1/events?from-collected-date=2023-01-01&until-collected-date=2023-01-01&mailto=test%40test.com&rows=10",
            request.make_url(rows=10),
        )
        self.assertEqual(
            "https://api.eventdata.crossref.org/v1/events?from-collected-date=2023-01-01&until-collected-date=2023-01-01&mailto=test%40test.com&rows=10&cursor=abcde",
            request.make_url(rows=10, cursor="abcde"),
        )

        request = EventRequest(Action.edit, day, mailto)
        self.assertEqual("edited-2023-01-01.jsonl", request.data_file_name)
        self.assertEqual("edited-2023-01-01-cursor.txt", request.cursor_file_name)
        self.assertEqual(
            "https://api.eventdata.crossref.org/v1/events/edited?from-updated-date=2023-01-01&until-updated-date=2023-01-01&mailto=test%40test.com&rows=10",
            request.make_url(rows=10),
        )
        self.assertEqual(
            "https://api.eventdata.crossref.org/v1/events/edited?from-updated-date=2023-01-01&until-updated-date=2023-01-01&mailto=test%40test.com&rows=10&cursor=abcde",
            request.make_url(rows=10, cursor="abcde"),
        )

        request = EventRequest(Action.delete, day, mailto)
        self.assertEqual("deleted-2023-01-01.jsonl", request.data_file_name)
        self.assertEqual("deleted-2023-01-01-cursor.txt", request.cursor_file_name)
        self.assertEqual(
            "https://api.eventdata.crossref.org/v1/events/deleted?from-updated-date=2023-01-01&until-updated-date=2023-01-01&mailto=test%40test.com&rows=10",
            request.make_url(rows=10),
        )
        self.assertEqual(
            "https://api.eventdata.crossref.org/v1/events/deleted?from-updated-date=2023-01-01&until-updated-date=2023-01-01&mailto=test%40test.com&rows=10&cursor=abcde",
            request.make_url(rows=10, cursor="abcde"),
        )

    def test_make_day_requests(self):
        requests = make_day_requests(pendulum.datetime(2023, 1, 1), pendulum.datetime(2023, 1, 7), "test@test.com")
        self.assertEqual(6, len(requests))
        self.assertEqual(
            [
                pendulum.datetime(2023, 1, 1),
                pendulum.datetime(2023, 1, 2),
                pendulum.datetime(2023, 1, 3),
                pendulum.datetime(2023, 1, 4),
                pendulum.datetime(2023, 1, 5),
                pendulum.datetime(2023, 1, 6),
            ],
            [req.date for req in requests],
        )
