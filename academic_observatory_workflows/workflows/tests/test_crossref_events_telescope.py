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

# Author: Aniek Roelofs

import os
from datetime import timedelta
from unittest.mock import patch

import pendulum
import vcr
from academic_observatory_workflows.config import test_fixtures_folder
from academic_observatory_workflows.workflows.crossref_events_telescope import (
    CrossrefEventsRelease,
    CrossrefEventsTelescope,
    parse_event_url,
    transform_batch,
)
from airflow.exceptions import AirflowSkipException
from click.testing import CliRunner
from google.cloud import bigquery
from observatory.platform.utils.test_utils import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    module_file_path,
)
from observatory.platform.utils.url_utils import get_user_agent
from observatory.platform.utils.workflow_utils import blob_name, create_date_table_id
from observatory.api.testing import ObservatoryApiEnvironment
from observatory.api.client import ApiClient, Configuration
from observatory.api.client.api.observatory_api import ObservatoryApi  # noqa: E501
from observatory.api.client.model.organisation import Organisation
from observatory.api.client.model.workflow import Workflow
from observatory.api.client.model.workflow_type import WorkflowType
from observatory.api.client.model.dataset import Dataset
from observatory.api.client.model.dataset_release import DatasetRelease
from observatory.api.client.model.dataset_type import DatasetType
from observatory.api.client.model.table_type import TableType
from observatory.platform.utils.release_utils import get_dataset_releases
from observatory.platform.utils.airflow_utils import AirflowConns
from airflow.models import Connection
from airflow.utils.state import State


class TestCrossrefEventsTelescope(ObservatoryTestCase):
    """Tests for the Crossref Events telescope"""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.
        :param args: arguments.
        :param kwargs: keyword arguments.
        """
        super(TestCrossrefEventsTelescope, self).__init__(*args, **kwargs)
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")

        self.first_execution_date = pendulum.datetime(year=2018, month=5, day=21)
        self.first_cassette = test_fixtures_folder("crossref_events", "crossref_events1.yaml")

        self.second_execution_date = pendulum.datetime(year=2018, month=5, day=28)
        self.second_cassette = test_fixtures_folder("crossref_events", "crossref_events2.yaml")

        # additional tests setup
        self.start_date = pendulum.datetime(2021, 5, 6)
        self.end_date = pendulum.datetime(2021, 5, 13)
        self.release = CrossrefEventsRelease(
            CrossrefEventsTelescope.DAG_ID,
            self.start_date,
            self.end_date,
            False,
            "mailto",
            max_threads=21,
            max_processes=1,
        )

        # API environment
        self.host = "localhost"
        self.port = 5001
        configuration = Configuration(host=f"http://{self.host}:{self.port}")
        api_client = ApiClient(configuration)
        self.api = ObservatoryApi(api_client=api_client)  # noqa: E501
        self.env = ObservatoryApiEnvironment(host=self.host, port=self.port)
        self.org_name = "Curtin University"

    def setup_api(self):
        dt = pendulum.now("UTC")

        name = "Crossref Events Telescope"
        workflow_type = WorkflowType(name=name, type_id=CrossrefEventsTelescope.DAG_ID)
        self.api.put_workflow_type(workflow_type)

        organisation = Organisation(
            name="Curtin University",
            project_id="project",
            download_bucket="download_bucket",
            transform_bucket="transform_bucket",
        )
        self.api.put_organisation(organisation)

        telescope = Workflow(
            name=name,
            workflow_type=WorkflowType(id=1),
            organisation=Organisation(id=1),
            extra={},
        )
        self.api.put_workflow(telescope)

        table_type = TableType(
            type_id="partitioned",
            name="partitioned bq table",
        )
        self.api.put_table_type(table_type)

        dataset_type = DatasetType(
            type_id=CrossrefEventsTelescope.DAG_ID,
            name="ds type",
            extra={},
            table_type=TableType(id=1),
        )
        self.api.put_dataset_type(dataset_type)

        dataset = Dataset(
            name="Crossref Events Dataset",
            address="project.dataset.table",
            service="bigquery",
            workflow=Workflow(id=1),
            dataset_type=DatasetType(id=1),
        )
        self.api.put_dataset(dataset)

    def setup_connections(self, env):
        # Add Observatory API connection
        conn = Connection(conn_id=AirflowConns.OBSERVATORY_API, uri=f"http://:password@{self.host}:{self.port}")
        env.add_connection(conn)

    def test_dag_structure(self):
        """Test that the Crossref Events DAG has the correct structure.
        :return: None
        """

        dag = CrossrefEventsTelescope().make_dag()
        self.assert_dag_structure(
            {
                "check_dependencies": ["download"],
                "download": ["upload_downloaded"],
                "upload_downloaded": ["transform"],
                "transform": ["upload_transformed"],
                "upload_transformed": ["bq_load_partition"],
                "bq_load_partition": ["bq_delete_old"],
                "bq_delete_old": ["bq_append_new"],
                "bq_append_new": ["cleanup"],
                "cleanup": ["add_new_dataset_releases"],
                "add_new_dataset_releases": [],
            },
            dag,
        )

    def test_dag_load(self):
        """Test that the Crossref Events DAG can be loaded from a DAG bag.
        :return: None
        """

        env = ObservatoryEnvironment(self.project_id, self.data_location, api_host=self.host, api_port=self.port)

        with env.create():
            self.setup_connections(env)
            self.setup_api()
            dag_file = os.path.join(
                module_file_path("academic_observatory_workflows.dags"), "crossref_events_telescope.py"
            )
            self.assert_dag_load("crossref_events", dag_file)

    def test_telescope(self):
        """Test the Crossref Events telescope end to end.
        :return: None.
        """
        # Setup Observatory environment
        env = ObservatoryEnvironment(self.project_id, self.data_location, api_host=self.host, api_port=self.port)
        dataset_id = env.add_dataset()

        # Setup Telescope
        telescope = CrossrefEventsTelescope(dataset_id=dataset_id, workflow_id=1)
        telescope.max_threads = 1
        telescope.max_processes = 1
        dag = telescope.make_dag()

        # Create the Observatory environment and run tests
        with env.create(task_logging=True):
            self.setup_connections(env)
            self.setup_api()
            # first run
            with env.create_dag_run(dag, self.first_execution_date) as dag_run:
                # Test that all dependencies are specified: no error should be thrown
                env.run_task(telescope.check_dependencies.__name__)

                start_date, end_date, first_release = telescope.get_release_info(
                    dag=dag,
                    data_interval_end=pendulum.datetime(2018, 5, 20),
                )

                self.assertEqual(start_date, dag.default_args["start_date"])
                self.assertEqual(pendulum.instance(end_date), pendulum.datetime(2018, 5, 20))
                self.assertTrue(first_release)

                # use release info for other tasks
                release = CrossrefEventsRelease(
                    telescope.dag_id,
                    start_date,
                    end_date,
                    first_release,
                    telescope.mailto,
                    telescope.max_threads,
                    telescope.max_processes,
                )

                # Test download task
                vcr_ = vcr.VCR(ignore_localhost=True)
                with vcr_.use_cassette(self.first_cassette):
                    env.run_task(telescope.download.__name__)
                self.assertEqual(6, len(release.download_files))
                for file in release.download_files:
                    if "2018-05-14" in file:
                        download_hash = "9a18d1002a5395de3cbcd9c61fb28c83"
                    else:
                        download_hash = "ad9cf98aab232eee7edf12375f016770"
                    self.assert_file_integrity(file, download_hash, "md5")

                # Test that files uploaded
                env.run_task(telescope.upload_downloaded.__name__)
                for file in release.download_files:
                    self.assert_blob_integrity(env.download_bucket, blob_name(file), file)

                # Test that files transformed
                env.run_task(telescope.transform.__name__)
                self.assertEqual(6, len(release.transform_files))
                for file in release.transform_files:
                    if "2018-05-14" in file:
                        transform_hash = "3e953d2424fe37739790bbc5c2410824"
                    else:
                        transform_hash = "d5e0a887656d1786a9e7c4dbdbf77ba1"
                    self.assert_file_integrity(file, transform_hash, "md5")

                # Test that transformed files uploaded
                env.run_task(telescope.upload_transformed.__name__)
                for file in release.transform_files:
                    self.assert_blob_integrity(env.transform_bucket, blob_name(file), file)

                # Test that load partition task is skipped for the first release
                ti = env.run_task(telescope.bq_load_partition.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # Test delete old task is skipped for the first release
                with patch("observatory.platform.utils.gc_utils.bq_query_bytes_daily_limit_check"):
                    ti = env.run_task(telescope.bq_delete_old.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # Test append new creates table
                env.run_task(telescope.bq_append_new.__name__)
                main_table_id, partition_table_id = release.dag_id, f"{release.dag_id}_partitions"
                table_id = f"{self.project_id}.{telescope.dataset_id}.{main_table_id}"
                expected_rows = 68
                self.assert_table_integrity(table_id, expected_rows)

                # Test that all telescope data deleted
                download_folder, extract_folder, transform_folder = (
                    release.download_folder,
                    release.extract_folder,
                    release.transform_folder,
                )
                env.run_task(telescope.cleanup.__name__)
                self.assert_cleanup(download_folder, extract_folder, transform_folder)

                # add_dataset_release_task
                dataset_releases = get_dataset_releases(dataset_id=1)
                self.assertEqual(len(dataset_releases), 0)
                ti = env.run_task("add_new_dataset_releases")
                self.assertEqual(ti.state, State.SUCCESS)
                dataset_releases = get_dataset_releases(dataset_id=1)
                self.assertEqual(len(dataset_releases), 1)

            # second run
            with env.create_dag_run(dag, self.second_execution_date) as dag_run:
                # Test that all dependencies are specified: no error should be thrown
                env.run_task(telescope.check_dependencies.__name__)

                start_date, end_date, first_release = telescope.get_release_info(
                    dag=dag,
                    data_interval_end=pendulum.datetime(2018, 5, 27),
                )

                self.assertEqual(release.end_date, start_date)
                self.assertEqual(pendulum.datetime(2018, 5, 27), end_date)
                self.assertFalse(first_release)

                # use release info for other tasks
                release = CrossrefEventsRelease(
                    telescope.dag_id,
                    start_date,
                    end_date,
                    first_release,
                    telescope.mailto,
                    telescope.max_threads,
                    telescope.max_processes,
                )

                # Test download task
                with vcr_.use_cassette(self.second_cassette):
                    env.run_task(telescope.download.__name__)

                self.assertEqual(20, len(release.download_files))
                for file in release.download_files:
                    if "edited" in file:
                        download_hash = "b1c8c856c29365efeeef8a7c1ccba7da"
                    elif "deleted" in file:
                        download_hash = "8d52425faa9192e8748865b8c53c2b3d"
                    else:
                        download_hash = "01aa964587e6296df5697d13a122e8ce"
                    self.assert_file_integrity(file, download_hash, "md5")

                # Test that file uploaded
                env.run_task(telescope.upload_downloaded.__name__)
                for file in release.download_files:
                    self.assert_blob_integrity(env.download_bucket, blob_name(file), file)

                # Test that file transformed
                env.run_task(telescope.transform.__name__)

                self.assertEqual(20, len(release.transform_files))
                for file in release.transform_files:
                    if "edited" in file:
                        transform_hash = "902437a731a4aed529f4e0d176d2222b"
                    elif "deleted" in file:
                        transform_hash = "10b6d1911aaaad14204d867884722da4"
                    else:
                        transform_hash = "513d71d356d8356d1365d1dd25b1f71a"
                    self.assert_file_integrity(file, transform_hash, "md5")

                # Test that transformed file uploaded
                env.run_task(telescope.upload_transformed.__name__)
                for file in release.transform_files:
                    self.assert_blob_integrity(env.transform_bucket, blob_name(file), file)

                # Test that load partition task creates partition
                env.run_task(telescope.bq_load_partition.__name__)
                main_table_id, partition_table_id = release.dag_id, f"{release.dag_id}_partitions"
                table_id = create_date_table_id(partition_table_id, release.end_date, bigquery.TimePartitioningType.DAY)
                table_id = f"{self.project_id}.{telescope.dataset_id}.{table_id}"
                expected_rows = 82
                self.assert_table_integrity(table_id, expected_rows)

                # Test task deleted rows from main table
                with patch("observatory.platform.utils.gc_utils.bq_query_bytes_daily_limit_check"):
                    env.run_task(telescope.bq_delete_old.__name__)
                table_id = f"{self.project_id}.{telescope.dataset_id}.{main_table_id}"
                expected_rows = 60
                self.assert_table_integrity(table_id, expected_rows)

                # Test append new adds rows to table
                env.run_task(telescope.bq_append_new.__name__)
                table_id = f"{self.project_id}.{telescope.dataset_id}.{main_table_id}"
                expected_rows = 142
                self.assert_table_integrity(table_id, expected_rows)

                # Test that all telescope data deleted
                download_folder, extract_folder, transform_folder = (
                    release.download_folder,
                    release.extract_folder,
                    release.transform_folder,
                )
                env.run_task(telescope.cleanup.__name__)
                self.assert_cleanup(download_folder, extract_folder, transform_folder)

                # add_dataset_release_task
                dataset_releases = get_dataset_releases(dataset_id=1)
                self.assertEqual(len(dataset_releases), 1)
                ti = env.run_task("add_new_dataset_releases")
                self.assertEqual(ti.state, State.SUCCESS)
                dataset_releases = get_dataset_releases(dataset_id=1)
                self.assertEqual(len(dataset_releases), 2)

    def test_urls(self):
        """Test the urls property of release
        :return: None.
        """
        events_url = (
            "https://api.eventdata.crossref.org/v1/events?mailto={mail_to}"
            "&from-collected-date={start_date}&until-collected-date={end_date}&rows=1000"
        )
        edited_url = (
            "https://api.eventdata.crossref.org/v1/events/edited?"
            "mailto={mail_to}&from-updated-date={start_date}"
            "&until-updated-date={end_date}&rows=1000"
        )
        deleted_url = (
            "https://api.eventdata.crossref.org/v1/events/deleted?"
            "mailto={mail_to}&from-updated-date={start_date}"
            "&until-updated-date={end_date}&rows=1000"
        )

        self.release.first_release = True
        urls = self.release.urls
        self.assertEqual(7, len(urls))
        for url in urls:
            event_type, date = parse_event_url(url)
            self.assertEqual(event_type, "events")
            expected_url = events_url.format(mail_to=self.release.mailto, start_date=date, end_date=date)
            self.assertEqual(expected_url, url)

        self.release.first_release = False
        urls = self.release.urls
        self.assertEqual(21, len(urls))
        for url in urls:
            event_type, date = parse_event_url(url)
            if event_type == "events":
                expected_url = events_url.format(mail_to=self.release.mailto, start_date=date, end_date=date)
            elif event_type == "edited":
                expected_url = edited_url.format(mail_to=self.release.mailto, start_date=date, end_date=date)
            else:
                expected_url = deleted_url.format(mail_to=self.release.mailto, start_date=date, end_date=date)
            self.assertEqual(expected_url, url)

    @patch.object(CrossrefEventsRelease, "download_batch")
    @patch("observatory.platform.utils.workflow_utils.Variable.get")
    def test_download(self, mock_variable_get, mock_download_batch):
        """Test the download method of the release in parallel mode
        :return: None.
        """
        mock_variable_get.return_value = "data"
        with CliRunner().isolated_filesystem():
            # Test download without any events returned
            with self.assertRaises(AirflowSkipException):
                self.release.download()

            # Test download with events returned
            mock_download_batch.reset_mock()
            events_path = os.path.join(self.release.download_folder, "events.jsonl")
            with open(events_path, "w") as f:
                f.write("[{'test': 'test'}]\n")

            self.release.download()
            self.assertEqual(len(self.release.urls), mock_download_batch.call_count)

    @patch("academic_observatory_workflows.workflows.crossref_events_telescope.download_events")
    @patch("observatory.platform.utils.workflow_utils.Variable.get")
    def test_download_batch(self, mock_variable_get, mock_download_events):
        """Test download_batch function
        :return: None.
        """
        mock_variable_get.return_value = os.path.join(os.getcwd(), "data")
        self.release.first_release = True
        batch_number = 0
        url = self.release.urls[batch_number]
        headers = {"User-Agent": get_user_agent(package_name="academic_observatory_workflows")}
        with CliRunner().isolated_filesystem():
            events_path = self.release.batch_path(url)
            cursor_path = self.release.batch_path(url, cursor=True)

            # Test with existing cursor path
            with open(cursor_path, "w") as f:
                f.write("cursor")
            mock_download_events.return_value = (None, 10, 10)
            self.release.download_batch(batch_number, url)
            self.assertFalse(os.path.exists(cursor_path))
            mock_download_events.assert_called_once_with(url, headers, events_path, cursor_path)

            # Test with no existing previous files
            mock_download_events.reset_mock()
            mock_download_events.return_value = (None, 10, 10)
            self.release.download_batch(batch_number, url)
            mock_download_events.assert_called_once_with(url, headers, events_path, cursor_path)

            # Test with events path and no cursor path, so previous successful attempt
            mock_download_events.reset_mock()
            with open(events_path, "w") as f:
                f.write("events")
            self.release.download_batch(batch_number, url)
            mock_download_events.assert_not_called()
            os.remove(events_path)

    @patch("observatory.platform.utils.workflow_utils.Variable.get")
    def test_transform_batch(self, mock_variable_get):
        """Test the transform_batch method of the release
        :return: None.
        """

        with CliRunner().isolated_filesystem() as t:
            mock_variable_get.return_value = os.path.join(t, "data")

            # Use release info so that we can download the right data
            release = CrossrefEventsRelease(
                "crossref_events",
                pendulum.datetime(2018, 5, 14),
                pendulum.datetime(2018, 5, 19),
                True,
                "aniek.roelofs@curtin.edu.au",
                max_threads=1,
                max_processes=1,
            )

            # Download files
            with vcr.use_cassette(self.first_cassette):
                release.download()

            # Transform batch
            for file_path in release.download_files:
                transform_batch(file_path, release.transform_folder)

            # Assert all transformed
            self.assertEqual(len(release.download_files), len(release.transform_files))
