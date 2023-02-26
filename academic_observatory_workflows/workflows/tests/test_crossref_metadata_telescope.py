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

import json
import os

import httpretty
import pendulum
from airflow.models import Connection
from airflow.utils.state import State
from click.testing import CliRunner

from academic_observatory_workflows.config import test_fixtures_folder
from academic_observatory_workflows.workflows.crossref_metadata_telescope import (
    CrossrefMetadataRelease,
    CrossrefMetadataTelescope,
    transform_item,
    transform_file,
    make_snapshot_url,
    check_release_exists,
)
from observatory.platform.api import get_dataset_releases
from observatory.platform.bigquery import bq_sharded_table_id
from observatory.platform.files import load_jsonl, list_files, is_gzip
from observatory.platform.gcs import gcs_blob_name_from_path
from observatory.platform.observatory_config import Workflow
from observatory.platform.observatory_environment import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    find_free_port,
)


class TestCrossrefMetadataTelescope(ObservatoryTestCase):
    """Tests for the Crossref Metadata telescope"""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        super(TestCrossrefMetadataTelescope, self).__init__(*args, **kwargs)
        self.dag_id = "crossref_metadata"
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")
        self.download_path = test_fixtures_folder("crossref_metadata", "crossref_metadata.json.tar.gz")

    def test_dag_structure(self):
        """Test that the DAG has the correct structure."""

        workflow = CrossrefMetadataTelescope(
            dag_id=self.dag_id,
            cloud_workspace=self.fake_cloud_workspace,
        )

        dag = workflow.make_dag()
        self.assert_dag_structure(
            {
                "check_dependencies": ["check_release_exists"],
                "check_release_exists": ["download"],
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
        """Test that the DAG can be loaded from a DAG bag."""

        env = ObservatoryEnvironment(
            workflows=[
                Workflow(
                    dag_id=self.dag_id,
                    name="Crossref Fundref Telescope",
                    class_name="academic_observatory_workflows.workflows.crossref_metadata_telescope.CrossrefMetadataTelescope",
                    cloud_workspace=self.fake_cloud_workspace,
                )
            ]
        )

        with env.create():
            self.assert_dag_load_from_config(self.dag_id)

    def test_telescope(self):
        """Test the Crossref Metadata telescope end to end."""

        env = ObservatoryEnvironment(self.project_id, self.data_location, api_port=find_free_port())
        bq_dataset_id = env.add_dataset()

        with env.create():
            # Setup Workflow
            # Execution date is always the 7th of the month and the start of the data interval
            # The DAG run for execution date of 2023-01-07 actually runs on 2023-02-07
            workflow = CrossrefMetadataTelescope(
                dag_id=self.dag_id,
                cloud_workspace=env.cloud_workspace,
                bq_dataset_id=bq_dataset_id,
            )
            dag = workflow.make_dag()
            execution_date = pendulum.datetime(year=2023, month=1, day=7)

            # Add Crossref Metadata connection
            env.add_connection(Connection(conn_id=workflow.crossref_metadata_conn_id, uri="http://:crossref-token@"))

            with env.create_dag_run(dag, execution_date) as dag_run:
                # Mocked and expected data
                # Snapshot date is the end of the execution date month
                snapshot_date = execution_date.end_of("month")
                release = CrossrefMetadataRelease(
                    dag_id=self.dag_id,
                    run_id=dag_run.run_id,
                    snapshot_date=snapshot_date,
                )

                # Test that all dependencies are specified: no error should be thrown
                ti = env.run_task(workflow.check_dependencies.__name__)
                self.assertEqual(State.SUCCESS, ti.state)

                # Test check release exists task, next tasks should not be skipped
                url = make_snapshot_url(execution_date)
                with httpretty.enabled():
                    httpretty.register_uri(httpretty.HEAD, url, body="", status=302)
                    ti = env.run_task(workflow.check_release_exists.__name__)
                    self.assertEqual(State.SUCCESS, ti.state)

                # Test download task
                with httpretty.enabled():
                    self.setup_mock_file_download(url, self.download_path)
                    ti = env.run_task(workflow.download.__name__)
                    self.assertEqual(State.SUCCESS, ti.state)
                expected_file_hash = "047770ae386f3376c08e3975d7f06016"
                self.assert_file_integrity(release.download_file_path, expected_file_hash, "md5")
                self.assertTrue(is_gzip(release.download_file_path))

                # Test that file uploaded
                ti = env.run_task(workflow.upload_downloaded.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                self.assert_blob_integrity(
                    env.download_bucket, gcs_blob_name_from_path(release.download_file_path), release.download_file_path
                )

                # Test that file extracted
                ti = env.run_task("extract")
                self.assertEqual(State.SUCCESS, ti.state)
                file_paths = list_files(release.extract_folder, release.extract_files_regex)
                self.assertEqual(5, len(file_paths))
                for file_path in file_paths:
                    self.assertTrue(os.path.isfile(file_path))
                    self.assertFalse(is_gzip(file_path))

                # Test that files transformed
                ti = env.run_task(workflow.transform.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                file_paths = list_files(release.transform_folder, release.transform_files_regex)
                self.assertEqual(1, len(file_paths))
                for file_path in file_paths:
                    self.assertTrue(os.path.isfile(file_path))
                    self.assertTrue(is_gzip(file_path))

                # Test that transformed files uploaded
                ti = env.run_task(workflow.upload_transformed.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                for file_path in list_files(release.transform_folder, release.transform_files_regex):
                    self.assert_blob_integrity(env.transform_bucket, gcs_blob_name_from_path(file_path), file_path)

                # Test that data loaded into BigQuery
                ti = env.run_task(workflow.bq_load.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                table_id = bq_sharded_table_id(
                    self.project_id, workflow.bq_dataset_id, workflow.bq_table_name, release.snapshot_date
                )
                expected_rows = 20
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

    def test_transform_file(self):
        """Test transform_file."""

        with CliRunner().isolated_filesystem() as t:
            # Save input file
            input_file_path = os.path.join(t, "input.json")
            input_data = {
                "items": [
                    {
                        "indexed": {
                            "date-parts": [[2019, 11, 19]],
                            "date-time": "2019-11-19T10:09:18Z",
                            "timestamp": 1574158158980,
                        },
                        "reference-count": 0,
                        "publisher": "American Medical Association (AMA)",
                        "issue": "2",
                        "content-domain": {"domain": [], "crossmark-restriction": False},
                        "short-container-title": [],
                        "published-print": {"date-parts": [[1994, 2, 1]]},
                        "DOI": "10.1001/archderm.130.2.225",
                        "type": "journal-article",
                        "created": {
                            "date-parts": [[2003, 3, 18]],
                            "date-time": "2003-03-18T21:22:40Z",
                            "timestamp": 1048022560000,
                        },
                        "page": "225-232",
                        "source": "Crossref",
                        "is-referenced-by-count": 23,
                        "title": ["Abnormalities of p53 protein expression in cutaneous disorders"],
                        "prefix": "10.1001",
                        "volume": "130",
                        "author": [{"given": "N. S.", "family": "McNutt", "affiliation": []}],
                        "member": "10",
                        "container-title": ["Archives of Dermatology"],
                        "original-title": [],
                        "deposited": {
                            "date-parts": [[2011, 7, 21]],
                            "date-time": "2011-07-21T07:23:09Z",
                            "timestamp": 1311232989000,
                        },
                        "score": None,
                        "subtitle": [],
                        "short-title": [],
                        "issued": {"date-parts": [[1994, 2, 1]]},
                        "references-count": 0,
                        "URL": "http://dx.doi.org/10.1001/archderm.130.2.225",
                        "relation": {},
                        "ISSN": ["0003-987X"],
                        "issn-type": [{"value": "0003-987X", "type": "print"}],
                    }
                ]
            }

            with open(input_file_path, mode="w") as f:
                json.dump(input_data, f)

            # Check results
            expected_results = [
                {
                    "indexed": {
                        "date_parts": [2019, 11, 19],
                        "date_time": "2019-11-19T10:09:18Z",
                        "timestamp": 1574158158980,
                    },
                    "reference_count": 0,
                    "publisher": "American Medical Association (AMA)",
                    "issue": "2",
                    "content_domain": {"domain": [], "crossmark_restriction": False},
                    "short_container_title": [],
                    "published_print": {"date_parts": [1994, 2, 1]},
                    "DOI": "10.1001/archderm.130.2.225",
                    "type": "journal-article",
                    "created": {
                        "date_parts": [2003, 3, 18],
                        "date_time": "2003-03-18T21:22:40Z",
                        "timestamp": 1048022560000,
                    },
                    "page": "225-232",
                    "source": "Crossref",
                    "is_referenced_by_count": 23,
                    "title": ["Abnormalities of p53 protein expression in cutaneous disorders"],
                    "prefix": "10.1001",
                    "volume": "130",
                    "author": [{"given": "N. S.", "family": "McNutt", "affiliation": []}],
                    "member": "10",
                    "container_title": ["Archives of Dermatology"],
                    "original_title": [],
                    "deposited": {
                        "date_parts": [2011, 7, 21],
                        "date_time": "2011-07-21T07:23:09Z",
                        "timestamp": 1311232989000,
                    },
                    "score": None,
                    "subtitle": [],
                    "short_title": [],
                    "issued": {"date_parts": [1994, 2, 1]},
                    "references_count": 0,
                    "URL": "http://dx.doi.org/10.1001/archderm.130.2.225",
                    "relation": {},
                    "ISSN": ["0003-987X"],
                    "issn_type": [{"value": "0003-987X", "type": "print"}],
                }
            ]
            actual_results = transform_file(input_file_path)
            self.assertEqual(expected_results, actual_results)

    def test_transform_item(self):
        """Test the cases that transform_item transforms"""

        # Replace hyphens with underscores
        item = {
            "hello": {},
            "hello-world": {"hello-world": [{"hello-world": 1}, {"hello-world": 1}, {"hello-world": 1}]},
        }
        expected = {
            "hello": {},
            "hello_world": {"hello_world": [{"hello_world": 1}, {"hello_world": 1}, {"hello_world": 1}]},
        }
        actual = transform_item(item)
        self.assertEqual(expected, actual)

        # date-parts
        item = {"date-parts": [[2021, 1, 1]]}
        expected = {"date_parts": [2021, 1, 1]}
        actual = transform_item(item)
        self.assertEqual(expected, actual)

        # date-parts with None inside inner list
        item = {"date-parts": [[None]]}
        expected = {"date_parts": []}
        actual = transform_item(item)
        self.assertEqual(expected, actual)

        # list with date-parts
        item = {"hello-world": {"hello-world": [{"date-parts": [[2021, 1, 1]]}, {"date-parts": [[None]]}]}}
        expected = {"hello_world": {"hello_world": [{"date_parts": [2021, 1, 1]}, {"date_parts": []}]}}
        actual = transform_item(item)
        self.assertEqual(expected, actual)

    def test_check_release_exists(self):
        """Test the 'check_release_exists' task with different responses."""

        mock_api_key = ""
        data_interval_start = pendulum.datetime(2020, 1, 7)
        url = make_snapshot_url(data_interval_start)
        with httpretty.enabled():
            # Register 3 responses, successful, release not found and 'other'
            httpretty.register_uri(
                httpretty.HEAD,
                uri=url,
                responses=[
                    httpretty.Response(body="", status=302),
                    httpretty.Response(body="", status=404, adding_headers={"reason": "Not Found"}),
                    httpretty.Response(body="", status=400),
                ],
            )

            exists = check_release_exists(data_interval_start, mock_api_key)
            self.assertTrue(exists)

            exists = check_release_exists(data_interval_start, mock_api_key)
            self.assertFalse(exists)

            exists = check_release_exists(data_interval_start, mock_api_key)
            self.assertFalse(exists)
