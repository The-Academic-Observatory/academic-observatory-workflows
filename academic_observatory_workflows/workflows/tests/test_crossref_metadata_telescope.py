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

import json
import os
from datetime import datetime
from unittest.mock import patch

import httpretty
import pendulum
from airflow.exceptions import AirflowException
from airflow.models.connection import Connection
from click.testing import CliRunner
from natsort import natsorted

from academic_observatory_workflows.config import test_fixtures_folder
from academic_observatory_workflows.workflows.crossref_metadata_telescope import (
    CrossrefMetadataRelease,
    CrossrefMetadataTelescope,
    transform_item,
    transform_file,
)
from observatory.platform.utils.airflow_utils import AirflowConns
from observatory.platform.utils.file_utils import load_jsonl
from observatory.platform.utils.gc_utils import bigquery_sharded_table_id
from observatory.platform.utils.test_utils import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    module_file_path,
)
from observatory.platform.utils.workflow_utils import blob_name


class TestCrossrefMetadataTelescope(ObservatoryTestCase):
    """Tests for the Crossref Metadata telescope"""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        super(TestCrossrefMetadataTelescope, self).__init__(*args, **kwargs)
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")
        self.download_path = test_fixtures_folder("crossref_metadata", "crossref_metadata.json.tar.gz")
        self.extract_file_hashes = [
            "42cab8ed20ef20bed51dacd3dc364589",
            "c45901a52154789470410aad51485e9c",
            "4c0fd617224a557b9ef04313cca0bd4a",
            "d93dc613e299871925532d906c3a44a1",
            "dd1ab247c55191a14bcd1bf32719c337",
        ]
        self.transform_hashes = [
            "a2be39d3c4d4c9dc20af768f8ae35476",
            "38b766ec494054e621787de00ff715c8",
            "70437aad7c4568ed07408baf034871e4",
            "c3e3285a48867c8b7c10b1c9c0c5ab8a",
            "71ba3612352bcb2a723d4aa33ec35b61",
        ]

        # release used for tests outside observatory test environment
        self.release = CrossrefMetadataRelease("crossref_metadata", datetime(2020, 1, 1))

    def test_dag_structure(self):
        """Test that the Crossref Metadata DAG has the correct structure.

        :return: None
        """

        dag = CrossrefMetadataTelescope().make_dag()
        self.assert_dag_structure(
            {
                "check_dependencies": ["check_release_exists"],
                "check_release_exists": ["download"],
                "download": ["upload_downloaded"],
                "upload_downloaded": ["extract"],
                "extract": ["transform"],
                "transform": ["upload_transformed"],
                "upload_transformed": ["bq_load"],
                "bq_load": ["cleanup"],
                "cleanup": [],
            },
            dag,
        )

    def test_dag_load(self):
        """Test that the Crossref Metadata DAG can be loaded from a DAG bag.

        :return: None
        """

        with ObservatoryEnvironment().create():
            dag_file = os.path.join(
                module_file_path("academic_observatory_workflows.dags"), "crossref_metadata_telescope.py"
            )
            self.assert_dag_load("crossref_metadata", dag_file)

    def test_telescope(self):
        """Test the Crossref Metadata telescope end to end.

        :return: None.
        """

        # Setup Observatory environment
        env = ObservatoryEnvironment(self.project_id, self.data_location)
        dataset_id = env.add_dataset()

        # Setup Telescope
        execution_date = pendulum.datetime(year=2022, month=1, day=1)
        telescope = CrossrefMetadataTelescope(dataset_id=dataset_id)
        dag = telescope.make_dag()

        # Create the Observatory environment and run tests
        with env.create():
            with env.create_dag_run(dag, execution_date):
                # Add Crossref Metadata connection
                env.add_connection(Connection(conn_id=AirflowConns.CROSSREF, uri="mysql://:crossref-token@"))

                # Test that all dependencies are specified: no error should be thrown
                env.run_task(telescope.check_dependencies.__name__)

                # Test check release exists task, next tasks should not be skipped
                with httpretty.enabled():
                    url = CrossrefMetadataTelescope.TELESCOPE_URL.format(
                        year=execution_date.year, month=execution_date.month
                    )
                    httpretty.register_uri(httpretty.HEAD, url, body="", status=302)
                    env.run_task(telescope.check_release_exists.__name__)

                release = CrossrefMetadataRelease(telescope.dag_id, execution_date)

                # Test download task
                with httpretty.enabled():
                    self.setup_mock_file_download(release.url, self.download_path)
                    env.run_task(telescope.download.__name__)
                self.assertEqual(1, len(release.download_files))
                expected_file_hash = "047770ae386f3376c08e3975d7f06016"
                self.assert_file_integrity(release.download_path, expected_file_hash, "md5")

                # Test that file uploaded
                env.run_task(telescope.upload_downloaded.__name__)
                self.assert_blob_integrity(env.download_bucket, blob_name(release.download_path), release.download_path)

                # Test that file extracted
                env.run_task(telescope.extract.__name__)
                self.assertEqual(5, len(release.extract_files))
                for i, file in enumerate(natsorted(release.extract_files)):
                    expected_file_hash = self.extract_file_hashes[i]
                    self.assert_file_integrity(file, expected_file_hash, "md5")

                # Test that files transformed
                env.run_task(telescope.transform.__name__)
                self.assertEqual(5, len(release.transform_files))
                for i, file in enumerate(natsorted(release.transform_files)):
                    expected_file_hash = self.transform_hashes[i]
                    self.assert_file_integrity(file, expected_file_hash, "md5")

                # Test that transformed files uploaded
                env.run_task(telescope.upload_transformed.__name__)
                for file in release.transform_files:
                    self.assert_blob_integrity(env.transform_bucket, blob_name(file), file)

                # Test that data loaded into BigQuery
                env.run_task(telescope.bq_load.__name__)
                table_id = (
                    f"{self.project_id}.{dataset_id}."
                    f"{bigquery_sharded_table_id(telescope.dag_id, release.release_date)}"
                )
                expected_rows = 20
                self.assert_table_integrity(table_id, expected_rows)

                # Test that all telescope data deleted
                download_folder, extract_folder, transform_folder = (
                    release.download_folder,
                    release.extract_folder,
                    release.transform_folder,
                )
                env.run_task(telescope.cleanup.__name__)
                self.assert_cleanup(download_folder, extract_folder, transform_folder)

    @patch("academic_observatory_workflows.workflows.crossref_metadata_telescope.BaseHook.get_connection")
    def test_download(self, mock_conn):
        """Test download method of release with failing response

        :param mock_conn: Mock Airflow crossref connection
        :return: None.
        """
        mock_conn.return_value = Connection(AirflowConns.CROSSREF, "http://:crossref-token@")
        release = self.release
        with httpretty.enabled():
            httpretty.register_uri(httpretty.GET, release.url, body="", status=400)
            with self.assertRaises(ConnectionError):
                release.download()

    @patch("academic_observatory_workflows.workflows.crossref_metadata_telescope.subprocess.Popen")
    @patch("observatory.platform.utils.workflow_utils.Variable.get")
    def test_extract(self, mock_variable_get, mock_subprocess):
        """Test extract method of release with failing extract command

        :param mock_variable_get: Mock Airflow data path variable
        :param mock_subprocess: Mock the subprocess output
        :return: None.
        """
        mock_variable_get.return_value = "data"
        release = self.release

        mock_subprocess().returncode = 1
        mock_subprocess().communicate.return_value = "stdout".encode(), "stderr".encode()
        with self.assertRaises(AirflowException):
            release.extract()

    @patch("academic_observatory_workflows.workflows.crossref_metadata_telescope.BaseHook.get_connection")
    def test_check_release_exists(self, mock_get_connection):
        """Test the 'check_release_exists' task with different responses.

        :return: None.
        """

        # Mock getting Crossref Metadata Connection
        mock_get_connection.return_value = Connection(password="crossref-token")

        release = self.release
        telescope = CrossrefMetadataTelescope()
        with httpretty.enabled():
            # register 3 responses, successful, release not found and 'other'
            httpretty.register_uri(
                httpretty.HEAD,
                uri=release.url,
                responses=[
                    httpretty.Response(body="", status=302),
                    httpretty.Response(body="", status=404, adding_headers={"reason": "Not Found"}),
                    httpretty.Response(body="", status=400),
                ],
            )

            continue_dag = telescope.check_release_exists(execution_date=release.release_date)
            self.assertTrue(continue_dag)

            continue_dag = telescope.check_release_exists(execution_date=release.release_date)
            self.assertFalse(continue_dag)

            with self.assertRaises(AirflowException):
                telescope.check_release_exists(execution_date=release.release_date)

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

            # Load Transform file
            output_file_path = os.path.join(t, "output.jsonl")
            transform_file(input_file_path, output_file_path)

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
            actual_results = load_jsonl(output_file_path)
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
