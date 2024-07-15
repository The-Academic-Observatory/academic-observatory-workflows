# Copyright 2020-2024 Curtin University
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

# Author: Aniek Roelofs, James Diprose, Keegan Smith

import json
import os
import unittest
from tempfile import TemporaryDirectory

import httpretty
import pendulum
from airflow.models import Connection
from airflow.utils.state import State
from click.testing import CliRunner

from academic_observatory_workflows.config import project_path
from academic_observatory_workflows.crossref_metadata_telescope.tasks import (
    check_release_exists,
    download,
    extract,
    CrossrefMetadataRelease,
    bq_load,
    make_snapshot_url,
    transform,
    transform_file,
    transform_item,
)
from observatory_platform.dataset_api import get_dataset_releases
from observatory_platform.google.bigquery import bq_sharded_table_id
from observatory_platform.config import module_file_path
from observatory_platform.files import is_gzip, list_files, load_jsonl
from observatory_platform.google.gcs import gcs_blob_name_from_path
from observatory_platform.airflow.workflow import Workflow
from observatory_platform.sandbox.sandbox_environment import find_free_port, ObservatoryEnvironment, ObservatoryTestCase


FIXTURES_FOLDER = project_path("crossref_metadata_telescope", "tests", "fixtures")


class TestCheckReleaseExists(unittest.TestCase):
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


class TestTransformFile(unittest.testcase):
    """Tests for the transform file function"""

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
            output_file_path = os.path.join(t, "output.jsonl")
            transform_file(input_file_path, output_file_path)
            actual_results = load_jsonl(output_file_path)
            self.assertEqual(expected_results, actual_results)


class TestTransformItem(unittest.testcase):
    """Tests for the transform item function"""

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

    def test_transform_item_date_parts(self):
        # date-parts
        item = {"date-parts": [[2021, 1, 1]]}
        expected = {"date_parts": [2021, 1, 1]}
        actual = transform_item(item)
        self.assertEqual(expected, actual)

    def test_transform_item_date_parts_none(self):
        # date-parts with None inside inner list
        item = {"date-parts": [[None]]}
        expected = {"date_parts": []}
        actual = transform_item(item)
        self.assertEqual(expected, actual)

    def test_transform_item_date_parts_list(self):
        # list with date-parts
        item = {"hello-world": {"hello-world": [{"date-parts": [[2021, 1, 1]]}, {"date-parts": [[None]]}]}}
        expected = {"hello_world": {"hello_world": [{"date_parts": [2021, 1, 1]}, {"date_parts": []}]}}
        actual = transform_item(item)
        self.assertEqual(expected, actual)
