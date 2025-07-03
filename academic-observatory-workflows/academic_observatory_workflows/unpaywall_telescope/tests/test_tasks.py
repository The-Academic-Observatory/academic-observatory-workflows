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

import os
from unittest.mock import patch

import pendulum
import vcr
from airflow import AirflowException

from observatory_platform.sandbox.test_utils import SandboxTestCase
from academic_observatory_workflows.config import project_path
from academic_observatory_workflows.unpaywall_telescope.release import Changefile
from academic_observatory_workflows.unpaywall_telescope.tasks import (
    snapshot_url,
    get_snapshot_file_name,
    changefiles_url,
    changefile_download_url,
    get_unpaywall_changefiles,
    unpaywall_filename_to_datetime,
    UNPAYWALL_BASE_URL,
)


FIXTURES_FOLDER = project_path("unpaywall_telescope", "tests", "fixtures")


class TestUnpaywallUtils(SandboxTestCase):
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
        url = snapshot_url(UNPAYWALL_BASE_URL, "my-api-key")
        self.assertEqual(f"https://api.unpaywall.org/feed/snapshot?api_key=my-api-key", url)

    def test_changefiles_url(self):
        url = changefiles_url(UNPAYWALL_BASE_URL, "my-api-key")
        self.assertEqual(f"https://api.unpaywall.org/feed/changefiles?interval=day&api_key=my-api-key", url)

    def test_changefile_download_url(self):
        url = changefile_download_url(
            UNPAYWALL_BASE_URL, "changed_dois_with_versions_2020-03-11T005336.jsonl.gz", "my-api-key"
        )
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

    @patch("observatory_platform.url_utils.get_http_text_response")
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
        url = changefiles_url(UNPAYWALL_BASE_URL, "my-api-key")
        changefiles = get_unpaywall_changefiles(url)
        self.assertEqual(expected_changefiles, changefiles)

    def test_get_snapshot_file_name(self):
        # This cassette was run with a valid api key, which is not saved into the cassette or code
        # Set UNPAYWALL_API_KEY to use the real api key
        with vcr.use_cassette(
            os.path.join(FIXTURES_FOLDER, "get_snapshot_file_name_success.yaml"),
            filter_query_parameters=["api_key"],
        ):
            filename = get_snapshot_file_name(UNPAYWALL_BASE_URL, os.getenv("UNPAYWALL_API_KEY", "my-api-key"))
            self.assertEqual("unpaywall_snapshot_2025-07-02T154512.jsonl.gz", filename)

        with vcr.use_cassette(
            os.path.join(FIXTURES_FOLDER, "get_snapshot_file_name_missing_location.yaml"),
            filter_query_parameters=["api_key"],
        ):
            with self.assertRaisesRegex(AirflowException, 'Missing "location"'):
                get_snapshot_file_name(UNPAYWALL_BASE_URL, os.getenv("UNPAYWALL_API_KEY", "my-api-key"))

        # An invalid API key
        with vcr.use_cassette(
            os.path.join(FIXTURES_FOLDER, "get_snapshot_file_name_failure.yaml"),
            filter_query_parameters=["api_key"],
        ):
            with self.assertRaisesRegex(AirflowException, "Unexpected status code"):
                get_snapshot_file_name(UNPAYWALL_BASE_URL, "invalid-api-key")
