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
from pathlib import Path
import shutil
import unittest
import uuid

from airflow.exceptions import AirflowException
from airflow.utils.session import provide_session
import httpretty
import pendulum
from click.testing import CliRunner

from academic_observatory_workflows.config import project_path
from academic_observatory_workflows.crossref_metadata_telescope.release import CrossrefMetadataRelease
from academic_observatory_workflows.crossref_metadata_telescope.tasks import (
    check_release_exists,
    fetch_release,
    download,
    upload_downloaded,
    extract,
    upload_transformed,
    CrossrefMetadataRelease,
    bq_load,
    make_snapshot_url,
    transform,
    transform_file,
    transform_item,
)
from observatory_platform.airflow.airflow import upsert_airflow_connection, clear_airflow_connections
from observatory_platform.airflow.workflow import CloudWorkspace
from observatory_platform.google.gcs import gcs_blob_name_from_path
from observatory_platform.files import load_jsonl, is_gzip
from observatory_platform.sandbox.sandbox_environment import SandboxEnvironment
from observatory_platform.sandbox.test_utils import SandboxTestCase


FIXTURES_FOLDER = project_path("crossref_metadata_telescope", "tests", "fixtures")


class TestFetchRelease(unittest.TestCase):
    @provide_session
    def test_fetch_release(self, session=None):
        """Tests the fetch_release function"""

        clear_airflow_connections()
        upsert_airflow_connection(conn_id="crossref_metadata", conn_type="http")

        cloud_workspace = CloudWorkspace(
            project_id="my_project",
            download_bucket="download_bucket",
            transform_bucket="transform_bucket",
            data_location="us",
        )
        data_interval_start = pendulum.datetime(2024, 1, 1)
        url = make_snapshot_url(data_interval_start)
        with httpretty.enabled():
            httpretty.register_uri(httpretty.HEAD, uri=url, responses=[httpretty.Response(body="", status=302)])
            release = fetch_release(
                cloud_workspace=cloud_workspace,
                crossref_metadata_conn_id="crossref_metadata",
                dag_id="dag_id",
                run_id="run_id",
                data_interval_start=data_interval_start,
                data_interval_end=pendulum.datetime(2024, 1, 31),
            )
        expected_release = {
            "cloud_workspace": {
                "data_location": "us",
                "download_bucket": "download_bucket",
                "output_project_id": "my_project",
                "project_id": "my_project",
                "transform_bucket": "transform_bucket",
            },
            "dag_id": "dag_id",
            "data_interval_end": "2024-01-31 00:00:00",
            "data_interval_start": "2024-01-01 00:00:00",
            "run_id": "run_id",
            "snapshot_date": "2024-01-31 23:59:59",
        }
        self.assertEqual(release, expected_release)

    @provide_session
    def test_fetch_release_400(self, session=None):
        """Tests the fetch_release function fails appropriately"""

        clear_airflow_connections()
        upsert_airflow_connection(conn_id="crossref_metadata", conn_type="http")

        cloud_workspace = CloudWorkspace(
            project_id="my_project",
            download_bucket="download_bucket",
            transform_bucket="transform_bucket",
            data_location="us",
        )
        data_interval_start = pendulum.datetime(2024, 1, 1)
        url = make_snapshot_url(data_interval_start)
        with httpretty.enabled():
            httpretty.register_uri(httpretty.HEAD, uri=url, responses=[httpretty.Response(body="", status=400)])
            with self.assertRaisesRegex(AirflowException, "Release doesn't exist"):
                fetch_release(
                    cloud_workspace=cloud_workspace,
                    crossref_metadata_conn_id="crossref_metadata",
                    dag_id="dag_id",
                    run_id="run_id",
                    data_interval_start=data_interval_start,
                    data_interval_end=pendulum.datetime(2024, 1, 31),
                )


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


class TestDownload(SandboxTestCase):

    project_id = os.getenv("TEST_GCP_PROJECT_ID")
    data_location = os.getenv("TEST_GCP_DATA_LOCATION")
    download_path = os.path.join(FIXTURES_FOLDER, "crossref_metadata.json.tar.gz")
    snapshot_date = pendulum.datetime(2024, 1, 1)

    def test_download(self):
        """Test the download function"""
        env = SandboxEnvironment(
            project_id=self.project_id, data_location=self.data_location, env_vars={"CROSSREF_METADATA_API_KEY": ""}
        )
        with env.create():
            release = CrossrefMetadataRelease(
                cloud_workspace=env.cloud_workspace,
                snapshot_date=self.snapshot_date,
                dag_id="crossref_metadata",
                run_id="run_id",
                data_interval_start=pendulum.now(),
                data_interval_end=pendulum.now(),
            )
            with httpretty.enabled():  # Mock the http return
                with open(self.download_path, "rb") as f:
                    body = f.read()
                url = make_snapshot_url(snapshot_date=self.snapshot_date)
                httpretty.register_uri(httpretty.GET, url, body=body)
                download(release.to_dict())
            expected_file_hash = "047770ae386f3376c08e3975d7f06016"
            self.assert_file_integrity(release.download_file_path, expected_file_hash, "md5")
            self.assertTrue(is_gzip(release.download_file_path))

    def test_download_no_api_key(self):
        """Test the download function fails when the api key is not available"""
        env = SandboxEnvironment(project_id=self.project_id, data_location=self.data_location)
        with env.create():
            release = CrossrefMetadataRelease(
                cloud_workspace=env.cloud_workspace,
                snapshot_date=self.snapshot_date,
                dag_id="crossref_metadata",
                run_id="run_id",
                data_interval_start=pendulum.now(),
                data_interval_end=pendulum.now(),
            )
            with self.assertRaisesRegex(AirflowException, "The CROSSREF_METADATA_API_KEY"):
                download(release.to_dict())


class TestUploadDownloaded(SandboxTestCase):

    project_id = os.getenv("TEST_GCP_PROJECT_ID")
    data_location = os.getenv("TEST_GCP_DATA_LOCATION")
    download_path = os.path.join(FIXTURES_FOLDER, "crossref_metadata.json.tar.gz")
    snapshot_date = pendulum.datetime(2024, 1, 1)

    def test_upload_downloaded(self):
        """Tests that the upload_downloaded function uploads to the GCS download bucket"""

        env = SandboxEnvironment(project_id=self.project_id, data_location=self.data_location)
        release = CrossrefMetadataRelease(
            cloud_workspace=env.cloud_workspace,
            snapshot_date=self.snapshot_date,
            dag_id="crossref_metadata",
            run_id="run_id",
            data_interval_start=pendulum.now(),
            data_interval_end=pendulum.now(),
        )
        with env.create():
            shutil.copy(self.download_path, release.download_file_path)
            upload_downloaded(release.to_dict())
            blob_name = gcs_blob_name_from_path(release.download_file_path)
            self.assert_blob_exists(env.download_bucket, blob_name)
            self.assert_blob_integrity(env.download_bucket, blob_name, release.download_file_path)


class TestUploadTransformed(SandboxTestCase):

    project_id = os.getenv("TEST_GCP_PROJECT_ID")
    data_location = os.getenv("TEST_GCP_DATA_LOCATION")
    download_path = os.path.join(FIXTURES_FOLDER, "crossref_metadata.json.tar.gz")
    snapshot_date = pendulum.datetime(2024, 1, 1)

    def test_upload_transformed(self):
        """Tests that the upload_transformed function uploads to the GCS transform bucket"""

        env = SandboxEnvironment(project_id=self.project_id, data_location=self.data_location)
        release = CrossrefMetadataRelease(
            cloud_workspace=env.cloud_workspace,
            snapshot_date=self.snapshot_date,
            dag_id="crossref_metadata",
            run_id="run_id",
            data_interval_start=pendulum.now(),
            data_interval_end=pendulum.now(),
        )

        with env.create():
            # Set up some files
            files = [
                os.path.join(release.transform_folder, str(uuid.uuid4())) + ".jsonl",
                os.path.join(release.transform_folder, str(uuid.uuid4())) + ".jsonl",
                os.path.join(release.transform_folder, str(uuid.uuid4())) + ".json",  # Should not be uploaded
            ]
            for f in files:
                Path(f).touch()
            blob_names = [gcs_blob_name_from_path(f) for f in files]

            # Run the upload function
            upload_transformed(release.to_dict())

            # Check files exist/do not exist
            self.assert_blob_exists(env.transform_bucket, blob_names[0])
            self.assert_blob_exists(env.transform_bucket, blob_names[1])
            bucket = self.storage_client.get_bucket(env.transform_bucket)
            blob = bucket.blob(blob_names[2])
            self.assertFalse(blob.exists())

            # Check file integrity
            self.assert_blob_integrity(env.transform_bucket, blob_names[0], files[0])
            self.assert_blob_integrity(env.transform_bucket, blob_names[1], files[1])


class TestTransformFile(unittest.TestCase):
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


class TestTransformItem(unittest.TestCase):
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
