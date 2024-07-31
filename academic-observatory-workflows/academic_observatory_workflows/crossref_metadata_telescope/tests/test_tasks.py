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
from unittest.mock import patch
import uuid

from airflow.exceptions import AirflowException
from airflow.utils.session import provide_session
import httpretty
import pendulum
from tempfile import TemporaryDirectory

from academic_observatory_workflows.config import project_path
from academic_observatory_workflows.crossref_metadata_telescope.release import CrossrefMetadataRelease
from academic_observatory_workflows.crossref_metadata_telescope.tasks import (
    check_release_exists,
    fetch_release,
    download,
    upload_downloaded,
    extract,
    upload_transformed,
    bq_load,
    make_snapshot_url,
    transform,
    transform_file,
    transform_item,
    add_dataset_release,
)
from observatory_platform.airflow.airflow import upsert_airflow_connection, clear_airflow_connections
from observatory_platform.airflow.workflow import CloudWorkspace
from observatory_platform.dataset_api import DatasetAPI
from observatory_platform.date_utils import datetime_normalise
from observatory_platform.google.bigquery import bq_sharded_table_id
from observatory_platform.google.gcs import gcs_blob_name_from_path, gcs_upload_files
from observatory_platform.files import load_jsonl, is_gzip
from observatory_platform.sandbox.sandbox_environment import SandboxEnvironment
from observatory_platform.sandbox.test_utils import SandboxTestCase


FIXTURES_FOLDER = project_path("crossref_metadata_telescope", "tests", "fixtures")
SCHEMA_FOLDER = project_path("crossref_metadata_telescope", "schema")


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

    download_path = os.path.join(FIXTURES_FOLDER, "crossref_metadata.json.tar.gz")
    snapshot_date = pendulum.datetime(2024, 1, 1)

    def test_download(self):
        """Test the download function"""
        env = SandboxEnvironment(
            project_id=os.getenv("TEST_GCP_PROJECT_ID"),
            data_location=os.getenv("TEST_GCP_DATA_LOCATION"),
            env_vars={"CROSSREF_METADATA_API_KEY": ""},
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
            self.assert_file_integrity(release.download_file_path, "047770ae386f3376c08e3975d7f06016", "md5")
            self.assertTrue(is_gzip(release.download_file_path))

    def test_download_no_api_key(self):
        """Test the download function fails when the api key is not available"""
        env = SandboxEnvironment(
            project_id=os.getenv("TEST_GCP_PROJECT_ID"), data_location=os.getenv("TEST_GCP_DATA_LOCATION")
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
            with self.assertRaisesRegex(AirflowException, "The CROSSREF_METADATA_API_KEY"):
                download(release.to_dict())


class TestUploadDownloaded(SandboxTestCase):

    download_path = os.path.join(FIXTURES_FOLDER, "crossref_metadata.json.tar.gz")
    snapshot_date = pendulum.datetime(2024, 1, 1)

    def test_upload_downloaded(self):
        """Tests that the upload_downloaded function uploads to the GCS download bucket"""

        env = SandboxEnvironment(
            project_id=os.getenv("TEST_GCP_PROJECT_ID"), data_location=os.getenv("TEST_GCP_DATA_LOCATION")
        )
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


class TestExtract(unittest.TestCase):

    snapshot_date = pendulum.datetime(2024, 1, 1)
    download_path = os.path.join(FIXTURES_FOLDER, "crossref_metadata.json.tar.gz")

    def test_extract(self):
        """Tests the extract function

        NOTE: If this fails locally, you may need to install pigz with `sudo apt-get install pigz`
        """

        env = SandboxEnvironment(
            project_id=os.getenv("TEST_GCP_PROJECT_ID"), data_location=os.getenv("TEST_GCP_DATA_LOCATION")
        )
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
            extract(release.to_dict())
            self.assertEqual(len(os.listdir(release.extract_folder)), 1)  # The crossref_metadata folder
            self.assertEqual(len(os.listdir(os.path.join(release.extract_folder, "crossref_metadata"))), 5)  # jsons


class TestTransforms(unittest.TestCase):
    """Tests for the transform, transform_file and transform_item functions"""

    snapshot_date = pendulum.datetime(2024, 1, 1)
    input_data_file = os.path.join(FIXTURES_FOLDER, "single_item_extracted.json")
    transformed_data_file = os.path.join(FIXTURES_FOLDER, "single_item_transformed.jsonl")

    def test_transform(self):
        """Test transfom"""
        with open(self.input_data_file) as f:
            input_data = json.load(f)
        expected_output = load_jsonl(self.transformed_data_file)

        env = SandboxEnvironment(
            project_id=os.getenv("TEST_GCP_PROJECT_ID"), data_location=os.getenv("TEST_GCP_DATA_LOCATION")
        )
        release = CrossrefMetadataRelease(
            cloud_workspace=env.cloud_workspace,
            snapshot_date=self.snapshot_date,
            dag_id="crossref_metadata",
            run_id="run_id",
            data_interval_start=pendulum.now(),
            data_interval_end=pendulum.now(),
        )

        with env.create():
            # Save input files
            input_file_path = os.path.join(release.extract_folder, "input1.json")
            with open(input_file_path, mode="w") as f:
                json.dump(input_data, f)
            input_file_path = os.path.join(release.extract_folder, "input2.json")
            with open(input_file_path, mode="w") as f:
                json.dump(input_data, f)
            input_file_path = os.path.join(release.extract_folder, "input3.txt")  # Should be ignored
            with open(input_file_path, mode="w") as f:
                json.dump(input_data, f)

            # Transform the files and make assertions
            transform(release.to_dict(), max_processes=2, batch_size=2)
            expected_files = [
                os.path.join(release.transform_folder, "input1.jsonl"),
                os.path.join(release.transform_folder, "input2.jsonl"),
            ]
            self.assertTrue(os.path.exists(expected_files[0]))
            self.assertTrue(os.path.exists(expected_files[1]))
            self.assertEqual(len(os.listdir(release.transform_folder)), 2)
            # Compare file content
            actual_output = load_jsonl(expected_files[0])
            self.assertEqual(actual_output, expected_output)
            actual_output = load_jsonl(expected_files[1])
            self.assertEqual(actual_output, expected_output)

    def test_transform_file(self):
        """Test transform_file."""
        with open(self.input_data_file) as f:
            input_data = json.load(f)
        expected_output = load_jsonl(self.transformed_data_file)

        with TemporaryDirectory() as t:
            # Save input file
            input_file_path = os.path.join(t, "input.json")
            with open(input_file_path, mode="w") as f:
                json.dump(input_data, f)

            # Check results
            output_file_path = os.path.join(t, "output.jsonl")
            transform_file(input_file_path, output_file_path)
            actual_output = load_jsonl(output_file_path)
            self.assertEqual(expected_output, actual_output)

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


class TestUploadTransformed(SandboxTestCase):

    download_path = os.path.join(FIXTURES_FOLDER, "crossref_metadata.json.tar.gz")
    snapshot_date = pendulum.datetime(2024, 1, 1)

    def test_upload_transformed(self):
        """Tests that the upload_transformed function uploads to the GCS transform bucket"""

        env = SandboxEnvironment(
            project_id=os.getenv("TEST_GCP_PROJECT_ID"), data_location=os.getenv("TEST_GCP_DATA_LOCATION")
        )
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


class TestBqLoad(SandboxTestCase):

    snapshot_date = pendulum.datetime(2024, 1, 1)
    transformed_data_file = os.path.join(FIXTURES_FOLDER, "single_item_transformed.jsonl")

    def test_bq_load(self):
        env = SandboxEnvironment(
            project_id=os.getenv("TEST_GCP_PROJECT_ID"), data_location=os.getenv("TEST_GCP_DATA_LOCATION")
        )
        dataset_id = env.add_dataset(prefix="crossref_metadata_test")

        release = CrossrefMetadataRelease(
            cloud_workspace=env.cloud_workspace,
            snapshot_date=self.snapshot_date,
            dag_id="crossref_metadata",
            run_id="run_id",
            data_interval_start=pendulum.now(),
            data_interval_end=pendulum.now(),
        )
        with env.create():
            # upload the transformed files to the bucket
            file_paths = [
                os.path.join(release.transform_folder, "row_1.jsonl"),
                os.path.join(release.transform_folder, "row_2.jsonl"),
                os.path.join(release.transform_folder, "row_3.json"),
            ]
            shutil.copy(self.transformed_data_file, file_paths[0])
            shutil.copy(self.transformed_data_file, file_paths[1])
            shutil.copy(self.transformed_data_file, file_paths[2])
            success = gcs_upload_files(bucket_name=release.cloud_workspace.transform_bucket, file_paths=file_paths)
            self.assertTrue(success)

            bq_load(
                release.to_dict(),
                bq_dataset_id=dataset_id,
                bq_table_name="crossref_metadata",
                dataset_description="",
                table_description="",
                schema_folder=SCHEMA_FOLDER,
            )
            table_id = bq_sharded_table_id(
                release.cloud_workspace.output_project_id, dataset_id, "crossref_metadata", release.snapshot_date
            )
            self.assert_table_integrity(table_id, expected_rows=2)


class TestAddDatasetRelease(unittest.TestCase):

    snapshot_date = pendulum.datetime(2024, 1, 1)

    def test_add_dataset_release(self):
        env = SandboxEnvironment(
            project_id=os.getenv("TEST_GCP_PROJECT_ID"), data_location=os.getenv("TEST_GCP_DATA_LOCATION")
        )
        api_dataset_id = env.add_dataset(prefix="crossref_metadata_test_api")
        now = pendulum.now()

        release = CrossrefMetadataRelease(
            cloud_workspace=env.cloud_workspace,
            snapshot_date=self.snapshot_date,
            dag_id="crossref_metadata",
            run_id="run_id",
            data_interval_start=self.snapshot_date,
            data_interval_end=self.snapshot_date.end_of("month"),
        )
        with env.create():
            expected_api_release = {
                "dag_id": "crossref_metadata",
                "entity_id": "crossref_metadata",
                "dag_run_id": "run_id",
                "created": datetime_normalise(now),
                "modified": datetime_normalise(now),
                "data_interval_start": "2024-01-01T00:00:00+00:00",
                "data_interval_end": "2024-01-31T23:59:59+00:00",
                "snapshot_date": "2024-01-01T00:00:00+00:00",
                "partition_date": None,
                "changefile_start_date": None,
                "changefile_end_date": None,
                "sequence_start": None,
                "sequence_end": None,
                "extra": {},
            }
            api = DatasetAPI(bq_project_id=release.cloud_workspace.project_id, bq_dataset_id=api_dataset_id)
            api.seed_db()

            # Should not be any releases in the API before the task is run
            self.assertEqual(len(api.get_dataset_releases(dag_id=release.dag_id, entity_id="crossref_metadata")), 0)
            with patch("academic_observatory_workflows.crossref_metadata_telescope.tasks.pendulum.now") as mock_now:
                mock_now.return_value = now
                add_dataset_release(release.to_dict(), api_bq_dataset_id=api_dataset_id)

            # Should be one release in the API
            api_releases = api.get_dataset_releases(dag_id=release.dag_id, entity_id="crossref_metadata")
        self.assertEqual(len(api_releases), 1)
        self.assertEqual(expected_api_release, api_releases[0].to_dict())
