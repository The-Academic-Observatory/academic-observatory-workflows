# Copyright 2021-2022 Curtin University
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

# Author: James Diprose


import io
import json
import os
from typing import Dict
from unittest import TestCase
from unittest.mock import MagicMock, patch

from airflow.exceptions import AirflowException
from click.testing import CliRunner

from academic_observatory_workflows.zenodo import make_draft_version, publish_new_version, Zenodo


class MockResponse:
    def __init__(self):
        self.data = None
        self.status_code = None

    def json(self):
        return self.data


class MockZenodo(Zenodo):
    """Mock Zenodo class for running tests."""

    def get_versions(self, conceptrecid: int, all_versions: int = 0, size: int = 10, sort: str = "mostrecent"):
        res = MockResponse()
        res.status_code = 200

        if all_versions == 0:
            res.data = [
                {
                    "conceptrecid": 1044668,
                    "id": 3,
                    "state": "unsubmitted",
                    "created": "2022-04-25T22:16:16.145039+00:00",
                }
            ]
        else:
            res.data = [
                {
                    "conceptrecid": 1044668,
                    "id": 3,
                    "state": "unsubmitted",
                    "created": "2022-04-25T22:16:16.145039+00:00",
                },
                {"conceptrecid": 1044668, "id": 2, "state": "done", "created": "2022-03-25T22:16:16.145039+00:00"},
                {"conceptrecid": 1044668, "id": 1, "state": "done", "created": "2022-02-25T22:16:16.145039+00:00"},
            ]

        return res

    def create_new_version(self, id: str):
        res = MockResponse()
        res.status_code = 201
        return res

    def get_deposition(self, id: str):
        res = MockResponse()
        res.status_code = 200
        res.data = {
            "conceptrecid": 1044668,
            "id": 3,
            "state": "unsubmitted",
            "created": "2022-04-25T22:16:16.145039+00:00",
            "files": [{"id": "596c128f-d240-4008-87b6-cecf143e9d48"}],
            "metadata": {},
        }
        return res

    def delete_file(self, id: str, file_id: str):
        res = MockResponse()
        res.status_code = 204
        return res

    def upload_file(self, id: str, file_path: str):
        res = MockResponse()
        res.status_code = 201
        return res

    def update(self, id: str, data: Dict):
        res = MockResponse()
        res.status_code = 200
        return res

    def publish(self, id: str):
        res = MockResponse()
        res.status_code = 202
        return res


class TestZenodo(TestCase):
    def setUp(self) -> None:
        self.host = "https://localhost"
        self.access_token = "abcdef"
        self.zenodo = Zenodo(host=self.host, access_token=self.access_token)

    def test_make_url(self):
        host = "https://localhost"
        zenodo = Zenodo(host=host)
        url = zenodo.make_url("/api/deposit")
        self.assertEqual("https://localhost/api/deposit", url)

        host = "https://localhost/"
        zenodo = Zenodo(host=host)
        url = zenodo.make_url("api/deposit")
        self.assertEqual("https://localhost/api/deposit", url)

        host = "https://localhost/"
        zenodo = Zenodo(host=host)
        url = zenodo.make_url("/api/deposit")
        self.assertEqual("https://localhost/api/deposit", url)

    @patch("academic_observatory_workflows.zenodo.requests.get")
    def test_get_versions(self, mock_get):
        conceptrecid = 1
        all_versions = 0
        size = 10
        sort = "mostrecent"
        self.zenodo.get_versions(conceptrecid, all_versions=all_versions, size=size, sort=sort)
        mock_get.assert_called_once_with(
            f"{self.host}/api/deposit/depositions",
            params={
                "q": f"conceptrecid:{conceptrecid}",
                "all_versions": all_versions,
                "access_token": self.access_token,
                "sort": sort,
                "size": size,
            },
            timeout=self.zenodo.timeout,
        )

    @patch("academic_observatory_workflows.zenodo.requests.post")
    def test_create_new_version(self, mock_post):
        id = 1
        self.zenodo.create_new_version(id)
        mock_post.assert_called_once_with(
            f"{self.host}/api/deposit/depositions/{id}/actions/newversion", params={"access_token": self.access_token}
        )

    @patch("academic_observatory_workflows.zenodo.requests.get")
    def test_get_deposition(self, mock_get):
        id = 1
        self.zenodo.get_deposition(id)
        mock_get.assert_called_once_with(
            f"{self.host}/api/deposit/depositions/{id}", params={"access_token": self.access_token}
        )

    @patch("academic_observatory_workflows.zenodo.requests.delete")
    def test_delete_file(self, mock_delete):
        id = 1
        file_id = "596c128f-d240-4008-87b6-cecf143e9d48"
        self.zenodo.delete_file(id, file_id)
        mock_delete.assert_called_once_with(
            f"{self.host}/api/deposit/depositions/{id}/files/{file_id}", params={"access_token": self.access_token}
        )

    @patch("academic_observatory_workflows.zenodo.requests.get")
    @patch("academic_observatory_workflows.zenodo.requests.put")
    def test_upload_file(self, mock_put: MagicMock, mock_get: MagicMock):
        with CliRunner().isolated_filesystem() as t:
            # Make file
            file_name = "file.txt"
            file_path = os.path.join(t, file_name)
            with open(file_path, mode="w") as f:
                f.write("Hello World")
            id = 1
            bucket_url = "https://bucket-url.com"
            mock_get.return_value.json.return_value = {"links": {"bucket": bucket_url}}
            self.zenodo.upload_file(id, file_path)

            mock_get.assert_called_once_with(
                f"{self.host}/api/deposit/depositions/{id}",
                json={},
                params={"access_token": self.access_token},
            )
            mock_put.assert_called_once_with(
                f"{bucket_url}/file.txt",
                data=mock_put.call_args.kwargs["data"],
                params={"access_token": self.access_token},
            )

            # Check that correct file was set to be uploaded
            actual_buffered_reader = mock_put.call_args.kwargs["data"]
            self.assertIsInstance(actual_buffered_reader, io.BufferedReader)
            self.assertEqual(file_path, actual_buffered_reader.name)

    @patch("academic_observatory_workflows.zenodo.requests.put")
    def test_update(self, mock_put: MagicMock):
        id = 1
        data = {"title": "hello"}
        self.zenodo.update(id, data)
        mock_put.assert_called_once_with(
            f"{self.host}/api/deposit/depositions/{id}",
            data=json.dumps(data),
            headers={"Content-Type": "application/json"},
            params={"access_token": self.access_token},
        )

    @patch("academic_observatory_workflows.zenodo.requests.post")
    def test_publish(self, mock_post):
        id = 1
        self.zenodo.publish(id)
        mock_post.assert_called_once_with(
            f"{self.host}/api/deposit/depositions/{id}/actions/publish", params={"access_token": self.access_token}
        )

    @patch("academic_observatory_workflows.zenodo.Zenodo.update")
    @patch("academic_observatory_workflows.zenodo.Zenodo.get_deposition")
    @patch("academic_observatory_workflows.zenodo.Zenodo.get_versions")
    @patch("academic_observatory_workflows.zenodo.Zenodo.create_new_version")
    def test_make_draft_version(self, mock_create_new_version, mock_get_versions, mock_get_deposition, mock_update):
        # An error
        res = MockResponse()
        res.status_code = 500
        mock_get_versions.return_value = res
        with self.assertRaises(AirflowException):
            make_draft_version(self.zenodo, 1)

        # No versions found
        res = MockResponse()
        res.data = []
        res.status_code = 200
        mock_get_versions.return_value = res
        with self.assertRaises(AirflowException):
            make_draft_version(self.zenodo, 1)

        # Could not create a new version
        res_get_versions = MockResponse()
        res_get_versions.status_code = 200
        res_get_versions.data = [{"id": 1, "state": "done"}]
        mock_get_versions.return_value = res_get_versions
        res_create_new_version = MockResponse()
        res_create_new_version.status_code = 500
        res_create_new_version.data = {"id": 2, "state": "unsubmitted", "links": {"latest_draft": "/2"}}
        mock_create_new_version.return_value = res_create_new_version
        with self.assertRaises(AirflowException):
            make_draft_version(self.zenodo, 1)

        # Could not get deposition
        res_create_new_version.status_code = 201
        res_get_deposition = MockResponse()
        res_get_deposition.status_code = 500
        res_get_deposition.data = {"id": 2, "state": "done", "metadata": {}}
        mock_get_deposition.return_value = res_get_deposition
        with self.assertRaises(AirflowException):
            make_draft_version(self.zenodo, 2)

        # Could not update
        res_get_deposition.status_code = 200
        res_update = MockResponse()
        res_update.status_code = 500
        mock_update.return_value = res_update
        with self.assertRaises(AirflowException):
            make_draft_version(self.zenodo, 2)

        # Success
        res_update.status_code = 200
        make_draft_version(self.zenodo, 1)

    @patch("academic_observatory_workflows.zenodo.Zenodo.get_deposition")
    @patch("academic_observatory_workflows.zenodo.Zenodo.delete_file")
    @patch("academic_observatory_workflows.zenodo.Zenodo.upload_file")
    @patch("academic_observatory_workflows.zenodo.Zenodo.publish")
    def test_publish_new_version(self, mock_publish, mock_upload_file, mock_delete_file, mock_get_deposition):
        draft_id = 3
        file_path = "/path/to/file"

        # Error getting deposition
        res_get_deposition = MockResponse()
        res_get_deposition.status_code = 500
        mock_get_deposition.return_value = res_get_deposition
        with self.assertRaises(AirflowException):
            publish_new_version(self.zenodo, draft_id, file_path)

        # Error deleting files
        res_get_deposition = MockResponse()
        res_get_deposition.status_code = 200
        res_get_deposition.data = {
            "conceptrecid": 1044668,
            "id": draft_id,
            "state": "unsubmitted",
            "created": "2022-04-25T22:16:16.145039+00:00",
            "files": [{"id": "596c128f-d240-4008-87b6-cecf143e9d48"}],
        }
        mock_get_deposition.return_value = res_get_deposition

        res_delete_file = MockResponse()
        res_delete_file.status_code = 500
        mock_delete_file.return_value = res_delete_file
        with self.assertRaises(AirflowException):
            publish_new_version(self.zenodo, draft_id, file_path)

        # Error uploading new file
        res_delete_file = MockResponse()
        res_delete_file.status_code = 204
        mock_delete_file.return_value = res_delete_file

        res_upload_file = MockResponse()
        res_upload_file.status_code = 500
        mock_upload_file.return_value = res_upload_file
        with self.assertRaises(AirflowException):
            publish_new_version(self.zenodo, draft_id, file_path)

        # Error publish
        res_upload_file = MockResponse()
        res_upload_file.status_code = 201
        mock_upload_file.return_value = res_upload_file

        res_publish = MockResponse()
        res_publish.status_code = 500
        mock_publish.return_value = res_publish

        with self.assertRaises(AirflowException):
            publish_new_version(self.zenodo, draft_id, file_path)

        # Success
        res_publish = MockResponse()
        res_publish.status_code = 202
        mock_publish.return_value = res_publish
        publish_new_version(self.zenodo, draft_id, file_path)
