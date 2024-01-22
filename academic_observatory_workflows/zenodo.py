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

from __future__ import annotations

import json
import os
import os.path
from typing import Dict

import pendulum
import requests
from airflow.exceptions import AirflowException


class Zenodo:
    def __init__(self, host: str = "https://zenodo.org", access_token: str = None, timeout: int = 60):
        self.host = host
        self.access_token = access_token
        self.timeout = timeout

    def make_url(self, path: str):
        # Remove last / from host
        host = self.host
        if self.host[-1] == "/":
            host = self.host[:-1]

        # Add leading / to path
        if path[0] != "/":
            path = f"/{path}"

        return f"{host}{path}"

    def get_versions(self, conceptrecid: int, all_versions: int = 0, size: int = 10, sort: str = "mostrecent"):
        query = f"conceptrecid:{conceptrecid}"
        return requests.get(
            self.make_url("/api/deposit/depositions"),
            params={
                "q": query,
                "all_versions": all_versions,
                "access_token": self.access_token,
                "sort": sort,
                "size": size,
            },
            timeout=self.timeout,
        )

    def create_new_version(self, id: int):
        url = self.make_url(f"/api/deposit/depositions/{id}/actions/newversion")
        return requests.post(url, params={"access_token": self.access_token})

    def get_deposition(self, id: int):
        url = self.make_url(f"/api/deposit/depositions/{id}")
        return requests.get(url, params={"access_token": self.access_token})

    def delete_file(self, id: int, file_id: str):
        url = self.make_url(f"/api/deposit/depositions/{id}/files/{file_id}")
        return requests.delete(url, params={"access_token": self.access_token})

    def update(self, id: int, data: Dict):
        url = self.make_url(f"/api/deposit/depositions/{id}")
        headers = {"Content-Type": "application/json"}
        return requests.put(url, data=json.dumps(data), headers=headers, params={"access_token": self.access_token})

    def upload_file(self, id: int, file_path: str):
        url = self.make_url(f"/api/deposit/depositions/{id}")
        params = {"access_token": self.access_token}
        res = requests.get(
            url,
            json={},
            params=params,
        )
        bucket_url = res.json()["links"]["bucket"]
        with open(file_path, "rb") as data:
            res = requests.put(
                f"{bucket_url}/{os.path.basename(file_path)}",
                data=data,
                params=params,
            )
        return res

    def publish(self, id: int):
        url = self.make_url(f"/api/deposit/depositions/{id}/actions/publish")
        return requests.post(url, params={"access_token": self.access_token})


def make_draft_version(zenodo: Zenodo, conceptrecid: int):
    # Make new draft version
    res = zenodo.get_versions(conceptrecid)
    if res.status_code != 200:
        raise AirflowException(f"zenodo.get_versions status_code {res.status_code}")

    versions = res.json()
    if len(versions) == 0:
        raise AirflowException(f"make_draft_version: at least 1 version must exist")

    latest = versions[0]
    draft_id = latest["id"]
    state = latest["state"]
    if state == "done":
        # If published then create a new draft
        res = zenodo.create_new_version(draft_id)
        if res.status_code != 201:
            raise AirflowException(f"zenodo.create_new_version status_code {res.status_code}")
        draft_id = int(res.json()["links"]["latest_draft"].split("/")[-1])

    # Fetch draft deposition
    res = zenodo.get_deposition(draft_id)
    if res.status_code != 200:
        raise AirflowException(f"zenodo.get_deposition status_code {res.status_code}")

    # Update metadata
    draft = res.json()
    publication_date = pendulum.now().format("YYYY-MM-DD")
    metadata = draft["metadata"]
    metadata["publication_date"] = publication_date
    metadata["version"] = publication_date
    data = {"metadata": metadata}
    res = zenodo.update(draft_id, data)
    if res.status_code != 200:
        raise AirflowException(f"zenodo.update status_code {res.status_code}")


def publish_new_version(zenodo: Zenodo, draft_id: int, file_path: str):
    # Get full deposition which contains files
    res = zenodo.get_deposition(draft_id)
    if res.status_code != 200:
        raise AirflowException(f"zenodo.get_deposition {res.status_code}")
    draft = res.json()

    # Delete existing files
    for file in draft["files"]:
        file_id = file["id"]
        res = zenodo.delete_file(draft_id, file_id)
        if res.status_code != 204:
            raise AirflowException(f"zenodo.delete_file status_code {res.status_code}")

    # Upload new file
    res = zenodo.upload_file(draft_id, file_path)
    if res.status_code != 201:
        raise AirflowException(f"zenodo.upload_file status_code {res.status_code}")

    # Publish
    res = zenodo.publish(draft_id)
    if res.status_code != 202:
        raise AirflowException(f"zenodo.publish status_code {res.status_code}")
