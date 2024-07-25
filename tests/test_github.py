# Copyright 2021 Curtin University
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

import unittest
from unittest.mock import patch

import requests
from airflow import AirflowException

from academic_observatory_workflows.github import trigger_repository_dispatch


class TestGithub(unittest.TestCase):
    @patch("academic_observatory_workflows.github.requests.post")
    def test_trigger_repository_dispatch(self, mock_requests_post):
        response = requests.Response()
        response.status_code = 204
        mock_requests_post.return_value = response

        trigger_repository_dispatch(org="org", repo_name="my-repo", token="my-token", event_type="my-event-type")
        mock_requests_post.called_once()

        response.status_code = 401
        with self.assertRaises(AirflowException):
            trigger_repository_dispatch(org="org", repo_name="my-repo", token="my-token", event_type="my-event-type")
