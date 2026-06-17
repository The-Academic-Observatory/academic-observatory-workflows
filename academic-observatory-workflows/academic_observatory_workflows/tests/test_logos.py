# Copyright 2021 Curtin University. All Rights Reserved.
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

# Author: Keegan Smith


import os
import tempfile
import unittest
from io import BytesIO
from unittest.mock import MagicMock, patch

from academic_observatory_workflows.config import project_path
from academic_observatory_workflows.logos import download_logo

FIXTURES_FOLDER = project_path("tests", "fixtures")


class TestLogos(unittest.TestCase):
    @patch("academic_observatory_workflows.logos.requests.get")
    def test_logo_download(self, mock_get):
        """Test logo_download"""

        # Success case: 200 OK
        domain = "example.com"
        key = "test_key"
        size = 128
        fmt = "png"
        content = b"fake_logo_data"
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raw = BytesIO(content)
        mock_get.return_value = mock_response

        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = os.path.join(temp_dir, "logo.png")
            result = download_logo(domain=domain, key=key, file_path=file_path, size=size, fmt=fmt)

            self.assertTrue(result)
            with open(file_path, "rb") as f:
                self.assertEqual(f.read(), content)

            expected_url = f"https://img.logo.dev/{domain}"
            expected_params = (("size", size), ("format", fmt), ("token", key))
            mock_get.assert_called_once_with(expected_url, params=expected_params, stream=True)

        # 404 Case: Not Found
        mock_get.reset_mock()
        mock_response.status_code = 404
        result = download_logo(domain=domain, key=key, file_path="path/to/nothing", size=size, fmt=fmt)
        self.assertFalse(result)

        # Other Error Case: 500 Internal Server Error
        mock_get.reset_mock()
        mock_response.status_code = 500
        mock_response.reason = "Internal Server Error"
        mock_response.url = f"https://img.logo.dev/{domain}"
        with self.assertLogs("academic_observatory_workflows.logos", level="WARNING") as cm:
            result = download_logo(domain=domain, key=key, file_path="path/to/nothing", size=size, fmt=fmt)
            self.assertFalse(result)
            self.assertIn("status_code=500, reason=Internal Server Error", cm.output[0])
