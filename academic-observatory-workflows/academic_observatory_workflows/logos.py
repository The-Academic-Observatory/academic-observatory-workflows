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

# Author: James Diprose, Keegan Smith

import logging
import shutil

import requests

logger = logging.getLogger(__name__)


def download_logo(*, domain: str, key: str, file_path: str, size: int = 24, fmt: str = "jpg") -> bool:
    """Download a company logo from the logo.dev Logo API tool: https://www.logo.dev

    :param domain: the URL of the company domain + suffix e.g. spotify.com
    :param key: The API key for logo.dev
    :param file_path: the path where the file should be saved.
    :param size: the desired size (pixels) of the logo.
    :param fmt: the format of the logo, either jpg or png.
    :return: whether the logo was found or not.
    """

    params = (("size", size), ("format", fmt), ("token", key))
    response = requests.get(f"https://img.logo.dev/{domain}", params=params, stream=True)
    if response.status_code == 200:
        with open(file_path, "wb") as f:
            shutil.copyfileobj(response.raw, f)
        del response
        return True
    elif response.status_code != 404:
        logger.warning(f"{response.url}: status_code={response.status_code}, reason={response.reason}")
    return False
