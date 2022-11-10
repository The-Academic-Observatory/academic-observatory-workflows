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

import requests


def trigger_repository_dispatch(*, token: str, event_type: str):
    """Trigger a Github repository dispatch event.

    :param event_type: the event type
    :param token: the Github token.
    :return: the response.
    """

    headers = {
        "Accept": "application/vnd.github.v3+json",
        "Authorization": f"token {token}",
    }
    data = {"event_type": event_type}

    return requests.post(
        "https://api.github.com/repos/The-Academic-Observatory/coki-oa-web/dispatches",
        headers=headers,
        data=json.dumps(data),
    )
