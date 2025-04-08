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

# Author: Aniek Roelofs

import os
import json
from typing import Any, Tuple
from academic_observatory_workflows_api.server.elastic import (
    get_pit_id,
    query_elasticsearch
)
from observatory.api.utils.auth_utils import set_auth_session, requires_auth
from flask import session, redirect, render_template
from six.moves.urllib.parse import urlencode
from flask import url_for, current_app


Response = Tuple[Any, int]
session_ = None  # Global session


# Here we're using the /callback route.
def callback_handling():
    set_auth_session(current_app.auth0)
    return redirect("/dashboard")


# Controllers API
def home():
    return render_template('home.html')


def login():
    return current_app.auth0.authorize_redirect(redirect_uri=url_for(
        '.academic_observatory_workflows_api_server_api_callback_handling', _external=True),
                                                audience="https://ao.api.observatory.academy") # audience of API
    # to api audience.


def logout():
    # Clear session stored data
    session.clear()
    # Redirect user to logout endpoint
    params = {'returnTo': url_for('.academic_observatory_workflows_api_server_api_home', _external=True), 'client_id': os.environ.get("AUTH0_CLIENT_ID")}
    return redirect(current_app.auth0.api_base_url + '/v2/logout?' + urlencode(params))


@requires_auth
def dashboard():
    return render_template('dashboard.html',
                           userinfo_pretty=json.dumps(session['jwt_payload'], indent=4))


def pit_id_agg(agg: str):
    return get_pit_id(agg=agg, subagg=None)


def pit_id_subagg(agg: str, subagg: str):
    return get_pit_id(agg=agg, subagg=subagg)


def query_agg(agg: str):
    return query_elasticsearch(agg=agg, subagg=None)


def query_subagg(agg: str, subagg: str):
    return query_elasticsearch(agg=agg, subagg=subagg)
