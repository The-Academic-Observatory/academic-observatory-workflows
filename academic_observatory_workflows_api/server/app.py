# Copyright 2020-2021 Curtin University
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

# Author: Aniek Roelofs, James Diprose

from __future__ import annotations

import logging
import os

import connexion
from observatory.api.cli.openapi_renderer import OpenApiRenderer
from observatory.api.utils.exception_utils import AuthError

from flask import jsonify, current_app
from authlib.integrations.flask_client import OAuth


def create_app() -> connexion.App:
    """Create a Connexion App.

    :return: the Connexion App.
    """

    logging.info("Creating app")

    # Create the application instance and don't sort JSON output alphabetically
    conn_app = connexion.App(__name__)
    conn_app.app.config["JSON_SORT_KEYS"] = False
    conn_app.app.config["JSONIFY_PRETTYPRINT_REGULAR"] = False
    # Use os.urandom(24).hex() to generate secret key
    conn_app.app.secret_key = os.environ.get("SECRET_KEY")

    # Add the OpenAPI specification
    specification_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "openapi.yaml.jinja2")
    builder = OpenApiRenderer(specification_path, usage_type="backend")
    specification = builder.to_dict()
    conn_app.add_api(specification)

    oauth = OAuth(conn_app.app)
    auth0 = oauth.register(
        'auth0',
        client_id=os.environ.get("AUTH0_CLIENT_ID"),
        client_secret=os.environ.get("AUTH0_CLIENT_SECRET"),
        api_base_url='https://dev-canowprn.us.auth0.com',
        access_token_url='https://dev-canowprn.us.auth0.com/oauth/token',
        authorize_url='https://dev-canowprn.us.auth0.com/authorize',
        client_kwargs={
            'scope': 'openid profile email offline_access',
        },
    )
    with conn_app.app.app_context():
        current_app.auth0 = auth0

    return conn_app


# Create the Connexion App
app = create_app()


@app.app.errorhandler(AuthError)
def handle_exception(err):
    """Return custom JSON when APIError or its children are raised"""
    response = {"error": err.code, "description": err.description}
    # Add some logging so that we can monitor different types of errors
    app.app.logger.error(f"{err.code}: {err.description}")
    return jsonify(response), err.status_code


@app.app.errorhandler(500)
def handle_exception(err):
    """Return JSON instead of HTML for any other server error"""
    response = {"error": "Unknown Exception."}
    app.app.logger.error(f"Unknown Exception: {str(err)}")
    return jsonify(response), 500


# Only called when testing locally
if __name__ == "__main__":
    app.run(host='0.0.0.0')
