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

# Author: James Diprose

from dataclasses import dataclass
import json
import os

import kubernetes

from observatory_platform.config import module_file_path


class Tag:
    """DAG tag."""

    academic_observatory = "academic-observatory"
    data_quality = "data-quality"


def project_path(*subpaths: str) -> str:
    """Make a path to a file or folder within the Academic Observatory Workflows project.

    :param subpaths: any sub paths.
    :return: a path to a file or folder.
    """

    return os.path.join(construct_module_path("academic_observatory_workflows"), *subpaths)


def construct_module_path(*parts: str) -> str:
    """Constructs the full module path given parts of a path."""

    module_path = ".".join(list(parts))
    file_path = module_file_path(module_path)
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"construct_module_path: directory {file_path} does not exist!")

    return file_path


@dataclass
class TestConfig:
    """Common parameters for end to end and unit testing"""

    gcp_project_id: str = os.getenv("TEST_GCP_PROJECT_ID")
    gcp_data_location: str = os.getenv("TEST_GCP_DATA_LOCATION")
    flask_service_url: str = "http://flask-app-service"
    gke_image: str = "academic-observatory:test"
    gke_namespace: str = "default"
    gke_volume_name: str = "ao-pvc"
    gke_volume_path: str = "/home/astro/data"
    gke_cluster_connection: dict = dict(
        conn_id="gke_cluster",
        conn_type="kubernetes",
        extra=json.dumps(
            {
                "extra__kubernetes__namespace": "default",
                "extra__kubernetes__kube_config_path": kubernetes.config.kube_config.KUBE_CONFIG_DEFAULT_LOCATION,
                "extra__kubernetes__context": "minikube",
            }
        ),
    )
