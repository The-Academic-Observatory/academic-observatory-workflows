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

import os

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
