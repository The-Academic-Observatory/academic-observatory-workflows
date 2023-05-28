# Copyright 2020 Curtin University
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

import json
import os
from typing import List

from observatory.platform.config import module_file_path
from observatory.platform.elastic.elastic import KeepInfo, KeepOrder
from observatory.platform.elastic.kibana import TimeField
from observatory.platform.utils.jinja2_utils import render_template
from observatory.platform.workflows.elastic_import_workflow import load_elastic_mappings_simple, ElasticImportConfig


class Tag:
    """DAG tag."""

    academic_observatory = "academic-observatory"


def test_fixtures_folder(*subdirs) -> str:
    """Get the path to the Academic Observatory Workflows test data directory.

    :return: the test data directory.
    """

    base_path = module_file_path("academic_observatory_workflows.fixtures")
    return os.path.join(base_path, *subdirs)


def schema_folder() -> str:
    """Return the path to the database schema template folder.

    :return: the path.
    """

    return module_file_path("academic_observatory_workflows.database.schema")


def sql_folder() -> str:
    """Return the path to the workflow SQL template folder.

    :return: the path.
    """

    return module_file_path("academic_observatory_workflows.database.sql")


def elastic_mappings_folder() -> str:
    """Get the Elasticsearch mappings path.

    :return: the elastic search schema path.
    """

    return module_file_path("academic_observatory_workflows.database.mappings")


def load_elastic_mappings_ao(path: str, table_prefix: str, simple_prefixes: List = None):
    """For the Observatory project, load the Elastic mappings for a given table_prefix.
    :param path: the path to the mappings files.
    :param table_prefix: the table_id prefix (without shard date).
    :param simple_prefixes: the prefixes of mappings to load with the load_elastic_mappings_simple function.
    :return: the rendered mapping as a Dict.
    """

    # Set default simple_prefixes
    if simple_prefixes is None:
        simple_prefixes = ["ao_doi"]

    if not table_prefix.startswith("ao"):
        raise ValueError("Table must begin with 'ao'")
    elif any([table_prefix.startswith(prefix) for prefix in simple_prefixes]):
        return load_elastic_mappings_simple(path, table_prefix)
    else:
        prefix, aggregate, facet = table_prefix.split("_", 2)
        mappings_file_name = "ao-relations-mappings.json.jinja2"
        is_fixed_facet = facet in ["unique_list", "access_types", "disciplines", "output_types", "events", "metrics"]
        if is_fixed_facet:
            mappings_file_name = f"ao-{facet.replace('_', '-')}-mappings.json.jinja2"
        mappings_path = os.path.join(path, mappings_file_name)
        return json.loads(render_template(mappings_path, aggregate=aggregate, facet=facet))


ELASTIC_IMPORT_CONFIG = ElasticImportConfig(
    elastic_mappings_path=elastic_mappings_folder(),
    elastic_mappings_func=load_elastic_mappings_ao,
    kibana_time_fields=[TimeField("^.*$", "published_year")],
    index_keep_info={
        "": KeepInfo(ordering=KeepOrder.newest, num=2),
        "ao": KeepInfo(ordering=KeepOrder.newest, num=2),
    },
)
