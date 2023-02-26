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

from __future__ import annotations

import json
import logging
import os

from academic_observatory_workflows.config import elastic_mappings_folder, load_elastic_mappings_ao
from observatory.platform.files import load_file
from observatory.platform.observatory_config import Workflow
from observatory.platform.observatory_environment import ObservatoryEnvironment, ObservatoryTestCase
from observatory.platform.utils.jinja2_utils import render_template


class TestElasticImportWorkflow(ObservatoryTestCase):
    """Tests for the Elastic Import Workflow"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.dag_id = "elastic_import_observatory"
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")

    def test_load_elastic_mappings_ao(self):
        """Test load_elastic_mappings_ao"""

        path = elastic_mappings_folder()
        aggregate = "author"
        expected = [
            ("ao_dois", load_file(os.path.join(path, "ao-dois-mappings.json"))),
            (
                "ao_author_access_types",
                render_template(
                    os.path.join(path, "ao-access-types-mappings.json.jinja2"),
                    aggregate=aggregate,
                    facet="access_types",
                ),
            ),
            (
                "ao_author_disciplines",
                render_template(
                    os.path.join(path, "ao-disciplines-mappings.json.jinja2"), aggregate=aggregate, facet="disciplines"
                ),
            ),
            (
                "ao_author_events",
                render_template(
                    os.path.join(path, "ao-events-mappings.json.jinja2"), aggregate=aggregate, facet="events"
                ),
            ),
            (
                "ao_author_metrics",
                render_template(
                    os.path.join(path, "ao-metrics-mappings.json.jinja2"), aggregate=aggregate, facet="metrics"
                ),
            ),
            (
                "ao_author_output_types",
                render_template(
                    os.path.join(path, "ao-output-types-mappings.json.jinja2"),
                    aggregate=aggregate,
                    facet="output_types",
                ),
            ),
            (
                "ao_author_unique_list",
                render_template(
                    os.path.join(path, "ao-unique-list-mappings.json.jinja2"), aggregate=aggregate, facet="unique_list"
                ),
            ),
            (
                "ao_author_output_types",
                render_template(
                    os.path.join(path, "ao-output-types-mappings.json.jinja2"),
                    aggregate=aggregate,
                    facet="output_types",
                ),
            ),
            (
                "ao_author_countries",
                render_template(
                    os.path.join(path, "ao-relations-mappings.json.jinja2"), aggregate=aggregate, facet="countries"
                ),
            ),
            (
                "ao_author_funders",
                render_template(
                    os.path.join(path, "ao-relations-mappings.json.jinja2"), aggregate=aggregate, facet="funders"
                ),
            ),
            (
                "ao_author_groupings",
                render_template(
                    os.path.join(path, "ao-relations-mappings.json.jinja2"), aggregate=aggregate, facet="groupings"
                ),
            ),
            (
                "ao_author_institutions",
                render_template(
                    os.path.join(path, "ao-relations-mappings.json.jinja2"), aggregate=aggregate, facet="institutions"
                ),
            ),
            (
                "ao_author_journals",
                render_template(
                    os.path.join(path, "ao-relations-mappings.json.jinja2"), aggregate=aggregate, facet="journals"
                ),
            ),
            (
                "ao_author_publishers",
                render_template(
                    os.path.join(path, "ao-relations-mappings.json.jinja2"), aggregate=aggregate, facet="publishers"
                ),
            ),
        ]

        for table_id, expected_mappings_str in expected:
            logging.info(table_id)
            expected_mappings = json.loads(expected_mappings_str)
            actual_mappings = load_elastic_mappings_ao(path, table_id)
            self.assertEqual(expected_mappings, actual_mappings)

    def test_dag_load(self):
        """Test that workflow can be loaded from a DAG bag."""

        env = ObservatoryEnvironment(
            workflows=[
                Workflow(
                    dag_id=self.dag_id,
                    name="Elastic Import Observatory Workflow",
                    class_name="observatory.platform.workflows.elastic_import_workflow.ElasticImportWorkflow",
                    cloud_workspace=self.fake_cloud_workspace,
                    kwargs=dict(
                        sensor_dag_ids=["doi"],
                        kibana_spaces=["test-space"],
                        elastic_import_config="academic_observatory_workflows.config.ELASTIC_IMPORT_CONFIG",
                        tags=["academic-observatory"],
                    ),
                )
            ]
        )

        with env.create():
            self.assert_dag_load_from_config(self.dag_id)
