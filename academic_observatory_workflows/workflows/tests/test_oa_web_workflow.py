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
import json
import os
import vcr
from unittest import TestCase
from unittest.mock import patch

import pandas as pd
import pendulum
from academic_observatory_workflows.config import test_fixtures_folder
from click.testing import CliRunner

from academic_observatory_workflows.workflows.oa_web_workflow import OaWebRelease, OaWebWorkflow

from observatory.platform.utils.test_utils import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    module_file_path,
)


class TestOaWebFunctions(TestCase):
    def test_draw_change_points(self):
        pass

    def test_bq_query_to_gcs(self):
        pass

    def test_calc_percentages(self):
        pass


class TestOaWebRelease(TestCase):
    def setUp(self) -> None:
        self.release = OaWebRelease(dag_id="dag", project_id="project", release_date=pendulum.now())

    # def test_make_index_table_data(self):
    #     self.release.make_index_table_data()
    #
    # def test_save_entity_details_data(self):
    #     self.release.save_entity_details_data()
    #

    @patch("academic_observatory_workflows.workflows.oa_web_workflow.Variable.get")
    def test_make_logos(self, mock_var_get):
        with CliRunner().isolated_filesystem() as t:
            mock_var_get.return_value = t

            # Country table
            category = "country"
            data = [
                {"id": "NZL"},
                {"id": "AUS"},
            ]
            df = pd.DataFrame(data)
            self.release.make_logos(df, category)
            for i, row in df.iterrows():
                # Check that logo key created
                self.assertTrue("logo" in row)

                # Check that correct logo path exists
                item_id = row["id"]
                expected_path = f"/logos/{category}/{item_id}.svg"
                actual_path = row["logo"]
                self.assertEqual(expected_path, actual_path)

            # Institution table
            category = "institution"
            data = [
                {"id": "grid.9654.e", "url": "https://www.auckland.ac.nz/"},
                {"id": "grid.1032.0", "url": "https://curtin.edu.au/"},
            ]
            df = pd.DataFrame(data)
            with vcr.use_cassette(test_fixtures_folder("oa_web_workflow", "test_make_logos.yaml")):
                self.release.make_logos(df, category)
                for i, row in df.iterrows():
                    # Check that logo was added to dataframe
                    self.assertTrue("logo" in row)

                    # Check that correct path created
                    item_id = row["id"]
                    expected_path = f"/logos/{category}/{item_id}.jpg"
                    actual_path = row["logo"]
                    self.assertEqual(expected_path, actual_path)

                    # Check that downloaded logo exists
                    full_path = os.path.join(self.release.transform_folder, expected_path[1:])
                    self.assertTrue(os.path.isfile(full_path))

    @patch("academic_observatory_workflows.workflows.oa_web_workflow.Variable.get")
    def test_save_timeseries_data(self, mock_var_get):
        category = "country"
        data = [
            {
                "id": "NZL",
                "year": 2020,
                "n_outputs": 1000,
                "n_outputs_oa": 500,
                "n_outputs_gold": 300,
                "n_outputs_hybrid": 50,
                "n_outputs_bronze": 50,
                "n_outputs_green": 200,
            },
            {
                "id": "NZL",
                "year": 2021,
                "n_outputs": 2000,
                "n_outputs_oa": 1200,
                "n_outputs_gold": 800,
                "n_outputs_hybrid": 200,
                "n_outputs_bronze": 50,
                "n_outputs_green": 250,
            },
            {
                "id": "AUS",
                "year": 2020,
                "n_outputs": 3000,
                "n_outputs_oa": 2000,
                "n_outputs_gold": 1500,
                "n_outputs_hybrid": 200,
                "n_outputs_bronze": 50,
                "n_outputs_green": 200,
            },
            {
                "id": "AUS",
                "year": 2021,
                "n_outputs": 4000,
                "n_outputs_oa": 3000,
                "n_outputs_gold": 2000,
                "n_outputs_hybrid": 500,
                "n_outputs_bronze": 300,
                "n_outputs_green": 500,
            },
        ]
        df = pd.DataFrame(data)

        with CliRunner().isolated_filesystem() as t:
            mock_var_get.return_value = t
            _ = self.release.save_timeseries_data(df, category)

            # Check that output files exist
            countries = ["NZL", "AUS"]
            for c in countries:
                path = os.path.join(self.release.transform_folder, "data", "country", f"{c}_ts.json")
                self.assertTrue(os.path.isfile(path))

                # TODO: check correctness
                with open(path) as f:
                    actual = json.load(f)

    def test_make_auto_complete_data(self):
        category = "country"
        expected = [
            {"id": "NZL", "name": "New Zealand", "logo": "/logos/country/NZL.svg"},
            {"id": "AUS", "name": "Australia", "logo": "/logos/country/AUS.svg"},
            {"id": "USA", "name": "United States", "logo": "/logos/country/USA.svg"},
        ]
        df = pd.DataFrame(expected)
        records = self.release.make_auto_complete_data(df, category)
        for e in expected:
            e["category"] = category
        self.assertEqual(expected, records)


class TestOaWebWorkflow(ObservatoryTestCase):
    def test_dag_structure(self):
        """Test that the DAG has the correct structure.

        :return: None
        """

        # dag = RorTelescope().make_dag()
        # self.assert_dag_structure(
        #     {
        #         "check_dependencies": ["list_releases"],
        #         "list_releases": ["download"],
        #         "download": ["upload_downloaded"],
        #         "upload_downloaded": ["extract"],
        #         "extract": ["transform"],
        #         "transform": ["upload_transformed"],
        #         "upload_transformed": ["bq_load"],
        #         "bq_load": ["cleanup"],
        #         "cleanup": [],
        #     },
        #     dag,
        # )
        pass

    def test_dag_load(self):
        """Test that the ROR DAG can be loaded from a DAG bag.

        :return: None
        """
        with ObservatoryEnvironment().create():
            dag_file = os.path.join(module_file_path("academic_observatory_workflows.dags"), "ror_telescope.py")
            self.assert_dag_load("ror", dag_file)

    def test_telescope(self):
        """Test the ROR telescope end to end.

        :return: None.
        """

        pass
        # # Setup Observatory environment
        # env = ObservatoryEnvironment(self.project_id, self.data_location)
        # dataset_id = env.add_dataset()
        #
        # # Setup Telescope
        # execution_date = pendulum.datetime(year=2021, month=9, day=1)
        # telescope = RorTelescope(dataset_id=dataset_id)
        # dag = telescope.make_dag()
        #
        # # Create the Observatory environment and run tests
        # with env.create():
        #     with env.create_dag_run(dag, execution_date):
        #         pass
