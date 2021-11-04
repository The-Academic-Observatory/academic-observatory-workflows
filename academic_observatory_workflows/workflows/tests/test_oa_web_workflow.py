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

import os
from unittest import TestCase
from unittest.mock import patch

import pandas as pd
import pendulum
import vcr
from airflow.models.connection import Connection
from airflow.models.variable import Variable
from click.testing import CliRunner

from academic_observatory_workflows.config import test_fixtures_folder
from academic_observatory_workflows.workflows.oa_web_workflow import OaWebRelease, OaWebWorkflow
from observatory.platform.utils.test_utils import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    module_file_path,
)


class TestOaWebFunctions(TestCase):
    def test_draw_change_points(self):
        self.fail()

    def test_bq_query_to_gcs(self):
        self.fail()


class TestOaWebRelease(TestCase):
    def setUp(self) -> None:
        self.release = OaWebRelease(dag_id="dag", project_id="project", release_date=pendulum.now())
        self.countries = [
            {
                "id": "NZL",
                "name": "New Zealand",
                "year": 2020,
                "url": None,
                "type": "",
                "date": pendulum.date(2020, 12, 31),
                "n_outputs": 1000,
                "n_outputs_oa": 500,
                "n_outputs_gold": 300,
                "n_outputs_hybrid": 50,
                "n_outputs_bronze": 50,
                "n_outputs_green": 200,
            },
            {
                "id": "NZL",
                "name": "New Zealand",
                "year": 2021,
                "url": None,
                "type": "",
                "date": pendulum.date(2021, 12, 31),
                "n_outputs": 2000,
                "n_outputs_oa": 1200,
                "n_outputs_gold": 800,
                "n_outputs_hybrid": 200,
                "n_outputs_bronze": 50,
                "n_outputs_green": 250,
            },
            {
                "id": "AUS",
                "name": "Australia",
                "year": 2020,
                "url": None,
                "type": "",
                "date": pendulum.date(2020, 12, 31),
                "n_outputs": 3000,
                "n_outputs_oa": 2000,
                "n_outputs_gold": 1500,
                "n_outputs_hybrid": 200,
                "n_outputs_bronze": 50,
                "n_outputs_green": 200,
            },
            {
                "id": "AUS",
                "name": "Australia",
                "year": 2021,
                "url": None,
                "type": "",
                "date": pendulum.date(2021, 12, 31),
                "n_outputs": 4000,
                "n_outputs_oa": 3000,
                "n_outputs_gold": 2000,
                "n_outputs_hybrid": 500,
                "n_outputs_bronze": 300,
                "n_outputs_green": 500,
            },
        ]
        self.institutions = [
            {
                "id": "grid.9654.e",
                "name": "The University of Auckland",
                "year": 2020,
                "url": "https://www.auckland.ac.nz/",
                "type": "",
                "date": pendulum.date(2020, 12, 31),
                "n_outputs": 1000,
                "n_outputs_oa": 500,
                "n_outputs_gold": 300,
                "n_outputs_hybrid": 50,
                "n_outputs_bronze": 50,
                "n_outputs_green": 200,
            },
            {
                "id": "grid.9654.e",
                "name": "The University of Auckland",
                "year": 2021,
                "url": "https://www.auckland.ac.nz/",
                "type": "",
                "date": pendulum.date(2021, 12, 31),
                "n_outputs": 2000,
                "n_outputs_oa": 1200,
                "n_outputs_gold": 800,
                "n_outputs_hybrid": 200,
                "n_outputs_bronze": 50,
                "n_outputs_green": 250,
            },
            {
                "id": "grid.1032.0",
                "name": "Curtin University",
                "year": 2020,
                "url": "https://curtin.edu.au/",
                "type": "",
                "date": pendulum.date(2020, 12, 31),
                "n_outputs": 3000,
                "n_outputs_oa": 2000,
                "n_outputs_gold": 1500,
                "n_outputs_hybrid": 200,
                "n_outputs_bronze": 50,
                "n_outputs_green": 200,
            },
            {
                "id": "grid.1032.0",
                "name": "Curtin University",
                "year": 2021,
                "url": "https://curtin.edu.au/",
                "type": "",
                "date": pendulum.date(2021, 12, 31),
                "n_outputs": 4000,
                "n_outputs_oa": 3000,
                "n_outputs_gold": 2000,
                "n_outputs_hybrid": 500,
                "n_outputs_bronze": 300,
                "n_outputs_green": 500,
            },
        ]
        self.entities = [
            ("country", self.countries, ["NZL", "AUS"]),
            ("institution", self.institutions, ["grid.9654.e", "grid.1032.0"]),
        ]

    def create_mock_csv(self, category, test_data):
        csv_path = os.path.join(self.release.download_folder, f"{category}.csv")
        df = pd.DataFrame(test_data)
        df.to_csv(csv_path, index=False)
        df.fillna("", inplace=True)
        return df

    @patch("academic_observatory_workflows.workflows.oa_web_workflow.Variable.get")
    def test_load_csv(self, mock_var_get):
        category = "country"
        with CliRunner().isolated_filesystem() as t:
            mock_var_get.return_value = t

            # Save CSV
            df = self.create_mock_csv(category, self.countries)

            # Load csv
            actual_df = self.release.load_csv(category)

            # Compare
            expected_countries = df.to_dict("records")
            actual_countries = actual_df.to_dict("records")
            self.assertEqual(expected_countries, actual_countries)

    def test_update_df_with_percentages(self):
        keys = ["hello", "world"]
        df = pd.DataFrame([{"n_hello": 20, "n_world": 50, "n_outputs": 100}])
        self.release.update_df_with_percentages(df, keys)
        expected = {"n_hello": 20, "n_world": 50, "n_outputs": 100, "p_hello": 20, "p_world": 50}
        actual = df.to_dict(orient="records")[0]
        self.assertEqual(expected, actual)

    @patch("academic_observatory_workflows.workflows.oa_web_workflow.Variable.get")
    def test_make_make_index(self, mock_var_get):
        with CliRunner().isolated_filesystem() as t:
            mock_var_get.return_value = t

            # Country
            category = "country"
            df = self.create_mock_csv(category, self.countries)
            df_country_index = self.release.make_index(df, category)
            expected = [
                {
                    "id": "AUS",
                    "name": "Australia",
                    "rank": 1,
                    "url": "https://en.wikipedia.org/wiki/Australia",
                    "friendly_url": ".",
                    "type": "",
                    "category": "country",
                    "n_outputs": 7000,
                    "n_outputs_oa": 5000,
                    "n_outputs_gold": 3500,
                    "n_outputs_hybrid": 700,
                    "n_outputs_bronze": 350,
                    "n_outputs_green": 700,
                    "p_outputs_oa": 71,
                    "p_outputs_gold": 50,
                    "p_outputs_hybrid": 10,
                    "p_outputs_bronze": 5,
                    "p_outputs_green": 10,
                },
                {
                    "id": "NZL",
                    "name": "New Zealand",
                    "rank": 2,
                    "url": "https://en.wikipedia.org/wiki/New%20Zealand",
                    "friendly_url": ".",
                    "type": "",
                    "category": "country",
                    "n_outputs": 3000,
                    "n_outputs_oa": 1700,
                    "n_outputs_gold": 1100,
                    "n_outputs_hybrid": 250,
                    "n_outputs_bronze": 100,
                    "n_outputs_green": 450,
                    "p_outputs_oa": 57,
                    "p_outputs_gold": 37,
                    "p_outputs_hybrid": 8,
                    "p_outputs_bronze": 3,
                    "p_outputs_green": 15,
                },
            ]
            actual = df_country_index.to_dict("records")
            self.assertEqual(len(expected), len(actual))
            for e, a in zip(expected, actual):
                self.assertDictEqual(e, a)

            # Institution
            category = "institution"
            df = self.create_mock_csv(category, self.institutions)
            df_institution_index = self.release.make_index(df, category)
            expected = [
                {
                    "id": "grid.1032.0",
                    "name": "Curtin University",
                    "rank": 1,
                    "url": "https://curtin.edu.au/",
                    "friendly_url": "curtin.edu.au",
                    "type": "",
                    "category": "institution",
                    "n_outputs": 7000,
                    "n_outputs_oa": 5000,
                    "n_outputs_gold": 3500,
                    "n_outputs_hybrid": 700,
                    "n_outputs_bronze": 350,
                    "n_outputs_green": 700,
                    "p_outputs_oa": 71,
                    "p_outputs_gold": 50,
                    "p_outputs_hybrid": 10,
                    "p_outputs_bronze": 5,
                    "p_outputs_green": 10,
                },
                {
                    "id": "grid.9654.e",
                    "name": "The University of Auckland",
                    "rank": 2,
                    "url": "https://www.auckland.ac.nz/",
                    "friendly_url": "auckland.ac.nz",
                    "type": "",
                    "category": "institution",
                    "n_outputs": 3000,
                    "n_outputs_oa": 1700,
                    "n_outputs_gold": 1100,
                    "n_outputs_hybrid": 250,
                    "n_outputs_bronze": 100,
                    "n_outputs_green": 450,
                    "p_outputs_oa": 57,
                    "p_outputs_gold": 37,
                    "p_outputs_hybrid": 8,
                    "p_outputs_bronze": 3,
                    "p_outputs_green": 15,
                },
            ]
            actual = df_institution_index.to_dict("records")
            self.assertEqual(len(expected), len(actual))
            for e, a in zip(expected, actual):
                self.assertDictEqual(e, a)

    @patch("academic_observatory_workflows.workflows.oa_web_workflow.Variable.get")
    def test_update_index_with_logos(self, mock_var_get):
        with CliRunner().isolated_filesystem() as t:
            mock_var_get.return_value = t

            # Country table
            category = "country"
            df = pd.DataFrame(self.countries)
            self.release.update_index_with_logos(df, category)
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
            df = pd.DataFrame(self.institutions)
            with vcr.use_cassette(test_fixtures_folder("oa_web_workflow", "test_make_logos.yaml")):
                self.release.update_index_with_logos(df, category)
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

    def test_make_change_points(self):
        self.fail()

    def test_update_index_with_change_points(self):
        self.fail()

    @patch("academic_observatory_workflows.workflows.oa_web_workflow.Variable.get")
    def test_save_index(self, mock_var_get):
        with CliRunner().isolated_filesystem() as t:
            mock_var_get.return_value = t

            for category, data, entity_ids in self.entities:
                df = pd.DataFrame(data)
                country_index = self.release.make_index(df, category)
                ts = self.release.make_timeseries(df)
                change_points = self.release.make_change_points(ts)
                self.release.update_index_with_change_points(country_index, change_points)
                self.release.update_index_with_logos(country_index, category)
                self.release.save_index(country_index, category)

                path = os.path.join(self.release.transform_folder, "data", category, "summary.json")
                self.assertTrue(os.path.isfile(path))

    @patch("academic_observatory_workflows.workflows.oa_web_workflow.Variable.get")
    def test_save_entity_details(self, mock_var_get):
        with CliRunner().isolated_filesystem() as t:
            mock_var_get.return_value = t

            for category, data, entity_ids in self.entities:
                df = pd.DataFrame(data)
                country_index = self.release.make_index(df, category)
                self.release.save_entity_details(country_index, category)

                for entity_id in entity_ids:
                    path = os.path.join(self.release.transform_folder, "data", category, f"{entity_id}_summary.json")
                    self.assertTrue(os.path.isfile(path))

    @patch("academic_observatory_workflows.workflows.oa_web_workflow.Variable.get")
    def test_make_timeseries(self, mock_var_get):
        with CliRunner().isolated_filesystem() as t:
            mock_var_get.return_value = t

            df = pd.DataFrame(self.countries)
            timeseries = self.release.make_timeseries(df)

            expected = [
                (
                    "AUS",
                    [
                        {
                            "year": 2020,
                            "n_outputs": 3000,
                            "n_outputs_oa": 2000,
                            "p_outputs_oa": 67,
                            "p_outputs_gold": 50.0,
                            "p_outputs_hybrid": 7.0,
                            "p_outputs_bronze": 2.0,
                            "p_outputs_green": 7.0,
                        },
                        {
                            "year": 2021,
                            "n_outputs": 4000,
                            "n_outputs_oa": 3000,
                            "p_outputs_oa": 75,
                            "p_outputs_gold": 50.0,
                            "p_outputs_hybrid": 12.0,
                            "p_outputs_bronze": 8.0,
                            "p_outputs_green": 12.0,
                        },
                    ],
                ),
                (
                    "NZL",
                    [
                        {
                            "year": 2020,
                            "n_outputs": 1000,
                            "n_outputs_oa": 500,
                            "p_outputs_oa": 50.0,
                            "p_outputs_gold": 30.0,
                            "p_outputs_hybrid": 5.0,
                            "p_outputs_bronze": 5.0,
                            "p_outputs_green": 20.0,
                        },
                        {
                            "year": 2021,
                            "n_outputs": 2000,
                            "n_outputs_oa": 1200,
                            "p_outputs_oa": 60.0,
                            "p_outputs_gold": 40.0,
                            "p_outputs_hybrid": 10.0,
                            "p_outputs_bronze": 2.0,
                            "p_outputs_green": 12.0,
                        },
                    ],
                ),
            ]
            self.assertEqual(len(expected), len(timeseries))

            for e, a in zip(expected, timeseries):
                e_entity_id, e_ts = e
                a_entity_id, a_ts_df = a
                self.assertEqual(e_entity_id, a_entity_id)
                a_ts = a_ts_df.to_dict("records")

                self.assertEqual(len(e_ts), len(a_ts))
                self.assertEqual(e_ts, a_ts)

    @patch("academic_observatory_workflows.workflows.oa_web_workflow.Variable.get")
    def test_save_timeseries(self, mock_var_get):
        with CliRunner().isolated_filesystem() as t:
            mock_var_get.return_value = t

            for category, data, entity_ids in self.entities:
                df = pd.DataFrame(data)
                ts = self.release.make_timeseries(df)
                self.release.save_timeseries(ts, category)

                for entity_id in entity_ids:
                    path = os.path.join(self.release.transform_folder, "data", category, f"{entity_id}_ts.json")
                    self.assertTrue(os.path.isfile(path))

    def test_make_auto_complete(self):
        category = "country"
        expected = [
            {"id": "NZL", "name": "New Zealand", "logo": "/logos/country/NZL.svg"},
            {"id": "AUS", "name": "Australia", "logo": "/logos/country/AUS.svg"},
            {"id": "USA", "name": "United States", "logo": "/logos/country/USA.svg"},
        ]
        df = pd.DataFrame(expected)
        records = self.release.make_auto_complete(df, category)
        for e in expected:
            e["category"] = category
        self.assertEqual(expected, records)

    @patch("academic_observatory_workflows.workflows.oa_web_workflow.Variable.get")
    def test_save_autocomplete(self, mock_var_get):
        with CliRunner().isolated_filesystem() as t:
            mock_var_get.return_value = t
            category = "country"
            expected = [
                {"id": "NZL", "name": "New Zealand", "logo": "/logos/country/NZL.svg"},
                {"id": "AUS", "name": "Australia", "logo": "/logos/country/AUS.svg"},
                {"id": "USA", "name": "United States", "logo": "/logos/country/USA.svg"},
            ]
            df = pd.DataFrame(expected)
            records = self.release.make_auto_complete(df, category)
            self.release.save_autocomplete(records)

            path = os.path.join(self.release.transform_folder, "data", "autocomplete.json")
            self.assertTrue(os.path.isfile(path))


class TestOaWebWorkflow(ObservatoryTestCase):
    def test_dag_structure(self):
        """Test that the DAG has the correct structure.

        :return: None
        """

        env = ObservatoryEnvironment(enable_api=False)
        with env.create():
            env.add_variable(Variable(key=OaWebWorkflow.AIRFLOW_VAR_WEBSITE_FOLDER, val="/path/to/website"))
            env.add_connection(
                Connection(
                    conn_id=OaWebWorkflow.AIRFLOW_CONN_CLOUDFLARE_API_TOKEN,
                    uri="http://:password@",
                )
            )

            dag = OaWebWorkflow().make_dag()
            self.assert_dag_structure(
                {
                    "doi_sensor": ["check_dependencies"],
                    "check_dependencies": ["query"],
                    "query": ["download"],
                    "download": ["transform"],
                    "transform": ["build_website"],
                    "build_website": ["deploy_website"],
                    "deploy_website": [],
                },
                dag,
            )

    def test_dag_load(self):
        """Test that the DAG can be loaded from a DAG bag.

        :return: None
        """

        env = ObservatoryEnvironment(enable_api=False)
        with env.create():
            env.add_variable(Variable(key=OaWebWorkflow.AIRFLOW_VAR_WEBSITE_FOLDER, val="/path/to/website"))
            env.add_connection(
                Connection(
                    conn_id=OaWebWorkflow.AIRFLOW_CONN_CLOUDFLARE_API_TOKEN,
                    uri="http://:password@",
                )
            )

            dag_file = os.path.join(module_file_path("academic_observatory_workflows.dags"), "oa_web_workflow.py")
            self.assert_dag_load("oa_web_workflow", dag_file)

    def test_telescope(self):
        """Test the telescope end to end.

        :return: None.
        """

        execution_date = pendulum.now()
        env = ObservatoryEnvironment(enable_api=False)
        with env.create():
            env.add_variable(Variable(key=OaWebWorkflow.AIRFLOW_VAR_WEBSITE_FOLDER, val="/path/to/website"))
            env.add_connection(
                Connection(
                    conn_id=OaWebWorkflow.AIRFLOW_CONN_CLOUDFLARE_API_TOKEN,
                    uri="http://:password@",
                )
            )

            dag = OaWebWorkflow().make_dag()

            with env.create_dag_run(dag, execution_date):
                self.fail()
