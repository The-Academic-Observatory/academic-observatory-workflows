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
import shutil
from typing import List
from unittest import TestCase
from unittest.mock import patch
import jsonlines
import pandas as pd
import pendulum
import vcr
from airflow.models.connection import Connection
from airflow.utils.state import State
from click.testing import CliRunner
from observatory.platform.utils.file_utils import load_jsonl
from observatory.platform.utils.test_utils import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    module_file_path,
)
from observatory.platform.utils.test_utils import Table, bq_load_tables
from observatory.platform.utils.test_utils import (
    make_dummy_dag,
)

from academic_observatory_workflows.config import test_fixtures_folder, schema_folder
from academic_observatory_workflows.workflows.oa_web_workflow import OaWebRelease, OaWebWorkflow, clean_url


class TestOaWebRelease(TestCase):
    def setUp(self) -> None:
        dt_fmt = "YYYY-MM-DD"
        self.release = OaWebRelease(dag_id="dag", project_id="project", release_date=pendulum.now())
        self.countries = [
            {
                "id": "NZL",
                "name": "New Zealand",
                "year": 2020,
                "date": pendulum.date(2020, 12, 31).format(dt_fmt),
                "url": None,
                "wikipedia_url": "https://en.wikipedia.org/wiki/New_Zealand",
                "country": None,
                "subregion": "Australia and New Zealand",
                "region": "Oceania",
                "institution_types": None,
                "n_outputs": 1000,
                "n_outputs_open": 500,
                "n_citations": 3000,
                "n_outputs_publisher_open": 300,
                "n_outputs_other_platform_open": 200,
                "n_outputs_other_platform_open_only": 100,
                "n_outputs_oa_journal": 200,
                "n_outputs_hybrid": 50,
                "n_outputs_no_guarantees": 50,
                "identifiers": [],
            },
            {
                "id": "NZL",
                "name": "New Zealand",
                "year": 2021,
                "date": pendulum.date(2021, 12, 31).format(dt_fmt),
                "url": None,
                "wikipedia_url": "https://en.wikipedia.org/wiki/New_Zealand",
                "country": None,
                "subregion": "Australia and New Zealand",
                "region": "Oceania",
                "institution_types": None,
                "n_outputs": 2000,
                "n_outputs_open": 1200,
                "n_citations": 4000,
                "n_outputs_publisher_open": 800,
                "n_outputs_other_platform_open": 250,
                "n_outputs_other_platform_open_only": 150,
                "n_outputs_oa_journal": 350,
                "n_outputs_hybrid": 200,
                "n_outputs_no_guarantees": 250,
                "identifiers": [],
            },
            {
                "id": "AUS",
                "name": "Australia",
                "year": 2020,
                "date": pendulum.date(2020, 12, 31).format(dt_fmt),
                "url": None,
                "wikipedia_url": "https://en.wikipedia.org/wiki/Australia",
                "country": None,
                "subregion": "Australia and New Zealand",
                "region": "Oceania",
                "institution_types": None,
                "n_outputs": 3000,
                "n_outputs_open": 2000,
                "n_citations": 4000,
                "n_outputs_publisher_open": 1500,
                "n_outputs_other_platform_open": 200,
                "n_outputs_other_platform_open_only": 100,
                "n_outputs_oa_journal": 1250,
                "n_outputs_hybrid": 200,
                "n_outputs_no_guarantees": 50,
                "identifiers": [],
            },
            {
                "id": "AUS",
                "name": "Australia",
                "year": 2021,
                "date": pendulum.date(2021, 12, 31).format(dt_fmt),
                "url": None,
                "wikipedia_url": "https://en.wikipedia.org/wiki/Australia",
                "country": None,
                "subregion": "Australia and New Zealand",
                "region": "Oceania",
                "institution_types": None,
                "n_outputs": 4000,
                "n_outputs_open": 3000,
                "n_citations": 5000,
                "n_outputs_publisher_open": 2000,
                "n_outputs_other_platform_open": 500,
                "n_outputs_other_platform_open_only": 400,
                "n_outputs_oa_journal": 1200,
                "n_outputs_hybrid": 500,
                "n_outputs_no_guarantees": 300,
                "identifiers": [],
            },
        ]
        self.institutions = [
            {
                "id": "https://ror.org/03b94tp07",
                "name": "The University of Auckland",
                "year": 2020,
                "date": pendulum.date(2020, 12, 31).format(dt_fmt),
                "url": "https://www.auckland.ac.nz/",
                "wikipedia_url": "https://en.wikipedia.org/wiki/University_of_Auckland",
                "country": "New Zealand",
                "subregion": "Australia and New Zealand",
                "region": "Oceania",
                "institution_types": ["Education"],
                "n_outputs": 1000,
                "n_outputs_open": 500,
                "n_citations": 1000,
                "n_outputs_publisher_open": 300,
                "n_outputs_other_platform_open": 200,
                "n_outputs_other_platform_open_only": 100,
                "n_outputs_oa_journal": 200,
                "n_outputs_hybrid": 50,
                "n_outputs_no_guarantees": 50,
                "identifiers": [],
            },
            {
                "id": "https://ror.org/03b94tp07",
                "name": "The University of Auckland",
                "year": 2021,
                "date": pendulum.date(2021, 12, 31).format(dt_fmt),
                "url": "https://www.auckland.ac.nz/",
                "wikipedia_url": "https://en.wikipedia.org/wiki/University_of_Auckland",
                "country": "New Zealand",
                "subregion": "Australia and New Zealand",
                "region": "Oceania",
                "institution_types": ["Education"],
                "n_outputs": 2000,
                "n_outputs_open": 1200,
                "n_citations": 1500,
                "n_outputs_publisher_open": 800,
                "n_outputs_other_platform_open": 250,
                "n_outputs_other_platform_open_only": 150,
                "n_outputs_oa_journal": 550,
                "n_outputs_hybrid": 200,
                "n_outputs_no_guarantees": 50,
                "identifiers": [],
            },
            {
                "id": "https://ror.org/02n415q13",
                "name": "Curtin University",
                "year": 2020,
                "date": pendulum.date(2020, 12, 31).format(dt_fmt),
                "url": "https://curtin.edu.au/",
                "wikipedia_url": "https://en.wikipedia.org/wiki/Curtin_University",
                "country": "Australia",
                "subregion": "Australia and New Zealand",
                "region": "Oceania",
                "institution_types": ["Education"],
                "n_outputs": 3000,
                "n_outputs_open": 2000,
                "n_citations": 800,
                "n_outputs_publisher_open": 1500,
                "n_outputs_other_platform_open": 200,
                "n_outputs_other_platform_open_only": 100,
                "n_outputs_oa_journal": 1250,
                "n_outputs_hybrid": 200,
                "n_outputs_no_guarantees": 50,
                "identifiers": [],
            },
            {
                "id": "https://ror.org/02n415q13",
                "name": "Curtin University",
                "year": 2021,
                "date": pendulum.date(2021, 12, 31).format(dt_fmt),
                "url": "https://curtin.edu.au/",
                "wikipedia_url": "https://en.wikipedia.org/wiki/Curtin_University",
                "country": "Australia",
                "subregion": "Australia and New Zealand",
                "region": "Oceania",
                "institution_types": ["Education"],
                "n_outputs": 4000,
                "n_outputs_open": 3000,
                "n_citations": 1200,
                "n_outputs_publisher_open": 2000,
                "n_outputs_other_platform_open": 500,
                "n_outputs_other_platform_open_only": 300,
                "n_outputs_oa_journal": 1200,
                "n_outputs_hybrid": 500,
                "n_outputs_no_guarantees": 300,
                "identifiers": [],
            },
        ]
        self.entities = [
            ("country", self.countries, ["NZL", "AUS"]),
            ("institution", self.institutions, ["https://ror.org/03b94tp07", "https://ror.org/02n415q13"]),
        ]

    def test_clean_url(self):
        url = "https://www.auckland.ac.nz/en.html"
        expected = "https://www.auckland.ac.nz/"
        actual = clean_url(url)
        self.assertEqual(expected, actual)

    def save_mock_data(self, category, test_data):
        path = os.path.join(self.release.download_folder, f"{category}.jsonl")
        with jsonlines.open(path, mode='w') as writer:
            writer.write_all(test_data)
        df = pd.DataFrame(test_data)
        return df

    @patch("academic_observatory_workflows.workflows.oa_web_workflow.Variable.get")
    def test_load_data(self, mock_var_get):
        category = "country"
        with CliRunner().isolated_filesystem() as t:
            mock_var_get.return_value = t

            # Save CSV
            df = self.save_mock_data(category, self.countries)

            # Load csv
            actual_df = self.release.load_data(category)

            # Compare
            expected_countries = df.to_dict("records")
            actual_countries = actual_df.to_dict("records")
            self.assertEqual(expected_countries, actual_countries)

    @patch("academic_observatory_workflows.workflows.oa_web_workflow.Variable.get")
    def test_preprocess_df(self, mock_var_get):
        pass

    def test_update_df_with_percentages(self):
        keys = [("hello", "n_outputs"), ("world", "n_outputs")]
        df = pd.DataFrame([{"n_hello": 20, "n_world": 50, "n_outputs": 100}])
        self.release.update_df_with_percentages(df, keys)
        expected = {"n_hello": 20, "n_world": 50, "n_outputs": 100, "p_hello": 20, "p_world": 50}
        actual = df.to_dict(orient="records")[0]
        self.assertEqual(expected, actual)

    @patch("academic_observatory_workflows.workflows.oa_web_workflow.Variable.get")
    def test_make_index(self, mock_var_get):
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
            sizes = ["l", "s"]

            # Country table
            category = "country"
            df = pd.DataFrame(self.countries)
            df = self.release.preprocess_df(category, df)
            self.release.update_index_with_logos(category, df)
            for i, row in df.iterrows():
                for size in sizes:
                    # Check that logo key created
                    key = f"logo_{size}"
                    self.assertTrue(key in row)

                    # Check that correct logo path exists
                    item_id = row["id"]
                    expected_path = f"/logos/{category}/{size}/{item_id}.svg"
                    actual_path = row[key]
                    self.assertEqual(expected_path, actual_path)

            # Institution table
            category = "institution"
            df = pd.DataFrame(self.institutions)
            df = self.release.preprocess_df(category, df)
            with vcr.use_cassette(test_fixtures_folder("oa_web_workflow", "test_make_logos.yaml")):
                self.release.update_index_with_logos(category, df)
                for i, row in df.iterrows():
                    for size in sizes:
                        # Check that logo was added to dataframe
                        key = f"logo_{size}"
                        self.assertTrue(key in row)

                        # Check that correct path created
                        item_id = row["id"]
                        expected_path = f"/logos/{category}/{size}/{item_id}.jpg"
                        actual_path = row[key]
                        self.assertEqual(expected_path, actual_path)

                        # Check that downloaded logo exists
                        full_path = os.path.join(self.release.build_path, expected_path[1:])
                        self.assertTrue(os.path.isfile(full_path))

    @patch("academic_observatory_workflows.workflows.oa_web_workflow.Variable.get")
    def test_save_index(self, mock_var_get):
        with CliRunner().isolated_filesystem() as t:
            mock_var_get.return_value = t

            for category, data, entity_ids in self.entities:
                df = pd.DataFrame(data)
                country_index = self.release.make_index(df, category)
                self.release.update_index_with_logos(country_index, category)
                self.release.save_index(country_index, category)

                path = os.path.join(self.release.build_path, "data", category, "summary.json")
                self.assertTrue(os.path.isfile(path))

    # @patch("academic_observatory_workflows.workflows.oa_web_workflow.Variable.get")
    # def test_save_entity_details(self, mock_var_get):
    #     with CliRunner().isolated_filesystem() as t:
    #         mock_var_get.return_value = t
    #
    #         for category, data, entity_ids in self.entities:
    #             df = pd.DataFrame(data)
    #             country_index = self.release.make_index(df, category)
    #             self.release.save_entity_details(country_index, category)
    #
    #             for entity_id in entity_ids:
    #                 path = os.path.join(self.release.build_path, "data", category, f"{entity_id}_summary.json")
    #                 self.assertTrue(os.path.isfile(path))

    @patch("academic_observatory_workflows.workflows.oa_web_workflow.Variable.get")
    def test_make_entities(self, mock_var_get):
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
    def test_save_entities(self, mock_var_get):
        with CliRunner().isolated_filesystem() as t:
            mock_var_get.return_value = t

            for category, data, entity_ids in self.entities:
                df = pd.DataFrame(data)
                ts = self.release.make_timeseries(df)
                self.release.save_timeseries(ts, category)

                for entity_id in entity_ids:
                    path = os.path.join(self.release.build_path, "data", category, f"{entity_id}_ts.json")
                    self.assertTrue(os.path.isfile(path))

    def test_make_auto_complete(self):
        category = "country"
        expected = [
            {"id": "NZL", "name": "New Zealand", "logo_s": "/logos/country/NZL.svg"},
            {"id": "AUS", "name": "Australia", "logo_s": "/logos/country/AUS.svg"},
            {"id": "USA", "name": "United States", "logo_s": "/logos/country/USA.svg"},
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
                {"id": "NZL", "name": "New Zealand", "logo_s": "/logos/country/NZL.svg"},
                {"id": "AUS", "name": "Australia", "logo_s": "/logos/country/AUS.svg"},
                {"id": "USA", "name": "United States", "logo_s": "/logos/country/USA.svg"},
            ]
            df = pd.DataFrame(expected)
            records = self.release.make_auto_complete(df, category)
            self.release.save_autocomplete(records)

            path = os.path.join(self.release.build_path, "data", "autocomplete.json")
            self.assertTrue(os.path.isfile(path))


class TestOaWebWorkflow(ObservatoryTestCase):
    def setUp(self) -> None:
        """TestOaWebWorkflow checks that the workflow functions correctly, i.e. outputs the correct files, but doesn't
        check that the calculations are correct (data correctness is tested in TestOaWebRelease)."""

        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")
        self.oa_web_fixtures = "oa_web_workflow"

    def test_dag_structure(self):
        """Test that the DAG has the correct structure.

        :return: None
        """

        env = ObservatoryEnvironment(enable_api=False)
        with env.create():
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
                    "transform": ["copy_static_assets"],
                    "copy_static_assets": ["build_website"],
                    "build_website": ["deploy_website"],
                    "deploy_website": [],
                },
                dag,
            )

    def test_dag_load(self):
        """Test that the DAG can be loaded from a DAG bag.

        :return: None
        """

        env = ObservatoryEnvironment(project_id=self.project_id, data_location=self.data_location, enable_api=False)
        with env.create():
            env.add_connection(
                Connection(
                    conn_id=OaWebWorkflow.AIRFLOW_CONN_CLOUDFLARE_API_TOKEN,
                    uri="http://:password@",
                )
            )

            dag_file = os.path.join(module_file_path("academic_observatory_workflows.dags"), "oa_web_workflow.py")
            self.assert_dag_load("oa_web_workflow", dag_file)

    def setup_tables(self, dataset_id_all: str, bucket_name: str, release_date: pendulum.DateTime):
        ror = load_jsonl(test_fixtures_folder("doi", "ror.jsonl"))
        country = load_jsonl(test_fixtures_folder(self.oa_web_fixtures, "country.jsonl"))
        institution = load_jsonl(test_fixtures_folder(self.oa_web_fixtures, "institution.jsonl"))

        analysis_schema_path = schema_folder()
        oa_web_schema_path = test_fixtures_folder(self.oa_web_fixtures, "schema")
        with CliRunner().isolated_filesystem() as t:
            tables = [
                Table("ror", True, dataset_id_all, ror, "ror", analysis_schema_path),
                Table("country", True, dataset_id_all, country, "country", oa_web_schema_path),
                Table("institution", True, dataset_id_all, institution, "institution", oa_web_schema_path),
            ]

            bq_load_tables(
                tables=tables, bucket_name=bucket_name, release_date=release_date, data_location=self.data_location
            )

    def test_telescope(self):
        """Test the telescope end to end.

        :return: None.
        """

        execution_date = pendulum.datetime(2021, 11, 13)
        env = ObservatoryEnvironment(project_id=self.project_id, data_location=self.data_location, enable_api=False)
        dataset_id = env.add_dataset("data")
        with env.create() as t:
            # Add required environment variables and connections
            env.add_connection(
                Connection(
                    conn_id=OaWebWorkflow.AIRFLOW_CONN_CLOUDFLARE_API_TOKEN,
                    uri="http://:password@",
                )
            )

            # Run fake DOI workflow
            dag = make_dummy_dag("doi", execution_date)
            with env.create_dag_run(dag, execution_date):
                # Running all of a DAGs tasks sets the DAG to finished
                ti = env.run_task("dummy_task")
                self.assertEqual(State.SUCCESS, ti.state)

            # Upload fake data to BigQuery
            self.setup_tables(dataset_id_all=dataset_id, bucket_name=env.download_bucket, release_date=execution_date)

            # Run workflow
            workflow = OaWebWorkflow(agg_dataset_id=dataset_id, ror_dataset_id=dataset_id)

            # Copy test website
            src_folder = test_fixtures_folder("oa_web_workflow", workflow.oa_website_name)
            dst_folder = workflow.website_folder
            shutil.copytree(src_folder, dst_folder)

            dag = workflow.make_dag()
            with env.create_dag_run(dag, execution_date):
                # DOI Sensor
                ti = env.run_task("doi_sensor")
                self.assertEqual(State.SUCCESS, ti.state)

                # Check dependencies
                ti = env.run_task(workflow.check_dependencies.__name__)
                self.assertEqual(State.SUCCESS, ti.state)

                # Run query
                ti = env.run_task(workflow.query.__name__)
                self.assertEqual(State.SUCCESS, ti.state)

                # Download data
                ti = env.run_task(workflow.download.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                base_folder = os.path.join(
                    t, "data", "telescopes", "download", "oa_web_workflow", "oa_web_workflow_2021_11_13"
                )
                expected_file_names = ["country.jsonl", "institution.jsonl"]
                for file_name in expected_file_names:
                    path = os.path.join(base_folder, file_name)
                    self.assertTrue(os.path.isfile(path))

                # Transform data
                ti = env.run_task(workflow.transform.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                base_folder = os.path.join(
                    t, "data", "telescopes", "transform", "oa_web_workflow", "oa_web_workflow_2021_11_13", "build"
                )
                expected_files = make_expected_build_files(base_folder)
                print("Checking expected transformed files")
                for file in expected_files:
                    print(f"\t{file}")
                    self.assertTrue(os.path.isfile(file))

                # Copy static assets
                ti = env.run_task(workflow.copy_static_assets.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                base_folder = os.path.join(t, "data", workflow.website_folder, "static", "build")
                expected_files = make_expected_build_files(base_folder)
                print("Checking expected static assets")
                for file in expected_files:
                    print(f"\t{file}")
                    self.assertTrue(os.path.isfile(file))
                self.assertFalse(os.path.isfile(os.path.join(base_folder, "static", "build", "old.txt")))

                # Build website
                ti = env.run_task(OaWebWorkflow.TASK_ID_BUILD_WEBSITE)
                self.assertEqual(State.SUCCESS, ti.state)

                # Deploy website
                ti = env.run_task(OaWebWorkflow.TASK_ID_DEPLOY_WEBSITE)
                self.assertEqual(State.SUCCESS, ti.state)


def make_expected_build_files(base_path: str) -> List[str]:
    countries = ["AUS", "NZL"]
    institutions = ["03b94tp07", "02n415q13"]  # Auckland, Curtin
    categories = ["country"] * len(countries) + ["institution"] * len(institutions)
    entity_ids = countries + institutions
    expected = []

    # Add base data files
    data_path = os.path.join(base_path, "data")
    file_names = [
        "autocomplete.json",
        "autocomplete.parquet",
        "country.json",
        "country.parquet",
        "institution.json",
        "institution.parquet",
    ]
    for file_name in file_names:
        expected.append(os.path.join(data_path, file_name))

    # Add country and institution specific data files
    for category, entity_id in zip(categories, entity_ids):
        path = os.path.join(data_path, category, f"{entity_id}.json")
        expected.append(path)

    # Add logos
    for category, entity_id in zip(categories, entity_ids):
        file_name = f"{entity_id}.svg"
        if category == "institution":
            file_name = f"{entity_id}.jpg"
        for size in ["l", "s"]:
            path = os.path.join(base_path, "logos", category, size, file_name)
            expected.append(path)

    return expected
