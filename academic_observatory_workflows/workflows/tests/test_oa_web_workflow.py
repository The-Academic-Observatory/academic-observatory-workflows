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

# Author: James Diprose, Aniek Roelofs

import os
from typing import List
from unittest import TestCase
from unittest.mock import patch

import jsonlines
import pandas as pd
import pendulum
import vcr
from airflow.models.connection import Connection
from airflow.models.variable import Variable
from airflow.utils.state import State
from click.testing import CliRunner

import academic_observatory_workflows.workflows.oa_web_workflow
from academic_observatory_workflows.config import schema_folder, test_fixtures_folder
from academic_observatory_workflows.workflows.oa_web_workflow import (
    Description,
    OaWebRelease,
    OaWebWorkflow,
    clean_ror_id,
    clean_url,
    get_institution_logo,
    make_logo_url,
    val_empty,
    make_entity_stats,
    Entity,
    PublicationStats,
    EntityStats,
    EntityHistograms,
    Histogram,
    ror_to_tree,
    make_institution_df,
    aggregate_institutions,
    load_data,
    preprocess_index_df,
    preprocess_data_df,
    make_index,
    make_entities,
    save_entities,
    update_index_with_logos,
    update_df_with_percentages,
)
from academic_observatory_workflows.tests.test_zenodo import MockZenodo
from observatory.platform.utils.file_utils import load_jsonl
from observatory.platform.utils.gc_utils import upload_file_to_cloud_storage
from observatory.platform.utils.test_utils import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    Table,
    bq_load_tables,
    make_dummy_dag,
    module_file_path,
)

academic_observatory_workflows.workflows.oa_web_workflow.INCLUSION_THRESHOLD = {"country": 0, "institution": 0}


class TestFunctions(TestCase):
    def test_val_empty(self):
        # Empty list
        self.assertTrue(val_empty([]))

        # Non empty list
        self.assertFalse(val_empty([1, 2, 3]))

        # None
        self.assertTrue(val_empty(None))

        # Empty string
        self.assertTrue(val_empty(""))

        # Non Empty string
        self.assertFalse(val_empty("hello"))

    def test_clean_ror_id(self):
        actual = clean_ror_id("https://ror.org/02n415q13")
        expected = "02n415q13"
        self.assertEqual(actual, expected)

    def test_clean_url(self):
        url = "https://www.auckland.ac.nz/en.html"
        expected = "https://www.auckland.ac.nz/"
        actual = clean_url(url)
        self.assertEqual(expected, actual)

    def test_make_logo_url(self):
        expected = "/logos/country/s/1234.jpg"
        actual = make_logo_url(category="country", entity_id="1234", size="s", fmt="jpg")
        self.assertEqual(expected, actual)

    @patch("academic_observatory_workflows.workflows.oa_web_workflow.make_logo_url")
    def test_get_institution_logo(self, mock_make_url):
        mock_make_url.return_value = "logo_path"
        mock_clearbit_ref = "academic_observatory_workflows.workflows.oa_web_workflow.clearbit_download_logo"

        def download_logo(company_url, file_path, size, fmt):
            if not os.path.isdir(os.path.dirname(file_path)):
                os.makedirs(os.path.dirname(file_path))
            with open(file_path, "w") as f:
                f.write("foo")

        ror_id, url, size, width, fmt, build_path = "ror_id", "url.com", "size", 10, "fmt", "build_path"
        with CliRunner().isolated_filesystem():
            # Test when logo file does not exist yet and logo download fails
            with patch(mock_clearbit_ref) as mock_clearbit_download:
                actual_ror_id, actual_logo_path = get_institution_logo(ror_id, url, size, width, fmt, build_path)
                self.assertEqual(ror_id, actual_ror_id)
                self.assertEqual("/unknown.svg", actual_logo_path)
                mock_clearbit_download.assert_called_once_with(
                    company_url=url, file_path="build_path/logos/institution/size/ror_id.fmt", size=width, fmt=fmt
                )
                mock_make_url.assert_not_called()

            mock_make_url.reset_mock()

            # Test when logo file does not exist yet and logo is downloaded successfully
            with patch(mock_clearbit_ref, wraps=download_logo) as mock_clearbit_download:
                actual_ror_id, actual_logo_path = get_institution_logo(ror_id, url, size, width, fmt, build_path)
                self.assertEqual(ror_id, actual_ror_id)
                self.assertEqual("logo_path", actual_logo_path)
                mock_clearbit_download.assert_called_once_with(
                    company_url=url, file_path="build_path/logos/institution/size/ror_id.fmt", size=width, fmt=fmt
                )
                mock_make_url.assert_called_once_with(category="institution", entity_id=ror_id, size=size, fmt=fmt)

            mock_make_url.reset_mock()

            # Test when logo file already exists
            with patch(mock_clearbit_ref, wraps=download_logo) as mock_clearbit_download:
                actual_ror_id, actual_logo_path = get_institution_logo(ror_id, url, size, width, fmt, build_path)
                self.assertEqual(ror_id, actual_ror_id)
                self.assertEqual("logo_path", actual_logo_path)
                mock_clearbit_download.assert_not_called()
                mock_make_url.assert_called_once_with(category="institution", entity_id=ror_id, size=size, fmt=fmt)

    def test_make_entity_stats(self):
        """Test make_entity_stats"""

        # Input figures for multiple entities
        p_outputs_open = [100, 50, 30]
        n_outputs = [10, 100, 1000]
        n_outputs_open = [10, 100, 1000]
        entities = [
            Entity(
                "",
                "",
                Description("", ""),
                stats=PublicationStats(
                    p_outputs_open=p_outputs_open_, n_outputs=n_outputs_, n_outputs_open=n_outputs_open_
                ),
            )
            for p_outputs_open_, n_outputs_, n_outputs_open_ in zip(p_outputs_open, n_outputs, n_outputs_open)
        ]
        stats = make_entity_stats(entities)
        expected_stats = EntityStats(
            3,
            min=PublicationStats(p_outputs_open=30.0, n_outputs=10, n_outputs_open=10),
            max=PublicationStats(p_outputs_open=100.0, n_outputs=1000, n_outputs_open=1000),
            median=PublicationStats(p_outputs_open=50),
            histograms=EntityHistograms(
                p_outputs_open=Histogram(data=[2, 0, 1], bins=[30.0, 53.33333333333333, 76.66666666666666, 100.0]),
                n_outputs=Histogram(data=[1, 1, 1], bins=[1.0, 1.6666666666666665, 2.333333333333333, 3.0]),
                n_outputs_open=Histogram(data=[1, 1, 1], bins=[1.0, 1.6666666666666665, 2.333333333333333, 3.0]),
            ),
        )
        self.assertEqual(expected_stats, stats)


class TestInstitutionAggregation(TestCase):
    def setUp(self) -> None:
        self.ror_path = test_fixtures_folder("oa_web_workflow", "aggregate", "ror.jsonl")

    def test_ror_to_tree(self):
        # Test that the following tree is produced
        #
        # Root
        # ├── Samsung (South Korea)
        # │   ├── Samsung (Brazil)
        # │   ├── Samsung (China)
        # │   ├── Samsung (Germany)
        # │   ├── Samsung (India)
        # │   ├── Samsung (Israel)
        # │   ├── Samsung (Japan)
        # │   ├── Samsung (Poland)
        # │   ├── Samsung (Switzerland)
        # │   ├── Samsung (United Kingdom)
        # │   └── Samsung (United States)
        # │       └── Harman (United States)
        # │           ├── Harman (China)
        # │           ├── Harman (Germany)
        # │           └── Harman (United Kingdom)

        ror = load_jsonl(self.ror_path)
        tree = ror_to_tree(ror)

        expected = {
            "Root": {
                "children": [
                    {
                        "Samsung (South Korea)": {
                            "children": [
                                "Samsung (Brazil)",
                                "Samsung (China)",
                                "Samsung (Germany)",
                                "Samsung (India)",
                                "Samsung (Israel)",
                                "Samsung (Japan)",
                                "Samsung (Poland)",
                                "Samsung (Switzerland)",
                                "Samsung (United Kingdom)",
                                {
                                    "Samsung (United States)": {
                                        "children": [
                                            {
                                                "Harman (United States)": {
                                                    "children": [
                                                        "Harman (China)",
                                                        "Harman (Germany)",
                                                        "Harman (United Kingdom)",
                                                    ]
                                                }
                                            }
                                        ]
                                    }
                                },
                            ]
                        }
                    }
                ]
            }
        }
        actual = tree.to_dict()
        self.assertEqual(expected, actual)

    def test_aggregate_institutions(self):
        # Test that institution aggregation functions work

        # Load ROR
        ror = load_jsonl(self.ror_path)
        tree = ror_to_tree(ror)

        repos = [
            {"id": "arXiv", "total_outputs": 2, "category": "Preprint", "home_repo": False},
            {"id": "PubMed Central", "total_outputs": 5, "category": "Domain", "home_repo": False},
        ]

        # Make raw data
        # fmt: off
        data = [
            ["04w3jy968", 2020, 240, repos],  # Samsung (South Korea)
                ["052a20h63", 2020, 20, repos],
                ["04yt00889", 2020, 22, repos],
                ["04s4k6s39", 2020, 10, repos],
                ["04cpx2569", 2020, 12, repos],
                ["03k1ymh78", 2020, 14, repos],
                ["01x29j481", 2020, 99, repos],
                ["0381acm07", 2020, 56, repos],
                ["04prhfn63", 2020, 42, repos],
                ["01w6gjq94", 2020, 1, repos],
                ["01bfbvm65", 2020, 90, repos],  # Samsung (United States)
                    ["03yfbaq97", 2020, 17, []],  # Harman (United States)
                        ["01k2z4866", 2020, 15, [{"id": "Harman Repository (China)", "total_outputs": 15, "category": "Institution", "home_repo": True}]],
                        ["02hnr5x72", 2020, 10, []],
                        ["05fzcfb55", 2020, 5, [{"id": "Harman Repository (China)", "total_outputs": 2, "category": "Institution", "home_repo": False}]],
            ["04w3jy968", 2019, 240, repos],  # Samsung (South Korea)
                ["052a20h63", 2019, 20, repos],
                ["04yt00889", 2019, 22, repos],
                ["04s4k6s39", 2019, 10, repos],
                ["04cpx2569", 2019, 12, repos],
                ["03k1ymh78", 2019, 14, repos],
                ["01x29j481", 2019, 99, repos],
                ["0381acm07", 2019, 56, repos],
                ["04prhfn63", 2019, 42, repos],
                ["01w6gjq94", 2019, 1, repos],
                ["01bfbvm65", 2019, 90, repos],  # Samsung (United States)
                    ["03yfbaq97", 2019, 17, []],  # Harman (United States)
                        ["01k2z4866", 2019, 15, [{"id": "Harman Repository (China)", "total_outputs": 15, "category": "Institution", "home_repo": True}]],
                        ["02hnr5x72", 2019, 10, []],
                        ["05fzcfb55", 2019, 5, [{"id": "Harman Repository (China)", "total_outputs": 2, "category": "Institution", "home_repo": False}]],
        ]
        # fmt: on

        # Create dataframe
        df_data = pd.DataFrame(data, columns=["id", "year", "n_outputs", "repositories"])
        df_data.set_index(["id", "year"], inplace=True, verify_integrity=True)
        df_data.sort_index(inplace=True)

        df_inst = make_institution_df(ror, start_year=2019, end_year=2020, column_names=["n_outputs"])
        df_inst.update(df_data)

        # Aggregate data
        aggregate_institutions(tree, df_inst)

        # Check if matches expected data
        # fmt: on
        repos = [
            {"id": "arXiv", "total_outputs": 2, "category": "Preprint", "home_repo": False},
            {"id": "PubMed Central", "total_outputs": 5, "category": "Domain", "home_repo": False},
        ]
        expected = [
            ["04w3jy968", 2019, 653, [
                {"id": "arXiv", "total_outputs": 2 * 11, "category": "Preprint", "home_repo": False},
                {"id": "Harman Repository (China)", "total_outputs": 17, "category": "Institution", "home_repo": True},
                {"id": "PubMed Central", "total_outputs": 5 * 11, "category": "Domain", "home_repo": False},
            ]],  # Samsung (South Korea)
            ["052a20h63", 2019, 20, repos],
            ["04yt00889", 2019, 22, repos],
            ["04s4k6s39", 2019, 10, repos],
            ["04cpx2569", 2019, 12, repos],
            ["03k1ymh78", 2019, 14, repos],
            ["01x29j481", 2019, 99, repos],
            ["0381acm07", 2019, 56, repos],
            ["04prhfn63", 2019, 42, repos],
            ["01w6gjq94", 2019, 1, repos],
            ["01bfbvm65", 2019, 137, [
                {"id": "arXiv", "total_outputs": 2, "category": "Preprint", "home_repo": False},
                {"id": "Harman Repository (China)", "total_outputs": 17, "category": "Institution", "home_repo": True},
                {"id": "PubMed Central", "total_outputs": 5, "category": "Domain", "home_repo": False},
            ]],  # Samsung (United States)
                    ["03yfbaq97", 2019, 47, [{"id": "Harman Repository (China)", "total_outputs": 17, "category": "Institution", "home_repo": True}]],  # Harman (United States)
                        ["01k2z4866", 2019, 15, [{"id": "Harman Repository (China)", "total_outputs": 15, "category": "Institution", "home_repo": True}]],
                        ["02hnr5x72", 2019, 10, []],
                        ["05fzcfb55", 2019, 5, [{"id": "Harman Repository (China)", "total_outputs": 2, "category": "Institution", "home_repo": False}]],

            ["04w3jy968", 2020, 653, [
                {"id": "arXiv", "total_outputs": 2 * 11, "category": "Preprint", "home_repo": False},
                {"id": "Harman Repository (China)", "total_outputs": 17, "category": "Institution", "home_repo": True},
                {"id": "PubMed Central", "total_outputs": 5 * 11, "category": "Domain", "home_repo": False},
            ]],  # Samsung (South Korea)
                ["052a20h63", 2020, 20, repos],
                ["04yt00889", 2020, 22, repos],
                ["04s4k6s39", 2020, 10, repos],
                ["04cpx2569", 2020, 12, repos],
                ["03k1ymh78", 2020, 14, repos],
                ["01x29j481", 2020, 99, repos],
                ["0381acm07", 2020, 56, repos],
                ["04prhfn63", 2020, 42, repos],
                ["01w6gjq94", 2020, 1, repos],
                ["01bfbvm65", 2020, 137, [
                    {"id": "arXiv", "total_outputs": 2, "category": "Preprint", "home_repo": False},
                    {"id": "Harman Repository (China)", "total_outputs": 17, "category": "Institution", "home_repo": True},
                    {"id": "PubMed Central", "total_outputs": 5, "category": "Domain", "home_repo": False},
                ]],  # Samsung (United States)
                    ["03yfbaq97", 2020, 47, [{"id": "Harman Repository (China)", "total_outputs": 17, "category": "Institution", "home_repo": True}]], # Harman (United States)
                        ["01k2z4866", 2020, 15, [{"id": "Harman Repository (China)", "total_outputs": 15, "category": "Institution", "home_repo": True}]],
                        ["02hnr5x72", 2020, 10, []],
                        ["05fzcfb55", 2020, 5, [{"id": "Harman Repository (China)", "total_outputs": 2, "category": "Institution", "home_repo": False}]],
        ]
        # fmt: on
        expected.sort(key=lambda x: x[0])

        df_inst = df_inst.reset_index()
        actual = df_inst.values.tolist()
        self.assertEqual(expected, actual)


class TestOaWebWorkflow(ObservatoryTestCase):
    maxDiff = None
    dt_fmt = "YYYY-MM-DD"

    def setUp(self) -> None:
        """TestOaWebWorkflow checks that the workflow functions correctly, i.e. outputs the correct files, but doesn't
        check that the calculations are correct (data correctness is tested in TestOaWebRelease)."""

        # For Airflow unit tests
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")
        self.oa_web_fixtures = "oa_web_workflow"

        # For testing workflow functions
        self.release = OaWebRelease(dag_id="dag", release_date=pendulum.now(), data_bucket_name="data-bucket-name")
        self.workflow = OaWebWorkflow(input_project_id=self.project_id, output_project_id=self.project_id)
        repositories = [
            {"id": "PubMed Central", "total_outputs": 15, "category": "Domain", "home_repo": False},
            {"id": "Europe PMC", "total_outputs": 12, "category": "Domain", "home_repo": False},
            {"id": "arXiv", "total_outputs": 10, "category": "Preprint", "home_repo": False},
        ]
        self.country_index = [
            {
                "id": "NZL",
                "name": "New Zealand",
                "wikipedia_url": "https://en.wikipedia.org/wiki/New_Zealand",
                "subregion": "Australia and New Zealand",
                "region": "Oceania"
            },
        ]
        self.country_data = [
            {
                "id": "NZL",
                "year": 2020,
                "n_citations": 121,
                "n_outputs": 100,
                "n_outputs_open": 48,
                "n_outputs_publisher_open": 37,
                "n_outputs_publisher_open_only": 11,
                "n_outputs_both": 26,
                "n_outputs_other_platform_open": 37,
                "n_outputs_other_platform_open_only": 11,
                "n_outputs_closed": 52,
                "n_outputs_oa_journal": 19,
                "n_outputs_hybrid": 10,
                "n_outputs_no_guarantees": 8,
                "n_outputs_preprint": 10,
                "n_outputs_domain": 27,
                "n_outputs_institution": 0,
                "n_outputs_public": 0,
                "n_outputs_other_internet": 0,
                "repositories": repositories,
            },
            {
                "id": "NZL",
                "year": 2021,
                "n_citations": 233,
                "n_outputs": 100,
                "n_outputs_open": 45,
                "n_outputs_publisher_open": 37,
                "n_outputs_publisher_open_only": 14,
                "n_outputs_both": 23,
                "n_outputs_other_platform_open": 31,
                "n_outputs_other_platform_open_only": 8,
                "n_outputs_closed": 55,
                "n_outputs_oa_journal": 20,
                "n_outputs_hybrid": 9,
                "n_outputs_no_guarantees": 8,
                "n_outputs_preprint": 10,
                "n_outputs_domain": 27,
                "n_outputs_institution": 0,
                "n_outputs_public": 0,
                "n_outputs_other_internet": 0,
                "repositories": repositories,
            },
        ]
        self.institution_index = [
            {
                "id": "https://ror.org/02n415q13",
                "name": "Curtin University",
                "url": "https://curtin.edu.au/",
                "wikipedia_url": "https://en.wikipedia.org/wiki/Curtin_University",
                "country": "Australia",
                "subregion": "Australia and New Zealand",
                "region": "Oceania",
                "institution_types": ["Education"],
                "identifiers": {
                    "ISNI": {"all": ["0000 0004 0375 4078"]},
                    "OrgRef": {"all": ["370725"]},
                    "Wikidata": {"all": ["Q1145497"]},
                    "GRID": {"preferred": "grid.1032.0"},
                    "FundRef": {"all": ["501100001797"]},
                },
                "acronyms": []
            },
        ]
        self.institution_data = [
            {
                "id": "https://ror.org/02n415q13",
                "year": 2020,
                "n_citations": 121,
                "n_outputs": 100,
                "n_outputs_open": 48,
                "n_outputs_publisher_open": 37,
                "n_outputs_publisher_open_only": 11,
                "n_outputs_both": 26,
                "n_outputs_other_platform_open": 37,
                "n_outputs_other_platform_open_only": 11,
                "n_outputs_closed": 52,
                "n_outputs_oa_journal": 19,
                "n_outputs_hybrid": 10,
                "n_outputs_no_guarantees": 8,
                "n_outputs_preprint": 10,
                "n_outputs_domain": 27,
                "n_outputs_institution": 0,
                "n_outputs_public": 0,
                "n_outputs_other_internet": 0,
                "repositories": repositories,
            },
            {
                "id": "https://ror.org/02n415q13",
                "year": 2021,
                "n_citations": 233,
                "n_outputs": 100,
                "n_outputs_open": 45,
                "n_outputs_publisher_open": 37,
                "n_outputs_publisher_open_only": 14,
                "n_outputs_both": 23,
                "n_outputs_other_platform_open": 31,
                "n_outputs_other_platform_open_only": 8,
                "n_outputs_closed": 55,
                "n_outputs_oa_journal": 20,
                "n_outputs_hybrid": 9,
                "n_outputs_no_guarantees": 8,
                "n_outputs_preprint": 10,
                "n_outputs_domain": 27,
                "n_outputs_institution": 0,
                "n_outputs_public": 0,
                "n_outputs_other_internet": 0,
                "repositories": repositories,
            },
        ]
        self.entities = [
            ("country", self.country_index, self.country_data, ["NZL"]),
            ("institution", self.institution_index, self.institution_data, ["02n415q13"]),
        ]

    ####################################
    # Test workflow with Airflow
    ####################################

    def test_dag_structure(self):
        """Test that the DAG has the correct structure.

        :return: None
        """

        env = ObservatoryEnvironment(enable_api=False)
        with env.create():
            dag = OaWebWorkflow(input_project_id=self.project_id, output_project_id=self.project_id).make_dag()
            self.assert_dag_structure(
                {
                    "doi_sensor": ["check_dependencies"],
                    "check_dependencies": ["query"],
                    "query": ["download"],
                    "download": ["make_draft_zenodo_version"],
                    "make_draft_zenodo_version": ["download_twitter_cards"],
                    "download_twitter_cards": ["preprocess_data"],
                    "preprocess_data": ["build_indexes"],
                    "build_indexes": ["download_logos"],
                    "download_logos": ["download_wiki_descriptions"],
                    "download_wiki_descriptions": ["build_datasets"],
                    "build_datasets": ["publish_zenodo_version"],
                    "publish_zenodo_version": ["upload_dataset"],
                    "upload_dataset": ["repository_dispatch"],
                    "repository_dispatch": ["cleanup"],
                    "cleanup": [],
                },
                dag,
            )

    def test_dag_load(self):
        """Test that the DAG can be loaded from a DAG bag.

        :return: None
        """

        env = ObservatoryEnvironment(project_id=self.project_id, data_location=self.data_location, enable_api=False)
        with env.create():
            dag_file = os.path.join(module_file_path("academic_observatory_workflows.dags"), "oa_web_workflow.py")
            self.assert_dag_load("oa_web_workflow", dag_file)

    def setup_tables(
        self, dataset_id_all: str, dataset_id_settings: str, bucket_name: str, release_date: pendulum.DateTime
    ):
        ror = load_jsonl(test_fixtures_folder("doi", "ror.jsonl"))
        country = load_jsonl(test_fixtures_folder(self.oa_web_fixtures, "country.jsonl"))
        institution = load_jsonl(test_fixtures_folder(self.oa_web_fixtures, "institution.jsonl"))
        settings_country = load_jsonl(test_fixtures_folder("doi", "country.jsonl"))

        analysis_schema_path = schema_folder()
        oa_web_schema_path = test_fixtures_folder(self.oa_web_fixtures, "schema")
        with CliRunner().isolated_filesystem() as t:
            tables = [
                Table("ror", True, dataset_id_all, ror, "ror", analysis_schema_path),
                Table("country", True, dataset_id_all, country, "country", oa_web_schema_path),
                Table("institution", True, dataset_id_all, institution, "institution", oa_web_schema_path),
                Table(
                    "country",
                    False,
                    dataset_id_settings,
                    settings_country,
                    "country",
                    analysis_schema_path,
                ),
            ]

            bq_load_tables(
                tables=tables, bucket_name=bucket_name, release_date=release_date, data_location=self.data_location
            )

    @patch("academic_observatory_workflows.workflows.oa_web_workflow.Zenodo")
    @patch("academic_observatory_workflows.workflows.oa_web_workflow.trigger_repository_dispatch")
    def test_telescope(self, mock_trigger_repository_dispatch, mock_zenodo):
        """Test the telescope end to end.

        :return: None.
        """

        mock_zenodo.return_value = MockZenodo()
        execution_date = pendulum.datetime(2021, 11, 13)
        env = ObservatoryEnvironment(project_id=self.project_id, data_location=self.data_location, enable_api=False)
        dataset_id = env.add_dataset("data")
        dataset_id_settings = env.add_dataset("settings")
        data_bucket = env.add_bucket()
        github_token = "github-token"
        zenodo_token = "zenodo-token"

        with env.create() as t:
            # Add data bucket variable
            env.add_variable(Variable(key=OaWebWorkflow.DATA_BUCKET, val=data_bucket))

            # Add Github token connection
            env.add_connection(Connection(conn_id=OaWebWorkflow.GITHUB_TOKEN_CONN, uri=f"http://:{github_token}@"))

            # Add Zenodo token connection
            env.add_connection(Connection(conn_id=OaWebWorkflow.ZENODO_TOKEN_CONN, uri=f"http://:{zenodo_token}@"))

            # Run fake DOI workflow
            dag = make_dummy_dag("doi", execution_date)
            with env.create_dag_run(dag, execution_date):
                # Running all of a DAGs tasks sets the DAG to finished
                ti = env.run_task("dummy_task")
                self.assertEqual(State.SUCCESS, ti.state)

            # Upload fake data to BigQuery
            self.setup_tables(
                dataset_id_all=dataset_id,
                dataset_id_settings=dataset_id_settings,
                bucket_name=env.download_bucket,
                release_date=execution_date,
            )

            # Upload fake twitter.zip file to bucket
            file_path = os.path.join(
                module_file_path("academic_observatory_workflows.workflows.data.oa_web_workflow"), "twitter.zip"
            )
            upload_file_to_cloud_storage(data_bucket, "twitter.zip", file_path)

            # Run workflow
            workflow = OaWebWorkflow(
                input_project_id=self.project_id,
                output_project_id=self.project_id,
                agg_dataset_id=dataset_id,
                ror_dataset_id=dataset_id,
                settings_dataset_id=dataset_id_settings,
            )

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

                # Make draft Zenodo version
                ti = env.run_task(workflow.make_draft_zenodo_version.__name__)
                self.assertEqual(State.SUCCESS, ti.state)

                # Download twitter cards
                ti = env.run_task(workflow.download_twitter_cards.__name__)
                self.assertEqual(State.SUCCESS, ti.state)

                # TODO: add new steps

                # Transform data
                ti = env.run_task(workflow.transform.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                base_folder = os.path.join(
                    t, "data", "telescopes", "transform", "oa_web_workflow", "oa_web_workflow_2021_11_13"
                )
                build_folder = os.path.join(base_folder, "build")
                expected_files = make_expected_build_files(build_folder)
                print("Checking expected transformed files")
                for file in expected_files:
                    print(f"\t{file}")
                    self.assertTrue(os.path.isfile(file))

                # Check that full dataset zip file exists
                archives = ["latest.zip", "coki-oa-dataset.zip"]
                for file_name in archives:
                    latest_file = os.path.join(base_folder, file_name)
                    print(f"\t{latest_file}")
                    self.assertTrue(os.path.isfile(latest_file))

                # Publish Zenodo version
                ti = env.run_task(workflow.publish_zenodo_version.__name__)
                self.assertEqual(State.SUCCESS, ti.state)

                # Upload data to bucket
                ti = env.run_task(workflow.upload_dataset.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                blob_name = f"{workflow.version}/latest.zip"
                self.assert_blob_exists(data_bucket, blob_name)

                # Trigger repository dispatch
                ti = env.run_task(workflow.repository_dispatch.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                mock_trigger_repository_dispatch.called_once_with(github_token, "data-update/develop")
                mock_trigger_repository_dispatch.called_once_with(github_token, "data-update/staging")
                mock_trigger_repository_dispatch.called_once_with(github_token, "data-update/production")

                # Test that all telescope data deleted
                download_folder, extract_folder, transform_folder = (
                    os.path.join(t, "data", "telescopes", "download", "oa_web_workflow", "oa_web_workflow_2021_11_13"),
                    os.path.join(t, "data", "telescopes", "extract", "oa_web_workflow", "oa_web_workflow_2021_11_13"),
                    os.path.join(t, "data", "telescopes", "transform", "oa_web_workflow", "oa_web_workflow_2021_11_13"),
                )
                env.run_task(workflow.cleanup.__name__)
                self.assert_cleanup(download_folder, extract_folder, transform_folder)

    ####################################
    # Test workflow functions
    ####################################

    def save_mock_data(self, path: str, test_data):
        with jsonlines.open(path, mode="w") as writer:
            writer.write_all(test_data)
        df = pd.DataFrame(test_data)
        return df

    @patch("academic_observatory_workflows.workflows.oa_web_workflow.Variable.get")
    def test_load_data(self, mock_var_get):
        category = "country"
        with CliRunner().isolated_filesystem() as t:
            mock_var_get.return_value = t

            # Save CSV
            path = os.path.join(self.release.download_folder, f"{category}.jsonl")
            df = self.save_mock_data(path, self.countries)

            # Load csv
            actual_df = load_data(path)

            # Compare
            expected_countries = df.to_dict("records")
            actual_countries = actual_df.to_dict("records")
            self.assertEqual(expected_countries, actual_countries)

    def test_update_df_with_percentages(self):
        keys = [("hello", "n_outputs"), ("world", "n_outputs")]
        df = pd.DataFrame([{"n_hello": 20, "n_world": 50, "n_outputs": 100}])
        update_df_with_percentages(df, keys)
        expected = {"n_hello": 20, "n_world": 50, "n_outputs": 100, "p_hello": 20, "p_world": 50}
        actual = df.to_dict(orient="records")[0]
        self.assertEqual(expected, actual)

    @patch("academic_observatory_workflows.workflows.oa_web_workflow.Variable.get")
    def test_make_index(self, mock_var_get):
        with CliRunner().isolated_filesystem() as t:
            mock_var_get.return_value = t

            # Country
            category = "country"
            df_index = pd.DataFrame(self.country_index)
            df_data = pd.DataFrame(self.country_data)

            preprocess_index_df(category, df_index)
            df_country_index = make_index(category, df_index, df_data)
            expected = [
                {
                    "alpha2": "NZ",
                    "category": "country",
                    "id": "NZL",
                    "name": "New Zealand",
                    "wikipedia_url": "https://en.wikipedia.org/wiki/New_Zealand",
                    "subregion": "Australia and New Zealand",
                    "region": "Oceania",
                    "n_citations": 354,
                    "n_outputs": 200,
                    "n_outputs_open": 93,
                    "n_outputs_publisher_open": 74,
                    "n_outputs_publisher_open_only": 25,
                    "n_outputs_both": 49,
                    "n_outputs_other_platform_open": 68,
                    "n_outputs_other_platform_open_only": 19,
                    "n_outputs_closed": 107,
                    "n_outputs_oa_journal": 39,
                    "n_outputs_hybrid": 19,
                    "n_outputs_no_guarantees": 16,
                    "n_outputs_preprint": 20,
                    "n_outputs_domain": 54,
                    "n_outputs_institution": 0,
                    "n_outputs_public": 0,
                    "n_outputs_other_internet": 0,
                    "p_outputs_open": 46.5,
                    "p_outputs_publisher_open": 37.0,
                    "p_outputs_publisher_open_only": 12.5,
                    "p_outputs_both": 24.5,
                    "p_outputs_other_platform_open": 34.0,
                    "p_outputs_other_platform_open_only": 9.5,
                    "p_outputs_closed": 53.5,
                    "p_outputs_oa_journal": 52.7,
                    "p_outputs_hybrid": 25.68,
                    "p_outputs_no_guarantees": 21.62,
                    "p_outputs_preprint": 29.41,
                    "p_outputs_domain": 79.41,
                    "p_outputs_institution": 0.0,
                    "p_outputs_public": 0.0,
                    "p_outputs_other_internet": 0.0,
                }
            ]
            print("Checking country records:")
            actual = df_country_index.to_dict("records")
            for e, a in zip(expected, actual):
                self.assertDictEqual(e, a)

            # Institution
            category = "institution"
            df = pd.DataFrame(self.institution_index)
            df = preprocess_index_df(category, df)
            df_institution_index = make_index(category, df)
            expected = [
                {
                    "category": "institution",
                    "id": "02n415q13",
                    "name": "Curtin University",
                    "url": "https://curtin.edu.au/",
                    "wikipedia_url": "https://en.wikipedia.org/wiki/Curtin_University",
                    "country": "Australia",
                    "subregion": "Australia and New Zealand",
                    "region": "Oceania",
                    "institution_types": ["Education"],
                    "n_citations": 354,
                    "n_outputs": 200,
                    "n_outputs_open": 93,
                    "n_outputs_publisher_open": 74,
                    "n_outputs_publisher_open_only": 25,
                    "n_outputs_both": 49,
                    "n_outputs_other_platform_open": 68,
                    "n_outputs_other_platform_open_only": 19,
                    "n_outputs_closed": 107,
                    "n_outputs_oa_journal": 39,
                    "n_outputs_hybrid": 19,
                    "n_outputs_no_guarantees": 16,
                    "n_outputs_preprint": 20,
                    "n_outputs_domain": 54,
                    "n_outputs_institution": 0,
                    "n_outputs_public": 0,
                    "n_outputs_other_internet": 0,
                    "p_outputs_open": 46.5,
                    "p_outputs_publisher_open": 37.0,
                    "p_outputs_publisher_open_only": 12.5,
                    "p_outputs_both": 24.5,
                    "p_outputs_other_platform_open": 34.0,
                    "p_outputs_other_platform_open_only": 9.5,
                    "p_outputs_closed": 53.5,
                    "p_outputs_oa_journal": 52.7,
                    "p_outputs_hybrid": 25.68,
                    "p_outputs_no_guarantees": 21.62,
                    "p_outputs_preprint": 29.41,
                    "p_outputs_domain": 79.41,
                    "p_outputs_institution": 0.0,
                    "p_outputs_public": 0.0,
                    "p_outputs_other_internet": 0.0,
                    "identifiers": [
                        {"type": "ROR", "id": "02n415q13", "url": "https://ror.org/02n415q13"},
                        {
                            "type": "ISNI",
                            "id": "0000 0004 0375 4078",
                            "url": "https://isni.org/isni/0000 0004 0375 4078",
                        },
                        {"type": "Wikidata", "id": "Q1145497", "url": "https://www.wikidata.org/wiki/Q1145497"},
                        {"type": "GRID", "id": "grid.1032.0", "url": "https://grid.ac/institutes/grid.1032.0"},
                        {
                            "type": "FundRef",
                            "id": "501100001797",
                            "url": "https://api.crossref.org/funders/501100001797",
                        },
                    ],
                }
            ]

            print("Checking institution records:")
            actual = df_institution_index.to_dict("records")
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
            df = preprocess_df(category, df)
            df_index_table = make_index(category, df)
            update_index_with_logos(
                self.release.build_path, self.release.assets_path, category, df_index_table
            )
            for i, row in df_index_table.iterrows():
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
            institutions = self.institutions + [
                {
                    "alpha2": None,
                    "id": "https://ror.org/12345",
                    "name": "Foo University",
                    "year": 2020,
                    "date": pendulum.date(2020, 12, 31).format(self.dt_fmt),
                    "url": None,
                    "wikipedia_url": None,
                    "country": "Australia",
                    "subregion": "Australia and New Zealand",
                    "region": "Oceania",
                    "institution_types": ["Education"],
                    "n_citations": 121,
                    "n_outputs": 100,
                    "n_outputs_open": 48,
                    "n_outputs_publisher_open": 37,
                    "n_outputs_publisher_open_only": 11,
                    "n_outputs_both": 26,
                    "n_outputs_other_platform_open": 37,
                    "n_outputs_other_platform_open_only": 11,
                    "n_outputs_closed": 52,
                    "n_outputs_oa_journal": 19,
                    "n_outputs_hybrid": 10,
                    "n_outputs_no_guarantees": 8,
                    "identifiers": {
                        "ISNI": {"all": ["0000 0004 0375 4078"]},
                        "OrgRef": {"all": ["370725"]},
                        "Wikidata": {"all": ["Q1145497"]},
                        "GRID": {"preferred": "grid.1032.0"},
                        "FundRef": {"all": ["501100001797"]},
                    },
                },
            ]
            df = pd.DataFrame(institutions)
            df = preprocess_df(category, df)
            df_index_table = make_index(category, df)
            sizes = ["l", "s", "xl"]
            with vcr.use_cassette(test_fixtures_folder("oa_web_workflow", "test_make_logos.yaml")):
                update_index_with_logos(
                    self.release.build_path, self.release.assets_path, category, df_index_table
                )
                curtin_row = df_index_table.loc["02n415q13"]
                foo_row = df_index_table.loc["12345"]
                for size in sizes:
                    # Check that logo was added to dataframe
                    key = f"logo_{size}"
                    self.assertTrue(key in curtin_row)
                    self.assertTrue(key in foo_row)

                    # Check that correct path created
                    item_id = curtin_row["id"]
                    fmt = "jpg"
                    if size == "xl":
                        fmt = "png"
                    expected_curtin_path = f"/logos/{category}/{size}/{item_id}.{fmt}"
                    expected_foo_path = f"/unknown.svg"
                    self.assertEqual(expected_curtin_path, curtin_row[key])
                    self.assertEqual(expected_foo_path, foo_row[key])

                    # Check that downloaded logo exists
                    full_path = os.path.join(self.release.build_path, expected_curtin_path[1:])
                    self.assertTrue(os.path.isfile(full_path))

    @patch("academic_observatory_workflows.workflows.oa_web_workflow.Variable.get")
    def test_save_index(self, mock_var_get):
        with CliRunner().isolated_filesystem() as t:
            mock_var_get.return_value = t

            for category, data, entity_ids in self.entities:
                df = pd.DataFrame(data)
                df = preprocess_df(category, df)
                df_index_table = make_index(category, df)
                update_index_with_logos(
                    self.release.build_path, self.release.assets_path, category, df_index_table
                )
                entities = make_entities(category, df_index_table, df)
                data_path = os.path.join(self.release.build_path, "data")
                os.makedirs(data_path, exist_ok=True)
                file_path = os.path.join(data_path, f"{category}.json")
                save_index(file_path, entities)
                self.assertTrue(os.path.isfile(file_path))

    @patch("academic_observatory_workflows.workflows.oa_web_workflow.Variable.get")
    def test_make_entities(self, mock_var_get):
        with CliRunner().isolated_filesystem() as t:
            mock_var_get.return_value = t

            # Country
            category = "country"
            df = pd.DataFrame(self.countries)
            df = preprocess_df(category, df)
            df_index_table = make_index(category, df)
            entities = make_entities(category, df_index_table, df)
            repositories = [
                {"id": "PubMed Central", "total_outputs": 30, "category": "Domain", "home_repo": False},
                {"id": "Europe PMC", "total_outputs": 24, "category": "Domain", "home_repo": False},
                {"id": "arXiv", "total_outputs": 20, "category": "Preprint", "home_repo": False},
            ]
            expected = [
                {
                    "id": "NZL",
                    "name": "New Zealand",
                    "category": category,
                    "description": {
                        "license": Description.license,
                        "text": None,
                        "url": "https://en.wikipedia.org/wiki/New_Zealand",
                    },
                    "wikipedia_url": "https://en.wikipedia.org/wiki/New_Zealand",
                    "subregion": "Australia and New Zealand",
                    "region": "Oceania",
                    "end_year": 2021,
                    "start_year": 2020,
                    "stats": {
                        "n_citations": 354,
                        "n_outputs": 200,
                        "n_outputs_open": 93,
                        "n_outputs_publisher_open": 74,
                        "n_outputs_publisher_open_only": 25,
                        "n_outputs_both": 49,
                        "n_outputs_other_platform_open": 68,
                        "n_outputs_other_platform_open_only": 19,
                        "n_outputs_closed": 107,
                        "n_outputs_oa_journal": 39,
                        "n_outputs_hybrid": 19,
                        "n_outputs_no_guarantees": 16,
                        "n_outputs_preprint": 20,
                        "n_outputs_domain": 54,
                        "n_outputs_institution": 0,
                        "n_outputs_public": 0,
                        "n_outputs_other_internet": 0,
                        "p_outputs_open": 46.5,
                        "p_outputs_publisher_open": 37.0,
                        "p_outputs_publisher_open_only": 12.5,
                        "p_outputs_both": 24.5,
                        "p_outputs_other_platform_open": 34.0,
                        "p_outputs_other_platform_open_only": 9.5,
                        "p_outputs_closed": 53.5,
                        "p_outputs_oa_journal": 52.7,
                        "p_outputs_hybrid": 25.68,
                        "p_outputs_no_guarantees": 21.62,
                        "p_outputs_preprint": 29.41,
                        "p_outputs_domain": 79.41,
                        "p_outputs_institution": 0.0,
                        "p_outputs_public": 0.0,
                        "p_outputs_other_internet": 0.0,
                    },
                    "years": [
                        {
                            "year": 2020,
                            "date": "2020-12-31",
                            "stats": {
                                "n_citations": 121,
                                "n_outputs": 100,
                                "n_outputs_open": 48,
                                "n_outputs_publisher_open": 37,
                                "n_outputs_publisher_open_only": 11,
                                "n_outputs_both": 26,
                                "n_outputs_other_platform_open": 37,
                                "n_outputs_other_platform_open_only": 11,
                                "n_outputs_closed": 52,
                                "n_outputs_oa_journal": 19,
                                "n_outputs_hybrid": 10,
                                "n_outputs_no_guarantees": 8,
                                "n_outputs_preprint": 10,
                                "n_outputs_domain": 27,
                                "n_outputs_institution": 0,
                                "n_outputs_public": 0,
                                "n_outputs_other_internet": 0,
                                "p_outputs_open": 48.0,
                                "p_outputs_publisher_open": 37.0,
                                "p_outputs_publisher_open_only": 11.0,
                                "p_outputs_both": 26.0,
                                "p_outputs_other_platform_open": 37.0,
                                "p_outputs_other_platform_open_only": 11.0,
                                "p_outputs_closed": 52.0,
                                "p_outputs_oa_journal": 51.35,
                                "p_outputs_hybrid": 27.03,
                                "p_outputs_no_guarantees": 21.62,
                                "p_outputs_preprint": 27.03,
                                "p_outputs_domain": 72.97,
                                "p_outputs_institution": 0.0,
                                "p_outputs_public": 0.0,
                                "p_outputs_other_internet": 0.0,
                            },
                        },
                        {
                            "year": 2021,
                            "date": "2021-12-31",
                            "stats": {
                                "n_citations": 233,
                                "n_outputs": 100,
                                "n_outputs_open": 45,
                                "n_outputs_publisher_open": 37,
                                "n_outputs_publisher_open_only": 14,
                                "n_outputs_both": 23,
                                "n_outputs_other_platform_open": 31,
                                "n_outputs_other_platform_open_only": 8,
                                "n_outputs_closed": 55,
                                "n_outputs_oa_journal": 20,
                                "n_outputs_hybrid": 9,
                                "n_outputs_no_guarantees": 8,
                                "n_outputs_preprint": 10,
                                "n_outputs_domain": 27,
                                "n_outputs_institution": 0,
                                "n_outputs_public": 0,
                                "n_outputs_other_internet": 0,
                                "p_outputs_open": 45.0,
                                "p_outputs_publisher_open": 37.0,
                                "p_outputs_publisher_open_only": 14.0,
                                "p_outputs_both": 23.0,
                                "p_outputs_other_platform_open": 31.0,
                                "p_outputs_other_platform_open_only": 8.0,
                                "p_outputs_closed": 55.0,
                                "p_outputs_oa_journal": 54.05,
                                "p_outputs_hybrid": 24.32,
                                "p_outputs_no_guarantees": 21.62,
                                "p_outputs_preprint": 32.26,
                                "p_outputs_domain": 87.1,
                                "p_outputs_institution": 0.0,
                                "p_outputs_public": 0.0,
                                "p_outputs_other_internet": 0.0,
                            },
                        },
                    ],
                    "repositories": repositories,
                }
            ]

            for e_dict, a_entity in zip(expected, entities):
                a_dict = a_entity.to_dict()
                self.assertDictEqual(e_dict, a_dict)

        # Institution
        category = "institution"
        df = pd.DataFrame(self.institutions)
        df = preprocess_df(category, df)
        df_index_table = make_index(category, df)
        entities = make_entities(category, df_index_table, df)

        expected = [
            {
                "id": "02n415q13",
                "name": "Curtin University",
                "country": "Australia",
                "description": {
                    "license": Description.license,
                    "text": None,
                    "url": "https://en.wikipedia.org/wiki/Curtin_University",
                },
                "category": category,
                "url": "https://curtin.edu.au/",
                "wikipedia_url": "https://en.wikipedia.org/wiki/Curtin_University",
                "subregion": "Australia and New Zealand",
                "region": "Oceania",
                "institution_types": ["Education"],
                "end_year": 2021,
                "start_year": 2020,
                "identifiers": [
                    {"type": "ROR", "id": "02n415q13", "url": "https://ror.org/02n415q13"},
                    {"type": "ISNI", "id": "0000 0004 0375 4078", "url": "https://isni.org/isni/0000 0004 0375 4078"},
                    {"type": "Wikidata", "id": "Q1145497", "url": "https://www.wikidata.org/wiki/Q1145497"},
                    {"type": "GRID", "id": "grid.1032.0", "url": "https://grid.ac/institutes/grid.1032.0"},
                    {"type": "FundRef", "id": "501100001797", "url": "https://api.crossref.org/funders/501100001797"},
                ],
                "stats": {
                    "n_citations": 354,
                    "n_outputs": 200,
                    "n_outputs_open": 93,
                    "n_outputs_publisher_open": 74,
                    "n_outputs_publisher_open_only": 25,
                    "n_outputs_both": 49,
                    "n_outputs_other_platform_open": 68,
                    "n_outputs_other_platform_open_only": 19,
                    "n_outputs_closed": 107,
                    "n_outputs_oa_journal": 39,
                    "n_outputs_hybrid": 19,
                    "n_outputs_no_guarantees": 16,
                    "n_outputs_preprint": 20,
                    "n_outputs_domain": 54,
                    "n_outputs_institution": 0,
                    "n_outputs_public": 0,
                    "n_outputs_other_internet": 0,
                    "p_outputs_open": 46.5,
                    "p_outputs_publisher_open": 37.0,
                    "p_outputs_publisher_open_only": 12.5,
                    "p_outputs_both": 24.5,
                    "p_outputs_other_platform_open": 34.0,
                    "p_outputs_other_platform_open_only": 9.5,
                    "p_outputs_closed": 53.5,
                    "p_outputs_oa_journal": 52.7,
                    "p_outputs_hybrid": 25.68,
                    "p_outputs_no_guarantees": 21.62,
                    "p_outputs_preprint": 29.41,
                    "p_outputs_domain": 79.41,
                    "p_outputs_institution": 0.0,
                    "p_outputs_public": 0.0,
                    "p_outputs_other_internet": 0.0,
                },
                "years": [
                    {
                        "year": 2020,
                        "date": "2020-12-31",
                        "stats": {
                            "n_citations": 121,
                            "n_outputs": 100,
                            "n_outputs_open": 48,
                            "n_outputs_publisher_open": 37,
                            "n_outputs_publisher_open_only": 11,
                            "n_outputs_both": 26,
                            "n_outputs_other_platform_open": 37,
                            "n_outputs_other_platform_open_only": 11,
                            "n_outputs_closed": 52,
                            "n_outputs_oa_journal": 19,
                            "n_outputs_hybrid": 10,
                            "n_outputs_no_guarantees": 8,
                            "n_outputs_preprint": 10,
                            "n_outputs_domain": 27,
                            "n_outputs_institution": 0,
                            "n_outputs_public": 0,
                            "n_outputs_other_internet": 0,
                            "p_outputs_open": 48.0,
                            "p_outputs_publisher_open": 37.0,
                            "p_outputs_publisher_open_only": 11.0,
                            "p_outputs_both": 26.0,
                            "p_outputs_other_platform_open": 37.0,
                            "p_outputs_other_platform_open_only": 11.0,
                            "p_outputs_closed": 52.0,
                            "p_outputs_oa_journal": 51.35,
                            "p_outputs_hybrid": 27.03,
                            "p_outputs_no_guarantees": 21.62,
                            "p_outputs_preprint": 27.03,
                            "p_outputs_domain": 72.97,
                            "p_outputs_institution": 0.0,
                            "p_outputs_public": 0.0,
                            "p_outputs_other_internet": 0.0,
                        },
                    },
                    {
                        "year": 2021,
                        "date": "2021-12-31",
                        "stats": {
                            "n_citations": 233,
                            "n_outputs": 100,
                            "n_outputs_open": 45,
                            "n_outputs_publisher_open": 37,
                            "n_outputs_publisher_open_only": 14,
                            "n_outputs_both": 23,
                            "n_outputs_other_platform_open": 31,
                            "n_outputs_other_platform_open_only": 8,
                            "n_outputs_closed": 55,
                            "n_outputs_oa_journal": 20,
                            "n_outputs_hybrid": 9,
                            "n_outputs_no_guarantees": 8,
                            "n_outputs_preprint": 10,
                            "n_outputs_domain": 27,
                            "n_outputs_institution": 0,
                            "n_outputs_public": 0,
                            "n_outputs_other_internet": 0,
                            "p_outputs_open": 45.0,
                            "p_outputs_publisher_open": 37.0,
                            "p_outputs_publisher_open_only": 14.0,
                            "p_outputs_both": 23.0,
                            "p_outputs_other_platform_open": 31.0,
                            "p_outputs_other_platform_open_only": 8.0,
                            "p_outputs_closed": 55.0,
                            "p_outputs_oa_journal": 54.05,
                            "p_outputs_hybrid": 24.32,
                            "p_outputs_no_guarantees": 21.62,
                            "p_outputs_preprint": 32.26,
                            "p_outputs_domain": 87.1,
                            "p_outputs_institution": 0.0,
                            "p_outputs_public": 0.0,
                            "p_outputs_other_internet": 0.0,
                        },
                    },
                ],
                "repositories": repositories,
            }
        ]

        for e_dict, a_entity in zip(expected, entities):
            a_dict = a_entity.to_dict()
            self.assertDictEqual(e_dict, a_dict)

    @patch("academic_observatory_workflows.workflows.oa_web_workflow.Variable.get")
    def test_save_entities(self, mock_var_get):
        with CliRunner().isolated_filesystem() as t:
            mock_var_get.return_value = t

            for category, data, entity_ids in self.entities:
                # Read data
                df = pd.DataFrame(data)
                df = preprocess_df(category, df)

                # Save entities
                df_index_table = make_index(category, df)
                entities = make_entities(category, df_index_table, df)
                path = os.path.join(self.release.build_path, "data", category)
                save_entities(path, entities)

                # Check that entity json files are saved
                for entity_id in entity_ids:
                    file_path = os.path.join(path, f"{entity_id}.json")
                    print(f"Assert exists: {file_path}")
                    self.assertTrue(os.path.isfile(file_path))


def make_expected_build_files(base_path: str) -> List[str]:
    countries = ["AUS", "NZL"]
    institutions = ["03b94tp07", "02n415q13"]  # Auckland, Curtin
    categories = ["country"] * len(countries) + ["institution"] * len(institutions)
    entity_ids = countries + institutions
    expected = []

    # Add base data files
    data_path = os.path.join(base_path, "data")
    file_names = ["stats.json", "country.json", "institution.json", "index.json"]
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
