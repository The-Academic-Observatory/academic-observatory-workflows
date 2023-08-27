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
from typing import List, Dict
from unittest import TestCase
from unittest.mock import patch

import jsonlines
import pandas as pd
import pendulum
import vcr
from airflow.models.connection import Connection
from airflow.utils.state import State
from click.testing import CliRunner

import academic_observatory_workflows.workflows.oa_web_workflow
from academic_observatory_workflows.config import schema_folder, test_fixtures_folder
from academic_observatory_workflows.tests.test_zenodo import MockZenodo
from academic_observatory_workflows.workflows.oa_web_workflow import (
    Description,
    OaWebWorkflow,
    OaWebRelease,
    clean_ror_id,
    clean_url,
    fetch_institution_logo,
    make_logo_url,
    val_empty,
    make_entity_stats,
    Entity,
    PublicationStats,
    EntityStats,
    EntityHistograms,
    Histogram,
    load_data,
    preprocess_index_df,
    preprocess_data_df,
    make_index_df,
    make_entities,
    save_entities,
    update_index_with_logos,
    update_df_with_percentages,
    make_index,
    save_json,
    load_data_glob,
    save_jsonl_gz,
)
from observatory.platform.bigquery import bq_find_schema
from observatory.platform.files import load_jsonl
from observatory.platform.gcs import gcs_upload_file
from observatory.platform.observatory_config import Workflow
from observatory.platform.observatory_environment import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    Table,
    bq_load_tables,
    make_dummy_dag,
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
        expected = "logos/country/s/1234.jpg"
        actual = make_logo_url(entity_type="country", entity_id="1234", size="s", fmt="jpg")
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
                actual_ror_id, actual_logo_path = fetch_institution_logo(ror_id, url, size, width, fmt, build_path)
                self.assertEqual(ror_id, actual_ror_id)
                self.assertEqual("unknown.svg", actual_logo_path)
                mock_clearbit_download.assert_called_once_with(
                    company_url=url,
                    file_path="build_path/images/logos/institution/size/ror_id.fmt",
                    size=width,
                    fmt=fmt,
                )
                mock_make_url.assert_not_called()

            mock_make_url.reset_mock()

            # Test when logo file does not exist yet and logo is downloaded successfully
            with patch(mock_clearbit_ref, wraps=download_logo) as mock_clearbit_download:
                actual_ror_id, actual_logo_path = fetch_institution_logo(ror_id, url, size, width, fmt, build_path)
                self.assertEqual(ror_id, actual_ror_id)
                self.assertEqual("logo_path", actual_logo_path)
                mock_clearbit_download.assert_called_once_with(
                    company_url=url,
                    file_path="build_path/images/logos/institution/size/ror_id.fmt",
                    size=width,
                    fmt=fmt,
                )
                mock_make_url.assert_called_once_with(entity_type="institution", entity_id=ror_id, size=size, fmt=fmt)

            mock_make_url.reset_mock()

            # Test when logo file already exists
            with patch(mock_clearbit_ref, wraps=download_logo) as mock_clearbit_download:
                actual_ror_id, actual_logo_path = fetch_institution_logo(ror_id, url, size, width, fmt, build_path)
                self.assertEqual(ror_id, actual_ror_id)
                self.assertEqual("logo_path", actual_logo_path)
                mock_clearbit_download.assert_not_called()
                mock_make_url.assert_called_once_with(entity_type="institution", entity_id=ror_id, size=size, fmt=fmt)

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


def load_index_and_data(entity_type: str, index: List[Dict], data: List[Dict]):
    df_index = pd.DataFrame(index)
    preprocess_index_df(entity_type, df_index)

    df_data = pd.DataFrame(data)
    preprocess_data_df(entity_type, df_data)

    df_index = make_index_df(entity_type, df_index, df_data)

    return df_index, df_data


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
        self.dag_id = "oa_web_workflow"
        self.data_bucket_name = "data-bucket-name"
        self.conceptrecid = 1055172
        # self.release = OaWebRelease(dag_id="dag", snapshot_date=pendulum.now(), data_bucket_name=)
        # self.workflow = OaWebWorkflow(dag_id=self.dag_id, input_project_id=self.project_id, output_project_id=self.project_id)
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
                "region": "Oceania",
                "alpha2": "NZ",
            },
        ]
        # The n_ fields are strings because BigQuery exports integers as strings in JSON Lines exports
        self.country_data = [
            {
                "id": "NZL",
                "year": "2020",
                "n_citations": "121",
                "n_outputs": "100",
                "n_outputs_open": "48",
                "n_outputs_publisher_open": "37",
                "n_outputs_publisher_open_only": "11",
                "n_outputs_both": "26",
                "n_outputs_other_platform_open": "37",
                "n_outputs_other_platform_open_only": "11",
                "n_outputs_closed": "52",
                "n_outputs_oa_journal": "19",
                "n_outputs_hybrid": "10",
                "n_outputs_no_guarantees": "8",
                "n_outputs_preprint": "10",
                "n_outputs_domain": "27",
                "n_outputs_institution": "0",
                "n_outputs_public": "0",
                "n_outputs_other_internet": "0",
                "repositories": repositories,
            },
            {
                "id": "NZL",
                "year": 2021,
                "n_citations": "233",
                "n_outputs": "100",
                "n_outputs_open": "45",
                "n_outputs_publisher_open": "37",
                "n_outputs_publisher_open_only": "14",
                "n_outputs_both": "23",
                "n_outputs_other_platform_open": "31",
                "n_outputs_other_platform_open_only": "8",
                "n_outputs_closed": "55",
                "n_outputs_oa_journal": "20",
                "n_outputs_hybrid": "9",
                "n_outputs_no_guarantees": "8",
                "n_outputs_preprint": "10",
                "n_outputs_domain": "27",
                "n_outputs_institution": "0",
                "n_outputs_public": "0",
                "n_outputs_other_internet": "0",
                "repositories": repositories,
            },
        ]
        self.institution_index = [
            {
                "id": "https://ror.org/02n415q13",
                "name": "Curtin University",
                "url": "https://curtin.edu.au/",
                "wikipedia_url": "https://en.wikipedia.org/wiki/Curtin_University",
                "country_code": "AUS",
                "country_name": "Australia",
                "subregion": "Australia and New Zealand",
                "region": "Oceania",
                "institution_type": "Education",
                "acronyms": [],
            },
        ]
        self.institution_data = [
            {
                "id": "https://ror.org/02n415q13",
                "year": 2020,
                "n_citations": "121",
                "n_outputs": "100",
                "n_outputs_open": "48",
                "n_outputs_publisher_open": "37",
                "n_outputs_publisher_open_only": "11",
                "n_outputs_both": "26",
                "n_outputs_other_platform_open": "37",
                "n_outputs_other_platform_open_only": "11",
                "n_outputs_closed": "52",
                "n_outputs_oa_journal": "19",
                "n_outputs_hybrid": "10",
                "n_outputs_no_guarantees": "8",
                "n_outputs_preprint": "10",
                "n_outputs_domain": "27",
                "n_outputs_institution": "0",
                "n_outputs_public": "0",
                "n_outputs_other_internet": "0",
                "repositories": repositories,
            },
            {
                "id": "https://ror.org/02n415q13",
                "year": 2021,
                "n_citations": "233",
                "n_outputs": "100",
                "n_outputs_open": "45",
                "n_outputs_publisher_open": "37",
                "n_outputs_publisher_open_only": "14",
                "n_outputs_both": "23",
                "n_outputs_other_platform_open": "31",
                "n_outputs_other_platform_open_only": "8",
                "n_outputs_closed": "55",
                "n_outputs_oa_journal": "20",
                "n_outputs_hybrid": "9",
                "n_outputs_no_guarantees": "8",
                "n_outputs_preprint": "10",
                "n_outputs_domain": "27",
                "n_outputs_institution": "0",
                "n_outputs_public": "0",
                "n_outputs_other_internet": "0",
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
        """Test that the DAG has the correct structure."""

        env = ObservatoryEnvironment(enable_api=False)
        with env.create():
            dag = OaWebWorkflow(
                dag_id=self.dag_id,
                cloud_workspace=env.cloud_workspace,
                data_bucket=self.data_bucket_name,
                conceptrecid=self.conceptrecid,
            ).make_dag()
            self.assert_dag_structure(
                {
                    "doi_sensor": ["check_dependencies"],
                    "check_dependencies": ["query"],
                    "query": ["download"],
                    "download": ["make_draft_zenodo_version"],
                    "make_draft_zenodo_version": ["download_assets"],
                    "download_assets": ["preprocess_data"],
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
        """Test that the DAG can be loaded from a DAG bag."""

        # Test successful
        env = ObservatoryEnvironment(
            workflows=[
                Workflow(
                    dag_id=self.dag_id,
                    name="Open Access Website Workflow",
                    class_name="academic_observatory_workflows.workflows.oa_web_workflow.OaWebWorkflow",
                    cloud_workspace=self.fake_cloud_workspace,
                    kwargs=dict(
                        data_bucket=self.data_bucket_name,
                        conceptrecid=self.conceptrecid,
                    ),
                )
            ]
        )

        with env.create():
            self.assert_dag_load_from_config(self.dag_id)

        # Test required kwargs
        env = ObservatoryEnvironment(
            workflows=[
                Workflow(
                    dag_id=self.dag_id,
                    name="Open Access Website Workflow",
                    class_name="academic_observatory_workflows.workflows.oa_web_workflow.OaWebWorkflow",
                    cloud_workspace=self.fake_cloud_workspace,
                    kwargs=dict(),
                )
            ]
        )

        with env.create():
            with self.assertRaises(AssertionError) as cm:
                self.assert_dag_load_from_config(self.dag_id)
            msg = cm.exception.args[0]
            self.assertTrue("missing 2 required keyword-only arguments" in msg)
            self.assertTrue("data_bucket" in msg)
            self.assertTrue("conceptrecid" in msg)

    def setup_tables(
        self, dataset_id_all: str, dataset_id_settings: str, bucket_name: str, snapshot_date: pendulum.DateTime
    ):
        ror = load_jsonl(test_fixtures_folder("doi", "ror.jsonl"))
        country = load_jsonl(test_fixtures_folder(self.oa_web_fixtures, "country.jsonl.gz"))
        institution = load_jsonl(test_fixtures_folder(self.oa_web_fixtures, "institution.jsonl.gz"))
        settings_country = load_jsonl(test_fixtures_folder("doi", "country.jsonl"))

        oa_web_schema_path = test_fixtures_folder(self.oa_web_fixtures, "schema")
        with CliRunner().isolated_filesystem() as t:
            tables = [
                Table(
                    "ror",
                    True,
                    dataset_id_all,
                    ror,
                    bq_find_schema(
                        path=os.path.join(schema_folder(), "ror"), table_name="ror", release_date=snapshot_date
                    ),
                ),
                Table(
                    "country",
                    True,
                    dataset_id_all,
                    country,
                    bq_find_schema(path=oa_web_schema_path, table_name="country"),
                ),
                Table(
                    "institution",
                    True,
                    dataset_id_all,
                    institution,
                    bq_find_schema(path=oa_web_schema_path, table_name="institution"),
                ),
                Table(
                    "country",
                    False,
                    dataset_id_settings,
                    settings_country,
                    bq_find_schema(path=os.path.join(schema_folder(), "doi"), table_name="country"),
                ),
            ]

            bq_load_tables(
                project_id=self.project_id,
                tables=tables,
                bucket_name=bucket_name,
                snapshot_date=snapshot_date,
            )

    @patch("academic_observatory_workflows.workflows.oa_web_workflow.Zenodo")
    @patch("academic_observatory_workflows.workflows.oa_web_workflow.trigger_repository_dispatch")
    def test_telescope(self, mock_trigger_repository_dispatch, mock_zenodo):
        """Test the telescope end to end."""

        mock_zenodo.return_value = MockZenodo()
        execution_date = pendulum.datetime(2021, 11, 14)
        snapshot_date = pendulum.datetime(2021, 11, 21)
        env = ObservatoryEnvironment(project_id=self.project_id, data_location=self.data_location, enable_api=False)
        bq_dataset_id = env.add_dataset("data")
        bq_dataset_id_settings = env.add_dataset("settings")
        data_bucket = env.add_bucket()
        github_token = "github-token"
        zenodo_token = "zenodo-token"

        with env.create() as t:
            # Run fake DOI workflow to test sensor
            dag = make_dummy_dag("doi", execution_date)
            with env.create_dag_run(dag, execution_date):
                # Running all of a DAGs tasks sets the DAG to finished
                ti = env.run_task("dummy_task")
                self.assertEqual(State.SUCCESS, ti.state)

            # Setup dependencies
            # Upload fake data to BigQuery
            self.setup_tables(
                dataset_id_all=bq_dataset_id,
                dataset_id_settings=bq_dataset_id_settings,
                bucket_name=env.download_bucket,
                snapshot_date=snapshot_date,
            )

            # Upload fake cached zip files file to bucket
            for file_name in ["images-base.zip", "images.zip"]:
                file_path = test_fixtures_folder("oa_web_workflow", file_name)
                gcs_upload_file(bucket_name=data_bucket, blob_name=file_name, file_path=file_path)

            # Setup workflow and connections
            workflow = OaWebWorkflow(
                dag_id=self.dag_id,
                cloud_workspace=env.cloud_workspace,
                data_bucket=data_bucket,
                conceptrecid=self.conceptrecid,
                bq_ror_dataset_id=bq_dataset_id,
                bq_agg_dataset_id=bq_dataset_id,
                bq_settings_dataset_id=bq_dataset_id_settings,
            )
            dag = workflow.make_dag()
            env.add_connection(Connection(conn_id=workflow.github_conn_id, uri=f"http://:{github_token}@"))
            env.add_connection(Connection(conn_id=workflow.zenodo_conn_id, uri=f"http://:{zenodo_token}@"))

            # Run workflow
            with env.create_dag_run(dag, execution_date) as dag_run:
                # Mocked and expected data
                release = OaWebRelease(
                    dag_id=self.dag_id,
                    run_id=dag_run.run_id,
                    snapshot_date=snapshot_date,
                )

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
                expected_file_names = [
                    "country-index.jsonl.gz",
                    "institution-index.jsonl.gz",
                    "country-data-000000000000.jsonl.gz",
                    "institution-data-000000000000.jsonl.gz",
                ]
                for file_name in expected_file_names:
                    path = os.path.join(release.download_folder, file_name)
                    self.assertTrue(os.path.isfile(path))

                # Make draft Zenodo version
                ti = env.run_task(workflow.make_draft_zenodo_version.__name__)
                self.assertEqual(State.SUCCESS, ti.state)

                # Download cached assets
                ti = env.run_task(workflow.download_assets.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                expected_file_names = [
                    "images.zip",
                    "images-base.zip",
                ]
                for file_name in expected_file_names:
                    path = os.path.join(release.download_folder, file_name)
                    self.assertTrue(os.path.isfile(path))

                # Preprocess data
                ti = env.run_task(workflow.preprocess_data.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                expected_file_names = [
                    "country-data.jsonl.gz",
                    "institution-data.jsonl.gz",
                ]
                for file_name in expected_file_names:
                    path = os.path.join(release.transform_folder, "intermediate", file_name)
                    self.assertTrue(os.path.isfile(path))

                # Build indexes
                ti = env.run_task(workflow.build_indexes.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                expected_file_names = [
                    "country-index.jsonl.gz",
                    "institution-index.jsonl.gz",
                ]
                for file_name in expected_file_names:
                    path = os.path.join(release.transform_folder, "intermediate", file_name)
                    self.assertTrue(os.path.isfile(path))

                # Download logos
                ti = env.run_task(workflow.download_logos.__name__)
                self.assertEqual(State.SUCCESS, ti.state)

                # Download wiki descriptions
                ti = env.run_task(workflow.download_wiki_descriptions.__name__)
                self.assertEqual(State.SUCCESS, ti.state)

                # Build datasets
                ti = env.run_task(workflow.build_datasets.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                build_folder = os.path.join(release.transform_folder, "build")
                expected_files = make_expected_build_files(build_folder)
                print("Checking expected transformed files")
                for file in expected_files:
                    print(f"\t{file}")
                    self.assertTrue(os.path.isfile(file))

                # Check that full dataset zip file exists
                archives = ["data.zip", "images.zip", "coki-oa-dataset.zip"]
                for file_name in archives:
                    latest_file = os.path.join(release.transform_folder, "out", file_name)
                    print(f"\t{latest_file}")
                    self.assertTrue(os.path.isfile(latest_file))

                # Publish Zenodo version
                ti = env.run_task(workflow.publish_zenodo_version.__name__)
                self.assertEqual(State.SUCCESS, ti.state)

                # Upload data to bucket
                ti = env.run_task(workflow.upload_dataset.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                blob_name = f"{workflow.version}/data.zip"
                self.assert_blob_exists(data_bucket, blob_name)
                blob_name = f"{workflow.version}/images.zip"
                self.assert_blob_exists(data_bucket, blob_name)

                # Trigger repository dispatch
                ti = env.run_task(workflow.repository_dispatch.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                mock_trigger_repository_dispatch.called_once_with(github_token, "data-update/develop")
                mock_trigger_repository_dispatch.called_once_with(github_token, "data-update/staging")
                mock_trigger_repository_dispatch.called_once_with(github_token, "data-update/production")

                # Test that all workflow data deleted
                ti = env.run_task(workflow.cleanup.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                self.assert_cleanup(release.workflow_folder)

    ####################################
    # Test workflow functions
    ####################################

    def save_mock_data(self, path: str, test_data):
        with jsonlines.open(path, mode="w") as writer:
            writer.write_all(test_data)
        df = pd.DataFrame(test_data)
        return df

    def test_load_data_glob(self):
        with CliRunner().isolated_filesystem() as t:
            path = os.path.join(t, "data-000000000000.jsonl.gz")
            save_jsonl_gz(path, [{"name": "Jim"}, {"name": "David"}, {"name": "Jane"}])

            path = os.path.join(t, "data-000000000001.jsonl.gz")
            save_jsonl_gz(path, [{"name": "Joe"}, {"name": "Blogs"}, {"name": "Daniels"}])

            # Compare
            expected = [
                {"name": "Jim"},
                {"name": "David"},
                {"name": "Jane"},
                {"name": "Joe"},
                {"name": "Blogs"},
                {"name": "Daniels"},
            ]

            actual = load_data_glob(os.path.join(t, "data-*.jsonl.gz"))
            self.assertEqual(expected, actual)

    def test_load_data(self):
        entity_type = "country"
        with CliRunner().isolated_filesystem() as t:
            # Save Data
            path = os.path.join(t, f"{entity_type}-index.jsonl")
            df = self.save_mock_data(path, self.country_index)

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

    def test_make_index_df(self):
        with CliRunner().isolated_filesystem() as t:
            # Country
            entity_type = "country"
            df_index, df_data = load_index_and_data(entity_type, self.country_index, self.country_data)

            expected = [
                {
                    "alpha2": "NZ",
                    "entity_type": "country",
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
            actual = df_index.to_dict("records")
            for e, a in zip(expected, actual):
                self.assertDictEqual(e, a)

            # Institution
            entity_type = "institution"
            df_index, df_data = load_index_and_data(entity_type, self.institution_index, self.institution_data)
            expected = [
                {
                    "entity_type": "institution",
                    "id": "02n415q13",
                    "name": "Curtin University",
                    "url": "https://curtin.edu.au/",
                    "wikipedia_url": "https://en.wikipedia.org/wiki/Curtin_University",
                    "country_code": "AUS",
                    "country_name": "Australia",
                    "subregion": "Australia and New Zealand",
                    "region": "Oceania",
                    "institution_type": "Education",
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
                    "acronyms": [],
                }
            ]

            print("Checking institution records:")
            actual = df_index.to_dict("records")
            for e, a in zip(expected, actual):
                self.assertDictEqual(e, a)

    def test_update_index_with_logos(self):
        with CliRunner().isolated_filesystem() as t:
            sizes = ["sm", "md", "lg"]

            # Country table
            entity_type = "country"
            df_index, _ = load_index_and_data(entity_type, self.country_index, self.country_data)
            update_index_with_logos(t, entity_type, df_index)

            for i, row in df_index.iterrows():
                for size in sizes:
                    # Check that logo key created
                    key = f"logo_{size}"
                    self.assertTrue(key in row)

                    # Redirect to md size
                    if size == "lg":
                        size = "md"

                    # Check that correct logo path exists
                    item_id = row["id"]
                    expected_path = f"logos/{entity_type}/{size}/{item_id}.svg"
                    actual_path = row[key]
                    self.assertEqual(expected_path, actual_path)

            # Institution table
            entity_type = "institution"
            institution_index = self.institution_index + [
                {
                    "id": "https://ror.org/12345",
                    "name": "Foo University",
                    "country_name": "Australia",
                    "country_code": "AUS",
                    "subregion": "Australia and New Zealand",
                    "region": "Oceania",
                    "url": None,
                    "wikipedia_url": None,
                    "institution_type": "Education",
                }
            ]
            institution_data = self.institution_data + [
                {
                    "id": "https://ror.org/12345",
                    "year": 2020,
                    "n_citations": "121",
                    "n_outputs": "100",
                    "n_outputs_open": "48",
                    "n_outputs_publisher_open": "37",
                    "n_outputs_publisher_open_only": "11",
                    "n_outputs_both": "26",
                    "n_outputs_other_platform_open": "37",
                    "n_outputs_other_platform_open_only": "11",
                    "n_outputs_closed": "52",
                    "n_outputs_oa_journal": "19",
                    "n_outputs_hybrid": "10",
                    "n_outputs_no_guarantees": "8",
                },
            ]

            # Create index
            df_index, _ = load_index_and_data(entity_type, institution_index, institution_data)
            sizes = ["sm", "md", "lg"]
            with vcr.use_cassette(test_fixtures_folder("oa_web_workflow", "test_make_logos.yaml")):
                df_index = update_index_with_logos(t, entity_type, df_index)
                curtin_row = df_index[df_index["id"] == "02n415q13"].iloc[0]
                foo_row = df_index[df_index["id"] == "12345"].iloc[0]
                for size in sizes:
                    # Check that logo was added to dataframe
                    key = f"logo_{size}"
                    self.assertTrue(key in curtin_row)
                    self.assertTrue(key in foo_row)

                    # Check that correct path created
                    item_id = curtin_row["id"]
                    fmt = "jpg"
                    if size == "lg":
                        fmt = "png"
                    expected_curtin_path = f"logos/{entity_type}/{size}/{item_id}.{fmt}"
                    expected_foo_path = f"unknown.svg"
                    self.assertEqual(expected_curtin_path, curtin_row[key])
                    self.assertEqual(expected_foo_path, foo_row[key])

                    # Check that downloaded logo exists
                    full_path = os.path.join(t, "images", expected_curtin_path)
                    self.assertTrue(os.path.isfile(full_path))

    def test_save_index_df(self):
        with CliRunner().isolated_filesystem() as t:
            for entity_type, index, data, entity_ids in self.entities:
                # Load index
                df_index = pd.DataFrame(index)
                preprocess_index_df(entity_type, df_index)

                # Load data
                df_data = pd.DataFrame(data)
                preprocess_data_df(entity_type, df_data)

                # Make index
                df_index = make_index_df(entity_type, df_index, df_data)
                update_index_with_logos(t, entity_type, df_index)

                # Make entities
                entities = make_entities(entity_type, df_index, df_data)

                # Save index from entities
                data_path = os.path.join(t, "data")
                os.makedirs(data_path, exist_ok=True)
                file_path = os.path.join(data_path, f"{entity_type}.json")
                data = make_index(entity_type, entities)
                save_json(file_path, data)
                self.assertTrue(os.path.isfile(file_path))

    def test_make_entities(self):
        with CliRunner().isolated_filesystem() as t:
            # Country
            entity_type = "country"

            # Load index
            df_index = pd.DataFrame(self.country_index)
            preprocess_index_df(entity_type, df_index)

            # Load data
            df_data = pd.DataFrame(self.country_data)
            preprocess_data_df(entity_type, df_data)

            # Make index and entities
            df_index = make_index_df(entity_type, df_index, df_data)
            entities = make_entities(entity_type, df_index, df_data)

            repositories = [
                {"id": "PubMed Central", "total_outputs": 30, "category": "Domain", "home_repo": False},
                {"id": "Europe PMC", "total_outputs": 24, "category": "Domain", "home_repo": False},
                {"id": "arXiv", "total_outputs": 20, "category": "Preprint", "home_repo": False},
            ]
            expected = [
                {
                    "id": "NZL",
                    "name": "New Zealand",
                    "entity_type": entity_type,
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
        entity_type = "institution"

        # Load index
        df_index = pd.DataFrame(self.institution_index)
        preprocess_index_df(entity_type, df_index)

        # Load data
        df_data = pd.DataFrame(self.institution_data)
        preprocess_data_df(entity_type, df_data)

        # Make index and entities
        df_index = make_index_df(entity_type, df_index, df_data)
        entities = make_entities(entity_type, df_index, df_data)

        expected = [
            {
                "id": "02n415q13",
                "name": "Curtin University",
                "country_code": "AUS",
                "country_name": "Australia",
                "description": {
                    "license": Description.license,
                    "text": None,
                    "url": "https://en.wikipedia.org/wiki/Curtin_University",
                },
                "entity_type": entity_type,
                "url": "https://curtin.edu.au/",
                "wikipedia_url": "https://en.wikipedia.org/wiki/Curtin_University",
                "subregion": "Australia and New Zealand",
                "region": "Oceania",
                "institution_type": "Education",
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

    def test_save_entities(self):
        with CliRunner().isolated_filesystem() as t:
            for entity_type, index, data, entity_ids in self.entities:
                # Read data
                df_index = pd.DataFrame(index)
                preprocess_index_df(entity_type, df_index)

                df_data = pd.DataFrame(data)
                preprocess_data_df(entity_type, df_data)

                # Save entities
                df_index = make_index_df(entity_type, df_index, df_data)
                entities = make_entities(entity_type, df_index, df_data)
                path = os.path.join(t, "data", entity_type)
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
    for entity_type, entity_id in zip(categories, entity_ids):
        path = os.path.join(data_path, entity_type, f"{entity_id}.json")
        expected.append(path)

    # Add logos
    for entity_type, entity_id in zip(categories, entity_ids):
        for size in ["sm", "md", "lg"]:
            if entity_type == "country" and size == "lg":
                continue

            file_type = "svg"
            if entity_type == "institution":
                file_type = "jpg"
                if size == "lg":
                    file_type = "png"

            path = os.path.join(base_path, "images", "logos", entity_type, size, f"{entity_id}.{file_type}")
            expected.append(path)

    return expected
