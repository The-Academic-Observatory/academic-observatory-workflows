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
import json
import os
from typing import List
from unittest import TestCase
from unittest.mock import patch

import pendulum
from airflow.models.connection import Connection
from airflow.utils.state import State
from click.testing import CliRunner
from observatory.platform.observatory_environment import compare_lists_of_dicts

import academic_observatory_workflows.workflows.oa_web_workflow
from academic_observatory_workflows.config import schema_folder, module_file_path, test_fixtures_folder
from academic_observatory_workflows.tests.test_zenodo import MockZenodo
from academic_observatory_workflows.workflows.oa_web_workflow.oa_web_workflow import (
    OaWebWorkflow,
    OaWebRelease,
    clean_url,
    fetch_institution_logo,
    make_logo_url,
    make_entity_stats,
    EntityStats,
    EntityHistograms,
    Histogram,
    yield_data_glob,
    data_file_pattern,
)
from observatory.platform.bigquery import bq_find_schema
from observatory.platform.files import load_jsonl
from observatory.platform.files import save_jsonl_gz
from observatory.platform.gcs import gcs_upload_file
from observatory.platform.observatory_config import Workflow
from observatory.platform.observatory_environment import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    Table,
    bq_load_tables,
    make_dummy_dag,
)

# Author: James Diprose, Aniek Roelofs

academic_observatory_workflows.workflows.oa_web_workflow.INCLUSION_THRESHOLD = {"country": 0, "institution": 0}


def oa_web_test_fixtures_folder(*subdirs) -> str:
    """Get the path to the Academic Observatory Workflows test data directory.

    :return: the test data directory.
    """

    base_path = module_file_path("academic_observatory_workflows.workflows.oa_web_workflow.tests.fixtures")
    return os.path.join(base_path, *subdirs)


class TestFunctions(TestCase):
    def test_clean_url(self):
        url = "https://www.auckland.ac.nz/en.html"
        expected = "https://www.auckland.ac.nz/"
        actual = clean_url(url)
        self.assertEqual(expected, actual)

    def test_make_logo_url(self):
        expected = "logos/country/s/1234.jpg"
        actual = make_logo_url(entity_type="country", entity_id="1234", size="s", fmt="jpg")
        self.assertEqual(expected, actual)

    @patch("academic_observatory_workflows.workflows.oa_web_workflow.oa_web_workflow.make_logo_url")
    def test_get_institution_logo(self, mock_make_url):
        mock_make_url.return_value = "logo_path"
        mock_clearbit_ref = (
            "academic_observatory_workflows.workflows.oa_web_workflow.oa_web_workflow.clearbit_download_logo"
        )

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
            dict(
                stats=dict(p_outputs_open=p_outputs_open_, n_outputs=n_outputs_, n_outputs_open=n_outputs_open_),
            )
            for p_outputs_open_, n_outputs_, n_outputs_open_ in zip(p_outputs_open, n_outputs, n_outputs_open)
        ]
        stats = make_entity_stats(entities)
        expected_stats = EntityStats(
            3,
            min=dict(p_outputs_open=30.0, n_outputs=10, n_outputs_open=10),
            max=dict(p_outputs_open=100.0, n_outputs=1000, n_outputs_open=1000),
            median=dict(p_outputs_open=50),
            histograms=EntityHistograms(
                p_outputs_open=Histogram(data=[2, 0, 1], bins=[30.0, 53.33333333333333, 76.66666666666666, 100.0]),
                n_outputs=Histogram(data=[1, 1, 1], bins=[1.0, 1.6666666666666665, 2.333333333333333, 3.0]),
                n_outputs_open=Histogram(data=[1, 1, 1], bins=[1.0, 1.6666666666666665, 2.333333333333333, 3.0]),
            ),
        )
        self.assertEqual(expected_stats, stats)

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

            actual = list(yield_data_glob(os.path.join(t, "data-*.jsonl.gz")))
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
        self.dag_id = "oa_web_workflow"
        self.data_bucket_name = "data-bucket-name"
        self.conceptrecid = 1055172

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
                    "check_dependencies": ["make_bq_datasets"],
                    "make_bq_datasets": ["upload_institution_ids"],
                    "upload_institution_ids": ["create_entity_tables"],
                    "create_entity_tables": ["add_wiki_descriptions_country"],
                    "add_wiki_descriptions_country": ["add_wiki_descriptions_institution"],
                    "add_wiki_descriptions_institution": ["download_assets"],
                    "download_assets": ["download_institution_logos"],
                    "download_institution_logos": ["export_tables"],
                    "export_tables": ["download_data"],
                    "download_data": ["make_draft_zenodo_version"],
                    "make_draft_zenodo_version": ["build_datasets"],
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
                    class_name="academic_observatory_workflows.workflows.oa_web_workflow.oa_web_workflow.OaWebWorkflow",
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
                    class_name="academic_observatory_workflows.workflows.oa_web_workflow.oa_web_workflow.OaWebWorkflow",
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
        country = load_jsonl(oa_web_test_fixtures_folder("country.jsonl.gz"))
        institution = load_jsonl(oa_web_test_fixtures_folder("institution.jsonl.gz"))
        settings_country = load_jsonl(test_fixtures_folder("doi", "country.jsonl"))

        oa_web_schema_path = oa_web_test_fixtures_folder("schema")
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

    @patch("academic_observatory_workflows.workflows.oa_web_workflow.oa_web_workflow.Zenodo")
    @patch("academic_observatory_workflows.workflows.oa_web_workflow.oa_web_workflow.trigger_repository_dispatch")
    def test_telescope(self, mock_trigger_repository_dispatch, mock_zenodo):
        """Test the telescope end to end."""

        mock_zenodo.return_value = MockZenodo()
        execution_date = pendulum.datetime(2021, 11, 14)
        snapshot_date = pendulum.datetime(2021, 11, 21)
        env = ObservatoryEnvironment(project_id=self.project_id, data_location=self.data_location, enable_api=False)
        bq_dataset_id = env.add_dataset("data")
        bq_dataset_id_settings = env.add_dataset("settings")
        bq_dataset_id_oa_dashboard = env.add_dataset("oa_dashboard")
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
                file_path = oa_web_test_fixtures_folder(file_name)
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
                bq_oa_dashboard_dataset_id=bq_dataset_id_oa_dashboard,
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
                    input_project_id=env.cloud_workspace.input_project_id,
                    output_project_id=env.cloud_workspace.output_project_id,
                    snapshot_date=snapshot_date,
                    bq_ror_dataset_id=bq_dataset_id,
                    bq_agg_dataset_id=bq_dataset_id,
                    bq_settings_dataset_id=bq_dataset_id_settings,
                    bq_oa_dashboard_dataset_id=bq_dataset_id_oa_dashboard,
                )

                # DOI Sensor
                ti = env.run_task("doi_sensor")
                self.assertEqual(State.SUCCESS, ti.state)

                # Check dependencies
                ti = env.run_task(workflow.check_dependencies.__name__)
                self.assertEqual(State.SUCCESS, ti.state)

                # Make BQ datasets
                ti = env.run_task(workflow.make_bq_datasets.__name__)
                self.assertEqual(State.SUCCESS, ti.state)

                # Upload institution IDs
                # These are the institutions that were in the previous version of the dashboard to include
                ti = env.run_task(workflow.upload_institution_ids.__name__)
                self.assertEqual(State.SUCCESS, ti.state)

                # Create country and institution tables
                ti = env.run_task(workflow.create_entity_tables.__name__)
                self.assertEqual(State.SUCCESS, ti.state)

                # Fetch and add Wikipedia descriptions
                ti = env.run_task("add_wiki_descriptions_country")
                self.assertEqual(State.SUCCESS, ti.state)
                ti = env.run_task("add_wiki_descriptions_institution")
                self.assertEqual(State.SUCCESS, ti.state)

                # Download cached assets
                ti = env.run_task(workflow.download_assets.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                for file_name in ["images.zip", "images-base.zip"]:
                    path = os.path.join(release.download_folder, file_name)
                    self.assertTrue(os.path.isfile(path))

                # Download institution logos
                ti = env.run_task(workflow.download_institution_logos.__name__)
                self.assertEqual(State.SUCCESS, ti.state)

                # Export county and institution tables
                ti = env.run_task(workflow.export_tables.__name__)
                self.assertEqual(State.SUCCESS, ti.state)

                # Download data
                ti = env.run_task(workflow.download_data.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                for file_name in ["country-data-000000000000.jsonl.gz", "institution-data-000000000000.jsonl.gz"]:
                    path = os.path.join(release.download_folder, file_name)
                    self.assertTrue(os.path.isfile(path))

                # Check that the data is as expected
                for entity_type in workflow.entity_types:
                    file_path = oa_web_test_fixtures_folder("expected", f"{entity_type}.json")
                    with open(file_path, "r") as f:
                        expected_data = json.load(f)
                    actual_data = list(yield_data_glob(data_file_pattern(release.download_folder, entity_type)))

                    results = compare_lists_of_dicts(expected_data, actual_data, "id")
                    assert results, "Rows in actual content do not match expected content"

                # Make draft Zenodo version
                ti = env.run_task(workflow.make_draft_zenodo_version.__name__)
                self.assertEqual(State.SUCCESS, ti.state)

                # Preprocess data
                ti = env.run_task(workflow.build_datasets.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                build_folder = os.path.join(release.transform_folder, "build")
                expected_files = make_expected_build_files(build_folder)
                print("Checking expected transformed files")
                for file in expected_files:
                    print(f"\t{file}")
                    self.assertTrue(os.path.isfile(file))

                # Check that full dataset zip file exists
                for file_name in ["data.zip", "images.zip", "coki-oa-dataset.zip"]:
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
