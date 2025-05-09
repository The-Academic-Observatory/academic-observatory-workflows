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

import json
import os
import tempfile
from typing import List
from unittest import TestCase
from unittest.mock import patch

import pendulum
from airflow.models.connection import Connection
from airflow.utils.state import State
from deepdiff import DeepDiff

import academic_observatory_workflows.oa_dashboard_workflow.workflow
from academic_observatory_workflows.config import project_path, TestConfig
from academic_observatory_workflows.oa_dashboard_workflow.tasks import (
    clean_url,
    data_file_pattern,
    EntityHistograms,
    EntityStats,
    fetch_institution_logo,
    Histogram,
    make_entity_stats,
    make_logo_url,
    OaDashboardRelease,
    yield_data_glob,
    ZenodoVersion,
)
from academic_observatory_workflows.tests.test_zenodo import MockZenodo
from observatory_platform.airflow.workflow import Workflow
from observatory_platform.files import load_jsonl, save_jsonl_gz
from observatory_platform.google.bigquery import bq_find_schema
from observatory_platform.google.gcs import gcs_upload_file
from observatory_platform.sandbox.sandbox_environment import SandboxEnvironment
from observatory_platform.sandbox.test_utils import bq_load_tables, log_diff, make_dummy_dag, SandboxTestCase, Table

academic_observatory_workflows.oa_dashboard_workflow.tasks.INCLUSION_THRESHOLD = {
    "country": 0,
    "institution": 0,
}
from academic_observatory_workflows.oa_dashboard_workflow.workflow import create_dag, DagParams


FIXTURES_FOLDER = project_path("oa_dashboard_workflow", "tests", "fixtures")
DOI_FIXTURES_FOLDER = project_path("doi_workflow", "tests", "fixtures")
DOI_SCHEMA_FOLDER = project_path("doi_workflow", "schema")
ROR_SCHEMA_FOLDER = project_path("ror_telescope", "schema")


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

    @patch("academic_observatory_workflows.oa_dashboard_workflow.tasks.make_logo_url")
    def test_get_institution_logo(self, mock_make_url):
        mock_make_url.return_value = "logo_path"
        mock_clearbit_ref = "academic_observatory_workflows.oa_dashboard_workflow.tasks.clearbit_download_logo"

        def download_logo(company_url, file_path, size, fmt):
            if not os.path.isdir(os.path.dirname(file_path)):
                os.makedirs(os.path.dirname(file_path))
            with open(file_path, "w") as f:
                f.write("foo")

        ror_id, url, size, width, fmt, build_path = "ror_id", "url.com", "size", 10, "fmt", "build_path"
        with tempfile.TemporaryDirectory():
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
                n_outputs=Histogram(
                    data=[1, 1, 1], bins=[1.041392685158225, 1.6944064825985894, 2.3474202800389543, 3.000434077479319]
                ),
                n_outputs_open=Histogram(
                    data=[1, 1, 1], bins=[1.041392685158225, 1.6944064825985894, 2.3474202800389543, 3.000434077479319]
                ),
            ),
        )
        self.assertEqual(expected_stats, stats)

    def test_load_data_glob(self):
        with tempfile.TemporaryDirectory() as t:
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


class TestOaDashboardWorkflow(SandboxTestCase):
    maxDiff = None
    dt_fmt = "YYYY-MM-DD"

    def setUp(self) -> None:
        """TestOaDashboardWorkflow checks that the workflow functions correctly, i.e. outputs the correct files, but doesn't
        check that the calculations are correct (data correctness is tested in TestOaDashboardRelease)."""

        # For Airflow unit tests
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")
        self.oa_web_fixtures = "oa_dashboard_workflow"

        # For testing workflow functions
        self.dag_id = "oa_dashboard_workflow"
        self.data_bucket_name = "data-bucket-name"
        self.conceptrecid = 1055172

    ####################################
    # Test workflow with Airflow
    ####################################

    def test_dag_structure(self):
        """Test that the DAG has the correct structure."""

        env = SandboxEnvironment()
        with env.create():
            dag_params = DagParams(
                dag_id=self.dag_id,
                cloud_workspace=env.cloud_workspace,
                data_bucket=self.data_bucket_name,
                conceptrecid=self.conceptrecid,
            )
            dag = create_dag(dag_params)
            self.assert_dag_structure(
                {
                    "doi_sensor": ["check_dependencies"],
                    "check_dependencies": ["create_dataset"],
                    "create_dataset": ["fetch_release"],
                    "fetch_release": [
                        "gke_create_storage",
                        "upload_institution_ids",
                        "create_entity_tables",
                        "add_wiki_descriptions_country",
                        "add_wiki_descriptions_institution",
                        "download_assets",
                        "download_institution_logos",
                        "export_tables",
                        "download_data",
                        "make_draft_zenodo_version",
                        "fetch_zenodo_versions",
                        "build_datasets",
                        "publish_zenodo_version",
                        "upload_dataset",
                        "repository_dispatch",
                        "cleanup_workflow",
                    ],
                    "gke_create_storage": ["upload_institution_ids"],
                    "upload_institution_ids": ["create_entity_tables"],
                    "create_entity_tables": ["add_wiki_descriptions_country"],
                    "add_wiki_descriptions_country": ["add_wiki_descriptions_institution"],
                    "add_wiki_descriptions_institution": ["download_assets"],
                    "download_assets": ["download_institution_logos"],
                    "download_institution_logos": ["export_tables"],
                    "export_tables": ["download_data"],
                    "download_data": ["make_draft_zenodo_version"],
                    "make_draft_zenodo_version": ["fetch_zenodo_versions"],
                    "fetch_zenodo_versions": ["build_datasets"],
                    "build_datasets": ["upload_dataset"],
                    "upload_dataset": ["publish_zenodo_version"],
                    "publish_zenodo_version": ["repository_dispatch"],
                    "repository_dispatch": ["gke_delete_storage"],
                    "gke_delete_storage": ["cleanup_workflow"],
                    "cleanup_workflow": [],
                },
                dag,
            )

    def test_dag_load(self):
        """Test that the DAG can be loaded from a DAG bag."""

        # Test successful
        env = SandboxEnvironment(
            workflows=[
                Workflow(
                    dag_id=self.dag_id,
                    name="Open Access Website Workflow",
                    class_name="academic_observatory_workflows.oa_dashboard_workflow.workflow",
                    cloud_workspace=self.fake_cloud_workspace,
                    kwargs=dict(
                        data_bucket=self.data_bucket_name,
                        conceptrecid=self.conceptrecid,
                    ),
                )
            ]
        )

        with env.create():
            dag_file = os.path.join(project_path(), "..", "..", "dags", "load_dags.py")
            self.assert_dag_load(self.dag_id, dag_file)

        # Test required kwargs
        env = SandboxEnvironment(
            workflows=[
                Workflow(
                    dag_id=self.dag_id,
                    name="Open Access Website Workflow",
                    class_name="academic_observatory_workflows.oa_dashboard_workflow.workflow",
                    cloud_workspace=self.fake_cloud_workspace,
                    kwargs=dict(),
                )
            ]
        )

        with env.create():
            dag_file = os.path.join(project_path(), "..", "..", "dags", "load_dags.py")
            with self.assertRaises(AssertionError) as cm:
                self.assert_dag_load(self.dag_id, dag_file)
            msg = cm.exception.args[0]
            self.assertTrue("missing 2 required positional arguments" in msg)
            self.assertTrue("data_bucket" in msg)
            self.assertTrue("conceptrecid" in msg)

    def setup_tables(
        self, dataset_id_all: str, dataset_id_settings: str, bucket_name: str, snapshot_date: pendulum.DateTime
    ):
        ror = load_jsonl(os.path.join(DOI_FIXTURES_FOLDER, "ror.jsonl"))
        settings_country = load_jsonl(os.path.join(DOI_FIXTURES_FOLDER, "country.jsonl"))
        country = load_jsonl(os.path.join(FIXTURES_FOLDER, "country.jsonl.gz"))
        institution = load_jsonl(os.path.join(FIXTURES_FOLDER, "institution.jsonl.gz"))

        with tempfile.TemporaryDirectory() as t:
            tables = [
                Table(
                    "ror",
                    True,
                    dataset_id_all,
                    ror,
                    bq_find_schema(
                        path=ROR_SCHEMA_FOLDER,
                        table_name="ror",
                        release_date=snapshot_date,
                    ),
                ),
                Table(
                    "country",
                    True,
                    dataset_id_all,
                    country,
                    bq_find_schema(path=os.path.join(FIXTURES_FOLDER, "schema"), table_name="country"),
                ),
                Table(
                    "institution",
                    True,
                    dataset_id_all,
                    institution,
                    bq_find_schema(path=os.path.join(FIXTURES_FOLDER, "schema"), table_name="institution"),
                ),
                Table(
                    "country",
                    False,
                    dataset_id_settings,
                    settings_country,
                    bq_find_schema(path=DOI_SCHEMA_FOLDER, table_name="country"),
                ),
            ]

            bq_load_tables(
                project_id=self.project_id,
                tables=tables,
                bucket_name=bucket_name,
                snapshot_date=snapshot_date,
            )

    def test_tasks(self):
        """Test data generation and transform tasks."""

        snapshot_date = pendulum.datetime(2021, 11, 21)
        env = SandboxEnvironment(project_id=self.project_id, data_location=self.data_location)
        bq_dataset_id = env.add_dataset("data")
        bq_dataset_id_settings = env.add_dataset("settings")
        bq_dataset_id_oa_dashboard = env.add_dataset("oa_dashboard")
        data_bucket = env.add_bucket()
        with env.create() as t:
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
                file_path = os.path.join(FIXTURES_FOLDER, file_name)
                gcs_upload_file(bucket_name=data_bucket, blob_name=file_name, file_path=file_path)

            # Setup workflow and connections
            entity_types = ["country", "institution"]
            cloud_workspace = env.cloud_workspace

            # Mocked and expected data
            release = OaDashboardRelease(
                dag_id=self.dag_id,
                run_id="manual__2021-11-21T0000000000-000000000",
                input_project_id=env.cloud_workspace.input_project_id,
                output_project_id=env.cloud_workspace.output_project_id,
                snapshot_date=snapshot_date,
                bq_ror_dataset_id=bq_dataset_id,
                bq_agg_dataset_id=bq_dataset_id,
                bq_settings_dataset_id=bq_dataset_id_settings,
                bq_oa_dashboard_dataset_id=bq_dataset_id_oa_dashboard,
            )

            # Run tasks
            import academic_observatory_workflows.oa_dashboard_workflow.tasks as tasks

            tasks.upload_institution_ids(release=release)
            tasks.create_entity_tables(
                release=release,
                entity_types=entity_types,
                start_year=tasks.START_YEAR,
                end_year=tasks.END_YEAR,
                inclusion_thresholds=tasks.INCLUSION_THRESHOLD,
            )
            tasks.add_wiki_descriptions(release=release, entity_type="country")
            tasks.add_wiki_descriptions(release=release, entity_type="institution")
            tasks.download_assets(release=release, bucket_name=data_bucket)
            tasks.download_institution_logos(release=release)
            tasks.export_tables(
                release=release,
                entity_types=entity_types,
                download_bucket=cloud_workspace.download_bucket,
            )
            tasks.download_data(release=release, download_bucket=cloud_workspace.download_bucket)
            tasks.build_datasets(
                release=release,
                entity_types=entity_types,
                zenodo_versions=[
                    ZenodoVersion(release_date=pendulum.datetime(2021, 1, 1), download_url="https://example.com")
                ],
                start_year=tasks.START_YEAR,
                end_year=tasks.END_YEAR,
                readme_text=tasks.README,
            )

            # Check that full dataset zip file exists
            for file_name in ["data.zip", "images.zip", "coki-oa-dataset.zip"]:
                latest_file = os.path.join(release.transform_folder, "out", file_name)
                print(f"\t{latest_file}")
                self.assertTrue(os.path.isfile(latest_file))

            # Check that assets downloaded
            for file_name in ["images.zip", "images-base.zip"]:
                path = os.path.join(release.download_folder, file_name)
                self.assertTrue(os.path.isfile(path))

            # Check that data downloaded
            for file_name in ["country-data-000000000000.jsonl.gz", "institution-data-000000000000.jsonl.gz"]:
                path = os.path.join(release.download_folder, file_name)
                self.assertTrue(os.path.isfile(path))

            # Check that the data is as expected
            for entity_type in entity_types:
                file_path = os.path.join(FIXTURES_FOLDER, "expected", f"{entity_type}.json")
                with open(file_path, "r") as f:
                    expected_data = json.load(f)
                actual_data = list(yield_data_glob(data_file_pattern(release.download_folder, entity_type)))
                diff = DeepDiff(expected_data, actual_data, ignore_order=False, significant_digits=4)
                all_matched = True
                for diff_type, changes in diff.items():
                    log_diff(diff_type, changes)
                assert all_matched, "Rows in actual content do not match expected content"

            # Check that data is transformed
            build_folder = os.path.join(release.transform_folder, "build")
            expected_files = make_expected_build_files(build_folder)
            print("Checking expected transformed files")
            for file in expected_files:
                print(f"\t{file}")
                self.assertTrue(os.path.isfile(file))

    @patch("academic_observatory_workflows.oa_dashboard_workflow.tasks.Zenodo")
    @patch("academic_observatory_workflows.oa_dashboard_workflow.tasks.trigger_repository_dispatch")
    def test_workflow(self, mock_trigger_repository_dispatch, mock_zenodo):
        """Test the telescope end to end."""

        mock_zenodo.return_value = MockZenodo()
        snapshot_date = pendulum.datetime(2021, 11, 21)
        env = SandboxEnvironment(project_id=self.project_id, data_location=self.data_location)
        bq_dataset_id = env.add_dataset("data")
        bq_dataset_id_settings = env.add_dataset("settings")
        bq_dataset_id_oa_dashboard = env.add_dataset("oa_dashboard")
        data_bucket = env.add_bucket()
        github_token = "github-token"
        zenodo_token = "zenodo-token"

        with env.create() as t:
            ##########
            # Setup and run fake DOI workflow to test sensor
            ##########

            dag = make_dummy_dag("doi", snapshot_date)
            with env.create_dag_run(dag, snapshot_date):
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
                file_path = os.path.join(FIXTURES_FOLDER, file_name)
                gcs_upload_file(bucket_name=data_bucket, blob_name=file_name, file_path=file_path)

            # Setup workflow and connections
            github_conn_id = "oa_dashboard_github_token"
            zenodo_conn_id = "oa_dashboard_zenodo_token"
            entity_types = ["country", "institution"]
            version = "v10"
            task_resources = {
                "download_assets": {"memory": "2G", "cpu": "2"},
                "download_institution_logos": {"memory": "2G", "cpu": "2"},
                "download_data": {"memory": "2G", "cpu": "2"},
                "build_datasets": {"memory": "2G", "cpu": "2"},
                "publish_zenodo_version": {"memory": "2G", "cpu": "2"},
                "upload_dataset": {"memory": "2G", "cpu": "2"},
            }
            dag_params = DagParams(
                dag_id=self.dag_id,
                cloud_workspace=env.cloud_workspace,
                data_bucket=data_bucket,
                conceptrecid=self.conceptrecid,
                bq_ror_dataset_id=bq_dataset_id,
                bq_agg_dataset_id=bq_dataset_id,
                bq_settings_dataset_id=bq_dataset_id_settings,
                bq_oa_dashboard_dataset_id=bq_dataset_id_oa_dashboard,
                retries=0,
                gke_image=TestConfig.gke_image,
                gke_namespace=TestConfig.gke_namespace,
                gke_resource_overrides=task_resources,
                gke_volume_size="100Mi",
                gke_startup_timeout_seconds=120,
            )
            dag = create_dag(dag_params)
            env.add_connection(Connection(conn_id=github_conn_id, uri=f"http://:{github_token}@"))
            env.add_connection(Connection(conn_id=zenodo_conn_id, uri=f"http://:{zenodo_token}@"))
            env.add_connection(Connection(**TestConfig.gke_cluster_connection))

            ##########
            # Run DAG
            ##########

            # Run DAG
            dag_run = dag.test(execution_date=snapshot_date, session=env.session)
            self.assertEqual(State.SUCCESS, dag_run.state)

            ##########
            # Assert results
            ##########

            # Check that data is uploaded to bucket
            blob_name = f"{version}/data.zip"
            self.assert_blob_exists(data_bucket, blob_name)
            blob_name = f"{version}/images.zip"
            self.assert_blob_exists(data_bucket, blob_name)
            blob_name = f"{version}/coki-oa-dataset.zip"
            self.assert_blob_exists(data_bucket, blob_name)

            # Check that repository dispatch called
            mock_trigger_repository_dispatch.called_once_with(github_token, "data-update/develop")
            mock_trigger_repository_dispatch.called_once_with(github_token, "data-update/staging")
            mock_trigger_repository_dispatch.called_once_with(github_token, "data-update/production")


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
