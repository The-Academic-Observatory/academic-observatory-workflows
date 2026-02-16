# Copyright 2022-2025 Curtin University
# Copyright 2024-2025 UC Curation Center (California Digital Library)
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

# Author: Aniek Roelofs, James Diprose, Alex Massen-Hane


import gzip
import json
import os
import pathlib
import tempfile
from typing import Dict
from unittest.mock import patch

import boto3
import pendulum
from airflow.models import Connection, DagRun
from airflow.utils.state import State
from kubernetes.client import models as k8s

from academic_observatory_workflows.config import project_path, TestConfig
from academic_observatory_workflows.openalex_telescope.release import ManifestEntry, Meta
from academic_observatory_workflows.openalex_telescope.tasks import Manifest
from academic_observatory_workflows.openalex_telescope.telescope import create_dag, DagParams
from observatory_platform.airflow.workflow import Workflow
from observatory_platform.config import AirflowConns
from observatory_platform.dataset_api import DatasetAPI
from observatory_platform.files import load_file, save_jsonl_gz
from observatory_platform.google.bigquery import bq_sharded_table_id
from observatory_platform.sandbox.sandbox_environment import SandboxEnvironment
from observatory_platform.sandbox.test_utils import aws_bucket_test_env, load_and_parse_json, SandboxTestCase

FIXTURES_FOLDER = project_path("openalex_telescope", "tests", "fixtures")


class TestOpenAlexTelescope(SandboxTestCase):
    """Tests for the OpenAlex telescope"""

    def __init__(self, *args, **kwargs):
        super(TestOpenAlexTelescope, self).__init__(*args, **kwargs)
        self.dag_id = "openalex"
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")
        self.aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        self.aws_region_name = os.getenv("AWS_DEFAULT_REGION")

    def test_dag_structure(self):
        """Test that the DAG has the correct structure."""

        dag = create_dag(DagParams(dag_id=self.dag_id, cloud_workspace=self.fake_cloud_workspace))
        entity_names = [
            "authors",
            "concepts",
            "funders",
            "institutions",
            "publishers",
            "sources",
            "works",
            "domains",
            "fields",
            "subfields",
            "topics",
        ]
        task_ids = [
            "aws_to_gcs_transfer",
            "download",
            "transform",
            "upload_schema",
            "compare_schemas",
            "upload_files",
            "bq_load_table",
            "expire_previous_version",
        ]
        expected = {
            "wait_for_prev_dag_run": ["check_dependencies"],
            "check_dependencies": ["fetch_entities"],
            "fetch_entities": [
                "short_circuit",
                *[f"{entity_name}.{task_id}" for entity_name in entity_names for task_id in task_ids],
                "add_dataset_release",
            ],
            "short_circuit": ["create_dataset"],
            "create_dataset": [f"{entity_name}.gke_create_storage" for entity_name in entity_names],
            # Author group
            "authors.gke_create_storage": ["authors.aws_to_gcs_transfer"],
            "authors.aws_to_gcs_transfer": ["authors.download"],
            "authors.download": ["authors.transform"],
            "authors.transform": ["authors.upload_schema"],
            "authors.upload_schema": ["authors.compare_schemas"],
            "authors.compare_schemas": ["authors.upload_files"],
            "authors.upload_files": ["authors.bq_load_table"],
            "authors.bq_load_table": ["authors.expire_previous_version"],
            "authors.expire_previous_version": ["authors.gke_delete_storage"],
            "authors.gke_delete_storage": ["add_dataset_release"],
            # Concepts group
            "concepts.gke_create_storage": ["concepts.aws_to_gcs_transfer"],
            "concepts.aws_to_gcs_transfer": ["concepts.download"],
            "concepts.download": ["concepts.transform"],
            "concepts.transform": ["concepts.upload_schema"],
            "concepts.upload_schema": ["concepts.compare_schemas"],
            "concepts.compare_schemas": ["concepts.upload_files"],
            "concepts.upload_files": ["concepts.bq_load_table"],
            "concepts.bq_load_table": ["concepts.expire_previous_version"],
            "concepts.expire_previous_version": ["concepts.gke_delete_storage"],
            "concepts.gke_delete_storage": ["add_dataset_release"],
            # Funders group
            "funders.gke_create_storage": ["funders.aws_to_gcs_transfer"],
            "funders.aws_to_gcs_transfer": ["funders.download"],
            "funders.download": ["funders.transform"],
            "funders.transform": ["funders.upload_schema"],
            "funders.upload_schema": ["funders.compare_schemas"],
            "funders.compare_schemas": ["funders.upload_files"],
            "funders.upload_files": ["funders.bq_load_table"],
            "funders.bq_load_table": ["funders.expire_previous_version"],
            "funders.expire_previous_version": ["funders.gke_delete_storage"],
            "funders.gke_delete_storage": ["add_dataset_release"],
            # Institutions group
            "institutions.gke_create_storage": ["institutions.aws_to_gcs_transfer"],
            "institutions.aws_to_gcs_transfer": ["institutions.download"],
            "institutions.download": ["institutions.transform"],
            "institutions.transform": ["institutions.upload_schema"],
            "institutions.upload_schema": ["institutions.compare_schemas"],
            "institutions.compare_schemas": ["institutions.upload_files"],
            "institutions.upload_files": ["institutions.bq_load_table"],
            "institutions.bq_load_table": ["institutions.expire_previous_version"],
            "institutions.expire_previous_version": ["institutions.gke_delete_storage"],
            "institutions.gke_delete_storage": ["add_dataset_release"],
            # Publishers group
            "publishers.gke_create_storage": ["publishers.aws_to_gcs_transfer"],
            "publishers.aws_to_gcs_transfer": ["publishers.download"],
            "publishers.download": ["publishers.transform"],
            "publishers.transform": ["publishers.upload_schema"],
            "publishers.upload_schema": ["publishers.compare_schemas"],
            "publishers.compare_schemas": ["publishers.upload_files"],
            "publishers.upload_files": ["publishers.bq_load_table"],
            "publishers.bq_load_table": ["publishers.expire_previous_version"],
            "publishers.expire_previous_version": ["publishers.gke_delete_storage"],
            "publishers.gke_delete_storage": ["add_dataset_release"],
            # Sources group
            "sources.gke_create_storage": ["sources.aws_to_gcs_transfer"],
            "sources.aws_to_gcs_transfer": ["sources.download"],
            "sources.download": ["sources.transform"],
            "sources.transform": ["sources.upload_schema"],
            "sources.upload_schema": ["sources.compare_schemas"],
            "sources.compare_schemas": ["sources.upload_files"],
            "sources.upload_files": ["sources.bq_load_table"],
            "sources.bq_load_table": ["sources.expire_previous_version"],
            "sources.expire_previous_version": ["sources.gke_delete_storage"],
            "sources.gke_delete_storage": ["add_dataset_release"],
            # Works group
            "works.gke_create_storage": ["works.aws_to_gcs_transfer"],
            "works.aws_to_gcs_transfer": ["works.download"],
            "works.download": ["works.transform"],
            "works.transform": ["works.upload_schema"],
            "works.upload_schema": ["works.compare_schemas"],
            "works.compare_schemas": ["works.upload_files"],
            "works.upload_files": ["works.bq_load_table"],
            "works.bq_load_table": ["works.expire_previous_version"],
            "works.expire_previous_version": ["works.gke_delete_storage"],
            "works.gke_delete_storage": ["add_dataset_release"],
            # Domains
            "domains.gke_create_storage": ["domains.aws_to_gcs_transfer"],
            "domains.aws_to_gcs_transfer": ["domains.download"],
            "domains.download": ["domains.transform"],
            "domains.transform": ["domains.upload_schema"],
            "domains.upload_schema": ["domains.compare_schemas"],
            "domains.compare_schemas": ["domains.upload_files"],
            "domains.upload_files": ["domains.bq_load_table"],
            "domains.bq_load_table": ["domains.expire_previous_version"],
            "domains.expire_previous_version": ["domains.gke_delete_storage"],
            "domains.gke_delete_storage": ["add_dataset_release"],
            # Fields
            "fields.gke_create_storage": ["fields.aws_to_gcs_transfer"],
            "fields.aws_to_gcs_transfer": ["fields.download"],
            "fields.download": ["fields.transform"],
            "fields.transform": ["fields.upload_schema"],
            "fields.upload_schema": ["fields.compare_schemas"],
            "fields.compare_schemas": ["fields.upload_files"],
            "fields.upload_files": ["fields.bq_load_table"],
            "fields.bq_load_table": ["fields.expire_previous_version"],
            "fields.expire_previous_version": ["fields.gke_delete_storage"],
            "fields.gke_delete_storage": ["add_dataset_release"],
            # Subfields
            "subfields.gke_create_storage": ["subfields.aws_to_gcs_transfer"],
            "subfields.aws_to_gcs_transfer": ["subfields.download"],
            "subfields.download": ["subfields.transform"],
            "subfields.transform": ["subfields.upload_schema"],
            "subfields.upload_schema": ["subfields.compare_schemas"],
            "subfields.compare_schemas": ["subfields.upload_files"],
            "subfields.upload_files": ["subfields.bq_load_table"],
            "subfields.bq_load_table": ["subfields.expire_previous_version"],
            "subfields.expire_previous_version": ["subfields.gke_delete_storage"],
            "subfields.gke_delete_storage": ["add_dataset_release"],
            # Topics
            "topics.gke_create_storage": ["topics.aws_to_gcs_transfer"],
            "topics.aws_to_gcs_transfer": ["topics.download"],
            "topics.download": ["topics.transform"],
            "topics.transform": ["topics.upload_schema"],
            "topics.upload_schema": ["topics.compare_schemas"],
            "topics.compare_schemas": ["topics.upload_files"],
            "topics.upload_files": ["topics.bq_load_table"],
            "topics.bq_load_table": ["topics.expire_previous_version"],
            "topics.expire_previous_version": ["topics.gke_delete_storage"],
            "topics.gke_delete_storage": ["add_dataset_release"],
            # Cleanup
            "add_dataset_release": ["cleanup_workflow"],
            "cleanup_workflow": ["dag_run_complete"],
            "dag_run_complete": [],
        }
        self.assert_dag_structure(expected, dag)

    def test_dag_load(self):
        """Test that the OpenAlex DAG can be loaded from a DAG bag."""

        env = SandboxEnvironment(
            workflows=[
                Workflow(
                    dag_id=self.dag_id,
                    name="OpenAlex Telescope",
                    class_name="academic_observatory_workflows.openalex_telescope.telescope",
                    cloud_workspace=self.fake_cloud_workspace,
                )
            ]
        )

        with env.create():
            dag_file = os.path.join(project_path(), "..", "..", "dags", "load_dags.py")
            self.assert_dag_load(self.dag_id, dag_file)

    @patch("observatory_platform.airflow.airflow.send_slack_msg")
    def test_telescope(self, m_send_slack_msg):
        env = SandboxEnvironment(self.project_id, self.data_location)
        bq_dataset_id = env.add_dataset("openalex")
        api_bq_dataset_id = env.add_dataset("dataset_api")
        schema_folder = project_path("openalex_telescope", "schema")
        entity_names = [
            "authors",
            "concepts",
            "funders",
            "institutions",
            "publishers",
            "sources",
            "works",
            "domains",
            "fields",
            "subfields",
            "topics",
        ]
        aws_conn_id = "aws_openalex"
        conn_aws = Connection(
            conn_id=aws_conn_id,
            conn_type="aws",
            login=self.aws_access_key_id,
            password=self.aws_secret_access_key,
        )
        conn_slack = Connection(
            conn_id=AirflowConns.SLACK,
            uri="https://:my-slack-token@https%3A%2F%2Fhooks.slack.com%2Fservices",
        )
        conn_gke = Connection(
            conn_id=TestConfig.gke_cluster_connection["conn_id"],
            conn_type=TestConfig.gke_cluster_connection["conn_type"],
            extra=TestConfig.gke_cluster_connection["extra"],
        )

        # Create bucket and dataset for use in third run
        with env.create(task_logging=True), aws_bucket_test_env(
            prefix=self.dag_id, region_name=self.aws_region_name
        ) as bucket_name:
            # Add Airflow Connections
            env.add_connection(conn_aws)
            env.add_connection(conn_slack)
            env.add_connection(conn_gke)

            # Upload test dataset
            create_openalex_dataset(
                pathlib.Path(os.path.join(FIXTURES_FOLDER, "2023-04-16")),
                bucket_name,
            )

            # Build DAG
            snapshot_date = pendulum.datetime(2023, 4, 16)
            container_resources = {
                "download": {"container_resources": k8s.V1ResourceRequirements(requests={"memory": "1G", "cpu": "1"})},
                "transform": {"container_resources": k8s.V1ResourceRequirements(requests={"memory": "1G", "cpu": "1"})},
                "upload_schema": {
                    "container_resources": k8s.V1ResourceRequirements(requests={"memory": "1G", "cpu": "1"})
                },
                "upload_files": {
                    "container_resources": k8s.V1ResourceRequirements(requests={"memory": "1G", "cpu": "1"})
                },
            }
            resource_map = {
                "small": container_resources,
                "medium": container_resources,
                "large": container_resources,
            }
            gke_volume_map = {key: {"size": "100Mi"} for key in entity_names}
            dag_params = DagParams(
                dag_id=self.dag_id,
                cloud_workspace=env.cloud_workspace,
                bq_dataset_id=bq_dataset_id,
                api_bq_dataset_id=api_bq_dataset_id,
                aws_openalex_bucket=bucket_name,
                entity_names=entity_names,
                schema_folder=schema_folder,
                gke_image=TestConfig.gke_image,
                gke_namespace=TestConfig.gke_namespace,
                gke_resource_map=resource_map,
                gke_volume_map=gke_volume_map,
                test_run=True,
                retries=0,
            )
            dag = create_dag(dag_params)

            # Run DAG
            dag_run: DagRun = dag.test(execution_date=snapshot_date, session=env.session)
            self.assertEqual(State.SUCCESS, dag_run.state)

            # Make assertions
            expected_row_count = {
                "authors": 4,
                "concepts": 5,
                "institutions": 4,
                "publishers": 5,
                "sources": 4,
                "works": 4,
                "funders": 5,
                "domains": 1,
                "fields": 2,
                "subfields": 2,
                "topics": 3,
            }
            for entity_name in entity_names:
                table_id = bq_sharded_table_id(
                    env.cloud_workspace.project_id, bq_dataset_id, entity_name, snapshot_date
                )
                print(f"Checking table: {table_id}")

                # Assert expected rows
                expected_rows = expected_row_count[entity_name]
                self.assert_table_integrity(table_id, expected_rows)

                # Assert content
                expected_data = load_and_parse_json(
                    os.path.join(FIXTURES_FOLDER, "2023-04-16", "expected", f"{entity_name}.json"),
                    date_fields={"publication_date"},
                    timestamp_fields={"updated_date", "created_date"},
                )
                self.assert_table_content(table_id, expected_data, "id")

            # Should be one release in the API
            api = DatasetAPI(bq_project_id=env.cloud_workspace.project_id, bq_dataset_id=api_bq_dataset_id)
            api_releases = api.get_dataset_releases(dag_id=self.dag_id, entity_id="openalex")
            self.assertEqual(len(api_releases), 1)


def create_openalex_dataset(input_path: pathlib.Path, bucket_name: str) -> Dict:
    """Create an OpenAlex dataset based on data from the fixtures folder, uploading it to AWS S3 storage.

    :param input_path: the input path of the dataset in the fixtures folder.
    :param bucket_name: the AWS S3 bucket name.
    :return: a manifest index, with entity name as key and manfiest as value, to use for testing.
    """

    with tempfile.TemporaryDirectory() as temp_dir:
        entry_index = {}

        # Create part files and merged_ids
        for root, dirs, files in os.walk(str(input_path)):
            for filename in files:
                if "expected" not in pathlib.Path(root).parts:
                    file_path = pathlib.Path(root) / filename
                    s3_object_key = pathlib.Path(root).relative_to(input_path)
                    output_root = pathlib.Path(temp_dir) / s3_object_key

                    os.makedirs(str(output_root), exist_ok=True)

                    # For JSON: convert to JSONL, save without extension and gzip
                    if file_path.suffix == ".json":
                        file_name = f"{file_path.stem}.gz"
                        output_path = output_root / file_name
                        data = json.loads(load_file(file_path))
                        save_jsonl_gz(str(output_path), data)

                        # Build manifest entries
                        entity_name = output_root.parts[-2]
                        if entity_name not in entry_index:
                            entry_index[entity_name] = []
                        entry_index[entity_name].append(
                            ManifestEntry(
                                f"s3://{bucket_name}/{s3_object_key}/{file_name}",
                                Meta(content_length=os.path.getsize(output_path), record_count=len(data)),
                            )
                        )

                    # For CSV: gzip
                    elif file_path.suffix == ".csv":
                        output_path = output_root / f"{file_path.stem}.csv.gz"
                        data = load_file(file_path)
                        with gzip.open(output_path, "wt") as f:
                            f.write(data)

        # Create and save manifests
        manifest_index = {}
        for entity_name, entries in entry_index.items():
            content_length, record_count = 0, 0
            for entry in entries:
                content_length += entry.meta.content_length
                record_count += entry.meta.record_count

            manifest = Manifest(entries, Meta(content_length=content_length, record_count=record_count))
            manifest_index[entity_name] = manifest
            output_path = pathlib.Path(temp_dir) / "data" / entity_name / "manifest"
            with open(output_path, mode="w") as f:
                json.dump(manifest.to_dict(), f, indent=2)

        # Upload data to s3
        upload_folder_to_s3(bucket_name, temp_dir)

        return manifest_index


def upload_folder_to_s3(bucket_name: str, folder_path: str, s3_prefix=None):
    s3 = boto3.client("s3")
    for root, dirs, files in os.walk(folder_path):
        for filename in files:
            local_path = os.path.join(root, filename)
            s3_path = os.path.relpath(local_path, folder_path)

            if s3_prefix:
                s3_path = os.path.join(s3_prefix, s3_path)

            s3.upload_file(local_path, bucket_name, s3_path)
            print(f"Uploaded {local_path} to s3://{bucket_name}/{s3_path}")
