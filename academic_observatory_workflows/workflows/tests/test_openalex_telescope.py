# Copyright 2022-2023 Curtin University
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

import copy
import datetime
import gzip
import json
import os
import pathlib
import tempfile
from typing import Dict
from unittest.mock import patch

import boto3
import pendulum
from airflow.models import Connection
from airflow.utils.state import State
from click.testing import CliRunner
from google.cloud.exceptions import NotFound

from academic_observatory_workflows.config import test_fixtures_folder
from academic_observatory_workflows.workflows.openalex_telescope import (
    OpenAlexRelease,
    OpenAlexTelescope,
    ManifestEntry,
    Manifest,
    Meta,
    MergedId,
    transform_object,
    s3_uri_parts,
    OpenAlexEntity,
    fetch_manifest,
    fetch_merged_ids,
    bq_compare_schemas,
)
from academic_observatory_workflows.workflows.openalex_telescope import (
    parse_release_msg,
)
from observatory.platform.config import AirflowConns
from observatory.platform.api import get_dataset_releases
from observatory.platform.bigquery import bq_table_id, bq_sharded_table_id
from observatory.platform.files import save_jsonl_gz, load_file
from observatory.platform.gcs import gcs_blob_name_from_path
from observatory.platform.observatory_config import Workflow, CloudWorkspace
from observatory.platform.observatory_environment import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    aws_bucket_test_env,
)
from observatory.platform.observatory_environment import (
    find_free_port,
    load_and_parse_json,
)


class TestOpenAlexUtils(ObservatoryTestCase):
    def __init__(self, *args, **kwargs):
        super(TestOpenAlexUtils, self).__init__(*args, **kwargs)
        self.dag_id = "openalex"
        self.aws_key = (os.getenv("AWS_ACCESS_KEY_ID"), os.getenv("AWS_SECRET_ACCESS_KEY"))
        self.aws_region_name = os.getenv("AWS_DEFAULT_REGION")

    def test_s3_uri_parts(self):
        # Bucket and object
        bucket_name, object_key = s3_uri_parts("s3://mybucketname/path/to/object.txt")
        self.assertEqual("mybucketname", bucket_name)
        self.assertEqual("path/to/object.txt", object_key)

        # Just bucket
        bucket_name, object_key = s3_uri_parts("s3://mybucketname")
        self.assertEqual("mybucketname", bucket_name)
        self.assertIsNone(object_key)

        # No s3://
        with self.assertRaises(ValueError):
            s3_uri_parts("mybucketname/path/to/object.txt")

    def test_meta(self):
        # Equal
        self.assertEqual(Meta(7073, 4), Meta(7073, 4))

        # Not equal
        self.assertNotEqual(Meta(7073, 3), Meta(7073, 4))
        self.assertNotEqual(Meta(7072, 4), Meta(7073, 4))

        # From Dict
        meta = Meta.from_dict(dict(content_length=7073, record_count=4))
        self.assertIsInstance(meta, Meta)
        self.assertEqual(Meta(7073, 4), meta)

        # To Dict
        obj = Meta(7073, 4).to_dict()
        self.assertIsInstance(obj, dict)
        self.assertEqual(dict(content_length=7073, record_count=4), obj)

    def test_manifest_entry(self):
        # Test manifest equality
        manifest_a = Manifest(
            [
                ManifestEntry("s3://openalex/data/works/updated_date=2022-12-20/part_000.gz", Meta(7073, 4)),
                ManifestEntry("s3://openalex/data/works/updated_date=2022-12-21/part_000.gz", Meta(9018, 8)),
                ManifestEntry("s3://openalex/data/works/updated_date=2022-12-22/part_000.gz", Meta(4035, 4)),
            ],
            Meta(20126, 16),
        )
        manifest_b = Manifest(
            [
                ManifestEntry("s3://openalex/data/works/updated_date=2022-12-21/part_000.gz", Meta(9018, 8)),
                ManifestEntry("s3://openalex/data/works/updated_date=2022-12-20/part_000.gz", Meta(7073, 4)),
                ManifestEntry("s3://openalex/data/works/updated_date=2022-12-22/part_000.gz", Meta(4035, 4)),
            ],
            Meta(20126, 16),
        )
        manifest_c = Manifest(
            [
                ManifestEntry("s3://openalex/data/works/updated_date=2023-03-28/part_013.gz", Meta(866388416, 721951)),
                ManifestEntry("s3://openalex/data/works/updated_date=2023-03-28/part_014.gz", Meta(860530408, 709123)),
                ManifestEntry("s3://openalex/data/works/updated_date=2023-03-28/part_015.gz", Meta(321944435, 262846)),
            ],
            Meta(2048863259, 1693920),
        )

        # Assert that two manifest instances with the same data are equal
        self.assertEqual(manifest_a, copy.copy(manifest_a))

        # Assert that two manifest instances with entries in a different order are not equal
        self.assertNotEqual(manifest_a, manifest_b)

        # Assert that two manifest instances with completely different data are not equal
        self.assertNotEqual(manifest_a, manifest_c)

        # Assert that manifest ManifestEntry parses updated_date and file_name properly
        entry = ManifestEntry("s3://openalex/data/works/updated_date=2022-12-20/part_000.gz", Meta(7073, 4))
        self.assertEqual(pendulum.datetime(2022, 12, 20), entry.updated_date)
        self.assertEqual("part_000.gz", entry.file_name)

        # Assert that manifest entry without a s3:// url prefix is still valid.
        manifest_entry_no_s3 = ManifestEntry("openalex/data/works/updated_date=2022-12-20/part_000.gz", Meta(7073, 4))
        self.assertEqual(manifest_entry_no_s3.url, "s3://openalex/data/works/updated_date=2022-12-20/part_000.gz")

        # object_key
        manifest_entry = ManifestEntry("s3://openalex/data/works/updated_date=2022-12-20/part_000.gz", Meta(7073, 4))
        self.assertEqual("data/works/updated_date=2022-12-20/part_000.gz", manifest_entry.object_key)

        # updated_date
        self.assertEqual(pendulum.datetime(2022, 12, 20), manifest_entry.updated_date)

        # file_name
        self.assertEqual("part_000.gz", manifest_entry.file_name)

        # from_dict
        manifest_entry_dict = dict(
            url="s3://openalex/data/works/updated_date=2022-12-20/part_000.gz",
            meta=dict(content_length=7073, record_count=4),
        )
        obj = ManifestEntry.from_dict(manifest_entry_dict)

        self.assertIsInstance(obj, ManifestEntry)
        self.assertEqual(manifest_entry, obj)

        # to_dict
        obj = manifest_entry.to_dict()
        self.assertIsInstance(obj, dict)
        self.assertEqual(
            manifest_entry_dict,
            obj,
        )

    def test_manifest(self):
        # Equality
        self.assertEqual(
            Manifest(
                [ManifestEntry("s3://openalex/data/works/updated_date=2022-12-20/part_000.gz", Meta(7073, 4))],
                Meta(7073, 4),
            ),
            Manifest(
                [ManifestEntry("s3://openalex/data/works/updated_date=2022-12-20/part_000.gz", Meta(7073, 4))],
                Meta(7073, 4),
            ),
        )
        self.assertNotEqual(
            Manifest(
                [ManifestEntry("s3://openalex/data/works/updated_date=2022-12-20/part_000.gz", Meta(7073, 4))],
                Meta(7073, 2),
            ),
            Manifest(
                [ManifestEntry("s3://openalex/data/works/updated_date=2022-12-20/part_000.gz", Meta(7073, 4))],
                Meta(7073, 3),
            ),
        )

        # from_dict
        manifest = Manifest(
            [ManifestEntry("s3://openalex/data/works/updated_date=2022-12-20/part_000.gz", Meta(7073, 4))],
            Meta(7073, 4),
        )
        manifest_dict = dict(
            entries=[
                dict(
                    url="s3://openalex/data/works/updated_date=2022-12-20/part_000.gz",
                    meta=dict(content_length=7073, record_count=4),
                )
            ],
            meta=dict(content_length=7073, record_count=4),
        )
        obj = Manifest.from_dict(manifest_dict)
        self.assertIsInstance(obj, Manifest)
        self.assertEqual(manifest, obj)

        # to_dict
        obj = manifest.to_dict()
        self.assertIsInstance(obj, dict)
        self.assertEqual(
            manifest_dict,
            obj,
        )

    def test_merged_id(self):
        # Equality
        self.assertEqual(
            MergedId("s3://openalex/data/merged_ids/authors/2022-10-12.csv.gz", 1000),
            MergedId("s3://openalex/data/merged_ids/authors/2022-10-12.csv.gz", 1000),
        )
        self.assertNotEqual(
            MergedId("s3://openalex/data/merged_ids/authors/2022-10-11.csv.gz", 1000),
            MergedId("s3://openalex/data/merged_ids/authors/2022-10-12.csv.gz", 1000),
        )
        self.assertNotEqual(
            MergedId("s3://openalex/data/merged_ids/authors/2022-10-12.csv.gz", 1000),
            MergedId("s3://openalex/data/merged_ids/authors/2022-10-12.csv.gz", 1001),
        )

        # object_key
        merged_id = MergedId("s3://openalex/data/merged_ids/authors/2022-10-12.csv.gz", 1000)
        self.assertEqual("data/merged_ids/authors/2022-10-12.csv.gz", merged_id.object_key)

        # updated_date
        self.assertEqual(pendulum.datetime(2022, 10, 12), merged_id.updated_date)

        # file_name
        self.assertEqual("2022-10-12.csv.gz", merged_id.file_name)

        # from_dict
        merged_id_dict = dict(url="s3://openalex/data/merged_ids/authors/2022-10-12.csv.gz", content_length=1000)
        obj = MergedId.from_dict(merged_id_dict)
        self.assertIsInstance(obj, MergedId)
        self.assertEqual(merged_id, obj)

        # to_dict
        obj = merged_id.to_dict()
        self.assertIsInstance(obj, dict)
        self.assertEqual(
            merged_id_dict,
            obj,
        )

    @patch("observatory.platform.airflow.Variable.get")
    def test_openalex_entity(self, m_variable_get):
        with CliRunner().isolated_filesystem() as t:
            m_variable_get.return_value = t
            dag_id = "openalex"
            workflow = OpenAlexTelescope(
                dag_id=dag_id,
                cloud_workspace=CloudWorkspace(
                    project_id="project-id",
                    download_bucket="download_bucket",
                    transform_bucket="transform_bucket",
                    data_location="us",
                ),
            )
            is_first_run = True
            entity = OpenAlexEntity(
                "authors",
                pendulum.datetime(2023, 1, 1),
                pendulum.datetime(2023, 1, 31),
                Manifest(
                    [ManifestEntry("s3://openalex/data/authors/updated_date=2023-01-28/part_000.gz", Meta(7073, 4))],
                    Meta(7073, 4),
                ),
                [MergedId("s3://openalex/data/merged_ids/authors/2023-01-28.csv.gz", 1000)],
                is_first_run,
                pendulum.datetime(2022, 12, 1),
            )
            release = OpenAlexRelease(
                dag_id=dag_id,
                run_id="scheduled__2023-02-01T00:00:00+00:00",
                cloud_workspace=workflow.cloud_workspace,
                bq_dataset_id=workflow.bq_dataset_id,
                entities=[entity],
                start_date=pendulum.datetime(2023, 1, 1),
                end_date=pendulum.datetime(2023, 1, 31),
                is_first_run=is_first_run,
                schema_folder=workflow.schema_folder,
            )
            entity.release = release
            entity.workflow = workflow

            # table_description
            self.assertEqual(
                "OpenAlex authors table: https://docs.openalex.org/api-entities/authors", entity.table_description
            )

            # schema_file_path
            self.assertTrue(entity.schema_file_path.endswith("authors.json"))

            # upsert_uri
            self.assertTrue(entity.upsert_uri.endswith("data/authors/*"))

            # merged_ids_uri
            self.assertTrue(entity.merged_ids_uri.endswith("data/merged_ids/authors/*"))

            # merged_ids_schema_file_path
            self.assertTrue(entity.merged_ids_schema_file_path.endswith("merged_ids.json"))

            # bq_main_table_id
            self.assertEqual("project-id.openalex.authors", entity.bq_main_table_id)

            # bq_upsert_table_id
            self.assertEqual("project-id.openalex.authors_upsert", entity.bq_upsert_table_id)

            # bq_snapshot_table_id
            self.assertEqual("project-id.openalex.authors_snapshot20221201", entity.bq_snapshot_table_id)

            # current_entries
            self.assertEqual(
                [ManifestEntry("s3://openalex/data/authors/updated_date=2023-01-28/part_000.gz", Meta(7073, 4))],
                entity.current_entries,
            )

            # has_merged_ids
            self.assertFalse(entity.has_merged_ids)
            self.assertEqual(0, len(entity.current_merged_ids))
            entity.is_first_run = False
            self.assertTrue(entity.has_merged_ids)
            self.assertEqual(1, len(entity.current_merged_ids))
            self.assertEqual(
                [MergedId("s3://openalex/data/merged_ids/authors/2023-01-28.csv.gz", 1000)], entity.current_merged_ids
            )

    def test_fetch_manifest(self):
        manifest_path = test_fixtures_folder(self.dag_id, "manifest")
        with aws_bucket_test_env(prefix=self.dag_id, region_name=self.aws_region_name) as bucket_name:
            s3 = boto3.client("s3")
            s3_object_key = "data/publishers/manifest"
            with open(manifest_path, "rb") as f:
                s3.upload_fileobj(f, bucket_name, s3_object_key)

            actual = fetch_manifest(bucket=bucket_name, aws_key=self.aws_key, entity_name="publishers")
            with open(manifest_path, "r") as f:
                expected = Manifest.from_dict(json.load(f))
            self.assertEqual(expected, actual)

    def test_fetch_merged_ids(self):
        with aws_bucket_test_env(prefix=self.dag_id, region_name=self.aws_region_name) as bucket_name:
            # Create empty files on bucket to act as merged_ids and expected MergedIds
            s3 = boto3.client("s3")
            file_names = ["2023-01-19.csv.gz", "2023-03-23.csv.gz", "2023-03-28.csv.gz"]
            expected = []
            for file_name in file_names:
                object_key = f"data/merged_ids/authors/{file_name}"
                s3.put_object(Bucket=bucket_name, Key=object_key, Body=b"")
                expected.append(MergedId(f"s3://{bucket_name}/{object_key}", 0))

            actual = fetch_merged_ids(bucket=bucket_name, aws_key=self.aws_key, entity_name="authors")
            self.assertEqual(expected, actual)

    def test_transform_object(self):
        """Test the transform_object function."""

        # Null
        obj = {"corresponding_institution_ids": None}
        transform_object(obj)
        self.assertDictEqual(
            {"corresponding_institution_ids": []},
            obj,
        )

        # Null
        obj = {"corresponding_author_ids": None}
        transform_object(obj)
        self.assertDictEqual(
            {"corresponding_author_ids": []},
            obj,
        )

        # Null in array
        obj = {"corresponding_institution_ids": [None]}
        transform_object(obj)
        self.assertDictEqual(
            {"corresponding_institution_ids": []},
            obj,
        )

        # Null in array
        obj = {"corresponding_author_ids": [None]}
        transform_object(obj)
        self.assertDictEqual(
            {"corresponding_author_ids": []},
            obj,
        )

        # Test object with nested "international" fields
        obj1 = {
            "international": {
                "display_name": {
                    "af": "Dokumentbestuurstelsel",
                    "fr": "type de logiciel",
                    "ro": "colecție organizată a documentelor",
                }
            }
        }
        transform_object(obj1)
        self.assertDictEqual(
            {
                "international": {
                    "display_name": {
                        "keys": ["af", "fr", "ro"],
                        "values": [
                            "Dokumentbestuurstelsel",
                            "type de logiciel",
                            "colecție organizată " "a documentelor",
                        ],
                    }
                }
            },
            obj1,
        )

        # Test object with nested "international" none
        obj2 = {"international": {"display_name": None}}
        transform_object(obj2)
        self.assertDictEqual({"international": {"display_name": None}}, obj2)

        # Test object with nested "abstract_inverted_index" fields
        obj3 = {
            "abstract_inverted_index": {
                "Malignant": [0],
                "hyperthermia": [1],
                "susceptibility": [2],
                "(MHS)": [3],
                "is": [4, 6],
                "primarily": [5],
            }
        }
        transform_object(obj3)
        self.assertDictEqual(
            {
                "abstract_inverted_index": {
                    "keys": ["Malignant", "hyperthermia", "susceptibility", "(MHS)", "is", "primarily"],
                    "values": ["0", "1", "2", "3", "4, 6", "5"],
                }
            },
            obj3,
        )

        # Test object with nested "abstract_inverted_index" none
        obj4 = {"abstract_inverted_index": None}
        transform_object(obj4)
        self.assertDictEqual({"abstract_inverted_index": None}, obj4)

    def test_bq_compare_schemas(self):
        # Test with matching fields and types.
        expected = [
            {"name": "field1", "type": "STRING", "mode": "NULLABLE"},
            {"name": "field2", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "field3", "type": "FLOAT", "mode": "REQUIRED"},
        ]
        actual = [
            {"name": "field1", "type": "STRING", "mode": "REQUIRED"},
            {"name": "field2", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "field3", "type": "FLOAT", "mode": "REQUIRED", "description": ""},
        ]

        self.assertTrue(bq_compare_schemas(expected, actual, True))

        # Test with non-matching number of fields.
        expected = [
            {"name": "field1", "type": "STRING", "mode": "REQUIRED"},
            {"name": "field2", "type": "INTEGER", "mode": "REQUIRED"},
        ]
        actual = [
            {"name": "field1", "type": "STRING", "mode": "REQUIRED"},
            {"name": "field2", "type": "FLOAT", "mode": "REQUIRED"},
            {"name": "field3", "type": "INTEGER", "mode": "REQUIRED"},
        ]

        self.assertFalse(bq_compare_schemas(expected, actual, True))

        # Test with non-matching field names
        expected = [
            {"name": "field1", "type": "STRING", "mode": "REQUIRED"},
            {"name": "field2", "type": "INTEGER", "mode": "REQUIRED"},
        ]
        actual = [
            {"name": "field1", "type": "STRING", "mode": "REQUIRED"},
            {"name": "field3", "type": "FLOAT", "mode": "REQUIRED"},
        ]

        self.assertFalse(bq_compare_schemas(expected, actual, True))

        # Test with non-matching data types
        expected = [
            {"name": "field1", "type": "STRING", "mode": "REQUIRED"},
            {"name": "field2", "type": "INTEGER", "mode": "REQUIRED"},
        ]
        actual = [
            {"name": "field1", "type": "STRING", "mode": "REQUIRED"},
            {"name": "field2", "type": "FLOAT", "mode": "REQUIRED"},
        ]

        self.assertFalse(bq_compare_schemas(expected, actual, True))

        # Test with non-matching sub fields
        expected = [
            {
                "name": "field1",
                "type": "STRING",
                "mode": "REQUIRED",
                "fields": [{"name": "field3", "type": "FLOAT", "mode": "REQUIRED"}],
            },
            {"name": "field2", "type": "INTEGER", "mode": "REQUIRED"},
        ]
        actual = [
            {"name": "field1", "type": "STRING", "mode": "REQUIRED"},
            {"name": "field2", "type": "INTEGER", "mode": "REQUIRED"},
        ]

        self.assertFalse(bq_compare_schemas(expected, actual, True))


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


class TestOpenAlexTelescope(ObservatoryTestCase):
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

        workflow = OpenAlexTelescope(
            dag_id=self.dag_id,
            cloud_workspace=self.fake_cloud_workspace,
        )
        dag = workflow.make_dag()
        dag_structure = {}
        for task_id, task in dag.task_dict.items():
            dag_structure[task_id] = list(task.downstream_task_ids)
        structure = {
            "wait_for_prev_dag_run": ["check_dependencies"],
            "check_dependencies": ["fetch_releases"],
            "fetch_releases": ["create_datasets"],
            "create_datasets": ["aws_to_gcs_transfer"],
            "aws_to_gcs_transfer": ["branch"],
            "branch": [
                "works.bq_snapshot",
                "sources.bq_snapshot",
                "authors.bq_snapshot",
                "funders.bq_snapshot",
                "institutions.bq_snapshot",
                "publishers.bq_snapshot",
                "concepts.bq_snapshot",
            ],
        }
        task_ids = [
            "bq_snapshot",
            "download",
            "transform",
            "upload_schema",
            "compare_schemas",
            "upload",
            "bq_load_upserts",
            "bq_upsert_records",
            "bq_load_deletes",
            "bq_delete_records",
            "add_dataset_releases",
        ]
        for entity_name in workflow.entity_names:
            for i, task_id_start in enumerate(task_ids):
                if i < len(task_ids) - 1:
                    task_id_end = task_ids[i + 1]
                    structure[f"{entity_name}.{task_id_start}"] = [f"{entity_name}.{task_id_end}"]
            structure[f"{entity_name}.add_dataset_releases"] = ["cleanup"]

        structure["cleanup"] = ["dag_run_complete"]
        structure["dag_run_complete"] = []

        self.assert_dag_structure(
            structure,
            dag,
        )

    def test_dag_load(self):
        """Test that the OpenAlex DAG can be loaded from a DAG bag."""

        env = ObservatoryEnvironment(
            workflows=[
                Workflow(
                    dag_id=self.dag_id,
                    name="OpenAlex Telescope",
                    class_name="academic_observatory_workflows.workflows.openalex_telescope.OpenAlexTelescope",
                    cloud_workspace=self.fake_cloud_workspace,
                )
            ]
        )

        with env.create():
            self.assert_dag_load_from_config(self.dag_id)

    @patch("observatory.platform.workflows.vm_workflow.send_slack_msg")
    def test_telescope(self, m_send_slack_msg):
        """Test the OpenAlex telescope end to end."""

        env = ObservatoryEnvironment(self.project_id, self.data_location, api_port=find_free_port(), age_to_delete=0.05)
        bq_dataset_id = env.add_dataset()

        # Create the Observatory environment and run tests
        with env.create(task_logging=True):
            workflow = OpenAlexTelescope(
                dag_id=self.dag_id,
                cloud_workspace=env.cloud_workspace,
                bq_dataset_id=bq_dataset_id,
            )
            conn_aws = Connection(
                conn_id=workflow.aws_conn_id,
                conn_type="aws",
                login=self.aws_access_key_id,
                password=self.aws_secret_access_key,
            )
            env.add_connection(conn_aws)

            conn_slack = Connection(
                conn_id=AirflowConns.SLACK,
                uri="https://:my-slack-token@https%3A%2F%2Fhooks.slack.com%2Fservices",
            )
            env.add_connection(conn_slack)

            dag = workflow.make_dag()

            # Create bucket and dataset for use in first and second run
            with aws_bucket_test_env(prefix=self.dag_id, region_name=self.aws_region_name) as bucket_name:
                # Create data
                workflow.aws_openalex_bucket = bucket_name
                manifest_index = create_openalex_dataset(
                    pathlib.Path(test_fixtures_folder(self.dag_id, "2023-04-02")),
                    bucket_name,
                )
                # fmt: off
                merged_ids_index = {
                    "authors": [MergedId(f"s3://{bucket_name}/data/merged_ids/authors/2023-04-02.csv.gz", 99)],
                    "institutions": [MergedId(f"s3://{bucket_name}/data/merged_ids/institutions/2023-04-02.csv.gz", 97)],
                    "sources": [MergedId(f"s3://{bucket_name}/data/merged_ids/sources/2023-04-02.csv.gz", 100)],
                    "works": [MergedId(f"s3://{bucket_name}/data/merged_ids/works/2023-04-02.csv.gz", 100)],
                }
                # fmt: on

                # Create expected data
                data_interval_start = pendulum.datetime(2023, 4, 2)
                is_first_run = True
                expected_entities = []
                for entity_name in workflow.entity_names:
                    expected_entities.append(
                        OpenAlexEntity(
                            entity_name,
                            data_interval_start,
                            data_interval_start,
                            manifest_index[entity_name],
                            merged_ids_index.get(entity_name, []),
                            is_first_run,
                            pendulum.instance(datetime.datetime.min),
                        )
                    )

                # First run: snapshot
                with env.create_dag_run(dag, data_interval_start) as dag_run:
                    # Mocked and expected data
                    release = OpenAlexRelease(
                        dag_id=self.dag_id,
                        run_id=dag_run.run_id,
                        cloud_workspace=workflow.cloud_workspace,
                        bq_dataset_id=workflow.bq_dataset_id,
                        entities=expected_entities,
                        start_date=data_interval_start,
                        end_date=data_interval_start,
                        is_first_run=True,
                        schema_folder=workflow.schema_folder,
                    )
                    for entity in expected_entities:
                        entity.workflow = workflow
                        entity.release = release

                    # Wait for the previous DAG run to finish
                    ti = env.run_task("wait_for_prev_dag_run")
                    self.assertEqual(State.SUCCESS, ti.state)

                    # Check dependencies
                    ti = env.run_task(workflow.check_dependencies.__name__)
                    self.assertEqual(State.SUCCESS, ti.state)

                    # Fetch releases and check that we have received the expected snapshot date and changefiles
                    task_id = workflow.fetch_releases.__name__
                    ti = env.run_task(task_id)
                    self.assertEqual(State.SUCCESS, ti.state)
                    msg = ti.xcom_pull(
                        key=workflow.RELEASE_INFO,
                        task_ids=task_id,
                        include_prior_dates=False,
                    )
                    actual_entities = parse_release_msg(msg)
                    self.assertEqual(7, len(actual_entities))
                    self.assertEqual(expected_entities, actual_entities)

                    ti = env.run_task(workflow.create_datasets.__name__)
                    self.assertEqual(State.SUCCESS, ti.state)

                    # Transfer from AWS to GCP
                    ti = env.run_task(workflow.aws_to_gcs_transfer.__name__)
                    self.assertEqual(State.SUCCESS, ti.state)
                    base_folder = gcs_blob_name_from_path(release.download_folder)
                    for entity in expected_entities:
                        for entry in entity.current_entries:
                            actual_path = os.path.join(base_folder, entry.object_key)
                            self.assert_blob_exists(env.download_bucket, actual_path)
                        for merged_id in entity.current_merged_ids:
                            actual_path = os.path.join(base_folder, merged_id.object_key)
                            self.assert_blob_exists(env.download_bucket, actual_path)

                    # Branch operator
                    ti = env.run_task(f"branch")
                    self.assertEqual(State.SUCCESS, ti.state)

                    # Task groups
                    entity: OpenAlexEntity
                    for entity in expected_entities:
                        ti = env.run_task(f"{entity.entity_name}.bq_snapshot")
                        self.assertEqual(State.SKIPPED, ti.state)

                        ti = env.run_task(f"{entity.entity_name}.download")
                        self.assertEqual(State.SUCCESS, ti.state)
                        for entry in entity.current_entries:
                            file_path = os.path.join(release.download_folder, entry.object_key)
                            self.assertTrue(os.path.isfile(file_path))

                        ti = env.run_task(f"{entity.entity_name}.transform")
                        self.assertEqual(State.SUCCESS, ti.state)
                        for entry in entity.current_entries:
                            file_path = os.path.join(release.transform_folder, entry.object_key)
                            self.assertTrue(os.path.isfile(file_path))
                            self.assertTrue(os.path.isfile(entity.generated_schema_path))

                        ti = env.run_task(f"{entity.entity_name}.upload_schema")
                        self.assertEqual(State.SUCCESS, ti.state)
                        for entry in entity.current_entries:
                            self.assert_blob_exists(
                                env.transform_bucket, gcs_blob_name_from_path(entity.generated_schema_path)
                            )

                        ti = env.run_task(f"{entity.entity_name}.compare_schemas")
                        self.assertEqual(State.SUCCESS, ti.state)

                        ti = env.run_task(f"{entity.entity_name}.upload")
                        self.assertEqual(State.SUCCESS, ti.state)
                        for entry in entity.current_entries:
                            file_path = os.path.join(release.transform_folder, entry.object_key)
                            self.assert_blob_exists(env.transform_bucket, gcs_blob_name_from_path(file_path))

                        ti = env.run_task(f"{entity.entity_name}.bq_load_upserts")
                        self.assertEqual(State.SUCCESS, ti.state)
                        self.assert_table_integrity(entity.bq_upsert_table_id, 4)

                        ti = env.run_task(f"{entity.entity_name}.bq_upsert_records")
                        self.assertEqual(State.SUCCESS, ti.state)
                        self.assert_table_integrity(entity.bq_main_table_id, 4)

                        # These two don't do anything on the first run
                        ti = env.run_task(f"{entity.entity_name}.bq_load_deletes")
                        self.assertEqual(State.SKIPPED, ti.state)
                        with self.assertRaises(NotFound):
                            self.bigquery_client.get_table(entity.bq_delete_table_id)

                        ti = env.run_task(f"{entity.entity_name}.bq_delete_records")
                        self.assertEqual(State.SKIPPED, ti.state)
                        self.assert_table_integrity(entity.bq_main_table_id, 4)

                        # Assert content
                        table_id = bq_table_id(self.project_id, workflow.bq_dataset_id, entity.entity_name)
                        print(f"Assert content run 1 {entity.entity_name}: {table_id}")
                        expected_data = load_and_parse_json(
                            test_fixtures_folder(self.dag_id, "2023-04-02", "expected", f"{entity.entity_name}.json"),
                            date_fields={"created_date", "publication_date"},
                            timestamp_fields={"updated_date"},
                        )
                        self.assert_table_content(table_id, expected_data, "id")

                        # Check that there is zero dataset release per entity before add_dataset_releases and 1 after
                        dataset_releases = get_dataset_releases(dag_id=self.dag_id, dataset_id=entity.entity_name)
                        self.assertEqual(len(dataset_releases), 0)

                        ti = env.run_task(f"{entity.entity_name}.add_dataset_releases")
                        self.assertEqual(State.SUCCESS, ti.state)
                        dataset_releases = get_dataset_releases(dag_id=self.dag_id, dataset_id=entity.entity_name)
                        self.assertEqual(len(dataset_releases), 1)

                    # Test that all workflow data deleted
                    ti = env.run_task(workflow.cleanup.__name__)
                    self.assertEqual(State.SUCCESS, ti.state)
                    self.assert_cleanup(release.workflow_folder)

                    ti = env.run_task("dag_run_complete")
                    self.assertEqual(State.SUCCESS, ti.state)

                # Second run: no updates
                data_interval_start = pendulum.datetime(2023, 4, 9)
                with env.create_dag_run(dag, data_interval_start) as dag_run:
                    # Check that first three tasks are successful
                    task_ids = ["wait_for_prev_dag_run", "check_dependencies", "fetch_releases"]
                    for task_id in task_ids:
                        print(f"Running task: {task_id}")
                        ti = env.run_task(task_id)
                        self.assertEqual(State.SUCCESS, ti.state)

                    task_ids = ["create_datasets", "aws_to_gcs_transfer", "branch"]
                    for entity_name in workflow.entity_names:
                        for task_id in [
                            "bq_snapshot",
                            "download",
                            "transform",
                            "upload_schema",
                            "compare_schemas",
                            "upload",
                            "bq_load_upserts",
                            "bq_upsert_records",
                            "bq_load_deletes",
                            "bq_delete_records",
                            "add_dataset_releases",
                        ]:
                            task_ids.append(f"{entity_name}.{task_id}")
                    task_ids.append("cleanup")
                    task_ids.append("dag_run_complete")
                    for task_id in task_ids:
                        print(f"Checking that skipped: {task_id}")
                        ti = env.get_task_instance(task_id)
                        self.assertEqual(State.SKIPPED, ti.state)

                    # Check that tables all still have the same number of rows
                    for entity in expected_entities:
                        self.assert_table_integrity(entity.bq_main_table_id, 4)

                    # Check that only 1 dataset release exists for each entity
                    for entity_name in workflow.entity_names:
                        dataset_releases = get_dataset_releases(dag_id=self.dag_id, dataset_id=entity_name)
                        self.assertEqual(len(dataset_releases), 1)

            # Create bucket and dataset for use in third run
            with aws_bucket_test_env(prefix=self.dag_id, region_name=self.aws_region_name) as bucket_name:
                workflow.aws_openalex_bucket = bucket_name
                manifest_index = create_openalex_dataset(
                    pathlib.Path(test_fixtures_folder(self.dag_id, "2023-04-16")),
                    bucket_name,
                )
                merged_ids_index = {
                    "authors": [
                        MergedId(f"s3://{bucket_name}/data/merged_ids/authors/2023-04-02.csv.gz", 99),
                        MergedId(f"s3://{bucket_name}/data/merged_ids/authors/2023-04-16.csv.gz", 95),
                    ],
                    "institutions": [
                        MergedId(f"s3://{bucket_name}/data/merged_ids/institutions/2023-04-02.csv.gz", 97),
                        MergedId(f"s3://{bucket_name}/data/merged_ids/institutions/2023-04-16.csv.gz", 97),
                    ],
                    "sources": [
                        MergedId(f"s3://{bucket_name}/data/merged_ids/sources/2023-04-02.csv.gz", 100),
                        MergedId(f"s3://{bucket_name}/data/merged_ids/sources/2023-04-16.csv.gz", 95),
                    ],
                    "works": [
                        MergedId(f"s3://{bucket_name}/data/merged_ids/works/2023-04-02.csv.gz", 100),
                        MergedId(f"s3://{bucket_name}/data/merged_ids/works/2023-04-16.csv.gz", 98),
                    ],
                }
                # Create expected data
                prev_end_date = pendulum.datetime(2023, 4, 2)
                data_interval_start = pendulum.datetime(2023, 4, 16)
                is_first_run = False
                expected_entities = []
                for entity_name in workflow.entity_names:
                    expected_entities.append(
                        OpenAlexEntity(
                            entity_name,
                            data_interval_start,
                            data_interval_start,
                            manifest_index[entity_name],
                            merged_ids_index.get(entity_name, []),
                            is_first_run,
                            prev_end_date,
                        )
                    )

                # Third run: changefiles
                with env.create_dag_run(dag, data_interval_start) as dag_run:
                    # Mocked and expected data
                    release = OpenAlexRelease(
                        dag_id=self.dag_id,
                        run_id=dag_run.run_id,
                        cloud_workspace=workflow.cloud_workspace,
                        bq_dataset_id=workflow.bq_dataset_id,
                        entities=expected_entities,
                        start_date=data_interval_start,
                        end_date=data_interval_start,
                        is_first_run=True,
                        schema_folder=workflow.schema_folder,
                    )
                    for entity in expected_entities:
                        entity.workflow = workflow
                        entity.release = release

                    # Wait for the previous DAG run to finish
                    ti = env.run_task("wait_for_prev_dag_run")
                    self.assertEqual(State.SUCCESS, ti.state)

                    # Check dependencies
                    ti = env.run_task(workflow.check_dependencies.__name__)
                    self.assertEqual(State.SUCCESS, ti.state)

                    # Fetch releases and check that we have received the expected snapshot date and changefiles
                    task_id = workflow.fetch_releases.__name__
                    ti = env.run_task(task_id)
                    self.assertEqual(State.SUCCESS, ti.state)
                    msg = ti.xcom_pull(
                        key=workflow.RELEASE_INFO,
                        task_ids=task_id,
                        include_prior_dates=False,
                    )
                    actual_entities = parse_release_msg(msg)
                    self.assertEqual(7, len(actual_entities))
                    self.assertEqual(expected_entities, actual_entities)

                    ti = env.run_task(workflow.create_datasets.__name__)
                    self.assertEqual(State.SUCCESS, ti.state)

                    # Transfer from AWS to GCP
                    ti = env.run_task(workflow.aws_to_gcs_transfer.__name__)
                    self.assertEqual(State.SUCCESS, ti.state)
                    base_folder = gcs_blob_name_from_path(release.download_folder)
                    for entity in expected_entities:
                        for entry in entity.current_entries:
                            actual_path = os.path.join(base_folder, entry.object_key)
                            self.assert_blob_exists(env.download_bucket, actual_path)
                        for merged_id in entity.current_merged_ids:
                            actual_path = os.path.join(base_folder, merged_id.object_key)
                            self.assert_blob_exists(env.download_bucket, actual_path)

                    # Branch operator
                    ti = env.run_task(f"branch")
                    self.assertEqual(State.SUCCESS, ti.state)

                    # Task groups
                    for entity in expected_entities:
                        print(f"Executing tasks for task group: {entity.entity_name}")
                        ti = env.run_task(f"{entity.entity_name}.bq_snapshot")
                        self.assertEqual(State.SUCCESS, ti.state)
                        self.assertEqual(
                            bq_sharded_table_id(
                                self.project_id,
                                workflow.bq_dataset_id,
                                f"{entity.entity_name}_snapshot",
                                prev_end_date,
                            ),
                            entity.bq_snapshot_table_id,
                        )
                        self.assert_table_integrity(entity.bq_snapshot_table_id, 4)

                        ti = env.run_task(f"{entity.entity_name}.download")
                        self.assertEqual(State.SUCCESS, ti.state)
                        for entry in entity.current_entries:
                            file_path = os.path.join(release.download_folder, entry.object_key)
                            self.assertTrue(os.path.isfile(file_path))

                        ti = env.run_task(f"{entity.entity_name}.transform")
                        self.assertEqual(State.SUCCESS, ti.state)
                        for entry in entity.current_entries:
                            file_path = os.path.join(release.transform_folder, entry.object_key)
                            self.assertTrue(os.path.isfile(file_path))
                            self.assertTrue(os.path.isfile(entity.generated_schema_path))

                        ti = env.run_task(f"{entity.entity_name}.upload_schema")
                        self.assertEqual(State.SUCCESS, ti.state)
                        for entry in entity.current_entries:
                            self.assert_blob_exists(
                                env.transform_bucket, gcs_blob_name_from_path(entity.generated_schema_path)
                            )

                        ti = env.run_task(f"{entity.entity_name}.compare_schemas")
                        self.assertEqual(State.SUCCESS, ti.state)

                        ti = env.run_task(f"{entity.entity_name}.upload")
                        self.assertEqual(State.SUCCESS, ti.state)
                        for entry in entity.current_entries:
                            file_path = os.path.join(release.transform_folder, entry.object_key)
                            self.assert_blob_exists(env.transform_bucket, gcs_blob_name_from_path(file_path))

                        ti = env.run_task(f"{entity.entity_name}.bq_load_upserts")
                        self.assertEqual(State.SUCCESS, ti.state)
                        expected_row_index = {
                            "authors": 3,
                            "concepts": 2,
                            "institutions": 3,
                            "publishers": 2,
                            "sources": 3,
                            "works": 3,
                            "funders": 2,
                        }
                        expected_rows = expected_row_index[entity.entity_name]
                        self.assert_table_integrity(entity.bq_upsert_table_id, expected_rows)

                        ti = env.run_task(f"{entity.entity_name}.bq_upsert_records")
                        self.assertEqual(State.SUCCESS, ti.state)
                        self.assert_table_integrity(entity.bq_main_table_id, 5)

                        ti = env.run_task(f"{entity.entity_name}.bq_load_deletes")
                        if entity.has_merged_ids:
                            self.assertEqual(State.SUCCESS, ti.state)
                            self.assert_table_integrity(entity.bq_delete_table_id, 1)
                        else:
                            self.assertEqual(State.SKIPPED, ti.state)
                            with self.assertRaises(NotFound):
                                self.bigquery_client.get_table(entity.bq_delete_table_id)

                        ti = env.run_task(f"{entity.entity_name}.bq_delete_records")
                        if entity.has_merged_ids:
                            self.assertEqual(State.SUCCESS, ti.state)
                        else:
                            self.assertEqual(State.SKIPPED, ti.state)
                        expected_row_index = {
                            "authors": 4,
                            "concepts": 5,
                            "institutions": 4,
                            "publishers": 5,
                            "sources": 4,
                            "works": 4,
                            "funders": 5,
                        }
                        expected_rows = expected_row_index[entity.entity_name]
                        self.assert_table_integrity(entity.bq_main_table_id, expected_rows)

                        # Assert content
                        table_id = bq_table_id(self.project_id, workflow.bq_dataset_id, entity.entity_name)
                        print(f"Assert content run 2 {entity.entity_name}: {table_id}")
                        expected_data = load_and_parse_json(
                            test_fixtures_folder(self.dag_id, "2023-04-16", "expected", f"{entity.entity_name}.json"),
                            date_fields={"created_date", "publication_date"},
                            timestamp_fields={"updated_date"},
                        )
                        self.assert_table_content(table_id, expected_data, "id")

                        # Check that there is zero dataset release per entity before add_dataset_releases and 1 after
                        dataset_releases = get_dataset_releases(dag_id=self.dag_id, dataset_id=entity.entity_name)
                        self.assertEqual(len(dataset_releases), 1)

                        ti = env.run_task(f"{entity.entity_name}.add_dataset_releases")
                        self.assertEqual(State.SUCCESS, ti.state)
                        dataset_releases = get_dataset_releases(dag_id=self.dag_id, dataset_id=entity.entity_name)
                        self.assertEqual(len(dataset_releases), 2)

                    # Test that all workflow data deleted
                    ti = env.run_task(workflow.cleanup.__name__)
                    self.assertEqual(State.SUCCESS, ti.state)
                    self.assert_cleanup(release.workflow_folder)

                    ti = env.run_task("dag_run_complete")
                    self.assertEqual(State.SUCCESS, ti.state)
