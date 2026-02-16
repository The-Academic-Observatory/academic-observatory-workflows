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


import copy
import json
import os
import tempfile
from collections import OrderedDict
import unittest
from unittest.mock import patch

import boto3
import pendulum
from bigquery_schema_generator.generate_schema import SchemaGenerator
from google.cloud import bigquery

from academic_observatory_workflows.openalex_telescope.release import ManifestEntry, Meta, s3_uri_parts
from academic_observatory_workflows.openalex_telescope.tasks import (
    bq_compare_schemas,
    fetch_manifest,
    fetch_merged_ids,
    flatten_schema,
    Manifest,
    merge_schema_maps,
    MergedId,
    OpenAlexEntity,
    transform_object,
    convert_field_to_int,
)
from observatory_platform.airflow.workflow import CloudWorkspace
from observatory_platform.files import load_jsonl
from academic_observatory_workflows.config import project_path
from observatory_platform.sandbox.test_utils import aws_bucket_test_env, load_and_parse_json, SandboxTestCase

FIXTURES_FOLDER = project_path("openalex_telescope", "tests", "fixtures")


class TestOpenAlexUtils(SandboxTestCase):
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

    @patch("observatory_platform.airflow.workflow.Variable.get")
    def test_openalex_entity(self, m_variable_get):
        with tempfile.TemporaryDirectory() as t:
            m_variable_get.return_value = t
            dag_id = "openalex"
            cloud_workspace = CloudWorkspace(
                project_id="project-id",
                download_bucket="download_bucket",
                transform_bucket="transform_bucket",
                data_location="us",
            )
            schema_folder = project_path("openalex_telescope", "schema")
            bq_dataset_id = "openalex"
            is_first_run = True
            snapshot_date = pendulum.datetime(2023, 1, 28)
            entity = OpenAlexEntity(
                dag_id=dag_id,
                run_id="scheduled__2023-02-01T00:00:00+00:00",
                cloud_workspace=cloud_workspace,
                entity_name="authors",
                bq_dataset_id=bq_dataset_id,
                schema_folder=schema_folder,
                snapshot_date=snapshot_date,
                manifest=Manifest(
                    [ManifestEntry("s3://openalex/data/authors/updated_date=2023-01-28/part_000.gz", Meta(7073, 4))],
                    Meta(7073, 4),
                ),
                merged_ids=[MergedId("s3://openalex/data/merged_ids/authors/2023-01-28.csv.gz", 1000)],
                is_first_run=is_first_run,
            )

            # table_description
            self.assertEqual(
                "OpenAlex authors table: https://docs.openalex.org/api-entities/authors", entity.table_description
            )

            # schema_file_path
            self.assertTrue(entity.schema_file_path.endswith("authors.json"))

            # data_uri
            self.assertTrue(entity.data_uri.endswith("data/authors/*"))

            # bq_table_id
            self.assertEqual("project-id.openalex.authors20230128", entity.bq_table_id)

            # entries
            self.assertEqual(
                [ManifestEntry("s3://openalex/data/authors/updated_date=2023-01-28/part_000.gz", Meta(7073, 4))],
                entity.entries,
            )

    def test_fetch_manifest(self):
        manifest_path = os.path.join(FIXTURES_FOLDER, "manifest")
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

        # Null in array
        obj = {"authorships": [{"affiliations": [{"institution_ids": [None]}]}]}
        transform_object(obj)
        self.assertDictEqual(
            {"authorships": [{"affiliations": [{"institution_ids": []}]}]},
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

        # Issues with nulls in locations
        obj = {"host_organization_lineage_names": [None]}
        transform_object(obj)
        self.assertDictEqual(
            {"host_organization_lineage_names": []},
            obj,
        )

        obj = {"primary_location": {"source": {"host_organization_lineage_names": [None]}}}
        transform_object(obj)
        self.assertDictEqual(
            {"primary_location": {"source": {"host_organization_lineage_names": []}}},
            obj,
        )

        obj = {"best_oa_location": {"source": {"host_organization_lineage_names": [None]}}}
        transform_object(obj)
        self.assertDictEqual(
            {"best_oa_location": {"source": {"host_organization_lineage_names": []}}},
            obj,
        )

        obj = {"locations": [{"source": {"host_organization_lineage_names": [None]}}]}
        transform_object(obj)
        self.assertDictEqual(
            {"locations": [{"source": {"host_organization_lineage_names": []}}]},
            obj,
        )

        obj = {"primary_location": None}
        transform_object(obj)
        self.assertDictEqual(
            {"primary_location": None},
            obj,
        )

        obj = {"primary_location": {"source": None}}
        transform_object(obj)
        self.assertDictEqual(
            {"primary_location": {"source": None}},
            obj,
        )

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

    def test_merge_schema_maps(self):
        test1 = load_jsonl(os.path.join(FIXTURES_FOLDER, "schema_generator", "part_000.jsonl"))
        test2 = load_jsonl(os.path.join(FIXTURES_FOLDER, "schema_generator", "part_001.jsonl"))

        expected_schema_path = os.path.join(FIXTURES_FOLDER, "schema_generator", "expected.json")
        expected = load_and_parse_json(file_path=expected_schema_path)

        # Create schema maps using both the test files
        schema_map1 = OrderedDict()
        schema_map2 = OrderedDict()
        schema_generator = SchemaGenerator(input_format="dict")

        # Both schema_maps need to be independent of each other here.
        for record1 in test1:
            schema_generator.deduce_schema_for_record(record1, schema_map1)

        for record2 in test2:
            schema_generator.deduce_schema_for_record(record2, schema_map2)

        # Merge the two schemas together - this is similar to how it will merge when each process from a ProcessPool
        # gives a new schema map from each data file that's been transformed.
        merged_schema_map = OrderedDict()
        for incoming in [schema_map1, schema_map2]:
            merged_schema_map = merge_schema_maps(to_add=incoming, old=merged_schema_map)
        merged_schema = flatten_schema(merged_schema_map)

        self.assertTrue(bq_compare_schemas(actual=merged_schema, expected=expected))


class TestConvertFieldToInt(unittest.TestCase):

    def test_successful_conversion_and_none_handling(self):
        """
        Tests:
        1. Successful conversion (from string and float).
        2. Handling of a None value (removal of the key).
        """
        data = {"str_id": "500", "float_val": 10.99, "none_val": None, "int_val": 42}

        result_str = convert_field_to_int(data, "str_id")
        self.assertTrue(result_str)
        self.assertEqual(data["str_id"], 500)
        self.assertIsInstance(data["str_id"], int)

        result_float = convert_field_to_int(data, "float_val")
        self.assertTrue(result_float)
        self.assertEqual(data["float_val"], 10)
        self.assertIsInstance(data["float_val"], int)

        result_none = convert_field_to_int(data, "none_val")
        self.assertTrue(result_none)
        self.assertNotIn("none_val", data)

        result_int = convert_field_to_int(data, "int_val")
        self.assertTrue(result_int)
        self.assertEqual(data["int_val"], 42)

    def test_conversion_failure_and_unchanged_data(self):
        """
        1. Failure when value cannot be converted (e.g., non-numeric string).
        2. Failure when value type is incompatible (e.g., list).
        3. Field remains unchanged on failure.
        """
        data = {"alpha_str": "abcd", "a_list": [1, 2], "float_str": "12.34"}  # int() raises ValueError on float strings

        original_alpha = data["alpha_str"]
        result_alpha = convert_field_to_int(data, "alpha_str")
        self.assertFalse(result_alpha)  # Expect False return on conversion failure
        self.assertEqual(data["alpha_str"], original_alpha)  # Data must be unchanged

        original_list = data["a_list"]
        result_list = convert_field_to_int(data, "a_list")
        self.assertFalse(result_list)  # Expect False return on conversion failure
        self.assertEqual(data["a_list"], original_list)  # Data must be unchanged

        original_float_str = data["float_str"]
        result_float_str = convert_field_to_int(data, "float_str")
        self.assertFalse(result_float_str)
        self.assertEqual(data["float_str"], original_float_str)

    def test_field_not_found_edge_cases(self):
        """
        1. Field key does not exist.
        2. Input dictionary is None.
        """
        data = {"key1": 1}

        result_missing = convert_field_to_int(data, "missing_key")
        self.assertFalse(result_missing)
        self.assertEqual(data, {"key1": 1})

        data_none = None
        result_none_obj = convert_field_to_int(data_none, "any_key")
        self.assertFalse(result_none_obj)


def get_table_expiry(table_id: str):
    client = bigquery.Client()
    table = client.get_table(table_id)
    return table.expires
