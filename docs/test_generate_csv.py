# Copyright 2021 Curtin University. All Rights Reserved.
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
import tempfile
import unittest

import docs.generate_schema_csv as gsc


# Author: Tuan Chien


class TestSchemaCSVGenerator(unittest.TestCase):

    test_schema = [
        {"name": "test", "type": "test_type", "mode": "test_mode", "description": "test_description"},
        {
            "name": "test2",
            "type": "RECORD",
            "mode": "test_mode",
            "description": "test_description",
            "fields": [{"name": "test3", "type": "test_type", "mode": "test_mode"}],
        },
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def test_schema_to_csv_prefix(self):
        rows = list()
        gsc.schema_to_csv(schema=TestSchemaCSVGenerator.test_schema, output=rows)
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0]["name"], "test")
        self.assertEqual(rows[0]["type"], "test_type")
        self.assertEqual(rows[0]["mode"], "test_mode")
        self.assertEqual(rows[0]["description"], "test_description")

        self.assertEqual(rows[1]["name"], "test2")
        self.assertEqual(rows[1]["type"], "RECORD")
        self.assertEqual(rows[1]["mode"], "test_mode")
        self.assertEqual(rows[1]["description"], "test_description")

        self.assertEqual(rows[2]["name"], "test2.test3")
        self.assertEqual(rows[2]["type"], "test_type")
        self.assertEqual(rows[2]["mode"], "test_mode")
        self.assertEqual(rows[2]["description"], None)

    def test_generate_csv(self):
        data = [{"name": "TestField", "type": "INTEGER", "description": "Some description"}]

        with tempfile.TemporaryDirectory() as t:
            with open(os.path.join(t, "test_schema_2021-01-01.json"), mode="w") as f:
                json.dump(data, f)

            gsc.generate_csv(schema_dir=t)
            self.assertTrue(os.path.exists(os.path.join(t, "schemas", "test_schema_2021-01-01.csv")))

    def test_generate_latest_files(self):
        data = [{"name": "TestField", "type": "INTEGER", "description": "Some description"}]
        with tempfile.TemporaryDirectory() as t:
            with open(os.path.join(t, "test_schema_2021-01-01.json"), mode="w") as f:
                json.dump(data, f)

            gsc.generate_csv(schema_dir=t)
            gsc.generate_latest_files()
            self.assertTrue(os.path.exists(os.path.join(t, "schemas", "test_schema_latest.csv")))
