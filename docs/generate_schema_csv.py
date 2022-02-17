# Copyright 2020 Curtin University
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

# Author: Tuan Chien, Aniek Roelofs


import csv
import json
import os
import re
import shutil
from glob import glob
from pathlib import Path
from typing import List


def flatten_schema(schema: List[dict], *, prefix: str = "") -> List[tuple]:
    """
    Flatten a schema with nested fields, each (nested) field will be a tuple with field information.

    :param schema: List with fields, each field is a dictionary.
    :param prefix: Field name prefix for a nested field.
    :return The flattened schema.
    """
    flat_schema = []
    for field in schema:
        # Get field info
        field_name = prefix + field["name"]
        field_type = field.get("type", "STRING")
        field_mode = field.get("mode", "NULLABLE")
        field_desc = field.get("description")

        # Add field info as tuple
        flat_schema.append((field_name, field_type, field_mode, field_desc))

        if field_type == "RECORD":
            # Get nested fields
            fields = field["fields"]

            # Store current prefix and update
            prev_prefix = prefix
            prefix = field_name + "."

            # Flatten schema recursively
            flat_schema += flatten_schema(schema=fields, prefix=prefix)

            # Reset prefix
            prefix = prev_prefix

    return flat_schema


def generate_csv(*, schema_dir):
    """Convert all observatory schema files in JSON format to CSV for inclusion in Sphinx.

    :param schema_dir: Path to schema directory.
    """
    schema_files = glob(os.path.join(schema_dir, "**", "*.json"), recursive=True)
    dst_dir = "schemas"

    if os.path.exists(dst_dir):
        shutil.rmtree(dst_dir)

    Path(dst_dir).mkdir(exist_ok=True, parents=True)

    for schema_file in schema_files:
        # Read schema file
        with open(schema_file, "r") as f_in:
            schema = json.load(f_in)

        # Flatten
        flat_schema = flatten_schema(schema=schema)

        # Write flat schema file as csv
        filename = os.path.relpath(schema_file, schema_dir)
        out_path = os.path.join(dst_dir, filename.replace(".json", ".csv"))
        Path(os.path.dirname(out_path)).mkdir(exist_ok=True, parents=True)
        with open(out_path, "w") as f_out:
            writer = csv.writer(f_out)
            writer.writerow(["name", "type", "mode", "description"])
            writer.writerows(flat_schema)


def generate_latest_files():
    """For each schema, generate a schema_latest.csv to indicate it is the latest dataset schema. Then if people just
    want to refer to the latest schema in the documentation they can refer to this link rather than having to update
    the documentation when there is a schema change.

    Does not handle versioned schemas  (wos/scopus). But maybe we should get rid of versioned schemas when those
    telescopes are ported to the new template framework anyway.
    """

    table_files = glob("schemas/**/*.csv", recursive=True)
    r = re.compile(r"\d{4}-\d{2}-\d{2}")

    # Build a database of schema files
    table_schemas = {}
    for file in table_files:
        filename = os.path.basename(file)
        date_str = r.search(filename)
        date_str_start = date_str.span()[0]
        table_name = filename[: date_str_start - 1]  # -1 accounts for _
        table_schemas[table_name] = table_schemas.get(table_name, list())
        table_schemas[table_name].append(file)

    # Sort schemas
    for table in table_schemas:
        table_schemas[table].sort()

    # Copy the last schema in list since it's latest, to a table_latest.csv
    for table, schema_paths in table_schemas.items():
        src_path = schema_paths[-1]

        dst_dir = os.path.dirname(src_path)
        dst_filename = f"{table}_latest.csv"
        dst_path = os.path.join(dst_dir, dst_filename)

        shutil.copyfile(src_path, dst_path)


if __name__ == "__main__":
    generate_csv(schema_dir="../academic_observatory_workflows/database/schema")
    generate_latest_files()
