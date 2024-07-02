from __future__ import annotations

import argparse
import json
import logging
import os
from collections import OrderedDict
from concurrent.futures import as_completed, ProcessPoolExecutor
from pathlib import Path

from academic_observatory_workflows.openalex_telescope.openalex_telescope import (
    flatten_schema,
    merge_schema_maps,
    transform_file,
)
from observatory_platform.files import list_files


def sort_schema(input_file: Path):
    def sort_schema_func(schema):
        # Sort schema entries by name and sort the fields of each entry by key_order
        key_order = ["name", "type", "mode", "description", "fields"]
        sorted_schema = [
            {k: field[k] for k in key_order if k in field} for field in sorted(schema, key=lambda x: x["name"])
        ]

        # Sort the fields recursively
        for field in sorted_schema:
            if field.get("type") == "RECORD" and "fields" in field:
                field["fields"] = sort_schema_func(field["fields"])

        return sorted_schema

    # Load the JSON schema from a string
    with open(input_file, mode="r") as f:
        data = json.load(f)

    # Sort the schema
    sorted_json_schema = sort_schema_func(data)

    # Save the schema
    with open(input_file, mode="w") as f:
        json.dump(sorted_json_schema, f, indent=2)


def generate_schema(entity_name: str, input_folder: Path, output_folder: Path, max_workers: int):
    merged_schema_map = OrderedDict()
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = []

        for input_path in list_files(str(input_folder), r"^part_\d{3}\.gz$"):
            output_path = str(output_folder / Path(input_path).relative_to(input_folder))
            futures.append(executor.submit(transform_file, input_path, output_path))

        for future in as_completed(futures):
            input_path, schema_map, schema_error = future.result()
            if schema_error:
                logging.info(f"Error generating schema for file {input_path}: {schema_error}")

            # Merge the schemas from each process. Each data file could have more fields than others.
            merged_schema_map = merge_schema_maps(to_add=schema_map, old=merged_schema_map)

    # Flatten schema from nested OrderedDicts to a regular Bigquery schema.
    merged_schema = flatten_schema(schema_map=merged_schema_map)

    # Save schema to file
    generated_schema_path = os.path.join(output_folder, f"{entity_name}.json")
    with open(generated_schema_path, mode="w") as f_out:
        json.dump(merged_schema, f_out, indent=2)

    sort_schema(Path(generated_schema_path))


def check_directory(path):
    """Check if the provided path is a valid directory."""
    if not Path(path).is_dir():
        raise argparse.ArgumentTypeError(f"The directory {path} does not exist.")
    return Path(path)


if __name__ == "__main__":
    """Simple command line tool to generate a BigQuery schema for an OpenAlex entity"""

    parser = argparse.ArgumentParser(description="Process OpenAlex entities.")

    # Required arguments
    parser.add_argument("entity_name", type=str, help="The name of the entity")
    parser.add_argument("input_folder", type=check_directory, help="The input folder path")
    parser.add_argument("output_folder", type=check_directory, help="The output folder path")

    # Optional argument with default value from os.cpu_count()
    parser.add_argument(
        "--max_workers",
        type=int,
        default=os.cpu_count(),
        help="The maximum number of workers (default: number of CPUs)",
    )

    # Parse the arguments
    args = parser.parse_args()

    generate_schema(args.entity_name, args.input_folder, args.output_folder, args.max_workers)
