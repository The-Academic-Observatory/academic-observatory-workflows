from __future__ import annotations

import argparse
import glob
import json
import logging
import os
from collections import OrderedDict
from concurrent.futures import as_completed, ProcessPoolExecutor
from json import JSONEncoder
from pathlib import Path
from typing import Any, List, Tuple

import json_lines
import jsonlines
from bigquery_schema_generator.generate_schema import flatten_schema_map, SchemaGenerator


def yield_jsonl(file_path: str):
    """Return or yield row of a JSON lines file as a dictionary. If the file
    is gz compressed then it will be extracted.

    :param file_path: the path to the JSON lines file.
    :return: generator.
    """

    with json_lines.open(file_path) as file:
        for row in file:
            yield row


def merge_schema_maps(to_add: OrderedDict, old: OrderedDict) -> OrderedDict:
    """Using the SchemaGenerator from the bigquery_schema_generator library, merge the schemas found
    when from scanning through files into one large nested OrderedDict.

    :param to_add: The incoming schema to add to the existing "old" schema.
    :param old: The existing old schema with previously populated values.
    :return: The old schema with newly added fields.
    """

    schema_generator = SchemaGenerator()

    if old:
        # Loop through the fields to add to the schema
        for key, value in to_add.items():
            if key in old:
                # Merge existing fields together.
                old[key] = schema_generator.merge_schema_entry(old_schema_entry=old[key], new_schema_entry=value)
            else:
                # New top level field is added.
                old[key] = value
    else:
        # Initialise it with first result if it is empty
        old = to_add.copy()

    return old


def flatten_schema(schema_map: OrderedDict) -> dict:
    """A quick trick using the JSON encoder and load string function to convert from a nested
    OrderedDict object to a regular dictionary.

    :param schema_map: The generated schema from SchemaGenerator.
    :return schema: A Bigquery style schema."""

    encoded_schema = JSONEncoder().encode(
        flatten_schema_map(
            schema_map,
            keep_nulls=False,
            sorted_schema=True,
            infer_mode=True,
            input_format="json",
        )
    )

    return json.loads(encoded_schema)


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


def list_jsonl_files(folder_path):
    # Use glob to recursively find all .jsonl files
    jsonl_files = glob.glob(os.path.join(folder_path, "**", "*.jsonl"), recursive=True)
    return jsonl_files


def remove_empty_dicts(arr):
    return [item for item in arr if not (isinstance(item, dict) and not item)]


def remove_nulls_from_list_field(obj: dict, field: str):
    if field in obj:
        value = obj.get(field) or []
        obj[field] = [x for x in value if x is not None]


def clean_string_or_bool(value):
    if value is None or (isinstance(value, str) and value.strip() == "") or isinstance(value, bool):
        return None
    return value


def format_geography_point(point):
    """Formats a geography point string if latitude and longitude are present."""
    lat, lng = point.get("pointLatitude"), point.get("pointLongitude")
    return f"POINT({lng} {lat})" if lat is not None and lng is not None else None


def normalize_to_string_or_none(value):
    """Normalizes a value to a string or returns None if the value is None or an empty string."""
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    return str(value)


def filter_non_empty_dicts(arr):
    """Filters out empty dictionaries from a list."""
    return [item for item in arr if not (isinstance(item, dict) and not item)]


def transform_geo_locations(geoLocations):
    """Transforms and cleans geo-location data."""
    for loc in geoLocations:
        if "geoLocationPoint" in loc:
            loc["geoLocationPoint"] = format_geography_point(loc["geoLocationPoint"])

        # string and not float
        if "geoLocationBox" in loc:
            box = loc["geoLocationBox"]
            for key in ["northBoundLatitude", "southBoundLatitude", "eastBoundLongitude", "westBoundLongitude"]:
                box[key] = normalize_to_string_or_none(box.get(key))

        if "geoLocationPolygon" in loc:
            del loc["geoLocationPolygon"]
            # points = loc["geoLocationPolygon"]
            # polygon = None
            # if isinstance(points, list) and len(points) > 0:
            #     if isinstance(points[0], list):
            #         points = points[0]
            #     data = []
            #     for point in points:
            #         polygon_point = point.get("polygonPoint", {})
            #         lng = polygon_point.get("pointLongitude")
            #         lat = polygon_point.get("pointLatitude")
            #         if lng is not None and lat is not None:
            #             data.append(f"{lng} {lat}")
            #     if data:
            #         polygon = f"POLYGON(({', '.join(data)}))"
            # if polygon is not None:
            #     loc["geoLocationPolygon"] = polygon
            # else:
            #     del loc["geoLocationPolygon"]
            #     print(f"Error geoLocationPolygon: {points}")

    return filter_non_empty_dicts(geoLocations)


def normalize_affiliations_and_identifiers(obj, field):
    """Normalizes affiliation and name identifier fields in a list."""
    items = obj.get(field, [])
    for item in items:
        for key in ["affiliation", "nameIdentifiers"]:
            if isinstance(item.get(key), dict):
                item[key] = filter_non_empty_dicts([item[key]])
            else:
                item[key] = filter_non_empty_dicts(item.get(key, []))
    obj[field] = items


def normalize_identifier_fields(obj: dict, field: str, subfield: str):
    """Normalizes identifier fields to strings within a specified field."""
    for item in obj.get(field, []):
        if subfield in item:
            item[subfield] = normalize_to_string_or_none(item[subfield])


def normalize_related_item(value):
    """Normalizes related items by converting to string or returning None."""
    if value is None or isinstance(value, list) or (isinstance(value, str) and value.strip() == ""):
        return None
    return str(value)


def transform_object(obj):
    # If null convert to empty array
    obj["contentUrl"] = obj.get("contentUrl") or []

    # Remove empty dicts and cleanup geography related data
    obj["geoLocations"] = transform_geo_locations(obj.get("geoLocations", []))

    # Remove empty dicts, convert single objects to arrays
    normalize_affiliations_and_identifiers(obj, "creators")
    normalize_affiliations_and_identifiers(obj, "contributors")

    # Remove empty dicts
    obj["rightsList"] = remove_empty_dicts(obj.get("rightsList", []))
    obj["subjects"] = remove_empty_dicts(obj.get("subjects", []))

    # Convert to strings
    container = obj.get("container", {})
    for key in ["volume", "issue", "firstPage", "lastPage"]:
        container[key] = normalize_to_string_or_none(container.get(key))
    obj["container"] = container

    # Remove nulls from array
    remove_nulls_from_list_field(obj, "formats")
    remove_nulls_from_list_field(obj, "sizes")

    # Some sizes are integers
    if "sizes" in obj:
        obj["sizes"] = [str(size) for size in obj["sizes"]]

    # Convert identifiers to strings
    # "identifiers":[{"identifier":9486968,"identifierType":"ISBN"},{"identifier":"0948695X","identifierType":"ISSN"}],
    normalize_identifier_fields(obj, "identifiers", "identifier")
    normalize_identifier_fields(obj, "alternateIdentifiers", "alternateIdentifier")
    normalize_identifier_fields(obj, "relatedIdentifiers", "relatedIdentifier")
    normalize_identifier_fields(obj, "dates", "date")

    # Clean relatedItems
    for item in obj.get("relatedItems", []):
        for key in ["firstPage", "lastPage"]:
            item[key] = normalize_related_item(item.get(key))

    # Descriptions
    for desc in obj.get("descriptions", []):
        desc["description"] = normalize_to_string_or_none(desc.get("description"))

    # Cleanup boolean string award URIs
    for item in obj.get("fundingReferences", []):
        item["awardUri"] = clean_string_or_bool(item.get("awardUri"))


def transform(input_path: str, output_path: str) -> Tuple[str, OrderedDict, list]:
    # Initialise the schema generator.
    schema_map = OrderedDict()
    schema_generator = SchemaGenerator(input_format="dict")

    # Make base folder
    base_folder = os.path.dirname(output_path)
    os.makedirs(base_folder, exist_ok=True)

    logging.info(f"generate_schema {input_path}")
    with open(output_path, mode="w") as f:
        with jsonlines.Writer(f) as writer:
            for obj in yield_jsonl(input_path):
                transform_object(obj)
                writer.write(obj)

                # Wrap this in a try and pass so that it doesn't
                # cause the transform step to fail unexpectedly.
                try:
                    schema_generator.deduce_schema_for_record(obj, schema_map)
                except Exception:
                    pass

    return input_path, schema_map, schema_generator.error_logs


def get_chunks(*, input_list: List[Any], chunk_size: int = 8) -> List[Any]:
    """Generator that splits a list into chunks of a fixed size.

    :param input_list: Input list.
    :param chunk_size: Size of chunks.
    :return: The next chunk from the input list.
    """

    n = len(input_list)
    for i in range(0, n, chunk_size):
        yield input_list[i : i + chunk_size]


def generate_schema_for_dataset(input_folder: Path, output_folder: Path, max_workers: int):
    merged_schema_map = OrderedDict()
    i = 1
    files = list_jsonl_files(str(input_folder))
    total_files = len(files)
    for c, chunk in enumerate(get_chunks(input_list=files, chunk_size=500)):
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = []

            for input_path in chunk:
                output_path = str(output_folder / Path(input_path).relative_to(input_folder))
                futures.append(executor.submit(transform, input_path, output_path))

            for future in as_completed(futures):
                input_path, schema_map, schema_error = future.result()
                if schema_error:
                    msg = f"File {input_path}: {schema_error}"
                    with open(input_folder / "errors.txt", mode="a") as f:
                        f.write(f"{msg}\n")
                    print(msg)

                # Merge the schemas from each process. Each data file could have more fields than others.
                try:
                    merged_schema_map = merge_schema_maps(to_add=schema_map, old=merged_schema_map)
                except Exception as e:
                    print(f"merge_schema_maps error: {e}")

                percent = i / total_files * 100
                print(f"Progress: {i} / {total_files}, {percent:.2f}%")
                i += 1

    # Flatten schema from nested OrderedDicts to a regular Bigquery schema.
    merged_schema = flatten_schema(schema_map=merged_schema_map)

    # Save schema to file
    generated_schema_path = os.path.join(input_folder, f"schema.json")
    with open(generated_schema_path, mode="w") as f_out:
        json.dump(merged_schema, f_out, indent=2)

    sort_schema(Path(generated_schema_path))


def check_directory(path):
    """Check if the provided path is a valid directory."""
    if not Path(path).is_dir():
        raise argparse.ArgumentTypeError(f"The directory {path} does not exist.")
    return Path(path)


if __name__ == "__main__":
    """Command line tool to transform DataCite and generate a first pass at a schema"""

    parser = argparse.ArgumentParser(description="Process OpenAlex entities.")

    # Required arguments
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

    generate_schema_for_dataset(args.input_folder, args.output_folder, args.max_workers)
