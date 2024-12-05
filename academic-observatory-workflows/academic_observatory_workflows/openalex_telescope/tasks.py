# Copyright 2022-2024 Curtin University
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

from __future__ import annotations

import datetime
import gzip
import json
import logging
import os
from collections import OrderedDict
from concurrent.futures import as_completed, ProcessPoolExecutor
from json.encoder import JSONEncoder
from typing import Any, List, Optional, Tuple

import boto3
import jsonlines
import pendulum
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models.taskinstance import TaskInstance
from airflow.operators.bash import BashOperator
from bigquery_schema_generator.generate_schema import flatten_schema_map, SchemaGenerator
from deepdiff import DeepDiff
from google.cloud import bigquery
from google.cloud.bigquery import SourceFormat

import observatory_platform.google.bigquery as bq
from academic_observatory_workflows.openalex_telescope.release import Manifest, MergedId, OpenAlexEntity
from observatory_platform.airflow.airflow import send_slack_msg
from observatory_platform.airflow.workflow import CloudWorkspace
from observatory_platform.dataset_api import DatasetAPI, DatasetRelease
from observatory_platform.files import clean_dir
from observatory_platform.google.gcs import (
    gcs_blob_name_from_path,
    gcs_create_aws_transfer,
    gcs_download_blob,
    gcs_upload_files,
    gcs_upload_transfer_manifest,
)
from observatory_platform.sandbox.test_utils import log_diff

DATASET_API_ENTITY_ID = "openalex"


def fetch_entities(
    *,
    dag_id: str,
    run_id: str,
    is_first_run: bool,
    entity_names: list[str],
    cloud_workspace: CloudWorkspace,
    schema_folder: str,
    bq_dataset_id: str,
    api_bq_dataset_id: str,
    aws_conn_id: str,
    aws_openalex_bucket: str,
    entity_id=DATASET_API_ENTITY_ID,
) -> dict[str, dict]:
    # Get previous release and on first run check that previous releases removed
    api = DatasetAPI(bq_project_id=cloud_workspace.output_project_id, bq_dataset_id=api_bq_dataset_id)
    prev_release = api.get_latest_dataset_release(dag_id=dag_id, entity_id=entity_id, date_key="snapshot_date")
    if is_first_run and prev_release is not None:
        raise AirflowException(
            f"fetch_releases: there should be no DatasetReleases for dag_id={dag_id}, dataset_id={entity_id} stored in the Observatory API on the first DAG run."
        )

    # Fetch manifests, calculate snapshot date and store manifests
    # Snapshot is the date of the latest file across all entities
    entity_index = {}
    aws_key = get_aws_key(aws_conn_id)
    snapshot_date = pendulum.instance(datetime.datetime.min)
    for entity_name in entity_names:
        manifest = fetch_manifest(bucket=aws_openalex_bucket, aws_key=aws_key, entity_name=entity_name)

        manifest_snapshot_date = max([entry.updated_date for entry in manifest.entries])
        if snapshot_date < manifest_snapshot_date:
            snapshot_date = manifest_snapshot_date

        entity_index[entity_name] = manifest

    # Return if there is no new snapshot
    if prev_release is not None and prev_release.snapshot_date >= snapshot_date:
        logging.info(f"fetch_entities: no new snapshot found")
        return {}

    # Build and return entities
    for entity_name, manifest in entity_index.items():
        logging.info(f"fetch_releases: adding OpenAlexEntity({entity_name}), snapshot_date={snapshot_date})")

        # Also fetch merged IDs so that we can check if they have changed later on
        merged_ids = fetch_merged_ids(bucket=aws_openalex_bucket, aws_key=aws_key, entity_name=entity_name)

        # Save metadata
        entity = OpenAlexEntity(
            dag_id=dag_id,
            run_id=run_id,
            cloud_workspace=cloud_workspace,
            entity_name=entity_name,
            bq_dataset_id=bq_dataset_id,
            schema_folder=schema_folder,
            snapshot_date=snapshot_date,
            manifest=manifest,
            merged_ids=merged_ids,
            is_first_run=is_first_run,
        )
        entity_index[entity_name] = entity.to_dict()

    # If no entities created then skip
    if len(entity_index) == 0:
        logging.info(f"fetch_releases: no updates found")

    # Print summary information
    logging.info(f"is_first_run: {is_first_run}")
    logging.info(f"entities: {entity_index}")

    return entity_index


def aws_to_gcs_transfer(
    *, entity: OpenAlexEntity, gc_project_id: str, aws_conn_id: str, n_transfer_trys: int, aws_openalex_bucket: str
):
    # Make GCS Transfer Manifest for files that we need for this release
    object_paths = []
    for entry in entity.entries:
        object_paths.append(entry.object_key)
    for merged_id in entity.merged_ids:
        object_paths.append(merged_id.object_key)
    gcs_upload_transfer_manifest(object_paths, entity.transfer_manifest_uri)

    # Transfer files
    count = 0
    success = False
    aws_key = get_aws_key(aws_conn_id)
    for i in range(n_transfer_trys):
        success, objects_count = gcs_create_aws_transfer(
            aws_key=aws_key,
            aws_bucket=aws_openalex_bucket,
            include_prefixes=[],
            gc_project_id=gc_project_id,  # ,
            gc_bucket_dst_uri=entity.gcs_openalex_data_uri,
            description=f"Transfer OpenAlex {entity.entity_name} from AWS to GCS",
            transfer_manifest=entity.transfer_manifest_uri,
        )
        logging.info(
            f"gcs_create_aws_transfer: try={i + 1}/{n_transfer_trys}, success={success}, objects_count={objects_count}"
        )
        count += objects_count
        if success:
            break

    logging.info(f"gcs_create_aws_transfer: success={success}, total_object_count={count}")
    if not success:
        raise AirflowException("Google Storage Transfer unsuccessful")

    # After the transfer, verify the manifests and merged_ids are the same as when we fetched them during
    # the fetch_releases task. If they are the same, the data did not change during transfer. If the
    # manifests do not match then the data has changed, and we need to restart the DAG run manually.
    # See step 3 : https://docs.openalex.org/download-all-data/snapshot-data-format#the-manifest-file
    current_manifest = fetch_manifest(bucket=aws_openalex_bucket, aws_key=aws_key, entity_name=entity.entity_name)
    current_merged_ids = fetch_merged_ids(bucket=aws_openalex_bucket, aws_key=aws_key, entity_name=entity.entity_name)

    msgs = []
    manifest_changed = entity.manifest != current_manifest
    merged_ids_changed = entity.merged_ids != current_merged_ids

    if manifest_changed:
        msg = f"OpenAlexEntity({entity.entity_name}) manifests have changed"
        logging.error(f"aws_to_gcs_transfer: {msg}")
        msgs.append(msg)

    if merged_ids_changed:
        msg = "OpenAlexEntity({entity.entity_name}) merged_ids have changed"
        logging.error(f"aws_to_gcs_transfer: {msg}")
        msgs.append(msg)

    if not manifest_changed and not merged_ids_changed:
        logging.info(f"aws_to_gcs_transfer: manifests and merged_ids the same")
    else:
        raise AirflowException(f"aws_to_gcs_transfer: {' ,'.join(msgs)}")


def download(*, entity: OpenAlexEntity, **context):
    output_folder = f"{entity.download_folder}/data/{entity.entity_name}/"
    bucket_path = f"{entity.gcs_openalex_data_uri}data/{entity.entity_name}/*"
    op = BashOperator(
        task_id="process_entity.download",
        bash_command="mkdir -p "
        + output_folder
        + " && gcloud auth activate-service-account --key-file=${GOOGLE_APPLICATION_CREDENTIALS}"
        + f" && gsutil -m -q cp -L {entity.log_path} -r "
        + bucket_path
        + " "
        + output_folder,
        do_xcom_push=False,
    )
    op.execute(context)


def transform(*, entity: OpenAlexEntity):
    # Cleanup in case we re-run task
    output_folder = os.path.join(entity.transform_folder, "data", entity.entity_name)
    if os.path.exists(output_folder):
        clean_dir(output_folder)

    # Initialise schema generator
    merged_schema_map = OrderedDict()

    with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        futures = []
        for entry in entity.entries:
            input_path = os.path.join(entity.download_folder, entry.object_key)
            output_path = os.path.join(entity.transform_folder, entry.object_key)
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
    with open(entity.generated_schema_path, mode="w") as f_out:
        json.dump(merged_schema, f_out, indent=2)


def upload_schema(*, entity: OpenAlexEntity, transform_bucket: str):
    success = gcs_upload_files(bucket_name=transform_bucket, file_paths=[entity.generated_schema_path])
    if not success:
        raise AirflowException("upload_schema: error uploading schema")


def compare_schemas(*, entity: OpenAlexEntity, transform_bucket: str, slack_conn_id: str, **context):
    logging.info(f"Loading schemas from file: generated: {entity.generated_schema_path}")
    logging.info(f"Expected: {entity.schema_file_path}")

    # Download merged schema
    success = gcs_download_blob(
        bucket_name=transform_bucket,
        blob_name=gcs_blob_name_from_path(entity.generated_schema_path),
        file_path=entity.generated_schema_path,
    )
    if not success:
        raise AirflowException("compare_schemas: error downloading schema")

    # Read in the expected schema for the entity.
    merged_schema = load_json(entity.generated_schema_path)
    expected_schema = load_json(entity.schema_file_path)

    try:
        match = bq_compare_schemas(expected_schema, merged_schema, check_types_match=False)
    except:
        match = False

    if not match:
        logging.info("Generated schema and expected do not match! - Sending a notification via Slack")
        slack_msg = f"Found differences in the OpenAlex entity {entity.entity_name} data structure for the data dump vs pre-defined Bigquery schema. Please investigate."
        ti: TaskInstance = context["ti"]
        execution_date = context["execution_date"]
        send_slack_msg(ti=ti, logical_date=execution_date, comments=slack_msg, slack_conn_id=slack_conn_id)


def upload_files(*, entity: OpenAlexEntity, transform_bucket: str):
    # Make files to upload
    file_paths = []
    for entry in entity.entries:
        file_path = os.path.join(entity.transform_folder, entry.object_key)
        file_paths.append(file_path)

    # Upload files
    success = gcs_upload_files(bucket_name=transform_bucket, file_paths=file_paths)  # cloud_workspace.transform_bucket
    if not success:
        raise AirflowException("upload_files: error uploading files to cloud storage")


def bq_load_table(*, entity: OpenAlexEntity):
    table_name = "main"
    table_id = entity.bq_table_id
    description = entity.table_description

    if bq.bq_table_exists(table_id):
        raise AirflowException(f"Error, {table_id} for OpenAlexEntity({entity.entity_name}) already exists")

    logging.info(f"Loading OpenAlexEntity({entity.entity_name}) {table_name} table {table_id}")
    success = bq.bq_load_table(
        uri=entity.data_uri,
        table_id=table_id,
        schema_file_path=entity.schema_file_path,
        source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        ignore_unknown_values=True,
        table_description=description,
    )
    if not success:
        raise AirflowException(f"Error loading OpenAlexEntity({entity.entity_name}) {table_name} table {table_id}")


def expire_previous_version(
    dag_id: str,
    project_id: str,
    dataset_id: str,
    table_id: str,
    snapshot_date: pendulum.DateTime,
    expiry_days: int,
    api_bq_dataset_id: str,
    entity_id: str = DATASET_API_ENTITY_ID,
    client: bigquery.Client = None,
):
    # Fetch previous releases
    api = DatasetAPI(bq_project_id=project_id, bq_dataset_id=api_bq_dataset_id)
    prev_release = api.get_latest_dataset_release(dag_id=dag_id, entity_id=entity_id, date_key="snapshot_date")

    # Check that previous release and this release are not the same
    if prev_release.snapshot_date == snapshot_date:
        raise AirflowException(
            f"expire_previous_version: previous release snapshot date should not match current snapshot date"
        )

    # Check that table expiry not set
    prev_table_id = bq.bq_sharded_table_id(project_id, dataset_id, table_id, prev_release.snapshot_date)
    if client is None:
        client = bigquery.Client()
    table = client.get_table(prev_table_id)
    if table.expires is None:
        logging.info(f"Setting expiry time for {prev_table_id} to {expiry_days} days")
        bq_set_table_expiry(table_id=prev_table_id, days=expiry_days)
    else:
        logging.info(f"Table expiry time already set. The table {prev_table_id} expires on {table.expires}")


def add_dataset_release(
    *,
    dag_id: str,
    run_id: str,
    snapshot_date: pendulum.DateTime,
    bq_project_id: str,
    api_bq_dataset_id: str,
    entity_id: str = DATASET_API_ENTITY_ID,
):
    api = DatasetAPI(bq_project_id=bq_project_id, bq_dataset_id=api_bq_dataset_id)
    now = pendulum.now()
    dataset_release = DatasetRelease(
        dag_id=dag_id,
        entity_id=entity_id,
        dag_run_id=run_id,
        created=now,
        modified=now,
        snapshot_date=snapshot_date,
    )
    api.add_dataset_release(dataset_release)


def get_entity(entity_index: dict, entity_name: str) -> Optional[OpenAlexEntity]:
    if entity_name not in entity_index:
        return None

    return OpenAlexEntity.from_dict(entity_index[entity_name])


def get_task_id(**context):
    return context["ti"].task_id


def make_no_updated_data_msg(task_id: str, entity_name: str) -> str:
    return (
        f"{task_id}: skipping this task, as there is no updated data for OpenAlexEntity({entity_name}) in this release"
    )


def make_first_run_message(task_id: str):
    return f"{task_id}: skipping this task, as it is not executed on the first run"


def make_no_merged_ids_msg(task_id: str, entity_name: str) -> str:
    return (
        f"{task_id}: skipping this task, as there are no merged_ids for OpenAlexEntity({entity_name}) in this release"
    )


def get_aws_key(aws_conn_id: str) -> Tuple[str, str]:
    """Get the AWS access key id and secret access key from the aws_conn_id airflow connection.

    :return: access key id and secret access key
    """

    conn = BaseHook.get_connection(aws_conn_id)
    access_key_id = conn.login
    secret_key = conn.password

    if access_key_id is None:
        raise ValueError(f"OpenAlexTelescope.aws_key: {aws_conn_id} login is None")

    if secret_key is None:
        raise ValueError(f"OpenAlexTelescope.aws_key: {aws_conn_id} password is None")

    return access_key_id, secret_key


def fetch_manifest(
    *,
    bucket: str,
    aws_key: Tuple[str, str],
    entity_name: str,
) -> Manifest:
    """Fetch OpenAlex manifests for a range of entity types.

    :param bucket: the OpenAlex AWS bucket.
    :param aws_key: the aws_access_key_id and aws_secret_key as a tuple.
    :param entity_name: the entity type.
    :return: None
    """

    aws_access_key_id, aws_secret_key = aws_key
    client = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_key,
    ).client("s3")
    obj = client.get_object(Bucket=bucket, Key=f"data/{entity_name}/manifest")
    data = json.loads(obj["Body"].read().decode())

    # Add s3:// as necessary

    return Manifest.from_dict(data)


def fetch_merged_ids(
    *, bucket: str, aws_key: Tuple[str, str], entity_name: str, prefix: str = "data/merged_ids"
) -> List[MergedId]:
    aws_access_key_id, aws_secret_key = aws_key
    client = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_key,
    ).client("s3")
    paginator = client.get_paginator("list_objects_v2")

    results = []
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{prefix}/{entity_name}"):
        for content in page.get("Contents", []):
            obj_key = content["Key"]
            # There is a dud file in data/merged_ids/sources/
            if obj_key != "data/merged_ids/sources/.csv":
                url = f"s3://{bucket}/{obj_key}"
                content_length = content["Size"]
                results.append(MergedId(url, content_length))

    # Sort from oldest to newest
    results.sort(key=lambda m: m.updated_date, reverse=False)

    return results


def transform_file(download_path: str, transform_path: str) -> Tuple[OrderedDict, list]:
    """Transforms a single file.
    Each entry/object in the gzip input file is transformed and the transformed object is immediately written out to
    a gzip file. For each entity only one field has to be transformed.

    This function generates and returnms a Bigquery style schema from the transformed object,
    using the ScehmaGenerator from the 'bigquery_schema_generator' package.

    :param download_path: The path to the file with the OpenAlex entries.
    :param transform_path: The path where transformed data will be saved.
    :return: schema_map. A nested OrderedDict object produced by the SchemaGenertaor.
    :return: schema_generator.error_logs: Possible error logs produced by the SchemaGenerator.
    """

    # Make base folder, e.g. authors/updated_date=2023-09-17
    base_folder = os.path.dirname(transform_path)
    os.makedirs(base_folder, exist_ok=True)

    # Initialise the schema generator.
    schema_map = OrderedDict()
    schema_generator = SchemaGenerator(input_format="dict")

    logging.info(f"Transforming {download_path}")
    with gzip.open(download_path, "rb") as f_in, gzip.open(transform_path, "wt", encoding="ascii") as f_out:
        reader = jsonlines.Reader(f_in)
        for obj in reader.iter(skip_empty=True):
            transform_object(obj)

            # Wrap this in a try and pass so that it doesn't
            # cause the transform step to fail unexpectedly.
            try:
                schema_generator.deduce_schema_for_record(obj, schema_map)
            except Exception:
                pass

            json.dump(obj, f_out)
            f_out.write("\n")

    logging.info(f"Finished transform, saved to {transform_path}")

    return download_path, schema_map, schema_generator.error_logs


def clean_array_field(obj: dict, field: str):
    if field in obj:
        value = obj.get(field) or []
        obj[field] = [x for x in value if x is not None]


def transform_object(obj: dict):
    """Transform an entry/object for one of the OpenAlex entities.
    For the Work entity only the "abstract_inverted_index" field is transformed.
    For the Concept and Institution entities only the "international" field is transformed.

    :param obj: Single object with entity information
    :param field: The field of interested that is transformed.
    :return: None.
    """

    # Remove nulls from arrays
    # And handle null value
    array_fields = ["corresponding_institution_ids", "corresponding_author_ids", "societies", "alternate_titles"]
    for field in array_fields:
        clean_array_field(obj, field)

    # Remove nulls from authors affiliations[].years
    for affiliation in obj.get("affiliations", []):
        if "years" in affiliation:
            affiliation["years"] = [x for x in affiliation["years"] if x is not None]

    field = "abstract_inverted_index"
    if field in obj:
        if not isinstance(obj.get(field), dict):
            return
        keys = list(obj[field].keys())
        values = [str(value)[1:-1] for value in obj[field].values()]

        obj[field] = {"keys": keys, "values": values}

    field = "international"
    if field in obj:
        for nested_field in obj.get(field, {}).keys():
            if not isinstance(obj[field][nested_field], dict):
                continue
            keys = list(obj[field][nested_field].keys())
            values = list(obj[field][nested_field].values())

            obj[field][nested_field] = {"keys": keys, "values": values}

    # Transform updated_date from a date into a datetime
    field = "updated_date"
    if field in obj:
        obj[field] = pendulum.parse(obj[field]).to_iso8601_string()


def bq_compare_schemas(expected: List[dict], actual: List[dict], check_types_match: Optional[bool] = False) -> bool:
    """Compare two Bigquery style schemas for if they have the same fields and/or data types.

    :param expected: the expected schema.
    :param actual: the actual schema.
    :check_types_match: Optional, if checking data types of fields is required.
    :return: whether the expected and actual match.
    """

    expected.sort(key=lambda c: c["name"], reverse=False)
    actual.sort(key=lambda c: c["name"], reverse=False)

    exp_names = [field_def["name"] for field_def in expected]
    act_names = [field_def["name"] for field_def in actual]

    if len(exp_names) != len(act_names):
        logging.info("Fields do not match:")
        logging.info(f"Only in expected: {set(exp_names) - set(act_names)}")
        logging.info(f"Only in actual: {set(act_names) - set(exp_names)}")
        return False

    # Check data types of fields
    if check_types_match:
        for exp_field, act_field in zip(expected, actual):
            if exp_field["type"] != act_field["type"]:
                logging.info(
                    f"Field types do not match for field  '{exp_field['name']}' ! Actual: {act_field['type']} vs Expected: {exp_field['type']}"
                )
            all_matched = False

    # Check for sub-fields within the schema.
    all_matched = True
    for exp_field, act_field in zip(expected, actual):
        # Ignore the "mode" and "description" definitions in fields as they are not required for check.
        diff = DeepDiff(exp_field, act_field, ignore_order=True, exclude_regex_paths=r"\s*(description|mode)")
        for diff_type, changes in diff.items():
            all_matched = False
            log_diff(diff_type, changes)

        if "fields" in exp_field and not "fields" in act_field:
            logging.info(f"Fields are present under expected but not in actual! Field name: {exp_field['name']}")
            all_mathced = False
        elif not "fields" in exp_field and "fields" in act_field:
            logging.info(f"Fields are present under actual but not in expected! Field name: {act_field['name']}")
            all_matched = False
        elif "fields" in exp_field and "fields" in act_field:
            all_matched = bq_compare_schemas(exp_field["fields"], act_field["fields"], check_types_match)

    return all_matched


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


def load_json(file_path: str) -> Any:
    """Read in a *.json file."""

    with open(file_path, "r") as f_in:
        data = json.load(f_in)

    return data


def bq_set_table_expiry(*, table_id: str, days: int):
    """Set the expiry time for a BigQuery table.

    :param table_id: the fully qualified BigQuery table identifier.
    :param days: the number of days from now until the table expires.
    :return:
    """

    client = bigquery.Client()
    table = bigquery.Table(table_id)
    table.expires = pendulum.now().add(days=days)
    client.update_table(table, ["expires"])
