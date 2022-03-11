# Copyright 2022 Curtin University
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

# Author: Aniek Roelofs

from __future__ import annotations

import gzip
import json
import logging
import os
import subprocess
from concurrent.futures import ProcessPoolExecutor, as_completed
from subprocess import Popen
from typing import List, Tuple

import boto3
import jsonlines
import pendulum
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.hooks.base import BaseHook
from airflow.models.variable import Variable
from google.cloud import storage
from observatory.platform.utils.airflow_utils import AirflowVars
from observatory.platform.utils.gc_utils import aws_to_google_cloud_storage_transfer
from observatory.platform.utils.proc_utils import wait_for_process
from observatory.platform.workflows.stream_telescope import (
    StreamRelease,
    StreamTelescope,
)

from academic_observatory_workflows.config import schema_folder as default_schema_folder


class OpenAlexRelease(StreamRelease):
    def __init__(
        self,
        dag_id: str,
        start_date: pendulum.DateTime,
        end_date: pendulum.DateTime,
        first_release: bool,
        max_processes: int,
    ):
        """Construct a OpenAlexRelease instance

        :param dag_id: the id of the DAG.
        :param start_date: the start_date of the release.
        :param end_date: the end_date of the release.
        :param first_release: whether this is the first release that is processed for this DAG
        :param max_processes: max processes for transforming files.
        """
        super().__init__(
            dag_id, start_date, end_date, first_release, download_files_regex=".*.gz", transform_files_regex=".*.gz"
        )
        self.max_processes = max_processes

    @property
    def transfer_manifest_path_download(self) -> str:
        """Get the path to the file with ids of updated entities that are transferred to the download bucket.

        :return: the file path.
        """
        return os.path.join(self.download_folder, "transfer_manifest_download.csv")

    @property
    def transfer_manifest_path_transform(self) -> str:
        """Get the path to the file with ids of updated entities that are transferred to the transform bucket.

        :return: the file path.
        """
        return os.path.join(self.download_folder, "transfer_manifest_transform.csv")

    # TODO uncomment when using transfer manifest
    # @property
    # def transfer_manifest_blob_download(self):
    #     return blob_name(self.transfer_manifest_path_download)
    # @property
    # def transfer_manifest_blob_transform(self):
    #     return blob_name(self.transfer_manifest_path_transform)

    def write_transfer_manifest(self):
        """Write a transfer manifest file with filenames of files changed since the start date of this release.
        A separate manifest file is created for the download and transform bucket.
        Each filename excludes the s3 bucket name (s3://openalex) and is between double quotes, e.g.:
        s3://openalex/data/works/updated_date=2021-12-17/0000_part_00.gz ->
        "data/works/updated_date=2021-12-17/0000_part_00.gz"

        :return: The number of updated entities.
        """
        logging.info(
            f"Writing info on updated entities from 'institution', 'concept' and 'work' to"
            f" {self.transfer_manifest_path_download}"
        )
        logging.info(
            f"Writing info on updated entities from 'author' and 'venue' to" f" {self.transfer_manifest_path_transform}"
        )
        aws_access_key_id, aws_secret_access_key = get_aws_conn_info()
        s3client = boto3.client("s3", aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

        updated_entities_count = 0
        with open(self.transfer_manifest_path_download, "w") as f_download, open(
            self.transfer_manifest_path_transform, "w"
        ) as f_transform:
            for entity in ["authors", "concepts", "institutions", "venues", "works"]:
                manifest_obj = s3client.get_object(Bucket=OpenAlexTelescope.AWS_BUCKET, Key=f"data/{entity}/manifest")
                content = manifest_obj["Body"].read()
                entries = json.loads(content.decode())["entries"]

                for entry in entries:
                    updated_date_str = entry["url"].split("updated_date=")[1].split("/")[0]
                    updated_date = pendulum.from_format(updated_date_str, "YYYY-MM-DD")
                    if updated_date >= self.start_date:
                        object_name = '"' + entry["url"].replace("s3://openalex/", "") + '"\n'
                        if entity in ["authors", "venues"]:
                            f_transform.write(object_name)
                        else:
                            f_download.write(object_name)
                        updated_entities_count += 1

        if updated_entities_count == 0:
            raise AirflowSkipException("No updated entities to process")

    def transfer(self, max_retries):
        """Transfer files from AWS bucket to Google Cloud bucket

        :param max_retries: Number of max retries to try the transfer
        :return: None.
        """
        aws_access_key_id, aws_secret_access_key = get_aws_conn_info()
        gc_project_id = Variable.get(AirflowVars.PROJECT_ID)

        # TODO use transfer manifest instead when that is working
        download_transfer = {"manifest": self.transfer_manifest_path_download, "bucket": self.download_bucket}
        transform_transfer = {"manifest": self.transfer_manifest_path_transform, "bucket": self.transform_bucket}

        total_count = 0
        for transfer in [download_transfer, transform_transfer]:
            success = False
            prefixes = []
            with open(transfer["manifest"], "r") as f:
                for line in f:
                    prefixes.append(line.strip("\n").strip('"'))

            if not prefixes:
                continue

            for i in range(max_retries):
                if success:
                    break
                success, objects_count = aws_to_google_cloud_storage_transfer(
                    aws_access_key_id,
                    aws_secret_access_key,
                    aws_bucket=OpenAlexTelescope.AWS_BUCKET,
                    include_prefixes=prefixes,
                    gc_project_id=gc_project_id,
                    gc_bucket=transfer["bucket"],
                    gc_bucket_path=f"telescopes/{self.dag_id}/{self.release_id}/",
                    description=f"Transfer OpenAlex data from Airflow telescope to {transfer['bucket']}",
                    # transfer_manifest=f"gs://{self.download_bucket}/{self.transfer_manifest_blob}"
                )
                total_count += objects_count

            if not success:
                raise AirflowException(f"Google Storage Transfer unsuccessful, status: {success}")

        logging.info(f"Total number of objects transferred: {total_count}")

    def download_transferred(self):
        """Download the updated entities from the Google Cloud download bucket to a local directory using gsutil.
        Gsutil is used instead of the standard Google Cloud Python library, because this is faster at downloading files.
        It supports multi-threading with the '-m' flag and can open multiple simultaneous connections to GCS.
        In future the 'gcloud storage' command might be used instead which is even faster, but still in preview.

        :return: None.
        """
        # Authenticate gcloud with service account
        args = [
            "gcloud",
            "auth",
            "activate-service-account",
            f"--key-file" f"={os.environ['GOOGLE_APPLICATION_CREDENTIALS']}",
        ]
        proc: Popen = subprocess.Popen(
            args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=dict(os.environ, CLOUDSDK_PYTHON="python3")
        )
        run_subprocess_cmd(proc, args)

        logging.info(f"Downloading transferred files from Google Cloud bucket: {self.download_bucket}")
        log_path = os.path.join(self.download_folder, "cp.log")
        # Download all records from bucket using Gsutil
        args = [
            "gsutil",
            "-m",
            "-q",
            "cp",
            "-L",
            log_path,
            "-r",
            f"gs://{self.download_bucket}/telescopes/{self.dag_id}/{self.release_id}/*",
            self.download_folder,
        ]
        proc: Popen = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        run_subprocess_cmd(proc, args)

    def transform(self):
        """Transform all files for the Work, Concept and Institution entities.
        Transforms one file per process.

        :return: None.
        """
        logging.info(f"Transforming files, no. workers: {self.max_processes}")

        with ProcessPoolExecutor(max_workers=self.max_processes) as executor:
            futures = []
            for file_path in self.download_files:
                file = os.path.relpath(file_path, self.download_folder)
                transform_path = os.path.join(self.transform_folder, file)
                futures.append(executor.submit(transform_file, file_path, transform_path))
            for future in as_completed(futures):
                future.result()


class OpenAlexTelescope(StreamTelescope):
    """OpenAlex telescope"""

    DAG_ID = "openalex"
    AWS_BUCKET = "openalex"
    AIRFLOW_CONN_AWS = "openalex"

    def __init__(
        self,
        dag_id: str = DAG_ID,
        start_date: pendulum.DateTime = pendulum.datetime(2021, 12, 1),
        schedule_interval: str = "@weekly",
        dataset_id: str = "openalex",
        dataset_description: str = "The OpenAlex dataset: https://docs.openalex.org/about-the-data",
        queue: str = "remote_queue",
        merge_partition_field: str = "id",
        schema_folder: str = os.path.join(default_schema_folder(), "openalex"),
        airflow_vars: List = None,
        airflow_conns: List = None,
        max_processes: int = os.cpu_count(),
    ):
        """Construct an OpenAlexTelescope instance.

        :param dag_id: the id of the DAG.
        :param start_date: the start date of the DAG.
        :param schedule_interval: the schedule interval of the DAG.
        :param dataset_id: the dataset id.
        :param dataset_description: the dataset description.
        :param queue: the queue that the tasks should run on.
        :param merge_partition_field: the BigQuery field used to match partitions for a merge
        :param schema_folder: the SQL schema path.
        :param airflow_vars: list of airflow variable keys, for each variable it is checked if it exists in airflow
        :param max_processes: max processes for transforming files.
        """

        if airflow_vars is None:
            airflow_vars = [
                AirflowVars.DATA_PATH,
                AirflowVars.PROJECT_ID,
                AirflowVars.DATA_LOCATION,
                AirflowVars.DOWNLOAD_BUCKET,
                AirflowVars.TRANSFORM_BUCKET,
            ]
        if airflow_conns is None:
            airflow_conns = [
                self.AIRFLOW_CONN_AWS,
            ]
        super().__init__(
            dag_id,
            start_date,
            schedule_interval,
            dataset_id,
            merge_partition_field,
            schema_folder,
            dataset_description=dataset_description,
            queue=queue,
            airflow_vars=airflow_vars,
            airflow_conns=airflow_conns,
            load_bigquery_table_kwargs={"ignore_unknown_values": True},
        )
        self.max_processes = max_processes

        self.add_setup_task(self.check_dependencies)
        self.add_task(self.write_transfer_manifest)
        # self.add_task(self.upload_transfer_manifest) #TODO uncomment when using transfer manifest
        self.add_task(self.transfer)
        self.add_task(self.download_transferred)
        self.add_task(self.transform)
        self.add_task(self.upload_transformed)
        self.add_task(self.bq_load_partition)
        self.add_task_chain([self.bq_delete_old, self.bq_append_new, self.cleanup], trigger_rule="none_failed")

    def get_bq_load_info(self, release: OpenAlexRelease) -> List[Tuple[str, str, str]]:
        """Get a list of the transform blob, main table id and partition table id that are used to load data into
        BigQuery.
        This method overrides the parent class' method for this telescope, because there are transform files
        inside the transform bucket that were transferred directly. Which means that they can't be found with
        the 'release.transform_files' property that is normally used.

        :param release: The release object.
        :return: List with tuples of transform_blob, main_table_id and partition_table_id
        """
        base_transform_blob = os.path.join("telescopes", "openalex", release.release_id, "data")
        bq_load_info = []
        for entity in ["authors", "concepts", "institutions", "venues", "works"]:
            # Check if files exist in folder
            client = storage.Client()
            exists = list(
                client.list_blobs(
                    release.transform_bucket,
                    prefix=f"telescopes/{release.dag_id}" f"/{release.release_id}/data/{entity}",
                    max_results=1,
                )
            )
            if exists:
                table_name = entity[:-1].capitalize()
                bq_load_info.append((f"{base_transform_blob}/{entity}/*", table_name, f"{table_name}_partitions"))
        return bq_load_info

    def make_release(self, **kwargs) -> OpenAlexRelease:
        """Make a Release instance

        :param kwargs: The context passed from the PythonOperator.
        :return: an OpenAlexRelease
        """

        start_date, end_date, first_release = self.get_release_info(**kwargs)
        release = OpenAlexRelease(self.dag_id, start_date, end_date, first_release, self.max_processes)
        return release

    def write_transfer_manifest(self, release: OpenAlexRelease, **kwargs):
        """Task to write transfer manifest files used during transfer.

        :param release: an OpenAlexRelease instance.
        :param kwargs: The context passed from the PythonOperator.
        :return: None.
        """
        release.write_transfer_manifest()

    # TODO uncomment when transfer manifest works
    # def upload_transfer_manifest(self, release: OpenAlexRelease, **kwargs):
    #     upload_file_to_cloud_storage(release.download_bucket, release.transfer_manifest_blob_download,
    #                                  release.transfer_manifest_path_download)
    #     upload_file_to_cloud_storage(release.download_bucket, release.transfer_manifest_blob_transform,
    #                                  release.transfer_manifest_path_transform)

    def transfer(self, release: OpenAlexRelease, **kwargs):
        """Task to transfer the OpenAlex data

        :param release: an OpenAlexRelease instance.
        :param kwargs: The context passed from the PythonOperator.
        :return: None.
        """
        release.transfer(max_retries=self.max_retries)

    def download_transferred(self, release: OpenAlexRelease, **kwargs):
        """Task to download the OpenAlexRelease data.

        :param release: an OpenAlexRelease instance.
        :param kwargs: The context passed from the PythonOperator.
        :return: None.
        """
        release.download_transferred()

    def transform(self, release: OpenAlexRelease, **kwargs):
        """Task to transform the OpenAlexRelease data.

        :param release: an OpenAlexRelease instance.
        :param kwargs: The context passed from the PythonOperator.
        :return: None.
        """
        release.transform()


def get_aws_conn_info() -> (str, str):
    """Get the AWS access key id and secret access key from the OpenAlex airflow connection.

    :return: access key id and secret access key
    """
    conn = BaseHook.get_connection(OpenAlexTelescope.AIRFLOW_CONN_AWS)
    access_key_id = conn.login
    secret_access_key = conn.password

    return access_key_id, secret_access_key


def run_subprocess_cmd(proc: Popen, args: list):
    """Execute and wait for subprocess to finish, also handle stdout & stderr from process.

    :param proc: subprocess proc
    :param args: args list that was passed on to subprocess
    :return: None.
    """
    logging.info(f"Executing bash command: {subprocess.list2cmdline(args)}")
    out, err = wait_for_process(proc)
    if out:
        logging.info(out)
    if err:
        logging.info(err)
    if proc.returncode != 0:
        # Don't raise exception if the only error is because blobs could not be found in bucket
        err_lines = err.split("\n")
        if err_lines:
            raise AirflowException("bash command failed")
    logging.info("Finished cmd successfully")


def transform_file(download_path: str, transform_path: str):
    """Transforms a single file.
    Each entry/object in the gzip input file is transformed and the transformed object is immediately written out to
    a gzip file.
    For each entity only one field has to be transformed.

    :param download_path: The path to the file with the OpenAlex entries.
    :param transform_path: The path where transformed data will be saved
    :return: None.
    """
    if not os.path.isdir(os.path.dirname(transform_path)):
        os.makedirs(os.path.dirname(transform_path))

    logging.info(f"Transforming {download_path}")
    with gzip.open(download_path, "rb") as f_in, gzip.open(transform_path, "wt", encoding="ascii") as f_out:
        reader = jsonlines.Reader(f_in)
        for obj in reader:
            if "works" in download_path:
                transform_object(obj, "abstract_inverted_index")
            else:
                transform_object(obj, "international")
            json.dump(obj, f_out)
            f_out.write("\n")
    logging.info(f"Finished transform, saved to {transform_path}")


def transform_object(obj: dict, field: str):
    """Transform an entry/object for one of the OpenAlex entities.
    For the Work entity only the "abstract_inverted_index" field is transformed.
    For the Concept and Institution entities only the "international" field is transformed.

    :param obj: Single object with entity information
    :param field: The field of interested that is transformed.
    :return: None.
    """
    if field == "international":
        for nested_field in obj.get(field, {}).keys():
            if not isinstance(obj[field][nested_field], dict):
                continue
            keys = list(obj[field][nested_field].keys())
            values = list(obj[field][nested_field].values())

            obj[field][nested_field] = {"keys": keys, "values": values}
    elif field == "abstract_inverted_index":
        if not isinstance(obj.get(field), dict):
            return
        keys = list(obj[field].keys())
        values = [str(value)[1:-1] for value in obj[field].values()]

        obj[field] = {"keys": keys, "values": values}
