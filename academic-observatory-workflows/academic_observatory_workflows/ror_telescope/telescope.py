# Copyright 2021-2024 Curtin University
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

# Author: Aniek Roelofs, James Diprose

from __future__ import annotations

import json
import logging
import math
import os
import shutil
import urllib.parse
from typing import Any, Dict, List
from zipfile import BadZipFile, ZipFile

import pendulum
from airflow import DAG
from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowException
from google.cloud.bigquery import SourceFormat

from academic_observatory_workflows.config import project_path
from observatory.api.client.model.dataset_release import DatasetRelease
from observatory_platform.airflow import on_failure_callback
from observatory_platform.dataset_api import make_observatory_api
from observatory_platform.google.bigquery import bq_create_dataset, bq_find_schema, bq_load_table, bq_sharded_table_id
from observatory_platform.config import AirflowConns
from observatory_platform.files import clean_dir, list_files, save_jsonl_gz
from observatory_platform.google.gcs import gcs_blob_name_from_path, gcs_blob_uri, gcs_download_blob, gcs_upload_files
from observatory_platform.airflow.workflow import CloudWorkspace
from observatory_platform.airflow.tasks import check_dependencies
from observatory_platform.utils.http_download import download_file
from observatory_platform.url_utils import retry_get_url
from observatory_platform.airflow.workflow import cleanup, set_task_state, SnapshotRelease


class DagParams:
    """

    :param dag_id: the id of the DAG.
    :param cloud_workspace: the cloud workspace settings.
    :param bq_dataset_id: the BigQuery dataset id.
    :param bq_table_name: the BigQuery table name.
    :param api_dataset_id: the Dataset ID to use when storing releases.
    :param schema_folder: the SQL schema path.
    :param dataset_description: description for the BigQuery dataset.
    :param table_description: description for the BigQuery table.
    :param observatory_api_conn_id: the Observatory API connection key.
    :param ror_conceptrecid: the Zenodo conceptrecid for the ROR dataset.
    :param start_date: the start date of the DAG.
    :param schedule: the schedule interval of the DAG.
    :param catchup: whether to catchup the DAG or not.
    :param max_active_runs: the maximum number of DAG runs that can be run at once.
    :param retries: the number of times to retry a task.
    """

    def __init__(
        self,
        dag_id: str,
        cloud_workspace: CloudWorkspace,
        bq_dataset_id: str = "ror",
        bq_table_name: str = "ror",
        api_dataset_id: str = "ror",
        schema_folder: str = project_path("ror_telescope", "schema"),
        dataset_description: str = "The Research Organization Registry (ROR) database: https://ror.org/",
        table_description: str = "The Research Organization Registry (ROR) database: https://ror.org/",
        observatory_api_conn_id: str = AirflowConns.OBSERVATORY_API,
        ror_conceptrecid: int = 6347574,
        start_date: pendulum.DateTime = pendulum.datetime(2021, 9, 1),
        schedule: str = "@weekly",
        catchup: bool = True,
        max_active_runs: int = 1,
        retries: int = 3,
    ):

        pass


def create_dag(dag_params: DagParams) -> DAG:
    """Construct a RorTelescope instance."""

    @dag(
        dag_id=dag_params.dag_id,
        start_date=dag_params.start_date,
        schedule=dag_params.schedule,
        catchup=dag_params.catchup,
        max_active_runs=dag_params.max_active_runs,
        tags=["academic-observatory"],
        default_args={
            "owner": "airflow",
            "on_failure_callback": on_failure_callback,
            "retries": dag_params.retries,
        },
    )
    def ror():
        @task
        def fetch_releases(**context) -> List[dict]:
            """Lists all ROR records and publishes their url, snapshot_date and checksum as an XCom."""

            from academic_observatory_workflows.ror_telescope import tasks

            return tasks.fetch_release()

        @task_group(group_id="process_release")
        def process_release(data):
            @task
            def download(release: dict, **context):
                """Task to download the ROR releases."""

                from academic_observatory_workflows.ror_telescope import tasks

                tasks.download(release)

            @task
            def transform(release: dict, **context):
                """Task to transform the ROR releases."""

                from academic_observatory_workflows.ror_telescope import tasks

                tasks.transform(release)

            @task
            def bq_load(release: dict, **context) -> None:
                """Load the data into BigQuery."""

                from academic_observatory_workflows.ror_telescope import tasks

                tasks.bq_load(release)

            @task
            def add_dataset_releases(release: dict, **context) -> None:
                """Adds release information to API."""

                from academic_observatory_workflows.ror_telescope import tasks

                tasks.add_dataset_releases(release)

            @task
            def cleanup_workflow(release: dict, **context) -> None:
                """Delete all files, folders and XComs associated with this release."""

                from academic_observatory_workflows.ror_telescope import tasks

                tasks.cleanup_workflow(release)

            (download(data) >> transform(data) >> bq_load(data) >> add_dataset_releases(data) >> cleanup_workflow(data))

        check_task = check_dependencies(airflow_conns=[observatory_api_conn_id])
        xcom_releases = fetch_releases()
        process_release_task_group = process_release.expand(data=xcom_releases)

        (check_task >> xcom_releases >> process_release_task_group)

    return ror()
