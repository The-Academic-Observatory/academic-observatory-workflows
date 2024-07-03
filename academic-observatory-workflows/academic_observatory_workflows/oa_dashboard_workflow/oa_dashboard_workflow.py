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

# Author: James Diprose, Aniek Roelofs

from __future__ import annotations

import dataclasses
import functools
import glob
import json
import logging
import math
import os
import os.path
import shutil
import statistics
from concurrent.futures import as_completed, ThreadPoolExecutor
from typing import Dict, List, Optional, Tuple, Union
from urllib.parse import urlparse
from zipfile import ZipFile

import google.cloud.bigquery as bigquery
import jsonlines
import numpy as np
import pendulum
from airflow import DAG
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.models.baseoperator import chain
from airflow.sensors.external_task import ExternalTaskSensor
from glom import Coalesce, glom, SKIP
from jinja2 import Template

from academic_observatory_workflows.clearbit import clearbit_download_logo
from academic_observatory_workflows.config import project_path
from academic_observatory_workflows.github import trigger_repository_dispatch
from academic_observatory_workflows.oa_dashboard_workflow.institution_ids import INSTITUTION_IDS
from academic_observatory_workflows.wikipedia import fetch_wikipedia_descriptions
from academic_observatory_workflows.zenodo import make_draft_version, publish_new_version, Zenodo
from observatory_platform.airflow import get_airflow_connection_password, on_failure_callback
from observatory_platform.google.bigquery import (
    bq_create_dataset,
    bq_create_table_from_query,
    bq_load_from_memory,
    bq_run_query,
    bq_select_latest_table,
    bq_sharded_table_id,
    bq_table_id,
)
from observatory_platform.files import yield_jsonl
from observatory_platform.google.gcs import (
    gcs_blob_name_from_path,
    gcs_download_blob,
    gcs_download_blobs,
    gcs_upload_file,
)
from observatory_platform.airflow.workflow import CloudWorkspace
from observatory_platform.airflow.tasks import check_dependencies
from observatory_platform.utils.jinja2_utils import render_template
from observatory_platform.airflow.workflow import (
    cleanup,
    make_snapshot_date,
    set_task_state,
    SnapshotRelease,
)

INCLUSION_THRESHOLD = {"country": 5, "institution": 50}
MAX_REPOSITORIES = 200
START_YEAR = 2000
END_YEAR = pendulum.now().year - 1


README = """# COKI Open Access Dataset
The COKI Open Access Dataset measures open access performance for {{ n_countries }} countries and {{ n_institutions }} institutions
and is available in JSON Lines format. The data is visualised at the COKI Open Access Dashboard: https://open.coki.ac/.

## Licence
[COKI Open Access Dataset](https://open.coki.ac/data/) © {{ year }} by [Curtin University](https://www.curtin.edu.au/)
is licenced under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)

## Citing
To cite the COKI Open Access Dashboard please use the following citation:
> Diprose, J., Hosking, R., Rigoni, R., Roelofs, A., Chien, T., Napier, K., Wilson, K., Huang, C., Handcock, R., Montgomery, L., & Neylon, C. (2023). A User-Friendly Dashboard for Tracking Global Open Access Performance. The Journal of Electronic Publishing 26(1). doi: https://doi.org/10.3998/jep.3398

If you use the website code, please cite it as below:
> James P. Diprose, Richard Hosking, Richard Rigoni, Aniek Roelofs, Kathryn R. Napier, Tuan-Yow Chien, Alex Massen-Hane, Katie S. Wilson, Lucy Montgomery, & Cameron Neylon. (2022). COKI Open Access Website. Zenodo. https://doi.org/10.5281/zenodo.6374486

If you use this dataset, please cite it as below:
> Richard Hosking, James P. Diprose, Aniek Roelofs, Tuan-Yow Chien, Lucy Montgomery, & Cameron Neylon. (2022). COKI Open Access Dataset [Data set]. Zenodo. https://doi.org/10.5281/zenodo.6399463

## Attributions
The COKI Open Access Dataset contains information from:
* [Open Alex](https://openalex.org/) which is made available under a [CC0 licence](https://creativecommons.org/publicdomain/zero/1.0/).
* [Crossref Metadata](https://www.crossref.org/documentation/metadata-plus/) via the Metadata Plus program. Bibliographic metadata is made available without copyright restriction and Crossref generated data with a [CC0 licence](https://creativecommons.org/share-your-work/public-domain/cc0/). See [metadata licence information](https://www.crossref.org/documentation/retrieve-metadata/rest-api/rest-api-metadata-license-information/) for more details.
* [Unpaywall](https://unpaywall.org/). The [Unpaywall Data Feed](https://unpaywall.org/products/data-feed) is used under license. Data is freely available from Unpaywall via the API, data dumps and as a data feed.
* [Research Organization Registry](https://ror.org/) which is made available under a [CC0 licence](https://creativecommons.org/share-your-work/public-domain/cc0/).
"""


###################
# Airflow Workflow
###################


class OaDashboardRelease(SnapshotRelease):
    def __init__(
        self,
        *,
        dag_id: str,
        run_id: str,
        snapshot_date: pendulum.DateTime,
        input_project_id: str,
        output_project_id: str,
        bq_agg_dataset_id: str,
        bq_ror_dataset_id: str,
        bq_settings_dataset_id: str,
        bq_oa_dashboard_dataset_id: str,
    ):
        """Create an OaDashboardRelease instance.

        :param dag_id: the dag id.
        :param run_id: the DAG run id.
        :param snapshot_date: the release date.
        :param input_project_id: the ID of the Google Cloud project where data will be pulled from.
        :param output_project_id: the ID of the Google Cloud project where data will be written to.
        :param bq_agg_dataset_id: the id of the BigQuery dataset where the Academic Observatory aggregated data lives.
        :param bq_ror_dataset_id: the id of the BigQuery dataset containing the ROR table.
        :param bq_settings_dataset_id: the id of the BigQuery settings dataset, which contains the country table.
        :param bq_oa_dashboard_dataset_id: the id of the BigQuery dataset where the tables produced by this workflow will be created.
        """

        super().__init__(dag_id=dag_id, run_id=run_id, snapshot_date=snapshot_date)
        self.input_project_id = input_project_id
        self.output_project_id = output_project_id
        self.bq_ror_dataset_id = bq_ror_dataset_id
        self.bq_settings_dataset_id = bq_settings_dataset_id
        self.bq_agg_dataset_id = bq_agg_dataset_id
        self.bq_oa_dashboard_dataset_id = bq_oa_dashboard_dataset_id

    @property
    def build_path(self):
        path = os.path.join(self.transform_folder, "build")
        os.makedirs(path, exist_ok=True)
        return path

    @property
    def intermediate_path(self):
        path = os.path.join(self.transform_folder, "intermediate")
        os.makedirs(path, exist_ok=True)
        return path

    @property
    def out_path(self):
        path = os.path.join(self.transform_folder, "out")
        os.makedirs(path, exist_ok=True)
        return path

    @functools.cached_property
    def ror_table_id(self):
        return bq_select_latest_table(
            table_id=bq_table_id(self.input_project_id, self.bq_ror_dataset_id, "ror"),
            end_date=self.snapshot_date,
            sharded=True,
        )

    @functools.cached_property
    def country_table_id(self):
        return bq_table_id(self.input_project_id, self.bq_settings_dataset_id, "country")

    def observatory_agg_table_id(self, table_name: str):
        return bq_select_latest_table(
            table_id=bq_table_id(self.input_project_id, self.bq_agg_dataset_id, table_name),
            end_date=self.snapshot_date,
            sharded=True,
        )

    @functools.cached_property
    def institution_ids_table_id(self):
        return bq_sharded_table_id(
            self.output_project_id, self.bq_oa_dashboard_dataset_id, "institution_ids", self.snapshot_date
        )

    def oa_dashboard_table_id(self, table_name: str):
        return bq_sharded_table_id(
            self.output_project_id, self.bq_oa_dashboard_dataset_id, table_name, self.snapshot_date
        )

    def descriptions_table_id(self, table_name: str):
        return bq_sharded_table_id(
            self.output_project_id, self.bq_oa_dashboard_dataset_id, f"{table_name}_descriptions", self.snapshot_date
        )

    def logos_table_id(self, table_name: str):
        return bq_sharded_table_id(
            self.output_project_id, self.bq_oa_dashboard_dataset_id, f"{table_name}_logos", self.snapshot_date
        )

    @staticmethod
    def from_dict(dict_: dict):
        return OaDashboardRelease(
            dag_id=dict_["dag_id"],
            run_id=dict_["run_id"],
            snapshot_date=pendulum.parse(dict_["snapshot_date"]),
            input_project_id=dict_["input_project_id"],
            output_project_id=dict_["output_project_id"],
            bq_agg_dataset_id=dict_["bq_agg_dataset_id"],
            bq_ror_dataset_id=dict_["bq_ror_dataset_id"],
            bq_settings_dataset_id=dict_["bq_settings_dataset_id"],
            bq_oa_dashboard_dataset_id=dict_["bq_oa_dashboard_dataset_id"],
        )

    def to_dict(self) -> dict:
        return dict(
            dag_id=self.dag_id,
            run_id=self.run_id,
            snapshot_date=self.snapshot_date.to_datetime_string(),
            input_project_id=self.input_project_id,
            output_project_id=self.output_project_id,
            bq_agg_dataset_id=self.bq_agg_dataset_id,
            bq_ror_dataset_id=self.bq_ror_dataset_id,
            bq_settings_dataset_id=self.bq_settings_dataset_id,
            bq_oa_dashboard_dataset_id=self.bq_oa_dashboard_dataset_id,
        )


def create_dag(
    *,
    dag_id: str,
    cloud_workspace: CloudWorkspace,
    data_bucket: str,
    conceptrecid: int,
    doi_dag_id: str = "doi",
    entity_types: List[str] = None,
    bq_agg_dataset_id: str = "observatory",
    bq_ror_dataset_id: str = "ror",
    bq_settings_dataset_id: str = "settings",
    bq_oa_dashboard_dataset_id: str = "oa_dashboard",
    version: str = "v10",
    zenodo_host: str = "https://zenodo.org",
    github_conn_id="oa_dashboard_github_token",
    zenodo_conn_id="oa_dashboard_zenodo_token",
    start_date: Optional[pendulum.DateTime] = pendulum.datetime(2021, 5, 2),
    schedule: Optional[str] = "@weekly",
    max_active_runs: int = 1,
    retries: int = 3,
) -> DAG:
    """Create the OaDashboardWorkflow, which generates data files for the COKI Open Access Dashboard.

    :param dag_id: the DAG id.
    :param cloud_workspace: The CloudWorkspace.
    :param data_bucket: the Google Cloud Storage bucket where image data should be stored.
    :param conceptrecid: the Zenodo Concept Record ID for the COKI Open Access Dataset. The Concept Record ID is
    the last set of numbers from the Concept DOI.
    :param doi_dag_id: the DAG id to wait for.
    :param entity_types: the table names.
    :param bq_agg_dataset_id: the id of the BigQuery dataset where the Academic Observatory aggregated data lives.
    :param bq_ror_dataset_id: the id of the BigQuery dataset containing the ROR table.
    :param bq_settings_dataset_id: the id of the BigQuery settings dataset, which contains the country table.
    :param bq_oa_dashboard_dataset_id: the id of the BigQuery dataset where the tables produced by this workflow will be created.
    :param version: the dataset version published by this workflow. The Github Action pulls from a specific dataset
    version: https://github.com/The-Academic-Observatory/coki-oa-web/blob/develop/.github/workflows/build-on-data-update.yml#L68-L74.
    This is so that when breaking changes are made to the schema, the web application won't break.
    :param zenodo_host: the Zenodo hostname, can be changed to https://sandbox.zenodo.org for testing.
    :param github_conn_id: the Github Token Airflow Connection ID.
    :param zenodo_conn_id: the Zenodo Token Airflow Connection ID.
    :param start_date: the start date.
    :param schedule: the schedule interval.
    :param max_active_runs: the maximum number of DAG runs that can be run at once.
    :param retries: the number of times to retry a task.

    The figure below illustrates the data files produced by this workflow:
    .
    ├── data: data
    │   ├── index.json: used by the Cloudflare Worker search and filtering API.
    │   ├── country: individual entity statistics files for countries. Used to build each country page.
    │   │   ├── ALB.json
    │   │   ├── ARE.json
    │   │   └── ARG.json
    │   ├── country.json: used to create the country table. First 18 countries used to build first page of country table
    │   │                 and then this file is included in the public folder and downloaded by the client to enable the
    │   │                 other pages of the table to be displayed. Copied into public/data folder.
    │   ├── institution: individual entity statistics files for institutions. Used to build each institution page.
    │   │   ├── 05ykr0121.json
    │   │   ├── 05ym42410.json
    │   │   └── 05ynxx418.json
    │   ├── institution.json: used to create the institution table. First 18 institutions used to build first page of institution table
    │   │                     and then this file is included in the public folder and downloaded by the client to enable the
    │   │                     other pages of the table to be displayed. Copied into public/data folder.
    │   └── stats.json: global statistics, e.g. the minimum and maximum date for the dataset, when it was last updated etc.
    └── images:
        └── logos: country and institution logos.
            ├── country
            │   ├── md: medium logos displayed on country pages.
            │   │   ├── ALB.svg
            │   │   ├── ARE.svg
            │   │   └── ARG.svg
            │   └── sm: small logos displayed in country table.
            │       ├── ALB.svg
            │       ├── ARE.svg
            │       └── ARG.svg
            └── institution
                ├── lg: large logos used for social media cards.
                │   ├── 05ykr0121.png
                │   ├── 05ym42410.png
                │   └── 05ynxx418.png
                ├── md: medium logos displayed on institution pages.
                │   ├── 05ykr0121.jpg
                │   ├── 05ym42410.jpg
                │   └── 05ynxx418.jpg
                └── sm: small logos displayed in institution table.
                    ├── 05ykr0121.jpg
                    ├── 05ym42410.jpg
                    └── 05ynxx418.jpg
    """

    if entity_types is None:
        entity_types = ["country", "institution"]

    @dag(
        dag_id=dag_id,
        start_date=start_date,
        schedule=schedule,
        catchup=False,
        max_active_runs=max_active_runs,
        tags=["academic-observatory"],
        default_args={
            "owner": "airflow",
            "on_failure_callback": on_failure_callback,
            "retries": retries,
        },
    )
    def oa_dashboard_workflow():
        @task
        def fetch_release(**context):
            """Fetch a release"""
            snapshot_date = make_snapshot_date(**context)
            release = OaDashboardRelease(
                dag_id=dag_id,
                run_id=context["run_id"],
                snapshot_date=snapshot_date,
                input_project_id=cloud_workspace.input_project_id,
                output_project_id=cloud_workspace.output_project_id,
                bq_ror_dataset_id=bq_ror_dataset_id,
                bq_settings_dataset_id=bq_settings_dataset_id,
                bq_agg_dataset_id=bq_agg_dataset_id,
                bq_oa_dashboard_dataset_id=bq_oa_dashboard_dataset_id,
            )

            return release.to_dict()

        @task
        def create_dataset(**context):
            """Make BigQuery datasets."""

            bq_create_dataset(
                project_id=cloud_workspace.output_project_id,
                dataset_id=bq_oa_dashboard_dataset_id,
                location=cloud_workspace.data_location,
                description="The COKI Open Access Dashboard dataset",
            )

        @task
        def upload_institution_ids(release: dict, **context):
            """Upload the institution IDs to BigQuery"""

            release = OaDashboardRelease.from_dict(release)
            data = [{"ror_id": ror_id} for ror_id in INSTITUTION_IDS]
            success = bq_load_from_memory(
                release.institution_ids_table_id,
                data,
                schema_file_path=project_path("oa_dashboard_workflow", "schema", "institution_ids.json"),
            )
            set_task_state(success, "upload_institution_ids", release)

        @task
        def create_entity_tables(release: dict, **context):
            """Create the country and institution tables"""

            release = OaDashboardRelease.from_dict(release)
            results, queries = [], []

            # Query the country and institution aggregations
            for entity_type in entity_types:
                template_path = project_path("oa_dashboard_workflow", "sql", f"{entity_type}.sql.jinja2")
                sql = render_template(
                    template_path,
                    agg_table_id=release.observatory_agg_table_id(entity_type),
                    start_year=START_YEAR,
                    end_year=END_YEAR,
                    ror_table_id=release.ror_table_id,
                    country_table_id=release.country_table_id,
                    institution_ids_table_id=release.institution_ids_table_id,
                    inclusion_threshold=INCLUSION_THRESHOLD[entity_type],
                )
                dst_table_id = release.oa_dashboard_table_id(entity_type)
                queries.append((sql, dst_table_id))

            # Run queries, saving to BigQuery
            for sql, dst_table_id in queries:
                success = bq_create_table_from_query(sql=sql, table_id=dst_table_id)
                results.append(success)

            state = all(results)
            if not state:
                raise AirflowException("OaDashboardWorkflow.query failed")

        @task
        def add_wiki_descriptions(release: dict, entity_type: str, **context):
            """Download wiki descriptions and update indexes."""

            logging.info(f"add_wiki_descriptions: {entity_type}")
            release = OaDashboardRelease.from_dict(release)

            # Get entities to fetch descriptions for
            results = bq_run_query(
                f"SELECT DISTINCT wikipedia_url FROM {release.oa_dashboard_table_id(entity_type)} WHERE wikipedia_url IS NOT NULL AND TRIM(wikipedia_url) != ''"
            )
            wikipedia_urls = [result["wikipedia_url"] for result in results]

            # Fetch Wikipedia descriptions
            results = fetch_wikipedia_descriptions(wikipedia_urls)

            # Upload to BigQuery
            data = [{"url": wikipedia_url, "text": description} for wikipedia_url, description in results]
            desc_table_id = release.descriptions_table_id(entity_type)
            success = bq_load_from_memory(
                desc_table_id,
                data,
                schema_file_path=project_path("oa_dashboard_workflow", "schema", "descriptions.json"),
            )
            if not success:
                raise AirflowException(f"Uploading data to {desc_table_id} table failed")

            # Update entity table
            template_path = project_path("oa_dashboard_workflow", "sql", "update_descriptions.sql.jinja2")
            sql = render_template(
                template_path,
                entity_table_id=release.oa_dashboard_table_id(entity_type),
                descriptions_table_id=desc_table_id,
            )
            bq_run_query(sql)

        @task
        def download_assets(release: dict, **context):
            """Download assets"""

            # Download assets
            # They are unzipped in this particular order so that images-base overwrites any files in images
            release = OaDashboardRelease.from_dict(release)
            blob_names = ["images.zip", "images-base.zip"]
            for blob_name in blob_names:
                # Download asset zip
                file_path = os.path.join(release.download_folder, blob_name)
                gcs_download_blob(bucket_name=data_bucket, blob_name=blob_name, file_path=file_path)

                # Unzip into build
                unzip_folder_path = os.path.join(release.build_path, "images")
                with ZipFile(file_path) as zip_file:
                    zip_file.extractall(unzip_folder_path)  # Overwrites by default

        @task
        def download_institution_logos(release: dict, **context):
            """Download logos and update indexes."""

            logging.info(f"download_logos: institution")

            # Get entities to fetch descriptions for
            release = OaDashboardRelease.from_dict(release)
            entity_type = "institution"
            results = bq_run_query(f"SELECT id, url FROM {release.oa_dashboard_table_id(entity_type)}")
            entities = [(result["id"], result["url"]) for result in results]

            # Update logos
            data = fetch_institution_logos(release.build_path, entities)

            # Upload to BigQuery
            logos_table_id = release.logos_table_id(entity_type)
            success = bq_load_from_memory(
                logos_table_id,
                data,
                schema_file_path=project_path("oa_dashboard_workflow", "schema", "logos.json"),
            )
            if not success:
                raise AirflowException(f"Uploading data to {logos_table_id} table failed")

            # Update with entity table
            template_path = project_path("oa_dashboard_workflow", "sql", "update_logos.sql.jinja2")
            sql = render_template(
                template_path,
                entity_table_id=release.oa_dashboard_table_id(entity_type),
                logos_table_id=logos_table_id,
            )
            bq_run_query(sql)

            # Zip dataset
            shutil.make_archive(
                os.path.join(release.out_path, "images"), "zip", os.path.join(release.build_path, "images")
            )

        @task
        def export_tables(release: dict, **context):
            """Export and download the queried data"""

            # Fetch data
            release = OaDashboardRelease.from_dict(release)
            blob_prefix = gcs_blob_name_from_path(release.download_folder)

            results = []
            for entity_type in entity_types:
                destination_uri = f"gs://{cloud_workspace.download_bucket}/{blob_prefix}/{entity_type}-data-*.jsonl.gz"
                table_id = release.oa_dashboard_table_id(entity_type)
                success = bq_query_to_gcs(
                    query=f"SELECT * FROM {table_id} ORDER BY stats.p_outputs_open DESC",  # Uses a query to export data to make sure it is in the correct order
                    project_id=release.output_project_id,
                    destination_uri=destination_uri,
                )
                results.append(success)

            if not all(results):
                raise AirflowException("OaDashboardWorkflow.download failed")

        @task
        def download_data(release: dict, **context):
            """Download the queried data."""

            release = OaDashboardRelease.from_dict(release)
            blob_prefix = gcs_blob_name_from_path(release.download_folder)
            state = gcs_download_blobs(
                bucket_name=cloud_workspace.download_bucket,
                prefix=blob_prefix,
                destination_path=release.download_folder,
            )
            if not state:
                raise AirflowException("OaDashboardWorkflow.download failed")

        @task
        def make_draft_zenodo_version(release: dict, **context):
            """Make a draft Zenodo version of the dataset."""

            zenodo_token = get_airflow_connection_password(zenodo_conn_id)
            zenodo = Zenodo(host=zenodo_host, access_token=zenodo_token)
            make_draft_version(zenodo, conceptrecid)

        @task
        def build_datasets(release: dict, **context):
            """Transform the queried data into the final format for the open access website."""

            # Get versions
            zenodo_token = get_airflow_connection_password(zenodo_conn_id)
            zenodo = Zenodo(host=zenodo_host, access_token=zenodo_token)
            res = zenodo.get_versions(conceptrecid, all_versions=1)
            if res.status_code != 200:
                raise AirflowException(f"zenodo.get_versions status_code {res.status_code}")
            zenodo_versions = [
                ZenodoVersion(
                    pendulum.parse(version["created"]),
                    f"https://zenodo.org/record/{version['id']}/files/coki-oa-dataset.zip?download=1",
                )
                for version in res.json()
            ]

            # Save OA Dashboard dataset
            release = OaDashboardRelease.from_dict(release)
            build_data_path = os.path.join(release.build_path, "data")
            save_oa_dashboard_dataset(release.download_folder, build_data_path, entity_types, zenodo_versions)
            shutil.make_archive(os.path.join(release.out_path, "data"), "zip", os.path.join(release.build_path, "data"))

            # Save COKI Open Access Dataset
            coki_dataset_path = os.path.join(release.transform_folder, "coki-oa-dataset")
            save_zenodo_dataset(release.download_folder, coki_dataset_path, entity_types)
            shutil.make_archive(os.path.join(release.out_path, "coki-oa-dataset"), "zip", coki_dataset_path)

        @task
        def publish_zenodo_version(release: dict, **context):
            """Publish the new Zenodo version of the dataset."""

            release = OaDashboardRelease.from_dict(release)
            zenodo_token = get_airflow_connection_password(zenodo_conn_id)
            zenodo = Zenodo(host=zenodo_host, access_token=zenodo_token)
            res = zenodo.get_versions(conceptrecid, all_versions=0)
            if res.status_code != 200:
                raise AirflowException(f"zenodo.get_versions status_code {res.status_code}")
            draft = res.json()[0]
            draft_id = draft["id"]
            if draft["state"] != "unsubmitted":
                raise AirflowException(f"Latest version is not a draft: {draft_id}")

            file_path = os.path.join(release.out_path, "coki-oa-dataset.zip")
            publish_new_version(zenodo, draft_id, file_path)

        @task
        def upload_dataset(release: dict, **context):
            """Publish the dataset produced by this workflow."""

            # gcs_upload_file should always rewrite a new version of latest.zip if it exists
            # object versioning on the bucket will keep the previous versions
            release = OaDashboardRelease.from_dict(release)
            for file_name in ["data.zip", "images.zip"]:
                blob_name = f"{version}/{file_name}"
                file_path = os.path.join(release.out_path, file_name)
                gcs_upload_file(
                    bucket_name=data_bucket, blob_name=blob_name, file_path=file_path, check_blob_hash=False
                )

        @task
        def repository_dispatch(release: dict, **context):
            """Trigger a Github repository_dispatch to trigger new website builds."""

            token = get_airflow_connection_password(github_conn_id)
            event_types = ["data-update/develop", "data-update/staging", "data-update/production"]
            for event_type in event_types:
                trigger_repository_dispatch(
                    org="The-Academic-Observatory", repo_name="coki-oa-web", token=token, event_type=event_type
                )

        @task
        def cleanup_workflow(release: dict, **context):
            """Delete all files and folders associated with this release."""

            release = OaDashboardRelease.from_dict(release)
            cleanup(dag_id=dag_id, execution_date=context["logical_date"], workflow_folder=release.workflow_folder)

        # Define task connections
        task_doi_sensor = ExternalTaskSensor(
            task_id=f"{doi_dag_id}_sensor", external_dag_id=doi_dag_id, mode="reschedule"
        )
        task_check_dependencies = check_dependencies(airflow_conns=[github_conn_id, zenodo_conn_id])
        task_create_dataset = create_dataset()
        xcom_release = fetch_release()
        task_upload_institution_ids = upload_institution_ids(xcom_release)
        task_create_entity_tables = create_entity_tables(xcom_release)

        tasks_wiki = []
        for entity_type in entity_types:
            tasks_wiki.append(
                add_wiki_descriptions.override(task_id=f"add_wiki_descriptions_{entity_type}")(
                    xcom_release, entity_type
                )
            )

        task_download_assets = download_assets(xcom_release)
        task_download_institution_logos = download_institution_logos(xcom_release)
        task_export_tables = export_tables(xcom_release)
        task_download_data = download_data(xcom_release)
        task_make_draft_zenodo_version = make_draft_zenodo_version(xcom_release)
        task_build_datasets = build_datasets(xcom_release)
        task_publish_zenodo_version = publish_zenodo_version(xcom_release)
        task_upload_dataset = upload_dataset(xcom_release)
        task_repository_dispatch = repository_dispatch(xcom_release)
        task_cleanup_workflow = cleanup_workflow(xcom_release)

        chain(
            task_doi_sensor,
            task_check_dependencies,
            task_create_dataset,
            xcom_release,
            task_upload_institution_ids,
            task_create_entity_tables,
            *tasks_wiki,
            task_download_assets,
            task_download_institution_logos,
            task_export_tables,
            task_download_data,
            task_make_draft_zenodo_version,
            task_build_datasets,
            task_publish_zenodo_version,
            task_upload_dataset,
            task_repository_dispatch,
            task_cleanup_workflow,
        )

    return oa_dashboard_workflow()


def bq_query_to_gcs(*, query: str, project_id: str, destination_uri: str, location: str = "us") -> bool:
    """Run a BigQuery query and save the results on Google Cloud Storage.

    :param query: the query string.
    :param project_id: the Google Cloud project id.
    :param destination_uri: the Google Cloud Storage destination uri.
    :param location: the BigQuery dataset location.
    :return: the status of the job.
    """

    client = bigquery.Client()

    # Run query
    query_job: bigquery.QueryJob = client.query(query, location=location)
    query_job.result()

    # Create and run extraction job
    source_table_id = f"{project_id}.{query_job.destination.dataset_id}.{query_job.destination.table_id}"
    config = bigquery.ExtractJobConfig()
    config.destination_format = bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON
    config.compression = bigquery.Compression.GZIP
    extract_job: bigquery.ExtractJob = client.extract_table(
        source_table_id, destination_uri, job_config=config, location=location
    )
    extract_job.result()

    return query_job.state == "DONE" and extract_job.state == "DONE"


def save_oa_dashboard_dataset(
    download_folder: str, build_data_path: str, entity_types: List[str], zenodo_versions: List[ZenodoVersion]
):
    # Iterate over entity data files
    index = []
    stats_index = {}
    for entity_type in entity_types:
        entities_path = os.path.join(build_data_path, entity_type)
        os.makedirs(entities_path, exist_ok=True)
        entities = []

        data_path = data_file_pattern(download_folder, entity_type)
        for entity in yield_data_glob(data_path):
            # Save entity file as JSON
            entity_id = entity["id"]
            output_path = os.path.join(entities_path, f"{entity_id}.json")
            save_json(output_path, entity)

            # Select subset of fields for index and stats calculations
            subset = oa_dashboard_subset(entity)
            entities.append(subset)

        # Save index for entity type
        path = os.path.join(build_data_path, f"{entity_type}.json")
        save_json(path, entities)

        stats_index[entity_type] = make_entity_stats(entities)
        index += entities

    # Save index
    index_path = os.path.join(build_data_path, "index.json")
    save_json(index_path, index)

    # Make stats from index
    last_updated = zenodo_versions[0].release_date.format("D MMMM YYYY")
    country_stats = stats_index["country"]
    institution_stats = stats_index["institution"]
    stats = Stats(START_YEAR, END_YEAR, last_updated, zenodo_versions, country_stats, institution_stats)
    output_path = os.path.join(build_data_path, "stats.json")
    save_json(output_path, stats.to_dict())
    logging.info(f"Saved stats data")


def save_zenodo_dataset(download_folder: str, dataset_path: str, entity_types: List[str]):
    """Save the COKI Open Access Dataset to a zip file.

    :param download_folder: the path where the downloaded data files can be found.
    :param dataset_path: the path to the folder where the dataset should be saved.
    :param entity_types: the entity types.
    :return: None.
    """

    os.makedirs(dataset_path, exist_ok=True)

    # For each entity type, save the jsonl file
    counts = {entity_type: 0 for entity_type in entity_types}
    for entity_type in entity_types:
        out_path = os.path.join(dataset_path, f"{entity_type}.jsonl")
        with open(out_path, mode="w") as out_file:
            with jsonlines.Writer(out_file) as writer:
                data_path = data_file_pattern(download_folder, entity_type)
                for entity in yield_data_glob(data_path):
                    subset = zenodo_subset(entity)
                    writer.write(subset)
                    counts[entity_type] += 1

    # Save README
    file_path = os.path.join(dataset_path, "README.md")
    template = Template(README, keep_trailing_newline=True)
    rendered = template.render(
        year=pendulum.now().year, n_countries=counts["country"], n_institutions=counts["institution"]
    )
    with open(file_path, mode="w") as f:
        f.write(rendered)


def oa_dashboard_subset(item: Dict) -> Dict:
    subset_spec = {
        "id": "id",
        "name": "name",
        "logo_sm": "logo_sm",
        "entity_type": "entity_type",
        "region": "region",
        "subregion": "subregion",
        "country_code": Coalesce("country_code", default=SKIP),
        "country_name": Coalesce("country_name", default=SKIP),
        "institution_type": Coalesce("institution_type", default=SKIP),
        "acronyms": "acronyms",
        "stats": {
            "n_outputs": "stats.n_outputs",
            "n_outputs_open": "stats.n_outputs_open",
            "n_outputs_black": "stats.n_outputs_black",
            "p_outputs_open": "stats.p_outputs_open",
            "p_outputs_publisher_open_only": "stats.p_outputs_publisher_open_only",
            "p_outputs_both": "stats.p_outputs_both",
            "p_outputs_other_platform_open_only": "stats.p_outputs_other_platform_open_only",
            "p_outputs_closed": "stats.p_outputs_closed",
            "p_outputs_black": "stats.p_outputs_black",
        },
    }

    return glom(item, subset_spec)


def zenodo_subset(item: Dict):
    subset_spec = {
        "id": "id",
        "name": "name",
        "region": "region",
        "subregion": "subregion",
        "country_code": Coalesce("country_code", default=SKIP),
        "country_name": Coalesce("country_name", default=SKIP),
        "institution_type": Coalesce("institution_type", default=SKIP),
        "start_year": "start_year",
        "end_year": "end_year",
        "acronyms": "acronyms",
        "stats": "stats",
        "years": "years",
    }

    return glom(item, subset_spec)


###############
# Data classes
###############


@dataclasses.dataclass
class ZenodoVersion:
    release_date: pendulum.DateTime
    download_url: str

    def to_dict(self) -> Dict:
        return {"release_date": self.release_date.strftime("%Y-%m-%d"), "download_url": self.download_url}


@dataclasses.dataclass
class Histogram:
    data: List[int]
    bins: List[float]

    def to_dict(self) -> Dict:
        return {"data": self.data, "bins": self.bins}


@dataclasses.dataclass
class EntityHistograms:
    p_outputs_open: Histogram
    n_outputs: Histogram
    n_outputs_open: Histogram

    def to_dict(self) -> Dict:
        return {
            "p_outputs_open": self.p_outputs_open.to_dict(),
            "n_outputs": self.n_outputs.to_dict(),
            "n_outputs_open": self.n_outputs_open.to_dict(),
        }


@dataclasses.dataclass
class EntityStats:
    n_items: int
    min: Dict
    max: Dict
    median: Dict
    histograms: EntityHistograms

    def to_dict(self) -> Dict:
        return {
            "n_items": self.n_items,
            "min": self.min,
            "max": self.max,
            "median": self.median,
            "histograms": self.histograms.to_dict(),
        }


@dataclasses.dataclass
class Stats:
    start_year: int
    end_year: int
    last_updated: str
    zenodo_versions: List[ZenodoVersion]
    country: EntityStats
    institution: EntityStats

    def to_dict(self) -> Dict:
        return {
            "start_year": self.start_year,
            "end_year": self.end_year,
            "last_updated": self.last_updated,
            "zenodo_versions": [z.to_dict() for z in self.zenodo_versions],
            "country": self.country.to_dict(),
            "institution": self.institution.to_dict(),
        }


#####################
# Helper functions
#####################


def save_json(path: str, data: Union[Dict, List]):
    """Save data to JSON.

    :param path: the output path.
    :param data: the data to save.
    :return: None.
    """

    with open(path, mode="w") as f:
        json.dump(data, f, separators=(",", ":"))


def data_file_pattern(download_folder: str, entity_type: str):
    return os.path.join(download_folder, f"{entity_type}-data-*.jsonl.gz")


def yield_data_glob(pattern: str) -> List[Dict]:
    """Load country or institution data files into a Pandas DataFrame.

    :param pattern: the file path including a glob pattern.
    :return: the list of dicts.
    """

    file_paths = sorted(glob.glob(pattern))

    for file_path in file_paths:
        for entity in yield_jsonl(file_path):
            yield entity


def make_entity_stats(entities: List[Dict]) -> EntityStats:
    """Calculate stats for entities.

    :param entities: a list of entities.
    :return: the entity stats object.
    """

    p_outputs_open = np.array([entity["stats"]["p_outputs_open"] for entity in entities])
    n_outputs = np.array([entity["stats"]["n_outputs"] for entity in entities])
    n_outputs_open = np.array([entity["stats"]["n_outputs_open"] for entity in entities])

    # Make median, min and max values
    stats_median = dict(p_outputs_open=statistics.median(p_outputs_open))
    stats_min = dict(
        p_outputs_open=math.floor(float(np.min(p_outputs_open))),
        n_outputs=int(np.min(n_outputs)),
        n_outputs_open=int(np.min(n_outputs_open)),
    )
    stats_max = dict(
        p_outputs_open=math.ceil(float(np.max(p_outputs_open))),
        n_outputs=int(np.max(n_outputs)),
        n_outputs_open=int(np.max(n_outputs_open)),
    )

    # Make histograms
    data, bins = np.histogram(p_outputs_open, bins="auto")
    hist_p_outputs_open = Histogram(data.tolist(), bins.tolist())

    # Make log10 shifted histograms. Add 1 to values to make consistent with UI
    data, bins = np.histogram(np.log10(n_outputs + 1), bins="auto")
    hist_n_outputs = Histogram(data.tolist(), bins.tolist())

    data, bins = np.histogram(np.log10(n_outputs_open + 1), bins="auto")
    hist_n_outputs_open = Histogram(data.tolist(), bins.tolist())

    return EntityStats(
        n_items=len(entities),
        min=stats_min,
        max=stats_max,
        median=stats_median,
        histograms=EntityHistograms(
            p_outputs_open=hist_p_outputs_open, n_outputs=hist_n_outputs, n_outputs_open=hist_n_outputs_open
        ),
    )


#################
# Logo fetching
#################


def make_logo_url(*, entity_type: str, entity_id: str, size: str, fmt: str) -> str:
    """Make a logo url.

    :param entity_type: the entity entity_type: country or institution.
    :param entity_id: the entity id.
    :param size: the size of the logo: s or l.
    :param fmt: the format of the logo.
    :return: the logo url.
    """

    return f"logos/{entity_type}/{size}/{entity_id}.{fmt}"


def fetch_institution_logo(ror_id: str, url: str, size: str, width: int, fmt: str, build_path: str) -> Tuple[str, str]:
    """Get the path to the logo for an institution.
    If the logo does not exist in the build path yet, download from the Clearbit Logo API tool.
    If the logo does not exist and failed to download, the path will default to "unknown.svg".

    :param ror_id: the institution's ROR id
    :param url: the URL of the company domain + suffix e.g. spotify.com
    :param size: the image size of the small logo for tables etc.
    :param width: the width of the image.
    :param fmt: the image format.
    :param build_path: the build path for files of this workflow
    :return: The ROR id and relative path (from build path) to the logo
    """

    logo_path = "unknown.svg"
    size_folder = os.path.join(build_path, "images", "logos", "institution", size)
    os.makedirs(size_folder, exist_ok=True)
    file_path = os.path.join(size_folder, f"{ror_id}.{fmt}")
    if not os.path.isfile(file_path):
        logging.debug(
            f"fetch_institution_logo: downloading logo company_url={url}, file_path={file_path}, size={width}, fmt={fmt}"
        )
        clearbit_download_logo(company_url=url, file_path=file_path, size=width, fmt=fmt)
    if os.path.isfile(file_path):
        logging.debug(f"fetch_institution_logo: {file_path} exists, skipping download")
        logo_path = make_logo_url(entity_type="institution", entity_id=ror_id, size=size, fmt=fmt)

    return ror_id, logo_path


def clean_url(url: str) -> str:
    """Remove path and query from URL.

    :param url: the url.
    :return: the cleaned url.
    """

    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}/"


def fetch_institution_logos(build_path: str, entities: List[Tuple[str, str]]) -> List[Dict]:
    """Update the index with logos, downloading logos if they don't exist.

    :param build_path: the path to the build folder.
    :param entities: the entities to process consisting of their id and url.
    :return: None.
    """

    # Get the institution logo and the path to the logo image
    logging.info("Downloading logos using Clearbit")
    total = len(entities)

    results = {
        entity_id: {"id": entity_id, "logo_sm": "unknown.svg", "logo_md": "unknown.svg", "logo_lg": "unknown.svg"}
        for entity_id, url in entities
    }
    for size, width, fmt in [("sm", 32, "jpg"), ("md", 128, "jpg"), ("lg", 532, "png")]:
        logging.info(f"Downloading logos: size={size}, width={width}, fmt={fmt}")

        # Create jobs
        futures = []
        with ThreadPoolExecutor() as executor:
            for entity_id, url in entities:
                if url:
                    url = clean_url(url)
                    futures.append(
                        executor.submit(fetch_institution_logo, entity_id, url, size, width, fmt, build_path)
                    )

            # Wait for results
            n_downloaded = 0
            for completed in as_completed(futures):
                entity_id, logo_path = completed.result()
                results[entity_id][f"logo_{size}"] = logo_path
                n_downloaded += 1

                # Print progress
                p_progress = n_downloaded / total * 100
                if n_downloaded % 100 == 0:
                    logging.info(f"Downloading logos {n_downloaded}/{total}: {p_progress:.2f}%")

        logging.info("Finished downloading logos")

    return list(results.values())
