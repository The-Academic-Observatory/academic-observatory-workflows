# Copyright 2021 Curtin University
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
import datetime
import glob
import json
import logging
import math
import os
import os.path
import shutil
import statistics
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import field
from typing import Tuple, Union, Optional, List, Dict
from urllib.parse import urlparse
from zipfile import ZipFile

import google.cloud.bigquery as bigquery
import jsonlines
import nltk
import numpy as np
import pandas as pd
import pendulum
from airflow.exceptions import AirflowException
from airflow.sensors.external_task import ExternalTaskSensor
from jinja2 import Template
from pandas.api.types import is_string_dtype

from academic_observatory_workflows.clearbit import clearbit_download_logo
from academic_observatory_workflows.config import Tag
from academic_observatory_workflows.github import trigger_repository_dispatch
from academic_observatory_workflows.institutions import INSTITUTION_IDS
from academic_observatory_workflows.wikipedia import fetch_wiki_descriptions
from academic_observatory_workflows.zenodo import Zenodo, make_draft_version, publish_new_version
from observatory.platform.airflow import get_airflow_connection_password
from observatory.platform.bigquery import (
    bq_sharded_table_id,
    bq_select_table_shard_dates,
    bq_table_id,
)
from observatory.platform.files import load_jsonl, save_jsonl_gz
from observatory.platform.gcs import (
    gcs_blob_name_from_path,
    gcs_download_blobs,
    gcs_upload_file,
    gcs_download_blob,
    gcs_blob_uri,
)
from observatory.platform.observatory_config import CloudWorkspace
from observatory.platform.workflows.workflow import Workflow, SnapshotRelease, cleanup, make_snapshot_date

# The minimum number of outputs before including an entity in the analysis
PERCENTAGE_FIELD_KEYS = [
    ("outputs_open", "n_outputs"),
    ("outputs_both", "n_outputs"),
    ("outputs_closed", "n_outputs"),
    ("outputs_publisher_open", "n_outputs"),
    ("outputs_publisher_open_only", "n_outputs"),
    ("outputs_other_platform_open", "n_outputs"),
    ("outputs_other_platform_open_only", "n_outputs"),
    ("outputs_oa_journal", "n_outputs_publisher_open"),
    ("outputs_hybrid", "n_outputs_publisher_open"),
    ("outputs_no_guarantees", "n_outputs_publisher_open"),
    ("outputs_preprint", "n_outputs_other_platform_open"),
    ("outputs_domain", "n_outputs_other_platform_open"),
    ("outputs_institution", "n_outputs_other_platform_open"),
    ("outputs_public", "n_outputs_other_platform_open"),
    ("outputs_other_internet", "n_outputs_other_platform_open"),
]
INCLUSION_THRESHOLD = {"country": 15, "institution": 1000}
MAX_REPOSITORIES = 200
START_YEAR = 2000
END_YEAR = pendulum.now().year - 1
WIKI_MAX_TITLES = 20  # Set the number of titles for which wiki descriptions are retrieved at once, the API can return max 20 extracts.

# Queries that pull data
INSTITUTION_INDEX_QUERY = """
SELECT
  ror.id,
  ror.name,
  (SELECT * from ror.links LIMIT 1) AS url,
  ror.wikipedia_url,
  country.alpha3 as country_code,
  country.wikipedia_name as country_name,
  country.subregion as subregion,
  country.region as region,
  (SELECT * from ror.types LIMIT 1) AS institution_type,
  ror.acronyms
FROM
  `{ror_table_id}` as ror
  LEFT OUTER JOIN `{country_table_id}` as country ON ror.country.country_code = country.alpha2
ORDER BY name ASC
"""

COUNTRY_INDEX_QUERY = """
SELECT
  country.alpha3 as id,
  country.wikipedia_name as name,
  country.wikipedia_url,
  country.subregion as subregion,
  country.region as region,
  country.alpha2 as alpha2 -- used for country flags
FROM
  `{country_table_id}` as country
ORDER BY name ASC
"""

DATA_QUERY = """
SELECT
  agg.id,
  agg.time_period as year,
  agg.citations.openalex.total_citations as n_citations,  
  agg.total_outputs as n_outputs,
  
  -- COKI OA Categories
  agg.coki.oa.coki.open.total AS n_outputs_open,
  agg.coki.oa.coki.publisher.total AS n_outputs_publisher_open,
  agg.coki.oa.coki.publisher_only.total AS n_outputs_publisher_open_only,
  agg.coki.oa.coki.both.total AS n_outputs_both,
  agg.coki.oa.coki.other_platform.total AS n_outputs_other_platform_open,
  agg.coki.oa.coki.other_platform_only.total AS n_outputs_other_platform_open_only,
  agg.coki.oa.coki.closed.total AS n_outputs_closed,
  
  -- Publisher Open Categories
  agg.coki.oa.coki.publisher_categories.oa_journal.total AS n_outputs_oa_journal,
  agg.coki.oa.coki.publisher_categories.hybrid.total AS n_outputs_hybrid,
  agg.coki.oa.coki.publisher_categories.no_guarantees.total AS n_outputs_no_guarantees,
  
  -- Other Platform Open Categories
  agg.coki.oa.coki.other_platform_categories.preprint.total AS n_outputs_preprint,
  agg.coki.oa.coki.other_platform_categories.domain.total AS n_outputs_domain,
  agg.coki.oa.coki.other_platform_categories.institution.total AS n_outputs_institution,
  agg.coki.oa.coki.other_platform_categories.public.total AS n_outputs_public,
  agg.coki.oa.coki.other_platform_categories.aggregator.total + agg.coki.oa.coki.other_platform_categories.other_internet.total + agg.coki.oa.coki.other_platform_categories.unknown.total AS n_outputs_other_internet, 

  agg.coki.repositories
FROM
  `{agg_table_id}` as agg
WHERE agg.time_period >= {start_year} AND agg.time_period <= {end_year}
ORDER BY year DESC
"""

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


class OaWebRelease(SnapshotRelease):
    def __init__(self, *, dag_id: str, run_id: str, snapshot_date: pendulum.DateTime):
        """Create an OaWebRelease instance.

        :param dag_id: the dag id.
        :param run_id: the DAG run id.
        :param snapshot_date: the release date.
        :param zenodo: the zenodo instance.
        """

        super().__init__(dag_id=dag_id, run_id=run_id, snapshot_date=snapshot_date)

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


class OaWebWorkflow(Workflow):
    ROR_FILE = "ror.jsonl.gz"
    COUNTRY_INDEX_FILE = "country-index.jsonl.gz"
    INSTITUTION_INDEX_FILE = "institution-index.jsonl.gz"

    """The OaWebWorkflow generates data files for the COKI Open Access Dashboard.

    The figure below illustrates the generated data and notes about what each file is used for.
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

    def __init__(
        self,
        *,
        dag_id: str,
        cloud_workspace: CloudWorkspace,
        data_bucket: str,
        conceptrecid: int,
        doi_dag_id: str = "doi",
        table_names: List[str] = None,
        bq_agg_dataset_id: str = "observatory",
        bq_ror_dataset_id: str = "ror",
        bq_settings_dataset_id: str = "settings",
        version: str = "v9",
        zenodo_host: str = "https://zenodo.org",
        github_conn_id="oa_web_github_token",
        zenodo_conn_id="oa_web_zenodo_token",
        start_date: Optional[pendulum.DateTime] = pendulum.datetime(2021, 5, 2),
        schedule: Optional[str] = "@weekly",
    ):
        """Create the OaWebWorkflow.

        :param dag_id: the DAG id.
        :param cloud_workspace: The CloudWorkspace.
        :param data_bucket: the Google Cloud Storage bucket where image data should be stored.
        :param conceptrecid: the Zenodo Concept Record ID for the COKI Open Access Dataset. The Concept Record ID is
        the last set of numbers from the Concept DOI.
        :param doi_dag_id: the DAG id to wait for.
        :param table_names: the table names.
        :param bq_agg_dataset_id: the id of the dataset where the Academic Observatory aggregated data lives.
        :param bq_ror_dataset_id: the id of the dataset containing the ROR table.
        :param bq_settings_dataset_id: the id of the settings dataset, which contains the country table.
        :param version: the dataset version published by this workflow. The Github Action pulls from a specific dataset
        version: https://github.com/The-Academic-Observatory/coki-oa-web/blob/develop/.github/workflows/build-on-data-update.yml#L68-L74.
        This is so that when breaking changes are made to the schema, the web application won't break.
        :param zenodo_host: the Zenodo hostname, can be changed to https://sandbox.zenodo.org for testing.
        :param github_conn_id: the Github Token Airflow Connection ID.
        :param zenodo_conn_id: the Zenodo Token Airflow Connection ID.
        :param start_date: the start date.
        :param schedule: the schedule interval.
        """

        if table_names is None:
            table_names = ["country", "institution"]

        super().__init__(
            dag_id=dag_id,
            start_date=start_date,
            schedule=schedule,
            catchup=False,
            airflow_conns=[github_conn_id, zenodo_conn_id],
            tags=[Tag.academic_observatory],
        )
        self.cloud_workspace = cloud_workspace
        self.input_project_id = cloud_workspace.input_project_id
        self.output_project_id = cloud_workspace.output_project_id
        self.data_bucket = data_bucket
        self.bq_agg_dataset_id = bq_agg_dataset_id
        self.bq_ror_dataset_id = bq_ror_dataset_id
        self.bq_settings_dataset_id = bq_settings_dataset_id
        self.table_names = table_names
        self.version = version
        self.conceptrecid = conceptrecid
        self.zenodo_host = zenodo_host

        self.github_conn_id = github_conn_id
        self.zenodo_conn_id = zenodo_conn_id
        self.zenodo: Optional[Zenodo] = None

        self.add_operator(
            ExternalTaskSensor(task_id=f"{doi_dag_id}_sensor", external_dag_id=doi_dag_id, mode="reschedule")
        )
        self.add_setup_task(self.check_dependencies)
        self.add_task(self.query)
        self.add_task(self.download)
        self.add_task(self.make_draft_zenodo_version)
        self.add_task(self.download_assets)
        self.add_task(self.preprocess_data)
        self.add_task(self.build_indexes)
        self.add_task(self.download_logos)
        self.add_task(self.download_wiki_descriptions)
        self.add_task(self.build_datasets)
        self.add_task(self.publish_zenodo_version)
        self.add_task(self.upload_dataset)
        self.add_task(self.repository_dispatch)
        self.add_task(self.cleanup)

    ######################################
    # Airflow tasks
    ######################################

    def make_release(self, **kwargs) -> OaWebRelease:
        """Make release instances. The release is passed as an argument to the function (TelescopeFunction) that is
        called in 'task_callable'.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are
        passed to this argument.
        :return: A list of OaWebRelease instances
        """

        # Make Zenodo instance
        zenodo_token = get_airflow_connection_password(self.zenodo_conn_id)
        self.zenodo = Zenodo(host=self.zenodo_host, access_token=zenodo_token)

        snapshot_date = make_snapshot_date(**kwargs)
        return OaWebRelease(
            dag_id=self.dag_id,
            run_id=kwargs["run_id"],
            snapshot_date=snapshot_date,
        )

    def query(self, release: OaWebRelease, **kwargs):
        """Fetch the data for each table.

        :param release: the release instance.
        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are
        passed to this argument.
        :return: None.
        """

        results = []
        queries = []
        blob_prefix = gcs_blob_name_from_path(release.download_folder)

        # Query the country and institution aggregations
        for table_name in self.table_names:
            # Aggregate release dates
            table_id = bq_table_id(self.input_project_id, self.bq_agg_dataset_id, table_name)
            agg_snapshot_date = bq_select_table_shard_dates(
                table_id=table_id,
                end_date=release.snapshot_date,
            )[0]
            agg_table_id = bq_sharded_table_id(
                self.input_project_id, self.bq_agg_dataset_id, table_name, agg_snapshot_date
            )

            # Fetch data
            destination_uri = f"gs://{self.cloud_workspace.download_bucket}/{blob_prefix}/{table_name}-data-*.jsonl.gz"
            query = (
                DATA_QUERY.format(
                    agg_table_id=agg_table_id,
                    start_year=START_YEAR,
                    end_year=END_YEAR,
                ),
            )
            queries.append((query, destination_uri))

        # Query ROR table
        ror_table_id = bq_table_id(self.input_project_id, self.bq_ror_dataset_id, "ror")
        ror_snapshot_date = bq_select_table_shard_dates(
            table_id=ror_table_id,
            end_date=release.snapshot_date,
        )[0]
        ror_table_id = bq_sharded_table_id(self.input_project_id, self.bq_ror_dataset_id, "ror", ror_snapshot_date)
        country_table_id = bq_table_id(self.input_project_id, self.bq_settings_dataset_id, "country")

        # Query institution index table
        destination_uri = gcs_blob_uri(
            self.cloud_workspace.download_bucket, f"{blob_prefix}/{self.INSTITUTION_INDEX_FILE}"
        )
        queries.append(
            (
                INSTITUTION_INDEX_QUERY.format(
                    ror_table_id=ror_table_id,
                    country_table_id=country_table_id,
                ),
                destination_uri,
            )
        )

        # Query the country index table
        destination_uri = gcs_blob_uri(self.cloud_workspace.download_bucket, f"{blob_prefix}/{self.COUNTRY_INDEX_FILE}")
        queries.append(
            (
                COUNTRY_INDEX_QUERY.format(
                    country_table_id=country_table_id,
                ),
                destination_uri,
            )
        )

        # Run queries, saving results to Google Cloud Storage
        for (query, destination_uri) in queries:
            success = bq_query_to_gcs(
                query=query,
                project_id=self.output_project_id,
                destination_uri=destination_uri,
            )
            results.append(success)

        state = all(results)
        if not state:
            raise AirflowException("OaWebWorkflow.query failed")

    def download(self, release: OaWebRelease, **kwargs):
        """Download the queried data.

        :param release: the release instance.
        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are
        passed to this argument.
        :return: None.
        """

        blob_prefix = gcs_blob_name_from_path(release.download_folder)
        state = gcs_download_blobs(
            bucket_name=self.cloud_workspace.download_bucket,
            prefix=blob_prefix,
            destination_path=release.download_folder,
        )
        if not state:
            raise AirflowException("OaWebWorkflow.download failed")

    def make_draft_zenodo_version(self, release: OaWebRelease, **kwargs):
        """Make a draft Zenodo version of the dataset.

        :param release: the release instance.
        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are
        passed to this argument.
        :return: None.
        """

        make_draft_version(self.zenodo, self.conceptrecid)

    def download_assets(self, release: OaWebRelease, **kwargs):
        """Download assets.

        :param release: the release instance.
        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are
        passed to this argument.
        :return: None.
        """

        # Download assets
        # They are unzipped in this particular order so that images-base overwrites any files in images
        blob_names = ["images.zip", "images-base.zip"]
        for blob_name in blob_names:
            # Download asset zip
            file_path = os.path.join(release.download_folder, blob_name)
            gcs_download_blob(bucket_name=self.data_bucket, blob_name=blob_name, file_path=file_path)

            # Unzip into build
            unzip_folder_path = os.path.join(release.build_path, "images")
            with ZipFile(file_path) as zip_file:
                zip_file.extractall(unzip_folder_path)  # Overwrites by default

    def preprocess_data(self, release: OaWebRelease, **kwargs):
        """Preprocess data.

        :param release: the release instance.
        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are
        passed to this argument.
        :return: None.
        """

        #################
        # Country
        #################

        for category in self.table_names:
            logging.info(f"preprocess_data: {category}")

            # Load and preprocess data
            data_path = os.path.join(release.download_folder, f"{category}-data-*.jsonl.gz")
            data = load_data_glob(data_path)
            df_data = pd.DataFrame(data)
            preprocess_data_df(category, df_data)

            # Save to intermediate path
            data_path = os.path.join(release.intermediate_path, f"{category}-data.jsonl.gz")
            records = df_data.to_dict("records")
            save_jsonl_gz(data_path, records)

    def build_indexes(self, release: OaWebRelease, **kwargs):
        """Build unique country and institution indexes.

        :param release: the release instance.
        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are
        passed to this argument.
        :return: None.
        """

        for category in self.table_names:
            logging.info(f"build_indexes: {category}")

            # Load downloaded index
            index_name = f"{category}-index.jsonl.gz"
            index_path = os.path.join(release.download_folder, index_name)
            df_index = load_data(index_path)
            preprocess_index_df(category, df_index)

            # Load data file
            data_path = os.path.join(release.intermediate_path, f"{category}-data.jsonl.gz")
            df_data = load_data(data_path)

            # Aggregate data file
            df_index = make_index_df(category, df_index, df_data)

            logging.info(f"Total {category} entities: {len(df_index)}")

            # Save index to intermediate
            index_path = os.path.join(release.intermediate_path, index_name)
            rows: List[Dict] = df_index.to_dict("records")
            save_jsonl_gz(index_path, rows)

    def download_logos(self, release: OaWebRelease, **kwargs):
        """Download logos and update indexes.

        :param release: the release.
        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are
        passed to this argument.
        :return: None.
        """

        for category in self.table_names:
            logging.info(f"download_logos: {category}")

            # Load index
            index_path = os.path.join(release.intermediate_path, f"{category}-index.jsonl.gz")
            df_index = load_data(index_path)

            # Update logos
            df_index = update_index_with_logos(release.build_path, category, df_index)

            # Save updated index
            rows: List[Dict] = df_index.to_dict("records")
            save_jsonl_gz(index_path, rows)

    def download_wiki_descriptions(self, release: OaWebRelease, **kwargs):
        """Download wiki descriptions and update indexes.

        :param release: the release instance.
        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are
        passed to this argument.
        :return: None.
        """

        for category in self.table_names:
            logging.info(f"download_wiki_descriptions: {category}")

            # Load index
            index_path = os.path.join(release.intermediate_path, f"{category}-index.jsonl.gz")
            df_index = load_data(index_path)

            # Update logos
            df_index = update_index_with_wiki_descriptions(df_index)

            # Save updated index
            rows: List[Dict] = df_index.to_dict("records")
            save_jsonl_gz(index_path, rows)

    def build_datasets(self, release: OaWebRelease, **kwargs):
        """Transform the queried data into the final format for the open access website.

        :param release: the release instance.
        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are
        passed to this argument.
        :return: None.
        """

        # Get versions
        res = self.zenodo.get_versions(self.conceptrecid, all_versions=1)
        if res.status_code != 200:
            raise AirflowException(f"zenodo.get_versions status_code {res.status_code}")
        versions = res.json()

        # Make required folders
        entity_index = {category: [] for category in self.table_names}
        build_data_path = os.path.join(release.build_path, "data")
        os.makedirs(build_data_path, exist_ok=True)
        for category in self.table_names:
            logging.info(f"Transforming {category} entity")

            # Load index
            index_path = os.path.join(release.intermediate_path, f"{category}-index.jsonl.gz")
            df_index = load_data(index_path)

            data_path = os.path.join(release.intermediate_path, f"{category}-data.jsonl.gz")
            df_data = load_data(data_path)

            # Make index table
            entities = make_entities(category, df_index, df_data)
            entity_index[category] = entities

            # Save index
            index_path = os.path.join(build_data_path, f"{category}.json")
            data = make_index(category, entities)
            save_json(index_path, data)

            # Save entities
            entities_path = os.path.join(build_data_path, category)
            save_entities(entities_path, entities)
            logging.info(f"Saved transformed {category} entity")

        # Unwrap lists
        countries = entity_index["country"]
        institutions = entity_index["institution"]

        # Save full index
        index_path = os.path.join(build_data_path, f"index.json")
        data = make_index("country", countries) + make_index("institution", institutions)
        save_json(index_path, data)

        # Save COKI Open Access Dataset
        coki_dataset_path = os.path.join(release.transform_folder, "coki-oa-dataset")
        save_coki_oa_dataset(coki_dataset_path, countries, institutions)
        shutil.make_archive(os.path.join(release.out_path, "coki-oa-dataset"), "zip", coki_dataset_path)

        # Make stats
        zenodo_versions = [
            ZenodoVersion(
                pendulum.parse(version["created"]),
                f"https://zenodo.org/record/{version['id']}/files/coki-oa-dataset.zip?download=1",
            )
            for version in versions
        ]
        last_updated = zenodo_versions[0].release_date.format("D MMMM YYYY")
        country_stats = make_entity_stats(countries)
        institution_stats = make_entity_stats(institutions)
        stats = Stats(START_YEAR, END_YEAR, last_updated, zenodo_versions, country_stats, institution_stats)
        stats_path = os.path.join(release.build_path, "data")
        save_stats(stats_path, stats)
        logging.info(f"Saved stats data")

        # Zip data and images
        folders = ["data", "images"]
        for folder_name in folders:
            shutil.make_archive(
                os.path.join(release.out_path, folder_name), "zip", os.path.join(release.build_path, folder_name)
            )

    def publish_zenodo_version(self, release: OaWebRelease, **kwargs):
        """Publish the new Zenodo version of the dataset.

        :param release: the release instance.
        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are
        passed to this argument.
        :return: None.
        """

        res = self.zenodo.get_versions(self.conceptrecid, all_versions=0)
        if res.status_code != 200:
            raise AirflowException(f"zenodo.get_versions status_code {res.status_code}")
        draft = res.json()[0]
        draft_id = draft["id"]
        if draft["state"] != "unsubmitted":
            raise AirflowException(f"Latest version is not a draft: {draft_id}")

        file_path = os.path.join(release.out_path, "coki-oa-dataset.zip")
        publish_new_version(self.zenodo, draft_id, file_path)

    def upload_dataset(self, release: OaWebRelease, **kwargs):
        """Publish the dataset produced by this workflow.

        :param release: the release instance.
        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are
        passed to this argument.
        :return: None.
        """

        # gcs_upload_file should always rewrite a new version of latest.zip if it exists
        # object versioning on the bucket will keep the previous versions
        for file_name in ["data.zip", "images.zip"]:
            blob_name = f"{self.version}/{file_name}"
            file_path = os.path.join(release.out_path, file_name)
            gcs_upload_file(
                bucket_name=self.data_bucket, blob_name=blob_name, file_path=file_path, check_blob_hash=False
            )

    def repository_dispatch(self, release: OaWebRelease, **kwargs):
        """Trigger a Github repository_dispatch to trigger new website builds.

        :param release: the release instance.
        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are
        passed to this argument.
        :return: None.
        """

        token = get_airflow_connection_password(self.github_conn_id)
        event_types = ["data-update/develop", "data-update/staging", "data-update/production"]
        for event_type in event_types:
            trigger_repository_dispatch(
                org="The-Academic-Observatory", repo_name="coki-oa-web", token=token, event_type=event_type
            )

    def cleanup(self, release: OaWebRelease, **kwargs):
        """Delete all files and folders associated with this release.

        :param release: the release instance.
        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are
        passed to this argument.
        :return: None.
        """

        cleanup(dag_id=self.dag_id, execution_date=kwargs["execution_date"], workflow_folder=release.workflow_folder)


###############
# Data classes
###############


@dataclasses.dataclass
class PublicationStats:
    # Number fields
    n_citations: int = None
    n_outputs: int = None
    n_outputs_open: int = None
    n_outputs_publisher_open: int = None
    n_outputs_publisher_open_only: int = None
    n_outputs_both: int = None
    n_outputs_other_platform_open: int = None
    n_outputs_other_platform_open_only: int = None
    n_outputs_closed: int = None
    n_outputs_oa_journal: int = None
    n_outputs_hybrid: int = None
    n_outputs_no_guarantees: int = None
    n_outputs_preprint: int = None
    n_outputs_domain: int = None
    n_outputs_institution: int = None
    n_outputs_public: int = None
    n_outputs_other_internet: int = None

    # Percentage fields
    p_outputs_open: float = None
    p_outputs_publisher_open: float = None
    p_outputs_publisher_open_only: float = None
    p_outputs_both: float = None
    p_outputs_other_platform_open: float = None
    p_outputs_other_platform_open_only: float = None
    p_outputs_closed: float = None
    p_outputs_oa_journal: float = None
    p_outputs_hybrid: float = None
    p_outputs_no_guarantees: float = None
    p_outputs_preprint: int = None
    p_outputs_domain: int = None
    p_outputs_institution: int = None
    p_outputs_public: int = None
    p_outputs_other_internet: int = None

    @staticmethod
    def from_dict(dict_: Dict) -> PublicationStats:
        n_citations = dict_.get("n_citations")
        n_outputs = dict_.get("n_outputs")
        n_outputs_open = dict_.get("n_outputs_open")
        n_outputs_publisher_open = dict_.get("n_outputs_publisher_open")
        n_outputs_publisher_open_only = dict_.get("n_outputs_publisher_open_only")
        n_outputs_both = dict_.get("n_outputs_both")
        n_outputs_other_platform_open = dict_.get("n_outputs_other_platform_open")
        n_outputs_other_platform_open_only = dict_.get("n_outputs_other_platform_open_only")
        n_outputs_closed = dict_.get("n_outputs_closed")
        n_outputs_oa_journal = dict_.get("n_outputs_oa_journal")
        n_outputs_hybrid = dict_.get("n_outputs_hybrid")
        n_outputs_no_guarantees = dict_.get("n_outputs_no_guarantees")
        n_outputs_preprint = dict_.get("n_outputs_preprint")
        n_outputs_domain = dict_.get("n_outputs_domain")
        n_outputs_institution = dict_.get("n_outputs_institution")
        n_outputs_public = dict_.get("n_outputs_public")
        n_outputs_other_internet = dict_.get("n_outputs_other_internet")

        p_outputs_open = dict_.get("p_outputs_open")
        p_outputs_publisher_open = dict_.get("p_outputs_publisher_open")
        p_outputs_publisher_open_only = dict_.get("p_outputs_publisher_open_only")
        p_outputs_both = dict_.get("p_outputs_both")
        p_outputs_other_platform_open = dict_.get("p_outputs_other_platform_open")
        p_outputs_other_platform_open_only = dict_.get("p_outputs_other_platform_open_only")
        p_outputs_closed = dict_.get("p_outputs_closed")
        p_outputs_oa_journal = dict_.get("p_outputs_oa_journal")
        p_outputs_hybrid = dict_.get("p_outputs_hybrid")
        p_outputs_no_guarantees = dict_.get("p_outputs_no_guarantees")
        p_outputs_preprint = dict_.get("p_outputs_preprint")
        p_outputs_domain = dict_.get("p_outputs_domain")
        p_outputs_institution = dict_.get("p_outputs_institution")
        p_outputs_public = dict_.get("p_outputs_public")
        p_outputs_other_internet = dict_.get("p_outputs_other_internet")

        return PublicationStats(
            n_citations=n_citations,
            n_outputs=n_outputs,
            n_outputs_open=n_outputs_open,
            n_outputs_publisher_open=n_outputs_publisher_open,
            n_outputs_publisher_open_only=n_outputs_publisher_open_only,
            n_outputs_both=n_outputs_both,
            n_outputs_other_platform_open=n_outputs_other_platform_open,
            n_outputs_other_platform_open_only=n_outputs_other_platform_open_only,
            n_outputs_closed=n_outputs_closed,
            n_outputs_oa_journal=n_outputs_oa_journal,
            n_outputs_hybrid=n_outputs_hybrid,
            n_outputs_no_guarantees=n_outputs_no_guarantees,
            n_outputs_preprint=n_outputs_preprint,
            n_outputs_domain=n_outputs_domain,
            n_outputs_institution=n_outputs_institution,
            n_outputs_public=n_outputs_public,
            n_outputs_other_internet=n_outputs_other_internet,
            p_outputs_open=p_outputs_open,
            p_outputs_publisher_open=p_outputs_publisher_open,
            p_outputs_publisher_open_only=p_outputs_publisher_open_only,
            p_outputs_both=p_outputs_both,
            p_outputs_other_platform_open=p_outputs_other_platform_open,
            p_outputs_other_platform_open_only=p_outputs_other_platform_open_only,
            p_outputs_closed=p_outputs_closed,
            p_outputs_oa_journal=p_outputs_oa_journal,
            p_outputs_hybrid=p_outputs_hybrid,
            p_outputs_no_guarantees=p_outputs_no_guarantees,
            p_outputs_preprint=p_outputs_preprint,
            p_outputs_domain=p_outputs_domain,
            p_outputs_institution=p_outputs_institution,
            p_outputs_public=p_outputs_public,
            p_outputs_other_internet=p_outputs_other_internet,
        )

    def to_dict(self) -> Dict:
        return {
            "n_citations": self.n_citations,
            "n_outputs": self.n_outputs,
            "n_outputs_open": self.n_outputs_open,
            "n_outputs_publisher_open": self.n_outputs_publisher_open,
            "n_outputs_publisher_open_only": self.n_outputs_publisher_open_only,
            "n_outputs_both": self.n_outputs_both,
            "n_outputs_other_platform_open": self.n_outputs_other_platform_open,
            "n_outputs_other_platform_open_only": self.n_outputs_other_platform_open_only,
            "n_outputs_closed": self.n_outputs_closed,
            "n_outputs_oa_journal": self.n_outputs_oa_journal,
            "n_outputs_hybrid": self.n_outputs_hybrid,
            "n_outputs_no_guarantees": self.n_outputs_no_guarantees,
            "n_outputs_preprint": self.n_outputs_preprint,
            "n_outputs_domain": self.n_outputs_domain,
            "n_outputs_institution": self.n_outputs_institution,
            "n_outputs_public": self.n_outputs_public,
            "n_outputs_other_internet": self.n_outputs_other_internet,
            "p_outputs_open": self.p_outputs_open,
            "p_outputs_publisher_open": self.p_outputs_publisher_open,
            "p_outputs_publisher_open_only": self.p_outputs_publisher_open_only,
            "p_outputs_both": self.p_outputs_both,
            "p_outputs_other_platform_open": self.p_outputs_other_platform_open,
            "p_outputs_other_platform_open_only": self.p_outputs_other_platform_open_only,
            "p_outputs_closed": self.p_outputs_closed,
            "p_outputs_oa_journal": self.p_outputs_oa_journal,
            "p_outputs_hybrid": self.p_outputs_hybrid,
            "p_outputs_no_guarantees": self.p_outputs_no_guarantees,
            "p_outputs_preprint": self.p_outputs_preprint,
            "p_outputs_domain": self.p_outputs_domain,
            "p_outputs_institution": self.p_outputs_institution,
            "p_outputs_public": self.p_outputs_public,
            "p_outputs_other_internet": self.p_outputs_other_internet,
        }


@dataclasses.dataclass
class Identifier:
    id: str
    type: str
    url: str

    @staticmethod
    def from_dict(dict_: Dict):
        i = dict_["id"]
        t = dict_["type"]
        u = dict_["url"]
        return Identifier(i, t, u)

    def to_dict(self) -> Dict:
        return {"id": self.id, "type": self.type, "url": self.url}


@dataclasses.dataclass
class Year:
    year: int
    date: datetime.datetime
    stats: PublicationStats

    def to_dict(self) -> Dict:
        return {"year": self.year, "date": self.date.strftime("%Y-%m-%d"), "stats": self.stats.to_dict()}


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
    min: PublicationStats
    max: PublicationStats
    median: PublicationStats
    histograms: EntityHistograms

    def to_dict(self) -> Dict:
        return {
            "n_items": self.n_items,
            "min": self.min.to_dict(),
            "max": self.max.to_dict(),
            "median": self.median.to_dict(),
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


@dataclasses.dataclass
class Description:
    text: str
    url: str
    license: str = (
        "https://en.wikipedia.org/wiki/Wikipedia:Text_of_Creative_Commons_Attribution-ShareAlike_3.0_Unported_License"
    )

    @staticmethod
    def from_dict(dict_: Dict) -> Description:
        text = dict_.get("description")
        url = dict_.get("wikipedia_url")

        return Description(text, url)

    def to_dict(self) -> Dict:
        return {"text": self.text, "license": self.license, "url": self.url}


@dataclasses.dataclass
class Repository:
    id: str
    total_outputs: int
    category: str
    home_repo: bool

    @staticmethod
    def from_dict(dict_: Dict) -> Repository:
        id = dict_.get("id")
        total_outputs = dict_.get("total_outputs")
        category = dict_.get("category")
        home_repo = dict_.get("home_repo")

        return Repository(id, total_outputs, category, home_repo)

    def to_dict(self) -> Dict:
        return {
            "id": self.id,
            "total_outputs": self.total_outputs,
            "category": self.category,
            "home_repo": self.home_repo,
        }


@dataclasses.dataclass
class Entity:
    id: str
    name: str
    description: Description
    entity_type: str = None
    logo_sm: str = None
    logo_md: str = None
    logo_lg: str = None
    url: str = None
    wikipedia_url: str = None
    country_code: Optional[str] = None
    country_name: Optional[str] = None
    subregion: str = None
    region: str = None
    start_year: int = None
    end_year: int = None
    institution_type: str = None
    stats: PublicationStats = None
    years: List[Year] = field(default_factory=lambda: [])
    acronyms: [str] = None
    repositories: List[Repository] = field(default_factory=lambda: [])

    @staticmethod
    def from_dict(dict_: Dict) -> Entity:
        id = dict_.get("id")
        name = dict_.get("name")
        wikipedia_url = dict_.get("wikipedia_url")
        description = Description.from_dict(dict_)
        entity_type = dict_.get("entity_type")
        logo_sm = dict_.get("logo_sm")
        logo_md = dict_.get("logo_md")
        logo_lg = dict_.get("logo_lg")
        url = dict_.get("url")
        country_code = dict_.get("country_code")
        country_name = dict_.get("country_name")
        subregion = dict_.get("subregion")
        region = dict_.get("region")
        start_year = dict_.get("start_year")
        end_year = dict_.get("end_year")
        institution_type = dict_.get("institution_type")
        acronyms = dict_.get("acronyms", [])

        return Entity(
            id,
            name,
            description=description,
            entity_type=entity_type,
            logo_sm=logo_sm,
            logo_md=logo_md,
            logo_lg=logo_lg,
            url=url,
            wikipedia_url=wikipedia_url,
            country_code=country_code,
            country_name=country_name,
            subregion=subregion,
            region=region,
            start_year=start_year,
            end_year=end_year,
            institution_type=institution_type,
            acronyms=acronyms,
        )

    def to_dict(self) -> Dict:
        dict_ = {
            "id": self.id,
            "name": self.name,
            "description": self.description.to_dict(),
            "entity_type": self.entity_type,
            "logo_sm": self.logo_sm,
            "logo_md": self.logo_md,
            "logo_lg": self.logo_lg,
            "url": self.url,
            "wikipedia_url": self.wikipedia_url,
            "region": self.region,
            "subregion": self.subregion,
            "country_code": self.country_code,
            "country_name": self.country_name,
            "institution_type": self.institution_type,
            "start_year": self.start_year,
            "end_year": self.end_year,
            "stats": self.stats.to_dict(),
            "years": [obj.to_dict() for obj in self.years],
            "acronyms": self.acronyms,
            "repositories": [obj.to_dict() for obj in self.repositories],
        }
        # Filter out key val pairs with empty lists and values
        dict_ = {k: v for k, v in dict_.items() if not val_empty(v)}
        return dict_


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


def val_empty(val):
    if isinstance(val, list):
        return len(val) == 0
    else:
        return val is None or val == ""


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


def save_as_jsonl(output_path: str, iterable: List[Dict]):
    """Save a list of dicts to JSON Lines format.

    :param output_path: the file path.
    :param iterable: the objects to save.
    :return: None.
    """

    with open(output_path, "w") as f:
        with jsonlines.Writer(f) as writer:
            writer.write_all(iterable)


def clean_ror_id(ror_id: str):
    """Remove the https://ror.org/ prefix from a ROR id.

    :param ror_id: original ROR id.
    :return: cleaned ROR id.
    """

    return ror_id.replace("https://ror.org/", "")


def repositories_sort_key(col):
    if is_string_dtype(col.dtype):
        return col.str.lower()
    return col


def repositories_merge_category(cat):
    if cat in {"Aggregator", "Unknown"}:
        return "Other Internet"
    return cat


######################
# Transform data
######################


def load_data_glob(pattern: str) -> List[Dict]:
    """Load country or institution data files into a Pandas DataFrame.

    :param pattern: the file path including a glob pattern.
    :return: the list of dicts.
    """

    file_paths = sorted(glob.glob(pattern))

    data = []
    for file_path in file_paths:
        data += load_jsonl(file_path)

    return data


def load_data(file_path: str) -> pd.DataFrame:
    """Load a country or institution data file into a Pandas DataFrame.

    :param file_path: the path to the file to load.
    :return: the Pandas Dataframe.
    """

    data = load_jsonl(file_path)
    return pd.DataFrame(data)


def preprocess_data_df(entity_type: str, df: pd.DataFrame):
    """Pre-process the data frame.

    :param entity_type: the entity_type.
    :param df: the dataframe.
    :return: the Pandas Dataframe.
    """

    # Convert data types
    df["year"] = pd.to_numeric(df["year"])
    df["date"] = df["year"].apply(lambda year: f"{year}-12-31")
    df.fillna("", inplace=True)
    for column in df.columns:
        if column.startswith("n_"):
            df[column] = pd.to_numeric(df[column])

    # entity_type specific processing
    if entity_type == "institution":
        # Clean RoR ids
        df["id"] = df["id"].apply(lambda i: clean_ror_id(i))


def preprocess_index_df(entity_type: str, df: pd.DataFrame):
    """Pre-process the index data frame.

    :param entity_type: the entity_type.
    :param df: the dataframe.
    :return: the Pandas Dataframe.
    """

    # Convert data types
    df.fillna("", inplace=True)

    # Clean RoR ids
    if entity_type == "institution":
        # Remove columns not used for institutions
        df.drop(columns=["alpha2"], inplace=True, errors="ignore")

        # Clean RoR ids
        df["id"] = df["id"].apply(lambda i: clean_ror_id(i))


def make_index_df(entity_type: str, df_index: pd.DataFrame, df_data: pd.DataFrame):
    """Make the data for the index tables.

    :param entity_type: the entity_type, i.e. country or institution.
    :param df_index: index dataframe.
    :param df_data: data dataframe.
    :return:
    """

    # Create aggregate
    agg = {}
    for column in df_data.columns:
        if column.startswith("n_"):
            agg[column] = "sum"
        else:
            agg[column] = "first"

    # Create aggregate
    df_agg = df_data.groupby(["id"]).agg(
        agg,
        index=False,
    )

    # Include entities that meet a criteria
    mask = df_agg.apply(lambda row: include_entity(entity_type, row["n_outputs"], row["id"]), axis=1)
    df_agg = df_agg[mask]

    # Add percentages to dataframe
    update_df_with_percentages(df_agg, PERCENTAGE_FIELD_KEYS)

    # Sort from highest oa percentage to lowest
    df_agg.sort_values(by=["n_outputs_open"], ascending=False, inplace=True)

    # Add entity_type
    df_agg["entity_type"] = entity_type

    # Remove date and repositories
    df_agg.drop(columns=["year", "date", "repositories"], inplace=True)
    df_agg.reset_index(drop=True, inplace=True)

    # Merge
    df_merged = pd.merge(df_index, df_agg, how="inner", on="id")

    return df_merged


def update_df_with_percentages(df: pd.DataFrame, keys: List[Tuple[str, str]]):
    """Calculate percentages for fields in a Pandas dataframe.

    :param df: the Pandas dataframe.
    :param keys: they keys to calculate percentages for.
    :return: None.
    """

    for numerator_key, denominator_key in keys:
        p_key = f"p_{numerator_key}"
        df[p_key] = round(df[f"n_{numerator_key}"] / df[denominator_key] * 100, 2)

        # Fill in NaN caused by denominator of zero
        df[p_key] = df[p_key].fillna(0)


def select_subset(original: Dict, include_keys: Dict):
    """Select a subset of a dictionary.

    :param original: the original dictionary.
    :param include_keys: the keys to include.
    :return:
    """
    output = {}
    for k, v in include_keys.items():
        if k in original:
            if isinstance(v, dict):
                output[k] = select_subset(original[k], v)
            else:
                output[k] = original[k]

    return output


def make_index(entity_type: str, entities: List[Entity]):
    """Make an index file.

    :param entity_type: the entity entity_type.
    :param entities: a list of entities.
    :return: None.
    """

    # Convert entities to dictionaries and select a subset of fields
    subset = {
        "id": None,
        "name": None,
        "logo_sm": None,
        "entity_type": None,
        "country_code": None,
        "country_name": None,
        "subregion": None,
        "region": None,
        "institution_type": None,
        "acronyms": None,
        "stats": {
            "n_outputs": None,
            "n_outputs_open": None,
            "p_outputs_open": None,
            "p_outputs_publisher_open_only": None,
            "p_outputs_both": None,
            "p_outputs_other_platform_open_only": None,
            "p_outputs_closed": None,
        },
    }
    data = []
    for entity in entities:
        # Select subset
        item = select_subset(entity.to_dict(), subset)

        # If country delete unused fields
        if entity_type == "country":
            for key in ["country_code", "country_name", "institution_type", "acronyms"]:
                try:
                    del item[key]
                except KeyError:
                    pass

        data.append(item)

    return data


def include_entity(entity_type: str, n_outputs: int, entity_id: str = None) -> bool:
    """Whether to include an entity or not

    :param entity_type: the entity type.
    :param n_outputs: the total number of outputs for the entity.
    :param entity_id: the entity id, which is used for including institutions.
    :return:
    """

    if entity_type == "country":
        return n_outputs >= INCLUSION_THRESHOLD[entity_type]
    elif entity_type == "institution":
        return entity_id in INSTITUTION_IDS or n_outputs >= INCLUSION_THRESHOLD[entity_type]
    return False


def make_entities(entity_type: str, df_index: pd.DataFrame, df_data: pd.DataFrame) -> List[Entity]:
    """Make entities.

    :param entity_type: the entity entity_type.
    :param df_index: the index dataframe.
    :param df_data: the data dataframe.
    :return: the Entity objects.
    """

    entities = []
    key_id = "id"
    key_year = "year"
    key_date = "date"
    key_records = "records"
    total = len(df_index)

    logging.info(f"Making entities: {entity_type}")
    ts_groups = df_data.groupby(key_id)

    for entity_id, df_group in ts_groups:
        # Exclude countries and institutions with small num outputs
        total_outputs = df_group["n_outputs"].sum()
        if include_entity(entity_type, total_outputs, entity_id):
            update_df_with_percentages(df_group, PERCENTAGE_FIELD_KEYS)
            df_group = df_group.sort_values(by=[key_year])
            df_group = df_group.loc[:, ~df_group.columns.str.contains("^Unnamed")]

            # Create entity
            entity_dict: Dict = df_index.loc[df_index[key_id] == entity_id].to_dict(key_records)[0]
            entity = Entity.from_dict(entity_dict)
            entity.stats = PublicationStats.from_dict(entity_dict)

            # Make timeseries data
            years = []
            rows: List[Dict] = df_group.to_dict(key_records)
            for row in rows:
                year = int(row.get(key_year))
                date = pendulum.parse(row.get(key_date))
                stats = PublicationStats.from_dict(row)
                years.append(Year(year=year, date=date, stats=stats))
            entity.years = years

            # Make repositories
            repositories = []
            for row in rows:
                repositories += row["repositories"]
            df_repos = pd.DataFrame(repositories, columns=["id", "total_outputs", "category", "home_repo"])
            df_repos["total_outputs"] = pd.to_numeric(df_repos["total_outputs"])
            df_repos["category"] = df_repos["category"].apply(repositories_merge_category)
            df_repos = (
                df_repos.groupby(["id"], as_index=False)
                .agg(
                    {
                        "id": "first",
                        "total_outputs": "sum",
                        "category": "first",
                        "home_repo": "first",
                    }
                )
                .sort_values(
                    by=["total_outputs", "id"], ascending=[False, True], inplace=False, key=repositories_sort_key
                )
            )
            repositories = df_repos.to_dict(key_records)
            repositories = [Repository.from_dict(repo) for repo in repositories]

            # Select a maximum number of repositories, however, make sure that all repositories that belong
            # to the given institution are always present
            entity.repositories = []
            for repo in repositories:
                if len(entity.repositories) <= MAX_REPOSITORIES or repo.home_repo:
                    entity.repositories.append(repo)

            # Set min and max years for data
            entity.start_year = years[0].year
            entity.end_year = years[-1].year

            entities.append(entity)

            # Print progress
            n_progress = len(entities)
            p_progress = n_progress / total * 100
            if n_progress % 100 == 0:
                logging.info(f"Making entities {n_progress}/{total}: {p_progress:.2f}%")

    # Ensure that entities are sorted based on p_outputs_open
    entities = sorted(entities, key=lambda e: e.stats.p_outputs_open, reverse=True)

    return entities


def make_entity_stats(entities: List[Entity]) -> EntityStats:
    """Calculate stats for entities.

    :param entities: a list of entities.
    :return: the entity stats object.
    """

    p_outputs_open = np.array([entity.stats.p_outputs_open for entity in entities])
    n_outputs = np.array([entity.stats.n_outputs for entity in entities])
    n_outputs_open = np.array([entity.stats.n_outputs_open for entity in entities])

    # Make median, min and max values
    stats_median = PublicationStats(p_outputs_open=statistics.median(p_outputs_open))
    stats_min = PublicationStats(
        p_outputs_open=math.floor(float(np.min(p_outputs_open))),
        n_outputs=int(np.min(n_outputs)),
        n_outputs_open=int(np.min(n_outputs_open)),
    )
    stats_max = PublicationStats(
        p_outputs_open=math.ceil(float(np.max(p_outputs_open))),
        n_outputs=int(np.max(n_outputs)),
        n_outputs_open=int(np.max(n_outputs_open)),
    )

    # Make histograms
    data, bins = np.histogram(p_outputs_open, bins="auto")
    hist_p_outputs_open = Histogram(data.tolist(), bins.tolist())

    data, bins = np.histogram(np.log10(n_outputs[n_outputs != 0]), bins="auto")
    hist_n_outputs = Histogram(data.tolist(), bins.tolist())

    data, bins = np.histogram(np.log10(n_outputs_open[n_outputs_open != 0]), bins="auto")
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
        clearbit_download_logo(company_url=url, file_path=file_path, size=width, fmt=fmt)
    if os.path.isfile(file_path):
        logo_path = make_logo_url(entity_type="institution", entity_id=ror_id, size=size, fmt=fmt)

    return ror_id, logo_path


def clean_url(url: str) -> str:
    """Remove path and query from URL.

    :param url: the url.
    :return: the cleaned url.
    """

    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}/"


def update_index_with_logos(build_path: str, entity_type: str, df_index: pd.DataFrame) -> pd.DataFrame:
    """Update the index with logos, downloading logos if they don't exist.

    :param build_path: the path to the build folder.
    :param entity_type: the entity_type, i.e. country or institution.
    :param df_index: the index table Pandas dataframe.
    :return: None.
    """

    # Make logos
    sizes = ["sm", "md", "lg"]
    if entity_type == "country":
        logging.info("Adding country logos to index")

        # Add logo urls to index
        for size in sizes:
            # For lg size point to md svg
            make_logo_url_size = size
            if size == "lg":
                make_logo_url_size = "md"
            df_index[f"logo_{size}"] = df_index["id"].apply(
                lambda country_code: make_logo_url(
                    entity_type=entity_type, entity_id=country_code, size=make_logo_url_size, fmt="svg"
                )
            )

    elif entity_type == "institution":
        # Get the institution logo and the path to the logo image
        logging.info("Downloading logos using Clearbit")
        institution_sizes = [("sm", 32, "jpg"), ("md", 128, "jpg"), ("lg", 532, "png")]
        total = len(df_index)

        for size, width, fmt in institution_sizes:
            logging.info(f"Downloading logos: size={size}, width={width}, fmt={fmt}")

            with ThreadPoolExecutor() as executor:
                # Create jobs
                futures, results = [], []
                for ror_id, url in zip(df_index["id"], df_index["url"]):
                    if url:
                        url = clean_url(url)
                        futures.append(
                            executor.submit(fetch_institution_logo, ror_id, url, size, width, fmt, build_path)
                        )
                    else:
                        results.append((ror_id, "unknown.svg"))

                # Wait for results
                for completed in as_completed(futures):
                    result = completed.result()
                    results.append(result)

                    # Print progress
                    n_progress = len(results)
                    p_progress = n_progress / total * 100
                    if n_progress % 100 == 0:
                        logging.info(f"Downloading logos {n_progress}/{total}: {p_progress:.2f}%")

            logging.info("Finished downloading logos")

            # Merge results
            col_name = f"logo_{size}"
            df_logos = pd.DataFrame(results, columns=["id", col_name])
            df_index = pd.merge(df_index.drop(columns=[col_name], errors="ignore"), df_logos, how="left", on="id")

    return df_index


#########################
# Wikipedia descriptions
#########################


def update_index_with_wiki_descriptions(df_index: pd.DataFrame) -> pd.DataFrame:
    """Get the wikipedia descriptions for each entity (institution or country) and add them to the index table.

    :param df_index: the index table Pandas dataframe.
    :return: None.
    """

    # Download 'punkt' resource, required when shortening wiki descriptions
    nltk.download("punkt")

    # Get all unique Wikipedia URLs as entities can share Wikipedia URLs
    wikipedia_url_key = "wikipedia_url"
    wikipedia_urls = list(set(df_index[wikipedia_url_key]))
    total = len(wikipedia_urls)

    # Create list with dictionaries of max 20 ids + titles (this is wiki api max)
    chunks = [wikipedia_urls[i : i + WIKI_MAX_TITLES] for i in range(0, len(wikipedia_urls), WIKI_MAX_TITLES)]
    logging.info(f"Downloading {total} wikipedia descriptions in {len(chunks)} chunks.")

    # Process each dictionary in separate thread to get wiki descriptions
    with ThreadPoolExecutor() as executor:
        # Queue tasks
        futures, results = [], []
        for chunk in chunks:
            futures.append(executor.submit(fetch_wiki_descriptions, chunk))

        # Wait for results
        for completed in as_completed(futures):
            results += completed.result()

            # Print progress
            n_progress = len(results)
            p_progress = n_progress / total * 100
            if n_progress % 100 == 0:
                logging.info(f"Downloading descriptions {n_progress}/{total}: {p_progress:.2f}%")

    logging.info(f"Finished downloading wikipedia descriptions")
    logging.info(f"Expected results: {total}, actual num descriptions returned: {len(wikipedia_urls)}")
    if total != len(results):
        raise Exception(f"Number of Wikipedia descriptions returned does not match the number of Wikipedia URLs sent")

    # Apply descriptions to index, where Wikipedia URL matches
    # as Wikipedia URLs can be shared with multiple institutions
    description_key = "description"
    df_index[description_key] = ""
    for wikipedia_url, description in results:
        df_index.loc[df_index[wikipedia_url_key] == wikipedia_url, description_key] = description

    return df_index


#############
# Save data
#############


def save_entities(path: str, entities: List[Entity]):
    """Save the data for each entity as a JSON file.

    :param path: the path where the entities should be saved.
    :param entities: the list of Entity objects.
    :return: None.
    """

    os.makedirs(path, exist_ok=True)
    for entity in entities:
        output_path = os.path.join(path, f"{entity.id}.json")
        entity_dict = entity.to_dict()
        save_json(output_path, entity_dict)


def save_stats(path: str, stats: Stats):
    """Save overall stats.

    :param path: the directory where the stats.json file should be saved.
    :param stats: stats object.
    :return: None.
    """

    os.makedirs(path, exist_ok=True)

    # Save as JSON
    output_path = os.path.join(path, "stats.json")
    save_json(output_path, stats.to_dict())


def save_coki_oa_dataset(path: str, countries: List[Entity], institutions: List[Entity]):
    """Save the COKI Open Access Dataset to a zip file.

    :param path: the path to the folder where the dataset should be saved.
    :param countries: the country entities.
    :param institutions: the institution entities.
    :return: None.
    """

    # Country table
    subset = {
        "id": None,
        "name": None,
        "subregion": None,
        "region": None,
        "start_year": None,
        "end_year": None,
        "stats": None,
        "years": None,
    }
    country = [select_subset(entity.to_dict(), subset) for entity in countries]

    # Institutions table
    subset = {
        "id": None,
        "name": None,
        "country_name": None,
        "country_code": None,
        "subregion": None,
        "region": None,
        "institution_type": None,
        "start_year": None,
        "end_year": None,
        "stats": None,
        "years": None,
    }
    institution = [select_subset(entity.to_dict(), subset) for entity in institutions]

    # Save to JSON Lines
    os.makedirs(path, exist_ok=True)

    file_path = os.path.join(path, "country.jsonl")
    save_as_jsonl(file_path, country)

    file_path = os.path.join(path, "institution.jsonl")
    save_as_jsonl(file_path, institution)

    # Save README
    file_path = os.path.join(path, "README.md")
    template = Template(README, keep_trailing_newline=True)
    rendered = template.render(year=pendulum.now().year, n_countries=len(countries), n_institutions=len(institutions))
    with open(file_path, mode="w") as f:
        f.write(rendered)
