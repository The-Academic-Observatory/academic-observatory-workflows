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


from __future__ import annotations, annotations

import dataclasses
import glob
import json
import logging
import os
import os.path
import os.path
import os.path
import shutil
import statistics
from typing import Dict, List, Union
from urllib.parse import urlparse
from zipfile import ZipFile

import google.cloud.bigquery as bigquery
import jsonlines
import math
import numpy as np
import pendulum
from airflow.exceptions import AirflowException
from glom import Coalesce, glom, SKIP
from jinja2 import Template

from academic_observatory_workflows.config import project_path
from academic_observatory_workflows.github import trigger_repository_dispatch
from academic_observatory_workflows.oa_dashboard_workflow.institution_ids import INSTITUTION_IDS
from academic_observatory_workflows.oa_dashboard_workflow.release import OaDashboardRelease
from academic_observatory_workflows.wikipedia import fetch_wikipedia_descriptions
from academic_observatory_workflows.zenodo import make_draft_version, publish_new_version, Zenodo
from observatory_platform.airflow.airflow import get_airflow_connection_password
from observatory_platform.files import yield_jsonl
from observatory_platform.google.bigquery import (
    bq_create_table_from_query,
    bq_load_from_memory,
    bq_run_query,
)
from observatory_platform.google.gcs import (
    gcs_blob_name_from_path,
    gcs_download_blob,
    gcs_download_blobs,
    gcs_upload_file,
)
from observatory_platform.jinja2_utils import render_template

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


def upload_institution_ids(*, release: OaDashboardRelease):
    data = [{"ror_id": ror_id} for ror_id in INSTITUTION_IDS]
    success = bq_load_from_memory(
        release.institution_ids_table_id,
        data,
        schema_file_path=project_path("oa_dashboard_workflow", "schema", "institution_ids.json"),
    )
    if not success:
        raise AirflowException(f"upload_institution_ids: error loading table {release.institution_ids_table_id}")


def create_entity_tables(
    *, release: OaDashboardRelease, entity_types: list[str], start_year: int, end_year: int, inclusion_thresholds: dict
):
    results, queries = [], []

    # Query the country and institution aggregations
    for entity_type in entity_types:
        template_path = project_path("oa_dashboard_workflow", "sql", f"{entity_type}.sql.jinja2")
        sql = render_template(
            template_path,
            agg_table_id=release.observatory_agg_table_id(entity_type),
            start_year=start_year,
            end_year=end_year,
            ror_table_id=release.ror_table_id,
            country_table_id=release.country_table_id,
            institution_ids_table_id=release.institution_ids_table_id,
            inclusion_threshold=inclusion_thresholds[entity_type],
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


def add_wiki_descriptions(*, release: OaDashboardRelease, entity_type: str):
    logging.info(f"add_wiki_descriptions: {entity_type}")

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


def download_assets(*, release: OaDashboardRelease, bucket_name: str):
    # Download assets
    # They are unzipped in this particular order so that images-base overwrites any files in images

    blob_names = ["images.zip", "images-base.zip"]
    for blob_name in blob_names:
        # Download asset zip
        file_path = os.path.join(release.download_folder, blob_name)
        gcs_download_blob(bucket_name=bucket_name, blob_name=blob_name, file_path=file_path)  #  data_bucket

        # Unzip into build
        unzip_folder_path = os.path.join(release.build_path, "images")
        with ZipFile(file_path) as zip_file:
            zip_file.extractall(unzip_folder_path)  # Overwrites by default


def update_institution_logos(*, release: OaDashboardRelease):
    logging.info(f"download_logos: institution")
    id_select_sql = "SELECT id FROM `{}`"

    # Get institution ids of all existing logos
    logos_table_id = release.logos_table_id("institution")
    existing_logos_by_id: List[bigquery.Row] = bq_run_query(id_select_sql.format(logos_table_id))
    existing_logo_set = set([v.values()[0] for v in existing_logos_by_id])

    # Get institution ids of all institutions we have data for
    institution_table_id = release.oa_dashboard_table_id("institution")
    inst_ids: List[bigquery.Row] = bq_run_query(id_select_sql.format(institution_table_id))
    new_inst_set = set([v.values()[0] for v in inst_ids])

    # For any institutions without existing logos, add 'unknown.svg' logos to the dataset
    missing_ids = set(i for i in new_inst_set if i not in existing_logo_set)
    if missing_ids:
        insert_sql = f"INSERT INTO `{logos_table_id}`  (id, logo_sm, logo_md, logo_lg)"
        values_sql = []
        for id in missing_ids:
            values_sql.append(f'\nVALUES ("{id}", "unknown.svg", "unknown.svg", "unknown.svg")')
        insert_sql += ", ".join(values_sql) + ";"
        logging.info(f"Running SQL Insert statement:\n{insert_sql}")
        bq_run_query(insert_sql)

    # Update with entity table
    template_path = project_path("oa_dashboard_workflow", "sql", "update_logos.sql.jinja2")
    sql = render_template(
        template_path,
        entity_table_id=release.oa_dashboard_table_id("institution"),
        logos_table_id=logos_table_id,
    )
    bq_run_query(sql)

    # Zip dataset
    shutil.make_archive(os.path.join(release.out_path, "images"), "zip", os.path.join(release.build_path, "images"))


def export_tables(*, release: OaDashboardRelease, entity_types: list[str], download_bucket: str):
    # Fetch data
    blob_prefix = gcs_blob_name_from_path(release.download_folder)

    results = []
    for entity_type in entity_types:
        destination_uri = f"gs://{download_bucket}/{blob_prefix}/{entity_type}-data-*.jsonl.gz"
        table_id = release.oa_dashboard_table_id(entity_type)
        success = bq_query_to_gcs(
            query=f"SELECT * FROM {table_id} ORDER BY stats.p_outputs_open DESC",
            # Uses a query to export data to make sure it is in the correct order
            project_id=release.output_project_id,
            destination_uri=destination_uri,
        )
        results.append(success)

    if not all(results):
        raise AirflowException("OaDashboardWorkflow.download failed")


def download_data(*, release: OaDashboardRelease, download_bucket: str):
    blob_prefix = gcs_blob_name_from_path(release.download_folder)
    state = gcs_download_blobs(
        bucket_name=download_bucket,
        prefix=blob_prefix,
        destination_path=release.download_folder,
    )
    if not state:
        raise AirflowException("OaDashboardWorkflow.download failed")


def make_draft_zenodo_version(*, zenodo_conn_id: str, zenodo_host: str, conceptrecid: int):
    zenodo_token = get_airflow_connection_password(zenodo_conn_id)
    zenodo = Zenodo(host=zenodo_host, access_token=zenodo_token)
    make_draft_version(zenodo, conceptrecid)


def fetch_zenodo_versions(
    *,
    zenodo_conn_id: str,
    zenodo_host: str,
    conceptrecid: int,
):
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
        ).to_dict()
        for version in res.json()
    ]
    return zenodo_versions


def build_datasets(
    *,
    release: OaDashboardRelease,
    entity_types: list[str],
    zenodo_versions: list[ZenodoVersion],
    start_year: int,
    end_year: int,
    readme_text: str,
):
    # Save OA Dashboard dataset
    build_data_path = os.path.join(release.build_path, "data")
    save_oa_dashboard_dataset(
        release.download_folder, build_data_path, entity_types, zenodo_versions, start_year, end_year
    )
    shutil.make_archive(os.path.join(release.out_path, "data"), "zip", os.path.join(release.build_path, "data"))

    # Save COKI Open Access Dataset
    coki_dataset_path = os.path.join(release.transform_folder, "coki-oa-dataset")
    save_zenodo_dataset(release.download_folder, coki_dataset_path, entity_types, readme_text)
    shutil.make_archive(os.path.join(release.out_path, "coki-oa-dataset"), "zip", coki_dataset_path)


def publish_zenodo_version(
    *,
    release: OaDashboardRelease,
    version: str,
    bucket_name: str,
    zenodo_conn_id: str,
    zenodo_host: str,
    conceptrecid: int,
):
    zenodo_token = get_airflow_connection_password(zenodo_conn_id)
    zenodo = Zenodo(host=zenodo_host, access_token=zenodo_token)
    res = zenodo.get_versions(conceptrecid, all_versions=0)
    if res.status_code != 200:
        raise AirflowException(f"zenodo.get_versions status_code {res.status_code}")
    draft = res.json()[0]
    draft_id = draft["id"]
    if draft["state"] != "unsubmitted":
        raise AirflowException(f"Latest version is not a draft: {draft_id}")

    # Download latest Zenodo version from bucket
    file_name = "coki-oa-dataset.zip"
    file_path = os.path.join(release.out_path, file_name)
    blob_name = f"{version}/{file_name}"
    success = gcs_download_blob(bucket_name=bucket_name, blob_name=blob_name, file_path=file_path)
    if not success:
        raise AirflowException(f"publish_zenodo_version: unable to download gs://{bucket_name}/{blob_name}")

    # Publish new version
    publish_new_version(zenodo, draft_id, file_path)


def upload_dataset(*, release: OaDashboardRelease, version: str, bucket_name: str):
    # gcs_upload_file should always rewrite a new version of latest.zip if it exists
    # object versioning on the bucket will keep the previous versions
    for file_name in ["data.zip", "images.zip", "coki-oa-dataset.zip"]:
        blob_name = f"{version}/{file_name}"
        file_path = os.path.join(release.out_path, file_name)
        gcs_upload_file(bucket_name=bucket_name, blob_name=blob_name, file_path=file_path, check_blob_hash=False)


def repository_dispatch(*, github_conn_id: str):
    token = get_airflow_connection_password(github_conn_id)
    event_types = ["data-update/develop", "data-update/staging", "data-update/production"]
    for event_type in event_types:
        trigger_repository_dispatch(
            org="The-Academic-Observatory", repo_name="coki-oa-web", token=token, event_type=event_type
        )


@dataclasses.dataclass
class ZenodoVersion:
    release_date: pendulum.DateTime
    download_url: str

    @staticmethod
    def from_dict(dict_: dict) -> ZenodoVersion:
        return ZenodoVersion(release_date=pendulum.parse(dict_["release_date"]), download_url=dict_["download_url"])

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
    download_folder: str,
    build_data_path: str,
    entity_types: List[str],
    zenodo_versions: List[ZenodoVersion],
    start_year: int,
    end_year: int,
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
    stats = Stats(start_year, end_year, last_updated, zenodo_versions, country_stats, institution_stats)
    output_path = os.path.join(build_data_path, "stats.json")
    save_json(output_path, stats.to_dict())
    logging.info(f"Saved stats data")


def save_zenodo_dataset(download_folder: str, dataset_path: str, entity_types: List[str], readme_text: str):
    """Save the COKI Open Access Dataset to a zip file.

    :param download_folder: the path where the downloaded data files can be found.
    :param dataset_path: the path to the folder where the dataset should be saved.
    :param entity_types: the entity types.
    :param readme_text: the readme text.
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
    template = Template(readme_text, keep_trailing_newline=True)
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
        "url": Coalesce("url", default=SKIP),
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

    subset = glom(item, subset_spec)
    if subset.get("url"):
        subset["url"] = urlparse(subset["url"]).netloc  # Strip url to domain only
    return subset


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
