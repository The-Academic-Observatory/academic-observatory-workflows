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
import json
import logging
import math
import os
import os.path
import shutil
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import field
from operator import itemgetter
from typing import Dict, List, Optional, Tuple, Union
from urllib.parse import urlparse

import google.cloud.bigquery as bigquery
import jsonlines
import nltk
import pandas as pd
import pendulum
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from airflow.exceptions import AirflowException
from airflow.models.variable import Variable
from airflow.sensors.external_task import ExternalTaskSensor
from pyarrow import json as pa_json

from academic_observatory_workflows.clearbit import clearbit_download_logo
from observatory.platform.utils.airflow_utils import AirflowVars, get_airflow_connection_password
from observatory.platform.utils.config_utils import module_file_path
from observatory.platform.utils.file_utils import load_jsonl
from observatory.platform.utils.gc_utils import (
    bigquery_sharded_table_id,
    download_blobs_from_cloud_storage,
    select_table_shard_dates,
    upload_file_to_cloud_storage,
)
from observatory.platform.utils.workflow_utils import make_release_date
from observatory.platform.workflows.snapshot_telescope import SnapshotRelease
from observatory.platform.workflows.workflow import Workflow

# The minimum number of outputs before including an entity in the analysis
INCLUSION_THRESHOLD = 1000

# The query that pulls data to be included in the dashboards
QUERY = """
SELECT
  agg.id,
  agg.name,
  agg.time_period as year,
  DATE(agg.time_period, 12, 31) as date,
  (SELECT * from ror.links LIMIT 1) AS url,
  COALESCE(ror.wikipedia_url, country.wikipedia_url) as wikipedia_url,
  country.alpha2 as alpha2,
  agg.country as country,
  agg.subregion as subregion,
  agg.region as region,
  ror.types AS institution_types,
  agg.total_outputs as n_outputs,
  agg.access_types.oa.total_outputs AS n_outputs_open,
  agg.citations.mag.total_citations as n_citations,  
  agg.access_types.publisher.total_outputs AS n_outputs_publisher_open,
  agg.access_types.green.total_outputs AS n_outputs_other_platform_open,
  agg.access_types.green_only.total_outputs AS n_outputs_other_platform_open_only,
  agg.access_types.gold_doaj.total_outputs AS n_outputs_oa_journal,
  agg.access_types.hybrid.total_outputs AS n_outputs_hybrid,
  agg.access_types.bronze.total_outputs AS n_outputs_no_guarantees,
  ror.external_ids AS identifiers
FROM
  `{project_id}.{agg_dataset_id}.{agg_table_id}` as agg 
  LEFT OUTER JOIN `{project_id}.{ror_dataset_id}.{ror_table_id}` as ror ON agg.id = ror.id
  LEFT OUTER JOIN `{project_id}.{settings_dataset_id}.{country_table_id}` as country ON agg.id = country.alpha3
WHERE agg.time_period >= 2000 AND agg.time_period <= (EXTRACT(YEAR FROM CURRENT_DATE()) - 1)
ORDER BY year DESC, name ASC
"""


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

    # Percentage fields
    p_outputs_open: int = None
    p_outputs_publisher_open: int = None
    p_outputs_publisher_open_only: int = None
    p_outputs_both: int = None
    p_outputs_other_platform_open: int = None
    p_outputs_other_platform_open_only: int = None
    p_outputs_closed: int = None
    p_outputs_oa_journal: int = None
    p_outputs_hybrid: int = None
    p_outputs_no_guarantees: int = None

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
        }


def split_largest_remainder(sample_size: int, *ratios) -> Tuple:
    """Split a sample size into different groups based on a list of ratios (that add to 1.0) using the largest
    remainder method: https://en.wikipedia.org/wiki/Largest_remainder_method.

    Copyright 2021 James Diprose

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

    :param sample_size: the absolute sample size.
    :param ratios: the list of ratios, must add to 1.0.
    :return: the absolute numbers of each group.
    """

    assert math.isclose(sum(ratios), 1), "ratios must sum to 1.0"
    sizes = [sample_size * ratio for ratio in ratios]
    sizes_whole = [math.floor(size) for size in sizes]

    while (sample_size - sum(sizes_whole)) > 0:
        remainders = [size % 1 for size in sizes]
        max_index = max(enumerate(remainders), key=itemgetter(1))[0]
        sizes_whole[max_index] = sizes_whole[max_index] + 1
        sizes[max_index] = sizes_whole[max_index]

    return tuple(sizes_whole)


@dataclasses.dataclass
class Subject:
    name: str
    n_outputs: float

    def to_dict(self) -> Dict:
        return {"name": self.name, "n_outputs": self.n_outputs}


@dataclasses.dataclass
class Collaborator:
    name: str
    n_outputs: float

    def to_dict(self) -> Dict:
        return {"name": self.name, "n_outputs": self.n_outputs}


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
class Stats:
    min_year: int
    max_year: int
    last_updated: str

    def to_dict(self) -> Dict:
        return {
            "min_year": self.min_year,
            "max_year": self.max_year,
            "last_updated": self.last_updated,
        }


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


def clean_ror_id(ror_id: str):
    """Remove the https://ror.org/ prefix from a ROR id.

    :param ror_id: original ROR id.
    :return: cleaned ROR id.
    """

    return ror_id.replace("https://ror.org/", "")


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


def trigger_repository_dispatch(*, token: str, event_type: str):
    """Trigger a Github repository dispatch event.

    :param event_type: the event type
    :param token: the Github token.
    :return: the response.
    """

    headers = {
        "Accept": "application/vnd.github.v3+json",
        "Authorization": f"token {token}",
    }
    data = {"event_type": event_type}

    return requests.post(
        "https://api.github.com/repos/The-Academic-Observatory/coki-oa-web/dispatches",
        headers=headers,
        data=json.dumps(data),
    )


@dataclasses.dataclass
class Entity:
    id: str
    name: str
    description: Description
    category: str = None
    logo_s: str = None
    logo_l: str = None
    url: str = None
    wikipedia_url: str = None
    country: Optional[str] = None
    subregion: str = None
    region: str = None
    min_year: int = None
    max_year: int = None
    institution_types: Optional[str] = field(default_factory=lambda: [])
    stats: PublicationStats = None
    identifiers: List[Identifier] = field(default_factory=lambda: [])
    collaborators: List[Collaborator] = field(default_factory=lambda: [])  # todo
    subjects: List[Subject] = field(default_factory=lambda: [])  # todo
    other_platform_locations: List[str] = field(default_factory=lambda: [])  # todo
    timeseries: List[Year] = field(default_factory=lambda: [])

    @staticmethod
    def from_dict(dict_: Dict) -> Entity:
        id = dict_.get("id")
        name = dict_.get("name")
        wikipedia_url = dict_.get("wikipedia_url")
        description = Description.from_dict(dict_)
        category = dict_.get("category")
        logo_s = dict_.get("logo_s")
        logo_l = dict_.get("logo_l")
        url = dict_.get("url")
        country = dict_.get("country")
        subregion = dict_.get("subregion")
        region = dict_.get("region")
        min_year = dict_.get("min_year")
        max_year = dict_.get("max_year")
        institution_types = dict_.get("institution_types", [])
        identifiers = [Identifier.from_dict(obj) for obj in dict_.get("identifiers", [])]

        return Entity(
            id,
            name,
            description=description,
            category=category,
            logo_s=logo_s,
            logo_l=logo_l,
            url=url,
            wikipedia_url=wikipedia_url,
            country=country,
            subregion=subregion,
            region=region,
            min_year=min_year,
            max_year=max_year,
            institution_types=institution_types,
            identifiers=identifiers,
        )

    def to_dict(self) -> Dict:
        dict_ = {
            "id": self.id,
            "name": self.name,
            "description": self.description.to_dict(),
            "category": self.category,
            "logo_s": self.logo_s,
            "logo_l": self.logo_l,
            "url": self.url,
            "wikipedia_url": self.wikipedia_url,
            "region": self.region,
            "subregion": self.subregion,
            "country": self.country,
            "institution_types": self.institution_types,
            "min_year": self.min_year,
            "max_year": self.max_year,
            "stats": self.stats.to_dict(),
            "identifiers": [obj.to_dict() for obj in self.identifiers],
            "collaborators": [obj.to_dict() for obj in self.collaborators],
            "subjects": [obj.to_dict() for obj in self.subjects],
            "other_platform_locations": self.other_platform_locations,
            "timeseries": [obj.to_dict() for obj in self.timeseries],
        }
        # Filter out key val pairs with empty lists and values
        dict_ = {k: v for k, v in dict_.items() if not val_empty(v)}
        return dict_


def get_institution_logo(ror_id: str, url: str, size: str, width: int, fmt: str, build_path) -> Tuple[str, str]:
    """Get the path to the logo for an institution.
    If the logo does not exist in the build path yet, download from the Clearbit Logo API tool.
    If the logo does not exist and failed to download, the path will default to "/unknown.svg".

    :param ror_id: the institution's ROR id
    :param url: the URL of the company domain + suffix e.g. spotify.com
    :param size: the image size of the small logo for tables etc.
    :param width: the width of the image.
    :param fmt: the image format.
    :param build_path: the build path for files of this workflow
    :return: The ROR id and relative path (from build path) to the logo
    """
    logo_path = f"/unknown.svg"

    file_path = os.path.join(build_path, "logos", "institution", size, f"{ror_id}.{fmt}")
    if not os.path.isfile(file_path):
        clearbit_download_logo(company_url=url, file_path=file_path, size=width, fmt=fmt)
    if os.path.isfile(file_path):
        logo_path = make_logo_url(category="institution", entity_id=ror_id, size=size, fmt=fmt)

    return ror_id, logo_path


def get_wiki_descriptions(titles: Dict[str, str]) -> List[Tuple[str, str]]:
    """Get the wikipedia descriptions for the given titles.

    :param titles: Dict with titles as keys and id's (either ror_id or alpha3 country code) as values
    :return: List with tuples (id, wiki description)
    """
    titles_arg = []
    for title, entity_id in titles.items():
        # URL encode title if it is not encoded yet
        if title == urllib.parse.unquote(title):
            titles_arg.append(urllib.parse.quote(title))
        # Append title directly if it is already encoded and not empty
        else:
            titles_arg.append(title)

    # Confirm that there is a max of 20 titles, the limit for the wikipedia API
    assert len(titles_arg) <= 20

    # Extract descriptions using the Wikipedia API
    url = f"https://en.wikipedia.org/w/api.php?action=query&format=json&prop=extracts&titles={'%7C'.join(titles_arg)}&redirects=1&exintro=1&explaintext=1"
    response = requests.get(url)
    if response.status_code != 200:
        raise AirflowException(f"Unsuccessful retrieving wikipedia extracts, url: {url}")
    response_json = response.json()
    pages = response_json["query"]["pages"]

    # Create mapping between redirected/normalized page title and original page title
    redirects = {}
    for title in response_json["query"].get("redirects", []):
        redirects[title["to"]] = title["from"]
    normalized = {}
    for title in response_json["query"].get("normalized", []):
        normalized[title["to"]] = title["from"]

    # Create mapping between entity_id and decoded page title.
    decoded_titles = {urllib.parse.unquote(k): v for k, v in titles.items()}
    descriptions = []
    for page_id, page in pages.items():
        page_title = page["title"]

        # Get page_title from redirected/normalized if it is present
        page_title = redirects.get(page_title, page_title)
        page_title = normalized.get(page_title, page_title)

        # Link original title to description
        entity_id = decoded_titles[urllib.parse.unquote(page_title)]

        # Get description and clean up
        description = page.get("extract", "")
        if description:
            description = remove_text_between_brackets(description)
            description = shorten_text_full_sentences(description)

        descriptions.append((entity_id, description))
    return descriptions


def remove_text_between_brackets(text: str) -> str:
    """Remove any text between (nested) brackets.
    If there is a space after the opening bracket, this is removed as well.
    E.g. 'Like this (foo, (bar)) example' -> 'Like this example'

    :param text: The text to modify
    :return: The modified text
    """
    new_text = []
    nested = 0
    for char in text:
        if char == "(":
            nested += 1
            new_text = new_text[:-1] if new_text[-1] == " " else new_text
        elif (char == ")") and nested:
            nested -= 1
        elif nested == 0:
            new_text.append(char)
    return "".join(new_text)


def shorten_text_full_sentences(text: str, *, char_limit: int = 300) -> str:
    """Shorten a text to as many complete sentences as possible, while the total number of characters stays below
    the char_limit.
    Always return at least one sentence, even if this exceeds the char_limit.

    :param text: A string with the complete text
    :param char_limit: The max number of characters
    :return: The shortened text.
    """
    # Create list of sentences
    sentences = nltk.tokenize.sent_tokenize(text)

    # Add sentences until char limit is reached
    sentences_output = []
    total_len = 0
    for sentence in sentences:
        total_len += len(sentence)
        if (total_len > char_limit) and sentences_output:
            break
        sentences_output.append(sentence)
    return " ".join(sentences_output)


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
    extract_job_config = bigquery.ExtractJobConfig()
    extract_job_config.destination_format = bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON
    extract_job: bigquery.ExtractJob = client.extract_table(
        source_table_id, destination_uri, job_config=extract_job_config, location=location
    )
    extract_job.result()

    return query_job.state == "DONE" and extract_job.state == "DONE"


def clean_url(url: str) -> str:
    """Remove path and query from URL.

    :param url: the url.
    :return: the cleaned url.
    """

    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}/"


def save_as_jsonl(output_path: str, iterable: List[Dict]):
    with open(output_path, "w") as f:
        with jsonlines.Writer(f) as writer:
            writer.write_all(iterable)


def jsonl_to_pyarrow(jsonl_path: str, output_path: str):
    table = pa_json.read_json(jsonl_path)
    pq.write_table(table, output_path)


def make_logo_url(*, category: str, entity_id: str, size: str, fmt: str) -> str:
    return f"/logos/{category}/{size}/{entity_id}.{fmt}"


def calc_oa_stats(
    n_outputs: int,
    n_outputs_open: int,
    n_outputs_publisher_open: int,
    n_outputs_other_platform_open: int,
    n_outputs_other_platform_open_only: int,
):
    # Closed
    n_outputs_closed = n_outputs - n_outputs_open

    # Both
    n_outputs_both = n_outputs_other_platform_open - n_outputs_other_platform_open_only

    # Publisher open only
    n_outputs_publisher_open_only = n_outputs_publisher_open - n_outputs_both

    return n_outputs_publisher_open_only, n_outputs_both, n_outputs_closed


class OaWebRelease(SnapshotRelease):
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
    ]

    def __init__(
        self,
        *,
        dag_id: str,
        project_id: str,
        release_date: pendulum.DateTime,
        data_bucket_name: str,
        change_chart_years: int = 10,
        agg_dataset_id: str = "observatory",
        ror_dataset_id: str = "ror",
    ):
        """Create an OaWebRelease instance.

        :param dag_id: the dag id.
        :param project_id: the Google Cloud project id.
        :param release_date: the release date.
        :param change_chart_years: the number of years to include in the change charts.
        :param agg_dataset_id: the dataset to use for aggregation.
        :param ror_dataset_id: the ROR dataset id.
        """

        super().__init__(dag_id=dag_id, release_date=release_date)
        self.project_id = project_id
        self.data_bucket_name = data_bucket_name
        self.change_chart_years = change_chart_years
        self.agg_dataset_id = agg_dataset_id
        self.ror_dataset_id = ror_dataset_id
        self.data_path = module_file_path("academic_observatory_workflows.workflows.data.oa_web_workflow")

    @property
    def build_path(self):
        return os.path.join(self.transform_folder, "build")

    def load_data(self, category: str) -> pd.DataFrame:
        """Load the data file for a given category.

        :param category: the category, i.e. country or institution.
        :return: the Pandas Dataframe.
        """

        path = os.path.join(self.download_folder, f"{category}.jsonl")
        data = load_jsonl(path)
        return pd.DataFrame(data)

    def preprocess_df(self, category: str, df: pd.DataFrame) -> pd.DataFrame:
        """Pre-process the data frame.

        :param category: the category.
        :param df: the dataframe.
        :return: the Pandas Dataframe.
        """

        # Convert data types
        df = df.copy(deep=True)
        df["date"] = pd.to_datetime(df["date"])
        df.fillna("", inplace=True)
        for column in df.columns:
            if column.startswith("n_"):
                df[column] = pd.to_numeric(df[column])

        # Create missing fields
        publisher_open_only = []
        both = []
        closed = []
        for i, row in df.iterrows():
            n_outputs = row["n_outputs"]
            n_outputs_open = row["n_outputs_open"]
            n_outputs_publisher_open = row["n_outputs_publisher_open"]
            n_outputs_other_platform_open = row["n_outputs_other_platform_open"]
            n_outputs_other_platform_open_only = row["n_outputs_other_platform_open_only"]
            n_outputs_publisher_open_only, n_outputs_both, n_outputs_closed = calc_oa_stats(
                n_outputs,
                n_outputs_open,
                n_outputs_publisher_open,
                n_outputs_other_platform_open,
                n_outputs_other_platform_open_only,
            )

            # Add to arrays
            publisher_open_only.append(n_outputs_publisher_open_only)
            both.append(n_outputs_both)
            closed.append(n_outputs_closed)

        df["n_outputs_publisher_open_only"] = publisher_open_only
        df["n_outputs_both"] = both
        df["n_outputs_closed"] = closed

        # Clean RoR ids
        if category == "institution":
            # Remove columns not used for institutions
            df.drop(columns=["alpha2"], inplace=True, errors="ignore")

            # Clean RoR ids
            df["id"] = df["id"].apply(lambda i: clean_ror_id(i))

            # Parse identifiers
            preferred_key = "preferred"
            identifiers = []
            for i, row in df.iterrows():
                # Parse identifier for each entry
                ent_ids = []
                ids_dict = row["identifiers"]

                # Add ROR id
                ror_id = row["id"]
                ent_ids.append({"id": ror_id, "type": "ROR", "url": f"https://ror.org/{ror_id}"})

                # Parse other ids
                for k, v in ids_dict.items():
                    url = None
                    id_type = k
                    if id_type != "OrgRef":
                        if preferred_key in v:
                            id_value = v[preferred_key]
                        else:
                            id_value = v["all"][0]

                        # Create URLs
                        if id_type == "ISNI":
                            url = f"https://isni.org/isni/{id_value}"
                        elif id_type == "Wikidata":
                            url = f"https://www.wikidata.org/wiki/{id_value}"
                        elif id_type == "GRID":
                            url = f"https://grid.ac/institutes/{id_value}"
                        elif id_type == "FundRef":
                            url = f"https://api.crossref.org/funders/{id_value}"

                        ent_ids.append({"id": id_value, "type": id_type, "url": url})
                identifiers.append(ent_ids)
            df["identifiers"] = identifiers

        if category == "country":
            # Remove columns not used for countries
            df.drop(columns=["url", "institution_types", "country", "identifiers"], inplace=True, errors="ignore")

        return df

    def make_index(self, category: str, df: pd.DataFrame):
        """Make the data for the index tables.

        :param category: the category, i.e. country or institution.
        :param df: Pandas dataframe with all data points.
        :return:
        """

        # Create aggregate
        agg = {}
        for column in df.columns:
            if column.startswith("n_"):
                agg[column] = "sum"
            else:
                agg[column] = "first"

        # Create aggregate
        df_index_table = df.groupby(["id"]).agg(
            agg,
            index=False,
        )

        # Exclude countries with small samples
        df_index_table = df_index_table[df_index_table["n_outputs"] >= INCLUSION_THRESHOLD]

        # Add percentages to dataframe
        self.update_df_with_percentages(df_index_table, self.PERCENTAGE_FIELD_KEYS)

        # Make percentages add to 100% when integers
        self.quantize_df_percentages(df_index_table)

        # Sort from highest oa percentage to lowest
        df_index_table.sort_values(by=["n_outputs_open"], ascending=False, inplace=True)

        # Add category
        df_index_table["category"] = category

        # Remove date and year
        df_index_table.drop(columns=["date", "year"], inplace=True)

        return df_index_table

    def update_df_with_percentages(self, df: pd.DataFrame, keys: List[Tuple[str, str]]):
        """Calculate percentages for fields in a Pandas dataframe.

        :param df: the Pandas dataframe.
        :param keys: they keys to calculate percentages for.
        :return: None.
        """

        for numerator_key, denominator_key in keys:
            p_key = f"p_{numerator_key}"
            df[p_key] = df[f"n_{numerator_key}"] / df[denominator_key] * 100

            # Fill in NaN caused by denominator of zero
            df[p_key] = df[p_key].fillna(0)

    def quantize_df_percentages(self, df: pd.DataFrame):
        """Makes percentages add to 100% when integers

        :param df: the Pandas dataframe.
        :return: None.
        """

        for i, row in df.iterrows():
            # Make percentage publisher open only, both, other platform open only and closed add to 100
            sample_size = 100
            keys = [
                "p_outputs_publisher_open_only",
                "p_outputs_both",
                "p_outputs_other_platform_open_only",
                "p_outputs_closed",
            ]
            ratios = [row[key] / 100.0 for key in keys]
            results = split_largest_remainder(sample_size, *ratios)
            for key, value in zip(keys, results):
                df.loc[i, key] = value

            # Make percentage oa_journal, hybrid and no_guarantees add to 100
            keys = ["p_outputs_oa_journal", "p_outputs_hybrid", "p_outputs_no_guarantees"]
            ratios = [row[key] / 100.0 for key in keys]
            has_publisher_open = row["n_outputs_publisher_open"] > 0
            if has_publisher_open:
                results = split_largest_remainder(sample_size, *ratios)
                for key, value in zip(keys, results):
                    df.loc[i, key] = value

    def update_index_with_logos(self, category: str, df_index_table: pd.DataFrame):
        """Update the index with logos, downloading logos if they don't exist.

        :param category: the category, i.e. country or institution.
        :param df_index_table: the index table Pandas dataframe.
        :return: None.
        """

        sizes = ["s", "l"]
        for size in sizes:
            base_path = os.path.join(self.build_path, "logos", category, size)
            os.makedirs(base_path, exist_ok=True)

        # Make logos
        if category == "country":
            logging.info("Copying local logos")
            with ThreadPoolExecutor() as executor:
                futures = []
                # Copy and rename logo images from using alpha2 to alpha3 country codes
                for size in sizes:
                    base_path = os.path.join(self.build_path, "logos", category, size)
                    for alpha3, alpha2 in zip(df_index_table["id"], df_index_table["alpha2"]):
                        src_path = os.path.join(self.data_path, "flags", size, f"{alpha2}.svg")
                        dst_path = os.path.join(base_path, f"{alpha3}.svg")
                        futures.append(executor.submit(shutil.copy, src_path, dst_path))
                [f.result() for f in as_completed(futures)]
            logging.info("Finished copying local logos")

            # Add logo urls to index
            for size in sizes:
                df_index_table[f"logo_{size}"] = df_index_table["id"].apply(
                    lambda country_code: make_logo_url(category=category, entity_id=country_code, size=size, fmt="svg")
                )
        elif category == "institution":
            logging.info("Downloading logos using Clearbit")
            fmt = "jpg"
            # Get the institution logo and the path to the logo image
            for size, width in zip(sizes, [32, 128]):
                with ThreadPoolExecutor() as executor:
                    futures = []
                    logo_paths = []
                    for ror_id, url in zip(df_index_table["id"], df_index_table["url"]):
                        if url:
                            url = clean_url(url)
                            futures.append(
                                executor.submit(get_institution_logo, ror_id, url, size, width, fmt, self.build_path)
                            )
                        else:
                            logo_paths.append((ror_id, "/unknown.svg"))
                    logo_paths += [f.result() for f in as_completed(futures)]
                logging.info("Finished downloading logos")

                # Sort table and results by id
                df_index_table.sort_index(inplace=True)
                logo_paths_sorted = [tup[1] for tup in sorted(logo_paths, key=lambda tup: tup[0])]

                # Add logo paths to table
                df_index_table[f"logo_{size}"] = logo_paths_sorted

    def update_index_with_wiki_descriptions(self, df_index_table: pd.DataFrame):
        """Get the wikipedia descriptions for each entity (institution or country) and add them to the index table.

        :param df_index_table: the index table Pandas dataframe.
        :return: None.
        """
        # Filter to select rows where url is not empty
        wikipedia_url_filter = df_index_table["wikipedia_url"] != ""

        # The wikipedia 'title' is the last part of the wikipedia url, without segments specified with '#'
        titles_all = list(
            zip(
                df_index_table.loc[wikipedia_url_filter, "wikipedia_url"]
                .str.split("wikipedia.org/wiki/")
                .str[-1]
                .str.split("#")
                .str[0],
                df_index_table.loc[wikipedia_url_filter, "id"],
            )
        )
        # Create list with dictionaries of max 20 ids + titles (this is wiki api max)
        titles_chunks = [
            dict(titles_all[i : i + OaWebWorkflow.WIKI_MAX_TITLES])
            for i in range(0, len(titles_all), OaWebWorkflow.WIKI_MAX_TITLES)
        ]

        logging.info(
            f"Downloading wikipedia descriptions for all {len(titles_all)} entities in {len(titles_chunks)} chunks."
        )
        # Download 'punkt' resource, required when shortening wiki descriptions
        nltk.download("punkt")

        # Process each dictionary in separate thread to get wiki descriptions
        with ThreadPoolExecutor() as executor:
            futures = []
            for titles in titles_chunks:
                futures.append(executor.submit(get_wiki_descriptions, titles))
            descriptions = []
            for f in as_completed(futures):
                descriptions += f.result()
        logging.info(f"Finished downloading wikipedia descriptions")

        # Sort table and results by id
        df_index_table.sort_index(inplace=True)
        descriptions_sorted = [tup[1] for tup in sorted(descriptions, key=lambda tup: tup[0])]

        # Add wiki descriptions to table
        df_index_table.loc[wikipedia_url_filter, "description"] = descriptions_sorted
        df_index_table.loc[~wikipedia_url_filter, "description"] = ""

    def save_index(self, category: str, df_index_table: pd.DataFrame):
        """Save the index table.

        :param category: the category, i.e. country or institution.
        :param df_index_table: the index table Pandas Dataframe.
        :return: None.
        """

        # Save subset
        base_path = os.path.join(self.build_path, "data")
        os.makedirs(base_path, exist_ok=True)
        df_index_table = df_index_table.drop(
            [
                "description",
                "year",
                "date",
                "institution_types",
                "identifiers",
                "collaborators",
                "subjects",
                "other_platform_locations",
                "timeseries",
            ],
            axis=1,
            errors="ignore",
        )

        # Make entities
        records = df_index_table.to_dict("records")
        entities = []
        for record in records:
            entity = Entity.from_dict(record)
            entity.stats = PublicationStats.from_dict(record)
            entities.append(entity)

        # Sort by Open %
        entities = sorted(entities, key=lambda e: e.stats.p_outputs_open, reverse=True)
        entities = [e.to_dict() for e in entities]

        # Save as JSON
        json_path = os.path.join(base_path, f"{category}.json")
        save_json(json_path, entities)

        # Save JSONL
        jsonl_path = os.path.join(base_path, f"{category}.jsonl")
        save_as_jsonl(jsonl_path, entities)

        # Save as PyArrow
        pyarrow_path = os.path.join(base_path, f"{category}.parquet")
        jsonl_to_pyarrow(jsonl_path, pyarrow_path)

    def make_entities(self, df_index_table: pd.DataFrame, df: pd.DataFrame) -> List[Entity]:
        """Make entities.

        :param df_index_table: the index table Pandas Dataframe.
        :param df: the Pandas dataframe.
        :return: the Entity objects.
        """

        entities = []
        key_id = "id"
        key_year = "year"
        key_date = "date"
        key_records = "records"
        ts_groups = df.groupby([key_id])
        for entity_id, df_group in ts_groups:
            # Exclude institutions with small num outputs
            total_outputs = df_group["n_outputs"].sum()
            if total_outputs >= INCLUSION_THRESHOLD:
                self.update_df_with_percentages(df_group, self.PERCENTAGE_FIELD_KEYS)
                df_group = df_group.sort_values(by=[key_year])
                df_group = df_group.loc[:, ~df_group.columns.str.contains("^Unnamed")]

                # Make percentages add to 100% when integers
                self.quantize_df_percentages(df_group)

                # Create entity
                entity_dict: Dict = df_index_table.loc[df_index_table[key_id] == entity_id].to_dict(key_records)[0]
                entity = Entity.from_dict(entity_dict)
                entity.stats = PublicationStats.from_dict(entity_dict)

                # Make timeseries data
                years = []
                rows: List[Dict] = df_group.to_dict(key_records)
                for row in rows:
                    year = int(row.get(key_year))
                    date = row.get(key_date)
                    stats = PublicationStats.from_dict(row)
                    years.append(Year(year=year, date=date, stats=stats))
                entity.timeseries = years

                # Set min and max years for data
                entity.min_year = years[0].year
                entity.max_year = years[-1].year

                entities.append(entity)

        return entities

    def save_entities(self, category: str, entities: List[Entity]):
        """Save the data for each entity as a JSON file.

        :param category: the entity category.
        :param entities: the list of Entity objects.
        :return: None.
        """

        base_path = os.path.join(self.build_path, "data", category)
        os.makedirs(base_path, exist_ok=True)
        for entity in entities:
            output_path = os.path.join(base_path, f"{entity.id}.json")
            entity_dict = entity.to_dict()
            save_json(output_path, entity_dict)

    def make_auto_complete(self, df_index_table: pd.DataFrame, category: str) -> List[Dict]:
        """Build the autocomplete data.

        :param df_index_table: index table Pandas dataframe.
        :param category: the category, i.e. country or institution.
        :return: autocomplete records.
        """

        records = []
        for i, row in df_index_table.iterrows():
            id = row["id"]
            name = row["name"]
            logo = row["logo_s"]
            records.append({"id": id, "name": name, "category": category, "logo_s": logo})
        return records

    def save_autocomplete(self, auto_complete: List[Dict]):
        """Save the autocomplete data.

        :param auto_complete: the autocomplete list.
        :return: None.
        """

        base_path = os.path.join(self.build_path, "data")
        os.makedirs(base_path, exist_ok=True)

        # Save as JSON
        output_path = os.path.join(base_path, "autocomplete.json")
        df_ac = pd.DataFrame(auto_complete)
        records = df_ac.to_dict("records")
        save_json(output_path, records)

        # Save as PyArrow
        table = pa.Table.from_pandas(df_ac)
        pyarrow_path = os.path.join(base_path, f"autocomplete.parquet")
        pq.write_table(table, pyarrow_path)

    def save_stats(self, stats: Stats):
        """Save overall stats.

        :param stats: stats object.
        :return: None.
        """

        base_path = os.path.join(self.build_path, "data")
        os.makedirs(base_path, exist_ok=True)

        # Save as JSON
        output_path = os.path.join(base_path, "stats.json")
        save_json(output_path, stats.to_dict())


class OaWebWorkflow(Workflow):
    DATA_BUCKET = "oa_web_data_bucket"
    GITHUB_TOKEN_CONN = "oa_web_github_token"

    """The OaWebWorkflow generates data files for the COKI Open Access Dashboard.

    The figure below illustrates the generated data and notes about what each file is used for.
    .
    ├── data: data
    │   ├── autocomplete.json: used for the website search functionality. Copied into public/data folder.
    │   ├── autocomplete.parquet: used for filtering in Cloudflare Worker.
    │   ├── country: individual entity statistics files for countries. Used to build each country page.
    │   │   ├── ALB.json
    │   │   ├── ARE.json
    │   │   └── ARG.json
    │   ├── country.json: used to create the country table. First 18 countries used to build first page of country table
    │   │                 and then this file is included in the public folder and downloaded by the client to enable the
    │   │                 other pages of the table to be displayed. Copied into public/data folder.
    │   ├── country.jsonl: used to generate the parquet file.
    │   ├── country.parquet: to be used along with apache-arrow to enable filtering from a Cloudflare Worker.
    │   ├── institution: individual entity statistics files for institutions. Used to build each institution page.
    │   │   ├── 05ykr0121.json
    │   │   ├── 05ym42410.json
    │   │   └── 05ynxx418.json
    │   ├── institution.json: used to create the institution table. First 18 institutions used to build first page of institution table
    │   │                     and then this file is included in the public folder and downloaded by the client to enable the
    │   │                     other pages of the table to be displayed. Copied into public/data folder.
    │   ├── institution.jsonl: used to generate the parquet file.
    │   ├── institution.parquet: to be used along with apache-arrow to enable filtering from a Cloudflare Worker.
    │   └── stats.json: global statistics, e.g. the minimum and maximum date for the dataset, when it was last updated etc.
    └── logos: country and institution logos. Copied into public/logos folder.
        ├── country
        │   ├── l: large logos displayed on country pages.
        │   │   ├── ALB.svg
        │   │   ├── ARE.svg
        │   │   └── ARG.svg
        │   └── s: small logos displayed in country table.
        │       ├── ALB.svg
        │       ├── ARE.svg
        │       └── ARG.svg
        └── institution
            ├── l: large logos displayed on institution pages.
            │   ├── 05ykr0121.jpg
            │   ├── 05ym42410.jpg
            │   └── 05ynxx418.jpg
            └── s: small logos displayed in institution table.
                ├── 05ykr0121.jpg
                ├── 05ym42410.jpg
                └── 05ynxx418.jpg
    """

    # Set the number of titles for which wiki descriptions are retrieved at once, the API can return max 20 extracts.
    WIKI_MAX_TITLES = 20

    def __init__(
        self,
        *,
        dag_id: str = "oa_web_workflow",
        start_date: Optional[pendulum.DateTime] = pendulum.datetime(2021, 5, 2),
        schedule_interval: Optional[str] = "@weekly",
        catchup: Optional[bool] = False,
        ext_dag_id: str = "doi",
        table_ids: List[str] = None,
        airflow_vars: List[str] = None,
        airflow_conns: List[str] = None,
        agg_dataset_id: str = "observatory",
        ror_dataset_id: str = "ror",
        settings_dataset_id: str = "settings",
        version: str = "v1",
    ):
        """Create the OaWebWorkflow.

        :param dag_id: the DAG id.
        :param start_date: the start date.
        :param schedule_interval: the schedule interval.
        :param catchup: whether to catchup or not.
        :param table_ids: the table ids.
        :param version: the dataset version published by this workflow. The Github Action pulls from a specific dataset
        version: https://github.com/The-Academic-Observatory/coki-oa-web/blob/develop/.github/workflows/build-on-data-update.yml#L68-L74.
        This is so that when breaking changes are made to the schema, the web application won't break.
        :param airflow_vars: required Airflow Variables.
        """

        if airflow_vars is None:
            airflow_vars = [
                AirflowVars.DATA_PATH,
                AirflowVars.PROJECT_ID,
                AirflowVars.DATA_LOCATION,
                AirflowVars.DOWNLOAD_BUCKET,
                AirflowVars.TRANSFORM_BUCKET,
                self.DATA_BUCKET,
            ]

        if airflow_conns is None:
            airflow_conns = [self.GITHUB_TOKEN_CONN]

        super().__init__(
            dag_id=dag_id,
            start_date=start_date,
            schedule_interval=schedule_interval,
            catchup=catchup,
            airflow_vars=airflow_vars,
            airflow_conns=airflow_conns,
        )
        self.agg_dataset_id = agg_dataset_id
        self.ror_dataset_id = ror_dataset_id
        self.settings_dataset_id = settings_dataset_id
        self.table_ids = table_ids
        self.version = version
        if table_ids is None:
            self.table_ids = ["country", "institution"]

        self.add_operator(
            ExternalTaskSensor(task_id=f"{ext_dag_id}_sensor", external_dag_id=ext_dag_id, mode="reschedule")
        )
        self.add_setup_task(self.check_dependencies)
        self.add_task(self.query)
        self.add_task(self.download)
        self.add_task(self.transform)
        self.add_task(self.upload_dataset)
        self.add_task(self.repository_dispatch)
        self.add_task(self.cleanup)

    def make_release(self, **kwargs) -> OaWebRelease:
        """Make release instances. The release is passed as an argument to the function (TelescopeFunction) that is
        called in 'task_callable'.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are
        passed to this argument.
        :return: A list of OaWebRelease instances
        """

        project_id = Variable.get(AirflowVars.PROJECT_ID)
        release_date = make_release_date(**kwargs)
        data_bucket_name = Variable.get(self.DATA_BUCKET)

        return OaWebRelease(
            dag_id=self.dag_id,
            project_id=project_id,
            data_bucket_name=data_bucket_name,
            release_date=release_date,
            ror_dataset_id=self.ror_dataset_id,
            agg_dataset_id=self.agg_dataset_id,
        )

    def query(self, release: OaWebRelease, **kwargs):
        """Fetch the data for each table.

        :param release: the release.
        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are
        passed to this argument.
        :return: None.
        """
        results = []

        # ROR release date
        ror_table_id = "ror"
        ror_release_date = select_table_shard_dates(
            project_id=release.project_id,
            dataset_id=release.ror_dataset_id,
            table_id=ror_table_id,
            end_date=release.release_date,
        )[0]
        ror_sharded_table_id = bigquery_sharded_table_id(ror_table_id, ror_release_date)

        for agg_table_id in self.table_ids:
            # Aggregate release dates
            agg_release_date = select_table_shard_dates(
                project_id=release.project_id,
                dataset_id=release.agg_dataset_id,
                table_id=agg_table_id,
                end_date=release.release_date,
            )[0]
            agg_sharded_table_id = bigquery_sharded_table_id(agg_table_id, agg_release_date)

            # Fetch data
            destination_uri = f"gs://{release.download_bucket}/{self.dag_id}/{release.release_id}/{agg_table_id}.jsonl"
            success = bq_query_to_gcs(
                query=QUERY.format(
                    project_id=release.project_id,
                    agg_dataset_id=release.agg_dataset_id,
                    agg_table_id=agg_sharded_table_id,
                    ror_dataset_id=release.ror_dataset_id,
                    ror_table_id=ror_sharded_table_id,
                    settings_dataset_id=self.settings_dataset_id,
                    country_table_id="country",
                ),
                project_id=release.project_id,
                destination_uri=destination_uri,
            )
            results.append(success)

        state = all(results)
        if not state:
            raise AirflowException("OaWebWorkflow.query failed")

    def download(self, release: OaWebRelease, **kwargs):
        """Download the queried data.

        :param release: the release.
        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are
        passed to this argument.
        :return: None.
        """

        prefix = f"{self.dag_id}/{release.release_id}"
        state = download_blobs_from_cloud_storage(
            bucket_name=release.download_bucket, prefix=prefix, destination_path=release.download_folder
        )
        if not state:
            raise AirflowException("OaWebWorkflow.download failed")

    def transform(self, release: OaWebRelease, **kwargs):
        """Transform the queried data into the final format for the open access website.

        :param release: the release.
        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are
        passed to this argument.
        :return: None.
        """

        # Make required folders
        auto_complete = []
        for category in self.table_ids:
            logging.info(f"Transforming {category} entity")
            # Load data
            df = release.load_data(category)

            # Pre-process data
            df = release.preprocess_df(category, df)

            # Make index table
            df_index_table = release.make_index(category, df)
            release.update_index_with_logos(category, df_index_table)
            release.update_index_with_wiki_descriptions(df_index_table)
            entities = release.make_entities(df_index_table, df)

            # Make autocomplete data for this category
            auto_complete += release.make_auto_complete(df_index_table, category)

            # Save category data
            release.save_index(category, df_index_table)
            release.save_entities(category, entities)
            logging.info(f"Saved transformed {category} entity")

        # Save auto complete data as json
        release.save_autocomplete(auto_complete)
        logging.info(f"Saved autocomplete data")

        # Save stats as json
        min_year = 2000
        max_year = pendulum.now().year - 1
        last_updated = pendulum.now().format("D MMMM YYYY")
        stats = Stats(min_year, max_year, last_updated)
        release.save_stats(stats)
        logging.info(f"Saved stats data")

        # Zip data
        dst = os.path.join(release.transform_folder, "latest")
        shutil.copytree(release.build_path, dst)
        base_name = os.path.join(release.transform_folder, "latest")
        shutil.make_archive(base_name, "zip", dst)

    def upload_dataset(self, release: OaWebRelease, **kwargs):
        """Publish the dataset produced by this workflow.

        :param release: the release.
        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are
        passed to this argument.
        :return: None.
        """

        # upload_file_to_cloud_storage should always rewrite a new version of latest.zip if it exists
        # object versioning on the bucket will keep the previous versions
        blob_name = f"{self.version}/latest.zip"
        file_path = os.path.join(release.transform_folder, "latest.zip")
        upload_file_to_cloud_storage(
            bucket_name=release.data_bucket_name, blob_name=blob_name, file_path=file_path, check_blob_hash=False
        )

    def repository_dispatch(self, release: OaWebRelease, **kwargs):
        """Trigger a Github repository_dispatch to trigger new website builds.

        :param release: the release.
        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are
        passed to this argument.
        :return: None.
        """

        token = get_airflow_connection_password(self.GITHUB_TOKEN_CONN)
        event_types = ["data-update/develop", "data-update/staging", "data-update/production"]
        for event_type in event_types:
            trigger_repository_dispatch(token=token, event_type=event_type)

    def cleanup(self, release: OaWebRelease, **kwargs):
        """Delete all files and folders associated with this release.

        :param release: the release.
        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are
        passed to this argument.
        :return: None.
        """
        release.cleanup()
