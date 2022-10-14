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
import statistics
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import field
from typing import List, Dict, Set
from typing import Optional, Tuple, Union
from urllib.parse import urlparse
from zipfile import ZipFile

import google.cloud.bigquery as bigquery
import jsonlines
import nltk
import numpy as np
import pandas as pd
import pendulum
import requests
import treelib
from airflow.exceptions import AirflowException
from airflow.models.variable import Variable
from airflow.sensors.external_task import ExternalTaskSensor
from jinja2 import Template
from pandas.api.types import is_string_dtype

from academic_observatory_workflows.clearbit import clearbit_download_logo
from academic_observatory_workflows.dag_tag import Tag
from observatory.platform.utils.airflow_utils import AirflowVars, get_airflow_connection_password
from observatory.platform.utils.config_utils import module_file_path
from observatory.platform.utils.file_utils import load_jsonl
from observatory.platform.utils.gc_utils import (
    bigquery_sharded_table_id,
    download_blobs_from_cloud_storage,
    select_table_shard_dates,
    upload_file_to_cloud_storage,
    download_blob_from_cloud_storage,
)
from observatory.platform.utils.workflow_utils import make_release_date
from observatory.platform.workflows.snapshot_telescope import SnapshotRelease
from observatory.platform.workflows.workflow import Workflow

# The minimum number of outputs before including an entity in the analysis
INCLUSION_THRESHOLD = {"country": 25, "institution": 350}
MAX_REPOSITORIES = 200
START_YEAR = 2000

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
  agg.citations.mag.total_citations as n_citations,  
  agg.total_outputs as n_outputs,
  
  -- COKI OA Categories
  agg.oa_coki.open.total AS n_outputs_open,
  agg.oa_coki.publisher.total AS n_outputs_publisher_open,
  agg.oa_coki.publisher_only.total AS n_outputs_publisher_open_only,
  agg.oa_coki.both.total AS n_outputs_both,
  agg.oa_coki.other_platform.total AS n_outputs_other_platform_open,
  agg.oa_coki.other_platform_only.total AS n_outputs_other_platform_open_only,
  agg.oa_coki.closed.total AS n_outputs_closed,
  
  -- Publisher Open Categories
  agg.oa_coki.publisher_categories.oa_journal.total AS n_outputs_oa_journal,
  agg.oa_coki.publisher_categories.hybrid.total AS n_outputs_hybrid,
  agg.oa_coki.publisher_categories.no_guarantees.total AS n_outputs_no_guarantees,
  
  -- Other Platform Open Categories
  agg.oa_coki.other_platform_categories.preprint.total AS n_outputs_preprint,
  agg.oa_coki.other_platform_categories.domain.total AS n_outputs_domain,
  agg.oa_coki.other_platform_categories.institution.total AS n_outputs_institution,
  agg.oa_coki.other_platform_categories.public.total AS n_outputs_public,
  agg.oa_coki.other_platform_categories.aggregator.total + agg.oa_coki.other_platform_categories.other_internet.total + agg.oa_coki.other_platform_categories.unknown.total AS n_outputs_other_internet, 
  
  ror.external_ids AS identifiers,
  agg.repositories,
  CASE
    WHEN agg.id = country.alpha3 THEN []
    ELSE ror.acronyms
    END AS acronyms,
FROM
  `{project_id}.{agg_dataset_id}.{agg_table_id}` as agg 
  LEFT OUTER JOIN `{project_id}.{ror_dataset_id}.{ror_table_id}` as ror ON agg.id = ror.id
  LEFT OUTER JOIN `{project_id}.{settings_dataset_id}.{country_table_id}` as country ON agg.id = country.alpha3
WHERE agg.time_period >= {start_year} AND agg.time_period <= (EXTRACT(YEAR FROM CURRENT_DATE()) - 1)
ORDER BY year DESC, name ASC
"""

README = """# COKI Open Access Dataset
The COKI Open Access Dataset measures open access performance for {{ n_countries }} countries and {{ n_institutions }} institutions
and is available in JSON Lines format. The data is visualised at the COKI Open Access Dashboard: https://open.coki.ac/.

## Licence
[COKI Open Access Dataset](https://open.coki.ac/data/) © {{ year }} by [Curtin University](https://www.curtin.edu.au/)
is licenced under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)

## Citing
If you use the website or website code, please cite it as below:

> James P. Diprose, Richard Hosking, Richard Rigoni, Aniek Roelofs, Kathryn R. Napier, Tuan-Yow Chien, Katie S. Wilson, Lucy Montgomery, & Cameron Neylon. (2022). COKI Open Access Website. Zenodo. https://doi.org/10.5281/zenodo.6374486

If you use this dataset, please cite it as below:

> Richard Hosking, James P. Diprose, Aniek Roelofs, Tuan-Yow Chien, Lucy Montgomery, & Cameron Neylon. (2022). COKI Open Access Dataset [Data set]. Zenodo. https://doi.org/10.5281/zenodo.6399463

## Attributions
The COKI Open Access Dataset contains information from:
* [Microsoft Academic Graph](https://www.microsoft.com/en-us/research/project/microsoft-academic-graph/) which is made available under the [ODC Attribution License](https://opendatacommons.org/licenses/by/1-0/).
* [Crossref Metadata](https://www.crossref.org/documentation/metadata-plus/) via the Metadata Plus program. Bibliographic metadata is made available without copyright restriction and Crossref generated data with a [CC0 licence](https://creativecommons.org/share-your-work/public-domain/cc0/). See [metadata licence information](https://www.crossref.org/documentation/retrieve-metadata/rest-api/rest-api-metadata-license-information/) for more details.
* [Unpaywall](https://unpaywall.org/). The [Unpaywall Data Feed](https://unpaywall.org/products/data-feed) is used under license. Data is freely available from Unpaywall via the API, data dumps and as a data feed.
* [Research Organization Registry](https://ror.org/) which is made available under a [CC0 licence](https://creativecommons.org/share-your-work/public-domain/cc0/).
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
    category: str = None
    logo_s: str = None
    logo_l: str = None
    logo_xl: str = None
    url: str = None
    wikipedia_url: str = None
    country: Optional[str] = None
    subregion: str = None
    region: str = None
    start_year: int = None
    end_year: int = None
    institution_types: Optional[str] = field(default_factory=lambda: [])
    stats: PublicationStats = None
    identifiers: List[Identifier] = field(default_factory=lambda: [])
    years: List[Year] = field(default_factory=lambda: [])
    acronyms: [str] = None
    repositories: List[Repository] = field(default_factory=lambda: [])

    @staticmethod
    def from_dict(dict_: Dict) -> Entity:
        id = dict_.get("id")
        name = dict_.get("name")
        wikipedia_url = dict_.get("wikipedia_url")
        description = Description.from_dict(dict_)
        category = dict_.get("category")
        logo_s = dict_.get("logo_s")
        logo_l = dict_.get("logo_l")
        logo_xl = dict_.get("logo_xl")
        url = dict_.get("url")
        country = dict_.get("country")
        subregion = dict_.get("subregion")
        region = dict_.get("region")
        start_year = dict_.get("start_year")
        end_year = dict_.get("end_year")
        institution_types = dict_.get("institution_types", [])
        identifiers = [Identifier.from_dict(obj) for obj in dict_.get("identifiers", [])]
        acronyms = dict_.get("acronyms", [])

        return Entity(
            id,
            name,
            description=description,
            category=category,
            logo_s=logo_s,
            logo_l=logo_l,
            logo_xl=logo_xl,
            url=url,
            wikipedia_url=wikipedia_url,
            country=country,
            subregion=subregion,
            region=region,
            start_year=start_year,
            end_year=end_year,
            institution_types=institution_types,
            identifiers=identifiers,
            acronyms=acronyms,
        )

    def to_dict(self) -> Dict:
        dict_ = {
            "id": self.id,
            "name": self.name,
            "description": self.description.to_dict(),
            "category": self.category,
            "logo_s": self.logo_s,
            "logo_l": self.logo_l,
            "logo_xl": self.logo_xl,
            "url": self.url,
            "wikipedia_url": self.wikipedia_url,
            "region": self.region,
            "subregion": self.subregion,
            "country": self.country,
            "institution_types": self.institution_types,
            "start_year": self.start_year,
            "end_year": self.end_year,
            "stats": self.stats.to_dict(),
            "identifiers": [obj.to_dict() for obj in self.identifiers],
            "years": [obj.to_dict() for obj in self.years],
            "acronyms": self.acronyms,
            "repositories": [obj.to_dict() for obj in self.repositories],
        }
        # Filter out key val pairs with empty lists and values
        dict_ = {k: v for k, v in dict_.items() if not val_empty(v)}
        return dict_


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

    # Resolve redirects referring to each other to 1 redirect
    for key, value in redirects.copy().items():
        if value in redirects:
            redirects[key] = redirects.pop(value)

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
            new_text = new_text[:-1] if new_text and new_text[-1] == " " else new_text
        elif (char == ")") and nested:
            nested -= 1
        elif nested == 0:
            new_text.append(char)
    return "".join(new_text).strip()


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
    """Save a list of dicts to JSON Lines format.

    :param output_path: the file path.
    :param iterable: the objects to save.
    :return: None.
    """

    with open(output_path, "w") as f:
        with jsonlines.Writer(f) as writer:
            writer.write_all(iterable)


def make_logo_url(*, category: str, entity_id: str, size: str, fmt: str) -> str:
    """Make a logo url.

    :param category: the entity category: country or institution.
    :param entity_id: the entity id.
    :param size: the size of the logo: s or l.
    :param fmt: the format of the logo.
    :return: the logo url.
    """

    return f"/logos/{category}/{size}/{entity_id}.{fmt}"


class Zenodo:
    def __init__(self, host: str = "https://zenodo.org", access_token: str = None, timeout: int = 60):
        self.host = host
        self.access_token = access_token
        self.timeout = timeout

    def make_url(self, path: str):
        # Remove last / from host
        host = self.host
        if self.host[-1] == "/":
            host = self.host[:-1]

        # Add leading / to path
        if path[0] != "/":
            path = f"/{path}"

        return f"{host}{path}"

    def get_versions(self, conceptrecid: int, all_versions: int = 0, size: int = 10, sort: str = "mostrecent"):
        query = f"conceptrecid:{conceptrecid}"
        return requests.get(
            self.make_url("/api/deposit/depositions"),
            params={
                "q": query,
                "all_versions": all_versions,
                "access_token": self.access_token,
                "sort": sort,
                "size": size,
            },
            timeout=self.timeout,
        )

    def create_new_version(self, id: int):
        url = self.make_url(f"/api/deposit/depositions/{id}/actions/newversion")
        return requests.post(url, params={"access_token": self.access_token})

    def get_deposition(self, id: int):
        url = self.make_url(f"/api/deposit/depositions/{id}")
        return requests.get(url, params={"access_token": self.access_token})

    def delete_file(self, id: int, file_id: str):
        url = self.make_url(f"/api/deposit/depositions/{id}/files/{file_id}")
        return requests.delete(url, params={"access_token": self.access_token})

    def update(self, id: int, data: Dict):
        url = self.make_url(f"/api/deposit/depositions/{id}")
        headers = {"Content-Type": "application/json"}
        return requests.put(url, data=json.dumps(data), headers=headers, params={"access_token": self.access_token})

    def upload_file(self, id: int, file_path: str):
        url = self.make_url(f"/api/deposit/depositions/{id}/files")
        data = {"name": os.path.basename(file_path)}
        files = {"file": open(file_path, "rb")}
        return requests.post(url, data=data, files=files, params={"access_token": self.access_token})

    def publish(self, id: int):
        url = self.make_url(f"/api/deposit/depositions/{id}/actions/publish")
        return requests.post(url, params={"access_token": self.access_token})


def make_draft_version(zenodo: Zenodo, conceptrecid: int):
    # Make new draft version
    res = zenodo.get_versions(conceptrecid)
    if res.status_code != 200:
        raise AirflowException(f"zenodo.get_versions status_code {res.status_code}")

    versions = res.json()
    if len(versions) == 0:
        raise AirflowException(f"make_draft_version: at least 1 version must exist")

    latest = versions[0]
    draft_id = latest["id"]
    state = latest["state"]
    if state == "done":
        # If published then create a new draft
        res = zenodo.create_new_version(draft_id)
        if res.status_code != 201:
            raise AirflowException(f"zenodo.create_new_version status_code {res.status_code}")
        draft_id = int(res.json()["links"]["latest_draft"].split("/")[-1])

    # Fetch draft deposition
    res = zenodo.get_deposition(draft_id)
    if res.status_code != 200:
        raise AirflowException(f"zenodo.get_deposition status_code {res.status_code}")

    # Update metadata
    draft = res.json()
    publication_date = pendulum.now().format("YYYY-MM-DD")
    metadata = draft["metadata"]
    metadata["publication_date"] = publication_date
    metadata["version"] = publication_date
    data = {"metadata": metadata}
    res = zenodo.update(draft_id, data)
    if res.status_code != 200:
        raise AirflowException(f"zenodo.update status_code {res.status_code}")


def publish_new_version(zenodo: Zenodo, draft_id: int, file_path: str):
    # Get full deposition which contains files
    res = zenodo.get_deposition(draft_id)
    if res.status_code != 200:
        raise AirflowException(f"zenodo.get_deposition {res.status_code}")
    draft = res.json()

    # Delete existing files
    for file in draft["files"]:
        file_id = file["id"]
        res = zenodo.delete_file(draft_id, file_id)
        if res.status_code != 204:
            raise AirflowException(f"zenodo.delete_file status_code {res.status_code}")

    # Upload new file
    res = zenodo.upload_file(draft_id, file_path)
    if res.status_code != 201:
        raise AirflowException(f"zenodo.upload_file status_code {res.status_code}")

    # Publish
    res = zenodo.publish(draft_id)
    if res.status_code != 202:
        raise AirflowException(f"zenodo.publish status_code {res.status_code}")


def load_data(file_path: str) -> pd.DataFrame:
    """Load the data file for a given category.

    :param file_path: the path to the file to load.
    :return: the Pandas Dataframe.
    """

    data = load_jsonl(file_path)
    return pd.DataFrame(data)

#
# def preprocess_df(category: str, df: pd.DataFrame):
#     """Pre-process the data frame.
#
#     :param category: the category.
#     :param df: the dataframe.
#     :return: the Pandas Dataframe.
#     """
#
#     # Convert data types
#     df["date"] = pd.to_datetime(df["date"])
#     df.fillna("", inplace=True)
#     for column in df.columns:
#         if column.startswith("n_"):
#             df[column] = pd.to_numeric(df[column])
#
#     # Clean RoR ids
#     if category == "institution":
#         # Remove columns not used for institutions
#         df.drop(columns=["alpha2"], inplace=True, errors="ignore")
#
#         # Clean RoR ids
#         df["id"] = df["id"].apply(lambda i: clean_ror_id(i))
#
#         # Parse identifiers
#         preferred_key = "preferred"
#         identifiers = []
#         for i, row in df.iterrows():
#             # Parse identifier for each entry
#             ent_ids = []
#             ids_dict = row["identifiers"]
#
#             # Add ROR id
#             ror_id = row["id"]
#             ent_ids.append({"id": ror_id, "type": "ROR", "url": f"https://ror.org/{ror_id}"})
#
#             # Parse other ids
#             for k, v in ids_dict.items():
#                 url = None
#                 id_type = k
#                 if id_type != "OrgRef":
#                     if preferred_key in v:
#                         id_value = v[preferred_key]
#                     else:
#                         id_value = v["all"][0]
#
#                     # Create URLs
#                     if id_type == "ISNI":
#                         url = f"https://isni.org/isni/{id_value}"
#                     elif id_type == "Wikidata":
#                         url = f"https://www.wikidata.org/wiki/{id_value}"
#                     elif id_type == "GRID":
#                         url = f"https://grid.ac/institutes/{id_value}"
#                     elif id_type == "FundRef":
#                         url = f"https://api.crossref.org/funders/{id_value}"
#
#                     ent_ids.append({"id": id_value, "type": id_type, "url": url})
#             identifiers.append(ent_ids)
#         df["identifiers"] = identifiers
#
#     if category == "country":
#         # Remove columns not used for countries
#         df.drop(columns=["url", "institution_types", "country", "identifiers"], inplace=True, errors="ignore")


def preprocess_df(category: str, df: pd.DataFrame):
    """Pre-process the data frame.

    :param category: the category.
    :param df: the dataframe.
    :return: the Pandas Dataframe.
    """

    # Convert data types
    # df["date"] = pd.to_datetime(df["date"])
    df["year"] = pd.to_numeric(df["year"])
    df.fillna("", inplace=True)
    for column in df.columns:
        if column.startswith("n_"):
            df[column] = pd.to_numeric(df[column])

    # Clean RoR ids
    if category == "institution":
        # Remove columns not used for institutions
        # df.drop(columns=["alpha2"], inplace=True, errors="ignore")

        # Clean RoR ids
        df["id"] = df["id"].apply(lambda i: clean_ror_id(i))

    df.drop(columns=["repositories"], inplace=True, errors="ignore")
    df.set_index(["id", "year"], inplace=True, verify_integrity=True)
    df.sort_index(inplace=True)


def ror_to_tree(ror: List[Dict]) -> treelib.Tree:
    tree = treelib.Tree()
    root_id = "root"
    tree.create_node(identifier=root_id, tag="Root")
    for row in ror:
        # Get node. If it doesn't exist then create it.
        node_id = clean_ror_id(row["id"])
        node_label = row["name"]
        node = tree.get_node(node_id)
        if node is None:
            node = tree.create_node(identifier=node_id, tag=node_label, parent=root_id)

        for rel in row["relationships"]:
            rel_id = clean_ror_id(rel["id"])
            rel_type = rel["type"]
            rel_label = rel["label"]

            if rel_type == "Parent":
                if not tree.contains(rel_id):
                    # Create rel parent node if doesn't exist
                    tree.create_node(identifier=rel_id, tag=rel_label, parent=root_id)

                # If rel node already exists, move node to parent (rel)
                tree.move_node(node_id, rel_id)
            elif rel_type == "Child":
                # Get rel node
                child = tree.get_node(rel_id)
                if child is None:
                    # If rel node doesn't exist then create it, setting parent to row node_id
                    tree.create_node(identifier=rel_id, tag=rel_label, parent=node_id)
                else:
                    # Set parent of child to row node_id
                    child.set_predecessor(node_id, tree.identifier)

    return tree


def aggregate_up(node_id: str, tree: treelib.Tree, df: pd.DataFrame, seen: Set):
    # Get parent
    parent = tree.parent(node_id)
    if parent.identifier == "root":
        return

    # For each child of the parent, traverse its children. Except node_id, because it is either a leaf or a parent that
    # was previously traversed.
    # Add node_id as a child
    children = [node_id]
    for child in tree.children(parent.identifier):
        if node_id == child.identifier:
            continue
        traverse_children(child.identifier, tree, df, seen)
        children.append(child.identifier)
        seen.add(child.identifier)

    # Aggregate parent and children
    aggregate_children(parent.identifier, children, df)

    # When finished aggregating parent and it's children, keep aggregating up the tree
    aggregate_up(parent.identifier, tree, df, seen)


def aggregate_children(pid: str, child_ids: List, df: pd.DataFrame):
    # Get a group with all records including parent and children
    group_ids = [pid] + child_ids
    df_group = df.loc[pd.IndexSlice[group_ids, :], :]

    # Aggregate all fields starting with "n_"
    agg = {}
    for column in df_group.columns:
        if column.startswith("n_"):
            agg[column] = "sum"
    df_agg = (
        df_group.groupby(["year"])
        .agg(
            agg,
            index=False,
        )
        .reset_index()
    )

    # TODO: aggregate repositories

    # Add id and make have same index structure as df
    df_agg["id"] = pid
    df_agg.set_index(["id", "year"], inplace=True, verify_integrity=True)

    # Update df with values from df_agg
    df.update(df_agg)


def traverse_children(parent_id: str, tree: treelib.Tree, df: pd.DataFrame, seen: Set):
    # Get children
    children = []
    for child in tree.children(parent_id):
        traverse_children(child.identifier, tree, df, seen)
        children.append(child.identifier)
        seen.add(child.identifier)

    # Aggregate parent and children
    if len(children):
        aggregate_children(parent_id, children, df)


def aggregate_institutions(tree: treelib.Tree, df: pd.DataFrame):
    seen = set()
    root_id = "root"
    leaves = tree.leaves(root_id)
    n_nodes = len(tree.nodes)

    for leaf in leaves:
        # If leaf already processed then continue
        leaf_id = leaf.identifier
        if leaf_id in seen:
            continue

        # Mark that leaf has been seen
        seen.add(leaf_id)

        # If leaf of root, no aggregation needed
        p = tree.parent(leaf_id)
        if p.identifier == root_id:
            continue

        # Get aggregated stats for leaf
        aggregate_up(leaf_id, tree, df, seen)

        # Print progress
        n_processed = len(seen) + 1
        p_processed = n_processed / n_nodes * 100
        print(f"Progress \r{n_processed} / {n_nodes}: {p_processed:.2f}%")


def make_institution_df(ror, start_year: int = 2000, end_year: int = 2021, column_names: List = None):
    data = []
    for r in ror:
        ror_id = clean_ror_id(r["id"])
        for year in range(start_year, end_year + 1):
            data.append([ror_id, year])

    df = pd.DataFrame(data, columns=["id", "year"])

    if column_names is None:
        column_names = [
            "n_citations",
            "n_outputs",
            "n_outputs_open",
            "n_outputs_publisher_open",
            "n_outputs_publisher_open_only",
            "n_outputs_both",
            "n_outputs_other_platform_open",
            "n_outputs_other_platform_open_only",
            "n_outputs_closed",
            "n_outputs_oa_journal",
            "n_outputs_hybrid",
            "n_outputs_no_guarantees",
            "n_outputs_preprint",
            "n_outputs_domain",
            "n_outputs_institution",
            "n_outputs_public",
            "n_outputs_other_internet",
        ]

    for name in column_names:
        df[name] = np.nan

    df.set_index(["id", "year"], inplace=True, verify_integrity=True)
    df.sort_index(inplace=True)

    return df


class OaWebRelease(SnapshotRelease):
    def __init__(
        self,
        *,
        dag_id: str,
        release_date: pendulum.DateTime,
        data_bucket_name: str,
        zenodo: Zenodo = Zenodo(),
    ):
        """Create an OaWebRelease instance.

        :param dag_id: the dag id.
        :param release_date: the release date.
        :param zenodo: the zenodo instance.
        """

        super().__init__(dag_id=dag_id, release_date=release_date)
        self.zenodo = zenodo
        self.data_bucket_name = data_bucket_name
        self.assets_path = module_file_path("academic_observatory_workflows.workflows.data.oa_web_workflow")

    @property
    def build_path(self):
        return os.path.join(self.transform_folder, "build")


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


class OaWebWorkflow(Workflow):
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
    DATA_BUCKET = "oa_web_data_bucket"
    GITHUB_TOKEN_CONN = "oa_web_github_token"
    ZENODO_TOKEN_CONN = "oa_web_zenodo_token"

    """The OaWebWorkflow generates data files for the COKI Open Access Dashboard.

    The figure below illustrates the generated data and notes about what each file is used for.
    .
    ├── data: data
    │   ├── index.json: used by the Cloudflare Worker search and filtering API.
    │   ├── autocomplete.json: used for the website search functionality. Copied into public/data folder.
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
        input_project_id: str = "academic-observatory",
        output_project_id: str = "academic-observatory",
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
        version: str = "v6",
        conceptrecid: int = 6399462,
        zenodo_host: str = "https://zenodo.org",
    ):
        """Create the OaWebWorkflow.

        :param project_id: the Google Cloud project id.
        :param dag_id: the DAG id.
        :param start_date: the start date.
        :param schedule_interval: the schedule interval.
        :param catchup: whether to catchup or not.
        :param ext_dag_id: the DAG id to wait for.
        :param table_ids: the table ids.
        :param airflow_vars: required Airflow Variables.
        :param airflow_conns: required Airflow Connections.
        :param agg_dataset_id: the id of the dataset where the Academic Observatory aggregated data lives.
        :param ror_dataset_id: the id of the dataset containing the ROR table.
        :param settings_dataset_id: the id of the settings dataset, which contains the country table.
        :param version: the dataset version published by this workflow. The Github Action pulls from a specific dataset
        version: https://github.com/The-Academic-Observatory/coki-oa-web/blob/develop/.github/workflows/build-on-data-update.yml#L68-L74.
        This is so that when breaking changes are made to the schema, the web application won't break.
        :param conceptrecid: the Zenodo Concept Record ID for the COKI Open Access Dataset. The Concept Record ID is
        the last set of numbers from the Concept DOI.
        :param zenodo_host: the Zenodo hostname, can be changed to https://sandbox.zenodo.org for testing.
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
            airflow_conns = [self.GITHUB_TOKEN_CONN, self.ZENODO_TOKEN_CONN]

        super().__init__(
            dag_id=dag_id,
            start_date=start_date,
            schedule_interval=schedule_interval,
            catchup=catchup,
            airflow_vars=airflow_vars,
            airflow_conns=airflow_conns,
            tags=[Tag.academic_observatory],
        )
        self.input_project_id = input_project_id
        self.output_project_id = output_project_id
        self.agg_dataset_id = agg_dataset_id
        self.ror_dataset_id = ror_dataset_id
        self.settings_dataset_id = settings_dataset_id
        self.table_ids = table_ids
        self.version = version
        self.conceptrecid = conceptrecid
        self.zenodo_host = zenodo_host
        if table_ids is None:
            self.table_ids = ["country", "institution"]

        self.add_operator(
            ExternalTaskSensor(task_id=f"{ext_dag_id}_sensor", external_dag_id=ext_dag_id, mode="reschedule")
        )
        self.add_setup_task(self.check_dependencies)
        self.add_task(self.query)
        self.add_task(self.download)
        self.add_task(self.make_draft_zenodo_version)
        self.add_task(self.download_twitter_cards)
        self.add_task(self.transform)
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

        release_date = make_release_date(**kwargs)
        data_bucket_name = Variable.get(self.DATA_BUCKET)
        zenodo_token = get_airflow_connection_password(self.ZENODO_TOKEN_CONN)
        zenodo = Zenodo(host=self.zenodo_host, access_token=zenodo_token)

        return OaWebRelease(
            dag_id=self.dag_id,
            data_bucket_name=data_bucket_name,
            release_date=release_date,
            zenodo=zenodo,
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
            project_id=self.input_project_id,
            dataset_id=self.ror_dataset_id,
            table_id=ror_table_id,
            end_date=release.release_date,
        )[0]
        ror_sharded_table_id = bigquery_sharded_table_id(ror_table_id, ror_release_date)

        for agg_table_id in self.table_ids:
            # Aggregate release dates
            agg_release_date = select_table_shard_dates(
                project_id=self.input_project_id,
                dataset_id=self.agg_dataset_id,
                table_id=agg_table_id,
                end_date=release.release_date,
            )[0]
            agg_sharded_table_id = bigquery_sharded_table_id(agg_table_id, agg_release_date)

            # Fetch data
            destination_uri = f"gs://{release.download_bucket}/{self.dag_id}/{release.release_id}/{agg_table_id}.jsonl"
            success = bq_query_to_gcs(
                query=QUERY.format(
                    start_year=START_YEAR,
                    project_id=self.input_project_id,
                    agg_dataset_id=self.agg_dataset_id,
                    agg_table_id=agg_sharded_table_id,
                    ror_dataset_id=self.ror_dataset_id,
                    ror_table_id=ror_sharded_table_id,
                    settings_dataset_id=self.settings_dataset_id,
                    country_table_id="country",
                ),
                project_id=self.output_project_id,
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

    def make_draft_zenodo_version(self, release: OaWebRelease, **kwargs):
        """Make a draft Zenodo version of the dataset.

        :param release: the release.
        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are
        passed to this argument.
        :return: None.
        """

        make_draft_version(release.zenodo, self.conceptrecid)

    def download_twitter_cards(self, release: OaWebRelease, **kwargs):
        """Download current twitter cards

        :param release: the release.
        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are
        passed to this argument.
        :return: None.
        """

        # Download twitter cards
        blob_name = "twitter.zip"
        file_path = os.path.join(release.transform_folder, blob_name)
        download_blob_from_cloud_storage(bucket_name=release.data_bucket_name, blob_name=blob_name, file_path=file_path)

        # Unzip into build
        unzip_folder_path = os.path.join(release.build_path)
        with ZipFile(file_path) as zip_file:
            zip_file.extractall(unzip_folder_path)

    def transform(self, release: OaWebRelease, **kwargs):
        """Transform the queried data into the final format for the open access website.

        :param release: the release.
        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are
        passed to this argument.
        :return: None.
        """

        # Get versions
        res = release.zenodo.get_versions(self.conceptrecid, all_versions=1)
        if res.status_code != 200:
            raise AirflowException(f"zenodo.get_versions status_code {res.status_code}")
        versions = res.json()

        # Make required folders
        all_entities = []
        countries = []
        institutions = []
        build_data_path = os.path.join(release.build_path, "data")
        os.makedirs(build_data_path, exist_ok=True)
        for category in self.table_ids:
            logging.info(f"Transforming {category} entity")
            # Load data
            path = os.path.join(release.download_folder, f"{category}.jsonl")
            df = self.load_data(path)

            # Pre-process data
            df = self.preprocess_df(category, df)

            # Make index table
            df_index_table = self.make_index(category, df)
            self.update_index_with_logos(release.build_path, release.assets_path, category, df_index_table)
            self.update_index_with_wiki_descriptions(df_index_table)
            entities = self.make_entities(category, df_index_table, df)

            # Make search index data for this category
            all_entities += entities

            # Save index
            index_path = os.path.join(build_data_path, f"{category}.json")
            self.save_index(index_path, entities)

            # Save entities
            entities_path = os.path.join(build_data_path, category)
            self.save_entities(entities_path, entities)
            logging.info(f"Saved transformed {category} entity")

            # Assign country or institution variables
            if category == "country":
                countries = entities
            elif category == "institution":
                institutions = entities
            else:
                raise AirflowException(f"Category type unknown: {category}")

        # Save all entities as json
        index_path = os.path.join(build_data_path, f"index.json")
        self.save_index(index_path, all_entities)

        # Save COKI Open Access Dataset
        coki_dataset_path = os.path.join(release.transform_folder, "coki-oa-dataset")
        self.save_coki_oa_dataset(coki_dataset_path, countries, institutions)

        # Make stats
        end_year = pendulum.now().year - 1
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
        stats = Stats(START_YEAR, end_year, last_updated, zenodo_versions, country_stats, institution_stats)
        stats_path = os.path.join(release.build_path, "data")
        self.save_stats(stats_path, stats)
        logging.info(f"Saved stats data")

        # Zip data
        dst = os.path.join(release.transform_folder, "latest")
        shutil.copytree(release.build_path, dst)
        shutil.make_archive(dst, "zip", dst)

    def publish_zenodo_version(self, release: OaWebRelease, **kwargs):
        """Publish the new Zenodo version of the dataset.

        :param release: the release.
        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are
        passed to this argument.
        :return: None.
        """

        zenodo = release.zenodo
        res = zenodo.get_versions(self.conceptrecid, all_versions=0)
        if res.status_code != 200:
            raise AirflowException(f"zenodo.get_versions status_code {res.status_code}")
        draft = res.json()[0]
        draft_id = draft["id"]
        if draft["state"] != "unsubmitted":
            raise AirflowException(f"Latest version is not a draft: {draft_id}")

        file_path = os.path.join(release.transform_folder, "coki-oa-dataset.zip")
        publish_new_version(release.zenodo, draft_id, file_path)

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

    ######################################
    # Other Functions
    ######################################

    def load_data(self, file_path: str) -> pd.DataFrame:
        """Load the data file for a given category.

        :param file_path: the path to the file to load.
        :return: the Pandas Dataframe.
        """

        data = load_jsonl(file_path)
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
        df_index_table = df_index_table[df_index_table["n_outputs"] >= INCLUSION_THRESHOLD[category]]

        # Add percentages to dataframe
        self.update_df_with_percentages(df_index_table, self.PERCENTAGE_FIELD_KEYS)

        # Sort from highest oa percentage to lowest
        df_index_table.sort_values(by=["n_outputs_open"], ascending=False, inplace=True)

        # Add category
        df_index_table["category"] = category

        # Remove date, year and repositories
        df_index_table.drop(columns=["date", "year", "repositories"], inplace=True)

        return df_index_table

    def update_df_with_percentages(self, df: pd.DataFrame, keys: List[Tuple[str, str]]):
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

    def update_index_with_logos(self, build_path: str, assets_path: str, category: str, df_index_table: pd.DataFrame):
        """Update the index with logos, downloading logos if they don't exist.

        :param build_path: the path to the build folder.
        :param assets_path: the path to oa-web-workflow assets folder which contains country flags.
        :param category: the category, i.e. country or institution.
        :param df_index_table: the index table Pandas dataframe.
        :return: None.
        """

        sizes = ["s", "l", "xl"]
        for size in sizes:
            base_path = os.path.join(build_path, "logos", category, size)
            os.makedirs(base_path, exist_ok=True)

        # Make logos
        if category == "country":
            logging.info("Copying local logos")
            country_sizes = sizes[:2]
            with ThreadPoolExecutor() as executor:
                futures = []
                # Copy and rename logo images from using alpha2 to alpha3 country codes
                for size in country_sizes:
                    base_path = os.path.join(build_path, "logos", category, size)
                    for alpha3, alpha2 in zip(df_index_table["id"], df_index_table["alpha2"]):
                        src_path = os.path.join(assets_path, "flags", size, f"{alpha2}.svg")
                        dst_path = os.path.join(base_path, f"{alpha3}.svg")
                        futures.append(executor.submit(shutil.copy, src_path, dst_path))
                [f.result() for f in as_completed(futures)]
            logging.info("Finished copying local logos")

            # Add logo urls to index
            for size in sizes:
                # For xl size point to l svg
                make_logo_url_size = size
                if size == "xl":
                    make_logo_url_size = "l"
                df_index_table[f"logo_{size}"] = df_index_table["id"].apply(
                    lambda country_code: make_logo_url(
                        category=category, entity_id=country_code, size=make_logo_url_size, fmt="svg"
                    )
                )

        elif category == "institution":
            # Get the institution logo and the path to the logo image
            logging.info("Downloading logos using Clearbit")
            institution_sizes = [("s", 32, "jpg"), ("l", 128, "jpg"), ("xl", 532, "png")]
            for size, width, fmt in institution_sizes:
                with ThreadPoolExecutor() as executor:
                    futures = []
                    logo_paths = []
                    for ror_id, url in zip(df_index_table["id"], df_index_table["url"]):
                        if url:
                            url = clean_url(url)
                            futures.append(
                                executor.submit(get_institution_logo, ror_id, url, size, width, fmt, build_path)
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

    def save_index(self, file_path: str, entities: List[Entity]):
        """Save an index file.

        :param file_path: the file path where the index should be saved.
        :param entities: a list of entities.
        :return: None.
        """

        # Convert entities to dictionaries and select a subset of fields
        subset = {
            "id": None,
            "name": None,
            "logo_s": None,
            "category": None,
            "country": None,
            "subregion": None,
            "region": None,
            "institution_types": None,
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
        data = [select_subset(entity.to_dict(), subset) for entity in entities]

        # Save as JSON
        save_json(file_path, data)

    def make_entities(self, category: str, df_index_table: pd.DataFrame, df: pd.DataFrame) -> List[Entity]:
        """Make entities.

        :param category: the entity category.
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
            if total_outputs >= INCLUSION_THRESHOLD[category]:
                self.update_df_with_percentages(df_group, self.PERCENTAGE_FIELD_KEYS)
                df_group = df_group.sort_values(by=[key_year])
                df_group = df_group.loc[:, ~df_group.columns.str.contains("^Unnamed")]

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
                entity.years = years

                # Make repositories
                def sort_key(col):
                    if is_string_dtype(col.dtype):
                        return col.str.lower()
                    return col

                def merge_category(cat):
                    if cat in {"Aggregator", "Unknown"}:
                        return "Other Internet"
                    return cat

                repositories = []
                for row in rows:
                    repositories += row["repositories"]
                df_repos = pd.DataFrame(repositories, columns=["id", "total_outputs", "category", "home_repo"])
                df_repos["total_outputs"] = pd.to_numeric(df_repos["total_outputs"])
                df_repos["category"] = df_repos["category"].apply(merge_category)
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
                    .sort_values(by=["total_outputs", "id"], ascending=[False, True], inplace=False, key=sort_key)
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

        # Ensure that entities are sorted based on p_outputs_open
        entities = sorted(entities, key=lambda e: e.stats.p_outputs_open, reverse=True)

        return entities

    def save_entities(self, path: str, entities: List[Entity]):
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

    def save_coki_oa_dataset(self, path: str, countries: List[Entity], institutions: List[Entity]):
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
            "country": None,
            "subregion": None,
            "region": None,
            "institution_types": None,
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
        rendered = template.render(
            year=pendulum.now().year, n_countries=len(countries), n_institutions=len(institutions)
        )
        with open(file_path, mode="w") as f:
            f.write(rendered)

        # Zip
        shutil.make_archive(path, "zip", path)

    def save_stats(self, path: str, stats: Stats):
        """Save overall stats.

        :param path: the directory where the stats.json file should be saved.
        :param stats: stats object.
        :return: None.
        """

        os.makedirs(path, exist_ok=True)

        # Save as JSON
        output_path = os.path.join(path, "stats.json")
        save_json(output_path, stats.to_dict())
