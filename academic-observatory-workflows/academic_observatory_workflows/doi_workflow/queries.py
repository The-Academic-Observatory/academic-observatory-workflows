# Copyright 2020-2024 Curtin University
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

from __future__ import annotations

import copy
import logging
from dataclasses import dataclass
from typing import Dict, List, Set

import pendulum
import requests
from airflow.exceptions import AirflowException

from observatory_platform.google.bigquery import bq_select_table_shard_dates, bq_table_id
from observatory_platform.url_utils import retry_get_url

MAX_QUERIES = 100


@dataclass
class Table:
    project_id: str
    dataset_id: str
    table_name: str = None
    sharded: bool = False
    snapshot_date: pendulum.DateTime = None

    @property
    def table_id(self):
        """Generates the BigQuery table_id for both sharded and non-sharded tables.

        :return: BigQuery table_id.
        """

        if self.sharded:
            return f"{self.table_name}{self.snapshot_date.strftime('%Y%m%d')}"
        return self.table_name


@dataclass
class SQLQuery:
    name: str
    inputs: Dict = None
    output_table: Table = None
    output_clustering_fields: List = None


@dataclass
class Aggregation:
    table_name: str
    aggregation_field: str
    group_by_time_field: str = "published_year"
    relate_to_institutions: bool = False
    relate_to_countries: bool = False
    relate_to_groups: bool = False
    relate_to_members: bool = False
    relate_to_journals: bool = False
    relate_to_funders: bool = False
    relate_to_publishers: bool = False


def make_sql_queries(
    input_project_id: str,
    output_project_id: str,
    dataset_id_crossref_events: str = "crossref_events",
    dataset_id_crossref_metadata: str = "crossref_metadata",
    dataset_id_crossref_fundref: str = "crossref_fundref",
    dataset_id_ror: str = "ror",
    dataset_id_orcid: str = "orcid",
    dataset_id_open_citations: str = "open_citations",
    dataset_id_unpaywall: str = "unpaywall",
    dataset_id_scihub: str = "scihub",
    dataset_id_openalex: str = "openalex",
    dataset_id_pubmed: str = "pubmed",
    dataset_id_settings: str = "settings",
    dataset_id_observatory: str = "observatory",
    dataset_id_observatory_intermediate: str = "observatory_intermediate",
) -> List[List[SQLQuery]]:
    return [
        [
            SQLQuery(
                "crossref_metadata",
                inputs={
                    "crossref_metadata": Table(
                        input_project_id, dataset_id_crossref_metadata, "crossref_metadata", sharded=True
                    )
                },
                output_table=Table(output_project_id, dataset_id_observatory_intermediate, "crossref_metadata"),
                output_clustering_fields=["doi"],
            )
        ],
        [
            SQLQuery(
                "crossref_events",
                inputs={"crossref_events": Table(input_project_id, dataset_id_crossref_events, "crossref_events")},
                output_table=Table(output_project_id, dataset_id_observatory_intermediate, "crossref_events"),
                output_clustering_fields=["doi"],
            ),
            SQLQuery(
                "crossref_fundref",
                inputs={
                    "crossref_fundref": Table(
                        input_project_id, dataset_id_crossref_fundref, "crossref_fundref", sharded=True
                    ),
                    "crossref_metadata": Table(
                        output_project_id, dataset_id_observatory_intermediate, "crossref_metadata", sharded=True
                    ),
                },
                output_table=Table(output_project_id, dataset_id_observatory_intermediate, "crossref_fundref"),
                output_clustering_fields=["doi"],
            ),
            SQLQuery(
                "ror",
                inputs={
                    "ror": Table(input_project_id, dataset_id_ror, "ror", sharded=True),
                    "country": Table(input_project_id, dataset_id_settings, "country"),
                },
                output_table=Table(output_project_id, dataset_id_observatory_intermediate, "ror"),
            ),
            SQLQuery(
                "orcid",
                inputs={"orcid": Table(input_project_id, dataset_id_orcid, "orcid")},
                output_table=Table(output_project_id, dataset_id_observatory_intermediate, "orcid"),
                output_clustering_fields=["doi"],
            ),
            SQLQuery(
                "open_citations",
                inputs={
                    "open_citations": Table(input_project_id, dataset_id_open_citations, "open_citations", sharded=True)
                },
                output_table=Table(output_project_id, dataset_id_observatory_intermediate, "open_citations"),
                output_clustering_fields=["doi"],
            ),
            SQLQuery(
                "openaccess",
                inputs={
                    "scihub": Table(input_project_id, dataset_id_scihub, "scihub", sharded=True),
                    "unpaywall": Table(input_project_id, dataset_id_unpaywall, "unpaywall", sharded=False),
                    "ror": Table(input_project_id, dataset_id_ror, "ror", sharded=True),
                    "repository": Table(input_project_id, dataset_id_settings, "repository"),
                    "repository_institution_to_ror": Table(
                        output_project_id,
                        dataset_id_observatory_intermediate,
                        "repository_institution_to_ror",
                        sharded=True,
                    ),
                    "crossref_metadata": Table(
                        output_project_id, dataset_id_observatory_intermediate, "crossref_metadata", sharded=True
                    ),
                },
                output_table=Table(output_project_id, dataset_id_observatory_intermediate, "openaccess"),
                output_clustering_fields=["doi"],
            ),
            SQLQuery(
                "openalex",
                inputs={"openalex_works": Table(input_project_id, dataset_id_openalex, "works", sharded=True)},
                output_table=Table(output_project_id, dataset_id_observatory_intermediate, "openalex"),
                output_clustering_fields=["doi"],
            ),
            SQLQuery(
                "pubmed",
                inputs={"pubmed": Table(input_project_id, dataset_id_pubmed, "pubmed", sharded=False)},
                output_table=Table(output_project_id, dataset_id_observatory_intermediate, "pubmed"),
                output_clustering_fields=["doi"],
            ),
        ],
        [
            SQLQuery(
                "doi",
                inputs={
                    "openalex": Table(output_project_id, dataset_id_observatory_intermediate, "openalex", sharded=True),
                    "ror_hierarchy": Table(
                        output_project_id, dataset_id_observatory_intermediate, "ror_hierarchy", sharded=True
                    ),
                    "openaccess": Table(
                        output_project_id, dataset_id_observatory_intermediate, "openaccess", sharded=True
                    ),
                    "unpaywall": Table(input_project_id, dataset_id_unpaywall, "unpaywall"),
                    "open_citations": Table(
                        output_project_id, dataset_id_observatory_intermediate, "open_citations", sharded=True
                    ),
                    "crossref_events": Table(
                        output_project_id, dataset_id_observatory_intermediate, "crossref_events", sharded=True
                    ),
                    "pubmed": Table(output_project_id, dataset_id_observatory_intermediate, "pubmed", sharded=True),
                    "crossref_metadata": Table(
                        output_project_id, dataset_id_observatory_intermediate, "crossref_metadata", sharded=True
                    ),
                    "ror": Table(output_project_id, dataset_id_observatory_intermediate, "ror", sharded=True),
                    "groupings": Table(input_project_id, dataset_id_settings, "groupings"),
                    "crossref_fundref": Table(
                        output_project_id, dataset_id_observatory_intermediate, "crossref_fundref", sharded=True
                    ),
                    "orcid": Table(output_project_id, dataset_id_observatory_intermediate, "orcid", sharded=True),
                },
                output_table=Table(output_project_id, dataset_id_observatory, "doi"),
                output_clustering_fields=["doi"],
            )
        ],
    ]


def fetch_ror_affiliations(repository_institution: str, num_retries: int = 3) -> Dict:
    """Fetch the ROR affiliations for a given affiliation string.

    :param repository_institution: the affiliation string to search with.
    :param num_retries: the number of retries.
    :return: the list of ROR affiliations.
    """

    print(f"fetch_ror_affiliations: {repository_institution}")
    rors = []
    try:
        response = retry_get_url(
            f"https://api.ror.org/v2/organizations",
            num_retries=num_retries,
            params={"affiliation": repository_institution},
        )
        items = response.json()["items"]

        # We execute a search of their API, which returns a "score" response.
        # We will use the response with the highest score.
        scores = [float(i["score"]) for i in items]
        try:
            i = scores.index(max(scores))
        except ValueError:
            logging.warning(f"No matching items found for institution: {repository_institution}")
            return {"repository_institution": repository_institution, "rors": []}
        matched_item = items[i]

        if not matched_item["organization"].get("id"):
            logging.warning(f"No ID found for item in instution: {repository_institution}")
            return {"repository_institution": repository_institution, "rors": []}

        display_found = False
        for name_obj in matched_item["organization"]["names"]:
            # The ror_display is what ROR uses as the display name. We will borrow this.
            if "ror_display" in name_obj["types"]:
                rors.append({"id": matched_item["organization"]["id"], "name": name_obj["value"]})
                display_found = True
                break

        # Use the first item in the list (if there are any items) if no display value found
        if not display_found:
            if matched_item["organization"].get("names"):
                rors.append(
                    {
                        "id": matched_item["organization"]["id"],
                        "name": matched_item["organization"]["names"][0]["value"],
                    }
                )

    except requests.exceptions.HTTPError as e:
        # If the repository_institution string causes a 500 error with the ROR affiliation matcher
        # Then catch the error and continue as if no ROR ids were matched for this entry.
        logging.error(f"requests.exceptions.HTTPError fetch_ror_affiliations error fetching: {e}")
        # TODO: it would be better to re-raise the error when it isn't a 500 error as something else is likely wrong

    return {"repository_institution": repository_institution, "rors": rors}


def get_snapshot_date(project_id: str, dataset_id: str, table_id: str, snapshot_date: pendulum.DateTime):
    # Get last table shard date before current end date
    logging.info(f"get_snapshot_date {project_id}.{dataset_id}.{table_id} {snapshot_date}")
    table_id = bq_table_id(project_id, dataset_id, table_id)
    table_shard_dates = bq_select_table_shard_dates(table_id=table_id, end_date=snapshot_date)

    if len(table_shard_dates):
        shard_date = table_shard_dates[0]
    else:
        raise AirflowException(f"{table_id} with a table shard date <= {snapshot_date} not found")

    return shard_date


def traverse_ancestors(index: Dict, child_ids: Set):
    """Traverse all of the ancestors of a set of child ROR ids.

    :param index: the index.
    :param child_ids: the child ids.
    :return: all of the ancestors of all child ids.
    """

    ancestors = child_ids.copy()
    for child_id in child_ids:
        parents = index[child_id]

        if not len(parents):
            continue

        child_ancestors = traverse_ancestors(index, parents)
        ancestors = ancestors.union(child_ancestors)

    return ancestors


def ror_to_ror_hierarchy_index(ror: List[Dict]) -> Dict:
    """Make an index of child to ancestor relationships.

    :param ror: the ROR dataset as a list of dicts.
    :return: the index.
    """

    index = {}
    names = {}
    # Add all items to index
    for row in ror:
        ror_id = row["id"]
        index[ror_id] = set()
        names[ror_id] = row["name"]

    # Add all child -> parent relationships to index
    for row in ror:
        ror_id = row["id"]
        name = row["name"]

        # Build index of parents and children
        parents = set()
        children = set()
        for rel in row["relationships"]:
            rel_id = rel["id"]
            rel_type = rel["type"]
            if rel_type == "Parent":
                parents.add(rel_id)
            elif rel_type == "Child":
                children.add(rel_id)

        for rel in row["relationships"]:
            rel_id = rel["id"]
            rel_type = rel["type"]
            rel_label = rel["label"]

            if rel_id in parents and rel_id in children:
                # Prevents infinite recursion
                logging.warning(
                    f"Skipping as: org({rel_id}, {rel_label}) is both a parent and a child of: org({ror_id}, {name})"
                )
                continue

            if rel_type == "Parent":
                if ror_id in index:
                    index[ror_id].add(rel_id)
                else:
                    logging.warning(
                        f"Parent does not exist in database for relationship: parent({rel_id}, {rel_label}), child({ror_id}, {name})"
                    )

            elif rel_type == "Child":
                if rel_id in index:
                    index[rel_id].add(ror_id)
                else:
                    logging.warning(
                        f"Child does not exist in database for relationship: parent({ror_id}, {name}), child({rel_id}, {rel_label})"
                    )

    # Convert parent sets to ancestor sets
    ancestor_index = copy.deepcopy(index)
    for ror_id in index.keys():
        parents = index[ror_id]
        ancestors = traverse_ancestors(index, parents)
        ancestor_index[ror_id] = ancestors

    return ancestor_index
