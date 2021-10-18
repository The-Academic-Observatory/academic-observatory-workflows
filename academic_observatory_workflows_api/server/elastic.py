# Copyright 2020 Curtin University
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

import os
from typing import List, Optional, Tuple, Union

import pendulum
from connexion import request
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError
from flask import Response


class ElasticsearchIndex:
    invalid_combinations = {
        "author": ["members"],
        "country": ["countries", "groupings", "institutions"],
        "funder": ["journals"],
        "group": ["countries", "groupings"],
        "institution": ["groupings", "members"],
        "journal": ["members", "publishers"],
        "publisher": ["journals", "members", "publishers"],
        "region": ["countries", "groupings", "institutions", "journals", "members"],
        "subregion": ["countries", "groupings", "institutions", "journals", "members"],
    }

    def __init__(self, es: Elasticsearch, agg: str, subagg: str = None, index_date: str = None):

        self.es = es
        self.agg = agg
        self.subagg = subagg
        self.error = None

        # Create alias from aggregate and subaggregate
        if self.subagg:
            # Check if combination of aggregate and subaggregate is valid
            if self.subagg in self.invalid_combinations[self.agg]:
                self.error = (
                    f"Index error: Combination of agg and subagg is invalid.<br>Invalid subaggregates "
                    f"for '{self.agg}':<br>- {'<br>- '.join(self.invalid_combinations[self.agg])}"
                )
                return
            self.alias = f"ao-{agg}-{subagg}"
        else:
            self.alias = f"ao-{agg}-unique-list"

        self.dates = list_available_index_dates(es, self.alias)
        if index_date and index_date not in self.dates:
            self.error = (
                f"Index error: Index not available for given index date: {index_date}.<br>Available index "
                f"dates:<br>- {'<br>- '.join(self.dates)}"
            )
        self.index_date = index_date if index_date else self.dates[0]

    @property
    def name(self) -> str:
        """"""
        return f"{self.alias}-{self.index_date}"

    @property
    def agg_field(self) -> str:
        """"""
        agg_mappings = {
            "author": "author_id",
            "country": "country_id",
            "funder": "funder_id",
            "group": "group_id",
            "institution": "institution_id",
            "journal": "journal_id",
            "publisher": "publisher_id",
            "region": "region_id",
            "subregion": "subregion_id",
        }
        return agg_mappings.get(self.agg)

    @property
    def subagg_field(self) -> Optional[str]:
        """"""

        subagg_mappings = {
            "access_types": "access_types_access_type",
            "countries": "countries_id",
            "disciplines": "disciplines_field",
            "events": "events_source",
            "funders": "funders_id",
            "groupings": "groupings_id",
            "institutions": "institutions_id",
            "journals": "journals_id",
            "members": "members_id",
            "metrics": None,
            "output_types": "output_types_output_type",
            "publishers": "publishers_id",
        }
        return subagg_mappings.get(self.subagg)


def parse_args() -> Tuple[List[str], List[str], str, str, str, int, str, str]:
    """Parse the arguments coming in from the request.
    alias: concatenate 'subset' and 'agg'
    index_date: directly from requests.args. None allowed
    from_date: from_date + '-12-31'. None allowed
    to_date: to_date + '-12-31'. None allowed
    filter_fields: directly from requests.args for each item in 'query_filter_parameters'. Empty dict allowed
    size: If 'limit' is given -> set to 'limit', can't be more than 10000. If no 'limit' -> 10000
    scroll_id: directly from requests.args

    :return: alias, index_date, from_date, to_date, filter_fields, size, scroll_id
    """

    agg_ids = request.args.getlist("agg_id")
    subagg_ids = request.args.getlist("subagg_id")
    index_date = request.args.get("index_date")
    from_date = request.args.get("from")
    to_date = request.args.get("to")
    limit = request.args.get("limit")
    search_after = request.args.get("search_after")
    pit = request.args.get("pit")

    # Convert index date to YYYYMMDD format
    index_date = pendulum.parse(index_date).strftime("%Y%m%d") if index_date else None

    # Convert from/to date to YYYY-12-31 format
    from_date = f"{from_date}-12-31" if from_date else None
    to_date = f"{to_date}-12-31" if to_date else None

    # Set size to limit when given and under 10000, else set size to 10000
    max_size = 10000
    if limit:
        limit = int(limit)
        size = min(max_size, limit)
    else:
        size = max_size

    return agg_ids, subagg_ids, index_date, from_date, to_date, size, search_after, pit


def create_es_connection() -> Union[Elasticsearch, str]:
    """Create an elasticsearch connection

    :return: elasticsearch connection
    """
    api_key = os.environ.get("ES_API_KEY")
    address = os.environ.get("ES_HOST")

    for value in [address, api_key]:
        if value is None or value == "":
            return "Environment variable(s) for Elasticsearch host and/or api key is empty"
    es = Elasticsearch(address, api_key=api_key)
    if not es.ping():
        return "Could not connect to elasticsearch server. Host and/or api_key are not empty, but might be invalid."
    return es


def list_available_index_dates(es: Elasticsearch, alias: str) -> List[str]:
    """For a given index name (e.g. journals-institution), list which dates are available

    :param es: elasticsearch connection
    :param alias: index alias
    :return: list of available dates for given index
    """
    available_dates = []
    # Search for indices that include alias, is not an exact match
    available_indices = es.cat.indices(alias, format="json")
    for index in available_indices:
        if index["index"].startswith(alias):
            index_date = index["index"][-8:]
            available_dates.append(index_date)
    return available_dates


def create_search_body(
        agg_field: str,
        agg_ids: List[str],
        subagg_field: str,
        subagg_ids: List[str],
        from_year: Union[str, None],
        to_year: Union[str, None],
        size: int,
        search_after: str = None,
        pit_id: str = None,
) -> dict:
    """Create a search body that is passed on to the elasticsearch 'search' method.

    :param from_year: Refers to published year, add to 'range'. Include results where published year >= from_year
    :param to_year: Refers to published year, add to 'rangen'. Include results where published year < to_year
    :param size: The returned size (number of hits)
    :return: search body
    """
    filter_list = []
    # Add filters for aggregate and subaggregate
    if agg_ids:
        filter_list.append({"terms": {f"{agg_field}.keyword": agg_ids}})
    if subagg_field and subagg_ids:  # ignore subagg ids for ao_*_metrics index
        filter_list.append({"terms": {f"{subagg_field}.keyword": subagg_ids}})

    # Add filters for year range
    if from_year or to_year:
        range_dict = {"range": {"published_year": {"format": "yyyy-MM-dd"}}}
        if from_year:
            range_dict["range"]["published_year"]["gte"] = from_year
        if to_year:
            range_dict["range"]["published_year"]["lte"] = to_year
        filter_list.append(range_dict)

    # Sort on agg id and subagg id if available
    search_body = {"size": size, "query": {"bool": {"filter": filter_list}}, "track_total_hits": True}

    # Use search after text to continue search
    if search_after:
        search_body["search_after"] = [search_after]

    # Use Point In Time id to query index frozen at specific time
    if pit_id:
        search_body["pit"] = {"id": pit_id, "keep_alive": "1m"}  # Extend PIT with 1m
        # Use _shard_doc to sort, more efficient but only available with PIT
        search_body["sort"] = "_shard_doc"
    else:
        # Use doc id to sort
        search_body["sort"] = "_id"
    return search_body


def process_response(res: dict) -> Tuple[Optional[str], Optional[str], list, Optional[str]]:
    """Get the scroll id and hits from the response of an elasticsearch search query.

    :param res: The response.
    :return: pit id, search after and hits
    """
    # TODO return only source fields?
    # # Return source fields only
    # source_hits = []
    # for hit in res["hits"]["hits"]:
    #     source = {}
    #     # Flatten nested dictionary '_source'
    #     for k, v in hit["_source"].items():
    #         source[k] = v
    #     source_hits.append(source)
    # search_after_text = None
    # if res["hits"]["hits"]:
    #     search_after = res["hits"]["hits"][-1]['sort']
    #     search_after_text = search_after[0]

    # Flatten nested dictionary '_source'
    hits = res["hits"]["hits"]
    for hit in hits:
        source = hit.pop("_source")
        for k, v in source.items():
            hit[k] = v

    # Get the sort value for the last item (sorted by aggregate id)
    search_after_text = None
    if hits:
        search_after = hits[-1]["sort"]
        search_after_text = search_after[0]

    # Get PIT id which might be updated after search
    new_pit_id = res.get("pit_id")

    # Get how long request took
    took = res.get("took")
    return new_pit_id, search_after_text, hits, took


def create_schema():
    """Create schema for the given index that is queried. Useful if there are no results returned.

    :return: schema of index
    """
    return {"schema": "to_be_created"}


def query_elasticsearch(agg: str, subagg: Optional[str]) -> Union[dict, Tuple[str, int]]:
    """"""
    agg_ids, subagg_ids, index_date, from_date, to_date, size, search_after, pit_id = parse_args()

    es = create_es_connection()
    if not isinstance(es, Elasticsearch):
        return es, 400
    es_index = ElasticsearchIndex(es, agg, subagg, index_date)
    if es_index.error:
        return Response(response=es_index.error, status=400, mimetype="text/html")
    search_body = create_search_body(
        es_index.agg_field, agg_ids, es_index.subagg_field, subagg_ids, from_date, to_date, size, search_after, pit_id
    )
    if pit_id:
        try:
            res = es.search(body=search_body)
        except NotFoundError:
            return "Error: Point In Time timed out", 400
    else:
        res = es.search(index=es_index.name, body=search_body)

    new_pit_id, search_after_text, results_data, took = process_response(res)
    number_total_results = res["hits"]["total"]["value"]

    results = {
        "version": "v1",
        "index": es_index.name,
        "pit": new_pit_id,
        "search_after": search_after_text,
        "returned_hits": len(results_data),
        "total_hits": number_total_results,
        "took": took,
        "schema": create_schema(),
        "results": results_data,
    }
    return results


def get_pit_id(agg: str, subagg: Optional[str]) -> Union[dict, Tuple[str, int]]:
    """"""
    index_date = request.args.get("index_date")
    keep_alive = f"{request.args.get('keep_alive', 1)}m"

    # Convert index date to YYYYMMDD format
    index_date = pendulum.parse(index_date).strftime("%Y%m%d") if index_date else None

    es = create_es_connection()
    es_index = ElasticsearchIndex(es, agg, subagg, index_date)
    if es_index.error:
        return Response(response=es_index.error, status=400, mimetype="text/html")

    pit_id = es.open_point_in_time(index=es_index.name, keep_alive=keep_alive)["id"]
    return {"pit_id": pit_id, "keep_alive": keep_alive, "index": es_index.name, "test": "test changes"}
