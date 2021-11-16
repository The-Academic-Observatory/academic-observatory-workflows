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

from typing import Optional, Tuple, Union

import pendulum
from connexion import request

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError
from flask import jsonify, current_app
from observatory.api.utils.exception_utils import APIError
from observatory.api.utils.elasticsearch_utils import create_es_connection, \
    ElasticsearchIndex, \
    create_search_body, process_response, parse_args
from observatory.api.utils.auth_utils import AuthError
from typing import Dict


class AOElasticsearchIndex(ElasticsearchIndex):
    def __init__(self, es: Elasticsearch, agg: str, subagg: str = None, index_date: str = None):
        super().__init__(es, agg, subagg, index_date)

    @property
    def agg_mappings(self) -> Dict[str, str]:
        """

        :return:
        """
        return {
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

    @property
    def subagg_mappings(self) -> Dict[str, str]:
        """

        :return:
        """
        return {
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

    @property
    def invalid_combinations(self) -> Dict[str, list]:
        """

        :return:
        """
        return {
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

    def get_required_scope(self) -> str:
        """

        :return:
        """
        scope = "group:COKI%20Team"
        return scope

    def set_alias(self) -> [bool, str]:
        """

        :return:
        """
        if self.subagg:
            # Check if combination of aggregate and subaggregate is valid
            if self.subagg in self.invalid_combinations[self.agg]:
                invalid_combinations_str = "\n".join(self.invalid_combinations[self.agg])
                raise AuthError({"code": "index_error",
                                 "description": "Combination of agg and subagg is invalid.\nInvalid "
                                                f"subaggregates for '{self.agg}': {invalid_combinations_str}"
                                 }
                                )
            alias = f"ao-{self.agg}-{self.subagg}"
        else:
            alias = f"ao-{self.agg}-unique-list"
        return alias


def create_schema():
    """Create schema for the given index that is queried. Useful if there are no results returned.

    :return: schema of index
    """
    return {"schema": "to_be_created"}


def initiate_elasticsearch_index(agg, subagg, index_date) -> ElasticsearchIndex:
    """

    :param agg:
    :param subagg:
    :param index_date:
    :return:
    """
    es = create_es_connection()
    es_index = AOElasticsearchIndex(es, agg, subagg, index_date)
    return es_index


def query_elasticsearch(agg: str, subagg: Optional[str]) -> Union[dict, Tuple[str, int]]:
    """

    :param agg:
    :param subagg:
    :return:
    """
    agg_ids, subagg_ids, index_date, from_date, to_date, size, search_after, pit_id, pretty_print = parse_args()

    es = create_es_connection()
    es_index = AOElasticsearchIndex(es, agg, subagg, index_date)

    search_body = create_search_body(
        es_index.agg_field, agg_ids, es_index.subagg_field, subagg_ids, from_date, to_date, size, search_after, pit_id
    )
    if pit_id:
        try:
            res = es.search(body=search_body)
        except NotFoundError:
            raise APIError({"code": "invalid_pit", "description": "Point In Time timed out"}, 500)
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
    if pretty_print:
        current_app.config["JSONIFY_PRETTYPRINT_REGULAR"] = pretty_print
    return jsonify(results)


def get_pit_id(agg: str, subagg: Optional[str]) -> Union[dict, Tuple[str, int]]:
    """

    :param agg:
    :param subagg:
    :return:
    """
    index_date = request.args.get("index_date")
    keep_alive = f"{request.args.get('keep_alive', 1)}m"

    # Convert index date to YYYYMMDD format
    index_date = pendulum.parse(index_date).strftime("%Y%m%d") if index_date else None

    es = create_es_connection()
    es_index = AOElasticsearchIndex(es, agg, subagg, index_date)

    pit_id = es.open_point_in_time(index=es_index.name, keep_alive=keep_alive)["id"]
    return {"pit_id": pit_id, "keep_alive": keep_alive, "index": es_index.name}
