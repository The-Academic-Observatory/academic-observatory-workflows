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
#
#
# Author: Tuan Chien


from collections import OrderedDict
from observatory.api.client.model.table_type import TableType
from observatory.api.client.model.dataset_type import DatasetType
from observatory.api.utils import get_api_client, seed_dataset_type, get_table_type_ids
from observatory.api.client.api.observatory_api import ObservatoryApi


def get_dataset_type_info(api: ObservatoryApi):
    ttids = get_table_type_ids(api)

    dataset_type_info = OrderedDict()
    dataset_type_info["crossref_events"] = DatasetType(type_id="crossref_events", name="Crossref Events", table_type=TableType(id=ttids["regular"]))
    dataset_type_info["crossref_fundref"] = DatasetType(type_id="crossref_fundref", name="Crossref Fundref", table_type=TableType(id=ttids["sharded"]))
    dataset_type_info["crossref_metadata"] = DatasetType(type_id="crossref_metadata", name="Crossref Metadata", table_type=TableType(id=ttids["sharded"]))
    dataset_type_info["geonames"] = DatasetType(type_id="geonames", name="Geonames", table_type=TableType(id=ttids["sharded"]))
    dataset_type_info["grid"] = DatasetType(type_id="grid", name="GRID", table_type=TableType(id=ttids["sharded"]))
    dataset_type_info["open_citations"] = DatasetType(type_id="open_citations", name="Open Citations", table_type=TableType(id=ttids["sharded"]))
    dataset_type_info["openalex"] = DatasetType(type_id="openalex", name="OpenAlex", table_type=TableType(id=ttids["regular"]))
    dataset_type_info["openalex_author"] = DatasetType(type_id="openalex_author", name="OpenAlex Author", table_type=TableType(id=ttids["regular"]))
    dataset_type_info["openalex_concept"] = DatasetType(type_id="openalex_concept", name="OpenAlex Concept", table_type=TableType(id=ttids["regular"]))
    dataset_type_info["openalex_institution"] = DatasetType(type_id="openalex_institution", name="OpenAlex Institution", table_type=TableType(id=ttids["regular"]))
    dataset_type_info["openalex_venue"] = DatasetType(type_id="openalex_venue", name="OpenAlex Venue", table_type=TableType(id=ttids["regular"]))
    dataset_type_info["openalex_work"] = DatasetType(type_id="openalex_work", name="OpenAlex Work", table_type=TableType(id=ttids["regular"]))
    dataset_type_info["orcid"] = DatasetType(type_id="orcid", name="ORCID", table_type=TableType(id=ttids["regular"]))
    dataset_type_info["ror"] = DatasetType(type_id="ror", name="Research Organisation Registry", table_type=TableType(id=ttids["sharded"]))
    dataset_type_info["scopus"] = DatasetType(type_id="scopus", name="SCOPUS", table_type=TableType(id=ttids["sharded"]))
    dataset_type_info["unpaywall_snapshot"] = DatasetType(type_id="unpaywall_snapshot", name="Unpaywall (snapshot)", table_type=TableType(id=ttids["sharded"]))
    dataset_type_info["unpaywall"] = DatasetType(type_id="unpaywall", name="Unpaywall (daily feeds)", table_type=TableType(id=ttids["regular"]))
    dataset_type_info["web_of_science"] = DatasetType(type_id="web_of_science", name="Web of Science", table_type=TableType(id=ttids["sharded"]))
    dataset_type_info["doi"] = DatasetType(type_id="doi", name="DOI Workflow", table_type=TableType(id=ttids["sharded"]))
    return dataset_type_info


if __name__ == "__main__":
    api = get_api_client()
    dataset_type_info = get_dataset_type_info(api)
    seed_dataset_type(api=api, dataset_type_info=dataset_type_info)
