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

from academic_observatory_workflows.api_type_ids import DatasetTypeId, TableTypeId
from observatory.api.client.api.observatory_api import ObservatoryApi
from observatory.api.client.model.dataset_type import DatasetType
from observatory.api.client.model.table_type import TableType
from observatory.api.utils import get_table_type_ids


def get_dataset_type_info(api: ObservatoryApi):
    ttids = get_table_type_ids(api)

    dataset_type_info = OrderedDict()
    dataset_type_info[DatasetTypeId.crossref_events] = DatasetType(
        type_id=DatasetTypeId.crossref_events,
        name="Crossref Events",
        table_type=TableType(id=ttids[TableTypeId.regular]),
    )
    dataset_type_info[DatasetTypeId.crossref_fundref] = DatasetType(
        type_id=DatasetTypeId.crossref_fundref,
        name="Crossref Fundref",
        table_type=TableType(id=ttids[TableTypeId.sharded]),
    )
    dataset_type_info[DatasetTypeId.crossref_metadata] = DatasetType(
        type_id=DatasetTypeId.crossref_metadata,
        name="Crossref Metadata",
        table_type=TableType(id=ttids[TableTypeId.sharded]),
    )
    dataset_type_info[DatasetTypeId.geonames] = DatasetType(
        type_id=DatasetTypeId.geonames,
        name="Geonames",
        table_type=TableType(id=ttids[TableTypeId.sharded]),
    )
    dataset_type_info[DatasetTypeId.grid] = DatasetType(
        type_id=DatasetTypeId.grid,
        name="GRID",
        table_type=TableType(id=ttids[TableTypeId.sharded]),
    )
    dataset_type_info[DatasetTypeId.open_citations] = DatasetType(
        type_id=DatasetTypeId.open_citations,
        name="Open Citations",
        table_type=TableType(id=ttids[TableTypeId.sharded]),
    )
    dataset_type_info[DatasetTypeId.openalex] = DatasetType(
        type_id=DatasetTypeId.openalex,
        name="OpenAlex",
        table_type=TableType(id=ttids[TableTypeId.regular]),
    )
    dataset_type_info[DatasetTypeId.openalex_author] = DatasetType(
        type_id=DatasetTypeId.openalex_author,
        name="OpenAlex Author",
        table_type=TableType(id=ttids[TableTypeId.regular]),
    )
    dataset_type_info[DatasetTypeId.openalex_concept] = DatasetType(
        type_id=DatasetTypeId.openalex_concept,
        name="OpenAlex Concept",
        table_type=TableType(id=ttids[TableTypeId.regular]),
    )
    dataset_type_info[DatasetTypeId.openalex_institution] = DatasetType(
        type_id=DatasetTypeId.openalex_institution,
        name="OpenAlex Institution",
        table_type=TableType(id=ttids[TableTypeId.regular]),
    )
    dataset_type_info[DatasetTypeId.openalex_venue] = DatasetType(
        type_id=DatasetTypeId.openalex_venue,
        name="OpenAlex Venue",
        table_type=TableType(id=ttids[TableTypeId.regular]),
    )
    dataset_type_info[DatasetTypeId.openalex_work] = DatasetType(
        type_id=DatasetTypeId.openalex_work,
        name="OpenAlex Work",
        table_type=TableType(id=ttids[TableTypeId.regular]),
    )
    dataset_type_info[DatasetTypeId.orcid] = DatasetType(
        type_id=DatasetTypeId.orcid,
        name="ORCID",
        table_type=TableType(id=ttids[TableTypeId.regular]),
    )
    dataset_type_info[DatasetTypeId.ror] = DatasetType(
        type_id=DatasetTypeId.ror,
        name="Research Organisation Registry",
        table_type=TableType(id=ttids[TableTypeId.sharded]),
    )
    dataset_type_info[DatasetTypeId.scopus] = DatasetType(
        type_id=DatasetTypeId.scopus,
        name="SCOPUS",
        table_type=TableType(id=ttids[TableTypeId.sharded]),
    )
    dataset_type_info[DatasetTypeId.unpaywall_snapshot] = DatasetType(
        type_id=DatasetTypeId.unpaywall_snapshot,
        name="Unpaywall (snapshot)",
        table_type=TableType(id=ttids[TableTypeId.sharded]),
    )
    dataset_type_info[DatasetTypeId.unpaywall] = DatasetType(
        type_id=DatasetTypeId.unpaywall,
        name="Unpaywall (daily feeds)",
        table_type=TableType(id=ttids[TableTypeId.regular]),
    )
    dataset_type_info[DatasetTypeId.web_of_science] = DatasetType(
        type_id=DatasetTypeId.web_of_science,
        name="Web of Science",
        table_type=TableType(id=ttids[TableTypeId.sharded]),
    )
    dataset_type_info[DatasetTypeId.doi_workflow] = DatasetType(
        type_id=DatasetTypeId.doi_workflow,
        name="DOI Workflow",
        table_type=TableType(id=ttids[TableTypeId.sharded]),
    )
    return dataset_type_info
