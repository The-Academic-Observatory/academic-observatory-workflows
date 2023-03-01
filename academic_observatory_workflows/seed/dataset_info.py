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

from academic_observatory_workflows.api_type_ids import DatasetTypeId
from observatory.api.client.api.observatory_api import ObservatoryApi
from observatory.api.client.model.dataset import Dataset
from observatory.api.utils import get_workflows, get_dataset_type


def get_dataset_info(api: ObservatoryApi):
    workflows = get_workflows(api=api)

    dataset_info = OrderedDict()
    dataset_info["Crossref Events Dataset"] = Dataset(
        name="Crossref Events Dataset",
        service="google",
        address="academic-observatory.crossref.crossref_events",
        workflow=workflows["Crossref Events Telescope"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.crossref_events),
    )
    dataset_info["Crossref Fundref Dataset"] = Dataset(
        name="Crossref Fundref Dataset",
        service="google",
        address="academic-observatory.crossref.crossref_fundref",
        workflow=workflows["Crossref Fundref Telescope"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.crossref_fundref),
    )
    dataset_info["Crossref Metadata Dataset"] = Dataset(
        name="Crossref Metadata Dataset",
        service="google",
        address="academic-observatory.crossref.crossref_metadata",
        workflow=workflows["Crossref Metadata Telescope"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.crossref_metadata),
    )
    dataset_info["Geonames Dataset"] = Dataset(
        name="Geonames Dataset",
        service="google",
        address="academic-observatory.geonames.geonames",
        workflow=workflows["Geonames Telescope"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.geonames),
    )
    dataset_info["GRID Dataset"] = Dataset(
        name="GRID Dataset",
        service="google",
        address="academic-observatory.digital_science.grid",
        workflow=workflows["GRID Telescope"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.grid),
    )
    dataset_info["Open Citations Dataset"] = Dataset(
        name="Open Citations Dataset",
        service="google",
        address="academic-observatory.open_citations.open_citations",
        workflow=workflows["Open Citations Telescope"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.open_citations),
    )
    dataset_info["OpenAlex Dataset"] = Dataset(
        name="OpenAlex Dataset",
        service="google",
        address="academic-observatory.openalex.openalex",
        workflow=workflows["OpenAlex Telescope"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.openalex),
    )
    dataset_info["OpenAlex Author Dataset"] = Dataset(
        name="OpenAlex Author Dataset",
        service="google",
        address="academic-observatory.openalex.Author",
        workflow=workflows["OpenAlex Telescope"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.openalex_author),
    )
    dataset_info["OpenAlex Concept Dataset"] = Dataset(
        name="OpenAlex Concept Dataset",
        service="google",
        address="academic-observatory.openalex.Concept",
        workflow=workflows["OpenAlex Telescope"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.openalex_concept),
    )
    dataset_info["OpenAlex Institution Dataset"] = Dataset(
        name="OpenAlex Institution Dataset",
        service="google",
        address="academic-observatory.openalex.Institution",
        workflow=workflows["OpenAlex Telescope"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.openalex_institution),
    )
    dataset_info["OpenAlex Venue Dataset"] = Dataset(
        name="OpenAlex Venue Dataset",
        service="google",
        address="academic-observatory.openalex.Venue",
        workflow=workflows["OpenAlex Telescope"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.openalex_venue),
    )
    dataset_info["OpenAlex Work Dataset"] = Dataset(
        name="OpenAlex Work Dataset",
        service="google",
        address="academic-observatory.openalex.Work",
        workflow=workflows["OpenAlex Telescope"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.openalex_work),
    )
    dataset_info["ORCID Dataset"] = Dataset(
        name="ORCID Dataset",
        service="google",
        address="academic-observatory.orcid.orcid",
        workflow=workflows["ORCID Telescope"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.orcid),
    )
    dataset_info["ROR Dataset"] = Dataset(
        name="ROR Dataset",
        service="google",
        address="academic-observatory.ror.ror",
        workflow=workflows["ROR Telescope"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.ror),
    )
    dataset_info["Unpaywall Snapshot Dataset"] = Dataset(
        name="Unpaywall Snapshot Dataset",
        service="google",
        address="academic-observatory.our_research.unpaywall_snapshot",
        workflow=workflows["Unpaywall Snapshot Telescope"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.unpaywall_snapshot),
    )
    dataset_info["Unpaywall Dataset"] = Dataset(
        name="Unpaywall Dataset",
        service="google",
        address="academic-observatory.our_research.unpaywall",
        workflow=workflows["Unpaywall Telescope"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.unpaywall),
    )
    dataset_info["DOI Workflow Dataset"] = Dataset(
        name="DOI Workflow Dataset",
        service="google",
        address="academic-observatory.observatory.doi",
        workflow=workflows["DOI Workflow"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.doi_workflow),
    )
    dataset_info["Curtin Scopus Dataset"] = Dataset(
        name="Curtin Scopus Dataset",
        service="google",
        address="academic-observatory.elsevier.scopus",
        workflow=workflows["Curtin Scopus Telescope"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.scopus),
    )
    dataset_info["Curtin Web of Science Dataset"] = Dataset(
        name="Curtin Web of Science Dataset",
        service="google",
        address="academic-observatory.clarivate.web_of_science",
        workflow=workflows["Curtin Web of Science Telescope"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.web_of_science),
    )

    dataset_info["Pubmed Dataset"] = Dataset(
        name="PubMed Dataset",
        service="google",
        address="academic-observatory.pubmed.pubmed",
        workflow=workflows["PubMed Telescope"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.pubmed),
    )

    return dataset_info
