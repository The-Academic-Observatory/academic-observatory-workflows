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
from observatory.api.client.model.workflow import Workflow
from observatory.api.client.model.workflow_type import WorkflowType
from observatory.api.client.model.organisation import Organisation
from observatory.api.utils import get_api_client, get_workflow_type_ids, seed_workflow, get_organisation_ids
from observatory.api.client.api.observatory_api import ObservatoryApi


def get_workflow_info(api: ObservatoryApi):
    wftids = get_workflow_type_ids(api)
    orgids = get_organisation_ids(api)

    workflow_info = OrderedDict()
    workflow_info["Crossref Events Telescope"] = Workflow(name="Crossref Events Telescope", workflow_type=WorkflowType(id=wftids["crossref_events"]), extra={}, tags=None)
    workflow_info["Crossref Fundref Telescope"] = Workflow(name="Crossref Fundref Telescope", workflow_type=WorkflowType(id=wftids["crossref_fundref"]), extra={}, tags=None)
    workflow_info["Crossref Metadata Telescope"] = Workflow(name="Crossref Metadata Telescope", workflow_type=WorkflowType(id=wftids["crossref_metadata"]), extra={}, tags=None)
    workflow_info["Geonames Telescope"] = Workflow(name="Geonames Telescope", workflow_type=WorkflowType(id=wftids["geonames"]), extra={}, tags=None)
    workflow_info["GRID Telescope"] = Workflow(name="GRID Telescope", workflow_type=WorkflowType(id=wftids["grid"]), extra={}, tags=None)
    workflow_info["Open Citations Telescope"] = Workflow(name="Open Citations Telescope", workflow_type=WorkflowType(id=wftids["open_citations"]), extra={}, tags=None)
    workflow_info["OpenAlex Telescope"] = Workflow(name="OpenAlex Telescope", workflow_type=WorkflowType(id=wftids["openalex"]), extra={}, tags=None)
    workflow_info["ORCID Telescope"] = Workflow(name="ORCID Telescope", workflow_type=WorkflowType(id=wftids["orcid"]), extra={}, tags=None)
    workflow_info["ROR Telescope"] = Workflow(name="ROR Telescope", workflow_type=WorkflowType(id=wftids["ror"]), extra={}, tags=None)
    workflow_info["Unpaywall Snapshot Telescope"] = Workflow(name="Unpaywall Snapshot Telescope", workflow_type=WorkflowType(id=wftids["unpaywall_snapshot"]), extra={}, tags=None)
    workflow_info["Unpaywall Telescope"] = Workflow(name="Unpaywall Telescope", workflow_type=WorkflowType(id=wftids["unpaywall"]), extra={}, tags=None)
    workflow_info["DOI Workflow"] = Workflow(name="DOI Workflow", workflow_type=WorkflowType(id=wftids["doi"]), extra={}, tags=None)
    workflow_info["Curtin Scopus Telescope"] = Workflow(
        name="Curtin Scopus Telescope",
        workflow_type=WorkflowType(id=wftids["scopus"]),
        tags=None,
        extra={
            "airflow_connections": [
                "curtin_scopus_1",
                "curtin_scopus_2",
                "curtin_scopus_3",
            ],
            "earliest_date": "1966-01-01",
            "institution_ids": ["60031226"],
            "view": "STANDARD",
        }
    )
    workflow_info["Curtin Web of Science Telescope"] = Workflow(
            name="Curtin Web of Science Telescope",
            organisation=Organisation(id=orgids["Curtin University"]),
            workflow_type=WorkflowType(id=wftids["web_of_science"]),
            extra={
                "airflow_connections": ["curtin_wos"],
                "earliest_date": "1966-01-01",
                "institution_ids": [
                    "Curtin University",
                    "Curtin University of Technology",
                    "Western Australian Institute of Technology",
                ],
            },
            tags=None,
    )

    return workflow_info


if __name__ == "__main__":
    api = get_api_client()
    workflow_info = get_workflow_info(api)
    seed_workflow(api=api, workflow_info=workflow_info)
