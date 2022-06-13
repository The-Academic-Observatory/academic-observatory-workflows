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

from academic_observatory_workflows.api_type_ids import WorkflowTypeId
from observatory.api.client.api.observatory_api import ObservatoryApi
from observatory.api.client.model.organisation import Organisation
from observatory.api.client.model.workflow import Workflow
from observatory.api.client.model.workflow_type import WorkflowType
from observatory.api.utils import get_workflow_type_ids, get_organisation_ids


def get_workflow_info(api: ObservatoryApi):
    wftids = get_workflow_type_ids(api)
    orgids = get_organisation_ids(api)

    workflow_info = OrderedDict()
    name = "Crossref Events Telescope"
    workflow_info[name] = Workflow(
        name=name,
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.crossref_events]),
        extra={},
        tags=None,
    )
    name = "Crossref Fundref Telescope"
    workflow_info[name] = Workflow(
        name=name,
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.crossref_fundref]),
        extra={},
        tags=None,
    )
    name = "Crossref Metadata Telescope"
    workflow_info[name] = Workflow(
        name=name,
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.crossref_metadata]),
        extra={},
        tags=None,
    )
    name = "Geonames Telescope"
    workflow_info[name] = Workflow(
        name=name,
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.geonames]),
        extra={},
        tags=None,
    )
    name = "GRID Telescope"
    workflow_info[name] = Workflow(
        name=name,
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.grid]),
        extra={},
        tags=None,
    )
    name = "Open Citations Telescope"
    workflow_info[name] = Workflow(
        name="Open Citations Telescope",
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.open_citations]),
        extra={},
        tags=None,
    )
    name = "OpenAlex Telescope"
    workflow_info[name] = Workflow(
        name=name,
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.openalex]),
        extra={},
        tags=None,
    )
    name = "ORCID Telescope"
    workflow_info[name] = Workflow(
        name=name,
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.orcid]),
        extra={},
        tags=None,
    )
    name = "ROR Telescope"
    workflow_info[name] = Workflow(
        name=name,
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.ror]),
        extra={},
        tags=None,
    )
    name = "Unpaywall Snapshot Telescope"
    workflow_info[name] = Workflow(
        name=name,
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.unpaywall_snapshot]),
        extra={},
        tags=None,
    )
    name = "Unpaywall Telescope"
    workflow_info[name] = Workflow(
        name=name,
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.unpaywall]),
        extra={},
        tags=None,
    )
    name = "DOI Workflow"
    workflow_info[name] = Workflow(
        name=name,
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.doi_workflow]),
        extra={},
        tags=None,
    )
    name = "Curtin Scopus Telescope"
    workflow_info[name] = Workflow(
        name=name,
        organisation=Organisation(id=orgids["Curtin University"]),
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.scopus]),
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
        },
    )
    name = "Curtin Web of Science Telescope"
    workflow_info[name] = Workflow(
        name=name,
        organisation=Organisation(id=orgids["Curtin University"]),
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.web_of_science]),
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
