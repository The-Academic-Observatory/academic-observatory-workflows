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
from observatory.api.client.model.workflow_type import WorkflowType


def get_workflow_type_info():
    workflow_type_info = OrderedDict()
    workflow_type_info[WorkflowTypeId.crossref_events] = WorkflowType(
        type_id=WorkflowTypeId.crossref_events,
        name="Crossref Events Telescope",
    )
    workflow_type_info[WorkflowTypeId.crossref_fundref] = WorkflowType(
        type_id=WorkflowTypeId.crossref_fundref,
        name="Crossref Fundref Telescope",
    )
    workflow_type_info[WorkflowTypeId.crossref_metadata] = WorkflowType(
        type_id=WorkflowTypeId.crossref_metadata,
        name="Crossref Metadata Telescope",
    )
    workflow_type_info[WorkflowTypeId.geonames] = WorkflowType(
        type_id=WorkflowTypeId.geonames,
        name="Geonames Telescope",
    )
    workflow_type_info[WorkflowTypeId.grid] = WorkflowType(
        type_id=WorkflowTypeId.grid,
        name="GRID Telescope",
    )
    workflow_type_info[WorkflowTypeId.open_citations] = WorkflowType(
        type_id="open_citations",
        name="Open Citations Telescope",
    )
    workflow_type_info[WorkflowTypeId.openalex] = WorkflowType(
        type_id=WorkflowTypeId.openalex,
        name="OpenAlex Telescope",
    )
    workflow_type_info[WorkflowTypeId.orcid] = WorkflowType(
        type_id=WorkflowTypeId.orcid,
        name="ORCID Telescope",
    )
    workflow_type_info[WorkflowTypeId.ror] = WorkflowType(
        type_id=WorkflowTypeId.ror,
        name="ROR Telescope",
    )
    workflow_type_info[WorkflowTypeId.scopus] = WorkflowType(
        type_id=WorkflowTypeId.scopus,
        name="Scopus Telescope",
    )
    workflow_type_info[WorkflowTypeId.unpaywall_snapshot] = WorkflowType(
        type_id=WorkflowTypeId.unpaywall_snapshot,
        name="Unpaywall Snapshot Telescope",
    )
    workflow_type_info[WorkflowTypeId.unpaywall] = WorkflowType(
        type_id=WorkflowTypeId.unpaywall,
        name="Unpaywall Telescope",
    )
    workflow_type_info[WorkflowTypeId.web_of_science] = WorkflowType(
        type_id=WorkflowTypeId.web_of_science,
        name="Web of Science Telescope",
    )
    workflow_type_info[WorkflowTypeId.doi_workflow] = WorkflowType(
        type_id=WorkflowTypeId.doi_workflow,
        name="DOI Workflow",
    )

    return workflow_type_info
