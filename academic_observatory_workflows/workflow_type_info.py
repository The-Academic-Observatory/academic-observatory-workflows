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

from observatory.api.client.model.workflow_type import WorkflowType
from observatory.api.utils import seed_workflow_type, get_api_client


def get_workflow_type_info():
    workflow_type_info = OrderedDict()
    workflow_type_info["crossref_events"] = WorkflowType(type_id="crossref_events", name="Crossref Events Telescope")
    workflow_type_info["crossref_fundref"] = WorkflowType(type_id="crossref_fundref", name="Crossref Fundref Telescope")
    workflow_type_info["crossref_metadata"] = WorkflowType(
        type_id="crossref_metadata", name="Crossref Metadata Telescope"
    )
    workflow_type_info["geonames"] = WorkflowType(type_id="geonames", name="Geonames Telescope")
    workflow_type_info["grid"] = WorkflowType(type_id="grid", name="GRID Telescope")
    workflow_type_info["open_citations"] = WorkflowType(type_id="open_citations", name="Open Citations Telescope")
    workflow_type_info["openalex"] = WorkflowType(type_id="openalex", name="OpenAlex Telescope")
    workflow_type_info["orcid"] = WorkflowType(type_id="orcid", name="ORCID Telescope")
    workflow_type_info["ror"] = WorkflowType(type_id="ror", name="Research Organisation Registry Telescope")
    workflow_type_info["scopus"] = WorkflowType(type_id="scopus", name="SCOPUS Telescope")
    workflow_type_info["unpaywall_snapshot"] = WorkflowType(
        type_id="unpaywall_snapshot", name="Unpaywall (snapshot) Telescope"
    )
    workflow_type_info["unpaywall"] = WorkflowType(type_id="unpaywall", name="Unpaywall (daily feeds) Telescope")
    workflow_type_info["web_of_science"] = WorkflowType(type_id="web_of_science", name="Web of Science Telescope")
    workflow_type_info["doi"] = WorkflowType(type_id="doi", name="DOI Workflow")

    return workflow_type_info


if __name__ == "__main__":
    api = get_api_client()
    workflow_type_info = get_workflow_type_info()
    seed_workflow_type(api=api, workflow_type_info=workflow_type_info)
