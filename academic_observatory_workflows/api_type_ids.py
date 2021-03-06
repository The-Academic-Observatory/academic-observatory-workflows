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


class TableTypeId:
    """TableTypeId type_id constants"""

    regular = "regular"
    sharded = "sharded"
    partitioned = "partitioned"


class DatasetTypeId:
    """DatasetType type_id constants"""

    crossref_events = "crossref_events"
    crossref_fundref = "crossref_fundref"
    crossref_metadata = "crossref_metadata"
    geonames = "geonames"
    grid = "grid"
    mag = "mag"
    open_citations = "open_citations"
    openalex = "openalex"
    openalex_author = "openalex_author"
    openalex_concept = "openalex_concept"
    openalex_institution = "openalex_institution"
    openalex_venue = "openalex_venue"
    openalex_work = "openalex_work"
    orcid = "orcid"
    ror = "ror"
    scopus = "scopus"
    unpaywall_snapshot = "unpaywall_snapshot"
    unpaywall = "unpaywall"
    web_of_science = "web_of_science"

    # Workflow dataset types, i.e., dataset types for datasets created by various non Telescope workflows.
    doi_workflow = "doi"


class WorkflowTypeId:
    """WorkflowTypeId type_id constants"""

    crossref_events = "crossref_events"
    crossref_fundref = "crossref_fundref"
    crossref_metadata = "crossref_metadata"
    geonames = "geonames"
    grid = "grid"
    mag = "mag"
    open_citations = "open_citations"
    openalex = "openalex"
    orcid = "orcid"
    ror = "ror"
    scopus = "scopus"
    unpaywall_snapshot = "unpaywall_snapshot"
    unpaywall = "unpaywall"
    web_of_science = "web_of_science"

    # Workflow dataset types, i.e., dataset types for datasets created by various non Telescope workflows.
    doi_workflow = "doi"
