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

# Author: Richard Hosking, James Diprose

# The keywords airflow and DAG are required to load the DAGs from this file, see bullet 2 in the Apache Airflow FAQ:
# https://airflow.apache.org/docs/stable/faq.html

"""
A DAG that produces the dois table and aggregated tables for the dashboards.

Each release is saved to the following BigQuery tables:
    <project_id>.observatory.countryYYYYMMDD
    <project_id>.observatory.doiYYYYMMDD
    <project_id>.observatory.funderYYYYMMDD
    <project_id>.observatory.groupYYYYMMDD
    <project_id>.observatory.institutionYYYYMMDD
    <project_id>.observatory.journalYYYYMMDD
    <project_id>.observatory.publisherYYYYMMDD
    <project_id>.observatory.regionYYYYMMDD
    <project_id>.observatory.subregionYYYYMMDD

Every week the following tables are overwritten for visualisation in the Data Studio dashboards:
    <project_id>.coki_dashboards.country
    <project_id>.coki_dashboards.doi
    <project_id>.coki_dashboards.funder
    <project_id>.coki_dashboards.group
    <project_id>.coki_dashboards.institution
    <project_id>.coki_dashboards.journal
    <project_id>.coki_dashboards.publisher
    <project_id>.coki_dashboards.region
    <project_id>.coki_dashboards.subregion
"""

from academic_observatory_workflows.workflows.doi_workflow import DoiWorkflow
from observatory.platform.utils.api import make_observatory_api


api = make_observatory_api()
workflow_type = api.get_workflow_type(type_id=DoiWorkflow.DAG_ID)
workflows = api.get_workflows(workflow_type_id=workflow_type.id, limit=1)
if len(workflows):
    doi_workflow = DoiWorkflow(workflow_id=workflows[0].id)
    globals()[doi_workflow.dag_id] = doi_workflow.make_dag()
