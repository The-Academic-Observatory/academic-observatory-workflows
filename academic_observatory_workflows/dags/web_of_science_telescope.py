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

# Author: Tuan Chien

# The keywords airflow and DAG are required to load the DAGs from this file, see bullet 2 in the Apache Airflow FAQ:
# https://airflow.apache.org/docs/stable/faq.html

import pendulum
from academic_observatory_workflows.workflows.web_of_science_telescope import (
    WebOfScienceTelescope,
)
from observatory.platform.utils.airflow_utils import AirflowVars
from observatory.platform.utils.api import make_observatory_api
from observatory.platform.utils.workflow_utils import make_dag_id

api = make_observatory_api()
workflow_type = api.get_workflow_type(type_id=WebOfScienceTelescope.DAG_ID)
workflows = api.get_workflows(workflow_type_id=workflow_type.id, limit=1000)

# Create workflows for each organisation
for workflow in workflows:
    dag_id = make_dag_id(WebOfScienceTelescope.DAG_ID, workflow.organisation.name)
    airflow_conns = workflow.extra.get("airflow_connections")
    institution_ids = workflow.extra.get("institution_ids")

    if airflow_conns is None or institution_ids is None:
        raise Exception(f"airflow_conns: {airflow_conns} or institution_ids: {institution_ids} is None")

    # earliest_date is parsed into a datetime.date object by the Python API client
    earliest_date_str = workflow.extra.get("earliest_date")
    earliest_date = pendulum.parse(earliest_date_str)

    airflow_vars = [
        AirflowVars.DATA_PATH,
        AirflowVars.DATA_LOCATION,
    ]

    workflow = WebOfScienceTelescope(
        dag_id=dag_id,
        airflow_conns=airflow_conns,
        airflow_vars=airflow_vars,
        institution_ids=institution_ids,
        earliest_date=earliest_date,
        workflow_id=workflow.id,
    )

    globals()[workflow.dag_id] = workflow.make_dag()
