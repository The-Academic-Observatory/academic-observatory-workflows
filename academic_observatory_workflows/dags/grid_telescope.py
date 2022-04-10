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

# Author: James Diprose

# The keywords airflow and DAG are required to load the DAGs from this file, see bullet 2 in the Apache Airflow FAQ:
# https://airflow.apache.org/docs/stable/faq.html

from academic_observatory_workflows.workflows.grid_telescope import GridTelescope
from observatory.platform.utils.api import make_observatory_api


api = make_observatory_api()
workflow_type = api.get_workflow_type(type_id=GridTelescope.DAG_ID)
workflows = api.get_workflows(workflow_type_id=workflow_type.id, limit=1000)
workflow = GridTelescope(workflow_id=workflows[0].id)
globals()[workflow.dag_id] = workflow.make_dag()
