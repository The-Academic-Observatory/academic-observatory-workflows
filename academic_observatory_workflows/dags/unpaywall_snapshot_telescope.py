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

# Author: Aniek Roelofs, James Diprose

# The keywords airflow and DAG are required to load the DAGs from this file, see bullet 2 in the Apache Airflow FAQ:
# https://airflow.apache.org/docs/stable/faq.html

""" A DAG that harvests the Unpaywall database: https://unpaywall.org/

Saved to the BigQuery table: <project_id>.our_research.unpaywallYYYYMMDD

Has been tested with the following Unpaywall releases:
* 2020-04-27, 2020-02-25, 2019-11-22, 2019-08-16, 2019-04-19, 2019-02-21, 2018-09-27, 2018-09-24

Does not work with the following releases:
* 2018-03-29, 2018-04-28, 2018-06-21, 2018-09-02, 2018-09-06
"""

from academic_observatory_workflows.workflows.unpaywall_snapshot_telescope import (
    UnpaywallSnapshotTelescope,
)
from observatory.platform.utils.api import make_observatory_api


api = make_observatory_api()
workflow_type = api.get_workflow_type(type_id=UnpaywallSnapshotTelescope.DAG_ID)
workflows = api.get_workflows(workflow_type_id=workflow_type.id, limit=1000)

# Better to throw error here. If setting up for multi instance, then need to standardise dag_id etc.
workflow = workflows[0]

workflow = UnpaywallSnapshotTelescope(workflow_id=workflow.id)
globals()[workflow.dag_id] = workflow.make_dag()
