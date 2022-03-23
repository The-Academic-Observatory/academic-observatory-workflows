# Copyright 2021 Curtin University
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

# Author: Aniek Roelofs

# The keywords airflow and DAG are required to load the DAGs from this file, see bullet 2 in the Apache Airflow FAQ:
# https://airflow.apache.org/docs/stable/faq.html

from academic_observatory_workflows.workflows.ror_telescope import RorTelescope
from observatory.platform.utils.api import make_observatory_api


api = make_observatory_api()
telescope_type = api.get_telescope_type(type_id=RorTelescope.DAG_ID)
telescopes = api.get_telescopes(telescope_type_id=telescope_type.id, limit=1000)
telescope = RorTelescope(workflow_id=telescopes[0].id)
globals()[telescope.dag_id] = telescope.make_dag()
