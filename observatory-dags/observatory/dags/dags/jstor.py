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

# Author: Aniek Roelofs

# The keywords airflow and DAG are required to load the DAGs from this file, see bullet 2 in the Apache Airflow FAQ:
# https://airflow.apache.org/docs/stable/faq.html

from observatory.api.client.identifiers import TelescopeTypes
from observatory.dags.telescopes.jstor import JstorTelescope
from observatory.platform.utils.telescope_utils import make_observatory_api

# Fetch all telescopes
api = make_observatory_api()
telescope_type = api.get_telescope_type(type_id=TelescopeTypes.jstor)
telescopes = api.get_telescopes(telescope_type_id=telescope_type.id, limit=1000)

# Make all telescopes
for telescope in telescopes:
    telescope_instance = JstorTelescope(telescope.organisation, telescope.extra.get('publisher_id'))
    globals()[telescope_instance.dag_id] = telescope_instance.make_dag()