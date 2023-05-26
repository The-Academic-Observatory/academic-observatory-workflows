# Copyright 2023 Curtin University
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

# Author: Alex Massen-Hane

# The keywords airflow and DAG are required to load the DAGs from this file, see bullet 2 in the Apache Airflow FAQ:
# https://airflow.apache.org/docs/stable/faq.html

import pendulum
from google.cloud.bigquery import SourceFormat

from academic_observatory_workflows.workflows.pubmed_telescope import PubMedTelescope
from observatory.platform.utils.api import make_observatory_api
from observatory.platform.utils.workflow_utils import make_dag_id


api = make_observatory_api()
workflow_type = api.get_workflow_type(type_id=PubMedTelescope.DAG_ID_PREFIX)
workflows = api.get_workflows(workflow_type_id=workflow_type.id, limit=1000)

for workflow in workflows:
    dag_id = make_dag_id(PubMedTelescope.DAG_ID_PREFIX, "pubmed")

    dataset_description = f"PubMed Dataset. Please see https://pubmed.ncbi.nlm.nih.gov/ for more information."

    workflow = PubMedTelescope(
        dag_id=dag_id,
        schema_folder="none",
        source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
        workflow_id=workflow.id,
        start_date=pendulum.datetime(year=2022, month=11, day=27),
        schedule_interval="@weekly",
        catchup=True,
        ftp_server_url="ftp.ncbi.nlm.nih.gov",
        check_md5_hash=True,
        dataset_id="pubmed",
        table_id="pubmed",
        dataset_description=dataset_description,
        max_processes=4,
    )

    globals()[workflow.dag_id] = workflow.make_dag()
