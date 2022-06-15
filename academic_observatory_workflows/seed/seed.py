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


from academic_observatory_workflows.seed.dataset_info import get_dataset_info
from academic_observatory_workflows.seed.dataset_type_info import get_dataset_type_info
from academic_observatory_workflows.seed.organisation_info import get_organisation_info
from academic_observatory_workflows.seed.table_type_info import get_table_type_info
from academic_observatory_workflows.seed.workflow_info import get_workflow_info
from academic_observatory_workflows.seed.workflow_type_info import get_workflow_type_info

from observatory.api.utils import (
    get_api_client,
    seed_dataset,
    seed_table_type,
    seed_organisation,
    seed_dataset_type,
    seed_workflow,
    seed_workflow_type,
)


def seed(host="localhost", port=5002):
    api = get_api_client(host=host, port=port)

    print("Seeding TableType")
    table_type_info = get_table_type_info()
    seed_table_type(api=api, table_type_info=table_type_info)

    print("Seeding WorkflowType")
    workflow_type_info = get_workflow_type_info()
    seed_workflow_type(api=api, workflow_type_info=workflow_type_info)

    print("Seeding DatasetType")
    dataset_type_info = get_dataset_type_info(api)
    seed_dataset_type(api=api, dataset_type_info=dataset_type_info)

    print("Seeding Organisation")
    organisation_info = get_organisation_info()
    seed_organisation(api=api, organisation_info=organisation_info)

    print("Seeding Workflow")
    workflow_info = get_workflow_info(api)
    seed_workflow(api=api, workflow_info=workflow_info)

    print("Seeding Dataset")
    dataset_info = get_dataset_info(api)
    seed_dataset(api=api, dataset_info=dataset_info)


if __name__ == "__main__":
    seed()
