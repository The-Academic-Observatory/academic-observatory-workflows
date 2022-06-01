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


from observatory.platform.utils.test_utils import ObservatoryTestCase
from observatory.api.client import ApiClient, Configuration
from observatory.api.client.api.observatory_api import ObservatoryApi  # noqa: E501
from observatory.api.testing import ObservatoryApiEnvironment
from observatory.api.table_type_info import get_table_type_info
from academic_observatory_workflows.dataset_type_info import get_dataset_type_info
from academic_observatory_workflows.dataset_info import get_dataset_info
from academic_observatory_workflows.workflow_info import get_workflow_info
from academic_observatory_workflows.workflow_type_info import get_workflow_type_info
from academic_observatory_workflows.organisation_info import get_organisation_info
from observatory.api.utils import (
    seed_table_type,
    seed_dataset,
    seed_dataset_type,
    seed_organisation,
    seed_workflow,
    seed_workflow_type
)


class TestSeeding(ObservatoryTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # API environment
        self.host = "localhost"
        self.port = 5001
        configuration = Configuration(host=f"http://{self.host}:{self.port}")
        api_client = ApiClient(configuration)
        self.api = ObservatoryApi(api_client=api_client)  # noqa: E501
        self.env = ObservatoryApiEnvironment(host=self.host, port=self.port)

    def test_seeding(self):
        limit = int(1e6)
        with self.env.create():
            organisation_info = get_organisation_info()
            seed_organisation(api=self.api, organisation_info=organisation_info)

            table_type_info = get_table_type_info()
            seed_table_type(api=self.api, table_type_info=table_type_info)

            dataset_type_info = get_dataset_type_info(self.api)
            seed_dataset_type(api=self.api, dataset_type_info=dataset_type_info)

            workflow_type_info = get_workflow_type_info()
            seed_workflow_type(api=self.api, workflow_type_info=workflow_type_info)

            workflow_info = get_workflow_info(self.api)
            seed_workflow(api=self.api, workflow_info=workflow_info)

            dataset_info = get_dataset_info(self.api)
            seed_dataset(api=self.api, dataset_info=dataset_info)
            
            orgs = self.api.get_organisations(limit=limit)
            self.assertEqual(len(orgs), 1)

            tt = self.api.get_table_types(limit=limit)
            self.assertEqual(len(tt), 3)

            dt = self.api.get_dataset_types(limit=limit)
            self.assertEqual(len(dt), 19)

            wt = self.api.get_workflow_types(limit=limit)
            self.assertEqual(len(wt), 14)

            wf = self.api.get_workflows(limit=limit)
            self.assertEqual(len(wf), 14)

            ds = self.api.get_datasets(limit=limit)
            self.assertEqual(len(ds), 19)
