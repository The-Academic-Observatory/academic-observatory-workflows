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


from observatory.platform.utils.test_utils import ObservatoryTestCase, find_free_port
from observatory.api.client import ApiClient, Configuration
from observatory.api.client.api.observatory_api import ObservatoryApi  # noqa: E501
from observatory.api.testing import ObservatoryApiEnvironment
from academic_observatory_workflows.seed.table_type_info import get_table_type_info
from academic_observatory_workflows.seed.dataset_type_info import get_dataset_type_info
from academic_observatory_workflows.seed.dataset_info import get_dataset_info
from academic_observatory_workflows.seed.workflow_info import get_workflow_info
from academic_observatory_workflows.seed.workflow_type_info import get_workflow_type_info
from academic_observatory_workflows.seed.organisation_info import get_organisation_info
from academic_observatory_workflows.seed.seed import seed
from observatory.api.utils import (
    seed_table_type,
    seed_dataset,
    seed_dataset_type,
    seed_organisation,
    seed_workflow,
    seed_workflow_type,
)


class TestSeeding(ObservatoryTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def api_env(self, host="localhost", port: int = 5001):
        env = ObservatoryApiEnvironment(host=host, port=port)
        return env

    def api_client(self, host: str = "localhost", port: int = 5001):
        configuration = Configuration(host=f"http://{host}:{port}")
        api_client = ApiClient(configuration)
        api = ObservatoryApi(api_client=api_client)  # noqa: E501
        return api

    def test_seeding(self):
        limit = int(1e6)
        port = find_free_port()
        api = self.api_client(port=port)
        env = self.api_env(port=port)
        with env.create():
            organisation_info = get_organisation_info()
            seed_organisation(api=api, organisation_info=organisation_info)

            table_type_info = get_table_type_info()
            seed_table_type(api=api, table_type_info=table_type_info)

            dataset_type_info = get_dataset_type_info(api)
            seed_dataset_type(api=api, dataset_type_info=dataset_type_info)

            workflow_type_info = get_workflow_type_info()
            seed_workflow_type(api=api, workflow_type_info=workflow_type_info)

            workflow_info = get_workflow_info(api)
            seed_workflow(api=api, workflow_info=workflow_info)

            dataset_info = get_dataset_info(api)
            seed_dataset(api=api, dataset_info=dataset_info)

            orgs = api.get_organisations(limit=limit)
            self.assertEqual(len(orgs), len(organisation_info))

            tt = api.get_table_types(limit=limit)
            self.assertEqual(len(tt), len(table_type_info))

            dt = api.get_dataset_types(limit=limit)
            self.assertEqual(len(dt), len(dataset_type_info))

            wt = api.get_workflow_types(limit=limit)
            self.assertEqual(len(wt), len(workflow_type_info))

            wf = api.get_workflows(limit=limit)
            self.assertEqual(len(wf), len(workflow_info))

            ds = api.get_datasets(limit=limit)
            self.assertEqual(len(ds), len(dataset_info))

    def test_seed(self):
        limit = int(1e6)
        port = find_free_port()
        api = self.api_client(port=port)
        env = self.api_env(port=port)
        with env.create():
            seed(port=port)

            organisation_info = get_organisation_info()
            table_type_info = get_table_type_info()
            dataset_type_info = get_dataset_type_info(api)
            workflow_type_info = get_workflow_type_info()
            workflow_info = get_workflow_info(api)
            dataset_info = get_dataset_info(api)

            orgs = api.get_organisations(limit=limit)
            self.assertEqual(len(orgs), len(organisation_info))

            tt = api.get_table_types(limit=limit)
            self.assertEqual(len(tt), len(table_type_info))

            dt = api.get_dataset_types(limit=limit)
            self.assertEqual(len(dt), len(dataset_type_info))

            wt = api.get_workflow_types(limit=limit)
            self.assertEqual(len(wt), len(workflow_type_info))

            wf = api.get_workflows(limit=limit)
            self.assertEqual(len(wf), len(workflow_info))

            ds = api.get_datasets(limit=limit)
            self.assertEqual(len(ds), len(dataset_info))
