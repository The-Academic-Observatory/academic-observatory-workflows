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

from __future__ import annotations

import os
from datetime import timedelta
from typing import Dict, List
from unittest.mock import patch

import pendulum
from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.utils.state import State

from academic_observatory_workflows.model import (
    Institution,
    bq_load_observatory_dataset,
    make_country_table,
    make_doi_table,
    make_observatory_dataset,
    sort_events,
)
from academic_observatory_workflows.workflows.doi_workflow import (
    DoiWorkflow,
    make_dataset_transforms,
    make_elastic_tables,
)
from observatory.api.client import ApiClient, Configuration
from observatory.api.client.api.observatory_api import ObservatoryApi  # noqa: E501
from observatory.api.client.model.dataset import Dataset
from observatory.api.client.model.dataset_type import DatasetType
from observatory.api.client.model.organisation import Organisation
from observatory.api.client.model.table_type import TableType
from observatory.api.client.model.workflow import Workflow
from observatory.api.client.model.workflow_type import WorkflowType
from observatory.api.testing import ObservatoryApiEnvironment
from observatory.platform.utils.airflow_utils import AirflowConns
from observatory.platform.utils.airflow_utils import set_task_state
from observatory.platform.utils.gc_utils import run_bigquery_query
from observatory.platform.utils.release_utils import get_dataset_releases
from observatory.platform.utils.test_utils import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    make_dummy_dag,
    module_file_path,
    find_free_port,
)


class TestDoiWorkflow(ObservatoryTestCase):
    """Tests for the functions used by the Doi workflow"""

    def __init__(self, *args, **kwargs):
        super(TestDoiWorkflow, self).__init__(*args, **kwargs)
        # GCP settings
        self.project_id: str = os.getenv("TEST_GCP_PROJECT_ID")
        self.bucket_name: str = os.getenv("TEST_GCP_BUCKET_NAME")
        self.data_location: str = os.getenv("TEST_GCP_DATA_LOCATION")

        # Institutions
        inst_curtin = Institution(
            1,
            name="Curtin University",
            grid_id="grid.1032.0",
            ror_id="https://ror.org/02n415q13",
            country_code="AUS",
            country_code_2="AU",
            region="Oceania",
            subregion="Australia and New Zealand",
            types="Education",
            country="Australia",
            coordinates="-32.005931, 115.894397",
        )
        inst_anu = Institution(
            2,
            name="Australian National University",
            grid_id="grid.1001.0",
            ror_id="https://ror.org/019wvm592",
            country_code="AUS",
            country_code_2="AU",
            region="Oceania",
            subregion="Australia and New Zealand",
            types="Education",
            country="Australia",
            coordinates="-35.2778, 149.1205",
        )
        inst_akl = Institution(
            3,
            name="University of Auckland",
            grid_id="grid.9654.e",
            ror_id="https://ror.org/03b94tp07",
            country_code="NZL",
            country_code_2="NZ",
            region="Oceania",
            subregion="Australia and New Zealand",
            types="Education",
            country="New Zealand",
            coordinates="-36.852304, 174.767734",
        )
        self.institutions = [inst_curtin, inst_anu, inst_akl]

        # API environment
        self.host = "localhost"
        self.port = find_free_port()
        configuration = Configuration(host=f"http://{self.host}:{self.port}")
        api_client = ApiClient(configuration)
        self.api = ObservatoryApi(api_client=api_client)  # noqa: E501
        self.env = ObservatoryApiEnvironment(host=self.host, port=self.port)
        self.org_name = "Curtin University"

    def setup_api(self):
        dt = pendulum.now("UTC")

        name = "Doi Workflow"
        workflow_type = WorkflowType(name=name, type_id="doi")
        self.api.put_workflow_type(workflow_type)

        organisation = Organisation(
            name="Curtin University",
            project_id="project",
            download_bucket="download_bucket",
            transform_bucket="transform_bucket",
        )
        self.api.put_organisation(organisation)

        telescope = Workflow(
            name=name,
            workflow_type=WorkflowType(id=1),
            organisation=Organisation(id=1),
            extra={},
        )
        self.api.put_workflow(telescope)

        table_type = TableType(
            type_id="partitioned",
            name="partitioned bq table",
        )
        self.api.put_table_type(table_type)

        dataset_type = DatasetType(
            type_id=DoiWorkflow.DAG_ID,
            name="ds type",
            extra={},
            table_type=TableType(id=1),
        )
        self.api.put_dataset_type(dataset_type)

        dataset = Dataset(
            name="Example Dataset",
            address="project.dataset.table",
            service="bigquery",
            workflow=Workflow(id=1),
            dataset_type=DatasetType(id=1),
        )
        self.api.put_dataset(dataset)

    def setup_connections(self, env):
        # Add Observatory API connection
        conn = Connection(conn_id=AirflowConns.OBSERVATORY_API, uri=f"http://:password@{self.host}:{self.port}")
        env.add_connection(conn)

    def test_set_task_state(self):
        """Test

        :return:
        """

        set_task_state(True, "my-task-id")
        with self.assertRaises(AirflowException):
            set_task_state(False, "my-task-id")

    def test_dag_structure(self):
        """Test that the DOI DAG has the correct structure.

        :return: None
        """

        dag = DoiWorkflow().make_dag()
        self.assert_dag_structure(
            {
                "crossref_metadata_sensor": ["check_dependencies"],
                "crossref_fundref_sensor": ["check_dependencies"],
                "geonames_sensor": ["check_dependencies"],
                "ror_sensor": ["check_dependencies"],
                "open_citations_sensor": ["check_dependencies"],
                "unpaywall_sensor": ["check_dependencies"],
                "orcid_sensor": ["check_dependencies"],
                "crossref_events_sensor": ["check_dependencies"],
                "check_dependencies": ["create_datasets"],
                "create_datasets": [
                    "create_crossref_events",
                    "create_crossref_fundref",
                    "create_ror",
                    "create_mag",
                    "create_orcid",
                    "create_open_citations",
                    "create_unpaywall",
                ],
                "create_crossref_events": ["create_doi"],
                "create_crossref_fundref": ["create_doi"],
                "create_ror": ["create_doi"],
                "create_mag": ["create_doi"],
                "create_orcid": ["create_doi"],
                "create_open_citations": ["create_doi"],
                "create_unpaywall": ["create_doi"],
                "create_doi": [
                    "create_book",
                ],
                "create_book": [
                    "create_country",
                    "create_funder",
                    "create_group",
                    "create_institution",
                    "create_author",
                    "create_journal",
                    "create_publisher",
                    "create_region",
                    "create_subregion",
                    "export_country",
                    "export_funder",
                    "export_group",
                    "export_institution",
                    "export_author",
                    "export_journal",
                    "export_publisher",
                    "export_region",
                    "export_subregion",
                ],
                "create_country": ["add_new_dataset_releases"],
                "create_funder": ["add_new_dataset_releases"],
                "create_group": ["add_new_dataset_releases"],
                "create_institution": ["add_new_dataset_releases"],
                "create_author": ["add_new_dataset_releases"],
                "create_journal": ["add_new_dataset_releases"],
                "create_publisher": ["add_new_dataset_releases"],
                "create_region": ["add_new_dataset_releases"],
                "create_subregion": ["add_new_dataset_releases"],
                "export_country": ["add_new_dataset_releases"],
                "export_funder": ["add_new_dataset_releases"],
                "export_group": ["add_new_dataset_releases"],
                "export_institution": ["add_new_dataset_releases"],
                "export_author": ["add_new_dataset_releases"],
                "export_journal": ["add_new_dataset_releases"],
                "export_publisher": ["add_new_dataset_releases"],
                "export_region": ["add_new_dataset_releases"],
                "export_subregion": ["add_new_dataset_releases"],
                "add_new_dataset_releases": [],
            },
            dag,
        )

    def test_dag_load(self):
        """Test that the DOI can be loaded from a DAG bag.

        :return: None
        """

        env = ObservatoryEnvironment(self.project_id, self.data_location, api_host=self.host, api_port=self.port)
        with env.create():
            self.setup_connections(env)
            self.setup_api()
            dag_file = os.path.join(module_file_path("academic_observatory_workflows.dags"), "doi_workflow.py")
            self.assert_dag_load("doi", dag_file)

    def test_telescope(self):
        """Test the DOI telescope end to end.

        :return: None.
        """

        # Create datasets
        env = ObservatoryEnvironment(
            project_id=self.project_id, data_location=self.data_location, api_host=self.host, api_port=self.port
        )
        fake_dataset_id = env.add_dataset(prefix="fake")
        intermediate_dataset_id = env.add_dataset(prefix="intermediate")
        dashboards_dataset_id = env.add_dataset(prefix="dashboards")
        observatory_dataset_id = env.add_dataset(prefix="observatory")
        elastic_dataset_id = env.add_dataset(prefix="elastic")
        settings_dataset_id = env.add_dataset(prefix="settings")
        dataset_transforms = make_dataset_transforms(
            dataset_id_crossref_events=fake_dataset_id,
            dataset_id_crossref_metadata=fake_dataset_id,
            dataset_id_crossref_fundref=fake_dataset_id,
            dataset_id_ror=fake_dataset_id,
            dataset_id_mag=fake_dataset_id,
            dataset_id_orcid=fake_dataset_id,
            dataset_id_open_citations=fake_dataset_id,
            dataset_id_unpaywall=fake_dataset_id,
            dataset_id_settings=settings_dataset_id,
            dataset_id_observatory=observatory_dataset_id,
            dataset_id_observatory_intermediate=intermediate_dataset_id,
        )
        transforms, transform_doi, transform_book = dataset_transforms

        with env.create(task_logging=True):
            self.setup_connections(env)
            self.setup_api()

            # Make dag
            start_date = pendulum.datetime(year=2021, month=10, day=10)
            workflow = DoiWorkflow(
                intermediate_dataset_id=intermediate_dataset_id,
                dashboards_dataset_id=dashboards_dataset_id,
                observatory_dataset_id=observatory_dataset_id,
                elastic_dataset_id=elastic_dataset_id,
                transforms=dataset_transforms,
                start_date=start_date,
                workflow_id=1,
            )

            # Disable dag check on dag run sensor
            for sensor in workflow.operators[0]:
                sensor.check_exists = False
                sensor.grace_period = timedelta(seconds=1)

            doi_dag = workflow.make_dag()

            # If there is no dag run for the DAG being monitored, the sensor will pass.  This is so we can
            # skip waiting on weeks when the DAG being waited on is not scheduled to run.
            expected_state = "success"
            with env.create_dag_run(doi_dag, start_date):
                for task_id in DoiWorkflow.SENSOR_DAG_IDS:
                    ti = env.run_task(f"{task_id}_sensor")
                    self.assertEqual(expected_state, ti.state)

            # Run Dummy Dags
            execution_date = pendulum.datetime(year=2021, month=10, day=17)
            release_date = pendulum.datetime(year=2021, month=10, day=23)
            release_suffix = release_date.strftime("%Y%m%d")
            expected_state = "success"
            for dag_id in DoiWorkflow.SENSOR_DAG_IDS:
                dag = make_dummy_dag(dag_id, execution_date)
                with env.create_dag_run(dag, execution_date):
                    # Running all of a DAGs tasks sets the DAG to finished
                    ti = env.run_task("dummy_task")
                    self.assertEqual(expected_state, ti.state)

            # Run end to end tests for DOI DAG
            with env.create_dag_run(doi_dag, execution_date):
                # Test that sensors go into 'success' state as the DAGs that they are waiting for have finished
                for task_id in DoiWorkflow.SENSOR_DAG_IDS:
                    ti = env.run_task(f"{task_id}_sensor")
                    self.assertEqual(expected_state, ti.state)

                # Check dependencies
                ti = env.run_task("check_dependencies")
                self.assertEqual(expected_state, ti.state)

                # Create datasets
                ti = env.run_task("create_datasets")
                self.assertEqual(expected_state, ti.state)

                # Generate fake dataset
                observatory_dataset = make_observatory_dataset(self.institutions)
                bq_load_observatory_dataset(
                    observatory_dataset,
                    env.download_bucket,
                    fake_dataset_id,
                    settings_dataset_id,
                    release_date,
                    self.data_location,
                    project_id=self.project_id,
                )

                # Test that source dataset transformations run
                for transform in transforms:
                    task_id = f"create_{transform.output_table.table_id}"
                    ti = env.run_task(task_id)
                    self.assertEqual(expected_state, ti.state)

                # Test create DOI task
                ti = env.run_task("create_doi")
                self.assertEqual(expected_state, ti.state)

                # DOI assert table exists
                expected_table_id = f"{self.project_id}.{observatory_dataset_id}.doi{release_suffix}"
                expected_rows = len(observatory_dataset.papers)
                self.assert_table_integrity(expected_table_id, expected_rows=expected_rows)

                # DOI assert correctness of output
                expected_output = make_doi_table(observatory_dataset)
                with patch("observatory.platform.utils.gc_utils.bq_query_bytes_daily_limit_check"):
                    actual_output = self.query_table(observatory_dataset_id, f"doi{release_suffix}", "doi")
                self.assert_doi(expected_output, actual_output)

                # Test create book
                ti = env.run_task("create_book")
                self.assertEqual(expected_state, ti.state)
                expected_table_id = f"{self.project_id}.{observatory_dataset_id}.book{release_suffix}"
                expected_rows = 0
                self.assert_table_integrity(expected_table_id, expected_rows)

                # Test aggregations tasks
                for agg in DoiWorkflow.AGGREGATIONS:
                    task_id = f"create_{agg.table_id}"
                    ti = env.run_task(task_id)
                    self.assertEqual(expected_state, ti.state)

                    # Aggregation assert table exists
                    expected_table_id = f"{self.project_id}.{observatory_dataset_id}.{agg.table_id}{release_suffix}"
                    self.assert_table_integrity(expected_table_id)

                # Assert country aggregation output
                expected_output = make_country_table(observatory_dataset)
                with patch("observatory.platform.utils.gc_utils.bq_query_bytes_daily_limit_check"):
                    actual_output = self.query_table(
                        observatory_dataset_id, f"country{release_suffix}", "id, time_period"
                    )
                self.assert_aggregate(expected_output, actual_output)
                # TODO: test correctness of remaining outputs

                # Test create exported tables for Elasticsearch
                for agg in DoiWorkflow.AGGREGATIONS:
                    table_id = agg.table_id
                    task_id = f"export_{table_id}"
                    ti = env.run_task(task_id)
                    self.assertEqual(expected_state, ti.state)

                    # Check that the correct tables exist for each aggregation
                    tables = make_elastic_tables(
                        table_id,
                        relate_to_institutions=agg.relate_to_institutions,
                        relate_to_countries=agg.relate_to_countries,
                        relate_to_groups=agg.relate_to_groups,
                        relate_to_members=agg.relate_to_members,
                        relate_to_journals=agg.relate_to_journals,
                        relate_to_funders=agg.relate_to_funders,
                        relate_to_publishers=agg.relate_to_publishers,
                    )
                    for table in tables:
                        aggregate = table["aggregate"]
                        facet = table["facet"]
                        expected_table_id = (
                            f"{self.project_id}.{elastic_dataset_id}.ao_{aggregate}_{facet}{release_suffix}"
                        )
                        self.assert_table_integrity(expected_table_id)

                # add_dataset_release_task
                dataset_releases = get_dataset_releases(dataset_id=1)
                self.assertEqual(len(dataset_releases), 0)
                ti = env.run_task("add_new_dataset_releases")
                self.assertEqual(ti.state, State.SUCCESS)
                dataset_releases = get_dataset_releases(dataset_id=1)
                self.assertEqual(len(dataset_releases), 1)

    def query_table(self, observatory_dataset_id: str, table_id: str, order_by_field: str) -> List[Dict]:
        """Query a BigQuery table, sorting the results and returning results as a list of dicts.

        :param observatory_dataset_id: the observatory dataset id.
        :param table_id: the table id.
        :param order_by_field: what field or fields to order by.
        :return: the table rows.
        """

        return [
            dict(row)
            for row in run_bigquery_query(
                f"SELECT * from {self.project_id}.{observatory_dataset_id}.{table_id} ORDER BY {order_by_field} ASC;"
            )
        ]

    def assert_aggregate(self, expected: List[Dict], actual: List[Dict]):
        """Assert an aggregate table.

        :param expected: the expected rows.
        :param actual: the actual rows.
        :return: None.
        """

        # Check that expected and actual are same length
        self.assertEqual(len(expected), len(actual))

        # Check that each item matches
        for expected_item, actual_item in zip(expected, actual):
            # Check that top level fields match
            for key in [
                "id",
                "time_period",
                "name",
                "country",
                "country_code",
                "country_code_2",
                "region",
                "subregion",
                "coordinates",
                "total_outputs",
            ]:
                self.assertEqual(expected_item[key], actual_item[key])

            # Access types
            self.assert_sub_fields(
                expected_item,
                actual_item,
                "access_types",
                ["oa", "green", "gold", "gold_doaj", "hybrid", "bronze", "green_only"],
            )

    def assert_sub_fields(self, expected: Dict, actual: Dict, field: str, sub_fields: List[str]):
        """Checks that the sub fields in the aggregate match.

        :param expected: the expected item.
        :param actual: the actual item.
        :param field: the field name.
        :param sub_fields: the sub field name.
        :return:
        """

        for key in sub_fields:
            self.assertEqual(expected[field][key], actual[field][key])

    def assert_doi(self, expected: List[Dict], actual: List[Dict]):
        """Assert the DOI table.

        :param expected: the expected DOI table rows.
        :param actual: the actual DOI table rows.
        :return: None.
        """

        # Assert DOI output is correct
        self.assertEqual(len(expected), len(actual))
        for expected_record, actual_record in zip(expected, actual):
            # Check that DOIs match
            self.assertEqual(expected_record["doi"], actual_record["doi"])

            # Check events
            self.assert_doi_events(expected_record["events"], actual_record["events"])

            # Check affiliations
            self.assert_doi_affiliations(expected_record["affiliations"], actual_record["affiliations"])

    def assert_doi_events(self, expected: Dict, actual: Dict):
        """Assert the DOI table events field.

        :param expected: the expected events field.
        :param actual: the actual events field.
        :return: None
        """

        if expected is None:
            # When no events exist assert they are None
            self.assertIsNone(actual)
        else:
            # When events exist check that they are equal
            self.assertEqual(expected["doi"], actual["doi"])
            sort_events(actual["events"], actual["months"], actual["years"])

            event_keys = ["events", "months", "years"]
            for key in event_keys:
                self.assertEqual(len(expected[key]), len(actual[key]))
                for ee, ea in zip(expected[key], actual[key]):
                    self.assertDictEqual(ee, ea)

    def assert_doi_affiliations(self, expected: Dict, actual: Dict):
        """Assert DOI affiliations.

        :param expected: the expected DOI affiliation rows.
        :param actual: the actual DOI affiliation rows.
        :return: None.
        """

        # DOI
        self.assertEqual(expected["doi"], actual["doi"])

        # Subfields
        fields = ["institutions", "countries", "subregions", "regions", "journals", "publishers", "funders"]
        for field in fields:
            self.assert_doi_affiliation(expected, actual, field)

    def assert_doi_affiliation(self, expected: Dict, actual: Dict, key: str):
        """Assert a DOI affiliation row.

        :param expected: the expected DOI affiliation row.
        :param actual: the actual DOI affiliation row.
        :return: None.
        """

        items_expected_ = expected[key]
        items_actual_ = actual[key]
        self.assertEqual(len(items_expected_), len(items_actual_))
        items_actual_.sort(key=lambda x: x["identifier"])
        for item_ in items_actual_:
            item_["members"].sort()
        self.assertListEqual(items_expected_, items_actual_)
