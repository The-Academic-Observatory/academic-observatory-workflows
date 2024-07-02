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
from typing import Dict, List

import pendulum
import vcr
from airflow.models import DagModel
from airflow.utils.session import provide_session
from airflow.utils.state import State

from academic_observatory_workflows.config import project_path
from academic_observatory_workflows.doi_workflow.doi_workflow import (
    AGGREGATIONS,
    create_dag,
    fetch_ror_affiliations,
    make_sql_queries,
    ror_to_ror_hierarchy_index,
    SENSOR_DAG_IDS,
)
from academic_observatory_workflows.model import (
    bq_load_observatory_dataset,
    Institution,
    make_aggregate_table,
    make_doi_table,
    make_observatory_dataset,
    Repository,
    sort_events,
)
from observatory.platform.api import get_dataset_releases
from observatory.platform.bigquery import bq_run_query, bq_sharded_table_id, bq_table_id
from observatory.platform.config import module_file_path
from observatory.platform.files import load_jsonl
from observatory.platform.observatory_config import Workflow
from observatory.platform.observatory_environment import (
    find_free_port,
    make_dummy_dag,
    ObservatoryEnvironment,
    ObservatoryTestCase,
)

FIXTURES_FOLDER = project_path("doi_workflow", "tests", "fixtures")


def query_table(table_id: str, order_by_field: str) -> List[Dict]:
    """Query a BigQuery table, sorting the results and returning results as a list of dicts.

    :param table_id: the table id.
    :param order_by_field: what field or fields to order by.
    :return: the table rows.
    """

    return [dict(row) for row in bq_run_query(f"SELECT * from {table_id} ORDER BY {order_by_field} ASC;")]


class TestDoiWorkflow(ObservatoryTestCase):
    """Tests for the functions used by the Doi workflow"""

    def __init__(self, *args, **kwargs):
        super(TestDoiWorkflow, self).__init__(*args, **kwargs)
        self.maxDiff = None
        self.dag_id = "doi"
        self.project_id: str = os.getenv("TEST_GCP_PROJECT_ID")
        self.bucket_name: str = os.getenv("TEST_GCP_BUCKET_NAME")
        self.data_location: str = os.getenv("TEST_GCP_DATA_LOCATION")

        # Institutions
        repo_curtin = Repository(
            "Curtin University Repository",
            category="Institution",
            url_domain="curtin.edu.au",
            ror_id="https://ror.org/02n415q13",
        )
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
            coordinates="-31.95224, 115.8614",
            repository=repo_curtin,
        )
        repo_anu = Repository(
            "Australian National University DSpace Repository",
            endpoint_id="2cb0529f001d4fe2c95",
            category="Institution",
            url_domain="anu.edu.au",
            ror_id="https://ror.org/019wvm592",
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
            repository=repo_anu,
        )
        repo_akl = Repository(
            "University of Auckland Repository",
            category="Institution",
            url_domain="auckland.ac.nz",
            ror_id="https://ror.org/03b94tp07",
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
            coordinates="-36.84853, 174.76349",
            repository=repo_akl,
        )
        self.institutions = [inst_curtin, inst_anu, inst_akl]

        # fmt: off
        self.repositories = [
            Repository("Europe PMC", url_domain="europepmc.org", category="Domain"),
            Repository("PubMed Central", url_domain="nih.gov", category="Domain"),
            Repository("arXiv", url_domain="arxiv.org", category="Preprint"),
            Repository("OSF Preprints - Arabixiv", url_domain="osf.io", category="Preprint", endpoint_id="033759a4c0076a700c4"),
            Repository("Repo 3", url_domain="library.auckland.ac.nz", category="Institution", pmh_domain="auckland.ac.nz", ror_id="https://ror.org/03b94tp07"),
            Repository("SciELO Preprints - SciELO", url_domain="scielo.org", category="Preprint", endpoint_id="wcmexgsfmvbrdjzx4l5m"),
            Repository("Zenodo", url_domain="zenodo.org", category="Public"),
            Repository("Figshare", url_domain="figshare.com", category="Public"),
            Repository("CiteSeer X", url_domain="citeseerx.ist.psu.edu", category="Aggregator", endpoint_id="CiteSeerX.psu"),
            Repository("Academia.edu", url_domain="academia.edu", category="Other Internet"),
            Repository("ResearchGate", url_domain="researchgate.net", category="Other Internet"),
            Repository("Unknown Repo 1", url_domain="unknown1.net", category="Unknown"),
            Repository("Unknown Repo 2", url_domain="unknown2.net", category="Unknown"),
        ]
        # fmt: on

    def test_fetch_ror_affiliations(self):
        """Test fetch_ror_affiliations"""

        with vcr.use_cassette(os.path.join(FIXTURES_FOLDER, "test_fetch_ror_affiliations.yaml")):
            # Single match
            repository_institution = "Augsburg University - OPUS - Augsburg University Publication Server"
            expected = {
                "repository_institution": repository_institution,
                "rors": [{"id": "https://ror.org/057ewhh68", "name": "Augsburg University"}],
            }
            actual = fetch_ror_affiliations(repository_institution)
            self.assertEqual(expected, actual)

            # Multiple matches
            repository_institution = '"4 institutions : Université de Strasbourg, Université de Haute Alsace, INSA Strasbourg, Bibliothèque Nationale et Universitaire de Strasbourg - univOAK"'
            expected = {
                "repository_institution": repository_institution,
                "rors": [
                    {
                        "id": "https://ror.org/001nta019",
                        "name": "Institut National des Sciences Appliquées de Strasbourg",
                    },
                    {"id": "https://ror.org/00pg6eq24", "name": "University of Strasbourg"},
                    {"id": "https://ror.org/04k8k6n84", "name": "University of Upper Alsace"},
                ],
            }
            actual = fetch_ror_affiliations(repository_institution)
            self.assertEqual(expected, actual)

    def test_dag_structure(self):
        """Test that the DOI DAG has the correct structure."""

        dag = create_dag(
            dag_id=self.dag_id,
            cloud_workspace=self.fake_cloud_workspace,
        )
        self.assert_dag_structure(
            {
                "sensors.crossref_metadata_sensor": ["check_dependencies"],
                "sensors.crossref_fundref_sensor": ["check_dependencies"],
                "sensors.geonames_sensor": ["check_dependencies"],
                "sensors.ror_sensor": ["check_dependencies"],
                "sensors.open_citations_sensor": ["check_dependencies"],
                "sensors.unpaywall_sensor": ["check_dependencies"],
                "sensors.orcid_sensor": ["check_dependencies"],
                "sensors.crossref_events_sensor": ["check_dependencies"],
                "sensors.openalex_sensor": ["check_dependencies"],
                "sensors.pubmed_sensor": ["check_dependencies"],
                "check_dependencies": ["fetch_release"],
                "fetch_release": [
                    "create_datasets",
                    "create_repo_institution_to_ror_table",
                    "create_ror_hierarchy_table",
                    "intermediate_tables.crossref_events",
                    "intermediate_tables.crossref_fundref",
                    "intermediate_tables.ror",
                    "intermediate_tables.orcid",
                    "intermediate_tables.open_citations",
                    "intermediate_tables.openaccess",
                    "intermediate_tables.openalex",
                    "intermediate_tables.pubmed",
                    "intermediate_tables.crossref_metadata",
                    "intermediate_tables.doi",
                    "aggregate_tables.country",
                    "aggregate_tables.funder",
                    "aggregate_tables.group",
                    "aggregate_tables.institution",
                    "aggregate_tables.author",
                    "aggregate_tables.journal",
                    "aggregate_tables.publisher",
                    # "aggregate_tables.region",
                    "aggregate_tables.subregion",
                    "update_table_descriptions",
                    "copy_to_dashboards",
                    "create_dashboard_views",
                    "add_dataset_release",
                ],
                "create_datasets": ["create_repo_institution_to_ror_table"],
                "create_repo_institution_to_ror_table": ["create_ror_hierarchy_table"],
                "create_ror_hierarchy_table": ["intermediate_tables.crossref_metadata"],
                "intermediate_tables.crossref_metadata": ["intermediate_tables.merge_0"],
                "intermediate_tables.merge_0": [
                    "intermediate_tables.crossref_events",
                    "intermediate_tables.crossref_fundref",
                    "intermediate_tables.ror",
                    "intermediate_tables.orcid",
                    "intermediate_tables.open_citations",
                    "intermediate_tables.openaccess",
                    "intermediate_tables.openalex",
                    "intermediate_tables.pubmed",
                ],
                "intermediate_tables.crossref_events": ["intermediate_tables.merge_1"],
                "intermediate_tables.crossref_fundref": ["intermediate_tables.merge_1"],
                "intermediate_tables.ror": ["intermediate_tables.merge_1"],
                "intermediate_tables.orcid": ["intermediate_tables.merge_1"],
                "intermediate_tables.open_citations": ["intermediate_tables.merge_1"],
                "intermediate_tables.openaccess": ["intermediate_tables.merge_1"],
                "intermediate_tables.openalex": ["intermediate_tables.merge_1"],
                "intermediate_tables.pubmed": ["intermediate_tables.merge_1"],
                "intermediate_tables.merge_1": ["intermediate_tables.doi"],
                "intermediate_tables.doi": ["intermediate_tables.merge_2"],
                "intermediate_tables.merge_2": [
                    "aggregate_tables.country",
                    "aggregate_tables.funder",
                    "aggregate_tables.group",
                    "aggregate_tables.institution",
                    "aggregate_tables.author",
                    "aggregate_tables.journal",
                    "aggregate_tables.publisher",
                    # "aggregate_tables.region",
                    "aggregate_tables.subregion",
                ],
                "aggregate_tables.country": ["update_table_descriptions"],
                "aggregate_tables.funder": ["update_table_descriptions"],
                "aggregate_tables.group": ["update_table_descriptions"],
                "aggregate_tables.institution": ["update_table_descriptions"],
                "aggregate_tables.author": ["update_table_descriptions"],
                "aggregate_tables.journal": ["update_table_descriptions"],
                "aggregate_tables.publisher": ["update_table_descriptions"],
                # "aggregate_tables.region": ["update_table_descriptions"],
                "aggregate_tables.subregion": ["update_table_descriptions"],
                "update_table_descriptions": ["copy_to_dashboards"],
                "copy_to_dashboards": ["create_dashboard_views"],
                "create_dashboard_views": ["add_dataset_release"],
                "add_dataset_release": [],
            },
            dag,
        )

    def test_dag_load(self):
        """Test that the DOI can be loaded from a DAG bag.

        :return: None
        """

        env = ObservatoryEnvironment(
            workflows=[
                Workflow(
                    dag_id=self.dag_id,
                    name="DOI Workflow",
                    class_name="academic_observatory_workflows.doi_workflow.doi_workflow.create_dag",
                    cloud_workspace=self.fake_cloud_workspace,
                )
            ]
        )

        with env.create():
            dag_file = os.path.join(module_file_path("observatory.platform.dags"), "load_dags.py")
            self.assert_dag_load(self.dag_id, dag_file)

    def test_ror_to_ror_hierarchy_index(self):
        """Test ror_to_ror_hierarchy_index. Check that correct ancestor relationships created."""

        ror = load_jsonl(os.path.join(FIXTURES_FOLDER, "ror.jsonl"))
        index = ror_to_ror_hierarchy_index(ror)
        self.assertEqual(247, len(index))

        # Auckland
        self.assertEqual(0, len(index["https://ror.org/03b94tp07"]))

        # Curtin
        self.assertEqual(0, len(index["https://ror.org/02n415q13"]))

        # International Centre for Radio Astronomy Research
        self.assertEqual({"https://ror.org/02n415q13", "https://ror.org/047272k79"}, index["https://ror.org/05sd1pp77"])

    @provide_session
    def update_db(self, *, session, object):
        session.merge(object)
        session.commit()

    def add_dummy_dag_model(self, *, tmp_dir: str, dag_id: str, schedule: str):
        model = DagModel()
        model.dag_id = dag_id
        model.schedule = schedule
        model.fileloc = os.path.join(tmp_dir, "dummy_dag.py")
        open(model.fileloc, mode="a").close()
        self.update_db(object=model)

    def test_telescope(self):
        """Test the DOI telescope end to end."""

        env = ObservatoryEnvironment(self.project_id, self.data_location, api_port=find_free_port())

        # Create datasets
        fake_dataset_id = env.add_dataset(prefix="fake")
        bq_intermediate_dataset_id = env.add_dataset(prefix="intermediate")
        bq_dashboards_dataset_id = env.add_dataset(prefix="dashboards")
        bq_observatory_dataset_id = env.add_dataset(prefix="observatory")
        bq_settings_dataset_id = env.add_dataset(prefix="settings")
        sql_queries = make_sql_queries(
            input_project_id=self.project_id,
            output_project_id=self.project_id,
            dataset_id_crossref_events=fake_dataset_id,
            dataset_id_crossref_metadata=fake_dataset_id,
            dataset_id_crossref_fundref=fake_dataset_id,
            dataset_id_ror=fake_dataset_id,
            dataset_id_orcid=fake_dataset_id,
            dataset_id_open_citations=fake_dataset_id,
            dataset_id_unpaywall=fake_dataset_id,
            dataset_id_scihub=fake_dataset_id,
            dataset_id_settings=bq_settings_dataset_id,
            dataset_id_observatory=bq_observatory_dataset_id,
            dataset_id_observatory_intermediate=bq_intermediate_dataset_id,
            dataset_id_openalex=fake_dataset_id,
            dataset_id_pubmed=fake_dataset_id,
        )

        with env.create(task_logging=True):
            start_date = pendulum.datetime(year=2021, month=10, day=10)
            api_dataset_id = env.add_dataset("api_dataset")
            doi_dag = create_dag(
                dag_id=self.dag_id,
                cloud_workspace=env.cloud_workspace,
                bq_intermediate_dataset_id=bq_intermediate_dataset_id,
                bq_dashboards_dataset_id=bq_dashboards_dataset_id,
                bq_observatory_dataset_id=bq_observatory_dataset_id,
                bq_unpaywall_dataset_id=fake_dataset_id,
                bq_ror_dataset_id=fake_dataset_id,
                sql_queries=sql_queries,
                start_date=start_date,
                max_fetch_threads=1,
                api_dataset_id=api_dataset_id,
            )

            # # Disable dag check on dag run sensor
            # for sensor in workflow.operators[0]:
            #     sensor.check_exists = False
            #     sensor.grace_period = timedelta(seconds=1)
            #
            # doi_dag = workflow.make_dag()

            # If there is no dag run for the DAG being monitored, the sensor will pass.  This is so we can
            # skip waiting on weeks when the DAG being waited on is not scheduled to run.
            # expected_state = "success"
            # with env.create_dag_run(doi_dag, start_date):
            #     for task_id in SENSOR_DAG_IDS:
            #         ti = env.run_task(f"sensors.{task_id}_sensor")
            #         self.assertEqual(expected_state, ti.state)

            # Run Dummy Dags
            logical_date = pendulum.datetime(year=2023, month=6, day=18)
            snapshot_date = pendulum.datetime(year=2023, month=6, day=25)
            expected_state = "success"
            for dag_id in SENSOR_DAG_IDS:
                dag = make_dummy_dag(dag_id, logical_date)
                with env.create_dag_run(dag, logical_date):
                    # Running all of a DAGs tasks sets the DAG to finished
                    self.add_dummy_dag_model(tmp_dir=env.temp_dir, dag_id=dag.dag_id, schedule=dag.schedule_interval)
                    ti = env.run_task("dummy_task")
                    self.assertEqual(expected_state, ti.state)

            # Run end to end tests for DOI DAG
            with env.create_dag_run(doi_dag, logical_date):
                # Test that sensors go into 'success' state as the DAGs that they are waiting for have finished
                for task_id in SENSOR_DAG_IDS:
                    ti = env.run_task(f"sensors.{task_id}_sensor")
                    self.assertEqual(expected_state, ti.state)

                # Check dependencies
                ti = env.run_task("check_dependencies")
                self.assertEqual(expected_state, ti.state)

                # Fetch release
                ti = env.run_task("fetch_release")
                self.assertEqual(expected_state, ti.state)

                # Create datasets
                ti = env.run_task("create_datasets")
                self.assertEqual(expected_state, ti.state)

                # Generate fake dataset
                repository = load_jsonl(os.path.join(FIXTURES_FOLDER, "repository.jsonl"))
                observatory_dataset = make_observatory_dataset(self.institutions, self.repositories)
                bq_load_observatory_dataset(
                    observatory_dataset,
                    repository,
                    env.download_bucket,
                    fake_dataset_id,
                    bq_settings_dataset_id,
                    snapshot_date,
                    self.project_id,
                )

                # Create repository institution table
                with vcr.use_cassette(
                    os.path.join(FIXTURES_FOLDER, "create_repo_institution_to_ror_table.yaml"),
                    ignore_hosts=["oauth2.googleapis.com", "bigquery.googleapis.com"],
                    ignore_localhost=True,
                ):
                    ti = env.run_task("create_repo_institution_to_ror_table")
                self.assertEqual(expected_state, ti.state)
                table_id = bq_sharded_table_id(
                    self.project_id, bq_intermediate_dataset_id, "repository_institution_to_ror", snapshot_date
                )
                rors = [
                    {"rors": [], "repository_institution": "Academia.edu"},
                    {"rors": [], "repository_institution": "Australian National University DSpace Repository"},
                    {"rors": [], "repository_institution": "CiteSeer X"},
                    {"rors": [], "repository_institution": "Europe PMC"},
                    {"rors": [], "repository_institution": "OSF Preprints - Arabixiv"},
                    {"rors": [], "repository_institution": "PubMed Central"},
                    {"rors": [], "repository_institution": "Repo 3"},
                    {"rors": [], "repository_institution": "SciELO Preprints - SciELO"},
                    {"rors": [], "repository_institution": "Unknown Repo 1"},
                    {"rors": [], "repository_institution": "Unknown Repo 2"},
                    {"rors": [], "repository_institution": "Zenodo"},
                    {"rors": [], "repository_institution": "arXiv"},
                    {
                        "rors": [{"name": "ResearchGate", "id": "https://ror.org/008f3q107"}],
                        "repository_institution": "ResearchGate",
                    },
                    {
                        "rors": [{"name": "Curtin University", "id": "https://ror.org/02n415q13"}],
                        "repository_institution": "Curtin University Repository",
                    },
                    {
                        "rors": [{"name": "University of Auckland", "id": "https://ror.org/03b94tp07"}],
                        "repository_institution": "University of Auckland Repository",
                    },
                    {
                        "rors": [{"name": "Figshare (United Kingdom)", "id": "https://ror.org/041mxqs23"}],
                        "repository_institution": "Figshare",
                    },
                ]
                names = set()
                for paper in observatory_dataset.papers:
                    for repo in paper.repositories:
                        for ror in rors:
                            if repo.name in ror["repository_institution"]:
                                names.add(repo.name)
                                break
                expected = []
                for ror in rors:
                    if ror["repository_institution"] in names:
                        expected.append(ror)
                self.assert_table_integrity(table_id, expected_rows=len(expected))
                self.assert_table_content(table_id, expected, "repository_institution")

                # Create ROR hierarchy table
                ti = env.run_task("create_ror_hierarchy_table")
                self.assertEqual(expected_state, ti.state)

                # Test that source dataset transformations run
                for i, batch in enumerate(sql_queries):
                    for sql_query in batch:
                        task_id = sql_query.name
                        ti = env.run_task(f"intermediate_tables.{task_id}")
                        self.assertEqual(expected_state, ti.state)
                    ti = env.run_task(f"intermediate_tables.merge_{i}")
                    self.assertEqual(expected_state, ti.state)

                # DOI assert table exists
                table_id = bq_sharded_table_id(self.project_id, bq_observatory_dataset_id, "doi", snapshot_date)
                expected_rows = len(observatory_dataset.papers)
                self.assert_table_integrity(table_id, expected_rows=expected_rows)

                # Check openalex table created
                table_id = bq_table_id(self.project_id, fake_dataset_id, "works")
                self.assert_table_integrity(table_id, expected_rows=expected_rows)

                table_id = bq_sharded_table_id(self.project_id, bq_intermediate_dataset_id, "openalex", snapshot_date)
                self.assert_table_integrity(table_id, expected_rows=expected_rows)

                # DOI assert correctness of output
                expected_output = make_doi_table(observatory_dataset)
                table_id = bq_sharded_table_id(self.project_id, bq_observatory_dataset_id, "doi", snapshot_date)
                actual_output = query_table(table_id, "doi")
                self.assert_doi(expected_output, actual_output)

                # Test aggregations tasks
                for agg in AGGREGATIONS:
                    task_id = f"aggregate_tables.{agg.table_name}"
                    ti = env.run_task(task_id)
                    self.assertEqual(expected_state, ti.state)

                    # Aggregation assert table exists
                    table_id = bq_sharded_table_id(
                        self.project_id, bq_observatory_dataset_id, agg.table_name, snapshot_date
                    )
                    self.assert_table_integrity(table_id)

                # Assert country aggregation output
                agg = "country"
                expected_output = make_aggregate_table(agg, observatory_dataset)
                table_id = bq_sharded_table_id(self.project_id, bq_observatory_dataset_id, agg, snapshot_date)
                actual_output = query_table(table_id, "id, time_period")
                self.assert_aggregate(expected_output, actual_output)

                # Assert institution aggregation output
                agg = "institution"
                expected_output = make_aggregate_table(agg, observatory_dataset)
                table_id = bq_sharded_table_id(self.project_id, bq_observatory_dataset_id, agg, snapshot_date)
                actual_output = query_table(table_id, "id, time_period")
                self.assert_aggregate(expected_output, actual_output)
                # TODO: test correctness of remaining outputs

                # Test updating table descriptions
                ti = env.run_task("update_table_descriptions")
                self.assertEqual(expected_state, ti.state)

                # Test copy to dashboards
                ti = env.run_task("copy_to_dashboards")
                self.assertEqual(expected_state, ti.state)
                for agg in AGGREGATIONS:
                    table_id = bq_table_id(self.project_id, bq_dashboards_dataset_id, agg.table_name)
                    self.assert_table_integrity(table_id)

                # Test create dashboard views
                ti = env.run_task("create_dashboard_views")
                self.assertEqual(expected_state, ti.state)
                for table_name in ["country", "funder", "group", "institution", "publisher", "subregion"]:
                    table_id = bq_table_id(self.project_id, bq_dashboards_dataset_id, f"{table_name}_comparison")
                    self.assert_table_integrity(table_id)

                # add_dataset_release_task
                dataset_releases = get_dataset_releases(dag_id=self.dag_id, dataset_id=api_dataset_id)
                self.assertEqual(len(dataset_releases), 0)
                ti = env.run_task("add_dataset_release")
                self.assertEqual(State.SUCCESS, ti.state)
                dataset_releases = get_dataset_releases(dag_id=self.dag_id, dataset_id=api_dataset_id)
                self.assertEqual(len(dataset_releases), 1)

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
                "total_outputs",
            ]:
                self.assertEqual(expected_item[key], actual_item[key])

            # Access types
            expected_coki = expected_item["coki"]
            actual_coki = actual_item["coki"]
            self.assert_sub_fields(
                expected_coki["oa"],
                actual_coki["oa"],
                "color",
                ["oa", "green", "gold", "gold_doaj", "hybrid", "bronze", "green_only", "black"],
            )

            # COKI Access types
            self.assert_sub_fields(
                expected_coki["oa"],
                actual_coki["oa"],
                "coki",
                [
                    "open",
                    "closed",
                    "publisher",
                    "other_platform",
                    "publisher_only",
                    "both",
                    "other_platform_only",
                    "publisher_categories",
                    "other_platform_categories",
                ],
            )

            # Repositories
            self.assertEqual(expected_coki["repositories"], actual_coki["repositories"])

    def assert_sub_fields(self, expected: Dict, actual: Dict, field: str, sub_fields: List[str]):
        """Checks that the sub fields in the aggregate match.

        :param expected: the expected item.
        :param actual: the actual item.
        :param field: the field name.
        :param sub_fields: the sub field name.
        :return:
        """

        eps = 0.01  # Allow slight rounding errors between Python and SQL

        for key in sub_fields:
            if type(expected[field][key]) == float:
                self.assertTrue(abs(expected[field][key] - actual[field][key]) <= eps)
            else:
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
        print("assert_doi_affiliations:")
        for field in fields:
            print(f"\t{field}")
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
