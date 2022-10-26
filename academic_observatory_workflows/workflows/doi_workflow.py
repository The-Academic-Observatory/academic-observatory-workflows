# Copyright 2020-2022 Curtin University
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

# Author: Richard Hosking, James Diprose

from __future__ import annotations

import copy
import gzip
import io
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import timedelta
from typing import Dict, List, Set, Optional, Tuple

import jsonlines
import pendulum
from airflow.exceptions import AirflowException
from google.api_core.exceptions import BadRequest
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig, LoadJob

from academic_observatory_workflows.config import sql_folder
from academic_observatory_workflows.dag_tag import Tag
from observatory.platform.utils.airflow_utils import AirflowVars, set_task_state
from observatory.platform.utils.dag_run_sensor import DagRunSensor
from observatory.platform.utils.gc_utils import (
    bigquery_sharded_table_id,
    copy_bigquery_table,
    create_bigquery_dataset,
    create_bigquery_table_from_query,
    create_bigquery_view,
    select_table_shard_dates,
)
from observatory.platform.utils.gc_utils import run_bigquery_query
from observatory.platform.utils.jinja2_utils import (
    make_sql_jinja2_filename,
    render_template,
)
from observatory.platform.utils.url_utils import retry_session
from observatory.platform.utils.workflow_utils import make_release_date
from observatory.platform.workflows.workflow import Workflow

MAX_QUERIES = 100


@dataclass
class Table:
    project_id: str
    dataset_id: str
    table_id: str = None
    sharded: bool = False
    release_date: pendulum.DateTime = None


@dataclass
class Transform:
    inputs: Dict = None
    output_table: Table = None
    output_clustering_fields: List = None


@dataclass
class Aggregation:
    table_id: str
    aggregation_field: str
    group_by_time_field: str = "published_year"
    relate_to_institutions: bool = False
    relate_to_countries: bool = False
    relate_to_groups: bool = False
    relate_to_members: bool = False
    relate_to_journals: bool = False
    relate_to_funders: bool = False
    relate_to_publishers: bool = False


def make_dataset_transforms(
    input_project_id: str,
    output_project_id: str,
    dataset_id_crossref_events: str = "crossref",
    dataset_id_crossref_metadata: str = "crossref",
    dataset_id_crossref_fundref: str = "crossref",
    dataset_id_ror: str = "ror",
    dataset_id_mag: str = "mag",
    dataset_id_orcid: str = "orcid",
    dataset_id_open_citations: str = "open_citations",
    dataset_id_unpaywall: str = "our_research",
    dataset_id_openalex: str = "openalex",
    dataset_id_settings: str = "settings",
    dataset_id_observatory: str = "observatory",
    dataset_id_observatory_intermediate: str = "observatory_intermediate",
) -> Tuple[List[Transform], Transform, Transform]:
    return (
        [
            Transform(
                inputs={"crossref_events": Table(input_project_id, dataset_id_crossref_events, "crossref_events")},
                output_table=Table(output_project_id, dataset_id_observatory_intermediate, "crossref_events"),
                output_clustering_fields=["doi"],
            ),
            Transform(
                inputs={
                    "crossref_fundref": Table(
                        input_project_id, dataset_id_crossref_fundref, "crossref_fundref", sharded=True
                    ),
                    "crossref_metadata": Table(
                        input_project_id, dataset_id_crossref_metadata, "crossref_metadata", sharded=True
                    ),
                },
                output_table=Table(output_project_id, dataset_id_observatory_intermediate, "crossref_fundref"),
                output_clustering_fields=["doi"],
            ),
            Transform(
                inputs={
                    "ror": Table(input_project_id, dataset_id_ror, "ror", sharded=True),
                    "settings": Table(input_project_id, dataset_id_settings),
                },
                output_table=Table(output_project_id, dataset_id_observatory_intermediate, "ror"),
            ),
            Transform(
                inputs={
                    "mag": Table(input_project_id, dataset_id_mag, "Affiliations", sharded=True),
                    "settings": Table(input_project_id, dataset_id_settings),
                },
                output_table=Table(output_project_id, dataset_id_observatory_intermediate, "mag"),
                output_clustering_fields=["Doi"],
            ),
            Transform(
                inputs={"orcid": Table(input_project_id, dataset_id_orcid, "orcid")},
                output_table=Table(output_project_id, dataset_id_observatory_intermediate, "orcid"),
                output_clustering_fields=["doi"],
            ),
            Transform(
                inputs={
                    "open_citations": Table(input_project_id, dataset_id_open_citations, "open_citations", sharded=True)
                },
                output_table=Table(output_project_id, dataset_id_observatory_intermediate, "open_citations"),
                output_clustering_fields=["doi"],
            ),
            Transform(
                inputs={
                    "unpaywall": Table(input_project_id, dataset_id_unpaywall, "unpaywall", sharded=False),
                    "ror": Table(input_project_id, dataset_id_ror, "ror", sharded=True),
                    "repository": Table(input_project_id, dataset_id_settings),
                    "repository_institution_to_ror": Table(
                        output_project_id,
                        dataset_id_observatory_intermediate,
                        "repository_institution_to_ror",
                        sharded=True,
                    ),
                },
                output_table=Table(output_project_id, dataset_id_observatory_intermediate, "unpaywall"),
                output_clustering_fields=["doi"],
            ),
            Transform(
                inputs={"openalex": Table(input_project_id, dataset_id_openalex, "Work", sharded=False)},
                output_table=Table(output_project_id, dataset_id_observatory_intermediate, "openalex"),
                output_clustering_fields=["doi"],
            ),
        ],
        Transform(
            inputs={
                "observatory_intermediate": Table(output_project_id, dataset_id_observatory_intermediate),
                "unpaywall": Table(output_project_id, dataset_id_unpaywall),
                "crossref_metadata": Table(
                    input_project_id, dataset_id_crossref_metadata, "crossref_metadata", sharded=True
                ),
                "settings": Table(input_project_id, dataset_id_settings),
            },
            output_table=Table(output_project_id, dataset_id_observatory, "doi"),
            output_clustering_fields=["doi"],
        ),
        Transform(
            inputs={
                "observatory": Table(output_project_id, dataset_id_observatory, "doi", sharded=True),
                "crossref_events": Table(
                    output_project_id, dataset_id_observatory_intermediate, "crossref_events", sharded=True
                ),
            },
            output_table=Table(output_project_id, dataset_id_observatory, "book"),
            output_clustering_fields=["isbn"],
        ),
    )


def make_elastic_tables(
    aggregate_table_id: str,
    relate_to_institutions: bool = False,
    relate_to_countries: bool = False,
    relate_to_groups: bool = False,
    relate_to_members: bool = False,
    relate_to_journals: bool = False,
    relate_to_funders: bool = False,
    relate_to_publishers: bool = False,
):
    # Always export
    tables = [
        {
            "file_name": make_sql_jinja2_filename("export_unique_list"),
            "aggregate": aggregate_table_id,
            "facet": "unique_list",
        },
        {
            "file_name": make_sql_jinja2_filename("export_access_types"),
            "aggregate": aggregate_table_id,
            "facet": "access_types",
        },
        {
            "file_name": make_sql_jinja2_filename("export_disciplines"),
            "aggregate": aggregate_table_id,
            "facet": "disciplines",
        },
        {
            "file_name": make_sql_jinja2_filename("export_output_types"),
            "aggregate": aggregate_table_id,
            "facet": "output_types",
        },
        {"file_name": make_sql_jinja2_filename("export_events"), "aggregate": aggregate_table_id, "facet": "events"},
        {"file_name": make_sql_jinja2_filename("export_metrics"), "aggregate": aggregate_table_id, "facet": "metrics"},
    ]

    # Optional Relationships
    export_relations = make_sql_jinja2_filename("export_relations")
    if relate_to_institutions:
        tables.append(
            {
                "file_name": export_relations,
                "aggregate": aggregate_table_id,
                "facet": "institutions",
            }
        )
    if relate_to_countries:
        tables.append(
            {
                "file_name": export_relations,
                "aggregate": aggregate_table_id,
                "facet": "countries",
            }
        )
    if relate_to_groups:
        tables.append(
            {
                "file_name": export_relations,
                "aggregate": aggregate_table_id,
                "facet": "groupings",
            }
        )
    if relate_to_members:
        tables.append(
            {
                "file_name": export_relations,
                "aggregate": aggregate_table_id,
                "facet": "members",
            }
        )
    if relate_to_journals:
        tables.append(
            {
                "file_name": export_relations,
                "aggregate": aggregate_table_id,
                "facet": "journals",
            }
        )

    if relate_to_funders:
        tables.append(
            {
                "file_name": export_relations,
                "aggregate": aggregate_table_id,
                "facet": "funders",
            }
        )

    if relate_to_publishers:
        tables.append(
            {
                "file_name": export_relations,
                "aggregate": aggregate_table_id,
                "facet": "publishers",
            }
        )

    return tables


def fetch_ror_affiliations(repository_institution: str, num_retries: int = 3) -> Dict:
    """Fetch the ROR affiliations for a given affiliation string.

    :param repository_institution: the affiliation string to search with.
    :param num_retries: the number of retries.
    :return: the list of ROR affiliations.
    """

    print(f"fetch_ror_affiliations: {repository_institution}")
    rors = []
    response = retry_session(num_retries=num_retries).get(
        "https://api.ror.org/organizations", params={"affiliation": repository_institution}
    )
    if response.status_code != 200:
        raise Exception(
            f"fetch_ror_affiliations error fetching: {response.url}, {response.status_code}, {response.reason}"
        )

    items = response.json()["items"]
    for item in items:
        if item["chosen"]:
            org = item["organization"]
            rors.append({"id": org["id"], "name": org["name"]})
    return {"repository_institution": repository_institution, "rors": rors}


def get_release_date(project_id: str, dataset_id: str, table_id: str, release_date: pendulum.DateTime):
    # Get last table shard date before current end date
    logging.info(f"get_release_date {project_id}.{dataset_id}.{table_id} {release_date}")
    table_shard_dates = select_table_shard_dates(project_id, dataset_id, table_id, release_date)
    if len(table_shard_dates):
        shard_date = table_shard_dates[0]
    else:
        raise AirflowException(
            f"{project_id}.{dataset_id}.{table_id} " f"with a table shard date <= {release_date} not found"
        )

    return shard_date


def traverse_ancestors(index: Dict, child_ids: Set):
    """Traverse all of the ancestors of a set of child ROR ids.

    :param index: the index.
    :param child_ids: the child ids.
    :return: all of the ancestors of all child ids.
    """

    ancestors = child_ids.copy()
    for child_id in child_ids:
        parents = index[child_id]

        if not len(parents):
            continue

        child_ancestors = traverse_ancestors(index, parents)
        ancestors = ancestors.union(child_ancestors)

    return ancestors


def ror_to_ror_hierarchy_index(ror: List[Dict]) -> Dict:
    """Make an index of child to ancestor relationships.

    :param ror: the ROR dataset as a list of dicts.
    :return: the index.
    """

    index = {}
    names = {}
    # Add all items to index
    for row in ror:
        ror_id = row["id"]
        index[ror_id] = set()
        names[ror_id] = row["name"]

    # Add all child -> parent relationships to index
    for row in ror:
        ror_id = row["id"]

        for rel in row["relationships"]:
            rel_id = rel["id"]
            rel_type = rel["type"]

            if rel_type == "Parent":
                index[ror_id].add(rel_id)
            elif rel_type == "Child":
                index[rel_id].add(ror_id)

    # Convert parent sets to ancestor sets
    ancestor_index = copy.deepcopy(index)
    for ror_id in index.keys():
        parents = index[ror_id]
        ancestors = traverse_ancestors(index, parents)
        ancestor_index[ror_id] = ancestors

    return ancestor_index


def bq_load_from_memory(table_id: str, records: List[Dict]) -> bool:
    """Load data into BigQuery from memory.

    :param table_id: the full table_id, including project id, dataset id and table name.
    :param records: the records to load.
    :return: whether the table loaded or not.
    """

    # Save as JSON Lines in memory
    with io.BytesIO() as bytes_io:
        with gzip.GzipFile(fileobj=bytes_io, mode="w") as gzip_file:
            with jsonlines.Writer(gzip_file) as writer:
                writer.write_all(records)

        # Load into BigQuery
        client = bigquery.Client()
        job_config = LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True,
        )

        try:
            load_job: LoadJob = client.load_table_from_file(bytes_io, table_id, job_config=job_config, rewind=True)
            success = load_job.result().state == "DONE"
        except BadRequest as e:
            logging.error(f"Load bigquery table {table_id} failed: {e}.")
            if load_job:
                logging.error(f"Errors:\n{load_job.errors}")
            success = False

    return success


class ObservatoryRelease:
    def __init__(
        self,
        *,
        release_date: pendulum.DateTime,
    ):
        """Construct an ObservatoryRelease.

        :param release_date: the release date.
        """

        self.release_date = release_date


class DoiWorkflow(Workflow):
    SENSOR_DAG_IDS = [
        "crossref_metadata",
        "crossref_fundref",
        "geonames",
        "ror",
        "open_citations",
        "unpaywall",
        "orcid",
        "crossref_events",
        "openalex",
    ]

    AGGREGATIONS = [
        Aggregation(
            "country",
            "countries",
            relate_to_members=True,
            relate_to_journals=True,
            relate_to_funders=True,
            relate_to_publishers=True,
        ),
        Aggregation(
            "funder",
            "funders",
            relate_to_institutions=True,
            relate_to_countries=True,
            relate_to_groups=True,
            relate_to_members=True,
            relate_to_funders=True,
            relate_to_publishers=True,
        ),
        Aggregation(
            "group",
            "groupings",
            relate_to_institutions=True,
            relate_to_members=True,
            relate_to_journals=True,
            relate_to_funders=True,
            relate_to_publishers=True,
        ),
        Aggregation(
            "institution",
            "institutions",
            relate_to_institutions=True,
            relate_to_countries=True,
            relate_to_journals=True,
            relate_to_funders=True,
            relate_to_publishers=True,
        ),
        Aggregation(
            "author",
            "authors",
            relate_to_institutions=True,
            relate_to_countries=True,
            relate_to_groups=True,
            relate_to_journals=True,
            relate_to_funders=True,
            relate_to_publishers=True,
        ),
        Aggregation(
            "journal",
            "journals",
            relate_to_institutions=True,
            relate_to_countries=True,
            relate_to_groups=True,
            relate_to_journals=True,
            relate_to_funders=True,
        ),
        Aggregation(
            "publisher",
            "publishers",
            relate_to_institutions=True,
            relate_to_countries=True,
            relate_to_groups=True,
            relate_to_funders=True,
        ),
        Aggregation("region", "regions", relate_to_funders=True, relate_to_publishers=True),
        Aggregation("subregion", "subregions", relate_to_funders=True, relate_to_publishers=True),
    ]

    DAG_ID = "doi"

    def __init__(
        self,
        *,
        input_project_id: str = "academic-observatory",
        output_project_id: str = "academic-observatory",
        data_location: str = "us",
        intermediate_dataset_id: str = "observatory_intermediate",
        dashboards_dataset_id: str = "coki_dashboards",
        observatory_dataset_id: str = "observatory",
        elastic_dataset_id: str = "data_export",
        unpaywall_dataset_id: str = "our_research",
        ror_dataset_id: str = "ror",
        transforms: Tuple = None,
        dag_id: Optional[str] = DAG_ID,
        start_date: Optional[pendulum.DateTime] = pendulum.datetime(2020, 8, 30),
        schedule_interval: Optional[str] = "@weekly",
        catchup: Optional[bool] = False,
        airflow_vars: List = None,
        workflow_id: int = None,
        max_fetch_threads: int = 4,
    ):
        """Create the DoiWorkflow.
        :param input_project_id: the project id for input tables. These are separated to make testing easier.
        :param output_project_id: the project id for output tables. These are separated to make testing easier.
        :param data_location: the BigQuery dataset location.
        :param intermediate_dataset_id: the BigQuery intermediate dataset id.
        :param dashboards_dataset_id: the BigQuery dashboards dataset id.
        :param observatory_dataset_id: the BigQuery observatory dataset id.
        :param elastic_dataset_id: the BigQuery elastic dataset id.
        :param dag_id: the DAG id.
        :param start_date: the start date.
        :param schedule_interval: the schedule interval.
        :param catchup: whether to catchup.
        :param airflow_vars: the required Airflow Variables.
        :param workflow_id: api workflow id.
        :param max_fetch_threads: maximum number of threads to use when fetching.
        """

        if airflow_vars is None:
            airflow_vars = [
                AirflowVars.DATA_PATH,
                AirflowVars.PROJECT_ID,
                AirflowVars.DATA_LOCATION,
                AirflowVars.DOWNLOAD_BUCKET,
                AirflowVars.TRANSFORM_BUCKET,
            ]

        # Initialise Telesecope base class
        super().__init__(
            dag_id=dag_id,
            start_date=start_date,
            schedule_interval=schedule_interval,
            catchup=catchup,
            airflow_vars=airflow_vars,
            workflow_id=workflow_id,
            tags=[Tag.academic_observatory],
        )

        self.input_project_id = input_project_id
        self.output_project_id = output_project_id
        self.data_location = data_location
        self.intermediate_dataset_id = intermediate_dataset_id
        self.dashboards_dataset_id = dashboards_dataset_id
        self.observatory_dataset_id = observatory_dataset_id
        self.elastic_dataset_id = elastic_dataset_id
        self.unpaywall_dataset_id = unpaywall_dataset_id
        self.ror_dataset_id = ror_dataset_id
        self.max_fetch_threads = max_fetch_threads

        if transforms is None:
            self.transforms, self.transform_doi, self.transform_book = make_dataset_transforms(
                input_project_id, output_project_id, dataset_id_observatory=observatory_dataset_id
            )
        else:
            self.transforms, self.transform_doi, self.transform_book = transforms

        self.create_tasks()

    def create_tasks(self):
        # Add sensors
        with self.parallel_tasks():
            for ext_dag_id in self.SENSOR_DAG_IDS:
                sensor = DagRunSensor(
                    task_id=f"{ext_dag_id}_sensor",
                    external_dag_id=ext_dag_id,
                    mode="reschedule",
                    duration=timedelta(days=7),  # Look back up to 7 days from execution date
                    poke_interval=int(timedelta(hours=1).total_seconds()),  # Check at this interval if dag run is ready
                    timeout=int(timedelta(days=2).total_seconds()),  # Sensor will fail after 2 days of waiting
                )
                self.add_operator(sensor)

        # Setup tasks
        self.add_setup_task(self.check_dependencies)

        # Create datasets
        self.add_task(self.create_datasets)

        # Create repository institution to ror table
        self.add_task(self.create_repo_institution_to_ror_table)
        self.add_task(self.create_ror_hierarchy_table)

        # Create tasks for processing intermediate tables
        with self.parallel_tasks():
            for transform in self.transforms:
                task_id = f"create_{transform.output_table.table_id}"
                self.add_task(
                    self.create_intermediate_table,
                    op_kwargs={"transform": transform, "task_id": task_id},
                    task_id=task_id,
                )

        # Create DOI Table
        task_id = f"create_{self.transform_doi.output_table.table_id}"
        self.add_task(
            self.create_intermediate_table,
            op_kwargs={"transform": self.transform_doi, "task_id": task_id},
            task_id=task_id,
        )

        # Create Book Table
        task_id = f"create_{self.transform_book.output_table.table_id}"
        self.add_task(
            self.create_intermediate_table,
            op_kwargs={"transform": self.transform_book, "task_id": task_id},
            task_id=task_id,
        )

        # Create final tables
        with self.parallel_tasks():
            for agg in self.AGGREGATIONS:
                task_id = f"create_{agg.table_id}"
                self.add_task(
                    self.create_aggregate_table, op_kwargs={"aggregation": agg, "task_id": task_id}, task_id=task_id
                )

        # Copy tables and create views
        self.add_task(self.copy_to_dashboards)
        self.add_task(self.create_dashboard_views)

        # Export for Elastic
        with self.parallel_tasks():
            # Remove the author aggregation from the list of aggregations to reduce cluster size on Elastic
            for agg in self.remove_aggregations(self.AGGREGATIONS, {"author"}):
                task_id = f"export_{agg.table_id}"
                self.add_task(
                    self.export_for_elastic, op_kwargs={"aggregation": agg, "task_id": task_id}, task_id=task_id
                )

        self.add_task(self.add_new_dataset_releases)

    def make_release(self, **kwargs) -> ObservatoryRelease:
        """Make a release instance. The release is passed as an argument to the function (TelescopeFunction) that is
        called in 'task_callable'.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return: A release instance or list of release instances
        """

        release_date = make_release_date(**kwargs)
        return ObservatoryRelease(
            release_date=release_date,
        )

    def create_datasets(self, release: ObservatoryRelease, **kwargs):
        """Create required BigQuery datasets.

        :param release: the ObservatoryRelease.
        :param kwargs: the context passed from the Airflow Operator.
        See https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return: None.
        """

        datasets = [
            (self.intermediate_dataset_id, "Intermediate processing dataset for the Academic Observatory."),
            (self.dashboards_dataset_id, "The latest data for display in the COKI dashboards."),
            (self.observatory_dataset_id, "The Academic Observatory dataset."),
            (self.elastic_dataset_id, "The Academic Observatory dataset for Elasticsearch."),
        ]

        for dataset_id, description in datasets:
            create_bigquery_dataset(
                self.output_project_id,
                dataset_id,
                self.data_location,
                description=description,
            )

    def create_repo_institution_to_ror_table(self, release: ObservatoryRelease, **kwargs):
        """Create the repository_institution_to_ror_table.

        :param release: the ObservatoryRelease.
        :param kwargs: the context passed from the Airflow Operator.
        See https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return: None.
        """

        # Fetch unique Unpaywall repository institution names
        template_path = os.path.join(sql_folder(), make_sql_jinja2_filename("create_unpaywall_repo_names"))
        sql = render_template(template_path, project_id=self.input_project_id, dataset_id=self.unpaywall_dataset_id)
        records = run_bigquery_query(sql)

        # Fetch affiliation strings
        results = []
        key = "repository_institution"
        repository_institutions = [dict(record)[key] for record in records]
        with ThreadPoolExecutor(max_workers=self.max_fetch_threads) as executor:
            futures = []
            for repo_inst in repository_institutions:
                futures.append(executor.submit(fetch_ror_affiliations, repo_inst))
            for future in as_completed(futures):
                data = future.result()
                results.append(data)
        results.sort(key=lambda r: r[key])

        # Load the BigQuery table
        table_id = bigquery_sharded_table_id(
            f"{self.output_project_id}.{self.intermediate_dataset_id}.repository_institution_to_ror",
            release.release_date,
        )
        success = bq_load_from_memory(table_id, results)
        set_task_state(success, self.create_repo_institution_to_ror_table.__name__)

    def create_ror_hierarchy_table(self, release: ObservatoryRelease, **kwargs):
        """Create the ROR hierarchy table.

        :param release: the ObservatoryRelease.
        :param kwargs: the context passed from the Airflow Operator.
        See https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return: None.
        """

        # Fetch latest ROR table
        ror_table_name = "ror"
        ror_release_date = select_table_shard_dates(
            project_id=self.input_project_id,
            dataset_id=self.ror_dataset_id,
            table_id=ror_table_name,
            end_date=release.release_date,
        )[0]
        table_id = bigquery_sharded_table_id(
            f"{self.input_project_id}.{self.ror_dataset_id}.{ror_table_name}", ror_release_date
        )
        ror = [dict(row) for row in run_bigquery_query(f"SELECT * FROM {table_id}")]

        # Create ROR hierarchy table
        index = ror_to_ror_hierarchy_index(ror)

        # Convert to list of dictionaries
        records = []
        for child_id, ancestor_ids in index.items():
            records.append({"child_id": child_id, "ror_ids": [child_id] + list(ancestor_ids)})

        # Upload to intermediate table
        table_id = bigquery_sharded_table_id(
            f"{self.output_project_id}.{self.intermediate_dataset_id}.ror_hierarchy",
            release.release_date,
        )
        success = bq_load_from_memory(table_id, records)
        set_task_state(success, self.create_ror_hierarchy_table.__name__)

    def create_intermediate_table(self, release: ObservatoryRelease, **kwargs):
        """Create an intermediate table.

        :param release: the ObservatoryRelease.
        :param kwargs: the context passed from the Airflow Operator.
        See https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return: None.
        """

        transform: Transform = kwargs["transform"]

        for k, table in transform.inputs.items():
            if table.sharded:
                table.release_date = get_release_date(
                    table.project_id, table.dataset_id, table.table_id, release.release_date
                )

        # Create processed table
        template_path = os.path.join(
            sql_folder(), make_sql_jinja2_filename(f"create_{transform.output_table.table_id}")
        )
        sql = render_template(
            template_path,
            release_date=release.release_date,
            **transform.inputs,
        )

        output_table_id_sharded = bigquery_sharded_table_id(transform.output_table.table_id, release.release_date)
        success = create_bigquery_table_from_query(
            sql=sql,
            project_id=self.output_project_id,
            dataset_id=transform.output_table.dataset_id,
            table_id=output_table_id_sharded,
            location=self.data_location,
            clustering_fields=transform.output_clustering_fields,
        )

        set_task_state(success, kwargs["task_id"])

    def create_aggregate_table(self, release: ObservatoryRelease, **kwargs):
        """Runs the aggregate table query.

        :param release: the ObservatoryRelease.
        :param kwargs: the context passed from the Airflow Operator.
        See https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return: None.
        """

        agg: Aggregation = kwargs["aggregation"]
        template_path = os.path.join(sql_folder(), make_sql_jinja2_filename("create_aggregate"))
        sql = render_template(
            template_path,
            project_id=self.output_project_id,
            dataset_id=self.observatory_dataset_id,
            release_date=release.release_date,
            aggregation_field=agg.aggregation_field,
            group_by_time_field=agg.group_by_time_field,
            relate_to_institutions=agg.relate_to_institutions,
            relate_to_countries=agg.relate_to_countries,
            relate_to_groups=agg.relate_to_groups,
            relate_to_members=agg.relate_to_members,
            relate_to_journals=agg.relate_to_journals,
            relate_to_funders=agg.relate_to_funders,
            relate_to_publishers=agg.relate_to_publishers,
        )

        sharded_table_id = bigquery_sharded_table_id(agg.table_id, release.release_date)
        success = create_bigquery_table_from_query(
            sql=sql,
            project_id=self.output_project_id,
            dataset_id=self.observatory_dataset_id,
            table_id=sharded_table_id,
            location=self.data_location,
            clustering_fields=["id"],
        )

        set_task_state(success, kwargs["task_id"])

    def copy_to_dashboards(self, release: ObservatoryRelease, **kwargs):
        """Copy tables to dashboards dataset.

        :param release: the ObservatoryRelease.
        :param kwargs: the context passed from the Airflow Operator.
        See https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return: None.
        """

        results = []
        table_ids = [agg.table_id for agg in DoiWorkflow.AGGREGATIONS]
        for table_id in table_ids:
            source_table_id = f"{self.output_project_id}.{self.observatory_dataset_id}.{bigquery_sharded_table_id(table_id, release.release_date)}"
            destination_table_id = f"{self.output_project_id}.{self.dashboards_dataset_id}.{table_id}"
            success = copy_bigquery_table(source_table_id, destination_table_id, self.data_location)
            if not success:
                logging.error(f"Issue copying table: {source_table_id} to {destination_table_id}")

            results.append(success)

        success = all(results)
        set_task_state(success, self.copy_to_dashboards.__name__)

    def create_dashboard_views(self, release: ObservatoryRelease, **kwargs):
        """Create views for dashboards dataset.

        :param release: the ObservatoryRelease.
        :param kwargs: the context passed from the Airflow Operator.
        See https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return: None.
        """

        # Create processed dataset
        template_path = os.path.join(sql_folder(), make_sql_jinja2_filename("comparison_view"))

        # Create views
        table_ids = ["country", "funder", "group", "institution", "publisher", "subregion"]
        for table_id in table_ids:
            view_name = f"{table_id}_comparison"
            query = render_template(
                template_path,
                project_id=self.output_project_id,
                dataset_id=self.dashboards_dataset_id,
                table_id=table_id,
            )
            create_bigquery_view(self.output_project_id, self.dashboards_dataset_id, view_name, query)

    def export_for_elastic(self, release: ObservatoryRelease, **kwargs):
        """Export data in a de-nested form for Elasticsearch.

        :param release: the ObservatoryRelease.
        :param kwargs: the context passed from the Airflow Operator.
        See https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return: None.
        """

        agg = kwargs["aggregation"]
        tables = make_elastic_tables(
            agg.table_id,
            relate_to_institutions=agg.relate_to_institutions,
            relate_to_countries=agg.relate_to_countries,
            relate_to_groups=agg.relate_to_groups,
            relate_to_members=agg.relate_to_members,
            relate_to_journals=agg.relate_to_journals,
            relate_to_funders=agg.relate_to_funders,
            relate_to_publishers=agg.relate_to_publishers,
        )

        # Calculate the number of parallel queries. Since all of the real work is done on BigQuery run each export task
        # in a separate thread so that they can be done in parallel.
        num_queries = min(len(tables), MAX_QUERIES)
        results = []

        with ThreadPoolExecutor(max_workers=num_queries) as executor:
            futures = list()
            futures_msgs = {}
            for table in tables:
                template_file_name = table["file_name"]
                aggregate = table["aggregate"]
                facet = table["facet"]

                msg = f"Exporting file_name={template_file_name}, aggregate={aggregate}, facet={facet}"
                logging.info(msg)
                future = executor.submit(
                    export_aggregate_table,
                    project_id=self.output_project_id,
                    release_date=release.release_date,
                    data_location=self.data_location,
                    input_dataset_id=self.observatory_dataset_id,
                    input_table_id=agg.table_id,
                    template_file_name=template_file_name,
                    output_table_id=self.elastic_dataset_id,
                    aggregate=aggregate,
                    facet=facet,
                )

                futures.append(future)
                futures_msgs[future] = msg

            # Wait for completed tasks
            for future in as_completed(futures):
                success = future.result()
                msg = futures_msgs[future]
                results.append(success)
                if success:
                    logging.info(f"Exporting feed success: {msg}")
                else:
                    logging.error(f"Exporting feed failed: {msg}")

        success = all(results)
        set_task_state(success, kwargs["task_id"])

    def remove_aggregations(
        self, input_aggregations: List[Aggregation], aggregations_to_remove: Set[str]
    ) -> List[Aggregation]:

        """Remove a set of aggregations from a given list. Removal is based on mathcing the table_id
        string in the Aggregation object.

        When removing items from the Aggregation list, you will need to reflect the same changes in
        unit test for the DOI Workflow DAG structure.

        This function actually works in reverse and builds a list of aggregations if it does not match to the
        table_id in the "aggregations_to_remove" set. This is to get around a strange memory bug that refuses
        to modify the AGGREGATIONS list with .remove()

        :param input_aggregations: List of Aggregations to have elements removed.
        :param aggregations_to_remove: Set of aggregations to remove, matching on "table_id" in the Aggregation class.
        :return aggregations_removed: Return a list of Aggregations with the desired items removed.
        """

        aggregations_removed = []
        for agg in input_aggregations:
            if agg.table_id not in aggregations_to_remove:
                aggregations_removed.append(agg)

        return aggregations_removed


def export_aggregate_table(
    *,
    project_id: str,
    release_date: pendulum.Date,
    data_location: str,
    input_dataset_id: str,
    input_table_id: str,
    template_file_name: str,
    output_table_id: str,
    aggregate: str,
    facet: str,
):
    """Export an aggregate table.

    :param project_id:
    :param release_date:
    :param data_location:
    :param input_dataset_id:
    :param input_table_id:
    :param template_file_name:
    :param output_table_id:
    :param aggregate:
    :param facet:
    :return:
    """

    template_path = os.path.join(sql_folder(), template_file_name)
    sql = render_template(
        template_path,
        project_id=project_id,
        dataset_id=input_dataset_id,
        table_id=input_table_id,
        release_date=release_date,
        aggregate=aggregate,
        facet=facet,
    )

    export_table_id = f"ao_{aggregate}_{facet}"
    processed_table_id = bigquery_sharded_table_id(export_table_id, release_date)

    success = create_bigquery_table_from_query(
        sql=sql,
        project_id=project_id,
        dataset_id=output_table_id,
        table_id=processed_table_id,
        location=data_location,
    )

    return success
