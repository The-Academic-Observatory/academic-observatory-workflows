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
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import timedelta
from typing import Dict, List, Set, Optional, Tuple

import pendulum
import requests
from airflow.exceptions import AirflowException

from academic_observatory_workflows.config import sql_folder, Tag
from observatory.api.client.model.dataset_release import DatasetRelease
from observatory.platform.api import make_observatory_api
from observatory.platform.bigquery import (
    bq_sharded_table_id,
    bq_copy_table,
    bq_create_dataset,
    bq_create_table_from_query,
    bq_create_view,
    bq_select_table_shard_dates,
    bq_run_query,
    bq_table_id,
    bq_select_latest_table,
    bq_load_from_memory,
    bq_update_table_description,
)
from observatory.platform.config import AirflowConns
from observatory.platform.observatory_config import CloudWorkspace
from observatory.platform.utils.dag_run_sensor import DagRunSensor
from observatory.platform.utils.jinja2_utils import (
    make_sql_jinja2_filename,
    render_template,
)
from observatory.platform.utils.url_utils import retry_get_url
from observatory.platform.workflows.workflow import Workflow, make_snapshot_date, set_task_state, SnapshotRelease

MAX_QUERIES = 100


@dataclass
class Table:
    project_id: str
    dataset_id: str
    table_name: str = None
    sharded: bool = False
    snapshot_date: pendulum.DateTime = None

    @property
    def table_id(self):
        """Generates the BigQuery table_id for both sharded and non-sharded tables.

        :return: BigQuery table_id.
        """

        if self.sharded:
            return f"{self.table_name}{self.snapshot_date.strftime('%Y%m%d')}"
        return self.table_name


@dataclass
class Transform:
    inputs: Dict = None
    output_table: Table = None
    output_clustering_fields: List = None


@dataclass
class Aggregation:
    table_name: str
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
                    "country": Table(input_project_id, dataset_id_settings, "country"),
                },
                output_table=Table(output_project_id, dataset_id_observatory_intermediate, "ror"),
            ),
            Transform(
                inputs={
                    "mag": Table(input_project_id, dataset_id_mag, "Affiliations", sharded=True),
                    "mag_affiliation_override": Table(
                        input_project_id, dataset_id_settings, "mag_affiliation_override"
                    ),
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
                    "repository": Table(input_project_id, dataset_id_settings, "repository"),
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
                inputs={"openalex": Table(input_project_id, dataset_id_openalex, "works", sharded=False)},
                output_table=Table(output_project_id, dataset_id_observatory_intermediate, "openalex"),
                output_clustering_fields=["doi"],
            ),
        ],
        Transform(
            inputs={
                "observatory_intermediate": Table(output_project_id, dataset_id_observatory_intermediate),
                "unpaywall": Table(output_project_id, dataset_id_unpaywall, "unpaywall"),
                "crossref_metadata": Table(
                    input_project_id, dataset_id_crossref_metadata, "crossref_metadata", sharded=True
                ),
                "groupings": Table(input_project_id, dataset_id_settings, "groupings"),
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
    aggregate_table_name: str,
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
            "aggregate": aggregate_table_name,
            "facet": "unique_list",
        },
        {
            "file_name": make_sql_jinja2_filename("export_access_types"),
            "aggregate": aggregate_table_name,
            "facet": "access_types",
        },
        {
            "file_name": make_sql_jinja2_filename("export_disciplines"),
            "aggregate": aggregate_table_name,
            "facet": "disciplines",
        },
        {
            "file_name": make_sql_jinja2_filename("export_output_types"),
            "aggregate": aggregate_table_name,
            "facet": "output_types",
        },
        {"file_name": make_sql_jinja2_filename("export_events"), "aggregate": aggregate_table_name, "facet": "events"},
        {
            "file_name": make_sql_jinja2_filename("export_metrics"),
            "aggregate": aggregate_table_name,
            "facet": "metrics",
        },
    ]

    # Optional Relationships
    export_relations = make_sql_jinja2_filename("export_relations")
    if relate_to_institutions:
        tables.append(
            {
                "file_name": export_relations,
                "aggregate": aggregate_table_name,
                "facet": "institutions",
            }
        )
    if relate_to_countries:
        tables.append(
            {
                "file_name": export_relations,
                "aggregate": aggregate_table_name,
                "facet": "countries",
            }
        )
    if relate_to_groups:
        tables.append(
            {
                "file_name": export_relations,
                "aggregate": aggregate_table_name,
                "facet": "groupings",
            }
        )
    if relate_to_members:
        tables.append(
            {
                "file_name": export_relations,
                "aggregate": aggregate_table_name,
                "facet": "members",
            }
        )
    if relate_to_journals:
        tables.append(
            {
                "file_name": export_relations,
                "aggregate": aggregate_table_name,
                "facet": "journals",
            }
        )

    if relate_to_funders:
        tables.append(
            {
                "file_name": export_relations,
                "aggregate": aggregate_table_name,
                "facet": "funders",
            }
        )

    if relate_to_publishers:
        tables.append(
            {
                "file_name": export_relations,
                "aggregate": aggregate_table_name,
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
    try:
        response = retry_get_url(
            "https://api.ror.org/organizations", num_retries=num_retries, params={"affiliation": repository_institution}
        )
        items = response.json()["items"]
        for item in items:
            if item["chosen"]:
                org = item["organization"]
                rors.append({"id": org["id"], "name": org["name"]})
    except requests.exceptions.HTTPError as e:
        # If the repository_institution string causes a 500 error with the ROR affiliation matcher
        # Then catch the error and continue as if no ROR ids were matched for this entry.
        logging.error(f"requests.exceptions.HTTPError fetch_ror_affiliations error fetching: {e}")
        # TODO: it would be better to re-raise the error when it isn't a 500 error as something else is likely wrong

    return {"repository_institution": repository_institution, "rors": rors}


def get_snapshot_date(project_id: str, dataset_id: str, table_id: str, snapshot_date: pendulum.DateTime):
    # Get last table shard date before current end date
    logging.info(f"get_snapshot_date {project_id}.{dataset_id}.{table_id} {snapshot_date}")
    table_id = bq_table_id(project_id, dataset_id, table_id)
    table_shard_dates = bq_select_table_shard_dates(table_id=table_id, end_date=snapshot_date)
    if len(table_shard_dates):
        shard_date = table_shard_dates[0]
    else:
        raise AirflowException(
            f"{project_id}.{dataset_id}.{table_id} " f"with a table shard date <= {snapshot_date} not found"
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
        Aggregation(
            "region",
            "regions",
            relate_to_funders=True,
            relate_to_publishers=True,
        ),
        Aggregation(
            "subregion",
            "subregions",
            relate_to_funders=True,
            relate_to_publishers=True,
        ),
    ]

    def __init__(
        self,
        *,
        dag_id: str,
        cloud_workspace: CloudWorkspace,
        bq_intermediate_dataset_id: str = "observatory_intermediate",
        bq_dashboards_dataset_id: str = "coki_dashboards",
        bq_observatory_dataset_id: str = "observatory",
        bq_elastic_dataset_id: str = "data_export",
        bq_unpaywall_dataset_id: str = "our_research",
        bq_ror_dataset_id: str = "ror",
        api_dataset_id: str = "doi",
        transforms: Tuple = None,
        max_fetch_threads: int = 4,
        observatory_api_conn_id: str = AirflowConns.OBSERVATORY_API,
        start_date: Optional[pendulum.DateTime] = pendulum.datetime(2020, 8, 30),
        schedule_interval: Optional[str] = "@weekly",
        sensor_dag_ids: List[str] = None,
    ):
        """Create the DoiWorkflow.
        :param dag_id: the DAG ID.
        :param cloud_workspace: the cloud workspace settings.
        :param bq_intermediate_dataset_id: the BigQuery intermediate dataset id.
        :param bq_dashboards_dataset_id: the BigQuery dashboards dataset id.
        :param bq_observatory_dataset_id: the BigQuery observatory dataset id.
        :param bq_elastic_dataset_id: the BigQuery elastic dataset id.
        :param bq_unpaywall_dataset_id: the BigQuery elastic dataset id.
        :param bq_ror_dataset_id: the BigQuery elastic dataset id.
        :param api_dataset_id: the DOI dataset id.
        :param max_fetch_threads: maximum number of threads to use when fetching.
        :param start_date: the start date.
        :param schedule_interval: the schedule interval.
        """

        super().__init__(
            dag_id=dag_id,
            start_date=start_date,
            schedule_interval=schedule_interval,
            catchup=False,
            airflow_conns=[observatory_api_conn_id],
            tags=[Tag.academic_observatory],
        )

        self.input_project_id = cloud_workspace.input_project_id
        self.output_project_id = cloud_workspace.output_project_id
        self.data_location = cloud_workspace.data_location

        self.sensor_dag_ids = sensor_dag_ids
        if sensor_dag_ids is None:
            self.sensor_dag_ids = DoiWorkflow.SENSOR_DAG_IDS

        self.bq_intermediate_dataset_id = bq_intermediate_dataset_id
        self.bq_dashboards_dataset_id = bq_dashboards_dataset_id
        self.bq_observatory_dataset_id = bq_observatory_dataset_id
        self.bq_elastic_dataset_id = bq_elastic_dataset_id
        self.bq_unpaywall_dataset_id = bq_unpaywall_dataset_id
        self.bq_ror_dataset_id = bq_ror_dataset_id
        self.api_dataset_id = api_dataset_id
        self.max_fetch_threads = max_fetch_threads
        self.observatory_api_conn_id = observatory_api_conn_id
        self.input_table_id_tasks = []

        if transforms is None:
            self.transforms, self.transform_doi, self.transform_book = make_dataset_transforms(
                self.input_project_id, self.output_project_id, dataset_id_observatory=bq_observatory_dataset_id
            )
        else:
            self.transforms, self.transform_doi, self.transform_book = transforms

        self.create_tasks()

    def create_tasks(self):
        # Add sensors
        with self.parallel_tasks():
            for ext_dag_id in self.sensor_dag_ids:
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
        self.input_table_task_ids = []
        with self.parallel_tasks():
            for transform in self.transforms:
                task_id = f"create_{transform.output_table.table_name}"
                self.add_task(
                    self.create_intermediate_table,
                    op_kwargs={"transform": transform, "task_id": task_id},
                    task_id=task_id,
                )
                self.input_table_task_ids.append(task_id)

        # Create DOI Table
        task_id = f"create_{self.transform_doi.output_table.table_name}"
        self.add_task(
            self.create_intermediate_table,
            op_kwargs={"transform": self.transform_doi, "task_id": task_id},
            task_id=task_id,
        )
        self.input_table_task_ids.append(task_id)

        # Create Book Table
        task_id = f"create_{self.transform_book.output_table.table_name}"
        self.add_task(
            self.create_intermediate_table,
            op_kwargs={"transform": self.transform_book, "task_id": task_id},
            task_id=task_id,
        )

        # Create final tables
        with self.parallel_tasks():
            for agg in self.AGGREGATIONS:
                task_id = f"create_{agg.table_name}"
                self.add_task(
                    self.create_aggregate_table, op_kwargs={"aggregation": agg, "task_id": task_id}, task_id=task_id
                )

        # Copy tables and create views
        self.add_task(self.update_table_descriptions)
        self.add_task(self.copy_to_dashboards)
        self.add_task(self.create_dashboard_views)

        # Export for Elastic
        with self.parallel_tasks():
            # Remove the author aggregation from the list of aggregations to reduce cluster size on Elastic
            for agg in self.remove_aggregations(self.AGGREGATIONS, {"author"}):
                task_id = f"export_{agg.table_name}"
                self.add_task(
                    self.export_for_elastic, op_kwargs={"aggregation": agg, "task_id": task_id}, task_id=task_id
                )

        self.add_task(self.add_new_dataset_releases)

    def make_release(self, **kwargs) -> SnapshotRelease:
        """Make a release instance. The release is passed as an argument to the function (TelescopeFunction) that is
        called in 'task_callable'.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return: A release instance or list of release instances
        """

        snapshot_date = make_snapshot_date(**kwargs)
        return SnapshotRelease(
            dag_id=self.dag_id,
            run_id=kwargs["run_id"],
            snapshot_date=snapshot_date,
        )

    def create_datasets(self, release: SnapshotRelease, **kwargs):
        """Create required BigQuery datasets."""

        datasets = [
            (self.bq_intermediate_dataset_id, "Intermediate processing dataset for the Academic Observatory."),
            (self.bq_dashboards_dataset_id, "The latest data for display in the COKI dashboards."),
            (self.bq_observatory_dataset_id, "The Academic Observatory dataset."),
            (self.bq_elastic_dataset_id, "The Academic Observatory dataset for Elasticsearch."),
        ]

        for dataset_id, description in datasets:
            bq_create_dataset(
                project_id=self.output_project_id,
                dataset_id=dataset_id,
                location=self.data_location,
                description=description,
            )

    def create_repo_institution_to_ror_table(self, release: SnapshotRelease, **kwargs):
        """Create the repository_institution_to_ror_table."""

        # Fetch unique Unpaywall repository institution names
        template_path = os.path.join(sql_folder(), make_sql_jinja2_filename("create_unpaywall_repo_names"))
        sql = render_template(template_path, project_id=self.input_project_id, dataset_id=self.bq_unpaywall_dataset_id)
        records = bq_run_query(sql)

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
        table_id = bq_sharded_table_id(
            self.output_project_id,
            self.bq_intermediate_dataset_id,
            "repository_institution_to_ror",
            release.snapshot_date,
        )
        success = bq_load_from_memory(table_id, results)
        set_task_state(success, self.create_repo_institution_to_ror_table.__name__)

    def create_ror_hierarchy_table(self, release: SnapshotRelease, **kwargs):
        """Create the ROR hierarchy table."""

        # Fetch latest ROR table
        ror_table_name = "ror"
        ror_table_id = bq_select_latest_table(
            table_id=bq_table_id(self.input_project_id, self.bq_ror_dataset_id, ror_table_name),
            end_date=release.snapshot_date,
            sharded=True,
        )
        ror = [dict(row) for row in bq_run_query(f"SELECT * FROM {ror_table_id}")]

        # Create ROR hierarchy table
        index = ror_to_ror_hierarchy_index(ror)

        # Convert to list of dictionaries
        records = []
        for child_id, ancestor_ids in index.items():
            records.append({"child_id": child_id, "ror_ids": [child_id] + list(ancestor_ids)})

        # Upload to intermediate table
        table_id = bq_sharded_table_id(
            self.output_project_id, self.bq_intermediate_dataset_id, "ror_hierarchy", release.snapshot_date
        )
        success = bq_load_from_memory(table_id, records)
        set_task_state(success, self.create_ror_hierarchy_table.__name__)

    def create_intermediate_table(self, release: SnapshotRelease, **kwargs):
        """Create an intermediate table."""

        transform: Transform = kwargs["transform"]
        input_tables = []
        for k, table in transform.inputs.items():
            # Add release_date so that table_id can be computed
            if table.sharded:
                table.snapshot_date = get_snapshot_date(
                    table.project_id, table.dataset_id, table.table_name, release.snapshot_date
                )

            # Only add input tables when the table_name is not None as there is the odd Table instance
            # that is just used to specify the project id and dataset id in the SQL queries
            if table.table_name is not None:
                input_tables.append(f"{table.project_id}.{table.dataset_id}.{table.table_id}")

        # Create processed table
        template_path = os.path.join(
            sql_folder(), make_sql_jinja2_filename(f"create_{transform.output_table.table_name}")
        )
        sql = render_template(
            template_path,
            snapshot_date=release.snapshot_date,
            **transform.inputs,
        )
        table_id = bq_sharded_table_id(
            self.output_project_id,
            transform.output_table.dataset_id,
            transform.output_table.table_name,
            release.snapshot_date,
        )
        success = bq_create_table_from_query(
            sql=sql,
            table_id=table_id,
            clustering_fields=transform.output_clustering_fields,
        )

        # Publish XComs with full table paths
        ti = kwargs["ti"]
        ti.xcom_push(key="input_tables", value=json.dumps(input_tables))

        set_task_state(success, kwargs["task_id"])

    def create_aggregate_table(self, release: SnapshotRelease, **kwargs):
        """Runs the aggregate table query."""

        agg: Aggregation = kwargs["aggregation"]
        template_path = os.path.join(sql_folder(), make_sql_jinja2_filename("create_aggregate"))
        sql = render_template(
            template_path,
            project_id=self.output_project_id,
            dataset_id=self.bq_observatory_dataset_id,
            snapshot_date=release.snapshot_date,
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

        table_id = bq_sharded_table_id(
            self.output_project_id, self.bq_observatory_dataset_id, agg.table_name, release.snapshot_date
        )
        success = bq_create_table_from_query(
            sql=sql,
            table_id=table_id,
            clustering_fields=["id"],
        )

        set_task_state(success, kwargs["task_id"])

    def update_table_descriptions(self, release: SnapshotRelease, **kwargs):
        """Update descriptions for tables."""

        # Create list of input tables that were used to create our datasets
        ti = kwargs["ti"]
        results = ti.xcom_pull(task_ids=self.input_table_task_ids, key="input_tables")
        input_tables = set()
        for task_input_tables in results:
            for input_table in json.loads(task_input_tables):
                input_tables.add(input_table)
        input_tables = list(input_tables)
        input_tables.sort()

        # Update descriptions
        description = f"The DOI table.\n\nInput sources:\n"
        description += "".join([f"  - {input_table}\n" for input_table in input_tables])
        table_id = bq_sharded_table_id(
            self.output_project_id, self.bq_observatory_dataset_id, "doi", release.snapshot_date
        )
        bq_update_table_description(
            table_id=table_id,
            description=description,
        )

        table_names = [agg.table_name for agg in self.AGGREGATIONS]
        for table_name in table_names:
            description = f"The DOI table aggregated by {table_name}.\n\nInput sources:\n"
            description += "".join([f"  - {input_table}\n" for input_table in input_tables])
            table_id = bq_sharded_table_id(
                self.output_project_id, self.bq_observatory_dataset_id, table_name, release.snapshot_date
            )
            bq_update_table_description(
                table_id=table_id,
                description=description,
            )

    def copy_to_dashboards(self, release: SnapshotRelease, **kwargs):
        """Copy tables to dashboards dataset."""

        results = []
        table_names = [agg.table_name for agg in DoiWorkflow.AGGREGATIONS]
        for table_id in table_names:
            src_table_id = bq_sharded_table_id(
                self.output_project_id, self.bq_observatory_dataset_id, table_id, release.snapshot_date
            )
            dst_table_id = bq_table_id(self.output_project_id, self.bq_dashboards_dataset_id, table_id)
            success = bq_copy_table(src_table_id=src_table_id, dst_table_id=dst_table_id)
            if not success:
                logging.error(f"Issue copying table: {src_table_id} to {dst_table_id}")

            results.append(success)

        success = all(results)
        set_task_state(success, self.copy_to_dashboards.__name__)

    def create_dashboard_views(self, release: SnapshotRelease, **kwargs):
        """Create views for dashboards dataset."""

        # Create processed dataset
        template_path = os.path.join(sql_folder(), make_sql_jinja2_filename("comparison_view"))

        # Create views
        table_names = ["country", "funder", "group", "institution", "publisher", "subregion"]
        for table_name in table_names:
            view_name = f"{table_name}_comparison"
            query = render_template(
                template_path,
                project_id=self.output_project_id,
                dataset_id=self.bq_dashboards_dataset_id,
                table_id=table_name,
            )
            view_id = bq_table_id(self.output_project_id, self.bq_dashboards_dataset_id, view_name)
            bq_create_view(view_id=view_id, query=query)

    def export_for_elastic(self, release: SnapshotRelease, **kwargs):
        """Export data in a de-nested form for Elasticsearch."""

        agg = kwargs["aggregation"]
        tables = make_elastic_tables(
            agg.table_name,
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
                input_table_id = bq_sharded_table_id(
                    self.output_project_id, self.bq_observatory_dataset_id, agg.table_name, release.snapshot_date
                )
                output_table_id = bq_sharded_table_id(
                    self.output_project_id, self.bq_elastic_dataset_id, f"ao_{aggregate}_{facet}", release.snapshot_date
                )
                future = executor.submit(
                    export_aggregate_table, template_file_name, input_table_id, output_table_id, aggregate, facet
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

    def add_new_dataset_releases(self, release: SnapshotRelease, **kwargs):
        """Adds release information to API."""

        dataset_release = DatasetRelease(
            dag_id=self.dag_id,
            dataset_id=self.api_dataset_id,
            dag_run_id=release.run_id,
            snapshot_date=release.snapshot_date,
            data_interval_start=kwargs["data_interval_start"],
            data_interval_end=kwargs["data_interval_end"],
        )
        api = make_observatory_api(observatory_api_conn_id=self.observatory_api_conn_id)
        api.post_dataset_release(dataset_release)

    def remove_aggregations(
        self, input_aggregations: List[Aggregation], aggregations_to_remove: Set[str]
    ) -> List[Aggregation]:

        """Remove a set of aggregations from a given list. Removal is based on mathcing the table_name
        string in the Aggregation object.

        When removing items from the Aggregation list, you will need to reflect the same changes in
        unit test for the DOI Workflow DAG structure.

        This function actually works in reverse and builds a list of aggregations if it does not match to the
        table_name in the "aggregations_to_remove" set. This is to get around a strange memory bug that refuses
        to modify the AGGREGATIONS list with .remove()

        :param input_aggregations: List of Aggregations to have elements removed.
        :param aggregations_to_remove: Set of aggregations to remove, matching on "table_name" in the Aggregation class.
        :return aggregations_removed: Return a list of Aggregations with the desired items removed.
        """

        aggregations_removed = []
        for agg in input_aggregations:
            if agg.table_name not in aggregations_to_remove:
                aggregations_removed.append(agg)

        return aggregations_removed


def export_aggregate_table(
    template_file_name: str,
    input_table_id: str,
    output_table_id: str,
    aggregate: str,
    facet: str,
):
    template_path = os.path.join(sql_folder(), template_file_name)
    sql = render_template(
        template_path,
        table_id=input_table_id,
        aggregate=aggregate,
        facet=facet,
    )
    success = bq_create_table_from_query(
        sql=sql,
        table_id=output_table_id,
    )

    return success
