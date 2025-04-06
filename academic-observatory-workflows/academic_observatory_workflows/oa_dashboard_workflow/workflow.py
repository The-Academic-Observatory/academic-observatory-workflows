# Copyright 2021-2024 Curtin University
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

# Author: James Diprose, Aniek Roelofs

from __future__ import annotations

from typing import List, Optional

import pendulum
from airflow import DAG
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.sensors.external_task import ExternalTaskSensor

from observatory_platform.airflow.airflow import on_failure_callback
from observatory_platform.airflow.release import make_snapshot_date
from observatory_platform.airflow.tasks import check_dependencies, gke_create_storage, gke_delete_storage
from observatory_platform.airflow.workflow import cleanup, CloudWorkspace
from observatory_platform.google.bigquery import bq_create_dataset
from observatory_platform.google.gke import gke_make_container_resources, gke_make_kubernetes_task_params, GkeParams


class DagParams:
    def __init__(
        self,
        dag_id: str,
        cloud_workspace: CloudWorkspace,
        data_bucket: str,
        conceptrecid: int,
        doi_dag_id: str = "doi",
        entity_types: List[str] = None,
        bq_agg_dataset_id: str = "observatory",
        bq_ror_dataset_id: str = "ror",
        bq_settings_dataset_id: str = "settings",
        bq_oa_dashboard_dataset_id: str = "oa_dashboard",
        version: str = "v10",
        zenodo_host: str = "https://zenodo.org",
        github_conn_id="oa_dashboard_github_token",
        zenodo_conn_id="oa_dashboard_zenodo_token",
        start_date: Optional[pendulum.DateTime] = pendulum.datetime(2021, 5, 2),
        schedule: Optional[str] = "@weekly",
        max_active_runs: int = 1,
        retries: int = 3,
        gke_volume_size: str = "200Gi",
        gke_namespace: str = "coki-astro",
        gke_volume_name: str = "oa-dashboard-workflow",
        **kwargs,
    ):
        """Create the OaDashboardWorkflow, which generates data files for the COKI Open Access Dashboard.

        :param dag_id: the DAG id.
        :param cloud_workspace: The CloudWorkspace.
        :param data_bucket: the Google Cloud Storage bucket where image data should be stored.
        :param conceptrecid: the Zenodo Concept Record ID for the COKI Open Access Dataset. The Concept Record ID is
        the last set of numbers from the Concept DOI.
        :param doi_dag_id: the DAG id to wait for.
        :param entity_types: the table names.
        :param bq_agg_dataset_id: the id of the BigQuery dataset where the Academic Observatory aggregated data lives.
        :param bq_ror_dataset_id: the id of the BigQuery dataset containing the ROR table.
        :param bq_settings_dataset_id: the id of the BigQuery settings dataset, which contains the country table.
        :param bq_oa_dashboard_dataset_id: the id of the BigQuery dataset where the tables produced by this workflow will be created.
        :param version: the dataset version published by this workflow. The Github Action pulls from a specific dataset
        version: https://github.com/The-Academic-Observatory/coki-oa-web/blob/develop/.github/workflows/build-on-data-update.yml#L68-L74.
        This is so that when breaking changes are made to the schema, the web application won't break.
        :param zenodo_host: the Zenodo hostname, can be changed to https://sandbox.zenodo.org for testing.
        :param github_conn_id: the Github Token Airflow Connection ID.
        :param zenodo_conn_id: the Zenodo Token Airflow Connection ID.
        :param start_date: the start date.
        :param schedule: the schedule interval.
        :param max_active_runs: the maximum number of DAG runs that can be run at once.
        :param retries: the number of times to retry a task.
        :param gke_namespace: The cluster namespace to use.
        :param gke_volume_name: The name of the persistent volume to create
        :param gke_volume_size: The amount of storage to request for the persistent volume
        :param kwargs: Takes kwargs for building a GkeParams object.

        The figure below illustrates the data files produced by this workflow:
        .
        ├── data: data
        │   ├── index.json: used by the Cloudflare Worker search and filtering API.
        │   ├── country: individual entity statistics files for countries. Used to build each country page.
        │   │   ├── ALB.json
        │   │   ├── ARE.json
        │   │   └── ARG.json
        │   ├── country.json: used to create the country table. First 18 countries used to build first page of country table
        │   │                 and then this file is included in the public folder and downloaded by the client to enable the
        │   │                 other pages of the table to be displayed. Copied into public/data folder.
        │   ├── institution: individual entity statistics files for institutions. Used to build each institution page.
        │   │   ├── 05ykr0121.json
        │   │   ├── 05ym42410.json
        │   │   └── 05ynxx418.json
        │   ├── institution.json: used to create the institution table. First 18 institutions used to build first page of institution table
        │   │                     and then this file is included in the public folder and downloaded by the client to enable the
        │   │                     other pages of the table to be displayed. Copied into public/data folder.
        │   └── stats.json: global statistics, e.g. the minimum and maximum date for the dataset, when it was last updated etc.
        └── images:
            └── logos: country and institution logos.
                ├── country
                │   ├── md: medium logos displayed on country pages.
                │   │   ├── ALB.svg
                │   │   ├── ARE.svg
                │   │   └── ARG.svg
                │   └── sm: small logos displayed in country table.
                │       ├── ALB.svg
                │       ├── ARE.svg
                │       └── ARG.svg
                └── institution
                    ├── lg: large logos used for social media cards.
                    │   ├── 05ykr0121.png
                    │   ├── 05ym42410.png
                    │   └── 05ynxx418.png
                    ├── md: medium logos displayed on institution pages.
                    │   ├── 05ykr0121.jpg
                    │   ├── 05ym42410.jpg
                    │   └── 05ynxx418.jpg
                    └── sm: small logos displayed in institution table.
                        ├── 05ykr0121.jpg
                        ├── 05ym42410.jpg
                        └── 05ynxx418.jpg
        """

        self.dag_id = dag_id
        self.cloud_workspace = cloud_workspace
        self.data_bucket = data_bucket
        self.conceptrecid = conceptrecid
        self.doi_dag_id = doi_dag_id

        if entity_types is None:
            entity_types = ["country", "institution"]
        self.entity_types = entity_types

        self.bq_agg_dataset_id = bq_agg_dataset_id
        self.bq_ror_dataset_id = bq_ror_dataset_id
        self.bq_settings_dataset_id = bq_settings_dataset_id
        self.bq_oa_dashboard_dataset_id = bq_oa_dashboard_dataset_id
        self.version = version
        self.zenodo_host = zenodo_host
        self.github_conn_id = github_conn_id
        self.zenodo_conn_id = zenodo_conn_id
        self.start_date = start_date
        self.schedule = schedule
        self.max_active_runs = max_active_runs
        self.retries = retries
        self.gke_params = GkeParams(
            gke_volume_size=gke_volume_size, gke_namespace=gke_namespace, gke_volume_name=gke_volume_name, **kwargs
        )


def create_dag(dag_params: DagParams) -> DAG:
    kubernetes_task_params = gke_make_kubernetes_task_params(dag_params.gke_params)

    @dag(
        dag_id=dag_params.dag_id,
        start_date=dag_params.start_date,
        schedule=dag_params.schedule,
        catchup=False,
        max_active_runs=dag_params.max_active_runs,
        tags=["academic-observatory"],
        default_args={
            "owner": "airflow",
            "on_failure_callback": on_failure_callback,
            "retries": dag_params.retries,
        },
    )
    def oa_dashboard_workflow():
        @task
        def fetch_release(dag_params, **context):
            """Fetch a release"""

            from academic_observatory_workflows.oa_dashboard_workflow.release import OaDashboardRelease

            snapshot_date = make_snapshot_date(**context)
            release = OaDashboardRelease(
                dag_id=dag_params.dag_id,
                run_id=context["run_id"],
                snapshot_date=snapshot_date,
                input_project_id=dag_params.cloud_workspace.input_project_id,
                output_project_id=dag_params.cloud_workspace.output_project_id,
                bq_ror_dataset_id=dag_params.bq_ror_dataset_id,
                bq_settings_dataset_id=dag_params.bq_settings_dataset_id,
                bq_agg_dataset_id=dag_params.bq_agg_dataset_id,
                bq_oa_dashboard_dataset_id=dag_params.bq_oa_dashboard_dataset_id,
            )

            return release.to_dict()

        @task
        def create_dataset(dag_params, **context):
            """Make BigQuery datasets."""

            bq_create_dataset(
                project_id=dag_params.cloud_workspace.output_project_id,
                dataset_id=dag_params.bq_oa_dashboard_dataset_id,
                location=dag_params.cloud_workspace.data_location,
                description="The COKI Open Access Dashboard dataset",
            )

        @task
        def upload_institution_ids(release: dict, dag_params, **context):
            """Upload the institution IDs to BigQuery"""

            import academic_observatory_workflows.oa_dashboard_workflow.tasks as tasks
            from academic_observatory_workflows.oa_dashboard_workflow.release import OaDashboardRelease

            release = OaDashboardRelease.from_dict(release)
            tasks.upload_institution_ids(release=release)

        @task
        def create_entity_tables(release: dict, dag_params, **context):
            """Create the country and institution tables"""

            import academic_observatory_workflows.oa_dashboard_workflow.tasks as tasks
            from academic_observatory_workflows.oa_dashboard_workflow.release import OaDashboardRelease

            release = OaDashboardRelease.from_dict(release)
            tasks.create_entity_tables(
                release=release,
                entity_types=dag_params.entity_types,
                start_year=tasks.START_YEAR,
                end_year=tasks.END_YEAR,
                inclusion_thresholds=tasks.INCLUSION_THRESHOLD,
            )

        @task
        def add_wiki_descriptions(release: dict, entity_type: str, dag_params, **context):
            """Download wiki descriptions and update indexes."""

            import academic_observatory_workflows.oa_dashboard_workflow.tasks as tasks
            from academic_observatory_workflows.oa_dashboard_workflow.release import OaDashboardRelease

            release = OaDashboardRelease.from_dict(release)
            tasks.add_wiki_descriptions(release=release, entity_type=entity_type)

        @task.kubernetes(
            name=f"{dag_params.dag_id}-download-assets",
            container_resources=gke_make_container_resources(
                {"memory": "2G", "cpu": "2"}, dag_params.gke_params.gke_resource_overrides.get("download_assets")
            ),
            **kubernetes_task_params,
        )
        def download_assets(release: dict, dag_params, **context):
            """Download assets"""

            import academic_observatory_workflows.oa_dashboard_workflow.tasks as tasks
            from academic_observatory_workflows.oa_dashboard_workflow.release import OaDashboardRelease

            release = OaDashboardRelease.from_dict(release)
            tasks.download_assets(release=release, bucket_name=dag_params.data_bucket)

        @task.kubernetes(
            name=f"{dag_params.dag_id}-download-institution-logos",
            container_resources=gke_make_container_resources(
                {"memory": "2G", "cpu": "2"},
                dag_params.gke_params.gke_resource_overrides.get("download_institution_logos"),
            ),
            **kubernetes_task_params,
        )
        def download_institution_logos(release: dict, dag_params, **context):
            """Download logos and update indexes."""

            import academic_observatory_workflows.oa_dashboard_workflow.tasks as tasks
            from academic_observatory_workflows.oa_dashboard_workflow.release import OaDashboardRelease

            release = OaDashboardRelease.from_dict(release)
            tasks.download_institution_logos(release=release)

        @task
        def export_tables(release: dict, dag_params, **context):
            """Export and download the queried data"""

            import academic_observatory_workflows.oa_dashboard_workflow.tasks as tasks
            from academic_observatory_workflows.oa_dashboard_workflow.release import OaDashboardRelease

            release = OaDashboardRelease.from_dict(release)
            tasks.export_tables(
                release=release,
                entity_types=dag_params.entity_types,
                download_bucket=dag_params.cloud_workspace.download_bucket,
            )

        @task.kubernetes(
            name=f"{dag_params.dag_id}-download-data",
            container_resources=gke_make_container_resources(
                {"memory": "2G", "cpu": "2"}, dag_params.gke_params.gke_resource_overrides.get("download_data")
            ),
            **kubernetes_task_params,
        )
        def download_data(release: dict, dag_params, **context):
            """Download the queried data."""

            import academic_observatory_workflows.oa_dashboard_workflow.tasks as tasks
            from academic_observatory_workflows.oa_dashboard_workflow.release import OaDashboardRelease

            release = OaDashboardRelease.from_dict(release)
            tasks.download_data(release=release, download_bucket=dag_params.cloud_workspace.download_bucket)

        @task
        def make_draft_zenodo_version(release: dict, dag_params, **context):
            """Make a draft Zenodo version of the dataset."""

            import academic_observatory_workflows.oa_dashboard_workflow.tasks as tasks

            tasks.make_draft_zenodo_version(
                zenodo_conn_id=dag_params.zenodo_conn_id,
                zenodo_host=dag_params.zenodo_host,
                conceptrecid=dag_params.conceptrecid,
            )

        @task
        def fetch_zenodo_versions(release: dict, dag_params, **context):
            """Make a draft Zenodo version of the dataset."""

            import academic_observatory_workflows.oa_dashboard_workflow.tasks as tasks

            return tasks.fetch_zenodo_versions(
                zenodo_conn_id=dag_params.zenodo_conn_id,
                zenodo_host=dag_params.zenodo_host,
                conceptrecid=dag_params.conceptrecid,
            )

        @task.kubernetes(
            name=f"{dag_params.dag_id}-build-datasets",
            container_resources=gke_make_container_resources(
                {"memory": "2G", "cpu": "2"}, dag_params.gke_params.gke_resource_overrides.get("build_datasets")
            ),
            **kubernetes_task_params,
        )
        def build_datasets(release: dict, zenodo_versions: list[dict], dag_params, **context):
            """Transform the queried data into the final format for the open access website."""

            import academic_observatory_workflows.oa_dashboard_workflow.tasks as tasks
            from academic_observatory_workflows.oa_dashboard_workflow.release import OaDashboardRelease
            from academic_observatory_workflows.oa_dashboard_workflow.tasks import ZenodoVersion

            # Build dataset
            release = OaDashboardRelease.from_dict(release)
            tasks.build_datasets(
                release=release,
                entity_types=dag_params.entity_types,
                zenodo_versions=[ZenodoVersion.from_dict(v) for v in zenodo_versions],
                start_year=tasks.START_YEAR,
                end_year=tasks.END_YEAR,
                readme_text=tasks.README,
            )

        @task.kubernetes(
            name=f"{dag_params.dag_id}-upload-dataset",
            container_resources=gke_make_container_resources(
                {"memory": "2G", "cpu": "2"}, dag_params.gke_params.gke_resource_overrides.get("upload_dataset")
            ),
            **kubernetes_task_params,
        )
        def upload_dataset(release: dict, dag_params, **context):
            """Publish the dataset produced by this workflow."""

            import academic_observatory_workflows.oa_dashboard_workflow.tasks as tasks
            from academic_observatory_workflows.oa_dashboard_workflow.release import OaDashboardRelease

            release = OaDashboardRelease.from_dict(release)
            tasks.upload_dataset(release=release, version=dag_params.version, bucket_name=dag_params.data_bucket)

        @task
        def publish_zenodo_version(release: dict, dag_params, **context):
            """Publish the new Zenodo version of the dataset."""

            import academic_observatory_workflows.oa_dashboard_workflow.tasks as tasks
            from academic_observatory_workflows.oa_dashboard_workflow.release import OaDashboardRelease

            release = OaDashboardRelease.from_dict(release)
            tasks.publish_zenodo_version(
                release=release,
                version=dag_params.version,
                bucket_name=dag_params.data_bucket,
                zenodo_conn_id=dag_params.zenodo_conn_id,
                zenodo_host=dag_params.zenodo_host,
                conceptrecid=dag_params.conceptrecid,
            )

        @task
        def repository_dispatch(release: dict, dag_params, **context):
            """Trigger a Github repository_dispatch to trigger new website builds."""

            import academic_observatory_workflows.oa_dashboard_workflow.tasks as tasks
            from academic_observatory_workflows.oa_dashboard_workflow.release import OaDashboardRelease

            release = OaDashboardRelease.from_dict(release)
            tasks.repository_dispatch(github_conn_id=dag_params.github_conn_id)

        @task
        def cleanup_workflow(release: dict, dag_params, **context):
            """Cleanup old Xcoms."""

            cleanup(dag_id=dag_params.dag_id)

        # Define task connections
        task_doi_sensor = ExternalTaskSensor(
            task_id=f"{dag_params.doi_dag_id}_sensor", external_dag_id=dag_params.doi_dag_id, mode="reschedule"
        )
        task_check_dependencies = check_dependencies(
            airflow_conns=[dag_params.github_conn_id, dag_params.zenodo_conn_id, dag_params.gke_params.gke_conn_id]
        )
        task_create_dataset = create_dataset(dag_params)
        xcom_release = fetch_release(dag_params)
        task_create_storage = gke_create_storage(
            volume_name=dag_params.gke_params.gke_volume_name,
            volume_size=dag_params.gke_params.gke_volume_size,
            kubernetes_conn_id=dag_params.gke_params.gke_conn_id,
        )
        task_upload_institution_ids = upload_institution_ids(xcom_release, dag_params)
        task_create_entity_tables = create_entity_tables(xcom_release, dag_params)

        tasks_wiki = []
        for entity_type in dag_params.entity_types:
            tasks_wiki.append(
                add_wiki_descriptions.override(task_id=f"add_wiki_descriptions_{entity_type}")(
                    xcom_release, entity_type, dag_params
                )
            )

        task_download_assets = download_assets(xcom_release, dag_params)
        task_download_institution_logos = download_institution_logos(xcom_release, dag_params)
        task_export_tables = export_tables(xcom_release, dag_params)
        task_download_data = download_data(xcom_release, dag_params)
        task_make_draft_zenodo_version = make_draft_zenodo_version(xcom_release, dag_params)
        task_fetch_zenodo_versions = fetch_zenodo_versions(xcom_release, dag_params)
        task_build_datasets = build_datasets(xcom_release, task_fetch_zenodo_versions, dag_params)
        task_upload_dataset = upload_dataset(xcom_release, dag_params)
        task_publish_zenodo_version = publish_zenodo_version(xcom_release, dag_params)
        task_repository_dispatch = repository_dispatch(xcom_release, dag_params)
        task_delete_storage = gke_delete_storage(
            volume_name=dag_params.gke_params.gke_volume_name,
            kubernetes_conn_id=dag_params.gke_params.gke_conn_id,
        )
        task_cleanup_workflow = cleanup_workflow(xcom_release, dag_params)

        chain(
            task_doi_sensor,
            task_check_dependencies,
            task_create_dataset,
            xcom_release,
            task_create_storage,
            task_upload_institution_ids,
            task_create_entity_tables,
            *tasks_wiki,
            task_download_assets,
            task_download_institution_logos,
            task_export_tables,
            task_download_data,
            task_make_draft_zenodo_version,
            task_fetch_zenodo_versions,
            task_build_datasets,
            task_upload_dataset,
            task_publish_zenodo_version,
            task_repository_dispatch,
            task_delete_storage,
            task_cleanup_workflow,
        )

    return oa_dashboard_workflow()
