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

import academic_observatory_workflows.oa_dashboard_workflow.tasks as tasks
from academic_observatory_workflows.oa_dashboard_workflow.release import OaDashboardRelease
from observatory_platform.airflow.airflow import on_failure_callback
from observatory_platform.airflow.release import make_snapshot_date
from observatory_platform.airflow.tasks import check_dependencies
from observatory_platform.airflow.workflow import cleanup, CloudWorkspace
from observatory_platform.google.bigquery import bq_create_dataset

INCLUSION_THRESHOLD = {"country": 5, "institution": 50}
MAX_REPOSITORIES = 200
START_YEAR = 2000
END_YEAR = pendulum.now().year - 1


README = """# COKI Open Access Dataset
The COKI Open Access Dataset measures open access performance for {{ n_countries }} countries and {{ n_institutions }} institutions
and is available in JSON Lines format. The data is visualised at the COKI Open Access Dashboard: https://open.coki.ac/.

## Licence
[COKI Open Access Dataset](https://open.coki.ac/data/) © {{ year }} by [Curtin University](https://www.curtin.edu.au/)
is licenced under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)

## Citing
To cite the COKI Open Access Dashboard please use the following citation:
> Diprose, J., Hosking, R., Rigoni, R., Roelofs, A., Chien, T., Napier, K., Wilson, K., Huang, C., Handcock, R., Montgomery, L., & Neylon, C. (2023). A User-Friendly Dashboard for Tracking Global Open Access Performance. The Journal of Electronic Publishing 26(1). doi: https://doi.org/10.3998/jep.3398

If you use the website code, please cite it as below:
> James P. Diprose, Richard Hosking, Richard Rigoni, Aniek Roelofs, Kathryn R. Napier, Tuan-Yow Chien, Alex Massen-Hane, Katie S. Wilson, Lucy Montgomery, & Cameron Neylon. (2022). COKI Open Access Website. Zenodo. https://doi.org/10.5281/zenodo.6374486

If you use this dataset, please cite it as below:
> Richard Hosking, James P. Diprose, Aniek Roelofs, Tuan-Yow Chien, Lucy Montgomery, & Cameron Neylon. (2022). COKI Open Access Dataset [Data set]. Zenodo. https://doi.org/10.5281/zenodo.6399463

## Attributions
The COKI Open Access Dataset contains information from:
* [Open Alex](https://openalex.org/) which is made available under a [CC0 licence](https://creativecommons.org/publicdomain/zero/1.0/).
* [Crossref Metadata](https://www.crossref.org/documentation/metadata-plus/) via the Metadata Plus program. Bibliographic metadata is made available without copyright restriction and Crossref generated data with a [CC0 licence](https://creativecommons.org/share-your-work/public-domain/cc0/). See [metadata licence information](https://www.crossref.org/documentation/retrieve-metadata/rest-api/rest-api-metadata-license-information/) for more details.
* [Unpaywall](https://unpaywall.org/). The [Unpaywall Data Feed](https://unpaywall.org/products/data-feed) is used under license. Data is freely available from Unpaywall via the API, data dumps and as a data feed.
* [Research Organization Registry](https://ror.org/) which is made available under a [CC0 licence](https://creativecommons.org/share-your-work/public-domain/cc0/).
"""


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


def create_dag(dag_params: DagParams) -> DAG:
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
        def fetch_release(**context):
            """Fetch a release"""
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
        def create_dataset(**context):
            """Make BigQuery datasets."""

            bq_create_dataset(
                project_id=dag_params.cloud_workspace.output_project_id,
                dataset_id=dag_params.bq_oa_dashboard_dataset_id,
                location=dag_params.cloud_workspace.data_location,
                description="The COKI Open Access Dashboard dataset",
            )

        @task
        def upload_institution_ids(release: dict, **context):
            """Upload the institution IDs to BigQuery"""

            release = OaDashboardRelease.from_dict(release)
            tasks.upload_institution_ids(release=release)

        @task
        def create_entity_tables(release: dict, **context):
            """Create the country and institution tables"""

            release = OaDashboardRelease.from_dict(release)
            tasks.create_entity_tables(
                release=release,
                entity_types=dag_params.entity_types,
                start_year=START_YEAR,
                end_year=END_YEAR,
                inclusion_thresholds=INCLUSION_THRESHOLD,
            )

        @task
        def add_wiki_descriptions(release: dict, entity_type: str, **context):
            """Download wiki descriptions and update indexes."""

            release = OaDashboardRelease.from_dict(release)
            tasks.add_wiki_descriptions(release=release, entity_type=entity_type)

        @task
        def download_assets(release: dict, **context):
            """Download assets"""

            release = OaDashboardRelease.from_dict(release)
            tasks.download_assets(release=release, bucket_name=dag_params.data_bucket)

        @task
        def download_institution_logos(release: dict, **context):
            """Download logos and update indexes."""

            release = OaDashboardRelease.from_dict(release)
            tasks.download_institution_logos(release=release)

        @task
        def export_tables(release: dict, **context):
            """Export and download the queried data"""

            release = OaDashboardRelease.from_dict(release)
            tasks.export_tables(
                release=release,
                entity_types=dag_params.entity_types,
                download_bucket=dag_params.cloud_workspace.download_bucket,
            )

        @task
        def download_data(release: dict, **context):
            """Download the queried data."""

            release = OaDashboardRelease.from_dict(release)
            tasks.download_data(release=release, download_bucket=dag_params.cloud_workspace.download_bucket)

        @task
        def make_draft_zenodo_version(release: dict, **context):
            """Make a draft Zenodo version of the dataset."""

            tasks.make_draft_zenodo_version(
                zenodo_conn_id=dag_params.zenodo_conn_id,
                zenodo_host=dag_params.zenodo_host,
                conceptrecid=dag_params.conceptrecid,
            )

        @task
        def build_datasets(release: dict, **context):
            """Transform the queried data into the final format for the open access website."""

            release = OaDashboardRelease.from_dict(release)
            tasks.build_datasets(
                release=release,
                entity_types=dag_params.entity_types,
                zenodo_conn_id=dag_params.zenodo_conn_id,
                zenodo_host=dag_params.zenodo_host,
                conceptrecid=dag_params.conceptrecid,
                start_year=START_YEAR,
                end_year=END_YEAR,
                readme_text=README,
            )

        @task
        def publish_zenodo_version(release: dict, **context):
            """Publish the new Zenodo version of the dataset."""

            release = OaDashboardRelease.from_dict(release)
            tasks.publish_zenodo_version(
                release=release,
                zenodo_conn_id=dag_params.zenodo_conn_id,
                zenodo_host=dag_params.zenodo_host,
                conceptrecid=dag_params.conceptrecid,
            )

        @task
        def upload_dataset(release: dict, **context):
            """Publish the dataset produced by this workflow."""

            release = OaDashboardRelease.from_dict(release)
            tasks.upload_dataset(release=release, version=dag_params.version, bucket_name=dag_params.data_bucket)

        @task
        def repository_dispatch(release: dict, **context):
            """Trigger a Github repository_dispatch to trigger new website builds."""

            release = OaDashboardRelease.from_dict(release)
            tasks.repository_dispatch(github_conn_id=dag_params.github_conn_id)

        @task
        def cleanup_workflow(release: dict, **context):
            """Delete all files and folders associated with this release."""

            release = OaDashboardRelease.from_dict(release)
            cleanup(dag_id=dag_params.dag_id, workflow_folder=release.workflow_folder)

        # Define task connections
        task_doi_sensor = ExternalTaskSensor(
            task_id=f"{dag_params.doi_dag_id}_sensor", external_dag_id=dag_params.doi_dag_id, mode="reschedule"
        )
        task_check_dependencies = check_dependencies(
            airflow_conns=[dag_params.github_conn_id, dag_params.zenodo_conn_id]
        )
        task_create_dataset = create_dataset()
        xcom_release = fetch_release()
        task_upload_institution_ids = upload_institution_ids(xcom_release)
        task_create_entity_tables = create_entity_tables(xcom_release)

        tasks_wiki = []
        for entity_type in dag_params.entity_types:
            tasks_wiki.append(
                add_wiki_descriptions.override(task_id=f"add_wiki_descriptions_{entity_type}")(
                    xcom_release, entity_type
                )
            )

        task_download_assets = download_assets(xcom_release)
        task_download_institution_logos = download_institution_logos(xcom_release)
        task_export_tables = export_tables(xcom_release)
        task_download_data = download_data(xcom_release)
        task_make_draft_zenodo_version = make_draft_zenodo_version(xcom_release)
        task_build_datasets = build_datasets(xcom_release)
        task_publish_zenodo_version = publish_zenodo_version(xcom_release)
        task_upload_dataset = upload_dataset(xcom_release)
        task_repository_dispatch = repository_dispatch(xcom_release)
        task_cleanup_workflow = cleanup_workflow(xcom_release)

        chain(
            task_doi_sensor,
            task_check_dependencies,
            task_create_dataset,
            xcom_release,
            task_upload_institution_ids,
            task_create_entity_tables,
            *tasks_wiki,
            task_download_assets,
            task_download_institution_logos,
            task_export_tables,
            task_download_data,
            task_make_draft_zenodo_version,
            task_build_datasets,
            task_publish_zenodo_version,
            task_upload_dataset,
            task_repository_dispatch,
            task_cleanup_workflow,
        )

    return oa_dashboard_workflow()
