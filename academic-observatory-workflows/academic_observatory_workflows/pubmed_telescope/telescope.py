# Copyright 2023-2024 Curtin University
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

from __future__ import annotations

from typing import Optional

import pendulum
from airflow import DAG
from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

from observatory_platform.airflow.airflow import on_failure_callback
from observatory_platform.airflow.sensors import PreviousDagRunSensor
from observatory_platform.airflow.tasks import check_dependencies, gke_create_storage, gke_delete_storage
from observatory_platform.airflow.workflow import CloudWorkspace
from observatory_platform.google.gke import gke_make_container_resources, gke_make_kubernetes_task_params, GkeParams


class DagParams:
    """
    :param dag_id: the id of the DAG.
    :param cloud_workspace: Cloud settings.
    :param bq_dataset_id: Dataset name for final tables.
    :param api_bq_dataset_id: The dataset ID of the bigquery API.
    :param bq_main_table_name: Table name of the final Pubmed table.
    :param bq_upsert_table_name: Table name of the Pubmed upsert table.
    :param bq_delete_table_name: Table name of the Pubmed delete table.
    :param bq_dataset_description: Description of the Pubmed dataset.
    :param start_date: The start date of the DAG.
    :param schedule: How often the DAG should run.
    :param ftp_server_url: Server address of Pubmed's FTP server.
    :param ftp_port: Port for connectiong to Pubmed's FTP server.
    :param reset_ftp_counter: Resets FTP connection after downloading x number of files.
    :param max_download_attempt: Maximum number of download attempts of a single Pubmed file from the FTP server before throwing an error.
    :param snapshot_expiry_days: How long until the backup snapshot (before this release's upserts and deletes) of the Pubmed table exist in BQ.
    :param max_processes: Max number of parallel processes. If None, will be determined at task runtime with cpu count.
    :param max_active_runs: the maximum number of DAG runs that can be run at once.
    :param retries: the number of times to retry a task.
    :param test_run: Whether this is a test run or not.
    :param gke_namespace: The cluster namespace to use.
    :param gke_volume_name: The name of the persistent volume to create
    :param gke_volume_size: The amount of storage to request for the persistent volume
    :param kwargs: Takes kwargs for building a GkeParams object.
    """

    def __init__(
        self,
        dag_id: str,
        cloud_workspace: CloudWorkspace,
        bq_dataset_id: str = "pubmed",
        api_bq_dataset_id: str = "dataset_api",
        bq_main_table_name: str = "pubmed",
        bq_upsert_table_name: str = "pubmed_upsert",
        bq_delete_table_name: str = "pubmed_delete",
        bq_dataset_description: str = "Pubmed Medline database, only PubmedArticle records: https://pubmed.ncbi.nlm.nih.gov/about/",
        start_date: pendulum.DateTime = pendulum.datetime(year=2021, month=1, day=1),
        schedule: str = "@weekly",
        ftp_server_url: str = "ftp.ncbi.nlm.nih.gov",
        ftp_port: int = 21,
        reset_ftp_counter: int = 40,
        max_download_attempt: int = 5,
        snapshot_expiry_days: int = 31,
        max_processes: Optional[int] = None,
        max_active_runs: int = 1,
        retries: int = 3,
        baseline_table_description="""Pubmed's main table of PubmedArticle reocrds - Includes all the metadata associated with a journal article citation, both the metadata to describe the published article, i.e. <MedlineCitation>, and additional metadata often pertaining to the publication's history or processing at NLM, i.e. <PubMedData>.""",
        upsert_table_description="""PubmedArticle upserts - Includes all the metadata associated with a journal article citation, both the metadata to describe the published article, i.e. <MedlineCitation>, and additional metadata often pertaining to the publication's history or processing at NLM, i.e. <PubMedData>.""",
        delete_table_description="""PubmedArticle deletes - Indicates one or more <PubmedArticle> or <PubmedBookArticle> that have been deleted. PMIDs in DeleteCitation will typically have been found to be duplicate citations, or citations to content that was determined to be out-of-scope for PubMed. It is possible that a PMID would appear in DeleteCitation without having been distributed in a previous file. This would happen if the creation and deletion of the record take place on the same day.""",
        test_run: bool = False,
        gke_volume_size: str = "1000Gi",
        gke_namespace: str = "coki-astro",
        gke_volume_name: str = "pubmed",
        **kwargs,
    ):
        self.dag_id = dag_id
        self.cloud_workspace = cloud_workspace
        self.bq_dataset_id = bq_dataset_id
        self.api_bq_dataset_id = api_bq_dataset_id
        self.bq_main_table_name = bq_main_table_name
        self.bq_upsert_table_name = bq_upsert_table_name
        self.bq_delete_table_name = bq_delete_table_name
        self.bq_dataset_description = bq_dataset_description
        self.baseline_table_description = baseline_table_description
        self.upsert_table_description = upsert_table_description
        self.delete_table_description = delete_table_description
        self.start_date = start_date
        self.schedule = schedule
        self.ftp_server_url = ftp_server_url
        self.ftp_port = ftp_port
        self.reset_ftp_counter = reset_ftp_counter
        self.max_download_attempt = max_download_attempt
        self.snapshot_expiry_days = snapshot_expiry_days
        self.max_processes = max_processes
        self.max_active_runs = max_active_runs
        self.retries = retries
        self.test_run = test_run
        self.gke_params = GkeParams(
            gke_volume_size=gke_volume_size, gke_namespace=gke_namespace, gke_volume_name=gke_volume_name, **kwargs
        )


def create_dag(dag_params: DagParams) -> DAG:
    """Construct a PubMed Telescope instance."""

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
    def pubmed():
        @task
        def fetch_release(**context) -> str:
            """Get a list of all files to process for this release.

            Determine if workflow needs to redownload the baseline files again because of a new yearly release.
            """

            from academic_observatory_workflows.pubmed_telescope import tasks

            ftp_server_url = "localhost" if dag_params.test_run else dag_params.ftp_server_url
            return tasks.fetch_release(
                dag_id=dag_params.dag_id,
                cloud_workspace=dag_params.cloud_workspace,
                run_id=context["run_id"],
                dag_run=context["dag_run"],
                data_interval_end=context["data_interval_end"],
                bq_dataset_id=dag_params.bq_dataset_id,
                api_bq_dataset_id=dag_params.api_bq_dataset_id,
                ftp_server_url=ftp_server_url,
                ftp_port=dag_params.ftp_port,
                reset_ftp_counter=dag_params.reset_ftp_counter,
            )

        @task.short_circuit
        def short_circuit(release_id: str, dag_params, **context) -> bool:
            """Determine whether to skip this dagrun/release or not"""

            from academic_observatory_workflows.pubmed_telescope import tasks
            from observatory_platform.airflow.release import release_from_bucket

            release = release_from_bucket(dag_params.cloud_workspace.download_bucket, release_id)
            return tasks.short_circuit(release)

        @task(trigger_rule=TriggerRule.ALL_SUCCESS)
        def create_snapshot(release_id: str, dag_params, **context):
            """Create a snapshot of main table as a backup just in case something happens when applying the upserts and deletes."""

            from academic_observatory_workflows.pubmed_telescope import tasks
            from observatory_platform.airflow.release import release_from_bucket

            release = release_from_bucket(dag_params.cloud_workspace.download_bucket, release_id)
            tasks.create_snapshot(
                release,
                bq_dataset_id=dag_params.bq_dataset_id,
                bq_main_table_name=dag_params.bq_main_table_name,
                snapshot_expiry_days=dag_params.snapshot_expiry_days,
            )

        @task.branch
        def branch_baseline_or_updatefiles(release_id: str, dag_params, **context):
            """ """
            from academic_observatory_workflows.pubmed_telescope import tasks
            from observatory_platform.airflow.release import release_from_bucket

            release = release_from_bucket(dag_params.cloud_workspace.download_bucket, release_id)
            return tasks.branch_baseline_or_updatefiles(release)

        @task_group
        def baseline(xcom: str, **context):
            @task.kubernetes(
                task_id="download",
                name=f"{dag_params.dag_id}-baseline-download",
                container_resources=gke_make_container_resources(
                    {"memory": "4G", "cpu": "4"}, dag_params.gke_params.gke_resource_overrides.get("baseline_download")
                ),
                trigger_rule=TriggerRule.ALL_SUCCESS,
                **kubernetes_task_params,
            )
            def baseline_download(release_id: str, dag_params, **context):
                from academic_observatory_workflows.pubmed_telescope import tasks
                from observatory_platform.airflow.release import release_from_bucket

                release = release_from_bucket(dag_params.cloud_workspace.download_bucket, release_id)
                tasks.baseline_download(
                    release,
                    ftp_server_url=dag_params.ftp_server_url,
                    ftp_port=dag_params.ftp_port,
                    reset_ftp_counter=dag_params.reset_ftp_counter,
                    max_download_attempt=dag_params.max_download_attempt,
                )

            @task.kubernetes(
                task_id="transform",
                name=f"{dag_params.dag_id}-baseline-transform",
                container_resources=gke_make_container_resources(
                    {"memory": "16G", "cpu": "16"},
                    dag_params.gke_params.gke_resource_overrides.get("baseline_transform"),
                ),
                trigger_rule=TriggerRule.ALL_SUCCESS,
                **kubernetes_task_params,
            )
            def baseline_transform(release_id: str, dag_params, **context):
                """
                Transform the *.xml.gz files downloaded from PubMed into usable json files for BigQuery import.
                """
                from academic_observatory_workflows.pubmed_telescope import tasks
                from observatory_platform.airflow.release import release_from_bucket

                release = release_from_bucket(dag_params.cloud_workspace.download_bucket, release_id)
                tasks.baseline_transform(release, max_processes=dag_params.max_processes)

            @task.kubernetes(
                task_id="upload_transformed",
                name=f"{dag_params.dag_id}-baseline-upload-transformed",
                container_resources=gke_make_container_resources(
                    {"memory": "4G", "cpu": "4"},
                    dag_params.gke_params.gke_resource_overrides.get("baseline_upload_transformed"),
                ),
                trigger_rule=TriggerRule.ALL_SUCCESS,
                **kubernetes_task_params,
            )
            def baseline_upload_transformed(release_id: str, dag_params, **context):
                """Upload transformed baseline files to GCS."""

                from academic_observatory_workflows.pubmed_telescope import tasks
                from observatory_platform.airflow.release import release_from_bucket

                release = release_from_bucket(dag_params.cloud_workspace.download_bucket, release_id)
                tasks.baseline_upload_transformed(release)

            @task(task_id="bq_load", trigger_rule=TriggerRule.ALL_SUCCESS)
            def baseline_bq_load(release_id: str, dag_params, **context):
                """Ingest the baseline table from GCS to BQ using a file pattern."""

                from academic_observatory_workflows.pubmed_telescope import tasks
                from observatory_platform.airflow.release import release_from_bucket

                release = release_from_bucket(dag_params.cloud_workspace.download_bucket, release_id)
                tasks.baseline_bq_load(
                    release,
                    bq_dataset_description=dag_params.bq_dataset_description,
                    main_table_name=dag_params.bq_main_table_name,
                    baseline_table_description=dag_params.baseline_table_description,
                )

            task_download = baseline_download(xcom, dag_params)
            task_transform = baseline_transform(xcom, dag_params)
            task_upload_transformed = baseline_upload_transformed(xcom, dag_params)
            task_bq_load = baseline_bq_load(xcom, dag_params)

            (task_download >> task_transform >> task_upload_transformed >> task_bq_load)

        @task_group
        def updatefiles(xcom: dict, **context):
            @task.kubernetes(
                task_id="download",
                name=f"{dag_params.dag_id}-updatefiles-download",
                container_resources=gke_make_container_resources(
                    {"memory": "4G", "cpu": "4"},
                    dag_params.gke_params.gke_resource_overrides.get("updatefiles_download"),
                ),
                trigger_rule=TriggerRule.NONE_FAILED,
                **kubernetes_task_params,
            )
            def updatefiles_download(release_id: str, dag_params, **context):
                """
                Download the updatefiles from PubMed's FTP server for this release.

                Unable to do this in parallel due to limitations of their FTP server.
                """

                from academic_observatory_workflows.pubmed_telescope import tasks
                from observatory_platform.airflow.release import release_from_bucket

                release = release_from_bucket(dag_params.cloud_workspace.download_bucket, release_id)
                tasks.updatefiles_download(
                    release,
                    ftp_server_url=dag_params.ftp_server_url,
                    ftp_port=dag_params.ftp_port,
                    reset_ftp_counter=dag_params.reset_ftp_counter,
                    max_download_attempt=dag_params.max_download_attempt,
                )

            @task.kubernetes(
                task_id="transform",
                name=f"{dag_params.dag_id}-updatefiles-transform",
                container_resources=gke_make_container_resources(
                    {"memory": "16G", "cpu": "16"},
                    dag_params.gke_params.gke_resource_overrides.get("updatefiles_transform"),
                ),
                trigger_rule=TriggerRule.NONE_FAILED,
                **kubernetes_task_params,
            )
            def updatefiles_transform(release_id: str, dag_params, **context):
                """
                Transform the *.xml.gz files downloaded from PubMed's FTP server into usable json-like files for BigQuery import.

                This is a multithreaded and pulls the PubmedArticle records from the downloaded XML files.
                """

                from academic_observatory_workflows.pubmed_telescope import tasks
                from observatory_platform.airflow.release import release_from_bucket, release_to_bucket

                release = release_from_bucket(dag_params.cloud_workspace.download_bucket, release_id)
                data = tasks.updatefiles_transform(release, max_processes=dag_params.max_processes)
                updatefiles_id = release_to_bucket(data, dag_params.cloud_workspace.download_bucket)
                return updatefiles_id

            @task.kubernetes(
                task_id="merge_upserts_deletes",
                name=f"{dag_params.dag_id}-updatefiles-merge-upserts-deletes",
                container_resources=gke_make_container_resources(
                    {"memory": "16G", "cpu": "4"},
                    dag_params.gke_params.gke_resource_overrides.get("updatefiles_merge_upserts_deletes"),
                ),
                trigger_rule=TriggerRule.NONE_FAILED,
                **kubernetes_task_params,
            )
            def updatefiles_merge_upserts_deletes(release_id: str, updatefiles_id, dag_params, **context):
                """Merge the upserts and deletes for this release period."""

                from academic_observatory_workflows.pubmed_telescope import tasks
                from observatory_platform.airflow.release import release_from_bucket

                release = release_from_bucket(dag_params.cloud_workspace.download_bucket, release_id)
                updatefiles_data = release_from_bucket(dag_params.cloud_workspace.download_bucket, updatefiles_id)
                tasks.updatefiles_merge_upserts_deletes(
                    release, updatefiles_data, max_processes=dag_params.max_processes
                )

            @task.kubernetes(
                task_id="upload_merged_upsert_records",
                name=f"{dag_params.dag_id}-updatefiles-upload-merged-upsert-records",
                container_resources=gke_make_container_resources(
                    {"memory": "4G", "cpu": "4"},
                    dag_params.gke_params.gke_resource_overrides.get("updatefiles_upload_merged_upsert_records"),
                ),
                trigger_rule=TriggerRule.NONE_FAILED,
                **kubernetes_task_params,
            )
            def updatefiles_upload_merged_upsert_records(release_id: str, dag_params, **context):
                """Upload the merged upsert records to GCS."""

                from academic_observatory_workflows.pubmed_telescope import tasks
                from observatory_platform.airflow.release import release_from_bucket

                release = release_from_bucket(dag_params.cloud_workspace.download_bucket, release_id)
                tasks.updatefiles_upload_merged_upsert_records(release)

            @task(task_id="bq_load_upsert_table", trigger_rule=TriggerRule.NONE_FAILED)
            def updatefiles_bq_load_upsert_table(release_id: str, dag_params, **context):
                """Ingest the upsert records from GCS to BQ using a glob pattern."""

                from academic_observatory_workflows.pubmed_telescope import tasks
                from observatory_platform.airflow.release import release_from_bucket

                release = release_from_bucket(dag_params.cloud_workspace.download_bucket, release_id)
                tasks.updatefiles_bq_load_upsert_table(
                    release,
                    upsert_table_name=dag_params.bq_upsert_table_name,
                    upsert_table_description=dag_params.upsert_table_description,
                )

            @task(task_id="bq_upsert_records", trigger_rule=TriggerRule.NONE_FAILED)
            def updatefiles_bq_upsert_records(release_id: str, dag_params, **context):
                """
                Upsert records into the main table.

                Has to match on both the PMID value and the Version number, as there could be multiple different versions in
                the main table.
                """

                from academic_observatory_workflows.pubmed_telescope import tasks
                from observatory_platform.airflow.release import release_from_bucket

                release = release_from_bucket(dag_params.cloud_workspace.download_bucket, release_id)
                tasks.updatefiles_bq_upsert_records(
                    release,
                    main_table_name=dag_params.bq_main_table_name,
                    upsert_table_name=dag_params.bq_upsert_table_name,
                )

            @task.kubernetes(
                task_id="upload_merged_delete_records",
                name=f"{dag_params.dag_id}-updatefiles-upload-merged-delete-records",
                container_resources=gke_make_container_resources(
                    {"memory": "4G", "cpu": "4"},
                    dag_params.gke_params.gke_resource_overrides.get("updatefiles_upload_merged_delete_records"),
                ),
                trigger_rule=TriggerRule.NONE_FAILED,
                **kubernetes_task_params,
            )
            def updatefiles_upload_merged_delete_records(release_id: str, dag_params, **context):
                """Upload the merged delete records to GCS."""

                from academic_observatory_workflows.pubmed_telescope import tasks
                from observatory_platform.airflow.release import release_from_bucket

                release = release_from_bucket(dag_params.cloud_workspace.download_bucket, release_id)
                tasks.updatefiles_upload_merged_delete_records(release)

            @task(task_id="bq_load_delete_table", trigger_rule=TriggerRule.NONE_FAILED)
            def updatefiles_bq_load_delete_table(release_id: str, dag_params, **context):
                """Ingest delete records from GCS to BQ."""

                from academic_observatory_workflows.pubmed_telescope import tasks
                from observatory_platform.airflow.release import release_from_bucket

                release = release_from_bucket(dag_params.cloud_workspace.download_bucket, release_id)
                tasks.updatefiles_bq_load_delete_table(
                    release,
                    delete_table_name=dag_params.bq_delete_table_name,
                    delete_table_description=dag_params.delete_table_description,
                )

            @task(task_id="bq_delete_records", trigger_rule=TriggerRule.NONE_FAILED)
            def updatefiles_bq_delete_records(release_id: str, dag_params, **context):
                """
                Removed records from the main table that are specified in delete table.

                Has to match on both the PMID value and the Version number, as there could be multiple different versions in
                the main table.
                """

                from academic_observatory_workflows.pubmed_telescope import tasks
                from observatory_platform.airflow.release import release_from_bucket

                release = release_from_bucket(dag_params.cloud_workspace.download_bucket, release_id)
                tasks.updatefiles_bq_delete_records(
                    release,
                    main_table_name=dag_params.bq_main_table_name,
                    delete_table_name=dag_params.bq_delete_table_name,
                )

            task_download = updatefiles_download(xcom, dag_params)
            task_transform_xcom_updatefiles = updatefiles_transform(xcom, dag_params)
            task_merge_upserts_deletes = updatefiles_merge_upserts_deletes(
                xcom, task_transform_xcom_updatefiles, dag_params
            )
            task_upload_merged_upsert_records = updatefiles_upload_merged_upsert_records(xcom, dag_params)
            task_bq_load_upsert_table = updatefiles_bq_load_upsert_table(xcom, dag_params)
            task_bq_upsert_records = updatefiles_bq_upsert_records(xcom, dag_params)
            task_upload_merged_delete_records = updatefiles_upload_merged_delete_records(xcom, dag_params)
            task_bq_load_delete_table = updatefiles_bq_load_delete_table(xcom, dag_params)
            task_bq_delete_records = updatefiles_bq_delete_records(xcom, dag_params)

            (
                task_download
                >> task_transform_xcom_updatefiles
                >> task_merge_upserts_deletes
                >> task_upload_merged_upsert_records
                >> task_bq_load_upsert_table
                >> task_bq_upsert_records
                >> task_upload_merged_delete_records
                >> task_bq_load_delete_table
                >> task_bq_delete_records
            )

        @task(trigger_rule=TriggerRule.NONE_FAILED)
        def add_dataset_releases(release_id: str, dag_params, **context):
            """Adds release information to the API."""

            from academic_observatory_workflows.pubmed_telescope import tasks
            from observatory_platform.airflow.release import release_from_bucket

            release = release_from_bucket(dag_params.cloud_workspace.download_bucket, release_id)
            tasks.add_dataset_releases(release, api_bq_dataset_id=dag_params.api_bq_dataset_id)

        @task
        def cleanup_workflow(release_id: str, dag_params, **context):
            """
            Cleanup files from this workflow run.

            Delete local download files, transform files and current task instance.transform
            """

            from academic_observatory_workflows.pubmed_telescope import tasks
            from observatory_platform.airflow.release import release_from_bucket

            release = release_from_bucket(dag_params.cloud_workspace.download_bucket, release_id)
            tasks.cleanup_workflow(release)

        external_task_id = "dag_run_complete"
        if dag_params.test_run:
            sensor = EmptyOperator(task_id="wait_for_prev_dag_run")
        else:
            sensor = PreviousDagRunSensor(dag_id=dag_params.dag_id, external_task_id=external_task_id)
        task_check_dependencies = check_dependencies()
        xcom_release_id = fetch_release()
        task_shortcircuit = short_circuit(xcom_release_id, dag_params)
        task_create_snapshot = create_snapshot(xcom_release_id, dag_params)
        task_create_storage = gke_create_storage(
            volume_name=dag_params.gke_params.gke_volume_name,
            volume_size=dag_params.gke_params.gke_volume_size,
            kubernetes_conn_id=dag_params.gke_params.gke_conn_id,
        )
        task_branch_baseline_or_updatefiles = branch_baseline_or_updatefiles(xcom_release_id, dag_params)
        task_group_baseline = baseline(xcom_release_id)
        task_group_updatefiles = updatefiles(xcom_release_id)
        task_delete_storage = gke_delete_storage(
            volume_name=dag_params.gke_params.gke_volume_name,
            kubernetes_conn_id=dag_params.gke_params.gke_conn_id,
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        )
        task_add_dataset_releases = add_dataset_releases(xcom_release_id, dag_params)
        task_cleanup_workflow = cleanup_workflow(xcom_release_id, dag_params)
        task_dag_run_complete = EmptyOperator(task_id=external_task_id)
        task_merge_branches = EmptyOperator(task_id="merge_branches")

        # Define DAG structure
        (
            sensor
            >> task_check_dependencies
            >> xcom_release_id
            >> task_shortcircuit
            >> task_create_snapshot
            >> task_create_storage
            >> task_branch_baseline_or_updatefiles
            >> [task_group_baseline, task_group_updatefiles]
        )

        task_group_baseline >> task_group_updatefiles
        task_group_updatefiles >> task_merge_branches

        # Final steps of the DAG
        (
            task_merge_branches
            >> task_delete_storage
            >> task_add_dataset_releases
            >> task_cleanup_workflow
            >> task_dag_run_complete
        )

    return pubmed()
