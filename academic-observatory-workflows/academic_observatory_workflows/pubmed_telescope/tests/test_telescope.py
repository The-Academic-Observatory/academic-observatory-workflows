# Copyright 2023 Curtin University
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

import os
from ftplib import FTP
from typing import Dict, List
import unittest

import pendulum
from airflow.utils.state import State

from academic_observatory_workflows.config import project_path
from academic_observatory_workflows.pubmed_telescope.datafile import Datafile
from observatory_platform.google.bigquery import bq_run_query, bq_sharded_table_id, bq_table_id
from observatory_platform.google.gcs import gcs_blob_name_from_path
from observatory_platform.airflow.workflow import Workflow
from observatory_platform.sandbox.test_utils import find_free_port

from academic_observatory_workflows.config import project_path, TestConfig
from academic_observatory_workflows.pubmed_telescope.telescope import create_dag, DagParams
from observatory_platform.airflow.airflow import clear_airflow_connections, upsert_airflow_connection
from observatory_platform.dataset_api import DatasetAPI
from observatory_platform.google.bigquery import bq_sharded_table_id
from observatory_platform.sandbox.sandbox_environment import SandboxEnvironment
from observatory_platform.sandbox.test_utils import SandboxTestCase

FIXTURES_FOLDER = project_path("pubmed_telescope", "tests", "fixtures")


def query_table(table_id: str, select_columns: str, order_by_field: str) -> List[Dict]:
    """Query a BigQuery table, sorting the results and returning results as a list of dicts.

    :param table_id: the table id.
    :param select_columns: Columns to pull from the table.
    :param order_by_field: what field or fields to order by.
    :return: the table rows.
    """

    return [
        dict(row) for row in bq_run_query(f"SELECT {select_columns} FROM {table_id} ORDER BY {order_by_field} ASC;")
    ]


class PubMedTest:
    # FTP Server params
    ftp_server_url = "localhost"
    ftp_port = 80
    baseline_path = "/pubmed/baseline/"
    updatefiles_path = "/pubmed/updatefiles/"

    # Expected values for how the Pubmed Telescope should run.
    first_run = {
        "ftp_hosted_files": {
            "pubmed/baseline/pubmed22n0001.xml.gz": pendulum.datetime(year=2021, month=12, day=2),
            "pubmed/baseline/pubmed22n0002.xml.gz": pendulum.datetime(year=2021, month=12, day=2),
            "pubmed/updatefiles/pubmed22n0003.xml.gz": pendulum.datetime(year=2021, month=12, day=3),
            "pubmed/updatefiles/pubmed22n0004.xml.gz": pendulum.datetime(year=2021, month=12, day=4),
            "pubmed/updatefiles/pubmed22n0005.xml.gz": pendulum.datetime(year=2021, month=12, day=30),
        },
        # logical_date is the end of the data_interval
        "logical_date": pendulum.datetime(year=2021, month=12, day=5),
        "release_interval_start": pendulum.datetime(year=2021, month=12, day=2),
        "release_interval_end": pendulum.datetime(year=2021, month=12, day=12),
        "baseline_upload_date": pendulum.datetime(year=2021, month=12, day=2),
        "is_first_run": True,
        "year_first_run": True,
        "md5hash_download": {
            "pubmed22n0001.xml.gz": "73624a987b3572221fdd53ebefa1043f",
            "pubmed22n0002.xml.gz": "24da7ffc1afb277044ee1ba8cddb4e74",
            "pubmed22n0003.xml.gz": "d6da2c87390489d22cdeb6e046b77da1",
            "pubmed22n0004.xml.gz": "83764fc19cd98d247dc5603ca65569e6",
        },
        "PMID_list": [
            {"f0_": {"_field_1": "1", "_field_2": "1"}},
            {"f0_": {"_field_1": "2", "_field_2": "2"}},
            {"f0_": {"_field_1": "1", "_field_2": "30970"}},
            {"f0_": {"_field_1": "1", "_field_2": "36519887"}},
            {"f0_": {"_field_1": "1", "_field_2": "36519888"}},
        ],
    }
    # Regular update for Pubmed. No new baseline files but download and process the updatefiles.
    second_run = {
        # Need to change the upload dates of the
        "ftp_hosted_files": {
            "pubmed/baseline/pubmed22n0001.xml.gz": pendulum.datetime(year=2021, month=12, day=2),
            "pubmed/baseline/pubmed22n0002.xml.gz": pendulum.datetime(year=2021, month=12, day=2),
            "pubmed/updatefiles/pubmed22n0003.xml.gz": pendulum.datetime(year=2021, month=12, day=3),
            "pubmed/updatefiles/pubmed22n0004.xml.gz": pendulum.datetime(year=2021, month=12, day=4),
            "pubmed/updatefiles/pubmed22n0005.xml.gz": pendulum.datetime(year=2021, month=12, day=11),
        },
        # logical_date is the end of the data_interval
        "logical_date": pendulum.datetime(year=2021, month=12, day=12),
        "release_interval_start": pendulum.datetime(year=2021, month=12, day=12),
        "release_interval_end": pendulum.datetime(year=2021, month=12, day=19),
        "baseline_upload_date": pendulum.datetime(year=2021, month=12, day=2),
        "is_first_run": False,
        "year_first_run": False,
        "md5hash_download": {
            "pubmed22n0005.xml.gz": "9c61c5b19f021cadfc57845d0d1dcbc9",
        },
        "update_tables": {
            "additions": 2,
            "deletions": 1,
        },
        "PMID_list": [
            {"f0_": {"_field_1": "1", "_field_2": "1"}},
            {"f0_": {"_field_1": "1", "_field_2": "2994179"}},
            {"f0_": {"_field_1": "1", "_field_2": "2994180"}},
            {"f0_": {"_field_1": "1", "_field_2": "30970"}},
            {"f0_": {"_field_1": "1", "_field_2": "36519887"}},
            {"f0_": {"_field_1": "1", "_field_2": "36519888"}},
        ],
    }
    # New yearly run of Pubmed. Grab newly available baseline files and process them.
    # This is to only make sure that the new yearly baseline is detected and will be downloaded and processed
    # along with any updatefiles with in the release period.
    third_run = {
        "ftp_hosted_files": {
            "pubmed/baseline/pubmed22n0001.xml.gz": pendulum.datetime(year=2022, month=12, day=8),
            "pubmed/baseline/pubmed22n0002.xml.gz": pendulum.datetime(year=2022, month=12, day=8),
            "pubmed/updatefiles/pubmed22n0003.xml.gz": pendulum.datetime(year=2022, month=12, day=9),
            "pubmed/updatefiles/pubmed22n0004.xml.gz": pendulum.datetime(year=2022, month=12, day=10),
            "pubmed/updatefiles/pubmed22n0005.xml.gz": pendulum.datetime(year=2022, month=12, day=21),
        },
        # logical_date is the end of the data_interval
        "logical_date": pendulum.datetime(year=2022, month=12, day=11),
        "release_interval_start": pendulum.datetime(year=2022, month=12, day=8),
        "release_interval_end": pendulum.datetime(year=2022, month=12, day=11),
        "baseline_upload_date": pendulum.datetime(year=2022, month=12, day=8),
        "is_first_run": False,
        "year_first_run": True,
        "PMID_list": [
            {"f0_": {"_field_1": "1", "_field_2": "1"}},
            {"f0_": {"_field_1": "2", "_field_2": "2"}},
            {"f0_": {"_field_1": "1", "_field_2": "30970"}},
            {"f0_": {"_field_1": "1", "_field_2": "36519887"}},
            {"f0_": {"_field_1": "1", "_field_2": "36519888"}},
        ],
    }


class TestPubMedTelescope(SandboxTestCase):
    """Tests for the Pubmed telescope"""

    def __init__(self, *args, **kwargs):
        self.dag_id = "pubmed"
        super(TestPubMedTelescope, self).__init__(*args, **kwargs)

    def test_dag_structure(self):
        """Test PubMed DAG structure."""

        dag_params = DagParams(dag_id=self.dag_id, cloud_workspace=self.fake_cloud_workspace)
        dag = create_dag(dag_params)

        self.assert_dag_structure(
            {
                "wait_for_prev_dag_run": ["check_dependencies"],
                "check_dependencies": ["fetch_release"],
                "fetch_release": [
                    "short_circuit",
                    "create_snapshot",
                    "branch_baseline_or_updatefiles",
                    "baseline.baseline_download",
                    "baseline.baseline_upload_downloaded",
                    "baseline.baseline_transform",
                    "baseline.baseline_upload_transformed",
                    "baseline.baseline_bq_load",
                    "branch_updatefiles_or_dataset_release",
                    "updatefiles.updatefiles_download",
                    "updatefiles.updatefiles_upload_downloaded",
                    "updatefiles.updatefiles_transform",
                    "updatefiles.updatefiles_merge_upserts_deletes",
                    "updatefiles.updatefiles_upload_merged_upsert_records",
                    "updatefiles.updatefiles_bq_load_upsert_table",
                    "updatefiles.updatefiles_bq_upsert_records",
                    "updatefiles.updatefiles_upload_merged_delete_records",
                    "updatefiles.updatefiles_bq_load_delete_table",
                    "updatefiles.updatefiles_bq_delete_records",
                    "add_dataset_releases",
                    "cleanup_workflow",
                ],
                "short_circuit": ["create_snapshot"],
                "create_snapshot": ["branch_baseline_or_updatefiles"],
                "branch_baseline_or_updatefiles": ["baseline.baseline_download", "updatefiles.updatefiles_download"],
                "baseline.baseline_download": ["baseline.baseline_upload_downloaded"],
                "baseline.baseline_upload_downloaded": ["baseline.baseline_transform"],
                "baseline.baseline_transform": ["baseline.baseline_upload_transformed"],
                "baseline.baseline_upload_transformed": ["baseline.baseline_bq_load"],
                "baseline.baseline_bq_load": ["branch_updatefiles_or_dataset_release"],
                "branch_updatefiles_or_dataset_release": ["updatefiles.updatefiles_download", "add_dataset_releases"],
                "updatefiles.updatefiles_download": ["updatefiles.updatefiles_upload_downloaded"],
                "updatefiles.updatefiles_upload_downloaded": ["updatefiles.updatefiles_transform"],
                "updatefiles.updatefiles_transform": ["updatefiles.updatefiles_merge_upserts_deletes"],
                "updatefiles.updatefiles_merge_upserts_deletes": [
                    "updatefiles.updatefiles_upload_merged_upsert_records"
                ],
                "updatefiles.updatefiles_upload_merged_upsert_records": [
                    "updatefiles.updatefiles_bq_load_upsert_table"
                ],
                "updatefiles.updatefiles_bq_load_upsert_table": ["updatefiles.updatefiles_bq_upsert_records"],
                "updatefiles.updatefiles_bq_upsert_records": ["updatefiles.updatefiles_upload_merged_delete_records"],
                "updatefiles.updatefiles_upload_merged_delete_records": [
                    "updatefiles.updatefiles_bq_load_delete_table"
                ],
                "updatefiles.updatefiles_bq_load_delete_table": ["updatefiles.updatefiles_bq_delete_records"],
                "updatefiles.updatefiles_bq_delete_records": ["add_dataset_releases"],
                "add_dataset_releases": ["cleanup_workflow"],
                "cleanup_workflow": ["dag_run_complete"],
                "dag_run_complete": [],
            },
            dag,
        )

    def test_dag_load(self):
        """Test that the PubMed DAG can be loaded from a DAG bag."""

        env = SandboxEnvironment(
            workflows=[
                Workflow(
                    dag_id=self.dag_id,
                    name="PubMed Telescope",
                    class_name="academic_observatory_workflows.pubmed_telescope.telescope",
                    cloud_workspace=self.fake_cloud_workspace,
                )
            ]
        )

        with env.create():
            dag_file = os.path.join(project_path(), "..", "..", "dags", "load_dags.py")
            self.assert_dag_load(self.dag_id, dag_file)

    def test_telescope(self):
        """Test the PubMed Telescope end to end"""
        env = SandboxEnvironment(project_id=TestConfig.gcp_project_id, data_location=TestConfig.gcp_data_location)
        api_bq_dataset_id = env.add_dataset("pubmed_api")
        bq_dataset_id = env.add_dataset("pubmed")

        with env.create(task_logging=True):
            clear_airflow_connections()
            upsert_airflow_connection(**TestConfig.gke_cluster_connection)

            # Make an http server to serve the test files
            task_resources = {
                "baseline_download": {"memory": "2G", "cpu": "2"},
                "baseline_upload_downloaded": {"memory": "2G", "cpu": "2"},
                "baseline_transform": {"memory": "2G", "cpu": "2"},
                "baseline_upload_transformed": {"memory": "2G", "cpu": "2"},
                "baseline_upload_transformed": {"memory": "2G", "cpu": "2"},
                "updatefiles_download": {"memory": "2G", "cpu": "2"},
                "updatefiles_upload_downloaded": {"memory": "2G", "cpu": "2"},
                "updatefiles_transform": {"memory": "2G", "cpu": "2"},
                "updatefiles_merge_upserts_deletes": {"memory": "2G", "cpu": "2"},
                "updatefiles_upload_merged_upsert_records": {"memory": "2G", "cpu": "2"},
                "updatefiles_upload_merged_delete_records": {"memory": "2G", "cpu": "2"},
            }
            test_params = DagParams(
                dag_id="test_pubmed",
                cloud_workspace=env.cloud_workspace,
                bq_dataset_id=bq_dataset_id,
                api_bq_dataset_id=api_bq_dataset_id,
                max_processes=2,
                retries=0,
                max_download_attempt=1,
                ftp_port=TestConfig.ftp_port,
                ftp_server_url=TestConfig.ftp_host_url,
                gke_image=TestConfig.gke_image,
                gke_namespace=TestConfig.gke_namespace,
                gke_volume_name=TestConfig.gke_volume_name,
                gke_volume_path=TestConfig.gke_volume_path,
                gke_resource_overrides=task_resources,
                test_run=True,
            )
            api = DatasetAPI(bq_project_id=env.cloud_workspace.project_id, bq_dataset_id=test_params.api_bq_dataset_id)
            api.seed_db()

            main_table_id = bq_table_id(
                project_id=env.cloud_workspace.project_id,
                dataset_id=test_params.bq_dataset_id,
                table_id=test_params.bq_main_table_name,
            )
            upsert_table_id = bq_table_id(
                project_id=env.cloud_workspace.project_id,
                dataset_id=test_params.bq_dataset_id,
                table_id=test_params.bq_upsert_table_name,
            )
            delete_table_id = bq_table_id(
                project_id=env.cloud_workspace.project_id,
                dataset_id=test_params.bq_dataset_id,
                table_id=test_params.bq_delete_table_name,
            )

            # First execution
            # Before the tests start, we need to manually change the modified dates of the datafiles
            # on the locally hosted FTP server so that the workflow can grab the correct updatefiles.
            # Login and change the modified time for the datafiles
            ftp_conn = FTP()
            ftp_conn.connect(host="localhost", port=test_params.ftp_port)
            ftp_conn.login()
            for file_path, upload_date in PubMedTest.first_run["ftp_hosted_files"].items():
                ftp_command = f"MFMT {upload_date.format('YYYYMMDDHHmmss')} {file_path}"
                ftp_conn.sendcmd(ftp_command)
            ftp_conn.close()

            dag = create_dag(dag_params=test_params)
            dagrun = dag.test(execution_date=PubMedTest.first_run["logical_date"])

            # Make assertions
            if not dagrun.state == "success":
                raise RuntimeError("Frist Dagrun did not complete successfully")

            self.assert_table_integrity(main_table_id, 5)
            self.assert_table_integrity(upsert_table_id, 4)
            self.assert_table_integrity(delete_table_id, 2)
            result = query_table(
                main_table_id,
                "(MedlineCitation.PMID.Version, MedlineCitation.PMID.value)",
                "MedlineCitation.PMID.value",
            )
            self.assertEqual(result, PubMedTest.first_run["PMID_list"])

            # Assert that the dataset has been added to the observatory-api
            dataset_releases = api.get_dataset_releases(dag_id=test_params.dag_id, entity_id="pubmed")
            self.assertEqual(len(dataset_releases), 1)

            # Second run
            # Update the files to the new dates
            ftp_conn = FTP()
            ftp_conn.connect(host="localhost", port=test_params.ftp_port)
            ftp_conn.login()
            for file_path, upload_date in PubMedTest.second_run["ftp_hosted_files"].items():
                ftp_command = f"MFMT {upload_date.format('YYYYMMDDHHmmss')} {file_path}"
                ftp_conn.sendcmd(ftp_command)
            ftp_conn.close()

            dag = create_dag(dag_params=test_params)
            dagrun = dag.test(execution_date=PubMedTest.second_run["logical_date"])

            # Second run asssertions
            if not dagrun.state == "success":
                raise RuntimeError("Second Dagrun did not complete successfully")
            # snapshot_table_id = bq_sharded_table_id(
            #     project_id=test_params.project_id,
            #     dataset_id=test_params.bq_dataset_id,
            #     table_name=f"{bq_table_id}_snapshot",
            #     date=release.start_date,
            # )
            # self.assert_table_integrity(snapshot_table_id, 5)
            self.assert_table_integrity(upsert_table_id, 2)
            self.assert_table_integrity(delete_table_id, 1)
            self.assert_table_integrity(main_table_id, 6)
            result = query_table(
                main_table_id,
                "(MedlineCitation.PMID.Version, MedlineCitation.PMID.value)",
                "MedlineCitation.PMID.value",
            )
            self.assertEqual(result, PubMedTest.second_run["PMID_list"])

            # Check the dataset releases
            dataset_releases = api.get_dataset_releases(dag_id=test_params.dag_id, entity_id="pubmed")
            self.assertEqual(len(dataset_releases), 2)

    @unittest.skip
    def test_telescope_old(self):
        """Test the PubMed Telescope end to end"""

        env = ObservatoryEnvironment(self.project_id, self.data_location, api_port=find_free_port())
        bq_dataset_id = env.add_dataset()

        # Create mock FTP server that holds the testing Pubmed Files.
        ftp_server = FtpServer(host=self.ftp_server_url, port=self.ftp_port, directory=FIXTURES_FOLDER)

        with ftp_server.create():
            with env.create(task_logging=True):
                # Initialise the telescope workflow.
                bq_table_id = "pubmed"
                main_table_id = f"{env.cloud_workspace.project_id}.{bq_dataset_id}.{bq_table_id}"
                upsert_table_id = f"{main_table_id}_upsert"
                delete_table_id = f"{main_table_id}_delete"
                dag = create_dag(
                    dag_id=self.dag_id,
                    cloud_workspace=env.cloud_workspace,
                    bq_dataset_id=bq_dataset_id,
                    ftp_server_url=self.ftp_server_url,
                    ftp_port=self.ftp_port,
                    max_processes=1,
                )

                ######################
                ##### SECOND RUN #####

                # This run is to make sure that it can apply a sequential update.

                run = self.second_run
                with env.create_dag_run(dag, run["logical_date"]) as dag_run:
                    # Before the tests start, we need to manually change the modified dates of the datafiles
                    # on the locally hosted FTP server so that the workflow can grab the correct updatefiles.

                    # Change the date modified on the FTP server.
                    # Login as root and change the modified time for the datafiles
                    ftp_conn = FTP()
                    ftp_conn.connect(host=self.ftp_server_url, port=self.ftp_port)
                    ftp_conn.login(user="root", passwd="pass")
                    for file_path, upload_date in run["ftp_hosted_files"].items():
                        ftp_command = f"MFMT {upload_date.format('YYYYMMDDHHmmss')} {file_path}"
                        logging.info("FTP send command - {ftp_command}")
                        ftp_conn.sendcmd(ftp_command)
                    ftp_conn.close()

                    logging.info(f"Start date this workflow run {run['logical_date']}")

                    ### Wait for the previous DAG run to finish ###
                    ti = env.run_task("wait_for_prev_dag_run")
                    self.assertEqual(State.SUCCESS, ti.state)

                    ### Check Dependancies ###
                    ti = env.run_task("check_dependencies")
                    self.assertEqual(State.SUCCESS, ti.state)

                    ### List datafiles for release ###

                    # Fetch datafiles
                    ti = env.run_task("fetch_release")
                    self.assertEqual(State.SUCCESS, ti.state)

                    # Pull list of datafiles for this run from the Xcom
                    release = PubMedRelease.from_dict(
                        ti.xcom_pull(
                            key="return_value",
                            task_ids="fetch_release",
                            include_prior_dates=False,
                        )
                    )

                    # Check that dates and bools for workflow are correct from the release metadata dictionary.
                    self.assertEqual(run["release_interval_start"], release.start_date)
                    self.assertEqual(run["release_interval_end"], release.end_date)
                    self.assertEqual(run["baseline_upload_date"], release.baseline_upload_date)
                    self.assertEqual(run["year_first_run"], release.year_first_run)

                    # Make sure list of datafiles were built correctly for the workflow run.
                    self.assertEqual(len(run["datafiles"]), len(release.datafile_list))
                    for i in range(len(run["datafiles"])):
                        self.assertEqual(run["datafiles"][i], release.datafile_list[i])

                    ### Create Snapshot ###
                    ti = env.run_task("short_circuit")
                    self.assertEqual(State.SUCCESS, ti.state)

                    ### Create Snapshot ###
                    ti = env.run_task("create_snapshot")
                    self.assertEqual(State.SUCCESS, ti.state)

                    # Check that that snapshot table exists.
                    snapshot_table_id = bq_sharded_table_id(
                        env.cloud_workspace.project_id,
                        bq_dataset_id,
                        f"{bq_table_id}_snapshot",
                        release.start_date,
                    )
                    self.assert_table_integrity(snapshot_table_id, 5)

                    ti = env.run_task("branch_baseline_or_updatefiles")
                    self.assertEqual(State.SUCCESS, ti.state)

                    ##### BASELINE #####

                    # No new baseline files should be downloaded as it's already been done for this year.
                    self.assertEqual(len(release.baseline_files), 0)

                    ti = env.run_task("baseline.download")
                    self.assertEqual(State.SKIPPED, ti.state)
                    task_ids = [
                        "baseline.upload_downloaded",
                        "baseline.transform",
                        "baseline.upload_transformed",
                        "baseline.bq_load",
                        "branch_updatefiles_or_dataset_release",
                    ]
                    for task_id in task_ids:
                        print(task_id)
                        ti = env.skip_task(task_id)
                        self.assertEqual(State.SKIPPED, ti.state)

                    full_table_id = f"{env.cloud_workspace.project_id}.{bq_dataset_id}.{bq_table_id}"
                    self.assert_table_integrity(full_table_id, 5)

                    ##### UPDATEFILES #####

                    ### Download updatefiles ###
                    ti = env.run_task("updatefiles.download")
                    self.assertEqual(State.SUCCESS, ti.state)

                    # Loop through downloaded baseline files, check that they exist and that hashes match.
                    for datafile in release.updatefiles:
                        self.assertTrue(os.path.exists(datafile.download_file_path))
                        with open(datafile.download_file_path, "rb") as f_hash:
                            data = f_hash.read()
                            md5hash = hashlib.md5(data).hexdigest()
                            logging.info(f"md5hash for {datafile.filename} - {md5hash}")
                            self.assertEqual(md5hash, run["md5hash_download"][datafile.filename])

                    ### Upload downloaded updatefiles ###
                    ti = env.run_task("updatefiles.upload_downloaded")
                    self.assertEqual(State.SUCCESS, ti.state)

                    for datafile in release.updatefiles:
                        self.assert_blob_integrity(
                            env.download_bucket,
                            gcs_blob_name_from_path(datafile.download_file_path),
                            datafile.download_file_path,
                        )

                    ### Transform updatefiles ###
                    ti = env.run_task("updatefiles.transform")
                    self.assertEqual(State.SUCCESS, ti.state)

                    # This step pulls out the upserts and delete records from the files
                    for datafile in release.updatefiles:
                        self.assertTrue(os.path.exists(datafile.transform_upsert_file_path))

                    ### Merge upserts and deletes  ###
                    ti = env.run_task("updatefiles.merge_upserts_deletes")
                    self.assertEqual(State.SUCCESS, ti.state)

                    # Check that the merged upserts and deletes have been written to disk.
                    self.assertTrue(os.path.exists(release.merged_delete_file_path))
                    for datafile in release.updatefiles:
                        self.assertTrue(os.path.exists(datafile.merged_upsert_file_path))

                    ##### UPSERTS #####

                    ### Upload merged upsert records ###
                    ti = env.run_task("updatefiles.upload_merged_upsert_records")
                    self.assertEqual(State.SUCCESS, ti.state)

                    file_paths = [datafile.merged_upsert_file_path for datafile in release.updatefiles]

                    # Check that they exist in the cloud.
                    for file in file_paths:
                        logging.info(f"Transform_file_path - {file}")
                        self.assert_blob_integrity(
                            env.transform_bucket,
                            gcs_blob_name_from_path(file),
                            file,
                        )

                    ###  BQ load upsert table ###
                    ti = env.run_task("updatefiles.bq_load_upsert_table")
                    self.assertEqual(State.SUCCESS, ti.state)

                    self.assert_table_integrity(upsert_table_id, 2)

                    ###  BQ upsert records ###
                    ti = env.run_task("updatefiles.bq_upsert_records")
                    self.assertEqual(State.SUCCESS, ti.state)

                    ##### DELETES #####

                    file = release.merged_delete_file_path

                    ### Upload merged delete records ###
                    ti = env.run_task("updatefiles.upload_merged_delete_records")
                    self.assertEqual(State.SUCCESS, ti.state)

                    # Check that it exists in the cloud.
                    logging.info(f"Transform_file_path - {file}")
                    self.assert_blob_integrity(
                        env.transform_bucket,
                        gcs_blob_name_from_path(file),
                        file,
                    )

                    ###  BQ load delete table ###
                    ti = env.run_task("updatefiles.bq_load_delete_table")
                    self.assertEqual(State.SUCCESS, ti.state)

                    self.assert_table_integrity(delete_table_id, 1)

                    ###  BQ delete records ###
                    ti = env.run_task("updatefiles.bq_delete_records")
                    self.assertEqual(State.SUCCESS, ti.state)

                    # Check that upserts and deletes were applied properly.
                    self.assert_table_integrity(full_table_id, 6)
                    result = query_table(
                        full_table_id,
                        "(MedlineCitation.PMID.Version, MedlineCitation.PMID.value)",
                        "MedlineCitation.PMID.value",
                    )
                    self.assertEqual(result, run["PMID_list"])

                    ### add_dataset_release ###
                    # Assert that the dataset has been added to the observatory-api
                    # Get dataset releases before task run
                    dataset_releases = get_dataset_releases(dag_id=self.dag_id, dataset_id=bq_dataset_id)
                    self.assertEqual(len(dataset_releases), 1)
                    # Run task
                    ti = env.run_task("add_dataset_releases")
                    self.assertEqual(State.SUCCESS, ti.state)
                    # Check after task run.
                    dataset_releases = get_dataset_releases(dag_id=self.dag_id, dataset_id=bq_dataset_id)
                    self.assertEqual(len(dataset_releases), 2)

                    ### cleanup ###
                    # Test that all workflow data was deleted
                    ti = env.run_task("cleanup_workflow")
                    self.assertEqual(State.SUCCESS, ti.state)
                    self.assert_cleanup(release.workflow_folder)

                    ### dag_run_complete ###
                    ti = env.run_task("dag_run_complete")
                    self.assertEqual(State.SUCCESS, ti.state)

                ######################
                ##### THIRD RUN #####

                # This run only needs to confirm that the first year run bool works and it downloads the new baseline dataset.
                run = self.third_run
                with env.create_dag_run(dag, run["logical_date"]) as dag_run:
                    # Before the tests start, we need to manually change the modified dates of the datafiles
                    # on the locally hosted FTP server so that the workflow can grab the correct updatefiles.

                    # Change the date modified on the FTP server.
                    # Login as root and change the modified time for the datafiles
                    ftp_conn = FTP()
                    ftp_conn.connect(host=self.ftp_server_url, port=self.ftp_port)
                    ftp_conn.login(user="root", passwd="pass")
                    for file_path, upload_date in run["ftp_hosted_files"].items():
                        ftp_command = f"MFMT {upload_date.format('YYYYMMDDHHmmss')} {file_path}"
                        logging.info("FTP send command - {ftp_command}")
                        ftp_conn.sendcmd(ftp_command)
                    ftp_conn.close()

                    logging.info(f"Start date this workflow run {run['logical_date']}")

                    # Forcing this to SUCCESS because we are skipping a year between releases.
                    ### Check Dependancies ###
                    ti = env.run_task("check_dependencies")
                    ti.set_state(State.SUCCESS)
                    self.assertEqual(State.SUCCESS, ti.state)

                    ### List datafiles for release ###

                    # Fetch datafiles
                    ti = env.run_task("fetch_release")
                    self.assertEqual(State.SUCCESS, ti.state)

                    # Pull list of datafiles for this run from the Xcom
                    release = PubMedRelease.from_dict(
                        ti.xcom_pull(
                            key="return_value",
                            task_ids="fetch_release",
                            include_prior_dates=False,
                        )
                    )

                    # Check that dates and bools for workflow are correct from the release metadata dictionary.
                    self.assertEqual(run["release_interval_start"], release.start_date)
                    self.assertEqual(run["release_interval_end"], release.end_date)
                    self.assertEqual(run["baseline_upload_date"], release.baseline_upload_date)
                    self.assertEqual(run["year_first_run"], release.year_first_run)

                    # Make sure list of datafiles were built correctly for the workflow run.
                    self.assertEqual(len(run["datafiles"]), len(release.datafile_list))
                    for i in range(len(run["datafiles"])):
                        self.assertEqual(run["datafiles"][i], release.datafile_list[i])

                    ##### BASELINE #####

                    self.assertEqual(len(release.baseline_files), 2)

                    task_ids = [
                        "short_circuit",
                        "create_snapshot",
                        "branch_baseline_or_updatefiles",
                        "baseline.download",
                        "baseline.upload_downloaded",
                        "baseline.transform",
                        "baseline.upload_transformed",
                        "baseline.bq_load",
                        "branch_updatefiles_or_dataset_release",
                    ]

                    for task_id in task_ids:
                        logging.info(f"Running task: {task_id}")
                        ti = env.run_task(task_id)
                        self.assertEqual(State.SUCCESS, ti.state)

                    full_table_id = f"{env.cloud_workspace.project_id}.{bq_dataset_id}.{bq_table_id}"
                    self.assert_table_integrity(full_table_id, 4)

                    ##### UPSERTS #####

                    task_ids = [
                        "updatefiles.download",
                        "updatefiles.upload_downloaded",
                        "updatefiles.transform",
                        "updatefiles.merge_upserts_deletes",
                        "updatefiles.upload_merged_upsert_records",
                        "updatefiles.bq_load_upsert_table",
                        "updatefiles.bq_upsert_records",
                        "updatefiles.upload_merged_delete_records",
                        "updatefiles.bq_load_delete_table",
                        "updatefiles.bq_delete_records",
                    ]

                    for task_id in task_ids:
                        logging.info(f"Running task: {task_id}")
                        ti = env.run_task(task_id)
                        self.assertEqual(State.SUCCESS, ti.state)

                    # Check that upserts and deletes were applied properly.
                    self.assert_table_integrity(full_table_id, 5)
                    result = query_table(
                        full_table_id,
                        "(MedlineCitation.PMID.Version, MedlineCitation.PMID.value)",
                        "MedlineCitation.PMID.value",
                    )
                    self.assertEqual(result, run["PMID_list"])

                    ### add_dataset_release ###
                    # Assert that the dataset has been added to the observatory-api
                    # Get dataset releases before task run
                    dataset_releases = get_dataset_releases(dag_id=self.dag_id, dataset_id=bq_dataset_id)
                    self.assertEqual(len(dataset_releases), 2)
                    # Run task
                    ti = env.run_task("add_dataset_releases")
                    self.assertEqual(State.SUCCESS, ti.state)
                    # Check after task run.
                    dataset_releases = get_dataset_releases(dag_id=self.dag_id, dataset_id=bq_dataset_id)
                    self.assertEqual(len(dataset_releases), 3)

                    ### cleanup ###
                    # Test that all workflow data deleted
                    ti = env.run_task("cleanup_workflow")
                    self.assertEqual(State.SUCCESS, ti.state)
                    self.assert_cleanup(release.workflow_folder)

                    ### dag_run_complete ###
                    ti = env.run_task("dag_run_complete")
                    self.assertEqual(State.SUCCESS, ti.state)
