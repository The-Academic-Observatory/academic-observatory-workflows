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

import gzip
import hashlib
import json
import logging
import os
import shutil
from ftplib import FTP
from typing import Dict, List

import pendulum
from airflow.utils.state import State
from Bio.Entrez.Parser import DictionaryElement, ListElement, StringElement
from click.testing import CliRunner

from academic_observatory_workflows.config import project_path
from academic_observatory_workflows.pubmed_telescope.pubmed_telescope import (
    add_attributes,
    change_pubmed_list_structure,
    create_dag,
    Datafile,
    download_datafiles,
    load_datafile,
    merge_upserts_and_deletes,
    parse_articles,
    parse_deletes,
    PMID,
    PubMedCustomEncoder,
    PubMedRelease,
    PubmedUpdatefile,
    save_pubmed_jsonl,
    save_pubmed_merged_upserts,
    transform_pubmed,
)
from observatory.platform.api import get_dataset_releases
from observatory.platform.bigquery import bq_run_query, bq_sharded_table_id
from observatory.platform.config import module_file_path
from observatory.platform.gcs import gcs_blob_name_from_path
from observatory.platform.observatory_config import Workflow
from observatory.platform.observatory_environment import (
    find_free_port,
    FtpServer,
    ObservatoryEnvironment,
    ObservatoryTestCase,
)
from observatory.platform.workflows.workflow import ChangefileRelease

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


class TestPubMedTelescope(ObservatoryTestCase):
    """Tests for the Pubmed telescope"""

    def __init__(self, *args, **kwargs):
        self.dag_id = "pubmed"
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")

        # FTP Server params
        self.ftp_server_url = "localhost"
        self.ftp_port = find_free_port()
        self.baseline_path = "/pubmed/baseline/"
        self.updatefiles_path = "/pubmed/updatefiles/"

        super(TestPubMedTelescope, self).__init__(*args, **kwargs)

        # Expected values for how the Pubmed Telescope should run.
        self.first_run = {
            "ftp_hosted_files": {
                "pubmed/baseline/pubmed22n0001.xml.gz": pendulum.datetime(year=2021, month=12, day=2),
                "pubmed/baseline/pubmed22n0002.xml.gz": pendulum.datetime(year=2021, month=12, day=2),
                "pubmed/updatefiles/pubmed22n0003.xml.gz": pendulum.datetime(year=2021, month=12, day=3),
                "pubmed/updatefiles/pubmed22n0004.xml.gz": pendulum.datetime(year=2021, month=12, day=4),
                "pubmed/updatefiles/pubmed22n0005.xml.gz": pendulum.datetime(year=2021, month=12, day=30),
            },
            # Execution_date is a week before the workflow's actual run date.
            "logical_date": pendulum.datetime(year=2021, month=12, day=5),
            "release_interval_start": pendulum.datetime(year=2021, month=12, day=2),
            "release_interval_end": pendulum.datetime(year=2021, month=12, day=12),
            "baseline_upload_date": pendulum.datetime(year=2021, month=12, day=2),
            "is_first_run": True,
            "year_first_run": True,
            "datafiles": [
                Datafile(
                    filename="pubmed22n0001.xml.gz",
                    file_index=1,
                    path_on_ftp=f"{self.baseline_path}pubmed22n0001.xml.gz",
                    baseline=True,
                    datafile_date=pendulum.datetime(year=2021, month=12, day=2),
                ),
                Datafile(
                    filename="pubmed22n0002.xml.gz",
                    file_index=2,
                    path_on_ftp=f"{self.baseline_path}pubmed22n0002.xml.gz",
                    baseline=True,
                    datafile_date=pendulum.datetime(year=2021, month=12, day=2),
                ),
                Datafile(
                    filename="pubmed22n0003.xml.gz",
                    file_index=3,
                    path_on_ftp=f"{self.updatefiles_path}pubmed22n0003.xml.gz",
                    baseline=False,
                    datafile_date=pendulum.datetime(year=2021, month=12, day=3),
                ),
                Datafile(
                    filename="pubmed22n0004.xml.gz",
                    file_index=4,
                    path_on_ftp=f"{self.updatefiles_path}pubmed22n0004.xml.gz",
                    baseline=False,
                    datafile_date=pendulum.datetime(year=2021, month=12, day=4),
                ),
            ],
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
        self.second_run = {
            # Need to change the upload dates of the
            "ftp_hosted_files": {
                "pubmed/baseline/pubmed22n0001.xml.gz": pendulum.datetime(year=2021, month=12, day=2),
                "pubmed/baseline/pubmed22n0002.xml.gz": pendulum.datetime(year=2021, month=12, day=2),
                "pubmed/updatefiles/pubmed22n0003.xml.gz": pendulum.datetime(year=2021, month=12, day=3),
                "pubmed/updatefiles/pubmed22n0004.xml.gz": pendulum.datetime(year=2021, month=12, day=4),
                "pubmed/updatefiles/pubmed22n0005.xml.gz": pendulum.datetime(year=2021, month=12, day=14),
            },
            # Execution_date is a week before the workflow's actual run date.
            "logical_date": pendulum.datetime(year=2021, month=12, day=12),
            "release_interval_start": pendulum.datetime(year=2021, month=12, day=12),
            "release_interval_end": pendulum.datetime(year=2021, month=12, day=19),
            "baseline_upload_date": pendulum.datetime(year=2021, month=12, day=2),
            "is_first_run": False,
            "year_first_run": False,
            "datafiles": [
                Datafile(
                    filename="pubmed22n0005.xml.gz",
                    file_index=5,
                    path_on_ftp=f"{self.updatefiles_path}pubmed22n0005.xml.gz",
                    baseline=False,
                    datafile_date=pendulum.datetime(year=2021, month=12, day=14),
                ),
            ],
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
        self.third_run = {
            "ftp_hosted_files": {
                "pubmed/baseline/pubmed22n0001.xml.gz": pendulum.datetime(year=2022, month=12, day=8),
                "pubmed/baseline/pubmed22n0002.xml.gz": pendulum.datetime(year=2022, month=12, day=8),
                "pubmed/updatefiles/pubmed22n0003.xml.gz": pendulum.datetime(year=2022, month=12, day=9),
                "pubmed/updatefiles/pubmed22n0004.xml.gz": pendulum.datetime(year=2022, month=12, day=10),
                "pubmed/updatefiles/pubmed22n0005.xml.gz": pendulum.datetime(year=2022, month=12, day=21),
            },
            # Execution_date is a week before the workflow's actual run date.
            "logical_date": pendulum.datetime(year=2022, month=12, day=4),
            "release_interval_start": pendulum.datetime(year=2022, month=12, day=8),
            "release_interval_end": pendulum.datetime(year=2022, month=12, day=11),
            "baseline_upload_date": pendulum.datetime(year=2022, month=12, day=8),
            "is_first_run": False,
            "year_first_run": True,
            "datafiles": [
                Datafile(
                    filename="pubmed22n0001.xml.gz",
                    file_index=1,
                    path_on_ftp=f"{self.baseline_path}pubmed22n0001.xml.gz",
                    baseline=True,
                    datafile_date=pendulum.datetime(year=2022, month=12, day=8),
                ),
                Datafile(
                    filename="pubmed22n0002.xml.gz",
                    file_index=2,
                    path_on_ftp=f"{self.baseline_path}pubmed22n0002.xml.gz",
                    baseline=True,
                    datafile_date=pendulum.datetime(year=2022, month=12, day=8),
                ),
                Datafile(
                    filename="pubmed22n0003.xml.gz",
                    file_index=3,
                    path_on_ftp=f"{self.updatefiles_path}pubmed22n0003.xml.gz",
                    baseline=False,
                    datafile_date=pendulum.datetime(year=2022, month=12, day=9),
                ),
                Datafile(
                    filename="pubmed22n0004.xml.gz",
                    file_index=4,
                    path_on_ftp=f"{self.updatefiles_path}pubmed22n0004.xml.gz",
                    baseline=False,
                    datafile_date=pendulum.datetime(year=2022, month=12, day=10),
                ),
            ],
            "PMID_list": [
                {"f0_": {"_field_1": "1", "_field_2": "1"}},
                {"f0_": {"_field_1": "2", "_field_2": "2"}},
                {"f0_": {"_field_1": "1", "_field_2": "30970"}},
                {"f0_": {"_field_1": "1", "_field_2": "36519887"}},
                {"f0_": {"_field_1": "1", "_field_2": "36519888"}},
            ],
        }

    def test_dag_structure(self):
        """Test PubMed DAG structure."""

        dag = create_dag(
            dag_id=self.dag_id,
            cloud_workspace=self.fake_cloud_workspace,
        )

        self.assert_dag_structure(
            {
                "wait_for_prev_dag_run": ["check_dependencies"],
                "check_dependencies": ["fetch_release"],
                "fetch_release": [
                    "short_circuit",
                    "create_snapshot",
                    "branch_baseline_or_updatefiles",
                    "baseline.download",
                    "baseline.upload_downloaded",
                    "baseline.transform",
                    "baseline.upload_transformed",
                    "baseline.bq_load",
                    "branch_updatefiles_or_dataset_release",
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
                    "add_dataset_releases",
                    "cleanup_workflow",
                ],
                "short_circuit": ["create_snapshot"],
                "create_snapshot": ["branch_baseline_or_updatefiles"],
                "branch_baseline_or_updatefiles": ["baseline.download", "updatefiles.download"],
                "baseline.download": ["baseline.upload_downloaded"],
                "baseline.upload_downloaded": ["baseline.transform"],
                "baseline.transform": ["baseline.upload_transformed"],
                "baseline.upload_transformed": ["baseline.bq_load"],
                "baseline.bq_load": ["branch_updatefiles_or_dataset_release"],
                "branch_updatefiles_or_dataset_release": ["updatefiles.download", "add_dataset_releases"],
                "updatefiles.download": ["updatefiles.upload_downloaded"],
                "updatefiles.upload_downloaded": ["updatefiles.transform"],
                "updatefiles.transform": ["updatefiles.merge_upserts_deletes"],
                "updatefiles.merge_upserts_deletes": ["updatefiles.upload_merged_upsert_records"],
                "updatefiles.upload_merged_upsert_records": ["updatefiles.bq_load_upsert_table"],
                "updatefiles.bq_load_upsert_table": ["updatefiles.bq_upsert_records"],
                "updatefiles.bq_upsert_records": ["updatefiles.upload_merged_delete_records"],
                "updatefiles.upload_merged_delete_records": ["updatefiles.bq_load_delete_table"],
                "updatefiles.bq_load_delete_table": ["updatefiles.bq_delete_records"],
                "updatefiles.bq_delete_records": ["add_dataset_releases"],
                "add_dataset_releases": ["cleanup_workflow"],
                "cleanup_workflow": ["dag_run_complete"],
                "dag_run_complete": [],
            },
            dag,
        )

    def test_dag_load(self):
        """Test that the PubMed DAG can be loaded from a DAG bag."""

        env = ObservatoryEnvironment(
            workflows=[
                Workflow(
                    dag_id=self.dag_id,
                    name="PubMed Telescope",
                    class_name="academic_observatory_workflows.pubmed_telescope.pubmed_telescope.create_dag",
                    cloud_workspace=self.fake_cloud_workspace,
                )
            ]
        )

        with env.create():
            dag_file = os.path.join(module_file_path("observatory.platform.dags"), "load_dags.py")
            self.assert_dag_load(self.dag_id, dag_file)

    def test_telescope(self):
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

                #####################
                ##### FIRST RUN #####

                # Initial intake of the Pubmed dataset.

                run = self.first_run
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

                    ti = env.run_task("short_circuit")
                    self.assertEqual(State.SUCCESS, ti.state)

                    ### Create Snapshot ###
                    ti = env.run_task("create_snapshot")
                    self.assertEqual(State.SUCCESS, ti.state)

                    ti = env.run_task("branch_baseline_or_updatefiles")
                    self.assertEqual(State.SUCCESS, ti.state)

                    ##### BASELINE #####

                    ### Download baseline ###
                    ti = env.run_task("baseline.download")
                    self.assertEqual(State.SUCCESS, ti.state)

                    # Loop through downloaded baseline files, check that they exist and that hashes match.
                    for datafile in release.baseline_files:
                        self.assertTrue(os.path.exists(datafile.download_file_path))
                        with open(datafile.download_file_path, "rb") as f_hash:
                            data = f_hash.read()
                            md5hash = hashlib.md5(data).hexdigest()
                            logging.info(f"md5hash for {datafile.filename} - {md5hash}")
                            self.assertEqual(md5hash, run["md5hash_download"][datafile.filename])

                    ### Upload downloaded baseline ###
                    ti = env.run_task("baseline.upload_downloaded")
                    self.assertEqual(State.SUCCESS, ti.state)

                    for datafile in release.baseline_files:
                        self.assert_blob_integrity(
                            env.download_bucket,
                            gcs_blob_name_from_path(datafile.download_file_path),
                            datafile.download_file_path,
                        )

                    ### Transform baseline ###
                    ti = env.run_task("baseline.transform")
                    self.assertEqual(State.SUCCESS, ti.state)

                    for datafile in release.baseline_files:
                        self.assertTrue(os.path.exists(datafile.transform_baseline_file_path))

                    ### Upload transformed baseline ###
                    ti = env.run_task("baseline.upload_transformed")
                    self.assertEqual(State.SUCCESS, ti.state)

                    # Get list of transformed files for upload.
                    file_paths = [datafile.transform_baseline_file_path for datafile in release.baseline_files]

                    for file in file_paths:
                        logging.info(f"Transform_file_path - {file}")
                        self.assert_blob_integrity(
                            env.transform_bucket,
                            gcs_blob_name_from_path(file),
                            file,
                        )

                    ###  BQ load main table ###
                    ti = env.run_task("baseline.bq_load")
                    self.assertEqual(State.SUCCESS, ti.state)

                    full_table_id = f"{env.cloud_workspace.project_id}.{bq_dataset_id}.{bq_table_id}"
                    self.assert_table_integrity(full_table_id, 4)

                    ti = env.run_task("branch_updatefiles_or_dataset_release")
                    self.assertEqual(State.SUCCESS, ti.state)

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

                    # This step pulls out the upserts from the updatefiles and writes them as *.jsonl
                    for datafile in release.updatefiles:
                        self.assertTrue(os.path.exists(datafile.transform_upsert_file_path))

                    ### Merge upserts and deletes ###
                    ti = env.run_task("updatefiles.merge_upserts_deletes")
                    self.assertEqual(State.SUCCESS, ti.state)

                    # Check that the merged upserts and deletes have been written to disk.
                    self.assertTrue(os.path.exists(release.merged_delete_file_path))
                    for datafile in release.updatefiles:
                        self.assertTrue(os.path.exists(datafile.merged_upsert_file_path))

                    ##### UPSERTS #####

                    file_paths = [datafile.merged_upsert_file_path for datafile in release.updatefiles]

                    ### Upload merged upsert records ###
                    ti = env.run_task("updatefiles.upload_merged_upsert_records")
                    self.assertEqual(State.SUCCESS, ti.state)

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

                    self.assert_table_integrity(upsert_table_id, 4)

                    ###  BQ upsert records ###
                    ti = env.run_task("updatefiles.bq_upsert_records")
                    self.assertEqual(State.SUCCESS, ti.state)

                    ##### DELETES #####

                    ### Upload merged delete records ###
                    ti = env.run_task("updatefiles.upload_merged_delete_records")
                    self.assertEqual(State.SUCCESS, ti.state)

                    # Only one delete file for all updatefiles.
                    file = release.merged_delete_file_path

                    # Check that it exists in the cloud.

                    logging.info(f"Transform_file_path - {file}")
                    self.assert_blob_integrity(
                        env.transform_bucket,
                        gcs_blob_name_from_path(file),
                        file,
                    )

                    ###  BQ load delete table
                    ti = env.run_task("updatefiles.bq_load_delete_table")
                    self.assertEqual(State.SUCCESS, ti.state)

                    self.assert_table_integrity(delete_table_id, 2)

                    ###  BQ delete records ###
                    ti = env.run_task("updatefiles.bq_delete_records")
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
                    self.assertEqual(len(dataset_releases), 0)
                    # Run task
                    ti = env.run_task("add_dataset_releases")
                    self.assertEqual(State.SUCCESS, ti.state)
                    # Check after task run.
                    dataset_releases = get_dataset_releases(dag_id=self.dag_id, dataset_id=bq_dataset_id)
                    self.assertEqual(len(dataset_releases), 1)

                    ### cleanup ###
                    # Test that all workflow data was deleted
                    ti = env.run_task("cleanup_workflow")
                    self.assertEqual(State.SUCCESS, ti.state)
                    self.assert_cleanup(release.workflow_folder)

                    ### dag_run_complete ###
                    ti = env.run_task("dag_run_complete")
                    self.assertEqual(State.SUCCESS, ti.state)

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


class TestPubMedUtils(ObservatoryTestCase):
    def __init__(self, *args, **kwargs):
        super(TestPubMedUtils, self).__init__(*args, **kwargs)

        self.dag_id = "pubmed"
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")

        # FTP Server params
        self.ftp_server_url = "localhost"
        self.ftp_port = find_free_port()
        self.baseline_path = "/pubmed/baseline/"
        self.updatefiles_path = "/pubmed/updatefiles/"

    def test_download_datafiles(self):
        """Test that an exmaple PubMed XMLs can be transformed successfully."""

        # Create mock FTP server to host the test Pubmed Files.
        ftp_server = FtpServer(host=self.ftp_server_url, port=self.ftp_port, directory=FIXTURES_FOLDER)

        with ftp_server.create():
            # Setup environment
            env = ObservatoryEnvironment(self.project_id, self.data_location, api_port=find_free_port())

            with env.create(task_logging=True):
                changefile_release = ChangefileRelease(
                    dag_id="pubmed_telescope",
                    run_id="something",
                    start_date=pendulum.now(),
                    end_date=pendulum.now(),
                    sequence_start=1,
                    sequence_end=1,
                )

                datafiles_to_download = [
                    Datafile(
                        filename="pubmed22n0001.xml.gz",
                        file_index=1,
                        path_on_ftp=f"{self.baseline_path}pubmed22n0001.xml.gz",
                        baseline=True,
                        datafile_date=pendulum.now(),
                        datafile_release=changefile_release,
                    ),
                    Datafile(
                        filename="pubmed22n0003.xml.gz",
                        file_index=1,
                        path_on_ftp=f"{self.updatefiles_path}pubmed22n0003.xml.gz",
                        baseline=False,
                        datafile_date=pendulum.now(),
                        datafile_release=changefile_release,
                    ),
                ]

                success = download_datafiles(
                    datafile_list=datafiles_to_download,
                    ftp_server_url=self.ftp_server_url,
                    ftp_port=self.ftp_port,
                    reset_ftp_counter=1,
                    max_download_retry=1,
                )

                self.assertTrue(success)

                for datafile in datafiles_to_download:
                    self.assertTrue(os.path.exists(datafile.download_file_path))

    def test_load_datafile(self):
        """Test that a Pubmed datafile can be read in and parsed."""

        xml_file_path = os.path.join(FIXTURES_FOLDER, "pubmed", "baseline", "pubmed22n0001.xml.gz")
        data = load_datafile(input_path=xml_file_path)

        self.assertTrue(data)

    def test_save_pubmed_jsonl(self):
        """Test that data can be saved from to a json.gz or a .jsonl file correctly."""

        data_to_write = [{"value": 12345, "Version": 1}]

        with CliRunner().isolated_filesystem() as tmp_dir:
            ### Uncompressed ###
            output_path = os.path.join(tmp_dir, "test_output_file.jsonl")
            save_pubmed_jsonl(output_path=output_path, data=data_to_write)
            self.assertTrue(os.path.exists(output_path))

            with open(output_path, "r") as f_in:
                data_read_in = [json.loads(line) for line in f_in]

            self.assertEqual(data_to_write, data_read_in)

            ### Compressed ###
            output_path = os.path.join(tmp_dir, "test_output_file.jsonl.gz")
            save_pubmed_jsonl(output_path=output_path, data=data_to_write)
            self.assertTrue(os.path.exists(output_path))

            with gzip.open(output_path, "rb") as f_in:
                data_read_in = [json.loads(line) for line in f_in]

            self.assertEqual(data_to_write, data_read_in)

    def test_save_pubmed_merged_upserts(self):
        """Test if records can be reliably pulled from transformed files and written to file."""

        filename = "pubmed_temp.jsonl"
        upsert_index = {PMID(12345, 1): filename}

        record = [
            {
                "MedlineCitation": {
                    "PMID": {"value": 12345, "Version": 1},
                    "AuthorList": [{"FirstName": "Foo", "Lastname": "Bar"}, {"FirstName": "James", "Lastname": "Bond"}],
                    "AbstractText": "Something",
                }
            }
        ]

        with CliRunner().isolated_filesystem() as tmp_dir:
            input_path = os.path.join(tmp_dir, filename)
            save_pubmed_jsonl(input_path, record)

            upsert_output_path = os.path.join(tmp_dir, "upsert_output.jsonl.gz")

            result_path = save_pubmed_merged_upserts(filename, upsert_index, input_path, upsert_output_path)

            # Ensure that merged records have been written to disk.
            self.assertTrue(os.path.exists(result_path))

            with gzip.open(upsert_output_path, "rb") as f_in:
                data = [json.loads(line) for line in f_in]
            self.assertListEqual(record, data)

    def test_parse_articles(self):
        """Test if PubmedArticle records (upserts) can be pulled out from a data dictionary."""

        data_good = {
            "PubmedArticle": [{"value": 12345, "Version": 1}, {"value": 67891, "Version": 2}],
            "NotPubmedArticle": [{"value": 999, "Version": 999}],
            "DeleteCitation": {"PMID": [{"value": 1, "Version": 1}]},
        }

        data_bad = {
            "NotPubmedArticle": [{"value": 999, "Version": 999}],
            "NotDeleteCitation": {"PMID": [{"value": 1, "Version": 1}]},
        }

        self.assertEqual([{"value": 12345, "Version": 1}, {"value": 67891, "Version": 2}], parse_articles(data_good))
        self.assertEqual([], parse_articles(data_bad))

    def test_parse_deletes(self):
        """Test if DeleteCiation records (deletes) can be pulled out from a data dictionary."""

        data_good = {
            "PubmedArticle": [{"value": 12345, "Version": 1}, {"value": 67891, "Version": 2}],
            "NotPubmedArticle": [{"value": 999, "Version": 999}],
            "DeleteCitation": {"PMID": [{"value": 1, "Version": 1}]},
        }

        data_bad = {
            "NotPubmedArticle": [{"value": 999, "Version": 999}],
            "NotDeleteCitation": {"PMID": [{"value": 1, "Version": 1}]},
        }

        self.assertEqual([{"value": 1, "Version": 1}], parse_deletes(data_good))
        self.assertEqual([], parse_deletes(data_bad))

    def test_transform_pubmed(self):
        """Test that exmaple PubMed XMLs can be transformed successfully."""

        # Setup environment
        env = ObservatoryEnvironment(self.project_id, self.data_location, api_port=find_free_port())

        with env.create(task_logging=True):
            changefile_release = ChangefileRelease(
                dag_id="pubmed_telescope",
                run_id="something",
                start_date=pendulum.now(),
                end_date=pendulum.now(),
                sequence_start=1,
                sequence_end=1,
            )

            ### Bad XML ###
            datafile_bad = Datafile(
                filename="pubmed22n0001_bad_fields.xml.gz",
                file_index=1,
                path_on_ftp="dummy_string",
                baseline=True,
                datafile_date=pendulum.now(),
                datafile_release=changefile_release,
            )
            bad_xml_file_path = os.path.join(FIXTURES_FOLDER, "pubmed", "pubmed22n0001_bad_fields.xml.gz")
            shutil.copy2(bad_xml_file_path, datafile_bad.download_file_path)

            # Attempt to transform bad xml - just a baseline file, returns a baseline file if it is successful.
            result: bool = transform_pubmed(
                input_path=datafile_bad.download_file_path, upsert_path=datafile_bad.transform_upsert_file_path
            )
            self.assertFalse(os.path.exists(datafile_bad.transform_baseline_file_path))
            self.assertFalse(result)

            datafile_good = Datafile(
                filename="pubmed22n0001.xml.gz",
                file_index=1,
                path_on_ftp="dummy_string",
                baseline=True,
                datafile_date=pendulum.now(),
                datafile_release=changefile_release,
            )

            ### VALID BASELINE XML ###
            valid_xml_file_path = os.path.join(FIXTURES_FOLDER, "pubmed", "baseline", "pubmed22n0001.xml.gz")
            shutil.copy2(valid_xml_file_path, datafile_good.download_file_path)

            # Attempt to transform valid xml - should output a transformed file if it is successful.
            result: str = transform_pubmed(
                input_path=datafile_good.download_file_path, upsert_path=datafile_good.transform_baseline_file_path
            )
            self.assertTrue(os.path.exists(datafile_good.transform_baseline_file_path))
            self.assertEqual(os.path.basename(datafile_good.download_file_path), result)

            ### VALID UPDATEFILE XML ###

            expected_keys = {
                "deletes": [{"value": "2", "Version": "1"}],
                "upserts": [
                    {"value": "1", "Version": "1"},
                    {"value": "2", "Version": "2"},
                ],
            }

            datafile_good = Datafile(
                filename="pubmed22n0003.xml.gz",
                file_index=1,
                path_on_ftp="dummy_string",
                baseline=False,
                datafile_date=pendulum.now(),
                datafile_release=changefile_release,
            )

            valid_xml_file_path = os.path.join(FIXTURES_FOLDER, "pubmed", "updatefiles", "pubmed22n0003.xml.gz")
            shutil.copy2(valid_xml_file_path, datafile_good.download_file_path)

            result: PubmedUpdatefile = transform_pubmed(
                input_path=datafile_good.download_file_path, upsert_path=datafile_good.transform_upsert_file_path
            )
            self.assertTrue(os.path.exists(datafile_good.transform_upsert_file_path))
            self.assertEqual(os.path.basename(datafile_good.download_file_path), result.name)
            self.assertListEqual([upsert.to_dict() for upsert in result.upserts], expected_keys["upserts"])
            self.assertListEqual([delete.to_dict() for delete in result.deletes], expected_keys["deletes"])

    def test_merge_upserts_and_deletes(self):
        updatefiles = [
            PubmedUpdatefile(
                "pubmed23n0001",
                # Insert new records
                [PMID(10, 1), PMID(11, 1), PMID(12, 1), PMID(13, 1), PMID(14, 1), PMID(15, 1)],
                # Delete records from baseline
                [PMID(1, 1), PMID(2, 1)],
            ),
            PubmedUpdatefile(
                "pubmed23n0002",
                # Upsert over a previous record: PMID(10, 1)
                # Add a new version: PMID(12, 2), PMID(13, 2)
                # Add a new record: PMID(16, 1), PMID(17, 1)
                [PMID(10, 1), PMID(12, 2), PMID(13, 2), PMID(16, 1), PMID(17, 1)],
                # Delete a record from the previous day: PMID(11, 1)
                # Delete a record that was upserted on the same day: PMID(18, 1) n.b. this is removed from upserts
                [PMID(11, 1), PMID(18, 1)],
            ),
            PubmedUpdatefile(
                "pubmed23n0003",
                # Upsert over a previous record: PMID(10, 1), PMID(17, 1)
                # Add new records:
                [PMID(10, 1), PMID(17, 1), PMID(19, 1), PMID(20, 1), PMID(21, 1)],
                # Delete record added on previous day: PMID(13, 2)
                [PMID(13, 2)],
            ),
            PubmedUpdatefile(
                "pubmed23n0004",
                # Add a record deleted on a previous day: PMID(13, 2)
                # Add new versions:
                # Add new records:
                [PMID(13, 2), PMID(17, 1), PMID(19, 1), PMID(22, 1), PMID(23, 1)],
                # Delete records:
                [],
            ),
            PubmedUpdatefile(
                "pubmed23n0005",
                # Upsert new records:
                [PMID(24, 1), PMID(25, 1)],
                # Delete records that were previously upserted multiple times: PMID(17, 1)
                [PMID(17, 1)],
            ),
        ]
        expected_upserts = {
            PMID(12, 1): "pubmed23n0001",
            PMID(13, 1): "pubmed23n0001",
            PMID(14, 1): "pubmed23n0001",
            PMID(15, 1): "pubmed23n0001",
            PMID(12, 2): "pubmed23n0002",
            PMID(16, 1): "pubmed23n0002",
            PMID(10, 1): "pubmed23n0003",
            PMID(20, 1): "pubmed23n0003",
            PMID(21, 1): "pubmed23n0003",
            PMID(19, 1): "pubmed23n0004",
            PMID(13, 2): "pubmed23n0004",
            PMID(22, 1): "pubmed23n0004",
            PMID(23, 1): "pubmed23n0004",
            PMID(24, 1): "pubmed23n0005",
            PMID(25, 1): "pubmed23n0005",
        }
        expected_deletes = {
            PMID(17, 1),
            PMID(1, 1),
            PMID(2, 1),
            PMID(11, 1),
            PMID(18, 1),
        }

        # Check if we receive expected output
        actual_upserts, actual_deletes = merge_upserts_and_deletes(updatefiles)
        self.assertDictEqual(expected_upserts, actual_upserts)
        self.assertSetEqual(expected_deletes, actual_deletes)

    def test_add_attributes(self):
        """
        Test that attributes from the Biopython data classes can be reliably pulled out and added to the dictionary.
        """

        biopython_str = StringElement("string", tag="data", attributes={"type": "str"}, key="data")
        biopython_list = ListElement("something", attributes={"type": "list"}, allowed_tags=None, key=None)
        biopython_dict = DictionaryElement({"data": ""}, attrs={"type": "dict"}, allowed_tags=None, key=None)
        biopython_dict.store(biopython_list)
        biopython_list.store(biopython_str)

        objects = [biopython_str, biopython_dict, biopython_list]

        expected = [
            {"value": "string", "type": "str"},
            {"type": "dict", "something": {"data": [{"value": "string", "type": "str"}], "type": "list"}},
            {"data": [{"value": "string", "type": "str"}], "type": "list"},
        ]

        with CliRunner().isolated_filesystem() as tmp_dir:
            # Write test files using custom encoder
            output_file = os.path.join(tmp_dir, "test_output_file.jsonl")
            with open(output_file, "wb") as f_out:
                for line in objects:
                    output = add_attributes(line)
                    f_out.write(str.encode(json.dumps(output, cls=PubMedCustomEncoder) + "\n"))

            with open(output_file, "r") as f_in:
                objects_in = [json.loads(line) for line in f_in]

        self.assertEqual(objects_in, expected)

    def test_change_pubmed_list_structure(self):
        """Test that the data in *list fields can be moved up one level to the parent field."""

        input = [
            {
                "DataBankList": {
                    "CompleteYN": "Y",
                    "DataBank": [
                        {"DataBankName": "TestDB1", "AccessionNumberList": {"AccessionNumber": ["12345", "678910"]}},
                        {"DataBankName": "TestDB2", "AccessionNumberList": {"AccessionNumber": "12345"}},
                    ],
                },
                "NotDataBankList": {"Background": "data", "OTHER": "data"},
                "Fake_upper_level": {
                    "MeshHeadingList": {
                        "MeshHeading": [
                            {
                                "QualifierName": [],
                                "DescriptorName": {"value": "Animals", "UI": "D000818", "MajorTopicYN": "N"},
                            },
                            {
                                "QualifierName": [{"value": "drug effects", "UI": "Q000187", "MajorTopicYN": "N"}],
                                "DescriptorName": {
                                    "value": "Cell Differentiation",
                                    "UI": "D002454",
                                    "MajorTopicYN": "N",
                                },
                            },
                        ]
                    },
                },
            },
        ]

        expected = [
            {
                "DataBankListCompleteYN": "Y",
                "DataBankList": [
                    {"DataBankName": "TestDB1", "AccessionNumberList": ["12345", "678910"]},
                    {"DataBankName": "TestDB2", "AccessionNumberList": "12345"},
                ],
                "NotDataBankList": {"Background": "data", "OTHER": "data"},
                "Fake_upper_level": {
                    "MeshHeadingList": [
                        {
                            "QualifierName": [],
                            "DescriptorName": {"value": "Animals", "UI": "D000818", "MajorTopicYN": "N"},
                        },
                        {
                            "QualifierName": [{"value": "drug effects", "UI": "Q000187", "MajorTopicYN": "N"}],
                            "DescriptorName": {"value": "Cell Differentiation", "UI": "D002454", "MajorTopicYN": "N"},
                        },
                    ],
                },
            }
        ]

        result = change_pubmed_list_structure(input)

        self.assertEqual(result, expected)

    def test_PubMedCustomEncoder(self):
        """Test that files are written out as expected using the CustomEncoder for PubMed-like files."""

        input_dict = [
            {
                "AbstractText": ["String", {"Background": "data"}, {"OTHER": "data"}],
                "NotAbstractText": {"col1": "row1", "col2": "row2"},
            },
            {
                "AbstractText": {"Background": "data", "OTHER": "data"},
                "NotAbstractText": {"col1": "row1", "col2": "row2"},
            },
        ]

        output_dict = [
            {
                "AbstractText": "['String', {'Background': 'data'}, {'OTHER': 'data'}]",
                "NotAbstractText": {"col1": "row1", "col2": "row2"},
            },
            {
                "AbstractText": "{'Background': 'data', 'OTHER': 'data'}",
                "NotAbstractText": {"col1": "row1", "col2": "row2"},
            },
        ]

        test_file = "test_output_file.jsonl.gz"

        with CliRunner().isolated_filesystem() as tmp_dir:
            test_file_path = os.path.join(tmp_dir, test_file)

            # Write out test data with specific key listed in the encoder.
            with gzip.open(test_file_path, "w") as f_out:
                for line in input_dict:
                    f_out.write(str.encode(json.dumps(line, cls=PubMedCustomEncoder) + "\n"))

            # Read data back in and expect it to the the correct form.
            with gzip.open(test_file_path, "rb") as f_in:
                data = [json.loads(line) for line in f_in]

            self.assertEqual(data, output_dict)
