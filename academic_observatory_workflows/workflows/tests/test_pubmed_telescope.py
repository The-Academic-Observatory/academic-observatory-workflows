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
import json
import gzip
import hashlib
import logging
import pendulum
import datetime
from ftplib import FTP
from click.testing import CliRunner
from airflow.utils.state import State

from Bio.Entrez.Parser import (
    StringElement,
    ListElement,
    DictionaryElement,
)

from observatory.platform.api import get_dataset_releases
from observatory.platform.observatory_config import Workflow
from observatory.platform.gcs import gcs_blob_name_from_path, gcs_download_blob
from observatory.platform.observatory_environment import ObservatoryEnvironment, ObservatoryTestCase
from observatory.platform.bigquery import bq_sharded_table_id, bq_create_table_from_query, bq_export_table
from observatory.platform.observatory_environment import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    find_free_port,
    FtpServer,
)
from academic_observatory_workflows.config import test_fixtures_folder
from academic_observatory_workflows.workflows.pubmed_telescope import (
    Changefile,
    PubMedCustomEncoder,
    PubMedRelease,
    PubMedTelescope,
    add_attributes_to_data_from_biopython_classes,
)


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
        self.ftp_file_updload_date: pendulum.DateTime = pendulum.datetime(
            year=2022,
            month=12,
            day=4,
        )

        super(TestPubMedTelescope, self).__init__(*args, **kwargs)

        # Changefile 23n0005 should be outside the window for the second run.
        self.ftp_hosted_files = {
            "pubmed/baseline/pubmed23n0001.xml.gz": self.ftp_file_updload_date,
            "pubmed/baseline/pubmed23n0002.xml.gz": self.ftp_file_updload_date,
            "pubmed/updatefiles/pubmed23n0003.xml.gz": pendulum.datetime(year=2022, month=12, day=9),
            "pubmed/updatefiles/pubmed23n0004.xml.gz": pendulum.datetime(year=2022, month=12, day=10),
            "pubmed/updatefiles/pubmed23n0005.xml.gz": pendulum.datetime(year=2022, month=12, day=20),
        }

        # Expected values for how the Pubmed Telescope should run.
        self.first_run = {
            "start_date": pendulum.datetime(year=2022, month=12, day=1),
            "data_interval_start": pendulum.datetime(year=2022, month=12, day=1),
            "is_first_run": True,
            "changefiles": [
                Changefile(
                    filename="pubmed23n0001.xml.gz",
                    file_index=1,
                    path_on_ftp=f"{self.baseline_path}pubmed23n0001.xml.gz",
                    is_first_run=True,
                    changefile_date=self.ftp_file_updload_date,
                ),
                Changefile(
                    filename="pubmed23n0002.xml.gz",
                    file_index=2,
                    path_on_ftp=f"{self.baseline_path}pubmed23n0002.xml.gz",
                    is_first_run=True,
                    changefile_date=self.ftp_file_updload_date,
                ),
            ],
            "md5hash_download": {
                "pubmed23n0001.xml.gz": "73624a987b3572221fdd53ebefa1043f",
                "pubmed23n0002.xml.gz": "24da7ffc1afb277044ee1ba8cddb4e74",
            },
            "merged_file_output": {
                "additions": "merged_additions_1-2_part_1.jsonl.gz",
                "deletions": "merged_deletions_1-2_part_1.jsonl.gz",
            },
            "PMID_list": [
                {"f0_": {"_field_1": "1", "_field_2": "1"}},
                {"f0_": {"_field_1": "1", "_field_2": "2"}},
                {"f0_": {"_field_1": "1", "_field_2": "30970"}},
                {"f0_": {"_field_1": "1", "_field_2": "30971"}},
            ],
        }
        self.second_run = {
            "start_date": pendulum.datetime(year=2022, month=12, day=8) + datetime.timedelta(days=7),
            "data_interval_start": pendulum.datetime(year=2022, month=12, day=8),
            "is_first_run": False,
            "changefiles": [
                Changefile(
                    filename="pubmed23n0003.xml.gz",
                    file_index=3,
                    path_on_ftp=f"{self.updatefiles_path}pubmed23n0003.xml.gz",
                    is_first_run=False,
                    changefile_date=pendulum.datetime(year=2022, month=12, day=9),
                ),
                Changefile(
                    filename="pubmed23n0004.xml.gz",
                    file_index=4,
                    path_on_ftp=f"{self.updatefiles_path}pubmed23n0004.xml.gz",
                    is_first_run=False,
                    changefile_date=pendulum.datetime(year=2022, month=12, day=10),
                ),
            ],
            "md5hash_download": {
                "pubmed23n0003.xml.gz": "d6da2c87390489d22cdeb6e046b77da1",
                "pubmed23n0004.xml.gz": "83764fc19cd98d247dc5603ca65569e6",
            },
            "merged_file_output": {
                "additions": "merged_additions_3-4.jsonl.gz",
                "deletions": "merged_deletions_3-4.jsonl.gz",
            },
            "update_tables": {
                "additions": 4,
                "deletions": 2,
            },
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

        dag = PubMedTelescope(
            dag_id=self.dag_id,
            cloud_workspace=self.fake_cloud_workspace,
        ).make_dag()

        self.assert_dag_structure(
            {
                "wait_for_prev_dag_run": ["check_dependencies"],
                "check_dependencies": ["list_changefiles_for_release"],
                "list_changefiles_for_release": ["download"],
                "download": ["upload_downloaded"],
                "upload_downloaded": ["transform"],
                "transform": ["merge_updatefiles"],
                "merge_updatefiles": ["upload_transformed"],
                "upload_transformed": ["bq_ingest_update_tables"],
                "bq_ingest_update_tables": ["bq_add_updates_to_main_table"],
                "bq_add_updates_to_main_table": ["add_new_dataset_release"],
                "add_new_dataset_release": ["cleanup"],
                "cleanup": ["dag_run_complete"],
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
                    class_name="academic_observatory_workflows.workflows.pubmed_telescope.PubMedTelescope",
                    cloud_workspace=self.fake_cloud_workspace,
                )
            ]
        )

        with env.create():
            self.assert_dag_load_from_config(self.dag_id)

    def test_telescope(self):
        """Test the PubMed Telescope end to end"""

        env = ObservatoryEnvironment(self.project_id, self.data_location, api_port=find_free_port())
        bq_dataset_id = env.add_dataset()

        # Create mock FTP server that holds the testing Pubmed Files.
        ftp_server = FtpServer(
            host=self.ftp_server_url, port=self.ftp_port, directory=os.path.join(test_fixtures_folder())
        )

        with ftp_server.create():
            # Before the tests start, we need to manually change the modified dates of the changefiles
            # on the locally hosted FTP server so that the workflow can grab the correct updatefiles.

            # Change the date modified on the FTP server.
            # Login as root and change the modified time for the updatedate files
            ftp_conn = FTP()
            ftp_conn.connect(host=self.ftp_server_url, port=self.ftp_port)
            ftp_conn.login(user="root", passwd="pass")
            for file_path, upload_date in self.ftp_hosted_files.items():
                ftp_command = f"MFMT {upload_date.format('YYYYMMDDHHmmss')} {file_path}"
                print("ftp send command ", ftp_command)
                ftp_conn.sendcmd(ftp_command)
            ftp_conn.close()

            with env.create(task_logging=True):
                # Initialise the telescope workflow.
                workflow = PubMedTelescope(
                    dag_id=self.dag_id,
                    cloud_workspace=env.cloud_workspace,
                    bq_dataset_id=bq_dataset_id,
                    ftp_server_url=self.ftp_server_url,
                    ftp_port=self.ftp_port,
                    start_date=self.ftp_file_updload_date,
                )
                dag = workflow.make_dag()

                #####################
                ##### FIRST RUN #####

                run = self.first_run
                with env.create_dag_run(dag, run["data_interval_start"]) as dag_run:
                    logging.info(f"Start date this workflow run {run['data_interval_start']}")

                    ### Wait for the previous DAG run to finish ###
                    ti = env.run_task("wait_for_prev_dag_run")
                    self.assertEqual(State.SUCCESS, ti.state)

                    ### Check Dependancies ###
                    ti = env.run_task("check_dependencies")
                    self.assertEqual(State.SUCCESS, ti.state)

                    ### List change files for release ###

                    # Fetch changefiles
                    task_id = workflow.list_changefiles_for_release.__name__
                    ti = env.run_task(task_id)
                    self.assertEqual(State.SUCCESS, ti.state)

                    # Pull list of changefiles for this run from the Xcom
                    files_to_download = ti.xcom_pull(
                        key="files_to_download",
                        task_ids=task_id,
                        include_prior_dates=False,
                    )

                    # Loop through the changefiles and check to see if they're the same
                    changefiles_to_download = [Changefile.from_dict(changefile) for changefile in files_to_download]
                    self.assertEqual(len(changefiles_to_download), len(run["changefiles"]))
                    for i in range(len(run["changefiles"])):
                        try:
                            self.assertTrue(changefiles_to_download[i].__eq__(run["changefiles"][i]))
                        except:
                            print("From test", changefiles_to_download[i].to_dict())
                            print("Expected changefile", run["changefiles"][i].to_dict())
                            raise NameError("sdfsdfgdfg")

                    # Create the release
                    release = PubMedRelease(
                        dag_id=self.dag_id,
                        run_id=dag_run.run_id,
                        cloud_workspace=workflow.cloud_workspace,
                        bq_dataset_id=workflow.bq_dataset_id,
                        start_date=dag_run.data_interval_start,
                        end_date=pendulum.instance(dag_run.data_interval_end),  # bug with the observatory instance.
                        is_first_run=run["is_first_run"],
                        changefile_list=run["changefiles"],
                    )

                    ### Download ###
                    task_id = workflow.download.__name__
                    ti = env.run_task(task_id)
                    self.assertEqual(State.SUCCESS, ti.state)

                    # Loop through downloaded files, check that they exist and that hashes match.
                    for changefile in release.changefile_list:
                        self.assertTrue(os.path.exists(changefile.download_file_path))
                        with open(changefile.download_file_path, "rb") as f_hash:
                            data = f_hash.read()
                            md5hash = hashlib.md5(data).hexdigest()
                            logging.info(f"md5hash for {changefile.filename} - {md5hash}")
                            self.assertEqual(md5hash, run["md5hash_download"][changefile.filename])

                    ### Upload downloaded ###
                    task_id = workflow.upload_downloaded.__name__
                    ti = env.run_task(task_id)
                    self.assertEqual(State.SUCCESS, ti.state)

                    for changefile in release.changefile_list:
                        self.assert_blob_integrity(
                            env.download_bucket,
                            gcs_blob_name_from_path(changefile.download_file_path),
                            changefile.download_file_path,
                        )

                    ### Transform ###
                    task_id = workflow.transform.__name__
                    ti = env.run_task(task_id)
                    self.assertEqual(State.SUCCESS, ti.state)

                    for changefile in release.changefile_list:
                        for entity in workflow.entity_list:
                            transform_file = changefile.transform_file_path(entity.type)
                            print(f"Transform file - {transform_file}")
                            self.assertTrue(os.path.exists(transform_file))

                    ### Merge transformed ###
                    task_id = workflow.merge_updatefiles.__name__
                    ti = env.run_task(task_id)
                    self.assertEqual(State.SUCCESS, ti.state)

                    # Check that the file exists for this run.
                    for entity in workflow.entity_list:
                        merged_file = os.path.join(release.transform_folder, run["merged_file_output"][entity.type])
                        print("merged file - ", merged_file)
                        self.assertTrue(os.path.exists(merged_file))

                    ### Upload transformed ###
                    task_id = workflow.upload_transformed.__name__
                    ti = env.run_task(task_id)
                    self.assertEqual(State.SUCCESS, ti.state)

                    changefile = release.changefile_list[0]
                    for entity in workflow.entity_list:
                        merged_transform_file_path = changefile.merged_transform_file_path(
                            entity.type,
                            release.changefile_list[0].file_index,
                            release.changefile_list[-1].file_index,
                            part_num=1,
                        )

                        logging.info(f"Merged_transform_file_path - {merged_transform_file_path}")
                        self.assert_blob_integrity(
                            env.transform_bucket,
                            gcs_blob_name_from_path(merged_transform_file_path),
                            merged_transform_file_path,
                        )

                    ### bq_ingest_update_tables ###
                    task_id = workflow.bq_ingest_update_tables.__name__
                    ti = env.run_task(task_id)
                    self.assertEqual(State.SUCCESS, ti.state)

                    for entity in workflow.entity_list:
                        table_id = (
                            f"{workflow.cloud_workspace.project_id}.{workflow.bq_dataset_id}.{workflow.main_table}"
                        )
                        self.assert_table_integrity(table_id, 4)

                    ### bq_add_updates_to_main_table
                    task_id = workflow.bq_add_updates_to_main_table.__name__
                    ti = env.run_task(task_id)
                    self.assertEqual(State.SUCCESS, ti.state)

                    # On first run of the workflow, the baseline main_table should be untouched.
                    main_table_id = f"{env.project_id}.{workflow.bq_dataset_id}.{workflow.main_table}"
                    self.assert_table_integrity(main_table_id, 4)

                    # Run query to get list of PMIDs that are present in the table and compare against what it should be.
                    PMID_list = f"{env.project_id}.{workflow.bq_dataset_id}.{workflow.main_table}_PMID_list_first_run"
                    bq_query_list_PMIDs = f"""
                    SELECT (MedlineCitation.PMID.Version, MedlineCitation.PMID.value) 
                    FROM `{main_table_id}` 
                    ORDER BY MedlineCitation.PMID.value
                    """
                    destination_uri = f"gs://{env.transform_bucket}/PMID_list_first_run.jsonl"
                    PMID_list_path = os.path.join(release.transform_folder, "PMID_list_first_run.jsonl")
                    bq_create_table_from_query(sql=bq_query_list_PMIDs, table_id=PMID_list)
                    bq_export_table(table_id=PMID_list, file_type="jsonl", destination_uri=destination_uri)
                    gcs_download_blob(
                        bucket_name=env.transform_bucket,
                        blob_name="PMID_list_first_run.jsonl",
                        file_path=PMID_list_path,
                    )
                    logging.info(f"Downloaded table to: {PMID_list_path}")
                    with open(PMID_list_path, "rb") as f_in:
                        PMID_list = [json.loads(line) for line in f_in]

                    self.assertEqual(PMID_list, run["PMID_list"])

                    ### add_new_dataset_release ###
                    task_id = workflow.add_new_dataset_release.__name__
                    # Assert that the dataset has been added to the observatory-api
                    # Get dataset releases before task run
                    dataset_releases = get_dataset_releases(dag_id=self.dag_id, dataset_id=workflow.bq_dataset_id)
                    self.assertEqual(len(dataset_releases), 0)
                    # Run task
                    ti = env.run_task(task_id)
                    self.assertEqual(State.SUCCESS, ti.state)
                    # Check after task run.
                    dataset_releases = get_dataset_releases(dag_id=self.dag_id, dataset_id=workflow.bq_dataset_id)
                    self.assertEqual(len(dataset_releases), 1)

                    ### cleanup ###
                    # Test that all workflow data deleted
                    ti = env.run_task(workflow.cleanup.__name__)
                    self.assertEqual(State.SUCCESS, ti.state)
                    self.assert_cleanup(release.workflow_folder)

                    ### dag_run_complete ###
                    ti = env.run_task("dag_run_complete")
                    self.assertEqual(State.SUCCESS, ti.state)

                ######################
                ##### SECOND RUN #####

                run = self.second_run
                with env.create_dag_run(dag, run["data_interval_start"]) as dag_run:
                    logging.info(f"Start date of the workflow run - {run['data_interval_start']}")

                    ### Wait for the previous DAG run to finish ###
                    ti = env.run_task("wait_for_prev_dag_run")
                    self.assertEqual(State.SUCCESS, ti.state)

                    ### Check Dependancies ###
                    ti = env.run_task("check_dependencies")
                    self.assertEqual(State.SUCCESS, ti.state)

                    ### List change files for release ###
                    task_id = workflow.list_changefiles_for_release.__name__
                    ti = env.run_task(task_id)
                    self.assertEqual(State.SUCCESS, ti.state)

                    # Pull list of changefiles for this run from the Xcom
                    files_to_download = ti.xcom_pull(
                        key="files_to_download",
                        task_ids=task_id,
                        include_prior_dates=False,
                    )

                    # Loop through the changefiles and check to see if they're the same
                    changefiles_to_download = [Changefile.from_dict(changefile) for changefile in files_to_download]
                    self.assertEqual(len(changefiles_to_download), len(run["changefiles"]))
                    for i in range(len(run["changefiles"])):
                        try:
                            self.assertTrue(changefiles_to_download[i].__eq__(run["changefiles"][i]))
                        except:
                            logging.info(f"From test - {changefiles_to_download[i].to_dict()}")
                            logging.info(f"Expected changefile {run['changefiles'][i].to_dict()}")
                            raise NameError("sdfsdfgdfg")

                    # Create the release
                    release = PubMedRelease(
                        dag_id=self.dag_id,
                        run_id=dag_run.run_id,
                        cloud_workspace=workflow.cloud_workspace,
                        bq_dataset_id=workflow.bq_dataset_id,
                        start_date=dag_run.data_interval_start,
                        end_date=pendulum.instance(dag_run.data_interval_end),
                        is_first_run=run["is_first_run"],
                        changefile_list=run["changefiles"],
                    )

                    ### Download ###
                    task_id = workflow.download.__name__
                    ti = env.run_task(task_id)
                    self.assertEqual(State.SUCCESS, ti.state)

                    # Loop through downloaded files, check that they exist and that hashes match.
                    for changefile in release.changefile_list:
                        self.assertTrue(os.path.exists(changefile.download_file_path))
                        with open(changefile.download_file_path, "rb") as f_hash:
                            data = f_hash.read()
                            md5hash = hashlib.md5(data).hexdigest()
                            logging.info(f"md5hash for {changefile.filename} - {md5hash}")
                            self.assertEqual(md5hash, run["md5hash_download"][changefile.filename])

                    ### Upload downloaded ###
                    task_id = workflow.upload_downloaded.__name__
                    ti = env.run_task(task_id)
                    self.assertEqual(State.SUCCESS, ti.state)

                    for changefile in release.changefile_list:
                        self.assert_blob_integrity(
                            env.download_bucket,
                            gcs_blob_name_from_path(changefile.download_file_path),
                            changefile.download_file_path,
                        )

                    ### Transform ###
                    task_id = workflow.transform.__name__
                    ti = env.run_task(task_id)
                    self.assertEqual(State.SUCCESS, ti.state)

                    for changefile in release.changefile_list:
                        for entity in workflow.entity_list:
                            transform_file = changefile.transform_file_path(entity.type)
                            print(f"Transform file - {transform_file}")
                            self.assertTrue(os.path.exists(transform_file))

                    ### Merge transformed ###
                    task_id = workflow.merge_updatefiles.__name__
                    ti = env.run_task(task_id)
                    self.assertEqual(State.SUCCESS, ti.state)

                    # Check that the file exists for this run.
                    for entity in workflow.entity_list:
                        merged_file = os.path.join(release.transform_folder, run["merged_file_output"][entity.type])
                        print("merged file - ", merged_file)
                        self.assertTrue(os.path.exists(merged_file))

                    ### Upload transformed ###
                    task_id = workflow.upload_transformed.__name__
                    ti = env.run_task(task_id)
                    self.assertEqual(State.SUCCESS, ti.state)

                    for changefile in release.changefile_list:
                        for entity in workflow.entity_list:
                            merged_transform_file_path = changefile.merged_transform_file_path(
                                entity.type,
                                release.changefile_list[0].file_index,
                                release.changefile_list[-1].file_index,
                                part_num=None,
                            )

                            logging.info(f"Merged_transform_file_path - {merged_transform_file_path}")
                            self.assert_blob_integrity(
                                env.transform_bucket,
                                gcs_blob_name_from_path(merged_transform_file_path),
                                merged_transform_file_path,
                            )

                    ### bq_ingest_update_tables ###
                    task_id = workflow.bq_ingest_update_tables.__name__
                    ti = env.run_task(task_id)
                    self.assertEqual(State.SUCCESS, ti.state)

                    for entity in workflow.entity_list:
                        table_id = bq_sharded_table_id(
                            workflow.cloud_workspace.project_id,
                            workflow.bq_dataset_id,
                            entity.name,
                            date=release.end_date,
                        )
                        logging.info(f"Table id of update tables - {table_id}")
                        self.assert_table_integrity(table_id, run["update_tables"][entity.type])

                    ### bq_add_updates_to_main_table
                    task_id = workflow.bq_add_updates_to_main_table.__name__
                    ti = env.run_task(task_id)
                    self.assertEqual(State.SUCCESS, ti.state)

                    # Check that the snapshot backup has been created.
                    snapshot_table_id = bq_sharded_table_id(
                        workflow.cloud_workspace.project_id,
                        workflow.bq_dataset_id,
                        f"{workflow.main_table}_backup",
                        date=release.end_date,
                    )
                    self.assert_table_integrity(snapshot_table_id, 4)

                    # Check that additions, updates and deletions have been applied successfully.
                    main_table_id = (
                        f"{workflow.cloud_workspace.project_id}.{workflow.bq_dataset_id}.{workflow.main_table}"
                    )
                    self.assert_table_integrity(main_table_id, 5)

                    # Run query to get list of PMIDs that are present in the table and compare against what it should be.
                    PMID_list = f"{env.project_id}.{workflow.bq_dataset_id}.{workflow.main_table}_PMID_list_second_run"
                    bq_query_list_PMIDs = f"""
                    SELECT (MedlineCitation.PMID.Version, MedlineCitation.PMID.value) 
                    FROM `{main_table_id}` 
                    ORDER BY MedlineCitation.PMID.value
                    """
                    destination_uri = f"gs://{env.transform_bucket}/PMID_list_second_run.jsonl"
                    PMID_list_path = os.path.join(release.transform_folder, "PMID_list_second_run.jsonl")
                    bq_create_table_from_query(sql=bq_query_list_PMIDs, table_id=PMID_list)
                    bq_export_table(table_id=PMID_list, file_type="jsonl", destination_uri=destination_uri)
                    gcs_download_blob(
                        bucket_name=env.transform_bucket,
                        blob_name="PMID_list_second_run.jsonl",
                        file_path=PMID_list_path,
                    )
                    logging.info(f"Downloaded table to: {PMID_list_path}")
                    with open(PMID_list_path, "rb") as f_in:
                        PMID_list = [json.loads(line) for line in f_in]

                    self.assertEqual(PMID_list, run["PMID_list"])

                    ### add_new_dataset_release ###
                    task_id = workflow.add_new_dataset_release.__name__

                    # Ensure that the dataset release has been added to the observatory-api
                    # Get dataset releases before task run
                    dataset_releases = get_dataset_releases(dag_id=self.dag_id, dataset_id=workflow.bq_dataset_id)
                    self.assertEqual(len(dataset_releases), 1)
                    # Run task
                    ti = env.run_task(task_id)
                    self.assertEqual(State.SUCCESS, ti.state)
                    # Check after task run.
                    dataset_releases = get_dataset_releases(dag_id=self.dag_id, dataset_id=workflow.bq_dataset_id)
                    self.assertEqual(len(dataset_releases), 2)

                    ### cleanup ###
                    # Test that all workflow data deleted
                    ti = env.run_task(workflow.cleanup.__name__)
                    self.assertEqual(State.SUCCESS, ti.state)
                    self.assert_cleanup(release.workflow_folder)

                    ### dag_run_complete ###
                    ti = env.run_task("dag_run_complete")
                    self.assertEqual(State.SUCCESS, ti.state)


class TestPubMedUtils(ObservatoryTestCase):
    def __init__(self, *args, **kwargs):
        super(TestPubMedUtils, self).__init__(*args, **kwargs)

    def test_add_attributes_to_data_from_biopython(self):
        """
        Test that attributes from the Biopython data classes can be reliably pulled out
        from the data and added to the dictionary.
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
            print(output_file)
            with open(output_file, "wb") as f_out:
                for line in objects:
                    output = add_attributes_to_data_from_biopython_classes(line)
                    f_out.write(str.encode(json.dumps(output, cls=PubMedCustomEncoder) + "\n"))

            with open(output_file, "r") as f_in:
                objects_in = [json.loads(line) for line in f_in]

        self.assertEqual(objects_in, expected)

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
