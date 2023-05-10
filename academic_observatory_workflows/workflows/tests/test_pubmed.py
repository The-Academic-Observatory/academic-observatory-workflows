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

import datetime
import gzip
import io
import json
import os
from subprocess import Popen
from types import SimpleNamespace
from unittest.mock import Mock, call, patch

import pendulum
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models import Connection
from airflow.utils.state import State
from botocore.response import StreamingBody
from click.testing import CliRunner
from dateutil import tz

from academic_observatory_workflows.api_type_ids import DatasetTypeId
from academic_observatory_workflows.config import test_fixtures_folder
from academic_observatory_workflows.workflows.pubmed_telescope import (
    PubMedRelease,
    PubMedTelescope,
    run_subprocess_cmd,
    pull_data_from_dict,
    transform_pubmed_xml_file_to_jsonl,
    add_attributes_to_data_from_biopython_classes,
    CustomEncoder,
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
from observatory.platform.utils.gc_utils import (
    upload_file_to_cloud_storage,
)
from observatory.platform.utils.release_utils import get_dataset_releases
from observatory.platform.utils.test_utils import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    SftpServer,
    module_file_path,
    find_free_port,
)

# Biopython Parser and data structures.
from Bio import Entrez
from Bio.Entrez.Parser import (
    StringElement,
    ListElement,
    DictionaryElement,
    OrderedListElement,
)


class TestPubMedTelescope(ObservatoryTestCase):
    """Tests for the OpenAlex telescope"""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.
        :param args: arguments.
        :param kwargs: keyword arguments.
        """


        

        super(TestPubMedTelescope, self).__init__(*args, **kwargs)
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")

        self.ftp_port = find_free_port()

        # self.pubmed_transform_list = [
        #     {
        #         "name": "pubmed_article",
        #         "data_type": "additions",
        #         "data": [],
        #         "sub_key": "PubmedArticle",
        #         "set_key": "PubmedArticleSet",
        #         "pmid_key_loc": "MedlineCitation",
        #         "output_file_base": f"pubmed_article_additions_{self.release_id}",
        #         "transform_files": [],
        #         "merged_transform_files": [],
        #     },
        #     {
        #         "name": "pubmed_book_article",
        #         "data_type": "additions",
        #         "data": [],
        #         "sub_key": "PubmedBookArticle",
        #         "set_key": "PubmedBookArticleSet",
        #         "pmid_key_loc": "MedlineCitation",
        #         "output_file_base": f"pubmed_book_article_additions_{self.release_id}",
        #         "transform_files": [],
        #         "merged_transform_files": [],
        #     },
        #     {
        #         "name": "book_document",
        #         "data_type": "additions",
        #         "data": [],
        #         "sub_key": "BookDocument",
        #         "set_key": "BookDocumentSet",
        #         "pmid_key_loc": "BookDocument",
        #         "output_file_base": f"book_document_additions_{self.release_id}",
        #         "transform_files": [],
        #         "merged_transform_files": [],
        #     },
        #     {
        #         "name": "pubmed_article",
        #         "data_type": "deletions",
        #         "data": [],
        #         "sub_key": "DeleteCitation",
        #         "set_key": None,
        #         "output_file_base": f"pubmed_article_deletions_{self.release_id}",
        #         "transform_files": [],
        #         "merged_transform_files": [],
        #     },
        #     {
        #         "name": "book_document",
        #         "data_type": "deletions",
        #         "data": [],
        #         "sub_key": "DeleteDocument",
        #         "set_key": None,
        #         "output_file_base": f"book_document_deletions_{self.release_id}",
        #         "transform_files": [],
        #         "merged_transform_files": [],
        #     },
        # ]

        # First ever run of the telescope for 2022 data. 
        self.run_list = [
            {
                "start": pendulum.datetime(year=2022, month=12, day=4), # Absolute start date of the telescope.
                "data_interval_start": pendulum.datetime(year=2022, month=12, day=4),
                "data_interval_end": pendulum.datetime(year=2022, month=12, day=11),
                "baseline_initial_file_date": pendulum.datetime(year=2022, month=12, day=8),
                "updatefiles_initial_file_date": pendulum.datetime(year=2022, month=12, day=9)
                "is_first_release": True,
            }, 
            {
                "start": pendulum.datetime(year=2022, month=12, day=4), # Absolute start date of the telescope.
                "data_interval_start": pendulum.datetime(year=2022, month=12, day=11),
                "data_interval_end": pendulum.datetime(year=2022, month=12, day=18),
                "baseline_initial_file_date": pendulum.datetime(year=2022, month=12, day=8),
                "updatefiles_initial_file_date": pendulum.datetime(year=2022, month=12, day=9),
                "is_first_release": False,
                "release_table_hash": "adfalsdfjhalsdjfalsdf"
            }
        ] 
    
        # API environment
        self.host = "localhost"
        configuration = Configuration(host=f"http://{self.host}:{self.port}")
        api_client = ApiClient(configuration)
        self.api = ObservatoryApi(api_client=api_client)  # noqa: E501
        self.env = ObservatoryApiEnvironment(host=self.host, port=self.port)
        self.org_name = "Curtin University"

    def test_dag_structure(self):
        """Test that the PubMed DAG has the correct structure.
        :return: None
        """

        dag = PubMedTelescope(workflow_id=0).make_dag()
        self.assert_dag_structure(
            {
                "check_dependencies": ["check_releases"],
                "check_releases": ["download"],
                "download": ["upload_downloaded"],
                "upload_downloaded": ["transform"],
                "transform": ["merged_transformed"],
                "merged_transformed": ["upload_transformed"],
                "upload_transformed": ["bq_append_new"],
                "bq_append_new": ["bq_delete_old"],
                "bq_delete_old": ["bq_create_snapshot"],
                "bq_create_snapshot": ["add_new_dataset_releases"],
                "add_new_dataset_releases": ["cleanup"],
                "cleanup": ["cleanup"]
            },
            dag,
        )

    @patch("observatory.platform.utils.release_utils.make_observatory_api")
    def test_dag_load(self, m_makeapi):
        """Test that the OpenAlex DAG can be loaded from a DAG bag.
        :return: None
        """

        m_makeapi.return_value = self.api
        env = ObservatoryEnvironment(self.project_id, self.data_location, api_host=self.host, api_port=self.port)

        with env.create():
            self.setup_connections(env)
            self.setup_api()
            dag_file = os.path.join(module_file_path("academic_observatory_workflows.dags"), "pubmed_telescope.py")
            self.assert_dag_load("pubmed", dag_file)

    # Patch whatever is needed for this test
    @patch('ftplib.FTP', autospec=True)
    def test_telescope(self, m_makeapi, mock_ftp_constructor):
        """Test the PubMed Telescope end to end.
        :return: None.
        """
        m_makeapi.return_value = self.api

        # Setup Observatory environment
        env = ObservatoryEnvironment(self.project_id, self.data_location, api_host=self.host, api_port=self.port)
        bq_dataset_id = env.add_dataset()


        # First run should download the baseline files and create the large initial table.
        # figure out how to make this change each year so that tests dont keep breaking, or just add a note?

        # For each run, make an environment with the telescope instance

        for run in self.run_list:

            with env.create():
                self.setup_api()

                # Setup Telescope
                # TODO: Get the workflow ID of the telescope for this section.
                workflow = PubMedTelescope(dag_id=self.dag_id, 
                                           ftp_server_url="ftp.server.local")

                dag = workflow.make_dag()

                # TODO: execution date is wrong here. need to use the data_interval_end date.
                # First snapshot instance
                run = self.first_run
                with env.create_dag_run(dag, run["data_interval_start"]) as dag_run:

                    # Test that all dependencies are specified: no error should be thrown
                    env.run_task(workflow.check_dependencies.__name__)

                    # Pull out ti from kwargs for data interval start and end

                    self.assertEqual(dag.default_args["start_date"], start_date) # The absolute start of the telescope
                    self.assertEqual(pendulum.datetime(year=2021, month=12, day=4), run["data_interval_start"])

                    # use telescope task itself to determine if it's the first release or not. 
                    is_first_release = workflow.check_releases.__name__

                    self.assertTrue(first_release)
                  
                    # Use release info for other tasks
                    # TODO: Fix the properties on the release. 
                    release = PubMedRelease(
                        workflow.dag_id,
                        workflow.workflow_id,
                        workflow.dataset_type_id,

                    )


                    ### CHECK RELEASES ###

                    # Check that telescope checks for the release files 
                    ti = env.run_task(workflow.check_releases.__name__)

                    mock_ftp = mock_ftp_constructor.return_value

                    # Check that server address is correct
                    mock_ftp_constructor.assert_called_with("ftp.server.local")
               
                    # Check that ftp.login() was called
                    self.assertTrue(mock_ftp.login.called)

                    # Check folders where changed into
                    mock_ftp.cwd.assert_called_with('change to folders')

                    ### DOWNLOAD ###

                    # Test that file was downloaded
                    env.run_task(workflow.download.__name__)
                    # TODO: Make a list of baseline release files to download for test
                    #self.assertEqual(len(files_downloaded), expected_files_downloaded)

                    ### UPLOAD DOWNLAODED ###

                    # Upload the mock downloaded files to GCS and check their hash as an assert.
                    for entity, info in self.entities.items():
                        gzip_path = f"{entity}.jsonl.gz"
                        with open(info["download_path"], "rb") as f_in, gzip.open(gzip_path, "wb") as f_out:
                            f_out.writelines(f_in)

                            download_blob = (
                                f"telescopes/{release.dag_id}/{release.release_id}/transform/"
                                f"data/{entity}/updated_date={run['manifest_date']}/0000_part_00.gz"
                            )
                            upload_file_to_cloud_storage(release.download_bucket, download_blob, gzip_path)


                    ### TRANSFORM ###

                    # Check if files exist and if the hash of the files are correct. 

                    # Test that files transformed
                    env.run_task(workflow.transform.__name__)
                    self.assertEqual(3, len(release.transform_files))
                    # Sort lines so that gzip crc is always the same
                    for file in release.transform_files:
                        entity = file.split("/")[-3]
                        with gzip.open(file, "rb") as f_in:
                            lines = sorted(f_in.readlines())
                        with gzip.open(file, "wb") as f_out:
                            f_out.writelines(lines)
                        self.assert_file_integrity(file, self.entities[entity]["transform_hash"], "gzip_crc")

                    ### UPLOAD TRANSFORMED ###

                    # Test that transformed files uploaded
                    ti = env.run_task(workflow.upload_transformed.__name__)
                    #ti[]
                    # pull updated list from kwargs of the uploaded files. 

                    for pubmed_data in pubmed_transform_list:
                        
                    ### CREATE RELEASE SNAPSHOT ###

                    # Get bq load info for BQ tasks
                    bq_load_info = workflow.get_bq_load_info(release)

                    # Test append new creates table
                    env.run_task(workflow.bq_append_new.__name__)
                    for _, table, _ in bq_load_info:
                        table_id = f"{self.project_id}.{worflow.dataset_id}.{table}"
                        expected_bytes = run["table_bytes"][table]
                        self.assert_table_bytes(table_id, expected_bytes)

                    # Test delete old task is skipped for the first release
                    with patch("observatory.platform.utils.gc_utils.bq_query_bytes_daily_limit_check"):
                        ti = env.run_task(workflow.bq_delete_old.__name__)
                    self.assertEqual(ti.state, State.SUCCESS)

                    # Test create bigquery snapshot
                    ti = env.run_task(workflow.bq_create_snapshot.__name__)
                    self.assertEqual(ti.state, State.SUCCESS)

                    # Test adding of dataset releases as well as cleanup
                    download_folder, extract_folder, transform_folder = (
                        release.download_folder,
                        release.extract_folder,
                        release.transform_folder,
                    )

                    ### CLEANUP ###

                    ti = env.run_task(workflow.cleanup.__name__)

                    # check that download and transform files were deleted from local storage
                    # and that local ti were cleared

                    openalex_dataset_releases = get_dataset_releases(dataset_id=1)
    
                    self.assert_cleanup(download_folder, extract_folder, transform_folder)

    @patch("academic_observatory_workflows.workflows.pubmed_telescope.wait_for_process")
    @patch("academic_observatory_workflows.workflows.pubmed_telescope.logging.info")
    def test_run_subprocess_cmd(self, mock_logging, mock_wait_for_proc):
        """Test the run_subprocess_cmd function.

        :return: None.
        """
        # Mock logging
        mock_wait_for_proc.return_value = ("out", "err")

        # Set up parameters
        args = ["run", "unittest"]
        proc = Mock(spec=Popen)

        # Test when return code is 0
        proc.returncode = 0
        run_subprocess_cmd(proc, args)
        expected_logs = ["Executing bash command: run unittest", "out", "err", "Finished cmd successfully"]
        self.assertListEqual([call(log) for log in expected_logs], mock_logging.call_args_list)

        # Test when return code is 1
        proc.returncode = 1
        with self.assertRaises(AirflowException):
            run_subprocess_cmd(proc, args)

    def test_pull_out_attributes(self):
        """
        Test that attributes from the Biopython data classes can be reliably pulled out from the data and added to the dictionary.

        :return: None.
        """

        # Give it a fake or random Biopython data class

        biopython_dict = DictionaryElement.__init__({"data": "something"}, attrs={"type": "dict"})
        biopython_list = ListElement.__init__(["data"], attrs={"type": "list"})
        biopython_str = StringElement.__init__("data", attrs={"type": "str"})

        objects = [biopython_dict, biopython_list, biopython_str]

        dict_expected = {"data": "something", "type": "dict"}
        list_expected = {"value": ["data"], "type": "list"}
        str_expected = {"value": "data", "type": "str"}

        expected_objects = [dict_expected, list_expected, str_expected]

        for obj, expected in zip(objects, expected_objects):
            # Pull out the attributes and ensure that data returned is data expected.
            pulled_data = add_attributes_to_data_from_biopython_classes(obj)

            self.assertEqual(pulled_data, expected)

    def test_biopython_read_xml(self):
        """
        Test that files can be reliably transformed.

        :return: None.
        """

        
        example_xml = """
        <?xml version="1.0" encoding="utf-8"?>
        <!DOCTYPE PubmedArticleSet PUBLIC "-//NLM//DTD PubMedArticle, 1st January 2023//EN" "https://dtd.nlm.nih.gov/ncbi/pubmed/out/pubmed_230101.dtd">
            <PubmedArticleSet>
                <PubmedArticle>
                    <MedlineCitation Status="MEDLINE" Owner="NLM">
                        <PMID Version="1">10753808</PMID>
                        <DateCompleted>
                            <Year>2000</Year>
                            <Month>06</Month>
                            <Day>12</Day>
                        </DateCompleted>
                    </MedlineCitation>
                    <PubmedData>
                        <History>
                            <PubMedPubDate PubStatus="pubmed">
                                <Year>2000</Year>
                                <Month>4</Month>
                                <Day>8</Day>
                                <Hour>9</Hour>
                                <Minute>0</Minute>
                            </PubMedPubDate>
                        </History>
                    </PubmedData>
                </PubmedArticle>
            </PubmedArticleSet>
        </xml>
        """

        expected_biopython_output = ListElement(
            DictionaryElement(),
            DictionaryElement()
        )

        # read it in using the biopython library for xml to data dict.

        #Check that they are the same.

        # Confirm that the data has been cross referenced against the schema file.

    def test_pull_data_from_dict(self):

        """Test that the incoming dictionary has the correct keys for the PubMed data. 
        
        :return: None.
        """

        example_transform_list = [
            {
                "sub_key": "something_deletion1",
                "set_key": "something_deletion1"
            },
            {
                "sub_key": "something_addition1", 
                "set_key": "something_addition2"
            }
        ]

        expected_data_parts = {

        }

        mock_input_file = "something_example.xml.gz"
        mock_data_dict = {}

        for i in range(len(example_transform_list)):
            pubmed_data = example_transform_list[i]

            data_part = pull_data_from_dict(
                filename=mock_input_file,
                data_dict=mock_data_dict,
                data_name=pubmed_data["data_type"],
                sub_set=pubmed_data["sub_key"],
                set_key=pubmed_data["set_key"],
            )

            # Confirm that data part is what we want. 
            # self.assertEquals(data_part, expected_data_part[i])


    def test_customEncoder(self):
        """Test that files are written out as expected using the CustomEncoder for PubMed files.

        :return: None.
        """

        # Read in json file 

        # use customEncoder to write out the file

        # Read in the output file from the customEncoder to ensure that it's written as required. 


