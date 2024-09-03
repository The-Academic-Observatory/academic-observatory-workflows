import gzip
import json
import os
import shutil

import pendulum
from Bio.Entrez.Parser import DictionaryElement, ListElement, StringElement
from click.testing import CliRunner

from academic_observatory_workflows.config import project_path
from academic_observatory_workflows.pubmed_telescope.datafile import Datafile
from academic_observatory_workflows.pubmed_telescope.tasks import (
    add_attributes,
    change_pubmed_list_structure,
    download_datafiles,
    load_datafile,
    merge_upserts_and_deletes,
    parse_articles,
    parse_deletes,
    PMID,
    PubMedCustomEncoder,
    PubmedUpdatefile,
    save_pubmed_jsonl,
    save_pubmed_merged_upserts,
    transform_pubmed,
)
from observatory_platform.sandbox.sandbox_environment import SandboxEnvironment
from observatory_platform.sandbox.test_utils import SandboxTestCase, find_free_port
from observatory_platform.sandbox.ftp_server import FtpServer
from observatory_platform.airflow.release import ChangefileRelease

FIXTURES_FOLDER = project_path("pubmed_telescope", "tests", "fixtures")


class TestPubMedUtils(SandboxTestCase):
    def __init__(self, *args, **kwargs):
        super(TestPubMedUtils, self).__init__(*args, **kwargs)

        self.dag_id = "pubmed"
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")

        # FTP Server params
        self.ftp_server_url = "localhost"
        self.ftp_port = find_free_port()
        self.baseline_path = "/baseline/"
        self.updatefiles_path = "/updatefiles/"

    def test_download_datafiles(self):
        """Test that an exmaple PubMed XMLs can be transformed successfully."""

        # Create mock FTP server to host the test Pubmed Files.
        ftp_server = FtpServer(host=self.ftp_server_url, port=self.ftp_port, directory=FIXTURES_FOLDER)

        with ftp_server.create():
            # Setup environment
            env = SandboxEnvironment(self.project_id, self.data_location)

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
                    max_download_attempt=1,
                )

                self.assertTrue(success)

                for datafile in datafiles_to_download:
                    self.assertTrue(os.path.exists(datafile.download_file_path))

    def test_load_datafile(self):
        """Test that a Pubmed datafile can be read in and parsed."""

        xml_file_path = os.path.join(FIXTURES_FOLDER, "baseline", "pubmed22n0001.xml.gz")
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
        env = SandboxEnvironment(self.project_id, self.data_location)

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
            bad_xml_file_path = os.path.join(FIXTURES_FOLDER, "pubmed22n0001_bad_fields.xml.gz")
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
            valid_xml_file_path = os.path.join(FIXTURES_FOLDER, "baseline", "pubmed22n0001.xml.gz")
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

            valid_xml_file_path = os.path.join(FIXTURES_FOLDER, "updatefiles", "pubmed22n0003.xml.gz")
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
