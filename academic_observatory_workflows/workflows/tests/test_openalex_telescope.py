# Copyright 2022 Curtin University
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

# Author: Aniek Roelofs

import gzip
import io
import json
import os
from datetime import timedelta
from subprocess import Popen
from unittest.mock import Mock, call, patch

import pendulum
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models.connection import Connection
from botocore.response import StreamingBody
from click.testing import CliRunner
from observatory.platform.utils.gc_utils import (
    upload_file_to_cloud_storage,
)
from observatory.platform.utils.jinja2_utils import render_template
from observatory.platform.utils.test_utils import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    module_file_path,
)

from academic_observatory_workflows.config import test_fixtures_folder
from academic_observatory_workflows.workflows.openalex_telescope import (
    OpenAlexRelease,
    OpenAlexTelescope,
    run_subprocess_cmd,
    transform_file,
    transform_object,
)


class TestOpenAlexTelescope(ObservatoryTestCase):
    """Tests for the OpenAlex telescope"""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.
        :param args: arguments.
        :param kwargs: keyword arguments.
        """
        super(TestOpenAlexTelescope, self).__init__(*args, **kwargs)
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")

        self.manifest_obj_path = test_fixtures_folder("openalex", "manifest_object.json.jinja2")
        self.entities = {
            "authors": {
                "download_path": test_fixtures_folder("openalex", "authors.jsonl"),
                "bucket": "transform_bucket",
            },
            "concepts": {
                "download_path": test_fixtures_folder("openalex", "concepts.jsonl"),
                "bucket": "download_bucket",
                "download_hash": "14bd0919",
                "transform_hash": "4bb6fe07",
            },
            "institutions": {
                "download_path": test_fixtures_folder("openalex", "institutions.jsonl"),
                "bucket": "download_bucket",
                "download_hash": "b23bb91c",
                "transform_hash": "a9cfff73",
            },
            "venues": {
                "download_path": test_fixtures_folder("openalex", "venues.jsonl"),
                "bucket": "transform_bucket",
            },
            "works": {
                "download_path": test_fixtures_folder("openalex", "works.jsonl"),
                "bucket": "download_bucket",
                "download_hash": "806d7995",
                "transform_hash": "0a783ffc",
            },
        }
        self.table_bytes = {
            "Author": 3965,
            "Author_partitions": 3965,
            "Concept": 3947,
            "Concept_partitions": 3947,
            "Institution": 3259,
            "Institution_partitions": 3259,
            "Venue": 2108,
            "Venue_partitions": 2108,
            "Work": 11804,
            "Work_partitions": 11804,
        }
        self.first_run = {
            "execution_date": pendulum.datetime(year=2022, month=1, day=1),
            "manifest_date": "2021-12-17",
            "manifest_download_hash": "9ab1f7c9eb0adbdaf07baaf8b97a110e",
            "manifest_transform_hash": "6400ca22b963599af6bad9db030fe11a",
        }
        self.second_run = {
            "execution_date": pendulum.datetime(year=2022, month=2, day=1),
            "manifest_date": "2022-01-17",
            "manifest_download_hash": "f4cea919d06caa0811ad5976bf98986a",
            "manifest_transform_hash": "50e2eff06007a32c4394df8df7f5e907",
        }

    def test_dag_structure(self):
        """Test that the OpenAlex DAG has the correct structure.
        :return: None
        """

        dag = OpenAlexTelescope().make_dag()
        self.assert_dag_structure(
            {
                "check_dependencies": ["write_transfer_manifest"],
                "write_transfer_manifest": ["transfer"],
                "transfer": ["download_transferred"],
                "download_transferred": ["transform"],
                "transform": ["upload_transformed"],
                "upload_transformed": ["bq_load_partition"],
                "bq_load_partition": ["bq_delete_old"],
                "bq_delete_old": ["bq_append_new"],
                "bq_append_new": ["cleanup"],
                "cleanup": [],
            },
            dag,
        )

    def test_dag_load(self):
        """Test that the OpenAlex DAG can be loaded from a DAG bag.
        :return: None
        """

        with ObservatoryEnvironment().create():
            dag_file = os.path.join(module_file_path("academic_observatory_workflows.dags"), "openalex_telescope.py")
            self.assert_dag_load("openalex", dag_file)

    @patch("academic_observatory_workflows.workflows.openalex_telescope.aws_to_google_cloud_storage_transfer")
    @patch("academic_observatory_workflows.workflows.openalex_telescope.boto3.client")
    def test_telescope(self, mock_client, mock_transfer):
        """Test the OpenAlex telescope end to end.
        :return: None.
        """
        # Setup Observatory environment
        env = ObservatoryEnvironment(self.project_id, self.data_location)
        dataset_id = env.add_dataset()

        # Setup Telescope
        telescope = OpenAlexTelescope(dataset_id=dataset_id)
        dag = telescope.make_dag()

        # Create the Observatory environment and run tests
        with env.create():
            # Add connection
            conn = Connection(
                conn_id=OpenAlexTelescope.AIRFLOW_CONN_AWS, uri="aws://UWLA41aAhdja:AJLD91saAJSKAL0AjAhkaka@"
            )
            env.add_connection(conn)

            run = self.first_run
            with env.create_dag_run(dag, run["execution_date"]) as dag_run:
                # Test that all dependencies are specified: no error should be thrown
                env.run_task(telescope.check_dependencies.__name__)
                start_date, end_date, first_release = telescope.get_release_info(
                    next_execution_date=pendulum.today("UTC"),
                    dag=dag,
                    dag_run=dag_run,
                )
                self.assertEqual(dag.default_args["start_date"], start_date)
                self.assertEqual(pendulum.today("UTC") - timedelta(days=1), end_date)
                self.assertTrue(first_release)

                # Use release info for other tasks
                release = OpenAlexRelease(
                    telescope.dag_id,
                    start_date,
                    end_date,
                    first_release,
                    max_processes=1,
                )

                # Mock response of get_object on last_modified file, mocking lambda file
                side_effect = []
                for entity in self.entities:
                    manifest_content = render_template(
                        self.manifest_obj_path, entity=entity, date=run["manifest_date"]
                    ).encode()
                    side_effect.append({"Body": StreamingBody(io.BytesIO(manifest_content), len(manifest_content))})
                mock_client().get_object.side_effect = side_effect

                # Test write transfer manifest task
                env.run_task(telescope.write_transfer_manifest.__name__)
                self.assert_file_integrity(
                    release.transfer_manifest_path_download, run["manifest_download_hash"], "md5"
                )
                self.assert_file_integrity(
                    release.transfer_manifest_path_transform, run["manifest_transform_hash"], "md5"
                )

                # Test transfer task
                mock_transfer.reset_mock()
                mock_transfer.return_value = True, 2
                env.run_task(telescope.transfer.__name__)
                self.assertEqual(2, mock_transfer.call_count)
                try:
                    self.assertTupleEqual(mock_transfer.call_args_list[0][0], (conn.login, conn.password))
                    self.assertTupleEqual(mock_transfer.call_args_list[1][0], (conn.login, conn.password))
                except AssertionError:
                    raise AssertionError("AWS key id and secret not passed correctly to transfer function")
                self.assertDictEqual(
                    mock_transfer.call_args_list[0][1],
                    {
                        "aws_bucket": OpenAlexTelescope.AWS_BUCKET,
                        "include_prefixes": [
                            f"data/concepts/updated_date={run['manifest_date']}/0000_part_00.gz",
                            f"data/institutions/updated_date={run['manifest_date']}/0000_part_00.gz",
                            f"data/works/updated_date={run['manifest_date']}/0000_part_00.gz",
                        ],
                        "gc_project_id": self.project_id,
                        "gc_bucket": release.download_bucket,
                        "gc_bucket_path": f"telescopes/{release.dag_id}/{release.release_id}/",
                        "description": f"Transfer OpenAlex data from Airflow telescope to {release.download_bucket}",
                    },
                )
                self.assertDictEqual(
                    mock_transfer.call_args_list[1][1],
                    {
                        "aws_bucket": OpenAlexTelescope.AWS_BUCKET,
                        "include_prefixes": [
                            f"data/authors/updated_date={run['manifest_date']}/0000_part_00.gz",
                            f"data/venues/updated_date={run['manifest_date']}/0000_part_00.gz",
                        ],
                        "gc_project_id": self.project_id,
                        "gc_bucket": release.transform_bucket,
                        "gc_bucket_path": f"telescopes/{release.dag_id}/{release.release_id}/",
                        "description": f"Transfer OpenAlex data from Airflow telescope to {release.transform_bucket}",
                    },
                )

                # Upload files to bucket, to mock transfer
                for entity, info in self.entities.items():
                    blob = f"telescopes/{release.dag_id}/{release.release_id}/data/{entity}/updated_date={run['manifest_date']}/0000_part_00.gz"
                    gzip_path = f"{entity}.jsonl.gz"
                    with open(info["download_path"], "rb") as f_in, gzip.open(gzip_path, "wb") as f_out:
                        f_out.writelines(f_in)

                    upload_file_to_cloud_storage(getattr(release, info["bucket"]), blob, gzip_path)

                # Test that file was downloaded
                env.run_task(telescope.download_transferred.__name__)
                self.assertEqual(3, len(release.download_files))
                for file in release.download_files:
                    entity = file.split("/")[-3]
                    self.assert_file_integrity(file, self.entities[entity]["download_hash"], "gzip_crc")

                # Test that files transformed
                env.run_task(telescope.transform.__name__)
                self.assertEqual(3, len(release.transform_files))
                # Sort lines so that gzip crc is always the same
                for file in release.transform_files:
                    entity = file.split("/")[-3]
                    with gzip.open(file, "rb") as f_in:
                        lines = sorted(f_in.readlines())
                    with gzip.open(file, "wb") as f_out:
                        f_out.writelines(lines)
                    self.assert_file_integrity(file, self.entities[entity]["transform_hash"], "gzip_crc")

                # Test that transformed files uploaded
                env.run_task(telescope.upload_transformed.__name__)
                for entity, info in self.entities.items():
                    if entity in ["concepts", "institutions", "works"]:
                        file = [file for file in release.transform_files if entity in file][0]
                    else:
                        file = f"{entity}.jsonl.gz"
                    blob = f"telescopes/{release.dag_id}/{release.release_id}/data/{entity}/updated_date={run['manifest_date']}/0000_part_00.gz"
                    self.assert_blob_integrity(env.transform_bucket, blob, file)

                # Get bq load info for BQ tasks
                bq_load_info = telescope.get_bq_load_info(release)

                # Test that load partition task is skipped for the first release
                ti = env.run_task(telescope.bq_load_partition.__name__)
                self.assertEqual(ti.state, "skipped")

                # Test delete old task is skipped for the first release
                with patch("observatory.platform.utils.gc_utils.bq_query_bytes_daily_limit_check"):
                    ti = env.run_task(telescope.bq_delete_old.__name__)
                self.assertEqual(ti.state, "skipped")

                # Test append new creates table
                env.run_task(telescope.bq_append_new.__name__)
                for _, main_table_id, _ in bq_load_info:
                    table_id = f"{self.project_id}.{telescope.dataset_id}.{main_table_id}"
                    expected_bytes = self.table_bytes[main_table_id]
                    self.assert_table_bytes(table_id, expected_bytes)

                # Test that all telescope data deleted
                download_folder, extract_folder, transform_folder = (
                    release.download_folder,
                    release.extract_folder,
                    release.transform_folder,
                )
                env.run_task(telescope.cleanup.__name__)
                self.assert_cleanup(download_folder, extract_folder, transform_folder)

            run = self.second_run
            with env.create_dag_run(dag, run["execution_date"]) as dag_run:
                # Test that all dependencies are specified: no error should be thrown
                env.run_task(telescope.check_dependencies.__name__)
                start_date, end_date, first_release = telescope.get_release_info(
                    next_execution_date=pendulum.today("UTC"),
                    dag=dag,
                    dag_run=dag_run,
                )
                self.assertEqual(release.end_date + timedelta(days=1), start_date)
                self.assertEqual(pendulum.today("UTC") - timedelta(days=1), end_date)
                self.assertFalse(first_release)

                # Use release info for other tasks
                release = OpenAlexRelease(
                    telescope.dag_id,
                    start_date,
                    end_date,
                    first_release,
                    max_processes=1,
                )

                # Mock response of get_object on last_modified file, mocking lambda file
                side_effect = []
                for entity in self.entities:
                    manifest_content = render_template(
                        self.manifest_obj_path, entity=entity, date=run["manifest_date"]
                    ).encode()
                    side_effect.append({"Body": StreamingBody(io.BytesIO(manifest_content), len(manifest_content))})
                mock_client().get_object.side_effect = side_effect

                # Test write transfer manifest task
                env.run_task(telescope.write_transfer_manifest.__name__)
                self.assert_file_integrity(
                    release.transfer_manifest_path_download, run["manifest_download_hash"], "md5"
                )
                self.assert_file_integrity(
                    release.transfer_manifest_path_transform, run["manifest_transform_hash"], "md5"
                )

                # Test transfer task
                mock_transfer.reset_mock()
                mock_transfer.return_value = True, 2
                env.run_task(telescope.transfer.__name__)
                self.assertEqual(2, mock_transfer.call_count)
                try:
                    self.assertTupleEqual(mock_transfer.call_args_list[0][0], (conn.login, conn.password))
                    self.assertTupleEqual(mock_transfer.call_args_list[1][0], (conn.login, conn.password))
                except AssertionError:
                    raise AssertionError("AWS key id and secret not passed correctly to transfer function")
                self.assertDictEqual(
                    mock_transfer.call_args_list[0][1],
                    {
                        "aws_bucket": OpenAlexTelescope.AWS_BUCKET,
                        "include_prefixes": [
                            f"data/concepts/updated_date={run['manifest_date']}/0000_part_00.gz",
                            f"data/institutions/updated_date={run['manifest_date']}/0000_part_00.gz",
                            f"data/works/updated_date={run['manifest_date']}/0000_part_00.gz",
                        ],
                        "gc_project_id": self.project_id,
                        "gc_bucket": release.download_bucket,
                        "gc_bucket_path": f"telescopes/{release.dag_id}/{release.release_id}/",
                        "description": f"Transfer OpenAlex data from Airflow telescope to {release.download_bucket}",
                    },
                )
                self.assertDictEqual(
                    mock_transfer.call_args_list[1][1],
                    {
                        "aws_bucket": OpenAlexTelescope.AWS_BUCKET,
                        "include_prefixes": [
                            f"data/authors/updated_date={run['manifest_date']}/0000_part_00.gz",
                            f"data/venues/updated_date={run['manifest_date']}/0000_part_00.gz",
                        ],
                        "gc_project_id": self.project_id,
                        "gc_bucket": release.transform_bucket,
                        "gc_bucket_path": f"telescopes/{release.dag_id}/{release.release_id}/",
                        "description": f"Transfer OpenAlex data from Airflow telescope to {release.transform_bucket}",
                    },
                )

                # Upload files to bucket, to mock transfer
                for entity, info in self.entities.items():
                    blob = f"telescopes/{release.dag_id}/{release.release_id}/data/{entity}/updated_date={run['manifest_date']}/0000_part_00.gz"
                    gzip_path = f"{entity}.jsonl.gz"
                    with open(info["download_path"], "rb") as f_in, gzip.open(gzip_path, "wb") as f_out:
                        f_out.writelines(f_in)

                    upload_file_to_cloud_storage(getattr(release, info["bucket"]), blob, gzip_path)

                # Test that file was downloaded
                env.run_task(telescope.download_transferred.__name__)
                self.assertEqual(3, len(release.download_files))
                for file in release.download_files:
                    entity = file.split("/")[-3]
                    self.assert_file_integrity(file, self.entities[entity]["download_hash"], "gzip_crc")

                # Test that files transformed
                env.run_task(telescope.transform.__name__)
                self.assertEqual(3, len(release.transform_files))
                # Sort lines so that gzip crc is always the same
                for file in release.transform_files:
                    entity = file.split("/")[-3]
                    with gzip.open(file, "rb") as f_in:
                        lines = sorted(f_in.readlines())
                    with gzip.open(file, "wb") as f_out:
                        f_out.writelines(lines)
                    self.assert_file_integrity(file, self.entities[entity]["transform_hash"], "gzip_crc")

                # Test that transformed files uploaded
                env.run_task(telescope.upload_transformed.__name__)
                for entity, info in self.entities.items():
                    if entity in ["concepts", "institutions", "works"]:
                        file = [file for file in release.transform_files if entity in file][0]
                    else:
                        file = f"{entity}.jsonl.gz"
                    blob = f"telescopes/{release.dag_id}/{release.release_id}/data/{entity}/updated_date={run['manifest_date']}/0000_part_00.gz"
                    self.assert_blob_integrity(env.transform_bucket, blob, file)

                # Get bq load info for BQ tasks
                bq_load_info = telescope.get_bq_load_info(release)

                # Test that partition is loaded
                ti = env.run_task(telescope.bq_load_partition.__name__)
                for _, _, partition_table_id in bq_load_info:
                    table_id = f"{self.project_id}.{telescope.dataset_id}.{partition_table_id}"
                    expected_bytes = self.table_bytes[partition_table_id]
                    self.assert_table_bytes(table_id, expected_bytes)

                # Test that partition is deleted from main table
                with patch("observatory.platform.utils.gc_utils.bq_query_bytes_daily_limit_check"):
                    ti = env.run_task(telescope.bq_delete_old.__name__)
                for _, main_table_id, _ in bq_load_info:
                    table_id = f"{self.project_id}.{telescope.dataset_id}.{main_table_id}"
                    expected_bytes = 0
                    self.assert_table_bytes(table_id, expected_bytes)

                # Test append new creates table
                env.run_task(telescope.bq_append_new.__name__)
                for _, main_table_id, _ in bq_load_info:
                    table_id = f"{self.project_id}.{telescope.dataset_id}.{main_table_id}"
                    expected_bytes = self.table_bytes[main_table_id]
                    self.assert_table_bytes(table_id, expected_bytes)

                # Test that all telescope data deleted
                download_folder, extract_folder, transform_folder = (
                    release.download_folder,
                    release.extract_folder,
                    release.transform_folder,
                )
                env.run_task(telescope.cleanup.__name__)
                self.assert_cleanup(download_folder, extract_folder, transform_folder)

    @patch("academic_observatory_workflows.workflows.openalex_telescope.boto3.client")
    @patch("academic_observatory_workflows.workflows.openalex_telescope.get_aws_conn_info")
    @patch("academic_observatory_workflows.workflows.openalex_telescope.Variable.get")
    def test_write_transfer_manifest(self, mock_variable_get, mock_aws_info, mock_boto3):
        """Test write_transfer_manifest method of the OpenAlex release.

        :param mock_variable_get: Mock Airflow Variable get() method
        :param mock_boto3: Mock the boto3 client
        :return: None.
        """
        mock_variable_get.return_value = "data"
        mock_aws_info.return_value = "key_id", "secret_key"
        # Mock response of get_object on last_modified file, mocking lambda file
        side_effect = []
        for tests in range(2):
            for entity in self.entities:
                manifest_content = render_template(self.manifest_obj_path, entity=entity, date="2022-01-01").encode()
                side_effect.append({"Body": StreamingBody(io.BytesIO(manifest_content), len(manifest_content))})
        mock_boto3().get_object.side_effect = side_effect

        with CliRunner().isolated_filesystem():
            # Test with entries in manifest objects that are after start date
            start_date = pendulum.DateTime(2022, 1, 1, tzinfo=pendulum.tz.UTC)
            end_date = pendulum.DateTime(2022, 2, 1, tzinfo=pendulum.tz.UTC)
            release = OpenAlexRelease("dag_id", start_date, end_date, False, 1)

            release.write_transfer_manifest()
            self.assert_file_integrity(
                release.transfer_manifest_path_download, "42fb45119bd34709001fd6c90a6ef60e", "md5"
            ),
            self.assert_file_integrity(
                release.transfer_manifest_path_transform, "fe8442cd31fec1c335379033afebc1ea", "md5"
            )

            # Test with entries in manifest objects that are before start date
            start_date = pendulum.DateTime(2022, 3, 1, tzinfo=pendulum.tz.UTC)
            end_date = pendulum.DateTime(2022, 4, 1, tzinfo=pendulum.tz.UTC)
            release = OpenAlexRelease("dag_id", start_date, end_date, False, 1)

            with self.assertRaises(AirflowSkipException):
                release.write_transfer_manifest()

    @patch("academic_observatory_workflows.workflows.openalex_telescope.aws_to_google_cloud_storage_transfer")
    @patch("academic_observatory_workflows.workflows.openalex_telescope.get_aws_conn_info")
    @patch("academic_observatory_workflows.workflows.openalex_telescope.Variable.get")
    def test_transfer(self, mock_variable_get, mock_aws_info, mock_transfer):
        """Test transfer method of the OpenAlex release.

        :param mock_variable_get: Mock Airflow Variable get() method
        :param mock_aws_info: Mock getting AWS info
        :param mock_transfer: Mock the transfer function called inside release.transfer()
        :return: None.
        """
        mock_variable_get.side_effect = lambda x: {
            "download_bucket": "download-bucket",
            "transform_bucket": "transform-bucket",
            "project_id": "project_id",
            "data_path": "data",
        }[x]
        mock_aws_info.return_value = "key_id", "secret_key"
        mock_transfer.return_value = True, 3

        with CliRunner().isolated_filesystem():
            # Create release
            start_date = pendulum.DateTime(2022, 1, 1)
            end_date = pendulum.DateTime(2022, 2, 1)
            release = OpenAlexRelease("dag_id", start_date, end_date, False, 1)

            # Create transfer manifest files
            with open(release.transfer_manifest_path_download, "w") as f:
                f.write('"prefix1"\n"prefix2"\n')
            with open(release.transfer_manifest_path_transform, "w") as f:
                f.write("")

            # Test succesful transfer with prefixes for download, no prefixes for transform
            release.transfer(max_retries=1)
            mock_transfer.assert_called_once_with(
                "key_id",
                "secret_key",
                aws_bucket=OpenAlexTelescope.AWS_BUCKET,
                include_prefixes=["prefix1", "prefix2"],
                gc_project_id="project_id",
                gc_bucket="download-bucket",
                gc_bucket_path="telescopes/dag_id/2022_01_01-2022_02_01/",
                description="Transfer OpenAlex data from Airflow telescope to download-bucket",
            )
            mock_transfer.reset_mock()

            # Test failed transfer
            mock_transfer.return_value = False, 4
            with self.assertRaises(AirflowException):
                release.transfer(1)

    @patch("academic_observatory_workflows.workflows.openalex_telescope.wait_for_process")
    @patch("academic_observatory_workflows.workflows.openalex_telescope.logging.info")
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

    @patch("academic_observatory_workflows.workflows.openalex_telescope.transform_object")
    def test_transform_file(self, mock_transform_object):
        """Test the transform_file function.

        :return: None.
        """
        mock_transform_object.return_value = {}
        with CliRunner().isolated_filesystem() as t:
            transform_path = "transform/out.jsonl.gz"

            # Create works entity file
            works = {"works": "content"}
            works_download_path = "works.jsonl.gz"
            with gzip.open(works_download_path, "wt", encoding="ascii") as f_out:
                json.dump(works, f_out)

            # Create other entity file (concepts or institution)
            concepts = {"concepts": "content"}
            concepts_download_path = "concepts.jsonl.gz"
            with gzip.open(concepts_download_path, "wt", encoding="ascii") as f_out:
                json.dump(concepts, f_out)

            # Test when dir of transform path does not exist yet, using 'works' entity'
            self.assertFalse(os.path.isdir(os.path.dirname(transform_path)))

            transform_file(works_download_path, transform_path)
            mock_transform_object.assert_called_once_with(works, "abstract_inverted_index")
            mock_transform_object.reset_mock()
            os.remove(transform_path)

            # Test when dir of transform path does exist, using 'works' entity
            self.assertTrue(os.path.isdir(os.path.dirname(transform_path)))

            transform_file(works_download_path, transform_path)
            self.assert_file_integrity(transform_path, "682a6d42", "gzip_crc")
            mock_transform_object.assert_called_once_with(works, "abstract_inverted_index")
            mock_transform_object.reset_mock()
            os.remove(transform_path)

            # Test for "concepts" and "institution" entities
            transform_file(concepts_download_path, transform_path)
            self.assert_file_integrity(transform_path, "d8cafe16", "gzip_crc")
            mock_transform_object.assert_called_once_with(concepts, "international")

    def test_transform_object(self):
        """Test the transform_object function.

        :return: None.
        """
        # Test object with nested "international" fields
        obj1 = {
            "international": {
                "display_name": {
                    "af": "Dokumentbestuurstelsel",
                    "fr": "type de logiciel",
                    "ro": "colecție organizată a documentelor",
                }
            }
        }
        transform_object(obj1, "international")
        self.assertDictEqual(
            {
                "international": {
                    "display_name": {
                        "keys": ["af", "fr", "ro"],
                        "values": [
                            "Dokumentbestuurstelsel",
                            "type de logiciel",
                            "colecție organizată " "a documentelor",
                        ],
                    }
                }
            },
            obj1,
        )

        # Test object with nested "international" none
        obj2 = {"international": {"display_name": None}}
        transform_object(obj2, "international")
        self.assertDictEqual({"international": {"display_name": None}}, obj2)

        # Test object with nested "abstract_inverted_index" fields
        obj3 = {
            "abstract_inverted_index": {
                "Malignant": [0],
                "hyperthermia": [1],
                "susceptibility": [2],
                "(MHS)": [3],
                "is": [4, 6],
                "primarily": [5],
            }
        }
        transform_object(obj3, "abstract_inverted_index")
        self.assertDictEqual(
            {
                "abstract_inverted_index": {
                    "keys": ["Malignant", "hyperthermia", "susceptibility", "(MHS)", "is", "primarily"],
                    "values": ["0", "1", "2", "3", "4, 6", "5"],
                }
            },
            obj3,
        )

        # Test object with nested "abstract_inverted_index" none
        obj4 = {"abstract_inverted_index": None}
        transform_object(obj4, "abstract_inverted_index")
        self.assertDictEqual({"abstract_inverted_index": None}, obj4)
