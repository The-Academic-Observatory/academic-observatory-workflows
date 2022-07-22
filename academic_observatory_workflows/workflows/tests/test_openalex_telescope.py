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
from academic_observatory_workflows.workflows.openalex_telescope import (
    OpenAlexRelease,
    OpenAlexTelescope,
    run_subprocess_cmd,
    transform_file,
    transform_object,
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
from observatory.platform.utils.jinja2_utils import render_template
from observatory.platform.utils.release_utils import get_dataset_releases
from observatory.platform.utils.test_utils import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    module_file_path,
    find_free_port,
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
            },
            "concepts": {
                "download_path": test_fixtures_folder("openalex", "concepts.jsonl"),
                "download_hash": "14bd0919",
                "transform_hash": "4bb6fe07",
            },
            "institutions": {
                "download_path": test_fixtures_folder("openalex", "institutions.jsonl"),
                "download_hash": "b23bb91c",
                "transform_hash": "a9cfff73",
            },
            "venues": {
                "download_path": test_fixtures_folder("openalex", "venues.jsonl"),
            },
            "works": {
                "download_path": test_fixtures_folder("openalex", "works.jsonl"),
                "download_hash": "806d7995",
                "transform_hash": "0a783ffc",
            },
        }

        self.first_run = {
            "execution_date": pendulum.datetime(year=2022, month=1, day=2),
            "manifest_date": "2021-12-17",
            "manifest_unchanged_hash": "6400ca22b963599af6bad9db030fe11a",
            "manifest_transform_hash": "9ab1f7c9eb0adbdaf07baaf8b97a110e",
            "table_bytes": {
                "Author": 3965,
                "Concept": 3947,
                "Institution": 3259,
                "Venue": 2108,
                "Work": 12657,
            },
        }
        self.second_run = {
            "execution_date": pendulum.datetime(year=2022, month=1, day=9),
            "manifest_date": "2022-01-17",
            "manifest_unchanged_hash": "50e2eff06007a32c4394df8df7f5e907",
            "manifest_transform_hash": "f4cea919d06caa0811ad5976bf98986a",
            "table_bytes": {
                "Author": 7930,
                "Concept": 7894,
                "Institution": 6518,
                "Venue": 4216,
                "Work": 25314,
            },
        }

        # API environment
        self.host = "localhost"
        self.port = find_free_port()
        configuration = Configuration(host=f"http://{self.host}:{self.port}")
        api_client = ApiClient(configuration)
        self.api = ObservatoryApi(api_client=api_client)  # noqa: E501
        self.env = ObservatoryApiEnvironment(host=self.host, port=self.port)
        self.org_name = "Curtin University"

    def setup_api(self):
        name = "OpenAlex Telescope"
        workflow_type = WorkflowType(name=name, type_id=OpenAlexTelescope.DAG_ID)
        self.api.put_workflow_type(workflow_type)

        organisation = Organisation(
            name="Curtin University",
            project_id="project",
            download_bucket="download_bucket",
            transform_bucket="transform_bucket",
        )
        self.api.put_organisation(organisation)

        telescope = Workflow(
            name=name,
            workflow_type=WorkflowType(id=1),
            organisation=Organisation(id=1),
            extra={},
        )
        self.api.put_workflow(telescope)

        table_type = TableType(
            type_id="partitioned",
            name="partitioned bq table",
        )
        self.api.put_table_type(table_type)

        dataset_type1 = DatasetType(
            type_id=DatasetTypeId.openalex,
            name="OpenAlex",
            extra={},
            table_type=TableType(id=1),
        )
        dataset_type2 = DatasetType(
            type_id=DatasetTypeId.openalex_institution,
            name="OpenAlex Institution",
            extra={},
            table_type=TableType(id=1),
        )
        dataset_type3 = DatasetType(
            type_id=DatasetTypeId.openalex_author,
            name="OpenAlex Author",
            extra={},
            table_type=TableType(id=1),
        )
        self.api.put_dataset_type(dataset_type1)
        self.api.put_dataset_type(dataset_type2)
        self.api.put_dataset_type(dataset_type3)

        dataset1 = Dataset(
            name="OpenAlex Dataset",
            address="project.dataset.table",
            service="bigquery",
            workflow=Workflow(id=1),
            dataset_type=DatasetType(id=1),
        )
        dataset2 = Dataset(
            name="OpenAlex Institution Dataset",
            address="project.dataset.table",
            service="bigquery",
            workflow=Workflow(id=1),
            dataset_type=DatasetType(id=2),
        )
        dataset3 = Dataset(
            name="OpenAlex Author Dataset",
            address="project.dataset.table",
            service="bigquery",
            workflow=Workflow(id=1),
            dataset_type=DatasetType(id=3),
        )
        self.api.put_dataset(dataset1)
        self.api.put_dataset(dataset2)
        self.api.put_dataset(dataset3)

    def setup_connections(self, env):
        # Add Observatory API connection
        conn = Connection(conn_id=AirflowConns.OBSERVATORY_API, uri=f"http://:password@{self.host}:{self.port}")
        env.add_connection(conn)

    def test_dag_structure(self):
        """Test that the OpenAlex DAG has the correct structure.
        :return: None
        """

        dag = OpenAlexTelescope(workflow_id=0).make_dag()
        self.assert_dag_structure(
            {
                "check_dependencies": ["write_transfer_manifest"],
                "write_transfer_manifest": ["upload_transfer_manifest"],
                "upload_transfer_manifest": ["transfer"],
                "transfer": ["download_transferred"],
                "download_transferred": ["transform"],
                "transform": ["upload_transformed"],
                "upload_transformed": ["bq_append_new"],
                "bq_append_new": ["bq_delete_old"],
                "bq_delete_old": ["bq_create_snapshot"],
                "bq_create_snapshot": ["add_new_dataset_releases"],
                "add_new_dataset_releases": [],
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
            dag_file = os.path.join(module_file_path("academic_observatory_workflows.dags"), "openalex_telescope.py")
            self.assert_dag_load("openalex", dag_file)

    @patch("observatory.platform.utils.release_utils.make_observatory_api")
    @patch("academic_observatory_workflows.workflows.openalex_telescope.aws_to_google_cloud_storage_transfer")
    @patch("academic_observatory_workflows.workflows.openalex_telescope.boto3.client")
    def test_telescope(self, mock_client, mock_transfer, m_makeapi):
        """Test the OpenAlex telescope end to end.
        :return: None.
        """
        m_makeapi.return_value = self.api

        # Setup Observatory environment
        env = ObservatoryEnvironment(self.project_id, self.data_location, api_host=self.host, api_port=self.port)
        dataset_id = env.add_dataset()

        # Setup Telescope
        telescope = OpenAlexTelescope(dataset_id=dataset_id, workflow_id=1)
        dag = telescope.make_dag()

        # Create the Observatory environment and run tests
        with env.create():
            self.setup_api()

            # Add connection
            conn = Connection(
                conn_id=OpenAlexTelescope.AIRFLOW_CONN_AWS, uri="aws://UWLA41aAhdja:AJLD91saAJSKAL0AjAhkaka@"
            )
            env.add_connection(conn)

            run = self.first_run
            with env.create_dag_run(dag, run["execution_date"]):
                # Test that all dependencies are specified: no error should be thrown
                env.run_task(telescope.check_dependencies.__name__)
                start_date, end_date, first_release = telescope.get_release_info(
                    dag=dag,
                    data_interval_end=pendulum.datetime(year=2021, month=12, day=26),
                )
                self.assertEqual(dag.default_args["start_date"], start_date)
                self.assertEqual(pendulum.datetime(year=2021, month=12, day=26), end_date)
                self.assertTrue(first_release)

                # Use release info for other tasks
                release = OpenAlexRelease(
                    telescope.dag_id,
                    telescope.workflow_id,
                    telescope.dataset_type_id,
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
                    release.transfer_manifest_path_unchanged, run["manifest_unchanged_hash"], "md5"
                )
                self.assert_file_integrity(
                    release.transfer_manifest_path_transform, run["manifest_transform_hash"], "md5"
                )

                # Test upload transfer manifest task
                env.run_task(telescope.upload_transfer_manifest.__name__)
                self.assert_blob_integrity(
                    env.download_bucket,
                    release.transfer_manifest_blob_unchanged,
                    release.transfer_manifest_path_unchanged,
                )
                self.assert_blob_integrity(
                    env.download_bucket,
                    release.transfer_manifest_blob_transform,
                    release.transfer_manifest_path_transform,
                )

                # Test transfer task
                mock_transfer.reset_mock()
                mock_transfer.return_value = True, 2
                env.run_task(telescope.transfer.__name__)
                self.assertEqual(3, mock_transfer.call_count)
                try:
                    self.assertTupleEqual(mock_transfer.call_args_list[0][0], (conn.login, conn.password))
                    self.assertTupleEqual(mock_transfer.call_args_list[1][0], (conn.login, conn.password))
                    self.assertTupleEqual(mock_transfer.call_args_list[2][0], (conn.login, conn.password))
                except AssertionError:
                    raise AssertionError("AWS key id and secret not passed correctly to transfer function")
                self.assertDictEqual(
                    mock_transfer.call_args_list[0][1],
                    {
                        "aws_bucket": OpenAlexTelescope.AWS_BUCKET,
                        "include_prefixes": [],
                        "gc_project_id": self.project_id,
                        "gc_bucket": release.download_bucket,
                        "gc_bucket_path": f"telescopes/{release.dag_id}/{release.release_id}/unchanged/",
                        "description": f"Transfer OpenAlex data from Airflow telescope to {release.download_bucket}",
                        "transfer_manifest": f"gs://{release.download_bucket}/{release.transfer_manifest_blob_unchanged}",
                    },
                )
                self.assertDictEqual(
                    mock_transfer.call_args_list[1][1],
                    {
                        "aws_bucket": OpenAlexTelescope.AWS_BUCKET,
                        "include_prefixes": [],
                        "gc_project_id": self.project_id,
                        "gc_bucket": release.transform_bucket,
                        "gc_bucket_path": f"telescopes/{release.dag_id}/{release.release_id}/",
                        "description": f"Transfer OpenAlex data from Airflow telescope to {release.transform_bucket}",
                        "transfer_manifest": f"gs://{release.download_bucket}/{release.transfer_manifest_blob_unchanged}",
                    },
                )
                self.assertDictEqual(
                    mock_transfer.call_args_list[2][1],
                    {
                        "aws_bucket": OpenAlexTelescope.AWS_BUCKET,
                        "include_prefixes": [],
                        "gc_project_id": self.project_id,
                        "gc_bucket": release.download_bucket,
                        "gc_bucket_path": f"telescopes/{release.dag_id}/{release.release_id}/transform/",
                        "description": f"Transfer OpenAlex data from Airflow telescope to {release.download_bucket}",
                        "transfer_manifest": f"gs://{release.download_bucket}/{release.transfer_manifest_blob_transform}",
                    },
                )

                # Upload files to bucket, to mock transfer
                for entity, info in self.entities.items():
                    gzip_path = f"{entity}.jsonl.gz"
                    with open(info["download_path"], "rb") as f_in, gzip.open(gzip_path, "wb") as f_out:
                        f_out.writelines(f_in)

                    if entity == "authors" or entity == "venues":
                        download_blob = (
                            f"telescopes/{release.dag_id}/{release.release_id}/unchanged/"
                            f"data/{entity}/updated_date={run['manifest_date']}/0000_part_00.gz"
                        )
                        transform_blob = (
                            f"telescopes/{release.dag_id}/{release.release_id}/"
                            f"data/{entity}/updated_date={run['manifest_date']}/0000_part_00.gz"
                        )
                        upload_file_to_cloud_storage(release.download_bucket, download_blob, gzip_path)
                        upload_file_to_cloud_storage(release.transform_bucket, transform_blob, gzip_path)
                    else:
                        download_blob = (
                            f"telescopes/{release.dag_id}/{release.release_id}/transform/"
                            f"data/{entity}/updated_date={run['manifest_date']}/0000_part_00.gz"
                        )
                        upload_file_to_cloud_storage(release.download_bucket, download_blob, gzip_path)

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

                # Test append new creates table
                env.run_task(telescope.bq_append_new.__name__)
                for _, table, _ in bq_load_info:
                    table_id = f"{self.project_id}.{telescope.dataset_id}.{table}"
                    expected_bytes = run["table_bytes"][table]
                    self.assert_table_bytes(table_id, expected_bytes)

                # Test delete old task is skipped for the first release
                with patch("observatory.platform.utils.gc_utils.bq_query_bytes_daily_limit_check"):
                    ti = env.run_task(telescope.bq_delete_old.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # Test create bigquery snapshot
                ti = env.run_task(telescope.bq_create_snapshot.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # Test adding of dataset releases as well as cleanup
                download_folder, extract_folder, transform_folder = (
                    release.download_folder,
                    release.extract_folder,
                    release.transform_folder,
                )

                openalex_dataset_releases = get_dataset_releases(dataset_id=1)
                institution_dataset_releases = get_dataset_releases(dataset_id=2)
                author_dataset_releases = get_dataset_releases(dataset_id=3)
                self.assertListEqual([], openalex_dataset_releases)
                self.assertListEqual([], institution_dataset_releases)
                self.assertListEqual([], author_dataset_releases)

                ti = env.run_task(telescope.add_new_dataset_releases.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                openalex_dataset_releases = get_dataset_releases(dataset_id=1)
                institution_dataset_releases = get_dataset_releases(dataset_id=2)
                author_dataset_releases = get_dataset_releases(dataset_id=3)
                self.assertEqual(1, len(openalex_dataset_releases))
                self.assertEqual(1, len(institution_dataset_releases))
                self.assertEqual(1, len(author_dataset_releases))
                self.assertEqual(release.end_date, openalex_dataset_releases[0].end_date)
                self.assertEqual(
                    pendulum.from_format("2021-12-17", "YYYY-MM-DD"), institution_dataset_releases[0].end_date
                )

                self.assert_cleanup(download_folder, extract_folder, transform_folder)

            run = self.second_run
            with env.create_dag_run(dag, run["execution_date"]) as dag_run:
                # Test that all dependencies are specified: no error should be thrown
                env.run_task(telescope.check_dependencies.__name__)
                start_date, end_date, first_release = telescope.get_release_info(
                    dag=dag,
                    data_interval_end=pendulum.datetime(year=2022, month=1, day=2),
                )
                self.assertEqual(release.end_date, start_date)
                self.assertEqual(pendulum.datetime(year=2022, month=1, day=2), end_date)
                self.assertFalse(first_release)

                # Use release info for other tasks
                release = OpenAlexRelease(
                    telescope.dag_id,
                    telescope.workflow_id,
                    telescope.dataset_type_id,
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
                    release.transfer_manifest_path_unchanged, run["manifest_unchanged_hash"], "md5"
                )
                self.assert_file_integrity(
                    release.transfer_manifest_path_transform, run["manifest_transform_hash"], "md5"
                )

                # Test upload transfer manifest task
                env.run_task(telescope.upload_transfer_manifest.__name__)
                self.assert_blob_integrity(
                    env.download_bucket,
                    release.transfer_manifest_blob_unchanged,
                    release.transfer_manifest_path_unchanged,
                )
                self.assert_blob_integrity(
                    env.download_bucket,
                    release.transfer_manifest_blob_transform,
                    release.transfer_manifest_path_transform,
                )

                # Test transfer task
                mock_transfer.reset_mock()
                mock_transfer.return_value = True, 2
                env.run_task(telescope.transfer.__name__)
                self.assertEqual(3, mock_transfer.call_count)
                try:
                    self.assertTupleEqual(mock_transfer.call_args_list[0][0], (conn.login, conn.password))
                    self.assertTupleEqual(mock_transfer.call_args_list[1][0], (conn.login, conn.password))
                    self.assertTupleEqual(mock_transfer.call_args_list[2][0], (conn.login, conn.password))
                except AssertionError:
                    raise AssertionError("AWS key id and secret not passed correctly to transfer function")

                self.assertDictEqual(
                    mock_transfer.call_args_list[0][1],
                    {
                        "aws_bucket": OpenAlexTelescope.AWS_BUCKET,
                        "include_prefixes": [],
                        "gc_project_id": self.project_id,
                        "gc_bucket": release.download_bucket,
                        "gc_bucket_path": f"telescopes/{release.dag_id}/{release.release_id}/unchanged/",
                        "description": f"Transfer OpenAlex data from Airflow telescope to {release.download_bucket}",
                        "transfer_manifest": f"gs://{release.download_bucket}/{release.transfer_manifest_blob_unchanged}",
                    },
                )
                self.assertDictEqual(
                    mock_transfer.call_args_list[1][1],
                    {
                        "aws_bucket": OpenAlexTelescope.AWS_BUCKET,
                        "include_prefixes": [],
                        "gc_project_id": self.project_id,
                        "gc_bucket": release.transform_bucket,
                        "gc_bucket_path": f"telescopes/{release.dag_id}/{release.release_id}/",
                        "description": f"Transfer OpenAlex data from Airflow telescope to {release.transform_bucket}",
                        "transfer_manifest": f"gs://{release.download_bucket}/{release.transfer_manifest_blob_unchanged}",
                    },
                )
                self.assertDictEqual(
                    mock_transfer.call_args_list[2][1],
                    {
                        "aws_bucket": OpenAlexTelescope.AWS_BUCKET,
                        "include_prefixes": [],
                        "gc_project_id": self.project_id,
                        "gc_bucket": release.download_bucket,
                        "gc_bucket_path": f"telescopes/{release.dag_id}/{release.release_id}/transform/",
                        "description": f"Transfer OpenAlex data from Airflow telescope to {release.download_bucket}",
                        "transfer_manifest": f"gs://{release.download_bucket}/{release.transfer_manifest_blob_transform}",
                    },
                )

                # Upload files to bucket, to mock transfer
                for entity, info in self.entities.items():
                    gzip_path = f"{entity}.jsonl.gz"
                    with open(info["download_path"], "rb") as f_in, gzip.open(gzip_path, "wb") as f_out:
                        f_out.writelines(f_in)

                    if entity == "authors" or entity == "venues":
                        download_blob = (
                            f"telescopes/{release.dag_id}/{release.release_id}/unchanged/"
                            f"data/{entity}/updated_date={run['manifest_date']}/0000_part_00.gz"
                        )
                        transform_blob = (
                            f"telescopes/{release.dag_id}/{release.release_id}/"
                            f"data/{entity}/updated_date={run['manifest_date']}/0000_part_00.gz"
                        )
                        upload_file_to_cloud_storage(release.download_bucket, download_blob, gzip_path)
                        upload_file_to_cloud_storage(release.transform_bucket, transform_blob, gzip_path)
                    else:
                        download_blob = (
                            f"telescopes/{release.dag_id}/{release.release_id}/transform/"
                            f"data/{entity}/updated_date={run['manifest_date']}/0000_part_00.gz"
                        )
                        upload_file_to_cloud_storage(release.download_bucket, download_blob, gzip_path)

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

                # Test append new creates table
                env.run_task(telescope.bq_append_new.__name__)
                for _, table, _ in bq_load_info:
                    table_id = f"{self.project_id}.{telescope.dataset_id}.{table}"
                    expected_bytes = run["table_bytes"][table]
                    self.assert_table_bytes(table_id, expected_bytes)

                # Test that old rows are deleted from table
                with patch("observatory.platform.utils.gc_utils.bq_query_bytes_daily_limit_check"):
                    env.run_task(telescope.bq_delete_old.__name__)
                for _, table, _ in bq_load_info:
                    table_id = f"{self.project_id}.{telescope.dataset_id}.{table}"
                    expected_bytes = self.first_run["table_bytes"][table]
                    self.assert_table_bytes(table_id, expected_bytes)

                # Test create bigquery snapshot
                ti = env.run_task(telescope.bq_create_snapshot.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # Test adding of dataset releases and cleanup of local files
                download_folder, extract_folder, transform_folder = (
                    release.download_folder,
                    release.extract_folder,
                    release.transform_folder,
                )

                openalex_dataset_releases = get_dataset_releases(dataset_id=1)
                institution_dataset_releases = get_dataset_releases(dataset_id=2)
                author_dataset_releases = get_dataset_releases(dataset_id=3)
                self.assertEqual(1, len(openalex_dataset_releases))
                self.assertEqual(1, len(institution_dataset_releases))
                self.assertEqual(1, len(author_dataset_releases))

                ti = env.run_task(telescope.add_new_dataset_releases.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                openalex_dataset_releases = get_dataset_releases(dataset_id=1)
                institution_dataset_releases = get_dataset_releases(dataset_id=2)
                author_dataset_releases = get_dataset_releases(dataset_id=3)
                self.assertEqual(2, len(openalex_dataset_releases))
                self.assertEqual(2, len(institution_dataset_releases))
                self.assertEqual(2, len(author_dataset_releases))
                self.assertEqual(release.end_date, openalex_dataset_releases[1].end_date)
                self.assertEqual(
                    pendulum.from_format("2022-1-17", "YYYY-MM-DD"), institution_dataset_releases[1].end_date
                )

                self.assert_cleanup(download_folder, extract_folder, transform_folder)

    @patch("academic_observatory_workflows.workflows.openalex_telescope.get_dataset_releases")
    @patch("academic_observatory_workflows.workflows.openalex_telescope.get_datasets")
    @patch("academic_observatory_workflows.workflows.openalex_telescope.boto3.client")
    @patch("academic_observatory_workflows.workflows.openalex_telescope.get_aws_conn_info")
    @patch("academic_observatory_workflows.workflows.openalex_telescope.Variable.get")
    def test_write_transfer_manifest(
        self, mock_variable_get, mock_aws_info, mock_boto3, mock_get_datasets, mock_get_releases
    ):
        """Test write_transfer_manifest method of the OpenAlex release.

        :param mock_variable_get: Mock Airflow Variable get() method
        :param mock_boto3: Mock the boto3 client
        :return: None.
        """
        mock_get_datasets.return_value = [
            SimpleNamespace(name="OpenAlex Dataset", dataset_type=SimpleNamespace(type_id="openalex"), id=1),
            SimpleNamespace(
                name="OpenAlex Author Dataset", dataset_type=SimpleNamespace(type_id="openalex_author"), id=2
            ),
            SimpleNamespace(
                name="OpenAlex Concept Dataset", dataset_type=SimpleNamespace(type_id="openalex_concept"), id=3
            ),
            SimpleNamespace(
                name="OpenAlex Institution Dataset", dataset_type=SimpleNamespace(type_id="openalex_institution"), id=4
            ),
            SimpleNamespace(
                name="OpenAlex Venue Dataset", dataset_type=SimpleNamespace(type_id="openalex_venue"), id=5
            ),
            SimpleNamespace(name="OpenAlex Work Dataset", dataset_type=SimpleNamespace(type_id="openalex_work"), id=6),
        ]
        mock_variable_get.return_value = "data"
        mock_aws_info.return_value = "key_id", "secret_key"
        # Mock response of get_object on last_modified file, mocking lambda file
        side_effect = []
        for tests in range(2):
            for entity in self.entities:
                manifest_content = render_template(self.manifest_obj_path, entity=entity, date="2022-01-01").encode()
                side_effect.append({"Body": StreamingBody(io.BytesIO(manifest_content), len(manifest_content))})
        mock_boto3().get_object.side_effect = side_effect

        start_date = pendulum.DateTime(2022, 1, 1, tzinfo=pendulum.tz.UTC)
        end_date = pendulum.DateTime(2022, 1, 1, tzinfo=pendulum.tz.UTC)

        with CliRunner().isolated_filesystem():
            # Test with entries in manifest objects that are after end date of latest release
            mock_get_releases.return_value = [{"end_date": datetime.datetime(2021, 1, 1, tzinfo=tz.UTC)}]
            release = OpenAlexRelease("dag_id", 1, OpenAlexTelescope.DAG_ID, start_date, end_date, False, 1)

            release.write_transfer_manifest()
            self.assert_file_integrity(
                release.transfer_manifest_path_unchanged, "fe8442cd31fec1c335379033afebc1ea", "md5"
            ),
            self.assert_file_integrity(
                release.transfer_manifest_path_transform, "42fb45119bd34709001fd6c90a6ef60e", "md5"
            )

            # Test with entries in manifest objects that are before end date of latest release
            mock_get_releases.return_value = [{"end_date": datetime.datetime(2022, 2, 1, tzinfo=tz.UTC)}]
            release = OpenAlexRelease("dag_id", 1, OpenAlexTelescope.DAG_ID, start_date, end_date, False, 1)

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
            release = OpenAlexRelease("dag_id", 1, "dataset_type_id", start_date, end_date, False, 1)

            # Create transfer manifest files
            with open(release.transfer_manifest_path_unchanged, "w") as f:
                f.write('"prefix1"\n"prefix2"\n')
            with open(release.transfer_manifest_path_transform, "w") as f:
                f.write("")

            # Test successful transfer with prefixes for unchanged, no prefixes for transform
            release.transfer(max_retries=1)
            self.assertEqual(3, len(mock_transfer.call_args_list))
            self.assertDictEqual(
                {
                    "aws_bucket": OpenAlexTelescope.AWS_BUCKET,
                    "include_prefixes": [],
                    "gc_project_id": "project_id",
                    "gc_bucket": "download-bucket",
                    "gc_bucket_path": "telescopes/dag_id/2022_01_01-2022_02_01/unchanged/",
                    "description": "Transfer OpenAlex data from Airflow telescope to download-bucket",
                    "transfer_manifest": "gs://download-bucket/telescopes/dag_id/2022_01_01-2022_02_01/transfer_manifest_unchanged.csv",
                },
                mock_transfer.call_args_list[0][1],
            )
            self.assertDictEqual(
                {
                    "aws_bucket": OpenAlexTelescope.AWS_BUCKET,
                    "include_prefixes": [],
                    "gc_project_id": "project_id",
                    "gc_bucket": "transform-bucket",
                    "gc_bucket_path": "telescopes/dag_id/2022_01_01-2022_02_01/",
                    "description": "Transfer OpenAlex data from Airflow telescope to transform-bucket",
                    "transfer_manifest": "gs://download-bucket/telescopes/dag_id/2022_01_01-2022_02_01/transfer_manifest_unchanged.csv",
                },
                mock_transfer.call_args_list[1][1],
            )
            self.assertDictEqual(
                {
                    "aws_bucket": OpenAlexTelescope.AWS_BUCKET,
                    "include_prefixes": [],
                    "gc_project_id": "project_id",
                    "gc_bucket": "download-bucket",
                    "gc_bucket_path": "telescopes/dag_id/2022_01_01-2022_02_01/transform/",
                    "description": "Transfer OpenAlex data from Airflow telescope to download-bucket",
                    "transfer_manifest": "gs://download-bucket/telescopes/dag_id/2022_01_01-2022_02_01/transfer_manifest_transform.csv",
                },
                mock_transfer.call_args_list[2][1],
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
