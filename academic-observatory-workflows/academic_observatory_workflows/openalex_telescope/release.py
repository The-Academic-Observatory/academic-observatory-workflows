# Copyright 2022-2025 Curtin University
# Copyright 2024-2025 UC Curation Center (California Digital Library)
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

from __future__ import annotations

import os
import re
from typing import Dict, List, Tuple

import pendulum

import observatory_platform.google.bigquery as bq
from observatory_platform.airflow.release import SnapshotRelease
from observatory_platform.airflow.workflow import CloudWorkspace
from observatory_platform.google.gcs import gcs_blob_name_from_path, gcs_blob_uri


class OpenAlexEntity(SnapshotRelease):
    def __init__(
        self,
        *,
        dag_id: str,
        run_id: str,
        cloud_workspace: CloudWorkspace,
        entity_name: str,
        bq_dataset_id: str,
        schema_folder: str,
        snapshot_date: pendulum.DateTime,
        manifest: Manifest,
        merged_ids: List[MergedId],
        is_first_run: bool,
    ):
        """This class represents the data and settings related to an OpenAlex entity or table.

        :param dag_id: the DAG ID.
        :param run_id: the DAG's run ID.
        :param cloud_workspace: the CloudWorkspace instance.
        :param entity_name: the name of the entity, e.g. authors, institutions etc.
        :param bq_dataset_id: the BigQuery dataset id.
        :param schema_folder: the path to the schema folder.
        :param snapshot_date: the OpenAlex snapshot date.
        :param manifest: the Redshift manifest provided by OpenAlex for this entity.
        :param merged_ids: the MergedIds provided by OpenAlex for this entity.
        :param is_first_run: whether this is the first run or not.
        """

        super().__init__(
            dag_id=dag_id,
            run_id=run_id,
            snapshot_date=snapshot_date,
        )
        self.cloud_workspace = cloud_workspace
        self.entity_name = entity_name
        self.bq_dataset_id = bq_dataset_id
        self.schema_folder = schema_folder
        self.manifest = manifest
        self.merged_ids = merged_ids
        self.is_first_run = is_first_run
        self.transfer_manifest_uri = gcs_blob_uri(
            cloud_workspace.download_bucket,
            f"{gcs_blob_name_from_path(self.download_folder)}/{entity_name}-manifest.csv",
        )
        self.gcs_openalex_data_uri = (
            f"gs://{cloud_workspace.download_bucket}/{gcs_blob_name_from_path(self.download_folder)}/"
        )
        self.log_path = os.path.join(self.download_folder, "gsutil.log")

    @property
    def table_description(self):
        return f"OpenAlex {self.entity_name} table: https://docs.openalex.org/api-entities/{self.entity_name}"

    @property
    def schema_file_path(self):
        return os.path.join(self.schema_folder, f"{self.entity_name}.json")

    @property
    def generated_schema_path(self):
        return os.path.join(self.transform_folder, f"generated_schema_{self.entity_name}.json")

    @property
    def data_uri(self):
        return gcs_blob_uri(
            self.cloud_workspace.transform_bucket,
            f"{gcs_blob_name_from_path(self.transform_folder)}/data/{self.entity_name}/*",
        )

    @property
    def bq_table_id(self):
        return bq.bq_sharded_table_id(
            self.cloud_workspace.output_project_id, self.bq_dataset_id, self.entity_name, self.snapshot_date
        )

    @property
    def entries(self):
        return [entry for entry in self.manifest.entries]

    @staticmethod
    def from_dict(dict_: dict) -> OpenAlexEntity:
        return OpenAlexEntity(
            dag_id=dict_["dag_id"],
            run_id=dict_["run_id"],
            cloud_workspace=CloudWorkspace.from_dict(dict_["cloud_workspace"]),
            entity_name=dict_["entity_name"],
            bq_dataset_id=dict_["bq_dataset_id"],
            schema_folder=dict_["schema_folder"],
            snapshot_date=pendulum.parse(dict_["snapshot_date"]),
            manifest=Manifest.from_dict(dict_["manifest"]),
            merged_ids=[MergedId.from_dict(merged_id) for merged_id in dict_["merged_ids"]],
            is_first_run=dict_["is_first_run"],
        )

    def to_dict(self) -> dict:
        return dict(
            dag_id=self.dag_id,
            run_id=self.run_id,
            cloud_workspace=self.cloud_workspace.to_dict(),
            entity_name=self.entity_name,
            bq_dataset_id=self.bq_dataset_id,
            schema_folder=self.schema_folder,
            snapshot_date=self.snapshot_date.isoformat(),
            manifest=self.manifest.to_dict(),
            merged_ids=[merged_id.to_dict() for merged_id in self.merged_ids],
            is_first_run=self.is_first_run,
        )


def s3_uri_parts(s3_uri: str) -> Tuple[str, str]:
    """Extracts the S3 bucket name and object key from the given S3 URI.

    :param s3_uri: str, S3 URI in format s3://mybucketname/path/to/object
    :return: tuple, (bucket_name, object_key)
    """

    if not s3_uri.startswith("s3://"):
        raise ValueError(f"Invalid S3 URI. URI should start with 's3://' - {s3_uri}")

    parts = s3_uri[5:].split("/", 1)  # Remove 's3://' and split the remaining string
    bucket_name = parts[0]
    object_key = parts[1] if len(parts) > 1 else None

    return bucket_name, object_key


class Manifest:
    def __init__(self, entries: List[ManifestEntry], meta: Meta):
        """An OpenAlex Entity Manifest file in Redshift Manifest format. It lists all the data files for each
        entity. See:

        * https://docs.openalex.org/download-all-data/snapshot-data-format
        * https://docs.aws.amazon.com/redshift/latest/dg/loading-data-files-using-manifest.html

        :param entries: a list of Manifest entries.
        :param meta: TODO not sure?
        """

        self.entries = entries
        self.meta = meta

    def __eq__(self, other):
        if isinstance(other, Manifest):
            return self.meta == other.meta and len(self.entries) == len(other.entries) and self.entries == other.entries
        return False

    @staticmethod
    def from_dict(dict_: Dict) -> Manifest:
        entries = [ManifestEntry.from_dict(entry) for entry in dict_["entries"]]
        meta = Meta.from_dict(dict_["meta"])
        return Manifest(entries, meta)

    def to_dict(self) -> Dict:
        return dict(entries=[entry.to_dict() for entry in self.entries], meta=self.meta.to_dict())


class Meta:
    def __init__(self, content_length, record_count):
        """Metadata about a file referenced in an OpenAlex Entity Manifest file. See:

        * https://docs.openalex.org/download-all-data/snapshot-data-format
        * https://docs.aws.amazon.com/redshift/latest/dg/loading-data-files-using-manifest.html

        :param content_length: size of the file in bytes.
        :param record_count: how many records in the file.
        """

        self.content_length = content_length
        self.record_count = record_count

    def __eq__(self, other):
        if isinstance(other, Meta):
            return self.content_length == other.content_length and self.record_count == other.record_count
        return False

    @staticmethod
    def from_dict(dict_: Dict) -> Meta:
        content_length = dict_["content_length"]
        record_count = dict_["record_count"]
        return Meta(content_length, record_count)

    def to_dict(self) -> Dict:
        return dict(content_length=self.content_length, record_count=self.record_count)


class ManifestEntry:
    def __init__(self, url: str, meta: Meta):
        """An entry in an OpenAlex Entity Manifest file, containing a URL of the file on an AWS S3
        bucket and metadata about the file (size in bytes and number of records).

        :param url: URL of the file resides on an AWS S3 bucket.
        :param meta: metadata about the file, including size in bytes and number of records.
        """

        # URLs given from OpenAlex may not be given with the 's3://' prefix.
        if not url.startswith("s3://"):
            self.url = f"s3://{url}"
        else:
            self.url = url
        self.meta = meta

    def __eq__(self, other):
        if isinstance(other, ManifestEntry):
            return self.url == other.url and self.meta == other.meta
        return False

    @property
    def object_key(self):
        bucket_name, object_key = s3_uri_parts(self.url)
        if object_key is None:
            raise ValueError(f"object_key for url={self.url} is None")
        return object_key

    @property
    def updated_date(self) -> pendulum.DateTime:
        return pendulum.parse(re.search(r"updated_date=(\d{4}-\d{2}-\d{2})", self.url).group(1))

    @property
    def file_name(self):
        return re.search(r"part_\d+\.gz", self.url).group(0)

    @staticmethod
    def from_dict(dict_: Dict) -> ManifestEntry:
        url = dict_["url"]
        meta = Meta.from_dict(dict_["meta"])
        return ManifestEntry(url, meta)

    def to_dict(self) -> Dict:
        return dict(url=self.url, meta=self.meta.to_dict())


class MergedId:
    def __init__(self, url: str, content_length: int):
        """A pointer to an OpenAlex Merged ID file. See:

        * https://docs.openalex.org/download-all-data/snapshot-data-format#merged-entities

        :param url: the path to the Merged ID file on an AWS bucket.
        :param content_length: size of the file in bytes.
        """

        self.url = url
        self.content_length = content_length

    def __eq__(self, other):
        if isinstance(other, MergedId):
            return self.url == other.url and self.content_length == other.content_length
        return False

    @property
    def object_key(self):
        bucket_name, object_key = s3_uri_parts(self.url)
        if object_key is None:
            raise ValueError(f"object_key for url={self.url} is None")
        return object_key

    @property
    def updated_date(self) -> pendulum.DateTime:
        return pendulum.parse(re.search(r"\d{4}-\d{2}-\d{2}", self.url).group(0))

    @property
    def file_name(self):
        return re.search(r"[^/]+\.csv\.gz$", self.url).group(0)

    @staticmethod
    def from_dict(dict_: Dict) -> MergedId:
        url = dict_["url"]
        content_length = dict_["content_length"]
        return MergedId(url, content_length)

    def to_dict(self) -> Dict:
        return dict(url=self.url, content_length=self.content_length)
