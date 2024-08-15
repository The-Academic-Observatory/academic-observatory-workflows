import csv
from typing import List
import os
import re
from functools import cached_property

from observatory_platform.files import list_files
from observatory_platform.google.gcs import gcs_blob_uri

BATCH_REGEX = r"^\d{2}(\d|X)$"
ORCID_RECORD_REGEX = r"\d{4}-\d{4}-\d{4}-\d{3}(\d|X)\.xml$"


class OrcidBatch:
    """Describes a single ORCID batch and its related files/folders"""

    def __init__(self, download_dir: str, transform_dir: str, batch_str: str):
        self.download_dir = download_dir
        self.transform_dir = transform_dir
        self.batch_str = batch_str
        self.download_batch_dir = os.path.join(self.download_dir, batch_str)
        self.download_log_file = os.path.join(self.download_dir, f"{self.batch_str}_log.txt")
        self.download_error_file = os.path.join(self.download_dir, f"{self.batch_str}_error.txt")
        self.manifest_file = os.path.join(self.download_dir, f"{self.batch_str}_manifest.csv")
        self.transform_upsert_file = os.path.join(self.transform_dir, f"{self.batch_str}_upsert.jsonl.gz")
        self.transform_delete_file = os.path.join(self.transform_dir, f"{self.batch_str}_delete.jsonl.gz")

        if not os.path.exists(self.download_dir):
            raise NotADirectoryError(f"Directory {self.download_dir} does not exist.")
        if not os.path.exists(self.transform_dir):
            raise NotADirectoryError(f"Directory {self.transform_dir} does not exist.")
        if not re.match(BATCH_REGEX, self.batch_str):
            raise ValueError(f"Batch string {self.batch_str} is not valid.")

        os.makedirs(self.download_batch_dir, exist_ok=True)

    @property
    def existing_records(self) -> List[str]:
        """List of existing ORCID records on disk for this ORCID directory."""
        return [os.path.basename(path) for path in list_files(self.download_batch_dir, ORCID_RECORD_REGEX)]

    @property
    def missing_records(self) -> List[str]:
        """List of missing ORCID records on disk for this ORCID directory."""
        return list(set(self.expected_records) - set(self.existing_records))

    @cached_property
    def expected_records(self) -> List[str]:
        """List of expected ORCID records for this ORCID directory. Derived from the manifest file"""
        with open(self.manifest_file, "r") as f:
            reader = csv.DictReader(f)
            return [os.path.basename(row["blob_name"]) for row in reader]

    @cached_property
    def blob_uris(self) -> List[str]:
        """List of blob URIs from the manifest this ORCID directory."""
        with open(self.manifest_file, "r") as f:
            reader = csv.DictReader(f)
            return [gcs_blob_uri(bucket_name=row["bucket_name"], blob_name=row["blob_name"]) for row in reader]
