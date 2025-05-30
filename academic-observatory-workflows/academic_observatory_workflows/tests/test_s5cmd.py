# Generated by CodiumAI

import os
import unittest
from tempfile import NamedTemporaryFile, TemporaryDirectory

from google.auth import default

from academic_observatory_workflows.s5cmd import S5Cmd, S5CmdCpConfig
from observatory_platform.files import list_files
from observatory_platform.google.gcs import gcs_blob_uri, gcs_hmac_key, gcs_list_blobs, gcs_upload_files
from observatory_platform.sandbox.sandbox_environment import SandboxEnvironment
from observatory_platform.sandbox.test_utils import SandboxTestCase


class TestS5CmdCpConfig(unittest.TestCase):
    def test_str(self):
        cfg = S5CmdCpConfig()
        self.assertEqual(str(cfg), "")

        cfg = S5CmdCpConfig(flatten_dir=True, no_overwrite=True, overwrite_if_size=True, overwrite_if_newer=True)
        self.assertEqual(str(cfg), "--flatten --no-clobber --if-size-differ --if-source-newer")


class TestS5Cmd(SandboxTestCase):
    """Tests for the ORCID telescope"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")
        self.sa_email = default()[0].service_account_email

    def test_download_from_bucket(self):
        """Tests that files can be downloaded from a bucket"""
        env = SandboxEnvironment(self.project_id, self.data_location)
        bucket = env.add_bucket()

        with env.create():
            # Upload some files to the bucket
            uploaded_files = []
            blob_uris = []
            for _ in range(3):
                with NamedTemporaryFile(delete=False) as tmp:
                    with open(tmp.name, "w"):
                        pass
                    gcs_upload_files(bucket_name=bucket, file_paths=[tmp.name], blob_names=[os.path.basename(tmp.name)])
                    uploaded_files.append(os.path.basename(tmp.name))
                    blob_uris.append(gcs_blob_uri(bucket_name=bucket, blob_name=os.path.basename(tmp.name)))

            # Do the download
            with gcs_hmac_key(self.project_id, self.sa_email) as (key, secret), TemporaryDirectory() as tmp_dir:
                s5cmd = S5Cmd(access_credentials=(key.access_id, secret))
                _, _, returncode = s5cmd.download_from_bucket(blob_uris, tmp_dir)

                # Assert that the files were downloaded successfully
                downloaded_files = list_files(tmp_dir)
                self.assertEqual(0, returncode)
                self.assertEqual(len(downloaded_files), len(uploaded_files))
                self.assertEqual(set([os.path.basename(f) for f in downloaded_files]), set(uploaded_files))

    def test_download_failures(self):
        """Tests that errors are raised when download could not be performed"""
        env = SandboxEnvironment(self.project_id, self.data_location)
        bucket = env.add_bucket()

        with env.create():
            # Upload some files to the bucket
            uploaded_files = []
            blob_uris = []
            for _ in range(3):
                with NamedTemporaryFile(delete=False) as tmp:
                    with open(tmp.name, "w"):
                        pass
                    gcs_upload_files(bucket_name=bucket, file_paths=[tmp.name], blob_names=[os.path.basename(tmp.name)])
                    uploaded_files.append(os.path.basename(tmp.name))
                    blob_uris.append(gcs_blob_uri(bucket_name=bucket, blob_name=os.path.basename(tmp.name)))

            with gcs_hmac_key(self.project_id, self.sa_email) as (key, secret), TemporaryDirectory() as tmp_dir:
                # URIs do not match the regex pattern
                unmatched_uris = [uri.replace("gs://", "www.") for uri in blob_uris]
                s5cmd = S5Cmd(access_credentials=(key.access_id, secret))
                with self.assertRaisesRegex(ValueError, "All URIs must begin with a qualified bucket prefix."):
                    s5cmd.download_from_bucket(unmatched_uris, tmp_dir)

                # URIs do not begin with the same prefix
                mixed_uris = blob_uris.copy()
                mixed_uris[0] = mixed_uris[0].replace("gs://", "s3://")
                with self.assertRaisesRegex(ValueError, "All URIs must begin with the same prefix."):
                    s5cmd.download_from_bucket(mixed_uris, tmp_dir)

                # URIs are not one of gs:// or s3://
                faulty_uris = [uri.replace("gs://", "ab://") for uri in blob_uris]
                with self.assertRaisesRegex(ValueError, "Only gs:// and s3:// URIs are supported."):
                    s5cmd.download_from_bucket(faulty_uris, tmp_dir)

                # Download a file that doesn't exist
                blob_uri = gcs_blob_uri(bucket_name=bucket, blob_name="does_not_exist.txt")
                _, _, returncode = s5cmd.download_from_bucket(blob_uri, tmp_dir)
                self.assertGreater(returncode, 0)

    def test_upload_to_bucket(self):
        """Tests that files can be uploaded to a bucket"""
        env = SandboxEnvironment(self.project_id, self.data_location)
        bucket = env.add_bucket()

        # Upload some files to the bucket
        with env.create():
            # Upload some files to the bucket
            local_files = []
            blob_uris = []
            for _ in range(3):
                with NamedTemporaryFile(delete=False) as tmp:
                    with open(tmp.name, "w"):
                        pass
                    local_files.append(tmp.name)
                    blob_uris.append(gcs_blob_uri(bucket_name=bucket, blob_name=os.path.basename(tmp.name)))

            with gcs_hmac_key(self.project_id, self.sa_email) as (key, secret), TemporaryDirectory() as tmp_dir:
                s5cmd = S5Cmd(access_credentials=(key.access_id, secret))
                bucket_uri = f"gs://{bucket}"
                _, _, returncode = s5cmd.upload_to_bucket(local_files, bucket_uri)

                # Assert that the files were uploaded successfully
                self.assertEqual(returncode, 0)
                uploaded_blobs = gcs_list_blobs(bucket_name=bucket)
                blob_names = [
                    gcs_blob_uri(bucket_name=blob.bucket.name, blob_name=blob.name) for blob in uploaded_blobs
                ]
                self.assertEqual(set(blob_names), set(blob_uris))

    def test_upload_failures(self):
        """Tests that errors are raised when upload could not be performed"""

        env = SandboxEnvironment(self.project_id, self.data_location)
        bucket = env.add_bucket()

        with env.create():
            # Check that value error is raised for bad bucket URI
            with gcs_hmac_key(self.project_id, self.sa_email) as (key, secret), TemporaryDirectory() as tmp_dir:
                s5cmd = S5Cmd(access_credentials=(key.access_id, secret))
                with self.assertRaisesRegex(ValueError, "Only gs:// and s3://"):
                    s5cmd.upload_to_bucket(files=[], bucket_uri="ab://")

                # Upload a file that does not exist
                _, _, returncode = s5cmd.upload_to_bucket(files=["does_not_exist.txt"], bucket_uri=f"gs://{bucket}")
                self.assertGreater(returncode, 0)
