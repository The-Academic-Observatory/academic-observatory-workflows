# Copyright 2023-2024 Curtin University
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

import logging
import re
import shlex
from contextlib import contextmanager
from subprocess import PIPE, Popen
from tempfile import NamedTemporaryFile
from typing import List, TextIO, Tuple, Union


class S5CmdCpConfig:
    """Configuration for S5Cmd cp command

    :param flatten_dir: Whether to flatten the directory structure
    :param no_overwrite: Whether to not overwrite files if they already exist
    :param overwrite_if_size: Whether to overwrite files only if source size differs
    :param overwrite_if_newer: Whether to overwrite files only if source is newer
    """

    def __init__(
        self,
        flatten_dir: bool = False,
        no_overwrite: bool = False,
        overwrite_if_size: bool = False,
        overwrite_if_newer: bool = False,
    ):
        self.flatten_dir = flatten_dir
        self.no_overwrite = no_overwrite
        self.overwrite_if_size = overwrite_if_size
        self.overwrite_if_newer = overwrite_if_newer

    def __str__(self):
        cfg = [
            "--flatten" if self.flatten_dir else "",
            "--no-clobber" if self.no_overwrite else "",
            "--if-size-differ" if self.overwrite_if_size else "",
            "--if-source-newer" if self.overwrite_if_newer else "",
        ]
        cfg = [i for i in cfg if i]  # Remove empty strings
        return " ".join(cfg)


class S5Cmd:
    def __init__(
        self,
        access_credentials: Tuple[str, str],
        out_stream: Union[None, int, TextIO] = PIPE,
    ):
        """S5Cmd class for executing s5cmd commands.

        :param access_credentials: Tuple of AWS (or aws-like) access credentials (key_id, secret_value)
        :param out_stream: Stream to write output to
        """
        self.access_credentials = access_credentials
        self.out_stream = out_stream
        self.uri_identifier_regex = r"^[a-zA-Z0-9_]{2}://"

    def _uri(self, uri: str):
        return uri.replace("gs://", "s3://")

    def _initialise_command(self, uri: str):
        """Initializes the command for the given bucket URI.
        :param uri: The URI being accessed.
        :return: The initialized command."""
        cmd = "s5cmd"

        # Check that the uri prefixes are supported
        uri_prefix = re.match(self.uri_identifier_regex, uri).group(0)
        if uri_prefix not in ["gs://", "s3://"]:
            raise ValueError(f"Only gs:// and s3:// URIs are supported. Found prefix: {uri_prefix}")

        # Amend the URIs with the s3:// prefix and add endpoint URL if required
        if uri_prefix == "gs://":
            cmd = " ".join([cmd, "--endpoint-url https://storage.googleapis.com"])

        return cmd

    @contextmanager
    def _bucket_credentials(self):
        try:
            with NamedTemporaryFile() as tmp:
                with open(tmp.name, "w") as f:
                    f.write("[default]\n")
                    f.write(f"aws_access_key_id = {self.access_credentials[0]}\n")
                    f.write(f"aws_secret_access_key = {self.access_credentials[1]}\n")
                yield tmp.name
        finally:
            pass

    def download_from_bucket(
        self, uris: Union[List[str], str], local_path: str, cp_config: S5CmdCpConfig = None
    ) -> Tuple[bytes, bytes, int]:
        """Downloads file(s) from a bucket using s5cmd and a supplied list of URIs.

        :param uris: The URI or list of URIs to download.
        :param local_path: The local path to download to.
        :param cp_config: Configuration for the s5cmd cp command.
        :return: A tuple of (stdout, stderr, s5cmd exit code).
        """
        if not isinstance(uris, list):
            uris = [uris]
        if not cp_config:
            cp_config = S5CmdCpConfig()

        # Check the integrity of the supplied URIs
        uri_prefixes = [re.match(self.uri_identifier_regex, uri) for uri in uris]
        if None in uri_prefixes:
            raise ValueError("All URIs must begin with a qualified bucket prefix.")
        uri_prefixes = [prefix.group() for prefix in uri_prefixes]
        if not len(set(uri_prefixes)) == 1:
            raise ValueError(f"All URIs must begin with the same prefix. Found prefixes: {set(uri_prefixes)}")
        uri_prefix = uri_prefixes[0]
        if uri_prefix not in ["gs://", "s3://"]:
            raise ValueError(f"Only gs:// and s3:// URIs are supported. Found prefix: {uri_prefix}")

        # Amend the URIs with the s3:// prefix and add endpoint URL if required
        cmd = self._initialise_command(uris[0])

        # Make the run commands
        blob_cmds = []
        for uri in map(self._uri, uris):
            blob_cmd = "cp"
            blob_cmd += f" {str(cp_config)}"
            blob_cmd += f" {uri} {local_path}"
            blob_cmds.append(blob_cmd)
        blob_stdin = "\n".join(blob_cmds)
        logging.info(f"s5cmd blob download command example: {blob_cmds[0]}")

        # Initialise credentials and execute
        with self._bucket_credentials() as credentials:
            cmd += f" --credentials-file {credentials} run"
            proc = Popen(shlex.split(cmd), stdout=self.out_stream, stderr=self.out_stream, stdin=PIPE)
            stdout, stderr = proc.communicate(input=blob_stdin.encode())
        returncode = proc.wait()
        if returncode > 0:
            logging.warning(f"s5cmd cp failed with return code {returncode}: {stderr}")
        return stdout, stderr, returncode

    def upload_to_bucket(
        self, files: Union[List[str], str], bucket_uri: str, cp_config: S5CmdCpConfig = None
    ) -> Tuple[bytes, bytes, int]:
        """Uploads file(s) to a bucket using s5cmd and a supplied list of local file paths.

        :param files: The file(s) to upload.
        :bucket_uri: The bucket URI to upload to.
        :param cp_config: Configuration for the s5cmd cp command.
        :return: A tuple of (stdout, stderr, s5cmd exit code).
        """
        if not isinstance(files, list):
            files = [files]
        if not cp_config:
            cp_config = S5CmdCpConfig()

        cmd = self._initialise_command(bucket_uri)

        blob_cmds = []
        for file in files:
            blob_cmd = "cp"
            blob_cmd += f" {str(cp_config)}"
            blob_cmd += f" {file} {self._uri(bucket_uri)}"
            blob_cmds.append(blob_cmd)
        blob_stdin = "\n".join(blob_cmds)
        logging.info(f"s5cmd blob upload command example: {blob_cmds[0]}")

        # Initialise credentials and execute
        with self._bucket_credentials() as credentials:
            cmd = " ".join([cmd, f" --credentials-file {credentials} run"])
            proc = Popen(shlex.split(cmd), shell=False, stdout=self.out_stream, stderr=self.out_stream, stdin=PIPE)
            stdout, stderr = proc.communicate(input=blob_stdin.encode())
        returncode = proc.wait()
        if returncode > 0:
            logging.warning(f"s5cmd cp failed with return code {returncode}: {stderr}")
        return stdout, stderr, returncode
