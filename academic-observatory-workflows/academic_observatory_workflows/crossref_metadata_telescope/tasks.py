import os
import shutil
import logging
import functools

import pendulum
from airflow.exceptions import AirflowException

from academic_observatory_workflows.crossref_metadata_telescope.release import CrossrefMetadataRelease
from observatory_platform.url_utils import retry_get_url
from observatory_platform.files import clean_dir

SNAPSHOT_URL = "https://api.crossref.org/snapshots/monthly/{year}/{month:02d}/all.json.tar.gz"


def download(release, **context):
    release = CrossrefMetadataRelease.from_dict(release)
    clean_dir(release.download_folder)

    url = make_snapshot_url(release.snapshot_date)
    logging.info(f"Downloading from url: {url}")

    # Set API token header
    api_key = os.environ.get("CROSSREF_METADATA_API_KEY")
    if api_key is None:
        raise AirflowException(
            f"download: the CROSSREF_METADATA_API_KEY environment variable is not set, please set it with a Kubernetes Secret."
        )
    header = {"Crossref-Plus-API-Token": f"Bearer {api_key}"}

    # Download release
    with retry_get_url(url, headers=header, stream=True) as response:
        with open(release.download_file_path, "wb") as file:
            response.raw.read = functools.partial(response.raw.read, decode_content=True)
            shutil.copyfileobj(response.raw, file)

    logging.info(f"Successfully download url to {release.download_file_path}")


def make_snapshot_url(snapshot_date: pendulum.DateTime) -> str:
    return SNAPSHOT_URL.format(year=snapshot_date.year, month=snapshot_date.month)
