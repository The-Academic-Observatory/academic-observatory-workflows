from __future__ import annotations

from dataclasses import dataclass
import json
import logging
import random
import shutil
import tarfile
import xml.etree.ElementTree as ET
from typing import Dict, List, Tuple

import pendulum
from airflow.exceptions import AirflowException, AirflowSkipException
from google.cloud.bigquery import SourceFormat

from academic_observatory_workflows.crossref_fundref_telescope.release import CrossrefFundrefRelease
from observatory_platform.dataset_api import make_observatory_api
from observatory_platform.google.bigquery import (
    bq_create_dataset,
    bq_find_schema,
    bq_load_table,
    bq_sharded_table_id,
    bq_table_exists,
)
from observatory_platform.airflow.workflow import CloudWorkspace, cleanup
from observatory_platform.airflow.release import set_task_state
from observatory_platform.files import clean_dir, save_jsonl_gz
from observatory_platform.google.gcs import gcs_download_blob, gcs_upload_file
from observatory_platform.url_utils import retry_get_url


def fetch_releases(
    cloud_workspace: CloudWorkspace,
    dag_id: str,
    run_id: str,
    data_interval_start: pendulum.DateTime,
    data_interval_end: pendulum.DateTime,
    bq_dataset_id: str,
    bq_table_name: str,
) -> List[Dict]:
    """Based on a list of all releases, checks which ones were released between the prev and this execution date
    of the DAG. If the release falls within the time period mentioned above, checks if a bigquery table doesn't
    exist yet for the release. A list of releases that passed both checks is passed to the next tasks. If the
    list is empty the workflow will stop."""

    # List releases between a start date and an end date
    start_date = pendulum.instance(data_interval_start)
    end_date = pendulum.instance(data_interval_end)
    all_releases = list_releases(start_date, end_date)
    all_releases = [
        CrossrefFundrefRelease.from_dict(
            dict(
                dag_id=dag_id,
                run_id=run_id,
                cloud_workspace=cloud_workspace.to_dict(),
                data_interval_start=data_interval_start,
                data_interval_end=data_interval_end,
                **release,
            )
        )
        for release in all_releases
    ]
    logging.info(f"Releases between start_date={start_date} and end_date={end_date}")
    logging.info(all_releases)

    # Check if the BigQuery table for each release already exists and only process release if the table doesn't exist
    releases = []
    for release in all_releases:
        table_id = bq_sharded_table_id(
            cloud_workspace.output_project_id, bq_dataset_id, bq_table_name, release.snapshot_date
        )
        logging.info("Checking if bigquery table already exists:")
        if bq_table_exists(table_id):
            logging.info(f"Skipping as table exists for {release.url}: {table_id}")
        else:
            logging.info(f"Table does not exist yet, processing {release.url} in this workflow")
            releases.append(release)

    if len(releases) > 0:
        return [release.to_dict() for release in releases]
    else:
        raise AirflowSkipException("No new releases found, skipping")


def download(release: Dict, cloud_workspace: CloudWorkspace) -> None:
    """Task to Download release tar.gz file from url."""
    release = CrossrefFundrefRelease.from_dict(release)
    clean_dir(release.release_folder)
    logging.info(f"Downloading file: {release.download_file_path}, url: {release.url}")

    # A selection of headers to prevent 403/forbidden error.
    headers_list = [
        {
            "authority": "gitlab.com",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/83.0.4103.116 Safari/537.36",
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,"
            "*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "sec-fetch-site": "none",
            "sec-fetch-mode": "navigate",
            "sec-fetch-dest": "document",
            "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
        },
        {
            "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:78.0) Gecko/20100101 Firefox/78.0",
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.5",
            "Referer": "https://gitlab.com/",
        },
    ]

    # Download release
    with retry_get_url(release.url, headers=random.choice(headers_list), stream=True) as response:
        with open(release.download_file_path, "wb") as file:
            shutil.copyfileobj(response.raw, file)

    # Upload to Cloud Storage
    success = gcs_upload_file(
        bucket_name=cloud_workspace.download_bucket,
        blob_name=release.download_blob_name,
        file_path=release.download_file_path,
    )
    if not success:
        raise AirflowException(f"Error uploading file {release.download_file_path} to bucket: {release.download_uri}")


def transform(release: Dict, cloud_workspace: CloudWorkspace) -> None:
    release = CrossrefFundrefRelease.from_dict(release)
    clean_dir(release.release_folder)

    # Download file
    success = gcs_download_blob(
        bucket_name=cloud_workspace.download_bucket,
        blob_name=release.download_blob_name,
        file_path=release.download_file_path,
    )
    if not success:
        raise AirflowException(f"Error downloading file: {release.download_uri}")

    # Extract file
    logging.info(f"Extracting file: {release.download_file_path}")
    with tarfile.open(release.download_file_path, "r:gz") as tar:
        for member in tar.getmembers():
            if member.isfile() and member.name.endswith(".rdf"):
                # Extract RDF file to memory
                rdf_file = tar.extractfile(member)

                # Save file to desired path
                with open(release.extract_file_path, "wb") as output_file:
                    output_file.write(rdf_file.read())
    logging.info(f"File extracted to: {release.extract_file_path}")

    # Parse RDF funders data
    logging.info(f"Transforming file: {release.extract_file_path}")
    funders, funders_by_key = parse_fundref_registry_rdf(release.extract_file_path)
    funders = add_funders_relationships(funders, funders_by_key)
    save_jsonl_gz(release.transform_file_path, funders)
    logging.info(f"Saved transformed file to: {release.transform_file_path}")

    # Upload to Bucket
    logging.info(f"Uploading file to bucket: {cloud_workspace.transform_bucket}")
    success = gcs_upload_file(
        bucket_name=cloud_workspace.transform_bucket,
        blob_name=release.transform_blob_name,
        file_path=release.transform_file_path,
    )
    if not success:
        raise AirflowException(f"Error uploading file {release.transform_file_path} to bucket: {release.transform_uri}")


def bq_load(
    release: Dict,
    cloud_workspace: CloudWorkspace,
    bq_dataset_id: str,
    dataset_description: str,
    bq_table_name: str,
    table_description: str,
    schema_folder: str,
) -> None:

    release = CrossrefFundrefRelease.from_dict(release)
    bq_create_dataset(
        project_id=cloud_workspace.output_project_id,
        dataset_id=bq_dataset_id,
        location=cloud_workspace.data_location,
        description=dataset_description,
    )
    schema_file_path = bq_find_schema(path=schema_folder, table_name=bq_table_name, release_date=release.snapshot_date)
    table_id = bq_sharded_table_id(
        cloud_workspace.output_project_id,
        bq_dataset_id,
        bq_table_name,
        release.snapshot_date,
    )
    success = bq_load_table(
        uri=release.transform_uri,
        table_id=table_id,
        schema_file_path=schema_file_path,
        source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
        table_description=table_description,
        ignore_unknown_values=True,
    )
    set_task_state(success, "bq_load", release)


def add_dataset_releases(release: Dict, dag_id: str, api_dataset_id: str, observatory_api_conn_id: str):

    release = CrossrefFundrefRelease.from_dict(release)
    dataset_release = DatasetRelease(
        dag_id=dag_id,
        dataset_id=api_dataset_id,
        dag_run_id=release.run_id,
        snapshot_date=release.snapshot_date,
        data_interval_start=release.data_interval_start,
        data_interval_end=release.data_interval_end,
    )
    api = make_observatory_api(observatory_api_conn_id=observatory_api_conn_id)
    api.post_dataset_release(dataset_release)


def cleanup_workflow(release: Dict, dag_id: str, logical_date):

    release = CrossrefFundrefRelease.from_dict(release)
    cleanup(dag_id=dag_id, workflow_folder=release.workflow_folder)


def list_releases(start_date: pendulum.DateTime, end_date: pendulum.DateTime) -> List[dict]:
    """List all available CrossrefFundref releases between the start and end date

    :param start_date: The start date of the period to look for releases
    :param end_date: The end date of the period to look for releases
    :return: list with dictionaries of release info (url and release date)
    """

    # A selection of headers to prevent 403/forbidden error.
    headers_list = [
        {
            "authority": "gitlab.com",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/84.0.4147.89 Safari/537.36",
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,"
            "*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "sec-fetch-site": "none",
            "sec-fetch-mode": "navigate",
            "sec-fetch-dest": "document",
            "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
        },
        {
            "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:78.0) Gecko/20100101 Firefox/78.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        },
    ]

    releases = []
    headers = random.choice(headers_list)
    current_page = 1

    while True:
        # Fetch page
        url = f"{RELEASES_URL}?per_page=100&page={current_page}"
        response = retry_get_url(url, headers=headers)

        # Parse json
        num_pages = int(response.headers["X-Total-Pages"])
        json_response = json.loads(response.text)

        def parse_version(r: Dict):
            try:
                # ValueError on "v.1.48"
                return float(r["tag_name"].lower().strip("v"))
            except ValueError:
                return float(r["name"].lower().strip("v"))

        # Parse release information
        for release in json_response:
            version = parse_version(release)
            for source in release["assets"]["sources"]:
                if source["format"] == "tar.gz":
                    # Parse release date
                    if version == 0.1:
                        snapshot_date = pendulum.datetime(year=2014, month=3, day=1)
                    elif version < 1.0:
                        date_string = release["description"].split("\n")[0]
                        snapshot_date = pendulum.from_format("01 " + date_string, "DD MMMM YYYY")
                    else:
                        snapshot_date = pendulum.parse(release["released_at"])

                    # Only include release if it is within start and end dates
                    if start_date <= snapshot_date < end_date:
                        releases.append({"url": source["url"], "snapshot_date": snapshot_date.to_date_string()})

        # Check if we should exit or get the next page
        if num_pages <= current_page:
            break
        current_page += 1

    return releases


def parse_fundref_registry_rdf(registry_file_path: str) -> Tuple[List, dict]:
    """Helper function to parse a fundref registry rdf file and to return a python list containing each funder.

    :param registry_file_path: the filename of the registry.rdf file to be parsed.
    :return: funders list containing all the funders parsed from the input rdf and dictionary of funders with their
    id as key.
    """

    funders = []
    funders_by_key = {}

    # Strip leading white space from file this is present in fundref release 2019-06-01. If not removed it will give
    # an XML ParseError.
    with open(registry_file_path, "r") as f:
        xml_string = f.read().strip()

    root = ET.fromstring(xml_string)
    tag_prefix = root.tag.split("}")[0] + "}"
    for record in root:
        tag = record.tag.split("}")[-1]
        if tag == "ConceptScheme":
            for nested in record:
                tag = nested.tag.split("}")[-1]
                if tag == "hasTopConcept":
                    funder_id = nested.attrib[tag_prefix + "resource"]
                    funders_by_key[funder_id] = new_funder_template()

        if tag == "Concept":
            funder_id = record.attrib[tag_prefix + "about"]
            funder = funders_by_key[funder_id]
            funder["funder"] = funder_id
            for nested in record:
                tag = nested.tag.split("}")[-1]
                if tag == "inScheme":
                    continue
                elif tag == "prefLabel":
                    funder["pre_label"] = nested[0][0].text
                elif tag == "altLabel":
                    alt_label = nested[0][0].text
                    if alt_label is not None:
                        funder["alt_label"].append(alt_label)
                elif tag == "narrower":
                    funder["narrower"].append(nested.attrib[tag_prefix + "resource"])
                elif tag == "broader":
                    funder["broader"].append(nested.attrib[tag_prefix + "resource"])
                elif tag == "modified":
                    funder["modified"] = nested.text
                elif tag == "created":
                    funder["created"] = nested.text
                elif tag == "fundingBodySubType":
                    funder["funding_body_type"] = nested.text
                elif tag == "fundingBodyType":
                    funder["funding_body_sub_type"] = nested.text
                elif tag == "region":
                    funder["region"] = nested.text
                elif tag == "country":
                    funder["country"] = nested.text
                elif tag == "state":
                    funder["state"] = nested.text
                elif tag == "address":
                    funder["country_code"] = nested[0][0].text
                elif tag == "taxId":
                    funder["tax_id"] = nested.text
                elif tag == "continuationOf":
                    funder["continuation_of"].append(nested.attrib[tag_prefix + "resource"])
                elif tag == "renamedAs":
                    funder["renamed_as"].append(nested.attrib[tag_prefix + "resource"])
                elif tag == "replaces":
                    funder["replaces"].append(nested.attrib[tag_prefix + "resource"])
                elif tag == "affilWith":
                    funder["affil_with"].append(nested.attrib[tag_prefix + "resource"])
                elif tag == "mergedWith":
                    funder["merged_with"].append(nested.attrib[tag_prefix + "resource"])
                elif tag == "incorporatedInto":
                    funder["incorporated_into"].append(nested.attrib[tag_prefix + "resource"])
                elif tag == "isReplacedBy":
                    funder["is_replaced_by"].append(nested.attrib[tag_prefix + "resource"])
                elif tag == "incorporates":
                    funder["incorporates"].append(nested.attrib[tag_prefix + "resource"])
                elif tag == "splitInto":
                    funder["split_into"].append(nested.attrib[tag_prefix + "resource"])
                elif tag == "status":
                    funder["status"] = nested.attrib[tag_prefix + "resource"]
                elif tag == "mergerOf":
                    funder["merger_of"].append(nested.attrib[tag_prefix + "resource"])
                elif tag == "splitFrom":
                    funder["split_from"] = nested.attrib[tag_prefix + "resource"]
                elif tag == "formerlyKnownAs":
                    funder["formly_known_as"] = nested.attrib[tag_prefix + "resource"]
                elif tag == "notation":
                    funder["notation"] = nested.text
                else:
                    logging.info(f"Unrecognized tag for element: {nested}")

            funders.append(funder)

    return funders, funders_by_key


def add_funders_relationships(funders: List, funders_by_key: Dict) -> List:
    """Adds any children/parent relationships to funder instances in the funders list.

    :param funders: List of funders
    :param funders_by_key: Dictionary of funders with their id as key.
    :return: funders with added relationships.
    """

    for funder in funders:
        children, returned_depth = recursive_funders(funders_by_key, funder, 0, "narrower", [])
        funder["children"] = children
        funder["bottom"] = len(children) > 0

        parent, returned_depth = recursive_funders(funders_by_key, funder, 0, "broader", [])
        funder["parents"] = parent
        funder["top"] = len(parent) > 0

    return funders


def new_funder_template():
    """Helper Function for creating a new Funder.

    :return: a blank funder object.
    """
    return {
        "funder": None,
        "pre_label": None,
        "alt_label": [],
        "narrower": [],
        "broader": [],
        "modified": None,
        "created": None,
        "funding_body_type": None,
        "funding_body_sub_type": None,
        "region": None,
        "country": None,
        "country_code": None,
        "state": None,
        "tax_id": None,
        "continuation_of": [],
        "renamed_as": [],
        "replaces": [],
        "affil_with": [],
        "merged_with": [],
        "incorporated_into": [],
        "is_replaced_by": [],
        "incorporates": [],
        "split_into": [],
        "status": None,
        "merger_of": [],
        "split_from": None,
        "formly_known_as": None,
        "notation": None,
    }


def recursive_funders(
    funders_by_key: Dict, funder: Dict, depth: int, direction: str, sub_funders: List
) -> Tuple[List, int]:
    """Recursively goes through a funder/sub_funder dict. The funder properties can be looked up with the
    funders_by_key dictionary that stores the properties per funder id. Any children/parents for the funder are
    already given in the xml element with the 'narrower' and 'broader' tags. For each funder in the list,
    it will recursively add any children/parents for those funders in 'narrower'/'broader' and their funder properties.

    :param funders_by_key: dictionary with id as key and funders object as value
    :param funder: dictionary of a given funder containing 'narrower' and 'broader' info
    :param depth: keeping track of nested depth
    :param direction: either 'narrower' or 'broader' to get 'children' or 'parents'
    :param sub_funders: list to keep track of which funder ids are parents
    :return: list of children and current depth
    """

    starting_depth = depth
    children = []
    # Loop through funder_ids in 'narrower' or 'broader' info
    for funder_id in funder[direction]:
        if funder_id in sub_funders:
            # Stop recursion if funder is it's own parent or child
            logging.info(f"Funder {funder_id} is it's own parent/child, skipping..")
            name = "NA"
            returned = []
            returned_depth = depth
            sub_funders.append(funder_id)
        else:
            try:
                sub_funder = funders_by_key[funder_id]
                # Add funder id of sub_funder to list to keep track of 'higher' sub_funders in the recursion
                sub_funders.append(sub_funder["funder"])
                # Store name to pass on to child object
                name = sub_funder["pre_label"]
                # Get children/parents of sub_funder
                returned, returned_depth = recursive_funders(
                    funders_by_key, sub_funder, starting_depth + 1, direction, sub_funders
                )
            except KeyError:
                logging.info(f"Could not find funder by id: {funder_id}, skipping..")
                name = "NA"
                returned = []
                returned_depth = depth
                sub_funders.append(funder_id)

        # Add child/parent (containing nested children/parents) to list
        if direction == "narrower":
            child = {"funder": funder_id, "name": name, "children": returned}
        else:
            child = {"funder": funder_id, "name": name, "parent": returned}
        children.append(child)
        sub_funders.pop(-1)
        if returned_depth > depth:
            depth = returned_depth
    return children, depth
