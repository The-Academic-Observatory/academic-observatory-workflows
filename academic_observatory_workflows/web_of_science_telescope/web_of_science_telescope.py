# Copyright 2020 Curtin University
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

# Author: Tuan Chien, James Diprose

import logging
import os
from collections import OrderedDict
from concurrent.futures import as_completed, ThreadPoolExecutor
from math import floor
from typing import Any, Dict, List, Tuple, Type, Union

import backoff
import pendulum
import xmltodict
from airflow.exceptions import AirflowException
from google.cloud.bigquery import SourceFormat, WriteDisposition
from observatory.api.client.model.dataset_release import DatasetRelease
from observatory.platform.airflow import (
    get_airflow_connection_login,
    get_airflow_connection_password,
)
from observatory.platform.api import build_schedule, make_observatory_api
from observatory.platform.bigquery import bq_create_dataset, bq_find_schema, bq_load_table, bq_sharded_table_id
from observatory.platform.config import AirflowConns
from observatory.platform.files import (
    clean_dir,
    get_as_list,
    get_as_list_or_none,
    get_chunks,
    get_entry_or_none,
    list_files,
    load_file,
    save_jsonl_gz,
    write_to_file,
)
from observatory.platform.gcs import gcs_blob_name_from_path, gcs_blob_uri, gcs_upload_files
from observatory.platform.observatory_config import CloudWorkspace
from observatory.platform.workflows.workflow import (
    cleanup,
    make_snapshot_date,
    set_task_state,
    SnapshotRelease,
    Workflow,
)
from ratelimit import limits, sleep_and_retry
from suds import WebFault
from wos import WosClient

from academic_observatory_workflows.config import project_path, Tag


class WebOfScienceRelease(SnapshotRelease):
    API_URL = "http://scientific.thomsonreuters.com"
    EXPECTED_SCHEMA = "http://scientific.thomsonreuters.com/schema/wok5.4/public/FullRecord"

    def __init__(
        self,
        *,
        dag_id: str,
        run_id: str,
        snapshot_date: pendulum.DateTime,
    ):
        """Construct a WebOfScienceRelease instance.

        :param dag_id: The DAG ID.
        :param snapshot_date: Release date.
        """

        super().__init__(
            dag_id=dag_id,
            run_id=run_id,
            snapshot_date=snapshot_date,
        )
        self.download_file_regex = r".*\.xml"
        self.transform_file_name = "wos.jsonl.gz"
        self.transform_file_path = os.path.join(self.transform_folder, self.transform_file_name)


class WebOfScienceTelescope(Workflow):
    SCHEMA_VERSION_ALT = "http://scientific.thomsonreuters.com/schema/wok5.4/public/FullRecord"

    def __init__(
        self,
        *,
        dag_id: str,
        cloud_workspace: CloudWorkspace,
        institution_ids: List[str],
        wos_conn_id: str,
        earliest_date: pendulum.DateTime = pendulum.datetime(1800, 1, 1),
        bq_dataset_id: str = "web_of_science",
        bq_table_name: str = "web_of_science",
        api_dataset_id: str = "web_of_science",
        schema_folder: str = project_path("web_of_science_telescope", "schema"),
        dataset_description: str = "The Web of Science citation database: https://clarivate.com/webofsciencegroup/solutions/web-of-science",
        table_description: str = "The Web of Science citation database: https://clarivate.com/webofsciencegroup/solutions/web-of-science",
        observatory_api_conn_id: str = AirflowConns.OBSERVATORY_API,
        start_date: pendulum.DateTime = pendulum.datetime(2018, 5, 14),
        schedule: str = "@monthly",
    ):
        """Web of Science telescope.

        :param dag_id: the id of the DAG.
        :param cloud_workspace: the cloud workspace settings.
        :param institution_ids: list of institution IDs to use for the WoS search query.
        :param wos_conn_id: the WoS connection ID.
        :param earliest_date: the earliest date to query for results.
        :param bq_dataset_id: the BigQuery dataset id.
        :param bq_table_name: the BigQuery table name.
        :param api_dataset_id: the Dataset ID to use when storing releases.
        :param schema_folder: the SQL schema path.
        :param dataset_description: description for the BigQuery dataset.
        :param table_description: description for the BigQuery table.
        :param observatory_api_conn_id: the Observatory API connection key.
        :param start_date: the start date of the DAG.
        :param schedule: the schedule interval of the DAG.
        """

        super().__init__(
            dag_id=dag_id,
            start_date=start_date,
            schedule=schedule,
            catchup=False,
            airflow_conns=[observatory_api_conn_id, wos_conn_id],
            tags=[Tag.academic_observatory],
        )
        self.cloud_workspace = cloud_workspace
        self.institution_ids = institution_ids
        self.wos_conn_id = wos_conn_id
        self.earliest_date = earliest_date
        self.bq_dataset_id = bq_dataset_id
        self.bq_table_name = bq_table_name
        self.api_dataset_id = api_dataset_id
        self.schema_folder = schema_folder
        self.dataset_description = dataset_description
        self.table_description = table_description
        self.observatory_api_conn_id = observatory_api_conn_id

        self.add_setup_task(self.check_dependencies)
        self.add_task(self.download)
        self.add_task(self.upload_downloaded)
        self.add_task(self.transform)
        self.add_task(self.upload_transformed)
        self.add_task(self.bq_load)
        self.add_task(self.add_new_dataset_releases)
        self.add_task(self.cleanup)

    def make_release(self, **kwargs) -> WebOfScienceRelease:
        """Make a list of WebOfScienceRelease instances.

        :param kwargs: the context passed from the Airflow Operator.
        See https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return: WebOfScienceRelease instance.
        """

        snapshot_date = make_snapshot_date(**kwargs)
        return WebOfScienceRelease(
            dag_id=self.dag_id,
            run_id=kwargs["run_id"],
            snapshot_date=snapshot_date,
        )

    def download(self, release: WebOfScienceRelease, **kwargs):
        """Download a Web of Science live snapshot."""

        clean_dir(release.download_folder)
        schedule = build_schedule(self.earliest_date, release.snapshot_date)
        login = get_airflow_connection_login(self.wos_conn_id)
        password = get_airflow_connection_password(self.wos_conn_id)
        WosUtility.download_wos_parallel(
            login=login,
            password=password,
            schedule=schedule,
            conn=self.dag_id,
            institution_ids=self.institution_ids,
            download_dir=release.download_folder,
        )

    def upload_downloaded(self, release: WebOfScienceRelease, **kwargs):
        """Upload data to Cloud Storage."""

        file_list = list_files(release.download_folder, release.download_file_regex)
        success = gcs_upload_files(bucket_name=self.cloud_workspace.download_bucket, file_paths=file_list)
        set_task_state(success, self.upload_downloaded.__name__, release)

    def transform(self, release: WebOfScienceRelease, **kwargs):
        """Convert the XML response into BQ friendly jsonlines."""

        clean_dir(release.transform_folder)
        data = []
        file_list = list_files(release.download_folder, release.download_file_regex)
        for xml_file in file_list:
            records = transform_xml_to_json(xml_file)
            data += transform_to_db_format(
                records=records, snapshot_date=release.snapshot_date, institution_ids=self.institution_ids
            )
        save_jsonl_gz(release.transform_file_path, data)

    def upload_transformed(self, release: WebOfScienceRelease, **kwargs) -> None:
        """Upload the transformed data to Cloud Storage."""

        success = gcs_upload_files(
            bucket_name=self.cloud_workspace.transform_bucket, file_paths=[release.transform_file_path]
        )
        set_task_state(success, self.upload_downloaded.__name__, release)

    def bq_load(self, release: WebOfScienceRelease, **kwargs):
        """Loads data into BigQuery."""

        bq_create_dataset(
            project_id=self.cloud_workspace.output_project_id,
            dataset_id=self.bq_dataset_id,
            location=self.cloud_workspace.data_location,
            description=self.dataset_description,
        )

        uri = gcs_blob_uri(self.cloud_workspace.transform_bucket, gcs_blob_name_from_path(release.transform_file_path))
        schema_file_path = bq_find_schema(
            path=self.schema_folder, table_name=self.bq_table_name, release_date=release.snapshot_date
        )
        table_id = bq_sharded_table_id(
            self.cloud_workspace.output_project_id, self.bq_dataset_id, self.bq_table_name, release.snapshot_date
        )
        success = bq_load_table(
            uri=uri,
            table_id=table_id,
            schema_file_path=schema_file_path,
            source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
            table_description=self.table_description,
            write_disposition=WriteDisposition.WRITE_APPEND,
            ignore_unknown_values=True,
        )
        set_task_state(success, self.bq_load.__name__, release)

    def add_new_dataset_releases(self, release: WebOfScienceRelease, **kwargs) -> None:
        """Adds release information to API."""

        dataset_release = DatasetRelease(
            dag_id=self.dag_id,
            dataset_id=self.api_dataset_id,
            dag_run_id=release.run_id,
            snapshot_date=release.snapshot_date,
            data_interval_start=kwargs["data_interval_start"],
            data_interval_end=kwargs["data_interval_end"],
        )
        api = make_observatory_api(observatory_api_conn_id=self.observatory_api_conn_id)
        api.post_dataset_release(dataset_release)

    def cleanup(self, release: WebOfScienceRelease, **kwargs) -> None:
        """Delete all files, folders and XComs associated with this release."""

        cleanup(dag_id=self.dag_id, execution_date=kwargs["execution_date"], workflow_folder=release.workflow_folder)


class WosUtilConst:
    """Class containing some WosUtility constants. Makes these values accessible by decorators."""

    # WoS limits as a guide for reference. Throttle limits more conservative than this.
    RESULT_LIMIT = 100  # Return 100 results max per query.
    CALL_LIMIT = 1  # WoS says they can do 2 api calls / second, but we're setting 1 per 2 seconds.
    CALL_PERIOD = 2  # seconds
    SESSION_CALL_LIMIT = 5  # 5 calls.
    SESSION_CALL_PERIOD = 360  # 6 minutes. [Actual WoS limit is 5 mins]
    RETRIES = 3


class WosUtility:
    """Handles the interaction with Web of Science"""

    @staticmethod
    def build_query(*, institution_ids: List[str], period: Type[pendulum.Period]) -> OrderedDict:
        """Build a WoS API query.

        :param institution_ids: List of Institutional ID to query, e.g, "Curtin University"
        :param period: A tuple containing start and end dates.
        :return: Constructed web query.
        """
        start_date = period.start.isoformat()
        end_date = period.end.isoformat()

        organisations = " OR ".join(institution_ids)
        query_str = f"OG=({organisations})"

        query = OrderedDict(
            [
                ("query", query_str),
                ("count", WosUtilConst.RESULT_LIMIT),
                ("offset", 1),
                ("timeSpan", {"begin": start_date, "end": end_date}),
            ]
        )
        return query

    @staticmethod
    def parse_query(records: Any) -> Tuple[dict, str]:
        """Parse XML tree record into a dict.

        :param records: XML tree returned by the web query.
        :return: Dictionary version of the web response and a schema version string.
        """

        records_dict = xmltodict.parse(records)["records"]
        schema_string = records_dict["@xmlns"]
        return get_as_list(records_dict, "REC"), schema_string

    @staticmethod
    @sleep_and_retry
    @limits(calls=WosUtilConst.CALL_LIMIT, period=WosUtilConst.CALL_PERIOD)
    def search(*, client: WosClient, query: OrderedDict) -> Any:
        """Throttling wrapper for the API call. This is a global limit for this API when called from a program on the
        same machine. If you are throttled, it will throw a WebFault and the exception message will contain the phrase
        'Request denied by Throttle server'

        Limiting to 1 call per second even though theoretical limit is 2 per second just in case.
        Throttle limits may or may not be enforced. Probably depends on how executors spin up tasks.

        :param client: WosClient object.
        :param query: Query object.
        :returns: Query results.
        """

        return client.search(**query)

    @staticmethod
    def make_query(*, client: WosClient, query: OrderedDict) -> List[Any]:
        """Make the API calls to retrieve information from Web of Science.

        :param client: WosClient object.
        :param query: Constructed search query from use build_query.

        :return: List of XML responses.
        """

        results = WosUtility.search(client=client, query=query)
        num_results = int(results.recordsFound)
        record_list = [results.records]

        if num_results > WosUtilConst.RESULT_LIMIT:
            for offset in range(WosUtilConst.RESULT_LIMIT + 1, num_results, WosUtilConst.RESULT_LIMIT):
                query["offset"] = offset
                record_list.append(WosUtility.search(client=client, query=query).records)

        return record_list

    @staticmethod
    def download_wos_period(
        *, client: WosClient, conn: str, period: pendulum.Period, institution_ids: List[str], download_dir: str
    ):
        """Download records for a stated date range.

        :param client: WebClient object.
        :param conn: file name for saved response as a pickle file.
        :param period: Period tuple containing (start date, end date).
        :param institution_ids: List of Institutional ID to query, e.g, "Curtin University"
        :param download_dir: Directory to download files to.
        """

        harvest_ts = pendulum.now("UTC")
        logging.info(f"{conn} with session id {client._SID}: retrieving period {period.start} - {period.end}")
        query = WosUtility.build_query(institution_ids=institution_ids, period=period)
        result = WosUtility.make_query(client=client, query=query)

        file_prefix = os.path.join(download_dir, f"{period.start}_{period.end}")
        for i, entry in enumerate(result):
            save_file = f"{file_prefix}_{i}_{harvest_ts}.xml"
            logging.info(f"Saving to file {save_file}")
            write_to_file(entry, save_file)

    @staticmethod
    @backoff.on_exception(
        backoff.constant, WebFault, max_tries=WosUtilConst.RETRIES, interval=WosUtilConst.SESSION_CALL_PERIOD
    )
    def download_wos_batch(
        *,
        login: str,
        password: str,
        batch: List[pendulum.Period],
        conn: str,
        institution_ids: List[str],
        download_dir: str,
    ):
        """Download one batch of WoS snapshots. Throttling limits are more conservative than WoS limits.
        Throttle limits may or may not be enforced. Probably depends on how executors spin up tasks.

        :param login: login.
        :param password: password.
        :param batch: List of tuples of (start_date, end_date) to fetch.
        :param conn: connection_id string from Airflow variable.
        :param institution_ids: List of Institutional ID to query, e.g, "Curtin University"
        :param download_dir: Download directory to save response to.
        :return: List of saved files from this batch.
        """

        with WosClient(login, password) as client:
            for period in batch:
                WosUtility.download_wos_period(
                    client=client, conn=conn, period=period, institution_ids=institution_ids, download_dir=download_dir
                )

    @staticmethod
    def get_parallel_batches(schedule: List[pendulum.Period]) -> Tuple[int, List[List[pendulum.Period]]]:
        """Split the schedule to download in parallel sessions.  If the number of periods is less than the number of sessions, just use a single session. If there is not an even split, then the extra periods will be distributed amongst the first few sessions evenly.

        :param schedule: Schedule to split.
        :return: Number of sessions, and the split schedule.
        """

        n_schedule = len(schedule)

        if n_schedule < WosUtilConst.SESSION_CALL_LIMIT:
            sessions = 1
            batches = [schedule]
        else:
            sessions = WosUtilConst.SESSION_CALL_LIMIT
            batch_size = int(floor(len(schedule) / sessions))
            batches = list(get_chunks(input_list=schedule, chunk_size=batch_size))

            # Evenly distribute the remainder amongst the sessions
            if len(schedule) % sessions != 0:
                last_batch = batches[-1]
                batches = batches[:-1]  # Remove last batch
                for i, period in enumerate(last_batch):
                    batches[i].append(period)

        return sessions, batches

    @staticmethod
    def download_wos_parallel(
        *,
        login: str,
        password: str,
        schedule: List[pendulum.Period],
        conn: str,
        institution_ids: List[str],
        download_dir: str,
    ):
        """Download WoS snapshot with parallel sessions. Using threads.

        :param login: WoS login
        :param password: WoS password
        :param schedule: List of date range (start_date, end_date) tuples to download.
        :param conn: Airflow connection_id string.
        :param institution_ids: List of Institutional ID to query, e.g, "Curtin University"
        :param download_dir: Path to download to.
        :return: List of files downloaded.
        """

        sessions, batches = WosUtility.get_parallel_batches(schedule)

        with ThreadPoolExecutor(max_workers=sessions) as executor:
            futures = []
            for i in range(sessions):
                futures.append(
                    executor.submit(
                        WosUtility.download_wos_batch,
                        login=login,
                        password=password,
                        batch=batches[i],
                        conn=conn,
                        institution_ids=institution_ids,
                        download_dir=download_dir,
                    )
                )
            for future in as_completed(futures):
                future.result()

    @staticmethod
    def download_wos_sequential(
        *, login: str, password: str, schedule: list, conn: str, institution_ids: List[str], download_dir: str
    ) -> List[str]:
        """Download WoS snapshot sequentially.

        :param login: WoS login
        :param password: WoS password
        :param schedule: List of date range (start_date, end_date) tuples to download.
        :param conn: Airflow connection_id string.
        :param institution_ids: List of Institutional ID to query, e.g, "Curtin University"
        :param download_dir: Path to download to.
        :return: List of files downloaded.
        """

        return WosUtility.download_wos_batch(
            login=login,
            password=password,
            batch=schedule,
            conn=conn,
            institution_ids=institution_ids,
            download_dir=download_dir,
        )


class WosNameAttributes:
    """Helper class for parsing name attributes."""

    def __init__(self, data: dict):
        self._contribs = WosNameAttributes._get_contribs(data)

    @staticmethod
    def _get_contribs(data: dict) -> dict:
        """Helper function to parse the contributors structure to aid extraction of fields.

        :param data: dictionary to query.
        :return: Dictionary of attributes keyed by full_name string.
        """

        contrib_dict = dict()

        try:
            contributors = get_as_list(data["static_data"]["contributors"], "contributor")
            for contributor in contributors:
                name_field = contributor["name"]
                first_name = name_field["first_name"]
                last_name = name_field["last_name"]
                attrib = dict()
                full_name = f"{first_name} {last_name}"
                if "@r_id" in name_field:
                    attrib["r_id"] = name_field["@r_id"]
                if "@orcid_id" in name_field:
                    attrib["orcid"] = name_field["@orcid_id"]
                contrib_dict[full_name] = attrib
        except:
            pass

        return contrib_dict

    def get_orcid(self, full_name: str) -> str:
        """Get the orcid id of a person. Note that full name must be the combination of first and last name.
        This is not necessarily the full_name field.

        :param full_name: The 'first_name last_name' string.
        :return: orcid id.
        """
        try:
            orcid = self._contribs[full_name]["orcid"]
            return orcid
        except:
            return None

    def get_r_id(self, full_name: str) -> str:
        """Get the r_id of a person. Note that full name must be the combination of first and last name.
        This is not necessarily the full_name field.

        :param full_name: The 'first_name last_name' string.
        :return: r_id.
        """

        try:
            rid = self._contribs[full_name]["r_id"]
            return rid
        except:
            return None


class WosJsonParser:
    """Helper methods to process the converted json from Web of Science."""

    @staticmethod
    def get_identifiers(data: dict) -> Dict[str, Any]:
        """Extract identifier information.

        :param data: dictionary of web response.
        :return: Identifier record.
        """

        recognised_types = {
            "issn",
            "eissn",
            "isbn",
            "eisbn",
            "art_no",
            "meeting_abs",
            "xref_doi",
            "parent_book_doi",
            "doi",
        }

        field = {rtype: None for rtype in recognised_types}
        field["uid"] = data["UID"]

        try:
            identifiers = data["dynamic_data"]["cluster_related"]["identifiers"]
            identifier = get_as_list(identifiers, "identifier")

            for entry in identifier:
                type_ = entry["@type"]
                value = entry["@value"]

                if type_ in recognised_types:
                    field[type_] = value
        except:
            pass

        return field

    @staticmethod
    def get_pub_info(data: dict) -> Dict[str, Any]:
        """Extract publication information fields.

        :param data: dictionary of web response.
        :return: Publication info record.
        """

        field = {
            "sort_date": None,
            "pub_type": None,
            "page_count": None,
            "source": None,
            "doc_type": None,
            "publisher": None,
            "publisher_city": None,
        }

        try:
            summary = data["static_data"]["summary"]

            if "pub_info" in summary:
                pub_info = summary["pub_info"]
                field["sort_date"] = pub_info["@sortdate"]
                field["pub_type"] = pub_info["@pubtype"]
                field["page_count"] = int(pub_info["page"]["@page_count"])

            if "publishers" in summary and "publisher" in summary["publishers"]:
                publisher = summary["publishers"]["publisher"]
                field["publisher"] = publisher["names"]["name"]["full_name"]
                field["publisher_city"] = publisher["address_spec"]["city"]

            if "titles" in summary and "title" in summary["titles"]:
                titles = get_as_list(summary["titles"], "title")
                for title in titles:
                    if title["@type"] == "source":
                        field["source"] = title["#text"]
                        break

            if "doctypes" in summary:
                doctypes = get_as_list(summary["doctypes"], "doctype")
                field["doc_type"] = doctypes[0]
        except:
            pass

        return field

    @staticmethod
    def get_title(data: dict) -> Union[None, str]:
        """Extract title. May raise exception on error.

        :param data: dictionary of web response.
        :return: String of title or None if not found.
        """

        try:
            for entry in data["static_data"]["summary"]["titles"]["title"]:
                if "@type" in entry and entry["@type"] == "item" and "#text" in entry:
                    return entry["#text"]
        except:
            return None

    @staticmethod
    def get_names(data: dict) -> List[Dict[str, Any]]:
        """Extract names fields.

        :param data: dictionary of web response.
        :return: List of name records.
        """

        field = list()
        try:
            data["static_data"]["summary"]["names"]
            attrib = WosNameAttributes(data)
            names = get_as_list(data["static_data"]["summary"]["names"], "name")

            for name in names:
                entry = dict()
                entry["seq_no"] = int(get_entry_or_none(name, "@seq_no"))
                entry["role"] = get_entry_or_none(name, "@role")
                entry["first_name"] = get_entry_or_none(name, "first_name")
                entry["last_name"] = get_entry_or_none(name, "last_name")
                entry["wos_standard"] = get_entry_or_none(name, "wos_standard")
                entry["daisng_id"] = get_entry_or_none(name, "@daisng_id")
                entry["full_name"] = get_entry_or_none(name, "full_name")

                # Get around errors / booby traps for name retrieval
                first_name = entry["first_name"]
                last_name = entry["last_name"]
                full_name = f"{first_name} {last_name}"
                entry["orcid"] = attrib.get_orcid(full_name)
                entry["r_id"] = attrib.get_r_id(full_name)
                field.append(entry)
        except:
            pass

        return field

    @staticmethod
    def get_languages(data: dict) -> List[Dict[str, str]]:
        """Extract language fields.

        :param data: dictionary of web response.
        :return: List of language records.
        """

        lang_list = list()

        try:
            data["static_data"]["fullrecord_metadata"]["languages"]
        except:
            return lang_list

        languages = get_as_list(data["static_data"]["fullrecord_metadata"]["languages"], "language")
        for entry in languages:
            lang_list.append({"type": entry["@type"], "name": entry["#text"]})
        return lang_list

    @staticmethod
    def get_refcount(data: dict) -> Union[int, None]:
        """Extract reference count.

        :param data: dictionary of web response.
        :return: Reference count.
        """

        try:
            refcount = int(data["static_data"]["fullrecord_metadata"]["refs"]["@count"])
            return refcount
        except:
            return None

    @staticmethod
    def get_abstract(data: dict) -> List[str]:
        """Extract abstracts.

        :param data: dictionary of web response.
        :return: List of abstracts.
        """

        abstract_list = list()
        try:
            abstracts = get_as_list(data["static_data"]["fullrecord_metadata"]["abstracts"], "abstract")
            for abstract in abstracts:
                texts = get_as_list(abstract["abstract_text"], "p")
                for text in texts:
                    abstract_list.append(text)
        except:
            pass

        return abstract_list

    @staticmethod
    def get_keyword(data: dict) -> List[str]:
        """Extract keywords. Will also get the keywords from keyword plus if they are available.

        :param data: dictionary of web response.
        :return: List of keywords.
        """

        keywords = list()
        try:
            keywords = get_as_list(data["static_data"]["fullrecord_metadata"]["keywords"], "keyword")
            if "item" in data["static_data"] and "keywords_plus" in data["static_data"]["item"]:
                plus = get_as_list(data["static_data"]["item"]["keywords_plus"], "keyword")
                keywords = keywords + plus
        except:
            pass

        return keywords

    @staticmethod
    def get_conference(data: dict) -> List[Dict[str, Any]]:
        """Extract conference information.

        :param data: dictionary of web response.
        :return: List of conferences.
        """

        conferences = list()
        try:
            conf_list = get_as_list(data["static_data"]["summary"]["conferences"], "conference")
            for conf in conf_list:
                conference = dict()
                conference["id"] = get_entry_or_none(conf, "@conf_id")
                if conference["id"] is not None:
                    conference["id"] = int(conference["id"])
                conference["name"] = None

                if "conf_titles" in conf and "conf_title" in conf["conf_titles"]:
                    titles = get_as_list(conf["conf_titles"], "conf_title")
                    conference["name"] = titles[0]
                conferences.append(conference)
        except:
            pass

        return conferences

    @staticmethod
    def get_orgs(data: dict) -> list:
        """Extract the organisation information.

        :param data: dictionary of web response.
        :return: list of organisations or None
        """

        orgs = list()

        try:
            addr_list = get_as_list(data["static_data"]["fullrecord_metadata"]["addresses"], "address_name")
        except:
            return orgs

        for addr in addr_list:
            spec = addr["address_spec"]
            org = dict()

            org["city"] = get_entry_or_none(spec, "city")
            org["state"] = get_entry_or_none(spec, "state")
            org["country"] = get_entry_or_none(spec, "country")

            if "organizations" not in addr["address_spec"]:
                orgs.append(org)
                return orgs

            org_list = get_as_list(addr["address_spec"]["organizations"], "organization")
            org["org_name"] = org_list[0] if len(org_list) > 0 else None

            for entry in org_list:
                if isinstance(entry, dict) and "@pref" in entry and entry["@pref"] == "Y":
                    org["org_name"] = entry["#text"]
                    break

            if "suborganizations" in addr["address_spec"]:
                org["suborgs"] = get_as_list(addr["address_spec"]["suborganizations"], "suborganization")

            if "names" in addr and "name" in addr["names"]:
                names = get_as_list(addr["names"], "name")
                names_list = list()
                for name in names:
                    entry = dict()
                    entry["first_name"] = get_entry_or_none(name, "first_name")
                    entry["last_name"] = get_entry_or_none(name, "last_name")
                    entry["daisng_id"] = get_entry_or_none(name, "@daisng_id")
                    entry["full_name"] = get_entry_or_none(name, "full_name")
                    entry["wos_standard"] = get_entry_or_none(name, "wos_standard")
                    names_list.append(entry)
                org["names"] = names_list
            orgs.append(org)
        return orgs

    @staticmethod
    def get_fund_ack(data: dict) -> dict:
        """Extract funding acknowledgements.

        :param data: dictionary of web response.
        :return: Funding acknowledgement information.
        """

        fund_ack = dict()
        fund_ack["text"] = list()
        fund_ack["grants"] = list()

        try:
            entry = data["static_data"]["fullrecord_metadata"]["fund_ack"]
            if "fund_text" in entry and "p" in entry["fund_text"]:
                fund_ack["text"] = get_as_list(entry["fund_text"], "p")

            grants = get_as_list(entry["grants"], "grant")
        except:
            return fund_ack

        for grant in grants:
            grant_info = dict()
            grant_info["agency"] = get_entry_or_none(grant, "grant_agency")
            grant_info["ids"] = list()
            if "grant_ids" in grant:
                grant_info["ids"] = get_as_list(grant["grant_ids"], "grant_id")
            fund_ack["grants"].append(grant_info)
        return fund_ack

    @staticmethod
    def get_categories(data: dict) -> dict:
        """Extract categories.

        :param data: dictionary of web response.
        :return: categories dictionary.
        """

        category_info = dict()
        try:
            entry = data["static_data"]["fullrecord_metadata"]["category_info"]
        except:
            return category_info

        entry = data["static_data"]["fullrecord_metadata"]["category_info"]
        category_info["headings"] = get_as_list_or_none(entry, "headings", "heading")
        category_info["subheadings"] = get_as_list_or_none(entry, "subheadings", "subheading")

        subject_list = list()
        subjects = get_as_list_or_none(entry, "subjects", "subject")
        for subject in subjects:
            subject_dict = dict()
            subject_dict["ascatype"] = get_entry_or_none(subject, "@ascatype")
            subject_dict["code"] = get_entry_or_none(subject, "@code")
            subject_dict["text"] = get_entry_or_none(subject, "#text")
            subject_list.append(subject_dict)
        category_info["subjects"] = subject_list

        return category_info

    @staticmethod
    def parse_json(*, data: dict, snapshot_date: pendulum.DateTime, institution_ids: List[str]) -> dict:
        """Turn json data into db schema format.

        :param data: dictionary of web response.
        :param snapshot_date: Dataset release date.
        :param institution_ids: List of institution ids used in the query.
        :return: dict of data in right field format.
        """

        entry = dict()
        entry["snapshot_date"] = snapshot_date.date().isoformat()
        entry["identifiers"] = WosJsonParser.get_identifiers(data)
        entry["pub_info"] = WosJsonParser.get_pub_info(data)
        entry["title"] = WosJsonParser.get_title(data)
        entry["names"] = WosJsonParser.get_names(data)
        entry["languages"] = WosJsonParser.get_languages(data)
        entry["ref_count"] = WosJsonParser.get_refcount(data)
        entry["abstract"] = WosJsonParser.get_abstract(data)
        entry["keywords"] = WosJsonParser.get_keyword(data)
        entry["conferences"] = WosJsonParser.get_conference(data)
        entry["fund_ack"] = WosJsonParser.get_fund_ack(data)
        entry["categories"] = WosJsonParser.get_categories(data)
        entry["orgs"] = WosJsonParser.get_orgs(data)
        entry["institution_ids"] = institution_ids

        return entry


def schema_check(schema: str):
    """Check that the schema hasn't changed. Throw on different schema.

    :param schema: Schema string from HTTP response.
    """

    if schema != WebOfScienceRelease.EXPECTED_SCHEMA:
        raise AirflowException(
            f"Schema change detected. Expected: {WebOfScienceRelease.EXPECTED_SCHEMA}, received: {schema}"
        )


def transform_xml_to_json(xml_file: str) -> Union[dict, list]:
    """Transform XML response to JSON. Throw if schema has changed.

    :param xml_file: XML file of the API response.
    :return: Converted dict or list of the response.
    """

    xml_data = load_file(xml_file)
    records, schema = WosUtility.parse_query(xml_data)
    schema_check(schema)
    return records


def transform_to_db_format(records: list, snapshot_date: pendulum.Date, institution_ids: List[str]) -> List[dict]:
    """Convert the json response to the expected schema.

    :param records: List of the records as json.
    :param harvest_datetime: Timestamp of when the API call was made.
    :return: List of transformed entries.
    """

    entries = []
    for data in records:
        entry = WosJsonParser.parse_json(
            data=data,
            snapshot_date=snapshot_date,
            institution_ids=institution_ids,
        )
        entries.append(entry)

    return entries
