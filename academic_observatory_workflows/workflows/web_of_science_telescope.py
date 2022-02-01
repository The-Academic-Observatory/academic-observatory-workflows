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

# Author: Tuan Chien


import logging
import os
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from math import floor
from typing import Any, Dict, List, Tuple, Type, Union

import backoff
import jsonlines
import pendulum
import xmltodict
from academic_observatory_workflows.config import schema_folder as default_schema_folder
from airflow.exceptions import AirflowException
from google.cloud.bigquery import WriteDisposition
from observatory.platform.utils.airflow_utils import (
    AirflowConns,
    AirflowVars,
    get_airflow_connection_login,
    get_airflow_connection_password,
)
from observatory.platform.utils.file_utils import load_file, write_to_file
from observatory.platform.utils.workflow_utils import (
    blob_name,
    bq_load_shard,
    build_schedule,
    get_as_list,
    get_as_list_or_none,
    get_chunks,
    get_entry_or_none,
)
from observatory.platform.workflows.snapshot_telescope import (
    SnapshotRelease,
    SnapshotTelescope,
)
from ratelimit import limits, sleep_and_retry
from suds import WebFault
from wos import WosClient


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
    ) -> List[str]:
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
    ) -> List[str]:
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
    ) -> List[str]:
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
    """Helper methods to process the the converted json from Web of Science."""

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
    def parse_json(*, data: dict, harvest_datetime: str, release_date: str, institution_ids: List[str]) -> dict:
        """Turn json data into db schema format.

        :param data: dictionary of web response.
        :param harvest_datetime: isoformat string of time the fetch took place.
        :param release_date: Dataset release date.
        :param institution_ids: List of institution ids used in the query.
        :return: dict of data in right field format.
        """

        entry = dict()
        entry["harvest_datetime"] = harvest_datetime
        entry["release_date"] = release_date
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


class WebOfScienceRelease(SnapshotRelease):
    API_URL = "http://scientific.thomsonreuters.com"
    EXPECTED_SCHEMA = "http://scientific.thomsonreuters.com/schema/wok5.4/public/FullRecord"

    def __init__(
        self,
        *,
        dag_id: str,
        release_date: pendulum.DateTime,
        login: str,
        password: str,
        institution_ids: List[str],
        earliest_date: pendulum.DateTime,
    ):
        """Construct an UnpaywallSnapshotRelease instance.

        :param dag_id: The DAG ID.
        :param release_date: Release date.
        :param login: WoS login.
        :param password: WoS password.
        :param institution_ids: List of institution IDs to query.
        :param earliest_date: Earliest date to query from.
        """

        super().__init__(
            dag_id=dag_id,
            release_date=release_date,
        )

        self.table_id = WebOfScienceTelescope.DAG_ID
        self.login = login
        self.password = password
        self.institution_ids = institution_ids
        self.earliest_date = earliest_date

    def download(self):
        """Download a Web of Science live snapshot."""

        self.harvest_datetime = pendulum.now("UTC")
        schedule = build_schedule(self.earliest_date, self.release_date)
        WosUtility.download_wos_parallel(
            login=self.login,
            password=self.password,
            schedule=schedule,
            conn=self.dag_id,
            institution_ids=self.institution_ids,
            download_dir=self.download_folder,
        )

    def transform(self):
        """Convert the XML response into BQ friendly jsonlines."""

        for xml_file in self.download_files:
            records = self._transform_xml_to_json(xml_file)
            harvest_datetime = self._get_harvest_datetime(xml_file)
            entries = self._transform_to_db_format(records=records, harvest_datetime=harvest_datetime)
            self._write_transform_files(entries=entries, xml_file=xml_file)

    def _schema_check(self, schema: str):
        """Check that the schema hasn't changed. Throw on different schema.

        :param schema: Schema string from HTTP response.
        """

        if schema != WebOfScienceRelease.EXPECTED_SCHEMA:
            raise AirflowException(
                f"Schema change detected. Expected: {WebOfScienceRelease.EXPECTED_SCHEMA}, received: {schema}"
            )

    def _get_harvest_datetime(self, filepath: str) -> str:
        """Get the harvest datetime from the filename. <startdate>_<enddate>_<page>_<timestamp>.xml

        :param filepath: XML file path.
        :return: Harvest datetime string.
        """

        filename = os.path.basename(filepath)
        file_tokens = filename.split("_")
        return file_tokens[3][:-4]

    def _transform_xml_to_json(self, xml_file: str) -> Union[dict, list]:
        """Transform XML response to JSON. Throw if schema has changed.

        :param xml_file: XML file of the API response.
        :return: Converted dict or list of the response.
        """

        xml_data = load_file(xml_file)
        records, schema = WosUtility.parse_query(xml_data)
        self._schema_check(schema)
        return records

    def _transform_to_db_format(self, records: list, harvest_datetime: str) -> List[dict]:
        """Convert the json response to the expected schema.

        :param records: List of the records as json.
        :param harvest_datetime: Timestamp of when the API call was made.
        :return: List of transformed entries.
        """

        entries = []
        for data in records:
            entry = WosJsonParser.parse_json(
                data=data,
                harvest_datetime=harvest_datetime,
                release_date=self.release_date.date().isoformat(),
                institution_ids=self.institution_ids,
            )

            entries.append(entry)

        return entries

    def _write_transform_files(self, *, entries: Union[Dict, List], xml_file: str):
        """Save the schema compatible dictionaries as jsonlines.

        :param entries: List of schema compatible entries.
        :param xml_file: The filepath to the xml file of API response.
        :param index: Index to use as the end of the name.
        """

        # Strip out the harvest time stamp from the filename so that schema detection works
        filename = os.path.basename(xml_file)
        filename = f"{filename[:23]}.jsonl"
        filename = f"{WebOfScienceTelescope.DAG_ID}.{filename}"
        dst_file = os.path.join(self.transform_folder, filename)

        with jsonlines.open(dst_file, mode="w") as writer:
            writer.write_all(entries)


class WebOfScienceTelescope(SnapshotTelescope):
    DAG_ID = "web_of_science"
    TABLE_DESCRIPTION = (
        "The Web of Science citation database: https://clarivate.com/webofsciencegroup/solutions/web-of-science"
    )

    def __init__(
        self,
        *,
        dag_id: str,
        airflow_conns: List[AirflowConns],
        airflow_vars: List[AirflowVars],
        institution_ids: List[str],
        earliest_date: pendulum.DateTime = pendulum.datetime(1800, 1, 1),
        start_date: pendulum.DateTime = pendulum.datetime(2018, 5, 14),
        schedule_interval: str = "@monthly",
        dataset_id: str = "clarivate",
        schema_folder: str = default_schema_folder(),
        catchup: bool = False,
    ):
        """Web of Science telescope.

        :param dag_id: the id of the DAG.
        :param start_date: the start date of the DAG.
        :param schedule_interval: the schedule interval of the DAG.
        :param dataset_id: the dataset id.
        :param schema_folder: the SQL schema path.
        :param airflow_vars: list of airflow variable keys to check the existence of
        :param airflow_conns: list of airflow connection ids to check the existence of
        :param institution_ids: list of institution IDs to use for the WoS search query.
        :param earliest_date: earliest date to query for results.
        :param catchup: whether to use catchup on missed runs.
        """

        load_bigquery_table_kwargs = {
            "write_disposition": WriteDisposition.WRITE_APPEND,
            "ignore_unknown_values": True
        }

        super().__init__(
            dag_id,
            start_date,
            schedule_interval,
            dataset_id,
            schema_folder,
            table_descriptions={dag_id: WebOfScienceTelescope.TABLE_DESCRIPTION},
            catchup=catchup,
            airflow_vars=airflow_vars,
            airflow_conns=airflow_conns,
            load_bigquery_table_kwargs=load_bigquery_table_kwargs,
        )

        if len(airflow_conns) == 0:
            raise AirflowException("You need to supply an Airflow connection with the login credentials.")

        if len(institution_ids) == 0:
            raise AirflowException("You need to supply at least one institution id to search for in the query.")

        self.institution_ids = institution_ids
        self.earliest_date = earliest_date

        self.add_setup_task(self.check_dependencies)
        self.add_task(self.download)
        self.add_task(self.upload_downloaded)
        self.add_task(self.transform)
        self.add_task(self.upload_transformed)
        self.add_task(self.bq_load)
        self.add_task(self.cleanup)

    def make_release(self, **kwargs) -> List[WebOfScienceRelease]:
        """Make a list of WebOfScienceRelease instances.

        :param kwargs: The context passed from the PythonOperator.
        :return: WebOfScienceRelease instance.
        """

        release_date = pendulum.now("UTC")
        conn = self.airflow_conns[0]
        login = get_airflow_connection_login(conn)
        password = get_airflow_connection_password(conn)

        release = WebOfScienceRelease(
            dag_id=self.dag_id,
            release_date=release_date,
            login=login,
            password=password,
            institution_ids=self.institution_ids,
            earliest_date=self.earliest_date,
        )
        return [release]

    def bq_load(self, releases: List[SnapshotRelease], **kwargs):
        """Task to load each transformed release to BigQuery.
        The table_id is set to the file name without the extension.
        :param releases: a list of releases.
        :return: None.
        """

        # Load each transformed release
        for release in releases:
            transform_blob = f"{blob_name(release.transform_folder)}/*"
            table_description = self.table_descriptions.get(self.dag_id, "")
            bq_load_shard(
                self.schema_folder,
                release.release_date,
                transform_blob,
                self.dataset_id,
                WebOfScienceTelescope.DAG_ID,
                self.source_format,
                prefix=self.schema_prefix,
                schema_version=self.schema_version,
                dataset_description=self.dataset_description,
                table_description=table_description,
                **self.load_bigquery_table_kwargs,
            )
