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


import json
import logging
import os
import unittest
from collections import OrderedDict
from typing import OrderedDict
from unittest.mock import MagicMock, call, patch

import observatory.api.server.orm as orm
import pendulum
from academic_observatory_workflows.config import test_fixtures_folder
from academic_observatory_workflows.workflows.web_of_science_telescope import (
    WebOfScienceRelease,
    WebOfScienceTelescope,
    WosJsonParser,
    WosNameAttributes,
    WosUtilConst,
    WosUtility,
)
from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.utils.state import State
from click.testing import CliRunner
from observatory.platform.utils.airflow_utils import AirflowConns, AirflowVars
from observatory.platform.utils.api import make_observatory_api
from observatory.platform.utils.gc_utils import run_bigquery_query
from observatory.platform.utils.test_utils import (
    HttpServer,
    ObservatoryEnvironment,
    ObservatoryTestCase,
    module_file_path,
)
from observatory.platform.utils.workflow_utils import (
    bigquery_sharded_table_id,
    blob_name,
    make_dag_id,
)


class TestWosUtility(unittest.TestCase):
    """Test WosUtility."""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        super(TestWosUtility, self).__init__(*args, **kwargs)

    def test_build_query(self):
        institution_ids = ["test1", "test2"]
        start_date = pendulum.datetime(2021, 1, 1)
        end_date = pendulum.datetime(2021, 2, 1)
        period = pendulum.period(start_date, end_date)
        query = WosUtility.build_query(institution_ids=institution_ids, period=period)

        expected_query = OrderedDict(
            [
                ("query", "OG=(test1 OR test2)"),
                ("count", WosUtilConst.RESULT_LIMIT),
                ("offset", 1),
                ("timeSpan", {"begin": start_date.isoformat(), "end": end_date.isoformat()}),
            ]
        )

        self.assertEqual(query, expected_query)

    @patch("academic_observatory_workflows.workflows.web_of_science_telescope.xmltodict.parse")
    def test_parse_query_none(self, m_xmlparse):
        expected_schema_version = "schema version"
        m_xmlparse.return_value = {"records": {"@xmlns": expected_schema_version, "REC": []}}
        records, schema_ver = WosUtility.parse_query(None)
        self.assertEqual(records, [])
        self.assertEqual(schema_ver, expected_schema_version)

    @patch("academic_observatory_workflows.workflows.web_of_science_telescope.xmltodict.parse")
    def test_parse_query(self, m_xmlparse):
        expected_schema_version = "schema version"
        m_xmlparse.return_value = {"records": {"@xmlns": expected_schema_version, "REC": [1, 2, 3]}}
        records, schema_ver = WosUtility.parse_query(None)
        self.assertEqual(records, [1, 2, 3])
        self.assertEqual(schema_ver, expected_schema_version)

    def test_search(self):
        institution_ids = ["test1"]
        start_date = pendulum.datetime(2021, 1, 1)
        end_date = pendulum.datetime(2021, 2, 1)
        period = pendulum.period(start_date, end_date)
        query = WosUtility.build_query(institution_ids=institution_ids, period=period)
        client = MagicMock()

        WosUtility.search(client=client, query=query)
        expected_call = call.search(
            query="OG=(test1)",
            count=100,
            offset=1,
            timeSpan={"begin": "2021-01-01T00:00:00+00:00", "end": "2021-02-01T00:00:00+00:00"},
        )
        self.assertEqual(client.method_calls[0], expected_call)

    @patch("academic_observatory_workflows.workflows.web_of_science_telescope.WosUtility.search")
    def test_make_query_not_limit(self, m_search):
        results = MagicMock()
        results.recordsFound = 2
        results.records = ""
        m_search.return_value = results
        client = MagicMock()
        institution_ids = ["test1"]
        start_date = pendulum.datetime(2021, 1, 1)
        end_date = pendulum.datetime(2021, 1, 31)
        period = pendulum.period(start_date, end_date)
        query = WosUtility.build_query(institution_ids=institution_ids, period=period)

        records = WosUtility.make_query(client=client, query=query)
        self.assertEqual(records, [""])

    @patch("academic_observatory_workflows.workflows.web_of_science_telescope.WosUtility.search")
    def test_make_query_over_limit(self, m_search):
        results = MagicMock()
        results.recordsFound = 200
        results.records = ""
        m_search.return_value = results
        client = MagicMock()
        institution_ids = ["test1"]
        start_date = pendulum.datetime(2021, 1, 1)
        end_date = pendulum.datetime(2021, 1, 31)
        period = pendulum.period(start_date, end_date)
        query = WosUtility.build_query(institution_ids=institution_ids, period=period)

        records = WosUtility.make_query(client=client, query=query)
        self.assertEqual(records, ["", ""])

    @patch("academic_observatory_workflows.workflows.web_of_science_telescope.write_to_file")
    @patch("academic_observatory_workflows.workflows.web_of_science_telescope.WosUtility.search")
    def test_download_wos_period(self, m_search, m_write_file):
        results = MagicMock()
        results.recordsFound = 100
        results.records = ""
        m_search.return_value = results

        client = MagicMock()
        conn = ""
        start_date = pendulum.datetime(2021, 1, 1)
        end_date = pendulum.datetime(2021, 1, 31)
        period = pendulum.period(start_date.date(), end_date.date())

        with CliRunner().isolated_filesystem() as tmpdir:
            WosUtility.download_wos_period(
                client=client, conn=conn, period=period, institution_ids=[""], download_dir=tmpdir
            )
            self.assertEqual(m_write_file.call_count, 1)
            args, _ = m_write_file.call_args
            self.assertEqual(args[0], "")

    @patch("academic_observatory_workflows.workflows.web_of_science_telescope.write_to_file")
    @patch("academic_observatory_workflows.workflows.web_of_science_telescope.WosUtility.search")
    @patch("academic_observatory_workflows.workflows.web_of_science_telescope.WosClient")
    def test_download_wos_batch(self, m_client, m_search, m_write_file):
        m_client.return_value.__enter__.return_value.name = MagicMock()
        results = MagicMock()
        results.recordsFound = 100
        results.records = ""
        m_search.return_value = results

        batch = [
            pendulum.period(pendulum.datetime(2021, 1, 1).date(), pendulum.datetime(2021, 1, 31).date()),
            pendulum.period(pendulum.datetime(2021, 2, 1).date(), pendulum.datetime(2021, 2, 28).date()),
        ]

        with CliRunner().isolated_filesystem() as tmpdir:
            WosUtility.download_wos_batch(
                login="login", password="pass", batch=batch, conn="conn", institution_ids=[""], download_dir=tmpdir
            )

            self.assertEqual(m_write_file.call_count, 2)
            self.assertEqual(m_write_file.call_args_list[0][0][0], "")
            self.assertEqual(m_write_file.call_args_list[1][0][0], "")

    @patch("academic_observatory_workflows.workflows.web_of_science_telescope.WosUtility.download_wos_batch")
    def test_download_wos_parallel_single_session(self, m_download):
        schedule = [1, 2, 3, 4]
        WosUtility.download_wos_parallel(
            login="", password="", schedule=schedule, conn="", institution_ids=[""], download_dir=""
        )
        self.assertEqual(m_download.call_count, 1)
        self.assertEqual(m_download.call_args_list[0][1]["batch"], schedule)

    @patch("academic_observatory_workflows.workflows.web_of_science_telescope.WosUtility.download_wos_batch")
    def test_download_wos_parallel_multi_session(self, m_download):
        schedule = [1, 2, 3, 4, 5, 6]
        WosUtility.download_wos_parallel(
            login="", password="", schedule=schedule, conn="", institution_ids=[""], download_dir=""
        )
        self.assertEqual(m_download.call_count, 5)
        self.assertEqual(m_download.call_args_list[0][1]["batch"], [1, 6])
        self.assertEqual(m_download.call_args_list[1][1]["batch"], [2])
        self.assertEqual(m_download.call_args_list[2][1]["batch"], [3])
        self.assertEqual(m_download.call_args_list[3][1]["batch"], [4])
        self.assertEqual(m_download.call_args_list[4][1]["batch"], [5])

    @patch("academic_observatory_workflows.workflows.web_of_science_telescope.WosUtility.download_wos_batch")
    def test_download_wos_parallel_multi_session_no_remainder(self, m_download):
        schedule = [1, 2, 3, 4, 5]
        WosUtility.download_wos_parallel(
            login="", password="", schedule=schedule, conn="", institution_ids=[""], download_dir=""
        )
        self.assertEqual(m_download.call_count, 5)
        self.assertEqual(m_download.call_args_list[0][1]["batch"], [1])
        self.assertEqual(m_download.call_args_list[1][1]["batch"], [2])
        self.assertEqual(m_download.call_args_list[2][1]["batch"], [3])
        self.assertEqual(m_download.call_args_list[3][1]["batch"], [4])
        self.assertEqual(m_download.call_args_list[4][1]["batch"], [5])

    @patch("academic_observatory_workflows.workflows.web_of_science_telescope.WosUtility.download_wos_batch")
    def test_download_wos_sequential(self, m_download):
        schedule = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        WosUtility.download_wos_sequential(
            login="", password="", schedule=schedule, conn="", institution_ids=[""], download_dir=""
        )
        self.assertEqual(m_download.call_count, 1)
        self.assertEqual(m_download.call_args_list[0][1]["batch"], schedule)


class TestWosNameAttributes(unittest.TestCase):
    """Test the WosNameAttributes class."""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        super(TestWosNameAttributes, self).__init__(*args, **kwargs)

    def test_get_contribs_blank(self):
        data = {}
        wna = WosNameAttributes(data)
        self.assertEqual(wna._contribs, {})

        data = {"static_data": {}}
        wna = WosNameAttributes(data)
        self.assertEqual(wna._contribs, {})

    def test_no_name(self):
        data = {}
        wna = WosNameAttributes(data)

        orcid = wna.get_orcid("no name")
        self.assertEqual(orcid, None)

        rid = wna.get_r_id("no name")
        self.assertEqual(rid, None)

        data = {
            "static_data": {
                "contributors": {
                    "contributor": [
                        {"name": {"first_name": "first", "last_name": "last", "@r_id": "rid", "@orcid_id": "orcid"}}
                    ]
                }
            }
        }

        wna = WosNameAttributes(data)

        orcid = wna.get_orcid("no name")
        self.assertEqual(orcid, None)

        rid = wna.get_r_id("no name")
        self.assertEqual(rid, None)

    def test_no_orcid_no_rid(self):
        data = {
            "static_data": {
                "contributors": {
                    "contributor": [
                        {
                            "name": {
                                "first_name": "first",
                                "last_name": "last",
                            }
                        }
                    ]
                }
            }
        }
        wna = WosNameAttributes(data)
        self.assertEqual(wna._contribs, {"first last": {}})

    def test_orcid_rid(self):
        data = {
            "static_data": {
                "contributors": {
                    "contributor": [
                        {"name": {"first_name": "first", "last_name": "last", "@r_id": "rid", "@orcid_id": "orcid"}}
                    ]
                }
            }
        }
        wna = WosNameAttributes(data)
        self.assertEqual(wna._contribs, {"first last": {"r_id": "rid", "orcid": "orcid"}})

        orcid = wna.get_orcid("first last")
        self.assertEqual(orcid, "orcid")

        rid = wna.get_r_id("first last")
        self.assertEqual(rid, "rid")


class TestWosParse(unittest.TestCase):
    """Test web of science response parsing."""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        super(TestWosParse, self).__init__(*args, **kwargs)

        self.fixtures_dir = test_fixtures_folder("web_of_science")
        self.fixture_file = "wos-2020-10-01.json"
        self.wos_2020_10_01_json_path = os.path.join(self.fixtures_dir, self.fixture_file)

        with open(self.wos_2020_10_01_json_path, "r") as f:
            self.data = json.load(f)

        self.harvest_datetime = pendulum.now().isoformat()
        self.release_date = pendulum.date(2020, 10, 1).isoformat()

    def test_get_identifiers(self):
        """Extract identifiers"""
        data = self.data[0]
        identifiers = WosJsonParser.get_identifiers(data)
        self.assertEqual(len(identifiers), 10)
        self.assertEqual(identifiers["uid"], "WOS:000000000000000")
        self.assertEqual(identifiers["issn"], "0000-0000")
        self.assertEqual(identifiers["eissn"], "0000-0000")
        self.assertEqual(identifiers["doi"], "10.0000/j.gaz.2020.01.001")

        data = {"UID": "ID"}
        identifiers = WosJsonParser.get_identifiers(data)
        expected_ids = {
            "parent_book_doi": None,
            "isbn": None,
            "art_no": None,
            "doi": None,
            "issn": None,
            "eissn": None,
            "eisbn": None,
            "meeting_abs": None,
            "xref_doi": None,
            "uid": "ID",
        }
        self.assertEqual(expected_ids, identifiers)

    def test_get_identifiers_types(self):
        data = {
            "UID": "ID",
            "dynamic_data": {
                "cluster_related": {
                    "identifiers": {
                        "identifier": [
                            {"@type": "bad_type", "@value": "something"},
                            {"@type": "isbn", "@value": "isbn"},
                        ]
                    }
                }
            },
        }
        identifiers = WosJsonParser.get_identifiers(data)
        expected_ids = {
            "parent_book_doi": None,
            "isbn": "isbn",
            "art_no": None,
            "doi": None,
            "issn": None,
            "eissn": None,
            "eisbn": None,
            "meeting_abs": None,
            "xref_doi": None,
            "uid": "ID",
        }
        self.assertEqual(expected_ids, identifiers)

    def test_get_pub_info(self):
        """Extract publication info"""
        data = self.data[0]
        pub_info = WosJsonParser.get_pub_info(data)
        self.assertEqual(pub_info["sort_date"], "2020-01-01")
        self.assertEqual(pub_info["pub_type"], "Journal")
        self.assertEqual(pub_info["page_count"], 2)
        self.assertEqual(pub_info["source"], "JUPITER GAZETTE")
        self.assertEqual(pub_info["doc_type"], "Article")
        self.assertEqual(pub_info["publisher"], "JUPITER PUBLISHING LTD")
        self.assertEqual(pub_info["publisher_city"], "SPRINGFIELD")

    def test_get_pub_info_no_fields(self):
        expected_pub_info = {
            "sort_date": None,
            "pub_type": None,
            "page_count": None,
            "source": None,
            "doc_type": None,
            "publisher": None,
            "publisher_city": None,
        }

        data = {}
        pub_info = WosJsonParser.get_pub_info(data)
        self.assertEqual(expected_pub_info, pub_info)

        data = {"static_data": {"summary": {}}}
        pub_info = WosJsonParser.get_pub_info(data)
        self.assertEqual(expected_pub_info, pub_info)

    def test_get_pub_info_no_title(self):
        expected_pub_info = {
            "sort_date": None,
            "pub_type": None,
            "page_count": None,
            "source": None,
            "doc_type": None,
            "publisher": None,
            "publisher_city": None,
        }

        data = {"static_data": {"summary": {"titles": {"title": []}}}}
        pub_info = WosJsonParser.get_pub_info(data)
        self.assertEqual(expected_pub_info, pub_info)

    def test_get_pub_info_non_source_title(self):
        expected_pub_info = {
            "sort_date": None,
            "pub_type": None,
            "page_count": None,
            "source": None,
            "doc_type": None,
            "publisher": None,
            "publisher_city": None,
        }

        data = {"static_data": {"summary": {"titles": {"title": [{"@type": "notsource"}]}}}}
        pub_info = WosJsonParser.get_pub_info(data)
        self.assertEqual(expected_pub_info, pub_info)

    def test_get_title(self):
        """Extract title"""
        data = self.data[0]
        title = WosJsonParser.get_title(data)
        truth = (
            "The habitats of endangered hypnotoads on the southern oceans of Europa: a Ophiophagus hannah perspective"
        )
        self.assertEqual(title, truth)

    def test_get_title_key_error(self):
        data = {}
        title = WosJsonParser.get_title(data)
        self.assertEqual(title, None)

    def test_get_title_no_titles(self):
        data = {"static_data": {"summary": {"titles": {"title": []}}}}
        title = WosJsonParser.get_title(data)
        self.assertEqual(title, None)

    def test_get_names(self):
        """Extract name information, e.g. authors"""
        data = self.data[0]
        names = WosJsonParser.get_names(data)
        self.assertEqual(len(names), 3)

        entry = names[0]
        self.assertEqual(entry["seq_no"], 1)
        self.assertEqual(entry["role"], "author")
        self.assertEqual(entry["first_name"], "Big Eaty")
        self.assertEqual(entry["last_name"], "Snake")
        self.assertEqual(entry["wos_standard"], "Snake, BE")
        self.assertEqual(entry["daisng_id"], "101010")
        self.assertEqual(entry["full_name"], "Snake, Big Eaty")
        self.assertEqual(entry["orcid"], "0000-0000-0000-0001")
        self.assertEqual(entry["r_id"], "D-0000-2000")

        entry = names[1]
        self.assertEqual(entry["seq_no"], 2)
        self.assertEqual(entry["role"], "author")
        self.assertEqual(entry["first_name"], "Hypno")
        self.assertEqual(entry["last_name"], "Toad")
        self.assertEqual(entry["wos_standard"], "Toad, H")
        self.assertEqual(entry["daisng_id"], "100000")
        self.assertEqual(entry["full_name"], "Toad, Hypno")
        self.assertEqual(entry["orcid"], "0000-0000-0000-0002")
        self.assertEqual(entry["r_id"], "H-0000-2001")

        entry = names[2]
        self.assertEqual(entry["seq_no"], 3)
        self.assertEqual(entry["role"], "author")
        self.assertEqual(entry["first_name"], "Great")
        self.assertEqual(entry["last_name"], "Historian")
        self.assertEqual(entry["wos_standard"], "Historian, G")
        self.assertEqual(entry["daisng_id"], "200000")
        self.assertEqual(entry["full_name"], "Historian, Great")
        self.assertEqual(entry["orcid"], "0000-0000-0000-0003")
        self.assertEqual(entry["r_id"], None)

    def test_get_names_no_fields(self):
        """Extract name information, e.g. authors"""
        data = {}
        names = WosJsonParser.get_names(data)
        self.assertEqual(names, [])

    def test_get_languages(self):
        """Extract language information"""
        data = self.data[0]
        languages = WosJsonParser.get_languages(data)
        self.assertEqual(len(languages), 1)
        self.assertEqual(languages[0]["type"], "primary")
        self.assertEqual(languages[0]["name"], "Mindwaves")

    def test_get_languages_no_field(self):
        data = {}
        languages = WosJsonParser.get_languages(data)
        self.assertEqual(languages, [])

    def test_get_refcount(self):
        """Extract reference count"""
        data = self.data[0]
        refs = WosJsonParser.get_refcount(data)
        self.assertEqual(refs, 10000)

    def test_get_refcount_no_field(self):
        data = {}
        refs = WosJsonParser.get_refcount(data)
        self.assertEqual(refs, None)

    def test_get_abstract(self):
        """Extract the abstract"""
        data = self.data[0]
        abstract = WosJsonParser.get_abstract(data)
        self.assertEqual(len(abstract), 1)
        head = abstract[0][0:38]
        truth = "Jupiter hypnotoads lead mysterious liv"
        self.assertEqual(head, truth)
        self.assertEqual(len(abstract[0]), 169)

    def test_get_abstract_no_field(self):
        data = {}
        abstract = WosJsonParser.get_abstract(data)
        self.assertEqual(abstract, [])

    def test_get_keyword(self):
        """Extract keywords and keywords plus if available"""
        data = self.data[0]
        keywords = WosJsonParser.get_keyword(data)
        self.assertEqual(len(keywords), 15)
        word_list = [
            "Jupiter",
            "Toads",
            "Snakes",
            "JPT",
            "JPS",
            "WORD1",
            "WORD2",
            "WORD3",
            "WORD4",
            "WORD5",
            "WORD6",
            "WORD7",
            "WORD8",
            "WORD9",
            "WORD0",
        ]
        self.assertListEqual(keywords, word_list)

    def test_get_keyword_no_field(self):
        data = {}
        keywords = WosJsonParser.get_keyword(data)
        self.assertEqual(keywords, [])

    def test_get_keyword_no_keyword_plus(self):
        data = {"static_data": {"fullrecord_metadata": {"keywords": {"keyword": []}}}}
        keywords = WosJsonParser.get_keyword(data)
        self.assertEqual(keywords, [])

    def test_get_conference(self):
        """Extract conference name"""
        data = self.data[0]
        conf = WosJsonParser.get_conference(data)
        name = "Annual Jupiter Meeting of the Minds"
        self.assertEqual(len(conf), 1)
        self.assertEqual(conf[0]["name"], name)
        self.assertEqual(conf[0]["id"], 12345)

    def test_get_conference_no_field(self):
        data = {}
        conf = WosJsonParser.get_conference(data)
        self.assertEqual(conf, [])

    def test_get_conference_no_confid(self):
        data = {"static_data": {"summary": {"conferences": {"conference": [{}]}}}}
        conf = WosJsonParser.get_conference(data)
        self.assertEqual(conf, [{"id": None, "name": None}])

    def test_get_fund_ack(self):
        """Extract funding information"""
        data = self.data[0]
        fund_ack = WosJsonParser.get_fund_ack(data)
        truth = "The authors would like to thank all life in the universe for not making us extinct yet."
        self.assertEqual(len(fund_ack["text"]), 1)
        self.assertEqual(fund_ack["text"][0], truth)
        self.assertEqual(len(fund_ack["grants"]), 1)
        self.assertEqual(fund_ack["grants"][0]["agency"], "Jupiter research council")
        self.assertEqual(len(fund_ack["grants"][0]["ids"]), 1)
        self.assertEqual(fund_ack["grants"][0]["ids"][0], "JP00000000HT1")

    def test_get_fund_ack_no_fund_text(self):
        data = {"static_data": {"fullrecord_metadata": {"fund_ack": {}}}}
        fund_ack = WosJsonParser.get_fund_ack(data)
        self.assertEqual(fund_ack, {"grants": [], "text": []})

    def test_get_fund_ack_fund_ack(self):
        data = {"static_data": {"fullrecord_metadata": {}}}
        fund_ack = WosJsonParser.get_fund_ack(data)
        self.assertEqual(fund_ack, {"grants": [], "text": []})

    def test_get_fund_ack_no_grantid(self):
        data = {
            "static_data": {"fullrecord_metadata": {"fund_ack": {"grants": {"grant": [{"grant_agency": "agency"}]}}}}
        }
        fund_ack = WosJsonParser.get_fund_ack(data)
        self.assertEqual(fund_ack, {"grants": [{"agency": "agency", "ids": []}], "text": []})

    def test_get_categories(self):
        """Extract WoS categories"""
        data = self.data[0]
        categories = WosJsonParser.get_categories(data)
        self.assertEqual(len(categories["headings"]), 1)
        self.assertEqual(len(categories["subheadings"]), 1)
        self.assertEqual(len(categories["subjects"]), 3)

        self.assertEqual(categories["headings"][0], "Hynology")
        self.assertEqual(categories["subheadings"][0], "Zoology")

        self.assertDictEqual(
            categories["subjects"][0], {"ascatype": "traditional", "code": "XX", "text": "Jupiter Toads"}
        )
        self.assertDictEqual(
            categories["subjects"][1], {"ascatype": "traditional", "code": "X", "text": "Jupiter life"}
        )
        self.assertDictEqual(
            categories["subjects"][2], {"ascatype": "extended", "code": None, "text": "Jupiter Science"}
        )

    def test_get_categories_no_fields(self):
        data = {}
        categories = WosJsonParser.get_categories(data)
        self.assertEqual(categories, {})

    def test_get_orgs(self):
        """Extract Wos organisations"""
        data = self.data[0]
        orgs = WosJsonParser.get_orgs(data)
        self.assertEqual(len(orgs), 1)
        self.assertEqual(orgs[0]["city"], "Springfield")
        self.assertEqual(orgs[0]["state"], "SF")
        self.assertEqual(orgs[0]["country"], "Jupiter")
        self.assertEqual(orgs[0]["org_name"], "Generic University")
        self.assertEqual(len(orgs[0]["suborgs"]), 2)
        self.assertEqual(orgs[0]["suborgs"][0], "Centre of Excellence for Extraterrestrial Telepathic Studies")
        self.assertEqual(orgs[0]["suborgs"][1], "Zoology")
        self.assertEqual(len(orgs[0]["names"]), 3)
        self.assertEqual(orgs[0]["names"][0]["first_name"], "Big Eaty")
        self.assertEqual(orgs[0]["names"][0]["last_name"], "Snake")
        self.assertEqual(orgs[0]["names"][0]["daisng_id"], "101010")
        self.assertEqual(orgs[0]["names"][0]["full_name"], "Snake, Big Eaty")
        self.assertEqual(orgs[0]["names"][0]["wos_standard"], "Snake, BE")
        self.assertEqual(orgs[0]["names"][1]["first_name"], "Hypno")
        self.assertEqual(orgs[0]["names"][1]["last_name"], "Toad")
        self.assertEqual(orgs[0]["names"][1]["daisng_id"], "100000")
        self.assertEqual(orgs[0]["names"][1]["full_name"], "Toad, Hypno")
        self.assertEqual(orgs[0]["names"][1]["wos_standard"], "Toad, H")
        self.assertEqual(orgs[0]["names"][2]["first_name"], "Great")
        self.assertEqual(orgs[0]["names"][2]["last_name"], "Historian")
        self.assertEqual(orgs[0]["names"][2]["daisng_id"], "200000")
        self.assertEqual(orgs[0]["names"][2]["full_name"], "Historian, Great")
        self.assertEqual(orgs[0]["names"][2]["wos_standard"], "Historian, G")

    def test_get_orgs_no_addr(self):
        data = {"static_data": {"fullrecord_metadata": {"addresses": {"address_name": [{"address_spec": {}}]}}}}
        orgs = WosJsonParser.get_orgs(data)
        self.assertEqual(orgs, [{"city": None, "country": None, "state": None}])

    def test_get_orgs_no_field(self):
        data = {"static_data": {}}

        orgs = WosJsonParser.get_orgs(data)
        self.assertEqual(orgs, [])

    def test_get_orgs_no_orgs(self):
        data = {
            "static_data": {
                "fullrecord_metadata": {
                    "addresses": {"address_name": [{"address_spec": {"organizations": {"organization": []}}}]}
                }
            }
        }

        orgs = WosJsonParser.get_orgs(data)
        self.assertEqual(orgs, [{"city": None, "country": None, "org_name": None, "state": None}])

    def test_parse_json(self):
        """Test whether the json file can be parsed into fields correctly."""

        self.assertEqual(len(self.data), 1)
        entry = self.data[0]

        wos_inst_id = ["Generic University"]
        entry = WosJsonParser.parse_json(
            data=entry,
            harvest_datetime=self.harvest_datetime,
            release_date=self.release_date,
            institution_ids=wos_inst_id,
        )
        self.assertEqual(entry["harvest_datetime"], self.harvest_datetime)
        self.assertEqual(entry["release_date"], self.release_date)
        self.assertEqual(entry["identifiers"]["uid"], "WOS:000000000000000")
        self.assertEqual(entry["pub_info"]["pub_type"], "Journal")
        self.assertEqual(
            entry["title"],
            "The habitats of endangered hypnotoads on the southern oceans of Europa: a Ophiophagus hannah perspective",
        )
        self.assertEqual(entry["names"][0]["first_name"], "Big Eaty")
        self.assertEqual(entry["languages"][0]["name"], "Mindwaves")
        self.assertEqual(entry["ref_count"], 10000)
        self.assertEqual(len(entry["abstract"][0]), 169)
        self.assertEqual(len(entry["keywords"]), 15)
        self.assertEqual(len(entry["conferences"]), 1)
        self.assertEqual(entry["fund_ack"]["grants"][0]["ids"][0], "JP00000000HT1")
        self.assertEqual(entry["categories"]["headings"][0], "Hynology")
        self.assertEqual(len(entry["orgs"]), 1)


class MockApiResponse:
    def __init__(self, file):
        fixture_dir = test_fixtures_folder("web_of_science")
        api_response_file = os.path.join(fixture_dir, file)
        with open(api_response_file, "r") as f:
            self.records = f.read()

        self.recordsFound = "1"


class TestWebOfScienceTelescope(ObservatoryTestCase):
    """Test the WebOfScienceTelescope."""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        super(TestWebOfScienceTelescope, self).__init__(*args, **kwargs)

        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.host = "localhost"
        self.api_port = 5000
        self.data_location = "us"

        self.org_name = "Curtin University"
        self.conn_id = "web_of_science_curtin_university"
        self.earliest_date = pendulum.datetime(2021, 1, 1).isoformat()

    def setup_api(self, env, extra=None):
        dt = pendulum.now("UTC")

        if extra is None:
            extra = {
                "airflow_connections": [self.conn_id],
                "institution_ids": ["Curtin University"],
                "earliest_date": self.earliest_date,
            }

        name = "Web of Science Telescope"
        telescope_type = orm.TelescopeType(name=name, type_id=WebOfScienceTelescope.DAG_ID, created=dt, modified=dt)
        env.api_session.add(telescope_type)

        organisation = orm.Organisation(
            name=self.org_name,
            created=dt,
            modified=dt,
            gcp_project_id=self.project_id,
            gcp_download_bucket=env.download_bucket,
            gcp_transform_bucket=env.transform_bucket,
        )

        env.api_session.add(organisation)
        telescope = orm.Telescope(
            name=name,
            telescope_type=telescope_type,
            organisation=organisation,
            modified=dt,
            created=dt,
            extra=extra,
        )
        env.api_session.add(telescope)
        env.api_session.commit()

    def setup_connections(self, env):
        # Add Observatory API connection
        conn = Connection(conn_id=AirflowConns.OBSERVATORY_API, uri=f"http://:password@{self.host}:{self.api_port}")
        env.add_connection(conn)

        # Add login/pass connection
        conn = Connection(conn_id=self.conn_id, uri=f"http://login:password@localhost")
        env.add_connection(conn)

    def get_telescope(self, dataset_id):
        api = make_observatory_api()
        telescope_type = api.get_telescope_type(type_id=WebOfScienceTelescope.DAG_ID)
        telescopes = api.get_telescopes(telescope_type_id=telescope_type.id, limit=1000)
        self.assertEqual(len(telescopes), 1)

        dag_id = make_dag_id(WebOfScienceTelescope.DAG_ID, telescopes[0].organisation.name)
        airflow_conns = telescopes[0].extra.get("airflow_connections")
        institution_ids = telescopes[0].extra.get("institution_ids")
        earliest_date_str = telescopes[0].extra.get("earliest_date")
        earliest_date = pendulum.parse(earliest_date_str)

        airflow_vars = [
            AirflowVars.DATA_PATH,
            AirflowVars.DATA_LOCATION,
        ]

        telescope = WebOfScienceTelescope(
            dag_id=dag_id,
            dataset_id=dataset_id,
            airflow_conns=airflow_conns,
            airflow_vars=airflow_vars,
            institution_ids=institution_ids,
            earliest_date=earliest_date,
        )

        return telescope

    def test_ctor(self):
        self.assertRaises(
            AirflowException,
            WebOfScienceTelescope,
            dag_id="dag",
            dataset_id="dataset",
            airflow_conns=[],
            airflow_vars=[],
            institution_ids=[],
        )

        self.assertRaises(
            AirflowException,
            WebOfScienceTelescope,
            dag_id="dag",
            dataset_id="dataset",
            airflow_conns=["conn"],
            airflow_vars=[],
            institution_ids=[],
        )

    def test_dag_structure(self):
        """Test that the Crossref Events DAG has the correct structure."""

        telescope = WebOfScienceTelescope(
            dag_id="web_of_science", airflow_conns=["conn"], airflow_vars=[], institution_ids=["123"]
        )
        dag = telescope.make_dag()
        self.assert_dag_structure(
            {
                "check_dependencies": ["download"],
                "download": ["upload_downloaded"],
                "upload_downloaded": ["transform"],
                "transform": ["upload_transformed"],
                "upload_transformed": ["bq_load"],
                "bq_load": ["cleanup"],
                "cleanup": [],
            },
            dag,
        )

    def test_dag_load(self):
        """Test that the DAG can be loaded from a DAG bag."""

        dag_file = os.path.join(module_file_path("academic_observatory_workflows.dags"), "web_of_science_telescope.py")
        env = ObservatoryEnvironment(self.project_id, self.data_location, api_host=self.host, api_port=self.api_port)

        with env.create():
            self.setup_connections(env)
            self.setup_api(env)

            dag_file = os.path.join(
                module_file_path("academic_observatory_workflows.dags"), "web_of_science_telescope.py"
            )
            dag_id = make_dag_id(WebOfScienceTelescope.DAG_ID, self.org_name)
            self.assert_dag_load(dag_id, dag_file)

    def test_dag_load_missing_params(self):
        """Make sure an exception is thrown if essential parameters are missing."""

        dag_file = os.path.join(module_file_path("academic_observatory_workflows.dags"), "web_of_science_telescope.py")
        env = ObservatoryEnvironment(self.project_id, self.data_location, api_host=self.host, api_port=self.api_port)
        with env.create():
            self.setup_connections(env)
            extra = {"airflow_connections": [self.conn_id]}
            self.setup_api(env, extra=extra)
            dag_file = os.path.join(
                module_file_path("academic_observatory_workflows.dags"), "web_of_science_telescope.py"
            )
            dag_id = make_dag_id(WebOfScienceTelescope.DAG_ID, self.org_name)
            self.assertRaises(AssertionError, self.assert_dag_load, dag_id, dag_file)

    def test_telescope_bad_schema(self):
        env = ObservatoryEnvironment(self.project_id, self.data_location, api_host=self.host, api_port=self.api_port)
        bad_api_response = MockApiResponse("api_response_diff_schema.xml")

        with env.create(task_logging=True):
            self.setup_connections(env)
            self.setup_api(env)
            dataset_id = env.add_dataset()
            execution_date = pendulum.datetime(2021, 1, 1)
            telescope = self.get_telescope(dataset_id)
            dag = telescope.make_dag()

            release_date = pendulum.datetime(2021, 2, 1)
            release = WebOfScienceRelease(
                dag_id=make_dag_id(WebOfScienceTelescope.DAG_ID, self.org_name),
                release_date=release_date,
                login="login",
                password="pass",
                institution_ids=["Curtin University"],
                earliest_date=pendulum.datetime(2021, 1, 1),
            )

            with env.create_dag_run(dag, execution_date):
                # check dependencies
                ti = env.run_task(telescope.check_dependencies.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # download
                with patch(
                    "academic_observatory_workflows.workflows.web_of_science_telescope.WosUtility.search"
                ) as m_search:
                    with patch(
                        "academic_observatory_workflows.workflows.web_of_science_telescope.WosClient"
                    ) as m_client:
                        m_client.return_value.__enter__.return_value.name = MagicMock()
                        m_search.return_value = bad_api_response
                        ti = env.run_task(telescope.download.__name__)
                        self.assertEqual(ti.state, State.SUCCESS)
                        self.assertEqual(len(release.download_files), 2)

                # upload_downloaded
                ti = env.run_task(telescope.upload_downloaded.__name__)
                self.assertEqual(ti.state, State.SUCCESS)
                self.assert_blob_integrity(
                    env.download_bucket, blob_name(release.download_files[0]), release.download_files[0]
                )

                # transform
                self.assertRaises(AirflowException, env.run_task, telescope.transform.__name__)

    def test_telescope(self):
        env = ObservatoryEnvironment(self.project_id, self.data_location, api_host=self.host, api_port=self.api_port)
        api_response = MockApiResponse("api_response.xml")

        with env.create():
            self.setup_connections(env)
            self.setup_api(env)
            dataset_id = env.add_dataset()

            execution_date = pendulum.datetime(2021, 1, 1)
            telescope = self.get_telescope(dataset_id)
            dag = telescope.make_dag()

            release_date = pendulum.datetime(2021, 2, 1)
            release = WebOfScienceRelease(
                dag_id=make_dag_id(WebOfScienceTelescope.DAG_ID, self.org_name),
                release_date=release_date,
                login="login",
                password="pass",
                institution_ids=["Curtin University"],
                earliest_date=pendulum.datetime(2021, 1, 1),
            )

            with env.create_dag_run(dag, execution_date):
                # check dependencies
                ti = env.run_task(telescope.check_dependencies.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # download
                with patch(
                    "academic_observatory_workflows.workflows.web_of_science_telescope.WosUtility.search"
                ) as m_search:
                    with patch(
                        "academic_observatory_workflows.workflows.web_of_science_telescope.WosClient"
                    ) as m_client:
                        m_client.return_value.__enter__.return_value.name = MagicMock()
                        m_search.return_value = api_response
                        ti = env.run_task(telescope.download.__name__)
                        self.assertEqual(ti.state, State.SUCCESS)
                        self.assertEqual(len(release.download_files), 2)

                # upload_downloaded
                ti = env.run_task(telescope.upload_downloaded.__name__)
                self.assertEqual(ti.state, State.SUCCESS)
                self.assert_blob_integrity(
                    env.download_bucket, blob_name(release.download_files[0]), release.download_files[0]
                )

                # transform
                ti = env.run_task(telescope.transform.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # upload_transformed
                ti = env.run_task(telescope.upload_transformed.__name__)
                self.assertEqual(ti.state, State.SUCCESS)
                for file in release.transform_files:
                    self.assert_blob_integrity(env.transform_bucket, blob_name(file), file)

                # bq_load
                ti = env.run_task(telescope.bq_load.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                table_id = (
                    f"{self.project_id}.{dataset_id}."
                    f"{bigquery_sharded_table_id(WebOfScienceTelescope.DAG_ID, release.release_date)}"
                )
                expected_rows = 2
                self.assert_table_integrity(table_id, expected_rows)

                # Sample some fields to check in the first row
                sql = f"SELECT * FROM {self.project_id}.{dataset_id}.web_of_science20210201"
                with patch("observatory.platform.utils.gc_utils.bq_query_bytes_daily_limit_check"):
                    records = list(run_bigquery_query(sql))
                self.assertEqual(records[0]["abstract"], [])
                self.assertEqual(records[0]["ref_count"], 1)
                self.assertEqual(records[0]["harvest_datetime"].strftime("%Y%m%d"), "20210201")
                self.assertEqual(records[0]["title"], "Fake title")
                self.assertEqual(records[0]["keywords"], [])
                self.assertEqual(records[0]["release_date"].strftime("%Y%m%d"), "20210201")
                self.assertEqual(records[0]["institution_ids"], ["Curtin University"])

                # cleanup
                download_folder, extract_folder, transform_folder = (
                    release.download_folder,
                    release.extract_folder,
                    release.transform_folder,
                )
                env.run_task(telescope.cleanup.__name__)
                self.assertEqual(ti.state, State.SUCCESS)
                self.assert_cleanup(download_folder, extract_folder, transform_folder)
