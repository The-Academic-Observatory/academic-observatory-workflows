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

import datetime
import json
import unittest
from queue import Empty, Queue
from threading import Event, Thread
from unittest.mock import MagicMock, patch

import pendulum
import time_machine
from airflow import AirflowException

from academic_observatory_workflows.config import project_path
from academic_observatory_workflows.scopus_telescope.tasks import (
    ScopusClient,
    ScopusJsonParser,
    ScopusUtility,
    ScopusUtilWorker,
)

from observatory_platform.url_utils import get_user_agent

FIXTURES_FOLDER = project_path("scopus_telescope", "tests", "fixtures")


class TestScopusUtility(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @patch("academic_observatory_workflows.scopus_telescope.scopus_telescope.Queue.empty")
    def test_clear_task_queue(self, m_empty):
        m_empty.side_effect = [False, False, True]

        q = Queue()
        q.put(1)

        ScopusUtility.clear_task_queue(q)
        self.assertRaises(Empty, q.get, False)
        q.join()  # Make sure no block


class MockUrlResponse:
    def __init__(self, *, response="{}", code=200):
        self.response = response
        self.code = code

    def getheader(self, header):
        if header == "X-RateLimit-Remaining":
            return 0

        if header == "X-RateLimit-Reset":
            return 10

    def getcode(self):
        return self.code

    def read(self):
        return self.response


class TestScopusClient(unittest.TestCase):
    """Test the ScopusClient class."""

    class MockMetadata:
        @classmethod
        def get(self, attribute):
            if attribute == "Version":
                return "1"
            if attribute == "Home-page":
                return "http://test.test"
            if attribute == "Author-email":
                return "test@test"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.api_key = "testkey"
        self.query = "dummyquery"

    def test_scopus_client_user_agent(self):
        """Test to make sure the user agent string is set correctly."""
        with patch("observatory.platform.utils.url_utils.metadata", return_value=TestScopusClient.MockMetadata):
            obj = ScopusClient(api_key="")
            generated_ua = obj._headers["User-Agent"]
            self.assertEqual(generated_ua, get_user_agent(package_name="academic_observatory_workflows"))

    def test_get_reset_date_from_error(self):
        msg = f"{ScopusClient.QUOTA_EXCEED_ERROR_PREFIX}2000"
        offset = ScopusClient.get_reset_date_from_error(msg)
        self.assertEqual(offset, 2)

    def test_get_next_page_url(self):
        links = []
        next_link = ScopusClient.get_next_page_url(links)
        self.assertEqual(next_link, None)

        expected_url = "http://next.url"
        links = [{"@ref": "next", "@href": expected_url}]
        next_link = ScopusClient.get_next_page_url(links)
        self.assertEqual(next_link, expected_url)

        links = [{"@ref": "self"}]
        next_link = ScopusClient.get_next_page_url(links)
        self.assertEqual(next_link, None)

        links = [{}]
        self.assertEqual(ScopusClient.get_next_page_url(links), None)

    @patch("academic_observatory_workflows.scopus_telescope.scopus_telescope.urllib.request.Request")
    @patch("academic_observatory_workflows.scopus_telescope.scopus_telescope.urllib.request.urlopen")
    def test_retrieve_exceeded(self, m_urlopen, m_request):
        m_urlopen.return_value = MockUrlResponse(code=429)

        client = ScopusClient(api_key=self.api_key)
        self.assertRaises(AirflowException, client.retrieve, self.query)

    @patch("academic_observatory_workflows.scopus_telescope.scopus_telescope.urllib.request.Request")
    @patch("academic_observatory_workflows.scopus_telescope.scopus_telescope.urllib.request.urlopen")
    def test_retrieve_noresults(self, m_urlopen, m_request):
        m_urlopen.return_value = MockUrlResponse(code=200, response=b"{}")

        client = ScopusClient(api_key=self.api_key)
        results, remaining, reset = client.retrieve(self.query)
        self.assertEqual(results, [])
        self.assertEqual(remaining, 0)
        self.assertEqual(reset, 10)

    @patch("academic_observatory_workflows.scopus_telescope.scopus_telescope.json.loads")
    @patch("academic_observatory_workflows.scopus_telescope.scopus_telescope.urllib.request.Request")
    @patch("academic_observatory_workflows.scopus_telescope.scopus_telescope.urllib.request.urlopen")
    def test_retrieve_totalresults_zero(self, m_urlopen, m_request, m_json):
        m_urlopen.return_value = MockUrlResponse(code=200, response=b"{}")

        m_json.return_value = {
            "search-results": {
                "entry": [None],
                "opensearch:totalResults": 0,
            }
        }

        client = ScopusClient(api_key=self.api_key)
        results, remaining, reset = client.retrieve(self.query)
        self.assertEqual(results, [])
        self.assertEqual(remaining, 0)
        self.assertEqual(reset, 10)

    @patch("academic_observatory_workflows.scopus_telescope.scopus_telescope.urllib.request.Request")
    @patch("academic_observatory_workflows.scopus_telescope.scopus_telescope.urllib.request.urlopen")
    def test_retrieve_unexpected_httpcode(self, m_urlopen, m_request):
        m_urlopen.return_value = MockUrlResponse(code=403, response=b"{}")

        client = ScopusClient(api_key=self.api_key)
        self.assertRaises(AirflowException, client.retrieve, self.query)

    @patch("academic_observatory_workflows.scopus_telescope.scopus_telescope.urllib.request.Request")
    @patch("academic_observatory_workflows.scopus_telescope.scopus_telescope.urllib.request.urlopen")
    def test_retrieve_max_results_exceeded(self, m_urlopen, m_request):
        response = b'{"search-results": {"entry": [1], "opensearch:totalResults": 5001}}'

        m_urlopen.return_value = MockUrlResponse(code=200, response=response)
        client = ScopusClient(api_key=self.api_key)
        self.assertRaises(AirflowException, client.retrieve, self.query)

    @patch("academic_observatory_workflows.scopus_telescope.scopus_telescope.urllib.request.Request")
    @patch("academic_observatory_workflows.scopus_telescope.scopus_telescope.urllib.request.urlopen")
    def test_retrieve_no_next_url(self, m_urlopen, m_request):
        response = b'{"search-results": {"entry": [1], "opensearch:totalResults": 2, "link": []}}'

        m_urlopen.return_value = MockUrlResponse(code=200, response=response)
        client = ScopusClient(api_key=self.api_key)
        self.assertRaises(AirflowException, client.retrieve, self.query)

    @patch("academic_observatory_workflows.scopus_telescope.scopus_telescope.urllib.request.Request")
    @patch("academic_observatory_workflows.scopus_telescope.scopus_telescope.urllib.request.urlopen")
    def test_retrieve(self, m_urlopen, m_request):
        response = b'{"search-results": {"entry": [1], "opensearch:totalResults": 2, "link": [{"@ref": "next", "@href": "someurl"}]}}'

        m_urlopen.return_value = MockUrlResponse(code=200, response=response)
        client = ScopusClient(api_key=self.api_key)
        results, _, _ = client.retrieve(self.query)
        self.assertEqual(len(results), 2)


class TestScopusUtilWorker(unittest.TestCase):
    def test_ctor(self):
        util = ScopusUtilWorker(
            client_id=0, client=None, quota_reset_date=pendulum.datetime(2000, 1, 1), quota_remaining=0
        )

        self.assertEqual(util.client_id, 0)
        self.assertEqual(util.client, None)
        self.assertEqual(util.quota_reset_date, pendulum.datetime(2000, 1, 1))
        self.assertEqual(util.quota_remaining, 0)

    def test_build_query(self):
        institution_ids = ["60031226"]
        period = pendulum.period(pendulum.datetime(2021, 1, 1), pendulum.datetime(2021, 2, 1))
        query = ScopusUtility.build_query(institution_ids=institution_ids, period=period)

    def test_make_query(self):
        worker = MagicMock()
        worker.client = MagicMock()
        worker.client.retrieve = MagicMock()
        worker.client.retrieve.return_value = [{}, {}], 2000, 10
        query = ""
        results, num_results = ScopusUtility.make_query(worker=worker, query=query)
        self.assertEqual(num_results, 2)
        self.assertEqual(results, "[{}, {}]")

    @time_machine.travel(datetime.datetime(2021, 2, 1))
    @patch("academic_observatory_workflows.scopus_telescope.scopus_telescope.write_to_file")
    def test_download_period(self, m_write_file):
        conn = "conn_id"
        worker = MagicMock()
        worker.client = MagicMock()
        worker.client.retrieve = MagicMock()
        results = [{}] * (ScopusClient.MAX_RESULTS + 1)
        worker.client.retrieve.return_value = results, 2000, 10
        period = pendulum.period(pendulum.date(2021, 1, 1), pendulum.date(2021, 2, 1))
        institution_ids = ["123"]
        ScopusUtility.download_period(
            worker=worker, conn=conn, period=period, institution_ids=institution_ids, download_dir="/tmp"
        )

        args, _ = m_write_file.call_args
        self.assertEqual(args[0], json.dumps(results))
        self.assertEqual(args[1], "/tmp/2021-01-01_2021-02-01_2021-02-01T00:00:00+00:00.json")

    @time_machine.travel(datetime.datetime(2021, 2, 2))
    def test_sleep_if_needed_needed(self):
        reset_date = pendulum.datetime(2021, 2, 2, 0, 0, 1)
        with patch("academic_observatory_workflows.scopus_telescope.scopus_telescope.logging.info") as m_log:
            ScopusUtility.sleep_if_needed(reset_date=reset_date, conn="conn")
            self.assertEqual(m_log.call_count, 1)

    @time_machine.travel(datetime.datetime(2021, 2, 2))
    def test_sleep_if_needed_not_needed(self):
        reset_date = pendulum.datetime(2021, 2, 1)
        with patch("academic_observatory_workflows.scopus_telescope.scopus_telescope.logging.info") as m_log:
            ScopusUtility.sleep_if_needed(reset_date=reset_date, conn="conn")
            self.assertEqual(m_log.call_count, 0)

    @time_machine.travel(datetime.datetime(2021, 2, 2))
    def test_update_reset_date(self):
        conn = "conn_id"
        worker = MagicMock()
        now = pendulum.now("UTC")
        worker.quota_reset_date = now
        new_ts = now.int_timestamp * 1000 + 2000
        error_msg = f"{ScopusClient.QUOTA_EXCEED_ERROR_PREFIX}{new_ts}"
        ScopusUtility.update_reset_date(conn=conn, error_msg=error_msg, worker=worker)
        self.assertTrue(worker.quota_reset_date > now)

    @patch.object(ScopusUtilWorker, "QUEUE_WAIT_TIME", 1)
    def test_download_worker_empty_retry_exit(self):
        def trigger_exit(event):
            now = pendulum.now("UTC")
            trigger = now.add(seconds=5)

            while pendulum.now("UTC") < trigger:
                continue

            event.set()

        conn = "conn"
        queue = Queue()
        event = Event()
        institution_ids = ["123"]

        thread = Thread(target=trigger_exit, args=(event,))
        thread.start()
        worker = ScopusUtilWorker(client_id=0, client=None, quota_reset_date=pendulum.now("UTC"), quota_remaining=10)

        ScopusUtility.download_worker(
            worker=worker,
            exit_event=event,
            taskq=queue,
            conn=conn,
            institution_ids=institution_ids,
            download_dir="",
        )
        thread.join()

    @patch("academic_observatory_workflows.scopus_telescope.scopus_telescope.ScopusUtility.download_period")
    def test_download_worker_download_exit(self, m_download):
        def trigger_exit(event):
            now = pendulum.now("UTC")
            trigger = now.add(seconds=5)

            while pendulum.now("UTC") < trigger:
                continue

            event.set()

        conn = "conn"
        queue = Queue()
        now = pendulum.now("UTC")
        queue.put(pendulum.period(now, now))
        event = Event()
        institution_ids = ["123"]

        thread = Thread(target=trigger_exit, args=(event,))
        thread.start()
        worker = ScopusUtilWorker(client_id=0, client=None, quota_reset_date=pendulum.now("UTC"), quota_remaining=10)

        ScopusUtility.download_worker(
            worker=worker,
            exit_event=event,
            taskq=queue,
            conn=conn,
            institution_ids=institution_ids,
            download_dir="",
        )
        thread.join()

    @patch("academic_observatory_workflows.scopus_telescope.scopus_telescope.ScopusUtility.download_period")
    def test_download_worker_download_quota_exceed_retry_exit(self, m_download):
        def trigger_exit(event):
            now = pendulum.now("UTC")
            trigger = now.add(seconds=1)

            while pendulum.now("UTC") < trigger:
                continue

            event.set()

        now = pendulum.now("UTC")
        next_reset = now.add(seconds=2).int_timestamp * 1000

        m_download.side_effect = [AirflowException(f"{ScopusClient.QUOTA_EXCEED_ERROR_PREFIX}{next_reset}"), None]

        conn = "conn"
        queue = Queue()
        queue.put(pendulum.period(now, now))
        event = Event()
        institution_ids = ["123"]

        thread = Thread(target=trigger_exit, args=(event,))
        thread.start()
        worker = ScopusUtilWorker(client_id=0, client=None, quota_reset_date=now, quota_remaining=10)

        ScopusUtility.download_worker(
            worker=worker,
            exit_event=event,
            taskq=queue,
            conn=conn,
            institution_ids=institution_ids,
            download_dir="",
        )
        thread.join()

    @patch("academic_observatory_workflows.scopus_telescope.scopus_telescope.ScopusUtility.download_period")
    def test_download_worker_download_uncaught_exception(self, m_download):
        def trigger_exit(event):
            now = pendulum.now("UTC")
            trigger = now.add(seconds=5)

            while pendulum.now("UTC") < trigger:
                continue

            event.set()

        now = pendulum.now("UTC")
        m_download.side_effect = AirflowException("Some other error")
        conn = "conn"
        queue = Queue()
        queue.put(pendulum.period(now, now))
        queue.put(pendulum.period(now, now))
        event = Event()
        institution_ids = ["123"]

        thread = Thread(target=trigger_exit, args=(event,))
        thread.start()
        worker = ScopusUtilWorker(client_id=0, client=None, quota_reset_date=now, quota_remaining=10)

        self.assertRaises(
            AirflowException,
            ScopusUtility.download_worker,
            worker=worker,
            exit_event=event,
            taskq=queue,
            conn=conn,
            institution_ids=institution_ids,
            download_dir="",
        )
        thread.join()

    @patch("academic_observatory_workflows.scopus_telescope.scopus_telescope.ScopusUtility.download_period")
    def test_download_parallel(self, m_download):
        now = pendulum.now("UTC")
        conn = "conn"
        queue = Queue()
        institution_ids = ["123"]
        m_download.return_value = None

        for _ in range(4):
            queue.put(pendulum.period(now, now))

        workers = [
            ScopusUtilWorker(client_id=i, client=None, quota_reset_date=now, quota_remaining=10) for i in range(2)
        ]

        ScopusUtility.download_parallel(
            workers=workers, taskq=queue, conn=conn, institution_ids=institution_ids, download_dir=""
        )


class TestScopusJsonParser(unittest.TestCase):
    """Test parsing facilities."""

    def __init__(self, *args, **kwargs):
        super(TestScopusJsonParser, self).__init__(*args, **kwargs)
        self.institution_ids = ["60031226"]  # Curtin University

        self.data = {
            "dc:identifier": "scopusid",
            "eid": "testid",
            "dc:title": "arttitle",
            "prism:aggregationType": "source",
            "subtypeDescription": "typedesc",
            "citedby-count": "345",
            "prism:publicationName": "pubname",
            "prism:isbn": "isbn",
            "prism:issn": "issn",
            "prism:eIssn": "eissn",
            "prism:coverDate": "2010-12-01",
            "prism:doi": "doi",
            "pii": "pii",
            "pubmed-id": "med",
            "orcid": "orcid",
            "dc:creator": "firstauth",
            "source-id": "1000",
            "openaccess": "1",
            "openaccessFlag": False,
            "affiliation": [
                {
                    "affilname": "aname",
                    "affiliation-city": "acity",
                    "affiliation-country": "country",
                    "afid": "id",
                    "name-variant": "variant",
                }
            ],
            "author": [
                {
                    "authid": "id",
                    "orcid": "id",
                    "authname": "name",
                    "given-name": "first",
                    "surname": "last",
                    "initials": "mj",
                    "afid": "id",
                }
            ],
            "dc:description": "abstract",
            "authkeywords": ["words"],
            "article-number": "artno",
            "fund-acr": "acr",
            "fund-no": "no",
            "fund-sponsor": "sponsor",
        }

    def test_get_affiliations(self):
        """Test get affiliations"""

        affil = ScopusJsonParser.get_affiliations({})
        self.assertEqual(affil, None)

        affil = ScopusJsonParser.get_affiliations(self.data)
        self.assertEqual(len(affil), 1)
        af = affil[0]
        self.assertEqual(af["name"], "aname")
        self.assertEqual(af["city"], "acity")
        self.assertEqual(af["country"], "country")
        self.assertEqual(af["id"], "id")
        self.assertEqual(af["name_variant"], "variant")

        # 0 length affiliations
        affil = ScopusJsonParser.get_affiliations({"affiliation": []})
        self.assertEqual(affil, None)

    def test_get_authors(self):
        """Test get authors"""

        author = ScopusJsonParser.get_authors({})
        self.assertEqual(author, None)

        author = ScopusJsonParser.get_authors(self.data)
        self.assertEqual(len(author), 1)
        au = author[0]
        self.assertEqual(au["authid"], "id")
        self.assertEqual(au["orcid"], "id")
        self.assertEqual(au["full_name"], "name")
        self.assertEqual(au["first_name"], "first")
        self.assertEqual(au["last_name"], "last")
        self.assertEqual(au["initials"], "mj")
        self.assertEqual(au["afid"], "id")

        # 0 length author
        author = ScopusJsonParser.get_authors({"author": []})
        self.assertEqual(author, None)

    def test_get_identifier_list(self):
        ids = ScopusJsonParser.get_identifier_list({}, "myid")
        self.assertEqual(ids, None)

        ids = ScopusJsonParser.get_identifier_list({"myid": "thing"}, "myid")
        self.assertEqual(ids, ["thing"])

        ids = ScopusJsonParser.get_identifier_list({"myid": []}, "myid")
        self.assertEqual(ids, None)

        ids = ScopusJsonParser.get_identifier_list({"myid": [{"$": "thing"}]}, "myid")
        self.assertEqual(ids, ["thing"])

    def test_parse_json(self):
        """Test the parser."""

        snapshot_date = pendulum.datetime(2018, 1, 1)
        entry = ScopusJsonParser.parse_json(
            data=self.data,
            snapshot_date=snapshot_date,
            institution_ids=self.institution_ids,
        )
        self.assertEqual(entry["snapshot_date"], snapshot_date.date().isoformat())
        self.assertEqual(entry["title"], "arttitle")
        self.assertEqual(entry["identifier"], "scopusid")
        self.assertEqual(entry["creator"], "firstauth")
        self.assertEqual(entry["publication_name"], "pubname")
        self.assertEqual(entry["cover_date"], "2010-12-01")
        self.assertEqual(entry["doi"][0], "doi")
        self.assertEqual(entry["eissn"][0], "eissn")
        self.assertEqual(entry["issn"][0], "issn")
        self.assertEqual(entry["isbn"][0], "isbn")
        self.assertEqual(entry["aggregation_type"], "source")
        self.assertEqual(entry["pubmed_id"], "med")
        self.assertEqual(entry["pii"], "pii")
        self.assertEqual(entry["eid"], "testid")
        self.assertEqual(entry["subtype_description"], "typedesc")
        self.assertEqual(entry["open_access"], 1)
        self.assertEqual(entry["open_access_flag"], False)
        self.assertEqual(entry["citedby_count"], 345)
        self.assertEqual(entry["source_id"], 1000)
        self.assertEqual(entry["orcid"], "orcid")

        self.assertEqual(len(entry["affiliations"]), 1)
        af = entry["affiliations"][0]
        self.assertEqual(af["name"], "aname")
        self.assertEqual(af["city"], "acity")
        self.assertEqual(af["country"], "country")
        self.assertEqual(af["id"], "id")
        self.assertEqual(af["name_variant"], "variant")

        self.assertEqual(entry["abstract"], "abstract")
        self.assertEqual(entry["article_number"], "artno")
        self.assertEqual(entry["fund_agency_ac"], "acr")
        self.assertEqual(entry["fund_agency_id"], "no")
        self.assertEqual(entry["fund_agency_name"], "sponsor")

        words = entry["keywords"]
        self.assertEqual(len(words), 1)
        self.assertEqual(words[0], "words")

        authors = entry["authors"]
        self.assertEqual(len(authors), 1)
        au = authors[0]
        self.assertEqual(au["authid"], "id")
        self.assertEqual(au["orcid"], "id")
        self.assertEqual(au["full_name"], "name")
        self.assertEqual(au["first_name"], "first")
        self.assertEqual(au["last_name"], "last")
        self.assertEqual(au["initials"], "mj")
        self.assertEqual(au["afid"], "id")

        self.assertEqual(len(entry["institution_ids"]), 1)
        self.assertEqual(entry["institution_ids"], self.institution_ids)
