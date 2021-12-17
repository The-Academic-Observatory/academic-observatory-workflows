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
import os
import unittest
import unittest.mock as mock
from logging import error
from queue import Empty, Queue
from threading import Event, Thread
from time import sleep
from unittest.mock import MagicMock, patch

import observatory.api.server.orm as orm
import pendulum
from academic_observatory_workflows.config import test_fixtures_folder
from academic_observatory_workflows.workflows.scopus_telescope import (
    ScopusClient,
    ScopusJsonParser,
    ScopusRelease,
    ScopusTelescope,
    ScopusUtility,
    ScopusUtilWorker,
)
from airflow import AirflowException
from airflow.models import Connection
from airflow.utils.state import State
from click.testing import CliRunner
from freezegun import freeze_time
from observatory.platform.utils.airflow_utils import AirflowConns, AirflowVars
from observatory.platform.utils.api import make_observatory_api
from observatory.platform.utils.gc_utils import run_bigquery_query
from observatory.platform.utils.test_utils import (
    HttpServer,
    ObservatoryEnvironment,
    ObservatoryTestCase,
    module_file_path,
)
from observatory.platform.utils.url_utils import get_user_agent
from observatory.platform.utils.workflow_utils import (
    bigquery_sharded_table_id,
    blob_name,
    build_schedule,
    make_dag_id,
)


class TestScopusUtility(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @patch("academic_observatory_workflows.workflows.scopus_telescope.Queue.empty")
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

    @patch("academic_observatory_workflows.workflows.scopus_telescope.urllib.request.Request")
    @patch("academic_observatory_workflows.workflows.scopus_telescope.urllib.request.urlopen")
    def test_retrieve_exceeded(self, m_urlopen, m_request):
        m_urlopen.return_value = MockUrlResponse(code=429)

        client = ScopusClient(api_key=self.api_key)
        self.assertRaises(AirflowException, client.retrieve, self.query)

    @patch("academic_observatory_workflows.workflows.scopus_telescope.urllib.request.Request")
    @patch("academic_observatory_workflows.workflows.scopus_telescope.urllib.request.urlopen")
    def test_retrieve_noresults(self, m_urlopen, m_request):
        m_urlopen.return_value = MockUrlResponse(code=200, response=b"{}")

        client = ScopusClient(api_key=self.api_key)
        results, remaining, reset = client.retrieve(self.query)
        self.assertEqual(results, [])
        self.assertEqual(remaining, 0)
        self.assertEqual(reset, 10)

    @patch("academic_observatory_workflows.workflows.scopus_telescope.json.loads")
    @patch("academic_observatory_workflows.workflows.scopus_telescope.urllib.request.Request")
    @patch("academic_observatory_workflows.workflows.scopus_telescope.urllib.request.urlopen")
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

    @patch("academic_observatory_workflows.workflows.scopus_telescope.urllib.request.Request")
    @patch("academic_observatory_workflows.workflows.scopus_telescope.urllib.request.urlopen")
    def test_retrieve_unexpected_httpcode(self, m_urlopen, m_request):
        m_urlopen.return_value = MockUrlResponse(code=403, response=b"{}")

        client = ScopusClient(api_key=self.api_key)
        self.assertRaises(AirflowException, client.retrieve, self.query)

    @patch("academic_observatory_workflows.workflows.scopus_telescope.urllib.request.Request")
    @patch("academic_observatory_workflows.workflows.scopus_telescope.urllib.request.urlopen")
    def test_retrieve_max_results_exceeded(self, m_urlopen, m_request):
        response = b'{"search-results": {"entry": [1], "opensearch:totalResults": 5001}}'

        m_urlopen.return_value = MockUrlResponse(code=200, response=response)
        client = ScopusClient(api_key=self.api_key)
        self.assertRaises(AirflowException, client.retrieve, self.query)

    @patch("academic_observatory_workflows.workflows.scopus_telescope.urllib.request.Request")
    @patch("academic_observatory_workflows.workflows.scopus_telescope.urllib.request.urlopen")
    def test_retrieve_no_next_url(self, m_urlopen, m_request):
        response = b'{"search-results": {"entry": [1], "opensearch:totalResults": 2, "link": []}}'

        m_urlopen.return_value = MockUrlResponse(code=200, response=response)
        client = ScopusClient(api_key=self.api_key)
        self.assertRaises(AirflowException, client.retrieve, self.query)

    @patch("academic_observatory_workflows.workflows.scopus_telescope.urllib.request.Request")
    @patch("academic_observatory_workflows.workflows.scopus_telescope.urllib.request.urlopen")
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

    @freeze_time("2021-02-01")
    @patch("academic_observatory_workflows.workflows.scopus_telescope.write_to_file")
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

    @freeze_time("2021-02-02")
    def test_sleep_if_needed_needed(self):
        reset_date = pendulum.datetime(2021, 2, 2, 0, 0, 1)
        with patch("academic_observatory_workflows.workflows.scopus_telescope.logging.info") as m_log:
            ScopusUtility.sleep_if_needed(reset_date=reset_date, conn="conn")
            self.assertEqual(m_log.call_count, 1)

    @freeze_time("2021-02-02")
    def test_sleep_if_needed_not_needed(self):
        reset_date = pendulum.datetime(2021, 2, 1)
        with patch("academic_observatory_workflows.workflows.scopus_telescope.logging.info") as m_log:
            ScopusUtility.sleep_if_needed(reset_date=reset_date, conn="conn")
            self.assertEqual(m_log.call_count, 0)

    @freeze_time("2021-02-02")
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

    @patch("academic_observatory_workflows.workflows.scopus_telescope.ScopusUtility.download_period")
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

    @patch("academic_observatory_workflows.workflows.scopus_telescope.ScopusUtility.download_period")
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

    @patch("academic_observatory_workflows.workflows.scopus_telescope.ScopusUtility.download_period")
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

    @patch("academic_observatory_workflows.workflows.scopus_telescope.ScopusUtility.download_period")
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

        harvest_datetime = pendulum.now("UTC").isoformat()
        release_date = "2018-01-01"
        entry = ScopusJsonParser.parse_json(
            data=self.data,
            harvest_datetime=harvest_datetime,
            release_date=release_date,
            institution_ids=self.institution_ids,
        )
        self.assertEqual(entry["harvest_datetime"], harvest_datetime)
        self.assertEqual(entry["release_date"], release_date)
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


class TestScopusTelescope(ObservatoryTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.host = "localhost"
        self.api_port = 5000
        self.data_location = "us"
        self.org_name = "Curtin University"
        self.conn_id = "scopus_curtin_university"
        self.earliest_date = pendulum.datetime(2021, 1, 1)

        self.fixture_dir = test_fixtures_folder("scopus")
        self.fixture_file = os.path.join(self.fixture_dir, "test.json")
        with open(self.fixture_file, "r") as f:
            self.results_str = f.read()
        self.results_len = 1

    def setup_connections(self, env):
        # Add Observatory API connection
        conn = Connection(conn_id=AirflowConns.OBSERVATORY_API, uri=f"http://:password@{self.host}:{self.api_port}")
        env.add_connection(conn)

        # Add login/pass connection
        conn = Connection(conn_id=self.conn_id, uri=f"http://login:password@localhost")
        env.add_connection(conn)

    def setup_api(self, env, extra=None):
        dt = pendulum.now("UTC")

        if extra is None:
            extra = {
                "airflow_connections": [self.conn_id],
                "institution_ids": ["123"],
                "earliest_date": self.earliest_date.isoformat(),
                "view": "STANDARD",
            }

        name = "Scopus Telescope"
        telescope_type = orm.TelescopeType(name=name, type_id=ScopusTelescope.DAG_ID, created=dt, modified=dt)
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

    def get_telescope(self, dataset_id):
        api = make_observatory_api()
        telescope_type = api.get_telescope_type(type_id=ScopusTelescope.DAG_ID)
        telescopes = api.get_telescopes(telescope_type_id=telescope_type.id, limit=1000)
        self.assertEqual(len(telescopes), 1)

        dag_id = make_dag_id(ScopusTelescope.DAG_ID, telescopes[0].organisation.name)
        airflow_conns = telescopes[0].extra.get("airflow_connections")
        institution_ids = telescopes[0].extra.get("institution_ids")
        earliest_date_str = telescopes[0].extra.get("earliest_date")
        earliest_date = pendulum.parse(earliest_date_str)

        airflow_vars = [
            AirflowVars.DATA_PATH,
            AirflowVars.DATA_LOCATION,
        ]

        telescope = ScopusTelescope(
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
            ScopusTelescope,
            dag_id="dag",
            dataset_id="dataset",
            airflow_conns=[],
            airflow_vars=[],
            institution_ids=[],
            earliest_date=pendulum.now("UTC"),
        )

        self.assertRaises(
            AirflowException,
            ScopusTelescope,
            dag_id="dag",
            dataset_id="dataset",
            airflow_conns=["conn"],
            airflow_vars=[],
            institution_ids=[],
            earliest_date=pendulum.now("UTC"),
        )

    def test_dag_structure(self):
        """Test that the ScopusTelescope DAG has the correct structure.

        :return: None
        """
        dag = ScopusTelescope(
            dag_id="dag",
            airflow_conns=["conn"],
            airflow_vars=[],
            institution_ids=["10"],
            earliest_date=pendulum.now("UTC"),
            view="standard",
        ).make_dag()
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

        dag_file = os.path.join(module_file_path("academic_observatory_workflows.dags"), "scopus_telescope.py")

        env = ObservatoryEnvironment(self.project_id, self.data_location, api_host=self.host, api_port=self.api_port)

        with env.create():
            self.setup_connections(env)
            self.setup_api(env)

            dag_file = os.path.join(module_file_path("academic_observatory_workflows.dags"), "scopus_telescope.py")
            dag_id = make_dag_id(ScopusTelescope.DAG_ID, self.org_name)
            self.assert_dag_load(dag_id, dag_file)

    def test_dag_load_missing_params(self):
        """Test that the DAG can be loaded from a DAG bag."""

        dag_file = os.path.join(module_file_path("academic_observatory_workflows.dags"), "scopus_telescope.py")
        env = ObservatoryEnvironment(self.project_id, self.data_location, api_host=self.host, api_port=self.api_port)
        extra = {
            "airflow_connections": [self.conn_id],
            "institution_ids": ["123"],
            "earliest_date": self.earliest_date.isoformat(),
        }

        with env.create():
            self.setup_connections(env)
            self.setup_api(env, extra=extra)

            dag_file = os.path.join(module_file_path("academic_observatory_workflows.dags"), "scopus_telescope.py")
            dag_id = make_dag_id(ScopusTelescope.DAG_ID, self.org_name)
            self.assertRaises(AssertionError, self.assert_dag_load, dag_id, dag_file)

    def test_telescope(self):
        env = ObservatoryEnvironment(self.project_id, self.data_location, api_host=self.host, api_port=self.api_port)

        with env.create():
            self.setup_connections(env)
            self.setup_api(env)
            dataset_id = env.add_dataset()

            execution_date = pendulum.datetime(2021, 1, 1)
            telescope = self.get_telescope(dataset_id)
            dag = telescope.make_dag()

            release_date = pendulum.datetime(2021, 2, 1)
            release = ScopusRelease(
                dag_id=make_dag_id(ScopusTelescope.DAG_ID, self.org_name),
                release_date=release_date,
                api_keys=["1"],
                institution_ids=["123"],
                view="standard",
                earliest_date=pendulum.datetime(2021, 1, 1),
            )

            with env.create_dag_run(dag, execution_date):
                # check dependencies
                ti = env.run_task(telescope.check_dependencies.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # download
                with patch(
                    "academic_observatory_workflows.workflows.scopus_telescope.ScopusUtility.make_query"
                ) as m_search:
                    m_search.return_value = self.results_str, self.results_len
                    ti = env.run_task(telescope.download.__name__)
                    self.assertEqual(ti.state, State.SUCCESS)
                    self.assertEqual(len(release.download_files), 1)
                    self.assertEqual(m_search.call_count, 1)

                # upload downloaded
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
                    f"{bigquery_sharded_table_id(ScopusTelescope.DAG_ID, release.release_date)}"
                )
                expected_rows = 1
                self.assert_table_integrity(table_id, expected_rows)

                # Sample some fields to check in the first row
                sql = f"SELECT * FROM {self.project_id}.{dataset_id}.scopus20210201"
                with patch("observatory.platform.utils.gc_utils.bq_query_bytes_daily_limit_check"):
                    records = list(run_bigquery_query(sql))
                self.assertEqual(records[0]["aggregation_type"], "Journal")
                self.assertEqual(records[0]["source_id"], 1)
                self.assertEqual(records[0]["eid"], "somedoi")
                self.assertEqual(records[0]["pii"], "S00000")
                self.assertEqual(records[0]["identifier"], "SCOPUS_ID:000000")
                self.assertEqual(records[0]["doi"], ["10.0000/00"])
                self.assertEqual(records[0]["publication_name"], "Journal of Things")
                self.assertEqual(records[0]["institution_ids"], [123])
                self.assertEqual(records[0]["creator"], "Name F.")
                self.assertEqual(records[0]["article_number"], "1")
                self.assertEqual(records[0]["title"], "Article title")
                self.assertEqual(records[0]["issn"], ["00000000"])
                self.assertEqual(records[0]["subtype_description"], "Article")
                self.assertEqual(records[0]["citedby_count"], 0)

                # cleanup
                download_folder, extract_folder, transform_folder = (
                    release.download_folder,
                    release.extract_folder,
                    release.transform_folder,
                )
                env.run_task(telescope.cleanup.__name__)
                self.assertEqual(ti.state, State.SUCCESS)
                self.assert_cleanup(download_folder, extract_folder, transform_folder)
