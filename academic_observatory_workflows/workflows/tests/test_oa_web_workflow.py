# Copyright 2021 Curtin University
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

# Author: James Diprose, Aniek Roelofs

import json
import os
from typing import List
from unittest import TestCase
from unittest.mock import patch

import httpretty
import jsonlines
import nltk
import pandas as pd
import pendulum
import vcr
from airflow.exceptions import AirflowException
from airflow.models.connection import Connection
from airflow.models.variable import Variable
from airflow.utils.state import State
from click.testing import CliRunner

import academic_observatory_workflows.workflows.oa_web_workflow
from academic_observatory_workflows.config import schema_folder, test_fixtures_folder
from academic_observatory_workflows.workflows.oa_web_workflow import (
    Description,
    OaWebRelease,
    OaWebWorkflow,
    calc_oa_stats,
    clean_ror_id,
    clean_url,
    get_institution_logo,
    get_wiki_descriptions,
    make_logo_url,
    remove_text_between_brackets,
    shorten_text_full_sentences,
    split_largest_remainder,
    val_empty,
    trigger_repository_dispatch,
)
from observatory.platform.utils.file_utils import load_jsonl
from observatory.platform.utils.test_utils import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    Table,
    bq_load_tables,
    make_dummy_dag,
    module_file_path,
)

academic_observatory_workflows.workflows.oa_web_workflow.INCLUSION_THRESHOLD = 0


class TestFunctions(TestCase):
    def test_val_empty(self):
        # Empty list
        self.assertTrue(val_empty([]))

        # Non empty list
        self.assertFalse(val_empty([1, 2, 3]))

        # None
        self.assertTrue(val_empty(None))

        # Empty string
        self.assertTrue(val_empty(""))

        # Non Empty string
        self.assertFalse(val_empty("hello"))

    def test_clean_ror_id(self):
        actual = clean_ror_id("https://ror.org/02n415q13")
        expected = "02n415q13"
        self.assertEqual(actual, expected)

    def test_split_largest_remainder(self):
        # Check that if ratios do not sum to 1 an AssertionError is raised
        with self.assertRaises(AssertionError):
            sample_size = 100
            ratios = [0.1, 0.2, 0.4, 100]
            split_largest_remainder(sample_size, *ratios)

        # Test that correct absolute values are returned
        sample_size = 10
        ratios = [0.11, 0.21, 0.68]
        results = split_largest_remainder(sample_size, *ratios)
        self.assertEqual((1, 2, 7), results)

    def test_clean_url(self):
        url = "https://www.auckland.ac.nz/en.html"
        expected = "https://www.auckland.ac.nz/"
        actual = clean_url(url)
        self.assertEqual(expected, actual)

    def test_make_logo_url(self):
        expected = "/logos/country/s/1234.jpg"
        actual = make_logo_url(category="country", entity_id="1234", size="s", fmt="jpg")
        self.assertEqual(expected, actual)

    def test_calc_oa_stats(self):
        n_outputs = 100
        n_outputs_open = 33
        n_outputs_publisher_open = 24
        n_outputs_other_platform_open = 22
        n_outputs_other_platform_open_only = 9

        n_outputs_publisher_open_only, n_outputs_both, n_outputs_closed = calc_oa_stats(
            n_outputs,
            n_outputs_open,
            n_outputs_publisher_open,
            n_outputs_other_platform_open,
            n_outputs_other_platform_open_only,
        )

        self.assertEqual(11, n_outputs_publisher_open_only)
        self.assertEqual(13, n_outputs_both)
        self.assertEqual(67, n_outputs_closed)

        total = n_outputs_publisher_open_only + n_outputs_both + n_outputs_other_platform_open_only + n_outputs_closed
        self.assertEqual(100, total)

    @patch("academic_observatory_workflows.workflows.oa_web_workflow.requests.post")
    def test_trigger_repository_dispatch(self, mock_requests_post):
        trigger_repository_dispatch(token="my-token", event_type="my-event-type")
        mock_requests_post.called_once()

    @patch("academic_observatory_workflows.workflows.oa_web_workflow.make_logo_url")
    def test_get_institution_logo(self, mock_make_url):
        mock_make_url.return_value = "logo_path"
        mock_clearbit_ref = "academic_observatory_workflows.workflows.oa_web_workflow.clearbit_download_logo"

        def download_logo(company_url, file_path, size, fmt):
            if not os.path.isdir(os.path.dirname(file_path)):
                os.makedirs(os.path.dirname(file_path))
            with open(file_path, "w") as f:
                f.write("foo")

        ror_id, url, size, width, fmt, build_path = "ror_id", "url.com", "size", 10, "fmt", "build_path"
        with CliRunner().isolated_filesystem():
            # Test when logo file does not exist yet and logo download fails
            with patch(mock_clearbit_ref) as mock_clearbit_download:
                actual_ror_id, actual_logo_path = get_institution_logo(ror_id, url, size, width, fmt, build_path)
                self.assertEqual(ror_id, actual_ror_id)
                self.assertEqual("/unknown.svg", actual_logo_path)
                mock_clearbit_download.assert_called_once_with(
                    company_url=url, file_path="build_path/logos/institution/size/ror_id.fmt", size=width, fmt=fmt
                )
                mock_make_url.assert_not_called()

            mock_make_url.reset_mock()

            # Test when logo file does not exist yet and logo is downloaded successfully
            with patch(mock_clearbit_ref, wraps=download_logo) as mock_clearbit_download:
                actual_ror_id, actual_logo_path = get_institution_logo(ror_id, url, size, width, fmt, build_path)
                self.assertEqual(ror_id, actual_ror_id)
                self.assertEqual("logo_path", actual_logo_path)
                mock_clearbit_download.assert_called_once_with(
                    company_url=url, file_path="build_path/logos/institution/size/ror_id.fmt", size=width, fmt=fmt
                )
                mock_make_url.assert_called_once_with(category="institution", entity_id=ror_id, size=size, fmt=fmt)

            mock_make_url.reset_mock()

            # Test when logo file already exists
            with patch(mock_clearbit_ref, wraps=download_logo) as mock_clearbit_download:
                actual_ror_id, actual_logo_path = get_institution_logo(ror_id, url, size, width, fmt, build_path)
                self.assertEqual(ror_id, actual_ror_id)
                self.assertEqual("logo_path", actual_logo_path)
                mock_clearbit_download.assert_not_called()
                mock_make_url.assert_called_once_with(category="institution", entity_id=ror_id, size=size, fmt=fmt)

    def test_remove_text_between_brackets(self):
        text_input = (
            "Sem Gordius (Nobis: Gestarum) at ea debile quantum si dis subordinatas Civiuni Magna. Ut "
            "oratione ut est enim subsolanea—aut Quasi Nemine (Ac (Hac)-y-Enim) hac dis Facer Eventu (Se Necessaria)—mus quod 400 srripta firmare, annuebat p illum quas te 068,721 verbum displicere (803,200 ea in). Cum Memento si lorem 9,200 dispositae (7,200 ut) eget te Ridiculus magnae leo Arduas Nec sed 4,800 rationibus (900 ut) louor in vel integer te Nec Evidenter, Illa, eum Porro. Sem euismod'a crimen praevenire nec neque diabolum saepe, iniunctum vel Cadentes Modi, quo modo si intendis licuit sem vindices laesionem. Quo Quantum'v hitmari sint id Malrimonii, rem sit odio nascetur iste at Sociosqu."
        )
        text_output = remove_text_between_brackets(text_input)
        text_expected = "Sem Gordius at ea debile quantum si dis subordinatas Civiuni Magna. Ut oratione ut est enim subsolanea—aut Quasi Nemine hac dis Facer Eventu—mus quod 400 srripta firmare, annuebat p illum quas te 068,721 verbum displicere. Cum Memento si lorem 9,200 dispositae eget te Ridiculus magnae leo Arduas Nec sed 4,800 rationibus louor in vel integer te Nec Evidenter, Illa, eum Porro. Sem euismod'a crimen praevenire nec neque diabolum saepe, iniunctum vel Cadentes Modi, quo modo si intendis licuit sem vindices laesionem. Quo Quantum'v hitmari sint id Malrimonii, rem sit odio nascetur iste at Sociosqu."
        self.assertEqual(text_expected, text_output)

    def test_shorten_text_full_sentences(self):
        nltk.download("punkt")

        text_input = "Sem Gordius at ea debile quantum si dis subordinatas Civiuni Magna. Ut oratione ut est enim subsolanea—aut Quasi Nemine hac dis Facer Eventu—mus quod 400 srripta firmare, annuebat p illum quas te 068,721 verbum displicere. Cum Memento si lorem 9,200 dispositae eget te Ridiculus magnae leo Arduas Nec sed 4,800 rationibus louor in vel integer te Nec Evidenter, Illa, eum Porro. Sem euismod'a crimen praevenire nec neque diabolum saepe, iniunctum vel Cadentes Modi, quo modo si intendis licuit sem vindices laesionem. Quo Quantum'v hitmari sint id Malrimonii, rem sit odio nascetur iste at Sociosqu."
        text_output = shorten_text_full_sentences(text_input, char_limit=300)
        text_expected = "Sem Gordius at ea debile quantum si dis subordinatas Civiuni Magna. Ut oratione ut est enim subsolanea—aut Quasi Nemine hac dis Facer Eventu—mus quod 400 srripta firmare, annuebat p illum quas te 068,721 verbum displicere."
        self.assertEqual(text_expected, text_output)

        text_input = 'Non Divini te Litigiorum sem Cruciatus Potentiores ut v equestrem mi dui Totius in Modeste futuri hic M.V. Centimanos mi Sensus. Sed Poenam Coepit Leo EA 009–08, Minimum 582, dantis dis leo consultationis si EROS: "Sem Subiungam, hominem est Nobili in Dignitatis non Habitasse Abdicatione, animi fortiaue nisi dui Necessitas privatis scientiam perditionis si vigilantia mus dignissim frefquentia veritatem eius secundam, caesarianis, promotionibus, rem laboriosam ulterioribus alliciebat discursus ex dui Imperiosus."'
        text_output = shorten_text_full_sentences(text_input, char_limit=300)
        text_expected = "Non Divini te Litigiorum sem Cruciatus Potentiores ut v equestrem mi dui Totius in Modeste futuri hic M.V. Centimanos mi Sensus."
        self.assertEqual(text_expected, text_output)

    def test_get_wiki_description(self):
        country = {
            "uri": "https://en.wikipedia.org/w/api.php?action=query&format=json&prop=extracts&"
            "titles=Panama%7CZambia%7CMalta%7CMali%7CAzerbaijan%7CSenegal%7CBotswana%7CEl_Salvador%7C"
            "North_Macedonia%7CGuatemala%7CUzbekistan%7CMontenegro%7CSaint_Kitts_and_Nevis%7CBahrain%7C"
            "Syria%7CYemen%7CMongolia%7CGrenada%7CAlbania%7CR%C3%A9union&redirects=1&exintro=1&explaintext=1",
            "response_file_path": test_fixtures_folder("oa_web_workflow", "country_wiki_response.json"),
            "descriptions_file_path": test_fixtures_folder("oa_web_workflow", "country_wiki_descriptions.json"),
        }
        institution = {
            "uri": "https://en.wikipedia.org/w/api.php?action=query&format=json&prop=extracts&"
            "titles=Pontifical_Catholic_University_of_Peru%7CSt._John%27s_University_%28New_York_City%29%7C"
            "St_George%27s_Hospital%7CCalifornia_Polytechnic_State_University%7CUniversity_of_Bath%7C"
            "Indian_Institute_of_Technology_Gandhinagar%7CMichigan_Technological_University%7C"
            "University_of_Guam%7CUniversity_of_Maragheh%7CUniversity_of_Detroit_Mercy%7C"
            "Bath_Spa_University%7CCollege_of_Charleston%7CUniversidade_Federal_de_Goi%C3%A1s%7C"
            "University_of_Almer%C3%ADa%7CNational_University_of_Computer_and_Emerging_Sciences%7C"
            "Sefako_Makgatho_Health_Sciences_University%7CKuwait_Institute_for_Scientific_Research%7C"
            "Chinese_Academy_of_Tropical_Agricultural_Sciences%7CUniversidade_Federal_do_Pampa%7C"
            "Nationwide_Children%27s_Hospital&redirects=1&exintro=1&explaintext=1",
            "response_file_path": test_fixtures_folder("oa_web_workflow", "institution_wiki_response.json"),
            "descriptions_file_path": test_fixtures_folder("oa_web_workflow", "institution_wiki_descriptions.json"),
        }

        for entity in [country, institution]:
            # Download required nltk resource
            nltk.download("punkt")

            # Set up titles arg and expected descriptions
            with open(entity["descriptions_file_path"], "r") as f:
                descriptions_info = json.load(f)
            titles = {}
            descriptions = []
            for item in descriptions_info:
                id, title, description = item
                titles[title] = id
                descriptions.append((id, description))

            with httpretty.enabled():
                # Set up mocked successful response
                with open(entity["response_file_path"], "rb") as f:
                    body = f.read()
                httpretty.register_uri(httpretty.GET, entity["uri"], body=body)

                # Get wiki descriptions
                actual_descriptions = get_wiki_descriptions(titles)

            actual_descriptions.sort(key=lambda x: x[0])
            self.assertListEqual(descriptions, actual_descriptions)

            with httpretty.enabled():
                # Set up mocked  failed response
                httpretty.register_uri(httpretty.GET, entity["uri"], status=400)

                with self.assertRaises(AirflowException):
                    # Get wiki descriptions
                    get_wiki_descriptions(titles)


class TestOaWebRelease(TestCase):
    maxDiff = None

    def setUp(self) -> None:
        dt_fmt = "YYYY-MM-DD"
        self.release = OaWebRelease(
            dag_id="dag", project_id="project", release_date=pendulum.now(), data_bucket_name="data-bucket-name"
        )
        self.countries = [
            {
                "alpha2": "NZ",
                "id": "NZL",
                "name": "New Zealand",
                "year": 2020,
                "date": pendulum.date(2020, 12, 31).format(dt_fmt),
                "url": None,
                "wikipedia_url": "https://en.wikipedia.org/wiki/New_Zealand",
                "country": None,
                "subregion": "Australia and New Zealand",
                "region": "Oceania",
                "institution_types": None,
                "n_citations": 121,
                "n_outputs": 100,
                "n_outputs_open": 48,
                "n_outputs_publisher_open": 37,
                # "n_outputs_publisher_open_only": 11,
                # "n_outputs_both": 26,
                "n_outputs_other_platform_open": 37,
                "n_outputs_other_platform_open_only": 11,
                # "n_outputs_closed": 52,
                "n_outputs_oa_journal": 19,
                "n_outputs_hybrid": 10,
                "n_outputs_no_guarantees": 8,
                "identifiers": None,
            },
            {
                "alpha2": "NZ",
                "id": "NZL",
                "name": "New Zealand",
                "year": 2021,
                "date": pendulum.date(2021, 12, 31).format(dt_fmt),
                "url": None,
                "wikipedia_url": "https://en.wikipedia.org/wiki/New_Zealand",
                "country": None,
                "subregion": "Australia and New Zealand",
                "region": "Oceania",
                "institution_types": None,
                "n_citations": 233,
                "n_outputs": 100,
                "n_outputs_open": 45,
                "n_outputs_publisher_open": 37,
                # "n_outputs_publisher_open_only": 14,
                # "n_outputs_both": 24, 23?
                "n_outputs_other_platform_open": 31,
                "n_outputs_other_platform_open_only": 8,
                # "n_outputs_closed": 55,
                "n_outputs_oa_journal": 20,
                "n_outputs_hybrid": 9,
                "n_outputs_no_guarantees": 8,
                "identifiers": None,
            },
        ]
        self.institutions = [
            {
                "alpha2": None,
                "id": "https://ror.org/02n415q13",
                "name": "Curtin University",
                "year": 2020,
                "date": pendulum.date(2020, 12, 31).format(dt_fmt),
                "url": "https://curtin.edu.au/",
                "wikipedia_url": "https://en.wikipedia.org/wiki/Curtin_University",
                "country": "Australia",
                "subregion": "Australia and New Zealand",
                "region": "Oceania",
                "institution_types": ["Education"],
                "n_citations": 121,
                "n_outputs": 100,
                "n_outputs_open": 48,
                "n_outputs_publisher_open": 37,
                # "n_outputs_publisher_open_only": 11,
                # "n_outputs_both": 26,
                "n_outputs_other_platform_open": 37,
                "n_outputs_other_platform_open_only": 11,
                # "n_outputs_closed": 52,
                "n_outputs_oa_journal": 19,
                "n_outputs_hybrid": 10,
                "n_outputs_no_guarantees": 8,
                "identifiers": {
                    "ISNI": {"all": ["0000 0004 0375 4078"]},
                    "OrgRef": {"all": ["370725"]},
                    "Wikidata": {"all": ["Q1145497"]},
                    "GRID": {"preferred": "grid.1032.0"},
                    "FundRef": {"all": ["501100001797"]},
                },
            },
            {
                "alpha2": None,
                "id": "https://ror.org/02n415q13",
                "name": "Curtin University",
                "year": 2021,
                "date": pendulum.date(2021, 12, 31).format(dt_fmt),
                "url": "https://curtin.edu.au/",
                "wikipedia_url": "https://en.wikipedia.org/wiki/Curtin_University",
                "country": "Australia",
                "subregion": "Australia and New Zealand",
                "region": "Oceania",
                "institution_types": ["Education"],
                "n_citations": 233,
                "n_outputs": 100,
                "n_outputs_open": 45,
                "n_outputs_publisher_open": 37,
                # "n_outputs_publisher_open_only": 14,
                # "n_outputs_both": 24, 23?
                "n_outputs_other_platform_open": 31,
                "n_outputs_other_platform_open_only": 8,
                # "n_outputs_closed": 55,
                "n_outputs_oa_journal": 20,
                "n_outputs_hybrid": 9,
                "n_outputs_no_guarantees": 8,
                "identifiers": {
                    "ISNI": {"all": ["0000 0004 0375 4078"]},
                    "OrgRef": {"all": ["370725"]},
                    "Wikidata": {"all": ["Q1145497"]},
                    "GRID": {"preferred": "grid.1032.0"},
                    "FundRef": {"all": ["501100001797"]},
                },
            },
            {
                "alpha2": None,
                "id": "https://ror.org/12345",
                "name": "Foo University",
                "year": 2020,
                "date": pendulum.date(2020, 12, 31).format(dt_fmt),
                "url": None,
                "wikipedia_url": None,
                "country": "Australia",
                "subregion": "Australia and New Zealand",
                "region": "Oceania",
                "institution_types": ["Education"],
                "n_citations": 121,
                "n_outputs": 100,
                "n_outputs_open": 48,
                "n_outputs_publisher_open": 37,
                # "n_outputs_publisher_open_only": 11,
                # "n_outputs_both": 26,
                "n_outputs_other_platform_open": 37,
                "n_outputs_other_platform_open_only": 11,
                # "n_outputs_closed": 52,
                "n_outputs_oa_journal": 19,
                "n_outputs_hybrid": 10,
                "n_outputs_no_guarantees": 8,
                "identifiers": {
                    "ISNI": {"all": ["0000 0004 0375 4078"]},
                    "OrgRef": {"all": ["370725"]},
                    "Wikidata": {"all": ["Q1145497"]},
                    "GRID": {"preferred": "grid.1032.0"},
                    "FundRef": {"all": ["501100001797"]},
                },
            },
        ]
        self.entities = [
            ("country", self.countries, ["NZL"]),
            ("institution", self.institutions, ["02n415q13"]),
        ]

    def save_mock_data(self, category, test_data):
        path = os.path.join(self.release.download_folder, f"{category}.jsonl")
        with jsonlines.open(path, mode="w") as writer:
            writer.write_all(test_data)
        df = pd.DataFrame(test_data)
        return df

    @patch("academic_observatory_workflows.workflows.oa_web_workflow.Variable.get")
    def test_load_data(self, mock_var_get):
        category = "country"
        with CliRunner().isolated_filesystem() as t:
            mock_var_get.return_value = t

            # Save CSV
            df = self.save_mock_data(category, self.countries)

            # Load csv
            actual_df = self.release.load_data(category)

            # Compare
            expected_countries = df.to_dict("records")
            actual_countries = actual_df.to_dict("records")
            self.assertEqual(expected_countries, actual_countries)

    def test_update_df_with_percentages(self):
        keys = [("hello", "n_outputs"), ("world", "n_outputs")]
        df = pd.DataFrame([{"n_hello": 20, "n_world": 50, "n_outputs": 100}])
        self.release.update_df_with_percentages(df, keys)
        expected = {"n_hello": 20, "n_world": 50, "n_outputs": 100, "p_hello": 20, "p_world": 50}
        actual = df.to_dict(orient="records")[0]
        self.assertEqual(expected, actual)

    @patch("academic_observatory_workflows.workflows.oa_web_workflow.Variable.get")
    def test_make_index(self, mock_var_get):
        with CliRunner().isolated_filesystem() as t:
            mock_var_get.return_value = t

            # Country
            category = "country"
            df = pd.DataFrame(self.countries)
            df = self.release.preprocess_df(category, df)
            df_country_index = self.release.make_index(category, df)
            expected = [
                {
                    "alpha2": "NZ",
                    "category": "country",
                    "id": "NZL",
                    "name": "New Zealand",
                    "wikipedia_url": "https://en.wikipedia.org/wiki/New_Zealand",
                    "subregion": "Australia and New Zealand",
                    "region": "Oceania",
                    "n_citations": 354,
                    "n_outputs": 200,
                    "n_outputs_open": 93,
                    "n_outputs_publisher_open": 74,
                    "n_outputs_publisher_open_only": 25,
                    "n_outputs_both": 49,
                    "n_outputs_other_platform_open": 68,
                    "n_outputs_other_platform_open_only": 19,
                    "n_outputs_closed": 107,
                    "n_outputs_oa_journal": 39,
                    "n_outputs_hybrid": 19,
                    "n_outputs_no_guarantees": 16,
                    "p_outputs_open": 46.5,
                    "p_outputs_publisher_open": 37.0,
                    "p_outputs_publisher_open_only": 13.0,
                    "p_outputs_both": 25.0,
                    "p_outputs_other_platform_open": 34.0,
                    "p_outputs_other_platform_open_only": 9.0,
                    "p_outputs_closed": 53.0,
                    "p_outputs_oa_journal": 53.0,
                    "p_outputs_hybrid": 26.0,
                    "p_outputs_no_guarantees": 21.0,
                }
            ]
            print("Checking country records:")
            actual = df_country_index.to_dict("records")
            for e, a in zip(expected, actual):
                self.assertDictEqual(e, a)

            # Institution
            category = "institution"
            df = pd.DataFrame(self.institutions)
            df = self.release.preprocess_df(category, df)
            df_institution_index = self.release.make_index(category, df)

            expected = [
                {
                    "category": "institution",
                    "id": "02n415q13",
                    "name": "Curtin University",
                    "url": "https://curtin.edu.au/",
                    "wikipedia_url": "https://en.wikipedia.org/wiki/Curtin_University",
                    "country": "Australia",
                    "subregion": "Australia and New Zealand",
                    "region": "Oceania",
                    "institution_types": ["Education"],
                    "n_citations": 354,
                    "n_outputs": 200,
                    "n_outputs_open": 93,
                    "n_outputs_publisher_open": 74,
                    "n_outputs_publisher_open_only": 25,
                    "n_outputs_both": 49,
                    "n_outputs_other_platform_open": 68,
                    "n_outputs_other_platform_open_only": 19,
                    "n_outputs_closed": 107,
                    "n_outputs_oa_journal": 39,
                    "n_outputs_hybrid": 19,
                    "n_outputs_no_guarantees": 16,
                    "p_outputs_open": 46.5,
                    "p_outputs_publisher_open": 37.0,
                    "p_outputs_publisher_open_only": 13.0,
                    "p_outputs_both": 25.0,
                    "p_outputs_other_platform_open": 34.0,
                    "p_outputs_other_platform_open_only": 9.0,
                    "p_outputs_closed": 53.0,
                    "p_outputs_oa_journal": 53.0,
                    "p_outputs_hybrid": 26.0,
                    "p_outputs_no_guarantees": 21.0,
                    "identifiers": [
                        {"type": "ROR", "id": "02n415q13", "url": "https://ror.org/02n415q13"},
                        {
                            "type": "ISNI",
                            "id": "0000 0004 0375 4078",
                            "url": "https://isni.org/isni/0000 0004 0375 4078",
                        },
                        {"type": "Wikidata", "id": "Q1145497", "url": "https://www.wikidata.org/wiki/Q1145497"},
                        {"type": "GRID", "id": "grid.1032.0", "url": "https://grid.ac/institutes/grid.1032.0"},
                        {
                            "type": "FundRef",
                            "id": "501100001797",
                            "url": "https://api.crossref.org/funders/501100001797",
                        },
                    ],
                }
            ]

            print("Checking institution records:")
            actual = df_institution_index.to_dict("records")
            for e, a in zip(expected, actual):
                self.assertDictEqual(e, a)

    @patch("academic_observatory_workflows.workflows.oa_web_workflow.Variable.get")
    def test_update_index_with_logos(self, mock_var_get):
        with CliRunner().isolated_filesystem() as t:
            mock_var_get.return_value = t
            sizes = ["l", "s"]

            # Country table
            category = "country"
            df = pd.DataFrame(self.countries)
            df = self.release.preprocess_df(category, df)
            df_index_table = self.release.make_index(category, df)
            self.release.update_index_with_logos(category, df_index_table)
            for i, row in df_index_table.iterrows():
                for size in sizes:
                    # Check that logo key created
                    key = f"logo_{size}"
                    self.assertTrue(key in row)

                    # Check that correct logo path exists
                    item_id = row["id"]
                    expected_path = f"/logos/{category}/{size}/{item_id}.svg"
                    actual_path = row[key]
                    self.assertEqual(expected_path, actual_path)

            # Institution table
            category = "institution"
            df = pd.DataFrame(self.institutions)
            df = self.release.preprocess_df(category, df)
            df_index_table = self.release.make_index(category, df)
            with vcr.use_cassette(test_fixtures_folder("oa_web_workflow", "test_make_logos.yaml")):
                self.release.update_index_with_logos(category, df_index_table)
                curtin_row = df_index_table.loc["02n415q13"]
                foo_row = df_index_table.loc["12345"]
                for size in sizes:
                    # Check that logo was added to dataframe
                    key = f"logo_{size}"
                    self.assertTrue(key in curtin_row)
                    self.assertTrue(key in foo_row)

                    # Check that correct path created
                    item_id = curtin_row["id"]
                    expected_curtin_path = f"/logos/{category}/{size}/{item_id}.jpg"
                    expected_foo_path = f"/unknown.svg"
                    self.assertEqual(expected_curtin_path, curtin_row[key])
                    self.assertEqual(expected_foo_path, foo_row[key])

                    # Check that downloaded logo exists
                    full_path = os.path.join(self.release.build_path, expected_curtin_path[1:])
                    self.assertTrue(os.path.isfile(full_path))

    @patch("academic_observatory_workflows.workflows.oa_web_workflow.Variable.get")
    def test_save_index(self, mock_var_get):
        with CliRunner().isolated_filesystem() as t:
            mock_var_get.return_value = t

            for category, data, entity_ids in self.entities:
                df = pd.DataFrame(data)
                df = self.release.preprocess_df(category, df)
                country_index = self.release.make_index(category, df)
                self.release.update_index_with_logos(category, country_index)
                self.release.save_index(category, country_index)

                path = os.path.join(self.release.build_path, "data", f"{category}.json")
                self.assertTrue(os.path.isfile(path))

    @patch("academic_observatory_workflows.workflows.oa_web_workflow.Variable.get")
    def test_make_entities(self, mock_var_get):
        with CliRunner().isolated_filesystem() as t:
            mock_var_get.return_value = t

            # Country
            category = "country"
            df = pd.DataFrame(self.countries)
            df = self.release.preprocess_df(category, df)
            df_index_table = self.release.make_index(category, df)
            entities = self.release.make_entities(df_index_table, df)

            expected = [
                {
                    "id": "NZL",
                    "name": "New Zealand",
                    "category": category,
                    "description": {
                        "license": Description.license,
                        "text": None,
                        "url": "https://en.wikipedia.org/wiki/New_Zealand",
                    },
                    "wikipedia_url": "https://en.wikipedia.org/wiki/New_Zealand",
                    "subregion": "Australia and New Zealand",
                    "region": "Oceania",
                    "max_year": 2021,
                    "min_year": 2020,
                    "stats": {
                        "n_citations": 354,
                        "n_outputs": 200,
                        "n_outputs_open": 93,
                        "n_outputs_publisher_open": 74,
                        "n_outputs_publisher_open_only": 25,
                        "n_outputs_both": 49,
                        "n_outputs_other_platform_open": 68,
                        "n_outputs_other_platform_open_only": 19,
                        "n_outputs_closed": 107,
                        "n_outputs_oa_journal": 39,
                        "n_outputs_hybrid": 19,
                        "n_outputs_no_guarantees": 16,
                        "p_outputs_open": 46.5,
                        "p_outputs_publisher_open": 37.0,
                        "p_outputs_publisher_open_only": 13.0,
                        "p_outputs_both": 25.0,
                        "p_outputs_other_platform_open": 34.0,
                        "p_outputs_other_platform_open_only": 9.0,
                        "p_outputs_closed": 53.0,
                        "p_outputs_oa_journal": 53.0,
                        "p_outputs_hybrid": 26.0,
                        "p_outputs_no_guarantees": 21.0,
                    },
                    "timeseries": [
                        {
                            "year": 2020,
                            "date": "2020-12-31",
                            "stats": {
                                "n_citations": 121,
                                "n_outputs": 100,
                                "n_outputs_open": 48,
                                "n_outputs_publisher_open": 37,
                                "n_outputs_publisher_open_only": 11,
                                "n_outputs_both": 26,
                                "n_outputs_other_platform_open": 37,
                                "n_outputs_other_platform_open_only": 11,
                                "n_outputs_closed": 52,
                                "n_outputs_oa_journal": 19,
                                "n_outputs_hybrid": 10,
                                "n_outputs_no_guarantees": 8,
                                "p_outputs_open": 48.0,
                                "p_outputs_publisher_open": 37.0,
                                "p_outputs_publisher_open_only": 11.0,
                                "p_outputs_both": 26.0,
                                "p_outputs_other_platform_open": 37.0,
                                "p_outputs_other_platform_open_only": 11.0,
                                "p_outputs_closed": 52.0,
                                "p_outputs_oa_journal": 51.0,
                                "p_outputs_hybrid": 27.0,
                                "p_outputs_no_guarantees": 22.0,
                            },
                        },
                        {
                            "year": 2021,
                            "date": "2021-12-31",
                            "stats": {
                                "n_citations": 233,
                                "n_outputs": 100,
                                "n_outputs_open": 45,
                                "n_outputs_publisher_open": 37,
                                "n_outputs_publisher_open_only": 14,
                                "n_outputs_both": 23,
                                "n_outputs_other_platform_open": 31,
                                "n_outputs_other_platform_open_only": 8,
                                "n_outputs_closed": 55,
                                "n_outputs_oa_journal": 20,
                                "n_outputs_hybrid": 9,
                                "n_outputs_no_guarantees": 8,
                                "p_outputs_open": 45.0,
                                "p_outputs_publisher_open": 37.0,
                                "p_outputs_publisher_open_only": 14.0,
                                "p_outputs_both": 23.0,
                                "p_outputs_other_platform_open": 31.0,
                                "p_outputs_other_platform_open_only": 8.0,
                                "p_outputs_closed": 55.0,
                                "p_outputs_oa_journal": 54.0,
                                "p_outputs_hybrid": 24.0,
                                "p_outputs_no_guarantees": 22.0,
                            },
                        },
                    ],
                }
            ]

            for a_entity, e_entity in zip(expected, entities):
                self.assertDictEqual(a_entity, e_entity.to_dict())

        # Institution
        category = "institution"
        df = pd.DataFrame(self.institutions)
        df = self.release.preprocess_df(category, df)
        df_index_table = self.release.make_index(category, df)
        entities = self.release.make_entities(df_index_table, df)

        expected = [
            {
                "id": "02n415q13",
                "name": "Curtin University",
                "country": "Australia",
                "description": {
                    "license": Description.license,
                    "text": None,
                    "url": "https://en.wikipedia.org/wiki/Curtin_University",
                },
                "category": category,
                "url": "https://curtin.edu.au/",
                "wikipedia_url": "https://en.wikipedia.org/wiki/Curtin_University",
                "subregion": "Australia and New Zealand",
                "region": "Oceania",
                "institution_types": ["Education"],
                "max_year": 2021,
                "min_year": 2020,
                "identifiers": [
                    {"type": "ROR", "id": "02n415q13", "url": "https://ror.org/02n415q13"},
                    {"type": "ISNI", "id": "0000 0004 0375 4078", "url": "https://isni.org/isni/0000 0004 0375 4078"},
                    {"type": "Wikidata", "id": "Q1145497", "url": "https://www.wikidata.org/wiki/Q1145497"},
                    {"type": "GRID", "id": "grid.1032.0", "url": "https://grid.ac/institutes/grid.1032.0"},
                    {"type": "FundRef", "id": "501100001797", "url": "https://api.crossref.org/funders/501100001797"},
                ],
                "stats": {
                    "n_citations": 354,
                    "n_outputs": 200,
                    "n_outputs_open": 93,
                    "n_outputs_publisher_open": 74,
                    "n_outputs_publisher_open_only": 25,
                    "n_outputs_both": 49,
                    "n_outputs_other_platform_open": 68,
                    "n_outputs_other_platform_open_only": 19,
                    "n_outputs_closed": 107,
                    "n_outputs_oa_journal": 39,
                    "n_outputs_hybrid": 19,
                    "n_outputs_no_guarantees": 16,
                    "p_outputs_open": 46.5,
                    "p_outputs_publisher_open": 37.0,
                    "p_outputs_publisher_open_only": 13.0,
                    "p_outputs_both": 25.0,
                    "p_outputs_other_platform_open": 34.0,
                    "p_outputs_other_platform_open_only": 9.0,
                    "p_outputs_closed": 53.0,
                    "p_outputs_oa_journal": 53.0,
                    "p_outputs_hybrid": 26.0,
                    "p_outputs_no_guarantees": 21.0,
                },
                "timeseries": [
                    {
                        "year": 2020,
                        "date": "2020-12-31",
                        "stats": {
                            "n_citations": 121,
                            "n_outputs": 100,
                            "n_outputs_open": 48,
                            "n_outputs_publisher_open": 37,
                            "n_outputs_publisher_open_only": 11,
                            "n_outputs_both": 26,
                            "n_outputs_other_platform_open": 37,
                            "n_outputs_other_platform_open_only": 11,
                            "n_outputs_closed": 52,
                            "n_outputs_oa_journal": 19,
                            "n_outputs_hybrid": 10,
                            "n_outputs_no_guarantees": 8,
                            "p_outputs_open": 48.0,
                            "p_outputs_publisher_open": 37.0,
                            "p_outputs_publisher_open_only": 11.0,
                            "p_outputs_both": 26.0,
                            "p_outputs_other_platform_open": 37.0,
                            "p_outputs_other_platform_open_only": 11.0,
                            "p_outputs_closed": 52.0,
                            "p_outputs_oa_journal": 51.0,
                            "p_outputs_hybrid": 27.0,
                            "p_outputs_no_guarantees": 22.0,
                        },
                    },
                    {
                        "year": 2021,
                        "date": "2021-12-31",
                        "stats": {
                            "n_citations": 233,
                            "n_outputs": 100,
                            "n_outputs_open": 45,
                            "n_outputs_publisher_open": 37,
                            "n_outputs_publisher_open_only": 14,
                            "n_outputs_both": 23,
                            "n_outputs_other_platform_open": 31,
                            "n_outputs_other_platform_open_only": 8,
                            "n_outputs_closed": 55,
                            "n_outputs_oa_journal": 20,
                            "n_outputs_hybrid": 9,
                            "n_outputs_no_guarantees": 8,
                            "p_outputs_open": 45.0,
                            "p_outputs_publisher_open": 37.0,
                            "p_outputs_publisher_open_only": 14.0,
                            "p_outputs_both": 23.0,
                            "p_outputs_other_platform_open": 31.0,
                            "p_outputs_other_platform_open_only": 8.0,
                            "p_outputs_closed": 55.0,
                            "p_outputs_oa_journal": 54.0,
                            "p_outputs_hybrid": 24.0,
                            "p_outputs_no_guarantees": 22.0,
                        },
                    },
                ],
            }
        ]

        for a_entity, e_entity in zip(expected, entities):
            self.assertDictEqual(a_entity, e_entity.to_dict())

    @patch("academic_observatory_workflows.workflows.oa_web_workflow.Variable.get")
    def test_save_entities(self, mock_var_get):
        with CliRunner().isolated_filesystem() as t:
            mock_var_get.return_value = t

            for category, data, entity_ids in self.entities:
                # Read data
                df = pd.DataFrame(data)
                df = self.release.preprocess_df(category, df)

                # Save entities
                df_index_table = self.release.make_index(category, df)
                entities = self.release.make_entities(df_index_table, df)
                self.release.save_entities(category, entities)

                # Check that entity json files are saved
                for entity_id in entity_ids:
                    path = os.path.join(self.release.build_path, "data", category, f"{entity_id}.json")
                    print(f"Assert exists: {path}")
                    self.assertTrue(os.path.isfile(path))

    def test_make_auto_complete(self):
        category = "country"
        expected = [
            {"id": "NZL", "name": "New Zealand", "logo_s": "/logos/country/NZL.svg"},
            {"id": "AUS", "name": "Australia", "logo_s": "/logos/country/AUS.svg"},
            {"id": "USA", "name": "United States", "logo_s": "/logos/country/USA.svg"},
        ]
        df = pd.DataFrame(expected)
        records = self.release.make_auto_complete(df, category)
        for e in expected:
            e["category"] = category
        self.assertEqual(expected, records)

    @patch("academic_observatory_workflows.workflows.oa_web_workflow.Variable.get")
    def test_save_autocomplete(self, mock_var_get):
        with CliRunner().isolated_filesystem() as t:
            mock_var_get.return_value = t
            category = "country"
            expected = [
                {"id": "NZL", "name": "New Zealand", "logo_s": "/logos/country/NZL.svg"},
                {"id": "AUS", "name": "Australia", "logo_s": "/logos/country/AUS.svg"},
                {"id": "USA", "name": "United States", "logo_s": "/logos/country/USA.svg"},
            ]
            df = pd.DataFrame(expected)
            records = self.release.make_auto_complete(df, category)
            self.release.save_autocomplete(records)

            path = os.path.join(self.release.build_path, "data", "autocomplete.json")
            self.assertTrue(os.path.isfile(path))


class TestOaWebWorkflow(ObservatoryTestCase):
    def setUp(self) -> None:
        """TestOaWebWorkflow checks that the workflow functions correctly, i.e. outputs the correct files, but doesn't
        check that the calculations are correct (data correctness is tested in TestOaWebRelease)."""

        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")
        self.oa_web_fixtures = "oa_web_workflow"

    def test_dag_structure(self):
        """Test that the DAG has the correct structure.

        :return: None
        """

        env = ObservatoryEnvironment(enable_api=False)
        with env.create():
            dag = OaWebWorkflow().make_dag()
            self.assert_dag_structure(
                {
                    "doi_sensor": ["check_dependencies"],
                    "check_dependencies": ["query"],
                    "query": ["download"],
                    "download": ["transform"],
                    "transform": ["upload_dataset"],
                    "upload_dataset": ["repository_dispatch"],
                    "repository_dispatch": ["cleanup"],
                    "cleanup": [],
                },
                dag,
            )

    def test_dag_load(self):
        """Test that the DAG can be loaded from a DAG bag.

        :return: None
        """

        env = ObservatoryEnvironment(project_id=self.project_id, data_location=self.data_location, enable_api=False)
        with env.create():
            dag_file = os.path.join(module_file_path("academic_observatory_workflows.dags"), "oa_web_workflow.py")
            self.assert_dag_load("oa_web_workflow", dag_file)

    def setup_tables(
        self, dataset_id_all: str, dataset_id_settings: str, bucket_name: str, release_date: pendulum.DateTime
    ):
        ror = load_jsonl(test_fixtures_folder("doi", "ror.jsonl"))
        country = load_jsonl(test_fixtures_folder(self.oa_web_fixtures, "country.jsonl"))
        institution = load_jsonl(test_fixtures_folder(self.oa_web_fixtures, "institution.jsonl"))
        settings_country = load_jsonl(test_fixtures_folder("doi", "country.jsonl"))

        analysis_schema_path = schema_folder()
        oa_web_schema_path = test_fixtures_folder(self.oa_web_fixtures, "schema")
        with CliRunner().isolated_filesystem() as t:
            tables = [
                Table("ror", True, dataset_id_all, ror, "ror", analysis_schema_path),
                Table("country", True, dataset_id_all, country, "country", oa_web_schema_path),
                Table("institution", True, dataset_id_all, institution, "institution", oa_web_schema_path),
                Table(
                    "country",
                    False,
                    dataset_id_settings,
                    settings_country,
                    "country",
                    analysis_schema_path,
                ),
            ]

            bq_load_tables(
                tables=tables, bucket_name=bucket_name, release_date=release_date, data_location=self.data_location
            )

    @patch("academic_observatory_workflows.workflows.oa_web_workflow.trigger_repository_dispatch")
    def test_telescope(self, mock_trigger_repository_dispatch):
        """Test the telescope end to end.

        :return: None.
        """

        execution_date = pendulum.datetime(2021, 11, 13)
        env = ObservatoryEnvironment(project_id=self.project_id, data_location=self.data_location, enable_api=False)
        dataset_id = env.add_dataset("data")
        dataset_id_settings = env.add_dataset("settings")
        data_bucket = env.add_bucket()
        github_token = "github-token"

        with env.create() as t:
            # Add data bucket variable
            env.add_variable(Variable(key=OaWebWorkflow.DATA_BUCKET, val=data_bucket))

            # Add Github token connection
            env.add_connection(Connection(conn_id=OaWebWorkflow.GITHUB_TOKEN_CONN, uri=f"http://:{github_token}@"))

            # Run fake DOI workflow
            dag = make_dummy_dag("doi", execution_date)
            with env.create_dag_run(dag, execution_date):
                # Running all of a DAGs tasks sets the DAG to finished
                ti = env.run_task("dummy_task")
                self.assertEqual(State.SUCCESS, ti.state)

            # Upload fake data to BigQuery
            self.setup_tables(
                dataset_id_all=dataset_id,
                dataset_id_settings=dataset_id_settings,
                bucket_name=env.download_bucket,
                release_date=execution_date,
            )

            # Run workflow
            workflow = OaWebWorkflow(
                agg_dataset_id=dataset_id, ror_dataset_id=dataset_id, settings_dataset_id=dataset_id_settings
            )

            dag = workflow.make_dag()
            with env.create_dag_run(dag, execution_date):
                # DOI Sensor
                ti = env.run_task("doi_sensor")
                self.assertEqual(State.SUCCESS, ti.state)

                # Check dependencies
                ti = env.run_task(workflow.check_dependencies.__name__)
                self.assertEqual(State.SUCCESS, ti.state)

                # Run query
                ti = env.run_task(workflow.query.__name__)
                self.assertEqual(State.SUCCESS, ti.state)

                # Download data
                ti = env.run_task(workflow.download.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                base_folder = os.path.join(
                    t, "data", "telescopes", "download", "oa_web_workflow", "oa_web_workflow_2021_11_13"
                )
                expected_file_names = ["country.jsonl", "institution.jsonl"]
                for file_name in expected_file_names:
                    path = os.path.join(base_folder, file_name)
                    self.assertTrue(os.path.isfile(path))

                # Transform data
                ti = env.run_task(workflow.transform.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                base_folder = os.path.join(
                    t, "data", "telescopes", "transform", "oa_web_workflow", "oa_web_workflow_2021_11_13"
                )
                build_folder = os.path.join(base_folder, "build")
                expected_files = make_expected_build_files(build_folder)
                print("Checking expected transformed files")
                for file in expected_files:
                    print(f"\t{file}")
                    self.assertTrue(os.path.isfile(file))

                # Check that zip file exists
                latest_file = os.path.join(base_folder, "latest.zip")
                print(f"\t{latest_file}")
                self.assertTrue(os.path.isfile(latest_file))

                # Upload data to bucket
                ti = env.run_task(workflow.upload_dataset.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                blob_name = f"{workflow.version}/latest.zip"
                self.assert_blob_exists(data_bucket, blob_name)

                # Trigger repository dispatch
                ti = env.run_task(workflow.repository_dispatch.__name__)
                self.assertEqual(State.SUCCESS, ti.state)
                mock_trigger_repository_dispatch.called_once_with(github_token, "data-update/develop")
                mock_trigger_repository_dispatch.called_once_with(github_token, "data-update/staging")
                mock_trigger_repository_dispatch.called_once_with(github_token, "data-update/production")

                # Test that all telescope data deleted
                download_folder, extract_folder, transform_folder = (
                    os.path.join(t, "data", "telescopes", "download", "oa_web_workflow", "oa_web_workflow_2021_11_13"),
                    os.path.join(t, "data", "telescopes", "extract", "oa_web_workflow", "oa_web_workflow_2021_11_13"),
                    os.path.join(t, "data", "telescopes", "transform", "oa_web_workflow", "oa_web_workflow_2021_11_13"),
                )
                env.run_task(workflow.cleanup.__name__)
                self.assert_cleanup(download_folder, extract_folder, transform_folder)


def make_expected_build_files(base_path: str) -> List[str]:
    countries = ["AUS", "NZL"]
    institutions = ["03b94tp07", "02n415q13"]  # Auckland, Curtin
    categories = ["country"] * len(countries) + ["institution"] * len(institutions)
    entity_ids = countries + institutions
    expected = []

    # Add base data files
    data_path = os.path.join(base_path, "data")
    file_names = [
        "stats.json",
        "autocomplete.json",
        "autocomplete.parquet",
        "country.json",
        "country.parquet",
        "institution.json",
        "institution.parquet",
    ]
    for file_name in file_names:
        expected.append(os.path.join(data_path, file_name))

    # Add country and institution specific data files
    for category, entity_id in zip(categories, entity_ids):
        path = os.path.join(data_path, category, f"{entity_id}.json")
        expected.append(path)

    # Add logos
    for category, entity_id in zip(categories, entity_ids):
        file_name = f"{entity_id}.svg"
        if category == "institution":
            file_name = f"{entity_id}.jpg"
        for size in ["l", "s"]:
            path = os.path.join(base_path, "logos", category, size, file_name)
            expected.append(path)

    return expected
