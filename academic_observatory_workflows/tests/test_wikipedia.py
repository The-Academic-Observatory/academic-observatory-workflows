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

# Author: Aniek Roelofs, James Diprose

import json
from unittest import TestCase

import httpretty
import nltk
from airflow.exceptions import AirflowException

from academic_observatory_workflows.config import test_fixtures_folder
from academic_observatory_workflows.wikipedia import (
    fetch_wiki_descriptions,
    shorten_text_full_sentences,
    remove_text_between_brackets,
)


class TestWikipedia(TestCase):
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
                actual_descriptions = fetch_wiki_descriptions(titles)

            actual_descriptions.sort(key=lambda x: x[0])
            self.assertListEqual(descriptions, actual_descriptions)

            with httpretty.enabled():
                # Set up mocked  failed response
                httpretty.register_uri(httpretty.GET, entity["uri"], status=400)

                with self.assertRaises(AirflowException):
                    # Get wiki descriptions
                    fetch_wiki_descriptions(titles)
