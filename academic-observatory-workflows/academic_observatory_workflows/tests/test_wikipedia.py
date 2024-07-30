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
import os.path
from unittest import TestCase

import nltk
import vcr

from academic_observatory_workflows.config import project_path
from academic_observatory_workflows.wikipedia import (
    fetch_wikipedia_descriptions_batch,
    remove_text_between_brackets,
    shorten_text_full_sentences,
)

FIXTURES_FOLDER = project_path("tests", "fixtures")


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

    def test_fetch_wiki_description(self):
        # Download required nltk resource
        nltk.download("punkt")

        wikipedia_urls = [
            "https://en.wikipedia.org/wiki/Pontifical_University_of_John_Paul_II",
            "https://en.wikipedia.org/wiki/Mali",
            "https://en.wikipedia.org/wiki/Indonesia",
            "https://en.wikipedia.org/wiki/Office_of_Scientific_and_Technical_Information",
            "https://en.wikipedia.org/wiki/National_Oilwell_Varco#Awards_and_Accolades",
            "https://en.wikipedia.org/wiki/%C3%85land",
            "https://en.wikipedia.org/wiki/Universiti_Teknologi_MARA_System",
            "https://en.wikipedia.org/wiki/Universiti_Teknologi_MARA",
            "https://en.wikipedia.org/wiki/Kellogg%27s",
            "https://en.wikipedia.org/wiki/Kellogg's",
        ]
        path = os.path.join(FIXTURES_FOLDER, "expected_fetch_wiki_descriptions.json")
        with open(path, "r") as f:
            expected = json.load(f)

        # Make expected in same format as results and sort
        expected = [tuple(row) for row in expected]
        expected.sort(key=lambda x: x[0])

        with vcr.use_cassette(os.path.join(FIXTURES_FOLDER, "test_fetch_wiki_description.yaml")):
            actual = fetch_wikipedia_descriptions_batch(wikipedia_urls)

        # Sort actual results based on Wikipedia url so that they match the expected results
        actual.sort(key=lambda x: x[0])
        self.assertEqual(expected, actual)
