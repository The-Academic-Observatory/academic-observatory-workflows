# Copyright 2021-2022 Curtin University
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

from __future__ import annotations

import logging
import urllib.parse
from typing import Dict, List, Tuple

import nltk
import requests
from airflow.exceptions import AirflowException


def fetch_wiki_descriptions(wikipedia_urls: List) -> List[Tuple[str, str]]:
    """Fetch the wikipedia descriptions for a set of Wikipedia URLs

    :param wikipedia_urls: a list of Wikipedia URLs.
    :return: List with tuples (id, wiki description)
    """

    titles = []
    title_url_index = {}
    for url in wikipedia_urls:
        # Extract title from URL
        title = url.split("wikipedia.org/wiki/")[-1].split("#")[0]
        title_url_index[urllib.parse.unquote(title)] = url

        # URL encode title if it is not encoded yet
        if title == urllib.parse.unquote(title):
            titles.append(urllib.parse.quote(title))
        # Append title directly if it is already encoded and not empty
        else:
            titles.append(title)

    # Confirm that there is a max of 20 titles, the limit for the wikipedia API
    assert len(titles) <= 20

    # Extract descriptions using the Wikipedia API
    url = f"https://en.wikipedia.org/w/api.php?action=query&format=json&prop=extracts&titles={'%7C'.join(titles)}&redirects=1&exintro=1&explaintext=1"
    response = requests.get(url)
    if response.status_code != 200:
        raise AirflowException(f"Unsuccessful retrieving wikipedia extracts, url: {url}")
    response_json = response.json()
    pages = response_json["query"]["pages"]

    # Create mapping between redirected/normalized page title and original page title
    redirects = {}
    for title in response_json["query"].get("redirects", []):
        redirects[title["to"]] = title["from"]
    normalized = {}
    for title in response_json["query"].get("normalized", []):
        normalized[title["to"]] = title["from"]

    # Resolve redirects referring to each other to 1 redirect
    for key, value in redirects.copy().items():
        if value in redirects:
            redirects[key] = redirects.pop(value)

    # Create mapping between Wikipedia URL and decoded page title.
    results = []
    for page_id, page in pages.items():
        page_title = page["title"]

        # Get page_title from redirected/normalized if it is present
        page_title = redirects.get(page_title, page_title)
        page_title = normalized.get(page_title, page_title)

        # Link original url to description
        wikipedia_url = title_url_index[urllib.parse.unquote(page_title)]

        # Get description and clean up
        description = page.get("extract", "")
        if description:
            description = remove_text_between_brackets(description)
            description = shorten_text_full_sentences(description)

        results.append((wikipedia_url, description))

    return results


def remove_text_between_brackets(text: str) -> str:
    """Remove any text between (nested) brackets.
    If there is a space after the opening bracket, this is removed as well.
    E.g. 'Like this (foo, (bar)) example' -> 'Like this example'

    :param text: The text to modify
    :return: The modified text
    """
    new_text = []
    nested = 0
    for char in text:
        if char == "(":
            nested += 1
            new_text = new_text[:-1] if new_text and new_text[-1] == " " else new_text
        elif (char == ")") and nested:
            nested -= 1
        elif nested == 0:
            new_text.append(char)
    return "".join(new_text).strip()


def shorten_text_full_sentences(text: str, *, char_limit: int = 300) -> str:
    """Shorten a text to as many complete sentences as possible, while the total number of characters stays below
    the char_limit.
    Always return at least one sentence, even if this exceeds the char_limit.

    :param text: A string with the complete text
    :param char_limit: The max number of characters
    :return: The shortened text.
    """
    # Create list of sentences
    sentences = nltk.tokenize.sent_tokenize(text)

    # Add sentences until char limit is reached
    sentences_output = []
    total_len = 0
    for sentence in sentences:
        total_len += len(sentence)
        if (total_len > char_limit) and sentences_output:
            break
        sentences_output.append(sentence)
    return " ".join(sentences_output)