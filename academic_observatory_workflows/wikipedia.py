# Copyright 2021-2024 Curtin University
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
from concurrent.futures import as_completed, ThreadPoolExecutor
from typing import List, Tuple
from urllib.parse import unquote, urlparse

import nltk
import requests
from airflow.exceptions import AirflowException

from observatory.platform.files import get_chunks

WIKI_MAX_TITLES = 20  # Set the number of titles for which wiki descriptions are retrieved at once, the API can return max 20 extracts.


def fetch_wikipedia_descriptions(wikipedia_urls: List[str]) -> List[Tuple[str, str]]:
    """Get the wikipedia descriptions for each entity (institution or country).

    :param wikipedia_urls: a list of Wikipedia URLs.
    :return: a list of tuples containing Wikipedia URL and Wikipedia description.
    """

    # Download 'punkt' resource, required when shortening wiki descriptions
    nltk.download("punkt")

    # Create list with dictionaries of max 20 ids + titles (this is wiki api max)
    chunks = list(get_chunks(input_list=wikipedia_urls, chunk_size=WIKI_MAX_TITLES))
    n_wikipedia_urls = len(wikipedia_urls)
    n_chunks = len(chunks)
    logging.info(f"Downloading {n_wikipedia_urls} wikipedia descriptions in {n_chunks} chunks.")

    # Process each dictionary in separate thread to get wiki descriptions
    futures, results = [], []
    with ThreadPoolExecutor() as executor:
        # Queue tasks
        for chunk in chunks:
            futures.append(executor.submit(fetch_wikipedia_descriptions_batch, chunk))

        # Wait for results
        for completed in as_completed(futures):
            results += completed.result()

            # Print progress
            n_downloaded = len(results)
            p_progress = n_downloaded / n_wikipedia_urls * 100
            logging.info(f"Downloading descriptions {n_downloaded}/{n_wikipedia_urls}: {p_progress:.2f}%")

    logging.info(f"Finished downloading wikipedia descriptions")
    logging.info(f"Expected results: {n_wikipedia_urls}, actual num descriptions returned: {n_downloaded}")
    if n_wikipedia_urls != n_downloaded:
        expected = set(wikipedia_urls)
        actual = set([url for url, desc in results])
        missing = expected - actual
        new = actual - expected
        logging.error(f"Num duplicate Wikipedia URLs: {len(wikipedia_urls) - len(expected)}")
        logging.error(f"Missing Wikipedia descriptions for: {missing}")
        logging.error(f"Unexpected Wikipedia descriptions for: {new}")

        raise Exception(f"Number of Wikipedia descriptions returned does not match the number of Wikipedia URLs sent")

    return results


def get_wikipedia_title(url: str) -> str:
    """Get a Wikipedia title from a Wikipedia URL.

    :param url: a Wikipedia URL.
    :return: the title.
    """

    parsed = urlparse(url)
    return parsed.path.split("/wiki/")[-1]


def fetch_wikipedia_descriptions_batch(urls: List) -> List[Tuple[str, str]]:
    """Fetch the wikipedia descriptions for a set of Wikipedia URLs

    :param urls: a list of Wikipedia URLs.
    :return: List with tuples (id, wiki description)
    """

    # Extract titles from URLs
    # URLs may be quoted or unquoted, so unquote titles to prevent requests from double quoting them
    titles = []
    for url in urls:
        title = get_wikipedia_title(url)
        titles.append(unquote(title))

    # Confirm that there is a max of 20 titles, the limit for the wikipedia API
    assert len(titles) <= 20

    # Extract descriptions using the Wikipedia API
    params = {
        "action": "query",
        "format": "json",
        "prop": "extracts",
        "titles": "|".join(titles),
        "redirects": "1",
        "exintro": "1",
        "explaintext": "1",
    }
    response = requests.get("https://en.wikipedia.org/w/api.php", params=params)
    if response.status_code != 200:
        raise AirflowException(f"Unsuccessfully retrieved Wikipedia extracts, url: {response.url}")
    response = response.json()
    pages = response["query"]["pages"]

    # Create redirect index
    redirects = {}
    for redirect in response["query"].get("redirects", []):
        redirects[redirect["from"]] = redirect["to"]

    # Create normalized index
    normalized = {}
    for norm in response["query"].get("normalized", []):
        normalized[norm["from"]] = norm["to"]

    # Create description index
    descriptions = {}
    for page_id, page in pages.items():
        description = page.get("extract", "")
        if description:
            # Cleanup description
            description = remove_text_between_brackets(description)
            description = shorten_text_full_sentences(description)
        descriptions[page["title"]] = description

    # Get description for each original URL
    results = []
    for url in urls:
        title = get_wikipedia_title(url)
        title_unquoted = unquote(title)
        title_norm = normalized.get(title_unquoted, title_unquoted)
        title_redirect = redirects.get(title_norm, title_norm)
        description = descriptions[title_redirect]
        results.append((url, description))

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
