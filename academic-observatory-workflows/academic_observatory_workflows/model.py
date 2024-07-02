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

# Author: James Diprose, Tuan Chien

from __future__ import annotations

import math
import os
import random
import urllib.parse
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Tuple

import pandas as pd
import pendulum
from click.testing import CliRunner
from faker import Faker
from observatory.platform.bigquery import bq_find_schema
from observatory.platform.files import load_jsonl
from observatory.platform.observatory_environment import bq_load_tables, Table
from pendulum import DateTime

from academic_observatory_workflows.config import project_path

LICENSES = ["cc-by", None]

EVENT_TYPES = [
    "f1000",
    "stackexchange",
    "datacite",
    "twitter",
    "reddit-links",
    "wordpressdotcom",
    "plaudit",
    "cambia-lens",
    "hypothesis",
    "wikipedia",
    "reddit",
    "crossref",
    "newsfeed",
    "web",
]

OUTPUT_TYPES = [
    "journal_articles",
    "book_sections",
    "authored_books",
    "edited_volumes",
    "reports",
    "datasets",
    "proceedings_article",
    "other_outputs",
]

FUNDREF_COUNTRY_CODES = ["usa", "gbr", "aus", "can"]

FUNDREF_REGIONS = {"usa": "Americas", "gbr": "Europe", "aus": "Oceania", "can": "Americas"}

FUNDING_BODY_TYPES = [
    "For-profit companies (industry)",
    "Trusts, charities, foundations (both public and private)",
    "Associations and societies (private and public)",
    "National government",
    "Universities (academic only)",
    "International organizations",
    "Research institutes and centers",
    "Other non-profit organizations",
    "Local government",
    "Libraries and data archiving organizations",
]

FUNDING_BODY_SUBTYPES = {
    "For-profit companies (industry)": "pri",
    "Trusts, charities, foundations (both public and private)": "pri",
    "Associations and societies (private and public)": "pri",
    "National government": "gov",
    "Universities (academic only)": "gov",
    "International organizations": "pri",
    "Research institutes and centers": "pri",
    "Other non-profit organizations": "pri",
    "Local government": "gov",
    "Libraries and data archiving organizations": "gov",
}


@dataclass
class Repository:
    """A repository."""

    name: str
    endpoint_id: str = None
    pmh_domain: str = None
    url_domain: str = None
    category: str = None
    ror_id: str = None

    def _key(self):
        return self.name, self.endpoint_id, self.pmh_domain, self.url_domain, self.category, self.ror_id

    def __eq__(self, other):
        if isinstance(other, Repository):
            return self._key() == other._key()
        raise NotImplementedError()

    def __hash__(self):
        return hash(self._key())

    @staticmethod
    def from_dict(dict_: Dict):
        name = dict_.get("name")
        endpoint_id = dict_.get("endpoint_id")
        pmh_domain = dict_.get("pmh_domain")
        url_domain = dict_.get("url_domain")
        category = dict_.get("category")
        ror_id = dict_.get("ror_id")

        return Repository(
            name,
            endpoint_id=endpoint_id,
            pmh_domain=pmh_domain,
            url_domain=url_domain,
            category=category,
            ror_id=ror_id,
        )


@dataclass
class Institution:
    """An institution.

    :param id: unique identifier.
    :param name: the institution's name.
    :param grid_id: the institution's GRID id.
    :param ror_id: the institution's ROR id.
    :param country_code: the institution's country code.
    :param country_code_2: the institution's country code.
    :param subregion: the institution's subregion.
    :param papers: the papers published by the institution.
    :param types: the institution type.
    :param country: the institution country name.
    :param coordinates: the institution's coordinates.
    """

    id: int
    name: str = None
    grid_id: str = None
    ror_id: str = None
    country_code: str = None
    country_code_2: str = None
    region: str = None
    subregion: str = None
    papers: List[Paper] = None
    types: str = None
    country: str = None
    coordinates: str = None
    repository: Repository = None


def date_between_dates(start_ts: int, end_ts: int) -> DateTime:
    """Return a datetime between two timestamps.

    :param start_ts: the start timestamp.
    :param end_ts: the end timestamp.
    :return: the DateTime datetime.
    """

    r_ts = random.randint(start_ts, end_ts - 1)
    return pendulum.from_timestamp(r_ts)


@dataclass
class Paper:
    """A paper.

    :param id: unique identifier.
    :param doi: the DOI of the paper.
    :param title: the title of the paper.
    :param published_date: the date the paper was published.
    :param output_type: the output type, see OUTPUT_TYPES.
    :param authors: the authors of the paper.
    :param funders: the funders of the research published in the paper.
    :param journal: the journal this paper is published in.
    :param publisher: the publisher of this paper (the owner of the journal).
    :param events: a list of events related to this paper.
    :param cited_by: a list of papers that this paper is cited by.
    :param fields_of_study: a list of the fields of study of the paper.
    :param license: the papers license at the publisher.
    :param is_free_to_read_at_publisher: whether the paper is free to read at the publisher.
    :param repositories: the list of repositories where the paper can be read.
    """

    id: int
    doi: str = None
    title: str = None
    type: str = None
    published_date: pendulum.Date = None
    output_type: str = None
    authors: List[Author] = None
    funders: List[Funder] = None
    journal: Journal = None
    publisher: Publisher = None
    events: List[Event] = None
    cited_by: List[Paper] = None
    fields_of_study: List[FieldOfStudy] = None
    publisher_license: str = None
    publisher_is_free_to_read: bool = False
    repositories: List[Repository] = None
    in_scihub: bool = False
    in_unpaywall: bool = True

    @property
    def access_type(self) -> AccessType:
        """Return the access type for the paper.

        :return: AccessType.
        """

        gold_doaj = self.in_unpaywall and self.journal.license is not None
        gold = self.in_unpaywall and (
            gold_doaj or (self.publisher_is_free_to_read and self.publisher_license is not None and not gold_doaj)
        )
        hybrid = (
            self.in_unpaywall
            and self.publisher_is_free_to_read
            and self.publisher_license is not None
            and not gold_doaj
        )
        bronze = (
            self.in_unpaywall and self.publisher_is_free_to_read and self.publisher_license is None and not gold_doaj
        )
        green = self.in_unpaywall and len(self.repositories) > 0
        green_only = self.in_unpaywall and green and not gold_doaj and not self.publisher_is_free_to_read
        oa = self.in_unpaywall and (gold or hybrid or bronze or green)
        black = self.in_scihub  # Add LibGen etc here

        return AccessType(
            oa=oa,
            green=green,
            gold=gold,
            gold_doaj=gold_doaj,
            hybrid=hybrid,
            bronze=bronze,
            green_only=green_only,
            black=black,
        )

    @property
    def oa_coki(self) -> COKIOpenAccess:
        """Return the access type for the paper.

        :return: AccessType.
        """

        at = self.access_type
        open = at.oa
        closed = not open
        publisher = at.gold_doaj or at.hybrid or at.bronze
        other_platform = at.green
        publisher_only = publisher and not other_platform
        both = publisher and other_platform
        other_platform_only = at.green_only

        # Publisher categories
        oa_journal = at.gold_doaj
        hybrid = at.hybrid
        no_guarantees = at.bronze
        publisher_categories = PublisherCategories(oa_journal, hybrid, no_guarantees)

        # Other platform categories
        preprint = self.in_unpaywall and any([repo.category == "Preprint" for repo in self.repositories])
        domain = self.in_unpaywall and any([repo.category == "Domain" for repo in self.repositories])
        institution = self.in_unpaywall and any([repo.category == "Institution" for repo in self.repositories])
        public = self.in_unpaywall and any([repo.category == "Public" for repo in self.repositories])
        aggregator = self.in_unpaywall and any([repo.category == "Aggregator" for repo in self.repositories])
        other_internet = self.in_unpaywall and any([repo.category == "Other Internet" for repo in self.repositories])
        unknown = self.in_unpaywall and any([repo.category == "Unknown" for repo in self.repositories])
        other_platform_categories = OtherPlatformCategories(
            preprint, domain, institution, public, aggregator, other_internet, unknown
        )

        return COKIOpenAccess(
            open,
            closed,
            publisher,
            other_platform,
            publisher_only,
            both,
            other_platform_only,
            publisher_categories,
            other_platform_categories,
        )


@dataclass
class AccessType:
    """The access type of a paper.

    :param oa: whether the paper is open access or not.
    :param green: when the paper is available in an institutional repository.
    :param gold: when the paper is an open access journal or (it is not in an open access journal and is free to read
    at the publisher and has an open access license).
    :param gold_doaj: when the paper is an open access journal.
    :param hybrid: where the paper is free to read at the publisher, it has an open access license and the journal is
    not open access.
    :param bronze: when the paper is free to read at the publisher website however there is no license.
    :param green_only: where the paper is not free to read from the publisher, however it is available at an
    :param black: where the paper is available at SciHub.
    institutional repository.
    """

    oa: bool = None
    green: bool = None
    gold: bool = None
    gold_doaj: bool = None
    hybrid: bool = None
    bronze: bool = None
    green_only: bool = None
    black: bool = None


@dataclass
class COKIOpenAccess:
    """The COKI Open Access types.

    :param open: .
    :param closed: .
    :param publisher: .
    :param other_platform: .
    :param publisher_only: .
    :param both: .
    :param other_platform_only: .
    :param publisher_categories: .
    :param other_platform_categories: .
    """

    open: bool = None
    closed: bool = None
    publisher: bool = None
    other_platform: bool = None
    publisher_only: bool = None
    both: bool = None
    other_platform_only: bool = None
    publisher_categories: PublisherCategories = None
    other_platform_categories: OtherPlatformCategories = None


@dataclass
class PublisherCategories:
    """The publisher open subcategories.

    :param oa_journal: .
    :param hybrid: .
    :param no_guarantees: .
    """

    oa_journal: bool = None
    hybrid: bool = None
    no_guarantees: bool = None


@dataclass
class OtherPlatformCategories:
    """The other platform open subcategories

    :param preprint: .
    :param domain: .
    :param institution: .
    :param public: .
    :param aggregator: .
    :param other_internet: .
    :param unknown: .
    """

    preprint: bool = None
    domain: bool = None
    institution: bool = None
    public: bool = None
    aggregator: bool = None
    other_internet: bool = None
    unknown: bool = None


@dataclass
class Author:
    """An author.

    :param id: unique identifier.
    :param name: the name of the author.
    :param institution: the author's institution.
    """

    id: int
    name: str = None
    institution: Institution = None


@dataclass
class Funder:
    """A research funder.

    :param id: unique identifier.
    :param name: the name of the funder.
    :param doi: the DOI of the funder.
    :param country_code: the country code of the funder.
    :param region: the region the funder is located in.
    :param funding_body_type: the funding body type, see FUNDING_BODY_TYPES.
    :param funding_body_subtype: the funding body subtype, see FUNDING_BODY_SUBTYPES.
    """

    id: int
    name: str = None
    doi: str = None
    country_code: str = None
    region: str = None
    funding_body_type: str = None
    funding_body_subtype: str = None


@dataclass
class Publisher:
    """A publisher.

    :param id: unique identifier.
    :param name: the name of the publisher.
    :param doi_prefix: the publisher DOI prefix.
    :param journals: the journals owned by the publisher.
    """

    id: int
    name: str = None
    doi_prefix: int = None
    journals: List[Journal] = None


@dataclass
class FieldOfStudy:
    """A field of study.

    :param id: unique identifier.
    :param name: the field of study name.
    :param level: the field of study level.
    """

    id: int
    name: str = None
    level: int = None


@dataclass
class Journal:
    """A journal

    :param id: unique identifier.
    :param name: the journal name.
    :param name: the license that articles are published under by the journal.
    """

    id: int
    name: str = None
    license: str = None


@dataclass
class Event:
    """An event.

    :param source: the source of the event, see EVENT_TYPES.
    :param event_date: the date of the event.
    """

    source: str = None
    event_date: DateTime = None


InstitutionList = List[Institution]
AuthorList = List[Author]
FunderList = List[Funder]
PublisherList = List[Publisher]
PaperList = List[Paper]
FieldOfStudyList = List[FieldOfStudy]
EventsList = List[Event]
RepositoryList = List[Repository]


@dataclass
class ObservatoryDataset:
    """The generated observatory dataset.

    :param institutions: list of institutions.
    :param authors: list of authors.
    :param funders: list of funders.
    :param publishers: list of publishers.
    :param papers: list of papers.
    :param fields_of_study: list of fields of study.
    :param fields_of_study: list of fields of study.
    """

    institutions: InstitutionList
    authors: AuthorList
    funders: FunderList
    publishers: PublisherList
    papers: PaperList
    fields_of_study: FieldOfStudyList
    repositories: RepositoryList


def make_doi(doi_prefix: int):
    """Makes a randomised DOI given a DOI prefix.

    :param doi_prefix: the DOI prefix.
    :return: the DOI.
    """

    return f"10.{doi_prefix}/{str(uuid.uuid4())}"


def make_observatory_dataset(
    institutions: List[Institution],
    repositories: List[Repository],
    n_funders: int = 5,
    n_publishers: int = 5,
    n_authors: int = 10,
    n_papers: int = 100,
    n_fields_of_study_per_level: int = 5,
) -> ObservatoryDataset:
    """Generate an observatory dataset.

    :param institutions: a list of institutions.
    :param repositories: a list of known repositories.
    :param n_funders: the number of funders to generate.
    :param n_publishers: the number of publishers to generate.
    :param n_authors: the number of authors to generate.
    :param n_papers: the number of papers to generate.
    :param n_fields_of_study_per_level: the number of fields of study to generate per level.
    :return: the observatory dataset.
    """

    faker = Faker()
    funder_doi_prefix = 1000
    funders = make_funders(n_funders=n_funders, doi_prefix=funder_doi_prefix, faker=faker)
    publisher_doi_prefix = funder_doi_prefix + len(funders)
    publishers = make_publishers(n_publishers=n_publishers, doi_prefix=publisher_doi_prefix, faker=faker)
    fields_of_study = make_fields_of_study(n_fields_of_study_per_level=n_fields_of_study_per_level, faker=faker)
    authors = make_authors(n_authors=n_authors, institutions=institutions, faker=faker)
    papers = make_papers(
        n_papers=n_papers,
        authors=authors,
        funders=funders,
        publishers=publishers,
        fields_of_study=fields_of_study,
        repositories=repositories,
        faker=faker,
    )

    return ObservatoryDataset(institutions, authors, funders, publishers, papers, fields_of_study, repositories)


def make_funders(*, n_funders: int, doi_prefix: int, faker: Faker) -> FunderList:
    """Make the funders ground truth dataset.

    :param n_funders: number of funders to generate.
    :param doi_prefix: the DOI prefix for the funders.
    :param faker: the faker instance.
    :return: a list of funders.
    """

    funders = []

    for i, _ in enumerate(range(n_funders)):
        country_code = random.choice(FUNDREF_COUNTRY_CODES)
        funding_body_type = random.choice(FUNDING_BODY_TYPES)
        funders.append(
            Funder(
                i,
                name=faker.company(),
                doi=make_doi(doi_prefix),
                country_code=country_code,
                region=FUNDREF_REGIONS[country_code],
                funding_body_type=funding_body_type,
                funding_body_subtype=FUNDING_BODY_SUBTYPES[funding_body_type],
            )
        )
        doi_prefix += 1

    return funders


def make_publishers(
    *,
    n_publishers: int,
    doi_prefix: int,
    faker: Faker,
    min_journals_per_publisher: int = 1,
    max_journals_per_publisher: int = 3,
) -> PublisherList:
    """Make publishers ground truth dataset.

    :param n_publishers: number of publishers.
    :param doi_prefix: the publisher DOI prefix.
    :param faker: the faker instance.
    :param min_journals_per_publisher: the min number of journals to generate per publisher.
    :param max_journals_per_publisher: the max number of journals to generate per publisher.
    :return:
    """

    publishers = []
    for i, _ in enumerate(range(n_publishers)):
        n_journals_ = random.randint(min_journals_per_publisher, max_journals_per_publisher)
        journals_ = []
        for _ in range(n_journals_):
            journals_.append(Journal(str(uuid.uuid4()), name=faker.company(), license=random.choice(LICENSES)))

        publishers.append(Publisher(i, name=faker.company(), doi_prefix=doi_prefix, journals=journals_))
        doi_prefix += 1

    return publishers


def make_fields_of_study(
    *,
    n_fields_of_study_per_level: int,
    faker: Faker,
    n_levels: int = 6,
    min_title_length: int = 1,
    max_title_length: int = 3,
) -> FieldOfStudyList:
    """Generate the fields of study for the ground truth dataset.

    :param n_fields_of_study_per_level: the number of fields of study per level.
    :param faker: the faker instance.
    :param n_levels: the number of levels.
    :param min_title_length: the minimum field of study title length (words).
    :param max_title_length: the maximum field of study title length (words).
    :return: a list of the fields of study.
    """

    fields_of_study = []
    fos_id_ = 0
    for level in range(n_levels):
        for _ in range(n_fields_of_study_per_level):
            n_words_ = random.randint(min_title_length, max_title_length)
            name_ = faker.sentence(nb_words=n_words_)
            fos_ = FieldOfStudy(fos_id_, name=name_, level=level)
            fields_of_study.append(fos_)
            fos_id_ += 1

    return fields_of_study


def make_authors(*, n_authors: int, institutions: InstitutionList, faker: Faker) -> AuthorList:
    """Generate the authors ground truth dataset.

    :param n_authors: the number of authors to generate.
    :param institutions: the institutions.
    :param faker: the faker instance.
    :return: a list of authors.
    """

    authors = []
    for i, _ in enumerate(range(n_authors)):
        author = Author(i, name=faker.name(), institution=random.choice(institutions))
        authors.append(author)

    return authors


def make_papers(
    *,
    n_papers: int,
    authors: AuthorList,
    funders: FunderList,
    publishers: PublisherList,
    fields_of_study: List,
    repositories: List[Repository],
    faker: Faker,
    min_title_length: int = 2,
    max_title_length: int = 10,
    min_authors: int = 1,
    max_authors: int = 10,
    min_funders: int = 0,
    max_funders: int = 3,
    min_events: int = 0,
    max_events: int = 100,
    min_fields_of_study: int = 1,
    max_fields_of_study: int = 20,
    min_repos: int = 1,
    max_repos: int = 10,
    min_year: int = 2017,
    max_year: int = 2021,
) -> PaperList:
    """Generate the list of ground truth papers.

    :param n_papers: the number of papers to generate.
    :param authors: the authors list.
    :param funders: the funders list.
    :param publishers: the publishers list.
    :param fields_of_study: the fields of study list.
    :param repositories: the repositories.
    :param faker: the faker instance.
    :param min_title_length: the min paper title length.
    :param max_title_length: the max paper title length.
    :param min_authors: the min number of authors for each paper.
    :param max_authors: the max number of authors for each paper.
    :param min_funders: the min number of funders for each paper.
    :param max_funders: the max number of funders for each paper.
    :param min_events: the min number of events per paper.
    :param max_events: the max number of events per paper.
    :param min_fields_of_study: the min fields of study per paper.
    :param max_fields_of_study: the max fields of study per paper.
    :param min_repos: the min repos per paper when green.
    :param max_repos: the max repos per paper when green.
    :param min_year: the min year.
    :param max_year: the max year.
    :return: the list of papers.
    """

    papers = []

    for i, _ in enumerate(range(n_papers)):
        # Random title
        n_words_ = random.randint(min_title_length, max_title_length)
        title_ = faker.sentence(nb_words=n_words_)

        # Random date
        published_date_ = pendulum.from_format(
            str(
                faker.date_between_dates(
                    date_start=pendulum.datetime(min_year, 1, 1), date_end=pendulum.datetime(max_year, 12, 31)
                )
            ),
            "YYYY-MM-DD",
        ).date()
        published_date_ = pendulum.date(year=published_date_.year, month=published_date_.month, day=published_date_.day)

        # Output type
        output_type_ = random.choice(OUTPUT_TYPES)

        # Pick a random list of authors
        n_authors_ = random.randint(min_authors, max_authors)
        authors_ = random.sample(authors, n_authors_)

        # Random funder
        n_funders_ = random.randint(min_funders, max_funders)
        if n_funders_ > 0:
            funders_ = random.sample(funders, n_funders_)
        else:
            funders_ = []

        # Random publisher
        publisher_ = random.choice(publishers)

        # Journal
        journal_ = random.choice(publisher_.journals)

        # Random DOI
        doi_ = make_doi(publisher_.doi_prefix)

        # Random events
        n_events_ = random.randint(min_events, max_events)
        events_ = []
        today = datetime.now()
        today_ts = int(today.timestamp())
        start_date = datetime(today.year - 2, today.month, today.day)
        start_ts = int(start_date.timestamp())

        for _ in range(n_events_):
            event_date_ = date_between_dates(start_ts=start_ts, end_ts=today_ts)
            events_.append(Event(source=random.choice(EVENT_TYPES), event_date=event_date_))

        # Fields of study
        n_fos_ = random.randint(min_fields_of_study, max_fields_of_study)
        level_0_index = 199
        fields_of_study_ = [random.choice(fields_of_study[:level_0_index])]
        fields_of_study_.extend(random.sample(fields_of_study, n_fos_))

        # Open access status
        publisher_is_free_to_read_ = True
        if journal_.license is not None:
            # Gold
            license_ = journal_.license
        else:
            license_ = random.choice(LICENSES)
            if license_ is None:
                # Bronze: free to read on publisher website but no license
                publisher_is_free_to_read_ = bool(random.getrandbits(1))
        # Hybrid: license=True

        # Green: in a 'repository'
        paper_repos = []
        if bool(random.getrandbits(1)):
            # There can be multiple authors from the same institution so the repos have to be sampled from a set
            n_repos_ = random.randint(min_repos, max_repos)
            repos = set()
            for repo in [author.institution.repository for author in authors_] + repositories:
                repos.add(repo)
            paper_repos += random.sample(repos, n_repos_)

        # Make paper
        paper = Paper(
            i,
            type="journal-article",
            doi=doi_,
            title=title_,
            published_date=published_date_,
            output_type=output_type_,
            authors=authors_,
            funders=funders_,
            journal=journal_,
            publisher=publisher_,
            events=events_,
            fields_of_study=fields_of_study_,
            publisher_license=license_,
            publisher_is_free_to_read=publisher_is_free_to_read_,
            repositories=paper_repos,
            in_scihub=bool(random.getrandbits(1)),
            in_unpaywall=True,
        )
        papers.append(paper)

    # Make a subset of papers not in Unpaywall
    not_in_unpaywall = random.sample([paper for paper in papers], 3)
    for paper in not_in_unpaywall:
        paper.in_unpaywall = False

    # Create paper citations
    # Sort from oldest to newest
    papers.sort(key=lambda p: p.published_date)

    for i, paper in enumerate(papers):
        # Create cited_by
        n_papers_forwards = len(papers) - i
        n_cited_by = random.randint(0, int(n_papers_forwards / 2))
        paper.cited_by = random.sample(papers[i + 1 :], n_cited_by)

    return papers


def make_open_citations(dataset: ObservatoryDataset) -> List[Dict]:
    """Generate an Open Citations table from an ObservatoryDataset instance.

    :param dataset: the Observatory Dataset.
    :return: table rows.
    """

    records = []

    def make_oc_timespan(cited_date: pendulum.Date, citing_date: pendulum.Date):
        ts = "P"
        delta = citing_date - cited_date
        years = delta.in_years()
        months = delta.in_months() - years * 12

        if years > 0:
            ts += f"{years}Y"

        if months > 0 or years == 0:
            ts += f"{months}M"

        return ts

    def is_author_sc(cited_: Paper, citing_: Paper):
        for cited_author in cited_.authors:
            for citing_author in citing_.authors:
                if cited_author.name == citing_author.name:
                    return True
        return False

    def is_journal_sc(cited_: Paper, citing_: Paper):
        return cited_.journal.name == citing_.journal.name

    for cited in dataset.papers:
        for citing in cited.cited_by:
            records.append(
                {
                    "oci": "",
                    "citing": citing.doi,
                    "cited": cited.doi,
                    "creation": citing.published_date.strftime("%Y-%m"),
                    "timespan": make_oc_timespan(cited.published_date, citing.published_date),
                    "journal_sc": is_author_sc(cited, citing),
                    "author_sc": is_journal_sc(cited, citing),
                }
            )

    return records


def make_crossref_events(dataset: ObservatoryDataset) -> List[Dict]:
    """Generate the Crossref Events table from an ObservatoryDataset instance.

    :param dataset: the Observatory Dataset.
    :return: table rows.
    """

    events = []

    for paper in dataset.papers:
        for event in paper.events:
            obj_id = f"https://doi.org/{paper.doi}"
            occurred_at = f"{event.event_date.to_datetime_string()} UTC"
            source_id = event.source
            events.append(
                {
                    "obj_id": obj_id,
                    "timestamp": occurred_at,
                    "occurred_at": occurred_at,
                    "source_id": source_id,
                    "id": str(uuid.uuid4()),
                }
            )

    return events


def make_scihub(dataset: ObservatoryDataset) -> List[Dict]:
    """Generate the SciHub table from an ObservatoryDataset instance.

    :param dataset: the Observatory Dataset.
    :return: table rows.
    """

    data = []
    for paper in dataset.papers:
        if paper.access_type.black:
            data.append({"doi": paper.doi})
    return data


def make_unpaywall(dataset: ObservatoryDataset) -> List[Dict]:
    """Generate the Unpaywall table from an ObservatoryDataset instance.

    :param dataset: the Observatory Dataset.
    :return: table rows.
    """

    records = []
    genre_lookup = {
        "journal_articles": ["journal-article"],
        "book_sections": ["book-section", "book-part", "book-chapter"],
        "authored_books": ["book", "monograph"],
        "edited_volumes": ["edited-book"],
        "reports": ["report"],
        "datasets": ["dataset"],
        "proceedings_article": ["proceedings-article"],
        "other_outputs": ["other-outputs"],
    }

    for paper in dataset.papers:
        # In our simulated model, a small number of papers can be closed and not in Unpaywall
        if paper.in_unpaywall:
            # Make OA status
            journal_is_in_doaj = paper.journal.license is not None

            # Add publisher oa locations
            oa_locations = []
            if paper.publisher_is_free_to_read:
                oa_location = {"host_type": "publisher", "license": paper.publisher_license, "url": ""}
                oa_locations.append(oa_location)

            # Add repository oa locations
            for repo in paper.repositories:
                pmh_id = None
                if repo.pmh_domain is not None:
                    pmh_id = f"oai:{repo.pmh_domain}:{str(uuid.uuid4())}"
                oa_location = {
                    "host_type": "repository",
                    "endpoint_id": repo.endpoint_id,
                    "url": f"https://{repo.url_domain}/{urllib.parse.quote(paper.title)}.pdf",
                    "pmh_id": pmh_id,
                    "repository_institution": repo.name,
                }
                oa_locations.append(oa_location)

            is_oa = len(oa_locations) > 0
            if is_oa:
                best_oa_location = oa_locations[0]
            else:
                best_oa_location = None

            # Create record
            records.append(
                {
                    "doi": paper.doi,
                    "year": paper.published_date.year,
                    "genre": random.choice(genre_lookup[paper.output_type]),
                    "publisher": paper.publisher.name,
                    "journal_name": paper.journal.name,
                    "journal_issn_l": paper.journal.id,
                    "is_oa": is_oa,
                    "journal_is_in_doaj": journal_is_in_doaj,
                    "best_oa_location": best_oa_location,
                    "oa_locations": oa_locations,
                }
            )

    return records


def make_openalex_dataset(dataset: ObservatoryDataset) -> List[dict]:
    """Generate the OpenAlex table data from an ObservatoryDataset instance.

    :param dataset: the Observatory Dataset.
    :return: OpenAlex table data.
    """

    result = []
    for paper in dataset.papers:
        entry = {
            "id": str(paper.id),
            "doi": f"https://doi.org/{paper.doi}",
            "cited_by_count": len(paper.cited_by),
            "concepts": [
                {"id": str(fos.id), "display_name": fos.name, "level": fos.level} for fos in paper.fields_of_study
            ],
            "authorships": [
                {
                    "author": {
                        "id": str(author.id),
                        "display_name": author.name,
                    },
                    "institutions": [
                        {
                            "id": str(author.institution.id),
                            "ror": author.institution.ror_id,
                            "display_name": author.institution.name,
                            "country_code": author.institution.country_code,
                            "type": author.institution.types,
                        }
                    ],
                }
                for author in paper.authors
            ],
        }
        result.append(entry)

    return result


def make_orcid(dataset: ObservatoryDataset) -> List[Dict]:
    # A single fake record so that the get_snapshot_date function works (can't have an empty table)
    return [
        {
            "orcid_identifier": {
                "host": "orcid.org",
                "path": "0000-0000-0000-0000",
                "uri": "http://orcid.org/0000-0000-0000-0000",
            },
            "person": {
                "last_modified_date": None,
                "name": {
                    "given_names": "Joe",
                    "family_name": "Blogs",
                },
            },
            "activities_summary": {
                "works": {
                    "last_modified_date": "2022-01-01 00:00:00.000000 UTC",
                    "group": [
                        {
                            "last_modified_date": "2022-01-01 00:00:00.000000 UTC",
                            "work_summary": [
                                {
                                    "created_date": "2022-01-01 00:00:00.000000 UTC",
                                    "last_modified_date": "2022-01-01 00:00:00.000000 UTC",
                                    "source": {
                                        "source_orcid": None,
                                        "source_client_id": {
                                            "host": "orcid.org",
                                            "path": "0000-0000-0000-0000",
                                            "uri": "http://orcid.org/client/0000-0000-0000-0000",
                                        },
                                        "source_name": "Crossref Metadata Search",
                                        "assertion_origin_orcid": None,
                                        "assertion_origin_client_id": None,
                                        "assertion_origin_name": None,
                                    },
                                    "put_code": "00000000",
                                    "visibility": "public",
                                    "display_index": "0",
                                    "path": "/0000-0000-0000-0000/work/00000000",
                                    "title": {"title": "A Paper", "subtitle": None, "translated_title": None},
                                    "external_ids": {
                                        "external_id": [
                                            {
                                                "created_date": None,
                                                "last_modified_date": None,
                                                "source": None,
                                                "put_code": None,
                                                "visibility": None,
                                                "display_index": None,
                                                "path": None,
                                                "external_id_type": "doi",
                                                "external_id_value": "10.0000/s00000-000-00000-0",
                                                "external_id_normalized": None,
                                                "external_id_normalized_error": None,
                                                "external_id_url": None,
                                                "external_id_relationship": "self",
                                            },
                                            {
                                                "created_date": None,
                                                "last_modified_date": None,
                                                "source": None,
                                                "put_code": None,
                                                "visibility": None,
                                                "display_index": None,
                                                "path": None,
                                                "external_id_type": "issn",
                                                "external_id_value": "0000-0000",
                                                "external_id_normalized": None,
                                                "external_id_normalized_error": None,
                                                "external_id_url": None,
                                                "external_id_relationship": "part-of",
                                            },
                                        ]
                                    },
                                    "url": None,
                                    "type": "journal-article",
                                    "publication_date": {"year": "2020", "month": "9", "day": None},
                                    "journal_title": None,
                                }
                            ],
                        }
                    ],
                    "path": "/0000-0000-0000-0000/works",
                },
                "path": "/0000-0000-0000-0000/activities",
            },
        }
    ]


def make_pubmed(dataset: ObservatoryDataset) -> List[Dict]:
    """Generate the Pubmed table from an ObservatoryDataset instance.

    :param dataset: the Observatory Dataset.
    :return: table rows.
    """

    records = []
    for paper in dataset.papers:
        # Create record
        records.append(
            {
                "MedlineCitation": {
                    "PMID": {"value": paper.id, "Version": "1"},
                    "Article": {
                        "ArticleTitle": paper.title,
                        "ArticleDate": {
                            "Day": paper.published_date.day,
                            "Month": paper.published_date.month,
                            "Year": paper.published_date.year,
                        },
                    },
                    "DateCompleted": {
                        "Day": paper.published_date.day,
                        "Month": paper.published_date.month,
                        "Year": paper.published_date.year,
                    },
                },
                "PubmedData": {"ArticleIdList": [{"Idtype": "doi", "value": paper.doi}]},
            }
        )

    # Append one last record with the same PMID but Version + 1 to check intermediate table works properly.
    # There should now be 101 records for the table, but the intermediate table should only have 100 as it only selects the
    # records with the highest Version value.
    records.append(
        {
            "MedlineCitation": {
                "PMID": {"value": paper.id, "Version": "2"},
                "Article": {
                    "ArticleTitle": paper.title,
                    "ArticleDate": {
                        "Day": paper.published_date.day,
                        "Month": paper.published_date.month,
                        "Year": paper.published_date.year,
                    },
                },
                "DateCompleted": {
                    "Day": paper.published_date.day,
                    "Month": paper.published_date.month,
                    "Year": paper.published_date.year,
                },
            },
            "PubmedData": {"ArticleIdList": [{"Idtype": "doi", "value": paper.doi}]},
        }
    )

    return records


def make_crossref_fundref(dataset: ObservatoryDataset) -> List[Dict]:
    """Generate the Crossref Fundref table from an ObservatoryDataset instance.

    :param dataset: the Observatory Dataset.
    :return: table rows.
    """

    records = []

    for funder in dataset.funders:
        records.append(
            {
                "pre_label": funder.name,
                "funder": f"http://dx.doi.org/{funder.doi}",
                "country_code": funder.country_code,
                "region": funder.region,
                "funding_body_type": funder.funding_body_type,
                "funding_body_sub_type": funder.funding_body_subtype,
            }
        )

    return records


def make_crossref_metadata(dataset: ObservatoryDataset) -> List[Dict]:
    """Generate the Crossref Metadata table from an ObservatoryDataset instance.

    :param dataset: the Observatory Dataset.
    :return: table rows.
    """

    records = []

    for paper in dataset.papers:
        # Create funders
        funders = []
        for funder in paper.funders:
            funders.append({"name": funder.name, "DOI": funder.doi, "award": None, "doi_asserted_by": None})

        # Add Crossref record
        records.append(
            {
                "type": paper.type,
                "title": [paper.title],
                "DOI": paper.doi,
                "is_referenced_by_count": len(paper.cited_by),
                "issued": {
                    "date_parts": [paper.published_date.year, paper.published_date.month, paper.published_date.day]
                },
                "funder": funders,
                "publisher": paper.publisher.name,
            }
        )

    return records


def bq_load_observatory_dataset(
    observatory_dataset: ObservatoryDataset,
    repository: List[Dict],
    bucket_name: str,
    dataset_id_all: str,
    dataset_id_settings: str,
    snapshot_date: DateTime,
    project_id: str,
):
    """Load the fake Observatory Dataset in BigQuery.

    :param observatory_dataset: the Observatory Dataset.
    :param repository: the repository table data.
    :param bucket_name: the Google Cloud Storage bucket name.
    :param dataset_id_all: the dataset id for all data tables.
    :param dataset_id_settings: the dataset id for settings tables.
    :param snapshot_date: the release date for the observatory dataset.
    :param project_id: api project id.
    :return: None.
    """

    # Generate source datasets
    open_citations = make_open_citations(observatory_dataset)
    crossref_events = make_crossref_events(observatory_dataset)
    openalex: List[dict] = make_openalex_dataset(observatory_dataset)
    crossref_fundref = make_crossref_fundref(observatory_dataset)
    unpaywall = make_unpaywall(observatory_dataset)
    crossref_metadata = make_crossref_metadata(observatory_dataset)
    scihub = make_scihub(observatory_dataset)
    pubmed: List[dict] = make_pubmed(observatory_dataset)
    orcid: List[dict] = make_orcid(observatory_dataset)

    # Load fake ROR and settings datasets
    doi_fixtures_path = project_path("doi_workflow", "tests", "fixtures")
    ror = load_jsonl(os.path.join(doi_fixtures_path, "ror.jsonl"))
    country = load_jsonl(os.path.join(doi_fixtures_path, "country.jsonl"))
    groupings = load_jsonl(os.path.join(doi_fixtures_path, "groupings.jsonl"))

    # schema_path = schema_folder()
    with CliRunner().isolated_filesystem() as t:
        tables = [
            Table(
                "repository",
                False,
                dataset_id_settings,
                repository,
                bq_find_schema(path=project_path("doi_workflow", "schema"), table_name="repository"),
            ),
            Table(
                "crossref_events",
                False,
                dataset_id_all,
                crossref_events,
                bq_find_schema(
                    path=project_path("crossref_events_telescope", "schema"),
                    table_name="crossref_events",
                ),
            ),
            Table(
                "crossref_metadata",
                True,
                dataset_id_all,
                crossref_metadata,
                bq_find_schema(
                    path=project_path("crossref_metadata_telescope", "schema"),
                    table_name="crossref_metadata",
                    release_date=snapshot_date,
                ),
            ),
            Table(
                "crossref_fundref",
                True,
                dataset_id_all,
                crossref_fundref,
                bq_find_schema(
                    path=project_path("crossref_fundref_telescope", "schema"),
                    table_name="crossref_fundref",
                    release_date=snapshot_date,
                ),
            ),
            Table(
                "open_citations",
                True,
                dataset_id_all,
                open_citations,
                bq_find_schema(
                    path=project_path("open_citations_telescope", "schema"),
                    table_name="open_citations",
                    release_date=snapshot_date,
                ),
            ),
            Table(
                "scihub",
                True,
                dataset_id_all,
                scihub,
                bq_find_schema(
                    path=project_path("scihub_telescope", "schema"), release_date=snapshot_date, table_name="scihub"
                ),
            ),
            Table(
                "unpaywall",
                False,
                dataset_id_all,
                unpaywall,
                bq_find_schema(path=project_path("unpaywall_telescope", "schema"), table_name="unpaywall"),
            ),
            Table(
                "ror",
                True,
                dataset_id_all,
                ror,
                bq_find_schema(
                    path=project_path("ror_telescope", "schema"), table_name="ror", release_date=snapshot_date
                ),
            ),
            Table(
                "country",
                False,
                dataset_id_settings,
                country,
                bq_find_schema(path=project_path("doi_workflow", "schema"), table_name="country"),
            ),
            Table(
                "groupings",
                False,
                dataset_id_settings,
                groupings,
                bq_find_schema(path=project_path("doi_workflow", "schema"), table_name="groupings"),
            ),
            Table(
                "orcid",
                False,
                dataset_id_all,
                orcid,
                bq_find_schema(path=project_path("orcid_telescope", "schema"), table_name="orcid"),
            ),
            Table(
                "works",
                False,
                dataset_id_all,
                openalex,
                bq_find_schema(path=project_path("openalex_telescope", "schema"), table_name="works"),
            ),
            Table(
                "pubmed",
                False,
                dataset_id_all,
                pubmed,
                bq_find_schema(path=project_path("pubmed_telescope", "schema"), table_name="pubmed"),
            ),
        ]

        bq_load_tables(
            project_id=project_id,
            tables=tables,
            bucket_name=bucket_name,
            snapshot_date=snapshot_date,
        )


def aggregate_events(events: List[Event]) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    """Aggregate events by source into total events for all time, monthly and yearly counts.

    :param events: list of events.
    :return: list of events for each source aggregated by all time, months and years.
    """

    lookup_totals = dict()
    lookup_months = dict()
    lookup_years = dict()
    for event in events:
        # Total events
        if event.source in lookup_totals:
            lookup_totals[event.source] += 1
        else:
            lookup_totals[event.source] = 1

        # Events by month
        month = event.event_date.strftime("%Y-%m")
        month_key = (event.source, month)
        if month_key in lookup_months:
            lookup_months[month_key] += 1
        else:
            lookup_months[month_key] = 1

        # Events by year
        year = event.event_date.year
        year_key = (event.source, year)
        if year_key in lookup_years:
            lookup_years[year_key] += 1
        else:
            lookup_years[year_key] = 1

    total = [{"source": source, "count": count} for source, count in lookup_totals.items()]
    months = [{"source": source, "month": month, "count": count} for (source, month), count in lookup_months.items()]
    years = [{"source": source, "year": year, "count": count} for (source, year), count in lookup_years.items()]

    # Sort
    sort_events(total, months, years)
    return total, months, years


def sort_events(events: List[Dict], months: List[Dict], years: List[Dict]):
    """Sort events in-place.

    :param events: events all time.
    :param months: events by month.
    :param years: events by year.
    :return: None.
    """

    events.sort(key=lambda x: x["source"])
    months.sort(key=lambda x: f"{x['month']}{x['source']}{x['count']}")
    years.sort(key=lambda x: f"{x['year']}{x['source']}{x['count']}")


def make_doi_table(dataset: ObservatoryDataset) -> List[Dict]:
    """Generate the DOI table from an ObservatoryDataset instance.

    :param dataset: the Observatory Dataset.
    :return: table rows.
    """

    records = []
    for paper in dataset.papers:
        # Doi, events and grids
        doi = paper.doi.upper()
        events = make_doi_events(doi, paper.events)

        # Affiliations: institutions, countries, regions, subregion, funders, journals, publishers
        institutions = make_doi_institutions(paper.authors)
        countries = make_doi_countries(paper.authors)
        regions = make_doi_regions(paper.authors)
        subregions = make_doi_subregions(paper.authors)
        funders = make_doi_funders(paper.funders)
        journals = make_doi_journals(paper.in_unpaywall, paper.journal)
        publishers = make_doi_publishers(paper.publisher)

        # Make final record
        records.append(
            {
                "doi": doi,
                "crossref": {
                    "type": paper.type,
                    "title": paper.title,
                    "published_year": paper.published_date.year,
                    "published_month": paper.published_date.month,
                    "published_year_month": f"{paper.published_date.year}-{paper.published_date.month}",
                    "funder": [{"name": funder.name, "DOI": funder.doi.upper()} for funder in paper.funders],
                },
                "unpaywall": {},
                "unpaywall_history": {},
                "open_citations": {},
                "pubmed": {},
                "events": events,
                "affiliations": {
                    "doi": doi,
                    "institutions": institutions,
                    "countries": countries,
                    "subregions": subregions,
                    "regions": regions,
                    "groupings": [],
                    "funders": funders,
                    "authors": [],
                    "journals": journals,
                    "publishers": publishers,
                },
            }
        )

    # Sort to match with sorted results
    records.sort(key=lambda r: r["doi"])

    return records


def make_doi_events(doi: str, event_list: EventsList) -> Dict:
    """Make the events for a DOI table row.

    :param doi: the DOI.
    :param event_list: a list of events for the paper.
    :return: the events for the DOI table.
    """

    events_total, events_months, events_years = aggregate_events(event_list)

    # When no events, events is None
    events = None
    if len(events_total):
        events = {
            "doi": doi,
            "events": events_total,
            "months": events_months,
            "years": events_years,
        }

    return events


def make_doi_funders(funder_list: FunderList) -> List[Dict]:
    """Make a DOI table row funders affiliation list.

    :param funder_list: the funders list.
    :return: the funders affiliation list.
    """

    # Funders
    funders = {}
    for funder in funder_list:
        funders[funder.doi.upper()] = {
            "identifier": funder.name,
            "name": funder.name,
            "doi": funder.doi.upper(),
            "types": ["Funder"],
            "country": None,
            "country_code": funder.country_code,
            "country_code_2": None,
            "region": funder.region,
            "subregion": None,
            "coordinates": None,
            "funding_body_type": funder.funding_body_type,
            "funding_body_subtype": funder.funding_body_subtype,
            "members": [],
        }
    funders = [v for k, v in funders.items()]
    funders.sort(key=lambda x: x["identifier"])

    return funders


def make_doi_journals(in_unpaywall: bool, journal: Journal) -> List[Dict]:
    """Make the journal affiliation list for a DOI table row.

    :param in_unpaywall: whether the work is in Unpaywall or not. At the moment the journal IDs come from Unpaywall,
    and if the work is not in Unpaywall then the journal id and name will be None.
    :param journal: the paper's journal.
    :return: the journal affiliation list.
    """

    identifier = None
    name = None
    if in_unpaywall:
        identifier = journal.id
        name = journal.name

    return [
        {
            "identifier": identifier,
            "types": ["Journal"],
            "name": name,
            "country": None,
            "country_code": None,
            "country_code_2": None,
            "region": None,
            "subregion": None,
            "coordinates": None,
            "members": [],
        }
    ]


def to_affiliations_list(dict_: Dict):
    """Convert affiliation dict into a list.

    :param dict_: affiliation dict.
    :return: affiliation list.
    """

    l_ = []
    for k, v in dict_.items():
        v["members"] = list(v["members"])
        v["members"].sort()
        if "count" in v:
            v["count"] = len(v["rors"])
            v.pop("rors", None)
        l_.append(v)
    l_.sort(key=lambda x: x["identifier"])
    return l_


def make_doi_publishers(publisher: Publisher) -> List[Dict]:
    """Make the publisher affiliations for a DOI table row.

    :param publisher: the paper's publisher.
    :return: the publisher affiliations list.
    """

    return [
        {
            "identifier": publisher.name,
            "types": ["Publisher"],
            "name": publisher.name,
            "country": None,
            "country_code": None,
            "country_code_2": None,
            "region": None,
            "subregion": None,
            "coordinates": None,
            "members": [],
        }
    ]


def make_doi_institutions(author_list: AuthorList) -> List[Dict]:
    """Make the institution affiliations for a DOI table row.

    :param author_list: the paper's author list.
    :return: the institution affiliation list.
    """

    institutions = {}

    for author in author_list:
        # Institution
        inst = author.institution
        if inst.ror_id not in institutions:
            institutions[inst.ror_id] = {
                "identifier": inst.ror_id,
                "types": [inst.types],
                "name": inst.name,
                "country": inst.country,
                "country_code": inst.country_code,
                "country_code_2": inst.country_code_2,
                "region": inst.region,
                "subregion": inst.subregion,
                "coordinates": inst.coordinates,
                "members": [],
            }

    return to_affiliations_list(institutions)


def make_doi_countries(author_list: AuthorList):
    """Make the countries affiliations for a DOI table row.

    :param author_list: the paper's author list.
    :return: the countries affiliation list.
    """

    countries = {}

    for author in author_list:
        inst = author.institution
        if inst.country not in countries:
            countries[inst.country] = {
                "identifier": inst.country_code,
                "name": inst.country,
                "types": ["Country"],
                "country": inst.country,
                "country_code": inst.country_code,
                "country_code_2": inst.country_code_2,
                "region": inst.region,
                "subregion": inst.subregion,
                "coordinates": None,
                "count": 0,
                "members": {inst.ror_id},
                "rors": {inst.ror_id},
            }
        else:
            countries[inst.country]["members"].add(inst.ror_id)
            countries[inst.country]["rors"].add(inst.ror_id)

    return to_affiliations_list(countries)


def make_doi_regions(author_list: AuthorList):
    """Make the regions affiliations for a DOI table row.

    :param author_list: the paper's author list.
    :return: the regions affiliation list.
    """

    regions = {}
    for author in author_list:
        inst = author.institution
        if inst.region not in regions:
            regions[inst.region] = {
                "identifier": inst.region,
                "name": inst.region,
                "types": ["Region"],
                "country": None,
                "country_code": None,
                "country_code_2": None,
                "region": inst.region,
                "subregion": None,
                "coordinates": None,
                "count": 0,
                "members": {inst.subregion},
                "rors": {inst.ror_id},
            }
        else:
            regions[inst.region]["members"].add(inst.subregion)
            regions[inst.region]["rors"].add(inst.ror_id)

    return to_affiliations_list(regions)


def make_doi_subregions(author_list: AuthorList):
    """Make the subregions affiliations for a DOI table row.

    :param author_list: the paper's author list.
    :return: the subregions affiliation list.
    """

    subregions = {}
    for author in author_list:
        inst = author.institution

        if inst.subregion not in subregions:
            subregions[inst.subregion] = {
                "identifier": inst.subregion,
                "name": inst.subregion,
                "types": ["Subregion"],
                "country": None,
                "country_code": None,
                "country_code_2": None,
                "region": inst.region,
                "subregion": None,
                "coordinates": None,
                "count": 0,
                "members": {inst.country_code},
                "rors": {inst.ror_id},
            }
        else:
            subregions[inst.subregion]["members"].add(inst.country_code)
            subregions[inst.subregion]["rors"].add(inst.ror_id)

    return to_affiliations_list(subregions)


def calc_percent(value: float, total: float) -> float:
    """Calculate a percentage and round to 2dp.

    :param value: the value.
    :param total: the total.
    :return: the percentage.
    """

    if math.isnan(value) or math.isnan(total) or total == 0:
        return None

    return round(value / total * 100, 2)


def make_aggregate_table(agg: str, dataset: ObservatoryDataset) -> List[Dict]:
    """Generate an aggregate table from an ObservatoryDataset instance.

    :param agg: the aggregation type, e.g. country, institution.
    :param dataset: the Observatory Dataset.
    :return: table rows.
    """

    data = []
    repos = []
    for paper in dataset.papers:
        for author in paper.authors:
            inst = author.institution
            at = paper.access_type
            oa_coki = paper.oa_coki

            # Choose id and name
            if agg == "country":
                id = inst.country_code
                name = inst.country
            elif agg == "institution":
                id = inst.ror_id
                name = inst.name
            else:
                raise ValueError(f"make_aggregate_table: agg type unknown: {agg}")

            # Add repository info
            for repo in paper.repositories:
                if paper.in_unpaywall:
                    repos.append(
                        {
                            "paper_id": paper.id,
                            "agg_id": id,
                            "time_period": paper.published_date.year,
                            "name": repo.name,
                            "name_lower": repo.name.lower(),
                            "endpoint_id": repo.endpoint_id,
                            "pmh_domain": repo.pmh_domain,
                            "url_domain": repo.url_domain,
                            "category": repo.category,
                            "ror_id": repo.ror_id,
                            "total_outputs": 1,
                        }
                    )

            data.append(
                {
                    "doi": paper.doi,
                    "id": id,
                    "time_period": paper.published_date.year,
                    "name": name,
                    "country": inst.country,
                    "country_code": inst.country_code,
                    "country_code_2": inst.country_code_2,
                    "region": inst.region,
                    "subregion": inst.subregion,
                    "coordinates": None,
                    "total_outputs": 1,
                    # Access Types
                    "oa": at.oa,
                    "green": at.green,
                    "gold": at.gold,
                    "gold_doaj": at.gold_doaj,
                    "hybrid": at.hybrid,
                    "bronze": at.bronze,
                    "green_only": at.green_only,
                    "black": at.black,
                    # COKI Open Access Types
                    "open": oa_coki.open,
                    "closed": oa_coki.closed,
                    "publisher": oa_coki.publisher,
                    "other_platform": oa_coki.other_platform,
                    "publisher_only": oa_coki.publisher_only,
                    "both": oa_coki.both,
                    "other_platform_only": oa_coki.other_platform_only,
                    # Publisher Open categories
                    "publisher_categories_oa_journal": oa_coki.publisher_categories.oa_journal,
                    "publisher_categories_hybrid": oa_coki.publisher_categories.hybrid,
                    "publisher_categories_no_guarantees": oa_coki.publisher_categories.no_guarantees,
                    # Other Platform categories
                    "publisher_categories_preprint": oa_coki.other_platform_categories.preprint,
                    "publisher_categories_domain": oa_coki.other_platform_categories.domain,
                    "publisher_categories_institution": oa_coki.other_platform_categories.institution,
                    "publisher_categories_public": oa_coki.other_platform_categories.public,
                    "publisher_categories_aggregator": oa_coki.other_platform_categories.aggregator,
                    "publisher_categories_other_internet": oa_coki.other_platform_categories.other_internet,
                    "publisher_categories_unknown": oa_coki.other_platform_categories.unknown,
                }
            )

    # Repos
    df_repos = pd.DataFrame(repos)
    df_repos.drop_duplicates(inplace=True)
    agg = {
        "agg_id": "first",
        "time_period": "first",
        "name": "first",
        "name_lower": "first",
        "endpoint_id": "first",
        "pmh_domain": "first",
        "url_domain": "first",
        "category": "first",
        "ror_id": "first",
        "total_outputs": "sum",
    }
    df_repos = df_repos.groupby(["agg_id", "name", "time_period"], as_index=False).agg(agg)
    df_repos.sort_values(
        by=["agg_id", "time_period", "total_outputs", "name_lower"], ascending=[True, False, False, True], inplace=True
    )

    # Aggregate info
    df = pd.DataFrame(data)
    df.drop_duplicates(inplace=True)
    agg = {
        "id": "first",
        "time_period": "first",
        "name": "first",
        "country": "first",
        "country_code": "first",
        "country_code_2": "first",
        "region": "first",
        "subregion": "first",
        "coordinates": "first",
        "total_outputs": "sum",
        # Access types
        "oa": "sum",
        "green": "sum",
        "gold": "sum",
        "gold_doaj": "sum",
        "hybrid": "sum",
        "bronze": "sum",
        "green_only": "sum",
        "black": "sum",
        # COKI OA types
        "open": "sum",
        "closed": "sum",
        "publisher": "sum",
        "other_platform": "sum",
        "publisher_only": "sum",
        "both": "sum",
        "other_platform_only": "sum",
        # Publisher Open categories
        "publisher_categories_oa_journal": "sum",
        "publisher_categories_hybrid": "sum",
        "publisher_categories_no_guarantees": "sum",
        # Other Platform categories
        "publisher_categories_preprint": "sum",
        "publisher_categories_domain": "sum",
        "publisher_categories_institution": "sum",
        "publisher_categories_public": "sum",
        "publisher_categories_aggregator": "sum",
        "publisher_categories_other_internet": "sum",
        "publisher_categories_unknown": "sum",
    }
    df = df.groupby(["id", "time_period"], as_index=False).agg(agg).sort_values(by=["id", "time_period"])

    records = []
    for i, row in df.iterrows():
        total_outputs = row["total_outputs"]

        # Access types
        oa = row["oa"]
        green = row["green"]
        gold = row["gold"]
        gold_doaj = row["gold_doaj"]
        hybrid = row["hybrid"]
        bronze = row["bronze"]
        green_only = row["green_only"]
        black = row["black"]

        # COKI access types
        open = row["open"]
        closed = row["closed"]
        publisher = row["publisher"]
        other_platform = row["other_platform"]
        publisher_only = row["publisher_only"]
        both = row["both"]
        other_platform_only = row["other_platform_only"]

        # Publisher Open
        publisher_categories_oa_journal = row["publisher_categories_oa_journal"]
        publisher_categories_hybrid = row["publisher_categories_hybrid"]
        publisher_categories_no_guarantees = row["publisher_categories_no_guarantees"]

        # Other Platform categories
        publisher_categories_preprint = row["publisher_categories_preprint"]
        publisher_categories_domain = row["publisher_categories_domain"]
        publisher_categories_institution = row["publisher_categories_institution"]
        publisher_categories_public = row["publisher_categories_public"]
        publisher_categories_aggregator = row["publisher_categories_aggregator"]
        publisher_categories_other_internet = row["publisher_categories_other_internet"]
        publisher_categories_unknown = row["publisher_categories_unknown"]

        # Get repositories for year and id
        id = row["id"]
        time_period = row["time_period"]
        df_repos_subset = df_repos[(df_repos["agg_id"] == id) & (df_repos["time_period"] == time_period)]
        repositories = []
        for j, repo_row in df_repos_subset.iterrows():
            ror_id = repo_row["ror_id"]
            home_repo = id == ror_id
            repositories.append(
                {
                    "id": repo_row["name"],
                    "total_outputs": repo_row["total_outputs"],
                    "category": repo_row["category"],
                    "home_repo": home_repo,
                }
            )

        # fmt: off
        records.append(
            {
                "id": id,
                "time_period": row["time_period"],
                "name": row["name"],
                "country": row["country"],
                "country_code": row["country_code"],
                "country_code_2": row["country_code_2"],
                "region": row["region"],
                "subregion": row["subregion"],
                "coordinates": row["coordinates"],
                "total_outputs": total_outputs,
                "coki": {
                    "oa": {
                        "color": {
                            "oa": {"total_outputs": oa, "percent": calc_percent(oa, total_outputs)},
                            "green": {"total_outputs": green, "percent": calc_percent(green, total_outputs)},
                            "gold": {"total_outputs": gold, "percent": calc_percent(gold, total_outputs)},
                            "gold_doaj": {"total_outputs": gold_doaj, "percent": calc_percent(gold_doaj, total_outputs)},
                            "hybrid": {"total_outputs": hybrid, "percent": calc_percent(hybrid, total_outputs)},
                            "bronze": {"total_outputs": bronze, "percent": calc_percent(bronze, total_outputs)},
                            "green_only": {"total_outputs": green_only, "percent": calc_percent(green_only, total_outputs)},
                            "black": {"total_outputs": black, "percent": calc_percent(black, total_outputs)},
                        },
                        "coki": {
                            "open": {"total": open, "percent": calc_percent(open, total_outputs)},
                            "closed": {"total": closed, "percent": calc_percent(closed, total_outputs)},
                            "publisher": {"total": publisher, "percent": calc_percent(publisher, total_outputs)},
                            "other_platform": {"total": other_platform, "percent": calc_percent(other_platform, total_outputs)},
                            "publisher_only": {"total": publisher_only, "percent": calc_percent(publisher_only, total_outputs)},
                            "both": {"total": both, "percent": calc_percent(both, total_outputs)},
                            "other_platform_only": {"total": other_platform_only, "percent": calc_percent(other_platform_only, total_outputs)},
                            "publisher_categories": {
                                "oa_journal": {"total": publisher_categories_oa_journal, "percent": calc_percent(publisher_categories_oa_journal, publisher)},
                                "hybrid": {"total": publisher_categories_hybrid, "percent": calc_percent(publisher_categories_hybrid, publisher)},
                                "no_guarantees": {"total": publisher_categories_no_guarantees, "percent": calc_percent(publisher_categories_no_guarantees, publisher)}
                            },
                            "other_platform_categories": {
                                "preprint": {"total": publisher_categories_preprint, "percent": calc_percent(publisher_categories_preprint, other_platform)},
                                "domain": {"total": publisher_categories_domain, "percent": calc_percent(publisher_categories_domain, other_platform)},
                                "institution": {"total": publisher_categories_institution, "percent": calc_percent(publisher_categories_institution, other_platform)},
                                "public": {"total": publisher_categories_public, "percent": calc_percent(publisher_categories_public, other_platform)},
                                "aggregator": {"total": publisher_categories_aggregator, "percent": calc_percent(publisher_categories_aggregator, other_platform)},
                                "other_internet": {"total": publisher_categories_other_internet, "percent": calc_percent(publisher_categories_other_internet, other_platform)},
                                "unknown": {"total": publisher_categories_unknown, "percent": calc_percent(publisher_categories_unknown, other_platform)},
                            },
                        }
                    },
                    "repositories": repositories
                },
                "citations": {},
                "output_types": [],
                "disciplines": {},
                "funders": [],
                "members": [],
                "publishers": [],
                "journals": [],
                "events": [],
            }
        )
        # fmt: on

    return records
