![Academic Observatory Workflows](logo.jpg)

Academic Observatory Workflows provides Apache Airflow workflows for fetching, processing and analysing 
data about academic institutions.

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python Version](https://img.shields.io/badge/python-3.10-blue)](https://img.shields.io/badge/python-3.10-blue)
![Python package](https://github.com/The-Academic-Observatory/academic-observatory-workflows/workflows/Unit%20Tests/badge.svg)
[![Documentation Status](https://readthedocs.org/projects/academic-observatory-workflows/badge/?version=latest)](https://academic-observatory-workflows.readthedocs.io/en/latest/?badge=latest)
[![codecov](https://codecov.io/gh/The-Academic-Observatory/academic-observatory-workflows/branch/develop/graph/badge.svg?token=V4WUZG74ZQ)](https://codecov.io/gh/The-Academic-Observatory/academic-observatory-workflows)
[![DOI](https://zenodo.org/badge/401298815.svg)](https://zenodo.org/badge/latestdoi/401298815)

## Telescope Workflows
A telescope a type of workflow used to ingest data from different data sources, and to run workflows that process and
output data to other places. Workflows are built on top of Apache Airflow's DAGs.

The workflows include: Crossref Events, Crossref Fundref, Crossref Metadata, Geonames, OpenAlex, Open Citations, ORCID, PubMed, ROR, Scopus, Unpaywall and Web of Science.

| Telescope Workflow  | Description |
| ------------- | ------------- |
| <img src="docs/logos/crossref-events.svg" alt="Crossref Events" width="150" /> | Crossref Event Data captures discussion on scholarly content and acts as a hub for the storage and distribution of this data. An event may be a citation in a dataset or patent, a mention in a news article, Wikipedia page or on a blog, or discussion and comment on social media.  |
| <img src="docs/logos/crossref-funder-registry.svg" alt="Crossref Funder Registry" width="150" />  | The Crossref Funder Registry is an open registry of grant-giving organization names and identifiers, which can be used to find funder IDs and include them as part of metadata deposits. It is a freely-downloadable RDF file. It is CC0-licensed and available to integrate with your own systems. Funder names from acknowledgements should be matched with the corresponding unique funder ID from the Funder Registry.  |
| <img src="docs/logos/crossref-metadata.svg" alt="Crossref Metadata" width="150" />  | Crossref is a non-for-profit membership organisation working on making scholarly communications better. It is an official Digital Object Identifier (DOI) Registration Agency of the International DOI Foundation. They provide metadata for every DOI that is registered with Crossref.  |
| <img src="docs/logos/geonames.png" alt="Geonames" width="150" />  | The GeoNames geographical database covers all countries. It contains over 25 million geographical names and consists of over 11 million unique features whereof 4.8 million populated places and 13 million alternate names.  |
| <img src="docs/logos/openalex.svg" alt="OpenAlex" width="150" />  | OpenAlex is a free and open catalog of the global research system. |
| <img src="docs/logos/open-citations.png" alt="Open Citations" width="150" />  | OpenCitations is an independent not-for-profit infrastructure organization for open scholarship dedicated to the publication of open bibliographic and citation data.  |
| <img src="docs/logos/orcid.svg" alt="ORCID" width="150" />  | ORCID is a non-profit organization that provides researchers with a unique digital identifier which eliminates the risk of confusing an identity with another researcher having the same name. ORCID provides a record that supports automatic links among all the researcher's professional activities.  |
| <img src="docs/logos/pubmed.svg" alt="PubMed" width="150" />  | PubMed is a free resource supporting the search and retrieval of biomedical and life sciences literature with the aim of improving health–both globally and personally. |
| <img src="docs/logos/ror.svg" alt="ROR" width="150" />  | ROR is a global, community-led registry of open persistent identifiers for research organizations. |
| <img src="docs/logos/scopus.svg" alt="Scopus" width="150" />  | SCOPUS is an Elsevier bibliometrics database containing abstracts, citations, of journals, books, and conference proceedings.  |
| <img src="docs/logos/unpaywall.png" alt="Unpaywall" width="150" />  | Unpaywall is an open database of free scholarly articles. It includes data from open indexes like Crossref and DOAJ where it exists. Data comes from “monitoring over 50,000 unique online content hosting locations, including Gold OA journals, Hybrid journals, institutional repositories, and disciplinary repositories.  |
| <img src="docs/logos/wos.svg" alt="Web of Science" width="150" />  | Web of science, previously Web of knowledge, provides bibliometric information, including funding acknowledgements, international publication identifiers, and abstracts.  |

## Documentation
For detailed documentation about the Academic Observatory see the Read the Docs website [https://academic-observatory-workflows.readthedocs.io](https://academic-observatory-workflows.readthedocs.io)
