# COCI, the OpenCitations Index of Crossref open DOI-to-DOI citations

>"OpenCitations is an independent not-for-profit infrastructure organization for open scholarship dedicated to the publication of open bibliographic and citation data by the use of Semantic Web (Linked Data) technologies. It is also engaged in advocacy for open citations, particularly in its role as a key founding member of the Initiative for Open Citations (I4OC). For administrative convenience, OpenCitations is managed by the Research Centre for Open Scholarly Metadata at the University of Bologna."
-- [Open Citations website](https://unpaywall.org/).

>"COCI, the OpenCitations Index of Crossref open DOI-to-DOI citations, is an RDF dataset containing details of all the citations that are specified by the open references to DOI-identified works present in Crossref, as of the latest COCI update. COCI does not index Crossref references that are not open, nor Crossref open references to entities that lack DOIs. The citations available in COCI are treated as first-class data entities, with accompanying properties including the citations timespan, modelled according to the OpenCitations Data Model." -- [COCI website](http://opencitations.net/index/coci).

A detailed description can be found in the paper [Software review: COCI, the OpenCitations Index of Crossref open DOI-to-DOI citations](https://doi.org/10.1007/s11192-019-03217-6).

The dataset is available through querying SPARQL, REST API, and the OpenCitations Indexes Search Interface.  This telescope fetches the dataset as a data dump from Figshare.


 ```eval_rst
+------------------------------+--------------------------------------+
| Summary                      |                                      |
+==============================+======================================+
| Harvest Type                 | URL                                  |
+------------------------------+--------------------------------------+
| Harvest frequency            | Default: @weekly                     |
+------------------------------+--------------------------------------+
| Runs on remote worker        | Default: True                        |
+------------------------------+--------------------------------------+
| Catchup missed runs          | Default: False                       |
+------------------------------+--------------------------------------+
| Table Write Disposition      | Append                               |
+------------------------------+--------------------------------------+
| Dataset Update Frequency     | Roughly 1-3 monthly                  |
+------------------------------+--------------------------------------+
| Credentials Required         | No                                   |
+------------------------------+--------------------------------------+
| Uses Workflow Template       | Snapshot                             |
+------------------------------+--------------------------------------+
| Each shard includes all data | Yes                                  |
+------------------------------+--------------------------------------+
```

## Latest schema
``` eval_rst
.. csv-table::
   :file: ../schemas/open_citations_latest.csv
   :width: 100%
   :header-rows: 1
```