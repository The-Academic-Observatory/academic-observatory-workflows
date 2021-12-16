# Scopus

"Scopus uniquely combines a comprehensive, curated abstract and citation database with enriched data and linked scholarly content.

Quickly find relevant and trusted research, identify experts, and access reliable data, metrics, and analytical tools for confident research strategy decisions – all from one database and one subscription."

"SCOPUS is an Elsevier bibliometrics database containing abstracts, citations, of journals, books, and conference
proceedings." -- [SCOPUS website](https://www.elsevier.com/solutions/scopus).

The telescope will look for connections conforming to the Airflow connection ID naming convention (below) and generate a
subdag to handle the entire ETL pipeline for each institution.

## Observatory Platform API

The telescope relies on the Observatory Platform API in order to create dags. A DAG will be created in Airflow for every scopus telescope returned by the API, i.e., one for each organisation.

The following fields need to be set in the extra field of the telescope:
 * `airflow_connections` which is a list of Airflow connection ID names with API keys set in the password field.
 * `institution_ids` which is a list of strings containing the institution IDs to search in SCOPUS, e.g., ['60031226'] for Curtin University.
 * `earliest_date` which is the earliest `datetime` to query.
 * `view` SCOPUS view, i.e., "STANDARD" or "COMPLETE".

## Storage location

The telescope saves the dataset to a Google BigQuery table with the `project_id` specified in the standard Airflow variable for the project ID. The `dataset_id` defaults to `elsevier`. The `table_id` is set to `scopus<date suffix>`.

## Download throttling limits

The telescope downloads SCOPUS data in parallel sessions up to the number of API keys supplied.  Each session observes the following [throttling limits](https://dev.elsevier.com/api_key_settings.html) imposed by Elsevier:
 * API calls are rate limited to 2 call/s (Elsevier sets 2 call/s as their documented rate).
 * Number of results returned per call is capped at 25 (Elsevier limit).
 * Maximum number of results per query is 5000 (Elsevier limit).

 ```eval_rst
+------------------------------+--------------------------------------+
| Summary                      |                                      |
+==============================+======================================+
| Harvest Type                 | API                                  |
+------------------------------+--------------------------------------+
| Harvest frequency            | Default: @monthly                    |
+------------------------------+--------------------------------------+
| Runs on remote worker        | Default: False                       |
+------------------------------+--------------------------------------+
| Catchup missed runs          | Default: False                       |
+------------------------------+--------------------------------------+
| Table Write Disposition      | Append                               |
+------------------------------+--------------------------------------+
| Dataset Update Frequency     | Daily                                |
+------------------------------+--------------------------------------+
| Credentials Required         | Yes                                  |
+------------------------------+--------------------------------------+
| Uses Workflow Template       | Snapshot                             |
+------------------------------+--------------------------------------+
| Each shard includes all data | Yes                                  |
+------------------------------+--------------------------------------+
```

## Latest schema
``` eval_rst
.. csv-table::
   :file: ../schemas/scopus_latest.csv
   :width: 100%
   :header-rows: 1
```

## External references
* [Developer API portal](https://dev.elsevier.com/scopus.html)
* [SCOPUS API specification](https://dev.elsevier.com/documentation/ScopusSearchAPI.wadl)
* [Search tips](https://dev.elsevier.com/sc_search_tips.html)
* [Search views (response description)](https://dev.elsevier.com/sc_search_views.html)
* [API key settings](https://dev.elsevier.com/api_key_settings.html)
* [SCOPUS](https://www.elsevier.com/en-gb/solutions/scopus)
* [API details](https://dev.elsevier.com/sc_api_spec.html)
