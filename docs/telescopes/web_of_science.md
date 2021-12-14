# Web of Science

"The Web of Science is the information and technology provider for the global scientific research community. We provide data, analytics and insights, as well as workflow tools and bespoke professional services to researchers and the entire research community that underpins research – universities and research institutions, national and local governments, private and public research funding organizations, publishers and research-intensive corporations, across the world."
-- [Web of Science website](https://www.clarivate.com/webofsciencegroup>).

Web of science, previously Web of knowledge, provides bibliometric information, including funding acknowledgements,
international publication identifiers, and abstracts. - source: [WOS](https://clarivate.com/webofsciencegroup) and
[data details](https://clarivate.com/webofsciencegroup/solutions/xml-and-apis).

## Observatory Platform API

The telescope relies on the Observatory Platform API in order to create dags.  A DAG will be created in Airflow for every web_of_science telescope returned by the API, i.e., one for each organisation.

The following fields need to be set in the `extra` field of the telescope:
* `airflow_connection` which is a list of Airflow connection ID names containing the login and password for accessing the Web of Science service.
* `institution_ids` which is a list of strings containing the institution IDs to search in Web of Science, for example "Curtin University".
* `earliest_date` which is the earliest `datetime` to query.


## Storage location

The telescope saves the dataset to a Google BigQuery table with the project_id specified in the standard Airflow variable for the project ID, the dataset as `clarivate` (unless overriden), and table id as `web_of_science<date suffix>`.


## Download throttling limits

The telescope downloads results in parallel. Web of Science has imposed [throttling limits](http://help.incites.clarivate.com/wosWebServicesExpanded/bandwidthThrottlingGroup/bandwidthThrottling.html) for API access.  The following limits are observed:
* New session creation: 5 per 5-min period.
* API calls: 2 calls/s
* Returned results: 100 max per call.
* Cited references: 100 max per article.
* Max records retrievable in period: licence dependent. Unclear what Curtin’s limit is if any.

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
   :file: ../schemas/wos_latest.csv
   :width: 100%
   :header-rows: 1
```
## External references
 * [Web of Science API documentation](http://help.incites.clarivate.com/wosWebServicesExpanded/WebServicesExpandedOverviewGroup/Introduction.html)
 * [Field name descriptions](http://help.incites.clarivate.com/wosWebServicesExpanded/appendix1Group/wosfieldNameTable.html)