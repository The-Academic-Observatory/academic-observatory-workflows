# Unpaywall Snapshot

"Unpaywall is a project of Our Research, a nonprofit building tools to help make scholarly
research more open, accessible, and reusable." ... they "harvest Open Access content from
over 50,000 publishers and repositories, and make it easy to find, track, and use."
-- [Unpaywall website](https://unpaywall.org/).

Unpaywall is an "open database of free scholarly articles." It includes "data from open indexes like Crossref 
and DOAJ where it exists." Data comes from "monitoring over 50,000 unique online content hosting locations, 
including Gold OA journals, Hybrid journals, institutional repositories, and disciplinary repositories." 
"Unpaywall assigns an OA Status to every article." "There are five possible values: closed, green, gold, 
hybrid, and bronze."
” _- source: [Unpaywall](https://unpaywall.org/)_ 
and [data details](https://unpaywall.org/data-format)

The Unpaywall snapshot dataset information can be found on the [product page](https://unpaywall.org/products/snapshot).  Users are required to fill in a form to get their download link. The link can be found on the bottom of the [product page](https://unpaywall.org/products/snapshot).

To give an estimate of dataset size, the 2021-07-02T151134 snapshot is 26 GiB compressed and 177 GiB uncompressed.

## Airflow Connection

The telescope requires an Airflow connection named Airflow http connection named `unpaywall_snapshot` to be set.  The hostname must be set to the URL given to you from Unpaywall for accessing the API to query for snapshot releases (not to be confused with the snapshot download link from the Data Feed service).

For example, the corresponding observatory `config.yaml` entry could be:

```
unpaywall_snapshot: http://some-valid-unpaywall-snapshot-api.link
```

The connection must be a valid URI supported by Airflow, but only the hostname is used by this telescope.

 ```eval_rst
+------------------------------+--------------------------------------+
| Summary                      |                                      |
+==============================+======================================+
| Harvest Type                 | URL                                  |
+------------------------------+--------------------------------------+
| Harvest frequency            | Default: @monthly                    |
+------------------------------+--------------------------------------+
| Runs on remote worker        | Default: True                        |
+------------------------------+--------------------------------------+
| Catchup missed runs          | Default: True                        |
+------------------------------+--------------------------------------+
| Table Write Disposition      | Truncate                             |
+------------------------------+--------------------------------------+
| Dataset Update Frequency     | Roughly 2-6 monthly                  |
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
   :file: ../schemas/unpaywall_latest.csv
   :width: 100%
   :header-rows: 1
```