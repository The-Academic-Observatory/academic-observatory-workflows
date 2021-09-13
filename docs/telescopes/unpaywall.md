# Unpaywall

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

 ```eval_rst
+------------------------------+--------------------------------------+
| Summary                      |                                      |
+==============================+======================================+
| Average runtime              |  ?  min                              |
+------------------------------+--------------------------------------+
| Average download size        |  >16 GB                              |
+------------------------------+--------------------------------------+
| Harvest Type                 | URL                                  |
+------------------------------+--------------------------------------+
| Harvest Frequency            | Daily/Weekly/Monthly/Other           |
+------------------------------+--------------------------------------+
| Runs on remote worker        | True/False                           |
+------------------------------+--------------------------------------+
| Catchup missed runs          | True/False                           |
+------------------------------+--------------------------------------+
| Table Write Disposition      | Truncate                             |
+------------------------------+--------------------------------------+
| Update Frequency             | Daily/Weekly/Monthly/Other           |
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