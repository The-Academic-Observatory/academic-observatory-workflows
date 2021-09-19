# Unpaywall Data Feed

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

The free Unpaywall snapshot service is only updated a few times a year.  It is also difficult to find changes
from snapshot to snapshot. The Data Feed service rectifies this by providing daily or weekly changefiles.
To use the Data Feed:
1. Get the current snapshot.
2. Get all changefiles with an update timestamp just before the snapshot date (first changefile) to the current date.
3. Apply the changefiles to the snapshot in date order.

The Data Feed service requires an API key in order to access the changefiles.  See the [data feed page](https://unpaywall.org/products/data-feed)
for more information on obtaining a key.


 ```eval_rst
+------------------------------+--------------------------------------+
| Summary                      |                                      |
+==============================+======================================+
| Average runtime              |  ?  min                              |
+------------------------------+--------------------------------------+
| Average download size        |  ?  MB                               |
+------------------------------+--------------------------------------+
| Harvest Type                 | URL                                  |
+------------------------------+--------------------------------------+
| Harvest Frequency            | Daily/Weekly/Monthly/Other           |
+------------------------------+--------------------------------------+
| Runs on remote worker        | True/False                           |
+------------------------------+--------------------------------------+
| Catchup missed runs          | True/False                           |
+------------------------------+--------------------------------------+
| Table Write Disposition      | Append                               |
+------------------------------+--------------------------------------+
| Update Frequency             | Daily/Weekly/Monthly/Other           |
+------------------------------+--------------------------------------+
| Credentials Required         | Yes                                  |
+------------------------------+--------------------------------------+
| Uses Workflow Template       | Stream                               |
+------------------------------+--------------------------------------+
| Each shard includes all data | No                                   |
+------------------------------+--------------------------------------+
```

## Latest schema
``` eval_rst
.. csv-table::
   :file: ../schemas/unpaywall_data_feed_latest.csv
   :width: 100%
   :header-rows: 1
```