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

This telescope uses the Unpaywall Data Feed service.  If you wish to ingest Unpaywall using the free non subscription snapshots, use the Unpaywall Snapshot telescope instead.

The free Unpaywall snapshot service is only updated a few times a year.  It is also difficult to find changes
from snapshot to snapshot. The Data Feed service rectifies this by providing daily or weekly changefiles.
To use the Data Feed:
1. Get the current snapshot using the API service which is updated daily, and available with the Data Feed subscription.
2. Get all changefiles starting with latest timestamp just before the snapshot date.
3. Apply the changefiles to the snapshot in date order from oldest to newest.

The Data Feed service requires an API key in order to access the changefiles.  See the [data feed page](https://unpaywall.org/products/data-feed)
for more information on obtaining a key.  

On first run, this telescope tries to pull the snapshot on the telescope's start date.  Note that the snapshot hosted at `https://api.unpaywall.org/feed/snapshot?api_key=YOUR_API_KEY` is currently updated daily by Unpaywall, so make sure you set the telescope's start date to be equal to that snapshot date.

Subsequent scheduled runs will download the daily changefile from __TWO DAYS PRIOR__ to the scheduled execution date to update the dataset.  You should set the scheduled_interval to "@daily" or some other equivalent interval which that results in scheduled runs on a daily basis. The telescope attempts to catch up on missed scheduled runs from the start date to the current execution date in case of interruption.

The reason that the changefile applied is from two days prior to each scheduled executin date is so that we can guarantee data integrity after applying the snapshot.

Unpaywall recommends applying changefiles starting from a timestamp just before the snapshot timestamp. This telescope uses daily updates. This is to minimise the amount of overlapping data downloaded.  To simplify the update process, we opt to apply daily updates to snapshots starting from one day before the snapshot timestamp. For example, if the first execution date was (2021,7,2), the snapshot frm (2021,7,2) is downloaded.  The next execution date on (2021,7,3) downloads the daily changefile from (2021,7,1).  The next execution date downloads the daily changefile from (2021,7,2).  The next one after that downloads the daily changefile from (2021,7,3), and so on.

The telescope maintains a single updated BigQuery table, that's updated to 2 days before the latest scheduled execution date.

## Airflow Connection

The telescope requires an Airflow connection named `unpaywall` with the password set to the API key from Unpaywall for accessing the Data Feed service.  For example, the corresponding observatory `config.yaml` entry could be:

```
unpaywall: http://:API_KEY@localhost
```

The connection must be a valid URI supported by Airflow, but only the password component (the API key) is used by this telescope.

 ```eval_rst
+------------------------------+--------------------------------------+
| Summary                      |                                      |
+==============================+======================================+
| Harvest Type                 | URL                                  |
+------------------------------+--------------------------------------+
| Harvest Frequency            | Daily                                |
+------------------------------+--------------------------------------+
| Runs on remote worker        | False                                |
+------------------------------+--------------------------------------+
| Catchup missed runs          | True                                 |
+------------------------------+--------------------------------------+
| Table Write Disposition      | Append                               |
+------------------------------+--------------------------------------+
| Update Frequency             | Daily                                |
+------------------------------+--------------------------------------+
| Credentials Required         | Yes                                  |
+------------------------------+--------------------------------------+
| Uses Workflow Template       | Stream                               |
+------------------------------+--------------------------------------+
```

## Latest schema
``` eval_rst
.. csv-table::
   :file: ../schemas/unpaywall_latest.csv
   :width: 100%
   :header-rows: 1
```