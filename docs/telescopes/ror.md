# ROR

"ROR (Research Organization Registry) is a community-led registry of open, sustainable, usable, and unique identifiers for every research organization in the world.

ROR includes identifiers and metadata for more than 100,000 organizations. 
ROR identifiers are designed to link research organizations to research outputs in scholarly infrastructure. 
Search the ROR registry at https://ror.org/search."
-- [ROR website](https://ror.readme.io/docs/ror-basics).

This telescope gets the latest ROR data from the available data dumps. 
See their [docs](https://ror.readme.io/docs/data-dump) for more information on the data dump.


 ```eval_rst
+------------------------------+-----------------------------------------+
| Summary                      |                                         |
+==============================+=========================================+
| Average runtime              | <1h                                     |
+------------------------------+-----------------------------------------+
| Average download size        | 100MB-1GB                               |
+------------------------------+-----------------------------------------+
| Harvest Type                 | API                                     |
+------------------------------+-----------------------------------------+
| Workflow Update Frequency    | Monthly                                 |
+------------------------------+-----------------------------------------+
| Runs on remote worker        | False                                   |
+------------------------------+-----------------------------------------+
| Catchup missed runs          | True                                    |
+------------------------------+-----------------------------------------+
| Table Write Disposition      | Truncate                                |
+------------------------------+-----------------------------------------+
| Provider Update Frequency    | Aprrox. Quarterly                       |
+------------------------------+-----------------------------------------+
| Credentials Required         | No                                      |
+------------------------------+-----------------------------------------+
| Uses Workflow Template       | Snapshot                                |
+------------------------------+-----------------------------------------+
| Each shard includes all data | Yes                                     |
+------------------------------+-----------------------------------------+
```

## Latest schema
``` eval_rst
.. csv-table::
   :file: ../schemas/ror_latest.csv
   :width: 100%
   :header-rows: 1
```