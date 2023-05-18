# GeoNames

"The GeoNames geographical database covers all countries. It contains over 25 million 
geographical names and consists of over 11 million unique features whereof 4.8 million 
populated places and 13 million alternate names. All features are categorized into one out of 
nine feature classes and further subcategorized into one out of 645 feature codes. 
GeoNames is integrating geographical data such as names of places in various languages, 
elevation, population and others from various sources."
 _- source: [GeoNames](https://www.geonames.org/about.html)_ 
and [data details](https://download.geonames.org/export/dump/readme.txt)


```eval_rst
+------------------------------+---------+
| Summary                      |         |
+==============================+=========+
| Average runtime              | ~20 min |
+------------------------------+---------+
| Average download size        | 350 MB  |
+------------------------------+---------+
| Harvest Type                 | URL     |
+------------------------------+---------+
| Harvest Frequency            | Weekly  |
+------------------------------+---------+
| Runs on remote worker        | False   |
+------------------------------+---------+
| Catchup missed runs          | False   |
+------------------------------+---------+
| Table Write Disposition      | Truncate|
+------------------------------+---------+
| Update Frequency             | ~Monthly|
+------------------------------+---------+
| Credentials Required         | No      |
+------------------------------+---------+
| Uses Telescope Template      | Snapshot|
+------------------------------+---------+
| Each shard includes all data | Yes     |
+------------------------------+---------+
```

## Latest schema
``` eval_rst
.. csv-table::
   :file: ../schemas/geonames/geonames_latest.csv
   :width: 100%
   :header-rows: 1
```
