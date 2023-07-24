# Pubmed Telescope

The Pubmed Medline database is a bibliographioc database of over 35 million medical related citations over the last 30 years.

More information on the database and the fields present in the data can be found here:

https://www.nlm.nih.gov/medline/medline_overview.html

## Workflow

This workflow processes the Pubmed Medline database by downloading the yearly snaphot and applies the necessary changes in weekly intervals while storing the raw, transformed and final tables in Googles Cloud Storage and Bigquery.

Pubmed's Medline database is split up into two parts; baseline and updatefiles. The 'baseline' portion is the yearly snapshot and the 'updatefiles' are the additions and or edits to the database that are released daily throughout the year. Each year the 'baseline' is re-released with the all of the previous 'updatefiles' and 'baseline' compiled together.

If it is the first run of the year for the workflow, it will download and process the baseline snapshot and any updatefiles that were released from the baseline release date until the execution date of the workflow. Subsequent runs of the workflow are scheduled at weekly intervals and will merge the updatefile modifications and apply the changes to the main table (in Bigquery) all at once. If it is a new yearly run of the workflow, it will download the new baseline release and apply any changes from updatefiles since the baseline release date.

The baseline files from the Pubmed essentially only hold records to upsert to the main table, whereas the updatefiles can hold both upserts and a list of records to delete.

### Download

The Baseline yearly snapshots are released in December of each year. The URL to the FTP server for the 'baseline' files is

https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/

and similarly the updatefiles are released daily

https://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/

Updatefiles can hold both records to upsert and delete. If there are more than 30,000 upserts, there are multiple updatefiles released for that day. All files that are downloaded and are also uploaded to Google Cloud Storage for archival purposes.

### Transform

All files downloaded from Pubmed have to be transformed from a compressed XML into a well defined json format so it can be import into Google Bigquery.

The package "Biopython" is used to parse and verify the Pubmed XMLs against it's own schema DTD file (defined in the header of each XML file). The resulting data dictionary from Biopython's `Entrez.read` holds the XML attributes on it's own data-classes. The attributes such as "CompleteYN" and others are pulled out and added as their own keys to the records.

In this transform step, XML lists are also simplified as the original XML structure can be difficult to query in Bigquery. Multiple unnests and aggregations are needed to pull data from particular fields. To reduce the complexity of the SQL quries for the table, the original XML-like formated data could be in the form of,

```json
{
  "AuthorList": [
    {
      "CompleteYN": "Y",
      "Author": [
        { "First": "Foo", "Last": "Bar" },
        { "First": "James", "Last": "Bond" }
      ]
    }
  ]
}
```

To simplify the above, the "Author" field will be removed and the data from it will be moved up
to the "List" level, along with any data specified under the List field,

```json
{
  "AuthorListCompleteYN": "Y",
  "AuthorList": [
    { "First": "Foo", "Last": "Bar" },
    { "First": "James", "Last": "Bond" }
  ]
}
```

The following list-like fields that have their structure modified in the workflow are:

- AuthorList
- ArticleIdList
- AuthorList
- GrantList
- ChemicalList
- CommentsCorrectionsList
- GeneSymbolList
- MeshHeadingList
- PersonalNameSubjectList
- InvestigatorList
- PublicationTypeList
- ObjectList
- KeywordList
- SupplMeshList
- DataBankList
- AccessionNumberList

Additionally, some fields that are commonly strings such as "AbstractText" could also be a list of values that include formulas or could be split up with fields such as Backgroud, Methods, etc. Since the data has to be well defined for importing into Bigquery, text fields that have this problem are forced to be strings when written to file using a custom encoder. The fields that are forced to be strings are:

- AbstractText
- Affiliation
- ArticleTitle
- b
- BookTitle
- Citation
- CoiStatement
- CollectionTitle
- CollectiveName
- i
- Param
- PublisherName
- SectionTitle
- sub
- Suffix
- sup
- u
- VernacularTitle
- VolumeTitle

All transformed files are uploaded into Google Cloud Storage for ingesting the data into Bigquery.

### Applying upserts and deletes

Merging both upsert and delete records are done to reduce Bigquery cost, as the main table ends up being approximatelly ~100 Gb. Everytime an upsert or delete is applied, the entire table has to be queried for the records to upsert and delete and doing that query for each daily updatefile will add the cost up quickly.

It is important to note that each upsert record contains the entire record again including the newer updated information, not just the new information for that particular record. Thus when merging the upsert records for a release, only the newest available upsert records are kept and applied to the main table. All the upsert records for a release period are merged and ingested into a date sharded table called "upsert". Additionally, delete records can sometimes appear multiple times in multiple different updatefiles. Duplicates are removed and ingested into a date sharded table called "delete". These upsert and delete tables are set to expire in 7 days after being created.

Upserts and delete records are applied by matching on the PMID value and the Version number of a record. A backup is taken of the main table before any of the upserts and deletions are applied. The backup table is set to expire in 31 days after it was created (to reduce table storage costs in Bigquery).

# Workflow Summary

```eval_rst
+------------------------------+-----------------------------------------+
| Summary                      |                                         |
+==============================+=========================================+
| Average runtime              | 6-8 hrs baseline,20 min weekly updates  |
+------------------------------+-----------------------------------------+
| Average download size        | ~100gb baseline, ~500mb weekly          |
+------------------------------+-----------------------------------------+
| Harvest Type                 | FTP transfer                            |
+------------------------------+-----------------------------------------+
| Workflow Update Frequency    | Weekly                                  |
+------------------------------+-----------------------------------------+
| Runs on remote worker        | True                                    |
+------------------------------+-----------------------------------------+
| Catchup missed runs          | False                                   |
+------------------------------+-----------------------------------------+
| Table Write Disposition      | Write Truncate                          |
+------------------------------+-----------------------------------------+
| Provider Update Frequency    | Daily                                   |
+------------------------------+-----------------------------------------+
| Credentials Required         | No                                      |
+------------------------------+-----------------------------------------+
| Each shard includes all data | No                                      |
+------------------------------+-----------------------------------------+
```
