# Pubmed

((( DRAFT, please edit as necessary )))

The Pubmed Medline database is a bibliographioc database of over 29 million medical related citations over the last 30 years.

More information on the database and the fields present in the data can be found here:

https://www.nlm.nih.gov/medline/medline_overview.html

## Telescope workflow

This workflow for Pubmed Medline database downloads the baseline yearly snaphot and applies the addition and deletion updates weekly, storing the raw, transformed and final data in Googles Cloud Storage and Bigquery.

## Download

The Baseline records are release December of each year, the last being released on 2022-12-08. The URL to the FTP server for the 'baseline' files is

https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/

If it is the first run of the workflow, the telescope only processes the baseline portion of the Pubmed Medline database.

Subsequent updatefiles to modify the Pubmed database are released 7-days a week.

https://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/

The telescope runs weekly and finds all updatefiles uploaded onto the server within that time period and apply the changes to main table on Google Biguqery.

All files that are downloaded and are also uploaded to Google Cloud Storage for archival.

## Tranform

All files downloaded from Pubmed have to be transformed from a compressed XML into a strict format such as \*.jsonl.gz to be import into Google Bigquery.

The Biopython package (TODO link to package) is used to read-in, parse and verify the Pubmed XMLs against it's own schema DTD file (TODO link to schema file).
All entities such as Pubmed Articles, Book Articles, Book Documents, Delete Citation and Delete Document are defined in the DTD schema file, however
only Pubmed Articles and Delete Citation fields are present in the baseline and updatefiles.

After cahngefiles are transformed, they are uploaded to Google Cloud Storage for archive and ingested into Biguqery using a glob pattern.

The schema for the Pubmed Article table was derived from the DTD file. It was firstly converted from DTD to XSD using IntelliJ IDEA and was manually gone through
to make sure no fields were missed.

Due to how XMLs can be stored, there can be multiple didferent types of data can present in a field. For example, AbstractText is most commonly a string, however there are times
where maths formula involved which create times where strings and arrays of strings can be mixed together, but Bigquery does not permit this. As a workaround, known text fields
with issues are written to file as a string ONLY, which allows the Pubmed records to be imported into Bigquery.

The Biguqery version of the Pubmed Article schema (including field descriptions) for the 2023 release of Pubmed can be found here:

(link to github repo of schema file)

## Applying changefiles

As mentioned previously, daily updatefiles for Pubmed are collected over a week period at a time and applied to the table all at once.
This is to reduce computation and Bigquery cost, as the main table ends up being 100 Gb, which will cost a lot to run and query the table daily.

Additions and deletions are applied using the PMID and Version values.

If there is an updated record in in the same week period, only the newest record is kept and merged with the main Pubmed Article table on Bigquery.
All Delete Citation records are merged and removed from the main table all at once.

Steps of how Pubmed's changefiles are applied on Biquery:

0. A backup copy of the main table is made before the additions and deletions are applied, just in case there is a problem part way through the update process.
1. The main table is queried to find the PMIDs that are to be upddated. These records are deleted from the table.
2. The list of record to be updated are then appended to the main table.
3. The main table is then queried again to find all records to be deleted, matching on both PMID and Version, and then deleting those records.

```eval_rst
+------------------------------+-----------------------------------------+
| Summary                      |                                         |
+==============================+=========================================+
| Average runtime              | 6 hrs baseline, 5-20 min weekly updates |
+------------------------------+-----------------------------------------+
| Average download size        | 80-100gb baseline, ~500mb weekly        |
+------------------------------+-----------------------------------------+
| Harvest Type                 | FTP transfer                            |
+------------------------------+-----------------------------------------+
| Workflow Update Frequency    | Weekly                                  |
+------------------------------+-----------------------------------------+
| Runs on remote worker        | True                                    |
+------------------------------+-----------------------------------------+
| Catchup missed runs          | False                                   |
+------------------------------+-----------------------------------------+
| Table Write Disposition      | Append                                  |
+------------------------------+-----------------------------------------+
| Provider Update Frequency    | Daily                                   |
+------------------------------+-----------------------------------------+
| Credentials Required         | No                                      |
+------------------------------+-----------------------------------------+
| Each shard includes all data | No                                      |
+------------------------------+-----------------------------------------+
```
