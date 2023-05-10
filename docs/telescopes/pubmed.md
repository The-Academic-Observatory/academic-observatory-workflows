# Pubmed

(((BIG DRAFT)))

## Download
### Downloads from FTP server. 

If baseline is required (first release or other), it will combine it with the updatefiles necessary for the snapshot period (data_interval_start - data_interval_end).

Resets the connection every 20-50 files downloaded, otherwise the connection times out or the FTP server rejects the request to get the file. 

## Tranform  
### Processing XML files with Biopython

Reads in the xml.gz files and does a validation step against the *.dtd file mentioned in the XML file itself.

Schema

As of 2023, Pubmed's schema is presented here as a *.dtd file:

(link for the schema file)

Which is an older format meant for importing into other database SQL systems.

For importing the data into bigquery, the schema was transformed into a *.xsd file using InteliJ (other weird program) and included the required math library files for it. 

The XSD schema is more human readable than the orginal, and was used to form the schemas for bigquery.

Pubmed holds 5 main types of data:

Pubmed Articles
Pubmed Book Articles
Book Documents
DeleteCitation 
DeleteDocument

Each of these data types are pulled out of the XML and writting to a .jsonl file for easy importing to bigquery.

### Problematic text fields

The AbstractText field cna hold either the entire abstract for a citattion or a separated version of it, having Background, Method, etc. 
To simplify the schema and to ensure that the data for the field is readin reliably, the 