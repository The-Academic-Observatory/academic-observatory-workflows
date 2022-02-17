# OpenAlex

> OpenAlex is a fully open catalog of the global research system. It's named after the ancient Library of Alexandria.  
> 
> The OpenAlex dataset describes scholarly entities and how those entities are connected to each other. 
> There are five types of entities:
> * Works are papers, books, datasets, etc; they cite other works
> * Authors are people who create works
> * Venues are journals and repositories that host works
> * Institutions are universities and other orgs that are affiliated with works (via authors)
> * Concepts tag Works with a topic
>
> Together, these make a huge web (or more technically, heterogeneous directed graph) of hundreds of millions of entities and over a billion connections between them all.

See [https://docs.openalex.org/](https://docs.openalex.org/) for more information.

This telescope transfers OpenAlex data from an AWS S3 bucket and loads it into multiple tables in BigQuery, with one 
table for each entity (Works, Authors, Venues, Institutions, Concepts).  
The first run will process all the files that are available in the S3 bucket. 
A manifest file is used for later runs to keep track of which files have changed since the last run.
Only the files that have changed will then be processed in this telescope.

The data for the Authors and Venues entities do not require any transformations before loading into BigQuery.
This means that the files for these entities are directly transferred to the transform bucket.

The other entities do require some transformation and those files are transferred to the download bucket.
After transforming the data the resulting files are then uploaded to the transform bucket.

The transformation that is required has to do with two fields that have nested fields with dynamic field names.
These make it impossible to create a schema beforehand and upload the data straight into BigQuery. 
The two mentioned fields are 'abstract_inverted_index' (present in Work entity only) and 'international' (present in 
Concept and Institute entities).

As a workaround, these fields are transformed into a RECORD of two arrays of the same length. 
The first array contains all the original field names and the second array the corresponding values.

 ```eval_rst
+------------------------------+-----------------------------------------+
| Summary                      |                                         |
+==============================+=========================================+
| Average runtime              | 12-24h                                  |
+------------------------------+-----------------------------------------+
| Average download size        | >100GB                                  |
+------------------------------+-----------------------------------------+
| Harvest Type                 | AWS transfer                            |
+------------------------------+-----------------------------------------+
| Workflow Update Frequency    | Weekly                                  |
+------------------------------+-----------------------------------------+
| Runs on remote worker        | True                                    |
+------------------------------+-----------------------------------------+
| Catchup missed runs          | False                                   |
+------------------------------+-----------------------------------------+
| Table Write Disposition      | Append                                  |
+------------------------------+-----------------------------------------+
| Provider Update Frequency    | Weekly                                  |
+------------------------------+-----------------------------------------+
| Credentials Required         | No                                      |
+------------------------------+-----------------------------------------+
| Uses Workflow Template       | Stream                                  |
+------------------------------+-----------------------------------------+
| Each shard includes all data | No                                      |
+------------------------------+-----------------------------------------+
```

## Using the transfer service
The files in the AWS bucket are transferred to a separate Google Cloud storage bucket using the storage transfer
 service.
To use the transfer service it is required to enable the Storage Transfer API and to set the correct permissions on
 the Google Cloud Storage bucket as well as the AWS bucket.
 
### Enabling the Storage Transfer API
The API should already be enabled from the Terraform set-up. If this is not the case, see the [google support answer
](https://support.google.com/googleapi/answer/6158841?hl=en) for info on how to enable an API.
Search for the Storage Transfer API and enable this.

### Setting permissions on Google Cloud bucket
The data is transferred to the standard download bucket and the following permissions are required on this Google Cloud 
bucket for the transfer service to work:
* storage.buckets.get
* storage.objects.list
* storage.objects.get
* storage.objects.create

The roles/storage.objectViewer and roles/storage.legacyBucketWriter roles together contain the permissions that are
 always required.
These roles or permissions need to be assigned at the specific bucket to the service account performing the transfer. 

The Storage Transfer Service uses the `project-[$PROJECT_NUMBER]@storage-transfer-service.iam.gserviceaccount.com` service account.

### Setting permissions on AWS bucket
The AWS bucket is managed by OpenAlex, the bucket that is used is `s3://openalex`.
The data in this bucket is publicly available and there aren't any permissions required to download or inspect the 
data using the AWS s3 CLI.

However, the transfer service in GCP does require permissions to transfer the data, so it is required to create a 
user from the AWS console with programmatic access (using a key id and secret key).

The key id and secret access key that are created can then be used for the Airflow connection that is described below.

The required policy that needs to be assigned to this user is:
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket",
                "s3:GetBucketLocation"
            ],
            "Resource": [
                "arn:aws:s3:::openalex"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject"
            ],
            "Resource": [
                "arn:aws:s3:::openalex/*"
            ]
        }
    ]
}
```

## Airflow connections
Note that all values need to be urlencoded.
In the config.yaml file, the following airflow connections are required:  

## openalex
This connection contains the AWS access key id and secret access key that are used to access data in the AWS buckets.
Make sure to URL encode each of the fields 'access_key_id' and 'secret_access_key'.
```yaml
openalex: aws://<access_key_id>:<secret_access_key>@
```

## Latest schema
### Author
``` eval_rst
.. csv-table::
   :file: ../schemas/openalex/Author_latest.csv
   :width: 100%
   :header-rows: 1
```

### Concept
``` eval_rst
.. csv-table::
   :file: ../schemas/openalex/Concept_latest.csv
   :width: 100%
   :header-rows: 1
```

### Institution
``` eval_rst
.. csv-table::
   :file: ../schemas/openalex/Institution_latest.csv
   :width: 100%
   :header-rows: 1
```

### Venue
``` eval_rst
.. csv-table::
   :file: ../schemas/openalex/Venue_latest.csv
   :width: 100%
   :header-rows: 1
```

### Work
``` eval_rst
.. csv-table::
   :file: ../schemas/openalex/Work_latest.csv
   :width: 100%
   :header-rows: 1
```