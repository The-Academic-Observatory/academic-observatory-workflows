#!/usr/bin/env bash

# Function to display usage
usage() {
    echo "Usage: $0 PROJECT DOWNLOAD_BUCKET TRANSFORM_BUCKET SERVICE_ACCOUNT"
    exit 1
}

# Check if the number of arguments is exactly 4
if [ "$#" -ne 4 ]; then
    usage
fi

# Assign arguments to variables
project=$1
download_bucket=$2
transform_bucket=$3
service_account=$4
echo "Google Cloud Project: $project"
echo "Google Cloud Download Bucket: $download_bucket"
echo "Google Cloud Transform Bucket: $transform_bucket"
echo "Service Account: $service_account"

# Create the Astro Airflow Role. A custom role is created so that we don't provide more permissions than necessary.

gcloud iam roles create AOAstroAirflowRole --project=$project \
 --title="Academic Observatory Workflows Astro Airflow Role" \
 --description="Gives Astro permissions to specific Google Cloud resources" \
 --permissions=bigquery.datasets.create,\
bigquery.jobs.create,\
bigquery.tables.create,\
bigquery.tables.createSnapshot,\
bigquery.tables.delete,\
bigquery.tables.deleteSnapshot,\
bigquery.tables.get,\
bigquery.tables.getData,\
bigquery.tables.list,\
bigquery.tables.update,\
bigquery.tables.updateData,\
storagetransfer.jobs.create,\
storagetransfer.operations.list,\
storagetransfer.projects.getServiceAccount,\
storage.objects.list,\
storage.hmacKeys.create,\
storage.hmacKeys.delete,\
storage.hmacKeys.update,\
container.pods.create,\
container.pods.delete,\
container.pods.update,\
container.pods.list,\
container.pods.get,\
container.pods.getLogs,\
container.pods.exec,\
container.events.list,\
container.persistentVolumeClaims.create,\
container.persistentVolumeClaims.delete,\
compute.disks.create,\
compute.disks.delete,\
secretmanager.secrets.get,\
secretmanager.versions.access

# Add Astro Airflow Role to our service account
gcloud projects add-iam-policy-binding $project --member=serviceAccount:$service_account --role=projects/$project/roles/AOAstroAirflowRole

# Give Service account access to download bucket
gsutil iam ch \
serviceAccount:$service_account:roles/storage.legacyBucketReader \
serviceAccount:$service_account:roles/storage.objectCreator \
serviceAccount:$service_account:roles/storage.objectViewer \
gs://$download_bucket

# Give Service account access to transform bucket
gsutil iam ch \
serviceAccount:$service_account:roles/storage.legacyBucketReader \
serviceAccount:$service_account:roles/storage.objectCreator \
serviceAccount:$service_account:roles/storage.objectViewer \
gs://$transform_bucket
