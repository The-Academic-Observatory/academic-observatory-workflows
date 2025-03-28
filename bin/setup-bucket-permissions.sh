#!/usr/bin/env bash

BUCKET_NAME=$1
SERVICE_ACCOUNT_EMAIL=$2

# Check if required positional arguments are provided and if not print usage
if [[ -z "$BUCKET_NAME" || -z "$SERVICE_ACCOUNT_EMAIL" ]]; then
    echo "Usage: $0 <BUCKET_NAME> <SERVICE_ACCOUNT_EMAIL> [optional arguments]"
    exit 1
fi

echo "Configuration:"
echo "  BUCKET_NAME: $BUCKET_NAME"
echo "  SERVICE_ACCOUNT_EMAIL: $SERVICE_ACCOUNT_EMAIL"
echo ""

#################################################
### Check that dependencies are installed
#################################################

DEPENDENCIES=(gcloud)
MISSING=false

for dep in "${DEPENDENCIES[@]}"; do
    if ! command -v "$dep" > /dev/null 2>&1; then
        echo "Missing dependency: $dep"
        MISSING=true
    fi
done

if [ "$MISSING" = true ]; then
    echo "Install the above missing dependencies."
    exit 1
fi

#################################################
### Apply permissions to bucket
#################################################

echo "Give $SERVICE_ACCOUNT_EMAIL permission to access bucket '$BUCKET_NAME'"
gsutil iam ch \
serviceAccount:$SERVICE_ACCOUNT_EMAIL:roles/storage.legacyBucketReader \
serviceAccount:$SERVICE_ACCOUNT_EMAIL:roles/storage.objectCreator \
serviceAccount:$SERVICE_ACCOUNT_EMAIL:roles/storage.objectViewer \
gs://$BUCKET_NAME
