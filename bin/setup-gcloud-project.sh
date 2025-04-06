#!/usr/bin/env bash
#  MIT License
#
#  Copyright (c) 2024 UC Curation Center (California Digital Library)
#  Copyright (c) 2025 Curtin University
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in all
#  copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#  SOFTWARE.

# Positional arguments
PROJECT_ID=$1
GKE_CLUSTER_NAME=$2
GKE_NAMESPACE=$3
DOWNLOAD_BUCKET_NAME=$4
TRANSFORM_BUCKET_NAME=$5

# Optional arguments with default values
BQ_REGION="us"
GCS_REGION="us-central1"
GKE_CLUSTER_REGION="us-central1"
ARTIFACT_REPO_REGION="us"
BQ_PER_USER_PER_DAY=$((10 * 1024 * 1024)) # 10 TiB in MiB
BQ_PER_PROJECT_PER_DAY=$((10 * 1024 * 1024)) # 10 TiB in MiB

# Get the path of the script directory so that we can call other scripts within the same folder
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Parse optional arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --bq-region) BQ_REGION="$2"; shift ;;
        --gcs-region) GCS_REGION="$2"; shift ;;
        --gke-cluster-region) GKE_CLUSTER_REGION="$2"; shift ;;
        --artifact-repo-region) ARTIFACT_REPO_REGION="$2"; shift ;;
        --bq-per-user-per-day) BQ_PER_USER_PER_DAY="$2"; shift ;;
        --bq-per-project-per-day) BQ_PER_PROJECT_PER_DAY="$2"; shift ;;
        *) break ;;
    esac
    shift
done

# Check if required positional arguments are provided and if not print usage
if [[ -z "$PROJECT_ID" || -z "$GKE_CLUSTER_NAME" || -z "$GKE_NAMESPACE" || -z "$DOWNLOAD_BUCKET_NAME" || -z "$TRANSFORM_BUCKET_NAME" ]]; then
    echo "Usage: $0 <PROJECT_ID> <GKE_CLUSTER_NAME> <GKE_NAMESPACE> <DOWNLOAD_BUCKET_NAME> <TRANSFORM_BUCKET_NAME> [optional arguments]"
    echo "Optional arguments:"
    echo "  --bq-region <value>                       Default: $BQ_REGION"
    echo "  --gcs-region <value>                      Default: $GCS_REGION"
    echo "  --gke-cluster-region <value>              Default: $GKE_CLUSTER_REGION"
    echo "  --artifact-repo-region <value>            Default: $ARTIFACT_REPO_REGION"
    echo "  --bq-per-user-per-day <value>             Default: $BQ_PER_USER_PER_DAY (10 TiB in MiB)"
    echo "  --bq-per-project-per-day <value>          Default: $BQ_PER_PROJECT_PER_DAY (10 TiB in MiB)"
    exit 1
fi

echo "Configuration:"
echo "  PROJECT_ID: $PROJECT_ID"
echo "  GKE_CLUSTER_NAME: $GKE_CLUSTER_NAME"
echo "  GKE_NAMESPACE: $GKE_NAMESPACE"
echo "  DOWNLOAD_BUCKET_NAME: $DOWNLOAD_BUCKET_NAME"
echo "  TRANSFORM_BUCKET_NAME: $TRANSFORM_BUCKET_NAME"
echo "  BQ_REGION: $BQ_REGION"
echo "  GKE_CLUSTER_REGION: $GKE_CLUSTER_REGION"
echo "  GCS_REGION: $GCS_REGION"
echo "  ARTIFACT_REPO_REGION: $ARTIFACT_REPO_REGION"
echo "  BQ_PER_USER_PER_DAY: $BQ_PER_USER_PER_DAY"
echo "  BQ_PER_PROJECT_PER_DAY: $BQ_PER_PROJECT_PER_DAY"
echo ""

#################################################
### Check that dependencies are installed
#################################################

DEPENDENCIES=(gcloud gsutil kubectl yq)
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

#################################
### Google Cloud APIs and Quotas
#################################

echo "Enable Google Cloud APIs"
gcloud services enable storage.googleapis.com \
artifactregistry.googleapis.com \
bigquery.googleapis.com --project="$PROJECT_ID"

echo "Set BigQuery Quota"
gcloud alpha services quota update \
  --consumer=projects/"$PROJECT_ID" \
  --service bigquery.googleapis.com \
  --metric bigquery.googleapis.com/quota/query/usage \
  --value "$BQ_PER_USER_PER_DAY" --unit "1/d/{project}/{user}" --force

gcloud alpha services quota update \
  --consumer=projects/"$PROJECT_ID" \
  --service bigquery.googleapis.com \
  --metric bigquery.googleapis.com/quota/query/usage \
  --value "$BQ_PER_PROJECT_PER_DAY" --unit "1/d/{project}" --force

###########################################
### Create Artifact Registry
###########################################

ARTIFACT_REPO_NAME=$PROJECT_ID
gcloud artifacts repositories create "$ARTIFACT_REPO_NAME" \
  --repository-format=docker \
  --location="$ARTIFACT_REPO_REGION" \
  --description="The AO artifact registry where images for GKE are stored" \
  --project="$PROJECT_ID"

gcloud artifacts repositories set-cleanup-policies "$ARTIFACT_REPO_NAME" \
  --location="$ARTIFACT_REPO_REGION" \
  --project="$PROJECT_ID" \
  --policy=policy.json

###########################################
### Fetch Kube Config
###########################################

echo "Authenticate with GKE Cluster: $GKE_CLUSTER_NAME"
KUBE_CONFIG_PATH="kube-config.json"
gcloud container clusters get-credentials "$GKE_CLUSTER_NAME" --project "$PROJECT_ID" --region "$GKE_CLUSTER_REGION"

echo "Created GKE Cluster Namespace: $GKE_NAMESPACE"
kubectl create namespace "$GKE_NAMESPACE"

echo "Set GKE Cluster Context: $GKE_NAMESPACE"
kubectl config set-context --current --namespace="$GKE_NAMESPACE"

echo "Convert Kube Config to JSON: $KUBE_CONFIG_PATH"
cat ~/.kube/config | yq eval -o=json > $KUBE_CONFIG_PATH

###########################################
### Create GCS buckets
###########################################

echo "Create GCS bucket '$DOWNLOAD_BUCKET_NAME' and add lifecycle rules"
gcloud storage buckets create "gs://$DOWNLOAD_BUCKET_NAME" --location="$GCS_REGION" --project="$PROJECT_ID"
gcloud storage buckets update "gs://$DOWNLOAD_BUCKET_NAME" --lifecycle-file=lifecycle.json --project="$PROJECT_ID"

echo "Create GCS bucket '$TRANSFORM_BUCKET_NAME' and add lifecycle rules"
gcloud storage buckets create "gs://$TRANSFORM_BUCKET_NAME" --location="$GCS_REGION" --project="$PROJECT_ID"
gcloud storage buckets update "gs://$TRANSFORM_BUCKET_NAME" --lifecycle-file=lifecycle.json --project="$PROJECT_ID"

######################################
### AO Astro Role and Service Account
######################################

echo "Create AO Astro IAM role"
ASTRO_ROLE_NAME="AOAstroRole"
gcloud iam roles create "$ASTRO_ROLE_NAME" --project="$PROJECT_ID" \
 --title="AO Astro Role" \
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
container.pods.create,\
container.pods.delete,\
container.pods.update,\
container.pods.list,\
container.pods.get,\
container.pods.getLogs,\
container.pods.exec,\
container.events.list,\
container.persistentVolumeClaims.create,\
container.persistentVolumeClaims.delete

echo "Create AO Astro Service Account"
AO_ASTRO_SERVICE_ACCOUNT_NAME="ao-astro"
AO_ASTRO_SERVICE_ACCOUNT="$AO_ASTRO_SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com"
AO_ASTRO_SERVICE_ACCOUNT_DISPLAY_NAME="AO Astro Service Account"
gcloud iam service-accounts create "$AO_ASTRO_SERVICE_ACCOUNT_NAME" \
  --project="$PROJECT_ID" \
  --description="The AO Astro Service Account" \
  --display-name="$AO_ASTRO_SERVICE_ACCOUNT_DISPLAY_NAME"

echo "Add $ASTRO_ROLE_NAME Role to $AO_ASTRO_SERVICE_ACCOUNT_DISPLAY_NAME"
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:$AO_ASTRO_SERVICE_ACCOUNT" \
  --role="projects/$PROJECT_ID/roles/$ASTRO_ROLE_NAME"

"$SCRIPT_DIR"/setup-bucket-permissions.sh "$DOWNLOAD_BUCKET_NAME" "$AO_ASTRO_SERVICE_ACCOUNT"
"$SCRIPT_DIR"/setup-bucket-permissions.sh "$TRANSFORM_BUCKET_NAME" "$AO_ASTRO_SERVICE_ACCOUNT"

###########################################
### GKE Service Account
###########################################

# Enables GKE pods to access storage buckets

echo "Create AO GKE IAM role"
GKE_ROLE_NAME="AOGKERole"
gcloud iam roles create "$GKE_ROLE_NAME" --project="$PROJECT_ID" \
 --title="AO GKE Role" \
 --description="Gives GKE pods permissions to specific Google Cloud resources" \
 --permissions=storage.hmacKeys.create,\
storage.hmacKeys.delete,\
storage.hmacKeys.update,\
bigquery.jobs.create,\
bigquery.tables.get,\
bigquery.tables.getData,\
bigquery.datasets.get

AO_GKE_SERVICE_ACCOUNT_NAME="ao-gke"
AO_GKE_SERVICE_ACCOUNT="$AO_GKE_SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com"
AO_GKE_SERVICE_ACCOUNT_DISPLAY_NAME="AO GKE Service Account"
gcloud iam service-accounts create "$AO_GKE_SERVICE_ACCOUNT_NAME" \
  --project="$PROJECT_ID" \
  --description="The AO GKE Service Account" \
  --display-name="$AO_GKE_SERVICE_ACCOUNT_DISPLAY_NAME"

echo "Add $GKE_ROLE_NAME Role to $AO_GKE_SERVICE_ACCOUNT_DISPLAY_NAME"
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:$AO_GKE_SERVICE_ACCOUNT" \
  --role="projects/$PROJECT_ID/roles/$GKE_ROLE_NAME"

gcloud iam service-accounts add-iam-policy-binding "$AO_GKE_SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com" \
    --role roles/iam.workloadIdentityUser \
    --member "serviceAccount:$PROJECT_ID.svc.id.goog[$GKE_NAMESPACE/default]"

kubectl annotate serviceaccount default \
    --namespace "$GKE_NAMESPACE" \
    iam.gke.io/gcp-service-account="$AO_GKE_SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com"

"$SCRIPT_DIR"/setup-bucket-permissions.sh "$DOWNLOAD_BUCKET_NAME" "$AO_GKE_SERVICE_ACCOUNT"
"$SCRIPT_DIR"/setup-bucket-permissions.sh "$TRANSFORM_BUCKET_NAME" "$AO_GKE_SERVICE_ACCOUNT"

echo ""
echo "$AO_ASTRO_SERVICE_ACCOUNT_DISPLAY_NAME: $AO_ASTRO_SERVICE_ACCOUNT"
echo "$AO_GKE_SERVICE_ACCOUNT_DISPLAY_NAME: $AO_GKE_SERVICE_ACCOUNT"
echo "Kube config path: $KUBE_CONFIG_PATH"