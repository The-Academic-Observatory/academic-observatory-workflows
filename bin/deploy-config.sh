#!/usr/bin/env bash

if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <DEPLOYMENT_ID> <GCP_PROJECT_ID> <CONFIG_FILE>"
    exit 1
fi

DEPLOYMENT_ID=$1
GCP_PROJECT_ID=$2
CONFIG_FILE=$3

if [ ! -f "$CONFIG_FILE" ]; then
  echo "CONFIG_FILE: ${CONFIG_FILE} not found."
  exit 1
fi

astro deployment variable create GOOGLE_CLOUD_PROJECT=$GCP_PROJECT_ID --deployment-id ${DEPLOYMENT_ID}
echo "Astro Deployment Variable 'GOOGLE_CLOUD_PROJECT' saved to deployment ${DEPLOYMENT_ID}"

astro deployment airflow-variable create --deployment-id ${DEPLOYMENT_ID} --key DATA_PATH  --value /home/astro/data
echo "Airflow Variable 'DATA_PATH' saved to deployment ${DEPLOYMENT_ID}"

astro deployment airflow-variable create --deployment-id ${DEPLOYMENT_ID} --key WORKFLOWS --value "$(yq -o=json '.workflows' $CONFIG_FILE)"
echo "Airflow Variable 'WORKFLOWS' saved to deployment ${DEPLOYMENT_ID}"
