#!/usr/bin/env bash

# Check if the project name is passed as an argument
if [ $# -ne 2 ]; then
    echo "Usage: $0 <gcp-project-id> <deployment-id>"
    exit 1
fi

# Assign the arguments to variables
PROJECT_ID="$1"
DEPLOYMENT_ID="$2"

# Setup auth with Docker so that Docker can download and upload images from the Google Cloud Artifact Registry
gcloud auth configure-docker us-docker.pkg.dev

# Build, tag, and push the Docker image with the specified project name
docker build --no-cache -t academic-observatory:latest .
docker tag academic-observatory us-docker.pkg.dev/${PROJECT_ID}/academic-observatory/academic-observatory
docker push us-docker.pkg.dev/${PROJECT_ID}/academic-observatory/academic-observatory

# Deploy using Astro
astro deploy -i academic-observatory -f ${DEPLOYMENT_ID}
