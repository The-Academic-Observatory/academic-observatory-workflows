#!/usr/bin/env bash

# Check if the project name is passed as an argument
if [ $# -ne 2 ]; then
    echo "Usage: $0 <project-name> <deployment-id>"
    exit 1
fi

# Assign the arguments to variables
PROJECT_NAME="$1"
DEPLOYMENT_ID="$2"

# Build, tag, and push the Docker image with the specified project name
docker build --no-cache -t academic-observatory .
docker tag academic-observatory us-docker.pkg.dev/${PROJECT_NAME}/academic-observatory/academic-observatory
docker push us-docker.pkg.dev/${PROJECT_NAME}/academic-observatory/academic-observatory

# Deploy using Astro
astro deploy -i academic-observatory -f ${DEPLOYMENT_ID}
