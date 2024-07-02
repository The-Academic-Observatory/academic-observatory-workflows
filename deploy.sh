#!/usr/bin/env bash

# Check if the project name is passed as an argument
if [ $# -eq 0 ]; then
    echo "Error: No project name provided."
    echo "Usage: $0 <project-name>"
    exit 1
fi

# Assign the first argument to a variable
PROJECT_NAME="$1"

# Build, tag, and push the Docker image with the specified project name
docker build -t academic-observatory .
docker tag academic-observatory us-docker.pkg.dev/${PROJECT_NAME}/academic-observatory/academic-observatory
docker push us-docker.pkg.dev/${PROJECT_NAME}/academic-observatory/academic-observatory

# Deploy using Astro
astro deploy -i academic-observatory -f
