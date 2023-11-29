#!/usr/bin/env bash

docker build -t academic-observatory .
docker tag academic-observatory us-docker.pkg.dev/$PROJECT_ID/academic-observatory/academic-observatory
docker push us-docker.pkg.dev/$PROJECT_ID/academic-observatory/academic-observatory
astro deploy -i academic-observatory -f
