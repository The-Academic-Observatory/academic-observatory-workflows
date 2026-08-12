FROM astrocrpublic.azurecr.io/runtime:3.2-6

# The following describes what various dependencies are used for. Some dependencies are specified in packages.txt
# and some are installed in this Dockerfile if not available in apt by default.
#
# KubernetesPodOperator:
#   * google-cloud-cli and google-cloud-cli-gke-gcloud-auth-plugin.
#   * apt-transport-https, ca-certificates, gnupg, curl, sudo are installed as dependencies the above packages
# OpenAlex: google-cloud-cli
# Crossref Metadata: pigz
# Open Citations: unzip
# ORCID: s5cmd

# Install custom dependencies for DAGs that are not available via apt by default
USER root

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# To connect to GKE with KubernetesPodOperator install google-cloud-cli and google-cloud-cli-gke-gcloud-auth-plugin
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.asc] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
RUN curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | tee /usr/share/keyrings/cloud.google.asc
RUN apt-get update && apt-get install -y google-cloud-cli google-cloud-cli-gke-gcloud-auth-plugin rclone

# ORCID install s5cmd
RUN curl -LO https://github.com/peak/s5cmd/releases/download/v2.1.0/s5cmd_2.1.0_linux_amd64.deb && \
    dpkg -i s5cmd_2.1.0_linux_amd64.deb


# Install Observatory Platform
RUN git clone --depth 1 https://github.com/The-Academic-Observatory/observatory-platform.git && uv pip install --system ./observatory-platform[tests] 

# Set working directory for subsequent commands
WORKDIR /app

# Copy the entire academic-observatory-workflows directory into the /app directory in the container
COPY --chown=astro:astro academic-observatory-workflows ./academic-observatory-workflows

# Explicitly add the directory containing the top-level package to PYTHONPATH
ENV PYTHONPATH="${PYTHONPATH}:/app/academic-observatory-workflows"

# Install Academic Observatory Workflows
RUN uv pip install --system ./academic-observatory-workflows[tests] --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.2.2/constraints-3.13.txt

USER astro
