FROM quay.io/astronomer/astro-runtime:12.11.0

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

# To connect to GKE with KubernetesPodOperator install google-cloud-cli and google-cloud-cli-gke-gcloud-auth-plugin
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.asc] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
RUN curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | tee /usr/share/keyrings/cloud.google.asc
RUN apt-get update && \
    apt-get install -y google-cloud-cli google-cloud-cli-gke-gcloud-auth-plugin

# ORCID install s5cmd
RUN curl -LO https://github.com/peak/s5cmd/releases/download/v2.1.0/s5cmd_2.1.0_linux_amd64.deb && \
    dpkg -i s5cmd_2.1.0_linux_amd64.deb

USER astro

# Install Observatory Platform
RUN git clone https://github.com/The-Academic-Observatory/observatory-platform.git && \
    pip install ./observatory-platform[tests] --constraint  https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-no-providers-3.10.txt

# Set working directory for subsequent commands
WORKDIR /app

# Copy the entire academic-observatory-workflows directory into the /app directory in the container
COPY --chown=astro:astro academic-observatory-workflows ./academic-observatory-workflows

# Explicitly add the directory containing the top-level package to PYTHONPATH
ENV PYTHONPATH="${PYTHONPATH}:/app/academic-observatory-workflows"

# Install Academic Observatory Workflows
# Now that we're in /app, the path to the package is academic-observatory-workflows
RUN pip install ./academic-observatory-workflows[tests] --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-no-providers-3.10.txt
