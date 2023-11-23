FROM quay.io/astronomer/astro-runtime:8.10.0

# To connect to GKE with KubernetesPodOperator install google-cloud-cli and google-cloud-cli-gke-gcloud-auth-plugin
USER root
RUN apt-get update && apt-get install -y apt-transport-https ca-certificates gnupg curl sudo
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.asc] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
RUN curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | tee /usr/share/keyrings/cloud.google.asc
RUN apt-get update && apt-get install -y google-cloud-cli google-cloud-cli-gke-gcloud-auth-plugin
USER astro

# Install Observatory Platform
RUN git clone --branch feature/astro https://github.com/The-Academic-Observatory/observatory-platform.git
RUN pip install -e ./observatory-platform/observatory-api --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.6.3/constraints-no-providers-3.10.txt
RUN pip install -e ./observatory-platform/observatory-platform --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.6.3/constraints-no-providers-3.10.txt

# Install Academic Observatory Workflows
RUN echo hello
COPY academic-observatory-workflows ./academic-observatory-workflows
RUN pip install -e ./academic-observatory-workflows --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.6.3/constraints-no-providers-3.10.txt