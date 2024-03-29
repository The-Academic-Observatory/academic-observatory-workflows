#!/usr/bin/env bash
# Copyright 2021 Curtin University

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Author: Aniek Roelofs, James Diprose

#####################################
# Install dependencies for workflows
#####################################

# Crossref Metadata (pigz), Open Citations (unzip)
apt-get install pigz unzip -y

# ORCID: install Google Cloud SDK as root user
echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | \
tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | \
apt-key --keyring /usr/share/keyrings/cloud.google.gpg  add - && apt-get update -y && apt-get install google-cloud-sdk -y

# ORCID install s5cmd
curl -LO https://github.com/peak/s5cmd/releases/download/v2.1.0/s5cmd_2.1.0_linux_amd64.deb && \
dpkg -i s5cmd_2.1.0_linux_amd64.deb

