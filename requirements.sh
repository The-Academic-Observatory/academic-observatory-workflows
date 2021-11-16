#!/usr/bin/env bash
# Copyright 2021 Curtin University
# Copyright 2021 Artificial Dimensions Limited
#
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

# ORCID: install Google Cloud SDK as root user, used in
echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | \
tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | \
apt-key --keyring /usr/share/keyrings/cloud.google.gpg  add - && apt-get update -y && apt-get install google-cloud-sdk -y

###########################################
# Install dependencies for oa-web-workflow
###########################################

# Add yarn sources
curl -sS https://dl.yarnpkg.com/debian/pubkey.gpg | apt-key add -
echo "deb https://dl.yarnpkg.com/debian/ stable main" | tee /etc/apt/sources.list.d/yarn.list

# Add nodejs sources
curl -sL https://deb.nodesource.com/setup_14.x -o nodesource_setup.sh
bash nodesource_setup.sh

# Install system dependencies
apt-get update -yqq && apt-get install git build-essential libxml2-dev libxslt-dev yarn nodejs net-tools -y

# Install wrangler which is required to deploy the website (installing with npm gives permission errors)
yarn config set prefix ~/.yarn
yarn global add @cloudflare/wrangler