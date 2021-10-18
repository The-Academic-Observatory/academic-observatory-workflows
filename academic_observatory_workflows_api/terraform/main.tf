########################################################################################################################
# Configure Google Cloud Provider
########################################################################################################################

terraform {
  backend "remote" {
    workspaces {
      prefix = "observatory-"
    }
  }
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 3.85.0"
    }
  }
}

provider "google" {
  credentials = var.google_cloud.credentials
  project     = var.google_cloud.project_id
  region      = var.google_cloud.region
  zone        = var.google_cloud.zone
}

# Get info from the observatory workspace if this is given
data "terraform_remote_state" "observatory" {
  count   = var.observatory_api.observatory_workspace != "" ? 1 : 0
  backend = "remote"
  config = {
    organization = var.observatory_api.observatory_organization
    workspaces = {
      name = var.observatory_api.observatory_workspace
    }
  }
}

# Get build info from a local file if no build_info is passed on
data "local_file" "image_tag" {
  count    = var.api.image_tag == "" ? 1 : 0
  filename = "./image_build.txt"
}

locals {
  # Set the build info, either from local file or from variable
  image_tag = try(data.local_file.image_tag[0].content, var.api.image_tag)
  # Set the vpc connector name and observatory db uri, obtained from other terraform workspace
  vpc_connector_name = try(data.terraform_remote_state.observatory[0].outputs.vpc_connector_name, null)
  observatory_db_uri = try(data.terraform_remote_state.observatory[0].outputs.observatory_db_uri, null)
  # Set the elasticsearch host and api key if not empty, otherwise null
  elasticsearch_host    = var.data_api.elasticsearch_host != "" ? var.data_api.elasticsearch_host : null
  elasticsearch_api_key = var.data_api.elasticsearch_api_key != "" ? var.data_api.elasticsearch_api_key : null
}


module "api" {
//  source       = "./api"
  source       = "The-Academic-Observatory/api/google"
  version      = "0.0.5"
  image_tag    = local.image_tag
  api          = var.api
  environment  = var.environment
  google_cloud = var.google_cloud
  observatory_api = {
    "vpc_connector_name" : local.vpc_connector_name,
    "observatory_db_uri" : local.observatory_db_uri
  }
  data_api = {
    "elasticsearch_host" : local.elasticsearch_host,
    "elasticsearch_api_key" : local.elasticsearch_api_key
  }
}
