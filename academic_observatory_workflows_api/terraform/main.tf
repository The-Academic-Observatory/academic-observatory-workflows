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

data "terraform_remote_state" "observatory" {
  count = var.observatory_api.observatory_workspace != "" ? 1 : 0
  backend = "remote"
  config = {
    organization = var.observatory_api.observatory_organization
    workspaces = {
      name = var.observatory_api.observatory_workspace
    }
  }
}

data "local_file" "build_image_info" {
  count = var.build_info == null ? 1 : 0
  filename = "./image_build.txt"
}

locals {
  build_info = try(data.local_file.build_image_info[0].content, var.build_info)
  vpc_connector_name = try(data.terraform_remote_state.observatory[0].outputs.vpc_connector_name, null)
  observatory_db_uri = try(data.terraform_remote_state.observatory[0].outputs.observatory_db_uri, null)
  elasticsearch_host = var.data_api.elasticsearch_host != "" ? var.data_api.elasticsearch_host : null
  elasticsearch_api_key = var.data_api.elasticsearch_api_key != "" ? var.data_api.elasticsearch_api_key : null
}


module "api" {
  source  = "The-Academic-Observatory/api/google"
  version = "0.0.4"
//  source = "./api"
  build_info = local.build_info
  api = var.api
  environment = var.environment
  google_cloud = var.google_cloud
  observatory_api = {
    "vpc_connector_name": local.vpc_connector_name,
    "observatory_db_uri": local.observatory_db_uri
  }
  data_api     = {
    "elasticsearch_host" : local.elasticsearch_host,
    "elasticsearch_api_key" : local.elasticsearch_api_key
  }
}

//########################################################################################################################
//# Cloud Build to automate build/deployment
//########################################################################################################################
//resource "google_cloudbuild_trigger" "build-trigger" {
//  github {
//    push {
//      branch = "INF-73_data_API"
//    }
//  }
//
//  build {
//    step {
//      name = "gcr.io/cloud-builders/docker"
//      args = ["build", "-t", "gcr.io/$PROJECT_ID/$REPO_NAME/${google_cloud_run_service.api_backend.name}:$COMMIT_SHA",
//        "-f", "${var.package_name}/Dockerfile"]
//      timeout = "120s"
//    }
//    step {
//      name = ""
//    }
//
//    source {
//      repoSource {
//        bucket = "mybucket"
//        object = "source_code.tar.gz"
//      }
//    }
//    tags = ["build", "newFeature"]
//    substitutions = {
//      _FOO = "bar"
//      _BAZ = "qux"
//    }
//    queue_ttl = "20s"
//    logs_bucket = "gs://mybucket/logs"
//    secret {
//      kms_key_name = "projects/myProject/locations/global/keyRings/keyring-name/cryptoKeys/key-name"
//      secret_env = {
//        PASSWORD = "ZW5jcnlwdGVkLXBhc3N3b3JkCg=="
//      }
//    }
//    artifacts {
//      images = ["gcr.io/$PROJECT_ID/$REPO_NAME:$COMMIT_SHA"]
//      objects {
//        location = "gs://bucket/path/to/somewhere/"
//        paths = ["path"]
//      }
//    }
//    options {
//      source_provenance_hash = ["MD5"]
//      requested_verify_option = "VERIFIED"
//      machine_type = "N1_HIGHCPU_8"
//      disk_size_gb = 100
//      substitution_option = "ALLOW_LOOSE"
//      dynamic_substitutions = true
//      log_streaming_option = "STREAM_OFF"
//      worker_pool = "pool"
//      logging = "LEGACY"
//      env = ["ekey = evalue"]
//      secret_env = ["secretenv = svalue"]
//      volumes {
//        name = "v1"
//        path = "v1"
//      }
//    }
//  }
//}