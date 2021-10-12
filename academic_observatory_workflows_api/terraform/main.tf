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

module "api" {
  source       = "The-Academic-Observatory/api/google"
  version      = "0.0.2"
  api          = var.api
  environment  = var.environment
  google_cloud = var.google_cloud
  data_api     = { "create" = true, "elasticsearch_api_key" : var.elasticsearch.api_key, "elasticsearch_host" : var.elasticsearch.host }
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