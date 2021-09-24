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
      source = "hashicorp/google"
      version = "~> 3.85.0"
    }
  }
}

provider "google" {
  credentials = var.google_cloud.credentials
  project = var.google_cloud.project_id
  region = var.google_cloud.region
  zone = var.google_cloud.zone
}

data "google_project" "project" {
  project_id = var.google_cloud.project_id
  depends_on = [google_project_service.cloud_resource_manager]
}

resource "google_project_service" "cloud_resource_manager" {
  project = var.google_cloud.project_id
  service = "cloudresourcemanager.googleapis.com"
  disable_dependent_services = true
}

########################################################################################################################
# Enable API services
########################################################################################################################

resource "google_project_service" "services" {
  for_each = toset(["servicemanagement.googleapis.com", "servicecontrol.googleapis.com", "endpoints.googleapis.com",
    "iam.googleapis.com", "secretmanager.googleapis.com"])
  project = var.google_cloud.project_id
  service = each.key
  disable_on_destroy = false
  depends_on = [google_project_service.cloud_resource_manager]
}


# Enable service
resource "google_project_service" "api-project-service" {
  service = google_endpoints_service.api.service_name
  project = var.google_cloud.project_id
  depends_on = [google_endpoints_service.api]
  disable_on_destroy = false
}

########################################################################################################################
# Cloud Run backend for API
########################################################################################################################

resource "google_service_account" "api-backend_service_account" {
  account_id   = "${var.api.name}-api-backend"
  display_name = "Cloud Run backend Service Account"
  description = "The Google Service Account used by the cloud run backend"
  depends_on = [google_project_service.services["iam.googleapis.com"]]
}

# Create Elasticsearch secrets
module "elasticsearch-logins" {
  for_each = toset(nonsensitive(keys(var.elasticsearch))) # Make keys of variable nonsensitive.
  source = "./secret"
  secret_id = "${var.api.name}-elasticsearch-${each.key}"
  secret_data = var.elasticsearch[each.key]
  service_account_email = google_service_account.api-backend_service_account.email
  depends_on = [google_project_service.services["secretmanager.googleapis.com"]]
}


resource "google_cloud_run_service" "api_backend" {
  name     = "${var.api.name}-api-backend"
  location = var.google_cloud.region

  template {
    spec {
      containers {
        image = "gcr.io/${var.google_cloud.project_id}/${var.api.name}-api"
        env {
          name = "ES_API_KEY"
          value = "sm://${var.google_cloud.project_id}/${var.api.name}-elasticsearch-api_key"
        }
        env {
          name = "ES_HOST"
          value = "sm://${var.google_cloud.project_id}/${var.api.name}-elasticsearch-host"
        }
      }
      service_account_name = google_service_account.api-backend_service_account.email
    }
    metadata {
      annotations = {
        "autoscaling.knative.dev/maxScale" = "10"
      }
    }
  }

  traffic {
    percent = 100
    latest_revision = true
  }
  depends_on = [module.elasticsearch-logins]
}

########################################################################################################################
# Endpoints service
########################################################################################################################

locals {
  # Use the project id as a subdomain for a project that will not host the final production API. The endpoint service/domain name is
  # unique and can only be used in 1 project. Once it is created in one project, it can't be fully deleted for 30 days.
  project_domain_name = "${var.google_cloud.project_id}.${var.api.name}.${var.api.domain_name}"
  environment_domain_name = var.environment == "production" ? "${var.api.name}.${var.api.domain_name}" : "${var.environment}.${var.api.name}.${var.api.domain_name}"
  full_domain_name =  var.api.subdomain == "project_id" ? local.project_domain_name : local.environment_domain_name
}

# Create/update endpoints configuration based on OpenAPI
resource "google_endpoints_service" "api" {
  project = var.google_cloud.project_id
  service_name = local.full_domain_name
  openapi_config = templatefile("./openapi.yaml.tpl", {
    host = local.full_domain_name
    backend_address = google_cloud_run_service.api_backend.status[0].url
  })
}

########################################################################################################################
# Cloud Run Gateway
########################################################################################################################

# Create service account used by Cloud Run
resource "google_service_account" "api-gateway_service_account" {
  account_id = "${var.api.name}-api-gateway"
  display_name = "Cloud Run gateway Service Account"
  description = "The Google Service Account used by the cloud run gateway"
  depends_on = [google_project_service.services["iam.googleapis.com"]]
}

# Give permission to Cloud Run gateway service-account to access private Cloud Run backend
resource "google_project_iam_member" "api-gateway_service_account_cloudrun_iam" {
  project = var.google_cloud.project_id
  role = "roles/run.invoker"
  member = "serviceAccount:${google_service_account.api-gateway_service_account.email}"
}

# Give permission to Cloud Run gateway service-account to control service management
resource "google_project_iam_member" "api-gateway_service_account_servicecontroller_iam" {
  project = var.google_cloud.project_id
  role = "roles/servicemanagement.serviceController"
  member = "serviceAccount:${google_service_account.api-gateway_service_account.email}"
}

# Create/update Cloud Run service
resource "google_cloud_run_service" "api_gateway" {
  name = "${var.api.name}-api-gateway"
  location = var.google_cloud.region
  project = var.google_cloud.project_id
  template {
    spec {
      containers {
        image = "gcr.io/endpoints-release/endpoints-runtime-serverless:2"
        env {
          name = "ENDPOINTS_SERVICE_NAME"
          value = google_endpoints_service.api.service_name
        }
      }
      service_account_name = google_service_account.api-gateway_service_account.email
    }
  }
  depends_on = [google_endpoints_service.api, google_project_iam_member.api-gateway_service_account_servicecontroller_iam]
}

# Create custom domain mapping for cloud run gateway
resource "google_cloud_run_domain_mapping" "default" {
  location = google_cloud_run_service.api_gateway.location
  name = local.full_domain_name

  metadata {
    namespace = var.google_cloud.project_id
  }

  spec {
    route_name = google_cloud_run_service.api_gateway.name
  }
}

# Create public access policy
data "google_iam_policy" "noauth" {
  binding {
    role = "roles/run.invoker"
    members = [
      "allUsers",
    ]
  }
}

# Enable public access policy on gateway (access is restricted with API key by openapi config)
resource "google_cloud_run_service_iam_policy" "noauth-endpoints" {
  location = google_cloud_run_service.api_gateway.location
  project = google_cloud_run_service.api_gateway.project
  service = google_cloud_run_service.api_gateway.name
  policy_data = data.google_iam_policy.noauth.policy_data
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