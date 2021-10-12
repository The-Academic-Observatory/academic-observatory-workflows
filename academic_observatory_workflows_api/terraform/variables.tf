variable "api" {
  description = <<EOF
Settings related to the Data API

name: Name of the Data API project, e.g. academic-observatory or oaebu
package_name: Local path to the Data API package, e.g. /path/to/academic_observatory_workflows_api
domain_name: the custom domain name for the API, used for the google cloud endpoints service
subdomain: can be either 'project_id' or 'environment', used to determine a prefix for the domain_name
EOF
  type = object({
    name        = string
    package     = string
    domain_name = string
    subdomain   = string
  })
}

variable "environment" {
  description = "The environment type: develop, staging or production."
  type        = string
}

variable "google_cloud" {
  description = <<EOF
The Google Cloud settings for the Observatory Platform.

project_id: the Google Cloud project id.
credentials: the path to the Google Cloud credentials.
region: the Google Cloud region.
zone: the Google Cloud zone.
data_location: the data location for storing buckets.
EOF
  type = object({
    project_id    = string
    credentials   = string
    region        = string
    zone          = string
    data_location = string
  })
}

variable "elasticsearch" {
  description = <<EOF
Elasticsearch login information

api_key: The elasticsearch api key
host: The address of the elasticsearch server
EOF
  type = object({
    api_key = string
    host    = string
  })
  sensitive = true
}