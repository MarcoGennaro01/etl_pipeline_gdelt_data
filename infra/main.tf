terraform {
  required_version = ">= 1.5.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  region = var.gcp_region
}

resource "google_project" "test" {
  name= var.gcp_project_id
  project_id= var.gcp_project_id
  billing_account= var.gcp_billing_account
}

# APIs
resource "google_project_service" "storage" {
  project = google_project.test.project_id
  service = "storage.googleapis.com"
  disable_dependent_services = true
}

resource "google_project_service" "bigquery" {
  project = google_project.test.project_id
  service = "bigquery.googleapis.com"
  disable_dependent_services = true
}

resource "google_project_service" "dataproc" {
  project = google_project.test.project_id
  service = "dataproc.googleapis.com"

  disable_dependent_services = true
}

# 4. Cloud Storage
resource "google_storage_bucket" "gdelt" {
  name     = var.gcp_bucket_name
  project  = google_project.test.project_id
  location = var.gcp_region

  force_destroy = true
  uniform_bucket_level_access = true

  depends_on = [google_project_service.storage]
}

# 5. BigQuery Dataset
resource "google_bigquery_dataset" "gdelt" {
  project    = google_project.test.project_id
  dataset_id = var.big_query_dataset
  location   = var.gcp_region

  delete_contents_on_destroy = true
  depends_on = [google_project_service.bigquery]
}

# 6. BigQuery - Staging
resource "google_bigquery_table" "staging" {
  project    = google_project.test.project_id
  dataset_id = google_bigquery_dataset.gdelt.dataset_id
  table_id   = var.staging_table_name

  deletion_protection = false
}


