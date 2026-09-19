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
  project = var.gcp_project_id
  region  = var.gcp_region
}


# Cloud Storage
resource "google_storage_bucket" "gdelt" {
  name     = var.gcp_bucket_name
  project  = var.gcp_project_id
  location = var.gcp_region

  uniform_bucket_level_access = true
}


# BigQuery Dataset
resource "google_bigquery_dataset" "gdelt" {
  project    = var.gcp_project_id
  dataset_id = var.big_query_dataset
  location   = var.gcp_region
}


# BigQuery - Staging
resource "google_bigquery_table" "staging" {
  project    = var.gcp_project_id
  dataset_id = google_bigquery_dataset.gdelt.dataset_id
  table_id   = var.staging_table_name

  deletion_protection = false
}


# BigQuery - Warehouse
resource "google_bigquery_table" "warehouse" {
  project    = var.gcp_project_id
  dataset_id = google_bigquery_dataset.gdelt.dataset_id
  table_id   = var.wh_table_name

  deletion_protection = false
}


# BigQuery - ML Mart
resource "google_bigquery_table" "mart_ml" {
  project    = var.gcp_project_id
  dataset_id = google_bigquery_dataset.gdelt.dataset_id
  table_id   = var.dm_table_name

  deletion_protection = false
}