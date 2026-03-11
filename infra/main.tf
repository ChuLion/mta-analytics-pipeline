terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# GCS Bucket — Raw landing zone for MTA CSVs
resource "google_storage_bucket" "mta_raw" {
  name          = "${var.project_id}-mta-raw"
  location      = var.region
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "Delete"
    }
  }
}

# BigQuery Datasets — Bronze / Silver / Gold
resource "google_bigquery_dataset" "bronze" {
  dataset_id  = "mta_bronze"
  description = "Raw ingested MTA data — no transformations"
  location    = var.region
}

resource "google_bigquery_dataset" "silver" {
  dataset_id  = "mta_silver"
  description = "Cleaned and joined MTA data — dbt staging and intermediate models"
  location    = var.region
}

resource "google_bigquery_dataset" "gold" {
  dataset_id  = "mta_gold"
  description = "Executive-ready mart tables — feeds Tableau dashboards"
  location    = var.region
}