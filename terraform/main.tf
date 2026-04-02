terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project     = var.project_id
  region      = var.region
  credentials = file("./credentials/gcp-key.json")
}

# GCS Bucket Module
module "gcs" {
  source      = "./modules/gcs"
  bucket_name = var.gcs_bucket_name
  region      = var.region
}

# Big Query Module
module "bigquery" {
  source     = "./modules/bigquery"
  location   = var.bq_location
  dataset_id = var.bq_dataset_id
}