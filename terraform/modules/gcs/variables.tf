variable "bucket_name" {
description = "Name of the GCS raw data lake bucket"
default        = "eia-oil-supply-and-demand-pipeline"
}

variable "region" {
  description = "GCP region"
  default     = "us-central1"
}

variable "location" {
  description = "GCS location"
  default     = "US"
}

variable "gcs_storage_class" {
  description = "GCP storage class"
  default     = "STANDARD"
}