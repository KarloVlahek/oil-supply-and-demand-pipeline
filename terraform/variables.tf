variable "project_id" {
  description = "GCP project ID"
  default     = "oil-supply-and-demand-pipeline"
}

variable "region" {
  description = "GCP region"
  default     = "us-central1"
}

variable "gcs_bucket_name" {
  description = "Name of the GCS raw data lake bucket"
  default     = "eia-oil-supply-and-demand-pipeline"
}

variable "bq_dataset_id" {
  description = "My BigQueryDataset Name"
  default     = "oil_supply_demand_dataset"
}

variable "bq_location" {
  description = "BigQuery dataset location"
  default     = "US"
}