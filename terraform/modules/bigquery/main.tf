resource "google_bigquery_dataset" "oil_supply_demand_dataset" {
  dataset_id  = var.dataset_id
  location    = var.location
  description = "EPA CEMS power plant emissions pipeline dataset"
}

resource "google_bigquery_table" "raw_cems_daily" {
  dataset_id          = google_bigquery_dataset.oil_supply_demand_dataset.dataset_id
  table_id            = "raw_cems_daily"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "date"
  }

  clustering = ["state", "facility_id"]

  schema = file("${path.module}/schemas/raw_cems_hourly.json")
}