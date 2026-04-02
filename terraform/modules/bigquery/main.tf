resource "google_bigquery_dataset" "oil_supply_demand_dataset" { #demo_dataset is the name of the resource within the TERRAFORM file we're working with
  dataset_id = var.dataset_id
  location   = var.location
}