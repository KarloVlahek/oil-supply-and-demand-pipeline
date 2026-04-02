resource "google_storage_bucket" "oil_supply_demand_bucket" {
  name          = var.bucket_name
  location      = var.location
  force_destroy = true
}