locals {
  raw_bucket_name = "sbb-punctuality-raw-${var.bucket_suffix}"
}

resource "google_storage_bucket" "raw" {
  name                        = local.raw_bucket_name
  location                    = var.region
  storage_class               = "STANDARD"
  force_destroy               = false
  uniform_bucket_level_access = true
}

resource "google_bigquery_dataset" "warehouse" {
  dataset_id = var.dataset_id
  location   = var.region
}

resource "google_service_account" "pipeline" {
  account_id   = "sbb-punctuality-pipeline"
  display_name = "SBB Punctuality Pipeline SA"
  description  = "Service account used by ingestion and transformation workloads."
}
