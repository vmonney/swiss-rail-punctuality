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

resource "google_storage_bucket_iam_member" "pipeline_raw_bucket_writer" {
  bucket = google_storage_bucket.raw.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_bigquery_dataset_iam_member" "pipeline_dataset_editor" {
  dataset_id = google_bigquery_dataset.warehouse.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_project_iam_member" "pipeline_bigquery_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}
