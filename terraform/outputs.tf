output "raw_bucket_name" {
  description = "Name of the raw GCS bucket."
  value       = google_storage_bucket.raw.name
}

output "bigquery_dataset_id" {
  description = "BigQuery dataset ID."
  value       = google_bigquery_dataset.warehouse.dataset_id
}

output "service_account_email" {
  description = "Service account email for pipeline workloads."
  value       = google_service_account.pipeline.email
}
