variable "project_id" {
  description = "GCP project ID."
  type        = string
}

variable "region" {
  description = "Primary GCP region for resources."
  type        = string
  default     = "europe-west6"
}

variable "bucket_suffix" {
  description = "Unique suffix used in the raw bucket name."
  type        = string
}

variable "dataset_id" {
  description = "BigQuery dataset ID."
  type        = string
  default     = "sbb_punctuality"
}
