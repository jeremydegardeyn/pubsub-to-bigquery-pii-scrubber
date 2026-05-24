variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region for all resources"
  type        = string
  default     = "us-central1"
}

variable "pubsub_topic_name" {
  description = "Name of the Pub/Sub input topic"
  type        = string
  default     = "pii-scrubber-input"
}

variable "bq_dataset_id" {
  description = "BigQuery dataset to write scrubbed messages into"
  type        = string
  default     = "pii_scrubbed_data"
}

variable "bq_table_id" {
  description = "BigQuery table name for scrubbed messages"
  type        = string
  default     = "scrubbed_messages"
}

variable "dataflow_bucket" {
  description = "GCS bucket for Dataflow staging and temp files (bucket name only, no gs://)"
  type        = string
}
