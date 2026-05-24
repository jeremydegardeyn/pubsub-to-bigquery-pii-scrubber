terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ---------------------------------------------------------------------------
# Pub/Sub
# ---------------------------------------------------------------------------

resource "google_pubsub_topic" "input" {
  name    = var.pubsub_topic_name
  project = var.project_id
}

resource "google_pubsub_topic" "dead_letter" {
  name    = "${var.pubsub_topic_name}-dead-letter"
  project = var.project_id
}

resource "google_pubsub_subscription" "input_sub" {
  name    = "${var.pubsub_topic_name}-sub"
  topic   = google_pubsub_topic.input.name
  project = var.project_id

  ack_deadline_seconds = 60

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.dead_letter.id
    max_delivery_attempts = 5
  }

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }
}

# ---------------------------------------------------------------------------
# BigQuery
# ---------------------------------------------------------------------------

resource "google_bigquery_dataset" "scrubbed" {
  dataset_id  = var.bq_dataset_id
  location    = var.region
  project     = var.project_id
  description = "PII-scrubbed messages written by the Dataflow pipeline"
}

resource "google_bigquery_table" "scrubbed_messages" {
  dataset_id          = google_bigquery_dataset.scrubbed.dataset_id
  table_id            = var.bq_table_id
  project             = var.project_id
  deletion_protection = true

  time_partitioning {
    type  = "DAY"
    field = "processed_at"
  }

  schema = jsonencode([
    { name = "message_id",       type = "STRING",    mode = "NULLABLE" },
    { name = "publish_time",     type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "scrubbed_payload", type = "STRING",    mode = "REQUIRED" },
    { name = "pii_detected",     type = "BOOL",      mode = "REQUIRED" },
    { name = "pii_types_found",  type = "STRING",    mode = "REPEATED" },
    { name = "scrub_count",      type = "INTEGER",   mode = "REQUIRED" },
    { name = "processed_at",     type = "TIMESTAMP", mode = "REQUIRED" },
  ])
}

resource "google_bigquery_table" "dead_letter" {
  dataset_id          = google_bigquery_dataset.scrubbed.dataset_id
  table_id            = "${var.bq_table_id}_dead_letter"
  project             = var.project_id
  deletion_protection = false

  schema = jsonencode([
    { name = "message_id",   type = "STRING",    mode = "NULLABLE" },
    { name = "error",        type = "STRING",    mode = "NULLABLE" },
    { name = "raw",          type = "STRING",    mode = "NULLABLE" },
    { name = "processed_at", type = "TIMESTAMP", mode = "REQUIRED" },
  ])
}

# ---------------------------------------------------------------------------
# Service Account & IAM
# ---------------------------------------------------------------------------

resource "google_service_account" "dataflow_runner" {
  account_id   = "dataflow-pii-scrubber"
  display_name = "Dataflow PII Scrubber"
  project      = var.project_id
}

locals {
  dataflow_roles = [
    "roles/dataflow.worker",
    "roles/bigquery.dataEditor",
    "roles/pubsub.subscriber",
    "roles/pubsub.viewer",
    "roles/storage.objectAdmin",
  ]
}

resource "google_project_iam_member" "dataflow_runner_roles" {
  for_each = toset(local.dataflow_roles)
  project  = var.project_id
  role     = each.value
  member   = "serviceAccount:${google_service_account.dataflow_runner.email}"
}
