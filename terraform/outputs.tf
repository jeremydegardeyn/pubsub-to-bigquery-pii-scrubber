output "pubsub_input_topic" {
  description = "Full resource ID of the Pub/Sub input topic"
  value       = google_pubsub_topic.input.id
}

output "pubsub_dead_letter_topic" {
  description = "Full resource ID of the dead-letter Pub/Sub topic"
  value       = google_pubsub_topic.dead_letter.id
}

output "bq_scrubbed_table" {
  description = "BigQuery table reference for use with --output_table"
  value       = "${var.project_id}:${var.bq_dataset_id}.${var.bq_table_id}"
}

output "bq_dead_letter_table" {
  description = "BigQuery dead-letter table reference for use with --dead_letter_table"
  value       = "${var.project_id}:${var.bq_dataset_id}.${var.bq_table_id}_dead_letter"
}

output "dataflow_service_account" {
  description = "Email of the Dataflow runner service account"
  value       = google_service_account.dataflow_runner.email
}
