#!/usr/bin/env bash
# Run the pipeline locally with DirectRunner for development and smoke-testing.
# Note: streaming mode with DirectRunner requires messages already on the topic.
#
# Required env vars:
#   INPUT_TOPIC  - Pub/Sub topic name (short name)
#   BQ_TABLE     - BigQuery table: dataset.table
#
# Optional:
#   GCP_PROJECT  - defaults to active gcloud project

set -euo pipefail

PROJECT="${GCP_PROJECT:-$(gcloud config get-value project)}"
TOPIC="${INPUT_TOPIC:?INPUT_TOPIC env var is required}"
BQ_TABLE="${BQ_TABLE:?BQ_TABLE env var is required}"

echo "Running locally (DirectRunner)..."
echo "  Project: ${PROJECT}"
echo "  Topic  : projects/${PROJECT}/topics/${TOPIC}"
echo "  Table  : ${PROJECT}:${BQ_TABLE}"

python -m pipeline.main \
  --runner=DirectRunner \
  --project="${PROJECT}" \
  --input_topic="projects/${PROJECT}/topics/${TOPIC}" \
  --output_table="${PROJECT}:${BQ_TABLE}" \
  --streaming
