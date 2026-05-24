#!/usr/bin/env bash
# Deploy the pipeline to Google Cloud Dataflow (streaming mode).
#
# Required env vars:
#   GCS_BUCKET    - GCS bucket name (no gs://) for staging/temp
#   INPUT_TOPIC   - Pub/Sub topic name (short name, not full path)
#   BQ_TABLE      - BigQuery table in the form dataset.table
#
# Optional:
#   GCP_PROJECT   - defaults to active gcloud project
#   REGION        - defaults to us-central1

set -euo pipefail

PROJECT="${GCP_PROJECT:-$(gcloud config get-value project)}"
REGION="${REGION:-us-central1}"
TOPIC="${INPUT_TOPIC:?INPUT_TOPIC env var is required}"
BQ_TABLE="${BQ_TABLE:?BQ_TABLE env var is required (e.g. pii_scrubbed_data.scrubbed_messages)}"
DEAD_LETTER_TABLE="${DEAD_LETTER_TABLE:-${BQ_TABLE}_dead_letter}"
BUCKET="${GCS_BUCKET:?GCS_BUCKET env var is required}"
JOB_NAME="pii-scrubber-$(date +%Y%m%d-%H%M%S)"

echo "============================================"
echo "  Deploying Dataflow PII Scrubber"
echo "  Project : ${PROJECT}"
echo "  Region  : ${REGION}"
echo "  Topic   : projects/${PROJECT}/topics/${TOPIC}"
echo "  Table   : ${PROJECT}:${BQ_TABLE}"
echo "  Job     : ${JOB_NAME}"
echo "============================================"

python -m pipeline.main \
  --runner=DataflowRunner \
  --project="${PROJECT}" \
  --region="${REGION}" \
  --temp_location="gs://${BUCKET}/tmp/pii-scrubber" \
  --staging_location="gs://${BUCKET}/staging/pii-scrubber" \
  --job_name="${JOB_NAME}" \
  --setup_file=./setup.py \
  --input_topic="projects/${PROJECT}/topics/${TOPIC}" \
  --output_table="${PROJECT}:${BQ_TABLE}" \
  --dead_letter_table="${PROJECT}:${DEAD_LETTER_TABLE}" \
  --streaming
