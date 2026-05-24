# pubsub-to-bigquery-pii-scrubber

An Apache Beam (Python) streaming pipeline that runs on **Google Cloud Dataflow** to:

1. Read JSON messages from a **Pub/Sub** topic
2. Walk every string field and **redact PII** using regex patterns
3. Stream the scrubbed records to **BigQuery**

Unparseable messages are routed to a dead-letter BigQuery table rather than dropped.

---

## Architecture

```
Pub/Sub Topic
      │
      ▼
┌─────────────────┐
│  Parse JSON     │──► Dead-letter BQ table (bad JSON)
└────────┬────────┘
         │ valid
         ▼
┌─────────────────┐
│  Scrub PII      │   regex scan of all string fields
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  BigQuery       │   scrubbed_messages (partitioned by processed_at)
└─────────────────┘
```

---

## PII Types Detected

| Type | Example | Replacement |
|------|---------|-------------|
| SSN | `123-45-6789` | `[SSN_REDACTED]` |
| Credit Card | `4111111111111111` | `[CREDIT_CARD_REDACTED]` |
| Email | `user@example.com` | `[EMAIL_REDACTED]` |
| US Phone | `(555) 867-5309` | `[PHONE_US_REDACTED]` |
| IP Address | `192.168.1.1` | `[IP_ADDRESS_REDACTED]` |
| Date of Birth | `01/15/1985` | `[DATE_OF_BIRTH_REDACTED]` |

Detection recurses into nested objects and arrays — every string value is scanned.

---

## BigQuery Schema

| Column | Type | Mode | Description |
|--------|------|------|-------------|
| `message_id` | STRING | NULLABLE | Pub/Sub message identifier |
| `publish_time` | TIMESTAMP | NULLABLE | When the message was published |
| `scrubbed_payload` | STRING | REQUIRED | Full JSON with PII redacted |
| `pii_detected` | BOOL | REQUIRED | True if any PII was found |
| `pii_types_found` | STRING | REPEATED | List of PII types detected |
| `scrub_count` | INTEGER | REQUIRED | Total number of redactions made |
| `processed_at` | TIMESTAMP | REQUIRED | Pipeline processing time (partition key) |

---

## Prerequisites

- Python 3.9+
- [Google Cloud SDK](https://cloud.google.com/sdk) (`gcloud` authenticated)
- GCP project with these APIs enabled:
  - Dataflow API
  - Pub/Sub API
  - BigQuery API
  - Cloud Storage API

---

## Quick Start

### 1. Install dependencies

```bash
pip install -e ".[dev]"
```

### 2. Provision GCP resources

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars with your project values
terraform init
terraform apply
```

### 3. Run the unit tests

```bash
pytest tests/ -v
```

### 4. Deploy to Dataflow

```bash
export GCP_PROJECT=your-project-id
export INPUT_TOPIC=pii-scrubber-input
export BQ_TABLE=pii_scrubbed_data.scrubbed_messages
export GCS_BUCKET=your-bucket-name

bash scripts/deploy.sh
```

### 5. Run locally (DirectRunner)

Useful for smoke-testing with a live Pub/Sub topic:

```bash
export GCP_PROJECT=your-project-id
export INPUT_TOPIC=pii-scrubber-input
export BQ_TABLE=pii_scrubbed_data.scrubbed_messages

bash scripts/run_local.sh
```

---

## Pipeline Options

| Argument | Required | Description |
|----------|----------|-------------|
| `--input_topic` | Yes | Full topic path: `projects/<proj>/topics/<topic>` |
| `--output_table` | Yes | BQ table: `<project>:<dataset>.<table>` |
| `--dead_letter_table` | No | BQ dead-letter table (same format) |
| `--runner` | No | `DataflowRunner` (default) or `DirectRunner` |
| `--project` | Yes (Dataflow) | GCP project ID |
| `--region` | No | GCP region (default: `us-central1`) |

All standard [Beam PipelineOptions](https://beam.apache.org/documentation/runners/dataflow/) are supported.

---

## Project Structure

```
├── pipeline/
│   ├── main.py          # Entry point & pipeline graph
│   ├── transforms.py    # ParsePubSubMessage + ScrubPIIDoFn
│   └── schema.py        # BigQuery table schemas
├── tests/
│   └── test_transforms.py
├── terraform/           # GCP infrastructure (Pub/Sub, BQ, SA, IAM)
├── scripts/
│   ├── deploy.sh        # Dataflow deployment
│   └── run_local.sh     # DirectRunner smoke test
├── .github/workflows/
│   └── ci.yml           # GitHub Actions CI (Python 3.10–3.12)
├── setup.py
└── requirements.txt
```
