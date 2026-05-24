BQ_SCHEMA = {
    "fields": [
        {"name": "message_id",       "type": "STRING",    "mode": "NULLABLE"},
        {"name": "publish_time",     "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "scrubbed_payload", "type": "STRING",    "mode": "REQUIRED"},
        {"name": "pii_detected",     "type": "BOOL",      "mode": "REQUIRED"},
        {"name": "pii_types_found",  "type": "STRING",    "mode": "REPEATED"},
        {"name": "scrub_count",      "type": "INTEGER",   "mode": "REQUIRED"},
        {"name": "processed_at",     "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}

DEAD_LETTER_SCHEMA = {
    "fields": [
        {"name": "message_id",   "type": "STRING",    "mode": "NULLABLE"},
        {"name": "error",        "type": "STRING",    "mode": "NULLABLE"},
        {"name": "raw",          "type": "STRING",    "mode": "NULLABLE"},
        {"name": "processed_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}
