import json
import logging
import re
import uuid
from datetime import datetime, timezone

import apache_beam as beam


# ---------------------------------------------------------------------------
# PII regex patterns
# ---------------------------------------------------------------------------
PII_PATTERNS = {
    "SSN": re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),
    "CREDIT_CARD": re.compile(
        r"\b(?:"
        r"4[0-9]{12}(?:[0-9]{3})?"           # Visa
        r"|5[1-5][0-9]{14}"                   # Mastercard
        r"|3[47][0-9]{13}"                    # Amex
        r"|3(?:0[0-5]|[68][0-9])[0-9]{11}"   # Diners
        r"|6(?:011|5[0-9]{2})[0-9]{12}"       # Discover
        r")\b"
    ),
    "EMAIL": re.compile(r"\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b"),
    "PHONE_US": re.compile(
        r"\b(?:\+?1[\s.\-]?)?\(?[2-9][0-9]{2}\)?[\s.\-]?[0-9]{3}[\s.\-]?[0-9]{4}\b"
    ),
    "IP_ADDRESS": re.compile(
        r"\b(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}"
        r"(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\b"
    ),
    "DATE_OF_BIRTH": re.compile(
        r"\b(?:0?[1-9]|1[0-2])[\/\-](?:0?[1-9]|[12][0-9]|3[01])[\/\-](?:19|20)\d{2}\b"
    ),
}


class ParsePubSubMessage(beam.DoFn):
    """Decode and JSON-parse a PubsubMessage; failed parses go to dead_letter."""

    def process(self, element, timestamp=beam.DoFn.TimestampParam, *args, **kwargs):
        try:
            raw_bytes = element.data if hasattr(element, "data") else element
            attributes = getattr(element, "attributes", {}) or {}
            message_id = attributes.get("message_id", str(uuid.uuid4()))

            payload = json.loads(raw_bytes.decode("utf-8"))

            yield {
                "message_id": message_id,
                "publish_time": timestamp.to_utc_datetime().isoformat(),
                "payload": payload,
                "attributes": attributes,
            }
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            logging.error("Failed to parse PubSub message: %s", exc)
            raw_text = (
                element.data.decode("utf-8", errors="replace")
                if hasattr(element, "data")
                else repr(element)
            )
            yield beam.pvalue.TaggedOutput(
                "dead_letter",
                {
                    "message_id": str(uuid.uuid4()),
                    "error": str(exc),
                    "raw": raw_text,
                    "processed_at": datetime.now(timezone.utc).isoformat(),
                },
            )


class ScrubPIIDoFn(beam.DoFn):
    """Walk every string value in the JSON payload and redact PII in place."""

    def _scrub_string(self, text: str, pii_found: dict) -> str:
        for pii_type, pattern in PII_PATTERNS.items():
            matches = pattern.findall(text)
            if matches:
                pii_found[pii_type] = pii_found.get(pii_type, 0) + len(matches)
                text = pattern.sub(f"[{pii_type}_REDACTED]", text)
        return text

    def _scrub_value(self, value, pii_found: dict):
        if isinstance(value, str):
            return self._scrub_string(value, pii_found)
        if isinstance(value, dict):
            return {k: self._scrub_value(v, pii_found) for k, v in value.items()}
        if isinstance(value, list):
            return [self._scrub_value(item, pii_found) for item in value]
        return value

    def process(self, element, *args, **kwargs):
        pii_found: dict = {}
        scrubbed = self._scrub_value(element["payload"], pii_found)

        total = sum(pii_found.values())
        if total:
            logging.info(
                "message_id=%s redacted %d PII instance(s): %s",
                element["message_id"],
                total,
                list(pii_found.keys()),
            )

        yield {
            "message_id": element["message_id"],
            "publish_time": element["publish_time"],
            "scrubbed_payload": json.dumps(scrubbed),
            "pii_detected": bool(pii_found),
            "pii_types_found": list(pii_found.keys()),
            "scrub_count": total,
            "processed_at": datetime.now(timezone.utc).isoformat(),
        }
