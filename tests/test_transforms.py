import json
import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from pipeline.transforms import ScrubPIIDoFn

# ---------------------------------------------------------------------------
# Shared fixture
# ---------------------------------------------------------------------------
SAMPLE_WITH_PII = {
    "message_id": "test-msg-001",
    "publish_time": "2024-01-01T00:00:00+00:00",
    "attributes": {},
    "payload": {
        "name": "John Doe",
        "email": "john.doe@example.com",
        "ssn": "123-45-6789",
        "phone": "555-867-5309",
        "dob": "01/15/1985",
        "notes": "Reach me at john.doe@example.com or call 555-867-5309",
        "nested": {
            "contact": {
                "backup_email": "backup@example.com",
                "ip": "192.168.1.100",
            }
        },
        "tags": ["user", "john.doe@example.com"],
        "safe_field": "nothing sensitive here",
        "count": 42,
    },
}

SAMPLE_CLEAN = {
    "message_id": "test-msg-002",
    "publish_time": "2024-01-01T00:00:00+00:00",
    "attributes": {},
    "payload": {
        "event": "page_view",
        "page": "/home",
        "duration_ms": 1234,
    },
}


def _scrub(inputs):
    fn = ScrubPIIDoFn()
    results = []
    for item in inputs:
        results.extend(fn.process(item))
    return results


class TestScrubPIIDoFn(unittest.TestCase):

    def setUp(self):
        self.results_pii = _scrub([SAMPLE_WITH_PII])
        self.payload_pii = json.loads(self.results_pii[0]["scrubbed_payload"])

    # --- PII detection flags ---

    def test_pii_detected_true_when_pii_present(self):
        self.assertTrue(self.results_pii[0]["pii_detected"])

    def test_pii_types_found_contains_expected(self):
        found = set(self.results_pii[0]["pii_types_found"])
        self.assertIn("EMAIL", found)
        self.assertIn("SSN", found)
        self.assertIn("PHONE_US", found)

    def test_scrub_count_reflects_all_occurrences(self):
        # email appears twice (field + notes), phone twice, SSN once, DOB once, IP once
        self.assertGreaterEqual(self.results_pii[0]["scrub_count"], 6)

    # --- Redaction correctness ---

    def test_email_redacted(self):
        self.assertNotIn("john.doe@example.com", self.payload_pii["email"])
        self.assertIn("[EMAIL_REDACTED]", self.payload_pii["email"])

    def test_ssn_redacted(self):
        self.assertNotIn("123-45-6789", self.payload_pii["ssn"])
        self.assertIn("[SSN_REDACTED]", self.payload_pii["ssn"])

    def test_phone_redacted(self):
        self.assertNotIn("555-867-5309", self.payload_pii["phone"])
        self.assertIn("[PHONE_US_REDACTED]", self.payload_pii["phone"])

    def test_dob_redacted(self):
        self.assertNotIn("01/15/1985", self.payload_pii["dob"])
        self.assertIn("[DATE_OF_BIRTH_REDACTED]", self.payload_pii["dob"])

    def test_pii_in_prose_field_redacted(self):
        self.assertNotIn("john.doe@example.com", self.payload_pii["notes"])
        self.assertNotIn("555-867-5309", self.payload_pii["notes"])

    # --- Nested structures ---

    def test_nested_email_redacted(self):
        backup = self.payload_pii["nested"]["contact"]["backup_email"]
        self.assertIn("[EMAIL_REDACTED]", backup)

    def test_nested_ip_redacted(self):
        ip = self.payload_pii["nested"]["contact"]["ip"]
        self.assertIn("[IP_ADDRESS_REDACTED]", ip)

    def test_list_values_redacted(self):
        self.assertIn("[EMAIL_REDACTED]", self.payload_pii["tags"][1])

    # --- Safe fields preserved ---

    def test_safe_string_unchanged(self):
        self.assertEqual(self.payload_pii["safe_field"], "nothing sensitive here")

    def test_numeric_field_unchanged(self):
        self.assertEqual(self.payload_pii["count"], 42)

    # --- Clean message ---

    def test_no_pii_clean_message(self):
        results = _scrub([SAMPLE_CLEAN])
        self.assertFalse(results[0]["pii_detected"])
        self.assertEqual(results[0]["scrub_count"], 0)
        self.assertEqual(results[0]["pii_types_found"], [])

    def test_clean_payload_unchanged(self):
        results = _scrub([SAMPLE_CLEAN])
        payload = json.loads(results[0]["scrubbed_payload"])
        self.assertEqual(payload["event"], "page_view")
        self.assertEqual(payload["duration_ms"], 1234)

    # --- Output shape ---

    def test_output_contains_required_keys(self):
        row = self.results_pii[0]
        for key in ("message_id", "publish_time", "scrubbed_payload",
                    "pii_detected", "pii_types_found", "scrub_count", "processed_at"):
            self.assertIn(key, row)

    def test_message_id_preserved(self):
        self.assertEqual(self.results_pii[0]["message_id"], "test-msg-001")


if __name__ == "__main__":
    unittest.main()
