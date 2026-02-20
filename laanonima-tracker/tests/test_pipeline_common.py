"""Tests for pipeline common helpers."""

import sys
import unittest
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.pipeline_common import db_fingerprint, resolve_ipc_window


class TestPipelineCommon(unittest.TestCase):
    def test_db_fingerprint_masks_credentials(self):
        db_url = "postgresql://user:secret@example.com:5432/prod_db"
        fingerprint = db_fingerprint(db_url)
        self.assertEqual(fingerprint, "postgresql://example.com:5432/prod_db")
        self.assertNotIn("secret", fingerprint)
        self.assertNotIn("user", fingerprint)

    def test_resolve_ipc_window_with_explicit_range(self):
        start, end = resolve_ipc_window("2026-01", "2026-03", lookback_months=2)
        self.assertEqual(start, "2026-01")
        self.assertEqual(end, "2026-03")

    def test_resolve_ipc_window_uses_lookback(self):
        # Deterministic check relative to current month shape only.
        start, end = resolve_ipc_window(None, None, lookback_months=2)
        self.assertRegex(start, r"^\d{4}-\d{2}$")
        self.assertRegex(end, r"^\d{4}-\d{2}$")
        self.assertLessEqual(start, end)

    def test_resolve_ipc_window_rejects_partial_input(self):
        with self.assertRaises(RuntimeError):
            resolve_ipc_window("2026-01", None, lookback_months=2)


if __name__ == "__main__":
    unittest.main()
