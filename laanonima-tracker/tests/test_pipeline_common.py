"""Tests for pipeline common helpers."""

import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.pipeline_common import db_fingerprint, resolve_ipc_window, write_github_summary


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

    def test_write_github_summary_includes_fallback_metadata_and_warnings(self):
        payload = {
            "status": "completed",
            "total_seconds": 12.3,
            "db_fingerprint": "postgresql://example.com:5432/prod_db",
            "ipc_window": {"from": "2026-01", "to": "2026-02"},
            "scrape_fallback_used": True,
            "scrape_block_retries_used": 1,
            "source_block_reason": "marker=request could not be satisfied",
            "data_age_hours": 6.5,
            "warnings": ["scrape-full fallback activado"],
            "stages": [{"stage": "publish-web", "duration_seconds": 4.2}],
        }
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            summary_path = tmp.name

        try:
            with patch.dict("os.environ", {"GITHUB_STEP_SUMMARY": summary_path}):
                write_github_summary(payload)

            rendered = Path(summary_path).read_text(encoding="utf-8")
            self.assertIn("Scrape fallback used", rendered)
            self.assertIn("scrape-full fallback activado", rendered)
            self.assertIn("Source block reason", rendered)
            self.assertIn("Data age hours", rendered)
        finally:
            Path(summary_path).unlink(missing_ok=True)


if __name__ == "__main__":
    unittest.main()
