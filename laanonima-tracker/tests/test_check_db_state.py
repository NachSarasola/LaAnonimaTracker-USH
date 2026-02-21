"""Tests for freshness and minimum-data gates in check_db_state script."""

import io
import sys
import unittest
from unittest.mock import patch
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import scripts.check_db_state as check_db_state


class TestCheckDBState(unittest.TestCase):
    def _run_main(self, argv, state):
        with patch("scripts.check_db_state._compute_state", return_value=state):
            with patch.object(sys, "argv", ["check_db_state.py", *argv]):
                with patch("sys.stderr", new_callable=io.StringIO) as stderr:
                    code = check_db_state.main()
                    return code, stderr.getvalue()

    def test_freshness_gate_passes_at_boundary(self):
        state = {
            "prices_count": 5,
            "price_candidates_count": 0,
            "scrape_runs_count": 1,
            "latest_scraped_at": "2026-02-21T00:00:00+00:00",
            "latest_scraped_age_hours": 168.0,
            "latest_run_started_at": "2026-02-21T00:00:00+00:00",
            "is_empty_for_bootstrap": False,
        }
        code, stderr = self._run_main(
            ["--backend", "postgresql", "--require-fresh-max-age-hours", "168"],
            state,
        )
        self.assertEqual(code, 0)
        self.assertEqual(stderr, "")

    def test_freshness_gate_fails_when_stale(self):
        state = {
            "prices_count": 5,
            "price_candidates_count": 0,
            "scrape_runs_count": 1,
            "latest_scraped_at": "2026-02-10T00:00:00+00:00",
            "latest_scraped_age_hours": 170.0,
            "latest_run_started_at": "2026-02-10T00:00:00+00:00",
            "is_empty_for_bootstrap": False,
        }
        code, stderr = self._run_main(
            ["--backend", "postgresql", "--require-fresh-max-age-hours", "168"],
            state,
        )
        self.assertEqual(code, 1)
        self.assertIn("Freshness gate failed", stderr)

    def test_freshness_gate_fails_when_latest_missing(self):
        state = {
            "prices_count": 5,
            "price_candidates_count": 0,
            "scrape_runs_count": 1,
            "latest_scraped_at": None,
            "latest_scraped_age_hours": None,
            "latest_run_started_at": None,
            "is_empty_for_bootstrap": False,
        }
        code, stderr = self._run_main(
            ["--backend", "postgresql", "--require-fresh-max-age-hours", "168"],
            state,
        )
        self.assertEqual(code, 1)
        self.assertIn("latest_scraped_at is not available", stderr)

    def test_freshness_gate_fails_when_min_prices_not_met(self):
        state = {
            "prices_count": 0,
            "price_candidates_count": 0,
            "scrape_runs_count": 0,
            "latest_scraped_at": None,
            "latest_scraped_age_hours": None,
            "latest_run_started_at": None,
            "is_empty_for_bootstrap": True,
        }
        code, stderr = self._run_main(
            [
                "--backend",
                "postgresql",
                "--require-fresh-max-age-hours",
                "168",
                "--require-min-prices",
                "1",
            ],
            state,
        )
        self.assertEqual(code, 1)
        self.assertIn("prices_count=0 < require_min_prices=1", stderr)


if __name__ == "__main__":
    unittest.main()

