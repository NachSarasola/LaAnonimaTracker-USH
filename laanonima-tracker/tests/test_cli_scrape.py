"""CLI tests for new scrape planning flags and defaults."""

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

from click.testing import CliRunner

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.cli import cli


class TestCliScrapeOptions(unittest.TestCase):
    def setUp(self):
        self.runner = CliRunner()

    @patch("src.cli.setup_logging")
    @patch("src.cli.ensure_directories")
    @patch("src.cli.load_config", return_value={"logging": {}})
    @patch("src.cli.run_scrape")
    def test_scrape_defaults_include_new_flags(self, mock_run_scrape, *_mocks):
        mock_run_scrape.return_value = {
            "run_uuid": "r1",
            "status": "completed",
            "started_at": "2026-02-19T00:00:00Z",
            "completed_at": "2026-02-19T00:01:00Z",
            "products_planned": 5,
            "products_scraped": 5,
            "products_failed": 0,
            "products_skipped": 0,
            "errors": [],
            "plan_summary": {"profile": "balanced", "mandatory_count": 3, "rotation_applied": 2, "estimated_duration_seconds": 300, "segments": {"cba": 3, "daily_rotation": 2}},
            "coverage_by_segment": {"cba": {"planned": 3, "scraped": 3, "failed": 0, "skipped": 0}},
            "budget": {"target_seconds": 1200, "estimated_seconds": 300, "actual_seconds": 290, "within_target": True},
            "observation_policy": "single+audit",
            "candidate_storage_mode": "json",
            "candidates_audit_path": None,
        }
        result = self.runner.invoke(cli, ["scrape"])
        self.assertEqual(result.exit_code, 0)

        kwargs = mock_run_scrape.call_args.kwargs
        self.assertEqual(kwargs["profile"], "balanced")
        self.assertEqual(kwargs["runtime_budget_minutes"], 20)
        self.assertEqual(kwargs["rotation_items"], 4)
        self.assertFalse(kwargs["sample_random"])
        self.assertFalse(kwargs["dry_plan"])
        self.assertEqual(kwargs["candidate_storage"], "db")
        self.assertEqual(kwargs["observation_policy"], "single+audit")
        self.assertIsNone(kwargs["commit_batch_size"])
        self.assertIsNone(kwargs["base_request_delay_ms"])
        self.assertIsNone(kwargs["fail_fast_min_attempts"])
        self.assertIsNone(kwargs["fail_fast_fail_ratio"])

    @patch("src.cli.setup_logging")
    @patch("src.cli.ensure_directories")
    @patch("src.cli.load_config", return_value={"logging": {}})
    @patch("src.cli.run_scrape")
    def test_scrape_passes_custom_new_flags(self, mock_run_scrape, *_mocks):
        mock_run_scrape.return_value = {
            "run_uuid": None,
            "status": "planned",
            "started_at": "2026-02-19T00:00:00Z",
            "completed_at": "2026-02-19T00:00:00Z",
            "products_planned": 10,
            "products_scraped": 0,
            "products_failed": 0,
            "products_skipped": 0,
            "errors": [],
            "plan_summary": {"profile": "full", "mandatory_count": 10, "rotation_applied": 0, "estimated_duration_seconds": 1200, "segments": {"daily_core": 10}},
            "coverage_by_segment": {},
            "budget": {"target_seconds": 1800, "estimated_seconds": 1200, "actual_seconds": 0, "within_target": True},
            "observation_policy": "single",
            "candidate_storage_mode": "off",
            "candidates_audit_path": None,
        }
        result = self.runner.invoke(
            cli,
            [
                "scrape",
                "--profile",
                "full",
                "--runtime-budget-minutes",
                "30",
                "--rotation-items",
                "6",
                "--sample-random",
                "--dry-plan",
                "--candidate-storage",
                "off",
                "--observation-policy",
                "single",
                "--commit-batch-size",
                "10",
                "--base-request-delay-ms",
                "333",
                "--fail-fast-min-attempts",
                "5",
                "--fail-fast-fail-ratio",
                "0.7",
            ],
        )
        self.assertEqual(result.exit_code, 0)

        kwargs = mock_run_scrape.call_args.kwargs
        self.assertEqual(kwargs["profile"], "full")
        self.assertEqual(kwargs["runtime_budget_minutes"], 30)
        self.assertEqual(kwargs["rotation_items"], 6)
        self.assertTrue(kwargs["sample_random"])
        self.assertTrue(kwargs["dry_plan"])
        self.assertEqual(kwargs["candidate_storage"], "off")
        self.assertEqual(kwargs["observation_policy"], "single")
        self.assertEqual(kwargs["commit_batch_size"], 10)
        self.assertEqual(kwargs["base_request_delay_ms"], 333)
        self.assertEqual(kwargs["fail_fast_min_attempts"], 5)
        self.assertEqual(kwargs["fail_fast_fail_ratio"], 0.7)


if __name__ == "__main__":
    unittest.main()
