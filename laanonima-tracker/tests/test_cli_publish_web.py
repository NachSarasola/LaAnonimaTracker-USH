"""CLI tests for static website publish command."""

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

from click.testing import CliRunner

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.cli import cli


class TestCliPublishWeb(unittest.TestCase):
    def setUp(self):
        self.runner = CliRunner()

    @patch("src.cli.setup_logging")
    @patch("src.cli.ensure_directories")
    @patch("src.cli.load_config", return_value={"logging": {}})
    @patch("src.cli.run_web_publish")
    def test_publish_web_runs(self, mock_publish, *_mocks):
        mock_publish.return_value = {
            "status": "completed",
            "web_status": "fresh",
            "is_stale": False,
            "output_dir": "public",
            "tracker_path": "public/tracker/index.html",
            "manifest_path": "public/data/manifest.json",
            "latest_metadata_path": "public/data/latest.metadata.json",
            "history_count": 2,
            "next_update_eta": "2026-02-20 09:10:00 UTC",
        }
        result = self.runner.invoke(
            cli,
            ["publish-web", "--basket", "all", "--from", "2026-01", "--to", "2026-02", "--skip-report"],
        )
        self.assertEqual(result.exit_code, 0)
        kwargs = mock_publish.call_args.kwargs
        self.assertEqual(kwargs["basket_type"], "all")
        self.assertEqual(kwargs["from_month"], "2026-01")
        self.assertEqual(kwargs["to_month"], "2026-02")
        self.assertEqual(kwargs["analysis_depth"], "executive")
        self.assertFalse(kwargs["build_report"])

    @patch("src.cli.setup_logging")
    @patch("src.cli.ensure_directories")
    @patch("src.cli.load_config", return_value={"logging": {}})
    @patch("src.cli.run_web_publish")
    def test_publish_web_allows_view_override(self, mock_publish, *_mocks):
        mock_publish.return_value = {
            "status": "completed",
            "web_status": "fresh",
            "is_stale": False,
            "output_dir": "public",
            "tracker_path": "public/tracker/index.html",
            "manifest_path": "public/data/manifest.json",
            "latest_metadata_path": "public/data/latest.metadata.json",
            "history_count": 2,
            "next_update_eta": "2026-02-20 09:10:00 UTC",
        }
        result = self.runner.invoke(
            cli,
            ["publish-web", "--view", "analyst", "--skip-report"],
        )
        self.assertEqual(result.exit_code, 0)
        kwargs = mock_publish.call_args.kwargs
        self.assertEqual(kwargs["analysis_depth"], "analyst")


if __name__ == "__main__":
    unittest.main()
