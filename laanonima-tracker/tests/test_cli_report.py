"""CLI validation tests for report/app commands."""

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

from click.testing import CliRunner

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.cli import cli


class TestCliReportMonthValidation(unittest.TestCase):
    def setUp(self):
        self.runner = CliRunner()

    @patch("src.cli.setup_logging")
    @patch("src.cli.ensure_directories")
    @patch("src.cli.load_config", return_value={"logging": {}})
    @patch("src.cli.run_report")
    def test_report_accepts_valid_month_range(self, mock_run_report, *_mocks):
        mock_run_report.return_value = {
            "inflation_total_pct": 1.23,
            "has_data": True,
            "kpis": {},
            "data_quality": {},
            "artifacts": {
                "html_path": "report.html",
                "metadata_path": "report.json",
            },
        }

        result = self.runner.invoke(
            cli,
            [
                "report",
                "--from",
                "2026-01",
                "--to",
                "2026-02",
            ],
        )

        self.assertEqual(result.exit_code, 0)
        mock_run_report.assert_called_once()

    @patch("src.cli.setup_logging")
    @patch("src.cli.ensure_directories")
    @patch("src.cli.load_config", return_value={"logging": {}})
    @patch("src.cli.run_report")
    def test_report_rejects_invalid_from_month(self, mock_run_report, *_mocks):
        result = self.runner.invoke(
            cli,
            [
                "report",
                "--from",
                "2026/01",
                "--to",
                "2026-02",
            ],
        )

        self.assertEqual(result.exit_code, 2)
        self.assertIn("Formato inv", result.output)
        self.assertIn("2026-02", result.output)
        mock_run_report.assert_not_called()

    @patch("src.cli.setup_logging")
    @patch("src.cli.ensure_directories")
    @patch("src.cli.load_config", return_value={"logging": {}})
    @patch("src.cli.run_report")
    def test_app_uses_auto_range_and_new_defaults(self, mock_run_report, *_mocks):
        mock_run_report.return_value = {
            "from_month": "2025-09",
            "to_month": "2026-02",
            "has_data": False,
            "kpis": {},
            "artifacts": {
                "html_path": "report.html",
                "metadata_path": "report.json",
            },
        }
        result = self.runner.invoke(cli, ["app"])
        self.assertEqual(result.exit_code, 0)
        kwargs = mock_run_report.call_args.kwargs
        self.assertIsNone(kwargs["from_month"])
        self.assertIsNone(kwargs["to_month"])
        self.assertEqual(kwargs["benchmark_mode"], "ipc")
        self.assertEqual(kwargs["analysis_depth"], "executive")
        self.assertEqual(kwargs["offline_assets"], "embed")

    @patch("src.cli.setup_logging")
    @patch("src.cli.ensure_directories")
    @patch("src.cli.load_config", return_value={"logging": {}})
    @patch("src.cli.run_report")
    def test_report_passes_new_flags(self, mock_run_report, *_mocks):
        mock_run_report.return_value = {
            "inflation_total_pct": None,
            "has_data": False,
            "kpis": {},
            "data_quality": {},
            "artifacts": {"html_path": "r.html", "metadata_path": "r.json"},
        }
        result = self.runner.invoke(
            cli,
            [
                "report",
                "--from",
                "2025-10",
                "--to",
                "2026-01",
                "--benchmark",
                "none",
                "--view",
                "analyst",
                "--offline-assets",
                "external",
            ],
        )
        self.assertEqual(result.exit_code, 0)
        kwargs = mock_run_report.call_args.kwargs
        self.assertEqual(kwargs["benchmark_mode"], "none")
        self.assertEqual(kwargs["analysis_depth"], "analyst")
        self.assertEqual(kwargs["offline_assets"], "external")

    @patch("src.cli.setup_logging")
    @patch("src.cli.ensure_directories")
    @patch("src.cli.load_config", return_value={"logging": {}})
    @patch("src.cli.run_report")
    def test_app_rejects_incomplete_month_args(self, mock_run_report, *_mocks):
        result = self.runner.invoke(cli, ["app", "--from", "2026-01"])
        self.assertEqual(result.exit_code, 2)
        mock_run_report.assert_not_called()


if __name__ == "__main__":
    unittest.main()
