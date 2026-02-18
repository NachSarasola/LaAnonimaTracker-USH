"""Basic CLI validation tests for report command."""

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
        self.assertIn("Formato inválido", result.output)
        self.assertIn("2026-02", result.output)
        mock_run_report.assert_not_called()


if __name__ == "__main__":
    unittest.main()
