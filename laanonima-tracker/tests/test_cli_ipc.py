"""CLI tests for new IPC commands (sync/build/publish)."""

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

from click.testing import CliRunner

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.cli import cli


class TestCliIPCCommands(unittest.TestCase):
    def setUp(self):
        self.runner = CliRunner()

    @patch("src.cli.setup_logging")
    @patch("src.cli.ensure_directories")
    @patch("src.cli.load_config", return_value={"logging": {}})
    @patch("src.cli.run_ipc_sync")
    def test_ipc_sync_runs(self, mock_sync, *_mocks):
        mock_sync.return_value = {
            "status": "completed",
            "source_mode": "auto_with_fallback",
            "source": "indec_patagonia",
            "region": "patagonia",
            "used_fallback": True,
            "fetched_rows": 10,
            "upserted_rows": 10,
            "warnings": [],
            "snapshot_path": None,
        }
        result = self.runner.invoke(cli, ["ipc-sync", "--from", "2024-01", "--to", "2024-02"])
        self.assertEqual(result.exit_code, 0)
        kwargs = mock_sync.call_args.kwargs
        self.assertEqual(kwargs["from_month"], "2024-01")
        self.assertEqual(kwargs["to_month"], "2024-02")
        self.assertIsNone(kwargs["pdf_policy"])
        self.assertFalse(kwargs["force_pdf_validation"])

    @patch("src.cli.setup_logging")
    @patch("src.cli.ensure_directories")
    @patch("src.cli.load_config", return_value={"logging": {}})
    @patch("src.cli.run_ipc_sync")
    def test_ipc_sync_passes_pdf_policy_overrides(self, mock_sync, *_mocks):
        mock_sync.return_value = {
            "status": "completed",
            "source_mode": "xls_pdf_hybrid",
            "source": "indec_patagonia",
            "region": "all",
            "used_fallback": False,
            "fetched_rows": 10,
            "upserted_rows": 10,
            "warnings": [],
            "snapshot_path": None,
        }
        result = self.runner.invoke(
            cli,
            [
                "ipc-sync",
                "--from",
                "2024-01",
                "--to",
                "2024-02",
                "--pdf-policy",
                "on_new_month",
                "--force-pdf-validation",
            ],
        )
        self.assertEqual(result.exit_code, 0)
        kwargs = mock_sync.call_args.kwargs
        self.assertEqual(kwargs["pdf_policy"], "on_new_month")
        self.assertTrue(kwargs["force_pdf_validation"])

    @patch("src.cli.setup_logging")
    @patch("src.cli.ensure_directories")
    @patch("src.cli.load_config", return_value={"logging": {}})
    @patch("src.cli.run_ipc_build")
    def test_ipc_build_runs(self, mock_build, *_mocks):
        mock_build.return_value = {
            "status": "completed",
            "basket_type": "all",
            "method_version": "v1_fixed_weight_robust_monthly",
            "from_month": "2024-01",
            "to_month": "2024-02",
            "months_processed": 2,
            "general_rows": 2,
            "category_rows": 3,
            "warnings": [],
        }
        result = self.runner.invoke(cli, ["ipc-build", "--basket", "all", "--from", "2024-01", "--to", "2024-02"])
        self.assertEqual(result.exit_code, 0)
        kwargs = mock_build.call_args.kwargs
        self.assertEqual(kwargs["basket_type"], "all")

    @patch("src.cli.setup_logging")
    @patch("src.cli.ensure_directories")
    @patch("src.cli.load_config", return_value={"logging": {}})
    @patch("src.cli.run_ipc_publish")
    def test_ipc_publish_runs(self, mock_publish, *_mocks):
        mock_publish.return_value = {
            "run_uuid": "abc",
            "status": "completed",
            "basket_type": "all",
            "region": "patagonia",
            "method_version": "v1_fixed_weight_robust_monthly",
            "from_month": "2024-01",
            "to_month": "2024-02",
            "official_rows": 5,
            "tracker_rows": 2,
            "tracker_category_rows": 3,
            "overlap_months": 2,
            "metrics": {},
            "warnings": [],
        }
        result = self.runner.invoke(cli, ["ipc-publish", "--from", "2024-01", "--to", "2024-02"])
        self.assertEqual(result.exit_code, 0)
        kwargs = mock_publish.call_args.kwargs
        self.assertEqual(kwargs["region"], "patagonia")


if __name__ == "__main__":
    unittest.main()
