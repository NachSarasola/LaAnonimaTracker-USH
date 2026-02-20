#!/usr/bin/env python3
"""Wrapper for daily production pipeline execution."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from typing import List


ROOT = Path(__file__).resolve().parents[1]


def _run(command: List[str]) -> int:
    print(f"\n>>> {' '.join(command)}")
    completed = subprocess.run(command, cwd=ROOT)
    return int(completed.returncode)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run daily real-price pipeline for production website.")
    parser.add_argument("--config", default=None, help="Optional config.yaml path")
    parser.add_argument("--basket", default="all", help="Basket type for scrape/publish (default: all)")
    parser.add_argument("--view", default="analyst", help="Public report view (default: analyst)")
    parser.add_argument("--benchmark", default="ipc", help="Benchmark mode for report (default: ipc)")
    parser.add_argument(
        "--offline-assets",
        default="external",
        choices=["embed", "external"],
        help="Asset mode for public report (default: external)",
    )
    parser.add_argument("--skip-scrape", action="store_true", help="Skip scrape step")
    parser.add_argument("--skip-smoke", action="store_true", help="Skip local HTTP smoke test")
    parser.add_argument(
        "--ipc-lookback-months",
        type=int,
        default=2,
        help="Incremental IPC lookback window in months (default: 2)",
    )
    parser.add_argument("--ipc-from", default=None, help="Optional fixed IPC from month (YYYY-MM)")
    parser.add_argument("--ipc-to", default=None, help="Optional fixed IPC to month (YYYY-MM)")
    parser.add_argument(
        "--pdf-policy",
        choices=["always", "on_new_month", "never"],
        default="on_new_month",
        help="PDF validation policy for IPC sync (default: on_new_month)",
    )
    parser.add_argument("--force-pdf-validation", action="store_true", help="Force PDF validation")
    args = parser.parse_args()

    cmd = ["python", "scripts/run_data_pipeline.py"]
    if args.config:
        cmd.extend(["--config", args.config])
    cmd.extend(
        [
            "--basket",
            args.basket,
            "--view",
            args.view,
            "--benchmark",
            args.benchmark,
            "--offline-assets",
            args.offline_assets,
            "--ipc-lookback-months",
            str(args.ipc_lookback_months),
            "--pdf-policy",
            args.pdf_policy,
        ]
    )
    if args.skip_scrape:
        cmd.append("--skip-scrape")
    if args.skip_smoke:
        cmd.append("--skip-smoke")
    if args.ipc_from and args.ipc_to:
        cmd.extend(["--ipc-from", args.ipc_from, "--ipc-to", args.ipc_to])
    if args.force_pdf_validation:
        cmd.append("--force-pdf-validation")

    return _run(cmd)


if __name__ == "__main__":
    sys.exit(main())
