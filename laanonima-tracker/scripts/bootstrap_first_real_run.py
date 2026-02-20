#!/usr/bin/env python3
"""Run first real production pipeline against a new empty DB."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import List


ROOT = Path(__file__).resolve().parents[1]


def run_cmd(args: List[str], env: dict) -> None:
    print(f"\n>>> {' '.join(args)}")
    completed = subprocess.run(args, cwd=ROOT, env=env)
    if completed.returncode != 0:
        raise RuntimeError(f"Command failed ({completed.returncode}): {' '.join(args)}")


def _month_label(dt: datetime) -> str:
    return f"{dt.year:04d}-{dt.month:02d}"


def _shift_months(reference: datetime, delta_months: int) -> datetime:
    month_index = (reference.year * 12 + (reference.month - 1)) + delta_months
    year = month_index // 12
    month = (month_index % 12) + 1
    return datetime(year=year, month=month, day=1)


def main() -> int:
    parser = argparse.ArgumentParser(description="Bootstrap first real run in production DB.")
    parser.add_argument("--skip-empty-check", action="store_true", help="Skip require-empty guard")
    parser.add_argument("--skip-scrape", action="store_true", help="Skip scrape step (not recommended)")
    parser.add_argument(
        "--ipc-lookback-months",
        type=int,
        default=2,
        help="Incremental IPC build lookback window in months (default: 2)",
    )
    parser.add_argument("--ipc-from", default=None, help="Optional fixed IPC from month (YYYY-MM)")
    parser.add_argument("--ipc-to", default=None, help="Optional fixed IPC to month (YYYY-MM)")
    args = parser.parse_args()

    db_url = str(os.getenv("DB_URL") or "").strip()
    if not db_url:
        print("ERROR: DB_URL is required.", file=sys.stderr)
        return 2

    env = os.environ.copy()
    env["STORAGE_BACKEND"] = "postgresql"

    if (args.ipc_from and not args.ipc_to) or (args.ipc_to and not args.ipc_from):
        print("ERROR: use --ipc-from and --ipc-to together.", file=sys.stderr)
        return 2

    if args.ipc_from and args.ipc_to:
        ipc_from = str(args.ipc_from)
        ipc_to = str(args.ipc_to)
    else:
        now = datetime.utcnow()
        ipc_to = _month_label(now)
        ipc_from = _month_label(_shift_months(now, -abs(int(args.ipc_lookback_months))))

    try:
        run_cmd(["python", "scripts/check_db_state.py", "--backend", "postgresql", "--init-db"], env)
        if not args.skip_empty_check:
            run_cmd(["python", "scripts/check_db_state.py", "--backend", "postgresql", "--require-empty"], env)

        run_cmd(["python", "-m", "src.cli", "init"], env)
        if not args.skip_scrape:
            run_cmd(
                [
                    "python",
                    "-m",
                    "src.cli",
                    "scrape",
                    "--basket",
                    "all",
                    "--backend",
                    "postgresql",
                    "--profile",
                    "full",
                    "--candidate-storage",
                    "db",
                    "--observation-policy",
                    "single+audit",
                ],
                env,
            )
        run_cmd(
            ["python", "-m", "src.cli", "ipc-sync", "--region", "all", "--from", ipc_from, "--to", ipc_to],
            env,
        )
        run_cmd(
            ["python", "-m", "src.cli", "ipc-build", "--basket", "all", "--from", ipc_from, "--to", ipc_to],
            env,
        )
        run_cmd(
            [
                "python",
                "-m",
                "src.cli",
                "ipc-publish",
                "--basket",
                "all",
                "--region",
                "patagonia",
                "--from",
                ipc_from,
                "--to",
                ipc_to,
                "--skip-sync",
                "--skip-build",
            ],
            env,
        )
        run_cmd(
            [
                "python",
                "-m",
                "src.cli",
                "ipc-publish",
                "--basket",
                "all",
                "--region",
                "nacional",
                "--from",
                ipc_from,
                "--to",
                ipc_to,
                "--skip-sync",
                "--skip-build",
            ],
            env,
        )
        run_cmd(
            [
                "python",
                "-m",
                "src.cli",
                "publish-web",
                "--basket",
                "all",
                "--view",
                "analyst",
                "--benchmark",
                "ipc",
                "--offline-assets",
                "external",
            ],
            env,
        )
        run_cmd(["python", "scripts/check_db_state.py", "--backend", "postgresql", "--require-has-data"], env)
    except Exception as exc:
        print(f"\nERROR: {exc}", file=sys.stderr)
        return 1

    print("\nBootstrap complete. Public site is ready to deploy from /public.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
