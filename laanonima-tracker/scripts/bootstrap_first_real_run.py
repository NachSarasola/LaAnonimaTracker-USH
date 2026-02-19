#!/usr/bin/env python3
"""Run first real production pipeline against a new empty DB."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path
from typing import List


ROOT = Path(__file__).resolve().parents[1]


def run_cmd(args: List[str], env: dict) -> None:
    print(f"\n>>> {' '.join(args)}")
    completed = subprocess.run(args, cwd=ROOT, env=env)
    if completed.returncode != 0:
        raise RuntimeError(f"Command failed ({completed.returncode}): {' '.join(args)}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Bootstrap first real run in production DB.")
    parser.add_argument("--skip-empty-check", action="store_true", help="Skip require-empty guard")
    parser.add_argument("--skip-scrape", action="store_true", help="Skip scrape step (not recommended)")
    args = parser.parse_args()

    db_url = str(os.getenv("DB_URL") or "").strip()
    if not db_url:
        print("ERROR: DB_URL is required.", file=sys.stderr)
        return 2

    env = os.environ.copy()
    env["STORAGE_BACKEND"] = "postgresql"

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
                    "--profile",
                    "full",
                    "--candidate-storage",
                    "db",
                    "--observation-policy",
                    "single+audit",
                ],
                env,
            )
        run_cmd(["python", "-m", "src.cli", "ipc-sync", "--region", "all"], env)
        run_cmd(["python", "-m", "src.cli", "ipc-publish", "--basket", "all", "--region", "patagonia"], env)
        run_cmd(["python", "-m", "src.cli", "ipc-publish", "--basket", "all", "--region", "nacional"], env)
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

