#!/usr/bin/env python3
"""Run the daily real-data pipeline against production PostgreSQL."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import threading
from datetime import datetime
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import List


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.smoke_public_site import run_smoke  # noqa: E402
from src.config_loader import load_config  # noqa: E402


def _run_cmd(args: List[str], env: dict) -> None:
    print(f"\n>>> {' '.join(args)}")
    completed = subprocess.run(args, cwd=ROOT, env=env)
    if completed.returncode != 0:
        raise RuntimeError(f"Command failed ({completed.returncode}): {' '.join(args)}")


def _cli_cmd(config_path: str | None, *extra: str) -> List[str]:
    cmd = ["python", "-m", "src.cli"]
    if config_path:
        cmd.extend(["--config", config_path])
    cmd.extend(extra)
    return cmd


def _run_local_smoke(public_dir: Path, expected_canonical_base: str) -> int:
    class QuietHandler(SimpleHTTPRequestHandler):
        def log_message(self, format: str, *args) -> None:  # noqa: A003
            return

    previous_cwd = Path.cwd()
    os.chdir(public_dir)
    server = ThreadingHTTPServer(("127.0.0.1", 0), QuietHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        port = int(server.server_address[1])
        return run_smoke(
            base_url=f"http://127.0.0.1:{port}",
            timeout=10.0,
            expected_canonical_base=expected_canonical_base,
            strict=True,
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
        os.chdir(previous_cwd)


def _month_label(dt: datetime) -> str:
    return f"{dt.year:04d}-{dt.month:02d}"


def _shift_months(reference: datetime, delta_months: int) -> datetime:
    month_index = (reference.year * 12 + (reference.month - 1)) + delta_months
    year = month_index // 12
    month = (month_index % 12) + 1
    return datetime(year=year, month=month, day=1)


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
        help="Incremental IPC build lookback window in months (default: 2)",
    )
    parser.add_argument("--ipc-from", default=None, help="Optional fixed IPC from month (YYYY-MM)")
    parser.add_argument("--ipc-to", default=None, help="Optional fixed IPC to month (YYYY-MM)")
    args = parser.parse_args()

    db_url = str(os.getenv("DB_URL") or "").strip()
    if not db_url:
        print("ERROR: DB_URL is required for real production runs.", file=sys.stderr)
        return 2

    env = os.environ.copy()
    env["STORAGE_BACKEND"] = "postgresql"

    config = load_config(args.config)
    canonical_base = str(
        config.get("deployment", {}).get("public_base_url", "https://preciosushuaia.com")
    ).strip().rstrip("/")
    if not canonical_base:
        canonical_base = "https://preciosushuaia.com"

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
        _run_cmd(["python", "scripts/check_db_state.py", "--backend", "postgresql", "--init-db"], env)
        _run_cmd(_cli_cmd(args.config, "init"), env)
        if not args.skip_scrape:
            _run_cmd(
                _cli_cmd(
                    args.config,
                    "scrape",
                    "--basket",
                    args.basket,
                    "--backend",
                    "postgresql",
                    "--profile",
                    "full",
                    "--candidate-storage",
                    "db",
                    "--observation-policy",
                    "single+audit",
                ),
                env,
            )
        _run_cmd(_cli_cmd(args.config, "ipc-sync", "--region", "all", "--from", ipc_from, "--to", ipc_to), env)
        _run_cmd(_cli_cmd(args.config, "ipc-build", "--basket", args.basket, "--from", ipc_from, "--to", ipc_to), env)
        _run_cmd(
            _cli_cmd(
                args.config,
                "ipc-publish",
                "--basket",
                args.basket,
                "--region",
                "patagonia",
                "--from",
                ipc_from,
                "--to",
                ipc_to,
                "--skip-sync",
                "--skip-build",
            ),
            env,
        )
        _run_cmd(
            _cli_cmd(
                args.config,
                "ipc-publish",
                "--basket",
                args.basket,
                "--region",
                "nacional",
                "--from",
                ipc_from,
                "--to",
                ipc_to,
                "--skip-sync",
                "--skip-build",
            ),
            env,
        )
        _run_cmd(
            _cli_cmd(
                args.config,
                "publish-web",
                "--basket",
                args.basket,
                "--view",
                args.view,
                "--benchmark",
                args.benchmark,
                "--offline-assets",
                args.offline_assets,
            ),
            env,
        )
        _run_cmd(["python", "scripts/check_db_state.py", "--backend", "postgresql", "--require-has-data"], env)
    except Exception as exc:
        print(f"\nERROR: {exc}", file=sys.stderr)
        return 1

    if not args.skip_smoke:
        output_dir = Path(config.get("deployment", {}).get("output_dir", "public")).resolve()
        if not output_dir.exists():
            print(f"ERROR: public output directory not found: {output_dir}", file=sys.stderr)
            return 1
        smoke_code = _run_local_smoke(output_dir, expected_canonical_base=canonical_base)
        if smoke_code != 0:
            print("ERROR: local smoke validation failed.", file=sys.stderr)
            return smoke_code

    print("\nDaily real pipeline completed. Static site is ready in /public.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
