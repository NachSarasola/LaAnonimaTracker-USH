#!/usr/bin/env python3
"""Unified production data pipeline runner with timing telemetry."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from typing import Any, Dict, List

from scripts.pipeline_common import (
    ROOT,
    build_env,
    cli_cmd,
    db_fingerprint,
    require_db_url,
    resolve_ipc_window,
    run_cmd,
    run_local_smoke,
    stage_record,
    write_github_summary,
    write_timing_payload,
)
from src.config_loader import load_config


def _run_stage(stages: List[Dict[str, Any]], name: str, command: List[str], env: Dict[str, str]) -> None:
    duration = run_cmd(command, env)
    stages.append(stage_record(name=name, command=command, duration_seconds=duration))


def main() -> int:
    parser = argparse.ArgumentParser(description="Run unified production pipeline (scrape + IPC + publish + smoke).")
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
        "--require-empty",
        action="store_true",
        help="Require an empty production DB before continuing (bootstrap guard)",
    )
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
    parser.add_argument(
        "--force-pdf-validation",
        action="store_true",
        help="Force PDF validation during IPC sync",
    )
    parser.add_argument("--commit-batch-size", type=int, default=None, help="Override scrape commit batch size")
    parser.add_argument("--base-request-delay-ms", type=int, default=None, help="Override scrape delay")
    parser.add_argument("--fail-fast-min-attempts", type=int, default=None, help="Override scrape fail-fast min attempts")
    parser.add_argument("--fail-fast-fail-ratio", type=float, default=None, help="Override scrape fail-fast ratio")
    parser.add_argument(
        "--timing-output",
        default="data/analysis/pipeline_timing_latest.json",
        help="Path for pipeline timing JSON",
    )
    args = parser.parse_args()

    env = build_env()
    stages: List[Dict[str, Any]] = []
    total_started = perf_counter()
    status = "completed"
    failure_message = ""
    failed_stage = ""

    try:
        db_url = require_db_url()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    config = load_config(args.config)
    canonical_base = str(
        config.get("deployment", {}).get("public_base_url", "https://preciosushuaia.com")
    ).strip().rstrip("/")
    if not canonical_base:
        canonical_base = "https://preciosushuaia.com"

    try:
        ipc_from, ipc_to = resolve_ipc_window(
            ipc_from=args.ipc_from,
            ipc_to=args.ipc_to,
            lookback_months=args.ipc_lookback_months,
        )
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    try:
        failed_stage = "init-db-schema"
        _run_stage(
            stages,
            name=failed_stage,
            command=["python", "scripts/check_db_state.py", "--backend", "postgresql", "--init-db"],
            env=env,
        )

        if args.require_empty:
            failed_stage = "require-empty-db"
            _run_stage(
                stages,
                name=failed_stage,
                command=["python", "scripts/check_db_state.py", "--backend", "postgresql", "--require-empty"],
                env=env,
            )

        failed_stage = "cli-init"
        _run_stage(stages, name=failed_stage, command=cli_cmd(args.config, "init"), env=env)

        if not args.skip_scrape:
            scrape_cmd = cli_cmd(
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
            )
            if args.commit_batch_size is not None:
                scrape_cmd.extend(["--commit-batch-size", str(args.commit_batch_size)])
            if args.base_request_delay_ms is not None:
                scrape_cmd.extend(["--base-request-delay-ms", str(args.base_request_delay_ms)])
            if args.fail_fast_min_attempts is not None:
                scrape_cmd.extend(["--fail-fast-min-attempts", str(args.fail_fast_min_attempts)])
            if args.fail_fast_fail_ratio is not None:
                scrape_cmd.extend(["--fail-fast-fail-ratio", str(args.fail_fast_fail_ratio)])

            failed_stage = "scrape-full"
            _run_stage(stages, name=failed_stage, command=scrape_cmd, env=env)

        failed_stage = "quality-gate-has-data"
        _run_stage(
            stages,
            name=failed_stage,
            command=["python", "scripts/check_db_state.py", "--backend", "postgresql", "--require-has-data"],
            env=env,
        )

        ipc_sync_cmd = cli_cmd(
            args.config,
            "ipc-sync",
            "--region",
            "all",
            "--from",
            ipc_from,
            "--to",
            ipc_to,
            "--pdf-policy",
            args.pdf_policy,
        )
        if args.force_pdf_validation:
            ipc_sync_cmd.append("--force-pdf-validation")
        failed_stage = "ipc-sync"
        _run_stage(stages, name=failed_stage, command=ipc_sync_cmd, env=env)

        failed_stage = "ipc-build"
        _run_stage(
            stages,
            name=failed_stage,
            command=cli_cmd(args.config, "ipc-build", "--basket", args.basket, "--from", ipc_from, "--to", ipc_to),
            env=env,
        )

        failed_stage = "ipc-publish-patagonia"
        _run_stage(
            stages,
            name=failed_stage,
            command=cli_cmd(
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
            env=env,
        )

        failed_stage = "ipc-publish-nacional"
        _run_stage(
            stages,
            name=failed_stage,
            command=cli_cmd(
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
            env=env,
        )

        failed_stage = "publish-web"
        _run_stage(
            stages,
            name=failed_stage,
            command=cli_cmd(
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
            env=env,
        )

        if not args.skip_smoke:
            failed_stage = "smoke-public-site"
            smoke_started = perf_counter()
            output_dir = Path(config.get("deployment", {}).get("output_dir", "public")).resolve()
            if not output_dir.exists():
                raise RuntimeError(f"Public output directory not found: {output_dir}")
            smoke_code = run_local_smoke(output_dir, expected_canonical_base=canonical_base)
            if smoke_code != 0:
                raise RuntimeError("Local smoke validation failed.")
            smoke_seconds = perf_counter() - smoke_started
            stages.append(stage_record(name=failed_stage, command=["local-smoke"], duration_seconds=smoke_seconds))

    except Exception as exc:
        status = "failed"
        failure_message = str(exc)
        print(f"\nERROR at stage '{failed_stage}': {exc}", file=sys.stderr)

    total_seconds = round(perf_counter() - total_started, 3)
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "failed_stage": failed_stage if status == "failed" else "",
        "error": failure_message if status == "failed" else "",
        "total_seconds": total_seconds,
        "db_fingerprint": db_fingerprint(db_url),
        "ipc_window": {"from": ipc_from, "to": ipc_to},
        "stages": stages,
    }
    timing_path = (ROOT / str(args.timing_output)).resolve()
    write_timing_payload(timing_path, payload)
    write_github_summary(payload)

    if status != "completed":
        return 1

    print("\nUnified production pipeline completed successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
