#!/usr/bin/env python3
"""Shared helpers for production pipeline scripts."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
from datetime import datetime
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from time import perf_counter
from typing import Any, Dict, List, Optional
from urllib.parse import urlsplit


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.smoke_public_site import run_smoke  # noqa: E402


def build_env() -> Dict[str, str]:
    env = os.environ.copy()
    env["STORAGE_BACKEND"] = "postgresql"
    return env


def require_db_url() -> str:
    db_url = str(os.getenv("DB_URL") or "").strip()
    if not db_url:
        raise RuntimeError("DB_URL is required for production pipeline runs.")
    return db_url


def db_fingerprint(db_url: str) -> str:
    parsed = urlsplit(db_url)
    scheme = parsed.scheme or "unknown"
    host = parsed.hostname or "unknown-host"
    database = (parsed.path or "").lstrip("/") or "unknown-db"
    port = f":{parsed.port}" if parsed.port else ""
    return f"{scheme}://{host}{port}/{database}"


def run_cmd(args: List[str], env: Dict[str, str]) -> float:
    print(f"\n>>> {' '.join(args)}")
    started = perf_counter()
    completed = subprocess.run(args, cwd=ROOT, env=env)
    duration = perf_counter() - started
    if completed.returncode != 0:
        raise RuntimeError(f"Command failed ({completed.returncode}): {' '.join(args)}")
    return duration


def cli_cmd(config_path: Optional[str], *extra: str) -> List[str]:
    cmd = ["python", "-m", "src.cli"]
    if config_path:
        cmd.extend(["--config", config_path])
    cmd.extend(extra)
    return cmd


def month_label(dt: datetime) -> str:
    return f"{dt.year:04d}-{dt.month:02d}"


def shift_months(reference: datetime, delta_months: int) -> datetime:
    month_index = (reference.year * 12 + (reference.month - 1)) + delta_months
    year = month_index // 12
    month = (month_index % 12) + 1
    return datetime(year=year, month=month, day=1)


def resolve_ipc_window(
    ipc_from: Optional[str],
    ipc_to: Optional[str],
    lookback_months: int,
) -> tuple[str, str]:
    if (ipc_from and not ipc_to) or (ipc_to and not ipc_from):
        raise RuntimeError("Use --ipc-from and --ipc-to together.")
    if ipc_from and ipc_to:
        return str(ipc_from), str(ipc_to)
    now = datetime.utcnow()
    to_month = month_label(now)
    from_month = month_label(shift_months(now, -abs(int(lookback_months))))
    return from_month, to_month


def run_local_smoke(public_dir: Path, expected_canonical_base: str) -> int:
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


def stage_record(name: str, command: List[str], duration_seconds: float) -> Dict[str, Any]:
    return {
        "stage": name,
        "command": " ".join(command),
        "duration_seconds": round(duration_seconds, 3),
    }


def write_timing_payload(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def write_github_summary(payload: Dict[str, Any]) -> None:
    summary_path = str(os.getenv("GITHUB_STEP_SUMMARY") or "").strip()
    if not summary_path:
        return

    lines = [
        "## Pipeline Timing Summary",
        "",
        f"- Status: `{payload.get('status', 'unknown')}`",
        f"- Total seconds: `{payload.get('total_seconds', 0)}`",
        f"- DB fingerprint: `{payload.get('db_fingerprint', 'n/a')}`",
        f"- IPC window: `{payload.get('ipc_window', {}).get('from')}` -> `{payload.get('ipc_window', {}).get('to')}`",
        f"- Scrape fallback used: `{payload.get('scrape_fallback_used', False)}`",
        f"- Scrape block retries used: `{payload.get('scrape_block_retries_used', 0)}`",
        f"- Source block reason: `{payload.get('source_block_reason') or 'n/a'}`",
        f"- Data age hours: `{payload.get('data_age_hours') if payload.get('data_age_hours') is not None else 'n/a'}`",
        "",
        "| Stage | Seconds |",
        "|---|---:|",
    ]
    warnings = payload.get("warnings", []) or []
    if warnings:
        lines.append("")
        lines.append("### Warnings")
        for warning in warnings:
            lines.append(f"- {warning}")
        lines.append("")
    for stage in payload.get("stages", []):
        lines.append(f"| `{stage.get('stage')}` | {stage.get('duration_seconds')} |")
    lines.append("")

    with open(summary_path, "a", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
