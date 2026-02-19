#!/usr/bin/env python3
"""Validate production launch configuration for preciosushuaia.com."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import List

from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.config_loader import load_config
from src.models import get_engine


def _is_placeholder_client_id(value: str) -> bool:
    client_id = value.strip().lower()
    if not client_id:
        return True
    if "xxxxxxxx" in client_id:
        return True
    return client_id in {"ca-pub-test", "ca-pub-0000000000000000"}


def main() -> int:
    parser = argparse.ArgumentParser(description="Production preflight validation.")
    parser.add_argument("--config", default=None, help="Optional config path")
    parser.add_argument(
        "--expected-base-url",
        default="https://preciosushuaia.com",
        help="Expected canonical public base URL",
    )
    parser.add_argument(
        "--allow-other-base-url",
        action="store_true",
        help="Do not fail if public_base_url differs from expected base URL",
    )
    args = parser.parse_args()

    cfg = load_config(args.config)
    errors: List[str] = []
    warnings: List[str] = []

    deployment = cfg.get("deployment", {}) if isinstance(cfg.get("deployment"), dict) else {}
    public_base_url = str(deployment.get("public_base_url") or "").strip().rstrip("/")
    expected_base_url = str(args.expected_base_url or "").strip().rstrip("/")
    contact_email = str(deployment.get("contact_email") or "").strip()

    backend = str(os.getenv("STORAGE_BACKEND") or cfg.get("storage", {}).get("default_backend", "sqlite")).strip().lower()
    db_url = str(os.getenv("DB_URL") or cfg.get("storage", {}).get("postgresql", {}).get("url") or "").strip()

    ads_cfg = cfg.get("ads", {}) if isinstance(cfg.get("ads"), dict) else {}
    ads_enabled = bool(ads_cfg.get("enabled", False))
    ads_client_id = str(ads_cfg.get("client_id") or ads_cfg.get("client_id_placeholder") or "").strip()

    plausible = cfg.get("analytics", {}).get("plausible", {})
    plausible = plausible if isinstance(plausible, dict) else {}
    plausible_enabled = bool(plausible.get("enabled", False))
    plausible_domain = str(plausible.get("domain") or "").strip()

    print("Prelaunch configuration check")
    print(f"- public_base_url={public_base_url or 'N/D'}")
    print(f"- expected_base_url={expected_base_url or 'N/D'}")
    print(f"- storage_backend={backend or 'N/D'}")
    print(f"- db_url_set={'yes' if db_url else 'no'}")
    print(f"- ads_enabled={'yes' if ads_enabled else 'no'}")
    print(f"- plausible_enabled={'yes' if plausible_enabled else 'no'}")

    if not public_base_url:
        errors.append("deployment.public_base_url is empty.")
    elif not args.allow_other_base_url and expected_base_url and public_base_url != expected_base_url:
        errors.append(
            f"deployment.public_base_url ({public_base_url}) does not match expected base URL ({expected_base_url})."
        )

    if not contact_email or "@" not in contact_email:
        errors.append("deployment.contact_email is missing or invalid.")

    if backend != "postgresql":
        errors.append(f"storage backend must be postgresql in production; current={backend}.")

    if not db_url:
        errors.append("DB_URL is required for production.")

    if ads_enabled and _is_placeholder_client_id(ads_client_id):
        errors.append("ads enabled but ADSENSE client_id is placeholder/missing.")

    if plausible_enabled and not plausible_domain:
        errors.append("analytics plausible enabled but domain is empty.")

    if not ads_enabled:
        warnings.append("ads are disabled (acceptable, but monetization is off).")
    if not plausible_enabled:
        warnings.append("analytics are disabled (acceptable, but no usage telemetry).")

    if db_url and backend == "postgresql":
        try:
            engine = get_engine(cfg, backend="postgresql")
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            engine.dispose()
            print("- db_connectivity=ok")
        except Exception as exc:
            errors.append(f"unable to connect to PostgreSQL DB_URL: {exc}")

    if warnings:
        for warning in warnings:
            print(f"WARN: {warning}")

    if errors:
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1

    print("Prelaunch check OK.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
