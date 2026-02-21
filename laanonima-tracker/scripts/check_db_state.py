#!/usr/bin/env python3
"""Validate production DB state for launch and daily operations."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional

from sqlalchemy import func

# Allow execution via `python scripts/check_db_state.py` from repo root.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.config_loader import load_config
from src.models import Price, PriceCandidate, ScrapeRun, get_engine, get_session_factory, init_db


def _to_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _compute_state(config_path: Optional[str], backend: Optional[str], ensure_schema: bool) -> Dict[str, object]:
    config = load_config(config_path)
    engine = get_engine(config, backend=backend)
    if ensure_schema:
        init_db(engine)
    session = get_session_factory(engine)()
    try:
        prices_count = int(session.query(func.count(Price.id)).scalar() or 0)
        candidates_count = int(session.query(func.count(PriceCandidate.id)).scalar() or 0)
        runs_count = int(session.query(func.count(ScrapeRun.id)).scalar() or 0)

        latest_scraped = session.query(func.max(Price.scraped_at)).scalar()
        latest_run_started = session.query(func.max(ScrapeRun.started_at)).scalar()
        latest_scraped_utc = _to_utc(latest_scraped)
        latest_run_started_utc = _to_utc(latest_run_started)

        latest_scraped_age_hours = None
        if latest_scraped_utc is not None:
            latest_scraped_age_hours = round(
                max(0.0, (datetime.now(timezone.utc) - latest_scraped_utc).total_seconds()) / 3600.0,
                3,
            )

        return {
            "prices_count": prices_count,
            "price_candidates_count": candidates_count,
            "scrape_runs_count": runs_count,
            "latest_scraped_at": latest_scraped_utc.isoformat() if latest_scraped_utc is not None else None,
            "latest_run_started_at": latest_run_started_utc.isoformat() if latest_run_started_utc is not None else None,
            "latest_scraped_age_hours": latest_scraped_age_hours,
            "is_empty_for_bootstrap": prices_count == 0 and runs_count == 0 and candidates_count == 0,
        }
    finally:
        session.close()
        engine.dispose()


def main() -> int:
    parser = argparse.ArgumentParser(description="Check DB state for production launch.")
    parser.add_argument("--config", default=None, help="Optional config path")
    parser.add_argument(
        "--backend",
        default=None,
        choices=["sqlite", "postgresql"],
        help="Override backend for this check",
    )
    parser.add_argument(
        "--init-db",
        action="store_true",
        help="Ensure schema exists before counting rows",
    )
    parser.add_argument(
        "--require-empty",
        action="store_true",
        help="Fail if DB already has historical data (for first production bootstrap only)",
    )
    parser.add_argument(
        "--require-has-data",
        action="store_true",
        help="Fail if DB has no price rows (post-scrape safety check)",
    )
    parser.add_argument(
        "--require-fresh-max-age-hours",
        type=int,
        default=None,
        help="Fail if latest scraped row is older than this number of hours.",
    )
    parser.add_argument(
        "--require-min-prices",
        type=int,
        default=1,
        help="Minimum price rows required when using --require-fresh-max-age-hours (default: 1).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        dest="as_json",
        help="Print state as JSON",
    )
    args = parser.parse_args()

    state = _compute_state(
        config_path=args.config,
        backend=args.backend,
        ensure_schema=args.init_db,
    )

    if args.as_json:
        print(json.dumps(state, ensure_ascii=False, indent=2))
    else:
        print(f"prices={state['prices_count']}")
        print(f"price_candidates={state['price_candidates_count']}")
        print(f"scrape_runs={state['scrape_runs_count']}")
        print(f"latest_scraped_at={state['latest_scraped_at'] or 'N/D'}")
        print(f"latest_scraped_age_hours={state.get('latest_scraped_age_hours', 'N/D')}")
        print(f"latest_run_started_at={state['latest_run_started_at'] or 'N/D'}")

    if args.require_empty and not bool(state["is_empty_for_bootstrap"]):
        print("ERROR: Production DB is not empty. Use a new DB for real-only launch.", file=sys.stderr)
        return 1

    if args.require_has_data and int(state["prices_count"]) <= 0:
        print("ERROR: No price rows found after pipeline run.", file=sys.stderr)
        return 1

    if args.require_fresh_max_age_hours is not None:
        min_prices = max(0, int(args.require_min_prices))
        prices_count = int(state.get("prices_count") or 0)
        max_age_hours = max(0, int(args.require_fresh_max_age_hours))
        latest_age = state.get("latest_scraped_age_hours")

        if prices_count < min_prices:
            print(
                f"ERROR: Freshness gate failed: prices_count={prices_count} < require_min_prices={min_prices}.",
                file=sys.stderr,
            )
            return 1
        if latest_age is None:
            print("ERROR: Freshness gate failed: latest_scraped_at is not available.", file=sys.stderr)
            return 1
        if float(latest_age) > float(max_age_hours):
            print(
                "ERROR: Freshness gate failed: "
                f"latest_scraped_age_hours={latest_age} > require_fresh_max_age_hours={max_age_hours}.",
                file=sys.stderr,
            )
            return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
