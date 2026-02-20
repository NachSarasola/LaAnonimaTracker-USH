"""Monthly IPC publication orchestration (official sync + tracker build + audit)."""

from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import pandas as pd
from loguru import logger
from sqlalchemy import func
from sqlalchemy.orm import Session

from src.config_loader import load_config
from src.ipc_official import sync_official_cpi
from src.ipc_tracker import TrackerIPCBuilder
from src.models import (
    IPCPublicationRun,
    OfficialCPIMonthly,
    TrackerIPCCategoryMonthly,
    TrackerIPCMonthly,
    get_engine,
    get_session_factory,
    init_db,
)


@dataclass
class PublicationSummary:
    run_uuid: str
    status: str
    basket_type: str
    region: str
    method_version: str
    from_month: Optional[str]
    to_month: Optional[str]
    official_rows: int
    tracker_rows: int
    tracker_category_rows: int
    overlap_months: int
    metrics: Dict[str, Any]
    warnings: list[str]


def _rebased_index_map(rows: list[dict[str, Any]], index_key: str) -> dict[str, float]:
    values = [(str(r["year_month"]), r.get(index_key)) for r in rows if r.get(index_key) is not None]
    if not values:
        return {}
    base_val = values[0][1]
    if base_val is None or base_val == 0:
        return {}
    return {month: (float(idx) / float(base_val)) * 100.0 for month, idx in values}


def _compute_comparison_metrics(
    session: Session,
    source_code: str,
    region: str,
    basket_type: str,
    method_version: str,
    from_month: Optional[str],
    to_month: Optional[str],
) -> Dict[str, Any]:
    tracker_q = (
        session.query(
            TrackerIPCMonthly.year_month,
            TrackerIPCMonthly.index_value,
            TrackerIPCMonthly.mom_change,
        )
        .filter(TrackerIPCMonthly.basket_type == basket_type)
        .filter(TrackerIPCMonthly.method_version == method_version)
    )
    official_q = (
        session.query(
            OfficialCPIMonthly.year_month,
            OfficialCPIMonthly.index_value,
            OfficialCPIMonthly.mom_change,
            OfficialCPIMonthly.metric_code,
        )
        .filter(OfficialCPIMonthly.source == source_code)
        .filter(OfficialCPIMonthly.region == region)
        .filter(OfficialCPIMonthly.metric_code == "general")
    )
    if from_month:
        tracker_q = tracker_q.filter(TrackerIPCMonthly.year_month >= from_month)
        official_q = official_q.filter(OfficialCPIMonthly.year_month >= from_month)
    if to_month:
        tracker_q = tracker_q.filter(TrackerIPCMonthly.year_month <= to_month)
        official_q = official_q.filter(OfficialCPIMonthly.year_month <= to_month)

    tracker_rows = [dict(r._mapping) for r in tracker_q.order_by(TrackerIPCMonthly.year_month.asc()).all()]
    official_rows = [dict(r._mapping) for r in official_q.order_by(OfficialCPIMonthly.year_month.asc()).all()]
    if not tracker_rows or not official_rows:
        return {
            "overlap_months": 0,
            "mae_base100": None,
            "rmse_base100": None,
            "mae_mom": None,
            "rmse_mom": None,
        }

    tracker_rebased = _rebased_index_map(tracker_rows, "index_value")
    official_rebased = _rebased_index_map(official_rows, "index_value")
    overlap_months = sorted(set(tracker_rebased.keys()) & set(official_rebased.keys()))
    if not overlap_months:
        return {
            "overlap_months": 0,
            "mae_base100": None,
            "rmse_base100": None,
            "mae_mom": None,
            "rmse_mom": None,
        }

    diffs = [tracker_rebased[m] - official_rebased[m] for m in overlap_months]
    mae_base100 = float(pd.Series(diffs).abs().mean()) if diffs else None
    rmse_base100 = float((pd.Series(diffs).pow(2).mean()) ** 0.5) if diffs else None

    tracker_mom = {str(r["year_month"]): r.get("mom_change") for r in tracker_rows if r.get("mom_change") is not None}
    official_mom = {str(r["year_month"]): r.get("mom_change") for r in official_rows if r.get("mom_change") is not None}
    overlap_mom = sorted(set(tracker_mom.keys()) & set(official_mom.keys()))
    mom_diffs = [float(tracker_mom[m]) - float(official_mom[m]) for m in overlap_mom]
    mae_mom = float(pd.Series(mom_diffs).abs().mean()) if mom_diffs else None
    rmse_mom = float((pd.Series(mom_diffs).pow(2).mean()) ** 0.5) if mom_diffs else None

    return {
        "overlap_months": len(overlap_months),
        "mae_base100": mae_base100,
        "rmse_base100": rmse_base100,
        "mae_mom": mae_mom,
        "rmse_mom": rmse_mom,
    }


def _count_existing_official_rows(
    session: Session,
    source_code: str,
    region: str,
    from_month: Optional[str],
    to_month: Optional[str],
) -> int:
    query = (
        session.query(func.count(OfficialCPIMonthly.id))
        .filter(OfficialCPIMonthly.source == source_code)
        .filter(OfficialCPIMonthly.region == region)
        .filter(OfficialCPIMonthly.metric_code == "general")
    )
    if from_month:
        query = query.filter(OfficialCPIMonthly.year_month >= from_month)
    if to_month:
        query = query.filter(OfficialCPIMonthly.year_month <= to_month)
    return int(query.scalar() or 0)


def _summarize_existing_tracker_rows(
    session: Session,
    basket_type: str,
    method_version: str,
    from_month: Optional[str],
    to_month: Optional[str],
) -> Dict[str, Optional[Any]]:
    base_query = (
        session.query(TrackerIPCMonthly)
        .filter(TrackerIPCMonthly.basket_type == basket_type)
        .filter(TrackerIPCMonthly.method_version == method_version)
    )
    if from_month:
        base_query = base_query.filter(TrackerIPCMonthly.year_month >= from_month)
    if to_month:
        base_query = base_query.filter(TrackerIPCMonthly.year_month <= to_month)

    general_rows = int(base_query.count())
    min_month = base_query.with_entities(func.min(TrackerIPCMonthly.year_month)).scalar()
    max_month = base_query.with_entities(func.max(TrackerIPCMonthly.year_month)).scalar()

    category_query = (
        session.query(func.count(TrackerIPCCategoryMonthly.id))
        .filter(TrackerIPCCategoryMonthly.basket_type == basket_type)
        .filter(TrackerIPCCategoryMonthly.method_version == method_version)
    )
    if from_month:
        category_query = category_query.filter(TrackerIPCCategoryMonthly.year_month >= from_month)
    if to_month:
        category_query = category_query.filter(TrackerIPCCategoryMonthly.year_month <= to_month)

    return {
        "general_rows": general_rows,
        "category_rows": int(category_query.scalar() or 0),
        "from_month": str(min_month) if min_month is not None else from_month,
        "to_month": str(max_month) if max_month is not None else to_month,
    }


def publish_ipc(
    config: Dict[str, Any],
    session: Session,
    basket_type: str = "all",
    from_month: Optional[str] = None,
    to_month: Optional[str] = None,
    region: Optional[str] = None,
    skip_sync: bool = False,
    skip_build: bool = False,
) -> PublicationSummary:
    """Execute monthly publication pipeline and audit the run."""
    ipc_official_cfg = config.get("analysis", {}).get("ipc_official", {})
    region_name = str(region or ipc_official_cfg.get("region_default", "patagonia"))
    source_code = str(ipc_official_cfg.get("source_code", "indec_patagonia"))

    run_uuid = str(uuid.uuid4())
    method_version = str(
        config.get("analysis", {}).get("ipc_tracker", {}).get("method_version", "v1_fixed_weight_robust_monthly")
    )
    run = IPCPublicationRun(
        run_uuid=run_uuid,
        basket_type=basket_type,
        region=region_name,
        method_version=method_version,
        from_month=from_month,
        to_month=to_month,
        status="running",
        started_at=datetime.now(timezone.utc),
    )
    session.add(run)
    session.commit()

    warnings: list[str] = []
    status = "completed"
    official_rows = 0
    tracker_rows = 0
    tracker_category_rows = 0
    metrics: Dict[str, Any] = {}
    tracker_method_version = method_version
    tracker_from_month = from_month
    tracker_to_month = to_month
    official_source_effective = "not_run"
    official_validation_status = "not_run"
    official_source_document_url: Optional[str] = None
    official_regions_synced: list[str] = [region_name]
    official_snapshot_paths: list[str] = []

    try:
        if skip_sync:
            official_rows = _count_existing_official_rows(
                session=session,
                source_code=source_code,
                region=region_name,
                from_month=from_month,
                to_month=to_month,
            )
            official_source_effective = "existing_rows"
        else:
            official_result = sync_official_cpi(
                config=config,
                session=session,
                from_month=from_month,
                to_month=to_month,
                region="all",
            )
            official_rows = int(official_result.upserted_rows)
            warnings.extend(official_result.warnings)
            official_source_effective = official_result.official_source
            official_validation_status = official_result.validation_status
            official_source_document_url = official_result.source_document_url
            official_regions_synced = official_result.regions
            official_snapshot_paths = official_result.snapshot_paths

        if skip_build:
            tracker_summary = _summarize_existing_tracker_rows(
                session=session,
                basket_type=basket_type,
                method_version=method_version,
                from_month=from_month,
                to_month=to_month,
            )
            tracker_rows = int(tracker_summary.get("general_rows") or 0)
            tracker_category_rows = int(tracker_summary.get("category_rows") or 0)
            tracker_from_month = tracker_summary.get("from_month") or from_month
            tracker_to_month = tracker_summary.get("to_month") or to_month
        else:
            tracker_builder = TrackerIPCBuilder(config=config, session=session)
            try:
                tracker_result = tracker_builder.build(
                    basket_type=basket_type,
                    from_month=from_month,
                    to_month=to_month,
                )
            finally:
                tracker_builder.close()
            tracker_rows = tracker_result.general_rows
            tracker_category_rows = tracker_result.category_rows
            warnings.extend(tracker_result.warnings)
            tracker_method_version = tracker_result.method_version
            tracker_from_month = tracker_result.from_month
            tracker_to_month = tracker_result.to_month

        metrics = _compute_comparison_metrics(
            session=session,
            source_code=source_code,
            region=region_name,
            basket_type=basket_type,
            method_version=tracker_method_version,
            from_month=from_month,
            to_month=to_month,
        )
        metrics["official_source_effective"] = official_source_effective
        metrics["official_validation_status"] = official_validation_status
        metrics["official_source_document_url"] = official_source_document_url
        metrics["official_regions_synced"] = official_regions_synced
        metrics["official_snapshot_paths"] = official_snapshot_paths
        metrics["official_sync_mode"] = "skipped" if skip_sync else "executed"
        metrics["tracker_build_mode"] = "skipped" if skip_build else "executed"

        if official_rows == 0 and tracker_rows > 0:
            status = "completed_official_missing"
            warnings.append("IPC oficial ausente: se publico IPC propio sin comparativa oficial.")
        elif int(metrics.get("overlap_months") or 0) == 0 and tracker_rows > 0:
            status = "completed_official_missing"
            warnings.append(f"Sin superposicion oficial para region={region_name}.")
        elif tracker_rows == 0:
            status = "failed_no_tracker_rows"
            warnings.append("No se generaron filas de IPC tracker.")
        elif warnings:
            status = "completed_with_warnings"

        run.status = status
        run.official_source = official_source_effective
        run.official_rows = int(official_rows)
        run.tracker_rows = int(tracker_rows)
        run.tracker_category_rows = int(tracker_category_rows)
        run.overlap_months = int(metrics.get("overlap_months") or 0)
        run.warnings_json = json.dumps(warnings, ensure_ascii=False)
        run.metrics_json = json.dumps(metrics, ensure_ascii=False)
        run.completed_at = datetime.now(timezone.utc)
        session.commit()

        return PublicationSummary(
            run_uuid=run_uuid,
            status=status,
            basket_type=basket_type,
            region=region_name,
            method_version=tracker_method_version,
            from_month=tracker_from_month,
            to_month=tracker_to_month,
            official_rows=official_rows,
            tracker_rows=tracker_rows,
            tracker_category_rows=tracker_category_rows,
            overlap_months=int(metrics.get("overlap_months") or 0),
            metrics=metrics,
            warnings=warnings,
        )
    except Exception as exc:
        status = "failed"
        logger.exception("IPC publish pipeline failed")
        run.status = status
        run.error_message = str(exc)
        run.warnings_json = json.dumps(warnings, ensure_ascii=False)
        run.metrics_json = json.dumps(metrics, ensure_ascii=False)
        run.completed_at = datetime.now(timezone.utc)
        session.commit()
        raise


def run_ipc_publish(
    config_path: Optional[str] = None,
    basket_type: str = "all",
    from_month: Optional[str] = None,
    to_month: Optional[str] = None,
    region: Optional[str] = None,
    skip_sync: bool = False,
    skip_build: bool = False,
) -> Dict[str, Any]:
    """CLI helper to execute and report the IPC publish pipeline."""
    config = load_config(config_path)
    engine = get_engine(config)
    init_db(engine)
    session_factory = get_session_factory(engine)
    session = session_factory()
    try:
        summary = publish_ipc(
            config=config,
            session=session,
            basket_type=basket_type,
            from_month=from_month,
            to_month=to_month,
            region=region,
            skip_sync=skip_sync,
            skip_build=skip_build,
        )
        payload = asdict(summary)
        payload["status"] = summary.status
        return payload
    finally:
        session.close()
