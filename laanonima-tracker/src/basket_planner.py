"""Deterministic scrape basket planning with runtime budget controls."""

from __future__ import annotations

import math
import random
from dataclasses import dataclass
from datetime import datetime
from statistics import median
from typing import Any, Dict, List, Optional, Sequence, Set

from sqlalchemy import func
from sqlalchemy.orm import Session

from src.config_loader import get_basket_items
from src.models import Price, ScrapeRun


DEFAULT_DAILY_CORE_IDS = [
    "ext_harina",
    "ext_pure_tomate",
    "ext_atun",
    "ext_gaseosa",
    "ext_shampoo",
    "ext_jabon",
    "ext_pasta_dental",
    "ext_papel_higienico",
    "ext_detergente",
    "ext_lavandina",
]

DEFAULT_DAILY_ROTATION_IDS = [
    "ext_aceite_oliva",
    "ext_azucar_morena",
    "ext_galletas_dulces",
    "ext_jugo",
    "ext_arvejas",
    "ext_choclo",
    "ext_desodorante",
]


@dataclass
class ScrapePlan:
    planned_items: List[Dict[str, Any]]
    mandatory_ids: Set[str]
    plan_summary: Dict[str, Any]
    budget: Dict[str, Any]

    def as_dict(self) -> Dict[str, Any]:
        return {
            "planned_items": self.planned_items,
            "mandatory_ids": sorted(self.mandatory_ids),
            "plan_summary": self.plan_summary,
            "budget": self.budget,
        }


def _get_planning_cfg(config: Dict[str, Any]) -> Dict[str, Any]:
    return config.get("scraping", {}).get("planning", {})


def _safe_positive_int(value: Any, fallback: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return fallback
    return parsed if parsed > 0 else fallback


def _estimate_seconds_per_product(session: Session, lookback_runs: int) -> float:
    rows = (
        session.query(ScrapeRun.duration_seconds, ScrapeRun.products_scraped)
        .filter(ScrapeRun.products_scraped > 0)
        .filter(ScrapeRun.duration_seconds.isnot(None))
        .order_by(ScrapeRun.started_at.desc())
        .limit(max(1, lookback_runs))
        .all()
    )
    per_product: List[float] = []
    for duration_seconds, products_scraped in rows:
        if not duration_seconds or not products_scraped:
            continue
        if products_scraped <= 0:
            continue
        per_product.append(float(duration_seconds) / float(products_scraped))

    if not per_product:
        return 20.0

    # Keep estimate bounded to avoid pathological planning.
    return max(12.0, min(45.0, float(median(per_product))))


def _last_scraped_by_canonical_id(session: Session) -> Dict[str, datetime]:
    rows = (
        session.query(
            Price.canonical_id,
            func.max(Price.scraped_at).label("last_scraped_at"),
        )
        .group_by(Price.canonical_id)
        .all()
    )
    out: Dict[str, datetime] = {}
    for canonical_id, last_scraped_at in rows:
        if canonical_id and last_scraped_at is not None:
            out[str(canonical_id)] = last_scraped_at
    return out


def _item_id(item: Dict[str, Any]) -> str:
    return str(item.get("id") or "")


def _annotate_segment(item: Dict[str, Any], segment: str) -> Dict[str, Any]:
    copy = dict(item)
    copy["_plan_segment"] = segment
    return copy


def _segment_counts(items: Sequence[Dict[str, Any]]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for item in items:
        segment = str(item.get("_plan_segment", "other"))
        counts[segment] = counts.get(segment, 0) + 1
    return counts


def build_scrape_plan(
    config: Dict[str, Any],
    session: Session,
    basket_type: str = "all",
    profile: str = "balanced",
    runtime_budget_minutes: Optional[int] = None,
    rotation_items: Optional[int] = None,
    limit: Optional[int] = None,
    sample_random: bool = False,
) -> ScrapePlan:
    profile = (profile or "balanced").lower()
    if profile not in {"balanced", "full", "cba_only"}:
        raise ValueError("profile invalido: use balanced, full o cba_only")

    planning_cfg = _get_planning_cfg(config)
    runtime_budget = _safe_positive_int(
        runtime_budget_minutes,
        _safe_positive_int(planning_cfg.get("runtime_budget_minutes"), 20),
    )
    rotation_target_default = _safe_positive_int(planning_cfg.get("rotation_items_default"), 4)
    rotation_target = _safe_positive_int(rotation_items, rotation_target_default)
    overhead_seconds = _safe_positive_int(planning_cfg.get("overhead_seconds"), 90)
    lookback_runs = _safe_positive_int(planning_cfg.get("lookback_runs"), 10)

    basket_items = [dict(item) for item in get_basket_items(config, basket_type)]
    basket_items.sort(key=lambda row: _item_id(row))
    item_by_id = {_item_id(item): item for item in basket_items if _item_id(item)}

    cba_items = [
        _annotate_segment(item, "cba")
        for item in basket_items
        if str(item.get("basket_type", "")) == "cba"
    ]
    cba_ids = {_item_id(item) for item in cba_items}

    core_ids = planning_cfg.get("daily_core_ids") or DEFAULT_DAILY_CORE_IDS
    rotation_ids = planning_cfg.get("daily_rotation_ids") or DEFAULT_DAILY_ROTATION_IDS
    core_items: List[Dict[str, Any]] = []
    rotation_pool: List[Dict[str, Any]] = []

    for item_id in core_ids:
        if item_id in item_by_id and item_id not in cba_ids:
            core_items.append(_annotate_segment(item_by_id[item_id], "daily_core"))
    core_ids_set = {_item_id(item) for item in core_items}

    for item_id in rotation_ids:
        if item_id in item_by_id and item_id not in cba_ids and item_id not in core_ids_set:
            rotation_pool.append(_annotate_segment(item_by_id[item_id], "daily_rotation"))

    # Any remaining non-CBA item is still eligible as rotation candidate.
    for item in basket_items:
        item_id = _item_id(item)
        if not item_id or item_id in cba_ids or item_id in core_ids_set:
            continue
        if item_id in {_item_id(x) for x in rotation_pool}:
            continue
        rotation_pool.append(_annotate_segment(item, "daily_rotation"))

    seconds_per_product = _estimate_seconds_per_product(session, lookback_runs=lookback_runs)
    budget_seconds = runtime_budget * 60
    capacity_items = max(0, int(math.floor(max(0, budget_seconds - overhead_seconds) / seconds_per_product)))

    mandatory_items: List[Dict[str, Any]]
    optional_items: List[Dict[str, Any]] = []

    if profile == "cba_only":
        mandatory_items = list(cba_items)
    elif profile == "full":
        mandatory_items = []
        for item in basket_items:
            if str(item.get("basket_type", "")) == "cba":
                mandatory_items.append(_annotate_segment(item, "cba"))
            else:
                mandatory_items.append(_annotate_segment(item, "daily_core"))
    else:
        mandatory_items = list(cba_items)
        mandatory_items.extend(core_items)

        last_seen = _last_scraped_by_canonical_id(session)

        def _rotation_sort_key(item: Dict[str, Any]):
            item_id = _item_id(item)
            last = last_seen.get(item_id)
            if last is None:
                return (0, datetime.min, item_id)
            return (1, last, item_id)

        rotation_pool.sort(key=_rotation_sort_key)
        optional_capacity = max(0, capacity_items - len(mandatory_items))
        optional_count = min(len(rotation_pool), rotation_target, optional_capacity)
        optional_items = rotation_pool[:optional_count]

    mandatory_ids = {_item_id(item) for item in mandatory_items if _item_id(item)}
    planned_items = list(mandatory_items) + list(optional_items)

    if sample_random:
        random_pool = list(planned_items) if planned_items else list(basket_items)
        if limit is not None and limit > 0 and len(random_pool) > limit:
            random_pool = random.sample(random_pool, limit)
        random_pool = [_annotate_segment(item, "random") for item in random_pool]
        planned_items = random_pool
        mandatory_ids = set()

    if limit is not None:
        if limit <= 0:
            raise ValueError("limit debe ser mayor a 0")
        if not sample_random and limit < len(mandatory_ids):
            raise ValueError(
                f"limit={limit} es menor al minimo obligatorio ({len(mandatory_ids)}). "
                "Aumenta --limit o usa --sample-random para debug."
            )
        if len(planned_items) > limit:
            planned_items = planned_items[:limit]

    planned_count = len(planned_items)
    estimated_duration_seconds = int(round(overhead_seconds + (planned_count * seconds_per_product)))
    segments = _segment_counts(planned_items)
    mandatory_segment_counts = _segment_counts(mandatory_items)

    plan_summary = {
        "profile": profile,
        "basket_type": basket_type,
        "planned_count": planned_count,
        "mandatory_count": len(mandatory_ids),
        "rotation_target": rotation_target,
        "rotation_applied": segments.get("daily_rotation", 0),
        "segments": segments,
        "mandatory_segments": mandatory_segment_counts,
        "sample_random": bool(sample_random),
        "seconds_per_product_estimate": round(seconds_per_product, 2),
        "estimated_duration_seconds": estimated_duration_seconds,
        "overhead_seconds": overhead_seconds,
        "capacity_items_estimate": capacity_items,
    }
    budget = {
        "runtime_budget_minutes": runtime_budget,
        "target_seconds": budget_seconds,
        "estimated_seconds": estimated_duration_seconds,
        "estimated_within_target": estimated_duration_seconds <= budget_seconds,
    }
    return ScrapePlan(
        planned_items=planned_items,
        mandatory_ids=mandatory_ids,
        plan_summary=plan_summary,
        budget=budget,
    )
