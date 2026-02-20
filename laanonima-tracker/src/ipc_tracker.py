"""Tracker-owned monthly IPC construction (robust fixed-weight methodology)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from loguru import logger
from sqlalchemy.orm import Session

from src.config_loader import get_basket_items, load_config, resolve_canonical_category
from src.models import (
    Price,
    Product,
    TrackerIPCCategoryMonthly,
    TrackerIPCMonthly,
    get_engine,
    get_session_factory,
    init_db,
    now_utc,
)

_LEGACY_MAPPING_WARNED = False


@dataclass
class TrackerBuildResult:
    """Result metadata for tracker IPC builds."""

    basket_type: str
    method_version: str
    from_month: Optional[str]
    to_month: Optional[str]
    months_processed: int
    general_rows: int
    category_rows: int
    warnings: List[str]


class TrackerIPCBuilder:
    """Computes monthly tracker IPC using winsorized product-month representative prices."""

    def __init__(self, config: Dict[str, Any], session: Optional[Session] = None):
        self.config = config
        self.ipc_cfg = config.get("analysis", {}).get("ipc_tracker", {})
        self.mapping_cfg = config.get("analysis", {}).get("ipc_category_mapping", {})

        self.method_version = str(self.ipc_cfg.get("method_version", "v1_fixed_weight_robust_monthly"))
        self.monthly_aggregation = str(self.ipc_cfg.get("monthly_aggregation", "winsorized_mean"))
        winsor_limits = self.ipc_cfg.get("winsor_limits", [0.1, 0.9])
        self.winsor_low = float(winsor_limits[0]) if isinstance(winsor_limits, (list, tuple)) and len(winsor_limits) >= 2 else 0.1
        self.winsor_high = float(winsor_limits[1]) if isinstance(winsor_limits, (list, tuple)) and len(winsor_limits) >= 2 else 0.9
        self.min_obs_per_product_month = int(self.ipc_cfg.get("min_obs_per_product_month", 1))
        self.coverage_min_weight_pct = float(self.ipc_cfg.get("coverage_min_weight_pct", 0.7))
        self.provisional_freeze_days = int(self.ipc_cfg.get("provisional_freeze_days", 7))

        self._owns_session = session is None
        if session is not None:
            self.session = session
        else:
            engine = get_engine(config)
            session_factory = get_session_factory(engine)
            self.session = session_factory()

    def close(self):
        if self._owns_session:
            self.session.close()

    @staticmethod
    def _month_start(month: str) -> pd.Timestamp:
        return pd.Period(month, freq="M").to_timestamp()

    @staticmethod
    def _next_month_start(month: str) -> pd.Timestamp:
        return pd.Period(month, freq="M").to_timestamp() + pd.offsets.MonthBegin(1)

    @staticmethod
    def _safe_float(value: Any) -> Optional[float]:
        if value is None or pd.isna(value):
            return None
        return float(value)

    def _month_range(
        self,
        observed_months: List[str],
        from_month: Optional[str],
        to_month: Optional[str],
    ) -> List[str]:
        if from_month and to_month:
            return [str(p) for p in pd.period_range(from_month, to_month, freq="M")]
        if from_month and not to_month:
            end = observed_months[-1] if observed_months else from_month
            return [str(p) for p in pd.period_range(from_month, end, freq="M")]
        if to_month and not from_month:
            start = observed_months[0] if observed_months else to_month
            return [str(p) for p in pd.period_range(start, to_month, freq="M")]
        if not observed_months:
            return []
        return [str(p) for p in pd.period_range(observed_months[0], observed_months[-1], freq="M")]

    def _load_prices(
        self,
        basket_type: str,
        from_month: Optional[str],
        to_month: Optional[str],
    ) -> pd.DataFrame:
        query = (
            self.session.query(
                Price.canonical_id,
                Price.basket_id,
                Price.current_price,
                Price.scraped_at,
                Product.category,
            )
            .outerjoin(Product, Price.canonical_id == Product.canonical_id)
        )
        if basket_type != "all":
            query = query.filter(Price.basket_id == basket_type)
        if from_month:
            query = query.filter(Price.scraped_at >= self._month_start(from_month).to_pydatetime())
        if to_month:
            query = query.filter(Price.scraped_at < self._next_month_start(to_month).to_pydatetime())

        df = pd.read_sql(query.statement, self.session.bind)
        if df.empty:
            return pd.DataFrame(
                columns=[
                    "canonical_id",
                    "basket_id",
                    "current_price",
                    "scraped_at",
                    "category",
                    "month",
                ]
            )

        df["scraped_at"] = pd.to_datetime(df["scraped_at"], errors="coerce")
        df["current_price"] = pd.to_numeric(df["current_price"], errors="coerce")
        df = df.dropna(subset=["scraped_at", "current_price"]).copy()
        df["month"] = df["scraped_at"].dt.to_period("M").astype(str)
        df["category"] = df["category"].fillna("sin_categoria")
        return df

    def _basket_weights(self, basket_type: str) -> Tuple[Dict[str, float], Dict[str, str]]:
        weights: Dict[str, float] = {}
        category_by_id: Dict[str, str] = {}
        for item in get_basket_items(self.config, basket_type):
            canonical_id = str(item.get("id") or "").strip()
            if not canonical_id:
                continue
            weight = float(item.get("quantity", 1.0))
            if weight <= 0:
                continue
            raw_category = item.get("category") or "sin_categoria"
            category_slug = resolve_canonical_category(self.config, raw_category) or str(raw_category)
            weights[canonical_id] = weight
            category_by_id[canonical_id] = str(category_slug).lower()
        return weights, category_by_id

    def _winsorized_mean(self, values: pd.Series) -> Tuple[Optional[float], int]:
        clean = pd.to_numeric(values, errors="coerce").dropna()
        if clean.empty:
            return None, 0
        if self.monthly_aggregation != "winsorized_mean" or len(clean) < 4:
            return float(clean.mean()), 0

        low = clean.quantile(self.winsor_low)
        high = clean.quantile(self.winsor_high)
        clipped = clean.clip(lower=low, upper=high)
        outliers = int(((clean < low) | (clean > high)).sum())
        return float(clipped.mean()), outliers

    def _representative_prices(
        self,
        prices_df: pd.DataFrame,
        weights: Dict[str, float],
        category_by_id: Dict[str, str],
    ) -> pd.DataFrame:
        if prices_df.empty or not weights:
            return pd.DataFrame(
                columns=[
                    "canonical_id",
                    "month",
                    "rep_price",
                    "obs_count",
                    "outlier_count",
                    "category_slug",
                    "weight",
                ]
            )

        working = prices_df[prices_df["canonical_id"].isin(set(weights.keys()))].copy()
        if working.empty:
            return pd.DataFrame(
                columns=[
                    "canonical_id",
                    "month",
                    "rep_price",
                    "obs_count",
                    "outlier_count",
                    "category_slug",
                    "weight",
                ]
            )

        rows: List[Dict[str, Any]] = []
        grouped = working.groupby(["canonical_id", "month"], sort=True)
        for (canonical_id, month), grp in grouped:
            obs_count = int(len(grp))
            if obs_count < self.min_obs_per_product_month:
                continue
            rep, outliers = self._winsorized_mean(grp["current_price"])
            if rep is None:
                continue
            rows.append(
                {
                    "canonical_id": canonical_id,
                    "month": str(month),
                    "rep_price": float(rep),
                    "obs_count": obs_count,
                    "outlier_count": outliers,
                    "category_slug": category_by_id.get(canonical_id, "sin_categoria"),
                    "weight": float(weights.get(canonical_id, 0.0)),
                }
            )

        return pd.DataFrame(rows)

    def _compute_monthly_rows(
        self,
        rep_df: pd.DataFrame,
        weights: Dict[str, float],
        month_list: List[str],
    ) -> List[Dict[str, Any]]:
        if not weights or not month_list:
            return []

        total_weight = float(sum(weights.values()))
        expected_products = int(len(weights))
        by_month: Dict[str, pd.DataFrame] = {m: rep_df[rep_df["month"] == m] for m in month_list}

        base_month: Optional[str] = None
        for month in month_list:
            current = by_month.get(month, pd.DataFrame())
            if current.empty:
                continue
            observed_weight = float(current["weight"].sum())
            if total_weight > 0 and (observed_weight / total_weight) >= self.coverage_min_weight_pct:
                base_month = month
                break

        rows: List[Dict[str, Any]] = []
        index_by_month: Dict[str, float] = {}
        now_ts = pd.Timestamp(datetime.now(timezone.utc)).tz_convert(None)
        prev_month: Optional[str] = None
        prev_index: Optional[float] = None

        for month in month_list:
            current = by_month.get(month, pd.DataFrame())
            observed_products = int(current["canonical_id"].nunique()) if not current.empty else 0
            observed_weight = float(current["weight"].sum()) if not current.empty else 0.0
            coverage_product_pct_current = (observed_products / expected_products) if expected_products > 0 else None
            base_coverage_weight = (observed_weight / total_weight) if total_weight > 0 else None

            products_with_relative = 0
            coverage_weight_pct = None
            coverage_product_pct = coverage_product_pct_current
            mom_change_pct = None
            missing_products = max(expected_products - observed_products, 0)
            outlier_count = int(current["outlier_count"].sum()) if not current.empty else 0

            if prev_month:
                prev = by_month.get(prev_month, pd.DataFrame())
                if not current.empty and not prev.empty:
                    merged = current[["canonical_id", "rep_price", "weight"]].merge(
                        prev[["canonical_id", "rep_price"]],
                        on="canonical_id",
                        how="inner",
                        suffixes=("_cur", "_prev"),
                    )
                    merged = merged[merged["rep_price_prev"] > 0]
                    if not merged.empty:
                        merged["ratio"] = merged["rep_price_cur"] / merged["rep_price_prev"]
                        rel_weight = float(merged["weight"].sum())
                        products_with_relative = int(merged["canonical_id"].nunique())
                        missing_products = max(expected_products - products_with_relative, 0)
                        if total_weight > 0:
                            coverage_weight_pct = rel_weight / total_weight
                        coverage_product_pct = (
                            products_with_relative / expected_products if expected_products > 0 else None
                        )
                        if rel_weight > 0:
                            weighted_ratio = float((merged["weight"] * merged["ratio"]).sum() / rel_weight)
                            mom_change_pct = (weighted_ratio - 1.0) * 100.0

            if base_month == month:
                index_val = 100.0
            elif prev_index is not None and mom_change_pct is not None:
                index_val = prev_index * (1.0 + (mom_change_pct / 100.0))
            else:
                index_val = None

            yoy_change = None
            prev_year = str(pd.Period(month, freq="M") - 12)
            if index_val is not None and prev_year in index_by_month and index_by_month[prev_year] > 0:
                yoy_change = ((index_val / index_by_month[prev_year]) - 1.0) * 100.0

            coverage_for_status = coverage_weight_pct if coverage_weight_pct is not None else base_coverage_weight
            status = "provisional"
            if coverage_for_status is not None and coverage_for_status < self.coverage_min_weight_pct:
                status = "provisional_low_coverage"
            else:
                month_freeze_dt = self._next_month_start(month) + pd.Timedelta(days=self.provisional_freeze_days)
                if now_ts >= month_freeze_dt:
                    status = "final"

            frozen_at = now_utc() if status == "final" else None
            if index_val is not None:
                index_by_month[month] = index_val
                prev_index = index_val
            elif month == base_month:
                prev_index = 100.0

            rows.append(
                {
                    "year_month": month,
                    "method_version": self.method_version,
                    "status": status,
                    "index_value": index_val,
                    "mom_change": mom_change_pct,
                    "yoy_change": yoy_change,
                    "coverage_weight_pct": coverage_weight_pct,
                    "coverage_product_pct": coverage_product_pct,
                    "products_expected": expected_products,
                    "products_observed": observed_products,
                    "products_with_relative": products_with_relative,
                    "outlier_count": outlier_count,
                    "missing_products": missing_products,
                    "base_month": base_month,
                    "frozen_at": frozen_at,
                }
            )
            prev_month = month

        return rows

    def _indec_code_by_category(self) -> Dict[str, str]:
        global _LEGACY_MAPPING_WARNED
        if not isinstance(self.mapping_cfg, dict):
            return {}
        explicit = self.mapping_cfg.get("app_to_indec_division")
        if isinstance(explicit, dict):
            out: Dict[str, str] = {}
            for k, v in explicit.items():
                if v in {None, ""}:
                    continue
                out[str(k).strip().lower()] = str(v).strip().lower()
            return out
        legacy = self.mapping_cfg.get("map")
        if isinstance(legacy, dict):
            if not _LEGACY_MAPPING_WARNED:
                logger.warning(
                    "Deprecated config path in use: analysis.ipc_category_mapping.map. "
                    "Use analysis.ipc_category_mapping.app_to_indec_division."
                )
                _LEGACY_MAPPING_WARNED = True
            return {
                str(k).strip().lower(): str(v).strip().lower()
                for k, v in legacy.items()
                if v not in {None, ""}
            }
        return {}

    def _upsert_general(
        self,
        basket_type: str,
        rows: List[Dict[str, Any]],
    ) -> int:
        upserted = 0
        for row in rows:
            existing = (
                self.session.query(TrackerIPCMonthly)
                .filter_by(
                    basket_type=basket_type,
                    year_month=row["year_month"],
                    method_version=self.method_version,
                )
                .first()
            )
            payload = {
                "status": row["status"],
                "index_value": row["index_value"],
                "mom_change": row["mom_change"],
                "yoy_change": row["yoy_change"],
                "coverage_weight_pct": row["coverage_weight_pct"],
                "coverage_product_pct": row["coverage_product_pct"],
                "products_expected": row["products_expected"],
                "products_observed": row["products_observed"],
                "products_with_relative": row["products_with_relative"],
                "outlier_count": row["outlier_count"],
                "missing_products": row["missing_products"],
                "base_month": row["base_month"],
                "notes": None,
                "computed_at": now_utc(),
                "frozen_at": row["frozen_at"],
            }
            if existing:
                for key, value in payload.items():
                    setattr(existing, key, value)
            else:
                self.session.add(
                    TrackerIPCMonthly(
                        basket_type=basket_type,
                        year_month=row["year_month"],
                        method_version=self.method_version,
                        **payload,
                    )
                )
            upserted += 1
        self.session.commit()
        return upserted

    def _upsert_categories(
        self,
        basket_type: str,
        rows: List[Dict[str, Any]],
    ) -> int:
        upserted = 0
        indec_map = self._indec_code_by_category()
        for row in rows:
            category_slug = row["category_slug"]
            existing = (
                self.session.query(TrackerIPCCategoryMonthly)
                .filter_by(
                    basket_type=basket_type,
                    category_slug=category_slug,
                    year_month=row["year_month"],
                    method_version=self.method_version,
                )
                .first()
            )
            payload = {
                "indec_division_code": indec_map.get(str(category_slug).lower()),
                "status": row["status"],
                "index_value": row["index_value"],
                "mom_change": row["mom_change"],
                "yoy_change": row["yoy_change"],
                "coverage_weight_pct": row["coverage_weight_pct"],
                "coverage_product_pct": row["coverage_product_pct"],
                "products_expected": row["products_expected"],
                "products_observed": row["products_observed"],
                "products_with_relative": row["products_with_relative"],
                "outlier_count": row["outlier_count"],
                "missing_products": row["missing_products"],
                "base_month": row["base_month"],
                "notes": None,
                "computed_at": now_utc(),
                "frozen_at": row["frozen_at"],
            }
            if existing:
                for key, value in payload.items():
                    setattr(existing, key, value)
            else:
                self.session.add(
                    TrackerIPCCategoryMonthly(
                        basket_type=basket_type,
                        category_slug=category_slug,
                        year_month=row["year_month"],
                        method_version=self.method_version,
                        **payload,
                    )
                )
            upserted += 1
        self.session.commit()
        return upserted

    def build(
        self,
        basket_type: str = "all",
        from_month: Optional[str] = None,
        to_month: Optional[str] = None,
    ) -> TrackerBuildResult:
        if basket_type not in {"cba", "extended", "all"}:
            raise ValueError("basket_type invalido: use cba, extended o all")

        warnings: List[str] = []
        weights, category_by_id = self._basket_weights(basket_type)
        prices_df = self._load_prices(basket_type=basket_type, from_month=from_month, to_month=to_month)
        rep_df = self._representative_prices(prices_df=prices_df, weights=weights, category_by_id=category_by_id)

        observed_months = sorted(rep_df["month"].astype(str).unique().tolist()) if not rep_df.empty else []
        month_list = self._month_range(observed_months=observed_months, from_month=from_month, to_month=to_month)
        if not month_list:
            warnings.append("No hay meses con precios representativos para construir IPC tracker.")
            return TrackerBuildResult(
                basket_type=basket_type,
                method_version=self.method_version,
                from_month=from_month,
                to_month=to_month,
                months_processed=0,
                general_rows=0,
                category_rows=0,
                warnings=warnings,
            )

        general_rows = self._compute_monthly_rows(rep_df=rep_df, weights=weights, month_list=month_list)
        general_upserted = self._upsert_general(basket_type=basket_type, rows=general_rows)

        category_rows: List[Dict[str, Any]] = []
        if not rep_df.empty:
            for category_slug, cat_df in rep_df.groupby("category_slug"):
                category_slug = str(category_slug).lower()
                cat_weights = {k: v for k, v in weights.items() if category_by_id.get(k, "sin_categoria") == category_slug}
                if not cat_weights:
                    continue
                cat_rows = self._compute_monthly_rows(cat_df, cat_weights, month_list)
                for row in cat_rows:
                    row["category_slug"] = category_slug
                category_rows.extend(cat_rows)
        category_upserted = self._upsert_categories(basket_type=basket_type, rows=category_rows) if category_rows else 0

        low_cov = [r for r in general_rows if r.get("status") == "provisional_low_coverage"]
        if low_cov:
            warnings.append(f"Meses con baja cobertura de peso: {', '.join(r['year_month'] for r in low_cov)}")

        logger.info(
            "Tracker IPC build done basket={} method={} months={} general={} categories={}",
            basket_type,
            self.method_version,
            len(month_list),
            general_upserted,
            category_upserted,
        )

        return TrackerBuildResult(
            basket_type=basket_type,
            method_version=self.method_version,
            from_month=from_month or month_list[0],
            to_month=to_month or month_list[-1],
            months_processed=len(month_list),
            general_rows=general_upserted,
            category_rows=category_upserted,
            warnings=warnings,
        )


def run_ipc_build(
    config_path: Optional[str] = None,
    basket_type: str = "all",
    from_month: Optional[str] = None,
    to_month: Optional[str] = None,
) -> Dict[str, Any]:
    """CLI helper for tracker IPC monthly build."""
    config = load_config(config_path)
    engine = get_engine(config)
    init_db(engine)
    session_factory = get_session_factory(engine)
    session = session_factory()
    builder = TrackerIPCBuilder(config=config, session=session)
    try:
        result = builder.build(
            basket_type=basket_type,
            from_month=from_month,
            to_month=to_month,
        )
        return {
            "status": "completed",
            "basket_type": result.basket_type,
            "method_version": result.method_version,
            "from_month": result.from_month,
            "to_month": result.to_month,
            "months_processed": result.months_processed,
            "general_rows": result.general_rows,
            "category_rows": result.category_rows,
            "warnings": result.warnings,
        }
    finally:
        builder.close()
        if session.is_active:
            session.close()
