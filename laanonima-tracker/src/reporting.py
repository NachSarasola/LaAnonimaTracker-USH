"""Economic interactive report generation for La Anonima Tracker."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from html import escape
from pathlib import Path
from time import perf_counter
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from loguru import logger
from sqlalchemy import func

from src.config_loader import get_basket_items, load_config, resolve_canonical_category
from src.models import Price, get_engine, get_session_factory
from src.repositories import SeriesRepository


@dataclass
class ReportArtifacts:
    html_path: str
    metadata_path: str
    pdf_path: Optional[str] = None


PUBLICATION_POLICY = "publish_with_alert_on_partial"
_LEGACY_MAPPING_WARNED = False


class ReportGenerator:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        engine = get_engine(config)
        self.session = get_session_factory(engine)()

    def close(self):
        self.session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _month_start(self, value: str) -> pd.Timestamp:
        return pd.to_datetime(value, format="%Y-%m").to_period("M").to_timestamp()

    def _next_month_start(self, value: str) -> pd.Timestamp:
        return self._month_start(value) + pd.offsets.MonthBegin(1)

    def _month_sequence(self, from_month: str, to_month: str) -> List[str]:
        return [str(period) for period in pd.period_range(from_month, to_month, freq="M")]

    def _resolve_effective_range(
        self,
        from_month: Optional[str],
        to_month: Optional[str],
        basket_type: str,
        default_months: int = 6,
    ) -> Tuple[str, str]:
        if basket_type not in {"cba", "extended", "all"}:
            raise ValueError("basket_type invalido: use cba, extended o all")

        q = self.session.query(func.max(Price.scraped_at))
        if basket_type != "all":
            q = q.filter(Price.basket_id == basket_type)
        max_dt = q.scalar()
        fallback_to = (
            datetime.now(timezone.utc).strftime("%Y-%m")
            if max_dt is None
            else str(pd.Timestamp(max_dt).to_period("M"))
        )

        if to_month is None:
            if from_month is None:
                to_month = fallback_to
            else:
                from_period = pd.Period(from_month, freq="M")
                fallback_period = pd.Period(fallback_to, freq="M")
                to_month = str(from_period if from_period > fallback_period else fallback_period)

        if from_month is None:
            from_month = str(pd.Period(to_month, freq="M") - (default_months - 1))

        if self._month_start(to_month) < self._month_start(from_month):
            raise ValueError("El rango es invalido: --to debe ser mayor o igual que --from")
        return from_month, to_month

    def _load_prices(self, from_month: str, to_month: str, basket_type: str) -> pd.DataFrame:
        repository = SeriesRepository(self.session)
        rows = repository.get_report_rows(
            basket_type=basket_type,
            start_dt=self._month_start(from_month).to_pydatetime(),
            end_exclusive_dt=self._next_month_start(to_month).to_pydatetime(),
        )
        df = pd.DataFrame(rows)
        if df.empty:
            return pd.DataFrame(
                columns=[
                    "canonical_id",
                    "product_name",
                    "basket_id",
                    "current_price",
                    "scraped_at",
                    "category",
                    "product_url",
                    "product_size",
                    "month",
                ]
            )
        df["scraped_at"] = pd.to_datetime(df["scraped_at"], utc=False, errors="coerce")
        df = df.dropna(subset=["scraped_at"]).copy()
        df["month"] = df["scraped_at"].dt.to_period("M").astype(str)
        df["current_price"] = pd.to_numeric(df["current_price"], errors="coerce")
        df["category"] = df["category"].map(
            lambda value: resolve_canonical_category(self.config, value)
            or (str(value).strip().lower() if value is not None and str(value).strip() else "sin_categoria")
        )
        df["product_size"] = df["product_size"].fillna("N/D")
        return df.dropna(subset=["current_price"]).copy()

    def _load_candidate_rows(self, from_month: str, to_month: str, basket_type: str) -> pd.DataFrame:
        repository = SeriesRepository(self.session)
        try:
            rows = repository.get_candidate_rows(
                basket_type=basket_type,
                start_dt=self._month_start(from_month).to_pydatetime(),
                end_exclusive_dt=self._next_month_start(to_month).to_pydatetime(),
            )
        except Exception:
            rows = []

        df = pd.DataFrame(rows)
        if df.empty:
            return pd.DataFrame(
                columns=[
                    "canonical_id",
                    "basket_id",
                    "product_name",
                    "tier",
                    "candidate_rank",
                    "candidate_price",
                    "confidence_score",
                    "is_selected",
                    "is_fallback",
                    "scraped_at",
                ]
            )
        df["scraped_at"] = pd.to_datetime(df["scraped_at"], utc=False, errors="coerce")
        df = df.dropna(subset=["scraped_at"]).copy()
        df["candidate_price"] = pd.to_numeric(df["candidate_price"], errors="coerce")
        df["confidence_score"] = pd.to_numeric(df["confidence_score"], errors="coerce")
        df["tier"] = df["tier"].astype(str).str.lower().replace({"single": "mid"})
        df = df[df["tier"].isin(["low", "mid", "high"])]
        return df.dropna(subset=["candidate_price"]).copy()

    def _load_ipc_data(
        self,
        from_month: str,
        to_month: str,
        benchmark_mode: str = "ipc",
        region: str = "patagonia",
    ) -> pd.DataFrame:
        columns = ["year_month", "cpi_index", "cpi_mom", "cpi_yoy", "status", "source"]
        if benchmark_mode == "none":
            return pd.DataFrame(columns=columns)

        repository = SeriesRepository(self.session)
        try:
            rows, _ = repository.get_official_ipc_patagonia(
                start_period=from_month,
                end_period=to_month,
                metric_code="general",
                region=region,
                page=1,
                page_size=2400,
            )
        except Exception:
            rows = []
        if rows:
            ipc_df = pd.DataFrame(rows)
            ipc_df = ipc_df.rename(
                columns={
                    "index_value": "cpi_index",
                    "mom_change": "cpi_mom",
                    "yoy_change": "cpi_yoy",
                }
            )
            ipc_df["year_month"] = ipc_df["year_month"].astype(str)
            ipc_df["cpi_index"] = pd.to_numeric(ipc_df["cpi_index"], errors="coerce")
            ipc_df["cpi_mom"] = pd.to_numeric(ipc_df["cpi_mom"], errors="coerce")
            ipc_df["cpi_yoy"] = pd.to_numeric(ipc_df["cpi_yoy"], errors="coerce")
            ipc_df = ipc_df.dropna(subset=["cpi_index"])
            if "status" not in ipc_df.columns:
                ipc_df["status"] = "final"
            if "source" not in ipc_df.columns:
                ipc_df["source"] = "indec_patagonia"
            return ipc_df.sort_values("year_month")[columns].reset_index(drop=True)

        # Backward-compatible fallback to legacy flat file.
        if region != "patagonia":
            return pd.DataFrame(columns=columns)
        cpi_path = Path("data/cpi/ipc_indec_patagonia.csv")
        if not cpi_path.exists():
            cpi_path = Path("data/cpi/ipc_indec.csv")
        if not cpi_path.exists():
            return pd.DataFrame(columns=columns)

        try:
            ipc_df = pd.read_csv(cpi_path)
        except Exception:
            return pd.DataFrame(columns=columns)

        if "year_month" not in ipc_df.columns:
            return pd.DataFrame(columns=columns)

        ipc_df = ipc_df.copy()
        ipc_df["year_month"] = ipc_df["year_month"].astype(str)
        if "cpi_index" in ipc_df.columns:
            ipc_df["cpi_index"] = pd.to_numeric(ipc_df["cpi_index"], errors="coerce")
        elif "index_value" in ipc_df.columns:
            ipc_df["cpi_index"] = pd.to_numeric(ipc_df["index_value"], errors="coerce")
        else:
            return pd.DataFrame(columns=columns)
        ipc_df["cpi_mom"] = pd.to_numeric(
            ipc_df["cpi_mom"] if "cpi_mom" in ipc_df.columns else ipc_df.get("mom_change"),
            errors="coerce",
        )
        ipc_df["cpi_yoy"] = pd.to_numeric(
            ipc_df["cpi_yoy"] if "cpi_yoy" in ipc_df.columns else ipc_df.get("yoy_change"),
            errors="coerce",
        )
        ipc_df["status"] = ipc_df.get("status", "final")
        ipc_df["source"] = ipc_df.get("source", "legacy_csv")
        ipc_df = ipc_df.dropna(subset=["cpi_index"])

        from_period = pd.Period(from_month, freq="M")
        to_period = pd.Period(to_month, freq="M")
        ipc_df = ipc_df[ipc_df["year_month"].map(lambda x: from_period <= pd.Period(x, freq="M") <= to_period)]
        return ipc_df.sort_values("year_month")[columns].reset_index(drop=True)

    def _load_tracker_ipc_general(self, from_month: str, to_month: str, basket_type: str) -> pd.DataFrame:
        repository = SeriesRepository(self.session)
        try:
            rows, _ = repository.get_tracker_ipc_general(
                basket_type=basket_type,
                start_period=from_month,
                end_period=to_month,
                page=1,
                page_size=2400,
            )
        except Exception:
            rows = []
        if not rows:
            return pd.DataFrame(
                columns=[
                    "year_month",
                    "index_value",
                    "mom_change",
                    "yoy_change",
                    "status",
                    "coverage_weight_pct",
                    "coverage_product_pct",
                    "method_version",
                    "base_month",
                ]
            )
        df = pd.DataFrame(rows)
        for col in ("index_value", "mom_change", "yoy_change", "coverage_weight_pct", "coverage_product_pct"):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        df["year_month"] = df["year_month"].astype(str)
        return df.sort_values("year_month").reset_index(drop=True)

    def _load_tracker_ipc_categories(self, from_month: str, to_month: str, basket_type: str) -> pd.DataFrame:
        repository = SeriesRepository(self.session)
        try:
            rows, _ = repository.get_tracker_ipc_categories(
                basket_type=basket_type,
                start_period=from_month,
                end_period=to_month,
                page=1,
                page_size=10000,
            )
        except Exception:
            rows = []
        if not rows:
            return pd.DataFrame(
                columns=[
                    "year_month",
                    "category_slug",
                    "index_value",
                    "mom_change",
                    "yoy_change",
                    "status",
                    "method_version",
                ]
            )
        df = pd.DataFrame(rows)
        for col in ("index_value", "mom_change", "yoy_change", "coverage_weight_pct", "coverage_product_pct"):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        df["year_month"] = df["year_month"].astype(str)
        df["category_slug"] = df["category_slug"].fillna("sin_categoria").astype(str)
        return df.sort_values(["category_slug", "year_month"]).reset_index(drop=True)

    def _build_ipc_comparison_series(
        self,
        tracker_df: pd.DataFrame,
        official_df: pd.DataFrame,
    ) -> List[Dict[str, Any]]:
        if tracker_df.empty and official_df.empty:
            return []

        tracker = tracker_df.copy()
        official = official_df.copy()
        tracker["year_month"] = tracker["year_month"].astype(str)
        official["year_month"] = official["year_month"].astype(str)
        official = official.rename(
            columns={
                "cpi_index": "official_index",
                "cpi_mom": "official_mom",
                "cpi_yoy": "official_yoy",
                "status": "official_status",
                "source": "official_source",
            }
        )
        for col in ("official_index", "official_mom", "official_yoy", "official_status", "official_source"):
            if col not in official.columns:
                official[col] = pd.NA
        if "official_status" not in official.columns:
            official["official_status"] = "final"
        if "official_source" not in official.columns:
            official["official_source"] = "legacy_csv"
        tracker = tracker.rename(
            columns={
                "index_value": "tracker_index",
                "mom_change": "tracker_mom",
                "yoy_change": "tracker_yoy",
                "status": "tracker_status",
            }
        )
        if "tracker_status" not in tracker.columns:
            tracker["tracker_status"] = pd.NA

        merged = tracker.merge(
            official[["year_month", "official_index", "official_mom", "official_yoy", "official_status", "official_source"]],
            on="year_month",
            how="outer",
        ).sort_values("year_month")

        overlap = merged.dropna(subset=["tracker_index", "official_index"])
        strict_comparable = not overlap.empty
        tracker_base_strict = self._safe_float(overlap.iloc[0]["tracker_index"]) if strict_comparable else None
        official_base_strict = self._safe_float(overlap.iloc[0]["official_index"]) if strict_comparable else None

        tracker_first = merged[merged["tracker_index"].notna()].head(1)
        official_first = merged[merged["official_index"].notna()].head(1)
        tracker_base_independent = (
            self._safe_float(tracker_first.iloc[0]["tracker_index"]) if not tracker_first.empty else None
        )
        official_base_independent = (
            self._safe_float(official_first.iloc[0]["official_index"]) if not official_first.empty else None
        )
        tracker_base_month_independent = (
            str(tracker_first.iloc[0]["year_month"]) if not tracker_first.empty else None
        )
        official_base_month_independent = (
            str(official_first.iloc[0]["year_month"]) if not official_first.empty else None
        )

        tracker_base_month_strict = str(overlap.iloc[0]["year_month"]) if strict_comparable else None
        official_base_month_strict = str(overlap.iloc[0]["year_month"]) if strict_comparable else None

        plot_mode = "strict_overlap" if strict_comparable else "independent_base"

        out: List[Dict[str, Any]] = []
        for _, row in merged.iterrows():
            tracker_idx = self._safe_float(row.get("tracker_index"))
            official_idx = self._safe_float(row.get("official_index"))
            tracker_base100_strict = None
            official_base100_strict = None
            if tracker_idx is not None and tracker_base_strict and tracker_base_strict > 0:
                tracker_base100_strict = (tracker_idx / tracker_base_strict) * 100.0
            if official_idx is not None and official_base_strict and official_base_strict > 0:
                official_base100_strict = (official_idx / official_base_strict) * 100.0

            if strict_comparable:
                plot_tracker_base100 = tracker_base100_strict
                plot_official_base100 = official_base100_strict
            else:
                plot_tracker_base100 = (
                    (tracker_idx / tracker_base_independent) * 100.0
                    if tracker_idx is not None and tracker_base_independent and tracker_base_independent > 0
                    else None
                )
                plot_official_base100 = (
                    (official_idx / official_base_independent) * 100.0
                    if official_idx is not None and official_base_independent and official_base_independent > 0
                    else None
                )
            out.append(
                {
                    "year_month": str(row["year_month"]),
                    "tracker_index": tracker_idx,
                    "official_index": official_idx,
                    "tracker_mom": self._safe_float(row.get("tracker_mom")),
                    "official_mom": self._safe_float(row.get("official_mom")),
                    "tracker_status": row.get("tracker_status"),
                    "official_status": row.get("official_status"),
                    "official_source": row.get("official_source"),
                    "tracker_index_base100": tracker_base100_strict,
                    "official_index_base100": official_base100_strict,
                    "plot_tracker_base100": plot_tracker_base100,
                    "plot_official_base100": plot_official_base100,
                    "plot_mode": plot_mode,
                    "tracker_base_month": tracker_base_month_strict if strict_comparable else tracker_base_month_independent,
                    "official_base_month": official_base_month_strict if strict_comparable else official_base_month_independent,
                    "is_strictly_comparable": bool(strict_comparable),
                    "gap_index_points": (
                        tracker_base100_strict - official_base100_strict
                        if tracker_base100_strict is not None and official_base100_strict is not None
                        else None
                    ),
                    "gap_mom_pp": (
                        self._safe_float(row.get("tracker_mom")) - self._safe_float(row.get("official_mom"))
                        if self._safe_float(row.get("tracker_mom")) is not None
                        and self._safe_float(row.get("official_mom")) is not None
                        else None
                    ),
                }
            )
        return out

    def _build_category_comparison_series(
        self,
        tracker_cat_df: pd.DataFrame,
        official_cat_df: pd.DataFrame,
    ) -> List[Dict[str, Any]]:
        if tracker_cat_df.empty and official_cat_df.empty:
            return []

        tracker = tracker_cat_df.copy()
        tracker = tracker.rename(
            columns={
                "index_value": "tracker_index",
                "mom_change": "tracker_mom",
                "yoy_change": "tracker_yoy",
                "status": "tracker_status",
            }
        )
        tracker["category_slug"] = tracker["category_slug"].fillna("sin_categoria").astype(str)
        tracker["year_month"] = tracker["year_month"].astype(str)
        tracker = tracker[tracker["indec_division_code"].notna()].copy()
        tracker["indec_division_code"] = tracker["indec_division_code"].astype(str).str.strip().str.lower()
        tracker = tracker[tracker["indec_division_code"] != ""]

        official = official_cat_df.copy()
        if not official.empty:
            official = official.rename(
                columns={
                    "metric_code": "indec_division_code",
                    "cpi_index": "official_index",
                    "cpi_mom": "official_mom",
                    "cpi_yoy": "official_yoy",
                    "status": "official_status",
                    "source": "official_source",
                }
            )
            official["year_month"] = official["year_month"].astype(str)
            official["indec_division_code"] = official["indec_division_code"].astype(str).str.strip().str.lower()
            if "official_status" not in official.columns:
                official["official_status"] = "final"
            if "official_source" not in official.columns:
                official["official_source"] = "legacy_csv"
            for col in ("official_index", "official_mom", "official_yoy"):
                if col in official.columns:
                    official[col] = pd.to_numeric(official[col], errors="coerce")
        if "tracker_status" not in tracker.columns:
            tracker["tracker_status"] = pd.NA

        out: List[Dict[str, Any]] = []
        categories = sorted(tracker["category_slug"].unique().tolist() if not tracker.empty else [])
        for category in categories:
            t = tracker[tracker["category_slug"] == category].copy()
            if t.empty:
                continue
            division = str(t["indec_division_code"].dropna().iloc[0]).strip().lower()
            o = (
                official[official["indec_division_code"] == division].copy()
                if not official.empty
                else pd.DataFrame()
            )
            months = sorted(
                set(t["year_month"].astype(str).tolist())
                | set(o["year_month"].astype(str).tolist() if not o.empty else [])
            )
            tracker_by_month = {str(r["year_month"]): r for _, r in t.iterrows()}
            official_by_month = {str(r["year_month"]): r for _, r in o.iterrows()} if not o.empty else {}

            overlap = [
                m for m in months
                if self._safe_float(tracker_by_month.get(m, {}).get("tracker_index")) is not None
                and self._safe_float(official_by_month.get(m, {}).get("official_index")) is not None
            ]
            tracker_base = (
                self._safe_float(tracker_by_month.get(overlap[0], {}).get("tracker_index"))
                if overlap
                else None
            )
            official_base = (
                self._safe_float(official_by_month.get(overlap[0], {}).get("official_index"))
                if overlap
                else None
            )

            for month in months:
                tracker_row = tracker_by_month.get(month, {})
                official_row = official_by_month.get(month, {})
                tracker_idx = self._safe_float(tracker_row.get("tracker_index"))
                official_idx = self._safe_float(official_row.get("official_index"))
                tracker_base100 = None
                official_base100 = None
                if tracker_idx is not None and tracker_base and tracker_base > 0:
                    tracker_base100 = (tracker_idx / tracker_base) * 100.0
                if official_idx is not None and official_base and official_base > 0:
                    official_base100 = (official_idx / official_base) * 100.0
                out.append(
                    {
                        "category_slug": category,
                        "indec_division_code": division,
                        "year_month": str(month),
                        "tracker_index": tracker_idx,
                        "official_index": official_idx,
                        "tracker_mom": self._safe_float(tracker_row.get("tracker_mom")),
                        "official_mom": self._safe_float(official_row.get("official_mom")),
                        "tracker_status": tracker_row.get("tracker_status"),
                        "official_status": official_row.get("official_status"),
                        "official_source": official_row.get("official_source"),
                        "tracker_index_base100": tracker_base100,
                        "official_index_base100": official_base100,
                        "gap_index_points": (
                            tracker_base100 - official_base100
                            if tracker_base100 is not None and official_base100 is not None
                            else None
                        ),
                        "gap_mom_pp": (
                            self._safe_float(tracker_row.get("tracker_mom")) - self._safe_float(official_row.get("official_mom"))
                            if self._safe_float(tracker_row.get("tracker_mom")) is not None
                            and self._safe_float(official_row.get("official_mom")) is not None
                            else None
                        ),
                    }
                )
        return out

    def _load_official_category_series(self, from_month: str, to_month: str, region: str) -> pd.DataFrame:
        repository = SeriesRepository(self.session)
        try:
            rows, _ = repository.get_official_ipc_patagonia(
                start_period=from_month,
                end_period=to_month,
                metric_code=None,
                region=region,
                page=1,
                page_size=10000,
            )
        except Exception:
            rows = []
        if not rows:
            return pd.DataFrame(columns=["year_month", "metric_code", "category_slug", "cpi_index", "cpi_mom", "cpi_yoy", "status", "source"])
        df = pd.DataFrame(rows)
        df = df[df["metric_code"].notna()].copy()
        df = df[df["metric_code"].astype(str).str.lower() != "general"]
        if df.empty:
            return pd.DataFrame(columns=["year_month", "metric_code", "category_slug", "cpi_index", "cpi_mom", "cpi_yoy", "status", "source"])
        df["year_month"] = df["year_month"].astype(str)
        df["metric_code"] = df["metric_code"].astype(str).str.strip().str.lower()
        df["category_slug"] = df["category_slug"].fillna(df["metric_code"]).astype(str).str.strip().str.lower()
        df["cpi_index"] = pd.to_numeric(df["index_value"], errors="coerce")
        df["cpi_mom"] = pd.to_numeric(df["mom_change"], errors="coerce")
        df["cpi_yoy"] = pd.to_numeric(df["yoy_change"], errors="coerce")
        df = df.rename(columns={"status": "status", "source": "source"})
        return df[["year_month", "metric_code", "category_slug", "cpi_index", "cpi_mom", "cpi_yoy", "status", "source"]].sort_values(
            ["metric_code", "year_month"]
        )

    def _official_regions(self) -> List[str]:
        cfg = self.config.get("analysis", {}).get("ipc_official", {})
        scope = cfg.get("region_scope")
        if isinstance(scope, list) and scope:
            regions = [str(r).strip().lower() for r in scope if str(r).strip()]
        else:
            regions = [str(cfg.get("region_default", "patagonia")).strip().lower()]
        regions = [r for r in regions if r not in {"", "all"}]
        if not regions:
            regions = ["patagonia", "nacional"]
        return sorted(set(regions))

    def _default_official_region(self) -> str:
        cfg = self.config.get("analysis", {}).get("ipc_official", {})
        preferred = str(cfg.get("region_default", "patagonia")).strip().lower()
        regions = self._official_regions()
        if preferred in regions:
            return preferred
        return regions[0] if regions else "patagonia"

    def _app_to_indec_mapping(self) -> Dict[str, Optional[str]]:
        global _LEGACY_MAPPING_WARNED
        mapping_cfg = self.config.get("analysis", {}).get("ipc_category_mapping", {})
        if not isinstance(mapping_cfg, dict):
            return {}
        explicit = mapping_cfg.get("app_to_indec_division")
        if isinstance(explicit, dict):
            return {str(k).strip().lower(): (str(v).strip().lower() if v else None) for k, v in explicit.items()}
        legacy = mapping_cfg.get("map")
        if isinstance(legacy, dict):
            if not _LEGACY_MAPPING_WARNED:
                logger.warning(
                    "Deprecated config path in use: analysis.ipc_category_mapping.map. "
                    "Use analysis.ipc_category_mapping.app_to_indec_division."
                )
                _LEGACY_MAPPING_WARNED = True
            return {str(k).strip().lower(): (str(v).strip().lower() if v else None) for k, v in legacy.items()}
        return {}

    def _mapping_metadata(self, basket_type: str) -> Dict[str, Any]:
        expected = self._expected_products_by_category(basket_type)
        mapping = self._app_to_indec_mapping()
        mapped_categories = sorted([cat for cat, target in mapping.items() if target])
        unmapped_categories = sorted([cat for cat in expected.keys() if not mapping.get(cat)])
        total_expected = int(sum(expected.values()))
        mapped_expected = int(sum(v for cat, v in expected.items() if mapping.get(cat)))
        mapped_coverage_pct = (mapped_expected / total_expected * 100.0) if total_expected > 0 else None
        return {
            "mapped_categories": mapped_categories,
            "unmapped_categories": unmapped_categories,
            "mapped_coverage_pct": mapped_coverage_pct,
            "mapped_expected_products": mapped_expected,
            "total_expected_products": total_expected,
        }

    @staticmethod
    def _is_placeholder_adsense_client(client_id: str) -> bool:
        value = str(client_id or "").strip().lower()
        if not value:
            return True
        return "xxxxxxxx" in value or value in {"ca-pub-test", "ca-pub-0000000000000000"}

    def _ads_payload(self) -> Dict[str, Any]:
        cfg = self.config.get("ads", {})
        if not isinstance(cfg, dict):
            cfg = {}
        slots_raw = cfg.get("slots") or ["header", "inline", "sidebar", "footer"]
        slots = [str(slot).strip().lower() for slot in slots_raw if str(slot).strip()]
        if not slots:
            slots = ["header", "inline", "sidebar", "footer"]
        provider = str(cfg.get("provider", "adsense"))
        client_id = str(cfg.get("client_id") or cfg.get("client_id_placeholder") or "ca-pub-xxxxxxxxxxxxxxxx")
        enabled = bool(cfg.get("enabled", False))
        if provider.lower() == "adsense" and self._is_placeholder_adsense_client(client_id):
            enabled = False
        return {
            "enabled": enabled,
            "provider": provider,
            "client_id": client_id,
            "client_id_placeholder": client_id,
            "slots": slots,
        }

    def _analytics_payload(self) -> Dict[str, Any]:
        analytics_cfg = self.config.get("analytics", {})
        if not isinstance(analytics_cfg, dict):
            analytics_cfg = {}
        plausible_cfg = analytics_cfg.get("plausible", {})
        if not isinstance(plausible_cfg, dict):
            plausible_cfg = {}
        return {
            "enabled": bool(plausible_cfg.get("enabled", False)),
            "domain": str(plausible_cfg.get("domain", "")).strip(),
            "script_url": str(plausible_cfg.get("script_url", "https://plausible.io/js/script.js")).strip(),
        }

    def _public_base_url(self) -> str:
        deployment_cfg = self.config.get("deployment", {})
        if not isinstance(deployment_cfg, dict):
            deployment_cfg = {}
        base_url = str(deployment_cfg.get("public_base_url", "https://preciosushuaia.com")).strip().rstrip("/")
        return base_url or "https://preciosushuaia.com"

    def _premium_placeholders_payload(self) -> Dict[str, Any]:
        cfg = self.config.get("premium_placeholders", {})
        if not isinstance(cfg, dict):
            cfg = {}
        features_raw = cfg.get("features") or [
            "Alertas de precio personalizadas",
            "Descarga avanzada CSV/API",
            "Comparador multi-zona",
            "Panel Pro sin anuncios",
        ]
        features = [str(item).strip() for item in features_raw if str(item).strip()]
        return {
            "enabled": bool(cfg.get("enabled", True)),
            "features": features,
        }

    def _next_update_eta(self) -> str:
        deployment_cfg = self.config.get("deployment", {})
        if not isinstance(deployment_cfg, dict):
            deployment_cfg = {}
        schedule_utc = str(deployment_cfg.get("schedule_utc", "09:10"))
        hour = 9
        minute = 10
        if re.match(r"^\d{2}:\d{2}$", schedule_utc):
            hour = int(schedule_utc.split(":", 1)[0])
            minute = int(schedule_utc.split(":", 1)[1])
        now = datetime.now(timezone.utc)
        candidate = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if candidate <= now:
            candidate = candidate + timedelta(days=1)
        return candidate.strftime("%Y-%m-%d %H:%M:%S UTC")

    def _web_status_payload(self, source_df: pd.DataFrame, quality_flags: Dict[str, Any]) -> Dict[str, Any]:
        deployment_cfg = self.config.get("deployment", {})
        if not isinstance(deployment_cfg, dict):
            deployment_cfg = {}
        fresh_max_hours = float(deployment_cfg.get("fresh_max_hours", 36))
        now = datetime.now(timezone.utc)
        last_data_timestamp = None
        age_hours = None

        if not source_df.empty and "scraped_at" in source_df.columns and source_df["scraped_at"].notna().any():
            last_ts = source_df["scraped_at"].max()
            if isinstance(last_ts, pd.Timestamp):
                if last_ts.tzinfo is None:
                    last_ts = last_ts.tz_localize("UTC")
                else:
                    last_ts = last_ts.tz_convert("UTC")
                last_data_timestamp = last_ts.isoformat()
                age_hours = (now - last_ts.to_pydatetime()).total_seconds() / 3600.0

        is_stale = bool(age_hours is None or age_hours >= fresh_max_hours)
        is_partial = bool(quality_flags.get("is_partial", False))
        if is_partial:
            status = "partial"
        elif is_stale:
            status = "stale"
        else:
            status = "fresh"
        return {
            "web_status": status,
            "is_stale": is_stale,
            "data_age_hours": age_hours,
            "last_data_timestamp": last_data_timestamp,
            "next_update_eta": self._next_update_eta(),
        }

    @staticmethod
    def _latest_month_row(df: pd.DataFrame, value_col: str, status_col: str) -> Tuple[Optional[str], Optional[str]]:
        if df.empty or "year_month" not in df.columns:
            return None, None
        working = df.copy()
        working["year_month"] = working["year_month"].astype(str)
        if value_col in working.columns:
            non_null = working[working[value_col].notna()].sort_values("year_month")
            if not non_null.empty:
                row = non_null.iloc[-1]
                return str(row.get("year_month")), (None if status_col not in non_null.columns else row.get(status_col))
        row = working.sort_values("year_month").iloc[-1]
        return str(row.get("year_month")), (None if status_col not in working.columns else row.get(status_col))

    def _load_publication_status(
        self,
        basket_type: str,
        region: str,
        tracker_df: Optional[pd.DataFrame] = None,
        official_df: Optional[pd.DataFrame] = None,
    ) -> Dict[str, Any]:
        tracker_df = tracker_df if isinstance(tracker_df, pd.DataFrame) else pd.DataFrame()
        official_df = official_df if isinstance(official_df, pd.DataFrame) else pd.DataFrame()
        latest_tracker_month, latest_tracker_status = self._latest_month_row(
            tracker_df,
            value_col="index_value",
            status_col="status",
        )
        latest_official_month, latest_official_status = self._latest_month_row(
            official_df,
            value_col="cpi_index",
            status_col="status",
        )

        repository = SeriesRepository(self.session)
        try:
            latest = repository.get_latest_ipc_publication_status(basket_type=basket_type, region=region)
        except Exception:
            latest = None
        if not latest:
            if latest_tracker_month or latest_official_month:
                if latest_tracker_month and latest_official_month:
                    derived_status = "derived_no_publication_run"
                elif latest_tracker_month:
                    derived_status = "derived_official_missing"
                else:
                    derived_status = "derived_tracker_missing"
            else:
                derived_status = "derived_no_series"
            return {
                "has_publication": False,
                "status": derived_status,
                "status_origin": "derived_from_series",
                "region": region,
                "latest_tracker_month": latest_tracker_month,
                "latest_tracker_status": latest_tracker_status,
                "latest_official_month": latest_official_month,
                "latest_official_status": latest_official_status,
                "warnings": [],
                "metrics": {},
                "official_source_effective": None,
                "validation_status": "not_available",
                "source_document_url": None,
            }
        warnings_raw = latest.get("warnings_json")
        metrics_raw = latest.get("metrics_json")
        warnings = []
        metrics = {}
        try:
            warnings = json.loads(warnings_raw) if warnings_raw else []
        except Exception:
            warnings = []
        try:
            metrics = json.loads(metrics_raw) if metrics_raw else {}
        except Exception:
            metrics = {}
        return {
            "has_publication": True,
            "run_uuid": latest.get("run_uuid"),
            "status": latest.get("status"),
            "basket_type": latest.get("basket_type"),
            "region": latest.get("region"),
            "method_version": latest.get("method_version"),
            "from_month": latest.get("from_month"),
            "to_month": latest.get("to_month"),
            "official_rows": latest.get("official_rows"),
            "tracker_rows": latest.get("tracker_rows"),
            "tracker_category_rows": latest.get("tracker_category_rows"),
            "overlap_months": latest.get("overlap_months"),
            "warnings": warnings,
            "metrics": metrics,
            "started_at": str(latest.get("started_at")) if latest.get("started_at") is not None else None,
            "completed_at": str(latest.get("completed_at")) if latest.get("completed_at") is not None else None,
            "official_source_effective": metrics.get("official_source_effective") or latest.get("official_source"),
            "validation_status": metrics.get("official_validation_status", "not_available"),
            "source_document_url": metrics.get("official_source_document_url"),
            "status_origin": "publication_run",
            "latest_tracker_month": latest_tracker_month,
            "latest_tracker_status": latest_tracker_status,
            "latest_official_month": latest_official_month,
            "latest_official_status": latest_official_status,
        }

    def _expected_catalog(self, basket_type: str) -> Tuple[Dict[str, int], Dict[str, str], set[str]]:
        expected_by_category: Dict[str, int] = {}
        expected_category_by_id: Dict[str, str] = {}
        expected_ids: set[str] = set()
        for item in get_basket_items(self.config, basket_type):
            canonical_id = str(item.get("id") or "").strip()
            if not canonical_id:
                continue
            raw = item.get("category") or "sin_categoria"
            category = resolve_canonical_category(self.config, raw) or str(raw).strip().lower()
            expected_by_category[category] = expected_by_category.get(category, 0) + 1
            expected_category_by_id[canonical_id] = category
            expected_ids.add(canonical_id)
        return expected_by_category, expected_category_by_id, expected_ids

    def _expected_products_by_category(self, basket_type: str) -> Dict[str, int]:
        expected: Dict[str, int] = {}
        expected, _, _ = self._expected_catalog(basket_type)
        return expected

    def _coverage_metrics(self, df: pd.DataFrame, from_month: str, to_month: str, basket_type: str) -> Dict[str, Any]:
        expected_by_category, expected_category_by_id, expected_ids = self._expected_catalog(basket_type)
        expected_products = sum(expected_by_category.values())
        safe_expected = expected_products if expected_products > 0 else 1

        observed_ids_total = (
            {str(v) for v in df["canonical_id"].dropna().astype(str).unique().tolist()}
            if not df.empty
            else set()
        )
        observed_ids_from = (
            {str(v) for v in df[df["month"] == from_month]["canonical_id"].dropna().astype(str).unique().tolist()}
            if not df.empty
            else set()
        )
        observed_ids_to = (
            {str(v) for v in df[df["month"] == to_month]["canonical_id"].dropna().astype(str).unique().tolist()}
            if not df.empty
            else set()
        )

        observed_expected_total = observed_ids_total & expected_ids
        observed_expected_from = observed_ids_from & expected_ids
        observed_expected_to = observed_ids_to & expected_ids
        observed_unexpected_ids = observed_ids_total - expected_ids

        by_category = []
        for category, exp in expected_by_category.items():
            ids_cat = {cid for cid, cat in expected_category_by_id.items() if cat == category}
            observed_cat = len(ids_cat & observed_expected_total)
            by_category.append(
                {
                    "category": category,
                    "expected_products": int(exp),
                    "observed_products": int(observed_cat),
                    "coverage_pct": (int(observed_cat) / int(exp) * 100.0) if int(exp) > 0 else None,
                }
            )
        by_category = sorted(by_category, key=lambda item: item["observed_products"], reverse=True)

        unexpected_by_category: List[Dict[str, Any]] = []
        if not df.empty and observed_unexpected_ids:
            unexpected_df = df[df["canonical_id"].astype(str).isin(observed_unexpected_ids)].copy()
            grouped_unexpected = unexpected_df.groupby("category")["canonical_id"].nunique().sort_values(ascending=False)
            for category, observed in grouped_unexpected.items():
                unexpected_by_category.append(
                    {
                        "category": str(category),
                        "observed_products": int(observed),
                    }
                )
        return {
            "basket_type": basket_type,
            "expected_products": expected_products,
            "expected_products_by_category": expected_by_category,
            "observed_products_total": int(len(observed_expected_total)),
            "observed_products_total_raw": int(len(observed_ids_total)),
            "unexpected_observed_products": int(len(observed_unexpected_ids)),
            "coverage_total_pct": (len(observed_expected_total) / safe_expected) * 100,
            "observed_from": int(len(observed_expected_from)),
            "observed_to": int(len(observed_expected_to)),
            "coverage_from_pct": (len(observed_expected_from) / safe_expected) * 100,
            "coverage_to_pct": (len(observed_expected_to) / safe_expected) * 100,
            "coverage_by_category": by_category,
            "unexpected_by_category": unexpected_by_category,
        }

    def _scrape_quality_summary(
        self,
        df: pd.DataFrame,
        basket_type: str,
        candidate_band_summary: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        planning_cfg = self.config.get("scraping", {}).get("planning", {})
        candidates_cfg = self.config.get("scraping", {}).get("candidates", {})
        storage_mode = str(candidates_cfg.get("storage_mode", "json")).lower()
        if storage_mode not in {"json", "db", "off"}:
            storage_mode = "json"
        observation_policy = "single+audit" if storage_mode in {"json", "db"} else "single"

        basket_items = get_basket_items(self.config, basket_type)
        available_ids = {str(item.get("id")) for item in basket_items if item.get("id")}
        cba_ids = {
            str(item.get("id"))
            for item in basket_items
            if item.get("id") and str(item.get("basket_type", "")) == "cba"
        }
        daily_core_ids = {
            str(item_id)
            for item_id in (planning_cfg.get("daily_core_ids") or [])
            if str(item_id) in available_ids
        }
        daily_rotation_ids = {
            str(item_id)
            for item_id in (planning_cfg.get("daily_rotation_ids") or [])
            if str(item_id) in available_ids
        }
        observed_ids = {str(v) for v in df["canonical_id"].dropna().astype(str).unique().tolist()} if not df.empty else set()

        def _mk(expected_ids: set[str]) -> Dict[str, Any]:
            expected = len(expected_ids)
            observed = len(expected_ids & observed_ids)
            pct = (observed / expected * 100.0) if expected > 0 else None
            return {
                "expected": expected,
                "observed": observed,
                "coverage_pct": pct,
            }

        cba = _mk(cba_ids)
        core = _mk(daily_core_ids)
        rotation = _mk(daily_rotation_ids)

        return {
            "observation_policy": observation_policy,
            "candidate_storage_mode": storage_mode,
            "tier_rule_target": max(3, int(candidates_cfg.get("min_candidates_per_product", 3))),
            "products_with_bands": int((candidate_band_summary or {}).get("products_with_bands", 0)),
            "products_with_full_terna": int((candidate_band_summary or {}).get("products_with_full_terna", 0)),
            "terna_compliance_pct": self._safe_float((candidate_band_summary or {}).get("terna_compliance_pct")),
            "cba": cba,
            "daily_core": core,
            "daily_rotation": rotation,
            "method_badge": "representativo + terna auditada" if observation_policy == "single+audit" else "representativo",
            "price_candidates_ready": storage_mode in {"json", "db"},
        }

    def _compute_inflation_total_pct(self, df: pd.DataFrame, from_month: str, to_month: str) -> Optional[float]:
        if df.empty:
            return None
        monthly = df.groupby("month", as_index=False)["current_price"].mean()
        from_avg = monthly.loc[monthly["month"] == from_month, "current_price"]
        to_avg = monthly.loc[monthly["month"] == to_month, "current_price"]
        if from_avg.empty or to_avg.empty or from_avg.iloc[0] <= 0:
            return None
        return float(((to_avg.iloc[0] - from_avg.iloc[0]) / from_avg.iloc[0]) * 100)

    @staticmethod
    def _safe_float(value: Any) -> Optional[float]:
        if value is None or pd.isna(value):
            return None
        return float(value)

    def _variation_between_months(
        self,
        grouped_df: pd.DataFrame,
        group_col: str,
        from_month: str,
        to_month: str,
    ) -> pd.DataFrame:
        from_df = grouped_df[grouped_df["month"] == from_month][[group_col, "current_price"]].rename(
            columns={"current_price": "price_from"}
        )
        to_df = grouped_df[grouped_df["month"] == to_month][[group_col, "current_price"]].rename(
            columns={"current_price": "price_to"}
        )
        merged = from_df.merge(to_df, on=group_col, how="inner")
        if merged.empty:
            return merged
        valid = merged["price_from"].notna() & (merged["price_from"] > 0)
        merged["variation_pct"] = pd.NA
        merged.loc[valid, "variation_pct"] = (
            (merged.loc[valid, "price_to"] - merged.loc[valid, "price_from"])
            / merged.loc[valid, "price_from"]
        ) * 100
        return merged.sort_values("variation_pct", ascending=False, na_position="last")

    def _base_cpi_index(self, ipc_df: pd.DataFrame, preferred_month: str) -> Tuple[Optional[float], Optional[str]]:
        if ipc_df.empty:
            return None, None
        cpi_map = {row["year_month"]: self._safe_float(row["cpi_index"]) for _, row in ipc_df.iterrows()}
        preferred_value = cpi_map.get(preferred_month)
        if preferred_value and preferred_value > 0:
            return preferred_value, preferred_month
        for _, row in ipc_df.sort_values("year_month").iterrows():
            value = self._safe_float(row["cpi_index"])
            if value and value > 0:
                return value, str(row["year_month"])
        return None, None

    def _compute_real_prices(
        self,
        base_df: pd.DataFrame,
        ipc_df: pd.DataFrame,
        preferred_base_month: str,
    ) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        out = base_df.copy()
        if out.empty:
            out["real_price"] = pd.NA
            return out, {"base_month": None, "base_cpi_index": None, "missing_cpi_months": []}

        cpi_map = {str(row["year_month"]): self._safe_float(row["cpi_index"]) for _, row in ipc_df.iterrows()}
        base_cpi, base_month = self._base_cpi_index(ipc_df, preferred_base_month)
        if not base_cpi:
            out["real_price"] = pd.NA
            missing = sorted(out["month"].dropna().astype(str).unique().tolist())
            return out, {"base_month": None, "base_cpi_index": None, "missing_cpi_months": missing}

        out["cpi_index"] = out["month"].map(cpi_map)
        out["real_price"] = pd.NA
        valid = out["cpi_index"].notna() & (out["cpi_index"] > 0)
        out.loc[valid, "real_price"] = (out.loc[valid, "current_price"] * base_cpi) / out.loc[valid, "cpi_index"]

        observed_months = set(out["month"].astype(str).unique().tolist())
        missing_months = sorted([month for month in observed_months if not cpi_map.get(month)])
        return out, {"base_month": base_month, "base_cpi_index": base_cpi, "missing_cpi_months": missing_months}

    def _build_balanced_panel(self, monthly_df: pd.DataFrame, from_month: str, to_month: str) -> pd.DataFrame:
        if monthly_df.empty:
            return pd.DataFrame(
                columns=[
                    "canonical_id",
                    "price_from",
                    "price_to",
                    "real_from",
                    "real_to",
                    "nominal_var_pct",
                    "real_var_pct",
                ]
            )

        from_df = monthly_df[monthly_df["month"] == from_month][
            ["canonical_id", "avg_price", "avg_real_price"]
        ].rename(columns={"avg_price": "price_from", "avg_real_price": "real_from"})
        to_df = monthly_df[monthly_df["month"] == to_month][["canonical_id", "avg_price", "avg_real_price"]].rename(
            columns={"avg_price": "price_to", "avg_real_price": "real_to"}
        )
        panel = from_df.merge(to_df, on="canonical_id", how="inner")
        if panel.empty:
            return panel

        panel["nominal_var_pct"] = pd.NA
        valid_nom = panel["price_from"].notna() & (panel["price_from"] > 0)
        panel.loc[valid_nom, "nominal_var_pct"] = (
            (panel.loc[valid_nom, "price_to"] - panel.loc[valid_nom, "price_from"]) / panel.loc[valid_nom, "price_from"]
        ) * 100

        panel["real_var_pct"] = pd.NA
        valid_real = panel["real_from"].notna() & (panel["real_from"] > 0) & panel["real_to"].notna()
        panel.loc[valid_real, "real_var_pct"] = (
            (panel.loc[valid_real, "real_to"] - panel.loc[valid_real, "real_from"]) / panel.loc[valid_real, "real_from"]
        ) * 100
        return panel

    def _compute_economic_kpis(
        self,
        monthly_df: pd.DataFrame,
        ipc_df: pd.DataFrame,
        balanced_panel: pd.DataFrame,
        from_month: str,
        to_month: str,
    ) -> Dict[str, Any]:
        effective_from = from_month
        effective_to = to_month
        fallback_used = False
        single_month_fallback = False

        if balanced_panel.empty and not monthly_df.empty:
            available_months = sorted(monthly_df["month"].dropna().astype(str).unique().tolist())
            if len(available_months) >= 2:
                effective_from = available_months[0]
                effective_to = available_months[-1]
                balanced_panel = self._build_balanced_panel(
                    monthly_df,
                    from_month=effective_from,
                    to_month=effective_to,
                )
                fallback_used = True
            elif len(available_months) == 1:
                effective_from = available_months[0]
                effective_to = available_months[0]
                fallback_used = True
                single_month_fallback = True

        nominal_pct: Optional[float] = None
        real_pct: Optional[float] = None
        amplitude_pct: Optional[float] = None
        dispersion_iqr_pct: Optional[float] = None

        if not balanced_panel.empty:
            nominal_from = balanced_panel["price_from"].mean()
            nominal_to = balanced_panel["price_to"].mean()
            if nominal_from and nominal_from > 0:
                nominal_pct = float(((nominal_to / nominal_from) - 1) * 100)

            real_valid = balanced_panel.dropna(subset=["real_from", "real_to"])
            if not real_valid.empty:
                real_from = real_valid["real_from"].mean()
                real_to = real_valid["real_to"].mean()
                if real_from and real_from > 0:
                    real_pct = float(((real_to / real_from) - 1) * 100)

            nominal_var = pd.to_numeric(balanced_panel["nominal_var_pct"], errors="coerce").dropna()
            if not nominal_var.empty:
                amplitude_pct = float((nominal_var > 0).mean() * 100)
                dispersion_iqr_pct = float(nominal_var.quantile(0.75) - nominal_var.quantile(0.25))

        ipc_period_pct: Optional[float] = None
        if not ipc_df.empty:
            cpi_map = {row["year_month"]: self._safe_float(row["cpi_index"]) for _, row in ipc_df.iterrows()}
            cpi_from = cpi_map.get(effective_from)
            cpi_to = cpi_map.get(effective_to)
            if cpi_from and cpi_to and cpi_from > 0:
                ipc_period_pct = float(((cpi_to / cpi_from) - 1) * 100)

        gap_pp = None
        if single_month_fallback and nominal_pct is None:
            nominal_pct = 0.0
        if single_month_fallback and real_pct is None:
            real_pct = 0.0
        if nominal_pct is not None and ipc_period_pct is not None:
            gap_pp = nominal_pct - ipc_period_pct
        elif single_month_fallback and nominal_pct is not None and ipc_period_pct is None:
            gap_pp = nominal_pct

        return {
            "inflation_basket_nominal_pct": nominal_pct,
            "ipc_period_pct": ipc_period_pct,
            "gap_vs_ipc_pp": gap_pp,
            "inflation_basket_real_pct": real_pct,
            "amplitude_up_pct": amplitude_pct,
            "dispersion_iqr_pct": dispersion_iqr_pct,
            "balanced_panel_n": int(balanced_panel["canonical_id"].nunique()) if not balanced_panel.empty else 0,
            "from_month": effective_from,
            "to_month": effective_to,
            "requested_from_month": from_month,
            "requested_to_month": to_month,
            "kpi_fallback_used": fallback_used,
        }

    def _build_quality_flags(
        self,
        coverage: Dict[str, Any],
        kpi_summary: Dict[str, Any],
        missing_cpi_months: List[str],
        min_coverage_threshold: float = 70.0,
    ) -> Dict[str, Any]:
        coverage_total = float(coverage.get("coverage_total_pct", 0.0))
        expected_products = int(coverage.get("expected_products", 0))
        balanced_panel_n = int(kpi_summary.get("balanced_panel_n", 0))
        min_panel = max(3, int(expected_products * 0.30)) if expected_products > 0 else 3

        coverage_warning = coverage_total < min_coverage_threshold
        panel_warning = balanced_panel_n < min_panel
        ipc_warning = len(missing_cpi_months) > 0

        warnings = []
        if coverage_warning:
            warnings.append(f"Cobertura baja ({coverage_total:.1f}% < {min_coverage_threshold:.0f}%).")
        if panel_warning:
            warnings.append(f"Panel balanceado reducido ({balanced_panel_n} productos).")
        if ipc_warning:
            warnings.append("Meses sin IPC para calculo real: " + ", ".join(missing_cpi_months))

        return {
            "coverage_warning": coverage_warning,
            "panel_warning": panel_warning,
            "ipc_warning": ipc_warning,
            "is_partial": coverage_warning or panel_warning or ipc_warning,
            "badge": "Datos parciales" if (coverage_warning or panel_warning or ipc_warning) else "Datos completos",
            "warnings": warnings,
            "missing_cpi_months": missing_cpi_months,
            "balanced_panel_n": balanced_panel_n,
            "coverage_total_pct": coverage_total,
            "min_coverage_threshold_pct": min_coverage_threshold,
        }

    def _downsample_timeline(self, timeline_df: pd.DataFrame, max_points_per_product: int = 240) -> pd.DataFrame:
        if timeline_df.empty:
            return timeline_df
        sampled: List[pd.DataFrame] = []
        for _, group in timeline_df.groupby("canonical_id", sort=False):
            group = group.sort_values("scraped_at")
            total = len(group)
            if total <= max_points_per_product:
                sampled.append(group)
                continue
            step = max(1, total // max_points_per_product)
            idx = list(range(0, total, step))
            if idx[-1] != total - 1:
                idx.append(total - 1)
            sampled.append(group.iloc[idx].drop_duplicates(subset=["scraped_at"], keep="last"))
        return pd.concat(sampled, ignore_index=True)

    def _build_candidate_bands(
        self,
        candidate_df: pd.DataFrame,
        snapshot_df: pd.DataFrame,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        if candidate_df.empty:
            return [], {
                "has_candidate_data": False,
                "products_with_bands": 0,
                "products_with_full_terna": 0,
                "terna_compliance_pct": None,
                "points": 0,
            }

        work = candidate_df.copy()
        work["day"] = work["scraped_at"].dt.floor("D")
        latest_by_day = (
            work.sort_values("scraped_at")
            .groupby(["canonical_id", "day", "tier"], as_index=False)
            .tail(1)
        )
        pivot = (
            latest_by_day.pivot_table(
                index=["canonical_id", "day"],
                columns="tier",
                values="candidate_price",
                aggfunc="first",
            )
            .reset_index()
            .rename(columns={"day": "scraped_at"})
        )
        for col in ("low", "mid", "high"):
            if col not in pivot.columns:
                pivot[col] = pd.NA

        product_name_map = {}
        if not snapshot_df.empty:
            product_name_map = (
                snapshot_df[["canonical_id", "product_name"]]
                .dropna()
                .drop_duplicates(subset=["canonical_id"], keep="last")
                .set_index("canonical_id")["product_name"]
                .to_dict()
            )

        pivot["product_name"] = pivot["canonical_id"].astype(str)
        if product_name_map:
            mapped_name = pivot["canonical_id"].map(product_name_map)
            pivot["product_name"] = mapped_name.fillna(pivot["product_name"])
        missing_name = pivot["product_name"].isna()
        if missing_name.any():
            fallback_map = (
                latest_by_day[["canonical_id", "product_name"]]
                .dropna()
                .drop_duplicates(subset=["canonical_id"], keep="last")
                .set_index("canonical_id")["product_name"]
                .to_dict()
            )
            mapped_fallback = pivot.loc[missing_name, "canonical_id"].map(fallback_map)
            pivot.loc[missing_name, "product_name"] = mapped_fallback.fillna(
                pivot.loc[missing_name, "canonical_id"].astype(str)
            )
        pivot["product_name"] = pivot["product_name"].fillna(pivot["canonical_id"].astype(str)).astype(str)

        pivot["spread_pct"] = pd.NA
        valid_spread = (
            pivot["mid"].notna()
            & (pivot["mid"] > 0)
            & pivot["low"].notna()
            & pivot["high"].notna()
        )
        pivot.loc[valid_spread, "spread_pct"] = (
            (pivot.loc[valid_spread, "high"] - pivot.loc[valid_spread, "low"]) / pivot.loc[valid_spread, "mid"]
        ) * 100

        pivot = pivot.sort_values(["canonical_id", "scraped_at"])
        rows = [
            {
                "canonical_id": row["canonical_id"],
                "product_name": row["product_name"],
                "scraped_at": pd.Timestamp(row["scraped_at"]).isoformat(),
                "low_price": self._safe_float(row.get("low")),
                "mid_price": self._safe_float(row.get("mid")),
                "high_price": self._safe_float(row.get("high")),
                "spread_pct": self._safe_float(row.get("spread_pct")),
            }
            for _, row in pivot.iterrows()
        ]

        by_product = pivot.groupby("canonical_id")
        products_with_bands = int(by_product.ngroups)
        products_full = int(
            (
                by_product["low"].apply(lambda s: s.notna().any())
                & by_product["mid"].apply(lambda s: s.notna().any())
                & by_product["high"].apply(lambda s: s.notna().any())
            ).sum()
        )
        terna_compliance = None
        if products_with_bands > 0:
            terna_compliance = (products_full / products_with_bands) * 100.0

        summary = {
            "has_candidate_data": products_with_bands > 0,
            "products_with_bands": products_with_bands,
            "products_with_full_terna": products_full,
            "terna_compliance_pct": terna_compliance,
            "points": len(rows),
        }
        return rows, summary

    def _build_interactive_payload(
        self,
        df: pd.DataFrame,
        from_month: str,
        to_month: str,
        basket_type: str,
        benchmark_mode: str = "ipc",
        analysis_depth: str = "executive",
    ) -> Dict[str, Any]:
        months = self._month_sequence(from_month, to_month)
        regions = self._official_regions()
        default_region = self._default_official_region()
        mapping_meta = self._mapping_metadata(basket_type)
        ipc_by_region: Dict[str, pd.DataFrame] = {}
        official_cat_by_region: Dict[str, pd.DataFrame] = {}
        comparison_by_region: Dict[str, List[Dict[str, Any]]] = {}
        category_comparison_by_region: Dict[str, List[Dict[str, Any]]] = {}

        tracker_ipc_df = self._load_tracker_ipc_general(from_month, to_month, basket_type)
        tracker_ipc_cat_df = self._load_tracker_ipc_categories(from_month, to_month, basket_type)
        mapped_categories = set(mapping_meta.get("mapped_categories", []))
        tracker_ipc_cat_df = tracker_ipc_cat_df[
            tracker_ipc_cat_df["category_slug"].astype(str).isin(mapped_categories)
        ].copy() if not tracker_ipc_cat_df.empty else tracker_ipc_cat_df

        for region in regions:
            ipc_region_df = self._load_ipc_data(
                from_month,
                to_month,
                benchmark_mode=benchmark_mode,
                region=region,
            )
            ipc_by_region[region] = ipc_region_df
            official_cat_df = self._load_official_category_series(from_month, to_month, region=region)
            official_cat_by_region[region] = official_cat_df
            comparison_by_region[region] = self._build_ipc_comparison_series(tracker_ipc_df, ipc_region_df)
            category_comparison_by_region[region] = self._build_category_comparison_series(
                tracker_ipc_cat_df,
                official_cat_df,
            )

        ipc_df = ipc_by_region.get(default_region, pd.DataFrame(columns=["year_month", "cpi_index", "cpi_mom", "cpi_yoy", "status", "source"]))
        ipc_comparison_series = comparison_by_region.get(default_region, [])
        category_comparison_series = category_comparison_by_region.get(default_region, [])
        macro_categories = sorted(
            {
                str(r.get("category_slug"))
                for rows in category_comparison_by_region.values()
                for r in rows
                if r.get("category_slug")
            }
        )
        publication_status_by_region = {
            region: self._load_publication_status(
                basket_type,
                region,
                tracker_df=tracker_ipc_df,
                official_df=ipc_by_region.get(region, pd.DataFrame()),
            )
            for region in regions
        }
        publication_status = publication_status_by_region.get(default_region, {})
        coverage = self._coverage_metrics(df, from_month, to_month, basket_type)
        candidate_df = self._load_candidate_rows(from_month, to_month, basket_type)
        candidate_bands, candidate_band_summary = self._build_candidate_bands(candidate_df, pd.DataFrame())
        scrape_quality = self._scrape_quality_summary(
            df,
            basket_type,
            candidate_band_summary=candidate_band_summary,
        )
        ads_payload = self._ads_payload()
        analytics_payload = self._analytics_payload()
        premium_payload = self._premium_placeholders_payload()

        if df.empty:
            quality_flags = self._build_quality_flags(
                coverage=coverage,
                kpi_summary={"balanced_panel_n": 0},
                missing_cpi_months=months if benchmark_mode == "ipc" else [],
            )
            web_payload = self._web_status_payload(df, quality_flags)
            return {
                "ui_version": 2,
                "from_month": from_month,
                "to_month": to_month,
                "basket_type": basket_type,
                "benchmark_mode": benchmark_mode,
                "analysis_depth": analysis_depth,
                "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
                "has_data": False,
                "timeline": [],
                "snapshot": [],
                "monthly_reference": [],
                "ipc_series": [
                    {
                        "year_month": row["year_month"],
                        "cpi_index": self._safe_float(row["cpi_index"]),
                        "cpi_mom": self._safe_float(row["cpi_mom"]),
                        "cpi_yoy": self._safe_float(row["cpi_yoy"]),
                        "status": row.get("status"),
                        "source": row.get("source"),
                    }
                    for _, row in ipc_df.iterrows()
                ],
                "tracker_ipc_series": [
                    {
                        "year_month": row["year_month"],
                        "index_value": self._safe_float(row.get("index_value")),
                        "mom_change": self._safe_float(row.get("mom_change")),
                        "yoy_change": self._safe_float(row.get("yoy_change")),
                        "status": row.get("status"),
                        "coverage_weight_pct": self._safe_float(row.get("coverage_weight_pct")),
                        "coverage_product_pct": self._safe_float(row.get("coverage_product_pct")),
                        "method_version": row.get("method_version"),
                        "base_month": row.get("base_month"),
                    }
                    for _, row in tracker_ipc_df.iterrows()
                ],
                "official_patagonia_series": [
                    {
                        "year_month": row["year_month"],
                        "cpi_index": self._safe_float(row["cpi_index"]),
                        "cpi_mom": self._safe_float(row["cpi_mom"]),
                        "cpi_yoy": self._safe_float(row["cpi_yoy"]),
                        "status": row.get("status"),
                        "source": row.get("source"),
                    }
                    for _, row in ipc_df.iterrows()
                ],
                "official_series_by_region": {
                    region: [
                        {
                            "year_month": row["year_month"],
                            "cpi_index": self._safe_float(row["cpi_index"]),
                            "cpi_mom": self._safe_float(row["cpi_mom"]),
                            "cpi_yoy": self._safe_float(row["cpi_yoy"]),
                            "status": row.get("status"),
                            "source": row.get("source"),
                        }
                        for _, row in ipc_by_region.get(region, pd.DataFrame()).iterrows()
                    ]
                    for region in regions
                },
                "official_category_series_by_region": {
                    region: [
                        {
                            "year_month": row["year_month"],
                            "metric_code": row.get("metric_code"),
                            "cpi_index": self._safe_float(row["cpi_index"]),
                            "cpi_mom": self._safe_float(row["cpi_mom"]),
                            "cpi_yoy": self._safe_float(row["cpi_yoy"]),
                            "status": row.get("status"),
                            "source": row.get("source"),
                        }
                        for _, row in official_cat_by_region.get(region, pd.DataFrame()).iterrows()
                    ]
                    for region in regions
                },
                "ipc_comparison_series": ipc_comparison_series,
                "category_comparison_series": category_comparison_series,
                "ipc_comparison_by_region": comparison_by_region,
                "category_comparison_by_region": category_comparison_by_region,
                "publication_status": publication_status,
                "publication_status_by_region": publication_status_by_region,
                "official_regions": regions,
                "macro_default_region": default_region,
                "category_mapping_meta": mapping_meta,
                "product_monthly_metrics": [],
                "basket_vs_ipc_series": [],
                "kpi_summary": {
                    "inflation_basket_nominal_pct": None,
                    "ipc_period_pct": None,
                    "gap_vs_ipc_pp": None,
                    "inflation_basket_real_pct": None,
                    "amplitude_up_pct": None,
                    "dispersion_iqr_pct": None,
                    "balanced_panel_n": 0,
                    "from_month": from_month,
                    "to_month": to_month,
                    "requested_from_month": from_month,
                    "requested_to_month": to_month,
                    "kpi_fallback_used": False,
                },
                "quality_flags": quality_flags,
                "months": months,
                "categories": [],
                "coverage": coverage,
                "scrape_quality": scrape_quality,
                "candidate_bands": candidate_bands,
                "candidate_band_summary": candidate_band_summary,
                "ads": ads_payload,
                "analytics": analytics_payload,
                "premium_placeholders": premium_payload,
                "publication_policy": PUBLICATION_POLICY,
                "web_status": web_payload["web_status"],
                "is_stale": web_payload["is_stale"],
                "data_age_hours": web_payload["data_age_hours"],
                "last_data_timestamp": web_payload["last_data_timestamp"],
                "next_update_eta": web_payload["next_update_eta"],
                "ui_defaults": {
                    "query": "",
                    "cba_filter": "all",
                    "category": "all",
                    "sort_by": "alphabetical",
                    "base_month": months[0] if months else "",
                    "selected_products": [],
                    "price_mode": "nominal",
                    "show_real_column": False,
                    "macro_scope": "general",
                    "macro_region": default_region,
                    "macro_category": macro_categories[0] if macro_categories else "",
                    "view": analysis_depth,
                    "band_product": "",
                    "page_size": 25,
                    "current_page": 1,
                },
                "filters_available": {
                    "cba_filter": ["all", "yes", "no"],
                    "categories": [],
                    "months": months,
                    "sort_by": ["alphabetical", "price", "var_nominal", "var_real"],
                    "page_sizes": [25, 50, 100, 250],
                    "macro_scopes": ["general", "rubros"],
                    "macro_regions": regions,
                    "macro_categories": macro_categories,
                },
            }

        base_df = df.sort_values(["canonical_id", "scraped_at"]).copy()
        base_df["is_cba"] = base_df["basket_id"] == "cba"
        real_df, real_meta = self._compute_real_prices(base_df, ipc_df, preferred_base_month=from_month)

        daily = real_df.assign(day=real_df["scraped_at"].dt.floor("D"))
        timeline_idx = daily.groupby(["canonical_id", "day"])["scraped_at"].idxmax()
        timeline_df = daily.loc[timeline_idx].sort_values(["canonical_id", "scraped_at"])
        timeline_df = self._downsample_timeline(timeline_df, max_points_per_product=240)

        snap_idx = real_df.groupby("canonical_id")["scraped_at"].idxmax()
        snapshot_df = real_df.loc[snap_idx].sort_values(["product_name", "canonical_id"])
        candidate_bands, candidate_band_summary = self._build_candidate_bands(candidate_df, snapshot_df)
        scrape_quality = self._scrape_quality_summary(
            df,
            basket_type,
            candidate_band_summary=candidate_band_summary,
        )

        monthly_ref = (
            real_df.groupby(["canonical_id", "month"], as_index=False)
            .agg(
                avg_price=("current_price", "mean"),
                avg_real_price=("real_price", "mean"),
            )
            .sort_values(["canonical_id", "month"])
        )
        monthly_metrics = monthly_ref.copy()
        monthly_metrics["nominal_mom_pct"] = (
            monthly_metrics.sort_values(["canonical_id", "month"]).groupby("canonical_id")["avg_price"].pct_change(fill_method=None) * 100
        )
        monthly_metrics["real_mom_pct"] = (
            monthly_metrics.sort_values(["canonical_id", "month"]).groupby("canonical_id")["avg_real_price"].pct_change(fill_method=None) * 100
        )

        balanced_panel = self._build_balanced_panel(monthly_ref, from_month=from_month, to_month=to_month)
        kpi_summary = self._compute_economic_kpis(monthly_ref, ipc_df, balanced_panel, from_month, to_month)
        quality_flags = self._build_quality_flags(
            coverage=coverage,
            kpi_summary=kpi_summary,
            missing_cpi_months=sorted(
                set(real_meta["missing_cpi_months"])
                | (
                    set(tracker_ipc_df["year_month"].astype(str).unique().tolist())
                    - set(ipc_df["year_month"].astype(str).unique().tolist())
                )
            ),
        )
        web_payload = self._web_status_payload(df, quality_flags)

        basket_monthly = monthly_ref.groupby("month", as_index=False)["avg_price"].mean().rename(
            columns={"avg_price": "basket_avg_price"}
        )
        basket_monthly = basket_monthly.sort_values("month")
        basket_vs_ipc_series: List[Dict[str, Any]] = []
        if not basket_monthly.empty:
            basket_base = self._safe_float(basket_monthly.iloc[0]["basket_avg_price"])
            cpi_map = {row["year_month"]: self._safe_float(row["cpi_index"]) for _, row in ipc_df.iterrows()}
            base_cpi = cpi_map.get(str(basket_monthly.iloc[0]["month"]))
            for _, row in basket_monthly.iterrows():
                month = str(row["month"])
                basket_avg = self._safe_float(row["basket_avg_price"])
                basket_idx = None
                if basket_base and basket_base > 0 and basket_avg is not None:
                    basket_idx = (basket_avg / basket_base) * 100
                ipc_idx = None
                cpi_val = cpi_map.get(month)
                if cpi_val and base_cpi and base_cpi > 0:
                    ipc_idx = (cpi_val / base_cpi) * 100
                gap = basket_idx - ipc_idx if basket_idx is not None and ipc_idx is not None else None
                basket_vs_ipc_series.append(
                    {
                        "year_month": month,
                        "basket_index_base100": basket_idx,
                        "ipc_index_base100": ipc_idx,
                        "gap_points": gap,
                    }
                )

        def _snapshot_row_dict(row: pd.Series) -> Dict[str, Any]:
            return {
                "canonical_id": row["canonical_id"],
                "product_name": row["product_name"],
                "basket_id": row["basket_id"],
                "is_cba": bool(row["is_cba"]),
                "category": row["category"] or "sin_categoria",
                "presentation": row["product_size"] or "N/D",
                "product_url": row.get("product_url"),
                "scraped_at": pd.Timestamp(row["scraped_at"]).isoformat(),
                "scraped_month": str(pd.Timestamp(row["scraped_at"]).to_period("M")),
                "current_price": self._safe_float(row["current_price"]),
                "current_real_price": self._safe_float(row.get("real_price")),
            }

        def _timeline_row_dict(row: pd.Series) -> Dict[str, Any]:
            return {
                "canonical_id": row["canonical_id"],
                "scraped_at": pd.Timestamp(row["scraped_at"]).isoformat(),
                "current_price": self._safe_float(row["current_price"]),
                "current_real_price": self._safe_float(row.get("real_price")),
            }

        categories = sorted(snapshot_df["category"].dropna().unique().tolist())
        selected = snapshot_df["canonical_id"].dropna().tolist()[:5]

        return {
            "ui_version": 2,
            "from_month": from_month,
            "to_month": to_month,
            "basket_type": basket_type,
            "benchmark_mode": benchmark_mode,
            "analysis_depth": analysis_depth,
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
            "has_data": True,
            "timeline": [_timeline_row_dict(row) for _, row in timeline_df.iterrows()],
            "snapshot": [_snapshot_row_dict(row) for _, row in snapshot_df.iterrows()],
            "monthly_reference": [
                {
                    "canonical_id": row["canonical_id"],
                    "month": row["month"],
                    "avg_price": self._safe_float(row["avg_price"]),
                    "avg_real_price": self._safe_float(row["avg_real_price"]),
                }
                for _, row in monthly_ref.iterrows()
            ],
            "ipc_series": [
                {
                    "year_month": row["year_month"],
                    "cpi_index": self._safe_float(row["cpi_index"]),
                    "cpi_mom": self._safe_float(row["cpi_mom"]),
                    "cpi_yoy": self._safe_float(row["cpi_yoy"]),
                    "status": row.get("status"),
                    "source": row.get("source"),
                }
                for _, row in ipc_df.iterrows()
            ],
            "tracker_ipc_series": [
                {
                    "year_month": row["year_month"],
                    "index_value": self._safe_float(row.get("index_value")),
                    "mom_change": self._safe_float(row.get("mom_change")),
                    "yoy_change": self._safe_float(row.get("yoy_change")),
                    "status": row.get("status"),
                    "coverage_weight_pct": self._safe_float(row.get("coverage_weight_pct")),
                    "coverage_product_pct": self._safe_float(row.get("coverage_product_pct")),
                    "method_version": row.get("method_version"),
                    "base_month": row.get("base_month"),
                }
                for _, row in tracker_ipc_df.iterrows()
            ],
            "official_patagonia_series": [
                {
                    "year_month": row["year_month"],
                    "cpi_index": self._safe_float(row["cpi_index"]),
                    "cpi_mom": self._safe_float(row["cpi_mom"]),
                    "cpi_yoy": self._safe_float(row["cpi_yoy"]),
                    "status": row.get("status"),
                    "source": row.get("source"),
                }
                for _, row in ipc_df.iterrows()
            ],
            "official_series_by_region": {
                region: [
                    {
                        "year_month": row["year_month"],
                        "cpi_index": self._safe_float(row["cpi_index"]),
                        "cpi_mom": self._safe_float(row["cpi_mom"]),
                        "cpi_yoy": self._safe_float(row["cpi_yoy"]),
                        "status": row.get("status"),
                        "source": row.get("source"),
                    }
                    for _, row in ipc_by_region.get(region, pd.DataFrame()).iterrows()
                ]
                for region in regions
            },
            "official_category_series_by_region": {
                region: [
                    {
                        "year_month": row["year_month"],
                        "metric_code": row.get("metric_code"),
                        "cpi_index": self._safe_float(row["cpi_index"]),
                        "cpi_mom": self._safe_float(row["cpi_mom"]),
                        "cpi_yoy": self._safe_float(row["cpi_yoy"]),
                        "status": row.get("status"),
                        "source": row.get("source"),
                    }
                    for _, row in official_cat_by_region.get(region, pd.DataFrame()).iterrows()
                ]
                for region in regions
            },
            "ipc_comparison_series": ipc_comparison_series,
            "category_comparison_series": category_comparison_series,
            "ipc_comparison_by_region": comparison_by_region,
            "category_comparison_by_region": category_comparison_by_region,
            "publication_status": publication_status,
            "publication_status_by_region": publication_status_by_region,
            "official_regions": regions,
            "macro_default_region": default_region,
            "category_mapping_meta": mapping_meta,
            "product_monthly_metrics": [
                {
                    "canonical_id": row["canonical_id"],
                    "month": row["month"],
                    "nominal_avg_price": self._safe_float(row["avg_price"]),
                    "real_avg_price": self._safe_float(row["avg_real_price"]),
                    "nominal_mom_pct": self._safe_float(row["nominal_mom_pct"]),
                    "real_mom_pct": self._safe_float(row["real_mom_pct"]),
                }
                for _, row in monthly_metrics.iterrows()
            ],
            "basket_vs_ipc_series": basket_vs_ipc_series,
            "kpi_summary": kpi_summary,
            "quality_flags": quality_flags,
            "months": months,
            "categories": categories,
            "coverage": coverage,
            "scrape_quality": scrape_quality,
            "candidate_bands": candidate_bands,
            "candidate_band_summary": candidate_band_summary,
            "ads": ads_payload,
            "analytics": analytics_payload,
            "premium_placeholders": premium_payload,
            "publication_policy": PUBLICATION_POLICY,
            "web_status": web_payload["web_status"],
            "is_stale": web_payload["is_stale"],
            "data_age_hours": web_payload["data_age_hours"],
            "last_data_timestamp": web_payload["last_data_timestamp"],
            "next_update_eta": web_payload["next_update_eta"],
            "ui_defaults": {
                "query": "",
                "cba_filter": "all",
                "category": "all",
                "sort_by": "alphabetical",
                "base_month": from_month if from_month in months else (months[0] if months else to_month),
                "selected_products": selected,
                "price_mode": "nominal",
                "show_real_column": False,
                "macro_scope": "general",
                "macro_region": default_region,
                "macro_category": macro_categories[0] if macro_categories else "",
                "view": analysis_depth,
                "band_product": "",
                "page_size": 25,
                "current_page": 1,
            },
            "filters_available": {
                "cba_filter": ["all", "yes", "no"],
                "categories": categories,
                "months": months,
                "sort_by": ["alphabetical", "price", "var_nominal", "var_real"],
                "page_sizes": [25, 50, 100, 250],
                "macro_scopes": ["general", "rubros"],
                "macro_regions": regions,
                "macro_categories": macro_categories,
            },
            "real_reference": real_meta,
        }

    def _render_interactive_html(
        self,
        payload: Dict[str, Any],
        generated_at: str,
        analysis_depth: str = "executive",
        offline_assets: str = "embed",
    ) -> str:
        payload_json = json.dumps(payload, ensure_ascii=False).replace("</", "<\\/")
        external_script = ""
        if offline_assets == "external":
            external_script = '<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>'
        analytics_script = ""
        analytics_payload = payload.get("analytics", {}) if isinstance(payload.get("analytics"), dict) else {}
        analytics_enabled = bool(analytics_payload.get("enabled", False))
        analytics_domain = str(analytics_payload.get("domain", "")).strip()
        analytics_src = str(
            analytics_payload.get("script_url", "https://plausible.io/js/script.js")
        ).strip()
        if analytics_enabled and analytics_domain:
            analytics_script = (
                f"<script defer data-domain=\"{analytics_domain}\" src=\"{analytics_src}\"></script>"
            )
        tracker_url = f"{self._public_base_url()}/tracker/"
        og_image_url = f"{self._public_base_url()}/assets/og-card.svg"

        template = r"""<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>La Anonima: rastreador de precios - Ushuaia</title>
<meta name="description" content="Panel profesional para seguimiento de precios historicos, variacion nominal/real y comparativa de IPC."/>
<meta name="theme-color" content="#0b607a"/>
<link rel="canonical" href="__TRACKER_URL__"/>
<meta property="og:type" content="website"/>
<meta property="og:title" content="La Anonima Tracker"/>
<meta property="og:description" content="Seguimiento historico de precios y comparativa macro IPC."/>
<meta property="og:image" content="__OG_IMAGE_URL__"/>
<meta property="og:url" content="__TRACKER_URL__"/>
<meta name="twitter:card" content="summary_large_image"/>
<meta name="twitter:title" content="La Anonima Tracker"/>
<meta name="twitter:description" content="Seguimiento historico de precios y comparativa macro IPC."/>
<meta name="twitter:image" content="__OG_IMAGE_URL__"/>
<link rel="icon" type="image/svg+xml" href="/favicon.svg"/>
<link rel="manifest" href="/site.webmanifest"/>
__EXTERNAL_SCRIPT__
__ANALYTICS_SCRIPT__
<style>
:root{
  --bg:#f3f6fa;
  --bg-soft:#eaf1f7;
  --panel:#ffffff;
  --panel-soft:#f6fafd;
  --text:#162236;
  --muted:#5d697a;
  --line:#d7e1ec;
  --line-strong:#bfcedd;
  --primary:#0b607a;
  --primary-strong:#084c62;
  --accent:#0b8b85;
  --danger:#b42318;
  --ok:#027a48;
  --warn:#b45309;
  --pos:#0b7a63;
  --neg:#9f1d27;
  --focus:#7bc5dd;
  --shadow-sm:0 2px 6px rgba(21,34,54,.07), 0 14px 28px rgba(21,34,54,.05);
  --shadow-lg:0 8px 16px rgba(21,34,54,.08), 0 24px 46px rgba(21,34,54,.09);
  --radius:16px;
  --radius-sm:11px;
  --font-display:"Iowan Old Style","Book Antiqua","Palatino Linotype",serif;
  --font-body:"Aptos","Segoe UI Variable","Trebuchet MS",sans-serif;
}
*{box-sizing:border-box}
html,body{
  margin:0;
  padding:0;
  color:var(--text);
  font-family:var(--font-body);
  font-size:16px;
  line-height:1.38;
}
html{scroll-behavior:smooth}
body{
  background:
    radial-gradient(1280px 460px at -4% -5%, rgba(11,96,122,.11) 0%, rgba(11,96,122,0) 58%),
    radial-gradient(1050px 420px at 100% -6%, rgba(11,139,133,.1) 0%, rgba(11,139,133,0) 56%),
    linear-gradient(180deg, var(--bg-soft) 0%, var(--bg) 36%, var(--bg) 100%);
}
.card,.kpi,.ad-slot,.pill,button,input,select,.head-links a{
  transition:box-shadow .2s ease, border-color .2s ease, background-color .2s ease, transform .2s ease, color .2s ease;
}
@media (hover:hover){
  .card:hover{box-shadow:var(--shadow-lg)}
  .kpi:hover{transform:translateY(-1px)}
}
@media (prefers-reduced-motion:reduce){
  *{
    animation:none !important;
    transition:none !important;
    scroll-behavior:auto !important;
  }
}
.wrap{max-width:1460px;margin:0 auto;padding:20px}
.stack{display:grid;gap:14px}
.card{
  background:var(--panel);
  border:1px solid var(--line);
  border-radius:var(--radius);
  padding:16px 18px;
  box-shadow:var(--shadow-sm);
  overflow:hidden;
}
.card h2{margin:0 0 10px 0;font-size:1.03rem;letter-spacing:.01em}
.card h2{font-family:var(--font-display)}
.header{
  background:
    radial-gradient(900px 280px at 0% 0%, rgba(11,96,122,.08) 0%, rgba(11,96,122,0) 62%),
    linear-gradient(180deg, #ffffff 0%, #fdfefe 100%);
  border-color:var(--line-strong);
  display:grid;
  grid-template-columns:minmax(0,1fr) auto;
  gap:14px;
  align-items:center;
}
.head-links{
  display:flex;
  gap:8px;
  flex-wrap:wrap;
  margin:0 0 8px 0;
}
.head-links a{
  display:inline-flex;
  align-items:center;
  min-height:30px;
  padding:6px 10px;
  border-radius:999px;
  border:1px solid #c8d6e6;
  background:#f7fbff;
  color:#2f4f6f;
  font-size:.78rem;
  font-weight:700;
  text-decoration:none;
}
.head-links a:hover{
  background:#ecf5ff;
  border-color:#b6c9de;
  text-decoration:none;
}
.title{
  margin:0 0 3px 0;
  font-size:1.56rem;
  line-height:1.16;
  letter-spacing:.01em;
  font-family:var(--font-display);
}
.meta{color:var(--muted);font-size:.92rem;margin:0 0 3px 0}
.badge{
  display:inline-flex;
  align-items:center;
  padding:6px 11px;
  border-radius:999px;
  font-size:.73rem;
  font-weight:700;
  letter-spacing:.045em;
  text-transform:uppercase;
  background:#e8f6ef;
  color:var(--ok);
  border:1px solid #cce8d7;
}
.badge.warn{background:#fff4eb;color:var(--warn);border-color:#f4d3b3}
.ui-version{margin-top:7px;text-align:right;font-weight:700}
.method{font-size:.8rem;color:var(--muted);margin:0}
.helper{
  display:grid;
  gap:10px;
  border-color:var(--line-strong);
  background:linear-gradient(180deg,#ffffff 0%,#f9fcfe 100%);
}
.guide-title{font-weight:800;font-size:1rem}
.pills{display:flex;gap:8px;flex-wrap:wrap;align-items:center}
.pill{
  display:inline-flex;
  align-items:center;
  gap:6px;
  border:1px solid var(--line);
  background:#fdfefe;
  border-radius:999px;
  padding:6px 11px;
  color:#445064;
  font-size:.78rem;
}
.pill-info{font-weight:700}
.pill-action{
  cursor:pointer;
  transition:all .16s ease;
}
.pill-action:hover{
  border-color:#abc0d8;
  background:#f5faff;
}
.pill-action span{
  font-weight:700;
  color:#607086;
}
.kpis{display:grid;gap:10px;grid-template-columns:repeat(6,minmax(136px,1fr))}
.kpi{
  background:linear-gradient(175deg,#ffffff 0%,var(--panel-soft) 100%);
  border:1px solid var(--line);
  border-radius:13px;
  padding:11px 10px 12px;
  text-align:center;
  box-shadow:inset 0 1px 0 rgba(255,255,255,.85);
}
.kpi .label{font-size:.74rem;color:var(--muted);margin-bottom:5px}
.kpi .value{font-size:1.2rem;font-weight:750;font-variant-numeric:tabular-nums}
.kpi .sub{font-size:.75rem;color:var(--muted)}
.kpi.good .value{color:var(--pos)}
.kpi.bad .value{color:var(--neg)}
.kpi.warn .value{color:var(--warn)}
details.filters{
  position:relative;
  z-index:1;
  background:var(--panel);
  border:1px solid var(--line-strong);
  border-radius:var(--radius);
  box-shadow:var(--shadow-sm);
}
details.filters>summary{
  cursor:pointer;
  list-style:none;
  padding:12px 14px;
  font-weight:780;
  display:flex;
  align-items:center;
  gap:9px;
}
details.filters>summary::-webkit-details-marker{display:none}
details.filters>summary::before{
  content:"";
  width:8px;
  height:8px;
  border-right:2px solid #4f5c70;
  border-bottom:2px solid #4f5c70;
  transform:rotate(-45deg);
  margin-top:-1px;
  transition:transform .18s ease;
}
details.filters[open]>summary::before{transform:rotate(45deg)}
.filters-grid{
  display:grid;
  gap:10px;
  padding:0 14px 14px 14px;
  grid-template-columns:repeat(12,minmax(0,1fr));
}
.filter-field{
  background:var(--panel-soft);
  border:1px solid #dde7f1;
  border-radius:12px;
  padding:9px 10px 10px;
  min-height:84px;
  display:flex;
  flex-direction:column;
  justify-content:flex-start;
  gap:4px;
  grid-column:span 2;
}
.filter-field.span-3{grid-column:span 3}
.filter-field.span-4{grid-column:span 4}
.filter-field.span-6{grid-column:span 6}
label{display:block;font-size:.76rem;color:var(--muted);margin-bottom:2px}
input,select,button{
  width:100%;
  min-height:36px;
  padding:8px 10px;
  border:1px solid #c2cfde;
  border-radius:var(--radius-sm);
  background:#fff;
  color:var(--text);
  font:inherit;
}
input,select{box-shadow:inset 0 1px 2px rgba(17,24,39,.04)}
input:hover,select:hover{border-color:#afbed0}
input:focus,select:focus{outline:2px solid var(--focus);outline-offset:1px;border-color:#84b6c7}
button:focus-visible,
a:focus-visible{
  outline:2px solid var(--focus);
  outline-offset:2px;
}
button{
  cursor:pointer;
  font-weight:650;
  transition:all .16s ease;
}
button:active{transform:translateY(1px)}
button:disabled{
  cursor:not-allowed;
  opacity:.58;
}
button.primary{
  background:linear-gradient(180deg,var(--primary),var(--primary-strong));
  color:#fff;
  border-color:var(--primary-strong);
  box-shadow:0 1px 0 rgba(255,255,255,.14) inset;
}
button.primary:hover{filter:brightness(1.04)}
button.ghost{background:#fff}
button.ghost:hover{background:#f3f8fb}
button.soft{
  background:#f8fbff;
  border-color:#c7d6e7;
  color:#3d556f;
}
button.soft:hover{background:#edf5fd}
.search-wrap{
  display:flex;
  align-items:center;
  gap:8px;
}
.search-wrap input{flex:1}
.search-wrap button{width:auto;min-width:92px}
.switchers{display:flex;gap:7px;flex-wrap:wrap}
.switchers button{width:auto;padding:7px 10px;min-height:34px}
.switchers button.active{
  background:linear-gradient(180deg,var(--accent),#0a6f6a);
  border-color:#086963;
  color:#fff;
}
.inline-toggle{display:flex;align-items:center;gap:8px;min-height:34px}
.inline-toggle input{width:auto;accent-color:var(--primary)}
#sel{min-height:116px}
#sel option{padding:2px 4px}
.field-meta{
  margin-top:2px;
  font-size:.75rem;
  color:var(--muted);
}
.filter-actions{
  justify-content:flex-end;
  gap:8px;
}
.filter-actions .switchers{width:100%}
.btn-inline{width:auto}
.copy-status{
  min-height:16px;
  text-align:left;
}
.copy-status.error{color:var(--danger)}
.workspace-grid{
  display:grid;
  gap:14px;
  grid-template-columns:minmax(0,1.45fr) minmax(320px,.95fr);
  align-items:start;
}
.workspace-main,.workspace-side{display:grid;gap:14px}
.chart-card h2{text-align:left}
.chart{
  border:1px solid var(--line);
  border-radius:13px;
  background:
    radial-gradient(580px 130px at 50% 0%, rgba(11,96,122,.08) 0%, rgba(11,96,122,0) 72%),
    linear-gradient(180deg,#ffffff,#f9fcff);
  position:relative;
  min-height:360px;
  display:flex;
  align-items:center;
  justify-content:center;
  box-shadow:inset 0 0 0 1px rgba(255,255,255,.7);
}
.chart.small{min-height:250px}
.chart canvas{width:100%;height:360px;display:block;margin:0 auto}
.chart.small canvas{height:250px}
.chart-empty{
  position:absolute;
  inset:0;
  display:flex;
  align-items:center;
  justify-content:center;
  color:var(--muted);
  font-size:.9rem;
}
.chart-tooltip{
  position:absolute;
  pointer-events:none;
  display:none;
  min-width:160px;
  max-width:260px;
  padding:8px 10px;
  border-radius:10px;
  border:1px solid #c7d4e3;
  background:rgba(255,255,255,.98);
  box-shadow:0 8px 18px rgba(18,34,56,.14);
  font-size:.77rem;
  color:#314357;
  z-index:2;
}
.chart-tooltip.visible{display:block}
.chart-tooltip strong{
  display:block;
  margin-bottom:5px;
  color:#1f334a;
}
.legend{
  display:flex;
  gap:8px;
  flex-wrap:wrap;
  padding-top:9px;
  font-size:.78rem;
  color:var(--muted);
  justify-content:flex-start;
}
.legend .item{
  display:flex;
  align-items:center;
  gap:6px;
  padding:4px 8px;
  border:1px solid #dde7f1;
  border-radius:999px;
  background:#fbfdff;
}
.dot{width:10px;height:10px;border-radius:999px;display:inline-block}
.band-toolbar{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-bottom:8px}
.band-select-wrap{min-width:260px;flex:1}
.macro-note{
  border:1px solid #edcf95;
  background:#fff8e9;
  color:#6f4e11;
  border-radius:10px;
  padding:8px 10px;
  font-size:.8rem;
  line-height:1.35;
  margin-bottom:8px;
}
.quality{
  display:grid;
  gap:8px;
  background:linear-gradient(180deg,#ffffff 0%,#fbfdff 100%);
}
.quality strong{display:block;margin-bottom:2px}
.quality-item{
  font-size:.84rem;
  color:var(--muted);
  padding:5px 8px;
  border:1px solid #e2e9f2;
  border-radius:10px;
  background:#f9fbfe;
}
.warn-list{
  margin:6px 0 0 18px;
  padding:0;
  color:#7a3104;
  font-size:.82rem;
}
.table-section h2{margin-bottom:8px}
.table-toolbar{
  display:flex;
  gap:10px;
  align-items:center;
  justify-content:space-between;
  flex-wrap:wrap;
  margin-bottom:8px;
}
.table-actions{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
.table-actions label{margin:0}
.table-actions .btn-inline{width:auto}
.table-actions .page-size{width:auto;min-width:88px}
.page-info{
  min-width:92px;
  text-align:center;
  border:1px solid #d7e2ee;
  border-radius:999px;
  padding:6px 8px;
  background:#f8fbff;
}
.table-wrap{
  overflow-x:auto;
  border:1px solid var(--line);
  border-radius:12px;
}
table{width:100%;border-collapse:separate;border-spacing:0;min-width:720px}
th,td{
  padding:9px 7px;
  border-bottom:1px solid #e8edf3;
  text-align:center;
  font-size:.9rem;
  vertical-align:middle;
}
th{
  font-size:.74rem;
  color:#516074;
  text-transform:uppercase;
  letter-spacing:.05em;
  background:linear-gradient(180deg,#f8fbff,#f3f7fd);
  border-bottom:1px solid #d7dee8;
}
thead th{position:sticky;top:0;z-index:1;backdrop-filter:blur(3px)}
td:nth-child(2),th:nth-child(2){text-align:center}
tbody tr:nth-child(even){background:#fbfdff}
tbody tr:hover{background:#eff7ff}
td.num{text-align:center;font-variant-numeric:tabular-nums;white-space:nowrap}
a{color:var(--primary);text-decoration:none}
a:hover{text-decoration:underline;text-decoration-thickness:1.5px}
td:first-child a{
  display:inline-block;
  max-width:380px;
  overflow:hidden;
  text-overflow:ellipsis;
  white-space:nowrap;
  vertical-align:bottom;
}
.muted{color:var(--muted)}
.sr{position:absolute;left:-9999px}
.var-up{color:var(--neg);font-weight:700}
.var-down{color:var(--pos);font-weight:700}
.var-flat{color:var(--muted);font-weight:700}
.empty-title{margin:0 0 8px 0}
.empty-text{margin:0 0 8px 0}
.ad-panel,.premium-panel{display:none}
.ad-grid{
  display:grid;
  grid-template-columns:repeat(2,minmax(0,1fr));
  gap:10px;
}
.ad-slot{
  border:1px dashed #9ab0c8;
  border-radius:10px;
  min-height:90px;
  background:#f5f9ff;
  display:flex;
  align-items:center;
  justify-content:center;
  color:#3c5a78;
  font-weight:600;
}
.premium-list{
  margin:0;
  padding-left:18px;
  color:var(--muted);
}
.premium-list li{margin:6px 0}
.onboarding-mobile{
  position:fixed;
  left:12px;
  right:12px;
  bottom:106px;
  z-index:54;
  background:#0f2237;
  color:#eaf2fe;
  border:1px solid #31506f;
  border-radius:12px;
  padding:11px 12px;
  box-shadow:0 10px 28px rgba(0,0,0,.24);
  display:grid;
  gap:8px;
}
.onboarding-mobile[hidden]{display:none}
.onboarding-title{
  font-size:.88rem;
  font-weight:780;
}
.onboarding-actions{
  display:flex;
  gap:8px;
  flex-wrap:wrap;
}
.onboarding-actions button{
  width:auto;
  min-height:32px;
  border:1px solid #48678d;
  background:#1d3652;
  color:#eaf2fe;
  border-radius:8px;
  padding:7px 10px;
}
.onboarding-actions button.primary{
  background:#10837b;
  border-color:#1aa497;
  color:#fff;
}
.cookie-banner{
  position:fixed;
  left:12px;
  right:12px;
  bottom:12px;
  z-index:55;
  background:#102338;
  color:#eaf1fb;
  border-radius:12px;
  border:1px solid #334f6f;
  padding:12px;
  box-shadow:0 10px 28px rgba(0,0,0,.24);
}
.cookie-grid{
  display:flex;
  align-items:center;
  justify-content:space-between;
  gap:12px;
  flex-wrap:wrap;
}
.cookie-actions{display:flex;gap:8px}
.cookie-actions button{
  border:1px solid #456489;
  background:#1b3552;
  color:#eaf1fb;
  border-radius:8px;
  padding:7px 10px;
  cursor:pointer;
}
.cookie-actions button.primary{
  background:#10837b;
  border-color:#1aa497;
  color:#fff;
}
@media (max-width:1320px){
  .workspace-grid{grid-template-columns:1fr}
}
@media (min-width:761px){
  .onboarding-mobile{display:none !important}
}
@media (max-width:1060px){
  .kpis{grid-template-columns:repeat(3,minmax(120px,1fr))}
  .filters-grid{grid-template-columns:repeat(6,minmax(0,1fr))}
  .filter-field,.filter-field.span-3,.filter-field.span-4,.filter-field.span-6{grid-column:span 2}
  .filter-field.span-6{grid-column:span 6}
}
@media (max-width:760px){
  .wrap{padding:10px}
  .card{padding:12px}
  .header{grid-template-columns:1fr}
  .head-links a{
    flex:1 1 calc(50% - 6px);
    justify-content:center;
  }
  .ui-version{text-align:left}
  .title{font-size:1.17rem}
  .kpis{grid-template-columns:repeat(2,minmax(120px,1fr))}
  .filters-grid{grid-template-columns:1fr}
  .filter-field,.filter-field.span-3,.filter-field.span-4,.filter-field.span-6{grid-column:span 1;min-height:auto}
  .search-wrap{flex-direction:column}
  .search-wrap button{width:100%}
  .switchers button{width:100%}
  table{min-width:640px}
  .ad-grid{grid-template-columns:1fr}
  .onboarding-mobile{left:8px;right:8px;bottom:104px}
  .cookie-banner{left:8px;right:8px;bottom:8px}
}
</style>
</head>
<body>
<div class="wrap stack">
  <section class="card header">
    <div>
      <nav class="head-links" aria-label="Navegacion del sitio">
        <a href="/tracker/">Tracker</a>
        <a href="/historico/">Historico</a>
        <a href="/metodologia/">Metodologia</a>
        <a href="/contacto/">Contacto</a>
      </nav>
      <h1 class="title">La Anonima Tracker: precios para decidir mejor</h1>
      <p class="meta">Generado: __GEN__ | Rango: __FROM__ a __TO__ | Canasta: __BASKET__</p>
      <p class="meta" id="freshness-meta">Estado web: pendiente | Proxima corrida estimada: N/D</p>
      <p class="method">Primero revisa el bloque macro. Luego filtra productos para ver variaciones nominales y reales.</p>
    </div>
    <div>
      <span id="quality-badge" class="badge">Datos completos</span>
      <div class="method ui-version">Version publica</div>
    </div>
  </section>

  <section class="card helper" id="quick-guide">
    <div class="guide-title">Resumen de vista</div>
    <p class="method">Aplicá filtros y compartí esta misma vista con "Copiar vista".</p>
    <div id="active-filters" class="pills"></div>
  </section>

  <section class="card ad-panel" id="ad-panel" style="display:none">
    <h2>Publicidad</h2>
    <div id="ad-slots" class="ad-grid"></div>
  </section>

  <section class="card premium-panel" id="premium-panel" style="display:none">
    <h2>Funciones opcionales</h2>
    <p class="muted">Modulos disponibles para una futura version Pro.</p>
    <ul id="premium-features" class="premium-list"></ul>
  </section>

  <section id="empty" class="card" style="display:none">
    <h2 class="empty-title">Sin datos para el rango seleccionado</h2>
    <p class="muted empty-text">El reporte se genero correctamente, pero no hay observaciones de precios en la base local.</p>
    <p class="muted">Paso sugerido: ejecutar <code>python -m src.cli scrape --basket all</code> y luego <code>python -m src.cli app</code>.</p>
  </section>

  <div id="app" class="stack">
    <section class="kpis" id="kpi-grid"></section>

    <article class="card chart-card" id="panel-secondary">
      <h2>IPC Propio vs IPC Oficial (indice base 100)</h2>
      <div class="band-toolbar">
        <div class="band-select-wrap">
          <label for="macro-scope">Vista macro</label>
          <select id="macro-scope">
            <option value="general">General</option>
            <option value="rubros">Rubros</option>
          </select>
        </div>
        <div class="band-select-wrap">
          <label for="macro-region">Region oficial</label>
          <select id="macro-region"></select>
        </div>
        <div class="band-select-wrap">
          <label for="macro-category">Rubro</label>
          <select id="macro-category"></select>
        </div>
        <div id="macro-status" class="muted"></div>
      </div>
      <div id="macro-notice" class="macro-note" hidden></div>
      <div id="chart-secondary" class="chart small"><div class="chart-empty">Sin comparativa IPC</div></div>
      <div id="legend-secondary" class="legend"></div>
    </article>

    <details class="filters" id="filters-panel" open>
      <summary>Filtros y seleccion de productos</summary>
      <div class="filters-grid">
        <div class="filter-field span-4">
          <label for="q">Buscar producto</label>
          <div class="search-wrap">
            <input id="q" placeholder="nombre del producto" />
            <button id="clear-search" type="button" class="soft">Limpiar</button>
          </div>
          <div class="field-meta">Tip rapido: presiona <strong>/</strong> para enfocar la busqueda.</div>
        </div>
        <div class="filter-field">
          <label for="cba">CBA</label>
          <select id="cba">
            <option value="all">Todos</option>
            <option value="yes">Si</option>
            <option value="no">No</option>
          </select>
        </div>
        <div class="filter-field">
          <label for="cat">Categoria</label>
          <select id="cat"></select>
        </div>
        <div class="filter-field">
          <label for="ord">Ordenar por</label>
          <select id="ord">
            <option value="alphabetical">Alfabetico</option>
            <option value="price">Precio</option>
            <option value="var_nominal">Var. nominal</option>
            <option value="var_real">Var. real</option>
          </select>
        </div>
        <div class="filter-field">
          <label for="mbase">Mes base variacion</label>
          <select id="mbase"></select>
        </div>
        <div class="filter-field">
          <label for="show-real">Tabla</label>
          <div class="inline-toggle">
            <input id="show-real" type="checkbox"/>
            <span class="muted">Mostrar var. real %</span>
          </div>
        </div>
        <div class="filter-field span-3">
          <label>Modo precio</label>
          <div class="switchers">
            <button id="mode-nominal" type="button" class="active">Nominal</button>
            <button id="mode-real" type="button">Real</button>
          </div>
        </div>
        <div class="filter-field span-3">
          <label>Seleccion rapida</label>
          <div class="switchers">
            <button id="quick-up" type="button">Ganadores</button>
            <button id="quick-down" type="button">Perdedores</button>
            <button id="quick-flat" type="button">Estables</button>
          </div>
        </div>
        <div class="filter-field span-4">
          <label for="sel">Productos en grafico</label>
          <select id="sel" multiple size="5"></select>
          <div id="selection-meta" class="field-meta">0 productos seleccionados</div>
        </div>
        <div class="filter-field span-4 filter-actions">
          <label>Acciones</label>
          <div class="switchers">
            <button id="reset" class="primary" type="button">Reset general</button>
            <button id="copy-link" type="button" class="soft btn-inline">Copiar vista</button>
            <button id="export-csv" type="button" class="ghost btn-inline">Exportar CSV</button>
          </div>
          <div id="copy-link-status" class="field-meta copy-status" aria-live="polite"></div>
        </div>
      </div>
    </details>

    <section class="workspace-grid">
      <div class="workspace-main">
        <article class="card chart-card">
          <h2>Comparativa de precios por producto</h2>
          <div id="chart-main" class="chart"><div class="chart-empty">Sin datos para graficar</div></div>
          <div id="legend-main" class="legend"></div>
        </article>

        <section class="card table-section">
          <h2>Listado de productos</h2>
          <div class="table-toolbar">
            <div id="table-meta" class="muted">0 productos</div>
            <div class="table-actions">
              <label for="page-size">Filas</label>
              <select id="page-size" class="page-size"></select>
              <button id="page-prev" type="button" class="ghost btn-inline">Anterior</button>
              <div id="page-info" class="muted page-info">1 / 1</div>
              <button id="page-next" type="button" class="ghost btn-inline">Siguiente</button>
            </div>
          </div>
          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Producto (hipervinculo)</th>
                  <th>Presentacion</th>
                  <th>Precio</th>
                  <th>Var. % vs mes elegido</th>
                  <th id="th-var-real" style="display:none">Var. real %</th>
                </tr>
              </thead>
              <tbody id="tb"></tbody>
            </table>
          </div>
        </section>
      </div>

      <aside class="workspace-side">
        <section class="card chart-card" id="panel-bands">
          <h2>Dispersión intra-producto (low/mid/high)</h2>
          <div class="band-toolbar">
            <div class="band-select-wrap">
              <label for="band-product">Producto para banda</label>
              <select id="band-product"></select>
            </div>
            <div id="band-meta" class="muted"></div>
          </div>
          <div id="chart-bands" class="chart small"><div class="chart-empty">Sin datos de terna auditada</div></div>
          <div id="legend-bands" class="legend"></div>
        </section>

        <section class="card quality" id="quality-panel">
          <strong>Calidad de datos</strong>
          <div class="quality-item" id="quality-coverage"></div>
          <div class="quality-item" id="quality-panel-size"></div>
          <div class="quality-item" id="quality-macro"></div>
          <div class="quality-item" id="quality-ipc"></div>
          <div class="quality-item" id="quality-segments"></div>
          <div class="quality-item" id="quality-policy"></div>
          <ul id="warnings" class="warn-list"></ul>
        </section>
      </aside>
    </section>
  </div>
</div>

<div id="mobile-onboarding" class="onboarding-mobile" hidden role="dialog" aria-live="polite">
  <div class="onboarding-title">Guia movil</div>
  <div>Abrí filtros, buscá con <strong>/</strong> y compartí con "Copiar vista".</div>
  <div class="onboarding-actions">
    <button id="onboarding-goto" type="button" class="primary">Ir a filtros</button>
    <button id="onboarding-close" type="button">Entendido</button>
  </div>
</div>

<div id="cookie-banner" class="cookie-banner" style="display:none" role="dialog" aria-live="polite">
  <div class="cookie-grid">
    <div>Usamos almacenamiento local para preferencias y, cuando aplique, consentimiento de anuncios.</div>
    <div class="cookie-actions">
      <button id="cookie-reject" type="button">Rechazar</button>
      <button id="cookie-accept" type="button" class="primary">Aceptar</button>
    </div>
  </div>
</div>

<script>
const p=__PAYLOAD__;
const defaults=p.ui_defaults||{};
const STORAGE_KEY="laanonima_tracker_report_state_v2";
const COOKIE_KEY="laanonima_tracker_cookie_consent_v1";
const ONBOARDING_KEY="laanonima_tracker_mobile_onboarding_v1";
const COLORS=["#005f73","#9b2226","#ee9b00","#0a9396","#3d405b","#588157","#7f5539","#6a4c93","#1d3557"];
const st={
  query:defaults.query||"",
  cba_filter:defaults.cba_filter||"all",
  category:defaults.category||"all",
  sort_by:defaults.sort_by||"alphabetical",
  base_month:defaults.base_month||"",
  selected_products:[...(defaults.selected_products||[])],
  price_mode:defaults.price_mode||"nominal",
  show_real_column:!!defaults.show_real_column,
  macro_scope:defaults.macro_scope||"general",
  macro_region:defaults.macro_region||p.macro_default_region||"patagonia",
  macro_category:defaults.macro_category||"",
  view:defaults.view||"executive",
  band_product:defaults.band_product||"",
  page_size:Number(defaults.page_size||25),
  current_page:Number(defaults.current_page||1)
};

const el={
  adPanel:document.getElementById("ad-panel"),
  adSlots:document.getElementById("ad-slots"),
  premiumPanel:document.getElementById("premium-panel"),
  premiumFeatures:document.getElementById("premium-features"),
  quickGuide:document.getElementById("quick-guide"),
  mobileOnboarding:document.getElementById("mobile-onboarding"),
  onboardingGoto:document.getElementById("onboarding-goto"),
  onboardingClose:document.getElementById("onboarding-close"),
  cookieBanner:document.getElementById("cookie-banner"),
  cookieAccept:document.getElementById("cookie-accept"),
  cookieReject:document.getElementById("cookie-reject"),
  q:document.getElementById("q"),
  clearSearch:document.getElementById("clear-search"),
  cba:document.getElementById("cba"),
  cat:document.getElementById("cat"),
  ord:document.getElementById("ord"),
  mb:document.getElementById("mbase"),
  sel:document.getElementById("sel"),
  selectionMeta:document.getElementById("selection-meta"),
  tb:document.getElementById("tb"),
  filtersPanel:document.getElementById("filters-panel"),
  reset:document.getElementById("reset"),
  copyLink:document.getElementById("copy-link"),
  copyLinkStatus:document.getElementById("copy-link-status"),
  showReal:document.getElementById("show-real"),
  modeNominal:document.getElementById("mode-nominal"),
  modeReal:document.getElementById("mode-real"),
  quickUp:document.getElementById("quick-up"),
  quickDown:document.getElementById("quick-down"),
  quickFlat:document.getElementById("quick-flat"),
  kpiGrid:document.getElementById("kpi-grid"),
  qualityBadge:document.getElementById("quality-badge"),
  qualityCoverage:document.getElementById("quality-coverage"),
  qualityPanelSize:document.getElementById("quality-panel-size"),
  qualityMacro:document.getElementById("quality-macro"),
  qualityIpc:document.getElementById("quality-ipc"),
  qualitySegments:document.getElementById("quality-segments"),
  qualityPolicy:document.getElementById("quality-policy"),
  warnings:document.getElementById("warnings"),
  qualityPanel:document.getElementById("quality-panel"),
  panelSecondary:document.getElementById("panel-secondary"),
  chartMain:document.getElementById("chart-main"),
  legendMain:document.getElementById("legend-main"),
  macroScope:document.getElementById("macro-scope"),
  macroRegion:document.getElementById("macro-region"),
  macroCategory:document.getElementById("macro-category"),
  macroStatus:document.getElementById("macro-status"),
  macroNotice:document.getElementById("macro-notice"),
  chartSecondary:document.getElementById("chart-secondary"),
  legendSecondary:document.getElementById("legend-secondary"),
  panelBands:document.getElementById("panel-bands"),
  bandProduct:document.getElementById("band-product"),
  bandMeta:document.getElementById("band-meta"),
  chartBands:document.getElementById("chart-bands"),
  legendBands:document.getElementById("legend-bands"),
  thVarReal:document.getElementById("th-var-real"),
  tableMeta:document.getElementById("table-meta"),
  pageSize:document.getElementById("page-size"),
  pagePrev:document.getElementById("page-prev"),
  pageNext:document.getElementById("page-next"),
  pageInfo:document.getElementById("page-info"),
  exportCsv:document.getElementById("export-csv"),
  activeFilters:document.getElementById("active-filters"),
  freshnessMeta:document.getElementById("freshness-meta")
};

const ADSENSE_SCRIPT_ID="laanonima-tracker-adsense";

function trackEvent(name,props){
  if(typeof window.plausible!=="function") return;
  try{
    window.plausible(name,{props:props||{}});
  }catch(_e){}
}

function consentState(){
  try{
    return window.localStorage?.getItem?.(COOKIE_KEY)||"";
  }catch(_e){
    return "";
  }
}

function ensureAdSenseScript(clientId){
  if(!clientId || document.getElementById(ADSENSE_SCRIPT_ID)) return;
  const script=document.createElement("script");
  script.id=ADSENSE_SCRIPT_ID;
  script.async=true;
  script.crossOrigin="anonymous";
  script.src=`https://pagead2.googlesyndication.com/pagead/js/adsbygoogle.js?client=${encodeURIComponent(clientId)}`;
  document.head.appendChild(script);
}

// Keep unicode range escaped to avoid encoding issues in generated standalone HTML.
const norm=v=>String(v||"").toLowerCase().normalize("NFD").replace(/[\\u0300-\\u036f]/g,"");
const esc=v=>String(v||"").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/\"/g,"&quot;").replace(/'/g,"&#39;");
const money=v=>{
  if(v==null||Number.isNaN(Number(v))) return "N/D";
  return new Intl.NumberFormat("es-AR",{
    style:"currency",
    currency:"ARS",
    minimumFractionDigits:0,
    maximumFractionDigits:2
  }).format(Number(v));
};
const pct=v=>v==null||Number.isNaN(Number(v))?"N/D":`${Number(v).toFixed(2)}%`;
const pctSigned=v=>{
  if(v==null||Number.isNaN(Number(v))) return "N/D";
  const n=Number(v);
  const sign=n>0?"+":"";
  return `${sign}${n.toFixed(2)}%`;
};
const monthLabel=v=>String(v||"");
const fmtNum=v=>v==null||Number.isNaN(Number(v))?"N/D":Number(v).toFixed(2);
const fmtAxisNum=v=>{
  if(v==null||Number.isNaN(Number(v))) return "N/D";
  const n=Number(v);
  const decimals=Math.abs(n)>=1000 ? 0 : 2;
  return new Intl.NumberFormat("es-AR",{maximumFractionDigits:decimals}).format(n);
};
const fmtDate=v=>{
  const d=v instanceof Date ? v : new Date(v);
  if(Number.isNaN(d.getTime())) return "N/D";
  return new Intl.DateTimeFormat("es-AR",{year:"numeric",month:"2-digit",day:"2-digit"}).format(d);
};
const fmtMonthTick=v=>{
  const d=v instanceof Date ? v : new Date(v);
  if(Number.isNaN(d.getTime())) return "N/D";
  return new Intl.DateTimeFormat("es-AR",{month:"short",year:"2-digit"}).format(d);
};
const trendClass=v=>{
  if(v==null||Number.isNaN(Number(v))) return "var-flat";
  if(Number(v)>0) return "var-up";
  if(Number(v)<0) return "var-down";
  return "var-flat";
};
const trendIcon=v=>{
  if(v==null||Number.isNaN(Number(v))) return "·";
  if(Number(v)>0) return "↑";
  if(Number(v)<0) return "↓";
  return "→";
};

function normalizePresentation(value){
  const raw=String(value||"").trim();
  if(!raw || raw.toUpperCase()==="N/D") return "N/D";
  const normalized=raw.replace(",",".").replace(/\s+/g," ").trim();
  const match=normalized.match(/^([0-9]+(?:\.[0-9]+)?)\s*([a-zA-Z]+)\b(.*)$/);
  if(!match){
    return raw.replace(/(\d+)[.,]0+(?=\s*[a-zA-Z]|$)/g,"$1");
  }
  const qty=Number(match[1]);
  if(!Number.isFinite(qty)) return raw;

  const unit=norm(match[2].replace(/[^a-z]/gi,""));
  const tail=String(match[3]||"").trim();
  const suffix=tail ? ` ${tail}` : "";
  const qtyLabel=new Intl.NumberFormat("es-AR",{maximumFractionDigits:3,useGrouping:false}).format(qty);
  const canonicalUnit=(()=>{
    if(unit==="kg" || unit==="kilo" || unit==="kilos") return "kg";
    if(unit==="g" || unit==="gr" || unit==="gramo" || unit==="gramos") return "g";
    if(unit==="l" || unit==="lt" || unit==="litro" || unit==="litros") return "l";
    if(unit==="ml") return "ml";
    if(unit==="unidad" || unit==="unidades" || unit==="un") return "un";
    if(unit==="docena" || unit==="docenas" || unit==="doc") return "doc";
    return match[2].toLowerCase();
  })();

  if(qty>0 && qty<1 && (unit==="kg" || unit==="kilo" || unit==="kilos")){
    return `${Math.round(qty*1000)} g${suffix}`;
  }
  if(qty>0 && qty<1 && (unit==="l" || unit==="lt" || unit==="litro" || unit==="litros")){
    return `${Math.round(qty*1000)} ml${suffix}`;
  }
  return `${qtyLabel} ${canonicalUnit}${suffix}`;
}

function inferPresentationFromName(name){
  const raw=String(name||"").trim();
  if(!raw) return "N/D";
  const fromQty=raw.match(/\bx\s*([0-9]+(?:[.,][0-9]+)?)\s*(kg|kilo|kilos|g|gr|gramos?|l|lt|litros?|ml|un|unidad(?:es)?|doc|docena(?:s)?)\b/i);
  if(fromQty){
    return normalizePresentation(`${fromQty[1]} ${fromQty[2]}`);
  }
  if(/\(\s*kg\s*\)/i.test(raw) || /\bpor\s*kg\b/i.test(raw)) return "1 kg";
  return "N/D";
}

function resolvePresentation(row){
  const direct=normalizePresentation(row.presentation||"N/D");
  if(direct!=="N/D") return direct;
  const inferred=inferPresentationFromName(row.product_name||"");
  if(inferred!=="N/D") return inferred;
  return "N/D";
}

function encodeHash(){
  const q=new URLSearchParams();
  q.set("q",st.query||"");
  q.set("cba",st.cba_filter);
  q.set("cat",st.category);
  q.set("ord",st.sort_by);
  q.set("mb",st.base_month||"");
  q.set("pm",st.price_mode);
  q.set("mscope",st.macro_scope||"general");
  q.set("mreg",st.macro_region||"patagonia");
  q.set("mcat",st.macro_category||"");
  q.set("bp",st.band_product||"");
  q.set("real",st.show_real_column?"1":"0");
  q.set("sel",(st.selected_products||[]).join(","));
  q.set("ps",String(st.page_size||25));
  q.set("pg",String(st.current_page||1));
  return q.toString();
}

function applyHashState(){
  const raw=window.location.hash?window.location.hash.slice(1):"";
  if(!raw)return false;
  try{
    const q=new URLSearchParams(raw);
    st.query=q.get("q")??st.query;
    st.cba_filter=q.get("cba")??st.cba_filter;
    st.category=q.get("cat")??st.category;
    st.sort_by=q.get("ord")??st.sort_by;
    st.base_month=q.get("mb")??st.base_month;
    st.price_mode=q.get("pm")??st.price_mode;
    st.macro_scope=q.get("mscope")??st.macro_scope;
    st.macro_region=q.get("mreg")??st.macro_region;
    st.macro_category=q.get("mcat")??st.macro_category;
    st.band_product=q.get("bp")??st.band_product;
    st.show_real_column=(q.get("real")||"0")==="1";
    const sel=q.get("sel");
    if(sel)st.selected_products=sel.split(",").filter(Boolean);
    st.page_size=Number(q.get("ps")||st.page_size||25);
    st.current_page=Number(q.get("pg")||st.current_page||1);
    return true;
  }catch(_e){return false;}
}

function loadState(){
  const hashUsed=applyHashState();
  if(hashUsed)return;
  try{
    const raw=localStorage.getItem(STORAGE_KEY);
    if(!raw)return;
    const obj=JSON.parse(raw);
    if(!obj||typeof obj!=="object")return;
    Object.assign(st,obj);
  }catch(_e){}
}

function saveState(){
  try{localStorage.setItem(STORAGE_KEY,JSON.stringify(st));}catch(_e){}
  const encoded=encodeHash();
  if(window.location.hash.slice(1)!==encoded){
    history.replaceState(null,"",`#${encoded}`);
  }
}

function buildShareUrl(){
  const encoded=encodeHash();
  const current=window.location.href.split("#")[0];
  return `${current}#${encoded}`;
}

let _copyStatusTimer=null;
function setCopyStatus(message,isError=false){
  if(!el.copyLinkStatus)return;
  el.copyLinkStatus.textContent=message||"";
  el.copyLinkStatus.classList.toggle("error",!!isError);
  if(_copyStatusTimer){
    clearTimeout(_copyStatusTimer);
    _copyStatusTimer=null;
  }
  if(message){
    _copyStatusTimer=window.setTimeout(()=>{
      if(el.copyLinkStatus){
        el.copyLinkStatus.textContent="";
        el.copyLinkStatus.classList.remove("error");
      }
    },2600);
  }
}

async function copyCurrentViewLink(){
  saveState();
  const link=buildShareUrl();
  try{
    if(navigator.clipboard && navigator.clipboard.writeText){
      await navigator.clipboard.writeText(link);
      setCopyStatus("Link copiado.");
      trackEvent("copy_view_link",{method:"clipboard"});
      return;
    }
  }catch(_e){}
  try{
    const ta=document.createElement("textarea");
    ta.value=link;
    ta.style.position="fixed";
    ta.style.left="-9999px";
    document.body.appendChild(ta);
    ta.focus();
    ta.select();
    const ok=document.execCommand("copy");
    document.body.removeChild(ta);
    if(ok){
      setCopyStatus("Link copiado.");
      trackEvent("copy_view_link",{method:"execCommand"});
      return;
    }
  }catch(_e){}
  setCopyStatus("No se pudo copiar. Copialo manualmente desde la barra.",true);
  trackEvent("copy_view_link_failed",{});
}

const refsNom={};
const refsReal={};
for(const r of (p.monthly_reference||[])){
  if(!refsNom[r.canonical_id])refsNom[r.canonical_id]={};
  if(!refsReal[r.canonical_id])refsReal[r.canonical_id]={};
  refsNom[r.canonical_id][r.month]=r.avg_price;
  refsReal[r.canonical_id][r.month]=r.avg_real_price;
}
const timelineById={};
for(const t of (p.timeline||[])){
  if(!timelineById[t.canonical_id]) timelineById[t.canonical_id]=[];
  timelineById[t.canonical_id].push(t);
}
const bandById={};
for(const b of (p.candidate_bands||[])){
  if(!bandById[b.canonical_id]) bandById[b.canonical_id]=[];
  bandById[b.canonical_id].push(b);
}
let _rowsCacheKey="";
let _rowsCacheValue=[];
let _lastMainChartKey="";
let _lastSecondaryChartKey="";
const lazyPanels={
  bands_ready:false,
  quality_ready:false,
  observer:null
};

function calcVar(current,base){
  if(base==null||Number(base)<=0||current==null)return null;
  return ((Number(current)-Number(base))/Number(base))*100;
}

function filteredRows(){
  const cacheKey=[
    st.query,st.cba_filter,st.category,st.sort_by,st.base_month,
    p.snapshot?.length||0,
  ].join("|");
  if(cacheKey===_rowsCacheKey){
    return _rowsCacheValue;
  }

  let out=[...(p.snapshot||[])];
  const needle=norm(st.query);
  if(needle){
    out=out.filter(r=>norm(`${r.product_name} ${r.canonical_id}`).includes(needle));
  }
  if(st.cba_filter==="yes")out=out.filter(r=>!!r.is_cba);
  if(st.cba_filter==="no")out=out.filter(r=>!r.is_cba);
  if(st.category!=="all")out=out.filter(r=>(r.category||"sin_categoria")===st.category);

  out=out.map(r=>{
    const nomBase=refsNom[r.canonical_id]?.[st.base_month];
    const realBase=refsReal[r.canonical_id]?.[st.base_month];
    return {
      ...r,
      variation_nominal_pct:calcVar(r.current_price,nomBase),
      variation_real_pct:calcVar(r.current_real_price,realBase)
    };
  });

  if(st.sort_by==="price"){
    out.sort((a,b)=>(a.current_price??Number.POSITIVE_INFINITY)-(b.current_price??Number.POSITIVE_INFINITY));
  }else if(st.sort_by==="var_nominal"){
    out.sort((a,b)=>(b.variation_nominal_pct??Number.NEGATIVE_INFINITY)-(a.variation_nominal_pct??Number.NEGATIVE_INFINITY));
  }else if(st.sort_by==="var_real"){
    out.sort((a,b)=>(b.variation_real_pct??Number.NEGATIVE_INFINITY)-(a.variation_real_pct??Number.NEGATIVE_INFINITY));
  }else{
    out.sort((a,b)=>norm(a.product_name||a.canonical_id).localeCompare(norm(b.product_name||b.canonical_id),"es"));
  }
  _rowsCacheKey=cacheKey;
  _rowsCacheValue=out;
  return _rowsCacheValue;
}

function syncSelection(rows){
  const ids=rows.map(r=>r.canonical_id);
  const prev=new Set(st.selected_products||[]);
  let selected=ids.filter(x=>prev.has(x));
  if(!selected.length){
    const pref=(defaults.selected_products||[]).filter(x=>ids.includes(x));
    selected=pref.length?pref:ids.slice(0,5);
  }
  st.selected_products=selected;
  el.sel.innerHTML="";
  for(const r of rows){
    const o=document.createElement("option");
    o.value=r.canonical_id;
    o.selected=selected.includes(r.canonical_id);
    o.textContent=`${r.product_name||r.canonical_id} (${r.canonical_id})`;
    el.sel.appendChild(o);
  }
  if(el.selectionMeta){
    el.selectionMeta.textContent=`${st.selected_products.length} productos seleccionados`;
  }
}

function paginatedRows(rows){
  const total=Math.max(0, rows.length);
  const pageSize=Math.max(1, Number(st.page_size||25));
  const totalPages=Math.max(1, Math.ceil(total/pageSize));
  st.current_page=Math.min(Math.max(1, Number(st.current_page||1)), totalPages);
  const start=(st.current_page-1)*pageSize;
  return {
    total,
    pageSize,
    totalPages,
    pageRows:rows.slice(start, start+pageSize),
  };
}

function updateTableMeta(total,totalPages){
  if(el.tableMeta){
    el.tableMeta.textContent=`${total} productos filtrados | ${st.selected_products.length} en grafico`;
  }
  if(el.pageInfo){
    el.pageInfo.textContent=`${st.current_page} / ${totalPages}`;
  }
  if(el.pagePrev){
    el.pagePrev.disabled=st.current_page<=1;
  }
  if(el.pageNext){
    el.pageNext.disabled=st.current_page>=totalPages;
  }
}

function drawActiveFilters(totalRows){
  if(!el.activeFilters) return;
  const defaultBase=p.months?.[0]||"";
  const sortLabels={alphabetical:"Alfabetico",price:"Precio",var_nominal:"Var. nominal",var_real:"Var. real"};
  const chips=[];
  if((st.query||"").trim()) chips.push({key:"query",label:`Busqueda: "${st.query.trim()}"`});
  if(st.cba_filter!=="all") chips.push({key:"cba",label:`CBA: ${st.cba_filter==="yes"?"Si":"No"}`});
  if(st.category!=="all") chips.push({key:"category",label:`Categoria: ${st.category}`});
  if(st.sort_by!=="alphabetical") chips.push({key:"sort_by",label:`Orden: ${sortLabels[st.sort_by]||st.sort_by}`});
  if(st.base_month && st.base_month!==defaultBase) chips.push({key:"base_month",label:`Base: ${st.base_month}`});
  if(st.price_mode==="real") chips.push({key:"price_mode",label:"Modo: Real"});
  if(st.show_real_column) chips.push({key:"show_real_column",label:"Tabla: var. real visible"});
  if(st.macro_scope==="rubros") chips.push({key:"macro_scope",label:"Macro: Rubros"});
  if(st.macro_region && st.macro_region!==(p.macro_default_region||"patagonia")) chips.push({key:"macro_region",label:`Region macro: ${st.macro_region}`});
  if(st.macro_scope==="rubros" && st.macro_category) chips.push({key:"macro_category",label:`Rubro macro: ${st.macro_category}`});

  const html=[`<span class="pill pill-info">Productos filtrados: ${totalRows}</span>`,`<span class="pill">En grafico: ${st.selected_products.length}</span>`];
  if(!chips.length){
    html.push(`<span class="pill">Sin filtros adicionales</span>`);
  }else{
    chips.forEach(item=>{
      html.push(
        `<button type="button" class="pill pill-action" data-filter="${item.key}" title="Quitar filtro">`
        + `${esc(item.label)} <span aria-hidden="true">x</span></button>`
      );
    });
  }
  el.activeFilters.innerHTML=html.join("");
}

function exportFilteredCsv(rows){
  const headers=["canonical_id","product_name","presentation","category","is_cba","current_price","var_nominal_pct","var_real_pct","product_url"];
  const lines=[headers.join(",")];
  const escCsv=v=>{
    const s=String(v??"");
    // Keep "\\n" literal in generated JS regex (Python string parsing would otherwise inject a real newline).
    if(/[",\\n;]/.test(s)) return `"${s.replace(/"/g,'""')}"`;
    return s;
  };
  rows.forEach(r=>{
    lines.push([
      r.canonical_id,
      r.product_name,
      resolvePresentation(r),
      r.category,
      r.is_cba ? "1" : "0",
      r.current_price,
      r.variation_nominal_pct,
      r.variation_real_pct,
      r.product_url || "",
    ].map(escCsv).join(","));
  });
  const blob=new Blob([lines.join("\\n")],{type:"text/csv;charset=utf-8;"});
  const a=document.createElement("a");
  a.href=URL.createObjectURL(blob);
  a.download=`reporte_filtrado_${p.from_month}_${p.to_month}.csv`;
  a.style.display="none";
  document.body.appendChild(a);
  a.click();
  setTimeout(()=>{URL.revokeObjectURL(a.href);a.remove();},0);
  trackEvent("export_csv",{rows:rows.length||0,from:p.from_month,to:p.to_month});
}

function toSeriesForMainChart(rows){
  const selected=new Set(st.selected_products||[]);
  const grouped={};
  for(const id of selected){
    const series=timelineById[id]||[];
    if(series.length){
      grouped[id]=series;
    }
  }
  const byId={};
  for(const item of rows)byId[item.canonical_id]=item;
  return Object.keys(grouped).map((id,i)=>{
    const pts=[...grouped[id]].sort((a,b)=>new Date(a.scraped_at)-new Date(b.scraped_at)).map((x)=>({
      x:new Date(x.scraped_at),
      y:st.price_mode==="real"?x.current_real_price:x.current_price
    })).filter(pt=>pt.y!=null && Number.isFinite(Number(pt.y)));
    return {
      name:`${byId[id]?.product_name||id} (${id})`,
      points:pts,
      color:COLORS[i%COLORS.length]
    };
  }).filter(s=>s.points.length>0);
}

function drawCanvasChart(container,legend,series,yLabel,xLabel="Tiempo"){
  container.innerHTML="";
  legend.innerHTML="";
  const normalizedSeries=(series||[]).map(s=>({
    ...s,
    points:(s.points||[]).map(pt=>({
      x:pt?.x instanceof Date ? pt.x : new Date(pt?.x),
      y:Number(pt?.y),
    })).filter(pt=>Number.isFinite(pt.x.getTime()) && Number.isFinite(pt.y))
  })).filter(s=>s.points.length>0);
  if(!normalizedSeries.length){
    const m=document.createElement("div");
    m.className="chart-empty";
    m.textContent="Sin datos para graficar";
    container.appendChild(m);
    return;
  }

  const canvas=document.createElement("canvas");
  const tooltip=document.createElement("div");
  tooltip.className="chart-tooltip";
  container.appendChild(canvas);
  container.appendChild(tooltip);
  const w=Math.max(320,container.clientWidth||760);
  const h=Math.max(220,container.classList.contains("small")?252:366);
  const ratio=Math.max(1,window.devicePixelRatio||1);
  canvas.width=Math.floor(w*ratio);
  canvas.height=Math.floor(h*ratio);
  canvas.style.width=`${w}px`;
  canvas.style.height=`${h}px`;
  const ctx=canvas.getContext("2d");
  if(!ctx){
    const m=document.createElement("div");
    m.className="chart-empty";
    m.textContent="No se pudo inicializar el canvas";
    container.appendChild(m);
    return;
  }
  ctx.scale(ratio,ratio);

  const xs=normalizedSeries.flatMap(s=>s.points.map(p=>p.x.getTime())).filter(Number.isFinite);
  const ys=normalizedSeries.flatMap(s=>s.points.map(p=>Number(p.y))).filter(Number.isFinite);
  if(!xs.length||!ys.length){
    const m=document.createElement("div");
    m.className="chart-empty";
    m.textContent="Sin datos para graficar";
    container.appendChild(m);
    return;
  }

  const minX=Math.min(...xs);
  const maxX=Math.max(...xs);
  const minY=Math.min(...ys);
  const maxY=Math.max(...ys);
  const ySpan=Math.max(1,maxY-minY||1);
  const yMin=minY-ySpan*0.08;
  const yMax=maxY+ySpan*0.08;

  const yTicks=[0,0.25,0.5,0.75,1];
  ctx.font="12px Aptos";
  const yTickLabels=yTicks.map(t=>fmtAxisNum(yMin+(yMax-yMin)*t));
  const maxYLabelW=yTickLabels.reduce((acc,label)=>Math.max(acc,ctx.measureText(label).width),0);
  const pad={l:Math.max(78,Math.ceil(maxYLabelW)+44),r:20,t:20,b:58};
  const innerW=Math.max(40,w-pad.l-pad.r);
  const innerH=Math.max(40,h-pad.t-pad.b);
  const mapX=x=>pad.l+((x-minX)/(Math.max(1,maxX-minX)))*innerW;
  const mapY=y=>pad.t+((yMax-y)/(Math.max(1,yMax-yMin)))*innerH;
  const xTickCount=maxX===minX?1:Math.min(6,Math.max(2,Math.floor(innerW/120)));
  const xTicks=xTickCount===1
    ? [0.5]
    : Array.from({length:xTickCount},(_,i)=>i/(xTickCount-1));
  const mappedSeries=normalizedSeries.map(s=>({
    ...s,
    mapped:s.points.map(pt=>{
      const rawX=pt.x.getTime();
      const rawY=Number(pt.y);
      return {rawX,rawY,px:mapX(rawX),py:mapY(rawY)};
    })
  }));
  const allPoints=mappedSeries.flatMap(s=>s.mapped.map(pt=>({...pt,name:s.name,color:s.color})));

  function drawFrame(hover){
    ctx.clearRect(0,0,w,h);
    ctx.strokeStyle="#d8e2ed";
    ctx.lineWidth=1;
    for(const t of yTicks){
      const y=pad.t+innerH*(1-t);
      ctx.beginPath();
      ctx.moveTo(pad.l,y);
      ctx.lineTo(w-pad.r,y);
      ctx.stroke();
    }
    for(const t of xTicks){
      const x=pad.l+innerW*t;
      ctx.beginPath();
      ctx.moveTo(x,pad.t);
      ctx.lineTo(x,h-pad.b);
      ctx.stroke();
    }
    ctx.beginPath();
    ctx.moveTo(pad.l,pad.t);
    ctx.lineTo(pad.l,h-pad.b);
    ctx.lineTo(w-pad.r,h-pad.b);
    ctx.strokeStyle="#8b98ac";
    ctx.stroke();

    ctx.fillStyle="#5a6577";
    ctx.font="12px Aptos";
    ctx.textBaseline="middle";
    ctx.textAlign="right";
    for(const t of yTicks){
      const y=pad.t+innerH*(1-t);
      const val=(yMin+(yMax-yMin)*t);
      ctx.fillText(fmtAxisNum(val),pad.l-12,y);
    }
    ctx.save();
    ctx.translate(18,pad.t+(innerH/2));
    ctx.rotate(-Math.PI/2);
    ctx.textAlign="center";
    ctx.fillText(yLabel,0,0);
    ctx.restore();

    ctx.textBaseline="top";
    xTicks.forEach((t,idx)=>{
      const x=pad.l+innerW*t;
      const ts=minX+(maxX-minX)*t;
      const lbl=fmtMonthTick(ts);
      ctx.textAlign=xTicks.length===1?"center":(idx===0?"left":(idx===xTicks.length-1?"right":"center"));
      ctx.fillText(lbl,x,h-pad.b+12);
    });
    ctx.textAlign="center";
    ctx.fillText(xLabel,w/2,h-17);

    mappedSeries.forEach(s=>{
      if(!s.mapped.length)return;
      ctx.strokeStyle=s.color;
      ctx.lineWidth=2.4;
      ctx.beginPath();
      s.mapped.forEach((pt,idx)=>{
        if(idx===0)ctx.moveTo(pt.px,pt.py);else ctx.lineTo(pt.px,pt.py);
      });
      ctx.stroke();

      if(s.mapped.length<=36){
        ctx.fillStyle=s.color;
        s.mapped.forEach(pt=>{
          ctx.beginPath();
          ctx.arc(pt.px,pt.py,2.5,0,Math.PI*2);
          ctx.fill();
        });
      }

      const latest=s.mapped[s.mapped.length-1];
      if(latest){
        ctx.beginPath();
        ctx.fillStyle="#fff";
        ctx.arc(latest.px,latest.py,3.7,0,Math.PI*2);
        ctx.fill();
        ctx.lineWidth=1.8;
        ctx.strokeStyle=s.color;
        ctx.stroke();
      }
    });

    if(hover?.points?.length){
      ctx.save();
      ctx.setLineDash([6,5]);
      ctx.strokeStyle="#75849a";
      ctx.lineWidth=1.2;
      ctx.beginPath();
      ctx.moveTo(hover.xPx,pad.t);
      ctx.lineTo(hover.xPx,h-pad.b);
      ctx.stroke();
      ctx.restore();

      hover.points.forEach(pt=>{
        ctx.beginPath();
        ctx.fillStyle=pt.color;
        ctx.arc(pt.px,pt.py,4.2,0,Math.PI*2);
        ctx.fill();
        ctx.lineWidth=2;
        ctx.strokeStyle="#fff";
        ctx.stroke();
      });
    }
  }
  function nearestHover(mouseX){
    if(!allPoints.length) return null;
    let nearest=allPoints[0];
    let minDist=Number.POSITIVE_INFINITY;
    for(const pt of allPoints){
      const dist=Math.abs(pt.px-mouseX);
      if(dist<minDist){
        minDist=dist;
        nearest=pt;
      }
    }
    if(!nearest) return null;
    const targetX=nearest.rawX;
    const points=mappedSeries.map(s=>{
      let localNearest=s.mapped[0];
      let localDist=Number.POSITIVE_INFINITY;
      for(const pt of s.mapped){
        const dist=Math.abs(pt.rawX-targetX);
        if(dist<localDist){
          localDist=dist;
          localNearest=pt;
        }
      }
      if(!localNearest) return null;
      return {...localNearest,name:s.name,color:s.color};
    }).filter(Boolean);
    if(!points.length) return null;
    return {xPx:mapX(targetX),targetX,points};
  }

  function hideTooltip(){
    tooltip.classList.remove("visible");
    tooltip.innerHTML="";
  }

  function showTooltip(hover,mouseX,mouseY){
    if(!hover?.points?.length){
      hideTooltip();
      return;
    }
    const lines=hover.points.map(pt=>
      `<div><span class="dot" style="background:${pt.color}"></span> ${esc(pt.name)}: <strong>${esc(fmtAxisNum(pt.rawY))}</strong></div>`
    );
    tooltip.innerHTML=`<strong>${esc(fmtDate(hover.targetX))}</strong>${lines.join("")}`;
    tooltip.classList.add("visible");
    const tipRect=tooltip.getBoundingClientRect();
    let left=mouseX+14;
    let top=mouseY-tipRect.height-10;
    if(left+tipRect.width>w-6) left=mouseX-tipRect.width-14;
    if(top<6) top=mouseY+14;
    tooltip.style.left=`${Math.max(6,Math.min(w-tipRect.width-6,left))}px`;
    tooltip.style.top=`${Math.max(6,Math.min(h-tipRect.height-6,top))}px`;
  }

  drawFrame(null);
  canvas.addEventListener("mousemove",(ev)=>{
    const rect=canvas.getBoundingClientRect();
    const mx=ev.clientX-rect.left;
    const my=ev.clientY-rect.top;
    const inside=mx>=pad.l && mx<=w-pad.r && my>=pad.t && my<=h-pad.b;
    if(!inside){
      hideTooltip();
      drawFrame(null);
      return;
    }
    const hover=nearestHover(mx);
    drawFrame(hover);
    if(hover){
      showTooltip(hover,mx,my);
    }else{
      hideTooltip();
    }
  });
  canvas.addEventListener("mouseleave",()=>{
    hideTooltip();
    drawFrame(null);
  });

  for(const s of mappedSeries){
    const lastPoint=s.mapped[s.mapped.length-1];
    const item=document.createElement("div");
    item.className="item";
    const valueLabel=lastPoint?fmtAxisNum(lastPoint.rawY):"N/D";
    item.innerHTML=`<span class="dot" style="background:${s.color}"></span>${esc(s.name)}: <strong>${esc(valueLabel)}</strong>`;
    legend.appendChild(item);
  }
}

function drawMainChart(rows,force=false){
  const chartKey=[
    st.price_mode,
    ...(st.selected_products||[]),
    rows.length,
    p.timeline?.length||0,
  ].join("|");
  if(!force && chartKey===_lastMainChartKey){
    return;
  }
  _lastMainChartKey=chartKey;
  const series=toSeriesForMainChart(rows);
  const yLabel=st.price_mode==="real"?"Precio real (ARS constantes)":"Precio nominal (ARS)";
  drawCanvasChart(el.chartMain,el.legendMain,series,yLabel);
}

function safeText(value,fallback="N/D"){
  if(value===null || value===undefined) return fallback;
  if(typeof value==="number" && Number.isNaN(value)) return fallback;
  const txt=String(value).trim();
  if(!txt) return fallback;
  const normalized=txt.toLowerCase();
  if(normalized==="nan" || normalized==="none" || normalized==="null") return fallback;
  return txt;
}

function computeIndependentBase100(rows,indexKey){
  const baseRow=rows.find(r=>r[indexKey]!=null);
  const base=baseRow?Number(baseRow[indexKey]):null;
  if(base==null || !Number.isFinite(base) || base<=0){
    return rows.map(()=>null);
  }
  return rows.map(r=>{
    const value=Number(r[indexKey]);
    if(!Number.isFinite(value)) return null;
    return (value/base)*100;
  });
}

function drawSecondaryChart(force=false){
  const region=st.macro_region||p.macro_default_region||"patagonia";
  const regionLabel=region==="nacional"?"Nacional":"Patagonia";
  const generalSrc=(p.ipc_comparison_by_region?.[region]||p.ipc_comparison_series||[]);
  const categorySrc=(p.category_comparison_by_region?.[region]||p.category_comparison_series||[]);
  const secondaryKey=[
    st.view,
    st.macro_scope,
    region,
    st.macro_category,
    generalSrc.length,
    categorySrc.length,
    p.basket_vs_ipc_series?.length||0
  ].join("|");
  if(!force && secondaryKey===_lastSecondaryChartKey){
    return;
  }
  _lastSecondaryChartKey=secondaryKey;
  if(st.view==="executive"){
    el.panelSecondary.style.display="none";
  }else{
    el.panelSecondary.style.display="";
  }

  if(el.macroScope){
    if(!["general","rubros"].includes(st.macro_scope)){
      st.macro_scope="general";
    }
    el.macroScope.value=st.macro_scope;
  }
  if(el.macroRegion){
    const validRegions=Array.from(el.macroRegion.options).map(o=>o.value);
    if(!validRegions.includes(st.macro_region)){
      st.macro_region=validRegions.includes(p.macro_default_region||"") ? (p.macro_default_region||"") : (validRegions[0]||"patagonia");
    }
    el.macroRegion.value=st.macro_region;
  }

  let src=[];
  let macroLabel="General";
  if(st.macro_scope==="rubros"){
    const categories=[...new Set(categorySrc.map(x=>x.category_slug).filter(Boolean))].sort();
    if(el.macroCategory){
      el.macroCategory.innerHTML="";
      if(!categories.length){
        const empty=document.createElement("option");
        empty.value="";
        empty.textContent="Sin rubros comparables";
        el.macroCategory.appendChild(empty);
      }else{
        categories.forEach(cat=>{
          const o=document.createElement("option");
          o.value=cat;
          o.textContent=cat;
          el.macroCategory.appendChild(o);
        });
      }
      if(!categories.includes(st.macro_category)){
        st.macro_category=categories[0]||"";
      }
      el.macroCategory.value=st.macro_category;
      el.macroCategory.disabled=!categories.length;
    }
    src=categorySrc.filter(x=>x.category_slug===st.macro_category);
    macroLabel=st.macro_category?`Rubro: ${st.macro_category}`:"Rubros";
  }else{
    if(el.macroCategory){
      const o=document.createElement("option");
      o.value="";
      o.textContent="No aplica en General";
      el.macroCategory.innerHTML="";
      el.macroCategory.appendChild(o);
      el.macroCategory.value="";
      el.macroCategory.disabled=true;
    }
    src=generalSrc;
  }

  if(!src.length && (p.basket_vs_ipc_series||[]).length){
    src=(p.basket_vs_ipc_series||[]).map(x=>({
      year_month:x.year_month,
      tracker_index_base100:x.basket_index_base100,
      official_index_base100:x.ipc_index_base100,
      plot_tracker_base100:x.basket_index_base100,
      plot_official_base100:x.ipc_index_base100,
      plot_mode:"strict_overlap",
      tracker_base_month:null,
      official_base_month:null,
      is_strictly_comparable:true,
      gap_index_points:x.gap_points,
      tracker_status:null,
      official_status:null
    }));
  }

  const trackerPlotRaw=src.map(x=>(x.plot_tracker_base100 ?? x.tracker_index_base100 ?? null));
  const officialPlotRaw=src.map(x=>(x.plot_official_base100 ?? x.official_index_base100 ?? null));
  let trackerPlot=trackerPlotRaw;
  let officialPlot=officialPlotRaw;
  let plotMode=src.some(x=>x.plot_mode==="independent_base") ? "independent_base" : "strict_overlap";
  if(!trackerPlot.some(v=>v!=null)){
    const rebuilt=computeIndependentBase100(src,"tracker_index");
    if(rebuilt.some(v=>v!=null)){
      trackerPlot=rebuilt;
      plotMode="independent_base";
    }
  }
  if(!officialPlot.some(v=>v!=null)){
    const rebuilt=computeIndependentBase100(src,"official_index");
    if(rebuilt.some(v=>v!=null)){
      officialPlot=rebuilt;
      plotMode="independent_base";
    }
  }
  const strictComparable=src.some(
    x=>((x.is_strictly_comparable===true) || (x.is_strictly_comparable===undefined && x.gap_index_points!=null))
      && x.gap_index_points!=null
  );
  const tracker=src
    .map((row,idx)=>({x:new Date(`${row.year_month}-01T00:00:00`),y:trackerPlot[idx]}))
    .filter(x=>x.y!=null);
  const official=src
    .map((row,idx)=>({x:new Date(`${row.year_month}-01T00:00:00`),y:officialPlot[idx]}))
    .filter(x=>x.y!=null);
  const gap=(strictComparable ? src : [])
    .map(x=>({x:new Date(`${x.year_month}-01T00:00:00`),y:x.gap_index_points}))
    .filter(x=>x.y!=null);
  const series=[
    {name:"IPC propio base 100",points:tracker,color:"#005f73"},
    {name:`IPC ${regionLabel} base 100`,points:official,color:"#ca6702"},
    {name:"Brecha (puntos)",points:gap,color:"#9b2226"}
  ].filter(s=>s.points.length>0);
  drawCanvasChart(el.chartSecondary,el.legendSecondary,series,"Indice base 100 / brecha");

  if(el.macroNotice){
    const latestTrackerMonth=safeText((p.publication_status_by_region?.[region]||{}).latest_tracker_month, null)
      || safeText(([...src].reverse().find(x=>x.tracker_index!=null)||{}).year_month, "N/D");
    const latestOfficialMonth=safeText((p.publication_status_by_region?.[region]||{}).latest_official_month, null)
      || safeText(([...src].reverse().find(x=>x.official_index!=null)||{}).year_month, "N/D");
    if((tracker.length>0 || official.length>0) && (plotMode==="independent_base" || !strictComparable)){
      el.macroNotice.hidden=false;
      el.macroNotice.textContent=
        `Comparacion parcial: IPC oficial disponible hasta ${latestOfficialMonth} e IPC propio hasta ${latestTrackerMonth}. `
        + "La brecha en puntos solo aparece cuando ambos tienen meses estrictamente comparables.";
    }else{
      el.macroNotice.hidden=true;
      el.macroNotice.textContent="";
    }
  }

  if(el.macroStatus){
    const latest=[...src].reverse().find(x=>x.year_month)||null;
    const latestTracker=[...src].reverse().find(x=>x.tracker_index!=null)||null;
    const latestOfficial=[...src].reverse().find(x=>x.official_index!=null)||null;
    const pub=(p.publication_status_by_region?.[region]||p.publication_status||{});
    const trackerStatus=safeText(pub.latest_tracker_status ?? latestTracker?.tracker_status, latestTracker ? "disponible" : "N/D");
    const officialStatus=safeText(pub.latest_official_status ?? latestOfficial?.official_status, latestOfficial ? "disponible" : "N/D");
    const trackerMonth=safeText(pub.latest_tracker_month ?? latestTracker?.year_month, "N/D");
    const officialMonth=safeText(pub.latest_official_month ?? latestOfficial?.year_month, "N/D");
    const pubStatus=safeText(pub.status, "sin_publicacion");
    const statusOrigin=safeText(pub.status_origin, "publication_run");
    const latestMonth=safeText(latest?.year_month, "N/D");
    const comparability=strictComparable ? "estricta" : "parcial";
    const derivedNote=statusOrigin==="derived_from_series" ? " (derivado de series)" : "";
    el.macroStatus.textContent=
      `${macroLabel} | region: ${regionLabel} | ultimo mes con dato: ${latestMonth} | `
      + `IPC propio: ${trackerMonth} (${trackerStatus}) | IPC oficial: ${officialMonth} (${officialStatus}) | `
      + `comparacion ${comparability} | publicacion: ${pubStatus}${derivedNote}`;
  }
}

function mountBandOptions(rows){
  if(!el.bandProduct) return;
  const candidates=rows.filter(r=>(bandById[r.canonical_id]||[]).length>0);
  el.bandProduct.innerHTML="";
  if(!candidates.length){
    st.band_product="";
    return;
  }
  candidates.forEach(r=>{
    const o=document.createElement("option");
    o.value=r.canonical_id;
    o.textContent=`${r.product_name||r.canonical_id} (${r.canonical_id})`;
    el.bandProduct.appendChild(o);
  });
  const validIds=candidates.map(r=>r.canonical_id);
  if(!validIds.includes(st.band_product)){
    st.band_product=validIds[0];
  }
  el.bandProduct.value=st.band_product;
}

function drawBandChart(rows){
  if(!el.panelBands||!el.chartBands||!el.legendBands){
    return;
  }
  const withBands=rows.filter(r=>(bandById[r.canonical_id]||[]).length>0);
  if(!withBands.length){
    el.panelBands.style.display="none";
    return;
  }
  el.panelBands.style.display="";
  mountBandOptions(rows);

  const selectedId=st.band_product;
  const src=[...(bandById[selectedId]||[])].sort((a,b)=>new Date(a.scraped_at)-new Date(b.scraped_at));
  if(!src.length){
    drawCanvasChart(el.chartBands,el.legendBands,[],"Precio (ARS)");
    if(el.bandMeta) el.bandMeta.textContent="Sin observaciones de terna para el producto seleccionado.";
    return;
  }

  const low=src.map(x=>({x:new Date(x.scraped_at),y:x.low_price})).filter(x=>x.y!=null);
  const mid=src.map(x=>({x:new Date(x.scraped_at),y:x.mid_price})).filter(x=>x.y!=null);
  const high=src.map(x=>({x:new Date(x.scraped_at),y:x.high_price})).filter(x=>x.y!=null);
  const series=[
    {name:"Low",points:low,color:"#6c757d"},
    {name:"Mid (representativo)",points:mid,color:"#005f73"},
    {name:"High",points:high,color:"#ca6702"},
  ].filter(s=>s.points.length>0);
  drawCanvasChart(el.chartBands,el.legendBands,series,"Precio nominal (ARS)");

  const latest=src[src.length-1]||{};
  if(el.bandMeta){
    el.bandMeta.textContent=
      `Ultima dispersion: low=${money(latest.low_price)} | mid=${money(latest.mid_price)} | `
      + `high=${money(latest.high_price)} | spread=${pct(latest.spread_pct)}`;
  }
}

function drawTable(rows){
  el.tb.innerHTML="";
  el.thVarReal.style.display=st.show_real_column?"":"none";
  const {total,pageRows,totalPages}=paginatedRows(rows);
  updateTableMeta(total,totalPages);
  if(!total){
    el.tb.innerHTML=`<tr><td colspan="${st.show_real_column?5:4}" class="muted">Sin datos para los filtros actuales.</td></tr>`;
    return;
  }
  for(const r of pageRows){
    const tr=document.createElement("tr");
    const name=esc(r.product_name||r.canonical_id);
    const linked=r.product_url
      ? `<a href="${esc(r.product_url)}" target="_blank" rel="noopener noreferrer" title="${name}">${name}</a>`
      : `<span title="${name}">${name}</span>`;
    const nomCls=trendClass(r.variation_nominal_pct);
    const realCls=trendClass(r.variation_real_pct);
    const presentation=resolvePresentation(r);
    tr.innerHTML=`
      <td>${linked}</td>
      <td>${esc(presentation)}</td>
      <td class="num">${money(r.current_price)}</td>
      <td class="num"><span class="${nomCls}">${trendIcon(r.variation_nominal_pct)} ${pctSigned(r.variation_nominal_pct)}</span></td>
      ${st.show_real_column?`<td class="num"><span class="${realCls}">${trendIcon(r.variation_real_pct)} ${pctSigned(r.variation_real_pct)}</span></td>`:""}
    `;
    el.tb.appendChild(tr);
  }
}

function drawKpis(){
  const k=p.kpi_summary||{};
  const periodLabel=k.kpi_fallback_used
    ? `Periodo efectivo ${monthLabel(k.from_month)} a ${monthLabel(k.to_month)} (fallback por datos)`
    : `Periodo ${monthLabel(k.from_month)} a ${monthLabel(k.to_month)}`;
  const cards=[
    ["Inflacion canasta nominal", pct(k.inflation_basket_nominal_pct), periodLabel],
    ["IPC oficial periodo", pct(k.ipc_period_pct), "Benchmark INDEC"],
    ["Brecha canasta - IPC", pct(k.gap_vs_ipc_pp), "puntos porcentuales"],
    ["Inflacion real canasta", pct(k.inflation_basket_real_pct), "Deflactada por IPC"],
    ["Amplitud de subas", pct(k.amplitude_up_pct), "% de productos con suba"],
    ["Dispersion (IQR)", pct(k.dispersion_iqr_pct), "P75 - P25 de variaciones"],
  ];
  const tone=(label,value)=>{
    if(value==null||Number.isNaN(Number(value))) return "warn";
    const n=Number(value);
    if(label.includes("Brecha")) return n>0 ? "bad" : (n<0 ? "good" : "warn");
    if(label.includes("Inflacion canasta nominal")) return n>0 ? "bad" : (n<0 ? "good" : "warn");
    if(label.includes("Inflacion real")) return n>0 ? "bad" : (n<0 ? "good" : "warn");
    if(label.includes("Amplitud")) return n>=50 ? "bad" : "good";
    if(label.includes("Dispersion")) return n>15 ? "bad" : "good";
    return n>0 ? "bad" : "good";
  };
  el.kpiGrid.innerHTML="";
  cards.forEach(([label,value,sub])=>{
    const card=document.createElement("article");
    card.className=`kpi ${tone(label,value)}`;
    card.innerHTML=`<div class="label">${esc(label)}</div><div class="value">${esc(value)}</div><div class="sub">${esc(sub)}</div>`;
    el.kpiGrid.appendChild(card);
  });
}

function drawQuality(){
  const qf=p.quality_flags||{};
  const cov=p.coverage||{};
  const k=p.kpi_summary||{};
  const sq=p.scrape_quality||{};
  const region=st.macro_region||p.macro_default_region||"patagonia";
  const regionLabel=region==="nacional"?"Nacional":"Patagonia";
  const pub=(p.publication_status_by_region?.[region]||p.publication_status||{});
  const trackerSeries=p.tracker_ipc_series||[];
  const officialSeries=(p.official_series_by_region?.[region]||p.official_patagonia_series||[]);
  const latestTracker=( [...trackerSeries].reverse().find(x=>x.index_value!=null) || trackerSeries[trackerSeries.length-1] || {} );
  const latestOfficial=( [...officialSeries].reverse().find(x=>x.cpi_index!=null) || officialSeries[officialSeries.length-1] || {} );
  const cba=sq.cba||{};
  const core=sq.daily_core||{};
  const rot=sq.daily_rotation||{};
  const missingMonths=(qf.missing_cpi_months||[]);
  el.qualityBadge.textContent=qf.badge||"Datos parciales";
  el.qualityBadge.className=`badge${qf.is_partial?" warn":""}`;
  el.qualityCoverage.textContent=
    `Cobertura canasta: ${fmtNum(cov.coverage_total_pct)}% `
    + `(${cov.observed_products_total ?? "N/D"} de ${cov.expected_products ?? "N/D"} productos esperados).`;
  el.qualityPanelSize.textContent=`Panel util para lectura: ${qf.balanced_panel_n ?? "N/D"} productos.`;
  if(el.qualityMacro){
    el.qualityMacro.textContent=
      `IPC propio: inicio ${(trackerSeries[0]?.year_month)||"N/D"} | ultimo ${(latestTracker?.year_month)||"N/D"} `
      + `| estado ${(safeText(latestTracker?.status) || "N/D")}.`;
  }
  el.qualityIpc.textContent=
    `IPC oficial (${regionLabel}): ultimo ${(latestOfficial?.year_month)||"N/D"} | `
    + `fuente ${(pub.official_source_effective)||pub.official_source||"N/D"} | `
    + `estado publicacion ${(pub.status)||"sin_publicacion"}`
    + `${safeText(pub.status_origin,"publication_run")==="derived_from_series" ? " (derivado de series)." : "."}`
    + ` Meses sin IPC: ${missingMonths.length? missingMonths.join(", ") : "ninguno"}.`;
  if(el.qualitySegments){
    el.qualitySegments.textContent=
      `Segmentos del dia -> CBA: ${cba.observed ?? 0}/${cba.expected ?? 0} (${fmtNum(cba.coverage_pct)}%), `
      + `Nucleo: ${core.observed ?? 0}/${core.expected ?? 0} (${fmtNum(core.coverage_pct)}%), `
      + `Rotacion: ${rot.observed ?? 0}/${rot.expected ?? 0} (${fmtNum(rot.coverage_pct)}%).`;
  }
  if(el.qualityPolicy){
    el.qualityPolicy.textContent=
      `Regla de publicacion: ${p.publication_policy || "N/D"}. `
      + `Se mantiene terna low/mid/high auditada con cumplimiento ${pct(sq.terna_compliance_pct)} `
      + `(${sq.products_with_full_terna ?? 0}/${sq.products_with_bands ?? 0}).`;
  }
  el.warnings.innerHTML="";
  for(const w of (qf.warnings||[])){
    const li=document.createElement("li");
    li.textContent=w;
    el.warnings.appendChild(li);
  }
  if(k.kpi_fallback_used){
    const li=document.createElement("li");
    li.textContent=`KPI calculado con ventana efectiva ${k.from_month} -> ${k.to_month} por disponibilidad de datos.`;
    el.warnings.appendChild(li);
  }
  if(st.view==="executive"){
    el.qualityPanel.style.display="none";
  }else{
    el.qualityPanel.style.display="";
  }
  if(el.freshnessMeta){
    const status=p.web_status||"partial";
    const nextRun=p.next_update_eta||"N/D";
    const lastData=p.last_data_timestamp?fmtDate(p.last_data_timestamp):"N/D";
    el.freshnessMeta.textContent=`Estado web: ${status} | ultimo dato: ${lastData} | proxima corrida estimada: ${nextRun}`;
  }
}

function drawMonetization(){
  const ads=p.ads||{};
  if(el.adPanel && el.adSlots){
    if(!ads.enabled){
      el.adPanel.style.display="none";
    }else{
      const slots=(ads.slots||["header","inline","sidebar","footer"]).map(v=>String(v||"").trim()).filter(Boolean);
      const provider=String(ads.provider||"").toLowerCase();
      const consent=consentState();
      const clientId=String(ads.client_id||ads.client_id_placeholder||"").trim();
      el.adPanel.style.display="";
      el.adSlots.innerHTML="";

      if(provider!=="adsense"){
        slots.forEach(slot=>{
          const div=document.createElement("div");
          div.className="ad-slot";
          div.setAttribute("data-slot",slot);
          div.textContent=`Espacio publicitario (${provider || "proveedor"}): ${slot}`;
          el.adSlots.appendChild(div);
        });
      }else if(consent!=="accepted"){
        const div=document.createElement("div");
        div.className="ad-slot";
        div.textContent=consent==="rejected"
          ? "Publicidad desactivada por preferencia de cookies."
          : "Acepta cookies para habilitar anuncios.";
        el.adSlots.appendChild(div);
      }else{
        ensureAdSenseScript(clientId);
        slots.forEach(slot=>{
          const div=document.createElement("div");
          div.className="ad-slot";
          div.setAttribute("data-slot",slot);
          const ins=document.createElement("ins");
          ins.className="adsbygoogle";
          ins.style.display="block";
          ins.setAttribute("data-ad-client", clientId);
          ins.setAttribute("data-ad-slot", String(slot||"").replace(/[^0-9]/g,"") || "0000000000");
          ins.setAttribute("data-ad-format", "auto");
          ins.setAttribute("data-full-width-responsive", "true");
          div.appendChild(ins);
          el.adSlots.appendChild(div);
          try{ (window.adsbygoogle = window.adsbygoogle || []).push({}); }catch(_e){}
        });
      }
    }
  }

  const premium=p.premium_placeholders||{};
  if(el.premiumPanel && el.premiumFeatures){
    if(premium.enabled){
      const features=(premium.features||[]).map(v=>String(v||"").trim()).filter(Boolean);
      el.premiumFeatures.innerHTML="";
      features.forEach(feature=>{
        const li=document.createElement("li");
        li.textContent=feature;
        el.premiumFeatures.appendChild(li);
      });
      el.premiumPanel.style.display="";
    }else{
      el.premiumPanel.style.display="none";
    }
  }
}

function initConsentBanner(){
  if(!el.cookieBanner) return;
  const saved=consentState();
  if(saved==="accepted" || saved==="rejected"){
    el.cookieBanner.style.display="none";
    drawMonetization();
    return;
  }
  el.cookieBanner.style.display="";
  if(el.cookieAccept){
    el.cookieAccept.addEventListener("click",()=>{
      try{window.localStorage?.setItem?.(COOKIE_KEY,"accepted");}catch(_e){}
      el.cookieBanner.style.display="none";
      drawMonetization();
      trackEvent("cookie_consent_updated",{state:"accepted"});
    });
  }
  if(el.cookieReject){
    el.cookieReject.addEventListener("click",()=>{
      try{window.localStorage?.setItem?.(COOKIE_KEY,"rejected");}catch(_e){}
      el.cookieBanner.style.display="none";
      drawMonetization();
      trackEvent("cookie_consent_updated",{state:"rejected"});
    });
  }
}

function dismissMobileOnboarding(persist=true){
  if(!el.mobileOnboarding) return;
  el.mobileOnboarding.setAttribute("hidden","hidden");
  if(persist){
    try{window.localStorage?.setItem?.(ONBOARDING_KEY,"dismissed");}catch(_e){}
  }
}

function initMobileOnboarding(){
  if(!el.mobileOnboarding) return;
  const isMobile=window.innerWidth<=760;
  const saved=window.localStorage?.getItem?.(ONBOARDING_KEY)||"";
  if(!isMobile || saved==="dismissed"){
    dismissMobileOnboarding(false);
    return;
  }
  window.setTimeout(()=>{
    if(el.mobileOnboarding){
      el.mobileOnboarding.removeAttribute("hidden");
    }
  },380);
  if(el.onboardingClose){
    el.onboardingClose.addEventListener("click",()=>dismissMobileOnboarding(true));
  }
  if(el.onboardingGoto){
    el.onboardingGoto.addEventListener("click",()=>{
      if(el.filtersPanel){
        el.filtersPanel.open=true;
        el.filtersPanel.scrollIntoView({behavior:"smooth",block:"start"});
      }
      dismissMobileOnboarding(true);
    });
  }
}

function mountFilterOptions(){
  el.cat.innerHTML="<option value='all'>Todas</option>";
  for(const c of (p.categories||[])){
    const o=document.createElement("option");
    o.value=c;o.textContent=c;el.cat.appendChild(o);
  }
  el.mb.innerHTML="";
  for(const m of (p.months||[])){
    const o=document.createElement("option");
    o.value=m;o.textContent=m;el.mb.appendChild(o);
  }
  if(el.pageSize){
    el.pageSize.innerHTML="";
    const sizes=(p.filters_available?.page_sizes||[25,50,100,250]).map(Number).filter(v=>v>0);
    sizes.forEach(v=>{
      const o=document.createElement("option");
      o.value=String(v);
      o.textContent=String(v);
      el.pageSize.appendChild(o);
    });
  }
  if(el.macroScope){
    const scopes=(p.filters_available?.macro_scopes||["general","rubros"]);
    el.macroScope.innerHTML="";
    scopes.forEach(scope=>{
      const o=document.createElement("option");
      o.value=scope;
      o.textContent=scope==="rubros"?"Rubros":"General";
      el.macroScope.appendChild(o);
    });
  }
  if(el.macroRegion){
    const regions=(p.filters_available?.macro_regions||p.official_regions||["patagonia"]);
    el.macroRegion.innerHTML="";
    regions.forEach(region=>{
      const o=document.createElement("option");
      o.value=region;
      o.textContent=region==="nacional"?"Nacional":"Patagonia";
      el.macroRegion.appendChild(o);
    });
  }
  if(el.macroCategory){
    const categories=(p.filters_available?.macro_categories||[]);
    el.macroCategory.innerHTML="";
    if(!categories.length){
      const o=document.createElement("option");
      o.value="";
      o.textContent="Sin rubros comparables";
      el.macroCategory.appendChild(o);
      el.macroCategory.disabled=true;
    }else{
      categories.forEach(cat=>{
        const o=document.createElement("option");
        o.value=cat;
        o.textContent=cat;
        el.macroCategory.appendChild(o);
      });
      el.macroCategory.disabled=false;
    }
  }
}

function setButtonsState(){
  el.modeNominal.classList.toggle("active",st.price_mode==="nominal");
  el.modeReal.classList.toggle("active",st.price_mode==="real");
}

function applyStateToControls(){
  el.q.value=st.query||"";
  if(el.clearSearch){
    el.clearSearch.disabled=!(st.query||"").trim();
  }
  el.cba.value=st.cba_filter||"all";
  el.cat.value=st.category||"all";
  el.ord.value=st.sort_by||"alphabetical";
  el.mb.value=(p.months||[]).includes(st.base_month)?st.base_month:(p.months?.[0]||"");
  st.base_month=el.mb.value;
  el.showReal.checked=!!st.show_real_column;
  if(el.macroScope){
    const validScopes=Array.from(el.macroScope.options).map(o=>o.value);
    if(!validScopes.includes(st.macro_scope)){
      st.macro_scope=validScopes.includes("general")?"general":(validScopes[0]||"general");
    }
    el.macroScope.value=st.macro_scope;
  }
  if(el.macroRegion){
    const validRegions=Array.from(el.macroRegion.options).map(o=>o.value);
    if(!validRegions.includes(st.macro_region)){
      st.macro_region=validRegions.includes(p.macro_default_region||"") ? (p.macro_default_region||"") : (validRegions[0]||"patagonia");
    }
    el.macroRegion.value=st.macro_region;
  }
  if(el.macroCategory){
    const validCats=Array.from(el.macroCategory.options).map(o=>o.value);
    if(!validCats.includes(st.macro_category)){
      st.macro_category=validCats[0]||"";
    }
    el.macroCategory.value=st.macro_category;
    el.macroCategory.disabled=st.macro_scope!=="rubros" || validCats.length===0;
  }
  if(el.pageSize){
    const validSizes=Array.from(el.pageSize.options).map(o=>Number(o.value));
    if(!validSizes.includes(Number(st.page_size))){
      st.page_size=validSizes.includes(25)?25:validSizes[0];
    }
    el.pageSize.value=String(st.page_size);
  }
  st.current_page=Math.max(1, Number(st.current_page||1));
  setButtonsState();
}

function maybeDrawBandPanel(rows, force=false){
  if(force || lazyPanels.bands_ready){
    drawBandChart(rows);
  }
}

function maybeDrawQualityPanel(force=false){
  if(force || lazyPanels.quality_ready){
    drawQuality();
  }
}

function initLazyPanels(){
  const hasBandPanel=!!(el.panelBands && el.chartBands && el.legendBands);
  const hasQualityPanel=!!el.qualityPanel;

  if(!hasBandPanel && !hasQualityPanel){
    lazyPanels.bands_ready=true;
    lazyPanels.quality_ready=true;
    return;
  }

  if(typeof window.IntersectionObserver!=="function"){
    lazyPanels.bands_ready=hasBandPanel;
    lazyPanels.quality_ready=hasQualityPanel;
    return;
  }

  const observer=new window.IntersectionObserver((entries)=>{
    entries.forEach((entry)=>{
      if(!entry.isIntersecting) return;

      if(hasBandPanel && entry.target===el.panelBands && !lazyPanels.bands_ready){
        lazyPanels.bands_ready=true;
        maybeDrawBandPanel(filteredRows(), true);
        observer.unobserve(entry.target);
      }
      if(hasQualityPanel && entry.target===el.qualityPanel && !lazyPanels.quality_ready){
        lazyPanels.quality_ready=true;
        maybeDrawQualityPanel(true);
        observer.unobserve(entry.target);
      }
    });
  },{
    root:null,
    rootMargin:"260px 0px 260px 0px",
    threshold:0.01
  });

  lazyPanels.observer=observer;
  if(hasBandPanel) observer.observe(el.panelBands);
  if(hasQualityPanel) observer.observe(el.qualityPanel);
}

function render(){
  if(el.clearSearch){
    el.clearSearch.disabled=!(st.query||"").trim();
  }
  const rows=filteredRows();
  syncSelection(rows);
  drawTable(rows);
  drawMainChart(rows);
  drawSecondaryChart();
  maybeDrawBandPanel(rows);
  drawKpis();
  drawActiveFilters(rows.length);
  maybeDrawQualityPanel();
  saveState();
}

function resetState(){
  st.query=defaults.query||"";
  st.cba_filter=defaults.cba_filter||"all";
  st.category=defaults.category||"all";
  st.sort_by=defaults.sort_by||"alphabetical";
  st.base_month=defaults.base_month||(p.months?.[0]||"");
  st.selected_products=[...(defaults.selected_products||[])];
  st.price_mode=defaults.price_mode||"nominal";
  st.show_real_column=!!defaults.show_real_column;
  st.macro_scope=defaults.macro_scope||"general";
  st.macro_region=defaults.macro_region||p.macro_default_region||"patagonia";
  st.macro_category=defaults.macro_category||"";
  st.view=defaults.view||"executive";
  st.band_product=defaults.band_product||"";
  st.page_size=Number(defaults.page_size||25);
  st.current_page=Number(defaults.current_page||1);
  _rowsCacheKey="";
  _rowsCacheValue=[];
  _lastMainChartKey="";
  _lastSecondaryChartKey="";
  applyStateToControls();
  render();
}

function quickPick(kind){
  const rows=filteredRows().filter(r=>r.variation_nominal_pct!=null);
  if(!rows.length)return;
  if(kind==="up"){
    rows.sort((a,b)=>(b.variation_nominal_pct??-Infinity)-(a.variation_nominal_pct??-Infinity));
  }else if(kind==="down"){
    rows.sort((a,b)=>(a.variation_nominal_pct??Infinity)-(b.variation_nominal_pct??Infinity));
  }else{
    rows.sort((a,b)=>Math.abs(a.variation_nominal_pct??999)-Math.abs(b.variation_nominal_pct??999));
  }
  st.selected_products=rows.slice(0,5).map(r=>r.canonical_id);
  st.current_page=1;
  render();
}

function clearFilterToken(filterKey){
  const defaultBase=p.months?.[0]||"";
  if(filterKey==="query"){
    st.query="";
  }else if(filterKey==="cba"){
    st.cba_filter="all";
  }else if(filterKey==="category"){
    st.category="all";
  }else if(filterKey==="sort_by"){
    st.sort_by="alphabetical";
  }else if(filterKey==="base_month"){
    st.base_month=defaultBase;
  }else if(filterKey==="price_mode"){
    st.price_mode="nominal";
    setButtonsState();
  }else if(filterKey==="show_real_column"){
    st.show_real_column=false;
  }else if(filterKey==="macro_scope"){
    st.macro_scope="general";
  }else if(filterKey==="macro_region"){
    st.macro_region=p.macro_default_region||"patagonia";
  }else if(filterKey==="macro_category"){
    st.macro_category=(p.filters_available?.macro_categories||[])[0]||"";
  }else{
    return;
  }
  st.current_page=1;
  _rowsCacheKey="";
  _lastMainChartKey="";
  applyStateToControls();
  render();
}

function debounce(fn,ms){
  let t=null;
  return (...args)=>{
    if(t)clearTimeout(t);
    t=setTimeout(()=>fn(...args),ms);
  };
}

function bindShortcuts(){
  document.addEventListener("keydown",(e)=>{
    if(e.defaultPrevented) return;
    const target=e.target;
    const tag=String(target?.tagName||"").toLowerCase();
    const editable=!!target?.isContentEditable || tag==="input" || tag==="textarea" || tag==="select";
    if(e.key==="/" && !editable){
      e.preventDefault();
      if(el.filtersPanel && window.innerWidth<900 && !el.filtersPanel.open){
        el.filtersPanel.open=true;
      }
      if(el.q){
        el.q.focus();
        el.q.select?.();
      }
      return;
    }
    if(e.key==="Escape" && target===el.q && (el.q?.value||"").trim()){
      e.preventDefault();
      st.query="";
      el.q.value="";
      st.current_page=1;
      _rowsCacheKey="";
      render();
    }
  });
}

function bindEvents(){
  el.q.addEventListener("input",debounce((e)=>{st.query=e.target.value||"";st.current_page=1;_rowsCacheKey="";render();},200));
  if(el.clearSearch){
    el.clearSearch.addEventListener("click",()=>{
      st.query="";
      el.q.value="";
      st.current_page=1;
      _rowsCacheKey="";
      render();
      el.q.focus();
    });
  }
  el.cba.addEventListener("change",(e)=>{st.cba_filter=e.target.value;st.current_page=1;_rowsCacheKey="";render();});
  el.cat.addEventListener("change",(e)=>{st.category=e.target.value;st.current_page=1;_rowsCacheKey="";render();});
  el.ord.addEventListener("change",(e)=>{st.sort_by=e.target.value;st.current_page=1;_rowsCacheKey="";render();});
  el.mb.addEventListener("change",(e)=>{st.base_month=e.target.value;st.current_page=1;_rowsCacheKey="";render();});
  el.sel.addEventListener("change",()=>{
    st.selected_products=Array.from(el.sel.selectedOptions).map(o=>o.value);
    _lastMainChartKey="";
    drawMainChart(filteredRows(),true);
    saveState();
  });
  el.showReal.addEventListener("change",(e)=>{st.show_real_column=!!e.target.checked;render();});
  el.modeNominal.addEventListener("click",()=>{st.price_mode="nominal";setButtonsState();_lastMainChartKey="";drawMainChart(filteredRows(),true);saveState();});
  el.modeReal.addEventListener("click",()=>{st.price_mode="real";setButtonsState();_lastMainChartKey="";drawMainChart(filteredRows(),true);saveState();});
  if(el.macroScope){
    el.macroScope.addEventListener("change",(e)=>{
      st.macro_scope=e.target.value||"general";
      _lastSecondaryChartKey="";
      drawSecondaryChart(true);
      drawActiveFilters(filteredRows().length);
      saveState();
    });
  }
  if(el.macroRegion){
    el.macroRegion.addEventListener("change",(e)=>{
      st.macro_region=e.target.value||p.macro_default_region||"patagonia";
      _lastSecondaryChartKey="";
      drawSecondaryChart(true);
      maybeDrawQualityPanel();
      drawActiveFilters(filteredRows().length);
      saveState();
    });
  }
  if(el.macroCategory){
    el.macroCategory.addEventListener("change",(e)=>{
      st.macro_category=e.target.value||"";
      _lastSecondaryChartKey="";
      drawSecondaryChart(true);
      drawActiveFilters(filteredRows().length);
      saveState();
    });
  }
  if(el.bandProduct){
    el.bandProduct.addEventListener("change",(e)=>{st.band_product=e.target.value||"";maybeDrawBandPanel(filteredRows(), true);saveState();});
  }
  el.quickUp.addEventListener("click",()=>quickPick("up"));
  el.quickDown.addEventListener("click",()=>quickPick("down"));
  el.quickFlat.addEventListener("click",()=>quickPick("flat"));
  if(el.pageSize){
    el.pageSize.addEventListener("change",(e)=>{
      st.page_size=Number(e.target.value||25);
      st.current_page=1;
      render();
    });
  }
  if(el.pagePrev){
    el.pagePrev.addEventListener("click",()=>{
      st.current_page=Math.max(1,Number(st.current_page||1)-1);
      render();
    });
  }
  if(el.pageNext){
    el.pageNext.addEventListener("click",()=>{
      st.current_page=Number(st.current_page||1)+1;
      render();
    });
  }
  if(el.exportCsv){
    el.exportCsv.addEventListener("click",()=>exportFilteredCsv(filteredRows()));
  }
  if(el.copyLink){
    el.copyLink.addEventListener("click",()=>{copyCurrentViewLink();});
  }
  if(el.activeFilters){
    el.activeFilters.addEventListener("click",(e)=>{
      const target=e.target?.closest?.("[data-filter]");
      if(!target)return;
      const key=target.getAttribute("data-filter");
      clearFilterToken(key||"");
    });
  }
  el.reset.addEventListener("click",resetState);
  window.addEventListener("resize",debounce(()=>{
    const rows=filteredRows();
    drawMainChart(rows,true);
    drawSecondaryChart(true);
    maybeDrawBandPanel(rows);
  },150));
}

function init(){
  initConsentBanner();
  initMobileOnboarding();
  drawMonetization();
  trackEvent("tracker_view",{status:p.web_status||"partial",has_data:p.has_data?"1":"0"});
  if(!p.has_data){
    document.getElementById("empty").style.display="";
    document.getElementById("app").style.display="none";
    if(el.quickGuide) el.quickGuide.style.display="none";
    dismissMobileOnboarding(false);
    maybeDrawQualityPanel(true);
    return;
  }
  if(window.innerWidth<900){
    const fp=document.getElementById("filters-panel");
    if(fp)fp.open=false;
  }
  loadState();
  mountFilterOptions();
  applyStateToControls();
  bindShortcuts();
  bindEvents();
  initLazyPanels();
  render();
}
init();
</script>
</body>
</html>"""

        return (
            template.replace("__PAYLOAD__", payload_json)
            .replace("__EXTERNAL_SCRIPT__", external_script)
            .replace("__ANALYTICS_SCRIPT__", analytics_script)
            .replace("__TRACKER_URL__", escape(tracker_url, quote=True))
            .replace("__OG_IMAGE_URL__", escape(og_image_url, quote=True))
            .replace("__GEN__", generated_at)
            .replace("__FROM__", payload["from_month"])
            .replace("__TO__", payload["to_month"])
            .replace("__BASKET__", payload["basket_type"])
            .replace("__VIEW__", analysis_depth)
        )

    def _write_pdf_if_requested(self, html_content: str, pdf_path: Path) -> Optional[str]:
        try:
            from weasyprint import HTML
        except Exception:
            return None
        HTML(string=html_content).write_pdf(str(pdf_path))
        return str(pdf_path)

    def generate(
        self,
        from_month: Optional[str] = None,
        to_month: Optional[str] = None,
        export_pdf: bool = False,
        basket_type: str = "all",
        benchmark_mode: str = "ipc",
        analysis_depth: str = "executive",
        offline_assets: str = "embed",
    ) -> Dict[str, Any]:
        started = perf_counter()
        effective_from, effective_to = self._resolve_effective_range(from_month, to_month, basket_type)
        df = self._load_prices(effective_from, effective_to, basket_type)
        payload = self._build_interactive_payload(
            df,
            effective_from,
            effective_to,
            basket_type,
            benchmark_mode=benchmark_mode,
            analysis_depth=analysis_depth,
        )
        inflation_total_pct = self._compute_inflation_total_pct(df, effective_from, effective_to)
        coverage = payload.get("coverage", self._coverage_metrics(df, effective_from, effective_to, basket_type))

        reports_dir = (
            self.config.get("analysis", {}).get("reports_dir", "data/analysis/reports")
            if isinstance(self.config.get("analysis"), dict)
            else "data/analysis/reports"
        )
        out_dir = Path(str(reports_dir))
        out_dir.mkdir(parents=True, exist_ok=True)
        generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        base = f"report_interactive_{effective_from}_to_{effective_to}_{stamp}".replace("-", "")

        html = self._render_interactive_html(
            payload,
            generated_at,
            analysis_depth=analysis_depth,
            offline_assets=offline_assets,
        )
        html_path = out_dir / f"{base}.html"
        html_path.write_text(html, encoding="utf-8")
        pdf_path = self._write_pdf_if_requested(html, out_dir / f"{base}.pdf") if export_pdf else None

        payload_kb = len(json.dumps(payload, ensure_ascii=False).encode("utf-8")) / 1024.0
        generation_ms = (perf_counter() - started) * 1000.0
        quality_flags = payload.get("quality_flags", {})
        scrape_quality = payload.get("scrape_quality", {})
        candidate_band_summary = payload.get("candidate_band_summary", {})
        web_status = str(payload.get("web_status", "partial"))
        is_stale = bool(payload.get("is_stale", False))
        next_update_eta = payload.get("next_update_eta")
        ad_slots_enabled = bool(payload.get("ads", {}).get("enabled", False))
        premium_placeholders_enabled = bool(payload.get("premium_placeholders", {}).get("enabled", False))

        metadata = {
            "generated_at": generated_at,
            "ui_version": 2,
            "benchmark_mode": benchmark_mode,
            "analysis_depth": analysis_depth,
            "offline_mode": offline_assets,
            "range": {"from": effective_from, "to": effective_to},
            "basket_type": basket_type,
            "data_source": "prices_sampled_daily",
            "has_data": payload["has_data"],
            "inflation_total_pct": inflation_total_pct,
            "coverage": coverage,
            "kpis": payload.get("kpi_summary", {}),
            "data_quality": {
                "coverage": coverage,
                "balanced_panel_n": quality_flags.get("balanced_panel_n", 0),
                "missing_cpi_months": quality_flags.get("missing_cpi_months", []),
                "quality_flags": quality_flags,
                "publication_status": payload.get("publication_status", {}),
                "scrape_quality": scrape_quality,
                "candidate_band_summary": candidate_band_summary,
            },
            "performance": {
                "generation_ms": generation_ms,
                "payload_kb": payload_kb,
                "n_products": len(payload.get("snapshot", [])),
                "n_points_chart": len(payload.get("timeline", [])),
                "n_points_candidate_bands": len(payload.get("candidate_bands", [])),
            },
            "ui_defaults": payload["ui_defaults"],
            "filters_available": payload["filters_available"],
            "observation_policy": scrape_quality.get("observation_policy", "single"),
            "candidate_storage_mode": scrape_quality.get("candidate_storage_mode", "off"),
            "price_candidates_ready": bool(scrape_quality.get("price_candidates_ready", False)),
            "publication_policy": payload.get("publication_policy", PUBLICATION_POLICY),
            "analytics": payload.get("analytics", {}),
            "web_status": web_status,
            "is_stale": is_stale,
            "next_update_eta": next_update_eta,
            "ad_slots_enabled": ad_slots_enabled,
            "premium_placeholders_enabled": premium_placeholders_enabled,
            "artifacts": {"html": str(html_path), "pdf": pdf_path},
        }
        metadata_path = out_dir / f"{base}.metadata.json"
        metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")

        return {
            "from_month": effective_from,
            "to_month": effective_to,
            "inflation_total_pct": inflation_total_pct,
            "has_data": payload["has_data"],
            "coverage": coverage,
            "payload": payload,
            "kpis": payload.get("kpi_summary", {}),
            "data_quality": metadata["data_quality"],
            "observation_policy": metadata["observation_policy"],
            "candidate_storage_mode": metadata["candidate_storage_mode"],
            "publication_policy": metadata["publication_policy"],
            "candidate_band_summary": candidate_band_summary,
            "performance": metadata["performance"],
            "artifacts": ReportArtifacts(str(html_path), str(metadata_path), pdf_path).__dict__,
        }


def run_report(
    config_path: Optional[str],
    from_month: Optional[str] = None,
    to_month: Optional[str] = None,
    export_pdf: bool = False,
    basket_type: str = "all",
    benchmark_mode: str = "ipc",
    analysis_depth: str = "executive",
    offline_assets: str = "embed",
) -> Dict[str, Any]:
    config = load_config(config_path)
    with ReportGenerator(config) as generator:
        return generator.generate(
            from_month=from_month,
            to_month=to_month,
            export_pdf=export_pdf,
            basket_type=basket_type,
            benchmark_mode=benchmark_mode,
            analysis_depth=analysis_depth,
            offline_assets=offline_assets,
        )
