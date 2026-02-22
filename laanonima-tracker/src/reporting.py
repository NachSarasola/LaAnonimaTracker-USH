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
from src.web_styles import get_tracker_css_bundle, get_tracker_css_version


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

    def _build_candidate_triplets_latest(self, candidate_df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
        if candidate_df.empty:
            return {}
        work = candidate_df.copy()
        work = work[work["tier"].astype(str).isin(["low", "mid", "high"])]
        if work.empty:
            return {}
        work = work.sort_values("scraped_at")
        latest = work.groupby(["canonical_id", "tier"], as_index=False).tail(1)
        by_id: Dict[str, Dict[str, Any]] = {}
        for _, row in latest.iterrows():
            canonical_id = str(row.get("canonical_id") or "").strip()
            tier = str(row.get("tier") or "").strip().lower()
            if not canonical_id or tier not in {"low", "mid", "high"}:
                continue
            if canonical_id not in by_id:
                by_id[canonical_id] = {}
            by_id[canonical_id][tier] = {
                "run_id": int(row.get("run_id")) if pd.notna(row.get("run_id")) else None,
                "product_id": str(row.get("product_id") or "").strip() or None,
                "candidate_name": str(row.get("candidate_name") or row.get("product_name") or "").strip() or None,
                "candidate_url": str(row.get("candidate_url") or "").strip() or None,
                "candidate_price": self._safe_float(row.get("candidate_price")),
                "scraped_at": pd.Timestamp(row.get("scraped_at")).isoformat() if pd.notna(row.get("scraped_at")) else None,
            }
        return by_id

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
        candidate_triplets_latest = self._build_candidate_triplets_latest(candidate_df)
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
                "candidate_triplets_latest_by_id": candidate_triplets_latest,
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
        candidate_triplets_latest = self._build_candidate_triplets_latest(candidate_df)
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
            "candidate_triplets_latest_by_id": candidate_triplets_latest,
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
        tracker_style_block = f"<style>{get_tracker_css_bundle()}</style>"
        if offline_assets == "external":
            external_script = '<script src="https://cdn.plot.ly/plotly-basic-2.35.2.min.js"></script>'
            tracker_style_block = f'<link rel="stylesheet" href="./tracker-ui.css?v={get_tracker_css_version()}"/>'
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

        # Read HTML and JS templates from src/templates/
        template_dir = Path(__file__).parent / 'templates'
        template_html = (template_dir / 'tracker.html').read_text(encoding='utf-8')
        template_js = (template_dir / 'tracker.js').read_text(encoding='utf-8')
        
        # Inject JS into HTML at the end of body
        template = template_html.replace('</body>', f'<script>\n{template_js}\n</script>\n</body>')

        return (
            template.replace("__PAYLOAD__", payload_json)
            .replace("__EXTERNAL_SCRIPT__", external_script)
            .replace("__ANALYTICS_SCRIPT__", analytics_script)
            .replace("__TRACKER_STYLE_BLOCK__", tracker_style_block)
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

        # Load ALL historical data for product detail pages and main tracker sparklines
        try:
            full_df = self._load_prices("2024-01", effective_to, basket_type)
            full_payload = self._build_interactive_payload(full_df, "2024-01", effective_to, basket_type)
            # Override dashboard monthly_reference so it contains up to 6 months of data for inline sparklines
            payload["monthly_reference"] = full_payload["monthly_reference"]
        except Exception:
            full_df = df
            full_payload = payload

        html = self._render_interactive_html(
            payload,
            generated_at,
            analysis_depth=analysis_depth,
            offline_assets=offline_assets,
        )
        html_path = out_dir / f"{base}.html"
        html_path.write_text(html, encoding="utf-8")

        # Write per-product detail JSON files alongside the tracker HTML.
        # web_publish.py reads these to generate /tracker/{id}/index.html pages.
        try:
            products_dir = out_dir / "products"
            products_dir.mkdir(parents=True, exist_ok=True)
            
            snapshot_by_id = {str(s.get('canonical_id') or ''): s for s in (payload.get('snapshot') or []) if s.get('canonical_id')}
            
            timeline_by_id = {}
            for t in (full_payload.get('timeline') or []):
                cid = str(t.get('canonical_id') or '')
                if cid:
                    timeline_by_id.setdefault(cid, []).append(t)
                    
            monthly_by_id = {}
            for m in (full_payload.get('monthly_reference') or []):
                cid = str(m.get('canonical_id') or '')
                if cid:
                    monthly_by_id.setdefault(cid, []).append(m)
                    
            bands_by_id = {}
            for b in (full_payload.get('candidate_bands') or []):
                cid = str(b.get('canonical_id') or '')
                if cid:
                    bands_by_id.setdefault(cid, []).append(b)
                    
            triplets_by_id = full_payload.get('candidate_triplets_latest_by_id') or {}
            
            for cid, snap in snapshot_by_id.items():
                if not cid:
                    continue
                pd_data = {
                    'canonical_id': cid,
                    'product_name': snap.get('product_name') or cid,
                    'category': snap.get('category') or '',
                    'basket_id': snap.get('basket_id') or '',
                    'presentation': snap.get('presentation') or '',
                    'product_url': snap.get('product_url'),
                    'from_month': '2024-01',
                    'to_month': payload.get('to_month'),
                    'generated_at': payload.get('generated_at'),
                    'current_price': snap.get('current_price'),
                    'current_real_price': snap.get('current_real_price'),
                    'scraped_at': snap.get('scraped_at'),
                    'monthly_series': sorted(monthly_by_id.get(cid, []), key=lambda x: str(x.get('month') or '')),
                    'daily_series': sorted(timeline_by_id.get(cid, []), key=lambda x: str(x.get('scraped_at') or '')),
                    'terna_series': sorted(bands_by_id.get(cid, []), key=lambda x: str(x.get('scraped_at') or '')),
                    'terna_latest': triplets_by_id.get(cid, {}),
                }
                monthly = pd_data['monthly_series']
                if len(monthly) >= 2:
                    p0 = self._safe_float(monthly[0].get('avg_price'))
                    p1 = self._safe_float(monthly[-1].get('avg_price'))
                    pd_data['var_pct'] = round((p1 / p0 - 1) * 100, 2) if p0 and p0 > 0 and p1 is not None else None
                    r0 = self._safe_float(monthly[0].get('avg_real_price'))
                    r1 = self._safe_float(monthly[-1].get('avg_real_price'))
                    pd_data['var_real_pct'] = round((r1 / r0 - 1) * 100, 2) if r0 and r0 > 0 and r1 is not None else None
                else:
                    pd_data['var_pct'] = None
                    pd_data['var_real_pct'] = None
                
                safe_cid = ''.join(c if c.isalnum() or c in '-_' else '_' for c in cid)
                (products_dir / f"{safe_cid}.json").write_text(
                    json.dumps(pd_data, ensure_ascii=False), encoding='utf-8'
                )
        except Exception as _pd_err:
            pass
        tracker_css_path = out_dir / "tracker-ui.css"

        if offline_assets == "external":
            tracker_css_path.write_text(get_tracker_css_bundle(), encoding="utf-8")
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
            "artifacts": {
                "html": str(html_path),
                "pdf": pdf_path,
                "tracker_css": str(tracker_css_path) if offline_assets == "external" else None,
            },
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