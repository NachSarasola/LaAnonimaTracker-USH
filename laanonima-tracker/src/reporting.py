"""Economic interactive report generation for La Anonima Tracker."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from sqlalchemy import func

from src.config_loader import get_basket_items, load_config, resolve_canonical_category
from src.models import Price, get_engine, get_session_factory
from src.repositories import SeriesRepository


@dataclass
class ReportArtifacts:
    html_path: str
    metadata_path: str
    pdf_path: Optional[str] = None


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
        df["category"] = df["category"].fillna("sin_categoria")
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
    ) -> pd.DataFrame:
        columns = ["year_month", "cpi_index", "cpi_mom", "cpi_yoy"]
        if benchmark_mode == "none":
            return pd.DataFrame(columns=columns)

        cpi_path = Path("data/cpi/ipc_indec.csv")
        if not cpi_path.exists():
            return pd.DataFrame(columns=columns)

        try:
            ipc_df = pd.read_csv(cpi_path)
        except Exception:
            return pd.DataFrame(columns=columns)

        if "year_month" not in ipc_df.columns or "cpi_index" not in ipc_df.columns:
            return pd.DataFrame(columns=columns)

        ipc_df = ipc_df.copy()
        ipc_df["year_month"] = ipc_df["year_month"].astype(str)
        ipc_df["cpi_index"] = pd.to_numeric(ipc_df["cpi_index"], errors="coerce")
        if "cpi_mom" in ipc_df.columns:
            ipc_df["cpi_mom"] = pd.to_numeric(ipc_df["cpi_mom"], errors="coerce")
        else:
            ipc_df["cpi_mom"] = pd.NA
        if "cpi_yoy" in ipc_df.columns:
            ipc_df["cpi_yoy"] = pd.to_numeric(ipc_df["cpi_yoy"], errors="coerce")
        else:
            ipc_df["cpi_yoy"] = pd.NA
        ipc_df = ipc_df.dropna(subset=["cpi_index"])

        from_period = pd.Period(from_month, freq="M")
        to_period = pd.Period(to_month, freq="M")
        ipc_df = ipc_df[ipc_df["year_month"].map(lambda x: from_period <= pd.Period(x, freq="M") <= to_period)]
        return ipc_df.sort_values("year_month")[columns].reset_index(drop=True)

    def _expected_products_by_category(self, basket_type: str) -> Dict[str, int]:
        expected: Dict[str, int] = {}
        for item in get_basket_items(self.config, basket_type):
            raw = item.get("category") or "sin_categoria"
            category = resolve_canonical_category(self.config, raw) or raw
            expected[category] = expected.get(category, 0) + 1
        return expected

    def _coverage_metrics(self, df: pd.DataFrame, from_month: str, to_month: str, basket_type: str) -> Dict[str, Any]:
        expected_by_category = self._expected_products_by_category(basket_type)
        expected_products = sum(expected_by_category.values())
        observed_total = int(df["canonical_id"].nunique()) if not df.empty else 0
        observed_from = int(df[df["month"] == from_month]["canonical_id"].nunique()) if not df.empty else 0
        observed_to = int(df[df["month"] == to_month]["canonical_id"].nunique()) if not df.empty else 0
        safe_expected = expected_products if expected_products > 0 else 1
        by_category = []
        if not df.empty:
            grouped = df.groupby("category")["canonical_id"].nunique().sort_values(ascending=False)
            for category, observed in grouped.items():
                exp = expected_by_category.get(category, 0)
                den = exp if exp > 0 else 1
                by_category.append(
                    {
                        "category": category,
                        "expected_products": int(exp),
                        "observed_products": int(observed),
                        "coverage_pct": (int(observed) / den) * 100,
                    }
                )
        return {
            "basket_type": basket_type,
            "expected_products": expected_products,
            "expected_products_by_category": expected_by_category,
            "observed_products_total": observed_total,
            "coverage_total_pct": (observed_total / safe_expected) * 100,
            "observed_from": observed_from,
            "observed_to": observed_to,
            "coverage_from_pct": (observed_from / safe_expected) * 100,
            "coverage_to_pct": (observed_to / safe_expected) * 100,
            "coverage_by_category": by_category,
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
        ipc_df = self._load_ipc_data(from_month, to_month, benchmark_mode=benchmark_mode)
        coverage = self._coverage_metrics(df, from_month, to_month, basket_type)
        candidate_df = self._load_candidate_rows(from_month, to_month, basket_type)
        candidate_bands, candidate_band_summary = self._build_candidate_bands(candidate_df, pd.DataFrame())
        scrape_quality = self._scrape_quality_summary(
            df,
            basket_type,
            candidate_band_summary=candidate_band_summary,
        )

        if df.empty:
            quality_flags = self._build_quality_flags(
                coverage=coverage,
                kpi_summary={"balanced_panel_n": 0},
                missing_cpi_months=months if benchmark_mode == "ipc" else [],
            )
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
                    }
                    for _, row in ipc_df.iterrows()
                ],
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
                "ui_defaults": {
                    "query": "",
                    "cba_filter": "all",
                    "category": "all",
                    "sort_by": "alphabetical",
                    "base_month": months[0] if months else "",
                    "selected_products": [],
                    "price_mode": "nominal",
                    "show_real_column": False,
                    "view": analysis_depth,
                    "band_product": "",
                    "page_size": 50,
                    "current_page": 1,
                },
                "filters_available": {
                    "cba_filter": ["all", "yes", "no"],
                    "categories": [],
                    "months": months,
                    "sort_by": ["alphabetical", "price", "var_nominal", "var_real"],
                    "page_sizes": [25, 50, 100, 250],
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
            missing_cpi_months=real_meta["missing_cpi_months"],
        )

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
                }
                for _, row in ipc_df.iterrows()
            ],
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
            "ui_defaults": {
                "query": "",
                "cba_filter": "all",
                "category": "all",
                "sort_by": "alphabetical",
                "base_month": from_month if from_month in months else (months[0] if months else to_month),
                "selected_products": selected,
                "price_mode": "nominal",
                "show_real_column": False,
                "view": analysis_depth,
                "band_product": "",
                "page_size": 50,
                "current_page": 1,
            },
            "filters_available": {
                "cba_filter": ["all", "yes", "no"],
                "categories": categories,
                "months": months,
                "sort_by": ["alphabetical", "price", "var_nominal", "var_real"],
                "page_sizes": [25, 50, 100, 250],
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

        template = """<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>La Anonima Tracker - Reporte Economico</title>
__EXTERNAL_SCRIPT__
<style>
:root{
  --bg:#f4f7f8;
  --panel:#ffffff;
  --panel-soft:#f6f8fb;
  --text:#1f2735;
  --muted:#556176;
  --line:#d7dee8;
  --accent:#005f73;
  --accent-2:#0a9396;
  --danger:#b42318;
  --ok:#027a48;
  --warn:#b54708;
  --pos:#0a7a5c;
  --neg:#9b2226;
  --focus:#8ecde1;
  --shadow-sm:0 1px 2px rgba(26,39,61,.07), 0 10px 26px rgba(26,39,61,.04);
  --shadow-lg:0 2px 8px rgba(26,39,61,.08), 0 16px 36px rgba(26,39,61,.07);
  --radius:14px;
  --radius-sm:10px;
}
*{box-sizing:border-box}
html,body{margin:0;padding:0;color:var(--text);font-family:"Aptos","Segoe UI Variable","Trebuchet MS",sans-serif;line-height:1.35}
body{
  background:
    radial-gradient(1400px 440px at 0% 0%, #e4efed 0%, rgba(228,239,237,0) 62%),
    radial-gradient(980px 380px at 100% 0%, #edf4f6 0%, rgba(237,244,246,0) 58%),
    var(--bg);
}
.wrap{max-width:1320px;margin:0 auto;padding:18px}
.stack{display:grid;gap:12px}
.card{background:var(--panel);border:1px solid var(--line);border-radius:var(--radius);padding:14px 16px;box-shadow:var(--shadow-sm);overflow:hidden}
.card h2{margin:0 0 10px 0;font-size:1.03rem;letter-spacing:.01em}
.header{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:14px;align-items:center}
.title{margin:0 0 2px 0;font-size:1.42rem;line-height:1.2;letter-spacing:.01em}
.meta{color:var(--muted);font-size:.92rem;margin:0 0 2px 0}
.badge{display:inline-flex;align-items:center;padding:6px 10px;border-radius:999px;font-size:.74rem;font-weight:700;letter-spacing:.04em;text-transform:uppercase;background:#e8f6ef;color:var(--ok);border:1px solid #cce8d7}
.badge.warn{background:#fff4eb;color:#b54708;border-color:#f5d5b5}
.ui-version{margin-top:6px;text-align:right}
.method{font-size:.8rem;color:var(--muted);margin:0}
.kpis{display:grid;gap:10px;grid-template-columns:repeat(6,minmax(132px,1fr))}
.kpi{background:linear-gradient(165deg,#fbfdfc 0%,var(--panel-soft) 100%);border:1px solid var(--line);border-radius:12px;padding:10px 10px 12px;text-align:center;box-shadow:inset 0 1px 0 rgba(255,255,255,.8)}
.kpi .label{font-size:.74rem;color:var(--muted);margin-bottom:5px}
.kpi .value{font-size:1.2rem;font-weight:700;font-variant-numeric:tabular-nums}
.kpi .sub{font-size:.74rem;color:var(--muted)}
.kpi.good .value{color:var(--pos)}
.kpi.bad .value{color:var(--neg)}
.kpi.warn .value{color:var(--warn)}
.helper{display:grid;gap:8px}
.guide-title{font-weight:700;font-size:.95rem}
.pills{display:flex;gap:8px;flex-wrap:wrap;align-items:center}
.pill{font-size:.77rem;border:1px solid var(--line);background:#fdfefe;border-radius:999px;padding:5px 10px;color:#445064}
details.filters{background:var(--panel);border:1px solid var(--line);border-radius:var(--radius);box-shadow:var(--shadow-sm)}
details.filters>summary{cursor:pointer;list-style:none;padding:11px 14px;font-weight:700;display:flex;align-items:center;gap:8px}
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
.filters-grid{display:grid;gap:10px;padding:0 14px 14px 14px;grid-template-columns:repeat(6,minmax(120px,1fr))}
.filters-grid>div{
  background:var(--panel-soft);
  border:1px solid #e2e8f0;
  border-radius:12px;
  padding:8px 10px;
  min-height:76px;
  display:flex;
  flex-direction:column;
  justify-content:flex-start;
}
label{display:block;font-size:.76rem;color:var(--muted);margin-bottom:4px}
input,select,button{width:100%;min-height:36px;padding:8px 10px;border:1px solid #c5cedb;border-radius:var(--radius-sm);background:#fff;color:var(--text);font:inherit}
input,select{box-shadow:inset 0 1px 2px rgba(17,24,39,.04)}
input:hover,select:hover{border-color:#b4c0cf}
input:focus,select:focus{outline:2px solid var(--focus);outline-offset:1px;border-color:#84b6c7}
button{cursor:pointer;font-weight:600;transition:all .18s ease}
button:active{transform:translateY(1px)}
button.primary{background:linear-gradient(180deg,#067086,#005f73);color:#fff;border-color:#005a6d;box-shadow:0 1px 0 rgba(255,255,255,.12) inset}
button.primary:hover{filter:brightness(1.05)}
button.ghost{background:#fff}
button.ghost:hover{background:#f3f8fb}
.switchers{display:flex;gap:8px;flex-wrap:wrap}
.switchers button{width:auto;padding:7px 10px;min-height:34px}
.switchers button.active{background:var(--accent-2);border-color:var(--accent-2);color:#fff}
.inline-toggle{display:flex;align-items:center;gap:8px;min-height:34px}
.inline-toggle input{width:auto;accent-color:var(--accent)}
.reset-wrap{display:flex;align-items:flex-end}
#sel{min-height:116px}
#sel option{padding:2px 4px}
.chart-wrap{display:grid;gap:12px;grid-template-columns:2fr 1fr}
.chart-card h2{text-align:center}
.chart{border:1px solid var(--line);border-radius:12px;background:linear-gradient(180deg,#ffffff,#fbfcfd);position:relative;min-height:340px;display:flex;align-items:center;justify-content:center;box-shadow:inset 0 0 0 1px rgba(255,255,255,.7)}
.chart canvas{width:100%;height:340px;display:block;margin:0 auto}
.chart.small canvas{height:220px}
.chart-empty{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;color:var(--muted);font-size:.9rem}
.legend{display:flex;gap:10px;flex-wrap:wrap;padding-top:8px;font-size:.8rem;color:var(--muted);justify-content:center}
.legend .item{display:flex;align-items:center;gap:6px}
.dot{width:10px;height:10px;border-radius:999px;display:inline-block}
.band-toolbar{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-bottom:8px}
.band-select-wrap{min-width:280px}
.quality{display:grid;gap:8px}
.quality strong{display:block;margin-bottom:2px}
.quality-item{font-size:.85rem;color:var(--muted)}
.warn-list{margin:6px 0 0 18px;padding:0;color:#7a3104;font-size:.82rem}
.table-section h2{margin-bottom:8px}
.table-toolbar{display:flex;gap:10px;align-items:center;justify-content:space-between;flex-wrap:wrap;margin-bottom:8px}
.table-actions{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
.table-actions label{margin:0}
.table-actions .btn-inline{width:auto}
.table-actions .page-size{width:auto;min-width:88px}
.page-info{min-width:92px;text-align:center}
.table-wrap{overflow-x:auto}
table{width:100%;border-collapse:separate;border-spacing:0;min-width:690px}
th,td{padding:8px 6px;border-bottom:1px solid #e8edf3;text-align:center;font-size:.9rem;vertical-align:middle}
th{
  font-size:.75rem;
  color:var(--muted);
  text-transform:uppercase;
  letter-spacing:.04em;
  background:#f7fafc;
  border-bottom:1px solid #d7dee8;
}
thead th{position:sticky;top:0;z-index:1;backdrop-filter:blur(3px)}
td:nth-child(2),th:nth-child(2){text-align:center}
tbody tr:nth-child(even){background:#fbfdfe}
tbody tr:hover{background:#f1f8fc}
td.num{text-align:center;font-variant-numeric:tabular-nums;white-space:nowrap}
a{color:var(--accent);text-decoration:none}
a:hover{text-decoration:underline;text-decoration-thickness:1.5px}
td:first-child a{display:inline-block;max-width:340px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;vertical-align:bottom}
.muted{color:var(--muted)}
.sr{position:absolute;left:-9999px}
.var-up{color:var(--neg);font-weight:700}
.var-down{color:var(--pos);font-weight:700}
.var-flat{color:var(--muted);font-weight:700}
.empty-title{margin:0 0 8px 0}
.empty-text{margin:0 0 8px 0}
@media (max-width:1100px){
  .kpis{grid-template-columns:repeat(3,minmax(120px,1fr))}
  .filters-grid{grid-template-columns:repeat(3,minmax(120px,1fr))}
  .chart-wrap{grid-template-columns:1fr}
}
@media (max-width:700px){
  .wrap{padding:10px}
  .card{padding:12px}
  .header{grid-template-columns:1fr}
  .ui-version{text-align:left}
  .kpis{grid-template-columns:repeat(2,minmax(120px,1fr))}
  .filters-grid{grid-template-columns:1fr}
  .filters-grid>div{min-height:auto}
  .title{font-size:1.12rem}
  table{min-width:620px}
}
</style>
</head>
<body>
<div class="wrap stack">
  <section class="card header">
    <div>
      <h1 class="title">La Anonima Tracker: Monitor Economico de Precios</h1>
      <p class="meta">Generado: __GEN__ | Rango: __FROM__ a __TO__ | Canasta: __BASKET__</p>
      <p class="method">Metodologia: representativo (single) + terna low/mid/high auditada; variacion nominal y real deflactada por IPC INDEC (si disponible).</p>
    </div>
    <div>
      <span id="quality-badge" class="badge">Datos completos</span>
      <div class="method ui-version">UI v2 (__VIEW__)</div>
    </div>
  </section>

  <section class="card helper" id="quick-guide">
    <div class="guide-title">Lectura rapida</div>
    <div class="pills">
      <span class="pill">1) Revisa KPIs arriba para señal macro.</span>
      <span class="pill">2) Compara nominal vs real en el grafico.</span>
      <span class="pill">3) Usa la tabla paginada para detalle y exportacion.</span>
    </div>
    <div id="active-filters" class="pills"></div>
  </section>

  <section id="empty" class="card" style="display:none">
    <h2 class="empty-title">Sin datos para el rango seleccionado</h2>
    <p class="muted empty-text">El reporte se genero correctamente, pero no hay observaciones de precios en la base local.</p>
    <p class="muted">Paso sugerido: ejecutar <code>python -m src.cli scrape --basket all</code> y luego <code>python -m src.cli app</code>.</p>
  </section>

  <div id="app" class="stack">
    <section class="kpis" id="kpi-grid"></section>

    <details class="filters" id="filters-panel" open>
      <summary>Filtros y seleccion de productos</summary>
      <div class="filters-grid">
        <div>
          <label for="q">Buscar producto</label>
          <input id="q" placeholder="nombre o canonical_id" />
        </div>
        <div>
          <label for="cba">CBA</label>
          <select id="cba">
            <option value="all">Todos</option>
            <option value="yes">Si</option>
            <option value="no">No</option>
          </select>
        </div>
        <div>
          <label for="cat">Categoria</label>
          <select id="cat"></select>
        </div>
        <div>
          <label for="ord">Ordenar por</label>
          <select id="ord">
            <option value="alphabetical">Alfabetico</option>
            <option value="price">Precio</option>
            <option value="var_nominal">Var. nominal</option>
            <option value="var_real">Var. real</option>
          </select>
        </div>
        <div>
          <label for="mbase">Mes base variacion</label>
          <select id="mbase"></select>
        </div>
        <div>
          <label for="show-real">Tabla</label>
          <div class="inline-toggle">
            <input id="show-real" type="checkbox"/>
            <span class="muted">Mostrar var. real %</span>
          </div>
        </div>
        <div>
          <label>Modo precio</label>
          <div class="switchers">
            <button id="mode-nominal" type="button" class="active">Nominal</button>
            <button id="mode-real" type="button">Real</button>
          </div>
        </div>
        <div>
          <label>Seleccion rapida</label>
          <div class="switchers">
            <button id="quick-up" type="button">Ganadores</button>
            <button id="quick-down" type="button">Perdedores</button>
            <button id="quick-flat" type="button">Estables</button>
          </div>
        </div>
        <div>
          <label for="sel">Productos en grafico</label>
          <select id="sel" multiple size="5"></select>
        </div>
        <div class="reset-wrap">
          <label>&nbsp;</label>
          <button id="reset" class="primary" type="button">Reset</button>
        </div>
      </div>
    </details>

    <section class="chart-wrap">
      <article class="card chart-card">
        <h2>Comparativa de precios por producto</h2>
        <div id="chart-main" class="chart"><div class="chart-empty">Sin datos para graficar</div></div>
        <div id="legend-main" class="legend"></div>
      </article>
      <article class="card chart-card" id="panel-secondary">
        <h2>Canasta vs IPC (indice base 100)</h2>
        <div id="chart-secondary" class="chart small"><div class="chart-empty">Sin comparativa IPC</div></div>
        <div id="legend-secondary" class="legend"></div>
      </article>
    </section>

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
      <div class="quality-item" id="quality-ipc"></div>
      <div class="quality-item" id="quality-segments"></div>
      <div class="quality-item" id="quality-policy"></div>
      <ul id="warnings" class="warn-list"></ul>
    </section>

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
          <button id="export-csv" type="button" class="ghost btn-inline">Exportar CSV</button>
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
</div>

<script>
const p=__PAYLOAD__;
const defaults=p.ui_defaults||{};
const STORAGE_KEY="laanonima_tracker_report_state_v2";
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
  view:defaults.view||"executive",
  band_product:defaults.band_product||"",
  page_size:Number(defaults.page_size||50),
  current_page:Number(defaults.current_page||1)
};

const el={
  q:document.getElementById("q"),
  cba:document.getElementById("cba"),
  cat:document.getElementById("cat"),
  ord:document.getElementById("ord"),
  mb:document.getElementById("mbase"),
  sel:document.getElementById("sel"),
  tb:document.getElementById("tb"),
  reset:document.getElementById("reset"),
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
  qualityIpc:document.getElementById("quality-ipc"),
  qualitySegments:document.getElementById("quality-segments"),
  qualityPolicy:document.getElementById("quality-policy"),
  warnings:document.getElementById("warnings"),
  qualityPanel:document.getElementById("quality-panel"),
  panelSecondary:document.getElementById("panel-secondary"),
  chartMain:document.getElementById("chart-main"),
  legendMain:document.getElementById("legend-main"),
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
  activeFilters:document.getElementById("active-filters")
};

// Keep unicode range escaped to avoid encoding issues in generated standalone HTML.
const norm=v=>String(v||"").toLowerCase().normalize("NFD").replace(/[\\u0300-\\u036f]/g,"");
const esc=v=>String(v||"").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/\"/g,"&quot;").replace(/'/g,"&#39;");
const money=v=>v==null||Number.isNaN(Number(v))?"N/D":new Intl.NumberFormat("es-AR",{style:"currency",currency:"ARS",maximumFractionDigits:2}).format(Number(v));
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
  const normalized=raw.replace(",",".");
  const parts=normalized.split(" ").filter(Boolean);
  if(parts.length<2) return raw;
  const qty=Number(parts[0]);
  if(!Number.isFinite(qty)) return raw;

  const unit=norm(parts[1].replace(/[^a-z]/gi,""));
  const tail=parts.slice(2).join(" ");
  const suffix=tail ? ` ${tail}` : "";

  if(qty>0 && qty<1 && (unit==="kg" || unit==="kilo" || unit==="kilos")){
    return `${Math.round(qty*1000)} g${suffix}`;
  }
  if(qty>0 && qty<1 && (unit==="l" || unit==="lt" || unit==="litro" || unit==="litros")){
    return `${Math.round(qty*1000)} ml${suffix}`;
  }
  return raw;
}

function encodeHash(){
  const q=new URLSearchParams();
  q.set("q",st.query||"");
  q.set("cba",st.cba_filter);
  q.set("cat",st.category);
  q.set("ord",st.sort_by);
  q.set("mb",st.base_month||"");
  q.set("pm",st.price_mode);
  q.set("bp",st.band_product||"");
  q.set("real",st.show_real_column?"1":"0");
  q.set("sel",(st.selected_products||[]).join(","));
  q.set("ps",String(st.page_size||50));
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
    st.band_product=q.get("bp")??st.band_product;
    st.show_real_column=(q.get("real")||"0")==="1";
    const sel=q.get("sel");
    if(sel)st.selected_products=sel.split(",").filter(Boolean);
    st.page_size=Number(q.get("ps")||st.page_size||50);
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
}

function paginatedRows(rows){
  const total=Math.max(0, rows.length);
  const pageSize=Math.max(1, Number(st.page_size||50));
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
  const items=[
    `Productos filtrados: ${totalRows}`,
    `Modo: ${st.price_mode==="real"?"Real":"Nominal"}`,
    `CBA: ${st.cba_filter==="all"?"Todos":(st.cba_filter==="yes"?"Si":"No")}`,
    `Categoria: ${st.category==="all"?"Todas":st.category}`,
    `Base: ${st.base_month||"N/D"}`,
  ];
  if((st.query||"").trim()){
    items.push(`Busqueda: "${st.query.trim()}"`);
  }
  el.activeFilters.innerHTML=items.map(v=>`<span class="pill">${esc(v)}</span>`).join("");
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
      normalizePresentation(r.presentation),
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
  if(!series.length){
    const m=document.createElement("div");
    m.className="chart-empty";
    m.textContent="Sin datos para graficar";
    container.appendChild(m);
    return;
  }

  const canvas=document.createElement("canvas");
  container.appendChild(canvas);
  const w=Math.max(320,container.clientWidth||760);
  const h=Math.max(220,container.classList.contains("small")?232:346);
  const ratio=Math.max(1,window.devicePixelRatio||1);
  canvas.width=Math.floor(w*ratio);
  canvas.height=Math.floor(h*ratio);
  canvas.style.width=`${w}px`;
  canvas.style.height=`${h}px`;
  const ctx=canvas.getContext("2d");
  ctx.scale(ratio,ratio);

  const xs=series.flatMap(s=>s.points.map(p=>p.x.getTime())).filter(Number.isFinite);
  const ys=series.flatMap(s=>s.points.map(p=>Number(p.y))).filter(Number.isFinite);
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
  const ySpan=Math.max(1,maxY-minY);
  const yMin=minY-ySpan*0.08;
  const yMax=maxY+ySpan*0.08;

  const yTicks=[0,0.25,0.5,0.75,1];
  ctx.font="12px Segoe UI";
  const yTickLabels=yTicks.map(t=>fmtAxisNum(yMin+(yMax-yMin)*t));
  const maxYLabelW=yTickLabels.reduce((acc,label)=>Math.max(acc,ctx.measureText(label).width),0);
  const pad={l:Math.max(78,Math.ceil(maxYLabelW)+44),r:20,t:20,b:56};
  const innerW=Math.max(40,w-pad.l-pad.r);
  const innerH=Math.max(40,h-pad.t-pad.b);
  const mapX=x=>pad.l+((x-minX)/(Math.max(1,maxX-minX)))*innerW;
  const mapY=y=>pad.t+((yMax-y)/(Math.max(1,yMax-yMin)))*innerH;
  const xTickCount=maxX===minX?1:Math.min(6,Math.max(2,Math.floor(innerW/120)));
  const xTicks=xTickCount===1
    ? [0.5]
    : Array.from({length:xTickCount},(_,i)=>i/(xTickCount-1));

  ctx.clearRect(0,0,w,h);
  ctx.strokeStyle="#d8dce3";
  ctx.lineWidth=1;
  for(const t of yTicks){
    const y=pad.t+innerH*(1-t);
    ctx.beginPath();ctx.moveTo(pad.l,y);ctx.lineTo(w-pad.r,y);ctx.stroke();
  }
  for(const t of xTicks){
    const x=pad.l+innerW*t;
    ctx.beginPath();ctx.moveTo(x,pad.t);ctx.lineTo(x,h-pad.b);ctx.stroke();
  }
  ctx.beginPath();
  ctx.moveTo(pad.l,pad.t);ctx.lineTo(pad.l,h-pad.b);ctx.lineTo(w-pad.r,h-pad.b);ctx.strokeStyle="#8b95a7";ctx.stroke();

  ctx.fillStyle="#5a6577";
  ctx.font="12px Segoe UI";
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
    const d=new Date(ts);
    const lbl=`${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,"0")}`;
    ctx.textAlign=xTicks.length===1?"center":(idx===0?"left":(idx===xTicks.length-1?"right":"center"));
    ctx.fillText(lbl,x,h-pad.b+12);
  });
  ctx.textAlign="center";
  ctx.fillText(xLabel,w/2,h-18);

  series.forEach(s=>{
    if(!s.points.length)return;
    ctx.strokeStyle=s.color;
    ctx.lineWidth=2.2;
    ctx.beginPath();
    s.points.forEach((pt,idx)=>{
      const x=mapX(pt.x.getTime());
      const y=mapY(Number(pt.y));
      if(idx===0)ctx.moveTo(x,y);else ctx.lineTo(x,y);
    });
    ctx.stroke();
    if(s.points.length<=24){
      ctx.fillStyle=s.color;
      s.points.forEach(pt=>{
        const x=mapX(pt.x.getTime());
        const y=mapY(Number(pt.y));
        ctx.beginPath();
        ctx.arc(x,y,2.4,0,Math.PI*2);
        ctx.fill();
      });
    }
  });

  for(const s of series){
    const item=document.createElement("div");
    item.className="item";
    item.innerHTML=`<span class="dot" style="background:${s.color}"></span>${esc(s.name)}`;
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

function drawSecondaryChart(force=false){
  const secondaryKey=[st.view,p.basket_vs_ipc_series?.length||0].join("|");
  if(!force && secondaryKey===_lastSecondaryChartKey){
    return;
  }
  _lastSecondaryChartKey=secondaryKey;
  if(st.view==="executive"){
    el.panelSecondary.style.display="none";
  }else{
    el.panelSecondary.style.display="";
  }
  const src=(p.basket_vs_ipc_series||[]);
  const basket=src.map(x=>({x:new Date(`${x.year_month}-01T00:00:00`),y:x.basket_index_base100})).filter(x=>x.y!=null);
  const ipc=src.map(x=>({x:new Date(`${x.year_month}-01T00:00:00`),y:x.ipc_index_base100})).filter(x=>x.y!=null);
  const gap=src.map(x=>({x:new Date(`${x.year_month}-01T00:00:00`),y:x.gap_points})).filter(x=>x.y!=null);
  const series=[
    {name:"Canasta base 100",points:basket,color:"#005f73"},
    {name:"IPC base 100",points:ipc,color:"#ca6702"},
    {name:"Brecha (puntos)",points:gap,color:"#9b2226"}
  ].filter(s=>s.points.length>0);
  drawCanvasChart(el.chartSecondary,el.legendSecondary,series,"Indice base 100 / brecha");
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
    const presentation=normalizePresentation(r.presentation||"N/D");
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
  const cba=sq.cba||{};
  const core=sq.daily_core||{};
  const rot=sq.daily_rotation||{};
  el.qualityBadge.textContent=qf.badge||"Datos parciales";
  el.qualityBadge.className=`badge${qf.is_partial?" warn":""}`;
  el.qualityCoverage.textContent=`Cobertura total: ${fmtNum(cov.coverage_total_pct)}% | Esperados: ${cov.expected_products ?? "N/D"} | Observados: ${cov.observed_products_total ?? "N/D"}`;
  el.qualityPanelSize.textContent=`Panel balanceado: ${qf.balanced_panel_n ?? "N/D"} productos`;
  el.qualityIpc.textContent=`Meses sin IPC: ${(qf.missing_cpi_months||[]).length? qf.missing_cpi_months.join(", ") : "ninguno"}`;
  if(el.qualitySegments){
    el.qualitySegments.textContent=
      `Cobertura segmentos -> CBA: ${cba.observed ?? 0}/${cba.expected ?? 0} (${fmtNum(cba.coverage_pct)}%), `
      + `Nucleo diario: ${core.observed ?? 0}/${core.expected ?? 0} (${fmtNum(core.coverage_pct)}%), `
      + `Rotacion: ${rot.observed ?? 0}/${rot.expected ?? 0} (${fmtNum(rot.coverage_pct)}%)`;
  }
  if(el.qualityPolicy){
    el.qualityPolicy.textContent=
      `Politica: ${sq.observation_policy || "single"} | Candidate storage: ${sq.candidate_storage_mode || "off"} | `
      + `Regla de terna objetivo: >=${sq.tier_rule_target ?? 3} | `
      + `Cumplimiento terna: ${pct(sq.terna_compliance_pct)} (${sq.products_with_full_terna ?? 0}/${sq.products_with_bands ?? 0})`;
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
}

function setButtonsState(){
  el.modeNominal.classList.toggle("active",st.price_mode==="nominal");
  el.modeReal.classList.toggle("active",st.price_mode==="real");
}

function applyStateToControls(){
  el.q.value=st.query||"";
  el.cba.value=st.cba_filter||"all";
  el.cat.value=st.category||"all";
  el.ord.value=st.sort_by||"alphabetical";
  el.mb.value=(p.months||[]).includes(st.base_month)?st.base_month:(p.months?.[0]||"");
  st.base_month=el.mb.value;
  el.showReal.checked=!!st.show_real_column;
  if(el.pageSize){
    const validSizes=Array.from(el.pageSize.options).map(o=>Number(o.value));
    if(!validSizes.includes(Number(st.page_size))){
      st.page_size=validSizes.includes(50)?50:validSizes[0];
    }
    el.pageSize.value=String(st.page_size);
  }
  st.current_page=Math.max(1, Number(st.current_page||1));
  setButtonsState();
}

function render(){
  const rows=filteredRows();
  syncSelection(rows);
  drawTable(rows);
  drawMainChart(rows);
  drawSecondaryChart();
  drawBandChart(rows);
  drawKpis();
  drawActiveFilters(rows.length);
  drawQuality();
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
  st.view=defaults.view||"executive";
  st.band_product=defaults.band_product||"";
  st.page_size=Number(defaults.page_size||50);
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

function debounce(fn,ms){
  let t=null;
  return (...args)=>{
    if(t)clearTimeout(t);
    t=setTimeout(()=>fn(...args),ms);
  };
}

function bindEvents(){
  el.q.addEventListener("input",debounce((e)=>{st.query=e.target.value||"";st.current_page=1;_rowsCacheKey="";render();},200));
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
  if(el.bandProduct){
    el.bandProduct.addEventListener("change",(e)=>{st.band_product=e.target.value||"";drawBandChart(filteredRows());saveState();});
  }
  el.quickUp.addEventListener("click",()=>quickPick("up"));
  el.quickDown.addEventListener("click",()=>quickPick("down"));
  el.quickFlat.addEventListener("click",()=>quickPick("flat"));
  if(el.pageSize){
    el.pageSize.addEventListener("change",(e)=>{
      st.page_size=Number(e.target.value||50);
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
  el.reset.addEventListener("click",resetState);
  window.addEventListener("resize",debounce(()=>{const rows=filteredRows();drawMainChart(rows,true);drawSecondaryChart(true);drawBandChart(rows);},150));
}

function init(){
  if(!p.has_data){
    document.getElementById("empty").style.display="";
    document.getElementById("app").style.display="none";
    const guide=document.getElementById("quick-guide");
    if(guide) guide.style.display="none";
    drawQuality();
    return;
  }
  if(window.innerWidth<900){
    const fp=document.getElementById("filters-panel");
    if(fp)fp.open=false;
  }
  loadState();
  mountFilterOptions();
  applyStateToControls();
  bindEvents();
  render();
}
init();
</script>
</body>
</html>"""

        return (
            template.replace("__PAYLOAD__", payload_json)
            .replace("__EXTERNAL_SCRIPT__", external_script)
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

        out_dir = Path("data/analysis/reports")
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
