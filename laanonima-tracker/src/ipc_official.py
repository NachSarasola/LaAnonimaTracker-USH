"""Official CPI ingestion pipeline (INDEC discovery + XLS/PDF hybrid + fallback CSV)."""

from __future__ import annotations

import io
import json
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import pandas as pd
import requests
from loguru import logger
from sqlalchemy.orm import Session

from src.config_loader import load_config
from src.models import OfficialCPIMonthly, get_engine, get_session_factory, init_db, now_utc

SPANISH_MONTHS = {
    "enero": 1,
    "febrero": 2,
    "marzo": 3,
    "abril": 4,
    "mayo": 5,
    "junio": 6,
    "julio": 7,
    "agosto": 8,
    "septiembre": 9,
    "setiembre": 9,
    "octubre": 10,
    "noviembre": 11,
    "diciembre": 12,
}

GENERAL_CATEGORY_SENTINEL = "__general__"


@dataclass
class OfficialSyncResult:
    """Result metadata for official CPI sync runs."""

    source_mode: str
    source: str
    official_source: str
    region: str
    regions: List[str]
    used_fallback: bool
    fetched_rows: int
    upserted_rows: int
    from_month: Optional[str]
    to_month: Optional[str]
    snapshot_path: Optional[str]
    snapshot_paths: List[str]
    source_document_url: Optional[str]
    source_assets: Dict[str, str]
    validation_status: str
    warnings: List[str]


class OfficialCPIProvider:
    """Base interface for official CPI data providers."""

    def discover_assets(self) -> Dict[str, str]:
        raise NotImplementedError

    def parse_xls_bytes(self, blob: bytes) -> pd.DataFrame:
        raise NotImplementedError

    def parse_pdf_bytes(self, blob: bytes) -> pd.DataFrame:
        raise NotImplementedError


class INDECPatagoniaProvider(OfficialCPIProvider):
    """INDEC provider with discovery and XLS/PDF parsers."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.ipc_cfg = config.get("analysis", {}).get("ipc_official", {})
        self.mapping_cfg = config.get("analysis", {}).get("ipc_category_mapping", {})

    @staticmethod
    def _normalize_text(value: Any) -> str:
        if value is None or pd.isna(value):
            return ""
        txt = str(value).strip().lower()
        txt = unicodedata.normalize("NFD", txt)
        txt = "".join(ch for ch in txt if not unicodedata.combining(ch))
        txt = re.sub(r"\s+", " ", txt)
        return txt

    @staticmethod
    def _slugify(value: Any) -> str:
        txt = INDECPatagoniaProvider._normalize_text(value)
        txt = re.sub(r"[^a-z0-9]+", "_", txt)
        txt = re.sub(r"_+", "_", txt).strip("_")
        return txt

    @staticmethod
    def _normalize_month(value: Any) -> Optional[str]:
        if value is None or pd.isna(value):
            return None
        if isinstance(value, pd.Period):
            return str(value.asfreq("M"))
        try:
            if isinstance(value, (datetime, pd.Timestamp)):
                return str(pd.Period(value, freq="M"))
        except Exception:
            pass
        txt = str(value).strip()
        if not txt:
            return None
        try:
            return str(pd.Period(pd.to_datetime(txt, errors="raise"), freq="M"))
        except Exception:
            pass
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d", "%Y-%m", "%Y/%m"):
            try:
                return str(pd.Period(datetime.strptime(txt, fmt), freq="M"))
            except Exception:
                continue
        return None

    @staticmethod
    def _normalize_numeric(value: Any) -> Optional[float]:
        if value is None or pd.isna(value):
            return None
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return float(value)
        txt = str(value).strip()
        if not txt:
            return None
        txt = (
            txt.replace("\u2212", "-")
            .replace("%", "")
            .replace("\xa0", "")
            .replace(",", ".")
        )
        txt = re.sub(r"[^0-9\.\-]", "", txt)
        if not txt or txt in {"-", ".", "-."}:
            return None
        try:
            return float(txt)
        except Exception:
            return None

    @staticmethod
    def _normalize_region(value: str) -> str:
        txt = INDECPatagoniaProvider._normalize_text(value)
        if "patagonia" in txt:
            return "patagonia"
        if "nacional" in txt:
            return "nacional"
        if txt in {"all", "todas", "*"}:
            return "all"
        return txt or "patagonia"

    @staticmethod
    def _persist_raw_blob(blob: bytes, suffix: str, prefix: str = "indec_raw") -> str:
        now = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        out_dir = Path("data/cpi/raw")
        out_dir.mkdir(parents=True, exist_ok=True)
        path = out_dir / f"{prefix}_{now}.{suffix}"
        path.write_bytes(blob)
        return str(path)

    @staticmethod
    def _persist_raw_text(text: str, suffix: str, prefix: str = "indec_discovery") -> str:
        now = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        out_dir = Path("data/cpi/raw")
        out_dir.mkdir(parents=True, exist_ok=True)
        path = out_dir / f"{prefix}_{now}.{suffix}"
        path.write_text(text, encoding="utf-8")
        return str(path)

    def _category_mapping(self) -> Dict[str, Optional[str]]:
        if not isinstance(self.mapping_cfg, dict):
            return {}
        explicit = self.mapping_cfg.get("app_to_indec_division")
        if isinstance(explicit, dict):
            return {
                str(k).strip().lower(): (
                    self._slugify(v) if v not in {None, ""} else None
                )
                for k, v in explicit.items()
            }
        legacy = self.mapping_cfg.get("map")
        if isinstance(legacy, dict):
            mapped: Dict[str, Optional[str]] = {}
            for k, v in legacy.items():
                key = str(k).strip().lower()
                if v in {None, ""}:
                    mapped[key] = None
                    continue
                val = str(v).strip().lower()
                mapped[key] = self._slugify(val)
            return mapped
        return {}

    def _division_to_app_reverse(self) -> Dict[str, str]:
        out: Dict[str, str] = {}
        for app_category, division in self._category_mapping().items():
            if division:
                out[str(division)] = app_category
        return out

    @staticmethod
    def _pick_first_column(df: pd.DataFrame, aliases: List[str]) -> Optional[str]:
        lowered = {str(col).lower(): col for col in df.columns}
        for alias in aliases:
            if alias.lower() in lowered:
                return lowered[alias.lower()]
        return None

    @staticmethod
    def _select_monthly_xls_link(candidates: List[str]) -> Optional[str]:
        if not candidates:
            return None
        deduped: List[str] = list(dict.fromkeys(candidates))
        period_matches: List[tuple[int, int, str]] = []
        for href in deduped:
            match = re.search(r"/sh_ipc_(\d{2})_(\d{2})\.(?:xls|xlsx)$", href, flags=re.IGNORECASE)
            if not match:
                continue
            month = int(match.group(1))
            year_two_digits = int(match.group(2))
            year = 2000 + year_two_digits
            period_matches.append((year, month, href))
        if period_matches:
            period_matches.sort(reverse=True)
            return period_matches[0][2]

        for href in deduped:
            norm = INDECPatagoniaProvider._normalize_text(Path(href).name)
            if "aperturas" in norm or "precios_promedio" in norm:
                continue
            return href
        return deduped[0]

    def _as_normalized_df(self, raw_df: pd.DataFrame, default_region: str) -> pd.DataFrame:
        if raw_df.empty:
            return pd.DataFrame(
                columns=[
                    "year_month",
                    "region",
                    "index_value",
                    "mom_change",
                    "yoy_change",
                    "metric_code",
                    "category_slug",
                    "status",
                ]
            )

        year_month_col = self._pick_first_column(raw_df, ["year_month", "periodo", "periodo_mensual", "mes", "month"])
        index_col = self._pick_first_column(raw_df, ["index_value", "cpi_index", "ipc_index", "indice", "index"])
        mom_col = self._pick_first_column(raw_df, ["mom_change", "cpi_mom", "ipc_mom", "mom", "mensual_pct", "var_mensual"])
        yoy_col = self._pick_first_column(raw_df, ["yoy_change", "cpi_yoy", "ipc_yoy", "yoy", "interanual_pct", "var_interanual"])
        metric_col = self._pick_first_column(raw_df, ["metric_code", "division_code", "indec_division_code", "series_code", "code"])
        category_col = self._pick_first_column(raw_df, ["category_slug", "categoria", "rubro", "division_slug", "division"])
        status_col = self._pick_first_column(raw_df, ["status", "estado"])
        region_col = self._pick_first_column(raw_df, ["region", "region_name", "zona"])

        if year_month_col is None or index_col is None:
            raise ValueError("Fuente oficial invalida: faltan columnas year_month/index")

        out = pd.DataFrame()
        out["year_month"] = raw_df[year_month_col].map(self._normalize_month)
        out["index_value"] = raw_df[index_col].map(self._normalize_numeric)
        out["mom_change"] = raw_df[mom_col].map(self._normalize_numeric) if mom_col else pd.NA
        out["yoy_change"] = raw_df[yoy_col].map(self._normalize_numeric) if yoy_col else pd.NA

        if metric_col:
            out["metric_code"] = raw_df[metric_col].map(self._slugify)
        else:
            out["metric_code"] = "general"

        if category_col:
            out["category_slug"] = raw_df[category_col].map(self._slugify)
        else:
            out["category_slug"] = pd.NA

        out["status"] = (
            raw_df[status_col].astype(str).str.strip().str.lower()
            if status_col
            else "final"
        )
        if region_col:
            out["region"] = raw_df[region_col].astype(str).map(self._normalize_region)
        else:
            out["region"] = self._normalize_region(default_region)

        app_to_division = self._category_mapping()
        reverse_map = self._division_to_app_reverse()
        if app_to_division:
            metric_as_app = out["metric_code"].isin(app_to_division.keys())
            out.loc[metric_as_app, "metric_code"] = out.loc[metric_as_app, "metric_code"].map(
                lambda c: app_to_division.get(str(c), c) or c
            )
            category_as_app = out["category_slug"].isin(app_to_division.keys())
            out.loc[category_as_app, "category_slug"] = out.loc[category_as_app, "category_slug"].map(
                lambda c: app_to_division.get(str(c), c) or c
            )

            missing_slug = out["category_slug"].isna() & (out["metric_code"] != "general")
            if missing_slug.any():
                out.loc[missing_slug, "category_slug"] = out.loc[missing_slug, "metric_code"]

            category_is_app = out["category_slug"].map(lambda v: str(v) in app_to_division if pd.notna(v) else False)
            out.loc[category_is_app, "category_slug"] = out.loc[category_is_app, "category_slug"].map(
                lambda c: app_to_division.get(str(c), c)
            )

            empty_metric = out["metric_code"].isin({"", "nan"}) & out["category_slug"].notna()
            out.loc[empty_metric, "metric_code"] = out.loc[empty_metric, "category_slug"]
            empty_category = out["category_slug"].isna() & out["metric_code"].isin(reverse_map.keys())
            out.loc[empty_category, "category_slug"] = out.loc[empty_category, "metric_code"]

        out["metric_code"] = out["metric_code"].replace({"": "general", "nan": "general"})
        out["category_slug"] = out["category_slug"].replace({"": pd.NA, "nan": pd.NA})
        non_general = out["metric_code"] != "general"
        out.loc[non_general & out["category_slug"].isna(), "category_slug"] = out.loc[non_general, "metric_code"]
        out = out.dropna(subset=["year_month", "index_value"])
        return out

    @staticmethod
    def _read_df_by_format(raw_text: str, fmt: str) -> pd.DataFrame:
        normalized_fmt = (fmt or "csv").strip().lower()
        if normalized_fmt == "json":
            payload = json.loads(raw_text)
            if isinstance(payload, dict):
                items = payload.get("items") or payload.get("data") or payload.get("rows") or []
                return pd.DataFrame(items)
            return pd.DataFrame(payload)
        return pd.read_csv(io.StringIO(raw_text))

    def fetch_auto_source(
        self,
        from_month: Optional[str],
        to_month: Optional[str],
        region: str,
    ) -> pd.DataFrame:
        auto_cfg = self.ipc_cfg.get("auto_source", {}) if isinstance(self.ipc_cfg, dict) else {}
        source = auto_cfg.get("url")
        fmt = auto_cfg.get("format", "csv")
        timeout_seconds = int(auto_cfg.get("timeout_seconds", 20))
        if not source:
            raise ValueError("analysis.ipc_official.auto_source.url no configurado")

        if str(source).startswith(("http://", "https://")):
            response = requests.get(str(source), timeout=timeout_seconds)
            response.raise_for_status()
            raw_text = response.text
        else:
            path = Path(str(source))
            if not path.exists():
                raise FileNotFoundError(f"Auto source no encontrado: {source}")
            raw_text = path.read_text(encoding="utf-8")
        raw_df = self._read_df_by_format(raw_text, str(fmt))
        df = self._as_normalized_df(raw_df, default_region=region)
        if from_month:
            df = df[df["year_month"] >= from_month]
        if to_month:
            df = df[df["year_month"] <= to_month]
        return df.sort_values(["region", "year_month", "metric_code"]).reset_index(drop=True)

    def discover_assets(self) -> Dict[str, str]:
        discovery_url = str(
            self.ipc_cfg.get(
                "discovery_url",
                "https://www.indec.gob.ar/Nivel4/Tema/3/5/31",
            )
        )
        timeout_seconds = int(
            self.ipc_cfg.get("auto_source", {}).get("timeout_seconds", 20)
            if isinstance(self.ipc_cfg.get("auto_source"), dict)
            else 20
        )
        response = requests.get(discovery_url, timeout=timeout_seconds)
        response.raise_for_status()
        html = response.text
        html_snapshot = self._persist_raw_text(html, suffix="html", prefix="indec_discovery")

        pdf_matches = re.findall(
            r'href=["\'](/uploads/informesdeprensa/ipc_[^"\']+\.pdf)["\']',
            html,
            flags=re.IGNORECASE,
        )
        xls_matches = re.findall(
            r'href=["\'](/ftp/cuadros/economia/sh_ipc_[^"\']+\.(?:xls|xlsx))["\']',
            html,
            flags=re.IGNORECASE,
        )
        if not pdf_matches and not xls_matches:
            raise ValueError("No se detectaron links PDF/XLS en la pagina de INDEC.")

        out: Dict[str, str] = {
            "discovery_url": discovery_url,
            "html_snapshot_path": html_snapshot,
        }
        if pdf_matches:
            out["pdf_url"] = urljoin(discovery_url, list(dict.fromkeys(pdf_matches))[0])
        selected_xls = self._select_monthly_xls_link(xls_matches)
        if selected_xls:
            out["xls_url"] = urljoin(discovery_url, selected_xls)
        return out

    @staticmethod
    def _metric_code_from_label(label: Any) -> Optional[str]:
        norm = INDECPatagoniaProvider._normalize_text(label)
        if not norm:
            return None
        if norm.startswith("nivel general y divisiones"):
            return None
        if norm.startswith("nivel general"):
            return "general"
        if norm.startswith("categorias"):
            return None
        if norm in {"bienes y servicios", "bienes y servicios:"}:
            return None
        if norm.startswith("fuente"):
            return None
        return INDECPatagoniaProvider._slugify(label)

    def _parse_sheet_metric_values(self, df: pd.DataFrame, value_col: str) -> pd.DataFrame:
        region_rows: List[tuple[int, str]] = []
        for idx in range(len(df)):
            first_col = self._normalize_text(df.iloc[idx, 0])
            if first_col.startswith("total nacional"):
                region_rows.append((idx, "nacional"))
            elif "region patagonia" in first_col:
                region_rows.append((idx, "patagonia"))
        if not region_rows:
            return pd.DataFrame(columns=["region", "year_month", "metric_code", "category_slug", value_col])

        records: List[Dict[str, Any]] = []
        for pos, (start_idx, region) in enumerate(region_rows):
            end_idx = region_rows[pos + 1][0] if pos + 1 < len(region_rows) else len(df)
            header = df.iloc[start_idx]
            month_cols: List[tuple[int, str]] = []
            for col_idx in range(1, len(header)):
                month = self._normalize_month(header.iloc[col_idx])
                if month:
                    month_cols.append((col_idx, month))
            if not month_cols:
                continue

            section_idx: Optional[int] = None
            scan_limit = min(start_idx + 15, end_idx)
            for row_idx in range(start_idx + 1, scan_limit):
                label_norm = self._normalize_text(df.iloc[row_idx, 0])
                if "nivel general y divisiones coicop" in label_norm:
                    section_idx = row_idx
                    break
            if section_idx is None:
                continue

            data_idx = section_idx + 1
            while data_idx < end_idx and not self._normalize_text(df.iloc[data_idx, 0]):
                data_idx += 1

            for row_idx in range(data_idx, end_idx):
                raw_label = df.iloc[row_idx, 0]
                label_norm = self._normalize_text(raw_label)
                if not label_norm:
                    continue
                if label_norm.startswith("categorias"):
                    break
                metric_code = self._metric_code_from_label(raw_label)
                if metric_code is None:
                    continue
                category_slug = GENERAL_CATEGORY_SENTINEL if metric_code == "general" else metric_code
                for col_idx, year_month in month_cols:
                    if col_idx >= df.shape[1]:
                        continue
                    value = self._normalize_numeric(df.iloc[row_idx, col_idx])
                    if value is None:
                        continue
                    records.append(
                        {
                            "region": region,
                            "year_month": year_month,
                            "metric_code": metric_code,
                            "category_slug": category_slug,
                            value_col: value,
                        }
                    )
        return pd.DataFrame(records)

    @staticmethod
    def _find_sheet_name(sheet_names: List[str], candidates: List[str]) -> Optional[str]:
        normalized = {INDECPatagoniaProvider._normalize_text(name): name for name in sheet_names}
        for candidate in candidates:
            needle = INDECPatagoniaProvider._normalize_text(candidate)
            for key, original in normalized.items():
                if needle in key:
                    return original
        return None

    @staticmethod
    def _first_non_null(series: pd.Series) -> Any:
        for value in series:
            if pd.notna(value):
                return value
        return None

    def parse_xls_bytes(self, blob: bytes) -> pd.DataFrame:
        xls = pd.ExcelFile(io.BytesIO(blob))
        sheet_names = xls.sheet_names

        monthly_sheet = self._find_sheet_name(sheet_names, ["variacion mensual ipc nacional"])
        yoy_sheet = self._find_sheet_name(sheet_names, ["interanual ipc nacional"])
        index_sheet = self._find_sheet_name(sheet_names, ["indices ipc cobertura nacional"])

        if not monthly_sheet or not yoy_sheet or not index_sheet:
            raise ValueError("No se pudieron ubicar las hojas requeridas en el XLS oficial.")

        monthly_df = pd.read_excel(io.BytesIO(blob), sheet_name=monthly_sheet, header=None)
        yoy_df = pd.read_excel(io.BytesIO(blob), sheet_name=yoy_sheet, header=None)
        index_df = pd.read_excel(io.BytesIO(blob), sheet_name=index_sheet, header=None)

        monthly_long = self._parse_sheet_metric_values(monthly_df, "mom_change")
        yoy_long = self._parse_sheet_metric_values(yoy_df, "yoy_change")
        index_long = self._parse_sheet_metric_values(index_df, "index_value")

        keys = ["region", "year_month", "metric_code", "category_slug"]
        merged = index_long.merge(monthly_long, on=keys, how="outer")
        merged = merged.merge(yoy_long, on=keys, how="outer")

        if merged.empty:
            return pd.DataFrame(
                columns=["region", "year_month", "metric_code", "category_slug", "index_value", "mom_change", "yoy_change", "status"]
            )

        grouped = (
            merged.groupby(keys, as_index=False, dropna=False)
            .agg(
                {
                    "index_value": self._first_non_null,
                    "mom_change": self._first_non_null,
                    "yoy_change": self._first_non_null,
                }
            )
            .reset_index(drop=True)
        )
        grouped["status"] = "final"
        grouped["category_slug"] = grouped["category_slug"].replace(GENERAL_CATEGORY_SENTINEL, pd.NA)
        grouped["category_slug"] = grouped.apply(
            lambda r: None if r["metric_code"] == "general" else r["category_slug"],
            axis=1,
        )
        grouped = grouped.dropna(subset=["year_month", "metric_code"])
        return grouped.sort_values(["region", "year_month", "metric_code"]).reset_index(drop=True)

    @staticmethod
    def _extract_pdf_year_month(text: str) -> Optional[str]:
        normalized = INDECPatagoniaProvider._normalize_text(text)
        match = re.search(
            r"\b(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|setiembre|octubre|noviembre|diciembre)\s+de\s+(\d{4})\b",
            normalized,
        )
        if not match:
            return None
        month_name = match.group(1)
        year = int(match.group(2))
        month = SPANISH_MONTHS.get(month_name)
        if not month:
            return None
        return f"{year:04d}-{month:02d}"

    def parse_pdf_bytes(self, blob: bytes) -> pd.DataFrame:
        try:
            import pdfplumber  # type: ignore
            import logging
        except Exception as exc:
            raise RuntimeError("pdfplumber no esta disponible para parsear el PDF oficial.") from exc
        logging.getLogger("pdfminer").setLevel(logging.ERROR)

        records: List[Dict[str, Any]] = []
        with pdfplumber.open(io.BytesIO(blob)) as pdf:
            combined_text = "\n".join((page.extract_text() or "") for page in pdf.pages[:2])
            year_month = self._extract_pdf_year_month(combined_text)

            selected_table = None
            data_start_idx = None
            idx_nacional = None
            idx_patagonia = None
            target_page_text = ""

            for page in pdf.pages:
                tables = page.extract_tables() or []
                try:
                    alt_table = page.extract_table(
                        table_settings={
                            "vertical_strategy": "lines",
                            "horizontal_strategy": "lines",
                        }
                    )
                    if alt_table:
                        tables.append(alt_table)
                except Exception:
                    pass

                for table in tables:
                    norm_rows = [[self._normalize_text(cell) for cell in (row or [])] for row in table]
                    preview_rows = norm_rows[:5]
                    flat_preview = [cell for row in preview_rows for cell in row if cell]
                    if not any("nacional" in cell for cell in flat_preview):
                        continue
                    if not any("patagonia" in cell for cell in flat_preview):
                        continue

                    nacional_row = None
                    patagonia_row = None
                    for row_idx, norm_row in enumerate(preview_rows):
                        for col_idx, cell in enumerate(norm_row):
                            if idx_nacional is None and "nacional" in cell:
                                idx_nacional = col_idx
                                nacional_row = row_idx
                            if idx_patagonia is None and "patagonia" in cell:
                                idx_patagonia = col_idx
                                patagonia_row = row_idx
                    if idx_nacional is None or idx_patagonia is None:
                        idx_nacional = None
                        idx_patagonia = None
                        continue

                    percentage_row = None
                    for row_idx, norm_row in enumerate(preview_rows):
                        if any("porcentaje" in cell for cell in norm_row):
                            percentage_row = row_idx
                            break
                    if percentage_row is not None:
                        data_start_idx = percentage_row + 1
                    else:
                        fallback_header = max(nacional_row or 0, patagonia_row or 0)
                        data_start_idx = fallback_header + 1

                    has_valid_rows = False
                    for sample in table[data_start_idx : data_start_idx + 12]:
                        if not sample:
                            continue
                        sample_label = self._normalize_text(sample[0] if len(sample) > 0 else None)
                        if not sample_label:
                            continue
                        if "nivel general" not in sample_label and "alimentos y bebidas" not in sample_label:
                            continue
                        sample_nacional = self._normalize_numeric(
                            sample[idx_nacional] if idx_nacional < len(sample) else None
                        )
                        sample_patagonia = self._normalize_numeric(
                            sample[idx_patagonia] if idx_patagonia < len(sample) else None
                        )
                        if sample_nacional is not None or sample_patagonia is not None:
                            has_valid_rows = True
                            break
                    if not has_valid_rows:
                        idx_nacional = None
                        idx_patagonia = None
                        data_start_idx = None
                        continue

                    selected_table = table
                    target_page_text = self._normalize_text(page.extract_text() or "")
                    break
                if selected_table is not None:
                    break

            if year_month is None and target_page_text:
                year_month = self._extract_pdf_year_month(target_page_text)
            if year_month is None:
                raise ValueError("No se pudo inferir year_month desde el PDF oficial.")
            if selected_table is None or data_start_idx is None or idx_nacional is None or idx_patagonia is None:
                raise ValueError("No se pudo extraer tabla Nacional/Patagonia desde el PDF.")

            for row in selected_table[data_start_idx:]:
                if not row:
                    continue
                label = row[0] if len(row) > 0 else None
                label_norm = self._normalize_text(label)
                if not label_norm:
                    continue
                if label_norm.startswith("porcentaje"):
                    continue
                if "nivel general y divisiones" in label_norm:
                    continue
                if label_norm.startswith("fuente"):
                    break
                metric_code = self._metric_code_from_label(label)
                if metric_code is None:
                    continue
                val_nacional = self._normalize_numeric(row[idx_nacional] if idx_nacional < len(row) else None)
                val_patagonia = self._normalize_numeric(row[idx_patagonia] if idx_patagonia < len(row) else None)
                if val_nacional is None and val_patagonia is None:
                    continue
                category_slug = None if metric_code == "general" else metric_code
                if val_nacional is not None:
                    records.append(
                        {
                            "region": "nacional",
                            "year_month": year_month,
                            "metric_code": metric_code,
                            "category_slug": category_slug,
                            "mom_change": val_nacional,
                        }
                    )
                if val_patagonia is not None:
                    records.append(
                        {
                            "region": "patagonia",
                            "year_month": year_month,
                            "metric_code": metric_code,
                            "category_slug": category_slug,
                            "mom_change": val_patagonia,
                        }
                    )

        out = pd.DataFrame(records)
        if out.empty:
            return pd.DataFrame(columns=["region", "year_month", "metric_code", "category_slug", "mom_change"])
        return out.sort_values(["region", "year_month", "metric_code"]).reset_index(drop=True)


def _validate_continuity(df: pd.DataFrame) -> List[str]:
    warnings: List[str] = []
    if df.empty:
        warnings.append("No se recibieron filas oficiales para sincronizar.")
        return warnings

    duplicated = df.duplicated(subset=["region", "year_month", "metric_code"], keep=False)
    if duplicated.any():
        dup_rows = df.loc[duplicated, ["region", "year_month", "metric_code"]].drop_duplicates()
        warnings.append(
            "Duplicados por region/mes/serie detectados: "
            + ", ".join(f"{r.region}:{r.year_month}:{r.metric_code}" for r in dup_rows.itertuples(index=False))
        )

    for (region, metric_code), grp in df.groupby(["region", "metric_code"]):
        months = sorted(grp["year_month"].astype(str).unique().tolist())
        if len(months) <= 1:
            continue
        expected = [str(p) for p in pd.period_range(months[0], months[-1], freq="M")]
        missing = [m for m in expected if m not in set(months)]
        if missing:
            sample = ", ".join(missing[:6])
            suffix = "..." if len(missing) > 6 else ""
            warnings.append(f"Serie {region}/{metric_code} con huecos mensuales: {sample}{suffix}")
    return warnings


def _persist_snapshot(df: pd.DataFrame, source_tag: str) -> Optional[str]:
    if df.empty:
        return None
    now = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_dir = Path("data/cpi/raw")
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"official_{source_tag}_{now}.csv"
    df.to_csv(path, index=False)
    return str(path)


def _upsert_official_rows(
    session: Session,
    df: pd.DataFrame,
    source_code: str,
    status: str,
    is_fallback: bool,
    snapshot_path: Optional[str],
) -> int:
    upserted = 0
    for row in df.itertuples(index=False):
        region = str(getattr(row, "region") or "").strip().lower()
        if not region:
            continue
        metric_code = str(getattr(row, "metric_code", "general") or "general")
        year_month = str(getattr(row, "year_month"))
        category_slug = getattr(row, "category_slug", None)
        if pd.isna(category_slug):
            category_slug = None
        elif category_slug is not None:
            category_slug = str(category_slug)

        existing = (
            session.query(OfficialCPIMonthly)
            .filter_by(
                source=source_code,
                region=region,
                metric_code=metric_code,
                year_month=year_month,
            )
            .first()
        )

        index_value = getattr(row, "index_value", None)
        index_num = float(index_value) if pd.notna(index_value) else None
        if index_num is None:
            continue

        payload = {
            "category_slug": category_slug,
            "index_value": index_num,
            "mom_change": float(getattr(row, "mom_change")) if pd.notna(getattr(row, "mom_change", None)) else None,
            "yoy_change": float(getattr(row, "yoy_change")) if pd.notna(getattr(row, "yoy_change", None)) else None,
            "status": str(getattr(row, "status", status) or status),
            "is_fallback": bool(is_fallback),
            "raw_snapshot_path": snapshot_path,
            "updated_at": now_utc(),
        }

        if existing:
            for key, value in payload.items():
                setattr(existing, key, value)
        else:
            rec = OfficialCPIMonthly(
                source=source_code,
                region=region,
                metric_code=metric_code,
                year_month=year_month,
                created_at=now_utc(),
                **payload,
            )
            session.add(rec)
        upserted += 1

    session.commit()
    return upserted


def _reconcile_xls_vs_pdf(
    xls_df: pd.DataFrame,
    pdf_df: pd.DataFrame,
    max_abs_diff_pp: float = 0.10,
) -> Dict[str, Any]:
    if xls_df.empty or pdf_df.empty:
        return {
            "status": "not_available",
            "checked_month": None,
            "compared_rows": 0,
            "max_abs_diff_pp": None,
            "mismatches_over_tolerance": 0,
        }

    x = xls_df.copy()
    p = pdf_df.copy()
    x = x[["region", "year_month", "metric_code", "mom_change"]].rename(columns={"mom_change": "mom_xls"})
    p = p[["region", "year_month", "metric_code", "mom_change"]].rename(columns={"mom_change": "mom_pdf"})
    merged = x.merge(p, on=["region", "year_month", "metric_code"], how="inner")
    merged = merged.dropna(subset=["mom_xls", "mom_pdf"])
    if merged.empty:
        return {
            "status": "not_available",
            "checked_month": None,
            "compared_rows": 0,
            "max_abs_diff_pp": None,
            "mismatches_over_tolerance": 0,
        }

    latest_month = str(merged["year_month"].max())
    latest = merged[merged["year_month"] == latest_month].copy()
    latest["abs_diff"] = (latest["mom_xls"] - latest["mom_pdf"]).abs()
    max_abs = float(latest["abs_diff"].max()) if not latest.empty else None
    mismatches = int((latest["abs_diff"] > max_abs_diff_pp).sum()) if not latest.empty else 0
    status = "ok" if mismatches == 0 else "warning"
    return {
        "status": status,
        "checked_month": latest_month,
        "compared_rows": int(len(latest)),
        "max_abs_diff_pp": max_abs,
        "mismatches_over_tolerance": mismatches,
    }


def _resolve_regions(ipc_cfg: Dict[str, Any], region: Optional[str]) -> List[str]:
    region_default = INDECPatagoniaProvider._normalize_region(str(ipc_cfg.get("region_default", "patagonia")))
    scope = ipc_cfg.get("region_scope")
    if isinstance(scope, list) and scope:
        scope_regions = [
            INDECPatagoniaProvider._normalize_region(str(r))
            for r in scope
            if str(r).strip()
        ]
    else:
        scope_regions = [region_default]
    scope_regions = [r for r in scope_regions if r not in {"", "all"}]
    if not scope_regions:
        scope_regions = ["patagonia", "nacional"]

    requested = INDECPatagoniaProvider._normalize_region(region or "all")
    if requested in {"all", "", "todas", "*"}:
        return sorted(set(scope_regions))
    return [requested]


def _hydrate_pdf_with_index(
    session: Session,
    source_code: str,
    pdf_df: pd.DataFrame,
    warnings: List[str],
) -> pd.DataFrame:
    if pdf_df.empty:
        return pdf_df
    out = pdf_df.copy()
    out["index_value"] = pd.NA
    out["yoy_change"] = pd.NA
    out["status"] = "final"

    for idx, row in out.sort_values(["region", "metric_code", "year_month"]).iterrows():
        region = str(row["region"])
        metric = str(row["metric_code"])
        month = str(row["year_month"])
        mom = row.get("mom_change")
        prev = (
            session.query(OfficialCPIMonthly)
            .filter(OfficialCPIMonthly.source == source_code)
            .filter(OfficialCPIMonthly.region == region)
            .filter(OfficialCPIMonthly.metric_code == metric)
            .filter(OfficialCPIMonthly.year_month < month)
            .order_by(OfficialCPIMonthly.year_month.desc())
            .first()
        )
        prev_index = float(prev.index_value) if prev and prev.index_value is not None else None
        if prev_index is not None and pd.notna(mom):
            derived = prev_index * (1.0 + (float(mom) / 100.0))
        elif prev_index is not None:
            derived = prev_index
        elif pd.notna(mom):
            derived = 100.0 * (1.0 + (float(mom) / 100.0))
        else:
            derived = 100.0
        out.at[idx, "index_value"] = derived

    warnings.append("Modo PDF fallback: index_value derivado desde historial previo y/o base 100.")
    return out


def sync_official_cpi(
    config: Dict[str, Any],
    session: Session,
    from_month: Optional[str] = None,
    to_month: Optional[str] = None,
    region: Optional[str] = None,
) -> OfficialSyncResult:
    """Sync official monthly CPI data into DB with hybrid xls/pdf/fallback strategy."""
    ipc_cfg = config.get("analysis", {}).get("ipc_official", {})
    source_mode_raw = str(ipc_cfg.get("source_mode", "xls_pdf_hybrid")).strip().lower()
    source_mode = source_mode_raw
    if source_mode_raw in {"auto", "auto_with_fallback"}:
        source_mode = "xls_pdf_hybrid"
    source_code = str(ipc_cfg.get("source_code", "indec_patagonia"))
    requested_region = str(region or "all")
    target_regions = _resolve_regions(ipc_cfg, region=requested_region)
    fallback_file = str(ipc_cfg.get("fallback_file", "data/cpi/ipc_indec_patagonia.csv"))
    validation_cfg = ipc_cfg.get("validation", {}) if isinstance(ipc_cfg.get("validation"), dict) else {}
    max_abs_diff_pp = float(validation_cfg.get("max_abs_diff_pp", 0.10))

    provider = INDECPatagoniaProvider(config)
    warnings: List[str] = []
    source_assets: Dict[str, str] = {}
    snapshot_paths: List[str] = []
    used_fallback = False
    validation_status = "not_run"
    official_source = "fallback_csv"
    source_document_url: Optional[str] = None
    df = pd.DataFrame()
    xls_df = pd.DataFrame()
    pdf_df = pd.DataFrame()

    if source_mode in {"xls_pdf_hybrid", "pdf", "xls"}:
        assets: Dict[str, str] = {}
        try:
            assets = provider.discover_assets()
            source_assets.update(assets)
            if assets.get("html_snapshot_path"):
                snapshot_paths.append(str(assets["html_snapshot_path"]))
        except Exception as exc:
            warnings.append(f"Discovery INDEC fallo: {exc}")
            logger.warning("INDEC discovery failed: {}", exc)

        xls_url = assets.get("xls_url")
        pdf_url = assets.get("pdf_url")

        if xls_url:
            try:
                xls_resp = requests.get(xls_url, timeout=40)
                xls_resp.raise_for_status()
                raw_xls_path = provider._persist_raw_blob(xls_resp.content, suffix="xls", prefix="indec_xls")  # noqa: SLF001
                snapshot_paths.append(raw_xls_path)
                source_assets["xls_raw_snapshot_path"] = raw_xls_path
                xls_df = provider.parse_xls_bytes(xls_resp.content)
                if not xls_df.empty:
                    official_source = "indec_nivel4_xls"
                    source_document_url = xls_url
                logger.info("Official IPC xls rows loaded: {}", len(xls_df))
            except Exception as exc:
                warnings.append(f"XLS oficial fallo: {exc}")
                logger.warning("Official IPC XLS parse failed: {}", exc)
        else:
            warnings.append("No se encontro URL XLS en discovery INDEC.")

        if pdf_url:
            try:
                pdf_resp = requests.get(pdf_url, timeout=40)
                pdf_resp.raise_for_status()
                raw_pdf_path = provider._persist_raw_blob(pdf_resp.content, suffix="pdf", prefix="indec_pdf")  # noqa: SLF001
                snapshot_paths.append(raw_pdf_path)
                source_assets["pdf_raw_snapshot_path"] = raw_pdf_path
                pdf_df = provider.parse_pdf_bytes(pdf_resp.content)
                source_assets["pdf_url"] = pdf_url
                logger.info("Official IPC pdf rows loaded: {}", len(pdf_df))
            except Exception as exc:
                warnings.append(f"PDF oficial fallo: {exc}")
                logger.warning("Official IPC PDF parse failed: {}", exc)
        else:
            warnings.append("No se encontro URL PDF en discovery INDEC.")

        if not xls_df.empty and not pdf_df.empty:
            validation = _reconcile_xls_vs_pdf(xls_df, pdf_df, max_abs_diff_pp=max_abs_diff_pp)
            validation_status = str(validation.get("status", "not_available"))
            if validation_status == "warning":
                warnings.append(
                    "Validacion XLS vs PDF fuera de tolerancia: "
                    f"mes {validation.get('checked_month')} | "
                    f"max_abs_diff_pp={validation.get('max_abs_diff_pp')} | "
                    f"mismatches={validation.get('mismatches_over_tolerance')}"
                )
            elif validation_status == "ok":
                warnings.append(
                    "Validacion XLS vs PDF OK: "
                    f"mes {validation.get('checked_month')} | "
                    f"max_abs_diff_pp={validation.get('max_abs_diff_pp')}"
                )
        elif not pdf_df.empty:
            validation_status = "not_available"
        else:
            validation_status = "failed"

        if not xls_df.empty:
            df = xls_df
        elif not pdf_df.empty:
            used_fallback = True
            official_source = "indec_nivel4_pdf"
            source_document_url = pdf_url
            df = _hydrate_pdf_with_index(session, source_code=source_code, pdf_df=pdf_df, warnings=warnings)

    if df.empty and source_mode in {"fallback", "xls_pdf_hybrid", "auto_with_fallback", "auto"}:
        try:
            fallback_path = Path(fallback_file)
            if not fallback_path.exists():
                raise FileNotFoundError(f"Fallback file no encontrado: {fallback_file}")
            raw_df = pd.read_csv(fallback_path)
            default_region = str(ipc_cfg.get("region_default", "patagonia"))
            df = provider._as_normalized_df(raw_df, default_region=default_region)  # noqa: SLF001
            used_fallback = True
            official_source = "fallback_csv"
            source_document_url = str(fallback_path)
            logger.info("Official IPC fallback rows loaded: {}", len(df))
        except Exception as exc:
            warnings.append(f"Fallback CSV fallo: {exc}")
            logger.warning("Official IPC fallback failed: {}", exc)
            if source_mode == "fallback":
                raise

    if df.empty and source_mode_raw in {"auto", "auto_with_fallback"}:
        try:
            default_region = str(ipc_cfg.get("region_default", "patagonia"))
            df = provider.fetch_auto_source(from_month=from_month, to_month=to_month, region=default_region)
            if not df.empty:
                official_source = "auto_source"
                source_document_url = str(
                    (ipc_cfg.get("auto_source") or {}).get("url")
                    if isinstance(ipc_cfg.get("auto_source"), dict)
                    else ""
                )
            logger.info("Official IPC legacy auto-source rows loaded: {}", len(df))
        except Exception as exc:
            warnings.append(f"Auto-source legado fallo: {exc}")
            logger.warning("Official IPC legacy auto-source failed: {}", exc)

    if df.empty:
        warnings.append("No se pudieron cargar filas oficiales desde XLS/PDF ni fallback.")
        return OfficialSyncResult(
            source_mode=source_mode_raw,
            source=source_code,
            official_source=official_source,
            region=requested_region,
            regions=target_regions,
            used_fallback=True,
            fetched_rows=0,
            upserted_rows=0,
            from_month=from_month,
            to_month=to_month,
            snapshot_path=None,
            snapshot_paths=snapshot_paths,
            source_document_url=source_document_url,
            source_assets=source_assets,
            validation_status=validation_status,
            warnings=warnings,
        )

    if "region" not in df.columns:
        df["region"] = str(ipc_cfg.get("region_default", "patagonia"))
    df["region"] = df["region"].astype(str).map(INDECPatagoniaProvider._normalize_region)
    df = df[df["region"].isin(set(target_regions))]
    if from_month:
        df = df[df["year_month"] >= from_month]
    if to_month:
        df = df[df["year_month"] <= to_month]
    df = df.sort_values(["region", "year_month", "metric_code"]).reset_index(drop=True)

    continuity_warnings = _validate_continuity(df)
    warnings.extend(continuity_warnings)
    final_snapshot = _persist_snapshot(df, official_source)
    if final_snapshot:
        snapshot_paths.append(final_snapshot)
    upserted = _upsert_official_rows(
        session=session,
        df=df,
        source_code=source_code,
        status="final",
        is_fallback=used_fallback,
        snapshot_path=final_snapshot,
    )

    return OfficialSyncResult(
        source_mode=source_mode_raw,
        source=source_code,
        official_source=official_source,
        region=requested_region,
        regions=target_regions,
        used_fallback=used_fallback,
        fetched_rows=int(len(df)),
        upserted_rows=int(upserted),
        from_month=from_month,
        to_month=to_month,
        snapshot_path=final_snapshot,
        snapshot_paths=snapshot_paths,
        source_document_url=source_document_url,
        source_assets=source_assets,
        validation_status=validation_status,
        warnings=warnings,
    )


def run_ipc_sync(
    config_path: Optional[str] = None,
    from_month: Optional[str] = None,
    to_month: Optional[str] = None,
    region: Optional[str] = None,
) -> Dict[str, Any]:
    """CLI helper for official IPC sync."""
    config = load_config(config_path)
    engine = get_engine(config)
    init_db(engine)
    session_factory = get_session_factory(engine)
    session = session_factory()
    try:
        result = sync_official_cpi(
            config=config,
            session=session,
            from_month=from_month,
            to_month=to_month,
            region=region,
        )
        return {
            "status": "completed",
            "source_mode": result.source_mode,
            "source": result.source,
            "official_source": result.official_source,
            "region": result.region,
            "regions": result.regions,
            "used_fallback": result.used_fallback,
            "fetched_rows": result.fetched_rows,
            "upserted_rows": result.upserted_rows,
            "from_month": result.from_month,
            "to_month": result.to_month,
            "snapshot_path": result.snapshot_path,
            "snapshot_paths": result.snapshot_paths,
            "source_document_url": result.source_document_url,
            "source_assets": result.source_assets,
            "validation_status": result.validation_status,
            "warnings": result.warnings,
        }
    finally:
        session.close()
