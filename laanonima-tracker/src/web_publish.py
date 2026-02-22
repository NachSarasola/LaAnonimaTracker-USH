"""Static web publication pipeline for low-cost public deployment."""

from __future__ import annotations

import json
import re
import shutil
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

from src.config_loader import load_config
from src.reporting import run_report
from src.web_styles import (
    get_shell_css_bundle,
    get_shell_css_version,
    get_tracker_css_bundle,
    get_tracker_css_version,
)

_REPORT_METADATA_RE = re.compile(r"report_interactive_(\d{6})_to_(\d{6})_(\d{8}_\d{6})\.metadata\.json$")
_MONTH_RE = re.compile(r"^\d{4}-\d{2}$")
_TRACKER_CSS_REF_RE = re.compile(r"""href=["'](?:\./)?tracker-ui\.css(?:\?[^"'<>]*)?["']""", re.IGNORECASE)
_HEAD_CLOSE_RE = re.compile(r"</head>", re.IGNORECASE)
_PUBLICATION_POLICY = "publish_with_alert_on_partial"
_PUBLICATION_POLICY_SUMMARY = "Se publica con alerta si falta cobertura o IPC."
_SITE_TITLE = "Tracker de precios: La Anónima Ushuaia"
_SITE_SHORT = "Precios Ushuaia"


@dataclass
class ReportEntry:
    month: str
    html_path: Path
    metadata_path: Path
    metadata: Dict[str, Any]
    generated_at: datetime


@dataclass
class PublishWebResult:
    status: str
    web_status: str
    is_stale: bool
    output_dir: str
    tracker_path: str
    manifest_path: str
    latest_metadata_path: str
    history_count: int
    source_report_html: str
    source_report_metadata: str
    next_update_eta: str


class StaticWebPublisher:
    """Build public static website artifacts from interactive reports."""

    @staticmethod
    def _is_placeholder_adsense_client(client_id: str) -> bool:
        value = str(client_id or "").strip().lower()
        if not value:
            return True
        return "xxxxxxxx" in value or value in {"ca-pub-test", "ca-pub-0000000000000000"}

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        deployment = config.get("deployment", {}) if isinstance(config.get("deployment"), dict) else {}
        self.output_dir = Path(str(deployment.get("output_dir", "public")))
        self.public_base_url = str(deployment.get("public_base_url", "https://preciosushuaia.com")).rstrip("/")
        self.contact_email = str(deployment.get("contact_email", "hola@preciosushuaia.com")).strip()
        if not self.public_base_url:
            self.public_base_url = "https://preciosushuaia.com"
        if not self.contact_email:
            self.contact_email = "hola@preciosushuaia.com"
        self.keep_history_months = max(1, int(deployment.get("keep_history_months", 24)))
        self.keep_history_runs = max(1, int(deployment.get("keep_history_runs", 180)))
        self.history_timezone = str(deployment.get("history_timezone", "America/Argentina/Ushuaia")).strip() or "America/Argentina/Ushuaia"
        try:
            self._history_tz = ZoneInfo(self.history_timezone)
        except Exception:
            self._history_tz = timezone.utc
            self.history_timezone = "UTC"
        self.fresh_max_hours = float(deployment.get("fresh_max_hours", 36))
        self.schedule_utc = str(deployment.get("schedule_utc", "09:10"))
        analysis_cfg = config.get("analysis", {}) if isinstance(config.get("analysis"), dict) else {}
        self.report_dir = Path(str(analysis_cfg.get("reports_dir", "data/analysis/reports")))

        ads_cfg = config.get("ads", {}) if isinstance(config.get("ads"), dict) else {}
        self.ads_enabled = bool(ads_cfg.get("enabled", False))
        self.ads_provider = str(ads_cfg.get("provider", "adsense"))
        self.ads_slots = [str(v) for v in (ads_cfg.get("slots") or ["header", "inline", "sidebar", "footer"]) if str(v).strip()]
        self.ads_client_id = str(
            ads_cfg.get("client_id")
            or ads_cfg.get("client_id_placeholder")
            or "ca-pub-xxxxxxxxxxxxxxxx"
        )
        if self.ads_provider.lower() == "adsense" and self._is_placeholder_adsense_client(self.ads_client_id):
            self.ads_enabled = False

        analytics_cfg = config.get("analytics", {}) if isinstance(config.get("analytics"), dict) else {}
        plausible_cfg = analytics_cfg.get("plausible", {}) if isinstance(analytics_cfg.get("plausible"), dict) else {}
        self.analytics_enabled = bool(plausible_cfg.get("enabled", False))
        self.analytics_domain = str(plausible_cfg.get("domain", "")).strip()
        self.analytics_script_url = str(
            plausible_cfg.get("script_url", "https://plausible.io/js/script.js")
        ).strip()

        premium_cfg = config.get("premium_placeholders", {}) if isinstance(config.get("premium_placeholders"), dict) else {}
        self.premium_enabled = bool(premium_cfg.get("enabled", True))
        self.premium_features = [
            str(v)
            for v in (premium_cfg.get("features") or [
                "Alertas de precio personalizadas",
                "Descarga avanzada CSV/API",
                "Comparador multi-zona",
                "Panel Pro sin anuncios",
            ])
            if str(v).strip()
        ]

    @staticmethod
    def _consent_script() -> str:
        return (
            "<script>"
            "(function(){"
            "var k='laanonima_tracker_cookie_consent_v1';"
            "var b=document.getElementById('cookie-banner');"
            "if(!b)return;"
            "var v=localStorage.getItem(k);"
            "if(v==='accepted'||v==='rejected'){b.style.display='none';return;}"
            "var ok=document.getElementById('cookie-accept');"
            "var no=document.getElementById('cookie-reject');"
            "if(ok){ok.addEventListener('click',function(){localStorage.setItem(k,'accepted');b.style.display='none';"
            "if(typeof window.__laTrackerOnConsentChanged==='function'){window.__laTrackerOnConsentChanged('accepted');}"
            "});}"
            "if(no){no.addEventListener('click',function(){localStorage.setItem(k,'rejected');b.style.display='none';"
            "if(typeof window.__laTrackerOnConsentChanged==='function'){window.__laTrackerOnConsentChanged('rejected');}"
            "});}"
            "})();"
            "</script>"
        )

    @staticmethod
    def _consent_banner_html() -> str:
        return (
            "<div id='cookie-banner' class='cookie-banner' role='dialog' aria-live='polite'>"
            "<div class='cookie-grid'>"
            "<div class='cookie-text'>Usamos cookies para preferencias basicas y, cuando corresponda, medicion publicitaria.</div>"
            "<div class='cookie-actions'>"
            "<button id='cookie-reject' type='button'>Rechazar</button>"
            "<button id='cookie-accept' type='button' class='primary'>Aceptar</button>"
            "</div>"
            "</div>"
            "</div>"
        )

    @staticmethod
    def _read_json(path: Path) -> Dict[str, Any]:
        return json.loads(path.read_text(encoding="utf-8"))

    def _analytics_head_script(self) -> str:
        if not self.analytics_enabled or not self.analytics_domain:
            return ""
        script_url = self.analytics_script_url or "https://plausible.io/js/script.js"
        return (
            f"<script defer data-domain='{self.analytics_domain}' "
            f"src='{script_url}'></script>"
        )

    def _adsense_head_script(self) -> str:
        if not self.ads_enabled or self.ads_provider.lower() != "adsense" or not self.ads_client_id:
            return ""
        if self._is_placeholder_adsense_client(self.ads_client_id):
            return ""
        return (
            f"<script async src=\"https://pagead2.googlesyndication.com/pagead/js/adsbygoogle.js?client={self.ads_client_id}\" "
            f"crossorigin=\"anonymous\"></script>"
        )

    @staticmethod
    def _parse_generated_at(value: Any) -> datetime:
        if value is None:
            return datetime.fromtimestamp(0, tz=timezone.utc)
        txt = str(value).strip()
        if not txt:
            return datetime.fromtimestamp(0, tz=timezone.utc)
        for fmt in ("%Y-%m-%d %H:%M:%S UTC", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S"):
            try:
                dt = datetime.strptime(txt, fmt)
                if dt.tzinfo is None:
                    return dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc)
            except Exception:
                continue
        try:
            dt_iso = datetime.fromisoformat(txt.replace("Z", "+00:00"))
            if dt_iso.tzinfo is None:
                return dt_iso.replace(tzinfo=timezone.utc)
            return dt_iso.astimezone(timezone.utc)
        except Exception:
            return datetime.fromtimestamp(0, tz=timezone.utc)

    @staticmethod
    def _month_from_metadata(meta: Dict[str, Any], path: Path) -> str:
        range_block = meta.get("range") if isinstance(meta.get("range"), dict) else {}
        month = str(range_block.get("to") or "").strip()
        if re.match(r"^\d{4}-\d{2}$", month):
            return month

        match = _REPORT_METADATA_RE.search(path.name)
        if match:
            to_compact = match.group(2)
            return f"{to_compact[:4]}-{to_compact[4:6]}"

        generated = StaticWebPublisher._parse_generated_at(meta.get("generated_at"))
        return generated.strftime("%Y-%m")

    @staticmethod
    def _range_from_metadata(meta: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
        range_block = meta.get("range") if isinstance(meta.get("range"), dict) else {}
        from_month = str(range_block.get("from") or "").strip()
        to_month = str(range_block.get("to") or "").strip()
        if not _MONTH_RE.match(from_month):
            from_month = None
        if not _MONTH_RE.match(to_month):
            to_month = None
        return from_month, to_month

    def _iter_report_entries(self) -> List[ReportEntry]:
        if not self.report_dir.exists():
            return []

        entries: List[ReportEntry] = []
        metadata_files = sorted(self.report_dir.glob("report_interactive_*.metadata.json"), key=lambda p: p.stat().st_mtime, reverse=True)
        for metadata_path in metadata_files:
            try:
                meta = self._read_json(metadata_path)
            except Exception:
                continue

            artifacts = meta.get("artifacts") if isinstance(meta.get("artifacts"), dict) else {}
            html_hint = str(artifacts.get("html") or "").strip()
            if html_hint:
                html_path = Path(html_hint)
                if not html_path.is_absolute():
                    html_path = Path.cwd() / html_path
            else:
                html_path = metadata_path.with_suffix("").with_suffix(".html")

            if not html_path.exists():
                sibling = metadata_path.with_name(metadata_path.name.replace(".metadata.json", ".html"))
                if sibling.exists():
                    html_path = sibling

            if not html_path.exists():
                continue

            month = self._month_from_metadata(meta, metadata_path)
            generated_at = self._parse_generated_at(meta.get("generated_at"))
            entries.append(
                ReportEntry(
                    month=month,
                    html_path=html_path,
                    metadata_path=metadata_path,
                    metadata=meta,
                    generated_at=generated_at,
                )
            )

        entries.sort(key=lambda item: item.generated_at, reverse=True)
        return entries

    def collect_latest_report(
        self,
        preferred_html: Optional[str] = None,
        preferred_metadata: Optional[str] = None,
        preferred_from_month: Optional[str] = None,
        preferred_to_month: Optional[str] = None,
    ) -> ReportEntry:
        if preferred_html and preferred_metadata:
            html_path = Path(preferred_html)
            metadata_path = Path(preferred_metadata)
            if html_path.exists() and metadata_path.exists():
                meta = self._read_json(metadata_path)
                return ReportEntry(
                    month=self._month_from_metadata(meta, metadata_path),
                    html_path=html_path,
                    metadata_path=metadata_path,
                    metadata=meta,
                    generated_at=self._parse_generated_at(meta.get("generated_at")),
                )

        entries = self._iter_report_entries()
        if not entries:
            raise FileNotFoundError("No se encontro ningun reporte interactivo en data/analysis/reports")

        if bool(preferred_from_month) ^ bool(preferred_to_month):
            raise ValueError("Si se define rango preferido, deben enviarse ambos meses from/to.")

        if preferred_from_month and preferred_to_month:
            if not _MONTH_RE.match(preferred_from_month) or not _MONTH_RE.match(preferred_to_month):
                raise ValueError("Los meses preferidos deben usar formato YYYY-MM.")
            matched = []
            for entry in entries:
                from_month, to_month = self._range_from_metadata(entry.metadata)
                if from_month == preferred_from_month and to_month == preferred_to_month:
                    matched.append(entry)
            if matched:
                return matched[0]
            raise FileNotFoundError(
                "No se encontro reporte interactivo para rango "
                f"{preferred_from_month} -> {preferred_to_month} en {self.report_dir}."
            )

        with_data = [entry for entry in entries if bool(entry.metadata.get("has_data", False))]
        if with_data:
            return with_data[0]
        return entries[0]

    def _collect_history(self) -> List[ReportEntry]:
        entries = self._iter_report_entries()
        return entries[: self.keep_history_runs]

    def _to_history_tz(self, value: datetime) -> datetime:
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(self._history_tz)

    @staticmethod
    def _metadata_run_stamp(path: Path) -> Optional[str]:
        match = _REPORT_METADATA_RE.search(path.name)
        if not match:
            return None
        return match.group(3)

    def _history_slug_for_entry(self, entry: ReportEntry, seen: Dict[Tuple[str, str], int]) -> str:
        local_stamp = self._to_history_tz(entry.generated_at).strftime("%Y-%m-%d_%H%M%S")
        key = (entry.month, local_stamp)
        seen[key] = seen.get(key, 0) + 1
        count = seen[key]
        if count == 1:
            return local_stamp
        return f"{local_stamp}_{count}"

    def _next_update_eta(self, now: Optional[datetime] = None) -> datetime:
        now_utc = now.astimezone(timezone.utc) if now else datetime.now(timezone.utc)
        schedule = self.schedule_utc.strip()
        hour = 9
        minute = 10
        if re.match(r"^\d{2}:\d{2}$", schedule):
            hour = int(schedule.split(":", 1)[0])
            minute = int(schedule.split(":", 1)[1])

        candidate = now_utc.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if candidate <= now_utc:
            candidate = candidate + timedelta(days=1)
        return candidate

    def _status_from_metadata(self, latest: ReportEntry) -> tuple[str, bool]:
        now_utc = datetime.now(timezone.utc)
        age_hours = (now_utc - latest.generated_at).total_seconds() / 3600.0
        is_stale = age_hours >= self.fresh_max_hours

        quality_flags = (
            latest.metadata.get("data_quality", {}).get("quality_flags", {})
            if isinstance(latest.metadata.get("data_quality"), dict)
            else {}
        )
        is_partial = bool(quality_flags.get("is_partial", False))
        if is_partial:
            return "partial", is_stale
        if is_stale:
            return "stale", is_stale
        return "fresh", is_stale

    @staticmethod
    def _ensure_dir(path: Path) -> None:
        path.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _normalize_tracker_css_ref(html: str) -> str:
        return _TRACKER_CSS_REF_RE.sub(
            f'href="./tracker-ui.css?v={get_tracker_css_version()}"',
            html,
        )

    @staticmethod
    def _ensure_tracker_css_link(html: str) -> str:
        """Ensure copied tracker pages always load modern external tracker CSS."""
        normalized = StaticWebPublisher._normalize_tracker_css_ref(html)
        if _TRACKER_CSS_REF_RE.search(normalized):
            return normalized
        link = f'<link rel="stylesheet" href="./tracker-ui.css?v={get_tracker_css_version()}"/>'
        if _HEAD_CLOSE_RE.search(normalized):
            return _HEAD_CLOSE_RE.sub(f"{link}</head>", normalized, count=1)
        return f"{link}\n{normalized}"

    def _meta_head(self, title: str, description: str, path: str) -> str:
        canonical_path = str(path or "/").strip()
        if not canonical_path.startswith("/"):
            canonical_path = "/" + canonical_path
        canonical_url = f"{self.public_base_url}{canonical_path}"
        og_image = f"{self.public_base_url}/assets/og-card.svg"
        return (
            "<meta charset='utf-8'/>"
            "<meta name='viewport' content='width=device-width,initial-scale=1'/>"
            f"<title>{title}</title>"
            f"<meta name='description' content='{description}'/>"
            "<meta name='theme-color' content='#1d4ed8'/>"
            f"<link rel='canonical' href='{canonical_url}'/>"
            "<link rel='icon' type='image/svg+xml' href='/favicon.svg'/>"
            "<link rel='manifest' href='/site.webmanifest'/>"
            f"<link rel='stylesheet' href='/assets/css/shell-ui.css?v={get_shell_css_version()}'/>"
            "<link rel='preconnect' href='https://fonts.googleapis.com'/>"
            "<link rel='preconnect' href='https://fonts.gstatic.com' crossorigin/>"
            "<link href='https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap' rel='stylesheet'/>"
            "<script async src='https://pagead2.googlesyndication.com/pagead/js/adsbygoogle.js?client=ca-pub-7036697873963446' crossorigin='anonymous'></script>"
            "<meta property='og:type' content='website'/>"
            "<meta property='og:locale' content='es_AR'/>"
            f"<meta property='og:title' content='{title}'/>"
            f"<meta property='og:description' content='{description}'/>"
            f"<meta property='og:url' content='{canonical_url}'/>"
            f"<meta property='og:image' content='{og_image}'/>"
            "<meta name='twitter:card' content='summary_large_image'/>"
            f"<meta name='twitter:title' content='{title}'/>"
            f"<meta name='twitter:description' content='{description}'/>"
            f"<meta name='twitter:image' content='{og_image}'/>"
        )

    def _write_brand_assets(self) -> None:
        assets_dir = self.output_dir / "assets"
        css_dir = assets_dir / "css"
        self._ensure_dir(assets_dir)
        self._ensure_dir(css_dir)
        favicon_svg = """<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 64 64'>
<defs>
  <linearGradient id='g' x1='0' y1='0' x2='1' y2='1'>
    <stop offset='0%' stop-color='#1d4ed8'/>
    <stop offset='100%' stop-color='#1e3a8a'/>
  </linearGradient>
</defs>
<rect width='64' height='64' rx='14' fill='url(#g)'/>
<path d='M15 45V19h8l9 13 9-13h8v26h-8V31l-9 12-9-12v14z' fill='#fff'/>
</svg>
"""
        og_card_svg = """<svg xmlns='http://www.w3.org/2000/svg' width='1200' height='630' viewBox='0 0 1200 630'>
<defs>
  <linearGradient id='bg' x1='0' y1='0' x2='1' y2='1'>
    <stop offset='0%' stop-color='#1d4ed8'/>
    <stop offset='100%' stop-color='#1e3a8a'/>
  </linearGradient>
</defs>
<rect width='1200' height='630' fill='#edf3fb'/>
<rect x='42' y='42' width='1116' height='546' rx='26' fill='url(#bg)'/>
<circle cx='1040' cy='130' r='130' fill='rgba(255,255,255,0.14)'/>
<circle cx='970' cy='520' r='190' fill='rgba(255,255,255,0.10)'/>
<text x='90' y='228' fill='#ffffff' font-size='60' font-family='Segoe UI, Arial, sans-serif' font-weight='700'>Tracker de precios: La Anonima Ushuaia</text>
<text x='90' y='298' fill='#dff4ff' font-size='34' font-family='Segoe UI, Arial, sans-serif'>Precios en La Anonima Ushuaia</text>
<text x='90' y='520' fill='#e8fbff' font-size='28' font-family='Segoe UI, Arial, sans-serif'>Series historicas simples y claras</text>
</svg>
"""
        web_manifest = {
            "name": _SITE_TITLE,
            "short_name": _SITE_SHORT,
            "start_url": "/",
            "display": "standalone",
            "background_color": "#f9fafb",
            "theme_color": "#1d4ed8",
            "icons": [
                {
                    "src": "/favicon.svg",
                    "sizes": "any",
                    "type": "image/svg+xml",
                    "purpose": "any",
                }
            ],
        }
        (self.output_dir / "favicon.svg").write_text(favicon_svg, encoding="utf-8")
        (assets_dir / "og-card.svg").write_text(og_card_svg, encoding="utf-8")
        (css_dir / "shell-ui.css").write_text(get_shell_css_bundle(), encoding="utf-8")
        (self.output_dir / "site.webmanifest").write_text(
            json.dumps(web_manifest, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    @staticmethod
    def _top_nav(active: str = "home") -> str:
        links = [
            ("home", "/", "Inicio"),
            ("tracker", "/tracker/", "Tracker"),
            ("historico", "/historico/", "Histórico"),
            ("metodologia", "/metodologia/", "Metodología"),
            ("contacto", "/contacto/", "Contacto"),
        ]
        nav_items = []
        for key, href, label in links:
            class_attr = " class='active'" if key == active else ""
            nav_items.append(f"<a href='{href}'{class_attr}>{label}</a>")
        return (
            "<section class='card topbar'>"
            "<a href='/' class='brand'>Precios La Anónima Ushuaia</a>"
            f"<nav class='nav' aria-label='Principal'>{''.join(nav_items)}</nav>"
            "</section>"
        )

    def _copy_latest_artifacts(self, latest: ReportEntry) -> Dict[str, str]:
        tracker_dir = self.output_dir / "tracker"
        data_dir = self.output_dir / "data"
        history_data_dir = data_dir / "history"

        self._ensure_dir(tracker_dir)
        self._ensure_dir(data_dir)
        self._ensure_dir(history_data_dir)

        tracker_path = tracker_dir / "index.html"
        tracker_css_path = tracker_dir / "tracker-ui.css"
        latest_meta_path = data_dir / "latest.metadata.json"

        source_tracker_css = latest.html_path.parent / "tracker-ui.css"
        tracker_html = latest.html_path.read_text(encoding="utf-8")
        tracker_path.write_text(self._ensure_tracker_css_link(tracker_html), encoding="utf-8")
        if source_tracker_css.exists():
            shutil.copy2(source_tracker_css, tracker_css_path)
        else:
            tracker_css_path.write_text(get_tracker_css_bundle(), encoding="utf-8")
        shutil.copy2(latest.metadata_path, latest_meta_path)

        # Also copy per-product JSON files so build_product_detail_pages() can read them.
        source_products_dir = latest.html_path.parent / "products"
        dest_products_dir = tracker_dir / "products"
        if source_products_dir.exists():
            if dest_products_dir.exists():
                shutil.rmtree(dest_products_dir)
            shutil.copytree(source_products_dir, dest_products_dir)

        return {
            "tracker_path": str(tracker_path),
            "latest_metadata_path": str(latest_meta_path),
        }

    def build_history_index(self) -> List[Dict[str, Any]]:
        historico_root = self.output_dir / "historico"
        data_history_root = self.output_dir / "data" / "history"
        self._ensure_dir(historico_root)
        self._ensure_dir(data_history_root)

        run_rows: List[Dict[str, Any]] = []
        rows_by_month: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        seen_slugs: Dict[Tuple[str, str], int] = {}

        for entry in self._collect_history():
            month_dir = historico_root / entry.month
            self._ensure_dir(month_dir)

            run_slug = self._history_slug_for_entry(entry, seen_slugs)
            run_dir = month_dir / run_slug
            self._ensure_dir(run_dir)

            run_html = run_dir / "index.html"
            run_tracker_css = run_dir / "tracker-ui.css"
            source_tracker_css = entry.html_path.parent / "tracker-ui.css"
            run_html_text = entry.html_path.read_text(encoding="utf-8")
            run_html.write_text(self._ensure_tracker_css_link(run_html_text), encoding="utf-8")
            if source_tracker_css.exists():
                shutil.copy2(source_tracker_css, run_tracker_css)
            else:
                run_tracker_css.write_text(get_tracker_css_bundle(), encoding="utf-8")

            data_month_dir = data_history_root / entry.month
            self._ensure_dir(data_month_dir)
            run_meta = data_month_dir / f"{run_slug}.metadata.json"
            shutil.copy2(entry.metadata_path, run_meta)

            local_dt = self._to_history_tz(entry.generated_at)
            coverage = (
                entry.metadata.get("coverage", {}).get("coverage_total_pct")
                if isinstance(entry.metadata.get("coverage"), dict)
                else None
            )
            is_partial = bool(
                entry.metadata.get("data_quality", {}).get("quality_flags", {}).get("is_partial", False)
                if isinstance(entry.metadata.get("data_quality"), dict)
                else False
            )
            quality_badge = (
                entry.metadata.get("data_quality", {}).get("quality_flags", {}).get("badge")
                if isinstance(entry.metadata.get("data_quality"), dict)
                else None
            )
            run_row = {
                "month": entry.month,
                "run_key": f"{entry.month}/{run_slug}",
                "run_slug": run_slug,
                "run_date_local": local_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "generated_at_utc": entry.generated_at.strftime("%Y-%m-%d %H:%M:%S UTC"),
                "report_path": f"/historico/{entry.month}/{run_slug}/",
                "metadata_path": f"/data/history/{entry.month}/{run_slug}.metadata.json",
                "generated_at": entry.generated_at.strftime("%Y-%m-%d %H:%M:%S UTC"),
                "has_data": bool(entry.metadata.get("has_data", False)),
                "coverage_total_pct": coverage,
                "is_partial": is_partial,
                "quality_badge": quality_badge,
                "history_timezone": self.history_timezone,
            }
            run_rows.append(run_row)
            rows_by_month[entry.month].append(run_row)

        run_rows.sort(key=lambda item: item.get("generated_at_utc", ""), reverse=True)
        for month in rows_by_month:
            rows_by_month[month].sort(key=lambda item: item.get("generated_at_utc", ""), reverse=True)

        def _run_item_html(item: Dict[str, Any]) -> str:
            state = "partial" if item.get("is_partial") else "fresh"
            state_label = "Parcial" if item.get("is_partial") else "Completo"
            badge_classes = f"badge {state}"
            coverage = item.get("coverage_total_pct")
            coverage_label = "N/D" if coverage is None else f"{float(coverage):.1f}%"
            run_key = str(item.get("run_key") or "")
            row_title = f"{run_key} | Calidad: {str(item.get('quality_badge') or 'Sin badge')}".replace("'", "&#39;")
            return (
                "<a href='{path}' data-month='{month}' data-run='{run_key}' title='{row_title}' class='history-run-row'>"
                "<div class='run-date'><strong>{run_local}</strong></div>"
                "<div class='run-coverage'><span class='muted'>Cobertura:</span> {coverage_label}</div>"
                "<div class='run-status'><span class='{badge_classes}'>{state_label}</span></div>"
                "<div class='run-action'><span class='btn-text'>Ver Reporte &rarr;</span></div>"
                "</a>"
            ).format(
                path=item["report_path"],
                month=item["month"],
                run_key=run_key,
                row_title=row_title,
                run_local=item["run_date_local"],
                badge_classes=badge_classes,
                state_label=state_label,
                coverage_label=coverage_label,
            )

        month_sections: List[str] = []
        for idx, month in enumerate(sorted(rows_by_month.keys(), reverse=True)):
            items = rows_by_month[month]
            runs_html = "".join(_run_item_html(item) for item in items)
            month_sections.append(
                f"<section class='card history-month-card' data-month-panel='{month}' style='margin-bottom: 24px;'>"
                f"<div class='history-month-header' style='display: flex; align-items: center; justify-content: space-between; border-bottom: 1px solid var(--line); padding-bottom: 16px; margin-bottom: 16px;'>"
                f"  <h2 style='margin: 0; font-size: 1.25rem;'>{month}</h2>"
                f"  <span class='status-chip'>{len(items)} corridas</span>"
                f"</div>"
                f"<div class='history-list-modern'>{runs_html}</div>"
                f"<div style='margin-top: 16px; text-align: center;'><a href='/historico/{month}/' class='btn btn-secondary' style='font-size: 0.9rem;'>Ver mes completo</a></div>"
                "</section>"
            )

        list_html = "".join(month_sections) if month_sections else "<p class='muted'>Sin corridas disponibles.</p>"

        root_html = f"""<!doctype html>
<html lang='es'>
<head>
{self._meta_head(f'Histórico de precios | {_SITE_TITLE}', 'Elige una corrida para ver precios.', '/historico/')}
{self._analytics_head_script()}
</head>
<body>
<main class='shell'>
  {self._top_nav(active='historico')}
  <section class='card'>
    <h1>Histórico de precios</h1>
    <p class='muted'>Elige una corrida para ver precios.</p>
    <div class='list-tools'>
      <input id='history-search' type='search' placeholder='Buscar por mes o fecha'/>
      <div class='status-chip'><span id='history-count'>{len(run_rows)}</span> corridas</div>
    </div>
  </section>
  <div id='history-list'>{list_html}</div>
</main>
{self._consent_banner_html()}
{self._consent_script()}
<script>
(function(){{
  const input=document.getElementById('history-search');
  const container=document.getElementById('history-list');
  const count=document.getElementById('history-count');
  if(!input || !container || !count) return;
  const monthPanels=Array.from(container.querySelectorAll('details[data-month-panel]'));
  const apply=()=>{{
    const q=String(input.value||'').trim().toLowerCase();
    let visible=0;
    monthPanels.forEach((panel)=>{{
      const panelRows=Array.from(panel.querySelectorAll('a[data-run]'));
      let panelVisible=0;
      panelRows.forEach((row)=>{{
        const month=String(row.getAttribute('data-month')||'').toLowerCase();
        const run=String(row.getAttribute('data-run')||'').toLowerCase();
        const text=String(row.textContent||'').toLowerCase();
        const match=!q || month.includes(q) || run.includes(q) || text.includes(q);
        row.style.display=match?'grid':'none';
        if(match) panelVisible+=1;
      }});
      panel.style.display=panelVisible?'':'none';
      if(q){{
        panel.open=panelVisible>0;
      }}else{{
        panel.open=String(panel.getAttribute('data-default-open')||'0')==='1';
      }}
      visible+=panelVisible;
    }});
    count.textContent=String(visible);
  }};
  input.addEventListener('input', apply);
  apply();
}})();
</script>
</body>
</html>
"""
        (historico_root / "index.html").write_text(root_html, encoding="utf-8")

        for month, items in rows_by_month.items():
            month_page_dir = historico_root / month
            self._ensure_dir(month_page_dir)
            month_list = "".join(_run_item_html(item) for item in items)
            month_html = f"""<!doctype html>
<html lang='es'>
<head>
{self._meta_head(f'Histórico {month} | {_SITE_TITLE}', f'Corridas del mes {month}.', f'/historico/{month}/')}
{self._analytics_head_script()}
</head>
<body>
<main class='shell'>
  {self._top_nav(active='historico')}
  <section class='card'>
    <h1>Corridas de {month}</h1>
    <p class='muted'>Hora local: {self.history_timezone}</p>
    <div class='status-chip'>{len(items)} corridas</div>
  </section>
  <section class='card'>
    <div class='history-list'>{month_list}</div>
  </section>
</main>
{self._consent_banner_html()}
{self._consent_script()}
</body>
</html>
"""
            (month_page_dir / "index.html").write_text(month_html, encoding="utf-8")

        return run_rows

    def build_manifest(self, latest: ReportEntry, history_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        web_status, is_stale = self._status_from_metadata(latest)
        from_month, to_month = self._range_from_metadata(latest.metadata)

        coverage = latest.metadata.get("coverage", {}) if isinstance(latest.metadata.get("coverage"), dict) else {}
        publication = (
            latest.metadata.get("data_quality", {}).get("publication_status", {})
            if isinstance(latest.metadata.get("data_quality"), dict)
            else {}
        )
        official_validation_status = (
            publication.get("validation_status")
            or publication.get("metrics", {}).get("official_validation_status")
            or "not_available"
        )

        manifest = {
            "version": 2,
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
            "status": web_status,
            "latest_report_path": "/tracker/",
            "latest_metadata_path": "/data/latest.metadata.json",
            "latest": {
                "from_month": from_month,
                "to_month": to_month,
                "generated_at": str(latest.metadata.get("generated_at") or ""),
                "has_data": bool(latest.metadata.get("has_data", False)),
                "web_status": web_status,
            },
            "history": history_rows,
            "quality": {
                "coverage_total_pct": coverage.get("coverage_total_pct"),
                "publication_status": publication.get("status") if isinstance(publication, dict) else None,
                "official_validation_status": official_validation_status,
            },
            "publication_policy": _PUBLICATION_POLICY,
            "publication_policy_summary": _PUBLICATION_POLICY_SUMMARY,
            "next_update_eta": self._next_update_eta().strftime("%Y-%m-%d %H:%M:%S UTC"),
            "is_stale": bool(is_stale),
            "ads": {
                "enabled": self.ads_enabled,
                "provider": self.ads_provider,
                "slots": self.ads_slots,
            },
            "analytics": {
                "enabled": self.analytics_enabled,
                "domain": self.analytics_domain,
            },
            "premium_placeholders": {
                "enabled": self.premium_enabled,
                "features": self.premium_features,
            },
        }

        data_dir = self.output_dir / "data"
        self._ensure_dir(data_dir)
        manifest_path = data_dir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        return manifest

    @staticmethod
    def _format_kpi_label(value: Any, unit: str) -> str:
        if value is None:
            return "N/D"
        if isinstance(value, bool):
            return str(value)
        if isinstance(value, (int, float)):
            if unit == "productos":
                return f"{int(round(float(value)))} {unit}"
            suffix = f" {unit}" if unit else ""
            return f"{float(value):.2f}{suffix}"
        return str(value)

    def _kpi_cards_from_metadata(self, meta: Dict[str, Any]) -> str:
        kpis = meta.get("kpis") if isinstance(meta.get("kpis"), dict) else {}
        rows = [
            ("Inflacion canasta nominal", kpis.get("inflation_basket_nominal_pct"), "%"),
            ("IPC oficial periodo", kpis.get("ipc_period_pct"), "%"),
            ("Brecha canasta vs IPC", kpis.get("gap_vs_ipc_pp"), "pp"),
            ("Panel balanceado", kpis.get("balanced_panel_n"), "productos"),
        ]

        cards = []
        for title, value, unit in rows:
            label = self._format_kpi_label(value, unit)
            cards.append(f"<div class='kpi'><strong>{title}</strong><span>{label}</span></div>")
        return "".join(cards)

    def build_home_page(self, manifest: Dict[str, Any], latest: ReportEntry) -> None:
        latest_block = manifest.get("latest") if isinstance(manifest.get("latest"), dict) else {}
        from_month = str(latest_block.get("from_month") or "N/D")
        to_month = str(latest_block.get("to_month") or "N/D")
        latest_generated = str(latest_block.get("generated_at") or latest.metadata.get("generated_at") or "N/D")
        latest_status_raw = str(latest_block.get("web_status") or manifest.get("status") or "partial").lower()
        _STATUS_LABELS = {"fresh": "Actualizado", "partial": "En proceso", "stale": "Desactualizado"}
        latest_status = _STATUS_LABELS.get(latest_status_raw, "En proceso")
        quality_flags = (
            latest.metadata.get("data_quality", {}).get("quality_flags", {})
            if isinstance(latest.metadata.get("data_quality"), dict)
            else {}
        )
        quality_warnings = quality_flags.get("warnings", []) if isinstance(quality_flags, dict) else []
        warnings_html = ""
        if quality_warnings:
            items = "".join(f"<li>{str(item)}</li>" for item in quality_warnings)
            warnings_html = (
                "<section class='card'>"
                "<ul class='clean muted'>"
                f"{items}"
                "</ul>"
                "</section>"
            )

        premium_html = ""
        if self.premium_enabled and self.premium_features:
            premium_items = "".join(f"<li>{f}</li>" for f in self.premium_features)
            premium_html = (
                "<section class='card'><h2>Hoja de ruta</h2>"
                "<p class='muted'>Mejoras opcionales previstas para una etapa posterior.</p>"
                f"<ul class='clean muted'>{premium_items}</ul></section>"
            )

        quality_block = manifest.get("quality") if isinstance(manifest.get("quality"), dict) else {}
        coverage_total = quality_block.get("coverage_total_pct")
        coverage_label = "N/D" if coverage_total is None else f"{float(coverage_total):.1f}%"
        has_data = bool(latest_block.get("has_data", latest.metadata.get("has_data", False)))
        data_label = "Con datos" if has_data else "Sin datos"
        html = f"""<!doctype html>
<html lang='es'>
<head>
{self._meta_head(_SITE_TITLE, 'Precios en La Anónima Ushuaia.', '/')}
{self._analytics_head_script()}
</head>
<body>
<main class='shell'>
  {self._top_nav(active='home')}
  <section style='text-align: center; padding: 64px 24px 32px 24px;'>
    <div class='hero-stack' style='align-items: center; max-width: 680px; margin: 0 auto; gap: 20px;'>
      <div class='status-chip' style='margin-bottom: 8px;'>Estado: {latest_status} &bull; Cobertura: {coverage_label}</div>
      <h1 style='font-size: clamp(2.2rem, 4vw, 3.2rem); letter-spacing: -0.02em; line-height: 1.15; margin: 0;'>
        Inflación medida en góndola<br><span style='color: var(--primary);'>en tiempo real.</span>
      </h1>
      <p class='muted' style='font-size: 1.15rem; max-width: 500px;'>
        Tracker independiente de precios en {_SITE_TITLE}.<br>
        Rango: {from_month} a {to_month}. Actualizado: {latest_generated}.
      </p>
      <div class='cta-row' style='justify-content: center; margin-top: 12px;'>
        <a id='cta-open-tracker' href='/tracker/' class='btn btn-primary' style='font-size: 1.05rem; padding: 12px 24px; min-height: 48px; border-radius: 12px;'>Abrir Tracker Interactivo &rarr;</a>
        <a href='/historico/' class='btn btn-secondary' style='font-size: 1.05rem; padding: 12px 24px; min-height: 48px; border-radius: 12px;'>Ver histórico</a>
      </div>
    </div>
  </section>

  <section class='card' style='margin-top: 24px;'>
    <h2 style='font-size: 1.25rem; margin-bottom: 16px;'>Resumen del último mes</h2>
    <div class='kpis'>{self._kpi_cards_from_metadata(latest.metadata)}</div>
  </section>

  {warnings_html}
  {premium_html}

  <section class='card muted' style='text-align: center; border: none; background: transparent; box-shadow: none;'>
    <strong>Legal:</strong>
    <a href='/legal/privacy.html' style='color: var(--muted); margin: 0 8px;'>Privacidad</a> |
    <a href='/legal/terms.html' style='color: var(--muted); margin: 0 8px;'>Terminos</a> |
    <a href='/legal/cookies.html' style='color: var(--muted); margin: 0 8px;'>Cookies</a>
  </section>
</main>
{self._consent_banner_html()}
{self._consent_script()}
<script>
(function(){{
  const CONSENT_KEY='laanonima_tracker_cookie_consent_v1';
  function track(eventName, props){{
    if(typeof window.plausible==='function'){{
      try{{ window.plausible(eventName, {{ props: props || {{}} }}); }}catch(_e){{}}
    }}
  }}
  window.__laTrackerOnConsentChanged=function(state){{
    // No-op for ads since removed.
  }};
  const cta=document.getElementById('cta-open-tracker');
  if(cta){{
    cta.addEventListener('click', ()=>track('open_tracker_click', {{origin:'home'}}));
  }}
  track('home_view', {{status:'{latest_status.lower()}' }});
}})();
</script>
</body>
</html>
"""
        (self.output_dir / "index.html").write_text(html, encoding="utf-8")

    def _write_platform_support_files(self) -> None:
        headers = """/*
  X-Frame-Options: SAMEORIGIN
  X-Content-Type-Options: nosniff
  Referrer-Policy: strict-origin-when-cross-origin
  Permissions-Policy: geolocation=(), microphone=(), camera=()
  Strict-Transport-Security: max-age=31536000; includeSubDomains; preload
  Content-Security-Policy: default-src 'self'; img-src 'self' data: https:; style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; font-src 'self' data: https://fonts.gstatic.com; script-src 'self' 'unsafe-inline' https://cdn.plot.ly https://pagead2.googlesyndication.com https://www.googletagmanager.com https://plausible.io; connect-src 'self' https://plausible.io https:; frame-src 'self' https://googleads.g.doubleclick.net https://tpc.googlesyndication.com;

/data/*
  Cache-Control: public, max-age=300

/tracker/*
  Cache-Control: public, max-age=300

/historico/*
  Cache-Control: public, max-age=86400

/assets/css/*
  Cache-Control: public, max-age=31536000, immutable

/assets/*
  Cache-Control: public, max-age=86400

/favicon.svg
  Cache-Control: public, max-age=86400

/site.webmanifest
  Cache-Control: public, max-age=86400

/ads.txt
  Cache-Control: public, max-age=86400
"""
        redirects_lines = []
        parsed = urlparse(self.public_base_url)
        scheme = parsed.scheme or "https"
        canonical_host = str(parsed.hostname or "").strip().lower()
        if canonical_host:
            if canonical_host.startswith("www."):
                from_host = canonical_host[4:]
                to_host = canonical_host
            else:
                from_host = f"www.{canonical_host}"
                to_host = canonical_host
            redirects_lines.append(f"{scheme}://{from_host}/* {scheme}://{to_host}/:splat 301!")

        redirects_lines.extend(
            [
                "/tracker /tracker/ 301",
                "/historico /historico/ 301",
                "/metodologia /metodologia/ 301",
                "/contacto /contacto/ 301",
            ]
        )
        redirects = "\n".join(redirects_lines) + "\n"
        not_found = (
            "<!doctype html><html lang='es'><head>"
            + self._meta_head(f"404 | {_SITE_TITLE}", "Página no encontrada.", "/404.html")
            + "</head><body><main class='shell'>"
            + self._top_nav(active="home")
            + "<section class='card'>"
            "<h1>404</h1>"
            "<p class='muted'>No encontramos la pagina solicitada.</p>"
            "<p><a href='/' class='btn btn-primary'>Volver al inicio</a></p>"
            "</section></main></body></html>"
        )
        (self.output_dir / "_headers").write_text(headers, encoding="utf-8")
        (self.output_dir / "_redirects").write_text(redirects, encoding="utf-8")
        (self.output_dir / "404.html").write_text(not_found, encoding="utf-8")

    def copy_legal_assets(self, manifest: Dict[str, Any]) -> None:
        legal_dir = self.output_dir / "legal"
        self._ensure_dir(legal_dir)

        def render_shell_page(title: str, active: str, body_html: str, path: str, description: str) -> str:
            return (
                "<!doctype html><html lang='es'><head>"
                + self._meta_head(title, description, path)
                + self._adsense_head_script()
                + self._analytics_head_script()
                + "</head><body>"
                "<main class='shell'>"
                f"{self._top_nav(active=active)}"
                "<section class='card'>"
                f"<h1>{title}</h1>{body_html}"
                "</section>"
                "</main>"
                + self._consent_banner_html()
                + self._consent_script()
                + "</body></html>"
            )

        privacy = render_shell_page(
            "Politica de privacidad",
            "home",
            (
                "<p class='muted'>Este sitio publica series agregadas de precios e indicadores para fines informativos.</p>"
                "<p class='muted'>Almacenamos preferencias locales (filtros, estado de interfaz y consentimiento) para mejorar la experiencia.</p>"
                "<p class='muted'>Si aceptas anuncios, se habilita carga de proveedores publicitarios (por ejemplo AdSense) segun su propia politica de datos.</p>"
                "<p class='muted'>La analitica agregada (cuando esta activa) se usa para medir uso de funcionalidades y mejorar contenido editorial.</p>"
                f"<p class='muted'>Contacto: <a href='mailto:{self.contact_email}'>{self.contact_email}</a></p>"
            ),
            "/legal/privacy.html",
            "Politica de privacidad y uso de cookies del tracker.",
        )
        terms = render_shell_page(
            "Terminos de uso",
            "home",
            (
                "<p class='muted'>La informacion se publica con fines informativos. Los precios pueden variar por sucursal, fecha y disponibilidad.</p>"
                "<p class='muted'>No se garantiza disponibilidad continua ni exactitud absoluta en cada observacion individual.</p>"
                "<p class='muted'>El indicador de estado refleja la cobertura actual de datos y puede cambiar a medida que se incorporan nuevas fuentes oficiales.</p>"
            ),
            "/legal/terms.html",
            "Terminos de uso del tracker publico de precios.",
        )
        cookies = render_shell_page(
            "Politica de cookies",
            "home",
            (
                "<p class='muted'>Este sitio usa almacenamiento local para recordar filtros, vista de analisis y consentimiento.</p>"
                "<p class='muted'>Si aceptas publicidad, se habilitan recursos de terceros para mostrar anuncios.</p>"
                "<p class='muted'>Puedes cambiar tu preferencia de cookies limpiando almacenamiento local del navegador.</p>"
            ),
            "/legal/cookies.html",
            "Politica de cookies y preferencias locales del tracker.",
        )
        ads_policy = render_shell_page(
            "Politica de publicidad",
            "home",
            (
                "<p class='muted'>Los anuncios se cargan solo con consentimiento explicito.</p>"
                "<p class='muted'>Proveedor previsto: Google AdSense (puede cambiar segun configuracion operativa).</p>"
                "<p class='muted'>Si rechazas cookies de publicidad, los slots no se activan y se mantiene la experiencia sin anuncios personalizados.</p>"
            ),
            "/legal/ads.html",
            "Politica de anuncios y consentimiento publicitario.",
        )
        metodologia = render_shell_page(
            "Metodologia",
            "metodologia",
            (
                "<div style='max-width: 650px; line-height: 1.7;'>"
                "<p class='muted'>Se comparan precios observados por producto entre meses y se muestra el estado de datos.</p>"
                "<aside class='callout' style='background: #FFF7ED; border-left: 4px solid #F59E0B; padding: 16px; margin: 24px 0; border-radius: 0 8px 8px 0;'>"
                "  <strong style='color: #B45309; display: block; margin-bottom: 8px;'>Importante sobre la fidelidad de datos</strong>"
                "  <p style='color: #92400E; margin: 0; font-size: 0.95rem;'>Los precios reflejan una captura en un momento específico. Pueden existir discrepancias menores por sucursal o cambios intradiarios no capturados en el scraping.</p>"
                "</aside>"
                "<p class='muted'><a href='/tracker/' class='btn btn-primary btn-inline' style='display: inline-flex; align-items: center; justify-content: center; height: 36px; padding: 0 16px;'>Ver tracker</a> <a href='/historico/' class='btn btn-secondary btn-inline' style='display: inline-flex; align-items: center; justify-content: center; height: 36px; padding: 0 16px; margin-left: 8px;'>Ver histórico</a></p>"
                "</div>"
            ),
            "/metodologia/",
            "Metodologia del tracker: recoleccion, calculo y comparativa de series.",
        )
        contacto = render_shell_page(
            "Contacto",
            "contacto",
            (
                "<div style='max-width: 500px; line-height: 1.6;'>"
                "<p class='muted' style='margin-bottom: 24px;'>Consultas sobre datos o funcionamiento del tracker.</p>"
                f"<div class='contact-card' style='background: #F8FAFC; border: 1px solid var(--line); border-radius: 12px; padding: 24px; text-align: center; margin-bottom: 32px;'>"
                "  <p style='margin: 0 0 16px 0; font-weight: 600;'>Envíanos un correo directamente</p>"
                f"  <a href='mailto:{self.contact_email}' class='btn btn-primary' style='display: block; width: 100%; box-sizing: border-box;'>{self.contact_email}</a>"
                "</div>"
                "<h3 style='margin-bottom: 16px;'>Preguntas Frecuentes</h3>"
                "<details style='margin-bottom: 12px; padding: 12px; background: #fff; border: 1px solid var(--line); border-radius: 8px;'>"
                "  <summary style='font-weight: 600; cursor: pointer; color: var(--text);'>¿Con qué frecuencia se actualizan los datos?</summary>"
                "  <p class='muted' style='margin-top: 12px; margin-bottom: 0; font-size: 0.9rem;'>Intentamos realizar una recolección al menos una vez por semana, dependiendo de la disponibilidad de los sistemas de origen.</p>"
                "</details>"
                "<details style='margin-bottom: 12px; padding: 12px; background: #fff; border: 1px solid var(--line); border-radius: 8px;'>"
                "  <summary style='font-weight: 600; cursor: pointer; color: var(--text);'>¿Por qué un producto figura sin datos?</summary>"
                "  <p class='muted' style='margin-top: 12px; margin-bottom: 0; font-size: 0.9rem;'>Puede suceder si el producto no está en stock online durante la recolección, o si cambió su identificador interno o presentación.</p>"
                "</details>"
                "<details style='margin-bottom: 12px; padding: 12px; background: #fff; border: 1px solid var(--line); border-radius: 8px;'>"
                "  <summary style='font-weight: 600; cursor: pointer; color: var(--text);'>¿Cómo se calcula el índice de inflación?</summary>"
                "  <p class='muted' style='margin-top: 12px; margin-bottom: 0; font-size: 0.9rem;'>Se calcula la variación nominal y real (ajustada por IPC general) únicamente sobre el subconjunto de productos idénticos presentes en ambos meses analizados (panel balanceado).</p>"
                "</details>"
                "</div>"
            ),
            "/contacto/",
            "Canales de contacto para consultas y propuestas comerciales.",
        )

        (legal_dir / "privacy.html").write_text(privacy, encoding="utf-8")
        (legal_dir / "terms.html").write_text(terms, encoding="utf-8")
        (legal_dir / "cookies.html").write_text(cookies, encoding="utf-8")
        (legal_dir / "ads.html").write_text(ads_policy, encoding="utf-8")
        (self.output_dir / "metodologia" / "index.html").parent.mkdir(parents=True, exist_ok=True)
        (self.output_dir / "metodologia" / "index.html").write_text(metodologia, encoding="utf-8")
        (self.output_dir / "contacto" / "index.html").parent.mkdir(parents=True, exist_ok=True)
        (self.output_dir / "contacto" / "index.html").write_text(contacto, encoding="utf-8")

        if self.ads_enabled and self.ads_provider.lower() == "adsense" and not self._is_placeholder_adsense_client(self.ads_client_id):
            ads_txt = f"google.com, {self.ads_client_id}, DIRECT, f08c47fec0942fa0\n"
        else:
            ads_txt = "# ads disabled or not configured\n"
        (self.output_dir / "ads.txt").write_text(ads_txt, encoding="utf-8")
        (self.output_dir / "robots.txt").write_text("User-agent: *\nAllow: /\nSitemap: /sitemap.xml\n", encoding="utf-8")

        urls = [
            "/",
            "/tracker/",
            "/historico/",
            "/metodologia/",
            "/contacto/",
            "/legal/privacy.html",
            "/legal/terms.html",
            "/legal/cookies.html",
            "/legal/ads.html",
            "/favicon.svg",
            "/site.webmanifest",
            "/assets/og-card.svg",
        ]
        for row in manifest.get("history", []):
            path = str(row.get("report_path") or "").strip()
            if path:
                urls.append(path)

        now = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        sitemap_lines = ["<?xml version='1.0' encoding='UTF-8'?>", "<urlset xmlns='http://www.sitemaps.org/schemas/sitemap/0.9'>"]
        for url in urls:
            loc = f"{self.public_base_url}{url}"
            sitemap_lines.append(f"<url><loc>{loc}</loc><lastmod>{now}</lastmod></url>")
        sitemap_lines.append("</urlset>")
        (self.output_dir / "sitemap.xml").write_text("\n".join(sitemap_lines), encoding="utf-8")
        self._write_platform_support_files()

    def publish(
        self,
        preferred_html: Optional[str] = None,
        preferred_metadata: Optional[str] = None,
        preferred_from_month: Optional[str] = None,
        preferred_to_month: Optional[str] = None,
    ) -> PublishWebResult:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._write_brand_assets()

        latest = self.collect_latest_report(
            preferred_html=preferred_html,
            preferred_metadata=preferred_metadata,
            preferred_from_month=preferred_from_month,
            preferred_to_month=preferred_to_month,
        )
        copied = self._copy_latest_artifacts(latest)
        self.build_product_detail_pages(copied, {})
        history_rows = self.build_history_index()
        manifest = self.build_manifest(latest, history_rows)

        # Enrich latest metadata with website publication fields.
        latest_meta_path = Path(copied["latest_metadata_path"])
        latest_meta = self._read_json(latest_meta_path)
        quality_flags = (
            latest_meta.get("data_quality", {}).get("quality_flags", {})
            if isinstance(latest_meta.get("data_quality"), dict)
            else {}
        )
        from_month, to_month = self._range_from_metadata(latest_meta)
        latest_manifest = manifest.get("latest", {}) if isinstance(manifest.get("latest"), dict) else {}
        latest_generated_at = str(
            latest_manifest.get("generated_at")
            or latest_meta.get("generated_at")
            or latest.metadata.get("generated_at")
            or ""
        )
        latest_meta["is_stale"] = bool(manifest.get("is_stale", False))
        latest_meta["next_update_eta"] = manifest.get("next_update_eta")
        latest_meta["ad_slots_enabled"] = bool(self.ads_enabled)
        latest_meta["premium_placeholders_enabled"] = bool(self.premium_enabled)
        latest_meta["from_month"] = from_month
        latest_meta["to_month"] = to_month
        latest_meta["generated_at"] = latest_generated_at
        latest_meta["has_data"] = bool(latest_manifest.get("has_data", latest_meta.get("has_data", False)))
        latest_meta["web_status"] = str(
            latest_manifest.get("web_status")
            or manifest.get("status")
            or latest_meta.get("web_status")
            or "partial"
        )
        latest_meta["latest_range_label"] = (
            f"{from_month} a {to_month}" if from_month and to_month else "N/D"
        )
        latest_meta["quality_warnings"] = quality_flags.get("warnings", []) if isinstance(quality_flags, dict) else []
        latest_meta["publication_policy"] = _PUBLICATION_POLICY
        latest_meta["publication_policy_summary"] = _PUBLICATION_POLICY_SUMMARY
        latest_meta_path.write_text(json.dumps(latest_meta, ensure_ascii=False, indent=2), encoding="utf-8")

        self.build_home_page(manifest, latest)
        self.copy_legal_assets(manifest)
        self.build_api_endpoint(manifest, latest)

        return PublishWebResult(
            status="completed",
            web_status=str(manifest.get("status")),
            is_stale=bool(manifest.get("is_stale", False)),
            output_dir=str(self.output_dir),
            tracker_path=copied["tracker_path"],
            manifest_path=str(self.output_dir / "data" / "manifest.json"),
            latest_metadata_path=copied["latest_metadata_path"],
            history_count=len(history_rows),
            source_report_html=str(latest.html_path),
            source_report_metadata=str(latest.metadata_path),
            next_update_eta=str(manifest.get("next_update_eta") or ""),
        )

    def build_product_detail_pages(self, copied: Dict[str, str], manifest: Dict[str, Any]) -> None:
        """Generate one /tracker/{canonical_id}/index.html per product."""
        tracker_path = Path(copied["tracker_path"])
        source_report_dir = tracker_path.parent  # public/tracker/
        products_source_dir = source_report_dir / "products"

        # products/ dir is written by reporting.py alongside the tracker HTML.
        # Look in the source report dir first, then in the original report dir.
        if not products_source_dir.exists():
            return  # no per-product data available yet

        tracker_css_bundle = get_tracker_css_bundle()
        analytics_script = self._analytics_head_script()
        site_url = str((self.config or {}).get("site_url") or "").rstrip("/")

        def _money(v: Any) -> str:
            if v is None:
                return "N/D"
            try:
                return f"${float(v):,.0f}".replace(",", ".")
            except (TypeError, ValueError):
                return "N/D"

        def _pct_signed(v: Any) -> str:
            if v is None:
                return "N/D"
            try:
                fv = float(v)
                sign = "+" if fv >= 0 else ""
                return f"{sign}{fv:.1f}%"
            except (TypeError, ValueError):
                return "N/D"

        def _pct_class(v: Any) -> str:
            if v is None:
                return ""
            try:
                return "pos" if float(v) >= 0 else "neg"
            except (TypeError, ValueError):
                return ""

        def _fmt_month(m: Optional[str]) -> str:
            if not m:
                return "N/D"
            months_es = {
                "01": "ene", "02": "feb", "03": "mar", "04": "abr",
                "05": "may", "06": "jun", "07": "jul", "08": "ago",
                "09": "sep", "10": "oct", "11": "nov", "12": "dic",
            }
            parts = str(m).split("-")
            if len(parts) == 2:
                return f"{months_es.get(parts[1], parts[1])} {parts[0]}"
            return str(m)

        def _terna_row(tier: str, info: Dict[str, Any]) -> str:
            name = str(info.get("candidate_name") or "").strip() or "—"
            price = _money(info.get("candidate_price"))
            scraped = str(info.get("scraped_at") or "")[:10] or "—"
            label_map = {"low": "Mínimo", "mid": "Representativo", "high": "Máximo"}
            label = label_map.get(tier, tier.upper())
            return (
                f"<tr>"
                f"<td><span class='terna-tier {tier}'>{label}</span></td>"
                f"<td>{name}</td>"
                f"<td style='font-weight:600;font-variant-numeric:tabular-nums;'>{price}</td>"
                f"<td class='muted' style='font-size:.8rem;'>{scraped}</td>"
                f"</tr>"
            )

        for json_file in sorted(products_source_dir.glob("*.json")):
            try:
                pd_data = json.loads(json_file.read_text(encoding="utf-8"))
            except Exception:
                continue

            canonical_id = str(pd_data.get("canonical_id") or json_file.stem)
            product_name = str(pd_data.get("product_name") or canonical_id)
            category = str(pd_data.get("category") or "").replace("_", " ").title()
            presentation = str(pd_data.get("presentation") or "")
            from_month = _fmt_month(pd_data.get("from_month"))
            to_month = _fmt_month(pd_data.get("to_month"))
            current_price = _money(pd_data.get("current_price"))
            var_pct = pd_data.get("var_pct")
            var_real_pct = pd_data.get("var_real_pct")

            # -- Terna table HTML --
            terna_latest = pd_data.get("terna_latest") or {}
            terna_rows_html = ""
            for tier in ("low", "mid", "high"):
                info = terna_latest.get(tier)
                if info:
                    terna_rows_html += _terna_row(tier, info)
            terna_section = ""
            if terna_rows_html:
                terna_section = f"""
  <section class='card'>
    <h2>Variantes de precio</h2>
    <div class='table-wrap' style='border-radius:var(--radius-card);margin-top:8px;'>
      <table class='terna-table'>
        <thead><tr>
          <th>Nivel</th><th>Variante</th><th>Último precio</th><th>Relevado</th>
        </tr></thead>
        <tbody>{terna_rows_html}</tbody>
      </table>
    </div>
  </section>"""

            # -- Monthly series for Plotly --
            monthly = pd_data.get("monthly_series") or []
            plot_js = ""
            if monthly:
                months_x = [str(r.get("month") or "") for r in monthly]
                prices_y = [r.get("avg_price") for r in monthly]
                series_json = json.dumps({"x": months_x, "y": prices_y}, ensure_ascii=False)
                plot_js = f"""
<script>
(function(){{
  var data={series_json};
  var el=document.getElementById('product-chart');
  if(!el||!data.x.length) return;
  var traces=[{{
    x:data.x.map(function(m){{return new Date(m+'-01T00:00:00');}}),
    y:data.y,
    mode:'lines+markers',
    line:{{color:'#1d4ed8',width:2.5,shape:'spline'}},
    marker:{{size:5,color:'#1d4ed8'}},
    name:'Precio promedio',
    hovertemplate:'%{{x|%b %Y}}: $%{{y:,.0f}}<extra></extra>',
  }}];
  var layout={{
    paper_bgcolor:'transparent',
    plot_bgcolor:'transparent',
    margin:{{l:72,r:20,t:16,b:52}},
    autosize:true,
    height:360,
    xaxis:{{
      type:'date',
      tickformat:'%b %Y',
      showgrid:true,
      gridcolor:'#e5e7eb',
      zeroline:false,
      tickfont:{{size:11,color:'#64748b'}},
    }},
    yaxis:{{
      title:{{text:'Precio promedio ($)',font:{{size:12,color:'#64748b'}}}},
      showgrid:true,
      gridcolor:'#e5e7eb',
      zeroline:false,
      tickfont:{{size:11,color:'#64748b'}},
      tickformat:',.0f',
      tickprefix:'$',
    }},
    hovermode:'x unified',
    showlegend:false,
    font:{{family:'Inter, Segoe UI, sans-serif',color:'#1e293b'}},
  }};
  try{{
    Plotly.react(el,traces,layout,{{displayModeBar:false,responsive:true}});
  }}catch(e){{
    el.innerHTML='<div class="detail-empty"><span>No se pudo cargar el gráfico.</span></div>';
  }}
}})();
</script>"""

            chart_section = f"""
  <details class='card chart-panel' open>
    <summary><strong>Evolución de precio</strong></summary>
    <div class='chart-panel-body'>
      <div id='product-chart' class='chart'></div>
    </div>
  </details>"""

            # -- KPI stats section --
            var_html = f"<div class='product-stat'><span class='stat-label'>Variación nominal</span><span class='stat-value {_pct_class(var_pct)}'>{_pct_signed(var_pct)}</span><span class='stat-sub'>{from_month} → {to_month}</span></div>"
            real_html = f"<div class='product-stat'><span class='stat-label'>Variación real</span><span class='stat-value {_pct_class(var_real_pct)}'>{_pct_signed(var_real_pct)}</span><span class='stat-sub'>Ajustado por IPC</span></div>" if var_real_pct is not None else ""

            # -- Build page --
            canonical_url = f"{site_url}/tracker/{canonical_id}/"
            og_image = f"{site_url}/assets/og-card.svg"
            summary_meta = f"{product_name} — evolución y comparativa de precios en La Anónima Ushuaia."

            page_html = f"""<!doctype html>
<html lang='es'>
<head>
<meta charset='utf-8'/>
<meta name='viewport' content='width=device-width,initial-scale=1'/>
<title>{product_name} | Tracker La Anónima Ushuaia</title>
<meta name='description' content='{summary_meta}'/>
<meta name='theme-color' content='#1d4ed8'/>
<link rel='canonical' href='{canonical_url}'/>
<meta property='og:type' content='website'/>
<meta property='og:title' content='{product_name} | La Anónima Ushuaia'/>
<meta property='og:description' content='{summary_meta}'/>
<meta property='og:image' content='{og_image}'/>
<link rel='icon' type='image/svg+xml' href='/favicon.svg'/>
<link rel='manifest' href='/site.webmanifest'/>
<link rel='preconnect' href='https://fonts.googleapis.com'/>
<link rel='preconnect' href='https://fonts.gstatic.com' crossorigin/>
<link href='https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap' rel='stylesheet'/>
<script src='https://cdn.plot.ly/plotly-basic-2.35.2.min.js'></script>
{analytics_script}
<link rel='stylesheet' href='/tracker/tracker-ui.css'/>
</head>
<body>
<main class='shell'>
  {self._top_nav(active='tracker')}

  <section class='card'>
    <div class='breadcrumb'>
      <a href='/'>Inicio</a>
      <span class='breadcrumb-sep'>›</span>
      <a href='/tracker/'>Tracker</a>
      <span class='breadcrumb-sep'>›</span>
      <span>{product_name}</span>
    </div>
    <div class='product-header'>
      <h1>{product_name}</h1>
      <div class='product-header-meta'>
        {f"<span>{category}</span>" if category else ""}
        {f"<span class='breadcrumb-sep'>·</span><span>{presentation}</span>" if presentation else ""}
        <span class='breadcrumb-sep'>·</span>
        <span>{from_month} – {to_month}</span>
      </div>
    </div>
  </section>

  <div class='product-stats'>
    <div class='product-stat'>
      <span class='stat-label'>Precio actual</span>
      <span class='stat-value'>{current_price}</span>
      <span class='stat-sub'>Último relevado</span>
    </div>
    {var_html}
    {real_html}
  </div>

{chart_section}
{terna_section}

  <section class='card'>
    <a href='/tracker/' class='back-link'>← Volver al Tracker</a>
  </section>
</main>
{plot_js}
</body>
</html>"""

            # Write to public/tracker/{canonical_id}/index.html
            safe_id = "".join(c if c.isalnum() or c in "-_" else "_" for c in canonical_id)
            detail_dir = self.output_dir / "tracker" / safe_id
            detail_dir.mkdir(parents=True, exist_ok=True)
            (detail_dir / "index.html").write_text(page_html, encoding="utf-8")

    def build_api_endpoint(self, manifest: Dict[str, Any], latest: ReportEntry) -> None:

        """Write a clean public JSON API at /api/latest.json for external consumers."""
        from_month, to_month = self._range_from_metadata(latest.metadata)
        kpis = latest.metadata.get("kpis", {}) if isinstance(latest.metadata.get("kpis"), dict) else {}
        coverage = latest.metadata.get("coverage", {}) if isinstance(latest.metadata.get("coverage"), dict) else {}
        web_status_raw = str(manifest.get("status") or "partial")
        _STATUS_LABELS = {"fresh": "actualizado", "partial": "en_proceso", "stale": "desactualizado"}
        api_payload = {
            "_info": {
                "description": "API pública del Tracker de Precios La Anónima Ushuaia.",
                "generated_at": manifest.get("generated_at", ""),
                "next_update_eta": manifest.get("next_update_eta", ""),
                "license": "Datos de dominio público con fines informativos. Ver /legal/terms.html",
            },
            "status": _STATUS_LABELS.get(web_status_raw, web_status_raw),
            "periodo": {
                "desde": from_month,
                "hasta": to_month,
            },
            "indicadores": {
                "inflacion_canasta_nominal_pct": kpis.get("inflation_basket_nominal_pct"),
                "ipc_oficial_periodo_pct": kpis.get("ipc_period_pct"),
                "brecha_canasta_vs_ipc_pp": kpis.get("gap_vs_ipc_pp"),
                "panel_balanceado_n": kpis.get("balanced_panel_n"),
            },
            "cobertura": {
                "total_pct": coverage.get("coverage_total_pct"),
                "productos_observados": coverage.get("total_observed"),
            },
            "links": {
                "tracker": "/tracker/",
                "historico": "/historico/",
                "manifest": "/data/manifest.json",
                "metadata": "/data/latest.metadata.json",
            },
        }
        api_dir = self.output_dir / "api"
        self._ensure_dir(api_dir)
        api_json_path = api_dir / "latest.json"
        api_json_path.write_text(json.dumps(api_payload, ensure_ascii=False, indent=2), encoding="utf-8")

        # Simple human-readable index page for the API
        meta_head = self._meta_head("API Pública | Tracker La Anónima", "Endpoint JSON público del tracker de precios.", "/api/")
        api_index_html = f"""<!doctype html>
<html lang='es'>
<head>{meta_head}{self._analytics_head_script()}</head>
<body>
<main class='shell'>
  {self._top_nav(active='home')}
  <section class='card'>
    <h1>API Pública</h1>
    <p class='muted'>Acceso programático a los datos del tracker para periodistas, investigadores y desarrolladores.</p>
  </section>
  <section class='card'>
    <h2>Endpoint</h2>
    <p><code>/api/latest.json</code> — Datos del período más reciente. Sin autenticación, sin límites.</p>
    <pre style='background:var(--panel-soft);border:1px solid var(--line);border-radius:12px;padding:16px;overflow-x:auto;font-size:.82rem;line-height:1.6;'>{{
  &quot;status&quot;: &quot;en_proceso | actualizado | desactualizado&quot;,
  &quot;periodo&quot;: {{ &quot;desde&quot;: &quot;2025-09&quot;, &quot;hasta&quot;: &quot;2026-02&quot; }},
  &quot;indicadores&quot;: {{
    &quot;inflacion_canasta_nominal_pct&quot;: 31.62,
    &quot;ipc_oficial_periodo_pct&quot;: null,
    &quot;brecha_canasta_vs_ipc_pp&quot;: null,
    &quot;panel_balanceado_n&quot;: 20
  }},
  &quot;cobertura&quot;: {{ &quot;total_pct&quot;: 30.7 }},
  &quot;links&quot;: {{ &quot;tracker&quot;: &quot;/tracker/&quot; }}
}}</pre>
  </section>
  <section class='card'>
    <h2>Uso</h2>
    <pre style='background:var(--panel-soft);border:1px solid var(--line);border-radius:12px;padding:16px;font-size:.82rem;'>fetch('https://tu-dominio.com/api/latest.json')
  .then(r =&gt; r.json())
  .then(data =&gt; console.log(data.indicadores))</pre>
    <p class='muted' style='margin-top:12px;'>Para datos históricos completos: <a href='/data/manifest.json'>/data/manifest.json</a> y <a href='/data/latest.metadata.json'>/data/latest.metadata.json</a>.</p>
  </section>
</main>
</body></html>"""
        (api_dir / "index.html").write_text(api_index_html, encoding="utf-8")


def run_web_publish(
    config_path: Optional[str] = None,
    from_month: Optional[str] = None,
    to_month: Optional[str] = None,
    basket_type: str = "all",
    benchmark_mode: str = "ipc",
    analysis_depth: str = "executive",
    offline_assets: str = "external",
    build_report: bool = True,
) -> Dict[str, Any]:
    """Generate public static website artifacts from interactive reports.

    When build_report is False and from/to are provided, publication enforces an exact
    report-range match; otherwise it fails fast.
    """
    config = load_config(config_path)

    preferred_html = None
    preferred_metadata = None
    if build_report:
        report_result = run_report(
            config_path=config_path,
            from_month=from_month,
            to_month=to_month,
            export_pdf=False,
            basket_type=basket_type,
            benchmark_mode=benchmark_mode,
            analysis_depth=analysis_depth,
            offline_assets=offline_assets,
        )
        preferred_html = str(report_result.get("artifacts", {}).get("html_path") or "")
        preferred_metadata = str(report_result.get("artifacts", {}).get("metadata_path") or "")

    publisher = StaticWebPublisher(config)
    result = publisher.publish(
        preferred_html=preferred_html if preferred_html else None,
        preferred_metadata=preferred_metadata if preferred_metadata else None,
        preferred_from_month=from_month if not build_report else None,
        preferred_to_month=to_month if not build_report else None,
    )
    return {
        "status": result.status,
        "web_status": result.web_status,
        "is_stale": result.is_stale,
        "output_dir": result.output_dir,
        "tracker_path": result.tracker_path,
        "manifest_path": result.manifest_path,
        "latest_metadata_path": result.latest_metadata_path,
        "history_count": result.history_count,
        "source_report_html": result.source_report_html,
        "source_report_metadata": result.source_report_metadata,
        "next_update_eta": result.next_update_eta,
    }
