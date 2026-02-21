"""Static web publication pipeline for low-cost public deployment."""

from __future__ import annotations

import json
import re
import shutil
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from src.config_loader import load_config
from src.reporting import run_report

_REPORT_METADATA_RE = re.compile(r"report_interactive_(\d{6})_to_(\d{6})_(\d{8}_\d{6})\.metadata\.json$")
_MONTH_RE = re.compile(r"^\d{4}-\d{2}$")
_PUBLICATION_POLICY = "publish_with_alert_on_partial"
_PUBLICATION_POLICY_SUMMARY = "Se publica con alerta si falta cobertura o IPC."


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
        unique: Dict[str, ReportEntry] = {}
        for entry in self._iter_report_entries():
            if entry.month not in unique:
                unique[entry.month] = entry
            if len(unique) >= self.keep_history_months:
                break
        rows = list(unique.values())
        rows.sort(key=lambda item: item.month, reverse=True)
        return rows

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
        self._ensure_dir(assets_dir)
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
<text x='90' y='228' fill='#ffffff' font-size='64' font-family='Segoe UI, Arial, sans-serif' font-weight='700'>La Anonima Tracker</text>
<text x='90' y='298' fill='#dff4ff' font-size='34' font-family='Segoe UI, Arial, sans-serif'>Precios historicos + comparativa macro IPC</text>
<text x='90' y='520' fill='#e8fbff' font-size='28' font-family='Segoe UI, Arial, sans-serif'>Datos publicos actualizados diariamente</text>
</svg>
"""
        web_manifest = {
            "name": "La Anonima Tracker",
            "short_name": "LA Tracker",
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
        (self.output_dir / "site.webmanifest").write_text(
            json.dumps(web_manifest, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    @staticmethod
    def _shell_css() -> str:
        return """
:root{
  --bg:#f9fafb;
  --panel:#ffffff;
  --line:#e5e7eb;
  --line-strong:#dbe2ea;
  --text:#1e293b;
  --muted:#64748b;
  --primary:#1d4ed8;
  --primary-strong:#1e3a8a;
  --ok:#047857;
  --warn:#b45309;
  --danger:#b91c1c;
  --shadow-sm:0 1px 2px rgba(15,23,42,.04), 0 10px 24px rgba(15,23,42,.05);
  --radius-card:16px;
  --radius-control:10px;
  --radius-pill:999px;
  --font-body:"Inter","Segoe UI","Roboto","Helvetica Neue",Arial,sans-serif;
}
*{box-sizing:border-box}
html,body{
  margin:0;
  padding:0;
  color:var(--text);
  font-family:var(--font-body);
}
body{
  background:
    radial-gradient(920px 280px at 20% -12%, rgba(29,78,216,.08) 0%, rgba(29,78,216,0) 56%),
    var(--bg);
  line-height:1.5;
}
a{
  color:var(--primary);
  text-decoration:none;
}
a:hover{text-decoration:underline}
main.shell{
  max-width:1120px;
  margin:0 auto;
  padding:24px;
  display:grid;
  gap:16px;
}
.card{
  background:var(--panel);
  border:1px solid var(--line);
  border-radius:var(--radius-card);
  padding:24px;
  box-shadow:var(--shadow-sm);
}
.topbar{
  display:flex;
  align-items:center;
  justify-content:space-between;
  gap:14px;
  flex-wrap:wrap;
  padding-top:14px;
  padding-bottom:14px;
}
.brand{
  color:var(--text);
  font-weight:800;
  text-decoration:none;
  letter-spacing:.01em;
  font-size:1rem;
}
.nav{
  display:flex;
  gap:10px;
  flex-wrap:wrap;
}
.nav a{
  display:inline-flex;
  align-items:center;
  justify-content:center;
  min-height:36px;
  padding:8px 14px;
  border-radius:var(--radius-pill);
  border:1px solid transparent;
  background:#f1f5f9;
  color:#475569;
  text-decoration:none;
  font-size:.84rem;
  font-weight:600;
  transition:all .18s ease;
}
.nav a:hover{
  background:#e8edf4;
  color:#334155;
  text-decoration:none;
}
.nav a.active{
  background:#0f172a;
  border-color:#0f172a;
  color:#fff;
}
h1,h2{
  margin:0 0 8px 0;
  letter-spacing:-.01em;
}
h1{font-size:clamp(1.4rem,2.7vw,1.9rem);line-height:1.2}
h2{font-size:1.08rem;line-height:1.3}
.muted{color:var(--muted)}
.status-chip{
  display:inline-flex;
  align-items:center;
  min-height:30px;
  padding:4px 10px;
  border-radius:var(--radius-pill);
  border:1px solid #dbeafe;
  background:#eff6ff;
  color:#1d4ed8;
  font-size:.78rem;
  font-weight:700;
  letter-spacing:.03em;
  text-transform:uppercase;
}
.cta-row{
  display:flex;
  flex-wrap:wrap;
  gap:10px;
}
.hero-grid{
  display:grid;
  gap:16px;
  grid-template-columns:minmax(0,1.35fr) minmax(240px,.9fr);
  align-items:start;
}
.hero-stack{
  display:grid;
  gap:10px;
}
.metric-strip{
  display:grid;
  gap:10px;
  grid-template-columns:repeat(auto-fit,minmax(160px,1fr));
}
.metric-tile{
  border:1px solid var(--line);
  border-radius:14px;
  padding:14px;
  background:#fff;
  display:grid;
  gap:6px;
}
.metric-tile strong{
  font-size:.72rem;
  color:var(--muted);
  text-transform:uppercase;
  letter-spacing:.08em;
  font-weight:600;
}
.metric-tile span{
  font-size:1.08rem;
  color:var(--text);
  font-weight:700;
  font-variant-numeric:tabular-nums;
}
.btn{
  display:inline-flex;
  align-items:center;
  justify-content:center;
  min-height:38px;
  padding:8px 14px;
  border-radius:var(--radius-control);
  text-decoration:none;
  font-weight:600;
  border:1px solid transparent;
  transition:all .18s ease;
}
.btn-primary{
  background:var(--primary);
  border-color:var(--primary);
  color:#fff;
}
.btn-primary:hover{background:var(--primary-strong);border-color:var(--primary-strong)}
.btn-secondary{
  background:#fff;
  border-color:var(--line);
  color:var(--text);
}
.grid-2{
  display:grid;
  gap:16px;
  grid-template-columns:repeat(2,minmax(0,1fr));
}
.kpis{
  display:grid;
  gap:12px;
  grid-template-columns:repeat(auto-fit,minmax(180px,1fr));
}
.kpi{
  border:1px solid var(--line);
  border-radius:14px;
  padding:16px;
  background:#fff;
  display:grid;
  gap:6px;
  align-content:center;
  min-height:112px;
}
.kpi strong{
  margin:0;
  font-size:.72rem;
  color:var(--muted);
  text-transform:uppercase;
  letter-spacing:.08em;
  font-weight:600;
}
.kpi span{
  margin:0;
  font-size:clamp(1.25rem,2.9vw,1.9rem);
  line-height:1.1;
  font-weight:700;
  color:var(--text);
  font-variant-numeric:tabular-nums;
}
.list-tools{
  display:flex;
  align-items:center;
  justify-content:space-between;
  gap:12px;
  flex-wrap:wrap;
  margin-top:10px;
}
.list-tools input{
  min-height:40px;
  min-width:230px;
  border:1px solid var(--line);
  border-radius:var(--radius-control);
  padding:8px 12px;
  font:inherit;
  color:var(--text);
  background:#fff;
}
.list-tools input:focus{
  border-color:#93c5fd;
  outline:2px solid rgba(29,78,216,.25);
  outline-offset:1px;
}
.history-list{
  display:grid;
  gap:12px;
}
.history-list a{
  display:grid;
  gap:10px;
  padding:16px;
  border:1px solid var(--line);
  border-radius:14px;
  text-decoration:none;
  color:var(--text);
  background:#fff;
  transition:all .18s ease;
}
.history-list a:hover{
  border-color:#cfd8e3;
  box-shadow:var(--shadow-sm);
  text-decoration:none;
}
.history-head{
  display:flex;
  justify-content:space-between;
  align-items:center;
  gap:8px;
}
.history-sub{
  display:flex;
  flex-wrap:wrap;
  gap:10px;
  font-size:.8rem;
  color:var(--muted);
}
.badge-mini{
  display:inline-flex;
  align-items:center;
  min-height:24px;
  padding:3px 10px;
  border-radius:var(--radius-pill);
  border:1px solid #bfdbfe;
  background:#eff6ff;
  color:#1d4ed8;
  font-size:.75rem;
  font-weight:700;
  letter-spacing:.03em;
  text-transform:uppercase;
}
.badge-mini.partial{
  border-color:#fcd9bd;
  background:#fff7ed;
  color:#9a3412;
}
.badge-mini.stale{
  border-color:#fecaca;
  background:#fef2f2;
  color:#991b1b;
}
.meta-line{
  font-size:.82rem;
  color:var(--muted);
}
.ads-grid{
  display:grid;
  gap:12px;
  grid-template-columns:repeat(auto-fit,minmax(210px,1fr));
}
.ad-slot{
  border:1px dashed #93a8c0;
  border-radius:12px;
  min-height:90px;
  background:#f8fafc;
  display:flex;
  align-items:center;
  justify-content:center;
  color:#334155;
  font-weight:600;
}
ul.clean{
  margin:0;
  padding-left:18px;
}
ul.clean li{margin:6px 0}
.cookie-banner{
  position:fixed;
  left:12px;
  right:12px;
  bottom:12px;
  z-index:9999;
  border:1px solid var(--line);
  border-radius:14px;
  background:#fff;
  color:var(--text);
  padding:12px;
  box-shadow:var(--shadow-sm);
}
.cookie-grid{
  display:flex;
  align-items:center;
  justify-content:space-between;
  gap:12px;
  flex-wrap:wrap;
}
.cookie-text{
  font-size:.86rem;
  color:var(--muted);
  max-width:760px;
}
.cookie-actions{
  display:flex;
  align-items:center;
  gap:8px;
}
.cookie-actions button{
  border:1px solid var(--line);
  border-radius:8px;
  min-height:34px;
  padding:7px 11px;
  background:#fff;
  color:var(--text);
  cursor:pointer;
  font:inherit;
  font-weight:600;
}
.cookie-actions button.primary{
  background:var(--primary);
  border-color:var(--primary);
  color:#fff;
}
@media (max-width:900px){
  main.shell{padding:16px}
  .hero-grid{grid-template-columns:1fr}
  .grid-2{grid-template-columns:1fr}
  .card{padding:18px}
}
@media (max-width:620px){
  h1{font-size:1.28rem}
  .nav a{flex:1 1 calc(50% - 6px);justify-content:center}
  .list-tools input{min-width:100%}
  .cookie-banner{left:8px;right:8px;bottom:8px}
}
"""

    @staticmethod
    def _top_nav(active: str = "home") -> str:
        links = [
            ("home", "/", "Inicio"),
            ("tracker", "/tracker/", "Tracker"),
            ("historico", "/historico/", "Historico"),
            ("metodologia", "/metodologia/", "Metodologia"),
            ("contacto", "/contacto/", "Contacto"),
        ]
        nav_items = []
        for key, href, label in links:
            class_attr = " class='active'" if key == active else ""
            nav_items.append(f"<a href='{href}'{class_attr}>{label}</a>")
        return (
            "<section class='card topbar'>"
            "<a href='/' class='brand'>La Anonima Tracker</a>"
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
        latest_meta_path = data_dir / "latest.metadata.json"

        shutil.copy2(latest.html_path, tracker_path)
        shutil.copy2(latest.metadata_path, latest_meta_path)

        return {
            "tracker_path": str(tracker_path),
            "latest_metadata_path": str(latest_meta_path),
        }

    def build_history_index(self) -> List[Dict[str, Any]]:
        historico_root = self.output_dir / "historico"
        data_history_root = self.output_dir / "data" / "history"
        self._ensure_dir(historico_root)
        self._ensure_dir(data_history_root)

        rows: List[Dict[str, Any]] = []
        for entry in self._collect_history():
            month_dir = historico_root / entry.month
            self._ensure_dir(month_dir)

            month_html = month_dir / "index.html"
            month_meta = data_history_root / f"{entry.month}.metadata.json"
            shutil.copy2(entry.html_path, month_html)
            shutil.copy2(entry.metadata_path, month_meta)

            rows.append(
                {
                    "month": entry.month,
                    "report_path": f"/historico/{entry.month}/",
                    "metadata_path": f"/data/history/{entry.month}.metadata.json",
                    "generated_at": entry.generated_at.strftime("%Y-%m-%d %H:%M:%S UTC"),
                    "has_data": bool(entry.metadata.get("has_data", False)),
                    "coverage_total_pct": (
                        entry.metadata.get("coverage", {}).get("coverage_total_pct")
                        if isinstance(entry.metadata.get("coverage"), dict)
                        else None
                    ),
                    "is_partial": bool(
                        entry.metadata.get("data_quality", {}).get("quality_flags", {}).get("is_partial", False)
                        if isinstance(entry.metadata.get("data_quality"), dict)
                        else False
                    ),
                    "quality_badge": (
                        entry.metadata.get("data_quality", {}).get("quality_flags", {}).get("badge")
                        if isinstance(entry.metadata.get("data_quality"), dict)
                        else None
                    ),
                }
            )

        list_rows = []
        for item in rows:
            state = "partial" if item.get("is_partial") else "fresh"
            state_label = "Parcial" if item.get("is_partial") else "Completo"
            badge_classes = f"badge-mini {state}"
            coverage = item.get("coverage_total_pct")
            coverage_label = "N/D" if coverage is None else f"{float(coverage):.1f}%"
            quality_badge = str(item.get("quality_badge") or "").strip() or "Sin badge"
            data_label = "Con datos" if item.get("has_data") else "Sin datos"
            list_rows.append(
                "<a href='{path}' data-month='{month}'>"
                "<div class='history-head'>"
                "<strong>{month}</strong>"
                "<span class='{badge_classes}'>{state_label}</span>"
                "</div>"
                "<div class='history-sub'>"
                "<span>Generado: {generated}</span>"
                "<span>Cobertura: {coverage_label}</span>"
                "<span>{data_label}</span>"
                "<span>{quality_badge}</span>"
                "</div>"
                "</a>".format(
                    path=item["report_path"],
                    month=item["month"],
                    generated=item["generated_at"],
                    badge_classes=badge_classes,
                    state_label=state_label,
                    coverage_label=coverage_label,
                    data_label=data_label,
                    quality_badge=quality_badge,
                )
            )
        list_html = "".join(list_rows) if list_rows else "<p class='muted'>Sin reportes historicos disponibles.</p>"

        html = f"""<!doctype html>
<html lang='es'>
<head>
{self._meta_head('Historico | La Anonima Tracker', 'Historico mensual de reportes publicados del tracker.', '/historico/')}
{self._analytics_head_script()}
<style>{self._shell_css()}</style>
</head>
<body>
<main class='shell'>
  {self._top_nav(active='historico')}
  <section class='card'>
    <h1>Historico de reportes</h1>
    <p class='muted'>Revisa meses publicados, cobertura y estado de calidad.</p>
    <div class='list-tools'>
      <input id='history-search' type='search' placeholder='Filtrar por mes (YYYY-MM)'/>
      <div class='status-chip'><span id='history-count'>{len(rows)}</span> periodos</div>
    </div>
  </section>
  <section class='card'>
    <div id='history-list' class='history-list'>{list_html}</div>
  </section>
</main>
{self._consent_banner_html()}
{self._consent_script()}
<script>
(function(){{
  const input=document.getElementById('history-search');
  const container=document.getElementById('history-list');
  const count=document.getElementById('history-count');
  if(!input || !container || !count) return;
  const rows=Array.from(container.querySelectorAll('a[data-month]'));
  const apply=()=>{{
    const q=String(input.value||'').trim().toLowerCase();
    let visible=0;
    rows.forEach((row)=>{{
      const month=String(row.getAttribute('data-month')||'').toLowerCase();
      const match=!q || month.includes(q);
      row.style.display=match?'flex':'none';
      if(match) visible+=1;
    }});
    count.textContent=String(visible);
  }};
  input.addEventListener('input', apply);
}})();
</script>
</body>
</html>
"""
        (historico_root / "index.html").write_text(html, encoding="utf-8")
        return rows

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
            "version": 1,
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
        latest_status = str(latest_block.get("web_status") or manifest.get("status") or "partial").upper()
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
                "<h2>Avisos del dia</h2>"
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

        ads_html = ""
        if self.ads_enabled and self.ads_slots:
            slot_blocks = "".join(
                f"<div class='ad-slot' data-slot='{slot}'>Slot publicitario: {slot}</div>" for slot in self.ads_slots
            )
            ads_html = (
                "<section class='card' id='home-ads-panel'><h2>Publicidad</h2>"
                "<p class='muted' id='home-ads-meta'>Acepta cookies para cargar anuncios relevantes.</p>"
                f"<div class='ads-grid' id='home-ads-grid'>{slot_blocks}</div></section>"
            )

        ads_config = {
            "enabled": self.ads_enabled,
            "provider": self.ads_provider,
            "client_id": self.ads_client_id,
            "slots": self.ads_slots,
        }
        ads_config_json = json.dumps(ads_config, ensure_ascii=False).replace("</", "<\\/")
        quality_block = manifest.get("quality") if isinstance(manifest.get("quality"), dict) else {}
        coverage_total = quality_block.get("coverage_total_pct")
        coverage_label = "N/D" if coverage_total is None else f"{float(coverage_total):.1f}%"
        publication_status = str(quality_block.get("publication_status") or "N/D")
        validation_status = str(quality_block.get("official_validation_status") or "N/D")
        policy_summary = str(manifest.get("publication_policy_summary") or _PUBLICATION_POLICY_SUMMARY)
        has_data = bool(latest_block.get("has_data", latest.metadata.get("has_data", False)))
        data_label = "Con datos recientes" if has_data else "Sin datos recientes"
        html = f"""<!doctype html>
<html lang='es'>
<head>
{self._meta_head('La Anonima Tracker', 'Tracker publico de precios historicos e inflacion comparada.', '/')}
{self._analytics_head_script()}
<style>{self._shell_css()}</style>
</head>
<body>
<main class='shell'>
  {self._top_nav(active='home')}
  <section class='card'>
    <div class='hero-grid'>
      <div class='hero-stack'>
        <h1>Estado de hoy: precios en seguimiento diario</h1>
        <div class='status-chip'>Estado del sitio: {latest_status}</div>
        <p class='muted'>Rango activo: {from_month} a {to_month}</p>
        <p class='muted'>Ultima actualizacion: {latest_generated} | Proxima corrida estimada: {manifest.get('next_update_eta')}</p>
        <p class='muted'>Macro oficial: {publication_status} | validacion: {validation_status} | cobertura: {coverage_label}</p>
        <div class='cta-row'>
          <a id='cta-open-tracker' href='/tracker/' class='btn btn-primary'>Abrir tracker</a>
          <a href='/historico/' class='btn btn-secondary'>Explorar historico</a>
        </div>
      </div>
      <div class='metric-strip'>
        <div class='metric-tile'><strong>Estado datos</strong><span>{data_label}</span></div>
        <div class='metric-tile'><strong>Cobertura</strong><span>{coverage_label}</span></div>
        <div class='metric-tile'><strong>Publicacion</strong><span>{policy_summary}</span></div>
      </div>
    </div>
  </section>

  <section class='card'>
    <h2>Resumen operativo</h2>
    <div class='kpis'>{self._kpi_cards_from_metadata(latest.metadata)}</div>
  </section>

  <section class='grid-2'>
    <article class='card'>
      <h2>Como leer en 30 segundos</h2>
      <ul class='clean muted'>
        <li>Revisa el bloque macro para contexto general.</li>
        <li>Filtra por categoria y compara variacion nominal y real.</li>
        <li>Si el estado es parcial, usa los avisos para interpretar limites.</li>
      </ul>
    </article>
    <article class='card'>
      <h2>Transparencia</h2>
      <ul class='clean muted'>
        <li>Frecuencia objetivo diaria con estado fresh/partial/stale.</li>
        <li>Si falta IPC oficial, se publica con alerta visible.</li>
        <li>Cada periodo queda trazable en Historico.</li>
      </ul>
    </article>
  </section>

  {warnings_html}
  {ads_html}
  {premium_html}

  <section class='card muted'>
    <strong>Legales:</strong>
    <a href='/legal/privacy.html'>Privacidad</a> |
    <a href='/legal/terms.html'>Terminos</a> |
    <a href='/legal/cookies.html'>Cookies</a> |
    <a href='/legal/ads.html'>Publicidad</a>
  </section>
</main>
{self._consent_banner_html()}
{self._consent_script()}
<script>
(function(){{
  const ADS={ads_config_json};
  const CONSENT_KEY='laanonima_tracker_cookie_consent_v1';
  function track(eventName, props){{
    if(typeof window.plausible==='function'){{
      try{{ window.plausible(eventName, {{ props: props || {{}} }}); }}catch(_e){{}}
    }}
  }}
  function ensureAdSense(clientId){{
    if(!clientId) return;
    if(document.getElementById('adsense-script')) return;
    const existing=document.querySelector(\"script[src*='pagead2.googlesyndication.com/pagead/js/adsbygoogle.js']\");
    if(existing) return;
    const script=document.createElement('script');
    script.id='adsense-script';
    script.async=true;
    script.src='https://pagead2.googlesyndication.com/pagead/js/adsbygoogle.js?client='+encodeURIComponent(clientId);
    script.crossOrigin='anonymous';
    document.head.appendChild(script);
  }}
  function renderAds(consent){{
    const panel=document.getElementById('home-ads-panel');
    const grid=document.getElementById('home-ads-grid');
    const meta=document.getElementById('home-ads-meta');
    if(!panel || !grid) return;
    if(!ADS.enabled){{
      panel.style.display='none';
      return;
    }}
    panel.style.display='';
    if(consent!=='accepted'){{
      if(meta) meta.textContent='Acepta cookies para habilitar anuncios.';
      return;
    }}
    if((ADS.provider||'').toLowerCase()!=='adsense'){{
      if(meta) meta.textContent='Proveedor de anuncios no soportado en Home.';
      return;
    }}
    ensureAdSense(ADS.client_id);
    if(meta) meta.textContent='Anuncios activos (AdSense).';
    const slots=Array.isArray(ADS.slots)?ADS.slots:[];
    grid.innerHTML='';
    slots.forEach((slotId)=>{{
      const wrap=document.createElement('div');
      wrap.className='ad-slot';
      const ins=document.createElement('ins');
      ins.className='adsbygoogle';
      ins.style.display='block';
      ins.setAttribute('data-ad-client', ADS.client_id || '');
      ins.setAttribute('data-ad-slot', String(slotId||'').replace(/[^0-9]/g,'') || '0000000000');
      ins.setAttribute('data-ad-format', 'auto');
      ins.setAttribute('data-full-width-responsive', 'true');
      wrap.appendChild(ins);
      grid.appendChild(wrap);
      try{{ (window.adsbygoogle = window.adsbygoogle || []).push({{}}); }}catch(_e){{}}
    }});
  }}
  window.__laTrackerOnConsentChanged=function(state){{
    renderAds(state);
  }};
  let current='rejected';
  try{{ current=localStorage.getItem(CONSENT_KEY)||'rejected'; }}catch(_e){{}}
  renderAds(current);
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
            + self._meta_head("404 | La Anonima Tracker", "Pagina no encontrada.", "/404.html")
            + f"<style>{self._shell_css()}</style></head><body><main class='shell'>"
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
                + self._analytics_head_script()
                + f"<style>{self._shell_css()}</style></head><body>"
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
                "<p class='muted'>El estado del sitio puede figurar como fresh, partial o stale segun cobertura y disponibilidad de fuentes oficiales.</p>"
                f"<p class='muted'>Politica de publicacion vigente: <code>{_PUBLICATION_POLICY}</code> ({_PUBLICATION_POLICY_SUMMARY.lower()})</p>"
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
                "<p class='muted'>El tracker calcula variaciones con observaciones reales de precios y compara con IPC oficial INDEC cuando hay solape temporal.</p>"
                "<p class='muted'>Se informan estados de calidad (fresh/stale/partial) y cobertura para interpretar correctamente los resultados.</p>"
                f"<p class='muted'>Regla de publicacion: <code>{_PUBLICATION_POLICY}</code> ({_PUBLICATION_POLICY_SUMMARY.lower()})</p>"
            ),
            "/metodologia/",
            "Metodologia del tracker: recoleccion, calculo y comparativa de series.",
        )
        contacto = render_shell_page(
            "Contacto",
            "contacto",
            (
                "<p class='muted'>Consultas editoriales, colaboraciones o propuestas comerciales.</p>"
                f"<p class='muted'>Correo: <a href='mailto:{self.contact_email}'>{self.contact_email}</a></p>"
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


def run_web_publish(
    config_path: Optional[str] = None,
    from_month: Optional[str] = None,
    to_month: Optional[str] = None,
    basket_type: str = "all",
    benchmark_mode: str = "ipc",
    analysis_depth: str = "analyst",
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
