"""Static web publication pipeline for low-cost public deployment."""

from __future__ import annotations

import json
import re
import shutil
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.config_loader import load_config
from src.reporting import run_report

_REPORT_METADATA_RE = re.compile(r"report_interactive_(\d{6})_to_(\d{6})_(\d{8}_\d{6})\.metadata\.json$")


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

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        deployment = config.get("deployment", {}) if isinstance(config.get("deployment"), dict) else {}
        self.output_dir = Path(str(deployment.get("output_dir", "public")))
        self.public_base_url = str(deployment.get("public_base_url", "https://example.com")).rstrip("/")
        self.keep_history_months = max(1, int(deployment.get("keep_history_months", 24)))
        self.fresh_max_hours = float(deployment.get("fresh_max_hours", 36))
        self.schedule_utc = str(deployment.get("schedule_utc", "09:10"))
        analysis_cfg = config.get("analysis", {}) if isinstance(config.get("analysis"), dict) else {}
        self.report_dir = Path(str(analysis_cfg.get("reports_dir", "data/analysis/reports")))

        ads_cfg = config.get("ads", {}) if isinstance(config.get("ads"), dict) else {}
        self.ads_enabled = bool(ads_cfg.get("enabled", False))
        self.ads_provider = str(ads_cfg.get("provider", "adsense_placeholder"))
        self.ads_slots = [str(v) for v in (ads_cfg.get("slots") or ["header", "inline", "sidebar", "footer"]) if str(v).strip()]
        self.ads_client_id = str(ads_cfg.get("client_id_placeholder", "ca-pub-xxxxxxxxxxxxxxxx"))

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
            "if(ok){ok.addEventListener('click',function(){localStorage.setItem(k,'accepted');b.style.display='none';});}"
            "if(no){no.addEventListener('click',function(){localStorage.setItem(k,'rejected');b.style.display='none';});}"
            "})();"
            "</script>"
        )

    @staticmethod
    def _consent_banner_html() -> str:
        return (
            "<div id='cookie-banner' style='position:fixed;left:12px;right:12px;bottom:12px;z-index:9999;"
            "background:#12233a;color:#f3f7ff;border-radius:12px;padding:12px;box-shadow:0 10px 24px rgba(0,0,0,.22)'>"
            "<div style='display:flex;flex-wrap:wrap;gap:10px;align-items:center;justify-content:space-between'>"
            "<div style='max-width:760px'>Usamos cookies para preferencias basicas y, cuando corresponda, medicion publicitaria.</div>"
            "<div style='display:flex;gap:8px'>"
            "<button id='cookie-reject' style='border:1px solid #5f6f86;background:#1f3552;color:#e6eefb;padding:8px 10px;border-radius:8px;cursor:pointer'>Rechazar</button>"
            "<button id='cookie-accept' style='border:1px solid #1f7d7a;background:#23a19a;color:white;padding:8px 10px;border-radius:8px;cursor:pointer'>Aceptar</button>"
            "</div></div></div>"
        )

    @staticmethod
    def _read_json(path: Path) -> Dict[str, Any]:
        return json.loads(path.read_text(encoding="utf-8"))

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

    def collect_latest_report(self, preferred_html: Optional[str] = None, preferred_metadata: Optional[str] = None) -> ReportEntry:
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
            "<meta name='theme-color' content='#0b607a'/>"
            f"<link rel='canonical' href='{canonical_url}'/>"
            "<link rel='icon' type='image/svg+xml' href='/favicon.svg'/>"
            "<link rel='manifest' href='/site.webmanifest'/>"
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
    <stop offset='0%' stop-color='#0b607a'/>
    <stop offset='100%' stop-color='#0b8b85'/>
  </linearGradient>
</defs>
<rect width='64' height='64' rx='14' fill='url(#g)'/>
<path d='M15 45V19h8l9 13 9-13h8v26h-8V31l-9 12-9-12v14z' fill='#fff'/>
</svg>
"""
        og_card_svg = """<svg xmlns='http://www.w3.org/2000/svg' width='1200' height='630' viewBox='0 0 1200 630'>
<defs>
  <linearGradient id='bg' x1='0' y1='0' x2='1' y2='1'>
    <stop offset='0%' stop-color='#0a5671'/>
    <stop offset='100%' stop-color='#0b8b85'/>
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
            "background_color": "#f3f7fb",
            "theme_color": "#0b607a",
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
  --bg:#f3f7fb;
  --panel:#ffffff;
  --line:#d8e3f0;
  --line-strong:#c3d4e7;
  --text:#15233a;
  --muted:#5c6c80;
  --primary:#0b607a;
  --primary-strong:#084b61;
  --accent:#0b8b85;
  --shadow:0 2px 8px rgba(14,34,57,.08), 0 18px 34px rgba(14,34,57,.07);
}
*{box-sizing:border-box}
body{
  margin:0;
  color:var(--text);
  font-family:"Aptos","Segoe UI Variable","Trebuchet MS",sans-serif;
  background:
    radial-gradient(1000px 360px at -8% -4%, rgba(11,96,122,.12) 0%, rgba(11,96,122,0) 60%),
    radial-gradient(950px 320px at 106% -3%, rgba(11,139,133,.10) 0%, rgba(11,139,133,0) 56%),
    linear-gradient(180deg,#eef4fa 0%, var(--bg) 32%, var(--bg) 100%);
}
main.shell{
  max-width:1080px;
  margin:0 auto;
  padding:18px;
  display:grid;
  gap:14px;
}
.card{
  background:var(--panel);
  border:1px solid var(--line);
  border-radius:14px;
  padding:14px 16px;
  box-shadow:var(--shadow);
}
.topbar{
  display:flex;
  align-items:center;
  justify-content:space-between;
  gap:12px;
  flex-wrap:wrap;
}
.brand{
  color:#173455;
  font-weight:800;
  text-decoration:none;
  letter-spacing:.01em;
}
.nav{
  display:flex;
  gap:8px;
  flex-wrap:wrap;
}
.nav a{
  display:inline-flex;
  align-items:center;
  min-height:32px;
  padding:7px 11px;
  border-radius:999px;
  border:1px solid #c8d8e8;
  background:#f7fbff;
  color:#2e4e6f;
  text-decoration:none;
  font-size:.84rem;
  font-weight:700;
}
.nav a:hover{
  background:#edf5ff;
  border-color:#b2c9e0;
}
.nav a.active{
  background:linear-gradient(180deg,var(--primary),var(--primary-strong));
  border-color:var(--primary-strong);
  color:#fff;
}
h1,h2{
  margin:0 0 8px 0;
  letter-spacing:.01em;
}
h1{font-size:1.55rem}
h2{font-size:1.08rem}
.muted{color:var(--muted)}
.status-chip{
  display:inline-flex;
  align-items:center;
  min-height:30px;
  padding:4px 10px;
  border-radius:999px;
  border:1px solid #cad8e7;
  background:#f1f7fe;
  color:#2a4b6c;
  font-size:.8rem;
  font-weight:700;
}
.cta-row{
  display:flex;
  flex-wrap:wrap;
  gap:9px;
}
.btn{
  display:inline-flex;
  align-items:center;
  justify-content:center;
  min-height:36px;
  padding:8px 14px;
  border-radius:10px;
  text-decoration:none;
  font-weight:700;
  border:1px solid transparent;
}
.btn-primary{
  background:linear-gradient(180deg,var(--primary),var(--primary-strong));
  color:#fff;
}
.btn-secondary{
  background:#f8fbff;
  border-color:#bfd1e4;
  color:#2f4f71;
}
.grid-2{
  display:grid;
  gap:12px;
  grid-template-columns:repeat(2,minmax(0,1fr));
}
.kpis{
  display:grid;
  gap:10px;
  grid-template-columns:repeat(auto-fit,minmax(180px,1fr));
}
.kpi{
  border:1px solid #d2deed;
  border-radius:11px;
  padding:10px;
  background:#f7fbff;
  display:grid;
  gap:4px;
}
.kpi strong{font-size:.8rem;color:#425873}
.kpi span{font-size:1.02rem;font-weight:800}
.list-tools{
  display:flex;
  align-items:center;
  justify-content:space-between;
  gap:10px;
  flex-wrap:wrap;
  margin-top:8px;
}
.list-tools input{
  min-height:36px;
  min-width:230px;
  border:1px solid #c5d5e6;
  border-radius:10px;
  padding:8px 10px;
}
.history-list{
  border:1px solid var(--line);
  border-radius:11px;
  overflow:hidden;
}
.history-list a{
  display:flex;
  align-items:center;
  justify-content:space-between;
  gap:10px;
  padding:11px 12px;
  border-bottom:1px solid #e6edf6;
  text-decoration:none;
  color:#244566;
  background:#fff;
}
.history-list a:last-child{border-bottom:none}
.history-list a:hover{background:#f3f9ff}
.meta-line{
  font-size:.82rem;
  color:#5f6d81;
}
.ads-grid{
  display:grid;
  gap:10px;
  grid-template-columns:repeat(auto-fit,minmax(210px,1fr));
}
.ad-slot{
  border:1px dashed #8da5c1;
  border-radius:10px;
  min-height:90px;
  background:#f5f9ff;
  display:flex;
  align-items:center;
  justify-content:center;
  color:#355574;
  font-weight:700;
}
ul.clean{
  margin:0;
  padding-left:18px;
}
ul.clean li{margin:6px 0}
@media (max-width:900px){
  main.shell{padding:12px}
  .grid-2{grid-template-columns:1fr}
}
@media (max-width:620px){
  h1{font-size:1.28rem}
  .nav a{flex:1 1 calc(50% - 6px);justify-content:center}
  .list-tools input{min-width:100%}
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
                }
            )

        list_rows = []
        for item in rows:
            list_rows.append(
                "<a href='{path}' data-month='{month}'>"
                "<strong>{month}</strong>"
                "<span class='meta-line'>{generated}</span>"
                "</a>".format(
                    path=item["report_path"],
                    month=item["month"],
                    generated=item["generated_at"],
                )
            )
        list_html = "".join(list_rows) if list_rows else "<p class='muted'>Sin reportes historicos disponibles.</p>"

        html = f"""<!doctype html>
<html lang='es'>
<head>
{self._meta_head('Historico | La Anonima Tracker', 'Historico mensual de reportes publicados del tracker.', '/historico/')}
<style>{self._shell_css()}</style>
</head>
<body>
<main class='shell'>
  {self._top_nav(active='historico')}
  <section class='card'>
    <h1>Historico de reportes</h1>
    <p class='muted'>Acceso por periodo publicado para comparar cambios en el tiempo.</p>
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

        coverage = latest.metadata.get("coverage", {}) if isinstance(latest.metadata.get("coverage"), dict) else {}
        publication = (
            latest.metadata.get("data_quality", {}).get("publication_status", {})
            if isinstance(latest.metadata.get("data_quality"), dict)
            else {}
        )
        official_validation_status = (
            publication.get("validation_status")
            or publication.get("metrics", {}).get("official_validation_status")
            or "unknown"
        )

        manifest = {
            "version": 1,
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
            "status": web_status,
            "latest_report_path": "/tracker/",
            "latest_metadata_path": "/data/latest.metadata.json",
            "history": history_rows,
            "quality": {
                "coverage_total_pct": coverage.get("coverage_total_pct"),
                "publication_status": publication.get("status") if isinstance(publication, dict) else None,
                "official_validation_status": official_validation_status,
            },
            "next_update_eta": self._next_update_eta().strftime("%Y-%m-%d %H:%M:%S UTC"),
            "is_stale": bool(is_stale),
            "ads": {
                "enabled": self.ads_enabled,
                "provider": self.ads_provider,
                "slots": self.ads_slots,
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
            if value is None:
                label = "N/D"
            else:
                label = f"{value:.2f}{unit}" if isinstance(value, (int, float)) else str(value)
            cards.append(f"<div class='kpi'><strong>{title}</strong><span>{label}</span></div>")
        return "".join(cards)

    def build_home_page(self, manifest: Dict[str, Any], latest: ReportEntry) -> None:
        range_block = latest.metadata.get("range") if isinstance(latest.metadata.get("range"), dict) else {}
        from_month = range_block.get("from") or "N/D"
        to_month = range_block.get("to") or "N/D"

        premium_html = ""
        if self.premium_enabled and self.premium_features:
            premium_items = "".join(f"<li>{f}</li>" for f in self.premium_features)
            premium_html = (
                "<section class='card'><h2>Proximamente (Premium)</h2>"
                "<p class='muted'>Bloques listos para activacion futura sin rehacer la arquitectura.</p>"
                f"<ul class='clean muted'>{premium_items}</ul></section>"
            )

        ads_html = ""
        if self.ads_enabled and self.ads_slots:
            slot_blocks = "".join(
                f"<div class='ad-slot' data-slot='{slot}'>Espacio publicitario: {slot}</div>" for slot in self.ads_slots
            )
            ads_html = (
                "<section class='card'><h2>Publicidad</h2>"
                "<p class='muted'>Slots preparados para monetizacion.</p>"
                f"<div class='ads-grid'>{slot_blocks}</div></section>"
            )

        status_chip = str(manifest.get("status", "unknown")).upper()
        html = f"""<!doctype html>
<html lang='es'>
<head>
{self._meta_head('La Anonima Tracker', 'Tracker publico de precios historicos e inflacion comparada.', '/')}
<style>{self._shell_css()}</style>
</head>
<body>
<main class='shell'>
  {self._top_nav(active='home')}
  <section class='card'>
    <h1>Monitor publico de precios historicos</h1>
    <div class='status-chip'>Estado del sitio: {status_chip}</div>
    <p class='muted'>Rango activo: {from_month} a {to_month}</p>
    <p class='muted'>Ultima actualizacion: {latest.metadata.get('generated_at','N/D')} | Proxima corrida estimada: {manifest.get('next_update_eta')}</p>
    <div class='cta-row'>
      <a href='/tracker/' class='btn btn-primary'>Abrir tracker</a>
      <a href='/historico/' class='btn btn-secondary'>Explorar historico</a>
    </div>
  </section>

  <section class='card'>
    <h2>Resumen operativo</h2>
    <div class='kpis'>{self._kpi_cards_from_metadata(latest.metadata)}</div>
  </section>

  <section class='grid-2'>
    <article class='card'>
      <h2>Valor publico</h2>
      <ul class='clean muted'>
        <li>Comparacion mensual de canasta propia contra referencia oficial.</li>
        <li>Reportes navegables por periodo y trazabilidad de publicacion.</li>
        <li>Estructura preparada para monetizacion y evolucion premium.</li>
      </ul>
    </article>
    <article class='card'>
      <h2>Hoja de ruta preparada</h2>
      <ul class='clean muted'>
        <li>Base actual: static-first de bajo costo.</li>
        <li>Escala natural: API publica, cache y rate-limit por etapas.</li>
        <li>Futuro: features premium reales sin rehacer frontend.</li>
      </ul>
    </article>
  </section>

  {ads_html}
  {premium_html}

  <section class='card muted'>
    <strong>Legales:</strong>
    <a href='/legal/privacy.html'>Privacidad</a> |
    <a href='/legal/terms.html'>Terminos</a>
  </section>
</main>
{self._consent_banner_html()}
{self._consent_script()}
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
  Content-Security-Policy: default-src 'self'; img-src 'self' data: https:; style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline' https://cdn.plot.ly https://pagead2.googlesyndication.com https://www.googletagmanager.com; connect-src 'self' https:;

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
        redirects = """/tracker /tracker/ 301
/historico /historico/ 301
/metodologia /metodologia/ 301
/contacto /contacto/ 301
"""
        not_found = (
            "<!doctype html><html lang='es'><head>"
            + self._meta_head("404 | La Anonima Tracker", "Pagina no encontrada.", "/404.html")
            + "<style>body{margin:0;font-family:Segoe UI,Arial,sans-serif;background:#f4f7fb;color:#142033}"
            "main{max-width:760px;margin:0 auto;padding:24px}.card{background:#fff;border:1px solid #d8e2ef;"
            "border-radius:14px;padding:18px}a{color:#0b607a}</style></head><body><main><section class='card'>"
            "<h1>404</h1><p>No encontramos la pagina solicitada.</p><p><a href='/'>Volver al inicio</a></p>"
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
                "<p class='muted'>Este sitio publica series agregadas de precios e indicadores. "
                "Podemos almacenar preferencias locales (filtros y consentimiento) para mejorar la experiencia.</p>"
                "<p class='muted'>Cuando la publicidad este activa, se aplicaran politicas y consentimientos correspondientes.</p>"
                "<p class='muted'>Contacto: <a href='/contacto/'>/contacto/</a></p>"
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
            ),
            "/legal/terms.html",
            "Terminos de uso del tracker publico de precios.",
        )
        metodologia = render_shell_page(
            "Metodologia",
            "metodologia",
            (
                "<p class='muted'>El tracker calcula variaciones con observaciones reales de precios y compara con IPC oficial INDEC cuando hay solape temporal.</p>"
                "<p class='muted'>Se informan estados de calidad (fresh/stale/partial) y cobertura para interpretar correctamente los resultados.</p>"
            ),
            "/metodologia/",
            "Metodologia del tracker: recoleccion, calculo y comparativa de series.",
        )
        contacto = render_shell_page(
            "Contacto",
            "contacto",
            (
                "<p class='muted'>Consultas editoriales, colaboraciones o propuestas comerciales.</p>"
                "<p class='muted'>Correo sugerido: contacto@tu-dominio.com (reemplazar en produccion).</p>"
            ),
            "/contacto/",
            "Canales de contacto para consultas y propuestas comerciales.",
        )

        (legal_dir / "privacy.html").write_text(privacy, encoding="utf-8")
        (legal_dir / "terms.html").write_text(terms, encoding="utf-8")
        (self.output_dir / "metodologia" / "index.html").parent.mkdir(parents=True, exist_ok=True)
        (self.output_dir / "metodologia" / "index.html").write_text(metodologia, encoding="utf-8")
        (self.output_dir / "contacto" / "index.html").parent.mkdir(parents=True, exist_ok=True)
        (self.output_dir / "contacto" / "index.html").write_text(contacto, encoding="utf-8")

        ads_txt = f"google.com, {self.ads_client_id}, DIRECT, f08c47fec0942fa0\n"
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

    def publish(self, preferred_html: Optional[str] = None, preferred_metadata: Optional[str] = None) -> PublishWebResult:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._write_brand_assets()

        latest = self.collect_latest_report(preferred_html=preferred_html, preferred_metadata=preferred_metadata)
        copied = self._copy_latest_artifacts(latest)
        history_rows = self.build_history_index()
        manifest = self.build_manifest(latest, history_rows)

        # Enrich latest metadata with website publication fields.
        latest_meta_path = Path(copied["latest_metadata_path"])
        latest_meta = self._read_json(latest_meta_path)
        latest_meta["web_status"] = manifest.get("status")
        latest_meta["is_stale"] = bool(manifest.get("is_stale", False))
        latest_meta["next_update_eta"] = manifest.get("next_update_eta")
        latest_meta["ad_slots_enabled"] = bool(self.ads_enabled)
        latest_meta["premium_placeholders_enabled"] = bool(self.premium_enabled)
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
    """Generate public static website artifacts from latest interactive report."""
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
