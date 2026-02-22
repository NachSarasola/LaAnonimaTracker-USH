const p = __PAYLOAD__;
const defaults = p.ui_defaults || {};
const STORAGE_KEY = "laanonima_tracker_report_state_v2";
const COOKIE_KEY = "laanonima_tracker_cookie_consent_v1";
const ONBOARDING_KEY = "laanonima_tracker_mobile_onboarding_v1";
const COLORS = ["#005f73", "#9b2226", "#ee9b00", "#0a9396", "#3d405b", "#588157", "#7f5539", "#6a4c93", "#1d3557"];
const st = {
  query: defaults.query || "",
  cba_filter: defaults.cba_filter || "all",
  category: defaults.category || "all",
  sort_by: defaults.sort_by || "alphabetical",
  base_month: defaults.base_month || "",
  selected_products: [...(defaults.selected_products || [])],
  price_mode: defaults.price_mode || "nominal",
  show_real_column: !!defaults.show_real_column,
  macro_scope: defaults.macro_scope || "general",
  macro_region: defaults.macro_region || p.macro_default_region || "patagonia",
  macro_category: defaults.macro_category || "",
  view: defaults.view || "executive",
  band_product: defaults.band_product || "",
  page_size: Number(defaults.page_size || 25),
  current_page: Number(defaults.current_page || 1)
};

const el = {
  premiumPanel: document.getElementById("premium-panel"),
  premiumFeatures: document.getElementById("premium-features"),
  quickGuide: document.getElementById("quick-guide"),
  mobileOnboarding: document.getElementById("mobile-onboarding"),
  onboardingGoto: document.getElementById("onboarding-goto"),
  onboardingClose: document.getElementById("onboarding-close"),
  cookieBanner: document.getElementById("cookie-banner"),
  cookieAccept: document.getElementById("cookie-accept"),
  cookieReject: document.getElementById("cookie-reject"),
  q: document.getElementById("q"),
  clearSearch: document.getElementById("clear-search"),
  cba: document.getElementById("cba"),
  cat: document.getElementById("cat"),
  ord: document.getElementById("ord"),
  mb: document.getElementById("mbase"),
  sel: document.getElementById("sel"),
  selectionMeta: document.getElementById("selection-meta"),
  tb: document.getElementById("tb"),
  filtersPanel: document.getElementById("filters-panel"),
  reset: document.getElementById("reset"),
  copyLink: document.getElementById("copy-link"),
  copyLinkStatus: document.getElementById("copy-link-status"),
  showReal: document.getElementById("show-real"),
  modeNominal: document.getElementById("mode-nominal"),
  modeReal: document.getElementById("mode-real"),
  quickUp: document.getElementById("quick-up"),
  quickDown: document.getElementById("quick-down"),
  quickFlat: document.getElementById("quick-flat"),
  kpiGrid: document.getElementById("kpi-grid"),
  qualityBadge: document.getElementById("quality-badge"),
  qualityCoverage: document.getElementById("quality-coverage"),
  qualityPanelSize: document.getElementById("quality-panel-size"),
  qualityMacro: document.getElementById("quality-macro"),
  qualityIpc: document.getElementById("quality-ipc"),
  qualitySegments: document.getElementById("quality-segments"),
  qualityPolicy: document.getElementById("quality-policy"),
  qualitySummary: document.getElementById("quality-summary"),
  warnings: document.getElementById("warnings"),
  qualityPanel: document.getElementById("quality-panel"),
  panelSecondary: document.getElementById("panel-secondary"),
  mainChartPanel: document.getElementById("main-chart-panel"),
  chartMain: document.getElementById("chart-main"),
  legendMain: document.getElementById("legend-main"),
  macroScope: document.getElementById("macro-scope"),
  macroRegion: document.getElementById("macro-region"),
  macroCategory: document.getElementById("macro-category"),
  macroStatus: document.getElementById("macro-status"),
  macroDetailText: document.getElementById("macro-detail-text"),
  macroNotice: document.getElementById("macro-notice"),
  chartSecondary: document.getElementById("chart-secondary"),
  legendSecondary: document.getElementById("legend-secondary"),
  panelBands: document.getElementById("panel-bands"),
  bandProduct: document.getElementById("band-product"),
  bandMeta: document.getElementById("band-meta"),
  chartBands: document.getElementById("chart-bands"),
  legendBands: document.getElementById("legend-bands"),
  thVarReal: document.getElementById("th-var-real"),
  tableMeta: document.getElementById("table-meta"),
  pageSize: document.getElementById("page-size"),
  pagePrev: document.getElementById("page-prev"),
  pageNext: document.getElementById("page-next"),
  pageInfo: document.getElementById("page-info"),
  exportCsv: document.getElementById("export-csv"),
  activeFilters: document.getElementById("active-filters"),
  freshnessMeta: document.getElementById("freshness-meta")
};

const ADSENSE_SCRIPT_ID = "laanonima-tracker-adsense";

function trackEvent(name, props) {
  if (typeof window.plausible !== "function") return;
  try {
    window.plausible(name, { props: props || {} });
  } catch (_e) { }
}

function consentState() {
  try {
    return window.localStorage?.getItem?.(COOKIE_KEY) || "";
  } catch (_e) {
    return "";
  }
}

// Keep unicode range escaped to avoid encoding issues in generated standalone HTML.
const norm = v => String(v || "").toLowerCase().normalize("NFD").replace(/[\\u0300-\\u036f]/g, "");
const esc = v => String(v || "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/\"/g, "&quot;").replace(/'/g, "&#39;");
const money = v => {
  if (v == null || Number.isNaN(Number(v))) return "N/D";
  return new Intl.NumberFormat("es-AR", {
    style: "currency",
    currency: "ARS",
    minimumFractionDigits: 0,
    maximumFractionDigits: 2
  }).format(Number(v));
};
const pct = v => v == null || Number.isNaN(Number(v)) ? "N/D" : `${Number(v).toFixed(2)}%`;
const pctSigned = v => {
  if (v == null || Number.isNaN(Number(v))) return "N/D";
  const n = Number(v);
  const sign = n > 0 ? "+" : "";
  return `${sign}${n.toFixed(2)}%`;
};
const monthLabel = v => String(v || "");
const fmtNum = v => v == null || Number.isNaN(Number(v)) ? "N/D" : Number(v).toFixed(2);
const fmtAxisNum = v => {
  if (v == null || Number.isNaN(Number(v))) return "N/D";
  const n = Number(v);
  const decimals = Math.abs(n) >= 1000 ? 0 : 2;
  return new Intl.NumberFormat("es-AR", { maximumFractionDigits: decimals }).format(n);
};
const formatMetricValue = (value, kind = "number") => {
  if (value == null || Number.isNaN(Number(value))) return "N/D";
  const n = Number(value);
  if (kind === "ars") return money(n);
  if (kind === "pp") return `${fmtAxisNum(n)} pp`;
  if (kind === "pct") return `${fmtAxisNum(n)}%`;
  if (kind === "index") return fmtAxisNum(n);
  return fmtAxisNum(n);
};
const fmtDate = v => {
  const d = v instanceof Date ? v : new Date(v);
  if (Number.isNaN(d.getTime())) return "N/D";
  return new Intl.DateTimeFormat("es-AR", { year: "numeric", month: "2-digit", day: "2-digit" }).format(d);
};
const fmtMonthTick = v => {
  const d = v instanceof Date ? v : new Date(v);
  if (Number.isNaN(d.getTime())) return "N/D";
  return new Intl.DateTimeFormat("es-AR", { month: "short", year: "2-digit" }).format(d);
};
const trendClass = v => {
  if (v == null || Number.isNaN(Number(v))) return "var-flat";
  if (Number(v) > 0) return "var-up";
  if (Number(v) < 0) return "var-down";
  return "var-flat";
};
const trendIcon = v => {
  if (v == null || Number.isNaN(Number(v))) return "\u00b7";
  if (Number(v) > 0) return "\u2191";
  if (Number(v) < 0) return "\u2193";
  return "\u2192";
};

function normalizePresentation(value) {
  const raw = String(value || "").trim();
  if (!raw || raw.toUpperCase() === "N/D") return "N/D";
  const normalized = raw
    .replace(",", ".")
    .replace(/c\.\s*c\./gi, "cc")
    .replace(/cm\s*3/gi, "cm3")
    .replace(/cm³/gi, "cm3")
    .replace(/\s+/g, " ")
    .trim();
  const match = normalized.match(/^([0-9]+(?:\.[0-9]+)?)\s*([a-zA-Z0-9\.]+)\b(.*)$/);
  if (!match) {
    return raw.replace(/(\d+)[.,]0+(?=\s*[a-zA-Z0-9]|$)/g, "$1");
  }
  const qty = Number(match[1]);
  if (!Number.isFinite(qty)) return raw;

  const unitToken = String(match[2] || "").toLowerCase().replace(/\./g, "").replace("³", "3");
  const unit = norm(unitToken).replace(/[^a-z0-9]/g, "");
  const tail = String(match[3] || "").trim();
  const suffix = tail ? ` ${tail}` : "";
  const qtyLabel = new Intl.NumberFormat("es-AR", { maximumFractionDigits: 3, useGrouping: false }).format(qty);
  const canonicalUnit = (() => {
    if (unit === "kg" || unit === "kilo" || unit === "kilos") return "kg";
    if (unit === "g" || unit === "gr" || unit === "gramo" || unit === "gramos") return "g";
    if (unit === "l" || unit === "lt" || unit === "litro" || unit === "litros") return "l";
    if (unit === "cc" || unit === "cm3" || unit === "centimetrocubico" || unit === "centimetroscubicos") return "ml";
    if (unit === "ml") return "ml";
    if (unit === "unidad" || unit === "unidades" || unit === "un") return "un";
    if (unit === "docena" || unit === "docenas" || unit === "doc") return "doc";
    return unitToken;
  })();

  if (qty > 0 && qty < 1 && (unit === "kg" || unit === "kilo" || unit === "kilos")) {
    return `${Math.round(qty * 1000)} g${suffix}`;
  }
  if (qty > 0 && qty < 1 && (unit === "l" || unit === "lt" || unit === "litro" || unit === "litros")) {
    return `${Math.round(qty * 1000)} ml${suffix}`;
  }
  return `${qtyLabel} ${canonicalUnit}${suffix}`;
}

function inferPresentationFromName(name) {
  const raw = String(name || "").trim();
  if (!raw) return "N/D";
  const fromQty = raw.match(/\bx\s*([0-9]+(?:[.,][0-9]+)?)\s*(kg|kilo|kilos|g|gr|gramos?|l|lt|litros?|ml|cc|c\.?c\.?|cm3|cm³|centimetros?\s*cubicos?|un|unidad(?:es)?|doc|docena(?:s)?)\b/i);
  if (fromQty) {
    return normalizePresentation(`${fromQty[1]} ${fromQty[2]}`);
  }
  if (/\(\s*kg\s*\)/i.test(raw) || /\bpor\s*kg\b/i.test(raw)) return "1 kg";
  return "N/D";
}

function resolvePresentation(row) {
  const direct = normalizePresentation(row.presentation || "N/D");
  if (direct !== "N/D") return direct;
  const inferred = inferPresentationFromName(row.product_name || "");
  if (inferred !== "N/D") return inferred;
  return "N/D";
}

function encodeHash() {
  const q = new URLSearchParams();
  q.set("q", st.query || "");
  q.set("cba", st.cba_filter);
  q.set("cat", st.category);
  q.set("ord", st.sort_by);
  q.set("mb", st.base_month || "");
  q.set("pm", st.price_mode);
  q.set("mscope", st.macro_scope || "general");
  q.set("mreg", st.macro_region || "patagonia");
  q.set("mcat", st.macro_category || "");
  q.set("bp", st.band_product || "");
  q.set("real", st.show_real_column ? "1" : "0");
  q.set("sel", (st.selected_products || []).join(","));
  q.set("ps", String(st.page_size || 25));
  q.set("pg", String(st.current_page || 1));
  return q.toString();
}

function applyHashState() {
  const raw = window.location.hash ? window.location.hash.slice(1) : "";
  if (!raw) return false;
  try {
    const q = new URLSearchParams(raw);
    st.query = q.get("q") ?? st.query;
    st.cba_filter = q.get("cba") ?? st.cba_filter;
    st.category = q.get("cat") ?? st.category;
    st.sort_by = q.get("ord") ?? st.sort_by;
    st.base_month = q.get("mb") ?? st.base_month;
    st.price_mode = q.get("pm") ?? st.price_mode;
    st.macro_scope = q.get("mscope") ?? st.macro_scope;
    st.macro_region = q.get("mreg") ?? st.macro_region;
    st.macro_category = q.get("mcat") ?? st.macro_category;
    st.band_product = q.get("bp") ?? st.band_product;
    st.show_real_column = (q.get("real") || "0") === "1";
    const sel = q.get("sel");
    if (sel) st.selected_products = sel.split(",").filter(Boolean);
    st.page_size = Number(q.get("ps") || st.page_size || 25);
    st.current_page = Number(q.get("pg") || st.current_page || 1);
    return true;
  } catch (_e) { return false; }
}

function loadState() {
  const hashUsed = applyHashState();
  if (hashUsed) return;
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return;
    const obj = JSON.parse(raw);
    if (!obj || typeof obj !== "object") return;
    Object.assign(st, obj);
  } catch (_e) { }
}

function saveState() {
  try { localStorage.setItem(STORAGE_KEY, JSON.stringify(st)); } catch (_e) { }
  const encoded = encodeHash();
  if (window.location.hash.slice(1) !== encoded) {
    history.replaceState(null, "", `#${encoded}`);
  }
}

function buildShareUrl() {
  const encoded = encodeHash();
  const current = window.location.href.split("#")[0];
  return `${current}#${encoded}`;
}

let _copyStatusTimer = null;
function setCopyStatus(message, isError = false) {
  if (!el.copyLinkStatus) return;
  el.copyLinkStatus.textContent = message || "";
  el.copyLinkStatus.classList.toggle("error", !!isError);
  if (_copyStatusTimer) {
    clearTimeout(_copyStatusTimer);
    _copyStatusTimer = null;
  }
  if (message) {
    _copyStatusTimer = window.setTimeout(() => {
      if (el.copyLinkStatus) {
        el.copyLinkStatus.textContent = "";
        el.copyLinkStatus.classList.remove("error");
      }
    }, 2600);
  }
}

async function copyCurrentViewLink() {
  saveState();
  const link = buildShareUrl();
  try {
    if (navigator.clipboard && navigator.clipboard.writeText) {
      await navigator.clipboard.writeText(link);
      setCopyStatus("Link copiado.");
      trackEvent("copy_view_link", { method: "clipboard" });
      return;
    }
  } catch (_e) { }
  try {
    const ta = document.createElement("textarea");
    ta.value = link;
    ta.style.position = "fixed";
    ta.style.left = "-9999px";
    document.body.appendChild(ta);
    ta.focus();
    ta.select();
    const ok = document.execCommand("copy");
    document.body.removeChild(ta);
    if (ok) {
      setCopyStatus("Link copiado.");
      trackEvent("copy_view_link", { method: "execCommand" });
      return;
    }
  } catch (_e) { }
  setCopyStatus("No se pudo copiar. Copialo manualmente desde la barra.", true);
  trackEvent("copy_view_link_failed", {});
}

const refsNom = {};
const refsReal = {};
for (const r of (p.monthly_reference || [])) {
  if (!refsNom[r.canonical_id]) refsNom[r.canonical_id] = {};
  if (!refsReal[r.canonical_id]) refsReal[r.canonical_id] = {};
  refsNom[r.canonical_id][r.month] = r.avg_price;
  refsReal[r.canonical_id][r.month] = r.avg_real_price;
}
const timelineById = {};
for (const t of (p.timeline || [])) {
  if (!timelineById[t.canonical_id]) timelineById[t.canonical_id] = [];
  timelineById[t.canonical_id].push(t);
}
const bandById = {};
for (const b of (p.candidate_bands || [])) {
  if (!bandById[b.canonical_id]) bandById[b.canonical_id] = [];
  bandById[b.canonical_id].push(b);
}
const candidateTripletsById = p.candidate_triplets_latest_by_id || {};
let _rowsCacheKey = "";
let _rowsCacheValue = [];
let _lastMainChartKey = "";
let _lastSecondaryChartKey = "";
let expandedProductId = "";
const lazyPanels = {
  bands_ready: false,
  quality_ready: false,
  observer: null
};

function calcVar(current, base) {
  if (base == null || Number(base) <= 0 || current == null) return null;
  return ((Number(current) - Number(base)) / Number(base)) * 100;
}

function filteredRows() {
  const cacheKey = [
    st.query, st.cba_filter, st.category, st.sort_by, st.base_month,
    p.snapshot?.length || 0,
  ].join("|");
  if (cacheKey === _rowsCacheKey) {
    return _rowsCacheValue;
  }

  let out = [...(p.snapshot || [])];
  const needle = norm(st.query);
  if (needle) {
    out = out.filter(r => norm(`${r.product_name} ${r.canonical_id}`).includes(needle));
  }
  if (st.cba_filter === "yes") out = out.filter(r => !!r.is_cba);
  if (st.cba_filter === "no") out = out.filter(r => !r.is_cba);
  if (st.category !== "all") out = out.filter(r => (r.category || "sin_categoria") === st.category);

  out = out.map(r => {
    const nomBase = refsNom[r.canonical_id]?.[st.base_month];
    const realBase = refsReal[r.canonical_id]?.[st.base_month];
    return {
      ...r,
      variation_nominal_pct: calcVar(r.current_price, nomBase),
      variation_real_pct: calcVar(r.current_real_price, realBase)
    };
  });

  if (st.sort_by === "price") {
    out.sort((a, b) => (a.current_price ?? Number.POSITIVE_INFINITY) - (b.current_price ?? Number.POSITIVE_INFINITY));
  } else if (st.sort_by === "var_nominal") {
    out.sort((a, b) => (b.variation_nominal_pct ?? Number.NEGATIVE_INFINITY) - (a.variation_nominal_pct ?? Number.NEGATIVE_INFINITY));
  } else if (st.sort_by === "var_real") {
    out.sort((a, b) => (b.variation_real_pct ?? Number.NEGATIVE_INFINITY) - (a.variation_real_pct ?? Number.NEGATIVE_INFINITY));
  } else {
    out.sort((a, b) => norm(a.product_name || a.canonical_id).localeCompare(norm(b.product_name || b.canonical_id), "es"));
  }
  _rowsCacheKey = cacheKey;
  _rowsCacheValue = out;
  return _rowsCacheValue;
}

function syncSelection(rows) {
  const ids = rows.map(r => r.canonical_id);
  const prev = new Set(st.selected_products || []);
  let selected = ids.filter(x => prev.has(x));
  if (!selected.length) {
    const pref = (defaults.selected_products || []).filter(x => ids.includes(x));
    selected = pref.length ? pref : ids.slice(0, 5);
  }
  st.selected_products = selected;
  el.sel.innerHTML = "";
  for (const r of rows) {
    const o = document.createElement("option");
    o.value = r.canonical_id;
    o.selected = selected.includes(r.canonical_id);
    o.textContent = `${r.product_name || r.canonical_id} (${r.canonical_id})`;
    el.sel.appendChild(o);
  }
  if (el.selectionMeta) {
    el.selectionMeta.textContent = `${st.selected_products.length} seleccionados`;
  }
}

function paginatedRows(rows) {
  const total = Math.max(0, rows.length);
  const pageSize = Math.max(1, Number(st.page_size || 25));
  const totalPages = Math.max(1, Math.ceil(total / pageSize));
  st.current_page = Math.min(Math.max(1, Number(st.current_page || 1)), totalPages);
  const start = (st.current_page - 1) * pageSize;
  return {
    total,
    pageSize,
    totalPages,
    pageRows: rows.slice(start, start + pageSize),
  };
}

function updateTableMeta(total, totalPages) {
  if (el.tableMeta) {
    el.tableMeta.textContent = `${total} resultados · ${st.selected_products.length} en gráfico`;
  }
  if (el.pageInfo) {
    el.pageInfo.textContent = `${st.current_page} / ${totalPages}`;
  }
  if (el.pagePrev) {
    el.pagePrev.disabled = st.current_page <= 1;
  }
  if (el.pageNext) {
    el.pageNext.disabled = st.current_page >= totalPages;
  }
}

function drawActiveFilters(totalRows) {
  if (!el.activeFilters) return;
  const defaultBase = p.months?.[0] || "";
  const sortLabels = { alphabetical: "Alfabético", price: "Precio", var_nominal: "Var. nominal", var_real: "Var. real" };
  const chips = [];
  if ((st.query || "").trim()) chips.push({ key: "query", label: `Búsqueda: "${st.query.trim()}"` });
  if (st.cba_filter !== "all") chips.push({ key: "cba", label: `CBA: ${st.cba_filter === "yes" ? "Sí" : "No"}` });
  if (st.category !== "all") chips.push({ key: "category", label: `Categoría: ${st.category}` });
  if (st.sort_by !== "alphabetical") chips.push({ key: "sort_by", label: `Orden: ${sortLabels[st.sort_by] || st.sort_by}` });
  if (st.base_month && st.base_month !== defaultBase) chips.push({ key: "base_month", label: `Base: ${st.base_month}` });
  if (st.price_mode === "real") chips.push({ key: "price_mode", label: "Modo: Real" });
  if (st.show_real_column) chips.push({ key: "show_real_column", label: "Tabla: var. real visible" });
  if (st.macro_scope === "rubros") chips.push({ key: "macro_scope", label: "Macro: Rubros" });
  if (st.macro_region && st.macro_region !== (p.macro_default_region || "patagonia")) chips.push({ key: "macro_region", label: `Región macro: ${st.macro_region}` });
  if (st.macro_scope === "rubros" && st.macro_category) chips.push({ key: "macro_category", label: `Rubro macro: ${st.macro_category}` });

  const html = [`<span class="pill pill-info">Resultados: ${totalRows}</span>`, `<span class="pill">En gráfico: ${st.selected_products.length}</span>`];
  if (!chips.length) {
    html.push(`<span class="pill">Sin filtros adicionales</span>`);
  } else {
    chips.forEach(item => {
      html.push(
        `<button type="button" class="pill pill-action" data-filter="${item.key}" title="Quitar filtro">`
        + `${esc(item.label)} <span aria-hidden="true">x</span></button>`
      );
    });
  }
  el.activeFilters.innerHTML = html.join("");
}

function exportFilteredCsv(rows) {
  const headers = ["canonical_id", "product_name", "presentation", "category", "is_cba", "current_price", "var_nominal_pct", "var_real_pct", "product_url"];
  const lines = [headers.join(",")];
  const escCsv = v => {
    const s = String(v ?? "");
    // Keep "\\n" literal in generated JS regex (Python string parsing would otherwise inject a real newline).
    if (/[",\\n;]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
    return s;
  };
  rows.forEach(r => {
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
  const blob = new Blob([lines.join("\\n")], { type: "text/csv;charset=utf-8;" });
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = `reporte_filtrado_${p.from_month}_${p.to_month}.csv`;
  a.style.display = "none";
  document.body.appendChild(a);
  a.click();
  setTimeout(() => { URL.revokeObjectURL(a.href); a.remove(); }, 0);
  trackEvent("export_csv", { rows: rows.length || 0, from: p.from_month, to: p.to_month });
}

function toSeriesForMainChart(rows) {
  const selected = new Set(st.selected_products || []);
  const grouped = {};
  for (const id of selected) {
    const series = timelineById[id] || [];
    if (series.length) {
      grouped[id] = series;
    }
  }
  const byId = {};
  for (const item of rows) byId[item.canonical_id] = item;
  return Object.keys(grouped).map((id, i) => {
    const pts = [...grouped[id]].sort((a, b) => new Date(a.scraped_at) - new Date(b.scraped_at)).map((x) => ({
      x: new Date(x.scraped_at),
      y: st.price_mode === "real" ? x.current_real_price : x.current_price
    })).filter(pt => pt.y != null && Number.isFinite(Number(pt.y)));
    return {
      name: `${byId[id]?.product_name || id} (${id})`,
      points: pts,
      color: COLORS[i % COLORS.length]
    };
  }).filter(s => s.points.length > 0);
}

function hasPlotly() {
  return typeof window.Plotly === "object" && typeof window.Plotly.react === "function";
}

// Set Spanish locale for Plotly axis tick labels
(function setPlotlyLocale() {
  const ES_MONTHS = ["enero", "febrero", "marzo", "abril", "mayo", "junio",
    "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"];
  const ES_MONTHS_SHORT = ["ene", "feb", "mar", "abr", "may", "jun",
    "jul", "ago", "sep", "oct", "nov", "dic"];
  const ES_DAYS = ["domingo", "lunes", "martes", "miércoles", "jueves", "viernes", "sábado"];
  const ES_DAYS_SHORT = ["dom", "lun", "mar", "mié", "jue", "vie", "sáb"];
  try {
    if (typeof window.Plotly === "object") {
      window.Plotly.register({
        moduleType: "locale",
        name: "es",
        dictionary: {},
        format: {
          days: ES_DAYS,
          shortDays: ES_DAYS_SHORT,
          months: ES_MONTHS,
          shortMonths: ES_MONTHS_SHORT,
          date: "%d/%m/%Y",
        }
      });
      window.Plotly.setPlotConfig({ locale: "es" });
    }
  } catch (_e) {
    console.warn('[Tracker] No se pudo registrar locale Plotly es:', _e);
  }
})();

function showChartError(container, msg) {
  if (!container) return;
  container.innerHTML = '';
  const box = document.createElement('div');
  box.style.cssText = 'display:flex;align-items:center;justify-content:center;height:200px;color:#64748b;font-size:.88rem;gap:8px;flex-direction:column;';
  const icon = document.createElement('span');
  icon.textContent = '⚠️';
  icon.style.fontSize = '1.4rem';
  const txt = document.createElement('span');
  txt.textContent = msg || 'No se pudo generar el gráfico. Probá recargando la página.';
  box.appendChild(icon);
  box.appendChild(txt);
  container.appendChild(box);
}

function niceStep(raw) {
  if (!Number.isFinite(raw) || raw <= 0) return 1;
  const power = Math.pow(10, Math.floor(Math.log10(raw)));
  const normalized = raw / power;
  if (normalized <= 1) return 1 * power;
  if (normalized <= 2) return 2 * power;
  if (normalized <= 5) return 5 * power;
  return 10 * power;
}

function niceRange(minVal, maxVal, targetTicks = 6, padRatio = 0.08) {
  if (!Number.isFinite(minVal) || !Number.isFinite(maxVal)) {
    return { min: 0, max: 1, dtick: 0.2 };
  }
  if (minVal === maxVal) {
    const abs = Math.abs(minVal) || 1;
    return { min: minVal - abs * 0.2, max: maxVal + abs * 0.2, dtick: abs * 0.1 };
  }
  const span = maxVal - minVal;
  const paddedMin = minVal - (span * padRatio);
  const paddedMax = maxVal + (span * padRatio);
  const step = niceStep((paddedMax - paddedMin) / Math.max(2, targetTicks - 1));
  const niceMin = Math.floor(paddedMin / step) * step;
  const niceMax = Math.ceil(paddedMax / step) * step;
  return { min: niceMin, max: niceMax, dtick: step };
}

function drawPlotlyChart(container, legend, series, yLabel, xLabel = "Tiempo", opts = {}) {
  container.innerHTML = "";
  legend.innerHTML = "";
  if (!hasPlotly()) {
    return false;
  }
  const options = opts || {};
  const normalizedSeries = (series || []).map(s => ({
    ...s,
    points: (s.points || []).map(pt => ({
      x: pt?.x instanceof Date ? pt.x : new Date(pt?.x),
      y: Number(pt?.y),
    })).filter(pt => Number.isFinite(pt.x.getTime()) && Number.isFinite(pt.y))
  })).filter(s => s.points.length > 0);
  if (!normalizedSeries.length) {
    return false;
  }
  const ys = normalizedSeries.flatMap(s => s.points.map(pt => pt.y)).filter(Number.isFinite);
  if (!ys.length) {
    return false;
  }
  const yr = niceRange(Math.min(...ys), Math.max(...ys), Number(options.targetTicks || 6), Number(options.padRatio || 0.08));
  const xTimes = normalizedSeries.flatMap(s => s.points.map(pt => pt.x.getTime())).filter(Number.isFinite);
  const minX = xTimes.length ? Math.min(...xTimes) : NaN;
  const maxX = xTimes.length ? Math.max(...xTimes) : NaN;
  const daySpan = Number.isFinite(minX) && Number.isFinite(maxX) ? ((maxX - minX) / 86400000) : 0;
  const xTickFormat = String(options.xTickFormat || (
    daySpan <= 45 ? "%d %b" :
      daySpan <= 420 ? "%b %Y" :
        "%Y"
  ));
  const yAxisKind = String(options.yAxisKind || "number");
  const yTickFormat = String(options.yTickFormat || (yAxisKind === "ars" ? ",.0f" : ",.2f"));
  const yTickPrefix = String(options.yTickPrefix || (yAxisKind === "ars" ? "$" : ""));
  const yTickSuffix = String(options.yTickSuffix || (yAxisKind === "pp" ? " pp" : ""));
  const traces = normalizedSeries.map(s => ({
    type: "scatter",
    mode: "lines+markers",
    name: s.name,
    x: s.points.map(pt => pt.x),
    y: s.points.map(pt => pt.y),
    line: { color: s.color, width: 2.4, shape: "spline", smoothing: 0.35 },
    marker: { color: s.color, size: 5 },
    customdata: s.points.map(pt => formatMetricValue(pt.y, String(s.value_kind || options.valueKind || yAxisKind))),
    hovertemplate: "%{x|%d/%m/%Y}<br>%{customdata}<extra>" + esc(s.name) + "</extra>",
  }));
  const layout = {
    paper_bgcolor: "transparent",
    plot_bgcolor: "transparent",
    margin: { l: 78, r: 20, t: 20, b: 56 },
    autosize: true,
    height: 420,
    xaxis: {
      title: { text: xLabel, font: { size: 12, color: "#64748b" } },
      type: "date",
      tickformat: xTickFormat,
      showgrid: true,
      gridcolor: "#e5e7eb",
      zeroline: false,
      tickfont: { size: 11, color: "#64748b" },
    },
    yaxis: {
      title: { text: yLabel, font: { size: 12, color: "#64748b" } },
      showgrid: true,
      gridcolor: "#e5e7eb",
      zeroline: false,
      range: [yr.min, yr.max],
      dtick: yr.dtick,
      tickfont: { size: 11, color: "#64748b" },
      tickformat: yTickFormat,
      tickprefix: yTickPrefix,
      ticksuffix: yTickSuffix,
    },
    hovermode: "x unified",
    showlegend: false,
    font: { family: "Inter, Segoe UI, sans-serif", color: "#1e293b" },
    shapes: [{ type: "line", xref: "paper", x0: 0, x1: 1, yref: "y", y0: 0, y1: 0, line: { color: "#0f172a", width: 1.4, dash: "dot" } }],
  };
  try {
    window.Plotly.react(container, traces, layout, {
      displayModeBar: false,
      responsive: true,
      staticPlot: false,
    });
  } catch (chartErr) {
    console.error('[Tracker] Error al renderizar gráfico principal:', chartErr);
    showChartError(container, 'No se pudo generar el gráfico. Probá recargando la página.');
    return false;
  }

  for (const s of normalizedSeries) {
    const latest = s.points[s.points.length - 1];
    const item = document.createElement("div");
    item.className = "item";
    item.innerHTML = (
      `<span class="dot" style="background:${s.color}"></span>${esc(s.name)}: `
      + `<strong>${esc(formatMetricValue(latest?.y, String(s.value_kind || options.valueKind || yAxisKind)))}</strong>`
    );
    legend.appendChild(item);
  }
  return true;
}

function drawCanvasChart(container, legend, series, yLabel, xLabel = "Tiempo") {
  container.innerHTML = "";
  legend.innerHTML = "";
  const normalizedSeries = (series || []).map(s => ({
    ...s,
    points: (s.points || []).map(pt => ({
      x: pt?.x instanceof Date ? pt.x : new Date(pt?.x),
      y: Number(pt?.y),
    })).filter(pt => Number.isFinite(pt.x.getTime()) && Number.isFinite(pt.y))
  })).filter(s => s.points.length > 0);
  if (!normalizedSeries.length) {
    const m = document.createElement("div");
    m.className = "chart-empty";
    m.textContent = "Sin datos para graficar";
    container.appendChild(m);
    return;
  }

  const canvas = document.createElement("canvas");
  const tooltip = document.createElement("div");
  tooltip.className = "chart-tooltip";
  container.appendChild(canvas);
  container.appendChild(tooltip);
  const w = Math.max(320, container.clientWidth || 760);
  const h = Math.max(220, container.classList.contains("small") ? 252 : 366);
  const ratio = Math.max(1, window.devicePixelRatio || 1);
  canvas.width = Math.floor(w * ratio);
  canvas.height = Math.floor(h * ratio);
  canvas.style.width = `${w}px`;
  canvas.style.height = `${h}px`;
  const ctx = canvas.getContext("2d");
  if (!ctx) {
    const m = document.createElement("div");
    m.className = "chart-empty";
    m.textContent = "No se pudo inicializar el canvas";
    container.appendChild(m);
    return;
  }
  ctx.scale(ratio, ratio);

  const xs = normalizedSeries.flatMap(s => s.points.map(p => p.x.getTime())).filter(Number.isFinite);
  const ys = normalizedSeries.flatMap(s => s.points.map(p => Number(p.y))).filter(Number.isFinite);
  if (!xs.length || !ys.length) {
    const m = document.createElement("div");
    m.className = "chart-empty";
    m.textContent = "Sin datos para graficar";
    container.appendChild(m);
    return;
  }

  const minX = Math.min(...xs);
  const maxX = Math.max(...xs);
  const minY = Math.min(...ys);
  const maxY = Math.max(...ys);
  const ySpan = Math.max(1, maxY - minY || 1);
  const yMin = minY - ySpan * 0.08;
  const yMax = maxY + ySpan * 0.08;

  const yTicks = [0, 0.25, 0.5, 0.75, 1];
  ctx.font = "12px Inter, Segoe UI, sans-serif";
  const yTickLabels = yTicks.map(t => fmtAxisNum(yMin + (yMax - yMin) * t));
  const maxYLabelW = yTickLabels.reduce((acc, label) => Math.max(acc, ctx.measureText(label).width), 0);
  const pad = { l: Math.max(78, Math.ceil(maxYLabelW) + 44), r: 20, t: 20, b: 58 };
  const innerW = Math.max(40, w - pad.l - pad.r);
  const innerH = Math.max(40, h - pad.t - pad.b);
  const mapX = x => pad.l + ((x - minX) / (Math.max(1, maxX - minX))) * innerW;
  const mapY = y => pad.t + ((yMax - y) / (Math.max(1, yMax - yMin))) * innerH;
  const xTickCount = maxX === minX ? 1 : Math.min(6, Math.max(2, Math.floor(innerW / 120)));
  const xTicks = xTickCount === 1
    ? [0.5]
    : Array.from({ length: xTickCount }, (_, i) => i / (xTickCount - 1));
  const mappedSeries = normalizedSeries.map(s => ({
    ...s,
    mapped: s.points.map(pt => {
      const rawX = pt.x.getTime();
      const rawY = Number(pt.y);
      return { rawX, rawY, px: mapX(rawX), py: mapY(rawY) };
    })
  }));
  const allPoints = mappedSeries.flatMap(s => s.mapped.map(pt => ({ ...pt, name: s.name, color: s.color })));

  function drawFrame(hover) {
    ctx.clearRect(0, 0, w, h);
    ctx.strokeStyle = "#d8e2ed";
    ctx.lineWidth = 1;
    for (const t of yTicks) {
      const y = pad.t + innerH * (1 - t);
      ctx.beginPath();
      ctx.moveTo(pad.l, y);
      ctx.lineTo(w - pad.r, y);
      ctx.stroke();
    }
    for (const t of xTicks) {
      const x = pad.l + innerW * t;
      ctx.beginPath();
      ctx.moveTo(x, pad.t);
      ctx.lineTo(x, h - pad.b);
      ctx.stroke();
    }
    ctx.beginPath();
    ctx.moveTo(pad.l, pad.t);
    ctx.lineTo(pad.l, h - pad.b);
    ctx.lineTo(w - pad.r, h - pad.b);
    ctx.strokeStyle = "#8b98ac";
    ctx.stroke();

    ctx.fillStyle = "#5a6577";
    ctx.font = "12px Inter, Segoe UI, sans-serif";
    ctx.textBaseline = "middle";
    ctx.textAlign = "right";
    for (const t of yTicks) {
      const y = pad.t + innerH * (1 - t);
      const val = (yMin + (yMax - yMin) * t);
      ctx.fillText(fmtAxisNum(val), pad.l - 12, y);
    }
    ctx.save();
    ctx.translate(18, pad.t + (innerH / 2));
    ctx.rotate(-Math.PI / 2);
    ctx.textAlign = "center";
    ctx.fillText(yLabel, 0, 0);
    ctx.restore();

    ctx.textBaseline = "top";
    xTicks.forEach((t, idx) => {
      const x = pad.l + innerW * t;
      const ts = minX + (maxX - minX) * t;
      const lbl = fmtMonthTick(ts);
      ctx.textAlign = xTicks.length === 1 ? "center" : (idx === 0 ? "left" : (idx === xTicks.length - 1 ? "right" : "center"));
      ctx.fillText(lbl, x, h - pad.b + 12);
    });
    ctx.textAlign = "center";
    ctx.fillText(xLabel, w / 2, h - 17);

    mappedSeries.forEach(s => {
      if (!s.mapped.length) return;
      ctx.strokeStyle = s.color;
      ctx.lineWidth = 2.4;
      ctx.beginPath();
      s.mapped.forEach((pt, idx) => {
        if (idx === 0) ctx.moveTo(pt.px, pt.py); else ctx.lineTo(pt.px, pt.py);
      });
      ctx.stroke();

      if (s.mapped.length <= 36) {
        ctx.fillStyle = s.color;
        s.mapped.forEach(pt => {
          ctx.beginPath();
          ctx.arc(pt.px, pt.py, 2.5, 0, Math.PI * 2);
          ctx.fill();
        });
      }

      const latest = s.mapped[s.mapped.length - 1];
      if (latest) {
        ctx.beginPath();
        ctx.fillStyle = "#fff";
        ctx.arc(latest.px, latest.py, 3.7, 0, Math.PI * 2);
        ctx.fill();
        ctx.lineWidth = 1.8;
        ctx.strokeStyle = s.color;
        ctx.stroke();
      }
    });

    if (hover?.points?.length) {
      ctx.save();
      ctx.setLineDash([6, 5]);
      ctx.strokeStyle = "#75849a";
      ctx.lineWidth = 1.2;
      ctx.beginPath();
      ctx.moveTo(hover.xPx, pad.t);
      ctx.lineTo(hover.xPx, h - pad.b);
      ctx.stroke();
      ctx.restore();

      hover.points.forEach(pt => {
        ctx.beginPath();
        ctx.fillStyle = pt.color;
        ctx.arc(pt.px, pt.py, 4.2, 0, Math.PI * 2);
        ctx.fill();
        ctx.lineWidth = 2;
        ctx.strokeStyle = "#fff";
        ctx.stroke();
      });
    }
  }
  function nearestHover(mouseX) {
    if (!allPoints.length) return null;
    let nearest = allPoints[0];
    let minDist = Number.POSITIVE_INFINITY;
    for (const pt of allPoints) {
      const dist = Math.abs(pt.px - mouseX);
      if (dist < minDist) {
        minDist = dist;
        nearest = pt;
      }
    }
    if (!nearest) return null;
    const targetX = nearest.rawX;
    const points = mappedSeries.map(s => {
      let localNearest = s.mapped[0];
      let localDist = Number.POSITIVE_INFINITY;
      for (const pt of s.mapped) {
        const dist = Math.abs(pt.rawX - targetX);
        if (dist < localDist) {
          localDist = dist;
          localNearest = pt;
        }
      }
      if (!localNearest) return null;
      return { ...localNearest, name: s.name, color: s.color };
    }).filter(Boolean);
    if (!points.length) return null;
    return { xPx: mapX(targetX), targetX, points };
  }

  function hideTooltip() {
    tooltip.classList.remove("visible");
    tooltip.innerHTML = "";
  }

  function showTooltip(hover, mouseX, mouseY) {
    if (!hover?.points?.length) {
      hideTooltip();
      return;
    }
    const lines = hover.points.map(pt =>
      `<div><span class="dot" style="background:${pt.color}"></span> ${esc(pt.name)}: <strong>${esc(fmtAxisNum(pt.rawY))}</strong></div>`
    );
    tooltip.innerHTML = `<strong>${esc(fmtDate(hover.targetX))}</strong>${lines.join("")}`;
    tooltip.classList.add("visible");
    const tipRect = tooltip.getBoundingClientRect();
    let left = mouseX + 14;
    let top = mouseY - tipRect.height - 10;
    if (left + tipRect.width > w - 6) left = mouseX - tipRect.width - 14;
    if (top < 6) top = mouseY + 14;
    tooltip.style.left = `${Math.max(6, Math.min(w - tipRect.width - 6, left))}px`;
    tooltip.style.top = `${Math.max(6, Math.min(h - tipRect.height - 6, top))}px`;
  }

  drawFrame(null);
  canvas.addEventListener("mousemove", (ev) => {
    const rect = canvas.getBoundingClientRect();
    const mx = ev.clientX - rect.left;
    const my = ev.clientY - rect.top;
    const inside = mx >= pad.l && mx <= w - pad.r && my >= pad.t && my <= h - pad.b;
    if (!inside) {
      hideTooltip();
      drawFrame(null);
      return;
    }
    const hover = nearestHover(mx);
    drawFrame(hover);
    if (hover) {
      showTooltip(hover, mx, my);
    } else {
      hideTooltip();
    }
  });
  canvas.addEventListener("mouseleave", () => {
    hideTooltip();
    drawFrame(null);
  });

  for (const s of mappedSeries) {
    const lastPoint = s.mapped[s.mapped.length - 1];
    const item = document.createElement("div");
    item.className = "item";
    const valueLabel = lastPoint ? fmtAxisNum(lastPoint.rawY) : "N/D";
    item.innerHTML = `<span class="dot" style="background:${s.color}"></span>${esc(s.name)}: <strong>${esc(valueLabel)}</strong>`;
    legend.appendChild(item);
  }
}

function drawMainChart(rows, force = false) {
  if (el.mainChartPanel && !el.mainChartPanel.open && !force) {
    return;
  }
  const chartKey = [
    st.price_mode,
    ...(st.selected_products || []),
    rows.length,
    p.timeline?.length || 0,
  ].join("|");
  if (!force && chartKey === _lastMainChartKey) {
    return;
  }
  _lastMainChartKey = chartKey;
  const series = toSeriesForMainChart(rows);
  const yLabel = st.price_mode === "real" ? "Precio real (ARS constantes)" : "Precio nominal (ARS)";
  if (!drawPlotlyChart(el.chartMain, el.legendMain, series, yLabel, "Fecha", {
    yAxisKind: "ars",
    yTickFormat: ",.0f",
    yTickPrefix: "$",
    targetTicks: 6,
    padRatio: 0.08
  })) {
    drawCanvasChart(el.chartMain, el.legendMain, series, yLabel);
  }
}

function safeText(value, fallback = "N/D") {
  if (value === null || value === undefined) return fallback;
  if (typeof value === "number" && Number.isNaN(value)) return fallback;
  const txt = String(value).trim();
  if (!txt) return fallback;
  const normalized = txt.toLowerCase();
  if (normalized === "nan" || normalized === "none" || normalized === "null") return fallback;
  return txt;
}

function computeIndependentBase100(rows, indexKey) {
  const baseRow = rows.find(r => r[indexKey] != null);
  const base = baseRow ? Number(baseRow[indexKey]) : null;
  if (base == null || !Number.isFinite(base) || base <= 0) {
    return rows.map(() => null);
  }
  return rows.map(r => {
    const value = Number(r[indexKey]);
    if (!Number.isFinite(value)) return null;
    return (value / base) * 100;
  });
}

function drawSecondaryPlotly(container, legend, tracker, official, gap, regionLabel) {
  container.innerHTML = "";
  legend.innerHTML = "";
  if (!hasPlotly()) {
    return false;
  }
  const normalizePoints = (rows) => (
    (rows || []).map(pt => ({
      x: pt?.x instanceof Date ? pt.x : new Date(pt?.x),
      y: Number(pt?.y),
    })).filter(pt => Number.isFinite(pt.x.getTime()) && Number.isFinite(pt.y))
  );
  const trackerPts = normalizePoints(tracker);
  const officialPts = normalizePoints(official);
  const gapPts = normalizePoints(gap);
  if (!trackerPts.length && !officialPts.length && !gapPts.length) {
    return false;
  }

  const indexValues = [...trackerPts, ...officialPts].map(pt => pt.y).filter(Number.isFinite);
  const gapValues = gapPts.map(pt => pt.y).filter(Number.isFinite);
  const yrIndex = indexValues.length ? niceRange(Math.min(...indexValues), Math.max(...indexValues), 6, 0.08) : { min: 0, max: 1, dtick: 0.2 };
  const yrGap = gapValues.length ? niceRange(Math.min(...gapValues), Math.max(...gapValues), 6, 0.12) : null;
  const xTimes = [...trackerPts, ...officialPts, ...gapPts].map(pt => pt.x.getTime()).filter(Number.isFinite);
  const daySpan = xTimes.length ? (Math.max(...xTimes) - Math.min(...xTimes)) / 86400000 : 0;
  const xTickFormat = daySpan <= 45 ? "%d %b" : (daySpan <= 420 ? "%b %Y" : "%Y");

  const traces = [];
  if (trackerPts.length) {
    traces.push({
      type: "scatter",
      mode: "lines+markers",
      name: "IPC propio base 100",
      x: trackerPts.map(pt => pt.x),
      y: trackerPts.map(pt => pt.y),
      line: { color: "#005f73", width: 2.4, shape: "spline", smoothing: 0.3 },
      marker: { color: "#005f73", size: 5 },
      customdata: trackerPts.map(pt => formatMetricValue(pt.y, "index")),
      hovertemplate: "%{x|%m/%Y}<br>%{customdata}<extra>IPC propio</extra>",
      yaxis: "y",
    });
  }
  if (officialPts.length) {
    traces.push({
      type: "scatter",
      mode: "lines+markers",
      name: `IPC ${regionLabel} base 100`,
      x: officialPts.map(pt => pt.x),
      y: officialPts.map(pt => pt.y),
      line: { color: "#ca6702", width: 2.4, shape: "spline", smoothing: 0.3 },
      marker: { color: "#ca6702", size: 5 },
      customdata: officialPts.map(pt => formatMetricValue(pt.y, "index")),
      hovertemplate: "%{x|%m/%Y}<br>%{customdata}<extra>IPC oficial</extra>",
      yaxis: "y",
    });
  }
  if (gapPts.length) {
    traces.push({
      type: "scatter",
      mode: "lines+markers",
      name: "Brecha (pp)",
      x: gapPts.map(pt => pt.x),
      y: gapPts.map(pt => pt.y),
      line: { color: "#9b2226", width: 2.1, dash: "dot" },
      marker: { color: "#9b2226", size: 5 },
      customdata: gapPts.map(pt => formatMetricValue(pt.y, "pp")),
      hovertemplate: "%{x|%m/%Y}<br>%{customdata}<extra>Brecha</extra>",
      yaxis: "y2",
    });
  }

  const layout = {
    paper_bgcolor: "transparent",
    plot_bgcolor: "transparent",
    margin: { l: 78, r: gapPts.length ? 74 : 20, t: 20, b: 56 },
    autosize: true,
    height: 360,
    xaxis: {
      title: { text: "Mes", font: { size: 12, color: "#64748b" } },
      type: "date",
      tickformat: xTickFormat,
      showgrid: true,
      gridcolor: "#e5e7eb",
      zeroline: false,
      tickfont: { size: 11, color: "#64748b" },
    },
    yaxis: {
      title: { text: "Indice base 100", font: { size: 12, color: "#64748b" } },
      showgrid: true,
      gridcolor: "#e5e7eb",
      zeroline: false,
      range: [yrIndex.min, yrIndex.max],
      dtick: yrIndex.dtick,
      tickfont: { size: 11, color: "#64748b" },
      tickformat: ",.2f",
    },
    hovermode: "x unified",
    showlegend: false,
    font: { family: "Inter, Segoe UI, sans-serif", color: "#1e293b" },
    shapes: [{ type: "line", xref: "paper", x0: 0, x1: 1, yref: "y", y0: 100, y1: 100, line: { color: "#0f172a", width: 1.4, dash: "dot" } }],
  };
  if (yrGap) {
    layout.yaxis2 = {
      title: { text: "Brecha (pp)", font: { size: 12, color: "#64748b" } },
      overlaying: "y",
      side: "right",
      showgrid: false,
      zeroline: false,
      range: [yrGap.min, yrGap.max],
      dtick: yrGap.dtick,
      tickfont: { size: 11, color: "#64748b" },
      tickformat: ",.2f",
      ticksuffix: " pp",
    };
  }

  try {
    window.Plotly.react(container, traces, layout, {
      displayModeBar: false,
      responsive: true,
      staticPlot: false,
    });
  } catch (chartErr) {
    console.error('[Tracker] Error al renderizar gráfico IPC:', chartErr);
    showChartError(container, 'No se pudo cargar el gráfico de IPC. Probá recargando.');
    return false;
  }

  traces.forEach((trace) => {
    const values = Array.isArray(trace.y) ? trace.y : [];
    const latestValue = values.length ? values[values.length - 1] : null;
    const item = document.createElement("div");
    item.className = "item";
    const color = trace?.line?.color || "#475569";
    const kind = trace.name === "Brecha (pp)" ? "pp" : "index";
    item.innerHTML = (
      `<span class="dot" style="background:${color}"></span>${esc(trace.name)}: `
      + `<strong>${esc(formatMetricValue(latestValue, kind))}</strong>`
    );
    legend.appendChild(item);
  });

  return true;
}

function drawSecondaryChart(force = false) {
  const panelClosed = !!(el.panelSecondary && !el.panelSecondary.open && !force);
  const region = st.macro_region || p.macro_default_region || "patagonia";
  const regionLabel = region === "nacional" ? "Nacional" : "Patagonia";
  const generalSrc = (p.ipc_comparison_by_region?.[region] || p.ipc_comparison_series || []);
  const categorySrc = (p.category_comparison_by_region?.[region] || p.category_comparison_series || []);
  const secondaryKey = [
    st.view,
    st.macro_scope,
    region,
    st.macro_category,
    generalSrc.length,
    categorySrc.length,
    p.basket_vs_ipc_series?.length || 0
  ].join("|");
  if (!force && secondaryKey === _lastSecondaryChartKey) {
    return;
  }
  _lastSecondaryChartKey = secondaryKey;

  if (el.macroScope) {
    if (!["general", "rubros"].includes(st.macro_scope)) {
      st.macro_scope = "general";
    }
    el.macroScope.value = st.macro_scope;
  }
  if (el.macroRegion) {
    const validRegions = Array.from(el.macroRegion.options).map(o => o.value);
    if (!validRegions.includes(st.macro_region)) {
      st.macro_region = validRegions.includes(p.macro_default_region || "") ? (p.macro_default_region || "") : (validRegions[0] || "patagonia");
    }
    el.macroRegion.value = st.macro_region;
  }

  let src = [];
  let macroLabel = "General";
  if (st.macro_scope === "rubros") {
    const categories = [...new Set(categorySrc.map(x => x.category_slug).filter(Boolean))].sort();
    if (el.macroCategory) {
      el.macroCategory.innerHTML = "";
      if (!categories.length) {
        const empty = document.createElement("option");
        empty.value = "";
        empty.textContent = "Sin rubros comparables";
        el.macroCategory.appendChild(empty);
      } else {
        categories.forEach(cat => {
          const o = document.createElement("option");
          o.value = cat;
          o.textContent = cat;
          el.macroCategory.appendChild(o);
        });
      }
      if (!categories.includes(st.macro_category)) {
        st.macro_category = categories[0] || "";
      }
      el.macroCategory.value = st.macro_category;
      el.macroCategory.disabled = !categories.length;
    }
    src = categorySrc.filter(x => x.category_slug === st.macro_category);
    macroLabel = st.macro_category ? `Rubro: ${st.macro_category}` : "Rubros";
  } else {
    if (el.macroCategory) {
      const o = document.createElement("option");
      o.value = "";
      o.textContent = "No aplica en General";
      el.macroCategory.innerHTML = "";
      el.macroCategory.appendChild(o);
      el.macroCategory.value = "";
      el.macroCategory.disabled = true;
    }
    src = generalSrc;
  }

  if (!src.length && (p.basket_vs_ipc_series || []).length) {
    src = (p.basket_vs_ipc_series || []).map(x => ({
      year_month: x.year_month,
      tracker_index_base100: x.basket_index_base100,
      official_index_base100: x.ipc_index_base100,
      plot_tracker_base100: x.basket_index_base100,
      plot_official_base100: x.ipc_index_base100,
      plot_mode: "strict_overlap",
      tracker_base_month: null,
      official_base_month: null,
      is_strictly_comparable: true,
      gap_index_points: x.gap_points,
      tracker_status: null,
      official_status: null
    }));
  }

  const trackerPlotRaw = src.map(x => (x.plot_tracker_base100 ?? x.tracker_index_base100 ?? null));
  const officialPlotRaw = src.map(x => (x.plot_official_base100 ?? x.official_index_base100 ?? null));
  let trackerPlot = trackerPlotRaw;
  let officialPlot = officialPlotRaw;
  let plotMode = src.some(x => x.plot_mode === "independent_base") ? "independent_base" : "strict_overlap";
  if (!trackerPlot.some(v => v != null)) {
    const rebuilt = computeIndependentBase100(src, "tracker_index");
    if (rebuilt.some(v => v != null)) {
      trackerPlot = rebuilt;
      plotMode = "independent_base";
    }
  }
  if (!officialPlot.some(v => v != null)) {
    const rebuilt = computeIndependentBase100(src, "official_index");
    if (rebuilt.some(v => v != null)) {
      officialPlot = rebuilt;
      plotMode = "independent_base";
    }
  }
  const strictComparable = src.some(
    x => ((x.is_strictly_comparable === true) || (x.is_strictly_comparable === undefined && x.gap_index_points != null))
      && x.gap_index_points != null
  );
  const tracker = src
    .map((row, idx) => ({ x: new Date(`${row.year_month}-01T00:00:00`), y: trackerPlot[idx] }))
    .filter(x => x.y != null);
  const official = src
    .map((row, idx) => ({ x: new Date(`${row.year_month}-01T00:00:00`), y: officialPlot[idx] }))
    .filter(x => x.y != null);
  const gap = (strictComparable ? src : [])
    .map(x => ({ x: new Date(`${x.year_month}-01T00:00:00`), y: x.gap_index_points }))
    .filter(x => x.y != null);
  const series = [
    { name: "IPC propio base 100", points: tracker, color: "#005f73", value_kind: "index" },
    { name: `IPC ${regionLabel} base 100`, points: official, color: "#ca6702", value_kind: "index" },
    { name: "Brecha (pp)", points: gap, color: "#9b2226", value_kind: "pp" }
  ].filter(s => s.points.length > 0);
  if (!panelClosed) {
    if (!drawSecondaryPlotly(el.chartSecondary, el.legendSecondary, tracker, official, gap, regionLabel)) {
      drawCanvasChart(el.chartSecondary, el.legendSecondary, series, "Indice base 100 / brecha");
    }
  }

  if (el.macroNotice) {
    if ((tracker.length > 0 || official.length > 0) && (plotMode === "independent_base" || !strictComparable)) {
      el.macroNotice.hidden = false;
      el.macroNotice.textContent =
        "Los meses disponibles del índice propio y del IPC oficial no coinciden completamente. "
        + "La brecha en puntos solo se muestra para períodos comparables.";
    } else {
      el.macroNotice.hidden = true;
      el.macroNotice.textContent = "";
    }
  }

  if (el.macroStatus) {
    const pub = (p.publication_status_by_region?.[region] || p.publication_status || {});
    const latestTracker = [...src].reverse().find(x => x.tracker_index != null) || null;
    const latestOfficial = [...src].reverse().find(x => x.official_index != null) || null;
    const trackerMonth = safeText(pub.latest_tracker_month ?? latestTracker?.year_month, null);
    const officialMonth = safeText(pub.latest_official_month ?? latestOfficial?.year_month, null);
    const parts = [regionLabel];
    if (trackerMonth) parts.push(`Índice propio: ${trackerMonth}`);
    if (officialMonth) parts.push(`IPC oficial: ${officialMonth}`);
    el.macroStatus.textContent = parts.join(" · ");
    if (el.macroDetailText) {
      const trackerStatus = safeText(pub.latest_tracker_status ?? latestTracker?.tracker_status, null);
      const officialStatus = safeText(pub.latest_official_status ?? latestOfficial?.official_status, null);
      const details = [];
      if (macroLabel) details.push(macroLabel);
      if (trackerStatus) details.push(`Índice: ${trackerStatus}`);
      if (officialStatus) details.push(`IPC: ${officialStatus}`);
      el.macroDetailText.textContent = details.join(" · ");
    }
  }
}

function mountBandOptions(rows) {
  if (!el.bandProduct) return;
  const candidates = rows.filter(r => (bandById[r.canonical_id] || []).length > 0);
  el.bandProduct.innerHTML = "";
  if (!candidates.length) {
    st.band_product = "";
    return;
  }
  candidates.forEach(r => {
    const o = document.createElement("option");
    o.value = r.canonical_id;
    o.textContent = `${r.product_name || r.canonical_id} (${r.canonical_id})`;
    el.bandProduct.appendChild(o);
  });
  const validIds = candidates.map(r => r.canonical_id);
  if (!validIds.includes(st.band_product)) {
    st.band_product = validIds[0];
  }
  el.bandProduct.value = st.band_product;
}

function drawBandChart(rows) {
  if (!el.panelBands || !el.chartBands || !el.legendBands) {
    return;
  }
  const withBands = rows.filter(r => (bandById[r.canonical_id] || []).length > 0);
  if (!withBands.length) {
    el.panelBands.style.display = "none";
    return;
  }
  el.panelBands.style.display = "";
  if (!el.panelBands.open) {
    return;
  }
  mountBandOptions(rows);

  const selectedId = st.band_product;
  const src = [...(bandById[selectedId] || [])].sort((a, b) => new Date(a.scraped_at) - new Date(b.scraped_at));
  if (!src.length) {
    drawCanvasChart(el.chartBands, el.legendBands, [], "Precio (ARS)");
    if (el.bandMeta) el.bandMeta.textContent = "Sin observaciones de terna para el producto seleccionado.";
    return;
  }

  const low = src.map(x => ({ x: new Date(x.scraped_at), y: x.low_price })).filter(x => x.y != null);
  const mid = src.map(x => ({ x: new Date(x.scraped_at), y: x.mid_price })).filter(x => x.y != null);
  const high = src.map(x => ({ x: new Date(x.scraped_at), y: x.high_price })).filter(x => x.y != null);
  const series = [
    { name: "Low", points: low, color: "#6c757d", value_kind: "ars" },
    { name: "Mid (representativo)", points: mid, color: "#005f73", value_kind: "ars" },
    { name: "High", points: high, color: "#ca6702", value_kind: "ars" },
  ].filter(s => s.points.length > 0);
  if (!drawPlotlyChart(el.chartBands, el.legendBands, series, "Precio nominal (ARS)", "Fecha", {
    yAxisKind: "ars",
    yTickFormat: ",.0f",
    yTickPrefix: "$",
    targetTicks: 6,
    padRatio: 0.08
  })) {
    drawCanvasChart(el.chartBands, el.legendBands, series, "Precio nominal (ARS)");
  }

  const latest = src[src.length - 1] || {};
  if (el.bandMeta) {
    el.bandMeta.textContent =
      `Ultima dispersion: low=${money(latest.low_price)} | mid=${money(latest.mid_price)} | `
      + `high=${money(latest.high_price)} | spread=${pct(latest.spread_pct)}`;
  }
}

function hasCandidateTriplet(triplet) {
  if (!triplet || typeof triplet !== "object") return false;
  return Object.keys(triplet).length > 0;
}

function buildCandidateSubRow(tier, candidate) {
  const candidateNameRaw = candidate?.candidate_name || "N/D";
  const candidateName = esc(candidateNameRaw);
  const candidateUrl = String(candidate?.candidate_url || "").trim();
  const candidateLinked = (candidateUrl && candidateNameRaw !== "N/D")
    ? `<a href="${esc(candidateUrl)}" target="_blank" rel="noopener noreferrer">${candidateName}</a>`
    : `<span>${candidateName}</span>`;
  const candidatePrice = (candidate?.candidate_price != null) ? money(candidate.candidate_price) : "N/D";
  const tierLabel = tier === "mid" ? "Mid" : "Low";
  const tierFinal = tier === "high" ? "High" : tierLabel;
  const tierClass = tier === "mid" ? "tier-mid" : (tier === "high" ? "tier-high" : "tier-low");
  const sub = document.createElement("tr");
  sub.className = `row-candidate ${tierClass}`;
  sub.innerHTML = `
    <td data-label="Info"><span class="candidate-tier">${tierFinal}</span></td>
    <td data-label="Variante">${candidateLinked}</td>
    <td class="num" data-label="Precio">${candidatePrice}</td>
    <td class="num muted" data-label="Var. (Nominal)">-</td>
    ${st.show_real_column ? `<td class="num muted" data-label="Var. (Real)">-</td>` : ""}
  `;
  return sub;
}

function createSparkline(prices) {
  if (!prices || prices.length < 2) return "";
  const h = 20, w = 48;
  const min = Math.min(...prices), max = Math.max(...prices);
  const range = max - min || 1;
  const pts = prices.map((p, i) => `${(i / (prices.length - 1)) * w},${h - ((p - min) / range) * h}`).join(" L ");
  const color = prices[prices.length - 1] > prices[0] ? "var(--bad)" : (prices[prices.length - 1] < prices[0] ? "var(--good)" : "var(--warn)");
  return `<svg width="${w}" height="${h}" viewBox="-2 -2 ${w + 4} ${h + 4}" style="overflow:visible; vertical-align:middle; margin-right:8px; opacity:0.8;">
    <path d="M ${pts}" fill="none" stroke="${color}" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/>
  </svg>`;
}

function drawTable(rows) {
  el.tb.innerHTML = "";
  el.thVarReal.style.display = st.show_real_column ? "" : "none";
  const { total, pageRows, totalPages } = paginatedRows(rows);
  updateTableMeta(total, totalPages);
  if (!total) {
    el.tb.innerHTML = `<tr><td colspan="${st.show_real_column ? 5 : 4}" class="muted">No hay resultados con estos filtros.</td></tr>`;
    return;
  }

  if (expandedProductId && !pageRows.some(r => r.canonical_id === expandedProductId)) {
    expandedProductId = "";
  }

  const fragment = document.createDocumentFragment();
  for (const r of pageRows) {
    const productId = String(r.canonical_id || "");
    const triplet = (candidateTripletsById || {})[productId] || {};
    const hasCandidates = hasCandidateTriplet(triplet);
    const isExpanded = hasCandidates && expandedProductId === productId;

    const tr = document.createElement("tr");
    tr.className = `row-main${hasCandidates ? " expandable" : ""}${isExpanded ? " is-expanded" : ""}`;
    const nameRaw = String(r.product_name || r.canonical_id || "");
    const name = esc(nameRaw);
    const detailHref = `/tracker/${encodeURIComponent(productId)}/`;
    const externalLink = r.product_url
      ? `<a href="${esc(r.product_url)}" target="_blank" rel="noopener noreferrer" class="product-ext-link" title="Ver en sitio La Anónima" aria-label="Ver en sitio externo">↗</a>`
      : "";
    const linked = `<a href="${detailHref}" class="product-detail-link" title="${name}">${name}</a>${externalLink}`; const nomCls = trendClass(r.variation_nominal_pct);
    const realCls = trendClass(r.variation_real_pct);
    const presentation = resolvePresentation(r);
    const chevron = hasCandidates ? (isExpanded ? "▾" : "▸") : "";
    const metaText = hasCandidates ? (isExpanded ? "Ocultar opciones" : "Ver opciones") : "";
    const sparkData = (r.monthly_series || []).slice(-6).map(x => x.avg_price).filter(p => typeof p === "number");
    const sparkHTML = createSparkline(sparkData);

    tr.innerHTML = `
      <td data-label="Producto">
        <div class="product-cell">
          <div class="product-copy">
            ${linked}
            ${metaText ? `<span class="row-meta">${metaText}</span>` : ""}
          </div>
          <span class="row-chevron" aria-hidden="true">${chevron}</span>
        </div>
      </td>
      <td data-label="Formato">${esc(presentation)}</td>
      <td class="num" data-label="Precio">${money(r.current_price)}</td>
      <td class="num flex-right" data-label="Var. (Nominal)">${sparkHTML}<span class="${nomCls}">${trendIcon(r.variation_nominal_pct)} ${pctSigned(r.variation_nominal_pct)}</span></td>
      ${st.show_real_column ? `<td class="num" data-label="Var. (Real)"><span class="${realCls}">${trendIcon(r.variation_real_pct)} ${pctSigned(r.variation_real_pct)}</span></td>` : ""}
    `;

    if (hasCandidates) {
      tr.tabIndex = 0;
      tr.setAttribute("role", "button");
      tr.setAttribute("aria-expanded", isExpanded ? "true" : "false");
      tr.setAttribute("aria-label", `${isExpanded ? "Ocultar" : "Ver"} opciones de ${nameRaw}`);
      const toggleRow = () => {
        expandedProductId = (expandedProductId === productId) ? "" : productId;
        drawTable(rows);
      };
      tr.addEventListener("click", (ev) => {
        const target = (ev.target instanceof Element) ? ev.target : null;
        if (target && target.closest("a")) return;
        toggleRow();
      });
      tr.addEventListener("keydown", (ev) => {
        if (ev.key === "Enter" || ev.key === " ") {
          ev.preventDefault();
          toggleRow();
        }
      });
    }
    fragment.appendChild(tr);

    if (isExpanded) {
      ["low", "mid", "high"].forEach((tier) => {
        const candidate = triplet[tier] || null;
        fragment.appendChild(buildCandidateSubRow(tier, candidate));
      });
    }
  }
  el.tb.appendChild(fragment);
}

function drawKpis() {
  const k = p.kpi_summary || {};
  const periodLabel = k.kpi_fallback_used
    ? `Periodo efectivo ${monthLabel(k.from_month)} a ${monthLabel(k.to_month)} (fallback por datos)`
    : `Periodo ${monthLabel(k.from_month)} a ${monthLabel(k.to_month)}`;
  const fullCards = [
    { key: "basket_nominal", label: "Canasta (nominal)", value: pct(k.inflation_basket_nominal_pct), sub: periodLabel },
    { key: "ipc", label: "IPC oficial", value: pct(k.ipc_period_pct), sub: "Periodo" },
    { key: "gap", label: "Brecha", value: pct(k.gap_vs_ipc_pp), sub: "Canasta - IPC" },
    { key: "basket_real", label: "Canasta (real)", value: pct(k.inflation_basket_real_pct), sub: "Deflactada" },
    { key: "amplitude", label: "Amplitud de subas", value: pct(k.amplitude_up_pct), sub: "% con suba" },
    { key: "dispersion", label: "Dispersion (IQR)", value: pct(k.dispersion_iqr_pct), sub: "P75 - P25" },
  ];
  const cards = st.view === "executive"
    ? fullCards
      .filter(card => ["basket_nominal", "ipc", "gap"].includes(card.key))
      .map(card => ({ ...card, sub: card.key === "basket_nominal" ? periodLabel : "" }))
    : fullCards;
  const tone = (key, value) => {
    if (value == null || Number.isNaN(Number(value))) return "warn";
    const n = Number(value);
    if (key === "gap") return n > 0 ? "bad" : (n < 0 ? "good" : "warn");
    if (key === "basket_nominal") return n > 0 ? "bad" : (n < 0 ? "good" : "warn");
    if (key === "basket_real") return n > 0 ? "bad" : (n < 0 ? "good" : "warn");
    if (key === "amplitude") return n >= 50 ? "bad" : "good";
    if (key === "dispersion") return n > 15 ? "bad" : "good";
    return n > 0 ? "bad" : "good";
  };
  el.kpiGrid.innerHTML = "";
  cards.forEach((cardData) => {
    const sub = cardData.sub || "";
    const card = document.createElement("article");
    card.className = `kpi ${tone(cardData.key, cardData.value)}`;
    card.innerHTML =
      `<div class="label">${esc(cardData.label)}</div>`
      + `<div class="value">${esc(cardData.value)}</div>`
      + `<div class="sub">${esc(sub)}</div>`;
    el.kpiGrid.appendChild(card);
  });
}

function drawQuality() {
  const qf = p.quality_flags || {};
  const cov = p.coverage || {};
  const k = p.kpi_summary || {};
  const sq = p.scrape_quality || {};
  const region = st.macro_region || p.macro_default_region || "patagonia";
  const regionLabel = region === "nacional" ? "Nacional" : "Patagonia";
  const pub = (p.publication_status_by_region?.[region] || p.publication_status || {});
  const trackerSeries = p.tracker_ipc_series || [];
  const officialSeries = (p.official_series_by_region?.[region] || p.official_patagonia_series || []);
  const latestTracker = ([...trackerSeries].reverse().find(x => x.index_value != null) || trackerSeries[trackerSeries.length - 1] || {});
  const latestOfficial = ([...officialSeries].reverse().find(x => x.cpi_index != null) || officialSeries[officialSeries.length - 1] || {});
  const cba = sq.cba || {};
  const core = sq.daily_core || {};
  const rot = sq.daily_rotation || {};
  const missingMonths = (qf.missing_cpi_months || []);
  const qualityLabel = qf.is_partial ? "Parcial" : "Completo";
  el.qualityBadge.textContent = qualityLabel;
  el.qualityBadge.className = `badge${qf.is_partial ? " warn" : ""}`;
  el.qualityCoverage.textContent =
    `Cobertura: ${fmtNum(cov.coverage_total_pct)}% `
    + `(${cov.observed_products_total ?? "N/D"}/${cov.expected_products ?? "N/D"}).`;
  el.qualityPanelSize.textContent = `Panel: ${qf.balanced_panel_n ?? "N/D"} productos.`;
  if (el.qualityMacro) {
    el.qualityMacro.textContent =
      `Tracker IPC: ${(trackerSeries[0]?.year_month) || "N/D"} a ${(latestTracker?.year_month) || "N/D"}.`;
  }
  el.qualityIpc.textContent =
    `IPC oficial (${regionLabel}): ${(latestOfficial?.year_month) || "N/D"} · `
    + `estado ${(pub.status) || "sin_publicacion"} · `
    + `sin IPC: ${missingMonths.length ? missingMonths.join(", ") : "ninguno"}.`;
  if (el.qualitySegments) {
    el.qualitySegments.textContent =
      `Segmentos: CBA ${cba.observed ?? 0}/${cba.expected ?? 0}, `
      + `Núcleo ${core.observed ?? 0}/${core.expected ?? 0}, `
      + `Rotación ${rot.observed ?? 0}/${rot.expected ?? 0}.`;
  }
  if (el.qualityPolicy) {
    el.qualityPolicy.textContent =
      `Publicación: ${p.publication_policy || "N/D"}. `
      + `terna low/mid/high auditada: ${pct(sq.terna_compliance_pct)} `
      + `(${sq.products_with_full_terna ?? 0}/${sq.products_with_bands ?? 0}).`;
  }
  if (el.qualitySummary) {
    el.qualitySummary.textContent =
      `${fmtNum(cov.coverage_total_pct)}% cobertura · ${qf.balanced_panel_n ?? "N/D"} productos`;
  }
  el.warnings.innerHTML = "";
  for (const w of (qf.warnings || [])) {
    const li = document.createElement("li");
    li.textContent = w;
    el.warnings.appendChild(li);
  }
  if (k.kpi_fallback_used) {
    const li = document.createElement("li");
    li.textContent = `KPI con ventana efectiva ${k.from_month} -> ${k.to_month}.`;
    el.warnings.appendChild(li);
  }
  if (el.freshnessMeta) {
    const statusRaw = (p.web_status || "partial").toLowerCase();
    const statusLabel = { fresh: "Actualizado", partial: "En proceso", stale: "Desactualizado" }[statusRaw] || "En proceso";
    const nextRun = p.next_update_eta || null;
    const lastData = p.last_data_timestamp ? fmtDate(p.last_data_timestamp) : null;
    const parts = [statusLabel];
    if (lastData) parts.push(`Último dato: ${lastData}`);
    el.freshnessMeta.textContent = parts.join(" · ");
    if (nextRun) el.freshnessMeta.title = `Próxima actualización: ${nextRun}`;
  }
}

function drawMonetization() {
  const ads = p.ads || {};
  if (el.adPanel && el.adSlots) {
    if (!ads.enabled) {
      el.adPanel.style.display = "none";
    } else {
      const slots = (ads.slots || ["header", "inline", "sidebar", "footer"]).map(v => String(v || "").trim()).filter(Boolean);
      const provider = String(ads.provider || "").toLowerCase();
      const consent = consentState();
      const clientId = String(ads.client_id || ads.client_id_placeholder || "").trim();
      el.adPanel.style.display = "";
      el.adSlots.innerHTML = "";

      if (provider !== "adsense") {
        slots.forEach(slot => {
          const div = document.createElement("div");
          div.className = "ad-slot";
          div.setAttribute("data-slot", slot);
          div.textContent = `Espacio publicitario (${provider || "proveedor"}): ${slot}`;
          el.adSlots.appendChild(div);
        });
      } else if (consent !== "accepted") {
        const div = document.createElement("div");
        div.className = "ad-slot";
        div.textContent = consent === "rejected"
          ? "Publicidad desactivada por preferencia de cookies."
          : "Acepta cookies para habilitar anuncios.";
        el.adSlots.appendChild(div);
      } else {
        ensureAdSenseScript(clientId);
        slots.forEach(slot => {
          const div = document.createElement("div");
          div.className = "ad-slot";
          div.setAttribute("data-slot", slot);
          const ins = document.createElement("ins");
          ins.className = "adsbygoogle";
          ins.style.display = "block";
          ins.setAttribute("data-ad-client", clientId);
          ins.setAttribute("data-ad-slot", String(slot || "").replace(/[^0-9]/g, "") || "0000000000");
          ins.setAttribute("data-ad-format", "auto");
          ins.setAttribute("data-full-width-responsive", "true");
          div.appendChild(ins);
          el.adSlots.appendChild(div);
          try { (window.adsbygoogle = window.adsbygoogle || []).push({}); } catch (_e) { }
        });
      }
    }
  }

  const premium = p.premium_placeholders || {};
  if (el.premiumPanel && el.premiumFeatures) {
    if (premium.enabled) {
      const features = (premium.features || []).map(v => String(v || "").trim()).filter(Boolean);
      el.premiumFeatures.innerHTML = "";
      features.forEach(feature => {
        const li = document.createElement("li");
        li.textContent = feature;
        el.premiumFeatures.appendChild(li);
      });
      el.premiumPanel.style.display = "";
    } else {
      el.premiumPanel.style.display = "none";
    }
  }
}

function initConsentBanner() {
  if (!el.cookieBanner) return;
  const saved = consentState();
  if (saved === "accepted" || saved === "rejected") {
    el.cookieBanner.style.display = "none";
    drawMonetization();
    return;
  }
  el.cookieBanner.style.display = "";
  if (el.cookieAccept) {
    el.cookieAccept.addEventListener("click", () => {
      try { window.localStorage?.setItem?.(COOKIE_KEY, "accepted"); } catch (_e) { }
      el.cookieBanner.style.display = "none";
      drawMonetization();
      trackEvent("cookie_consent_updated", { state: "accepted" });
    });
  }
  if (el.cookieReject) {
    el.cookieReject.addEventListener("click", () => {
      try { window.localStorage?.setItem?.(COOKIE_KEY, "rejected"); } catch (_e) { }
      el.cookieBanner.style.display = "none";
      drawMonetization();
      trackEvent("cookie_consent_updated", { state: "rejected" });
    });
  }
}

function dismissMobileOnboarding(persist = true) {
  if (!el.mobileOnboarding) return;
  el.mobileOnboarding.setAttribute("hidden", "hidden");
  if (persist) {
    try { window.localStorage?.setItem?.(ONBOARDING_KEY, "dismissed"); } catch (_e) { }
  }
}

function initMobileOnboarding() {
  if (!el.mobileOnboarding) return;
  const isMobile = window.innerWidth <= 760;
  const saved = window.localStorage?.getItem?.(ONBOARDING_KEY) || "";
  if (!isMobile || saved === "dismissed") {
    dismissMobileOnboarding(false);
    return;
  }
  window.setTimeout(() => {
    if (el.mobileOnboarding) {
      el.mobileOnboarding.removeAttribute("hidden");
    }
  }, 380);
  if (el.onboardingClose) {
    el.onboardingClose.addEventListener("click", () => dismissMobileOnboarding(true));
  }
  if (el.onboardingGoto) {
    el.onboardingGoto.addEventListener("click", () => {
      if (el.filtersPanel) {
        el.filtersPanel.open = true;
        el.filtersPanel.scrollIntoView({ behavior: "smooth", block: "start" });
      }
      dismissMobileOnboarding(true);
    });
  }
}

function mountFilterOptions() {
  el.cat.innerHTML = "<option value='all'>Todas</option>";
  for (const c of (p.categories || [])) {
    const o = document.createElement("option");
    o.value = c; o.textContent = c; el.cat.appendChild(o);
  }
  el.mb.innerHTML = "";
  for (const m of (p.months || [])) {
    const o = document.createElement("option");
    o.value = m; o.textContent = m; el.mb.appendChild(o);
  }
  if (el.pageSize) {
    el.pageSize.innerHTML = "";
    const sizes = (p.filters_available?.page_sizes || [25, 50, 100, 250]).map(Number).filter(v => v > 0);
    sizes.forEach(v => {
      const o = document.createElement("option");
      o.value = String(v);
      o.textContent = String(v);
      el.pageSize.appendChild(o);
    });
  }
  if (el.macroScope) {
    const scopes = (p.filters_available?.macro_scopes || ["general", "rubros"]);
    el.macroScope.innerHTML = "";
    scopes.forEach(scope => {
      const o = document.createElement("option");
      o.value = scope;
      o.textContent = scope === "rubros" ? "Rubros" : "General";
      el.macroScope.appendChild(o);
    });
  }
  if (el.macroRegion) {
    const regions = (p.filters_available?.macro_regions || p.official_regions || ["patagonia"]);
    el.macroRegion.innerHTML = "";
    regions.forEach(region => {
      const o = document.createElement("option");
      o.value = region;
      o.textContent = region === "nacional" ? "Nacional" : "Patagonia";
      el.macroRegion.appendChild(o);
    });
  }
  if (el.macroCategory) {
    const categories = (p.filters_available?.macro_categories || []);
    el.macroCategory.innerHTML = "";
    if (!categories.length) {
      const o = document.createElement("option");
      o.value = "";
      o.textContent = "Sin rubros comparables";
      el.macroCategory.appendChild(o);
      el.macroCategory.disabled = true;
    } else {
      categories.forEach(cat => {
        const o = document.createElement("option");
        o.value = cat;
        o.textContent = cat;
        el.macroCategory.appendChild(o);
      });
      el.macroCategory.disabled = false;
    }
  }
}

function setButtonsState() {
  el.modeNominal.classList.toggle("active", st.price_mode === "nominal");
  el.modeReal.classList.toggle("active", st.price_mode === "real");
}

function applyStateToControls() {
  el.q.value = st.query || "";
  if (el.clearSearch) {
    el.clearSearch.disabled = !(st.query || "").trim();
  }
  el.cba.value = st.cba_filter || "all";
  el.cat.value = st.category || "all";
  el.ord.value = st.sort_by || "alphabetical";
  el.mb.value = (p.months || []).includes(st.base_month) ? st.base_month : (p.months?.[0] || "");
  st.base_month = el.mb.value;
  el.showReal.checked = !!st.show_real_column;
  if (el.macroScope) {
    const validScopes = Array.from(el.macroScope.options).map(o => o.value);
    if (!validScopes.includes(st.macro_scope)) {
      st.macro_scope = validScopes.includes("general") ? "general" : (validScopes[0] || "general");
    }
    el.macroScope.value = st.macro_scope;
  }
  if (el.macroRegion) {
    const validRegions = Array.from(el.macroRegion.options).map(o => o.value);
    if (!validRegions.includes(st.macro_region)) {
      st.macro_region = validRegions.includes(p.macro_default_region || "") ? (p.macro_default_region || "") : (validRegions[0] || "patagonia");
    }
    el.macroRegion.value = st.macro_region;
  }
  if (el.macroCategory) {
    const validCats = Array.from(el.macroCategory.options).map(o => o.value);
    if (!validCats.includes(st.macro_category)) {
      st.macro_category = validCats[0] || "";
    }
    el.macroCategory.value = st.macro_category;
    el.macroCategory.disabled = st.macro_scope !== "rubros" || validCats.length === 0;
  }
  if (el.pageSize) {
    const validSizes = Array.from(el.pageSize.options).map(o => Number(o.value));
    if (!validSizes.includes(Number(st.page_size))) {
      st.page_size = validSizes.includes(25) ? 25 : validSizes[0];
    }
    el.pageSize.value = String(st.page_size);
  }
  st.current_page = Math.max(1, Number(st.current_page || 1));
  setButtonsState();
}

function maybeDrawBandPanel(rows, force = false) {
  if (force || lazyPanels.bands_ready) {
    drawBandChart(rows);
  }
}

function maybeDrawQualityPanel(force = false) {
  if (force || lazyPanels.quality_ready) {
    drawQuality();
  }
}

function initLazyPanels() {
  const hasBandPanel = !!(el.panelBands && el.chartBands && el.legendBands);
  const hasQualityPanel = !!el.qualityPanel;

  if (!hasBandPanel && !hasQualityPanel) {
    lazyPanels.bands_ready = true;
    lazyPanels.quality_ready = true;
    return;
  }

  if (typeof window.IntersectionObserver !== "function") {
    lazyPanels.bands_ready = hasBandPanel;
    lazyPanels.quality_ready = hasQualityPanel;
    return;
  }

  const observer = new window.IntersectionObserver((entries) => {
    entries.forEach((entry) => {
      if (!entry.isIntersecting) return;

      if (hasBandPanel && entry.target === el.panelBands && !lazyPanels.bands_ready) {
        lazyPanels.bands_ready = true;
        maybeDrawBandPanel(filteredRows(), true);
        observer.unobserve(entry.target);
      }
      if (hasQualityPanel && entry.target === el.qualityPanel && !lazyPanels.quality_ready) {
        lazyPanels.quality_ready = true;
        maybeDrawQualityPanel(true);
        observer.unobserve(entry.target);
      }
    });
  }, {
    root: null,
    rootMargin: "260px 0px 260px 0px",
    threshold: 0.01
  });

  lazyPanels.observer = observer;
  if (hasBandPanel) observer.observe(el.panelBands);
  if (hasQualityPanel) observer.observe(el.qualityPanel);
}

function render() {
  if (el.clearSearch) {
    el.clearSearch.disabled = !(st.query || "").trim();
  }
  const rows = filteredRows();
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

function resetState() {
  st.query = defaults.query || "";
  st.cba_filter = defaults.cba_filter || "all";
  st.category = defaults.category || "all";
  st.sort_by = defaults.sort_by || "alphabetical";
  st.base_month = defaults.base_month || (p.months?.[0] || "");
  st.selected_products = [...(defaults.selected_products || [])];
  st.price_mode = defaults.price_mode || "nominal";
  st.show_real_column = !!defaults.show_real_column;
  st.macro_scope = defaults.macro_scope || "general";
  st.macro_region = defaults.macro_region || p.macro_default_region || "patagonia";
  st.macro_category = defaults.macro_category || "";
  st.view = defaults.view || "executive";
  st.band_product = defaults.band_product || "";
  st.page_size = Number(defaults.page_size || 25);
  st.current_page = Number(defaults.current_page || 1);
  _rowsCacheKey = "";
  _rowsCacheValue = [];
  _lastMainChartKey = "";
  _lastSecondaryChartKey = "";
  applyStateToControls();
  applyViewDensityDefaults();
  render();
}

function quickPick(kind) {
  const rows = filteredRows().filter(r => r.variation_nominal_pct != null);
  if (!rows.length) return;
  if (kind === "up") {
    rows.sort((a, b) => (b.variation_nominal_pct ?? -Infinity) - (a.variation_nominal_pct ?? -Infinity));
  } else if (kind === "down") {
    rows.sort((a, b) => (a.variation_nominal_pct ?? Infinity) - (b.variation_nominal_pct ?? Infinity));
  } else {
    rows.sort((a, b) => Math.abs(a.variation_nominal_pct ?? 999) - Math.abs(b.variation_nominal_pct ?? 999));
  }
  st.selected_products = rows.slice(0, 5).map(r => r.canonical_id);
  st.current_page = 1;
  render();
}

function clearFilterToken(filterKey) {
  const defaultBase = p.months?.[0] || "";
  if (filterKey === "query") {
    st.query = "";
  } else if (filterKey === "cba") {
    st.cba_filter = "all";
  } else if (filterKey === "category") {
    st.category = "all";
  } else if (filterKey === "sort_by") {
    st.sort_by = "alphabetical";
  } else if (filterKey === "base_month") {
    st.base_month = defaultBase;
  } else if (filterKey === "price_mode") {
    st.price_mode = "nominal";
    setButtonsState();
  } else if (filterKey === "show_real_column") {
    st.show_real_column = false;
  } else if (filterKey === "macro_scope") {
    st.macro_scope = "general";
  } else if (filterKey === "macro_region") {
    st.macro_region = p.macro_default_region || "patagonia";
  } else if (filterKey === "macro_category") {
    st.macro_category = (p.filters_available?.macro_categories || [])[0] || "";
  } else {
    return;
  }
  st.current_page = 1;
  _rowsCacheKey = "";
  _lastMainChartKey = "";
  applyStateToControls();
  render();
}

function debounce(fn, ms) {
  let t = null;
  return (...args) => {
    if (t) clearTimeout(t);
    t = setTimeout(() => fn(...args), ms);
  };
}

function applyViewDensityDefaults() {
  const compact = st.view === "executive";
  if (el.mainChartPanel) {
    el.mainChartPanel.open = !compact;
  }
  if (el.panelSecondary) {
    el.panelSecondary.open = !compact;
  }
  if (el.panelBands) {
    el.panelBands.open = !compact;
  }
  if (el.qualityPanel) {
    el.qualityPanel.open = !compact;
  }
}

function bindShortcuts() {
  document.addEventListener("keydown", (e) => {
    if (e.defaultPrevented) return;
    const target = e.target;
    const tag = String(target?.tagName || "").toLowerCase();
    const editable = !!target?.isContentEditable || tag === "input" || tag === "textarea" || tag === "select";
    if (e.key === "/" && !editable) {
      e.preventDefault();
      if (el.filtersPanel && window.innerWidth < 900 && !el.filtersPanel.open) {
        el.filtersPanel.open = true;
      }
      if (el.q) {
        el.q.focus();
        el.q.select?.();
      }
      return;
    }
    if (e.key === "Escape" && target === el.q && (el.q?.value || "").trim()) {
      e.preventDefault();
      st.query = "";
      el.q.value = "";
      st.current_page = 1;
      _rowsCacheKey = "";
      render();
    }
  });
}

function bindEvents() {
  el.q.addEventListener("input", debounce((e) => { st.query = e.target.value || ""; st.current_page = 1; _rowsCacheKey = ""; render(); }, 300));
  if (el.clearSearch) {
    el.clearSearch.addEventListener("click", () => {
      st.query = "";
      el.q.value = "";
      st.current_page = 1;
      _rowsCacheKey = "";
      render();
      el.q.focus();
    });
  }
  el.cba.addEventListener("change", (e) => { st.cba_filter = e.target.value; st.current_page = 1; _rowsCacheKey = ""; render(); });
  el.cat.addEventListener("change", (e) => { st.category = e.target.value; st.current_page = 1; _rowsCacheKey = ""; render(); });
  el.ord.addEventListener("change", (e) => { st.sort_by = e.target.value; st.current_page = 1; _rowsCacheKey = ""; render(); });
  el.mb.addEventListener("change", (e) => { st.base_month = e.target.value; st.current_page = 1; _rowsCacheKey = ""; render(); });
  el.sel.addEventListener("change", () => {
    st.selected_products = Array.from(el.sel.selectedOptions).map(o => o.value);
    _lastMainChartKey = "";
    drawMainChart(filteredRows(), true);
    saveState();
  });
  el.showReal.addEventListener("change", (e) => { st.show_real_column = !!e.target.checked; render(); });
  el.modeNominal.addEventListener("click", () => { st.price_mode = "nominal"; setButtonsState(); _lastMainChartKey = ""; drawMainChart(filteredRows(), true); saveState(); });
  el.modeReal.addEventListener("click", () => { st.price_mode = "real"; setButtonsState(); _lastMainChartKey = ""; drawMainChart(filteredRows(), true); saveState(); });
  if (el.macroScope) {
    el.macroScope.addEventListener("change", (e) => {
      st.macro_scope = e.target.value || "general";
      _lastSecondaryChartKey = "";
      drawSecondaryChart(true);
      drawActiveFilters(filteredRows().length);
      saveState();
    });
  }
  if (el.macroRegion) {
    el.macroRegion.addEventListener("change", (e) => {
      st.macro_region = e.target.value || p.macro_default_region || "patagonia";
      _lastSecondaryChartKey = "";
      drawSecondaryChart(true);
      maybeDrawQualityPanel();
      drawActiveFilters(filteredRows().length);
      saveState();
    });
  }
  if (el.macroCategory) {
    el.macroCategory.addEventListener("change", (e) => {
      st.macro_category = e.target.value || "";
      _lastSecondaryChartKey = "";
      drawSecondaryChart(true);
      drawActiveFilters(filteredRows().length);
      saveState();
    });
  }
  if (el.bandProduct) {
    el.bandProduct.addEventListener("change", (e) => { st.band_product = e.target.value || ""; maybeDrawBandPanel(filteredRows(), true); saveState(); });
  }
  el.quickUp.addEventListener("click", () => quickPick("up"));
  el.quickDown.addEventListener("click", () => quickPick("down"));
  el.quickFlat.addEventListener("click", () => quickPick("flat"));
  if (el.pageSize) {
    el.pageSize.addEventListener("change", (e) => {
      st.page_size = Number(e.target.value || 25);
      st.current_page = 1;
      render();
    });
  }
  if (el.pagePrev) {
    el.pagePrev.addEventListener("click", () => {
      st.current_page = Math.max(1, Number(st.current_page || 1) - 1);
      render();
    });
  }
  if (el.pageNext) {
    el.pageNext.addEventListener("click", () => {
      st.current_page = Number(st.current_page || 1) + 1;
      render();
    });
  }
  if (el.exportCsv) {
    el.exportCsv.addEventListener("click", () => exportFilteredCsv(filteredRows()));
  }
  if (el.copyLink) {
    el.copyLink.addEventListener("click", () => { copyCurrentViewLink(); });
  }
  if (el.activeFilters) {
    el.activeFilters.addEventListener("click", (e) => {
      const target = e.target?.closest?.("[data-filter]");
      if (!target) return;
      const key = target.getAttribute("data-filter");
      clearFilterToken(key || "");
    });
  }
  if (el.mainChartPanel) {
    el.mainChartPanel.addEventListener("toggle", () => {
      if (el.mainChartPanel.open) {
        _lastMainChartKey = "";
        drawMainChart(filteredRows(), true);
      }
    });
  }
  if (el.panelSecondary) {
    el.panelSecondary.addEventListener("toggle", () => {
      if (el.panelSecondary.open) {
        _lastSecondaryChartKey = "";
        drawSecondaryChart(true);
      }
    });
  }
  if (el.panelBands) {
    el.panelBands.addEventListener("toggle", () => {
      if (el.panelBands.open) {
        maybeDrawBandPanel(filteredRows(), true);
      }
    });
  }
  el.reset.addEventListener("click", resetState);
  window.addEventListener("resize", debounce(() => {
    const rows = filteredRows();
    _lastMainChartKey = "";
    _lastSecondaryChartKey = "";
    drawMainChart(rows);
    drawSecondaryChart();
    maybeDrawBandPanel(rows);
  }, 150));
}

function init() {
  const loadEl = document.getElementById("tracker-loading");
  if (loadEl) loadEl.style.display = "none";

  initConsentBanner();
  initMobileOnboarding();
  drawMonetization();
  trackEvent("tracker_view", { status: p.web_status || "partial", has_data: p.has_data ? "1" : "0" });
  if (!p.has_data) {
    document.getElementById("empty").style.display = "";
    document.getElementById("app").style.display = "none";
    if (el.quickGuide) el.quickGuide.style.display = "none";
    dismissMobileOnboarding(false);
    maybeDrawQualityPanel(true);
    return;
  }
  const appEl = document.getElementById("app");
  if (appEl) appEl.style.display = "";
  if (window.innerWidth < 900) {
    const fp = document.getElementById("filters-panel");
    if (fp) fp.open = false;
  }
  loadState();
  mountFilterOptions();
  applyStateToControls();
  applyViewDensityDefaults();
  bindShortcuts();
  bindEvents();
  initLazyPanels();
  render();
}
init();