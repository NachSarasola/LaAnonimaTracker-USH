from __future__ import annotations

from datetime import date, datetime
from io import BytesIO
from typing import Dict, List, Optional

import pandas as pd
import plotly.express as px
import requests
import streamlit as st

DEFAULT_API_BASE = "http://127.0.0.1:8000"


st.set_page_config(page_title="La Anónima Tracker Dashboard", layout="wide")
st.title("📈 La Anónima Tracker Dashboard")
st.caption("Visualización de evolución por producto y categoría consumiendo la API de tracker.")


def _iso(d: Optional[date]) -> Optional[str]:
    return d.isoformat() if d else None


@st.cache_data(ttl=300)
def fetch_paginated(endpoint: str, params: Dict[str, str], api_base: str) -> pd.DataFrame:
    all_items: List[Dict] = []
    page = 1

    while True:
        req_params = {**params, "page": page, "page_size": 500}
        response = requests.get(f"{api_base.rstrip('/')}{endpoint}", params=req_params, timeout=30)
        response.raise_for_status()
        payload = response.json()

        items = payload.get("items", [])
        pagination = payload.get("pagination", {})
        all_items.extend(items)

        total_pages = pagination.get("total_pages", 0)
        if page >= total_pages or total_pages == 0:
            break
        page += 1

    return pd.DataFrame(all_items)


def to_csv_bytes(df: pd.DataFrame) -> bytes:
    buffer = BytesIO()
    df.to_csv(buffer, index=False)
    return buffer.getvalue()


with st.sidebar:
    st.header("⚙️ Controles")
    api_base = st.text_input("Base URL API", value=DEFAULT_API_BASE)
    start_date, end_date = st.date_input(
        "Rango de fechas",
        value=(date.today().replace(day=1), date.today()),
        format="YYYY-MM-DD",
    )
    if start_date > end_date:
        st.error("La fecha inicial no puede ser mayor a la fecha final")
        st.stop()

product_params = {"from": _iso(start_date), "to": _iso(end_date)}
category_params = {
    "from": start_date.strftime("%Y-%m"),
    "to": end_date.strftime("%Y-%m"),
}

try:
    product_df = fetch_paginated("/series/producto", product_params, api_base)
    ipc_df = fetch_paginated("/ipc/categorias", category_params, api_base)
except requests.RequestException as exc:
    st.error(f"No se pudo obtener datos de la API: {exc}")
    st.stop()

if not product_df.empty:
    product_df["scraped_at"] = pd.to_datetime(product_df["scraped_at"])

if not ipc_df.empty:
    ipc_df["year_month"] = pd.to_datetime(ipc_df["year_month"], format="%Y-%m")

st.subheader("1) Evolución por producto")
if product_df.empty:
    st.info("No hay datos de productos para el rango seleccionado.")
else:
    query = st.text_input("Buscar por nombre o canonical_id")

    filtered_products = product_df.copy()
    if query:
        mask = filtered_products["canonical_id"].str.contains(query, case=False, na=False) | filtered_products[
            "product_name"
        ].str.contains(query, case=False, na=False)
        filtered_products = filtered_products[mask]

    product_options = sorted(filtered_products["canonical_id"].dropna().unique().tolist())
    selected_products = st.multiselect(
        "Seleccionar productos (canonical_id)",
        options=product_options,
        default=product_options[:3],
    )

    if selected_products:
        plot_df = filtered_products[filtered_products["canonical_id"].isin(selected_products)].copy()
        plot_df = plot_df.sort_values("scraped_at")
        fig = px.line(
            plot_df,
            x="scraped_at",
            y="current_price",
            color="canonical_id",
            hover_data=["product_name", "basket_id", "in_stock", "is_promotion"],
            markers=True,
            title="Evolución de precios por producto",
        )
        st.plotly_chart(fig, use_container_width=True)
        st.download_button(
            "Exportar CSV (productos filtrados)",
            data=to_csv_bytes(plot_df),
            file_name="evolucion_productos_filtrados.csv",
            mime="text/csv",
        )
    else:
        st.warning("Seleccioná al menos un producto para visualizar la evolución.")

st.divider()
st.subheader("2) Evolución por categoría")
if ipc_df.empty:
    st.info("No hay datos de categorías para el rango seleccionado.")
else:
    category_options = sorted(ipc_df["category"].dropna().unique().tolist())
    selected_categories = st.multiselect(
        "Categorías",
        options=category_options,
        default=category_options[:5],
    )

    cat_df = ipc_df[ipc_df["category"].isin(selected_categories)].copy() if selected_categories else pd.DataFrame()

    if not cat_df.empty and "coverage_warning" in cat_df.columns:
        warning_rows = cat_df[cat_df["coverage_warning"].fillna(False)]
        if not warning_rows.empty:
            categories_with_warning = sorted(warning_rows["category"].dropna().unique().tolist())
            st.warning(
                "Cobertura insuficiente detectada en: "
                + ", ".join(categories_with_warning)
                + ". Revisar métricas coverage_rate/missing_count antes de interpretar estos índices."
            )
            warning_table = warning_rows[
                [
                    "year_month",
                    "category",
                    "coverage_rate",
                    "min_coverage_required",
                    "missing_count",
                    "outlier_count",
                ]
            ].sort_values(["year_month", "category"])
            st.dataframe(warning_table, use_container_width=True)

    if not cat_df.empty:
        fig_cat = px.line(
            cat_df.sort_values("year_month"),
            x="year_month",
            y="index_value",
            color="category",
            markers=True,
            title="Índice por categoría",
        )
        st.plotly_chart(fig_cat, use_container_width=True)

        variation_table = (
            cat_df.sort_values(["category", "year_month"]).groupby("category", as_index=False).agg(
                indice_inicial=("index_value", "first"),
                indice_final=("index_value", "last"),
            )
        )
        variation_table["variacion_periodo_pct"] = (
            (variation_table["indice_final"] / variation_table["indice_inicial"] - 1) * 100
        ).round(2)
        st.dataframe(variation_table, use_container_width=True)
        st.download_button(
            "Exportar CSV (categorías filtradas)",
            data=to_csv_bytes(cat_df.sort_values(["category", "year_month"])),
            file_name="evolucion_categorias_filtradas.csv",
            mime="text/csv",
        )
    else:
        st.warning("Seleccioná al menos una categoría para mostrar el índice.")

st.divider()
st.subheader("3) Comparativa categorías (ranking inflación del período)")
if ipc_df.empty:
    st.info("No hay datos para construir ranking de inflación.")
else:
    ranking_base = ipc_df.copy()
    if 'selected_categories' in locals() and selected_categories:
        ranking_base = ranking_base[ranking_base["category"].isin(selected_categories)]

    ranking_df = (
        ranking_base.sort_values(["category", "year_month"]).groupby("category", as_index=False).agg(
            indice_inicial=("index_value", "first"),
            indice_final=("index_value", "last"),
        )
    )

    if ranking_df.empty:
        st.info("No hay categorías para rankear con el filtro actual.")
    else:
        ranking_df["inflacion_periodo_pct"] = (
            (ranking_df["indice_final"] / ranking_df["indice_inicial"] - 1) * 100
        ).round(2)
        ranking_df = ranking_df.sort_values("inflacion_periodo_pct", ascending=False)

        bar = px.bar(
            ranking_df,
            x="category",
            y="inflacion_periodo_pct",
            color="inflacion_periodo_pct",
            title="Ranking de inflación por categoría",
            text="inflacion_periodo_pct",
        )
        bar.update_traces(texttemplate="%{text:.2f}%", textposition="outside")
        st.plotly_chart(bar, use_container_width=True)
        st.dataframe(ranking_df, use_container_width=True)
        st.download_button(
            "Exportar CSV (ranking categorías)",
            data=to_csv_bytes(ranking_df),
            file_name="ranking_inflacion_categorias.csv",
            mime="text/csv",
        )

st.caption(f"Última actualización dashboard: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
