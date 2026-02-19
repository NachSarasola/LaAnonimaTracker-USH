# Dashboard (Streamlit)

> Nota: este dashboard es opcional. Para una salida lista para usuario final sin levantar API/Streamlit, usa:
>
> `python -m src.cli app` (desde `laanonima-tracker/`)
>
> Tambien soporta modo economico avanzado:
> `python -m src.cli app --benchmark ipc --view executive --offline-assets embed`

Interfaz web para visualizar series de precios e índices por categoría consumiendo exclusivamente la API (`src/api.py`).

## Requisitos

```bash
pip install -r dashboard/requirements.txt
```

Además, levantar la API del proyecto:

```bash
cd laanonima-tracker
uvicorn src.api:app --host 0.0.0.0 --port 8000
```

## Ejecutar dashboard

Desde la raíz del repo:

```bash
streamlit run dashboard/app.py --server.port 8501
```

## Pantallas incluidas

1. **Evolución por producto**
   - Filtro por rango de fechas.
   - Búsqueda por `canonical_id` o nombre de producto.
   - Selección de múltiples productos.
   - Exportación CSV de datos filtrados.

2. **Evolución por categoría**
   - Selección múltiple de categorías.
   - Línea temporal del índice por categoría.
   - Tabla de variación porcentual del período.
   - Exportación CSV de datos filtrados.

3. **Comparativa categorías**
   - Ranking de inflación del período seleccionado.
   - Tabla y gráfico de barras.
   - Exportación CSV del ranking.
