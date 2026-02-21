# La Anónima Price Tracker

[![Scraper](https://github.com/YOUR_USERNAME/laanonima-tracker/actions/workflows/scrape.yml/badge.svg)](https://github.com/YOUR_USERNAME/laanonima-tracker/actions/workflows/scrape.yml)

A comprehensive price tracking system for La Anónima supermarket in Argentina, designed to monitor price changes and compare them with official CPI (IPC) inflation data.

## Features

- **Automated Price Scraping**: Uses Playwright to scrape prices from La Anónima's website
- **Branch Selection**: Automatically selects Ushuaia (CP 9410) branch for consistent local pricing
- **Basket Management**: Supports INDEC's Canasta Básica Alimentaria (CBA) and extended product lists
- **Time Series Storage**: SQLite/PostgreSQL with historical price tracking
- **Index Calculation**: Computes Laspeyres-style basket indices
- **CPI Comparison**: Compares observed inflation with official INDEC CPI data
- **Scheduled Execution**: GitHub Actions workflow for bi-monthly runs
- **Data Export**: CSV and Parquet exports for analysis

## Quick Start

### Prerequisites

- Python 3.11+
- Playwright browsers

### Installation

```bash
# Clone the repository
git clone https://github.com/YOUR_USERNAME/laanonima-tracker.git
cd laanonima-tracker

# Install dependencies
pip install -r requirements.txt

# Install Playwright browsers
playwright install chromium

# Initialize the system
python -m src.cli init
```

### Run a Scrape

```bash
# Scrape the default CBA basket
python -m src.cli scrape

# Scrape with visible browser (for debugging)
python -m src.cli scrape --no-headless

# Scrape extended basket
python -m src.cli scrape --basket extended

# Scrape all baskets
python -m src.cli scrape --basket all

# Perfil balanceado (default): CBA completa + nucleo diario + rotacion
python -m src.cli scrape --profile balanced

# Ver plan sin ejecutar navegador
python -m src.cli scrape --dry-plan --profile balanced

# Ajustar presupuesto de tiempo y rotacion
python -m src.cli scrape --runtime-budget-minutes 20 --rotation-items 4

# Auditoria de terna low/mid/high en DB (prepara modo futuro "guardar 3")
python -m src.cli scrape --observation-policy single+audit --candidate-storage db
```

Por defecto, `scrape` usa `--candidate-storage db` y deja la serie principal en `prices` con observacion representativa (`single`), manteniendo la terna auditada separada en `price_candidates`.

Modo liviano por defecto:
- Cada búsqueda toma una terna de candidatos por precio (`barato/medio/caro`) y usa el candidato medio como representativo.
- Esto evita abrir fichas de detalle para cada resultado y reduce consumo de CPU/memoria.
- Configurable en `config.yaml`: `scraping.min_candidates_per_product` (default `3`) y `scraping.min_match_confidence` (default `0.2`).
- Optimización de tiempo: se usan timeouts cortos de parseo/listado (`quick_selector_timeout_ms`, `search_settle_delay_ms`) para reducir latencia por producto.

### Recommended runnable flow (single HTML for end users)

```bash
# 1) Inicializa DB y carpetas
python -m src.cli init

# 2) Trae observaciones de precios
python -m src.cli scrape --basket all

# 3) Genera app HTML interactiva (autorange ultimos 6 meses)
python -m src.cli app
```

El comando `app` siempre genera un HTML local y su metadata JSON.
Si no hay datos, genera un estado vacio con instrucciones.
Por defecto usa benchmark IPC, vista ejecutiva y assets embebidos (offline total).

Tambien puedes forzar rango:

```bash
python -m src.cli app --from 2025-09 --to 2026-02 --basket all
```

### Reporte economico standalone (UI v2)

Comandos compatibles:

```bash
# Ejecuta reporte de rango fijo
python -m src.cli report --from 2025-09 --to 2026-02 --basket all

# App autorange (ultimos 6 meses segun datos)
python -m src.cli app
```

Flags nuevos en `report` y `app`:

```bash
--benchmark [ipc|none]          # default: ipc
--view [executive|intermediate|analyst]   # default: executive
--offline-assets [embed|external]         # default: embed
```

### Publicar web estatica (modo costo ultra-bajo)

Para publicar una web navegable (home + tracker + historico) en `public/`:

```bash
# Generar reporte y empaquetar sitio estatico
python -m src.cli publish-web --basket all --view executive --offline-assets external

# Usar el ultimo reporte ya generado
python -m src.cli publish-web --basket all --skip-report

# Rango explicito para publicacion mensual
python -m src.cli publish-web --basket all --from 2026-01 --to 2026-02 --view executive
```

Salida esperada:
- `public/index.html`
- `public/tracker/index.html`
- `public/historico/index.html`
- `public/data/manifest.json`
- `public/data/latest.metadata.json`
- `public/_headers`, `public/_redirects`, `public/404.html`

Automatizacion diaria:
- Workflow incluido: `.github/workflows/publish-web.yml`
- Deploy target: Cloudflare Pages
- En corridas programadas, `DB_URL` es obligatorio para mantener historico
- El workflow fuerza backend `postgresql` en CI (`STORAGE_BACKEND=postgresql`)
- Incluye smoke gate antes de deploy (endpoints + consistencia manifest/latest.metadata)
- Ver runbook operativo: `docs/runbook_publish_web.md`

Primer lanzamiento real (DB nueva, sin datos de prueba):

```bash
cd laanonima-tracker
set DB_URL=postgresql://USER:PASSWORD@HOST:5432/DBNAME
set STORAGE_BACKEND=postgresql
python scripts/check_db_state.py --backend postgresql --init-db --require-empty
```

Plantilla de entorno productivo:
- `.env.production.example`

Bootstrap del primer run real (en DB nueva):

```bash
cd laanonima-tracker
set DB_URL=postgresql://USER:PASSWORD@HOST:5432/DBNAME
python scripts/bootstrap_first_real_run.py
```

Preflight de lanzamiento (dominio/config/DB):

```bash
cd laanonima-tracker
set DB_URL=postgresql://USER:PASSWORD@HOST:5432/DBNAME
set STORAGE_BACKEND=postgresql
python scripts/prelaunch_check.py --expected-base-url https://preciosushuaia.com
```

Operacion diaria real (una corrida por dia, serie historica por producto):

```bash
cd laanonima-tracker
set DB_URL=postgresql://USER:PASSWORD@HOST:5432/DBNAME
python scripts/daily_real_run.py
```

Ese flujo diario:
- scrapea precios reales (`profile full`, 51 productos esperados),
- actualiza/valida IPC oficial en ventana incremental,
- construye IPC tracker una sola vez por corrida,
- publica `patagonia` y `nacional` reutilizando build (`--skip-sync --skip-build`),
- reconstruye web publica (`public/`),
- verifica persistencia de datos y smoke HTTP local en modo estricto.

Ventana IPC incremental por defecto en `daily_real_run.py`:
- desde hace 2 meses hasta mes actual (`--ipc-lookback-months 2`).
- override opcional: `--ipc-from YYYY-MM --ipc-to YYYY-MM`.

Metodologia resumida:
- `nominal`: variacion directa de precios observados.
- `real`: precio deflactado por IPC INDEC del mes.
- `brecha`: inflacion canasta nominal menos IPC del periodo.
- el KPI de inflacion usa panel balanceado (productos presentes en ambos extremos del rango).
- si no hay panel balanceado para el rango pedido, el reporte aplica fallback de ventana efectiva y lo informa en calidad.

Mejoras UX/performance del reporte:
- tabla paginada (25/50/100/250 filas), con contador y navegación.
- exportación CSV de la tabla filtrada.
- cache de filtros/render para reducir recálculos innecesarios en navegador.
- guía de lectura rápida + chips de filtros activos para entender el estado actual de análisis.

### IPC mensual propio + comparacion oficial Patagonia

El tracker ahora calcula su propio IPC mensual (canasta fija robusta, encadenado base 100) y lo compara con IPC oficial Patagonia.

Comandos nuevos:

```bash
# 1) Sincroniza IPC oficial (auto + fallback)
python -m src.cli ipc-sync --region patagonia

# 2) Construye IPC tracker mensual (general + rubros)
python -m src.cli ipc-build --basket all

# 3) Pipeline completo de publicacion mensual
python -m src.cli ipc-publish --basket all
```

Tambien puedes acotar rango:

```bash
python -m src.cli ipc-sync --from 2026-01 --to 2026-02 --region patagonia
python -m src.cli ipc-build --basket all --from 2026-01 --to 2026-02
python -m src.cli ipc-publish --basket all --from 2026-01 --to 2026-02
```

Estados mensuales de IPC tracker:
- `provisional`: mes abierto (antes de freeze).
- `final`: mes congelado tras politica D+7.
- `provisional_low_coverage`: cobertura insuficiente para cierre robusto.

Endpoints API nuevos:
- `GET /ipc/tracker`
- `GET /ipc/tracker/categorias`
- `GET /ipc/oficial/patagonia`
- `GET /ipc/comparacion`
- `GET /ipc/comparacion/categorias`
- `GET /ipc/publicacion/latest`

Compatibilidad:
- Se mantiene `GET /ipc/categorias` para clientes actuales.

### Run Analysis

```bash
# Analyze scraped data and generate reports
python -m src.cli analyze

# Generate plots
python -m src.cli analyze --plot
```

### Export Data

```bash
# Export to CSV
python -m src.cli export --format csv

# Export to Parquet
python -m src.cli export --format parquet

# Export both formats
python -m src.cli export --format both
```


### Dashboard web (Streamlit, opcional)

```bash
# Instalar dependencias del dashboard
pip install -r ../dashboard/requirements.txt

# Levantar API
uvicorn src.api:app --host 0.0.0.0 --port 8000

# En otra terminal, desde la raíz del repo
streamlit run dashboard/app.py --server.port 8501
```

El dashboard consume datos únicamente desde la API y permite filtros por fecha/categoría/producto y exportación CSV.

### View price history (time series)

Each product has a unique **canonical_id** (e.g. `cba_leche`, `cba_arroz`). Every scrape run adds one price observation per product, so you get a historical series over time.

```bash
# List recent runs and total observations
python -m src.cli history

# Show series for one product
python -m src.cli history --product cba_leche

# Export full history to CSV (one row per product per run)
python -m src.cli history --export data/exports/history.csv

# Export one product's series
python -m src.cli history --product cba_arroz --export data/exports/arroz.csv
```

Exported CSV columns: `canonical_id`, `product_name`, `basket_id`, `scraped_at`, `run_uuid`, `run_started_at`, `current_price`, `original_price`, `price_per_unit`, `in_stock`, `is_promotion`.

## Project Structure

```
laanonima-tracker/
├── .github/
│   └── workflows/
│       └── scrape.yml          # GitHub Actions workflow
├── data/
│   ├── laanonima_prices.db     # SQLite database
│   ├── exports/                # CSV/Parquet exports
│   ├── analysis/               # Analysis outputs
│   ├── logs/                   # Log files
│   └── cpi/                    # CPI data for comparison
├── notebooks/
│   └── analysis.ipynb          # Jupyter notebook for analysis
├── src/
│   ├── __init__.py
│   ├── cli.py                  # Command-line interface
│   ├── config_loader.py        # Configuration management
│   ├── models.py               # Database models
│   ├── scraper.py              # Playwright scraper
│   ├── analysis.py             # Index calculation & CPI comparison
│   └── exporter.py             # Data export functions
├── config.yaml                 # Main configuration
├── requirements.txt
└── README.md
```

## Configuration

Edit `config.yaml` to customize:

### Branch Settings

```yaml
branch:
  postal_code: "9410"        # Ushuaia
  branch_name: "USHUAIA 5"
  branch_id: "75"
```

### Basket Definition

Add or modify products in the baskets section:

```yaml
baskets:
  cba:
    items:
      - id: "cba_leche"
        name: "Leche fluida entera"
        keywords: ["leche entera", "leche fluida"]
        category: "lacteos"
        unit: "litro"
        quantity: 6
        matching: "loose"
```

### Storage Backend

```yaml
storage:
  default_backend: "sqlite"  # or "postgresql"
  
  sqlite:
    database_path: "data/laanonima_prices.db"
    
  postgresql:
    host: "${DB_HOST:localhost}"
    port: "${DB_PORT:5432}"
    database: "${DB_NAME:laanonima_tracker}"
    user: "${DB_USER:tracker}"
    password: "${DB_PASSWORD:}"
```

## Scheduling

### GitHub Actions (Recommended)

The included workflow runs automatically on:
- **1st and 15th of each month at 12:00 UTC** (9:00 AM Argentina Time)

To enable:
1. Push to GitHub
2. Go to Actions tab
3. Enable workflows
4. (Optional) Add repository secrets for PostgreSQL if using external DB

### Manual Trigger

You can also trigger runs manually from the GitHub Actions tab with custom parameters.

### Local Scheduling

#### Linux (systemd timer)

Create `/etc/systemd/system/laanonima-scraper.service`:

```ini
[Unit]
Description=La Anónima Price Scraper

[Service]
Type=oneshot
WorkingDirectory=/path/to/laanonima-tracker
ExecStart=/usr/bin/python3 -m src.cli scrape
User=youruser
```

Create `/etc/systemd/system/laanonima-scraper.timer`:

```ini
[Unit]
Description=Run La Anónima scraper twice monthly

[Timer]
OnCalendar=*-*-1,15 09:00:00
Persistent=true

[Install]
WantedBy=timers.target
```

Enable:
```bash
sudo systemctl daemon-reload
sudo systemctl enable laanonima-scraper.timer
sudo systemctl start laanonima-scraper.timer
```

#### Windows (Task Scheduler)

Create a batch file `run_scraper.bat`:

```batch
@echo off
cd /d C:\path\to\laanonima-tracker
python -m src.cli scrape
```

Then create a scheduled task in Windows Task Scheduler to run on the 1st and 15th.

## Database Schema

Each **product** has a unique `canonical_id`; each **run** adds one **price** row per product, so you get a clean time series (evolution over time) per product.

### Tables

**products**: Canonical product definitions (one row per product)
- `canonical_id`: Unique product identifier (e.g. `cba_leche`, `cba_arroz`)
- `basket_id`: Basket type (cba/extended)
- `name`, `category`, `unit`, `quantity`
- `signature_*`: For detecting product changes

**prices**: Price observations (long format)
- `canonical_id`, `basket_id`
- `current_price`, `original_price`, `price_per_unit`
- `in_stock`, `is_promotion`
- `confidence_score`, `match_method`
- `scraped_at`, `run_id`

**scrape_runs**: Execution log
- `run_uuid`, `status`
- `branch_id`, `postal_code`
- `products_scraped`, `products_failed`
- `started_at`, `completed_at`

**scrape_errors**: Error tracking
- `run_id`, `product_id`
- `stage`, `error_type`, `error_message`

**basket_indices**: Pre-computed indices
- `basket_type`, `year_month`
- `index_value`, `base_period`
- `mom_change`, `yoy_change`

## CPI Comparison

To compare with official CPI data:

1. Create `data/cpi/ipc_indec.csv` with columns:
   - `year_month`: YYYY-MM format
   - `cpi_index`: CPI index value (base = 100)
   - `cpi_mom`: Month-over-month change (%)
   - `cpi_yoy`: Year-over-year change (%)

2. Run analysis:
   ```bash
   python -m src.cli analyze
   ```

3. Check `data/analysis/` for comparison plots and CSV files.

## Troubleshooting

### Branch Selection Fails

1. Check if the website structure has changed
2. Try running with `--no-headless` to see the browser
3. Update selectors in `config.yaml` under `scraping.selectors`
4. Check screenshots in `data/logs/` for debugging

### Product Not Found

1. Verify keywords in config.yaml
2. Try alternative search terms
3. Check if product is available on the website
4. Review confidence scores in the database

### Database Issues

```bash
# Reset database (WARNING: deletes all data)
rm data/laanonima_prices.db
python -m src.cli init

# Check database status
python -m src.cli status
```

### View Logs

```bash
# Recent logs
tail -f data/logs/tracker.log

# Specific run logs
grep "RUN_UUID" data/logs/tracker.log
```

## Development

### Running Tests

```bash
pytest tests/
```

### Code Formatting

```bash
black src/
flake8 src/
```

### Adding New Products

1. Edit `config.yaml`
2. Add item to appropriate basket
3. Run test scrape: `python -m src.cli scrape --basket cba --no-headless`
4. Verify product is found with good confidence score

## Data Analysis

### Using the Jupyter Notebook

```bash
jupyter notebook notebooks/analysis.ipynb
```

### Key Metrics

- **Basket Index**: Laspeyres-style fixed-weight index
- **MoM Change**: Month-over-month inflation rate
- **YoY Change**: Year-over-year inflation rate
- **MAE/RMSE**: Error metrics vs official CPI

### Example Queries

```python
from src.models import get_engine, get_session_factory, Price, ScrapeRun
from sqlalchemy import func

engine = get_engine(config)
Session = get_session_factory(engine)
session = Session()

# Latest prices
latest = session.query(
    Price.canonical_id,
    Price.product_name,
    Price.current_price,
    Price.scraped_at
).order_by(Price.scraped_at.desc()).all()

# Average price by category
avg_by_category = session.query(
    Price.basket_id,
    func.avg(Price.current_price)
).group_by(Price.basket_id).all()
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

MIT License - See LICENSE file for details.

## Disclaimer

This tool is for educational and research purposes. Prices are scraped from publicly available data. Respect the website's robots.txt and terms of service.

## Contact

For issues or questions, please open a GitHub issue.

---

**Note**: This is an independent project not affiliated with La Anónima or INDEC.
