# Runbook: Public Static Website

## Publication Policy

- Policy name: `publish_with_alert_on_partial`
- Rule: if daily run has low coverage or missing IPC, publish anyway with visible status/alerts (`partial`).
- Target cadence: daily static publication.
- Cloudflare staging + DNS guide: `docs/runbook_cloudflare_pages.md`

## Quick Start (local)

Single-command public build (recommended):

```bash
cd laanonima-tracker
python -m src.cli publish-web --basket all --view analyst --benchmark ipc --offline-assets external
```

## First Real Launch (new production DB, no test carry-over)

Use a brand new production DB URL and force backend to PostgreSQL:

```bash
cd laanonima-tracker
set DB_URL=postgresql://USER:PASSWORD@HOST:5432/DBNAME
set STORAGE_BACKEND=postgresql
python scripts/check_db_state.py --backend postgresql --init-db --require-empty
```

If this check fails, you are pointing to a DB with existing data and should create a new one.

Preflight de lanzamiento (dominio + DB + ads/analytics):

```bash
cd laanonima-tracker
set DB_URL=postgresql://USER:PASSWORD@HOST:5432/DBNAME
set STORAGE_BACKEND=postgresql
python scripts/prelaunch_check.py --expected-base-url https://preciosushuaia.com
```

One-command bootstrap (first real run):

```bash
cd laanonima-tracker
set DB_URL=postgresql://USER:PASSWORD@HOST:5432/DBNAME
python scripts/bootstrap_first_real_run.py
```

Recommended daily pipeline (production parity):

```bash
python -m src.cli scrape --basket all --profile full --candidate-storage db --observation-policy single+audit
python -m src.cli ipc-sync --region all
python -m src.cli ipc-publish --basket all --region patagonia
python -m src.cli ipc-publish --basket all --region nacional
python -m src.cli publish-web --basket all --view analyst --benchmark ipc --offline-assets external
python scripts/check_db_state.py --backend postgresql --require-has-data
```

Daily one-command runner (recommended for operación diaria):

```bash
cd laanonima-tracker
set DB_URL=postgresql://USER:PASSWORD@HOST:5432/DBNAME
python scripts/daily_real_run.py
```

Range-locked rebuild (for audits):

```bash
python -m src.cli publish-web --basket all --from 2026-01 --to 2026-02 --view analyst --offline-assets external
```

Manual recovery from existing artifacts (no report rebuild):

```bash
python -m src.cli publish-web --basket all --skip-report --from 2026-01 --to 2026-02
```

`--skip-report` now enforces exact range match. If no matching report exists, command fails with non-zero exit code.

## Expected Artifacts

- `public/index.html`
- `public/tracker/index.html`
- `public/historico/index.html`
- `public/data/manifest.json`
- `public/data/latest.metadata.json`
- `public/_headers`
- `public/_redirects`

Production domain default in this repo: `https://preciosushuaia.com`.

## Consistency Checks (must pass)

1. `public/data/manifest.json` includes:
- `status`
- `latest` block (`from_month`, `to_month`, `generated_at`, `has_data`, `web_status`)
- `publication_policy`

2. `public/data/latest.metadata.json` includes:
- `web_status`
- `is_stale`
- `next_update_eta`
- `latest_range_label`
- `quality_warnings`
- `publication_policy`

3. Home and tracker must point to the same active range.

4. Macro comparison must not appear empty if at least one series exists:
- `plot_mode=strict_overlap`: tracker/oficial and brecha strictly comparable.
- `plot_mode=independent_base`: each series uses own base month; brecha disabled until overlap exists.

Optional automated smoke:

```bash
cd laanonima-tracker
python scripts/smoke_public_site.py --base-url http://127.0.0.1:8080

# Modo estricto pre-lanzamiento / staging
python scripts/smoke_public_site.py --base-url http://127.0.0.1:8080 --strict --expected-canonical-base https://preciosushuaia.com
```

## CI Secrets

Required for automated deploy:
- `CLOUDFLARE_API_TOKEN`
- `CLOUDFLARE_ACCOUNT_ID`
- `CLOUDFLARE_PROJECT_NAME`
- `DB_URL` (required in scheduled runs to preserve historical series)

Optional:
- `TRACKER_CONFIG_B64` (base64-encoded production `config.yaml`)
- `PUBLIC_BASE_URL` (override production domain when needed)
- `PUBLIC_CONTACT_EMAIL` (contact email shown in legal/contact pages)
- `PLAUSIBLE_DOMAIN` (if analytics enabled)
- `STORAGE_BACKEND=postgresql` (recommended in CI/production to avoid sqlite fallback)

## Incident Response

1. Daily workflow fails:
- Inspect last GitHub Actions logs.
- Re-run workflow manually.

2. Data pipeline degrades but website must stay online:
- Rebuild with `--skip-report --from YYYY-MM --to YYYY-MM` using last validated range.
- Redeploy artifacts.
- If macro panel shows partial comparison, verify official availability month-by-month before escalating.

4. Macro panel shows "comparación parcial" for current month:
- Confirm whether official IPC for latest month exists in `official_cpi_monthly`.
- If official month is missing (normal publication lag), keep site online with alert.
- If official month exists but not shown, run:
  - `python -m src.cli ipc-sync --region all`
  - `python -m src.cli ipc-publish --basket all --region patagonia`
  - `python -m src.cli ipc-publish --basket all --region nacional`
  - then rebuild `publish-web`.

3. Cloudflare deploy fails:
- Verify `CLOUDFLARE_*` secrets and project permissions.

## Go-Live Checklist

1. `/tracker/` renders charts, filters, CSV export, copy-link.
2. `fresh/partial/stale` status visible and coherent.
3. Cookie consent controls ad activation.
   - Safety guard: if `ads.enabled=true` but `ADSENSE_CLIENT_ID` is placeholder/empty, ads stay disabled automatically.
4. Canonical/OG/sitemap use production domain.
5. `_headers` and `_redirects` are present.

## Budget Scale Path

1. `USD 0-20`: static-first (current mode).
2. `USD 20-40`: public API with cache/rate-limit.
3. `USD 40-80`: split API + worker + managed DB observability.
4. `USD 80+`: staging/prod split, queue, formal SLOs.
