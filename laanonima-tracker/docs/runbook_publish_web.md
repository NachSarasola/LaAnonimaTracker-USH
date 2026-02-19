# Runbook: Public Static Website

## Quick Start (local)

```bash
cd laanonima-tracker
python -m src.cli app --basket all --view analyst --benchmark ipc --offline-assets external
python -m src.cli publish-web --basket all --from 2026-01 --to 2026-02 --view analyst --offline-assets external
```

Artifacts:
- `public/index.html`
- `public/tracker/index.html`
- `public/historico/index.html`
- `public/data/manifest.json`

## CI Secrets

Required for automated deploy:
- `CLOUDFLARE_API_TOKEN`
- `CLOUDFLARE_ACCOUNT_ID`
- `CLOUDFLARE_PROJECT_NAME`
- `DB_URL` (required in scheduled runs to preserve historical series)

Optional:
- `TRACKER_CONFIG_B64` (base64-encoded production `config.yaml`)

## Incident Response

1. If daily workflow fails:
- Open latest Actions run logs.
- Re-run workflow manually.

2. If scraping fails but site must stay online:
- Use latest successful build artifact and redeploy.
- `manifest.status` should remain `stale` until next successful run.

3. If Cloudflare deploy fails:
- Verify secret validity and project permissions.

## Operational Checks

- `public/data/manifest.json` has `status`, `next_update_eta`, `history`.
- `public/data/latest.metadata.json` includes `web_status`, `is_stale`.
- `public/_headers` and `public/_redirects` exist for Cloudflare hardening.
- `/tracker/` loads and filters/charts work.

## Budget Scale Path

1. `USD 0-20`: static-first (actual mode).
2. `USD 20-40`: expose API publicly behind cache/rate limit.
3. `USD 40-80`: split API + worker + managed DB observability.
4. `USD 80+`: staging/prod, queue, formal SLOs.
