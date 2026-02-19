# Runbook: Cloudflare Pages (Staging + Go-Live)

## Objetivo

Publicar `La Anonima Tracker` en Cloudflare Pages con:
- dominio final `https://preciosushuaia.com`
- staging previo en `https://staging.preciosushuaia.com`
- despliegue desde workflow `publish-web`.

## 1) Configuracion en Cloudflare Pages

1. Crear proyecto Pages (si no existe):
- nombre sugerido: `preciosushuaia`
- framework preset: `None`
- output directory: `laanonima-tracker/public`

2. Si deployas por GitHub Action (recomendado en este repo), no hace falta build command en Pages.

### Si ahora estas en "Import Git repository" (paso exacto)

Usa estos valores en el wizard:
- `Project name`: `preciosushuaia`
- `Production branch`: `main` (o tu rama principal real)
- `Framework preset`: `None`
- `Build command`: `echo "Deploy via GitHub Actions"`
- `Build output directory`: `.`

Luego, al terminar import:
1. Ir a Pages > `preciosushuaia` > Settings > Builds & deployments.
2. Desactivar Auto deployments si aparece la opcion.
   - Si no aparece, no pasa nada: el deploy real igual lo empuja GitHub Action con `cloudflare/pages-action`.
3. Verificar que el proyecto existe y copiar exactamente su nombre para secret `CLOUDFLARE_PROJECT_NAME`.

## 2) Secrets en GitHub (Repository Secrets)

Requeridos:
- `CLOUDFLARE_API_TOKEN`
- `CLOUDFLARE_ACCOUNT_ID`
- `CLOUDFLARE_PROJECT_NAME`
- `DB_URL`

Opcionales:
- `TRACKER_CONFIG_B64` (config de produccion completa)
- `PUBLIC_BASE_URL` (`https://preciosushuaia.com`)
- `PUBLIC_CONTACT_EMAIL` (`hola@preciosushuaia.com`)
- `PLAUSIBLE_DOMAIN` (si analitica activa)
- `ADSENSE_CLIENT_ID` (si anuncios activos)

Token Cloudflare recomendado:
- Permisos minimos: `Account` -> `Cloudflare Pages:Edit`, `Zone` -> `Zone:Read`
- Account scope: tu cuenta Cloudflare activa.

## 3) Dominios (staging y produccion)

1. En Pages > Custom domains:
- agregar `staging.preciosushuaia.com`
- agregar `preciosushuaia.com`
- agregar `www.preciosushuaia.com` (opcional, recomendado para redireccion)

2. SSL/TLS:
- modo `Full (strict)`
- Always Use HTTPS: `ON`

## 4) DNS esperado

Si el dominio esta en Cloudflare, Pages normalmente crea/verifica registros automaticamente.
Validar al menos:

- `staging.preciosushuaia.com` -> target Pages project.
- `preciosushuaia.com` -> target Pages project (CNAME flattening activo para apex).
- `www.preciosushuaia.com` -> CNAME a `preciosushuaia.com` (o al proyecto Pages).

## 5) Deploy de staging

1. Ejecutar workflow manual `publish-web` (GitHub Actions).
   - Para primer lanzamiento real: ejecutar con `enforce_empty_db=true`.
   - Para corridas normales posteriores: usar `enforce_empty_db=false`.
   - Antes de disparar, validar config localmente:

```bash
cd laanonima-tracker
set DB_URL=postgresql://USER:PASSWORD@HOST:5432/DBNAME
set STORAGE_BACKEND=postgresql
python scripts/prelaunch_check.py --expected-base-url https://preciosushuaia.com
```

2. Confirmar deployment exitoso en Pages.
3. Smoke test HTTP:

```bash
curl -I https://staging.preciosushuaia.com/
curl -I https://staging.preciosushuaia.com/tracker/
curl -I https://staging.preciosushuaia.com/historico/
curl -I https://staging.preciosushuaia.com/data/manifest.json
curl -I https://staging.preciosushuaia.com/data/latest.metadata.json
```

4. Smoke funcional (consistencia manifest/metadata + canonical):

```bash
cd laanonima-tracker
python scripts/smoke_public_site.py \
  --base-url https://staging.preciosushuaia.com \
  --expected-canonical-base https://preciosushuaia.com \
  --strict
```

5. Verificar en contenido:
- canonical y OG apuntan a `https://preciosushuaia.com`
- `manifest.latest` y `latest.metadata` coherentes
- estado visible `fresh/partial/stale`
- macro IPC no vacio en modo `independent_base`

## 6) Go-live (produccion)

1. Confirmar staging estable.
2. Asociar/activar `preciosushuaia.com` en Pages.
3. Verificar:
- `/`, `/tracker/`, `/historico/`, `/legal/*`
- `sitemap.xml`, `robots.txt`, `ads.txt`
- cookie consent, analytics, anuncios (si habilitados)

## 7) Rollback rapido

Si falla produccion:
1. Re-ejecutar workflow con ultimo commit estable.
2. Si problema es de datos del dia, publicar ultimo rango valido:

```bash
python -m src.cli publish-web --basket all --skip-report --from YYYY-MM --to YYYY-MM
```

3. Redeploy a Pages y validar endpoints criticos.
