# Sistema de estilos web (UI publica)

## Objetivo
Unificar el estilo visual del sitio publico con una arquitectura CSS modular, sin depender de Node o Tailwind.

## Estructura
- `src/web_assets/css/tokens.css`: variables de diseno (`:root`) y capas.
- `src/web_assets/css/base.css`: reset, tipografia base y reglas globales.
- `src/web_assets/css/shell.css`: componentes/layout de home, historico y paginas shell.
- `src/web_assets/css/tracker.css`: componentes/layout especificos del tracker.
- `src/web_assets/css/utilities.css`: clases utilitarias pequenas.
- `src/web_styles.py`: construccion de bundles (`shell-ui.css`, `tracker-ui.css`).

## Bundles
- `get_shell_css_bundle()`:
  - `tokens.css + base.css + shell.css + utilities.css`
- `get_tracker_css_bundle()`:
  - `tokens.css + base.css + tracker.css + utilities.css`

## Publicacion
- `web_publish.py` escribe `public/assets/css/shell-ui.css`.
- Todas las paginas shell cargan:
  - `<link rel="stylesheet" href="/assets/css/shell-ui.css?v=<hash>">`
- Tracker:
  - `offline_assets=embed`: CSS inline embebido para uso offline.
  - `offline_assets=external`: `<link rel="stylesheet" href="./tracker-ui.css?v=<hash>">` y archivo companion.
  - El hash viene de `web_styles.py` y permite cache largo sin stale visual.

## Reglas de mantenimiento
- No agregar bloques CSS grandes inline dentro de templates Python.
- Priorizar tokens en `tokens.css` antes de crear nuevos colores/espaciados.
- Mantener IDs y hooks JS intactos; cambios de estilo no deben tocar contratos funcionales.
