import os
from pathlib import Path

repo_dir = Path(r"C:\Users\Usuario\Desktop\Proyectos IA\LaAnonimaTracker-USH\laanonima-tracker")
src_dir = repo_dir / "src"

lines = (src_dir / "reporting.py").read_text(encoding="utf-8").splitlines()

start_idx = -1
end_idx = -1

for i, line in enumerate(lines):
    if 'generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")' in line:
        start_idx = i
    elif start_idx != -1 and 'snapshot_by_id = {str(s.get(\'canonical_id\')' in line:
        end_idx = i
        break

if start_idx != -1 and end_idx != -1:
    print(f"Replacing lines {start_idx} to {end_idx-1}")
    
    replacement = [
        '        generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")',
        '        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")',
        '        base = f"report_interactive_{effective_from}_to_{effective_to}_{stamp}".replace("-", "")',
        '',
        '        # Load ALL historical data for product detail pages and main tracker sparklines',
        '        try:',
        '            full_df = self._load_prices("2024-01", effective_to, basket_type)',
        '            full_payload = self._build_interactive_payload(full_df, "2024-01", effective_to, basket_type)',
        '            # Override dashboard monthly_reference so it contains up to 6 months of data for inline sparklines',
        '            payload["monthly_reference"] = full_payload["monthly_reference"]',
        '        except Exception:',
        '            full_df = df',
        '            full_payload = payload',
        '',
        '        html = self._render_interactive_html(',
        '            payload,',
        '            generated_at,',
        '            analysis_depth=analysis_depth,',
        '            offline_assets=offline_assets,',
        '        )',
        '        html_path = out_dir / f"{base}.html"',
        '        html_path.write_text(html, encoding="utf-8")',
        '',
        '        # Write per-product detail JSON files alongside the tracker HTML.',
        '        # web_publish.py reads these to generate /tracker/{id}/index.html pages.',
        '        try:',
        '            products_dir = out_dir / "products"',
        '            products_dir.mkdir(parents=True, exist_ok=True)',
        '            ',
    ]
    
    new_lines = lines[:start_idx] + replacement + lines[end_idx:]
    (src_dir / "reporting.py").write_text('\n'.join(new_lines), encoding='utf-8')
    print("reporting.py updated successfully to move history injection.")
else:
    print("Could not find bounds.")
