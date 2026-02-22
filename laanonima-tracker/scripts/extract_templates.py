import os
from pathlib import Path

repo_dir = Path(r"C:\Users\Usuario\Desktop\Proyectos IA\LaAnonimaTracker-USH\laanonima-tracker")
src_dir = repo_dir / "src"
templates_dir = src_dir / "templates"
templates_dir.mkdir(exist_ok=True)

lines = (src_dir / "reporting.py").read_text(encoding="utf-8").splitlines()

start_idx = -1
end_idx = -1
script_start_idx = -1
script_end_idx = -1

for i, line in enumerate(lines):
    if 'template = r"""<!doctype html>' in line:
        start_idx = i
    elif start_idx != -1 and '</html>"""' in line and i > start_idx + 100:
        end_idx = i
        break
    elif start_idx != -1 and '<script>' in line and i > start_idx:
        script_start_idx = i
    elif script_start_idx != -1 and '</script>' in line and i > script_start_idx:
        script_end_idx = i

if start_idx != -1 and end_idx != -1:
    print(f"Template indices: start={start_idx}, end={end_idx}")
    print(f"Script indices: script_start={script_start_idx}, script_end={script_end_idx}")
    
    html_lines = lines[start_idx:script_start_idx] + lines[script_end_idx+1:end_idx+1]
    # Remove 'template = r"""' from the first line
    html_lines[0] = html_lines[0].split('r"""', 1)[1]
    # Remove '"""' from the last line
    html_lines[-1] = html_lines[-1].replace('"""', '')
    
    (templates_dir / 'tracker.html').write_text('\n'.join(html_lines), encoding='utf-8')
    print('Wrote tracker.html')

    if script_start_idx != -1 and script_end_idx != -1:
        js_lines = lines[script_start_idx+1:script_end_idx]
        (templates_dir / 'tracker.js').write_text('\n'.join(js_lines), encoding='utf-8')
        print('Wrote tracker.js')
else:
    print(f"Failed to find bounds. start={start_idx} end={end_idx}")
