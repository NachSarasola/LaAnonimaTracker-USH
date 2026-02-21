"""Centralized CSS bundling for public web artifacts."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path


_CSS_DIR = Path(__file__).resolve().parent / "web_assets" / "css"
_BASE_PARTS = ("tokens.css", "base.css")
_UTILITY_PARTS = ("utilities.css",)
_SHELL_PARTS = ("shell.css",)
_TRACKER_PARTS = ("tracker.css",)


def _read_css(name: str) -> str:
    css = (_CSS_DIR / name).read_text(encoding="utf-8").strip()
    if not css:
        raise ValueError(f"CSS file is empty: {name}")
    return css


def _component_layer(css: str) -> str:
    if "@layer " in css:
        return css
    return f"@layer components {{\n{css}\n}}"


def _build_bundle(*parts: str) -> str:
    chunks = []
    for part in parts:
        css = _read_css(part)
        if part in _SHELL_PARTS or part in _TRACKER_PARTS:
            css = _component_layer(css)
        chunks.append(css)
    return "\n\n".join(chunks) + "\n"


@lru_cache(maxsize=1)
def get_shell_css_bundle() -> str:
    return _build_bundle(*_BASE_PARTS, *_SHELL_PARTS, *_UTILITY_PARTS)


@lru_cache(maxsize=1)
def get_tracker_css_bundle() -> str:
    return _build_bundle(*_BASE_PARTS, *_TRACKER_PARTS, *_UTILITY_PARTS)

