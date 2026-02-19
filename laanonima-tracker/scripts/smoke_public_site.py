#!/usr/bin/env python3
"""Smoke checks for static public website endpoints and metadata consistency."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from urllib.error import URLError, HTTPError
from urllib.parse import urljoin
from urllib.request import Request, urlopen


DEFAULT_PATHS = [
    "/",
    "/tracker/",
    "/historico/",
    "/data/manifest.json",
    "/data/latest.metadata.json",
]

STRICT_EXTRA_PATHS = [
    "/metodologia/",
    "/contacto/",
    "/legal/privacy.html",
    "/legal/terms.html",
    "/legal/cookies.html",
    "/legal/ads.html",
    "/sitemap.xml",
    "/robots.txt",
    "/ads.txt",
]

DEFAULT_PUBLICATION_POLICY = "publish_with_alert_on_partial"


@dataclass
class FetchResult:
    url: str
    status: Optional[int]
    body: bytes
    error: Optional[str]


def _fetch(url: str, timeout: float) -> FetchResult:
    req = Request(url, headers={"User-Agent": "laanonima-smoke/1.0"})
    try:
        with urlopen(req, timeout=timeout) as resp:  # nosec: B310 (controlled URL from args)
            return FetchResult(url=url, status=int(resp.status), body=resp.read(), error=None)
    except HTTPError as exc:
        return FetchResult(url=url, status=int(exc.code), body=b"", error=str(exc))
    except URLError as exc:
        return FetchResult(url=url, status=None, body=b"", error=str(exc))
    except Exception as exc:  # pragma: no cover
        return FetchResult(url=url, status=None, body=b"", error=str(exc))


def _decode_json(blob: bytes, name: str) -> Tuple[Optional[Dict], Optional[str]]:
    try:
        return json.loads(blob.decode("utf-8")), None
    except Exception as exc:
        return None, f"{name}: JSON invalido ({exc})"


def run_smoke(
    base_url: str,
    timeout: float,
    expected_canonical_base: Optional[str],
    strict: bool = False,
    expected_publication_policy: Optional[str] = None,
) -> int:
    base = base_url.rstrip("/") + "/"
    paths = list(DEFAULT_PATHS)
    if strict:
        paths.extend(STRICT_EXTRA_PATHS)
    checks: List[FetchResult] = []

    for path in paths:
        checks.append(_fetch(urljoin(base, path.lstrip("/")), timeout=timeout))

    errors: List[str] = []
    by_path = {path: checks[idx] for idx, path in enumerate(paths)}

    for path in paths:
        result = by_path[path]
        if result.status != 200:
            msg = result.error or f"status inesperado {result.status}"
            errors.append(f"{path}: {msg}")

    manifest_data: Optional[Dict] = None
    latest_data: Optional[Dict] = None

    if by_path["/data/manifest.json"].status == 200:
        manifest_data, err = _decode_json(by_path["/data/manifest.json"].body, "manifest.json")
        if err:
            errors.append(err)
    if by_path["/data/latest.metadata.json"].status == 200:
        latest_data, err = _decode_json(by_path["/data/latest.metadata.json"].body, "latest.metadata.json")
        if err:
            errors.append(err)

    if manifest_data and latest_data:
        latest_block = manifest_data.get("latest", {})
        required_latest_keys = ["from_month", "to_month", "generated_at", "has_data", "web_status"]
        for key in required_latest_keys:
            if key not in latest_block:
                errors.append(f"manifest.latest sin campo requerido: {key}")

        required_manifest_keys = ["status", "latest", "publication_policy"]
        for key in required_manifest_keys:
            if key not in manifest_data:
                errors.append(f"manifest.json sin campo requerido: {key}")

        required_latest_metadata_keys = [
            "from_month",
            "to_month",
            "web_status",
            "latest_range_label",
            "quality_warnings",
            "publication_policy",
        ]
        for key in required_latest_metadata_keys:
            if key not in latest_data:
                errors.append(f"latest.metadata.json sin campo requerido: {key}")

        if latest_block.get("from_month") != latest_data.get("from_month"):
            errors.append("Rango inconsistente: manifest.latest.from_month != latest.metadata.from_month")
        if latest_block.get("to_month") != latest_data.get("to_month"):
            errors.append("Rango inconsistente: manifest.latest.to_month != latest.metadata.to_month")
        if str(latest_block.get("web_status")) != str(latest_data.get("web_status")):
            errors.append("Estado inconsistente: manifest.latest.web_status != latest.metadata.web_status")

        expected_policy = (
            expected_publication_policy
            if expected_publication_policy is not None
            else (DEFAULT_PUBLICATION_POLICY if strict else None)
        )
        if expected_policy:
            if str(manifest_data.get("publication_policy")) != expected_policy:
                errors.append(
                    "publication_policy inconsistente en manifest "
                    f"(esperado={expected_policy}, actual={manifest_data.get('publication_policy')})"
                )
            if str(latest_data.get("publication_policy")) != expected_policy:
                errors.append(
                    "publication_policy inconsistente en latest.metadata "
                    f"(esperado={expected_policy}, actual={latest_data.get('publication_policy')})"
                )

    if expected_canonical_base:
        expected = expected_canonical_base.rstrip("/")
        home_html = by_path["/"].body.decode("utf-8", errors="ignore")
        tracker_html = by_path["/tracker/"].body.decode("utf-8", errors="ignore")
        if expected not in home_html:
            errors.append(f"Home sin canonical/OG esperado: {expected}")
        if expected not in tracker_html:
            errors.append(f"Tracker sin canonical/OG esperado: {expected}")
        if strict:
            historico_html = by_path["/historico/"].body.decode("utf-8", errors="ignore")
            if expected not in historico_html:
                errors.append(f"Historico sin canonical/OG esperado: {expected}")

    if strict:
        tracker_html = by_path["/tracker/"].body.decode("utf-8", errors="ignore")
        macro_markers = ["id=\"macro-notice\"", "Comparacion parcial"]
        for marker in macro_markers:
            if marker not in tracker_html:
                errors.append(f"Tracker sin marcador macro requerido: {marker}")
        ads_txt = by_path["/ads.txt"].body.decode("utf-8", errors="ignore")
        if "ca-pub-xxxxxxxx" in ads_txt:
            errors.append("ads.txt contiene client_id placeholder.")
        manifest_ads_enabled = bool((manifest_data or {}).get("ads", {}).get("enabled", False))
        if manifest_ads_enabled and "google.com," not in ads_txt:
            errors.append("ads habilitados en manifest, pero ads.txt no contiene declaracion de publisher.")

    for path in paths:
        result = by_path[path]
        label = str(result.status) if result.status is not None else "ERROR"
        print(f"{path} -> {label}")

    if manifest_data:
        print(f"manifest.status={manifest_data.get('status')}")
        print(f"manifest.latest={manifest_data.get('latest')}")
    if latest_data:
        print(
            "latest.metadata.range="
            f"{latest_data.get('from_month')}->{latest_data.get('to_month')} "
            f"status={latest_data.get('web_status')}"
        )

    if errors:
        print("\nSMOKE FAILED")
        for err in errors:
            print(f"- {err}")
        return 1

    print("\nSMOKE OK")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Smoke checks for public static website.")
    parser.add_argument("--base-url", required=True, help="Base URL (example: https://staging.preciosushuaia.com)")
    parser.add_argument("--timeout", type=float, default=10.0, help="HTTP timeout seconds")
    parser.add_argument(
        "--expected-canonical-base",
        default=None,
        help="Optional canonical/OG base URL to validate in home and tracker HTML",
    )
    parser.add_argument("--strict", action="store_true", help="Enable extended go-live validations.")
    parser.add_argument(
        "--expected-publication-policy",
        default=None,
        help="Optional expected publication policy name.",
    )
    args = parser.parse_args()
    return run_smoke(
        base_url=args.base_url,
        timeout=args.timeout,
        expected_canonical_base=args.expected_canonical_base,
        strict=args.strict,
        expected_publication_policy=args.expected_publication_policy,
    )


if __name__ == "__main__":
    sys.exit(main())
