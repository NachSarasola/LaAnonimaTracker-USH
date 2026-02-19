"""Lightweight HTTP API for exposing tracker series and IPC datasets."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, Query
from sqlalchemy.orm import Session

from src.config_loader import load_config
from src.models import get_engine, get_session_factory
from src.repositories import SeriesRepository

app = FastAPI(title="La Anonima Tracker API", version="1.1.0")


def _load_api_config():
    try:
        return load_config()
    except FileNotFoundError:
        fallback = Path(__file__).resolve().parents[1] / "config.yaml"
        return load_config(str(fallback))


_config = _load_api_config()
_engine = get_engine(_config)
_SessionFactory = get_session_factory(_engine)


def get_session():
    session = _SessionFactory()
    try:
        yield session
    finally:
        session.close()


def _validate_date_range(start_date: Optional[date], end_date: Optional[date]) -> None:
    if start_date and end_date and start_date > end_date:
        raise HTTPException(status_code=400, detail="El parametro 'from' no puede ser mayor a 'to'.")


def _validate_period(period: Optional[str], label: str) -> None:
    if period and len(period) != 7:
        raise HTTPException(status_code=400, detail=f"El parametro '{label}' debe tener formato YYYY-MM.")


def _validate_period_range(from_period: Optional[str], to_period: Optional[str]) -> None:
    _validate_period(from_period, "from")
    _validate_period(to_period, "to")
    if from_period and to_period and from_period > to_period:
        raise HTTPException(status_code=400, detail="El parametro 'from' no puede ser mayor a 'to'.")


def _official_meta(repository: SeriesRepository, region: str) -> dict:
    latest = repository.get_latest_ipc_publication_status(basket_type="all", region=region)
    if not latest:
        return {
            "region": region,
            "official_source": None,
            "validation_status": "unknown",
            "source_document_url": None,
        }
    metrics = {}
    try:
        metrics = json.loads(latest.get("metrics_json") or "{}")
    except Exception:
        metrics = {}
    return {
        "region": region,
        "official_source": metrics.get("official_source_effective") or latest.get("official_source"),
        "validation_status": metrics.get("official_validation_status") or "unknown",
        "source_document_url": metrics.get("official_source_document_url"),
    }


@app.get("/series/producto")
def get_series_producto(
    canonical_id: Optional[str] = Query(default=None),
    from_date: Optional[date] = Query(default=None, alias="from"),
    to_date: Optional[date] = Query(default=None, alias="to"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=100, ge=1, le=500),
    session: Session = Depends(get_session),
):
    _validate_date_range(from_date, to_date)

    repository = SeriesRepository(session)
    rows, pagination = repository.get_product_series(
        canonical_id=canonical_id,
        start_date=from_date,
        end_date=to_date,
        page=page,
        page_size=page_size,
    )

    return {"items": rows, "pagination": pagination.as_dict()}


@app.get("/series/categoria")
def get_series_categoria(
    category: str = Query(..., min_length=1),
    from_date: Optional[date] = Query(default=None, alias="from"),
    to_date: Optional[date] = Query(default=None, alias="to"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=100, ge=1, le=500),
    session: Session = Depends(get_session),
):
    _validate_date_range(from_date, to_date)

    repository = SeriesRepository(session)
    if not repository.category_exists(category):
        raise HTTPException(status_code=404, detail=f"Categoria inexistente: {category}")

    rows, pagination = repository.get_category_series(
        category=category,
        start_date=from_date,
        end_date=to_date,
        page=page,
        page_size=page_size,
    )

    return {"items": rows, "pagination": pagination.as_dict()}


@app.get("/ipc/categorias")
def get_ipc_categorias(
    from_period: Optional[str] = Query(default=None, alias="from"),
    to_period: Optional[str] = Query(default=None, alias="to"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=100, ge=1, le=500),
    session: Session = Depends(get_session),
):
    """Legacy endpoint kept for backward compatibility."""
    _validate_period_range(from_period, to_period)
    repository = SeriesRepository(session)
    rows, pagination = repository.get_ipc_categories(
        start_period=from_period,
        end_period=to_period,
        page=page,
        page_size=page_size,
    )
    return {"items": rows, "pagination": pagination.as_dict()}


@app.get("/ipc/tracker")
def get_ipc_tracker(
    basket: str = Query(default="all", pattern="^(cba|extended|all)$"),
    from_period: Optional[str] = Query(default=None, alias="from"),
    to_period: Optional[str] = Query(default=None, alias="to"),
    method_version: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=100, ge=1, le=500),
    session: Session = Depends(get_session),
):
    _validate_period_range(from_period, to_period)
    repository = SeriesRepository(session)
    rows, pagination = repository.get_tracker_ipc_general(
        basket_type=basket,
        start_period=from_period,
        end_period=to_period,
        method_version=method_version,
        status=status,
        page=page,
        page_size=page_size,
    )
    return {"items": rows, "pagination": pagination.as_dict()}


@app.get("/ipc/tracker/categorias")
def get_ipc_tracker_categorias(
    basket: str = Query(default="all", pattern="^(cba|extended|all)$"),
    category: Optional[str] = Query(default=None),
    from_period: Optional[str] = Query(default=None, alias="from"),
    to_period: Optional[str] = Query(default=None, alias="to"),
    method_version: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=100, ge=1, le=500),
    session: Session = Depends(get_session),
):
    _validate_period_range(from_period, to_period)
    repository = SeriesRepository(session)
    rows, pagination = repository.get_tracker_ipc_categories(
        basket_type=basket,
        category_slug=category,
        start_period=from_period,
        end_period=to_period,
        method_version=method_version,
        status=status,
        page=page,
        page_size=page_size,
    )
    return {"items": rows, "pagination": pagination.as_dict()}


@app.get("/ipc/oficial")
def get_ipc_oficial(
    region: str = Query(default="patagonia"),
    from_period: Optional[str] = Query(default=None, alias="from"),
    to_period: Optional[str] = Query(default=None, alias="to"),
    metric_code: Optional[str] = Query(default="general"),
    category: Optional[str] = Query(default=None),
    source: Optional[str] = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=100, ge=1, le=500),
    session: Session = Depends(get_session),
):
    _validate_period_range(from_period, to_period)
    repository = SeriesRepository(session)
    rows, pagination = repository.get_official_ipc_patagonia(
        start_period=from_period,
        end_period=to_period,
        metric_code=metric_code,
        category_slug=category,
        region=region,
        source=source,
        page=page,
        page_size=page_size,
    )
    return {"items": rows, "pagination": pagination.as_dict(), "meta": _official_meta(repository, region=region)}


@app.get("/ipc/oficial/patagonia")
def get_ipc_oficial_patagonia(
    from_period: Optional[str] = Query(default=None, alias="from"),
    to_period: Optional[str] = Query(default=None, alias="to"),
    metric_code: Optional[str] = Query(default="general"),
    category: Optional[str] = Query(default=None),
    source: Optional[str] = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=100, ge=1, le=500),
    session: Session = Depends(get_session),
):
    return get_ipc_oficial(
        region="patagonia",
        from_period=from_period,
        to_period=to_period,
        metric_code=metric_code,
        category=category,
        source=source,
        page=page,
        page_size=page_size,
        session=session,
    )


@app.get("/ipc/comparacion")
def get_ipc_comparacion(
    basket: str = Query(default="all", pattern="^(cba|extended|all)$"),
    region: str = Query(default="patagonia"),
    from_period: Optional[str] = Query(default=None, alias="from"),
    to_period: Optional[str] = Query(default=None, alias="to"),
    method_version: Optional[str] = Query(default=None),
    source: Optional[str] = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=100, ge=1, le=500),
    session: Session = Depends(get_session),
):
    _validate_period_range(from_period, to_period)
    repository = SeriesRepository(session)
    rows, pagination = repository.get_ipc_comparison_general(
        basket_type=basket,
        start_period=from_period,
        end_period=to_period,
        method_version=method_version,
        region=region,
        source=source,
        page=page,
        page_size=page_size,
    )
    return {"items": rows, "pagination": pagination.as_dict(), "meta": {"region": region}}


@app.get("/ipc/comparacion/categorias")
def get_ipc_comparacion_categorias(
    basket: str = Query(default="all", pattern="^(cba|extended|all)$"),
    region: str = Query(default="patagonia"),
    category: Optional[str] = Query(default=None),
    from_period: Optional[str] = Query(default=None, alias="from"),
    to_period: Optional[str] = Query(default=None, alias="to"),
    method_version: Optional[str] = Query(default=None),
    source: Optional[str] = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=100, ge=1, le=500),
    session: Session = Depends(get_session),
):
    _validate_period_range(from_period, to_period)
    repository = SeriesRepository(session)
    rows, pagination = repository.get_ipc_comparison_categories(
        basket_type=basket,
        category_slug=category,
        start_period=from_period,
        end_period=to_period,
        method_version=method_version,
        region=region,
        source=source,
        page=page,
        page_size=page_size,
    )
    return {"items": rows, "pagination": pagination.as_dict(), "meta": {"region": region}}


@app.get("/ipc/publicacion/latest")
def get_ipc_publicacion_latest(
    basket: str = Query(default="all", pattern="^(cba|extended|all)$"),
    region: str = Query(default="patagonia"),
    session: Session = Depends(get_session),
):
    repository = SeriesRepository(session)
    latest = repository.get_latest_ipc_publication_status(basket_type=basket, region=region)
    return {"item": latest}
