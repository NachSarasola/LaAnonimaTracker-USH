"""Lightweight HTTP API for exposing tracker series."""

from __future__ import annotations

from datetime import date
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, Query
from sqlalchemy.orm import Session

from src.config_loader import load_config
from src.models import get_engine, get_session_factory
from src.repositories import SeriesRepository

app = FastAPI(title="La Anónima Tracker API", version="1.0.0")


_config = load_config()
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
        raise HTTPException(status_code=400, detail="El parámetro 'from' no puede ser mayor a 'to'.")


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
        raise HTTPException(status_code=404, detail=f"Categoría inexistente: {category}")

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
    if from_period and len(from_period) != 7:
        raise HTTPException(status_code=400, detail="El parámetro 'from' debe tener formato YYYY-MM.")
    if to_period and len(to_period) != 7:
        raise HTTPException(status_code=400, detail="El parámetro 'to' debe tener formato YYYY-MM.")
    if from_period and to_period and from_period > to_period:
        raise HTTPException(status_code=400, detail="El parámetro 'from' no puede ser mayor a 'to'.")

    repository = SeriesRepository(session)
    rows, pagination = repository.get_ipc_categories(
        start_period=from_period,
        end_period=to_period,
        page=page,
        page_size=page_size,
    )

    return {"items": rows, "pagination": pagination.as_dict()}
