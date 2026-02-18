"""Repository layer for reusable series and index queries."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
from math import ceil
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import func
from sqlalchemy.orm import Session

from src.models import CategoryIndex, Price, Product, ScrapeRun


@dataclass
class Pagination:
    """Pagination metadata."""

    page: int
    page_size: int
    total: int

    @property
    def total_pages(self) -> int:
        if self.total == 0:
            return 0
        return ceil(self.total / self.page_size)

    def as_dict(self) -> Dict[str, int]:
        return {
            "page": self.page,
            "page_size": self.page_size,
            "total": self.total,
            "total_pages": self.total_pages,
        }


class SeriesRepository:
    """Reusable SQLAlchemy queries used by API and exporters."""

    def __init__(self, session: Session):
        self.session = session

    def get_product_series(
        self,
        canonical_id: Optional[str] = None,
        basket_type: str = "all",
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        page: int = 1,
        page_size: int = 100,
    ) -> Tuple[List[Dict[str, Any]], Pagination]:
        query = self._base_product_series_query()
        query = self._apply_series_filters(
            query,
            canonical_id=canonical_id,
            basket_type=basket_type,
            start_date=start_date,
            end_date=end_date,
        )
        query = query.order_by(Price.scraped_at.asc(), Price.canonical_id.asc())

        return self._paginate_query(query, page=page, page_size=page_size)

    def get_all_product_series(
        self,
        canonical_id: Optional[str] = None,
        basket_type: str = "all",
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        query = self._base_product_series_query()
        query = self._apply_series_filters(
            query,
            canonical_id=canonical_id,
            basket_type=basket_type,
            start_date=start_date,
            end_date=end_date,
        )
        rows = query.order_by(Price.canonical_id.asc(), Price.scraped_at.asc()).all()
        return [dict(row._mapping) for row in rows]

    def get_category_series(
        self,
        category: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        page: int = 1,
        page_size: int = 100,
    ) -> Tuple[List[Dict[str, Any]], Pagination]:
        query = self.session.query(
            Price.canonical_id,
            Price.product_name,
            Product.category,
            Price.current_price,
            Price.original_price,
            Price.price_per_unit,
            Price.in_stock,
            Price.is_promotion,
            Price.scraped_at,
            ScrapeRun.run_uuid,
        ).join(ScrapeRun, Price.run_id == ScrapeRun.id).outerjoin(
            Product, Price.canonical_id == Product.canonical_id
        ).filter(func.lower(Product.category) == category.lower())

        query = self._apply_series_filters(query, start_date=start_date, end_date=end_date)
        query = query.order_by(Price.scraped_at.asc(), Price.canonical_id.asc())

        return self._paginate_query(query, page=page, page_size=page_size)

    def get_ipc_categories(
        self,
        start_period: Optional[str] = None,
        end_period: Optional[str] = None,
        page: int = 1,
        page_size: int = 100,
    ) -> Tuple[List[Dict[str, Any]], Pagination]:
        query = self.session.query(
            CategoryIndex.category,
            CategoryIndex.basket_type,
            CategoryIndex.year_month,
            CategoryIndex.index_value,
            CategoryIndex.mom_change,
            CategoryIndex.yoy_change,
            CategoryIndex.products_included,
            CategoryIndex.products_missing,
            CategoryIndex.computed_at,
        )

        if start_period:
            query = query.filter(CategoryIndex.year_month >= start_period)
        if end_period:
            query = query.filter(CategoryIndex.year_month <= end_period)

        query = query.order_by(CategoryIndex.year_month.asc(), CategoryIndex.category.asc())

        return self._paginate_query(query, page=page, page_size=page_size)

    def category_exists(self, category: str) -> bool:
        return (
            self.session.query(Product.id)
            .filter(Product.category.isnot(None))
            .filter(func.lower(Product.category) == category.lower())
            .first()
            is not None
        )

    def _base_product_series_query(self):
        return self.session.query(
            Price.canonical_id,
            Price.product_name,
            Price.basket_id,
            Price.current_price,
            Price.original_price,
            Price.price_per_unit,
            Price.in_stock,
            Price.is_promotion,
            Price.scraped_at,
            ScrapeRun.run_uuid,
            ScrapeRun.started_at.label("run_started_at"),
        ).join(ScrapeRun, Price.run_id == ScrapeRun.id)

    def _apply_series_filters(
        self,
        query,
        canonical_id: Optional[str] = None,
        basket_type: str = "all",
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ):
        if canonical_id:
            query = query.filter(Price.canonical_id == canonical_id)

        if basket_type != "all":
            query = query.filter(Price.basket_id == basket_type)

        if start_date:
            query = query.filter(Price.scraped_at >= datetime.combine(start_date, time.min))
        if end_date:
            query = query.filter(Price.scraped_at <= datetime.combine(end_date, time.max))

        return query

    def _paginate_query(self, query, page: int, page_size: int) -> Tuple[List[Dict[str, Any]], Pagination]:
        total = query.count()
        rows = query.offset((page - 1) * page_size).limit(page_size).all()
        return [dict(row._mapping) for row in rows], Pagination(page=page, page_size=page_size, total=total)
