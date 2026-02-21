"""Repository layer for reusable series and index queries."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
from math import ceil
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import func
from sqlalchemy.orm import Session

from src.models import (
    CategoryIndex,
    IPCPublicationRun,
    IndexQualityAudit,
    OfficialCPIMonthly,
    Price,
    PriceCandidate,
    Product,
    ScrapeRun,
    TrackerIPCCategoryMonthly,
    TrackerIPCMonthly,
)


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

    def get_report_rows(
        self,
        basket_type: str,
        start_dt: datetime,
        end_exclusive_dt: datetime,
    ) -> List[Dict[str, Any]]:
        """Return raw rows needed by interactive HTML report."""
        query = (
            self.session.query(
                Price.canonical_id,
                Price.product_name,
                Price.basket_id,
                Price.current_price,
                Price.scraped_at,
                Price.product_url,
                Price.product_size,
                Product.category,
            )
            .outerjoin(Product, Price.canonical_id == Product.canonical_id)
            .filter(Price.scraped_at >= start_dt)
            .filter(Price.scraped_at < end_exclusive_dt)
        )
        if basket_type != "all":
            query = query.filter(Price.basket_id == basket_type)

        rows = query.order_by(Price.canonical_id.asc(), Price.scraped_at.asc()).all()
        return [dict(row._mapping) for row in rows]

    def get_candidate_rows(
        self,
        basket_type: str,
        start_dt: datetime,
        end_exclusive_dt: datetime,
    ) -> List[Dict[str, Any]]:
        """Return candidate low/mid/high rows for interactive report overlays."""
        query = (
            self.session.query(
                PriceCandidate.run_id,
                PriceCandidate.canonical_id,
                PriceCandidate.basket_id,
                PriceCandidate.product_id,
                PriceCandidate.product_name,
                PriceCandidate.candidate_name,
                PriceCandidate.candidate_url,
                PriceCandidate.tier,
                PriceCandidate.candidate_rank,
                PriceCandidate.candidate_price,
                PriceCandidate.confidence_score,
                PriceCandidate.is_selected,
                PriceCandidate.is_fallback,
                PriceCandidate.scraped_at,
            )
            .filter(PriceCandidate.scraped_at >= start_dt)
            .filter(PriceCandidate.scraped_at < end_exclusive_dt)
        )
        if basket_type != "all":
            query = query.filter(PriceCandidate.basket_id == basket_type)

        rows = query.order_by(PriceCandidate.canonical_id.asc(), PriceCandidate.scraped_at.asc()).all()
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
            IndexQualityAudit.coverage_rate,
            IndexQualityAudit.outlier_count,
            IndexQualityAudit.missing_count,
            IndexQualityAudit.min_coverage_required,
            IndexQualityAudit.is_coverage_sufficient,
            (IndexQualityAudit.is_coverage_sufficient == False).label("coverage_warning"),
        ).outerjoin(
            IndexQualityAudit,
            (IndexQualityAudit.basket_type == CategoryIndex.basket_type)
            & (IndexQualityAudit.year_month == CategoryIndex.year_month)
            & (IndexQualityAudit.category == CategoryIndex.category),
        )

        if start_period:
            query = query.filter(CategoryIndex.year_month >= start_period)
        if end_period:
            query = query.filter(CategoryIndex.year_month <= end_period)

        query = query.order_by(CategoryIndex.year_month.asc(), CategoryIndex.category.asc())

        return self._paginate_query(query, page=page, page_size=page_size)

    def _latest_tracker_method(self, basket_type: str) -> Optional[str]:
        query = self.session.query(TrackerIPCMonthly.method_version)
        if basket_type != "all":
            query = query.filter(TrackerIPCMonthly.basket_type == basket_type)
        row = query.order_by(TrackerIPCMonthly.computed_at.desc()).first()
        return str(row[0]) if row and row[0] else None

    def get_tracker_ipc_general(
        self,
        basket_type: str = "all",
        start_period: Optional[str] = None,
        end_period: Optional[str] = None,
        method_version: Optional[str] = None,
        status: Optional[str] = None,
        page: int = 1,
        page_size: int = 100,
    ) -> Tuple[List[Dict[str, Any]], Pagination]:
        method = method_version or self._latest_tracker_method(basket_type) or None
        query = self.session.query(
            TrackerIPCMonthly.basket_type,
            TrackerIPCMonthly.year_month,
            TrackerIPCMonthly.method_version,
            TrackerIPCMonthly.status,
            TrackerIPCMonthly.index_value,
            TrackerIPCMonthly.mom_change,
            TrackerIPCMonthly.yoy_change,
            TrackerIPCMonthly.coverage_weight_pct,
            TrackerIPCMonthly.coverage_product_pct,
            TrackerIPCMonthly.products_expected,
            TrackerIPCMonthly.products_observed,
            TrackerIPCMonthly.products_with_relative,
            TrackerIPCMonthly.outlier_count,
            TrackerIPCMonthly.missing_products,
            TrackerIPCMonthly.base_month,
            TrackerIPCMonthly.computed_at,
            TrackerIPCMonthly.frozen_at,
        )
        if basket_type != "all":
            query = query.filter(TrackerIPCMonthly.basket_type == basket_type)
        if method:
            query = query.filter(TrackerIPCMonthly.method_version == method)
        if status:
            query = query.filter(TrackerIPCMonthly.status == status)
        if start_period:
            query = query.filter(TrackerIPCMonthly.year_month >= start_period)
        if end_period:
            query = query.filter(TrackerIPCMonthly.year_month <= end_period)

        query = query.order_by(TrackerIPCMonthly.year_month.asc(), TrackerIPCMonthly.basket_type.asc())
        return self._paginate_query(query, page=page, page_size=page_size)

    def get_tracker_ipc_categories(
        self,
        basket_type: str = "all",
        category_slug: Optional[str] = None,
        start_period: Optional[str] = None,
        end_period: Optional[str] = None,
        method_version: Optional[str] = None,
        status: Optional[str] = None,
        page: int = 1,
        page_size: int = 100,
    ) -> Tuple[List[Dict[str, Any]], Pagination]:
        method = method_version or self._latest_tracker_method(basket_type) or None
        query = self.session.query(
            TrackerIPCCategoryMonthly.basket_type,
            TrackerIPCCategoryMonthly.category_slug,
            TrackerIPCCategoryMonthly.indec_division_code,
            TrackerIPCCategoryMonthly.year_month,
            TrackerIPCCategoryMonthly.method_version,
            TrackerIPCCategoryMonthly.status,
            TrackerIPCCategoryMonthly.index_value,
            TrackerIPCCategoryMonthly.mom_change,
            TrackerIPCCategoryMonthly.yoy_change,
            TrackerIPCCategoryMonthly.coverage_weight_pct,
            TrackerIPCCategoryMonthly.coverage_product_pct,
            TrackerIPCCategoryMonthly.products_expected,
            TrackerIPCCategoryMonthly.products_observed,
            TrackerIPCCategoryMonthly.products_with_relative,
            TrackerIPCCategoryMonthly.outlier_count,
            TrackerIPCCategoryMonthly.missing_products,
            TrackerIPCCategoryMonthly.base_month,
            TrackerIPCCategoryMonthly.computed_at,
            TrackerIPCCategoryMonthly.frozen_at,
        )
        if basket_type != "all":
            query = query.filter(TrackerIPCCategoryMonthly.basket_type == basket_type)
        if category_slug:
            query = query.filter(TrackerIPCCategoryMonthly.category_slug == category_slug)
        if method:
            query = query.filter(TrackerIPCCategoryMonthly.method_version == method)
        if status:
            query = query.filter(TrackerIPCCategoryMonthly.status == status)
        if start_period:
            query = query.filter(TrackerIPCCategoryMonthly.year_month >= start_period)
        if end_period:
            query = query.filter(TrackerIPCCategoryMonthly.year_month <= end_period)

        query = query.order_by(TrackerIPCCategoryMonthly.category_slug.asc(), TrackerIPCCategoryMonthly.year_month.asc())
        return self._paginate_query(query, page=page, page_size=page_size)

    def get_official_ipc_patagonia(
        self,
        start_period: Optional[str] = None,
        end_period: Optional[str] = None,
        metric_code: Optional[str] = "general",
        category_slug: Optional[str] = None,
        region: str = "patagonia",
        source: Optional[str] = None,
        page: int = 1,
        page_size: int = 100,
    ) -> Tuple[List[Dict[str, Any]], Pagination]:
        query = self.session.query(
            OfficialCPIMonthly.source,
            OfficialCPIMonthly.region,
            OfficialCPIMonthly.metric_code,
            OfficialCPIMonthly.category_slug,
            OfficialCPIMonthly.year_month,
            OfficialCPIMonthly.index_value,
            OfficialCPIMonthly.mom_change,
            OfficialCPIMonthly.yoy_change,
            OfficialCPIMonthly.status,
            OfficialCPIMonthly.is_fallback,
            OfficialCPIMonthly.raw_snapshot_path,
            OfficialCPIMonthly.updated_at,
        ).filter(OfficialCPIMonthly.region == region)

        if source:
            query = query.filter(OfficialCPIMonthly.source == source)
        if metric_code:
            query = query.filter(OfficialCPIMonthly.metric_code == metric_code)
        if category_slug:
            query = query.filter(OfficialCPIMonthly.category_slug == category_slug)
        if start_period:
            query = query.filter(OfficialCPIMonthly.year_month >= start_period)
        if end_period:
            query = query.filter(OfficialCPIMonthly.year_month <= end_period)

        query = query.order_by(OfficialCPIMonthly.year_month.asc(), OfficialCPIMonthly.metric_code.asc())
        return self._paginate_query(query, page=page, page_size=page_size)

    def get_ipc_comparison_general(
        self,
        basket_type: str = "all",
        start_period: Optional[str] = None,
        end_period: Optional[str] = None,
        method_version: Optional[str] = None,
        region: str = "patagonia",
        source: Optional[str] = None,
        page: int = 1,
        page_size: int = 100,
    ) -> Tuple[List[Dict[str, Any]], Pagination]:
        tracker_rows, _ = self.get_tracker_ipc_general(
            basket_type=basket_type,
            start_period=start_period,
            end_period=end_period,
            method_version=method_version,
            page=1,
            page_size=2000,
        )
        official_rows, _ = self.get_official_ipc_patagonia(
            start_period=start_period,
            end_period=end_period,
            metric_code="general",
            region=region,
            source=source,
            page=1,
            page_size=2000,
        )
        tracker_by_month = {str(r["year_month"]): r for r in tracker_rows}
        official_by_month = {str(r["year_month"]): r for r in official_rows}
        months = sorted(set(tracker_by_month.keys()) | set(official_by_month.keys()))

        overlap = [
            m for m in months
            if tracker_by_month.get(m, {}).get("index_value") is not None
            and official_by_month.get(m, {}).get("index_value") is not None
        ]
        tracker_base = float(tracker_by_month[overlap[0]]["index_value"]) if overlap else None
        official_base = float(official_by_month[overlap[0]]["index_value"]) if overlap else None

        rows: List[Dict[str, Any]] = []
        for month in months:
            tracker = tracker_by_month.get(month, {})
            official = official_by_month.get(month, {})
            tracker_idx = tracker.get("index_value")
            official_idx = official.get("index_value")
            tracker_base100 = None
            official_base100 = None
            if tracker_idx is not None and tracker_base and tracker_base != 0:
                tracker_base100 = (float(tracker_idx) / tracker_base) * 100.0
            if official_idx is not None and official_base and official_base != 0:
                official_base100 = (float(official_idx) / official_base) * 100.0

            gap_index = (
                tracker_base100 - official_base100
                if tracker_base100 is not None and official_base100 is not None
                else None
            )
            tracker_mom = tracker.get("mom_change")
            official_mom = official.get("mom_change")
            gap_mom = (
                float(tracker_mom) - float(official_mom)
                if tracker_mom is not None and official_mom is not None
                else None
            )
            rows.append(
                {
                    "year_month": month,
                    "basket_type": basket_type,
                    "method_version": tracker.get("method_version"),
                    "region": region,
                    "tracker_index": tracker_idx,
                    "official_index": official_idx,
                    "tracker_mom": tracker_mom,
                    "official_mom": official_mom,
                    "tracker_status": tracker.get("status"),
                    "official_status": official.get("status"),
                    "tracker_index_base100": tracker_base100,
                    "official_index_base100": official_base100,
                    "gap_index_points": gap_index,
                    "gap_mom_pp": gap_mom,
                    "is_overlap": month in overlap,
                }
            )

        total = len(rows)
        start = (page - 1) * page_size
        paged = rows[start:start + page_size]
        return paged, Pagination(page=page, page_size=page_size, total=total)

    def get_ipc_comparison_categories(
        self,
        basket_type: str = "all",
        category_slug: Optional[str] = None,
        start_period: Optional[str] = None,
        end_period: Optional[str] = None,
        method_version: Optional[str] = None,
        region: str = "patagonia",
        source: Optional[str] = None,
        page: int = 1,
        page_size: int = 100,
    ) -> Tuple[List[Dict[str, Any]], Pagination]:
        tracker_rows, _ = self.get_tracker_ipc_categories(
            basket_type=basket_type,
            category_slug=category_slug,
            start_period=start_period,
            end_period=end_period,
            method_version=method_version,
            page=1,
            page_size=5000,
        )
        # Official rows keep INDEC division slug in metric_code/category_slug.
        official_rows, _ = self.get_official_ipc_patagonia(
            start_period=start_period,
            end_period=end_period,
            metric_code=None,
            region=region,
            source=source,
            page=1,
            page_size=5000,
        )

        tracker_rows = [
            r
            for r in tracker_rows
            if r.get("category_slug")
            and r.get("indec_division_code")
            and str(r.get("indec_division_code")).strip().lower() not in {"", "none", "nan"}
        ]
        if category_slug:
            tracker_rows = [r for r in tracker_rows if str(r.get("category_slug")) == str(category_slug)]

        official_rows = [
            r
            for r in official_rows
            if r.get("metric_code")
            and str(r.get("metric_code")) not in {"general", "", "None"}
        ]

        tracker_by_key = {(str(r["category_slug"]), str(r["year_month"])): r for r in tracker_rows}
        official_by_key = {(str(r["metric_code"]), str(r["year_month"])): r for r in official_rows}
        category_to_division: Dict[str, str] = {}
        for row in tracker_rows:
            cat = str(row["category_slug"])
            division = str(row["indec_division_code"])
            category_to_division.setdefault(cat, division)
        categories = sorted(category_to_division.keys())

        rows: List[Dict[str, Any]] = []
        for cat in categories:
            division_code = category_to_division.get(cat)
            months = sorted(
                set(m for c, m in tracker_by_key.keys() if c == cat)
                | set(m for d, m in official_by_key.keys() if d == division_code)
            )
            overlap = [
                m for m in months
                if tracker_by_key.get((cat, m), {}).get("index_value") is not None
                and official_by_key.get((division_code, m), {}).get("index_value") is not None
            ]
            tracker_base = float(tracker_by_key[(cat, overlap[0])]["index_value"]) if overlap else None
            official_base = float(official_by_key[(division_code, overlap[0])]["index_value"]) if overlap else None

            for month in months:
                tracker = tracker_by_key.get((cat, month), {})
                official = official_by_key.get((division_code, month), {})
                tracker_idx = tracker.get("index_value")
                official_idx = official.get("index_value")
                tracker_base100 = None
                official_base100 = None
                if tracker_idx is not None and tracker_base and tracker_base != 0:
                    tracker_base100 = (float(tracker_idx) / tracker_base) * 100.0
                if official_idx is not None and official_base and official_base != 0:
                    official_base100 = (float(official_idx) / official_base) * 100.0
                gap_index = (
                    tracker_base100 - official_base100
                    if tracker_base100 is not None and official_base100 is not None
                    else None
                )
                tracker_mom = tracker.get("mom_change")
                official_mom = official.get("mom_change")
                gap_mom = (
                    float(tracker_mom) - float(official_mom)
                    if tracker_mom is not None and official_mom is not None
                    else None
                )
                rows.append(
                    {
                        "category_slug": cat,
                        "indec_division_code": division_code,
                        "year_month": month,
                        "basket_type": basket_type,
                        "method_version": tracker.get("method_version"),
                        "region": region,
                        "tracker_index": tracker_idx,
                        "official_index": official_idx,
                        "tracker_mom": tracker_mom,
                        "official_mom": official_mom,
                        "tracker_status": tracker.get("status"),
                        "official_status": official.get("status"),
                        "tracker_index_base100": tracker_base100,
                        "official_index_base100": official_base100,
                        "gap_index_points": gap_index,
                        "gap_mom_pp": gap_mom,
                        "is_overlap": month in overlap,
                    }
                )

        rows.sort(key=lambda r: (r["category_slug"], r["year_month"]))
        total = len(rows)
        start = (page - 1) * page_size
        paged = rows[start:start + page_size]
        return paged, Pagination(page=page, page_size=page_size, total=total)

    def get_latest_ipc_publication_status(
        self,
        basket_type: str = "all",
        region: str = "patagonia",
    ) -> Optional[Dict[str, Any]]:
        query = (
            self.session.query(
                IPCPublicationRun.run_uuid,
                IPCPublicationRun.status,
                IPCPublicationRun.basket_type,
                IPCPublicationRun.region,
                IPCPublicationRun.method_version,
                IPCPublicationRun.from_month,
                IPCPublicationRun.to_month,
                IPCPublicationRun.official_source,
                IPCPublicationRun.official_rows,
                IPCPublicationRun.tracker_rows,
                IPCPublicationRun.tracker_category_rows,
                IPCPublicationRun.overlap_months,
                IPCPublicationRun.warnings_json,
                IPCPublicationRun.metrics_json,
                IPCPublicationRun.started_at,
                IPCPublicationRun.completed_at,
            )
            .filter(IPCPublicationRun.region == region)
        )
        if basket_type != "all":
            query = query.filter(IPCPublicationRun.basket_type == basket_type)
        row = query.order_by(IPCPublicationRun.started_at.desc()).first()
        if row is None:
            return None
        return dict(row._mapping)

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
