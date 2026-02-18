"""Utilities to backfill canonical categories for historical data."""

from typing import Dict

from sqlalchemy import func
from sqlalchemy.orm import Session

from src.config_loader import get_category_display_names, resolve_canonical_category
from src.models import Category, Price, Product


def backfill_canonical_categories(session: Session, config: Dict) -> Dict[str, int]:
    """Populate canonical categories for products and prices already stored."""
    display_names = get_category_display_names(config)

    products_updated = 0
    prices_updated = 0
    unresolved_products = 0

    products = session.query(Product).all()
    for product in products:
        canonical_slug = resolve_canonical_category(config, product.category)
        if not canonical_slug:
            unresolved_products += 1
            continue

        category_obj = session.query(Category).filter_by(slug=canonical_slug).first()
        if not category_obj:
            category_obj = Category(
                slug=canonical_slug,
                name=display_names.get(canonical_slug, canonical_slug.replace("_", " ").title()),
                description="Categoría canónica generada durante backfill",
            )
            session.add(category_obj)
            session.flush()

        if product.category_id != category_obj.id:
            product.category_id = category_obj.id
            products_updated += 1

    session.flush()

    prices_missing = session.query(Price).filter(Price.category_id.is_(None)).all()
    for price in prices_missing:
        if price.product and price.product.category_id:
            price.category_id = price.product.category_id
            prices_updated += 1

    session.commit()

    prices_without_category = (
        session.query(func.count(Price.id)).filter(Price.category_id.is_(None)).scalar() or 0
    )

    return {
        "products_updated": products_updated,
        "prices_updated": prices_updated,
        "unresolved_products": unresolved_products,
        "prices_without_category": int(prices_without_category),
    }


def validate_price_category_traceability(session: Session) -> Dict[str, int]:
    """Return traceability counters for canonical category integrity."""
    total_prices = session.query(func.count(Price.id)).scalar() or 0
    prices_without_category = session.query(func.count(Price.id)).filter(Price.category_id.is_(None)).scalar() or 0

    return {
        "total_prices": int(total_prices),
        "prices_without_category": int(prices_without_category),
        "traceable_prices": int(total_prices - prices_without_category),
    }
