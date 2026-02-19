"""Database models for La Anónima Price Tracker."""

from datetime import datetime, timezone
import os

def now_utc():
    return datetime.now(timezone.utc)
from decimal import Decimal
from typing import Optional

from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    Boolean,
    UniqueConstraint,
    create_engine,
    event,
    inspect,
    text,
)
from sqlalchemy.engine import Engine
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
from sqlalchemy.sql import func

Base = declarative_base()


@event.listens_for(Engine, "connect")
def _set_sqlite_pragma(dbapi_conn, connection_record):
    """Enable foreign key support for SQLite (no-op for other backends)."""
    pool = getattr(connection_record, "pool", None)
    engine = getattr(pool, "engine", None) if pool else None
    if engine is not None and engine.dialect.name == "sqlite":
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()


class Product(Base):
    """Canonical product mapping table."""
    
    __tablename__ = "products"
    
    id = Column(Integer, primary_key=True)
    canonical_id = Column(String(50), unique=True, nullable=False, index=True)
    basket_id = Column(String(50), nullable=False, index=True)
    name = Column(String(255), nullable=False)
    category = Column(String(50), nullable=True)
    category_id = Column(Integer, ForeignKey("categories.id"), nullable=True, index=True)
    unit = Column(String(20), nullable=True)
    quantity = Column(Numeric(10, 3), nullable=True)
    keywords = Column(Text, nullable=True)
    brand_hint = Column(Text, nullable=True)
    matching_rules = Column(String(20), default="loose")
    
    # Product signature for change detection
    signature_name = Column(String(255), nullable=True)
    signature_size = Column(String(50), nullable=True)
    signature_brand = Column(String(100), nullable=True)
    
    # Tracking
    created_at = Column(DateTime, default=now_utc)
    updated_at = Column(DateTime, default=now_utc, onupdate=now_utc)
    is_active = Column(Boolean, default=True)
    
    # Relationships
    prices = relationship("Price", back_populates="product", cascade="all, delete-orphan")
    canonical_category = relationship("Category", back_populates="products")
    
    def __repr__(self):
        return f"<Product(canonical_id='{self.canonical_id}', name='{self.name}')>"


class Price(Base):
    """Price observations table (long format)."""
    
    __tablename__ = "prices"
    __table_args__ = (
        Index("ix_prices_canonical_scraped_at", "canonical_id", "scraped_at"),
        Index("ix_prices_basket_scraped_at", "basket_id", "scraped_at"),
    )
    
    id = Column(Integer, primary_key=True)
    
    # Foreign keys
    product_id = Column(Integer, ForeignKey("products.id"), nullable=False, index=True)
    run_id = Column(Integer, ForeignKey("scrape_runs.id"), nullable=False, index=True)
    category_id = Column(Integer, ForeignKey("categories.id"), nullable=True, index=True)
    
    # Product identification
    canonical_id = Column(String(50), nullable=False, index=True)
    basket_id = Column(String(50), nullable=False, index=True)
    
    # Price data
    product_name = Column(String(255), nullable=False)
    product_size = Column(String(50), nullable=True)
    product_brand = Column(String(100), nullable=True)
    product_url = Column(Text, nullable=True)
    
    # Pricing
    current_price = Column(Numeric(12, 2), nullable=False)
    original_price = Column(Numeric(12, 2), nullable=True)  # For discounts
    price_per_unit = Column(Numeric(12, 2), nullable=True)
    currency = Column(String(3), default="ARS")
    
    # Availability
    in_stock = Column(Boolean, default=True)
    stock_status = Column(String(50), nullable=True)
    
    # Promotion info
    promotion_text = Column(Text, nullable=True)
    is_promotion = Column(Boolean, default=False)
    
    # Scrape metadata
    confidence_score = Column(Numeric(3, 2), nullable=True)  # 0.0 to 1.0
    match_method = Column(String(20), nullable=True)  # 'exact', 'fuzzy', 'manual'
    
    # Timestamps
    scraped_at = Column(DateTime, default=func.now())
    
    # Relationships
    product = relationship("Product", back_populates="prices")
    canonical_category = relationship("Category", back_populates="prices")
    run = relationship("ScrapeRun", back_populates="prices")
    
    def __repr__(self):
        return f"<Price(product='{self.product_name}', price={self.current_price}, date={self.scraped_at})>"
    
    @property
    def discount_percentage(self) -> Optional[Decimal]:
        """Calculate discount percentage if there's a promotion."""
        if self.original_price and self.original_price > 0:
            discount = (self.original_price - self.current_price) / self.original_price * 100
            return Decimal(discount).quantize(Decimal("0.01"))
        return None


class PriceCandidate(Base):
    """Low/mid/high candidate prices captured during matching."""

    __tablename__ = "price_candidates"
    __table_args__ = (
        Index("ix_price_candidates_run_canonical", "run_id", "canonical_id"),
        Index("ix_price_candidates_canonical_scraped_at", "canonical_id", "scraped_at"),
    )

    id = Column(Integer, primary_key=True)
    run_id = Column(Integer, ForeignKey("scrape_runs.id"), nullable=False, index=True)

    canonical_id = Column(String(50), nullable=False, index=True)
    basket_id = Column(String(50), nullable=False, index=True)
    product_id = Column(String(50), nullable=True, index=True)
    product_name = Column(String(255), nullable=True)

    tier = Column(String(10), nullable=False)  # low|mid|high|single
    candidate_rank = Column(Integer, nullable=True)

    candidate_price = Column(Numeric(12, 2), nullable=True)
    candidate_name = Column(String(255), nullable=True)
    candidate_url = Column(Text, nullable=True)
    confidence_score = Column(Numeric(3, 2), nullable=True)

    is_selected = Column(Boolean, default=False)
    is_fallback = Column(Boolean, default=False)
    scraped_at = Column(DateTime, default=func.now())

    run = relationship("ScrapeRun", back_populates="price_candidates")

    def __repr__(self):
        return (
            f"<PriceCandidate(canonical_id='{self.canonical_id}', tier='{self.tier}', "
            f"price={self.candidate_price})>"
        )


class ScrapeRun(Base):
    """Scrape execution log table."""
    
    __tablename__ = "scrape_runs"
    
    id = Column(Integer, primary_key=True)
    
    # Run identification
    run_uuid = Column(String(36), unique=True, nullable=False, index=True)
    
    # Configuration
    branch_id = Column(String(20), nullable=False)
    branch_name = Column(String(100), nullable=False)
    postal_code = Column(String(10), nullable=False)
    basket_type = Column(String(20), nullable=False)  # 'cba', 'extended', 'all'
    
    # Timing
    started_at = Column(DateTime, default=func.now())
    completed_at = Column(DateTime, nullable=True)
    duration_seconds = Column(Integer, nullable=True)
    
    # Status
    status = Column(String(20), default="running")  # 'running', 'completed', 'failed', 'partial'
    
    # Statistics
    products_planned = Column(Integer, default=0)
    products_scraped = Column(Integer, default=0)
    products_failed = Column(Integer, default=0)
    products_skipped = Column(Integer, default=0)
    
    # Metadata
    scraper_version = Column(String(20), default="1.0.0")
    config_hash = Column(String(64), nullable=True)
    
    # Relationships
    prices = relationship("Price", back_populates="run", cascade="all, delete-orphan")
    price_candidates = relationship(
        "PriceCandidate",
        back_populates="run",
        cascade="all, delete-orphan",
    )
    errors = relationship("ScrapeError", back_populates="run", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<ScrapeRun(id={self.id}, status='{self.status}', started='{self.started_at}')>"


class ScrapeError(Base):
    """Scrape errors table."""
    
    __tablename__ = "scrape_errors"
    
    id = Column(Integer, primary_key=True)
    run_id = Column(Integer, ForeignKey("scrape_runs.id"), nullable=False, index=True)
    
    # Error context
    product_id = Column(String(50), nullable=True)
    product_name = Column(String(255), nullable=True)
    stage = Column(String(50), nullable=False)  # 'branch_selection', 'search', 'parsing', etc.
    
    # Error details
    error_type = Column(String(50), nullable=False)
    error_message = Column(Text, nullable=False)
    stack_trace = Column(Text, nullable=True)
    
    # Context
    url = Column(Text, nullable=True)
    screenshot_path = Column(Text, nullable=True)
    html_snippet = Column(Text, nullable=True)
    
    # Timestamp
    occurred_at = Column(DateTime, default=func.now())
    
    # Relationships
    run = relationship("ScrapeRun", back_populates="errors")
    
    def __repr__(self):
        return f"<ScrapeError(stage='{self.stage}', type='{self.error_type}')>"


class BasketIndex(Base):
    """Pre-computed basket index values."""
    
    __tablename__ = "basket_indices"
    
    id = Column(Integer, primary_key=True)
    
    # Index identification
    basket_type = Column(String(20), nullable=False, index=True)  # 'cba', 'extended'
    index_type = Column(String(20), default="laspeyres")  # 'laspeyres', 'paasche', 'fisher'
    
    # Period
    year_month = Column(String(7), nullable=False, index=True)  # YYYY-MM format
    
    # Index value (base = 100)
    index_value = Column(Numeric(10, 2), nullable=False)
    base_period = Column(String(7), nullable=False)
    
    # Change metrics
    mom_change = Column(Numeric(6, 2), nullable=True)  # Month-over-month %
    yoy_change = Column(Numeric(6, 2), nullable=True)  # Year-over-year %
    
    # Basket value in ARS
    total_value = Column(Numeric(12, 2), nullable=False)
    
    # Metadata
    products_included = Column(Integer, nullable=False)
    products_missing = Column(Integer, default=0)
    
    computed_at = Column(DateTime, default=func.now())
    
    def __repr__(self):
        return f"<BasketIndex(basket='{self.basket_type}', period='{self.year_month}', value={self.index_value})>"


class CategoryIndex(Base):
    """Pre-computed category-level Laspeyres index values."""

    __tablename__ = "category_indices"

    id = Column(Integer, primary_key=True)

    # Index identification
    basket_type = Column(String(20), nullable=False, index=True)
    category = Column(String(100), nullable=False, index=True)

    # Period
    year_month = Column(String(7), nullable=False, index=True)

    # Index value (base = 100)
    index_value = Column(Numeric(10, 2), nullable=False)

    # Change metrics
    mom_change = Column(Numeric(6, 2), nullable=True)
    yoy_change = Column(Numeric(6, 2), nullable=True)

    # Metadata
    products_included = Column(Integer, nullable=False)
    products_missing = Column(Integer, nullable=False, default=0)

    computed_at = Column(DateTime, default=func.now())

    def __repr__(self):
        return (
            f"<CategoryIndex(basket='{self.basket_type}', category='{self.category}', "
            f"period='{self.year_month}', value={self.index_value})>"
        )


class IndexQualityAudit(Base):
    """Quality metrics for index computation by period and category."""

    __tablename__ = "index_quality_audit"

    id = Column(Integer, primary_key=True)

    basket_type = Column(String(20), nullable=False, index=True)
    year_month = Column(String(7), nullable=False, index=True)
    category = Column(String(100), nullable=False, index=True)

    coverage_rate = Column(Numeric(6, 4), nullable=False)
    outlier_count = Column(Integer, nullable=False, default=0)
    missing_count = Column(Integer, nullable=False, default=0)

    min_coverage_required = Column(Numeric(6, 4), nullable=False)
    is_coverage_sufficient = Column(Boolean, nullable=False, default=True)

    computed_at = Column(DateTime, default=func.now())

    def __repr__(self):
        return (
            f"<IndexQualityAudit(basket='{self.basket_type}', category='{self.category}', "
            f"period='{self.year_month}', coverage={self.coverage_rate})>"
        )


class Category(Base):
    """Canonical product category/rubro table."""

    __tablename__ = "categories"

    id = Column(Integer, primary_key=True)
    slug = Column(String(50), unique=True, nullable=False, index=True)
    name = Column(String(100), nullable=False)
    description = Column(Text, nullable=True)
    created_at = Column(DateTime, default=now_utc)
    updated_at = Column(DateTime, default=now_utc, onupdate=now_utc)

    products = relationship("Product", back_populates="canonical_category")
    prices = relationship("Price", back_populates="canonical_category")

    def __repr__(self):
        return f"<Category(slug='{self.slug}', name='{self.name}')>"


class OfficialCPIMonthly(Base):
    """Official monthly CPI observations (INDEC or fallback source)."""

    __tablename__ = "official_cpi_monthly"
    __table_args__ = (
        UniqueConstraint(
            "source",
            "region",
            "metric_code",
            "year_month",
            name="uq_official_cpi_source_region_metric_month",
        ),
    )

    id = Column(Integer, primary_key=True)
    source = Column(String(50), nullable=False, index=True)  # indec_auto | indec_fallback
    region = Column(String(50), nullable=False, index=True)  # patagonia
    metric_code = Column(String(64), nullable=False, index=True)  # general | division code
    category_slug = Column(String(64), nullable=True, index=True)
    year_month = Column(String(7), nullable=False, index=True)  # YYYY-MM

    index_value = Column(Numeric(12, 4), nullable=False)
    mom_change = Column(Numeric(8, 4), nullable=True)
    yoy_change = Column(Numeric(8, 4), nullable=True)

    status = Column(String(32), nullable=False, default="final", index=True)
    is_fallback = Column(Boolean, nullable=False, default=False)
    raw_snapshot_path = Column(Text, nullable=True)

    created_at = Column(DateTime, default=now_utc)
    updated_at = Column(DateTime, default=now_utc, onupdate=now_utc)

    def __repr__(self):
        return (
            f"<OfficialCPIMonthly(region='{self.region}', metric='{self.metric_code}', "
            f"period='{self.year_month}', index={self.index_value})>"
        )


class TrackerIPCMonthly(Base):
    """Tracker-owned monthly CPI index (general)."""

    __tablename__ = "tracker_ipc_monthly"
    __table_args__ = (
        UniqueConstraint(
            "basket_type",
            "year_month",
            "method_version",
            name="uq_tracker_ipc_basket_month_method",
        ),
    )

    id = Column(Integer, primary_key=True)
    basket_type = Column(String(20), nullable=False, index=True)  # cba|extended|all
    year_month = Column(String(7), nullable=False, index=True)
    method_version = Column(String(64), nullable=False, index=True)
    status = Column(String(32), nullable=False, default="provisional", index=True)

    index_value = Column(Numeric(12, 4), nullable=True)  # base 100
    mom_change = Column(Numeric(8, 4), nullable=True)  # percent
    yoy_change = Column(Numeric(8, 4), nullable=True)  # percent

    coverage_weight_pct = Column(Numeric(8, 4), nullable=True)
    coverage_product_pct = Column(Numeric(8, 4), nullable=True)
    products_expected = Column(Integer, nullable=False, default=0)
    products_observed = Column(Integer, nullable=False, default=0)
    products_with_relative = Column(Integer, nullable=False, default=0)
    outlier_count = Column(Integer, nullable=False, default=0)
    missing_products = Column(Integer, nullable=False, default=0)

    base_month = Column(String(7), nullable=True)
    notes = Column(Text, nullable=True)

    computed_at = Column(DateTime, default=now_utc)
    frozen_at = Column(DateTime, nullable=True)

    def __repr__(self):
        return (
            f"<TrackerIPCMonthly(basket='{self.basket_type}', period='{self.year_month}', "
            f"method='{self.method_version}', index={self.index_value})>"
        )


class TrackerIPCCategoryMonthly(Base):
    """Tracker-owned monthly CPI index by category."""

    __tablename__ = "tracker_ipc_category_monthly"
    __table_args__ = (
        UniqueConstraint(
            "basket_type",
            "category_slug",
            "year_month",
            "method_version",
            name="uq_tracker_ipc_cat_basket_cat_month_method",
        ),
    )

    id = Column(Integer, primary_key=True)
    basket_type = Column(String(20), nullable=False, index=True)
    category_slug = Column(String(64), nullable=False, index=True)
    indec_division_code = Column(String(32), nullable=True, index=True)
    year_month = Column(String(7), nullable=False, index=True)
    method_version = Column(String(64), nullable=False, index=True)
    status = Column(String(32), nullable=False, default="provisional", index=True)

    index_value = Column(Numeric(12, 4), nullable=True)
    mom_change = Column(Numeric(8, 4), nullable=True)
    yoy_change = Column(Numeric(8, 4), nullable=True)

    coverage_weight_pct = Column(Numeric(8, 4), nullable=True)
    coverage_product_pct = Column(Numeric(8, 4), nullable=True)
    products_expected = Column(Integer, nullable=False, default=0)
    products_observed = Column(Integer, nullable=False, default=0)
    products_with_relative = Column(Integer, nullable=False, default=0)
    outlier_count = Column(Integer, nullable=False, default=0)
    missing_products = Column(Integer, nullable=False, default=0)

    base_month = Column(String(7), nullable=True)
    notes = Column(Text, nullable=True)

    computed_at = Column(DateTime, default=now_utc)
    frozen_at = Column(DateTime, nullable=True)

    def __repr__(self):
        return (
            f"<TrackerIPCCategoryMonthly(basket='{self.basket_type}', category='{self.category_slug}', "
            f"period='{self.year_month}', method='{self.method_version}', index={self.index_value})>"
        )


class IPCPublicationRun(Base):
    """Audit trail for each IPC publication pipeline execution."""

    __tablename__ = "ipc_publication_runs"

    id = Column(Integer, primary_key=True)
    run_uuid = Column(String(36), unique=True, nullable=False, index=True)

    basket_type = Column(String(20), nullable=False, index=True)
    region = Column(String(50), nullable=False, index=True)
    method_version = Column(String(64), nullable=False, index=True)
    from_month = Column(String(7), nullable=True, index=True)
    to_month = Column(String(7), nullable=True, index=True)

    status = Column(String(32), nullable=False, default="running", index=True)
    official_source = Column(String(64), nullable=True)
    official_rows = Column(Integer, nullable=False, default=0)
    tracker_rows = Column(Integer, nullable=False, default=0)
    tracker_category_rows = Column(Integer, nullable=False, default=0)

    overlap_months = Column(Integer, nullable=False, default=0)
    warnings_json = Column(Text, nullable=True)
    metrics_json = Column(Text, nullable=True)
    error_message = Column(Text, nullable=True)

    started_at = Column(DateTime, default=now_utc)
    completed_at = Column(DateTime, nullable=True)

    def __repr__(self):
        return f"<IPCPublicationRun(run_uuid='{self.run_uuid}', status='{self.status}')>"


# Database initialization functions

def get_engine(config: dict, backend: Optional[str] = None):
    """Create database engine based on configuration."""
    backend = backend or config.get("storage", {}).get("default_backend", "sqlite")
    
    if backend == "sqlite":
        db_path = config.get("storage", {}).get("sqlite", {}).get("database_path", "data/prices.db")
        return create_engine(f"sqlite:///{db_path}")
    elif backend == "postgresql":
        pg_config = config.get("storage", {}).get("postgresql", {})
        db_url = str(pg_config.get("url") or os.getenv("DB_URL") or "").strip()
        if db_url:
            return create_engine(db_url)
        host = pg_config.get("host", "localhost")
        port = pg_config.get("port", "5432")
        database = pg_config.get("database", "laanonima_tracker")
        user = pg_config.get("user", "tracker")
        password = pg_config.get("password", "")
        
        return create_engine(
            f"postgresql://{user}:{password}@{host}:{port}/{database}"
        )
    else:
        raise ValueError(f"Unsupported database backend: {backend}")


def init_db(engine):
    """Initialize database tables."""
    Base.metadata.create_all(engine)
    _ensure_category_columns(engine)
    _ensure_runtime_indexes(engine)


def get_session_factory(engine):
    """Get session factory for database operations."""
    return sessionmaker(bind=engine)


def _sqlite_has_column(conn, table_name: str, column_name: str) -> bool:
    rows = conn.execute(text(f"PRAGMA table_info({table_name})")).mappings().all()
    return column_name in {row["name"] for row in rows}


def _ensure_category_columns(engine):
    """Best-effort schema migration for category foreign keys in existing DBs."""
    inspector = inspect(engine)
    dialect = engine.dialect.name

    with engine.begin() as conn:
        if dialect == "sqlite":
            if not _sqlite_has_column(conn, "products", "category_id"):
                try:
                    conn.execute(text("ALTER TABLE products ADD COLUMN category_id INTEGER"))
                except Exception as exc:
                    if "duplicate column name" not in str(exc).lower():
                        raise
            if not _sqlite_has_column(conn, "prices", "category_id"):
                try:
                    conn.execute(text("ALTER TABLE prices ADD COLUMN category_id INTEGER"))
                except Exception as exc:
                    if "duplicate column name" not in str(exc).lower():
                        raise
            return

        product_columns = {c["name"] for c in inspector.get_columns("products")}
        if "category_id" not in product_columns and dialect == "postgresql":
            conn.execute(text("ALTER TABLE products ADD COLUMN IF NOT EXISTS category_id INTEGER"))

        price_columns = {c["name"] for c in inspector.get_columns("prices")}
        if "category_id" not in price_columns and dialect == "postgresql":
            conn.execute(text("ALTER TABLE prices ADD COLUMN IF NOT EXISTS category_id INTEGER"))


def _ensure_runtime_indexes(engine):
    """Create performance indexes if they do not exist."""
    with engine.begin() as conn:
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_prices_canonical_scraped_at "
                "ON prices (canonical_id, scraped_at)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_prices_basket_scraped_at "
                "ON prices (basket_id, scraped_at)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_price_candidates_run_canonical "
                "ON price_candidates (run_id, canonical_id)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_price_candidates_canonical_scraped_at "
                "ON price_candidates (canonical_id, scraped_at)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_official_cpi_monthly_region_month "
                "ON official_cpi_monthly (region, year_month)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_official_cpi_monthly_category_month "
                "ON official_cpi_monthly (category_slug, year_month)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_official_cpi_monthly_status "
                "ON official_cpi_monthly (status)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_tracker_ipc_monthly_basket_month "
                "ON tracker_ipc_monthly (basket_type, year_month)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_tracker_ipc_monthly_status "
                "ON tracker_ipc_monthly (status)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_tracker_ipc_category_monthly_key "
                "ON tracker_ipc_category_monthly (basket_type, category_slug, year_month)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_tracker_ipc_category_monthly_status "
                "ON tracker_ipc_category_monthly (status)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_ipc_publication_runs_status "
                "ON ipc_publication_runs (status)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_ipc_publication_runs_region_month "
                "ON ipc_publication_runs (region, from_month, to_month)"
            )
        )
