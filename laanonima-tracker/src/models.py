"""Database models for La Anónima Price Tracker."""

from datetime import datetime, timezone

def now_utc():
    return datetime.now(timezone.utc)
from decimal import Decimal
from typing import Optional

from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Text,
    Boolean,
    create_engine,
    event,
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
    
    def __repr__(self):
        return f"<Product(canonical_id='{self.canonical_id}', name='{self.name}')>"


class Price(Base):
    """Price observations table (long format)."""
    
    __tablename__ = "prices"
    
    id = Column(Integer, primary_key=True)
    
    # Foreign keys
    product_id = Column(Integer, ForeignKey("products.id"), nullable=False, index=True)
    run_id = Column(Integer, ForeignKey("scrape_runs.id"), nullable=False, index=True)
    
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


# Database initialization functions

def get_engine(config: dict, backend: Optional[str] = None):
    """Create database engine based on configuration."""
    backend = backend or config.get("storage", {}).get("default_backend", "sqlite")
    
    if backend == "sqlite":
        db_path = config.get("storage", {}).get("sqlite", {}).get("database_path", "data/prices.db")
        return create_engine(f"sqlite:///{db_path}")
    elif backend == "postgresql":
        pg_config = config.get("storage", {}).get("postgresql", {})
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


def get_session_factory(engine):
    """Get session factory for database operations."""
    return sessionmaker(bind=engine)
