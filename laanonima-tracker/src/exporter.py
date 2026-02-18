"""Export module for La Anónima Price Tracker."""

from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from loguru import logger

from src.models import get_engine, get_session_factory, Product, Price, ScrapeRun
from src.config_loader import get_storage_config
from src.repositories import SeriesRepository


def export_to_csv(
    config: dict,
    output_dir: Optional[str] = None,
    basket_type: str = "all",
) -> Dict[str, str]:
    """Export price data to CSV files.
    
    Args:
        config: Configuration dictionary
        output_dir: Output directory (uses config default if None)
        basket_type: Filter by basket type
        
    Returns:
        Dictionary with paths to exported files
    """
    if output_dir is None:
        storage = get_storage_config(config)
        output_dir = storage.get("exports", {}).get("csv_path", "data/exports/csv")
    
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    engine = get_engine(config)
    Session = get_session_factory(engine)
    session = Session()
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    paths = {}
    
    try:
        # Export prices
        query = session.query(
            Price.canonical_id,
            Price.basket_id,
            Price.product_name,
            Price.product_size,
            Price.product_brand,
            Price.current_price,
            Price.original_price,
            Price.price_per_unit,
            Price.in_stock,
            Price.is_promotion,
            Price.promotion_text,
            Price.confidence_score,
            Price.scraped_at,
            ScrapeRun.run_uuid,
            ScrapeRun.branch_name,
            ScrapeRun.postal_code,
        ).join(ScrapeRun, Price.run_id == ScrapeRun.id)
        
        if basket_type != "all":
            query = query.filter(Price.basket_id == basket_type)
        
        df = pd.read_sql(query.statement, session.bind)
        
        if not df.empty:
            path = f"{output_dir}/prices_{basket_type}_{timestamp}.csv"
            df.to_csv(path, index=False)
            paths["prices"] = path
            logger.info(f"Exported {len(df)} prices to {path}")
        
        # Export products
        products_query = session.query(Product)
        if basket_type != "all":
            products_query = products_query.filter(Product.basket_id == basket_type)
        
        products_df = pd.read_sql(products_query.statement, session.bind)
        
        if not products_df.empty:
            path = f"{output_dir}/products_{basket_type}_{timestamp}.csv"
            products_df.to_csv(path, index=False)
            paths["products"] = path
            logger.info(f"Exported {len(products_df)} products to {path}")
        
        # Export runs
        runs_query = session.query(ScrapeRun)
        if basket_type != "all":
            runs_query = runs_query.filter(ScrapeRun.basket_type == basket_type)
        
        runs_df = pd.read_sql(runs_query.statement, session.bind)
        
        if not runs_df.empty:
            path = f"{output_dir}/scrape_runs_{timestamp}.csv"
            runs_df.to_csv(path, index=False)
            paths["scrape_runs"] = path
            logger.info(f"Exported {len(runs_df)} runs to {path}")
        
    finally:
        session.close()
    
    return paths


def export_to_parquet(
    config: dict,
    output_dir: Optional[str] = None,
    basket_type: str = "all",
) -> Dict[str, str]:
    """Export price data to Parquet files.
    
    Args:
        config: Configuration dictionary
        output_dir: Output directory (uses config default if None)
        basket_type: Filter by basket type
        
    Returns:
        Dictionary with paths to exported files
    """
    if output_dir is None:
        storage = get_storage_config(config)
        output_dir = storage.get("exports", {}).get("parquet_path", "data/exports/parquet")
    
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    engine = get_engine(config)
    Session = get_session_factory(engine)
    session = Session()
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    paths = {}
    
    try:
        # Export prices
        query = session.query(
            Price.canonical_id,
            Price.basket_id,
            Price.product_name,
            Price.product_size,
            Price.product_brand,
            Price.current_price,
            Price.original_price,
            Price.price_per_unit,
            Price.in_stock,
            Price.is_promotion,
            Price.promotion_text,
            Price.confidence_score,
            Price.scraped_at,
            ScrapeRun.run_uuid,
            ScrapeRun.branch_name,
            ScrapeRun.postal_code,
        ).join(ScrapeRun, Price.run_id == ScrapeRun.id)
        
        if basket_type != "all":
            query = query.filter(Price.basket_id == basket_type)
        
        df = pd.read_sql(query.statement, session.bind)
        
        if not df.empty:
            path = f"{output_dir}/prices_{basket_type}_{timestamp}.parquet"
            df.to_parquet(path, index=False, compression="snappy")
            paths["prices"] = path
            logger.info(f"Exported {len(df)} prices to {path}")
        
        # Export products
        products_query = session.query(Product)
        if basket_type != "all":
            products_query = products_query.filter(Product.basket_id == basket_type)
        
        products_df = pd.read_sql(products_query.statement, session.bind)
        
        if not products_df.empty:
            path = f"{output_dir}/products_{basket_type}_{timestamp}.parquet"
            products_df.to_parquet(path, index=False, compression="snappy")
            paths["products"] = path
            logger.info(f"Exported {len(products_df)} products to {path}")
        
        # Export runs
        runs_query = session.query(ScrapeRun)
        if basket_type != "all":
            runs_query = runs_query.filter(ScrapeRun.basket_type == basket_type)
        
        runs_df = pd.read_sql(runs_query.statement, session.bind)
        
        if not runs_df.empty:
            path = f"{output_dir}/scrape_runs_{timestamp}.parquet"
            runs_df.to_parquet(path, index=False, compression="snappy")
            paths["scrape_runs"] = path
            logger.info(f"Exported {len(runs_df)} runs to {path}")
        
    finally:
        session.close()
    
    return paths


def export_run_to_csv(
    config: dict,
    run_uuid: str,
    output_path: Optional[str] = None,
) -> str:
    """Export a specific run to CSV.
    
    Args:
        config: Configuration dictionary
        run_uuid: UUID of the run to export
        output_path: Output file path
        
    Returns:
        Path to exported file
    """
    engine = get_engine(config)
    Session = get_session_factory(engine)
    session = Session()
    
    try:
        query = session.query(
            Price.canonical_id,
            Price.basket_id,
            Price.product_name,
            Price.product_size,
            Price.product_brand,
            Price.current_price,
            Price.original_price,
            Price.price_per_unit,
            Price.in_stock,
            Price.is_promotion,
            Price.scraped_at,
        ).join(ScrapeRun, Price.run_id == ScrapeRun.id).filter(
            ScrapeRun.run_uuid == run_uuid
        )
        
        df = pd.read_sql(query.statement, session.bind)
        
        if output_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = f"data/exports/run_{run_uuid}_{timestamp}.csv"
        
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_path, index=False)
        
        logger.info(f"Exported run {run_uuid} to {output_path}")
        return output_path
        
    finally:
        session.close()


def create_price_timeseries(
    config: dict,
    basket_type: str = "all",
) -> pd.DataFrame:
    """Create a wide-format price time series.
    
    Args:
        config: Configuration dictionary
        basket_type: Filter by basket type
        
    Returns:
        DataFrame with products as columns and dates as rows
    """
    engine = get_engine(config)
    Session = get_session_factory(engine)
    session = Session()
    
    try:
        query = session.query(
            Price.canonical_id,
            Price.product_name,
            Price.current_price,
            Price.scraped_at,
        ).join(ScrapeRun, Price.run_id == ScrapeRun.id)
        
        if basket_type != "all":
            query = query.filter(Price.basket_id == basket_type)
        
        df = pd.read_sql(query.statement, session.bind)
        
        if df.empty:
            return pd.DataFrame()
        
        # Convert to datetime and extract date
        df["scraped_at"] = pd.to_datetime(df["scraped_at"])
        df["date"] = df["scraped_at"].dt.date
        
        # Pivot to wide format
        pivot = df.pivot_table(
            index="date",
            columns="canonical_id",
            values="current_price",
            aggfunc="last"  # Take last price of the day
        )
        
        return pivot

    finally:
        session.close()


def get_history_series(
    config: dict,
    basket_type: str = "all",
    canonical_id: Optional[str] = None,
) -> pd.DataFrame:
    """Get price history as long-format series (one row per product per run).

    Each product is identified by canonical_id; each run adds one observation
    so you can track price evolution over time.

    Args:
        config: Configuration dictionary
        basket_type: Filter by basket type ('cba', 'extended', 'all')
        canonical_id: If set, filter to this product id only

    Returns:
        DataFrame with columns: canonical_id, product_name, basket_id,
        scraped_at, run_uuid, run_started_at, current_price, original_price,
        price_per_unit, in_stock, is_promotion
    """
    engine = get_engine(config)
    Session = get_session_factory(engine)
    session = Session()

    try:
        repository = SeriesRepository(session)
        rows = repository.get_all_product_series(
            canonical_id=canonical_id,
            basket_type=basket_type,
        )
        df = pd.DataFrame(rows)

        if not df.empty:
            df = df.sort_values(["canonical_id", "scraped_at"])
        if not df.empty:
            df["scraped_at"] = pd.to_datetime(df["scraped_at"])
            df["run_started_at"] = pd.to_datetime(df["run_started_at"])
        return df
    finally:
        session.close()


def export_history_series(
    config: dict,
    output_path: Optional[str] = None,
    basket_type: str = "all",
    canonical_id: Optional[str] = None,
) -> str:
    """Export price history series to CSV (one row per observation)."""
    if output_path is None:
        storage = get_storage_config(config)
        out_dir = storage.get("exports", {}).get("csv_path", "data/exports/csv")
        Path(out_dir).mkdir(parents=True, exist_ok=True)
        suffix = f"_{canonical_id}" if canonical_id else ""
        output_path = (
            f"{out_dir}/price_history{suffix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        )

    df = get_history_series(config, basket_type=basket_type, canonical_id=canonical_id)
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    logger.info(f"Exported {len(df)} price observations to {output_path}")
    return output_path
