"""Analysis module for La Anónima Price Tracker.

Computes basket indices and compares with official CPI data.
"""

import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from loguru import logger
from sqlalchemy import func
from sqlalchemy.orm import Session

from src.models import (
    Product, Price, ScrapeRun, BasketIndex,
    get_engine, get_session_factory
)
from src.config_loader import load_config


class BasketAnalyzer:
    """Analyzer for computing basket indices and price comparisons."""
    
    def __init__(self, config: Dict[str, Any], db_session: Optional[Session] = None):
        """Initialize analyzer.
        
        Args:
            config: Configuration dictionary
            db_session: Optional database session
        """
        self.config = config
        self.analysis_config = config.get("analysis", {})
        self.base_period = self.analysis_config.get("base_period", "2024-01")
        self.index_type = self.analysis_config.get("index_type", "laspeyres")
        
        if db_session:
            self.session = db_session
            self.owns_session = False
        else:
            engine = get_engine(config)
            SessionFactory = get_session_factory(engine)
            self.session = SessionFactory()
            self.owns_session = True
        
        # Set style for plots
        sns.set_style("whitegrid")
        plt.rcParams["figure.figsize"] = (12, 6)
    
    def close(self):
        """Close database session if owned."""
        if self.owns_session:
            self.session.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    def get_price_data(
        self,
        basket_type: str = "cba",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Get price data from database.
        
        Args:
            basket_type: Type of basket ('cba', 'extended', 'all')
            start_date: Start date filter (YYYY-MM-DD)
            end_date: End date filter (YYYY-MM-DD)
            
        Returns:
            DataFrame with price data
        """
        query = self.session.query(
            Price.canonical_id,
            Price.basket_id,
            Price.product_name,
            Price.current_price,
            Price.original_price,
            Price.price_per_unit,
            Price.in_stock,
            Price.is_promotion,
            Price.confidence_score,
            Price.scraped_at,
            ScrapeRun.run_uuid,
        ).join(ScrapeRun, Price.run_id == ScrapeRun.id)
        
        if basket_type != "all":
            query = query.filter(Price.basket_id == basket_type)
        
        if start_date:
            query = query.filter(Price.scraped_at >= start_date)
        
        if end_date:
            query = query.filter(Price.scraped_at <= end_date)
        
        query = query.order_by(Price.scraped_at)
        
        df = pd.read_sql(query.statement, self.session.bind)
        
        # Convert scraped_at to datetime
        df["scraped_at"] = pd.to_datetime(df["scraped_at"])
        
        # Add year-month column for grouping
        df["year_month"] = df["scraped_at"].dt.to_period("M").astype(str)
        
        return df
    
    def get_basket_weights(self, basket_type: str = "cba") -> Dict[str, Decimal]:
        """Get basket item quantities as weights.
        
        Args:
            basket_type: Type of basket
            
        Returns:
            Dictionary mapping canonical_id to quantity
        """
        from src.config_loader import get_basket_items
        
        items = get_basket_items(self.config, basket_type)
        weights = {}
        
        for item in items:
            item_id = item.get("id")
            quantity = item.get("quantity", 1)
            if item_id:
                weights[item_id] = Decimal(str(quantity))
        
        return weights
    
    def compute_basket_value(
        self,
        prices_df: pd.DataFrame,
        weights: Dict[str, Decimal],
        period: str,
    ) -> Tuple[Decimal, int, int]:
        """Compute total basket value for a period.
        
        Args:
            prices_df: DataFrame with price data
            weights: Dictionary of item weights
            period: Period string (YYYY-MM)
            
        Returns:
            Tuple of (total_value, products_included, products_missing)
        """
        period_df = prices_df[prices_df["year_month"] == period]
        
        if period_df.empty:
            return Decimal("0"), 0, len(weights)
        
        # Get latest price per product in period
        latest_prices = period_df.sort_values("scraped_at").groupby("canonical_id").last()
        
        total_value = Decimal("0")
        products_included = 0
        products_missing = 0
        
        for item_id, quantity in weights.items():
            if item_id in latest_prices.index:
                price = latest_prices.loc[item_id, "current_price"]
                if pd.notna(price):
                    total_value += Decimal(str(price)) * quantity
                    products_included += 1
                else:
                    products_missing += 1
            else:
                products_missing += 1
        
        return total_value, products_included, products_missing
    
    def compute_basket_index(
        self,
        basket_type: str = "cba",
        save_to_db: bool = True,
    ) -> pd.DataFrame:
        """Compute basket index over time.
        
        Args:
            basket_type: Type of basket
            save_to_db: Whether to save results to database
            
        Returns:
            DataFrame with index values by period
        """
        logger.info(f"Computing basket index for '{basket_type}'")
        
        # Get price data
        prices_df = self.get_price_data(basket_type)
        
        if prices_df.empty:
            logger.warning("No price data found")
            return pd.DataFrame()
        
        # Get weights
        weights = self.get_basket_weights(basket_type)
        
        # Get all periods
        periods = sorted(prices_df["year_month"].unique())
        
        if not periods:
            logger.warning("No periods found in data")
            return pd.DataFrame()
        
        # Compute base period value
        base_value, base_included, base_missing = self.compute_basket_value(
            prices_df, weights, self.base_period
        )
        
        if base_value == 0:
            # Use first available period as base
            self.base_period = periods[0]
            base_value, base_included, base_missing = self.compute_basket_value(
                prices_df, weights, self.base_period
            )
            logger.info(f"Using {self.base_period} as base period")
        
        logger.info(f"Base period {self.base_period}: ${base_value:.2f}")
        
        # Compute index for each period
        results = []
        prev_value = None
        
        for period in periods:
            value, included, missing = self.compute_basket_value(
                prices_df, weights, period
            )
            
            if base_value > 0:
                index_value = (value / base_value) * 100
            else:
                index_value = Decimal("100")
            
            # Calculate month-over-month change
            mom_change = None
            if prev_value is not None and prev_value > 0:
                mom_change = ((value - prev_value) / prev_value) * 100
            
            results.append({
                "year_month": period,
                "basket_type": basket_type,
                "index_value": float(index_value),
                "total_value": float(value),
                "base_period": self.base_period,
                "products_included": included,
                "products_missing": missing,
                "mom_change": float(mom_change) if mom_change is not None else None,
            })
            
            prev_value = value
        
        # Calculate year-over-year % change: (value - value_same_month_last_year) / value_same_month_last_year * 100
        for row in results:
            period = row["year_month"]
            year_s, month_s = period.split("-")
            prev_year_period = f"{int(year_s) - 1:04d}-{month_s}"
            prev_year_row = next(
                (r for r in results if r["year_month"] == prev_year_period),
                None,
            )
            if prev_year_row and prev_year_row.get("total_value", 0) > 0:
                val_now = row["total_value"]
                val_prev = prev_year_row["total_value"]
                row["yoy_change"] = ((val_now - val_prev) / val_prev) * 100
            else:
                row["yoy_change"] = None
        
        results_df = pd.DataFrame(results)
        
        # Save to database
        if save_to_db:
            self._save_index_to_db(results)
        
        return results_df
    
    def _save_index_to_db(self, results: List[Dict]):
        """Save computed indices to database."""
        for row in results:
            # Check if record exists
            existing = self.session.query(BasketIndex).filter_by(
                basket_type=row["basket_type"],
                year_month=row["year_month"],
            ).first()
            
            if existing:
                # Update
                existing.index_value = Decimal(str(row["index_value"]))
                existing.total_value = Decimal(str(row["total_value"]))
                existing.mom_change = Decimal(str(row["mom_change"])) if row["mom_change"] else None
                existing.yoy_change = Decimal(str(row["yoy_change"])) if row.get("yoy_change") else None
                existing.products_included = row["products_included"]
                existing.products_missing = row["products_missing"]
                existing.computed_at = datetime.now(timezone.utc)
            else:
                # Create new
                index_record = BasketIndex(
                    basket_type=row["basket_type"],
                    year_month=row["year_month"],
                    index_value=Decimal(str(row["index_value"])),
                    total_value=Decimal(str(row["total_value"])),
                    base_period=row["base_period"],
                    mom_change=Decimal(str(row["mom_change"])) if row["mom_change"] else None,
                    yoy_change=Decimal(str(row["yoy_change"])) if row.get("yoy_change") else None,
                    products_included=row["products_included"],
                    products_missing=row["products_missing"],
                )
                self.session.add(index_record)
        
        self.session.commit()
        logger.info(f"Saved {len(results)} index records to database")
    
    def load_cpi_data(self, cpi_file: Optional[str] = None) -> pd.DataFrame:
        """Load CPI data from file.
        
        Args:
            cpi_file: Path to CPI CSV file
            
        Returns:
            DataFrame with CPI data
        """
        if cpi_file is None:
            cpi_file = self.analysis_config.get("cpi_file", "data/cpi/ipc_indec.csv")
        
        cpi_path = Path(cpi_file)
        
        if not cpi_path.exists():
            logger.warning(f"CPI file not found: {cpi_file}")
            return pd.DataFrame()
        
        try:
            df = pd.read_csv(cpi_file)
            
            # Expected columns: year_month, cpi_index, cpi_mom, cpi_yoy
            required_cols = ["year_month", "cpi_index"]
            
            for col in required_cols:
                if col not in df.columns:
                    logger.warning(f"CPI file missing required column: {col}")
                    return pd.DataFrame()
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading CPI data: {e}")
            return pd.DataFrame()
    
    def compare_with_cpi(
        self,
        basket_type: str = "cba",
        cpi_file: Optional[str] = None,
    ) -> pd.DataFrame:
        """Compare basket index with official CPI.
        
        Args:
            basket_type: Type of basket
            cpi_file: Path to CPI CSV file
            
        Returns:
            DataFrame with comparison data
        """
        logger.info(f"Comparing basket '{basket_type}' with CPI")
        
        # Get basket index
        basket_df = self.compute_basket_index(basket_type, save_to_db=False)
        
        if basket_df.empty:
            logger.warning("No basket index data")
            return pd.DataFrame()
        
        # Load CPI data
        cpi_df = self.load_cpi_data(cpi_file)
        
        if cpi_df.empty:
            logger.warning("No CPI data available")
            return basket_df
        
        # Merge data
        merged = basket_df.merge(
            cpi_df,
            on="year_month",
            how="outer",
            suffixes=("_basket", "_cpi")
        ).sort_values("year_month")
        
        # Calculate differences
        if "cpi_index" in merged.columns:
            merged["index_diff"] = merged["index_value"] - merged["cpi_index"]
            merged["index_diff_pct"] = (
                (merged["index_value"] - merged["cpi_index"]) / merged["cpi_index"] * 100
            ).replace([float("inf"), -float("inf")], None)
        
        if "cpi_mom" in merged.columns and "mom_change" in merged.columns:
            merged["mom_diff"] = merged["mom_change"] - merged["cpi_mom"]
        
        # Calculate error metrics
        valid_rows = merged.dropna(subset=["index_value", "cpi_index"])
        
        if len(valid_rows) > 0:
            mae = abs(valid_rows["index_value"] - valid_rows["cpi_index"]).mean()
            rmse = ((valid_rows["index_value"] - valid_rows["cpi_index"]) ** 2).mean() ** 0.5
            
            logger.info(f"MAE vs CPI: {mae:.2f}")
            logger.info(f"RMSE vs CPI: {rmse:.2f}")
            
            merged.attrs["mae"] = mae
            merged.attrs["rmse"] = rmse
        
        return merged
    
    def plot_index_comparison(
        self,
        comparison_df: pd.DataFrame,
        output_path: Optional[str] = None,
        show_plot: bool = False,
    ) -> str:
        """Plot basket index vs CPI comparison.
        
        Args:
            comparison_df: DataFrame from compare_with_cpi()
            output_path: Path to save plot
            show_plot: Whether to display the plot
            
        Returns:
            Path to saved plot
        """
        if comparison_df.empty:
            logger.warning("No data to plot")
            return ""
        
        fig, axes = plt.subplots(2, 1, figsize=(14, 10))
        
        # Plot 1: Index values over time
        ax1 = axes[0]
        ax1.plot(
            comparison_df["year_month"],
            comparison_df["index_value"],
            marker="o",
            linewidth=2,
            label="Basket Index (Online)",
            color="#2E86AB"
        )
        
        if "cpi_index" in comparison_df.columns:
            ax1.plot(
                comparison_df["year_month"],
                comparison_df["cpi_index"],
                marker="s",
                linewidth=2,
                label="CPI (INDEC)",
                color="#A23B72"
            )
        
        ax1.set_xlabel("Period")
        ax1.set_ylabel("Index Value (Base = 100)")
        ax1.set_title("Basket Price Index vs Official CPI")
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        ax1.tick_params(axis="x", rotation=45)
        
        # Plot 2: Month-over-month changes
        ax2 = axes[1]
        
        if "mom_change" in comparison_df.columns:
            ax2.bar(
                comparison_df["year_month"],
                comparison_df["mom_change"],
                alpha=0.7,
                label="Basket MoM %",
                color="#2E86AB"
            )
        
        if "cpi_mom" in comparison_df.columns:
            ax2.bar(
                comparison_df["year_month"],
                comparison_df["cpi_mom"],
                alpha=0.5,
                label="CPI MoM %",
                color="#A23B72"
            )
        
        ax2.set_xlabel("Period")
        ax2.set_ylabel("Month-over-Month Change (%)")
        ax2.set_title("Monthly Inflation: Basket vs CPI")
        ax2.legend()
        ax2.grid(True, alpha=0.3, axis="y")
        ax2.tick_params(axis="x", rotation=45)
        ax2.axhline(y=0, color="black", linestyle="-", linewidth=0.5)
        
        plt.tight_layout()
        
        # Save plot
        if output_path is None:
            plots_dir = self.analysis_config.get("plots_dir", "data/analysis/plots")
            output_path = f"{plots_dir}/index_comparison_{datetime.now().strftime('%Y%m%d')}.png"
        
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(output_path, dpi=150, bbox_inches="tight")
        logger.info(f"Plot saved to {output_path}")
        
        if show_plot:
            plt.show()
        else:
            plt.close()
        
        return output_path
    
    def export_summary(
        self,
        basket_type: str = "cba",
        output_dir: Optional[str] = None,
    ) -> Dict[str, str]:
        """Export analysis summary to files.
        
        Args:
            basket_type: Type of basket
            output_dir: Directory for output files
            
        Returns:
            Dictionary with paths to exported files
        """
        if output_dir is None:
            output_dir = self.analysis_config.get("output_dir", "data/analysis")
        
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        paths = {}
        
        # Compute basket index
        index_df = self.compute_basket_index(basket_type)
        
        if not index_df.empty:
            # Export to CSV
            csv_path = f"{output_dir}/basket_index_{basket_type}_{timestamp}.csv"
            index_df.to_csv(csv_path, index=False)
            paths["index_csv"] = csv_path
            
            # Export to JSON
            json_path = f"{output_dir}/basket_index_{basket_type}_{timestamp}.json"
            index_df.to_json(json_path, orient="records", indent=2)
            paths["index_json"] = json_path
        
        # Compare with CPI
        comparison_df = self.compare_with_cpi(basket_type)
        
        if not comparison_df.empty and "cpi_index" in comparison_df.columns:
            csv_path = f"{output_dir}/cpi_comparison_{basket_type}_{timestamp}.csv"
            comparison_df.to_csv(csv_path, index=False)
            paths["comparison_csv"] = csv_path
            
            # Plot
            plot_path = self.plot_index_comparison(comparison_df)
            paths["comparison_plot"] = plot_path
        
        # Price summary by product
        prices_df = self.get_price_data(basket_type)
        
        if not prices_df.empty:
            # Latest prices
            latest = prices_df.sort_values("scraped_at").groupby("canonical_id").last()
            csv_path = f"{output_dir}/latest_prices_{basket_type}_{timestamp}.csv"
            latest.to_csv(csv_path)
            paths["latest_prices"] = csv_path
        
        logger.info(f"Summary exported to {output_dir}")
        return paths


def run_analysis(
    config_path: Optional[str] = None,
    basket_type: str = "cba",
    export: bool = True,
    plot: bool = True,
) -> Dict[str, Any]:
    """Run complete analysis.
    
    Args:
        config_path: Path to config file
        basket_type: Type of basket
        export: Whether to export results
        plot: Whether to generate plots
        
    Returns:
        Dictionary with analysis results
    """
    config = load_config(config_path)
    
    results = {
        "basket_type": basket_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    
    with BasketAnalyzer(config) as analyzer:
        # Compute basket index
        index_df = analyzer.compute_basket_index(basket_type)
        results["index_data"] = index_df.to_dict("records") if not index_df.empty else []
        
        # Compare with CPI
        comparison_df = analyzer.compare_with_cpi(basket_type)
        results["comparison_data"] = comparison_df.to_dict("records") if not comparison_df.empty else []
        
        # Error metrics
        if hasattr(comparison_df, "attrs"):
            results["mae"] = comparison_df.attrs.get("mae")
            results["rmse"] = comparison_df.attrs.get("rmse")
        
        # Export
        if export:
            paths = analyzer.export_summary(basket_type)
            results["exported_files"] = paths
        
        # Plot
        if plot and not comparison_df.empty:
            plot_path = analyzer.plot_index_comparison(comparison_df)
            results["plot_path"] = plot_path
    
    return results


if __name__ == "__main__":
    # Simple CLI for testing
    results = run_analysis(basket_type="cba")
    print(f"Analysis completed. Index periods: {len(results['index_data'])}")
