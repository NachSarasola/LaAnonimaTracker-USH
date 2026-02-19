"""Command-line interface for La AnÃ³nima Price Tracker."""

import sys
import re
from pathlib import Path
from typing import Optional

import click
from loguru import logger

from src.scraper import run_scrape
from src.analysis import run_analysis
from src.reporting import run_report
from src.config_loader import load_config, ensure_directories
from src.exporter import (
    export_to_csv,
    export_to_parquet,
    get_history_series,
    export_history_series,
)

from src.category_backfill import (
    backfill_canonical_categories,
    validate_price_category_traceability,
)

# Configure logging
MONTH_PATTERN = re.compile(r"^\d{4}-\d{2}$")


class MonthParamType(click.ParamType):
    """Click param type to validate month values in YYYY-MM format."""

    name = "month"

    def convert(self, value, param, ctx):
        if value is None:
            return value

        if not MONTH_PATTERN.match(value):
            self.fail(
                "Formato invÃ¡lido. UsÃ¡ YYYY-MM (ejemplo vÃ¡lido: 2026-02).",
                param,
                ctx,
            )

        return value


MONTH_TYPE = MonthParamType()


def setup_logging(config: dict):
    """Setup logging configuration."""
    log_config = config.get("logging", {})
    level = log_config.get("level", "INFO")
    log_file = log_config.get("file", "data/logs/tracker.log")
    
    # Ensure log directory exists
    Path(log_file).parent.mkdir(parents=True, exist_ok=True)
    
    logger.remove()
    logger.add(
        sys.stdout,
        level=level,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan> | {message}",
    )
    logger.add(
        log_file,
        level=level,
        rotation=log_config.get("rotation", "1 week"),
        retention=log_config.get("retention", "1 month"),
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name} | {message}",
    )


@click.group()
@click.option("--config", "-c", type=click.Path(exists=True), help="Path to config file")
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose output")
@click.pass_context
def cli(ctx, config: Optional[str], verbose: bool):
    """La AnÃ³nima Price Tracker - Supermarket price tracking for Argentina."""
    # Ensure context object exists
    ctx.ensure_object(dict)
    
    # Load configuration
    try:
        cfg = load_config(config)
        ctx.obj["config"] = cfg
        ctx.obj["config_path"] = config
        
        # Ensure directories exist
        ensure_directories(cfg)
        
        # Setup logging
        setup_logging(cfg)
        
        if verbose:
            logger.level("DEBUG")
        
        logger.info("La AnÃ³nima Price Tracker initialized")
        
    except Exception as e:
        click.echo(f"Error loading configuration: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option(
    "--basket", "-b",
    type=click.Choice(["cba", "extended", "all"], case_sensitive=False),
    default="all",
    help="Basket type to scrape"
)
@click.option("--headless/--no-headless", default=True, help="Run browser in headless mode")
@click.option("--backend", type=click.Choice(["sqlite", "postgresql"]), default="sqlite", help="Database backend")
@click.option("--limit", "-n", type=int, default=None, help="Only scrape N products from deterministic plan")
@click.option(
    "--profile",
    type=click.Choice(["balanced", "full", "cba_only"], case_sensitive=False),
    default="balanced",
    show_default=True,
    help="Planning profile for run size and representativeness",
)
@click.option(
    "--runtime-budget-minutes",
    type=int,
    default=20,
    show_default=True,
    help="Target runtime budget for basket planning",
)
@click.option(
    "--rotation-items",
    type=int,
    default=4,
    show_default=True,
    help="Max rotation items in balanced profile",
)
@click.option("--sample-random", is_flag=True, help="Enable random sample mode (debug only)")
@click.option("--dry-plan", is_flag=True, help="Show deterministic plan and exit without scraping")
@click.option(
    "--candidate-storage",
    type=click.Choice(["json", "db", "off"], case_sensitive=False),
    default="db",
    show_default=True,
    help="Where to store low/mid/high candidate audit",
)
@click.option(
    "--observation-policy",
    type=click.Choice(["single", "single+audit"], case_sensitive=False),
    default="single+audit",
    show_default=True,
    help="Representative observation policy for price history",
)
@click.pass_context
def scrape(
    ctx,
    basket: str,
    headless: bool,
    backend: str,
    limit: Optional[int],
    profile: str,
    runtime_budget_minutes: int,
    rotation_items: int,
    sample_random: bool,
    dry_plan: bool,
    candidate_storage: str,
    observation_policy: str,
):
    """Run price scraping for the configured basket."""
    config = ctx.obj["config"]
    config_path = ctx.obj["config_path"]

    logger.info(
        "Starting scrape: basket={}, headless={}, backend={}, limit={}, profile={}, budget_min={}, "
        "rotation_items={}, sample_random={}, dry_plan={}, candidate_storage={}, observation_policy={}",
        basket,
        headless,
        backend,
        limit,
        profile,
        runtime_budget_minutes,
        rotation_items,
        sample_random,
        dry_plan,
        candidate_storage,
        observation_policy,
    )

    try:
        results = run_scrape(
            config_path=config_path,
            basket_type=basket,
            headless=headless,
            output_format=backend,
            limit=limit,
            profile=profile,
            runtime_budget_minutes=runtime_budget_minutes,
            rotation_items=rotation_items,
            sample_random=sample_random,
            dry_plan=dry_plan,
            candidate_storage=candidate_storage,
            observation_policy=observation_policy,
        )

        click.echo(f"\n{'='*70}")
        click.echo("SCRAPE PLAN / RESULTS")
        click.echo(f"{'='*70}")
        click.echo(f"Status: {results.get('status', 'unknown')}")
        if results.get("run_uuid"):
            click.echo(f"Run UUID: {results['run_uuid']}")
        click.echo(f"Products planned: {results.get('products_planned', 0)}")
        click.echo(f"Products scraped: {results.get('products_scraped', 0)}")
        click.echo(f"Products failed: {results.get('products_failed', 0)}")
        click.echo(f"Products skipped: {results.get('products_skipped', 0)}")
        click.echo(f"Started: {results.get('started_at', 'N/A')}")
        click.echo(f"Completed: {results.get('completed_at', 'N/A')}")

        plan_summary = results.get("plan_summary", {})
        if plan_summary:
            click.echo("\nPlan summary:")
            click.echo(
                f"  profile={plan_summary.get('profile')} | "
                f"mandatory={plan_summary.get('mandatory_count')} | "
                f"rotation_applied={plan_summary.get('rotation_applied')} | "
                f"estimated_duration_s={plan_summary.get('estimated_duration_seconds')}"
            )
            seg = plan_summary.get("segments", {})
            if seg:
                click.echo("  segments=" + ", ".join(f"{k}:{v}" for k, v in seg.items()))

        budget = results.get("budget", {})
        if budget:
            click.echo(
                f"\nBudget: target_s={budget.get('target_seconds')} | "
                f"estimated_s={budget.get('estimated_seconds')} | "
                f"actual_s={budget.get('actual_seconds')} | "
                f"within_target={budget.get('within_target')}"
            )

        by_segment = results.get("coverage_by_segment", {})
        if by_segment:
            click.echo("\nCoverage by segment:")
            for segment, row in by_segment.items():
                click.echo(
                    f"  - {segment}: planned={row.get('planned', 0)} | "
                    f"scraped={row.get('scraped', 0)} | failed={row.get('failed', 0)} | "
                    f"skipped={row.get('skipped', 0)}"
                )

        click.echo(
            f"\nObservation policy: {results.get('observation_policy', 'N/D')} | "
            f"candidate_storage={results.get('candidate_storage_mode', 'N/D')}"
        )
        if results.get("candidates_audit_path"):
            click.echo(f"Candidate audit: {results['candidates_audit_path']}")

        if results.get('errors'):
            click.echo(f"\nErrors ({len(results['errors'])}):")
            for error in results['errors'][:5]:
                click.echo(f"  - {error['product']}: {error['error']}")

        click.echo(f"{'='*70}")

        # Exit with error code if scrape failed
        if results.get('status') == 'failed':
            sys.exit(1)

    except Exception as e:
        logger.exception("Scrape failed")
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option(
    "--basket", "-b",
    type=click.Choice(["cba", "extended", "all"], case_sensitive=False),
    default="all",
    help="Basket type to analyze"
)
@click.option("--export/--no-export", default=True, help="Export results to files")
@click.option("--plot/--no-plot", default=True, help="Generate plots")
@click.pass_context
def analyze(ctx, basket: str, export: bool, plot: bool):
    """Run analysis on scraped price data."""
    config = ctx.obj["config"]
    config_path = ctx.obj["config_path"]
    
    logger.info(f"Starting analysis: basket={basket}, export={export}, plot={plot}")
    
    try:
        results = run_analysis(
            config_path=config_path,
            basket_type=basket,
            export=export,
            plot=plot,
        )
        
        click.echo(f"\n{'='*60}")
        click.echo("ANALYSIS RESULTS")
        click.echo(f"{'='*60}")
        click.echo(f"Basket type: {results['basket_type']}")
        click.echo(f"Index periods: {len(results['index_data'])}")
        
        if results.get('mae'):
            click.echo(f"MAE vs CPI: {results['mae']:.2f}")
        if results.get('rmse'):
            click.echo(f"RMSE vs CPI: {results['rmse']:.2f}")
        
        if results.get('exported_files'):
            click.echo(f"\nExported files:")
            for key, path in results['exported_files'].items():
                click.echo(f"  - {key}: {path}")
        
        if results.get('plot_path'):
            click.echo(f"\nPlot saved: {results['plot_path']}")
        
        click.echo(f"{'='*60}")
        
    except Exception as e:
        logger.exception("Analysis failed")
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option(
    "--format", "-f",
    "export_format",
    type=click.Choice(["csv", "parquet", "both"], case_sensitive=False),
    default="csv",
    help="Export format"
)
@click.option("--output", "-o", type=click.Path(), help="Output directory")
@click.option(
    "--basket", "-b",
    type=click.Choice(["cba", "extended", "all"], case_sensitive=False),
    default="all",
    help="Filter by basket type"
)
@click.pass_context
def export(ctx, export_format: str, output: Optional[str], basket: str):
    """Export price data to CSV or Parquet."""
    config = ctx.obj["config"]
    
    logger.info(f"Exporting data: format={export_format}, basket={basket}")
    
    try:
        if export_format in ["csv", "both"]:
            paths = export_to_csv(config, output_dir=output, basket_type=basket)
            click.echo(f"CSV exports:")
            for name, path in paths.items():
                click.echo(f"  - {name}: {path}")
        
        if export_format in ["parquet", "both"]:
            paths = export_to_parquet(config, output_dir=output, basket_type=basket)
            click.echo(f"Parquet exports:")
            for name, path in paths.items():
                click.echo(f"  - {name}: {path}")
        
    except Exception as e:
        logger.exception("Export failed")
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.pass_context
def init(ctx):
    """Initialize the database and directories."""
    config = ctx.obj["config"]
    
    logger.info("Initializing tracker...")
    
    try:
        # Ensure directories
        ensure_directories(config)
        
        # Initialize database
        from src.models import get_engine, init_db
        
        engine = get_engine(config)
        init_db(engine)
        
        click.echo("[OK] Directories created")
        click.echo("[OK] Database initialized")
        click.echo("\nTracker is ready to use!")
        click.echo("\nNext steps:")
        click.echo("  1. Run: python -m src.cli scrape")
        click.echo("  2. Run: python -m src.cli analyze")
        
    except Exception as e:
        logger.exception("Initialization failed")
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)




@cli.command()
@click.option("--from", "from_month", required=True, type=MONTH_TYPE, help="Mes inicial (YYYY-MM)")
@click.option("--to", "to_month", required=True, type=MONTH_TYPE, help="Mes final (YYYY-MM)")
@click.option(
    "--basket", "basket_type",
    type=click.Choice(["cba", "extended", "all"], case_sensitive=False),
    default="all",
    show_default=True,
    help="Canasta a usar para el reporte",
)
@click.option("--pdf/--no-pdf", "export_pdf", default=False, help="Exportar tambien PDF si la dependencia esta disponible")
@click.option(
    "--benchmark",
    "benchmark_mode",
    type=click.Choice(["ipc", "none"], case_sensitive=False),
    default="ipc",
    show_default=True,
    help="Benchmark macroeconomico para metricas reales",
)
@click.option(
    "--view",
    "analysis_depth",
    type=click.Choice(["executive", "intermediate", "analyst"], case_sensitive=False),
    default="executive",
    show_default=True,
    help="Profundidad visual del reporte",
)
@click.option(
    "--offline-assets",
    "offline_assets",
    type=click.Choice(["embed", "external"], case_sensitive=False),
    default="embed",
    show_default=True,
    help="Modo de assets JS (embed recomendado para offline total)",
)
@click.pass_context
def report(
    ctx,
    from_month: str,
    to_month: str,
    basket_type: str,
    export_pdf: bool,
    benchmark_mode: str,
    analysis_depth: str,
    offline_assets: str,
):
    """Generate interactive HTML report for a specific month range."""
    config_path = ctx.obj["config_path"]

    logger.info(
        f"Generating report: from={from_month}, to={to_month}, basket={basket_type}, "
        f"pdf={export_pdf}, benchmark={benchmark_mode}, view={analysis_depth}, offline_assets={offline_assets}"
    )

    try:
        results = run_report(
            config_path=config_path,
            from_month=from_month,
            to_month=to_month,
            export_pdf=export_pdf,
            basket_type=basket_type,
            benchmark_mode=benchmark_mode,
            analysis_depth=analysis_depth,
            offline_assets=offline_assets,
        )

        click.echo(f"\n{'='*60}")
        click.echo("REPORT RESULTS")
        click.echo(f"{'='*60}")
        inflation_pct = results.get("inflation_total_pct")
        if inflation_pct is None:
            click.echo("Inflacion total canasta: N/D")
        else:
            click.echo(f"Inflacion total canasta: {inflation_pct:.2f}%")
        click.echo(f"Datos disponibles: {'si' if results.get('has_data') else 'no'}")
        kpis = results.get("kpis", {})
        quality = results.get("data_quality", {}).get("quality_flags", {})
        if kpis:
            click.echo(
                "KPIs: "
                f"canasta_nom={kpis.get('inflation_basket_nominal_pct', 'N/D')} | "
                f"ipc={kpis.get('ipc_period_pct', 'N/D')} | "
                f"brecha={kpis.get('gap_vs_ipc_pp', 'N/D')} | "
                f"canasta_real={kpis.get('inflation_basket_real_pct', 'N/D')}"
            )
        if quality:
            click.echo(
                f"Calidad: {quality.get('badge', 'N/D')} | "
                f"Cobertura={quality.get('coverage_total_pct', 'N/D')}% | "
                f"Panel={quality.get('balanced_panel_n', 'N/D')}"
            )
        click.echo("\nArtefactos:")
        click.echo(f"  - HTML: {results['artifacts']['html_path']}")
        click.echo(f"  - Metadata: {results['artifacts']['metadata_path']}")
        if results['artifacts'].get('pdf_path'):
            click.echo(f"  - PDF: {results['artifacts']['pdf_path']}")
        else:
            click.echo("  - PDF: no generado")
        click.echo(f"{'='*60}")

    except Exception as e:
        logger.exception("Report generation failed")
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option("--from", "from_month", required=False, type=MONTH_TYPE, help="Mes inicial opcional (YYYY-MM)")
@click.option("--to", "to_month", required=False, type=MONTH_TYPE, help="Mes final opcional (YYYY-MM)")
@click.option(
    "--basket", "basket_type",
    type=click.Choice(["cba", "extended", "all"], case_sensitive=False),
    default="all",
    show_default=True,
    help="Canasta a usar para el reporte interactivo",
)
@click.option("--pdf/--no-pdf", "export_pdf", default=False, help="Exportar tambien PDF si la dependencia esta disponible")
@click.option(
    "--benchmark",
    "benchmark_mode",
    type=click.Choice(["ipc", "none"], case_sensitive=False),
    default="ipc",
    show_default=True,
    help="Benchmark macroeconomico para metricas reales",
)
@click.option(
    "--view",
    "analysis_depth",
    type=click.Choice(["executive", "intermediate", "analyst"], case_sensitive=False),
    default="executive",
    show_default=True,
    help="Profundidad visual del reporte",
)
@click.option(
    "--offline-assets",
    "offline_assets",
    type=click.Choice(["embed", "external"], case_sensitive=False),
    default="embed",
    show_default=True,
    help="Modo de assets JS (embed recomendado para offline total)",
)
@click.pass_context
def app(
    ctx,
    from_month: Optional[str],
    to_month: Optional[str],
    basket_type: str,
    export_pdf: bool,
    benchmark_mode: str,
    analysis_depth: str,
    offline_assets: str,
):
    """One-command interactive HTML app (auto-range if months are omitted)."""
    config_path = ctx.obj["config_path"]

    if (from_month and not to_month) or (to_month and not from_month):
        click.echo("Error: si usas --from o --to debes indicar ambos.", err=True)
        sys.exit(2)

    logger.info(
        f"Generating app report: from={from_month}, to={to_month}, basket={basket_type}, "
        f"pdf={export_pdf}, benchmark={benchmark_mode}, view={analysis_depth}, offline_assets={offline_assets}"
    )

    try:
        results = run_report(
            config_path=config_path,
            from_month=from_month,
            to_month=to_month,
            export_pdf=export_pdf,
            basket_type=basket_type,
            benchmark_mode=benchmark_mode,
            analysis_depth=analysis_depth,
            offline_assets=offline_assets,
        )

        click.echo(f"\n{'='*60}")
        click.echo("APP REPORT READY")
        click.echo(f"{'='*60}")
        click.echo(f"Rango usado: {results.get('from_month')} -> {results.get('to_month')}")
        click.echo(f"Datos disponibles: {'si' if results.get('has_data') else 'no'}")
        kpis = results.get("kpis", {})
        if kpis:
            click.echo(
                f"KPIs: canasta_nom={kpis.get('inflation_basket_nominal_pct', 'N/D')} | "
                f"ipc={kpis.get('ipc_period_pct', 'N/D')} | brecha={kpis.get('gap_vs_ipc_pp', 'N/D')}"
            )
        click.echo(f"HTML: {results['artifacts']['html_path']}")
        click.echo(f"Metadata: {results['artifacts']['metadata_path']}")
        if results["artifacts"].get("pdf_path"):
            click.echo(f"PDF: {results['artifacts']['pdf_path']}")
        click.echo(f"{'='*60}")
    except Exception as e:
        logger.exception("App report generation failed")
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option("--days", "-d", default=30, help="Number of days to look back")
@click.pass_context
def status(ctx, days: int):
    """Show status of recent scrape runs."""
    config = ctx.obj["config"]
    
    try:
        from src.models import get_engine, get_session_factory, ScrapeRun
        from datetime import datetime, timedelta
        from sqlalchemy import func
        
        engine = get_engine(config)
        Session = get_session_factory(engine)
        session = Session()
        
        since = datetime.utcnow() - timedelta(days=days)
        
        runs = session.query(ScrapeRun).filter(
            ScrapeRun.started_at >= since
        ).order_by(ScrapeRun.started_at.desc()).all()
        
        if not runs:
            click.echo(f"No runs found in the last {days} days")
            return
        
        click.echo(f"\n{'='*80}")
        click.echo(f"RECENT SCRAPE RUNS (last {days} days)")
        click.echo(f"{'='*80}")
        click.echo(f"{'ID':<5} {'Date':<20} {'Status':<12} {'Basket':<10} {'Scraped':<8} {'Failed':<8}")
        click.echo("-"*80)
        
        for run in runs[:20]:  # Show last 20
            date_str = run.started_at.strftime("%Y-%m-%d %H:%M") if run.started_at else "N/A"
            click.echo(
                f"{run.id:<5} {date_str:<20} {run.status:<12} {run.basket_type:<10} "
                f"{run.products_scraped:<8} {run.products_failed:<8}"
            )
        
        # Summary stats
        total_runs = len(runs)
        successful = sum(1 for r in runs if r.status == "completed")
        failed = sum(1 for r in runs if r.status == "failed")
        partial = sum(1 for r in runs if r.status == "partial")
        
        click.echo(f"\nSummary:")
        click.echo(f"  Total runs: {total_runs}")
        click.echo(f"  Completed: {successful}")
        click.echo(f"  Partial: {partial}")
        click.echo(f"  Failed: {failed}")
        
        session.close()

    except Exception as e:
        logger.exception("Status check failed")
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option("--days", "-d", default=90, help="Days of runs to consider")
@click.option(
    "--basket", "-b",
    type=click.Choice(["cba", "extended", "all"], case_sensitive=False),
    default="all",
    help="Basket filter for series",
)
@click.option("--product", "-p", "canonical_id", default=None, help="Product id (canonical_id) to show series for")
@click.option("--export", "export_path", default=None, type=click.Path(), help="Export series to this CSV path")
@click.option("--limit", "-n", default=15, help="Max runs to list")
@click.pass_context
def history(ctx, days: int, basket: str, canonical_id: Optional[str], export_path: Optional[str], limit: int):
    """Show scrape history and price series (one row per product per run)."""
    config = ctx.obj["config"]

    try:
        from src.models import get_engine, get_session_factory, ScrapeRun, Price
        from datetime import datetime, timedelta

        engine = get_engine(config)
        Session = get_session_factory(engine)
        session = Session()

        since = datetime.utcnow() - timedelta(days=days)
        runs = (
            session.query(ScrapeRun)
            .filter(ScrapeRun.started_at >= since)
            .order_by(ScrapeRun.started_at.desc())
            .limit(limit)
            .all()
        )
        total_observations = (
            session.query(Price)
            .join(ScrapeRun, Price.run_id == ScrapeRun.id)
            .filter(ScrapeRun.started_at >= since)
            .count()
        )
        session.close()

        if not runs and not export_path:
            click.echo(f"No runs in the last {days} days. Run: python -m src.cli scrape")
            return

        if export_path:
            path = export_history_series(
                config,
                output_path=export_path,
                basket_type=basket,
                canonical_id=canonical_id,
            )
            click.echo(f"Series exported to {path}")

        df = get_history_series(config, basket_type=basket, canonical_id=canonical_id)
        if df.empty:
            click.echo("No price observations in the selected period/basket/product.")
            return

        click.echo(f"\n{'='*80}")
        click.echo("PRICE HISTORY (one row per product per run)")
        click.echo(f"{'='*80}")
        click.echo(f"Total observations: {len(df)}  |  Products: {df['canonical_id'].nunique()}  |  Runs: {df['run_uuid'].nunique()}")
        if canonical_id:
            click.echo(f"Filtered by product: {canonical_id}")
        click.echo(f"{'='*80}")

        if limit > 0 and runs:
            click.echo(f"\nLast {len(runs)} runs:")
            for r in runs:
                dt = r.started_at.strftime("%Y-%m-%d %H:%M") if r.started_at else "N/A"
                click.echo(f"  {r.run_uuid[:8]}...  {dt}  {r.status}  basket={r.basket_type}  scraped={r.products_scraped}")

        if canonical_id and not df.empty:
            click.echo(f"\nPrice series for {canonical_id}:")
            for _, row in df.iterrows():
                click.echo(f"  {row['scraped_at']}  run={str(row['run_uuid'])[:8]}...  price={row['current_price']}")

    except Exception as e:
        logger.exception("History failed")
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command("backfill-categories")
@click.option("--backend", type=click.Choice(["sqlite", "postgresql"]), default="sqlite", help="Database backend")
@click.pass_context
def backfill_categories(ctx, backend: str):
    """Backfill canonical category assignments for historical products/prices."""
    config = ctx.obj["config"]

    try:
        from src.models import get_engine, get_session_factory, init_db

        engine = get_engine(config, backend)
        init_db(engine)
        Session = get_session_factory(engine)
        session = Session()

        result = backfill_canonical_categories(session, config)
        traceability = validate_price_category_traceability(session)

        click.echo(f"\n{'='*70}")
        click.echo("CANONICAL CATEGORY BACKFILL")
        click.echo("="*70)
        click.echo(f"Products updated: {result['products_updated']}")
        click.echo(f"Prices updated: {result['prices_updated']}")
        click.echo(f"Products unresolved: {result['unresolved_products']}")
        click.echo(f"Prices without category: {result['prices_without_category']}")
        click.echo("-"*70)
        click.echo(f"Traceable prices: {traceability['traceable_prices']} / {traceability['total_prices']}")
        click.echo("="*70)

        session.close()

        if traceability["prices_without_category"] > 0:
            sys.exit(2)

    except Exception as e:
        logger.exception("Category backfill failed")
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    cli()
