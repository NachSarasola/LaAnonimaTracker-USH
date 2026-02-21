"""Tests for interactive HTML report payload and empty-state behavior."""

import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.models import Price, PriceCandidate, Product, ScrapeRun, get_engine, get_session_factory, init_db
from src.reporting import ReportGenerator


class TestInteractiveReport(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.config = {
            "storage": {"default_backend": "sqlite", "sqlite": {"database_path": self.tmp.name}},
            "baskets": {
                "cba": {"items": [{"id": "prod_1", "category": "lacteos"}]},
                "extended": {"items": [{"id": "prod_2", "category": "bebidas"}]},
            },
        }
        self.engine = get_engine(self.config, "sqlite")
        init_db(self.engine)
        self.session = get_session_factory(self.engine)()
        self.generator = ReportGenerator(self.config)

    def tearDown(self):
        self.session.close()
        self.generator.close()
        self.engine.dispose()
        try:
            Path(self.tmp.name).unlink(missing_ok=True)
        except PermissionError:
            pass

    def _seed(self):
        run = ScrapeRun(
            run_uuid="44444444-4444-4444-4444-444444444444",
            branch_id="75",
            branch_name="USHUAIA",
            postal_code="9410",
            basket_type="all",
        )
        self.session.add(run)
        self.session.flush()

        p1 = Product(canonical_id="prod_1", basket_id="cba", name="Leche", category="lacteos")
        p2 = Product(canonical_id="prod_2", basket_id="extended", name="Jugo", category="bebidas")
        self.session.add_all([p1, p2])
        self.session.flush()

        self.session.add_all(
            [
                Price(
                    product_id=p1.id,
                    run_id=run.id,
                    canonical_id="prod_1",
                    basket_id="cba",
                    product_name="Leche Entera 1L",
                    product_size="1 L",
                    product_url="https://www.laanonima.com.ar/producto/art_123",
                    current_price=100,
                    scraped_at=datetime(2024, 1, 5, 10, 0, 0),
                ),
                Price(
                    product_id=p1.id,
                    run_id=run.id,
                    canonical_id="prod_1",
                    basket_id="cba",
                    product_name="Leche Entera 1L",
                    product_size="1 L",
                    product_url="https://www.laanonima.com.ar/producto/art_123",
                    current_price=120,
                    scraped_at=datetime(2024, 2, 5, 10, 0, 0),
                ),
                Price(
                    product_id=p2.id,
                    run_id=run.id,
                    canonical_id="prod_2",
                    basket_id="extended",
                    product_name="Jugo Naranja 1L",
                    product_size="1 L",
                    product_url="https://www.laanonima.com.ar/producto/art_999",
                    current_price=300,
                    scraped_at=datetime(2024, 2, 6, 10, 0, 0),
                ),
            ]
        )
        self.session.add_all(
            [
                PriceCandidate(
                    run_id=run.id,
                    canonical_id="prod_1",
                    basket_id="cba",
                    product_id="cand-low-1",
                    product_name="Leche Entera 1L",
                    tier="low",
                    candidate_rank=1,
                    candidate_price=95,
                    candidate_name="Leche Marca A 1L",
                    candidate_url="https://www.laanonima.com.ar/producto/art_low_1",
                    scraped_at=datetime(2024, 2, 5, 10, 0, 0),
                ),
                PriceCandidate(
                    run_id=run.id,
                    canonical_id="prod_1",
                    basket_id="cba",
                    product_id="cand-mid-1",
                    product_name="Leche Entera 1L",
                    tier="mid",
                    candidate_rank=2,
                    candidate_price=120,
                    candidate_name="Leche Marca B 1L",
                    candidate_url="https://www.laanonima.com.ar/producto/art_mid_1",
                    scraped_at=datetime(2024, 2, 5, 10, 0, 0),
                ),
                PriceCandidate(
                    run_id=run.id,
                    canonical_id="prod_1",
                    basket_id="cba",
                    product_id="cand-high-1",
                    product_name="Leche Entera 1L",
                    tier="high",
                    candidate_rank=3,
                    candidate_price=139,
                    candidate_name="Leche Marca C 1L",
                    candidate_url="https://www.laanonima.com.ar/producto/art_high_1",
                    scraped_at=datetime(2024, 2, 5, 10, 0, 0),
                ),
            ]
        )
        self.session.commit()

    def test_payload_contract_contains_required_fields(self):
        self._seed()
        df = self.generator._load_prices("2024-01", "2024-02", "all")
        payload = self.generator._build_interactive_payload(df, "2024-01", "2024-02", "all")

        self.assertTrue(payload["has_data"])
        self.assertIn("timeline", payload)
        self.assertIn("snapshot", payload)
        self.assertIn("monthly_reference", payload)
        self.assertIn("months", payload)
        self.assertIn("categories", payload)
        self.assertIn("ipc_series", payload)
        self.assertIn("tracker_ipc_series", payload)
        self.assertIn("official_patagonia_series", payload)
        self.assertIn("official_series_by_region", payload)
        self.assertIn("ipc_comparison_series", payload)
        self.assertIn("category_comparison_series", payload)
        self.assertIn("ipc_comparison_by_region", payload)
        self.assertIn("category_comparison_by_region", payload)
        self.assertIn("publication_status", payload)
        self.assertIn("publication_status_by_region", payload)
        self.assertIn("category_mapping_meta", payload)
        self.assertIn("product_monthly_metrics", payload)
        self.assertIn("kpi_summary", payload)
        self.assertIn("quality_flags", payload)
        self.assertIn("scrape_quality", payload)
        self.assertIn("candidate_bands", payload)
        self.assertIn("candidate_band_summary", payload)
        self.assertEqual(payload["ui_version"], 2)
        self.assertIn("page_size", payload["ui_defaults"])
        self.assertIn("page_sizes", payload["filters_available"])
        self.assertIn("macro_region", payload["ui_defaults"])
        self.assertIn("macro_regions", payload["filters_available"])

        row = payload["snapshot"][0]
        self.assertIn("product_url", row)
        self.assertIn("presentation", row)
        self.assertIn("category", row)
        self.assertIn("basket_id", row)
        self.assertIn("current_price", row)
        self.assertIn("current_real_price", row)

    def test_generate_empty_database_still_creates_html(self):
        result = self.generator.generate(from_month="2024-01", to_month="2024-02", basket_type="all")
        self.assertFalse(result["has_data"])
        self.assertIn("observation_policy", result)
        self.assertIn("candidate_storage_mode", result)
        self.assertTrue(Path(result["artifacts"]["html_path"]).exists())
        self.assertTrue(Path(result["artifacts"]["metadata_path"]).exists())

    def test_default_offline_embed_does_not_use_cdn(self):
        self._seed()
        result = self.generator.generate(from_month="2024-01", to_month="2024-02", basket_type="all")
        html = Path(result["artifacts"]["html_path"]).read_text(encoding="utf-8")
        self.assertNotIn("https://cdn.plot.ly", html)

    def test_payload_includes_candidate_triplets_latest_by_id(self):
        self._seed()
        df = self.generator._load_prices("2024-01", "2024-02", "all")
        payload = self.generator._build_interactive_payload(df, "2024-01", "2024-02", "all")

        triplets = payload.get("candidate_triplets_latest_by_id", {})
        self.assertIn("prod_1", triplets)
        self.assertIn("low", triplets["prod_1"])
        self.assertIn("mid", triplets["prod_1"])
        self.assertIn("high", triplets["prod_1"])
        self.assertEqual(triplets["prod_1"]["low"]["candidate_name"], "Leche Marca A 1L")
        self.assertIn("art_low_1", str(triplets["prod_1"]["low"]["candidate_url"]))

    def test_render_contains_candidate_subrows_with_links(self):
        self._seed()
        df = self.generator._load_prices("2024-01", "2024-02", "all")
        payload = self.generator._build_interactive_payload(df, "2024-01", "2024-02", "all")
        html = self.generator._render_interactive_html(payload, "2026-02-21 00:00:00 UTC")

        self.assertIn("candidateTripletsById", html)
        self.assertIn("row-candidate", html)
        self.assertIn("candidate-tier", html)
        self.assertIn("target=\"_blank\" rel=\"noopener noreferrer\"", html)

    def test_plotly_mode_uses_normalized_axis_config_and_canvas_fallback(self):
        self._seed()
        result = self.generator.generate(
            from_month="2024-01",
            to_month="2024-02",
            basket_type="all",
            offline_assets="external",
        )
        html = Path(result["artifacts"]["html_path"]).read_text(encoding="utf-8")

        self.assertIn("function niceStep(raw)", html)
        self.assertIn("function niceRange(minVal,maxVal,targetTicks=6,padRatio=0.08)", html)
        self.assertIn("dtick:yr.dtick", html)
        self.assertIn("drawSecondaryPlotly(", html)
        self.assertIn("if(!hasPlotly())", html)
        self.assertIn("drawCanvasChart(", html)

    def test_reporting_table_shows_only_main_rows_until_expanded(self):
        self._seed()
        result = self.generator.generate(from_month="2024-01", to_month="2024-02", basket_type="all")
        html = Path(result["artifacts"]["html_path"]).read_text(encoding="utf-8")

        self.assertIn("if(isExpanded){", html)
        self.assertIn("row-main", html)
        self.assertIn("row-candidate", html)

    def test_reporting_row_click_expands_but_anchor_click_does_not_toggle(self):
        self._seed()
        result = self.generator.generate(from_month="2024-01", to_month="2024-02", basket_type="all")
        html = Path(result["artifacts"]["html_path"]).read_text(encoding="utf-8")

        self.assertIn('if(target && target.closest("a")) return;', html)
        self.assertIn('tr.setAttribute("aria-expanded"', html)

    def test_reporting_executive_mode_collapses_technical_details(self):
        self._seed()
        result = self.generator.generate(
            from_month="2024-01",
            to_month="2024-02",
            basket_type="all",
            analysis_depth="executive",
        )
        html = Path(result["artifacts"]["html_path"]).read_text(encoding="utf-8")

        self.assertIn('<details class="card quality" id="quality-panel">', html)
        self.assertIn('<details class="card chart-card chart-panel secondary-chart-panel" id="panel-secondary">', html)
        self.assertIn('<details class="card chart-card chart-panel main-chart-panel" id="main-chart-panel">', html)
        self.assertNotIn('id="quality-panel" open', html)
        self.assertNotIn('id="panel-secondary" open', html)

    def test_reporting_normalize_presentation_recognizes_cc_cm3(self):
        self._seed()
        result = self.generator.generate(from_month="2024-01", to_month="2024-02", basket_type="all")
        html = Path(result["artifacts"]["html_path"]).read_text(encoding="utf-8")

        self.assertIn('replace(/c\\.\\s*c\\./gi,"cc")', html)
        self.assertIn('replace(/cm\\s*3/gi,"cm3")', html)
        self.assertIn('unit==="cc" || unit==="cm3"', html)


if __name__ == "__main__":
    unittest.main()
