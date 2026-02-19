"""Tests for reporting date-range boundaries."""

import sys
import unittest
from datetime import datetime
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.models import Price, Product, ScrapeRun, get_engine, get_session_factory, init_db
from src.reporting import ReportGenerator


class TestReportMonthlyRange(unittest.TestCase):
    def setUp(self):
        self.config = {
            "storage": {"default_backend": "sqlite", "sqlite": {"database_path": ":memory:"}},
            "baskets": {
                "cba": {"items": [{"id": "prod_leche", "quantity": 1, "category": "lacteos"}]},
                "extended": {"items": []},
            },
        }

        engine = get_engine(self.config, "sqlite")
        init_db(engine)
        session_factory = get_session_factory(engine)
        self.session = session_factory()
        self._seed_data()

        self.generator = ReportGenerator(self.config)
        self.generator.session = self.session

    def tearDown(self):
        self.generator.close()

    def _seed_data(self):
        run = ScrapeRun(
            run_uuid="33333333-3333-3333-3333-333333333333",
            branch_id="75",
            branch_name="USHUAIA",
            postal_code="9410",
            basket_type="cba",
        )
        self.session.add(run)
        self.session.flush()

        product = Product(
            canonical_id="prod_leche",
            basket_id="cba",
            name="Leche",
            category="lacteos",
            is_active=True,
        )
        self.session.add(product)
        self.session.flush()

        self.session.add_all(
            [
                Price(
                    product_id=product.id,
                    run_id=run.id,
                    canonical_id="prod_leche",
                    basket_id="cba",
                    product_name="Leche",
                    current_price=100,
                    in_stock=True,
                    scraped_at=datetime(2024, 2, 29, 0, 0, 0),
                ),
                Price(
                    product_id=product.id,
                    run_id=run.id,
                    canonical_id="prod_leche",
                    basket_id="cba",
                    product_name="Leche",
                    current_price=110,
                    in_stock=True,
                    scraped_at=datetime(2024, 2, 29, 10, 0, 0),
                ),
                Price(
                    product_id=product.id,
                    run_id=run.id,
                    canonical_id="prod_leche",
                    basket_id="cba",
                    product_name="Leche",
                    current_price=120,
                    in_stock=True,
                    scraped_at=datetime(2024, 3, 1, 0, 0, 0),
                ),
            ]
        )
        self.session.commit()

    def test_load_prices_includes_entire_last_day_of_month(self):
        result = self.generator._load_prices("2024-02", "2024-02", "all")

        self.assertEqual(len(result), 2)
        self.assertTrue((result["month"] == "2024-02").all())

    def test_load_prices_excludes_next_month_start(self):
        result = self.generator._load_prices("2024-02", "2024-02", "all")

        self.assertFalse((result["scraped_at"] >= datetime(2024, 3, 1, 0, 0, 0)).any())

    def test_variation_between_months_marks_nd_when_price_from_zero_or_nan(self):
        grouped = pd.DataFrame(
            [
                {"canonical_id": "prod_zero", "month": "2024-01", "current_price": 0.0},
                {"canonical_id": "prod_zero", "month": "2024-02", "current_price": 20.0},
                {"canonical_id": "prod_nan", "month": "2024-01", "current_price": pd.NA},
                {"canonical_id": "prod_nan", "month": "2024-02", "current_price": 50.0},
                {"canonical_id": "prod_valid", "month": "2024-01", "current_price": 100.0},
                {"canonical_id": "prod_valid", "month": "2024-02", "current_price": 120.0},
            ]
        )

        result = self.generator._variation_between_months(grouped, "canonical_id", "2024-01", "2024-02")

        by_id = result.set_index("canonical_id")
        self.assertAlmostEqual(float(by_id.loc["prod_valid", "variation_pct"]), 20.0)
        self.assertTrue(pd.isna(by_id.loc["prod_zero", "variation_pct"]))
        self.assertTrue(pd.isna(by_id.loc["prod_nan", "variation_pct"]))

    def test_render_interactive_html_contains_required_controls(self):
        payload = self.generator._build_interactive_payload(
            self.generator._load_prices("2024-02", "2024-03", "all"),
            "2024-02",
            "2024-03",
            "all",
        )
        html = self.generator._render_interactive_html(payload, "2024-03-10 00:00:00 UTC")

        self.assertIn("id=\"q\"", html)
        self.assertIn("id=\"cba\"", html)
        self.assertIn("id=\"cat\"", html)
        self.assertIn("id=\"ord\"", html)
        self.assertIn("id=\"mbase\"", html)
        self.assertIn("id=\"reset\"", html)
        self.assertIn("id=\"copy-link\"", html)
        self.assertIn("id=\"copy-link-status\"", html)
        self.assertIn("id=\"mobile-onboarding\"", html)
        self.assertIn("Producto (hipervinculo)", html)
        self.assertIn("id=\"kpi-grid\"", html)
        self.assertIn("id=\"quality-panel\"", html)
        self.assertIn("id=\"quality-segments\"", html)
        self.assertIn("id=\"quality-policy\"", html)
        self.assertIn("IPC Propio vs IPC Oficial", html)
        self.assertIn("id=\"macro-scope\"", html)
        self.assertIn("id=\"macro-region\"", html)
        self.assertIn("id=\"macro-category\"", html)
        self.assertIn("id=\"macro-status\"", html)
        self.assertIn("id=\"panel-bands\"", html)
        self.assertIn("id=\"band-product\"", html)
        self.assertIn("id=\"chart-bands\"", html)
        self.assertIn("id=\"page-size\"", html)
        self.assertIn("id=\"export-csv\"", html)
        self.assertIn("id=\"quality-macro\"", html)
        self.assertIn("terna low/mid/high auditada", html)

    def test_coverage_metrics_uses_expected_products_by_selected_basket(self):
        df = pd.DataFrame(
            [
                {"canonical_id": "prod_leche", "month": "2024-02", "category": "lacteos"},
                {"canonical_id": "prod_carne", "month": "2024-02", "category": "carnes"},
            ]
        )

        coverage = self.generator._coverage_metrics(df, "2024-02", "2024-02", "cba")

        self.assertEqual(coverage["expected_products"], 1)
        self.assertEqual(coverage["expected_products_by_category"]["lacteos"], 1)
        self.assertEqual(coverage["observed_products_total"], 1)
        self.assertEqual(coverage["unexpected_observed_products"], 1)
        self.assertAlmostEqual(coverage["coverage_total_pct"], 100.0)

    def test_coverage_metrics_tracks_category_denominator_independently(self):
        self.generator.config["baskets"]["cba"]["items"] = [
            {"id": "a", "category": "lacteos"},
            {"id": "b", "category": "lacteos"},
            {"id": "c", "category": "carnes"},
        ]
        df = pd.DataFrame(
            [
                {"canonical_id": "a", "month": "2024-02", "category": "lacteos"},
                {"canonical_id": "b", "month": "2024-02", "category": "lacteos"},
                {"canonical_id": "c", "month": "2024-02", "category": "carnes"},
            ]
        )

        coverage = self.generator._coverage_metrics(df, "2024-02", "2024-02", "cba")
        by_category = {item["category"]: item for item in coverage["coverage_by_category"]}

        self.assertAlmostEqual(coverage["coverage_total_pct"], 100.0)
        self.assertEqual(by_category["lacteos"]["expected_products"], 2)
        self.assertAlmostEqual(by_category["lacteos"]["coverage_pct"], 100.0)
        self.assertEqual(by_category["carnes"]["expected_products"], 1)
        self.assertAlmostEqual(by_category["carnes"]["coverage_pct"], 100.0)



if __name__ == "__main__":
    unittest.main()
