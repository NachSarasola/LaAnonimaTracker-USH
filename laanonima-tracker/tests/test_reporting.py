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
                "cba": {"items": [{"id": "prod_leche", "quantity": 1}]},
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
        result = self.generator._load_prices("2024-02", "2024-02")

        self.assertEqual(len(result), 2)
        self.assertTrue((result["month"] == "2024-02").all())

    def test_load_prices_excludes_next_month_start(self):
        result = self.generator._load_prices("2024-02", "2024-02")

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

    def test_render_html_displays_nd_for_missing_variation(self):
        top_products = pd.DataFrame(
            [
                {
                    "canonical_id": "prod_zero",
                    "product_name": "Producto cero",
                    "price_from": 0.0,
                    "price_to": 20.0,
                    "variation_pct": pd.NA,
                }
            ]
        )
        top_categories = pd.DataFrame(
            [
                {
                    "category": "lacteos",
                    "price_from": pd.NA,
                    "price_to": 10.0,
                    "variation_pct": pd.NA,
                    "mom_change": pd.NA,
                    "yoy_change": pd.NA,
                    "products_included": pd.NA,
                    "products_missing": pd.NA,
                }
            ]
        )

        html = self.generator._render_html(
            from_month="2024-01",
            to_month="2024-02",
            inflation_total_pct=10.0,
            top_categories=top_categories,
            top_products=top_products,
            coverage={
                "expected_products": 1,
                "observed_products_total": 1,
                "coverage_total_pct": 100.0,
                "observed_from": 1,
                "observed_to": 1,
                "coverage_from_pct": 100.0,
                "coverage_to_pct": 100.0,
            },
            generated_at="2024-02-01 00:00:00 UTC",
        )

        self.assertIn("<td>N/D</td>", html)


if __name__ == "__main__":
    unittest.main()
