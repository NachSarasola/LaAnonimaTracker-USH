"""Tests for reporting date-range boundaries."""

import sys
import unittest
from datetime import datetime
from pathlib import Path

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


if __name__ == "__main__":
    unittest.main()
