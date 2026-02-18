"""Quality validation tests for basket/category analysis."""

import sys
import unittest
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.analysis import BasketAnalyzer
from src.models import (
    CategoryIndex,
    IndexQualityAudit,
    Price,
    Product,
    ScrapeRun,
    get_engine,
    get_session_factory,
    init_db,
)


class TestAnalysisQuality(unittest.TestCase):
    def setUp(self):
        self.config = {
            "analysis": {
                "base_period": "2024-01",
                "index_type": "laspeyres",
                "validation": {
                    "min_coverage_rate": 0.8,
                    "max_price_jump_pct": 150,
                },
            },
            "storage": {"default_backend": "sqlite", "sqlite": {"database_path": ":memory:"}},
            "baskets": {
                "cba": {
                    "items": [
                        {"id": "prod_leche", "quantity": 1},
                        {"id": "prod_pan", "quantity": 1},
                    ]
                },
                "extended": {"items": []},
            },
        }

        engine = get_engine(self.config, "sqlite")
        init_db(engine)
        Session = get_session_factory(engine)
        self.session = Session()
        self._seed_data()

    def tearDown(self):
        self.session.close()

    def _seed_data(self):
        run = ScrapeRun(
            run_uuid="quality-run",
            branch_id="75",
            branch_name="USHUAIA",
            postal_code="9410",
            basket_type="cba",
        )
        self.session.add(run)
        self.session.flush()

        leche = Product(canonical_id="prod_leche", basket_id="cba", name="Leche", category="lacteos", is_active=True)
        pan = Product(canonical_id="prod_pan", basket_id="cba", name="Pan", category="almacen", is_active=True)
        self.session.add_all([leche, pan])
        self.session.flush()

        self.session.add_all(
            [
                Price(
                    product_id=leche.id,
                    run_id=run.id,
                    canonical_id="prod_leche",
                    basket_id="cba",
                    product_name="Leche",
                    current_price=100,
                    in_stock=True,
                    scraped_at=datetime(2024, 1, 10),
                ),
                Price(
                    product_id=pan.id,
                    run_id=run.id,
                    canonical_id="prod_pan",
                    basket_id="cba",
                    product_name="Pan",
                    current_price=50,
                    in_stock=True,
                    scraped_at=datetime(2024, 1, 10),
                ),
                Price(
                    product_id=leche.id,
                    run_id=run.id,
                    canonical_id="prod_leche",
                    basket_id="cba",
                    product_name="Leche",
                    current_price=500,
                    in_stock=True,
                    scraped_at=datetime(2024, 2, 10),
                ),
                Price(
                    product_id=pan.id,
                    run_id=run.id,
                    canonical_id="prod_pan",
                    basket_id="cba",
                    product_name="Pan",
                    current_price=55,
                    in_stock=False,
                    scraped_at=datetime(2024, 2, 10),
                ),
            ]
        )
        self.session.commit()

    def test_quality_filters_and_audit_metrics(self):
        analyzer = BasketAnalyzer(self.config, db_session=self.session)
        category_df = analyzer.compute_category_indices(basket_type="cba", save_to_db=True)

        self.assertFalse(category_df.empty)
        self.assertEqual(self.session.query(CategoryIndex).count(), 4)

        lacteos_feb = category_df[(category_df["category"] == "lacteos") & (category_df["year_month"] == "2024-02")].iloc[0]
        self.assertAlmostEqual(lacteos_feb["index_value"], 0.0, places=2)

        lacteos_audit = (
            self.session.query(IndexQualityAudit)
            .filter_by(category="lacteos", year_month="2024-02")
            .one()
        )
        self.assertEqual(lacteos_audit.outlier_count, 1)
        self.assertAlmostEqual(float(lacteos_audit.coverage_rate), 0.0, places=3)
        self.assertFalse(lacteos_audit.is_coverage_sufficient)

        almacen_audit = (
            self.session.query(IndexQualityAudit)
            .filter_by(category="almacen", year_month="2024-02")
            .one()
        )
        self.assertEqual(almacen_audit.missing_count, 1)
        self.assertAlmostEqual(float(almacen_audit.coverage_rate), 0.0, places=3)


if __name__ == "__main__":
    unittest.main()
