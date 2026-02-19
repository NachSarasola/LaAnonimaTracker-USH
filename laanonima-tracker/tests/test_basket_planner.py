"""Tests for deterministic basket planning and runtime budgeting."""

import sys
import unittest
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.basket_planner import build_scrape_plan
from src.models import Price, Product, ScrapeRun, get_engine, get_session_factory, init_db


class TestBasketPlanner(unittest.TestCase):
    def setUp(self):
        self.config = {
            "storage": {"default_backend": "sqlite", "sqlite": {"database_path": ":memory:"}},
            "scraping": {
                "planning": {
                    "runtime_budget_minutes": 20,
                    "rotation_items_default": 2,
                    "lookback_runs": 10,
                    "overhead_seconds": 90,
                    "daily_core_ids": ["ext_harina"],
                    "daily_rotation_ids": ["ext_jugo", "ext_choclo", "ext_arvejas"],
                }
            },
            "baskets": {
                "cba": {
                    "items": [
                        {"id": "cba_arroz", "name": "Arroz", "category": "legumbres"},
                        {"id": "cba_fideos", "name": "Fideos", "category": "legumbres"},
                    ]
                },
                "extended": {
                    "items": [
                        {"id": "ext_harina", "name": "Harina", "category": "panaderia"},
                        {"id": "ext_jugo", "name": "Jugo", "category": "bebidas"},
                        {"id": "ext_choclo", "name": "Choclo", "category": "conservas"},
                        {"id": "ext_arvejas", "name": "Arvejas", "category": "conservas"},
                    ]
                },
            },
        }
        engine = get_engine(self.config, "sqlite")
        init_db(engine)
        self.session = get_session_factory(engine)()
        self._seed_history()

    def tearDown(self):
        self.session.close()

    def _seed_history(self):
        run = ScrapeRun(
            run_uuid="planner-run-1",
            branch_id="75",
            branch_name="USHUAIA",
            postal_code="9410",
            basket_type="all",
            status="completed",
            duration_seconds=200,
            products_scraped=10,
            started_at=datetime(2026, 2, 1, 10, 0, 0),
            completed_at=datetime(2026, 2, 1, 10, 3, 20),
        )
        self.session.add(run)
        self.session.flush()

        products = [
            Product(canonical_id="ext_jugo", basket_id="extended", name="Jugo", category="bebidas"),
            Product(canonical_id="ext_choclo", basket_id="extended", name="Choclo", category="conservas"),
        ]
        self.session.add_all(products)
        self.session.flush()
        self.session.add_all(
            [
                Price(
                    product_id=products[0].id,
                    run_id=run.id,
                    canonical_id="ext_jugo",
                    basket_id="extended",
                    product_name="Jugo",
                    current_price=100,
                    in_stock=True,
                    scraped_at=datetime(2026, 2, 10, 10, 0, 0),
                ),
                Price(
                    product_id=products[1].id,
                    run_id=run.id,
                    canonical_id="ext_choclo",
                    basket_id="extended",
                    product_name="Choclo",
                    current_price=120,
                    in_stock=True,
                    scraped_at=datetime(2026, 1, 20, 10, 0, 0),
                ),
            ]
        )
        self.session.commit()

    def test_balanced_plan_always_contains_cba(self):
        plan = build_scrape_plan(
            config=self.config,
            session=self.session,
            basket_type="all",
            profile="balanced",
            runtime_budget_minutes=20,
            rotation_items=2,
        )
        planned_ids = {row["id"] for row in plan.planned_items}
        self.assertIn("cba_arroz", planned_ids)
        self.assertIn("cba_fideos", planned_ids)
        self.assertIn("ext_harina", planned_ids)

    def test_rotation_is_deterministic_by_oldest_or_never_scraped(self):
        plan = build_scrape_plan(
            config=self.config,
            session=self.session,
            basket_type="all",
            profile="balanced",
            runtime_budget_minutes=20,
            rotation_items=2,
        )
        rotation_ids = [row["id"] for row in plan.planned_items if row.get("_plan_segment") == "daily_rotation"]
        # ext_arvejas never scraped -> first. ext_choclo older than ext_jugo -> second.
        self.assertGreaterEqual(len(rotation_ids), 2)
        self.assertEqual(rotation_ids[0], "ext_arvejas")
        self.assertEqual(rotation_ids[1], "ext_choclo")

    def test_budget_cut_keeps_mandatory_segments(self):
        plan = build_scrape_plan(
            config=self.config,
            session=self.session,
            basket_type="all",
            profile="balanced",
            runtime_budget_minutes=1,  # low budget
            rotation_items=3,
        )
        planned_ids = {row["id"] for row in plan.planned_items}
        self.assertIn("cba_arroz", planned_ids)
        self.assertIn("cba_fideos", planned_ids)
        self.assertIn("ext_harina", planned_ids)
        self.assertTrue(plan.plan_summary["mandatory_count"] <= len(plan.planned_items))

    def test_limit_smaller_than_mandatory_raises(self):
        with self.assertRaises(ValueError):
            build_scrape_plan(
                config=self.config,
                session=self.session,
                basket_type="all",
                profile="balanced",
                runtime_budget_minutes=20,
                rotation_items=2,
                limit=1,
            )


if __name__ == "__main__":
    unittest.main()
