"""Tests for interactive HTML report payload and empty-state behavior."""

import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.models import Price, Product, ScrapeRun, get_engine, get_session_factory, init_db
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
        self.assertIn("product_monthly_metrics", payload)
        self.assertIn("kpi_summary", payload)
        self.assertIn("quality_flags", payload)
        self.assertIn("scrape_quality", payload)
        self.assertIn("candidate_bands", payload)
        self.assertIn("candidate_band_summary", payload)
        self.assertEqual(payload["ui_version"], 2)
        self.assertIn("page_size", payload["ui_defaults"])
        self.assertIn("page_sizes", payload["filters_available"])

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


if __name__ == "__main__":
    unittest.main()
