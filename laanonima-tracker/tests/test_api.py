"""API and repository integration tests."""

import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.api import app, get_session
from src.models import CategoryIndex, IndexQualityAudit, Price, Product, ScrapeRun, get_engine, get_session_factory, init_db


class TestAPI(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()

        config = {
            "storage": {
                "default_backend": "sqlite",
                "sqlite": {"database_path": self.tmp.name},
            }
        }
        engine = get_engine(config, "sqlite")
        init_db(engine)
        Session = get_session_factory(engine)

        self.session = Session()
        self._seed_data(self.session)

        def override_session():
            test_session = Session()
            try:
                yield test_session
            finally:
                test_session.close()

        app.dependency_overrides[get_session] = override_session
        self.client = TestClient(app)

    def tearDown(self):
        app.dependency_overrides.clear()
        self.session.close()
        Path(self.tmp.name).unlink(missing_ok=True)

    def _seed_data(self, session):
        run = ScrapeRun(
            run_uuid="run-1",
            branch_id="75",
            branch_name="USHUAIA",
            postal_code="9410",
            basket_type="cba",
        )
        session.add(run)
        session.flush()

        leche = Product(canonical_id="prod_1", basket_id="cba", name="Leche", category="lacteos")
        pan = Product(canonical_id="prod_2", basket_id="cba", name="Pan", category="almacen")
        session.add_all([leche, pan])
        session.flush()

        session.add_all(
            [
                Price(
                    product_id=leche.id,
                    run_id=run.id,
                    canonical_id="prod_1",
                    basket_id="cba",
                    product_name="Leche",
                    current_price=100,
                    scraped_at=datetime(2024, 1, 10, 10, 0, 0),
                ),
                Price(
                    product_id=pan.id,
                    run_id=run.id,
                    canonical_id="prod_2",
                    basket_id="cba",
                    product_name="Pan",
                    current_price=50,
                    scraped_at=datetime(2024, 1, 11, 10, 0, 0),
                ),
            ]
        )

        session.add(
            CategoryIndex(
                basket_type="cba",
                category="lacteos",
                year_month="2024-01",
                index_value=100,
                mom_change=None,
                yoy_change=None,
                products_included=1,
                products_missing=0,
            )
        )
        session.add(
            IndexQualityAudit(
                basket_type="cba",
                category="lacteos",
                year_month="2024-01",
                coverage_rate=0.6,
                outlier_count=2,
                missing_count=1,
                min_coverage_required=0.7,
                is_coverage_sufficient=False,
            )
        )
        session.commit()

    def test_series_producto_with_pagination(self):
        response = self.client.get("/series/producto", params={"page": 1, "page_size": 1})
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["pagination"]["total"], 2)
        self.assertEqual(len(payload["items"]), 1)

    def test_series_categoria_not_found(self):
        response = self.client.get("/series/categoria", params={"category": "no_existe"})
        self.assertEqual(response.status_code, 404)

    def test_series_producto_invalid_range(self):
        response = self.client.get(
            "/series/producto",
            params={"from": "2024-02-01", "to": "2024-01-01"},
        )
        self.assertEqual(response.status_code, 400)

    def test_ipc_categorias(self):
        response = self.client.get("/ipc/categorias", params={"from": "2024-01", "to": "2024-01"})
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["pagination"]["total"], 1)
        self.assertEqual(payload["items"][0]["category"], "lacteos")
        self.assertTrue(payload["items"][0]["coverage_warning"])
        self.assertEqual(payload["items"][0]["outlier_count"], 2)


if __name__ == "__main__":
    unittest.main()
