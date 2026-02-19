"""API and repository integration tests."""

import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.api import app, get_session
from src.models import (
    CategoryIndex,
    IPCPublicationRun,
    IndexQualityAudit,
    OfficialCPIMonthly,
    Price,
    Product,
    ScrapeRun,
    TrackerIPCCategoryMonthly,
    TrackerIPCMonthly,
    get_engine,
    get_session_factory,
    init_db,
)


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
        self.engine = get_engine(config, "sqlite")
        init_db(self.engine)
        Session = get_session_factory(self.engine)

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
        if hasattr(self, "client"):
            self.client.close()
        if hasattr(self, "session"):
            self.session.close()
        if hasattr(self, "engine"):
            self.engine.dispose()
        try:
            Path(self.tmp.name).unlink(missing_ok=True)
        except PermissionError:
            pass

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
        session.add(
            TrackerIPCMonthly(
                basket_type="all",
                year_month="2024-01",
                method_version="v1_fixed_weight_robust_monthly",
                status="final",
                index_value=100,
                mom_change=None,
                yoy_change=None,
                coverage_weight_pct=1.0,
                coverage_product_pct=1.0,
                products_expected=2,
                products_observed=2,
                products_with_relative=0,
                outlier_count=0,
                missing_products=0,
                base_month="2024-01",
            )
        )
        session.add(
            TrackerIPCCategoryMonthly(
                basket_type="all",
                category_slug="lacteos",
                indec_division_code="DIV03",
                year_month="2024-01",
                method_version="v1_fixed_weight_robust_monthly",
                status="final",
                index_value=100,
                mom_change=None,
                yoy_change=None,
                coverage_weight_pct=1.0,
                coverage_product_pct=1.0,
                products_expected=1,
                products_observed=1,
                products_with_relative=0,
                outlier_count=0,
                missing_products=0,
                base_month="2024-01",
            )
        )
        session.add(
            OfficialCPIMonthly(
                source="indec_patagonia",
                region="patagonia",
                metric_code="general",
                category_slug=None,
                year_month="2024-01",
                index_value=100,
                mom_change=None,
                yoy_change=None,
                status="final",
                is_fallback=False,
            )
        )
        session.add(
            OfficialCPIMonthly(
                source="indec_patagonia",
                region="patagonia",
                metric_code="DIV03",
                category_slug="lacteos",
                year_month="2024-01",
                index_value=100,
                mom_change=None,
                yoy_change=None,
                status="final",
                is_fallback=False,
            )
        )
        session.add(
            IPCPublicationRun(
                run_uuid="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                basket_type="all",
                region="patagonia",
                method_version="v1_fixed_weight_robust_monthly",
                from_month="2024-01",
                to_month="2024-01",
                status="completed",
                official_source="indec_patagonia",
                official_rows=2,
                tracker_rows=1,
                tracker_category_rows=1,
                overlap_months=1,
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

    def test_ipc_tracker(self):
        response = self.client.get("/ipc/tracker", params={"basket": "all", "from": "2024-01", "to": "2024-01"})
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["pagination"]["total"], 1)
        self.assertEqual(payload["items"][0]["year_month"], "2024-01")
        self.assertEqual(payload["items"][0]["status"], "final")

    def test_ipc_oficial_patagonia(self):
        response = self.client.get("/ipc/oficial/patagonia", params={"from": "2024-01", "to": "2024-01"})
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["pagination"]["total"], 1)
        self.assertEqual(payload["items"][0]["metric_code"], "general")
        self.assertIn("meta", payload)
        self.assertEqual(payload["meta"]["region"], "patagonia")

    def test_ipc_oficial_region(self):
        response = self.client.get("/ipc/oficial", params={"region": "patagonia", "from": "2024-01", "to": "2024-01"})
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["pagination"]["total"], 1)
        self.assertEqual(payload["meta"]["region"], "patagonia")

    def test_ipc_comparacion_general(self):
        response = self.client.get("/ipc/comparacion", params={"basket": "all", "from": "2024-01", "to": "2024-01"})
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["pagination"]["total"], 1)
        self.assertTrue(payload["items"][0]["is_overlap"])

    def test_ipc_comparacion_categorias(self):
        response = self.client.get(
            "/ipc/comparacion/categorias",
            params={"basket": "all", "from": "2024-01", "to": "2024-01", "category": "lacteos"},
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["pagination"]["total"], 1)
        self.assertEqual(payload["items"][0]["category_slug"], "lacteos")

    def test_ipc_publicacion_latest(self):
        response = self.client.get("/ipc/publicacion/latest", params={"basket": "all"})
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIsNotNone(payload["item"])
        self.assertEqual(payload["item"]["status"], "completed")


if __name__ == "__main__":
    unittest.main()
