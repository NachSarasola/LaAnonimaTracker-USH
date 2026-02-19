"""Tests for tracker IPC build and publish pipeline orchestration."""

import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.ipc_pipeline import publish_ipc
from src.ipc_tracker import TrackerIPCBuilder
from src.models import (
    IPCPublicationRun,
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


class TestTrackerIPCPipeline(unittest.TestCase):
    def setUp(self):
        self.tmp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp_db.close()
        self.tmp_official_csv = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        self.tmp_official_csv.close()

        Path(self.tmp_official_csv.name).write_text(
            "\n".join(
                [
                    "year_month,index_value,mom_change,yoy_change,metric_code,category_slug,status",
                    "2024-01,100.0,,,general,,final",
                    "2024-02,110.0,10.0,10.0,general,,final",
                    "2024-01,100.0,,,DIV03,lacteos,final",
                    "2024-02,108.0,8.0,8.0,DIV03,lacteos,final",
                ]
            ),
            encoding="utf-8",
        )

        self.config = {
            "storage": {
                "default_backend": "sqlite",
                "sqlite": {"database_path": self.tmp_db.name},
            },
            "baskets": {
                "cba": {
                    "items": [
                        {"id": "p1", "quantity": 1.0, "category": "lacteos"},
                        {"id": "p2", "quantity": 2.0, "category": "almacen"},
                    ]
                },
                "extended": {"items": []},
            },
            "analysis": {
                "ipc_tracker": {
                    "method_version": "v1_fixed_weight_robust_monthly",
                    "monthly_aggregation": "winsorized_mean",
                    "winsor_limits": [0.1, 0.9],
                    "min_obs_per_product_month": 1,
                    "coverage_min_weight_pct": 0.5,
                    "provisional_freeze_days": 7,
                },
                "ipc_official": {
                    "source_mode": "fallback",
                    "source_code": "indec_patagonia",
                    "region_default": "patagonia",
                    "fallback_file": self.tmp_official_csv.name,
                    "auto_source": {"url": self.tmp_official_csv.name, "format": "csv"},
                },
                "ipc_category_mapping": {"map": {"lacteos": "DIV03", "almacen": "DIV01"}},
            },
        }

        self.engine = get_engine(self.config, "sqlite")
        init_db(self.engine)
        self.session = get_session_factory(self.engine)()
        self._seed_prices()

    def tearDown(self):
        self.session.close()
        self.engine.dispose()
        Path(self.tmp_db.name).unlink(missing_ok=True)
        Path(self.tmp_official_csv.name).unlink(missing_ok=True)

    def _seed_prices(self):
        run = ScrapeRun(
            run_uuid="77777777-7777-7777-7777-777777777777",
            branch_id="75",
            branch_name="USHUAIA",
            postal_code="9410",
            basket_type="cba",
        )
        self.session.add(run)
        self.session.flush()

        p1 = Product(canonical_id="p1", basket_id="cba", name="Leche", category="lacteos")
        p2 = Product(canonical_id="p2", basket_id="cba", name="Pan", category="almacen")
        self.session.add_all([p1, p2])
        self.session.flush()

        self.session.add_all(
            [
                Price(
                    product_id=p1.id,
                    run_id=run.id,
                    canonical_id="p1",
                    basket_id="cba",
                    product_name="Leche",
                    current_price=100,
                    scraped_at=datetime(2024, 1, 10, 10, 0, 0),
                ),
                Price(
                    product_id=p2.id,
                    run_id=run.id,
                    canonical_id="p2",
                    basket_id="cba",
                    product_name="Pan",
                    current_price=50,
                    scraped_at=datetime(2024, 1, 10, 10, 0, 0),
                ),
                Price(
                    product_id=p1.id,
                    run_id=run.id,
                    canonical_id="p1",
                    basket_id="cba",
                    product_name="Leche",
                    current_price=120,
                    scraped_at=datetime(2024, 2, 10, 10, 0, 0),
                ),
                Price(
                    product_id=p2.id,
                    run_id=run.id,
                    canonical_id="p2",
                    basket_id="cba",
                    product_name="Pan",
                    current_price=55,
                    scraped_at=datetime(2024, 2, 10, 10, 0, 0),
                ),
            ]
        )
        self.session.commit()

    def test_tracker_build_persists_general_and_categories(self):
        builder = TrackerIPCBuilder(config=self.config, session=self.session)
        result = builder.build(basket_type="all", from_month="2024-01", to_month="2024-02")
        builder.close()

        self.assertEqual(result.general_rows, 2)
        self.assertGreaterEqual(result.category_rows, 2)

        general = (
            self.session.query(TrackerIPCMonthly)
            .filter(TrackerIPCMonthly.basket_type == "all")
            .order_by(TrackerIPCMonthly.year_month.asc())
            .all()
        )
        self.assertEqual(len(general), 2)
        self.assertAlmostEqual(float(general[0].index_value), 100.0, places=4)
        self.assertGreater(float(general[1].index_value), 100.0)
        self.assertEqual(general[1].status, "final")
        self.assertGreaterEqual(self.session.query(TrackerIPCCategoryMonthly).count(), 2)

    def test_publish_pipeline_writes_audit_and_overlap_metrics(self):
        summary = publish_ipc(
            config=self.config,
            session=self.session,
            basket_type="all",
            from_month="2024-01",
            to_month="2024-02",
            region="patagonia",
        )

        self.assertIn(summary.status, {"completed", "completed_with_warnings"})
        self.assertGreater(summary.official_rows, 0)
        self.assertGreater(summary.tracker_rows, 0)
        self.assertGreaterEqual(summary.overlap_months, 1)
        self.assertGreaterEqual(self.session.query(OfficialCPIMonthly).count(), 2)
        self.assertEqual(self.session.query(IPCPublicationRun).count(), 1)


if __name__ == "__main__":
    unittest.main()
