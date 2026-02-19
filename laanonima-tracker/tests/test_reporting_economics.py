"""Economic calculation tests for interactive reporting."""

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.reporting import ReportGenerator


class TestReportingEconomics(unittest.TestCase):
    def setUp(self):
        self.config = {
            "storage": {"default_backend": "sqlite", "sqlite": {"database_path": ":memory:"}},
            "baskets": {
                "cba": {"items": [{"id": "p1", "category": "lacteos"}]},
                "extended": {"items": [{"id": "p2", "category": "bebidas"}]},
            },
        }
        self.generator = ReportGenerator(self.config)

    def tearDown(self):
        self.generator.close()

    def test_compute_real_prices_deflates_with_base_month(self):
        base_df = pd.DataFrame(
            [
                {
                    "canonical_id": "p1",
                    "product_name": "Leche",
                    "basket_id": "cba",
                    "current_price": 100.0,
                    "scraped_at": pd.Timestamp("2024-01-15"),
                    "month": "2024-01",
                    "category": "lacteos",
                    "product_size": "1 L",
                },
                {
                    "canonical_id": "p1",
                    "product_name": "Leche",
                    "basket_id": "cba",
                    "current_price": 120.0,
                    "scraped_at": pd.Timestamp("2024-02-15"),
                    "month": "2024-02",
                    "category": "lacteos",
                    "product_size": "1 L",
                },
            ]
        )
        ipc_df = pd.DataFrame(
            [
                {"year_month": "2024-01", "cpi_index": 100.0, "cpi_mom": 0.0, "cpi_yoy": 0.0},
                {"year_month": "2024-02", "cpi_index": 125.0, "cpi_mom": 25.0, "cpi_yoy": 25.0},
            ]
        )

        out, meta = self.generator._compute_real_prices(base_df, ipc_df, preferred_base_month="2024-01")
        feb_real = float(out.loc[out["month"] == "2024-02", "real_price"].iloc[0])

        self.assertAlmostEqual(feb_real, 96.0, places=4)
        self.assertEqual(meta["base_month"], "2024-01")

    def test_build_balanced_panel_uses_only_products_present_in_both_months(self):
        monthly_df = pd.DataFrame(
            [
                {"canonical_id": "p1", "month": "2024-01", "avg_price": 100.0, "avg_real_price": 100.0},
                {"canonical_id": "p1", "month": "2024-02", "avg_price": 120.0, "avg_real_price": 110.0},
                {"canonical_id": "p2", "month": "2024-02", "avg_price": 200.0, "avg_real_price": 190.0},
            ]
        )

        panel = self.generator._build_balanced_panel(monthly_df, from_month="2024-01", to_month="2024-02")
        self.assertEqual(set(panel["canonical_id"].tolist()), {"p1"})
        self.assertAlmostEqual(float(panel.iloc[0]["nominal_var_pct"]), 20.0)

    def test_compute_kpis_includes_gap_vs_ipc(self):
        monthly_df = pd.DataFrame(
            [
                {"canonical_id": "p1", "month": "2024-01", "avg_price": 100.0, "avg_real_price": 100.0},
                {"canonical_id": "p1", "month": "2024-02", "avg_price": 120.0, "avg_real_price": 109.09},
                {"canonical_id": "p2", "month": "2024-01", "avg_price": 100.0, "avg_real_price": 100.0},
                {"canonical_id": "p2", "month": "2024-02", "avg_price": 130.0, "avg_real_price": 118.18},
            ]
        )
        panel = self.generator._build_balanced_panel(monthly_df, from_month="2024-01", to_month="2024-02")
        ipc_df = pd.DataFrame(
            [
                {"year_month": "2024-01", "cpi_index": 100.0, "cpi_mom": 0.0, "cpi_yoy": 0.0},
                {"year_month": "2024-02", "cpi_index": 110.0, "cpi_mom": 10.0, "cpi_yoy": 10.0},
            ]
        )

        kpis = self.generator._compute_economic_kpis(monthly_df, ipc_df, panel, "2024-01", "2024-02")
        self.assertAlmostEqual(kpis["inflation_basket_nominal_pct"], 25.0, places=4)
        self.assertAlmostEqual(kpis["ipc_period_pct"], 10.0, places=4)
        self.assertAlmostEqual(kpis["gap_vs_ipc_pp"], 15.0, places=4)

    def test_compute_kpis_uses_fallback_window_when_requested_window_is_empty(self):
        monthly_df = pd.DataFrame(
            [
                {"canonical_id": "p1", "month": "2024-03", "avg_price": 100.0, "avg_real_price": 100.0},
                {"canonical_id": "p1", "month": "2024-04", "avg_price": 120.0, "avg_real_price": 110.0},
                {"canonical_id": "p2", "month": "2024-03", "avg_price": 200.0, "avg_real_price": 200.0},
                {"canonical_id": "p2", "month": "2024-04", "avg_price": 220.0, "avg_real_price": 210.0},
            ]
        )
        ipc_df = pd.DataFrame(
            [
                {"year_month": "2024-03", "cpi_index": 100.0, "cpi_mom": 0.0, "cpi_yoy": 0.0},
                {"year_month": "2024-04", "cpi_index": 105.0, "cpi_mom": 5.0, "cpi_yoy": 5.0},
            ]
        )
        empty_panel = self.generator._build_balanced_panel(monthly_df, from_month="2024-01", to_month="2024-02")

        kpis = self.generator._compute_economic_kpis(monthly_df, ipc_df, empty_panel, "2024-01", "2024-02")
        self.assertTrue(kpis["kpi_fallback_used"])
        self.assertEqual(kpis["from_month"], "2024-03")
        self.assertEqual(kpis["to_month"], "2024-04")
        self.assertEqual(kpis["requested_from_month"], "2024-01")
        self.assertEqual(kpis["requested_to_month"], "2024-02")

    def test_compute_kpis_single_month_fallback_returns_zero_change(self):
        monthly_df = pd.DataFrame(
            [
                {"canonical_id": "p1", "month": "2024-03", "avg_price": 100.0, "avg_real_price": 98.0},
                {"canonical_id": "p2", "month": "2024-03", "avg_price": 120.0, "avg_real_price": 117.0},
            ]
        )
        ipc_df = pd.DataFrame(
            [
                {"year_month": "2024-03", "cpi_index": 100.0, "cpi_mom": 0.0, "cpi_yoy": 0.0},
            ]
        )
        empty_panel = self.generator._build_balanced_panel(monthly_df, from_month="2024-01", to_month="2024-02")

        kpis = self.generator._compute_economic_kpis(monthly_df, ipc_df, empty_panel, "2024-01", "2024-02")
        self.assertTrue(kpis["kpi_fallback_used"])
        self.assertEqual(kpis["from_month"], "2024-03")
        self.assertEqual(kpis["to_month"], "2024-03")
        self.assertAlmostEqual(kpis["inflation_basket_nominal_pct"], 0.0)
        self.assertAlmostEqual(kpis["inflation_basket_real_pct"], 0.0)

    def test_payload_supports_nominal_and_real_variation_against_base_month(self):
        df = pd.DataFrame(
            [
                {
                    "canonical_id": "p1",
                    "product_name": "Leche",
                    "basket_id": "cba",
                    "current_price": 100.0,
                    "scraped_at": pd.Timestamp("2024-01-05T10:00:00"),
                    "category": "lacteos",
                    "product_url": "https://example.com/p1",
                    "product_size": "1 L",
                    "month": "2024-01",
                },
                {
                    "canonical_id": "p1",
                    "product_name": "Leche",
                    "basket_id": "cba",
                    "current_price": 120.0,
                    "scraped_at": pd.Timestamp("2024-02-05T10:00:00"),
                    "category": "lacteos",
                    "product_url": "https://example.com/p1",
                    "product_size": "1 L",
                    "month": "2024-02",
                },
            ]
        )
        ipc_df = pd.DataFrame(
            [
                {"year_month": "2024-01", "cpi_index": 100.0, "cpi_mom": 0.0, "cpi_yoy": 0.0},
                {"year_month": "2024-02", "cpi_index": 120.0, "cpi_mom": 20.0, "cpi_yoy": 20.0},
            ]
        )

        with patch.object(self.generator, "_load_ipc_data", return_value=ipc_df):
            payload = self.generator._build_interactive_payload(
                df=df,
                from_month="2024-01",
                to_month="2024-02",
                basket_type="all",
            )

        snap = payload["snapshot"][0]
        ref_map = {r["month"]: r for r in payload["monthly_reference"]}
        nominal_var = ((snap["current_price"] - ref_map["2024-01"]["avg_price"]) / ref_map["2024-01"]["avg_price"]) * 100
        real_var = (
            (snap["current_real_price"] - ref_map["2024-01"]["avg_real_price"]) / ref_map["2024-01"]["avg_real_price"]
        ) * 100

        self.assertAlmostEqual(nominal_var, 20.0, places=4)
        self.assertAlmostEqual(real_var, 0.0, places=4)


if __name__ == "__main__":
    unittest.main()
