"""Tests for official IPC sync (auto + fallback + idempotent upsert)."""

import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.ipc_official import INDECPatagoniaProvider, _reconcile_xls_vs_pdf, sync_official_cpi
from src.models import OfficialCPIMonthly, get_engine, get_session_factory, init_db


class TestOfficialIPCSync(unittest.TestCase):
    def setUp(self):
        self.tmp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp_db.close()
        self.tmp_csv = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        self.tmp_csv.close()

        Path(self.tmp_csv.name).write_text(
            "\n".join(
                [
                    "year_month,index_value,mom_change,yoy_change,metric_code,category_slug,status",
                    "2024-01,100.0,,,general,,final",
                    "2024-03,121.0,10.0,21.0,general,,final",
                ]
            ),
            encoding="utf-8",
        )

        self.config = {
            "storage": {
                "default_backend": "sqlite",
                "sqlite": {"database_path": self.tmp_db.name},
            },
            "analysis": {
                "ipc_official": {
                    "source_mode": "fallback",
                    "source_code": "indec_patagonia",
                    "region_default": "patagonia",
                    "region_scope": ["patagonia"],
                    "fallback_file": self.tmp_csv.name,
                    "auto_source": {
                        "url": str(Path(self.tmp_csv.name).with_suffix(".missing.csv")),
                        "format": "csv",
                        "timeout_seconds": 5,
                    },
                },
                "ipc_category_mapping": {"map": {"lacteos": "DIV03"}},
            },
        }

        self.engine = get_engine(self.config, "sqlite")
        init_db(self.engine)
        self.session = get_session_factory(self.engine)()

    def tearDown(self):
        self.session.close()
        self.engine.dispose()
        Path(self.tmp_db.name).unlink(missing_ok=True)
        Path(self.tmp_csv.name).unlink(missing_ok=True)

    def test_sync_fallback_and_idempotent_upsert(self):
        result1 = sync_official_cpi(config=self.config, session=self.session, region="patagonia")
        self.assertTrue(result1.used_fallback)
        self.assertEqual(result1.upserted_rows, 2)
        self.assertEqual(self.session.query(OfficialCPIMonthly).count(), 2)
        self.assertTrue(any("huecos mensuales" in w for w in result1.warnings))

        result2 = sync_official_cpi(config=self.config, session=self.session, region="patagonia")
        self.assertTrue(result2.used_fallback)
        self.assertEqual(result2.upserted_rows, 2)
        self.assertEqual(self.session.query(OfficialCPIMonthly).count(), 2)

    def test_discovery_extracts_pdf_and_xls_links(self):
        provider = INDECPatagoniaProvider(self.config)
        html = """
        <html><body>
          <a href="/uploads/informesdeprensa/ipc_02_261443D4406C.pdf">Leer informe</a>
          <a href="/ftp/cuadros/economia/sh_ipc_02_26.xls">Anexo XLS</a>
        </body></html>
        """
        with patch("src.ipc_official.requests.get") as mock_get:
            mock_resp = Mock()
            mock_resp.text = html
            mock_resp.raise_for_status = Mock()
            mock_get.return_value = mock_resp
            assets = provider.discover_assets()

        self.assertIn("pdf_url", assets)
        self.assertIn("xls_url", assets)
        self.assertTrue(assets["pdf_url"].endswith(".pdf"))
        self.assertTrue(assets["xls_url"].endswith(".xls"))

    def test_discovery_prefers_monthly_xls_over_auxiliary_files(self):
        provider = INDECPatagoniaProvider(self.config)
        html = """
        <html><body>
          <a href="/ftp/cuadros/economia/sh_ipc_aperturas.xls">Aperturas</a>
          <a href="/ftp/cuadros/economia/sh_ipc_01_26.xls">Mensual enero</a>
          <a href="/ftp/cuadros/economia/sh_ipc_precios_promedio.xls">Precios promedio</a>
          <a href="/ftp/cuadros/economia/sh_ipc_02_26.xls">Mensual febrero</a>
        </body></html>
        """
        with patch("src.ipc_official.requests.get") as mock_get:
            mock_resp = Mock()
            mock_resp.text = html
            mock_resp.raise_for_status = Mock()
            mock_get.return_value = mock_resp
            assets = provider.discover_assets()

        self.assertTrue(assets["xls_url"].endswith("sh_ipc_02_26.xls"))

    def test_parse_sheet_extracts_nacional_and_patagonia(self):
        provider = INDECPatagoniaProvider(self.config)
        df = pd.DataFrame(
            [
                ["Total nacional", pd.Timestamp("2026-01-01"), pd.Timestamp("2026-02-01")],
                [None, None, None],
                ["Nivel general y divisiones COICOP", None, None],
                [None, None, None],
                ["Nivel general", 2.9, 2.2],
                ["Alimentos y bebidas no alcohólicas", 3.1, 2.8],
                ["Categorías", None, None],
                ["Región Patagonia", pd.Timestamp("2026-01-01"), pd.Timestamp("2026-02-01")],
                [None, None, None],
                ["Nivel general y divisiones COICOP", None, None],
                [None, None, None],
                ["Nivel general", 2.9, 2.5],
                ["Alimentos y bebidas no alcohólicas", 3.4, 3.0],
                ["Categorías", None, None],
            ]
        )
        out = provider._parse_sheet_metric_values(df, "mom_change")
        regions = sorted(out["region"].unique().tolist())
        metrics = sorted(out["metric_code"].unique().tolist())

        self.assertEqual(regions, ["nacional", "patagonia"])
        self.assertIn("general", metrics)
        self.assertIn("alimentos_y_bebidas_no_alcoholicas", metrics)

    def test_parse_xls_bytes_preserves_general_and_bienes_servicios_varios(self):
        provider = INDECPatagoniaProvider(self.config)
        month = pd.Timestamp("2026-01-01")

        monthly_df = pd.DataFrame(
            [
                ["Total nacional", month],
                [None, None],
                ["Nivel general y divisiones COICOP", None],
                [None, None],
                ["Nivel general", 2.9],
                ["Bienes y servicios varios", 1.8],
                ["Categorias", None],
                ["Region Patagonia", month],
                [None, None],
                ["Nivel general y divisiones COICOP", None],
                [None, None],
                ["Nivel general", 2.7],
                ["Bienes y servicios varios", 1.5],
                ["Categorias", None],
            ]
        )
        yoy_df = pd.DataFrame(
            [
                ["Total nacional", month],
                [None, None],
                ["Nivel general y divisiones COICOP", None],
                [None, None],
                ["Nivel general", 20.0],
                ["Bienes y servicios varios", 18.0],
                ["Categorias", None],
                ["Region Patagonia", month],
                [None, None],
                ["Nivel general y divisiones COICOP", None],
                [None, None],
                ["Nivel general", 19.5],
                ["Bienes y servicios varios", 17.0],
                ["Categorias", None],
            ]
        )
        index_df = pd.DataFrame(
            [
                ["Total nacional", month],
                [None, None],
                ["Nivel general y divisiones COICOP", None],
                [None, None],
                ["Nivel general", 100.0],
                ["Bienes y servicios varios", 105.0],
                ["Categorias", None],
                ["Region Patagonia", month],
                [None, None],
                ["Nivel general y divisiones COICOP", None],
                [None, None],
                ["Nivel general", 100.0],
                ["Bienes y servicios varios", 103.0],
                ["Categorias", None],
            ]
        )

        sheet_monthly = "Variacion mensual IPC Nacional"
        sheet_yoy = "Var. interanual IPC Nacional"
        sheet_index = "Indices IPC Cobertura Nacional"

        def fake_read_excel(_blob, sheet_name=None, header=None):  # noqa: ANN001
            mapping = {
                sheet_monthly: monthly_df,
                sheet_yoy: yoy_df,
                sheet_index: index_df,
            }
            return mapping[sheet_name]

        fake_excel = Mock()
        fake_excel.sheet_names = [sheet_monthly, sheet_yoy, sheet_index]

        with patch("src.ipc_official.pd.ExcelFile", return_value=fake_excel), patch(
            "src.ipc_official.pd.read_excel",
            side_effect=fake_read_excel,
        ):
            out = provider.parse_xls_bytes(b"fake")

        self.assertIn("general", out["metric_code"].tolist())
        self.assertIn("bienes_y_servicios_varios", out["metric_code"].tolist())

        nat_general = out[(out["region"] == "nacional") & (out["metric_code"] == "general")].iloc[0]
        self.assertAlmostEqual(float(nat_general["mom_change"]), 2.9)
        self.assertAlmostEqual(float(nat_general["index_value"]), 100.0)

    def test_reconcile_warns_when_tolerance_exceeded(self):
        xls_df = pd.DataFrame(
            [
                {"region": "nacional", "year_month": "2026-01", "metric_code": "general", "mom_change": 2.9},
                {"region": "patagonia", "year_month": "2026-01", "metric_code": "general", "mom_change": 2.9},
            ]
        )
        pdf_df = pd.DataFrame(
            [
                {"region": "nacional", "year_month": "2026-01", "metric_code": "general", "mom_change": 2.1},
                {"region": "patagonia", "year_month": "2026-01", "metric_code": "general", "mom_change": 2.9},
            ]
        )
        result = _reconcile_xls_vs_pdf(xls_df, pdf_df, max_abs_diff_pp=0.10)
        self.assertEqual(result["status"], "warning")
        self.assertEqual(result["checked_month"], "2026-01")
        self.assertGreater(result["max_abs_diff_pp"], 0.10)


if __name__ == "__main__":
    unittest.main()
