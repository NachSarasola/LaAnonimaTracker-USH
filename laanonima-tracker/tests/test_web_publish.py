"""Tests for static web publication pipeline."""

import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from src.web_publish import StaticWebPublisher


class TestWebPublish(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        root = Path(self.tmp_dir.name)
        self.output_dir = root / "public"
        self.report_dir = root / "reports"
        self.report_dir.mkdir(parents=True, exist_ok=True)

        self.config = {
            "deployment": {
                "output_dir": str(self.output_dir),
                "public_base_url": "https://example.com",
                "keep_history_months": 24,
                "fresh_max_hours": 36,
                "schedule_utc": "09:10",
            },
            "ads": {
                "enabled": True,
                "provider": "adsense_placeholder",
                "slots": ["header", "inline"],
                "client_id_placeholder": "ca-pub-test",
            },
            "premium_placeholders": {
                "enabled": True,
                "features": ["Alertas", "CSV Pro"],
            },
        }

    def tearDown(self):
        self.tmp_dir.cleanup()

    def _write_report(self, from_month: str, to_month: str, generated_at: datetime) -> tuple[Path, Path]:
        stamp = generated_at.strftime("%Y%m%d_%H%M%S")
        base = f"report_interactive_{from_month.replace('-', '')}_to_{to_month.replace('-', '')}_{stamp}"
        html_path = self.report_dir / f"{base}.html"
        metadata_path = self.report_dir / f"{base}.metadata.json"

        html_path.write_text("<html><body>report</body></html>", encoding="utf-8")
        metadata = {
            "generated_at": generated_at.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "range": {"from": from_month, "to": to_month},
            "coverage": {"coverage_total_pct": 88.5},
            "data_quality": {
                "publication_status": {
                    "status": "completed",
                    "metrics": {"official_validation_status": "ok"},
                },
                "quality_flags": {"is_partial": False},
            },
            "kpis": {
                "inflation_basket_nominal_pct": 10.2,
                "ipc_period_pct": 8.9,
                "gap_vs_ipc_pp": 1.3,
                "balanced_panel_n": 25,
            },
            "artifacts": {"html": str(html_path), "pdf": None},
        }
        metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
        return html_path, metadata_path

    def test_publish_builds_public_tree_and_manifest(self):
        now = datetime.now(timezone.utc)
        latest_html, latest_meta = self._write_report("2026-01", "2026-02", now)
        self._write_report("2025-12", "2026-01", now - timedelta(days=30))

        publisher = StaticWebPublisher(self.config)
        publisher.report_dir = self.report_dir
        result = publisher.publish(preferred_html=str(latest_html), preferred_metadata=str(latest_meta))

        self.assertEqual(result.status, "completed")
        self.assertTrue((self.output_dir / "tracker" / "index.html").exists())
        self.assertTrue((self.output_dir / "historico" / "index.html").exists())
        self.assertTrue((self.output_dir / "data" / "manifest.json").exists())
        self.assertTrue((self.output_dir / "_headers").exists())
        self.assertTrue((self.output_dir / "_redirects").exists())
        self.assertTrue((self.output_dir / "404.html").exists())
        self.assertTrue((self.output_dir / "favicon.svg").exists())
        self.assertTrue((self.output_dir / "site.webmanifest").exists())
        self.assertTrue((self.output_dir / "assets" / "og-card.svg").exists())
        self.assertTrue((self.output_dir / "legal" / "privacy.html").exists())
        self.assertTrue((self.output_dir / "legal" / "terms.html").exists())
        historico_html = (self.output_dir / "historico" / "index.html").read_text(encoding="utf-8")
        home_html = (self.output_dir / "index.html").read_text(encoding="utf-8")
        self.assertIn("id='history-search'", historico_html)
        self.assertIn("id='history-count'", historico_html)
        self.assertIn("property='og:title'", home_html)
        self.assertIn("link rel='manifest' href='/site.webmanifest'", home_html)

        manifest = json.loads((self.output_dir / "data" / "manifest.json").read_text(encoding="utf-8"))
        self.assertEqual(manifest["status"], "fresh")
        self.assertEqual(manifest["latest_report_path"], "/tracker/")
        self.assertGreaterEqual(len(manifest["history"]), 2)
        self.assertTrue(manifest["ads"]["enabled"])
        self.assertTrue(manifest["premium_placeholders"]["enabled"])

        latest_meta_public = json.loads((self.output_dir / "data" / "latest.metadata.json").read_text(encoding="utf-8"))
        self.assertIn("web_status", latest_meta_public)
        self.assertIn("is_stale", latest_meta_public)
        self.assertIn("next_update_eta", latest_meta_public)
        ads_txt = (self.output_dir / "ads.txt").read_text(encoding="utf-8")
        self.assertIn("ca-pub-test", ads_txt)

    def test_publish_marks_stale_when_report_is_old(self):
        old_time = datetime.now(timezone.utc) - timedelta(hours=60)
        html_path, metadata_path = self._write_report("2025-11", "2025-12", old_time)

        publisher = StaticWebPublisher(self.config)
        publisher.report_dir = self.report_dir
        result = publisher.publish(preferred_html=str(html_path), preferred_metadata=str(metadata_path))

        self.assertTrue(result.is_stale)
        self.assertEqual(result.web_status, "stale")


if __name__ == "__main__":
    unittest.main()
