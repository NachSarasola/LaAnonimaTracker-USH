"""Tests for static web publication pipeline."""

import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

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
                "provider": "adsense",
                "slots": ["header", "inline"],
                "client_id": "ca-pub-1234567890123456",
            },
            "premium_placeholders": {
                "enabled": True,
                "features": ["Alertas", "CSV Pro"],
            },
        }

    def tearDown(self):
        self.tmp_dir.cleanup()

    def _write_report(
        self,
        from_month: str,
        to_month: str,
        generated_at: datetime,
        has_data: bool = True,
    ) -> tuple[Path, Path]:
        stamp = generated_at.strftime("%Y%m%d_%H%M%S")
        base = f"report_interactive_{from_month.replace('-', '')}_to_{to_month.replace('-', '')}_{stamp}"
        html_path = self.report_dir / f"{base}.html"
        metadata_path = self.report_dir / f"{base}.metadata.json"

        html_path.write_text("<html><body>report</body></html>", encoding="utf-8")
        metadata = {
            "generated_at": generated_at.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "range": {"from": from_month, "to": to_month},
            "has_data": has_data,
            "coverage": {"coverage_total_pct": 88.5},
            "data_quality": {
                "publication_status": {
                    "status": "completed",
                    "metrics": {"official_validation_status": "ok"},
                },
                "quality_flags": {"is_partial": False, "warnings": []},
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
        self.assertTrue((self.output_dir / "legal" / "cookies.html").exists())
        self.assertTrue((self.output_dir / "legal" / "ads.html").exists())
        self.assertTrue((self.output_dir / "contacto" / "index.html").exists())
        historico_html = (self.output_dir / "historico" / "index.html").read_text(encoding="utf-8")
        home_html = (self.output_dir / "index.html").read_text(encoding="utf-8")
        contact_html = (self.output_dir / "contacto" / "index.html").read_text(encoding="utf-8")
        self.assertIn("id='history-search'", historico_html)
        self.assertIn("id='history-count'", historico_html)
        self.assertIn("property='og:title'", home_html)
        self.assertIn("link rel='manifest' href='/site.webmanifest'", home_html)
        self.assertIn("mailto:", contact_html)
        redirects_txt = (self.output_dir / "_redirects").read_text(encoding="utf-8")
        self.assertIn("https://www.example.com/* https://example.com/:splat 301!", redirects_txt)

        manifest = json.loads((self.output_dir / "data" / "manifest.json").read_text(encoding="utf-8"))
        self.assertEqual(manifest["status"], "fresh")
        self.assertEqual(manifest["latest_report_path"], "/tracker/")
        self.assertGreaterEqual(len(manifest["history"]), 2)
        self.assertEqual(manifest["version"], 2)
        self.assertTrue(manifest["ads"]["enabled"])
        self.assertTrue(manifest["premium_placeholders"]["enabled"])
        self.assertEqual(manifest["latest"]["from_month"], "2026-01")
        self.assertEqual(manifest["latest"]["to_month"], "2026-02")
        self.assertTrue(manifest["latest"]["has_data"])
        self.assertEqual(manifest["latest"]["web_status"], "fresh")
        self.assertEqual(manifest["publication_policy"], "publish_with_alert_on_partial")
        self.assertIn("publication_policy_summary", manifest)
        self.assertIn("run_key", manifest["history"][0])
        self.assertIn("run_date_local", manifest["history"][0])
        self.assertIn("generated_at_utc", manifest["history"][0])
        self.assertTrue(str(manifest["history"][0]["report_path"]).startswith("/historico/"))

        latest_meta_public = json.loads((self.output_dir / "data" / "latest.metadata.json").read_text(encoding="utf-8"))
        self.assertEqual(latest_meta_public.get("from_month"), "2026-01")
        self.assertEqual(latest_meta_public.get("to_month"), "2026-02")
        self.assertEqual(latest_meta_public.get("generated_at"), manifest["latest"]["generated_at"])
        self.assertTrue(latest_meta_public.get("has_data"))
        self.assertEqual(latest_meta_public.get("web_status"), "fresh")
        self.assertIn("web_status", latest_meta_public)
        self.assertIn("is_stale", latest_meta_public)
        self.assertIn("next_update_eta", latest_meta_public)
        self.assertIn("latest_range_label", latest_meta_public)
        self.assertIn("quality_warnings", latest_meta_public)
        self.assertEqual(latest_meta_public.get("publication_policy"), "publish_with_alert_on_partial")
        self.assertIn("publication_policy_summary", latest_meta_public)
        ads_txt = (self.output_dir / "ads.txt").read_text(encoding="utf-8")
        self.assertIn("ca-pub-1234567890123456", ads_txt)

    def test_publish_marks_stale_when_report_is_old(self):
        old_time = datetime.now(timezone.utc) - timedelta(hours=60)
        html_path, metadata_path = self._write_report("2025-11", "2025-12", old_time)

        publisher = StaticWebPublisher(self.config)
        publisher.report_dir = self.report_dir
        result = publisher.publish(preferred_html=str(html_path), preferred_metadata=str(metadata_path))

        self.assertTrue(result.is_stale)
        self.assertEqual(result.web_status, "stale")

    def test_collect_latest_report_prefers_has_data_when_no_range(self):
        now = datetime.now(timezone.utc)
        self._write_report("2026-01", "2026-02", now - timedelta(minutes=10), has_data=True)
        self._write_report("2025-12", "2026-01", now, has_data=False)

        publisher = StaticWebPublisher(self.config)
        publisher.report_dir = self.report_dir
        chosen = publisher.collect_latest_report()

        self.assertEqual(chosen.metadata.get("range", {}).get("from"), "2026-01")
        self.assertEqual(chosen.metadata.get("range", {}).get("to"), "2026-02")
        self.assertTrue(chosen.metadata.get("has_data"))

    def test_collect_latest_report_with_exact_range(self):
        now = datetime.now(timezone.utc)
        self._write_report("2025-12", "2026-01", now - timedelta(days=1), has_data=True)
        self._write_report("2026-01", "2026-02", now - timedelta(hours=2), has_data=True)
        self._write_report("2026-01", "2026-02", now - timedelta(hours=1), has_data=True)

        publisher = StaticWebPublisher(self.config)
        publisher.report_dir = self.report_dir
        chosen = publisher.collect_latest_report(
            preferred_from_month="2026-01",
            preferred_to_month="2026-02",
        )

        self.assertEqual(chosen.metadata.get("range", {}).get("from"), "2026-01")
        self.assertEqual(chosen.metadata.get("range", {}).get("to"), "2026-02")
        self.assertEqual(chosen.generated_at.strftime("%Y%m%d%H%M"), (now - timedelta(hours=1)).strftime("%Y%m%d%H%M"))

    def test_collect_latest_report_with_missing_range_raises(self):
        now = datetime.now(timezone.utc)
        self._write_report("2026-01", "2026-02", now, has_data=True)

        publisher = StaticWebPublisher(self.config)
        publisher.report_dir = self.report_dir

        with self.assertRaises(FileNotFoundError):
            publisher.collect_latest_report(
                preferred_from_month="2027-01",
                preferred_to_month="2027-02",
            )

    def test_web_publish_shell_pages_use_stylesheet_link(self):
        now = datetime.now(timezone.utc)
        html_path, metadata_path = self._write_report("2026-01", "2026-02", now)
        publisher = StaticWebPublisher(self.config)
        publisher.report_dir = self.report_dir
        publisher.publish(preferred_html=str(html_path), preferred_metadata=str(metadata_path))

        home_html = (self.output_dir / "index.html").read_text(encoding="utf-8")
        historico_html = (self.output_dir / "historico" / "index.html").read_text(encoding="utf-8")
        contacto_html = (self.output_dir / "contacto" / "index.html").read_text(encoding="utf-8")
        shell_css = (self.output_dir / "assets" / "css" / "shell-ui.css").read_text(encoding="utf-8")

        self.assertIn("href='/assets/css/shell-ui.css?v=", home_html)
        self.assertIn("href='/assets/css/shell-ui.css?v=", historico_html)
        self.assertIn("href='/assets/css/shell-ui.css?v=", contacto_html)
        self.assertNotIn("<style>", home_html)
        self.assertGreater(len(shell_css.strip()), 1000)

    def test_headers_include_assets_css_cache_rule(self):
        now = datetime.now(timezone.utc)
        html_path, metadata_path = self._write_report("2026-01", "2026-02", now)
        publisher = StaticWebPublisher(self.config)
        publisher.report_dir = self.report_dir
        publisher.publish(preferred_html=str(html_path), preferred_metadata=str(metadata_path))

        headers = (self.output_dir / "_headers").read_text(encoding="utf-8")
        self.assertIn("/assets/css/*", headers)
        self.assertIn("max-age=31536000, immutable", headers)

    def test_web_publish_copies_tracker_css_companion(self):
        now = datetime.now(timezone.utc)
        html_path, metadata_path = self._write_report("2026-01", "2026-02", now)
        tracker_css_source = self.report_dir / "tracker-ui.css"
        tracker_css_source.write_text("body{background:#fff;}", encoding="utf-8")

        publisher = StaticWebPublisher(self.config)
        publisher.report_dir = self.report_dir
        publisher.publish(preferred_html=str(html_path), preferred_metadata=str(metadata_path))

        tracker_css = self.output_dir / "tracker" / "tracker-ui.css"
        month_run_css = list((self.output_dir / "historico" / "2026-02").glob("*/tracker-ui.css"))

        self.assertTrue(tracker_css.exists())
        self.assertTrue(month_run_css)
        self.assertEqual(tracker_css.read_text(encoding="utf-8"), "body{background:#fff;}")

    def test_web_publish_writes_tracker_css_fallback_when_companion_missing(self):
        now = datetime.now(timezone.utc)
        html_path, metadata_path = self._write_report("2026-01", "2026-02", now)
        html_path.write_text(
            "<!doctype html><html><head><link rel=\"stylesheet\" href=\"./tracker-ui.css\"/></head><body>report</body></html>",
            encoding="utf-8",
        )

        publisher = StaticWebPublisher(self.config)
        publisher.report_dir = self.report_dir
        publisher.publish(preferred_html=str(html_path), preferred_metadata=str(metadata_path))

        tracker_css = self.output_dir / "tracker" / "tracker-ui.css"
        month_run_css = list((self.output_dir / "historico" / "2026-02").glob("*/tracker-ui.css"))
        self.assertTrue(tracker_css.exists())
        self.assertTrue(month_run_css)
        self.assertGreater(len(tracker_css.read_text(encoding="utf-8").strip()), 1000)

    def test_web_publish_detects_tracker_css_link_with_query_string(self):
        now = datetime.now(timezone.utc)
        html_path, metadata_path = self._write_report("2026-01", "2026-02", now)
        html_path.write_text(
            "<!doctype html><html><head><link rel=\"stylesheet\" href=\"./tracker-ui.css?v=123\"/></head><body>report</body></html>",
            encoding="utf-8",
        )

        publisher = StaticWebPublisher(self.config)
        publisher.report_dir = self.report_dir
        publisher.publish(preferred_html=str(html_path), preferred_metadata=str(metadata_path))

        tracker_css = self.output_dir / "tracker" / "tracker-ui.css"
        month_run_css = list((self.output_dir / "historico" / "2026-02").glob("*/tracker-ui.css"))
        self.assertTrue(tracker_css.exists())
        self.assertTrue(month_run_css)
        self.assertGreater(len(tracker_css.read_text(encoding="utf-8").strip()), 1000)

    def test_web_publish_normalizes_tracker_css_href_with_version(self):
        now = datetime.now(timezone.utc)
        html_path, metadata_path = self._write_report("2026-01", "2026-02", now)
        html_path.write_text(
            "<!doctype html><html><head><link rel=\"stylesheet\" href=\"./tracker-ui.css\"/></head><body>report</body></html>",
            encoding="utf-8",
        )
        tracker_css_source = self.report_dir / "tracker-ui.css"
        tracker_css_source.write_text("body{background:#fff;}", encoding="utf-8")

        publisher = StaticWebPublisher(self.config)
        publisher.report_dir = self.report_dir
        publisher.publish(preferred_html=str(html_path), preferred_metadata=str(metadata_path))

        tracker_html = (self.output_dir / "tracker" / "index.html").read_text(encoding="utf-8")
        month_run_html = list((self.output_dir / "historico" / "2026-02").glob("*/index.html"))[0].read_text(encoding="utf-8")
        self.assertIn('href="./tracker-ui.css?v=', tracker_html)
        self.assertIn('href="./tracker-ui.css?v=', month_run_html)

    def test_history_keeps_multiple_runs_same_month(self):
        now = datetime.now(timezone.utc)
        html_a, meta_a = self._write_report("2026-01", "2026-02", now - timedelta(hours=2))
        html_b, meta_b = self._write_report("2026-01", "2026-02", now - timedelta(hours=1))

        publisher = StaticWebPublisher(self.config)
        publisher.report_dir = self.report_dir
        publisher.publish(preferred_html=str(html_b), preferred_metadata=str(meta_b))

        run_dirs = [p for p in (self.output_dir / "historico" / "2026-02").iterdir() if p.is_dir()]
        self.assertGreaterEqual(len(run_dirs), 2)
        self.assertTrue((self.output_dir / "historico" / "2026-02" / "index.html").exists())
        manifest = json.loads((self.output_dir / "data" / "manifest.json").read_text(encoding="utf-8"))
        feb_runs = [row for row in manifest.get("history", []) if row.get("month") == "2026-02"]
        self.assertGreaterEqual(len(feb_runs), 2)

    def test_history_uses_ushuaia_local_datetime_labels(self):
        generated = datetime(2026, 2, 21, 12, 30, 0, tzinfo=timezone.utc)
        html_path, metadata_path = self._write_report("2026-01", "2026-02", generated)

        publisher = StaticWebPublisher(self.config)
        publisher.report_dir = self.report_dir
        publisher.publish(preferred_html=str(html_path), preferred_metadata=str(metadata_path))

        manifest = json.loads((self.output_dir / "data" / "manifest.json").read_text(encoding="utf-8"))
        row = next(item for item in manifest["history"] if item["month"] == "2026-02")
        expected_local = generated.astimezone(ZoneInfo("America/Argentina/Ushuaia")).strftime("%Y-%m-%d %H:%M:%S")
        self.assertEqual(row.get("run_date_local"), expected_local)
        self.assertEqual(row.get("history_timezone"), "America/Argentina/Ushuaia")

    def test_history_generates_month_index_and_run_paths(self):
        generated = datetime(2026, 2, 21, 12, 30, 0, tzinfo=timezone.utc)
        html_path, metadata_path = self._write_report("2026-01", "2026-02", generated)

        publisher = StaticWebPublisher(self.config)
        publisher.report_dir = self.report_dir
        publisher.publish(preferred_html=str(html_path), preferred_metadata=str(metadata_path))

        manifest = json.loads((self.output_dir / "data" / "manifest.json").read_text(encoding="utf-8"))
        row = next(item for item in manifest["history"] if item["month"] == "2026-02")
        self.assertTrue(str(row.get("report_path", "")).startswith("/historico/2026-02/"))
        self.assertIn("run_key", row)
        self.assertIn("generated_at_utc", row)
        self.assertIn("metadata_path", row)

        run_slug = str(row["run_slug"])
        run_dir = self.output_dir / "historico" / "2026-02" / run_slug
        self.assertTrue((self.output_dir / "historico" / "2026-02" / "index.html").exists())
        self.assertTrue((run_dir / "index.html").exists())
        self.assertTrue((self.output_dir / "data" / "history" / "2026-02" / f"{run_slug}.metadata.json").exists())


if __name__ == "__main__":
    unittest.main()
