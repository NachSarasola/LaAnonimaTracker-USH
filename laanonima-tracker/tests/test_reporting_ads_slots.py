"""Tests for reporting ads and premium placeholder wiring."""

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.models import get_engine, get_session_factory, init_db
from src.reporting import ReportGenerator


class TestReportingAdsSlots(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.config = {
            "storage": {"default_backend": "sqlite", "sqlite": {"database_path": self.tmp.name}},
            "baskets": {
                "cba": {"items": []},
                "extended": {"items": []},
            },
            "ads": {
                "enabled": True,
                "provider": "adsense_placeholder",
                "slots": ["header", "inline", "footer"],
                "client_id_placeholder": "ca-pub-test",
            },
            "premium_placeholders": {
                "enabled": True,
                "features": ["Alertas", "CSV Pro"],
            },
            "deployment": {
                "fresh_max_hours": 36,
                "schedule_utc": "09:10",
            },
        }

        self.engine = get_engine(self.config, "sqlite")
        init_db(self.engine)
        self.session = get_session_factory(self.engine)()
        self.generator = ReportGenerator(self.config)
        self.generator.session = self.session

    def tearDown(self):
        self.session.close()
        self.generator.close()
        self.engine.dispose()
        try:
            Path(self.tmp.name).unlink(missing_ok=True)
        except PermissionError:
            pass

    def test_payload_contains_ads_and_premium_flags(self):
        df = self.generator._load_prices("2024-01", "2024-01", "all")
        payload = self.generator._build_interactive_payload(df, "2024-01", "2024-01", "all")

        self.assertIn("ads", payload)
        self.assertIn("premium_placeholders", payload)
        self.assertTrue(payload["ads"]["enabled"])
        self.assertEqual(payload["ads"]["slots"], ["header", "inline", "footer"])
        self.assertTrue(payload["premium_placeholders"]["enabled"])
        self.assertEqual(payload["premium_placeholders"]["features"], ["Alertas", "CSV Pro"])
        self.assertIn("web_status", payload)
        self.assertIn("is_stale", payload)
        self.assertIn("next_update_eta", payload)

    def test_render_includes_ads_and_premium_containers(self):
        df = self.generator._load_prices("2024-01", "2024-01", "all")
        payload = self.generator._build_interactive_payload(df, "2024-01", "2024-01", "all")
        html = self.generator._render_interactive_html(payload, "2026-02-19 00:00:00 UTC")

        self.assertIn('id="ad-panel"', html)
        self.assertIn('id="ad-slots"', html)
        self.assertIn('id="premium-panel"', html)
        self.assertIn('id="premium-features"', html)
        self.assertIn('id="freshness-meta"', html)
        self.assertIn('id="copy-link"', html)
        self.assertIn('id="copy-link-status"', html)
        self.assertIn('id="mobile-onboarding"', html)
        self.assertIn('id="onboarding-goto"', html)
        self.assertIn('id="onboarding-close"', html)
        self.assertIn('id="cookie-banner"', html)
        self.assertIn("function initConsentBanner()", html)
        self.assertIn("function drawMonetization()", html)
        self.assertIn("function bindShortcuts()", html)
        self.assertIn("function initMobileOnboarding()", html)


if __name__ == "__main__":
    unittest.main()
