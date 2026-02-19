"""Tests candidate audit persistence modes in run_scrape."""

import json
import sys
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.basket_planner import ScrapePlan
from src.models import Price, PriceCandidate, get_engine, get_session_factory
from src.scraper import run_scrape


class FakeScraper:
    """Small in-memory scraper fake to avoid browser usage in tests."""

    def __init__(self, _config, headless=True):
        self.headless = headless
        self.min_candidates_per_product = 3
        self.min_match_confidence = 0.2
        self._branch_attempt = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def select_branch(self):
        return True

    def search_product(self, _keywords):
        return [
            {"name": "Arroz Bajo 1kg", "price": Decimal("1000"), "url": "https://www.laanonima.com.ar/producto/art_low"},
            {"name": "Arroz Medio 1kg", "price": Decimal("1400"), "url": "https://www.laanonima.com.ar/producto/art_mid"},
            {"name": "Arroz Alto 1kg", "price": Decimal("1900"), "url": "https://www.laanonima.com.ar/producto/art_high"},
        ]

    def select_tiered_candidates(self, search_results, _item, min_candidates=None):
        _ = min_candidates
        selected = [
            {
                "product": search_results[0],
                "confidence": 0.8,
                "tie_break": (1, 1, 1),
                "fallback": False,
                "tier": "low",
            },
            {
                "product": search_results[1],
                "confidence": 0.9,
                "tie_break": (1, 1, 1),
                "fallback": False,
                "tier": "mid",
            },
            {
                "product": search_results[2],
                "confidence": 0.85,
                "tie_break": (1, 1, 1),
                "fallback": False,
                "tier": "high",
            },
        ]
        return selected, selected[1]

    def _canonical_product_url(self, url):
        return url or ""

    def _is_valid_product_url(self, _url):
        return True


class TestScraperCandidateStorage(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tmpdir.name) / "prices.db")
        self.config = {
            "branch": {"postal_code": "9410", "branch_name": "USHUAIA", "branch_id": "75"},
            "website": {"timeout": 1000, "retry_attempts": 1},
            "storage": {"default_backend": "sqlite", "sqlite": {"database_path": self.db_path}},
            "scraping": {
                "request_delay": 0,
                "min_candidates_per_product": 3,
                "min_match_confidence": 0.2,
                "candidates": {"storage_mode": "json", "min_candidates_per_product": 3},
            },
            "baskets": {
                "cba": {
                    "items": [
                        {
                            "id": "cba_arroz",
                            "name": "Arroz",
                            "keywords": ["arroz"],
                            "category": "legumbres",
                            "matching": "loose",
                            "unit": "kg",
                            "quantity": 1,
                        }
                    ]
                },
                "extended": {"items": []},
            },
        }
        self.plan = ScrapePlan(
            planned_items=[
                {
                    "id": "cba_arroz",
                    "name": "Arroz",
                    "keywords": ["arroz"],
                    "category": "legumbres",
                    "matching": "loose",
                    "unit": "kg",
                    "quantity": 1,
                    "basket_type": "cba",
                    "_plan_segment": "cba",
                }
            ],
            mandatory_ids={"cba_arroz"},
            plan_summary={
                "profile": "balanced",
                "segments": {"cba": 1},
                "mandatory_count": 1,
                "rotation_applied": 0,
                "estimated_duration_seconds": 30,
            },
            budget={
                "runtime_budget_minutes": 20,
                "target_seconds": 1200,
                "estimated_seconds": 30,
                "estimated_within_target": True,
            },
        )

    def tearDown(self):
        self.tmpdir.cleanup()

    def _count_rows(self):
        engine = get_engine(self.config, "sqlite")
        session = get_session_factory(engine)()
        try:
            return {
                "prices": session.query(Price).count(),
                "price_candidates": session.query(PriceCandidate).count(),
            }
        finally:
            session.close()
            engine.dispose()

    @patch("src.scraper.LaAnonimaScraper", FakeScraper)
    @patch("src.scraper.build_scrape_plan")
    @patch("src.scraper.load_config")
    def test_candidate_storage_json_writes_audit_file(self, mock_load_config, mock_build_plan):
        mock_load_config.return_value = self.config
        mock_build_plan.return_value = self.plan

        result = run_scrape(
            config_path="ignored.yaml",
            basket_type="all",
            output_format="sqlite",
            candidate_storage="json",
            observation_policy="single+audit",
        )

        self.assertEqual(result["status"], "completed")
        self.assertIsNotNone(result.get("candidates_audit_path"))
        audit_path = Path(result["candidates_audit_path"])
        self.assertTrue(audit_path.exists())
        payload = json.loads(audit_path.read_text(encoding="utf-8"))
        self.assertEqual(len(payload), 3)

        counts = self._count_rows()
        self.assertEqual(counts["prices"], 1)
        self.assertEqual(counts["price_candidates"], 0)

    @patch("src.scraper.LaAnonimaScraper", FakeScraper)
    @patch("src.scraper.build_scrape_plan")
    @patch("src.scraper.load_config")
    def test_candidate_storage_db_persists_price_candidates(self, mock_load_config, mock_build_plan):
        mock_load_config.return_value = self.config
        mock_build_plan.return_value = self.plan

        result = run_scrape(
            config_path="ignored.yaml",
            basket_type="all",
            output_format="sqlite",
            candidate_storage="db",
            observation_policy="single+audit",
        )

        self.assertEqual(result["status"], "completed")
        self.assertIsNone(result.get("candidates_audit_path"))

        counts = self._count_rows()
        self.assertEqual(counts["prices"], 1)
        self.assertEqual(counts["price_candidates"], 3)


if __name__ == "__main__":
    unittest.main()
