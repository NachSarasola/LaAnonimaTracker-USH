"""Contract tests for scraper URL and product parsing behavior."""

import sys
import unittest
from decimal import Decimal
from pathlib import Path
from urllib.parse import quote

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config_loader import load_config
from src.scraper import LaAnonimaScraper


class FakeLeafLocator:
    """Leaf locator with text and attributes."""

    def __init__(self, text=None, attributes=None, should_fail=False, count=0):
        self._text = text
        self._attributes = attributes or {}
        self._should_fail = should_fail
        self._count = count

    def inner_text(self):
        if self._should_fail:
            raise RuntimeError("selector failed")
        return self._text

    def get_attribute(self, name):
        if self._should_fail:
            raise RuntimeError("selector failed")
        return self._attributes.get(name)

    @property
    def first(self):
        return self

    def count(self):
        if self._should_fail:
            raise RuntimeError("selector failed")
        return self._count


class FakeProductElement:
    """Minimal product element mock compatible with _parse_product usage."""

    def __init__(self, selectors):
        self._selectors = selectors

    def locator(self, selector):
        value = self._selectors.get(selector)
        if value is None:
            return FakeLeafLocator(should_fail=True)
        return value


class TestScraperContract(unittest.TestCase):
    """Contract-level unit tests for scraper internals."""

    def setUp(self):
        config_path = Path(__file__).parent.parent / "config.yaml"
        self.config = load_config(str(config_path))
        self.scraper = LaAnonimaScraper(self.config)

    def test_search_url_uses_buscar_query_path(self):
        query = "yerba mate"
        built_url = self.scraper.search_url_template.format(query=quote(query))

        self.assertIn("/buscar/", built_url)
        self.assertTrue(built_url.endswith("yerba%20mate"))

    def test_product_url_accepts_art_identifier(self):
        self.assertTrue(
            self.scraper._is_valid_product_url(
                "https://www.laanonima.com.ar/producto/art_1234_arroz"
            )
        )

    def test_parse_price_argentine_format(self):
        parsed = self.scraper._parse_price("$ 1.234,56")
        self.assertEqual(parsed, Decimal("1234.56"))

    def test_detect_closed_target_error_message(self):
        exc = RuntimeError("Target page, context or browser has been closed")
        self.assertTrue(self.scraper._is_closed_target_error(exc))

    def test_parse_product_uses_fallback_selectors_when_primary_fails(self):
        product_element = FakeProductElement(
            {
                "#broken-name": FakeLeafLocator(should_fail=True),
                ".nombre-producto": FakeLeafLocator(text="Arroz Largo Fino 1 kg"),
                "#broken-price": FakeLeafLocator(should_fail=True),
                ".precio-actual": FakeLeafLocator(text="$ 1.234,56"),
                ".precio-anterior": FakeLeafLocator(text="$ 1.500,00"),
                ".precio-unitario": FakeLeafLocator(text="$ 1.234,56"),
                "#broken-url": FakeLeafLocator(should_fail=True),
                "a[href*='art_']": FakeLeafLocator(
                    attributes={"href": "/producto/art_98765_arroz"}
                ),
                ".sin-stock": FakeLeafLocator(count=0),
            }
        )

        self.scraper.selectors.update(
            {
                "product_name": "#broken-name",
                "product_price": "#broken-price",
                "product_url": "#broken-url",
            }
        )

        parsed = self.scraper._parse_product(product_element)

        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["name"], "Arroz Largo Fino 1 kg")
        self.assertEqual(parsed["price"], Decimal("1234.56"))
        self.assertEqual(parsed["original_price"], Decimal("1500.00"))
        self.assertTrue(parsed["url_valid"])
        self.assertIn("/art_98765_arroz", parsed["url"])

    def test_select_tiered_candidates_returns_low_mid_high(self):
        basket_item = {"name": "Arroz", "keywords": ["arroz"], "quantity": 1, "unit": "kg"}
        search_results = [
            {"name": "Arroz A 1kg", "price": Decimal("1200"), "url": "https://www.laanonima.com.ar/a/art_1/"},
            {"name": "Arroz B 1kg", "price": Decimal("1800"), "url": "https://www.laanonima.com.ar/b/art_2/"},
            {"name": "Arroz C 1kg", "price": Decimal("2400"), "url": "https://www.laanonima.com.ar/c/art_3/"},
            {"name": "Arroz D 1kg", "price": Decimal("2600"), "url": "https://www.laanonima.com.ar/d/art_4/"},
        ]

        selected, representative = self.scraper.select_tiered_candidates(search_results, basket_item, min_candidates=3)

        self.assertGreaterEqual(len(selected), 3)
        tiers = {row["tier"] for row in selected}
        self.assertIn("low", tiers)
        self.assertIn("mid", tiers)
        self.assertIn("high", tiers)
        self.assertIsNotNone(representative)
        self.assertIn(representative["tier"], {"mid", "high", "low"})


if __name__ == "__main__":
    unittest.main()
