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
        self.config = load_config()
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


if __name__ == "__main__":
    unittest.main()
