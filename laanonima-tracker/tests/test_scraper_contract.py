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

    def test_select_tiered_candidates_prefers_same_grammage_group(self):
        basket_item = {"name": "Arroz", "keywords": ["arroz"], "quantity": 1, "unit": "kg"}
        search_results = [
            {"name": "Arroz Mini 500g", "price": Decimal("900"), "url": "https://www.laanonima.com.ar/a/art_10/"},
            {"name": "Arroz Clasico 1kg", "price": Decimal("1500"), "url": "https://www.laanonima.com.ar/a/art_11/"},
            {"name": "Arroz Premium 1000g", "price": Decimal("1700"), "url": "https://www.laanonima.com.ar/a/art_12/"},
            {"name": "Arroz Seleccion 1 kg", "price": Decimal("1900"), "url": "https://www.laanonima.com.ar/a/art_13/"},
            {"name": "Arroz Familiar 2kg", "price": Decimal("2800"), "url": "https://www.laanonima.com.ar/a/art_14/"},
        ]

        selected, representative = self.scraper.select_tiered_candidates(search_results, basket_item, min_candidates=3)

        self.assertEqual(len(selected), 3)
        self.assertIsNotNone(representative)
        selected_names = {row["product"]["name"] for row in selected}
        self.assertNotIn("Arroz Mini 500g", selected_names)
        self.assertNotIn("Arroz Familiar 2kg", selected_names)
        self.assertIn("Arroz Clasico 1kg", selected_names)
        self.assertIn("Arroz Premium 1000g", selected_names)
        self.assertIn("Arroz Seleccion 1 kg", selected_names)

    def test_select_tiered_candidates_returns_fewer_when_same_size_not_available(self):
        basket_item = {"name": "Arroz", "keywords": ["arroz"], "quantity": 1, "unit": "kg"}
        search_results = [
            {"name": "Arroz A 500g", "price": Decimal("950"), "url": "https://www.laanonima.com.ar/a/art_21/"},
            {"name": "Arroz B 1kg", "price": Decimal("1600"), "url": "https://www.laanonima.com.ar/a/art_22/"},
            {"name": "Arroz C 2kg", "price": Decimal("2900"), "url": "https://www.laanonima.com.ar/a/art_23/"},
        ]

        selected, representative = self.scraper.select_tiered_candidates(search_results, basket_item, min_candidates=3)

        self.assertEqual(len(selected), 1)
        self.assertIsNotNone(representative)
        self.assertEqual(representative["product"]["name"], "Arroz B 1kg")

    def test_select_tiered_candidates_treats_900g_as_comparable_to_1kg(self):
        basket_item = {"name": "Arroz", "keywords": ["arroz"], "quantity": 1, "unit": "kg"}
        search_results = [
            {"name": "Arroz Eco 900g", "price": Decimal("1300"), "url": "https://www.laanonima.com.ar/a/art_31/"},
            {"name": "Arroz Clasico 1kg", "price": Decimal("1500"), "url": "https://www.laanonima.com.ar/a/art_32/"},
            {"name": "Arroz Premium 1000g", "price": Decimal("1700"), "url": "https://www.laanonima.com.ar/a/art_33/"},
            {"name": "Arroz Familiar 1200g", "price": Decimal("2100"), "url": "https://www.laanonima.com.ar/a/art_34/"},
        ]

        selected, representative = self.scraper.select_tiered_candidates(search_results, basket_item, min_candidates=3)

        self.assertEqual(len(selected), 3)
        self.assertIsNotNone(representative)
        selected_names = {row["product"]["name"] for row in selected}
        self.assertIn("Arroz Eco 900g", selected_names)
        self.assertIn("Arroz Clasico 1kg", selected_names)
        self.assertIn("Arroz Premium 1000g", selected_names)
        self.assertNotIn("Arroz Familiar 1200g", selected_names)


if __name__ == "__main__":
    unittest.main()
