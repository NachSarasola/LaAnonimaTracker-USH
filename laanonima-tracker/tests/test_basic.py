"""Basic tests for La Anónima Price Tracker."""

import os
import sys
import unittest
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config_loader import load_config, get_basket_items, get_branch_config
from src.models import get_engine, init_db


class TestConfig(unittest.TestCase):
    """Test configuration loading."""
    
    def test_load_config(self):
        """Test that config loads successfully."""
        config = load_config()
        self.assertIsNotNone(config)
        self.assertIn("baskets", config)
        self.assertIn("branch", config)
    
    def test_branch_config(self):
        """Test branch configuration."""
        config = load_config()
        branch = get_branch_config(config)
        
        self.assertEqual(branch.get("postal_code"), "9410")
        self.assertEqual(branch.get("branch_name"), "USHUAIA 5")
        self.assertEqual(branch.get("branch_id"), "75")
    
    def test_basket_items(self):
        """Test basket items loading."""
        config = load_config()
        
        cba_items = get_basket_items(config, "cba")
        self.assertGreater(len(cba_items), 0)
        
        ext_items = get_basket_items(config, "extended")
        self.assertGreater(len(ext_items), 0)
        
        all_items = get_basket_items(config, "all")
        self.assertEqual(len(all_items), len(cba_items) + len(ext_items))


class TestDatabase(unittest.TestCase):
    """Test database functionality."""
    
    def test_engine_creation(self):
        """Test database engine creation."""
        config = load_config()
        engine = get_engine(config, "sqlite")
        self.assertIsNotNone(engine)
    
    def test_init_db(self):
        """Test database initialization."""
        config = load_config()
        engine = get_engine(config, "sqlite")
        
        # Should not raise
        init_db(engine)


class TestModels(unittest.TestCase):
    """Test data models."""
    
    def test_product_creation(self):
        """Test Product model."""
        from src.models import Product
        
        product = Product(
            canonical_id="test_product",
            basket_id="cba",
            name="Test Product",
            category="test",
            unit="kg",
            quantity=1.0,
        )
        
        self.assertEqual(product.canonical_id, "test_product")
        self.assertEqual(product.name, "Test Product")
    
    def test_price_creation(self):
        """Test Price model."""
        from decimal import Decimal
        from src.models import Price
        
        price = Price(
            canonical_id="test_product",
            basket_id="cba",
            product_name="Test Product",
            current_price=Decimal("100.50"),
            in_stock=True,
        )
        
        self.assertEqual(price.current_price, Decimal("100.50"))
        self.assertTrue(price.in_stock)


class TestAnalysis(unittest.TestCase):
    """Test analysis functions."""
    
    def test_basket_analyzer_init(self):
        """Test BasketAnalyzer initialization."""
        from src.analysis import BasketAnalyzer
        
        config = load_config()
        analyzer = BasketAnalyzer(config)
        self.assertIsNotNone(analyzer)
        analyzer.close()


if __name__ == "__main__":
    unittest.main()
