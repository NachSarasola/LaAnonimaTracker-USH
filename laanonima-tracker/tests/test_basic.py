"""Basic tests for La Anónima Price Tracker."""

import os
import sys
import unittest
from datetime import datetime
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config_loader import load_config, get_basket_items, get_branch_config, resolve_canonical_category
from src.models import get_engine, init_db, get_session_factory, Product, Price, CategoryIndex
from src.category_backfill import backfill_canonical_categories, validate_price_category_traceability


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


class TestCanonicalCategories(unittest.TestCase):
    """Test canonical category mapping and backfill."""

    def test_resolve_canonical_category(self):
        config = load_config()
        self.assertEqual(resolve_canonical_category(config, "carnes"), "carniceria")
        self.assertEqual(resolve_canonical_category(config, "HIGIENE"), "perfumeria")

    def test_backfill_traceability(self):
        config = load_config()
        engine = get_engine({"storage": {"default_backend": "sqlite", "sqlite": {"database_path": ":memory:"}}}, "sqlite")
        init_db(engine)
        Session = get_session_factory(engine)
        session = Session()

        from src.models import ScrapeRun

        run = ScrapeRun(
            run_uuid="11111111-1111-1111-1111-111111111111",
            branch_id="75",
            branch_name="USHUAIA",
            postal_code="9410",
            basket_type="cba",
        )
        session.add(run)
        session.flush()

        product = Product(canonical_id="prod_1", basket_id="cba", name="Leche", category="lacteos")
        session.add(product)
        session.flush()

        price = Price(
            product_id=product.id,
            run_id=run.id,
            canonical_id="prod_1",
            basket_id="cba",
            product_name="Leche",
            current_price=100,
        )
        session.add(price)
        session.commit()

        result = backfill_canonical_categories(session, config)
        traceability = validate_price_category_traceability(session)

        self.assertGreaterEqual(result["products_updated"], 1)
        self.assertEqual(traceability["prices_without_category"], 0)

        session.close()


class TestCategoryIndices(unittest.TestCase):
    """Test category-level index computation and persistence."""

    def test_compute_category_indices(self):
        from src.analysis import BasketAnalyzer
        from src.models import ScrapeRun

        config = {
            "analysis": {"base_period": "2024-01", "index_type": "laspeyres"},
            "storage": {"default_backend": "sqlite", "sqlite": {"database_path": ":memory:"}},
            "baskets": {
                "cba": {"items": [
                    {"id": "prod_leche", "quantity": 1},
                    {"id": "prod_pan", "quantity": 2},
                ]},
                "extended": {"items": []},
            },
        }

        engine = get_engine(config, "sqlite")
        init_db(engine)
        Session = get_session_factory(engine)
        session = Session()

        run = ScrapeRun(
            run_uuid="22222222-2222-2222-2222-222222222222",
            branch_id="75",
            branch_name="USHUAIA",
            postal_code="9410",
            basket_type="cba",
        )
        session.add(run)
        session.flush()

        leche = Product(canonical_id="prod_leche", basket_id="cba", name="Leche", category="lacteos")
        pan = Product(canonical_id="prod_pan", basket_id="cba", name="Pan", category="almacen")
        session.add_all([leche, pan])
        session.flush()

        session.add_all([
            Price(
                product_id=leche.id,
                run_id=run.id,
                canonical_id="prod_leche",
                basket_id="cba",
                product_name="Leche",
                current_price=100,
                scraped_at=datetime(2024, 1, 15),
            ),
            Price(
                product_id=pan.id,
                run_id=run.id,
                canonical_id="prod_pan",
                basket_id="cba",
                product_name="Pan",
                current_price=50,
                scraped_at=datetime(2024, 1, 15),
            ),
            Price(
                product_id=leche.id,
                run_id=run.id,
                canonical_id="prod_leche",
                basket_id="cba",
                product_name="Leche",
                current_price=120,
                scraped_at=datetime(2024, 2, 15),
            ),
            Price(
                product_id=pan.id,
                run_id=run.id,
                canonical_id="prod_pan",
                basket_id="cba",
                product_name="Pan",
                current_price=55,
                scraped_at=datetime(2024, 2, 15),
            ),
        ])
        session.commit()

        analyzer = BasketAnalyzer(config, db_session=session)
        result = analyzer.compute_category_indices(basket_type="cba", save_to_db=True)

        self.assertFalse(result.empty)
        self.assertIn("category", result.columns)
        self.assertEqual(session.query(CategoryIndex).count(), 4)

        lacteos_feb = result[(result["category"] == "lacteos") & (result["year_month"] == "2024-02")].iloc[0]
        self.assertAlmostEqual(lacteos_feb["index_value"], 120.0, places=2)

        analyzer.close()



if __name__ == "__main__":
    unittest.main()
