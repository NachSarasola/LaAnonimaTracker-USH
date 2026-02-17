"""Playwright-based scraper for La Anónima supermarket."""

import re
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Dict, List, Optional, Tuple, Any
from urllib.parse import quote, urljoin

from loguru import logger
from playwright.sync_api import Page, sync_playwright, TimeoutError as PlaywrightTimeout
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from src.models import (
    Product, Price, ScrapeRun, ScrapeError,
    get_engine, init_db, get_session_factory
)
from src.config_loader import load_config, get_basket_items, get_branch_config, get_scraping_config


class BranchSelectionError(Exception):
    """Raised when branch selection fails."""
    pass


class ProductNotFoundError(Exception):
    """Raised when a product cannot be found."""
    pass


class LaAnonimaScraper:
    """Main scraper class for La Anónima supermarket."""
    
    def __init__(self, config: Dict[str, Any], headless: Optional[bool] = None):
        """Initialize the scraper.
        
        Args:
            config: Configuration dictionary
            headless: Override headless mode from config
        """
        self.config = config
        self.branch_config = get_branch_config(config)
        self.scraping_config = get_scraping_config(config)
        
        self.base_url = config.get("website", {}).get("base_url", "https://laanonima.com.ar/supermercado/")
        self.search_url_template = config.get("website", {}).get(
            "search_url", "https://laanonima.com.ar/supermercado/buscar/{query}"
        )
        self.timeout = config.get("website", {}).get("timeout", 30000)
        self.retry_attempts = config.get("website", {}).get("retry_attempts", 3)
        
        self.headless = headless if headless is not None else self.scraping_config.get("browser", {}).get("headless", True)
        
        # Selectors
        self.selectors = self.scraping_config.get("selectors", {})
        
        # State
        self.page: Optional[Page] = None
        self.playwright = None
        self.browser = None
        self.context = None
        self.current_branch: Optional[str] = None
        
        logger.info(f"Scraper initialized (headless={self.headless})")
    
    def start(self):
        """Start the browser and create a new page."""
        logger.info("Starting browser...")
        self.playwright = sync_playwright().start()
        
        browser_config = self.scraping_config.get("browser", {})
        viewport = browser_config.get("viewport", {"width": 1920, "height": 1080})
        
        self.browser = self.playwright.chromium.launch(
            headless=self.headless,
            args=["--no-sandbox", "--disable-dev-shm-usage"]
        )
        
        self.context = self.browser.new_context(
            viewport=viewport,
            user_agent=browser_config.get("user_agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
        )
        
        self.page = self.context.new_page()
        self.page.set_default_timeout(self.timeout)
        
        logger.info("Browser started successfully")
    
    def stop(self):
        """Stop the browser and cleanup."""
        logger.info("Stopping browser...")
        if self.context:
            self.context.close()
        if self.browser:
            self.browser.close()
        if self.playwright:
            self.playwright.stop()
        logger.info("Browser stopped")
    
    def __enter__(self):
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
    
    def _get_selector(self, key: str) -> str:
        """Get a CSS selector from config."""
        return self.selectors.get(key, "")
    
    def check_branch_set(self) -> Tuple[bool, Optional[str]]:
        """Check if branch is already set to target.
        
        Returns:
            Tuple of (is_set_correctly, current_branch_name)
        """
        if not self.page:
            raise RuntimeError("Browser not started")
        
        try:
            # Look for branch indicator in the page
            # Try multiple possible selectors
            selectors_to_try = [
                ".sucursal-actual",
                ".branch-name",
                "[data-branch-name]",
                ".sucursal-seleccionada",
                ".header-sucursal",
            ]
            
            for selector in selectors_to_try:
                try:
                    element = self.page.locator(selector).first
                    if element.is_visible(timeout=2000):
                        text = element.inner_text()
                        if text:
                            logger.debug(f"Found branch indicator: {text}")
                            target_branch = self.branch_config.get("branch_name", "USHUAIA 5")
                            if target_branch.lower() in text.lower():
                                return True, text.strip()
                            return False, text.strip()
                except:
                    continue
            
            # Check cookies or localStorage for branch info
            try:
                branch_cookie = self.page.evaluate("() => document.cookie")
                if "sucursal" in branch_cookie or "branch" in branch_cookie:
                    logger.debug(f"Found branch in cookie: {branch_cookie}")
            except:
                pass
            
            return False, None
            
        except Exception as e:
            logger.warning(f"Error checking branch status: {e}")
            return False, None
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((PlaywrightTimeout, BranchSelectionError)),
        reraise=True
    )
    def select_branch(self) -> bool:
        """Select the target branch (Ushuaia 9410).
        
        Returns:
            True if branch selection was successful
            
        Raises:
            BranchSelectionError: If branch selection fails after retries
        """
        if not self.page:
            raise RuntimeError("Browser not started")
        
        postal_code = self.branch_config.get("postal_code", "9410")
        branch_name = self.branch_config.get("branch_name", "USHUAIA 5")
        branch_id = self.branch_config.get("branch_id", "75")
        
        logger.info(f"Selecting branch: {branch_name} (CP: {postal_code})")
        
        # Navigate to base URL first (timeout to avoid hanging on slow site)
        logger.info(f"Navigating to {self.base_url}")
        self.page.goto(
            self.base_url,
            wait_until="domcontentloaded",
            timeout=self.timeout,
        )
        time.sleep(2)

        # Dismiss any modal/overlay that blocks clicks (e.g. cookie or promo)
        try:
            overlay = self.page.locator(".reveal-overlay").first
            if overlay.is_visible(timeout=1000):
                overlay.click(force=True)
                time.sleep(0.5)
        except Exception:
            pass
        try:
            self.page.keyboard.press("Escape")
            time.sleep(0.3)
        except Exception:
            pass

        # Check if already set correctly
        is_set, current = self.check_branch_set()
        if is_set:
            logger.info(f"Branch already set to {current}")
            self.current_branch = current
            return True

        logger.info(f"Current branch: {current}, need to change to {branch_name}")

        try:
            # Click the branch selector trigger
            trigger_selector = self._get_selector("branch_trigger") or "a[data-toggle='codigo-postal']"
            logger.info(f"Clicking branch selector: {trigger_selector}")

            trigger = self.page.locator(trigger_selector).first
            if not trigger.is_visible(timeout=3000):
                # Try alternative selectors
                alt_selectors = [
                    "a:has-text('Código Postal')",
                    "a:has-text('Sucursal')",
                    ".seleccionar-sucursal",
                    "[data-target='#codigo-postal']",
                ]
                for alt in alt_selectors:
                    try:
                        alt_trigger = self.page.locator(alt).first
                        if alt_trigger.is_visible(timeout=2000):
                            trigger = alt_trigger
                            break
                    except:
                        continue
            
            trigger.click(force=True, timeout=10000)
            logger.info("Branch selector clicked")

            time.sleep(1)
            input_selector = self._get_selector("postal_input") or "#idCodigoPostalUnificado"
            logger.info(f"Filling postal code: {postal_code}")
            postal_input = self.page.locator(input_selector)
            postal_input.fill(postal_code)
            postal_input.press("Tab")
            time.sleep(2)

            # Wait for branch options in DOM (may be hidden)
            options_selector = self._get_selector("branch_options") or "#opcionesSucursal"
            radio_selector = self._get_selector("branch_radio") or "input[name='sucursalSuper']"
            self.page.wait_for_selector(
                f"{options_selector} {radio_selector}",
                timeout=10000,
                state="attached",
            )

            # Select branch via JS (works when radios are hidden) and confirm
            branch_found = self.page.evaluate(
                """([branchId, confirmSel]) => {
                    const radio = document.querySelector(
                        "input[name='sucursalSuper'][value='" + branchId + "']"
                    );
                    if (!radio) return false;
                    radio.checked = true;
                    radio.dispatchEvent(new Event("change", { bubbles: true }));
                    const btn = document.querySelector(confirmSel);
                    if (btn) btn.click();
                    else if (typeof setSucursalSuper === "function") setSucursalSuper();
                    return true;
                }""",
                [branch_id, self._get_selector("confirm_button") or "#btn_setSucursalSuper"],
            )
            if not branch_found:
                raise BranchSelectionError(f"Branch '{branch_name}' (id={branch_id}) not found in options")
            
            logger.info("Branch selection confirmed")

            # Wait for any AJAX/navigation to apply selection
            time.sleep(3)
            try:
                self.page.wait_for_load_state("networkidle", timeout=8000)
            except Exception:
                pass

            is_set, current = self.check_branch_set()
            if is_set:
                logger.info(f"Branch selection verified: {current}")
                self.current_branch = current
                return True
            # Selection may be stored in cookie/session; continue and rely on prices at scrape time
            logger.warning(
                "Branch indicator not found after selection (may still be applied). Continuing."
            )
            self.current_branch = branch_name
            return True
                
        except Exception as e:
            logger.error(f"Branch selection failed: {e}")
            # Take screenshot for debugging
            try:
                screenshot_path = f"data/logs/branch_error_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
                self.page.screenshot(path=screenshot_path)
                logger.info(f"Error screenshot saved to {screenshot_path}")
            except:
                pass
            raise BranchSelectionError(f"Failed to select branch: {e}")
    
    def search_product(self, keywords: List[str]) -> List[Dict[str, Any]]:
        """Search for a product using keywords.
        
        Args:
            keywords: List of search terms to try
            
        Returns:
            List of product results
        """
        if not self.page:
            raise RuntimeError("Browser not started")
        
        results = []
        
        for keyword in keywords:
            try:
                logger.info(f"Searching for: {keyword}")
                strategy_used = "direct_url"

                # Primary strategy: direct navigation to search URL
                encoded_keyword = quote(keyword)
                search_url = self.search_url_template.format(query=encoded_keyword)

                try:
                    self.page.goto(
                        search_url,
                        wait_until="domcontentloaded",
                        timeout=self.timeout,
                    )
                    logger.info(f"Search strategy used: {strategy_used} ({search_url})")
                except Exception as nav_error:
                    # Fallback strategy: use search input from home
                    strategy_used = "home_input_fallback"
                    logger.warning(
                        f"Direct search navigation failed for '{keyword}', falling back to input strategy: {nav_error}"
                    )
                    self.page.goto(
                        self.base_url,
                        wait_until="domcontentloaded",
                        timeout=self.timeout,
                    )

                    search_selector = self._get_selector("search_input") or "#idBuscarProducto"
                    search_input = self.page.locator(search_selector).first
                    search_input.fill("")
                    search_input.fill(keyword)
                    search_input.press("Enter")
                    logger.info(f"Search strategy used: {strategy_used}")
                
                # Wait for results
                time.sleep(2)
                
                # Parse results
                product_selector = self._get_selector("product_list") or ".producto"
                products = self.page.locator(product_selector).all()
                
                logger.info(f"Found {len(products)} products for '{keyword}'")
                
                for product in products[:5]:  # Limit to first 5
                    try:
                        product_data = self._parse_product(product)
                        if product_data:
                            results.append(product_data)
                    except Exception as e:
                        logger.debug(f"Error parsing product: {e}")
                
                if results:
                    break  # Found results, no need to try other keywords
                    
            except Exception as e:
                logger.warning(f"Search failed for '{keyword}': {e}")
                continue
        
        return results
    
    def _parse_product(self, product_element) -> Optional[Dict[str, Any]]:
        """Parse a product element into data dictionary."""
        try:
            # Product name
            name_selector = self._get_selector("product_name") or ".nombre-producto"
            name = product_element.locator(name_selector).inner_text()
            
            # Price
            price_selector = self._get_selector("product_price") or ".precio-actual"
            price_text = product_element.locator(price_selector).inner_text()
            price = self._parse_price(price_text)
            
            # Original price (for discounts)
            old_price_selector = self._get_selector("product_price_old") or ".precio-anterior"
            old_price = None
            try:
                old_price_text = product_element.locator(old_price_selector).inner_text()
                old_price = self._parse_price(old_price_text)
            except:
                pass
            
            # Price per unit
            unit_price_selector = self._get_selector("product_unit_price") or ".precio-unitario"
            unit_price = None
            try:
                unit_price_text = product_element.locator(unit_price_selector).inner_text()
                unit_price = self._parse_price(unit_price_text)
            except:
                pass
            
            # URL
            url = ""
            try:
                url_selector = self._get_selector("product_url") or "a[href*='producto']"
                url = product_element.locator(url_selector).first.get_attribute("href")
                url = urljoin(self.base_url, url)
            except:
                pass
            
            # Stock status
            in_stock = True
            try:
                oos_selector = self._get_selector("out_of_stock") or ".sin-stock"
                if product_element.locator(oos_selector).count() > 0:
                    in_stock = False
            except:
                pass
            
            # Check for promotion (guard against None price)
            is_promotion = (
                old_price is not None and price is not None and old_price > price
            )
            
            return {
                "name": name.strip(),
                "price": price,
                "original_price": old_price,
                "price_per_unit": unit_price,
                "url": url,
                "in_stock": in_stock,
                "is_promotion": is_promotion,
            }
            
        except Exception as e:
            logger.debug(f"Error parsing product element: {e}")
            return None
    
    def _parse_price(self, price_text: str) -> Optional[Decimal]:
        """Parse price text to Decimal."""
        if not price_text:
            return None
        
        # Remove currency symbols, dots (thousand separator), and whitespace
        # Argentine format: $ 1.234,56 or $1234,56
        cleaned = price_text.replace("$", "").replace(".", "").replace(" ", "").strip()
        
        # Replace comma with dot for decimal
        cleaned = cleaned.replace(",", ".")
        
        # Extract numeric part
        match = re.search(r'[\d.]+', cleaned)
        if match:
            try:
                return Decimal(match.group())
            except InvalidOperation:
                pass
        
        return None
    
    def match_product(
        self, 
        search_results: List[Dict[str, Any]], 
        basket_item: Dict[str, Any]
    ) -> Tuple[Optional[Dict[str, Any]], float]:
        """Match search results to a basket item.
        
        Args:
            search_results: List of products from search
            basket_item: Basket item configuration
            
        Returns:
            Tuple of (best_match, confidence_score)
        """
        if not search_results:
            return None, 0.0
        
        keywords = [k.lower() for k in basket_item.get("keywords", [])]
        brand_hints = [b.lower() for b in basket_item.get("brand_hint", [])]
        matching = basket_item.get("matching", "loose")
        
        best_match = None
        best_score = 0.0
        
        for product in search_results:
            product_name = product.get("name", "").lower()
            score = 0.0
            
            # Keyword matching
            keyword_matches = sum(1 for kw in keywords if kw in product_name)
            score += (keyword_matches / len(keywords)) * 0.6 if keywords else 0
            
            # Brand matching
            if brand_hints:
                brand_matches = sum(1 for brand in brand_hints if brand in product_name)
                score += (brand_matches / len(brand_hints)) * 0.3
            
            # Price validity (prefer in-stock)
            if product.get("in_stock"):
                score += 0.1
            
            # Strict matching requires all keywords
            if matching == "strict" and keyword_matches < len(keywords):
                score = 0
            
            if score > best_score:
                best_score = score
                best_match = product
        
        return best_match, best_score


def run_scrape(
    config_path: Optional[str] = None,
    basket_type: str = "cba",
    headless: bool = True,
    output_format: str = "sqlite",
    limit: Optional[int] = None,
) -> Dict[str, Any]:
    """Run a complete scrape operation.

    Args:
        config_path: Path to config file
        basket_type: Type of basket to scrape ('cba', 'extended', 'all')
        headless: Run browser in headless mode
        output_format: Output format ('sqlite', 'postgresql')
        limit: If set, only scrape this many products (random sample)

    Returns:
        Dictionary with scrape results and statistics
    """
    # Load configuration
    config = load_config(config_path)
    
    # Setup database
    engine = get_engine(config, output_format)
    init_db(engine)
    Session = get_session_factory(engine)
    session = Session()
    
    run_uuid = str(uuid.uuid4())
    branch_config = get_branch_config(config)
    
    # Create run record
    scrape_run = ScrapeRun(
        run_uuid=run_uuid,
        branch_id=branch_config.get("branch_id", "75"),
        branch_name=branch_config.get("branch_name", "USHUAIA 5"),
        postal_code=branch_config.get("postal_code", "9410"),
        basket_type=basket_type,
        status="running",
        scraper_version="1.0.0",
    )
    session.add(scrape_run)
    session.commit()
    
    results = {
        "run_uuid": run_uuid,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "products_scraped": 0,
        "products_failed": 0,
        "errors": [],
    }
    
    try:
        with LaAnonimaScraper(config, headless=headless) as scraper:
            # Select branch
            logger.info("Selecting branch...")
            branch_success = scraper.select_branch()
            
            if not branch_success:
                raise BranchSelectionError("Failed to select target branch")
            
            # Get basket items (optionally limit to random sample)
            basket_items = get_basket_items(config, basket_type)
            if limit is not None and limit > 0 and len(basket_items) > limit:
                import random
                basket_items = random.sample(basket_items, limit)
                logger.info(f"Limited to {limit} random products")
            scrape_run.products_planned = len(basket_items)
            session.commit()

            logger.info(f"Scraping {len(basket_items)} products from basket '{basket_type}'")
            
            for item in basket_items:
                item_id = item.get("id", "unknown")
                item_name = item.get("name", "Unknown")
                
                try:
                    logger.info(f"Processing: {item_name} ({item_id})")
                    
                    # Search for product
                    keywords = item.get("keywords", [item_name])
                    search_results = scraper.search_product(keywords)
                    
                    if not search_results:
                        raise ProductNotFoundError(f"No results for {item_name}")
                    
                    # Match to best product
                    match, confidence = scraper.match_product(search_results, item)

                    # Skip if no valid match or no price (so we keep a clean time series per product)
                    if not match or confidence < 0.3:
                        raise ProductNotFoundError(
                            f"No acceptable match for {item_name} (confidence={confidence:.2f})"
                        )
                    price_value = match.get("price")
                    if price_value is None:
                        raise ProductNotFoundError(f"No price found for {item_name}")

                    # Get or create product record (one per canonical_id for history)
                    product = session.query(Product).filter_by(canonical_id=item_id).first()
                    if not product:
                        product = Product(
                            canonical_id=item_id,
                            basket_id=item.get("basket_type", "cba"),
                            name=item_name,
                            category=item.get("category"),
                            unit=item.get("unit"),
                            quantity=item.get("quantity"),
                            keywords=",".join(keywords),
                            brand_hint=",".join(item.get("brand_hint", [])),
                            matching_rules=item.get("matching", "loose"),
                            signature_name=match.get("name"),
                        )
                        session.add(product)
                        session.flush()

                    # Create price record for this run (one row per product per run = time series)
                    price_record = Price(
                        product_id=product.id,
                        run_id=scrape_run.id,
                        canonical_id=item_id,
                        basket_id=item.get("basket_type", "cba"),
                        product_name=match.get("name", item_name),
                        product_size=match.get("size"),
                        product_brand=match.get("brand"),
                        product_url=match.get("url"),
                        current_price=price_value,
                        original_price=match.get("original_price"),
                        price_per_unit=match.get("price_per_unit"),
                        in_stock=match.get("in_stock", True),
                        is_promotion=match.get("is_promotion", False),
                        confidence_score=Decimal(str(confidence)),
                        match_method="fuzzy" if confidence < 1.0 else "exact",
                        scraped_at=datetime.now(timezone.utc),
                    )
                    session.add(price_record)

                    results["products_scraped"] += 1
                    scrape_run.products_scraped += 1
                    session.commit()
                    
                    # Delay between requests
                    delay = config.get("scraping", {}).get("request_delay", 1000) / 1000
                    time.sleep(delay)
                    
                except Exception as e:
                    logger.error(f"Error processing {item_name}: {e}")
                    
                    # Record error
                    error = ScrapeError(
                        run_id=scrape_run.id,
                        product_id=item_id,
                        product_name=item_name,
                        stage="scraping",
                        error_type=type(e).__name__,
                        error_message=str(e),
                    )
                    session.add(error)
                    
                    results["products_failed"] += 1
                    results["errors"].append({
                        "product": item_name,
                        "error": str(e),
                    })
                    scrape_run.products_failed += 1
                    session.commit()
            
            # Mark run as completed
            scrape_run.status = "completed" if results["products_failed"] == 0 else "partial"
            scrape_run.completed_at = datetime.now(timezone.utc)
            if scrape_run.started_at:
                scrape_run.duration_seconds = int(
                    (scrape_run.completed_at - scrape_run.started_at).total_seconds()
                )
            session.commit()
            
            results["status"] = scrape_run.status
            results["completed_at"] = datetime.utcnow().isoformat()
            
    except Exception as e:
        logger.error(f"Scrape failed: {e}")
        scrape_run.status = "failed"
        scrape_run.completed_at = datetime.now(timezone.utc)
        session.commit()
        
        results["status"] = "failed"
        results["error"] = str(e)
        raise
    
    finally:
        session.close()
    
    return results


if __name__ == "__main__":
    # Simple CLI for testing
    import sys
    
    logging_config = {
        "handlers": [
            {"sink": sys.stdout, "format": "{time:HH:mm:ss} | {level} | {message}"},
            {"sink": "data/logs/scraper.log", "rotation": "1 MB"},
        ]
    }
    
    results = run_scrape(basket_type="cba", headless=False)
    print(f"Scrape completed: {results['products_scraped']} products scraped")
