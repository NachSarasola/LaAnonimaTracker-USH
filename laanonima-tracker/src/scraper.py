"""Playwright-based scraper for La Anónima supermarket."""

import re
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Dict, List, Optional, Tuple, Any
from urllib.parse import quote, urljoin, urlsplit, urlunsplit

from loguru import logger
from playwright.sync_api import Page, sync_playwright, TimeoutError as PlaywrightTimeout
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from src.models import (
    Product, Price, ScrapeRun, ScrapeError, Category,
    get_engine, init_db, get_session_factory
)
from src.config_loader import (
    load_config,
    get_basket_items,
    get_branch_config,
    get_scraping_config,
    resolve_canonical_category,
    get_category_display_names,
)


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

    @staticmethod
    def _canonical_product_url(url: Optional[str]) -> str:
        """Canonicalize product URL by dropping query params and fragments."""
        if not url:
            return ""

        parsed = urlsplit(url)
        return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))

    def _is_valid_product_url(self, url: Optional[str]) -> bool:
        """Validate La Anónima product URL format."""
        return "/art_" in self._canonical_product_url(url)

    @staticmethod
    def _normalize_unit(unit: Optional[str]) -> Optional[str]:
        """Normalize unit aliases to canonical units used for matching."""
        if not unit:
            return None

        normalized = unit.strip().lower()
        aliases = {
            "kg": "kg",
            "kilo": "kg",
            "kilos": "kg",
            "g": "g",
            "gr": "g",
            "grs": "g",
            "gramo": "g",
            "gramos": "g",
            "l": "l",
            "lt": "l",
            "lts": "l",
            "litro": "l",
            "litros": "l",
            "ml": "ml",
            "mililitro": "ml",
            "mililitros": "ml",
            "un": "un",
            "u": "un",
            "unidad": "un",
            "unidades": "un",
        }
        return aliases.get(normalized, normalized)

    def _parse_presentation_from_name(self, name: str) -> Dict[str, Any]:
        """Extract presentation amount/unit from product name.

        Returns quantity in the parsed unit (kg, g, l, ml, un) when detectable.
        """
        normalized_name = (name or "").lower()

        patterns = [
            r"(?P<qty>\d+[\.,]?\d*)\s*(?P<unit>kg|kilo(?:s)?|g|grs?|gramos?|l|lt?s?|litros?|ml|mililitros?|un|u|unidad(?:es)?)\b",
            r"\b(?P<unit>kg|g|l|ml|un|u)\s*(?P<qty>\d+[\.,]?\d*)\b",
        ]

        for pattern in patterns:
            match = re.search(pattern, normalized_name)
            if not match:
                continue

            qty_text = match.group("qty").replace(",", ".")
            try:
                quantity = float(qty_text)
            except ValueError:
                continue

            unit = self._normalize_unit(match.group("unit"))
            if unit in {"kg", "g", "l", "ml", "un"}:
                return {
                    "presentation_quantity": quantity,
                    "presentation_unit": unit,
                }

        return {
            "presentation_quantity": None,
            "presentation_unit": None,
        }

    def _convert_quantity(self, quantity: float, unit: str, target_unit: str) -> Optional[float]:
        """Convert quantity to target unit when conversion is possible."""
        if unit == target_unit:
            return quantity

        if unit == "g" and target_unit == "kg":
            return quantity / 1000
        if unit == "kg" and target_unit == "g":
            return quantity * 1000
        if unit == "ml" and target_unit == "l":
            return quantity / 1000
        if unit == "l" and target_unit == "ml":
            return quantity * 1000
        return None

    def _score_product_match(
        self,
        product: Dict[str, Any],
        basket_item: Dict[str, Any],
    ) -> Tuple[float, Dict[str, Any], Tuple[int, int, int]]:
        """Compute product match score and tie-break tuple for deterministic ranking."""
        keywords = [k.lower() for k in basket_item.get("keywords", [])]
        brand_hints = [b.lower() for b in basket_item.get("brand_hint", [])]
        matching = basket_item.get("matching", "loose")

        product_name = product.get("name", "").lower()
        score = 0.0
        breakdown: Dict[str, Any] = {}

        keyword_matches = sum(1 for kw in keywords if kw in product_name)
        keyword_score = (keyword_matches / len(keywords)) * 0.6 if keywords else 0.0
        score += keyword_score
        breakdown["keyword_score"] = round(keyword_score, 4)

        brand_score = 0.0
        if brand_hints:
            brand_matches = sum(1 for brand in brand_hints if brand in product_name)
            brand_score = (brand_matches / len(brand_hints)) * 0.3
            score += brand_score
        breakdown["brand_score"] = round(brand_score, 4)

        stock_bonus = 0.1 if product.get("in_stock") else 0.0
        score += stock_bonus
        breakdown["stock_bonus"] = round(stock_bonus, 4)

        presentation_bonus = 0.0
        size_penalty = 0.0
        target_unit = self._normalize_unit(basket_item.get("unit"))
        target_quantity = basket_item.get("quantity")

        product_unit = self._normalize_unit(product.get("presentation_unit"))
        product_quantity = product.get("presentation_quantity")

        comparable_quantity = None
        if product_unit and target_unit:
            if product_unit == target_unit:
                presentation_bonus = 0.15
            else:
                converted = self._convert_quantity(float(product_quantity), product_unit, target_unit) if product_quantity is not None else None
                if converted is not None:
                    comparable_quantity = converted
                    presentation_bonus = 0.1
                else:
                    presentation_bonus = -0.2

        score += presentation_bonus
        breakdown["presentation_bonus"] = round(presentation_bonus, 4)

        if target_quantity is not None and product_quantity is not None and target_unit and product_unit:
            try:
                target_quantity_value = float(target_quantity)
                if comparable_quantity is None:
                    comparable_quantity = self._convert_quantity(float(product_quantity), product_unit, target_unit)
                if comparable_quantity is None and product_unit == target_unit:
                    comparable_quantity = float(product_quantity)

                if comparable_quantity is not None and target_quantity_value > 0:
                    relative_delta = abs(comparable_quantity - target_quantity_value) / target_quantity_value
                    qty_score = max(0.0, 0.2 * (1 - relative_delta))
                    score += qty_score
                    breakdown["quantity_bonus"] = round(qty_score, 4)

                    if relative_delta >= 0.5:
                        size_penalty = min(0.3, 0.3 * relative_delta)
                        score -= size_penalty
                else:
                    breakdown["quantity_bonus"] = 0.0
            except (TypeError, ValueError):
                breakdown["quantity_bonus"] = 0.0
        else:
            breakdown["quantity_bonus"] = 0.0

        if size_penalty:
            breakdown["large_size_penalty"] = round(size_penalty, 4)

        if not product.get("url_valid", False):
            score *= 0.3
            breakdown["url_penalty_multiplier"] = 0.3

        if matching == "strict" and keywords and keyword_matches < len(keywords):
            breakdown["strict_matching_rejected"] = True
            logger.debug(
                "Score breakdown {}: {} (strict mismatch)",
                product.get("name", "<sin nombre>"),
                breakdown,
            )
            return 0.0, breakdown, (0, 0, 0)

        tie_break = (
            1 if product.get("in_stock") else 0,
            1 if self._is_valid_product_url(product.get("url")) else 0,
            1 if product.get("price") is not None else 0,
        )

        logger.debug("Score breakdown {}: {}", product.get("name", "<sin nombre>"), breakdown)
        return score, breakdown, tie_break
    
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
            def _first_text(selectors: List[str]) -> str:
                last_error = None
                for selector in selectors:
                    if not selector:
                        continue
                    try:
                        text = product_element.locator(selector).inner_text()
                        if text:
                            return text
                    except Exception as exc:
                        last_error = exc
                        continue

                raise last_error or ValueError("No selector returned text")

            # Product name
            name = _first_text([
                self._get_selector("product_name"),
                ".nombre-producto",
                ".product-name",
            ])
            
            # Price
            price_text = _first_text([
                self._get_selector("product_price"),
                ".precio-actual",
                ".price",
            ])
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
            url_valid = False
            for url_selector in [
                self._get_selector("product_url"),
                "a[href*='producto']",
                "a[href*='art_']",
            ]:
                if not url_selector:
                    continue
                try:
                    raw_url = product_element.locator(url_selector).first.get_attribute("href")
                    normalized_url = urljoin("https://www.laanonima.com.ar/", raw_url or "")
                    url = self._canonical_product_url(normalized_url)
                    url_valid = self._is_valid_product_url(url)
                    if url:
                        break
                except Exception:
                    continue
            
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
            presentation = self._parse_presentation_from_name(name)
            
            return {
                "name": name.strip(),
                "price": price,
                "original_price": old_price,
                "price_per_unit": unit_price,
                "size": (
                    f"{presentation['presentation_quantity']} {presentation['presentation_unit']}"
                    if presentation["presentation_quantity"] is not None and presentation["presentation_unit"]
                    else None
                ),
                "presentation_quantity": presentation["presentation_quantity"],
                "presentation_unit": presentation["presentation_unit"],
                "url": url,
                "url_valid": url_valid,
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
        
        best_match = None
        best_score = 0.0
        best_tie_break = (-1, -1, -1)
        
        for product in search_results:
            score, breakdown, tie_break = self._score_product_match(product, basket_item)
            logger.debug(
                "Match candidate '{}': score={:.4f} tie_break={} breakdown={}",
                product.get("name", "<sin nombre>"),
                score,
                tie_break,
                breakdown,
            )

            if score > best_score or (score == best_score and tie_break > best_tie_break):
                best_score = score
                best_tie_break = tie_break
                best_match = product
        
        return best_match, best_score

    def open_selected_product(
        self,
        search_results: List[Dict[str, Any]],
        basket_item: Dict[str, Any],
    ) -> Tuple[Optional[Dict[str, Any]], float, str]:
        """Open top candidate product detail and verify pricing data.

        If detail verification fails for the best candidate (navigation/click error
        or missing detail price), this method retries with the second-best result.

        Returns:
            Tuple of (selected_product, confidence_score, match_method)
        """
        if not self.page:
            raise RuntimeError("Browser not started")

        if not search_results:
            return None, 0.0, "list_match"

        ranked = sorted(
            (
                (product, *self._score_product_match(product, basket_item))
                for product in search_results
            ),
            key=lambda item: (item[1], item[3]),
            reverse=True,
        )
        ranked = [item for item in ranked if item[1] > 0]
        if not ranked:
            return None, 0.0, "list_match"

        # Keep list-level fallback in case detail verification fails for top candidates.
        list_candidate, list_confidence, _, _ = ranked[0]
        list_method = "list_match"

        def _first_text(selectors: List[str]) -> Optional[str]:
            for selector in selectors:
                try:
                    locator = self.page.locator(selector).first
                    if locator.count() > 0 and locator.is_visible(timeout=1500):
                        text = locator.inner_text().strip()
                        if text:
                            return text
                except Exception:
                    continue
            return None

        def _open_and_extract_detail(product: Dict[str, Any]) -> Optional[Dict[str, Any]]:
            product_url = product.get("url")
            opened = False

            if product_url:
                try:
                    self.page.goto(product_url, wait_until="domcontentloaded", timeout=self.timeout)
                    opened = True
                except Exception as nav_error:
                    logger.warning(f"Failed opening product URL '{product_url}': {nav_error}")

            if not opened:
                try:
                    product_name = product.get("name", "")
                    safe_name = product_name.replace("'", "\\'")
                    click_selector = (
                        f".producto:has-text('{safe_name}') a[href*='art_'], "
                        f".producto:has-text('{safe_name}') a[href*='producto']"
                    )
                    self.page.locator(click_selector).first.click(timeout=5000)
                    self.page.wait_for_load_state("domcontentloaded", timeout=self.timeout)
                    opened = True
                except Exception as click_error:
                    logger.warning(f"Failed opening product by click '{product.get('name')}': {click_error}")

            if not opened:
                return None

            try:
                self.page.wait_for_selector(
                    ", ".join([
                        ".ficha-producto",
                        ".detalle-producto",
                        ".producto-detalle",
                        "h1.nombre-producto",
                    ]),
                    timeout=7000,
                    state="attached",
                )
            except Exception:
                pass

            detail_name = _first_text([
                "h1.nombre-producto",
                ".detalle-producto h1",
                "h1[itemprop='name']",
                ".ficha-producto .nombre-producto",
            ])
            detail_price_text = _first_text([
                ".detalle-producto .precio-actual",
                ".ficha-producto .precio-actual",
                ".contenedor-precios .precio-actual",
                "[itemprop='price']",
            ])
            detail_unit_price_text = _first_text([
                ".detalle-producto .precio-unitario",
                ".ficha-producto .precio-unitario",
                ".contenedor-precios .precio-unitario",
                ".precio-por-unidad",
            ])

            detail_price = self._parse_price(detail_price_text) if detail_price_text else None
            detail_unit_price = self._parse_price(detail_unit_price_text) if detail_unit_price_text else None

            if detail_price is None:
                return None

            detail_oos_selectors = [
                ".sin-stock",
                ".agotado",
                "button:has-text('Sin stock')",
                "[data-stock='0']",
            ]
            in_stock = True
            for selector in detail_oos_selectors:
                try:
                    if self.page.locator(selector).count() > 0:
                        in_stock = False
                        break
                except Exception:
                    continue

            verified = dict(product)
            verified["name"] = (detail_name or product.get("name", "")).strip()
            verified["price"] = detail_price
            verified["price_per_unit"] = detail_unit_price
            verified["in_stock"] = in_stock
            verified_url = self._canonical_product_url(self.page.url or product_url)
            verified["url"] = verified_url
            verified["url_valid"] = self._is_valid_product_url(verified_url)

            if not verified["url_valid"]:
                return None
            return verified

        for product, confidence in ranked[:2]:
            detailed_product = _open_and_extract_detail(product)
            if detailed_product:
                return detailed_product, confidence, "detail_verified"

        if list_candidate.get("price") is not None:
            return list_candidate, list_confidence, list_method
        return None, list_confidence, list_method


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
                    
                    # Match and verify product on detail page (with fallback to 2nd best)
                    match, confidence, match_method = scraper.open_selected_product(search_results, item)

                    # Normalize and validate canonical URL before confidence threshold.
                    if match:
                        canonical_url = scraper._canonical_product_url(match.get("url"))
                        match["url"] = canonical_url
                        if not scraper._is_valid_product_url(canonical_url):
                            confidence *= 0.3

                    # Skip if no valid match or no price (so we keep a clean time series per product)
                    if not match or confidence < 0.3:
                        raise ProductNotFoundError(
                            f"No acceptable match for {item_name} (confidence={confidence:.2f})"
                        )
                    price_value = match.get("price")
                    if price_value is None:
                        raise ProductNotFoundError(f"No price found for {item_name}")

                    # Resolve canonical business category/rubro
                    raw_category = item.get("category")
                    canonical_slug = resolve_canonical_category(config, raw_category)
                    display_labels = get_category_display_names(config)
                    category_obj = None
                    if canonical_slug:
                        category_obj = session.query(Category).filter_by(slug=canonical_slug).first()
                        if not category_obj:
                            category_obj = Category(
                                slug=canonical_slug,
                                name=display_labels.get(canonical_slug, canonical_slug.replace("_", " ").title()),
                                description=f"Rubro canónico para '{raw_category}'",
                            )
                            session.add(category_obj)
                            session.flush()

                    # Get or create product record (one per canonical_id for history)
                    product = session.query(Product).filter_by(canonical_id=item_id).first()
                    if not product:
                        product = Product(
                            canonical_id=item_id,
                            basket_id=item.get("basket_type", "cba"),
                            name=item_name,
                            category=raw_category,
                            category_id=category_obj.id if category_obj else None,
                            unit=item.get("unit"),
                            quantity=item.get("quantity"),
                            keywords=",".join(keywords),
                            brand_hint=",".join(item.get("brand_hint", [])),
                            matching_rules=item.get("matching", "loose"),
                            signature_name=match.get("name"),
                        )
                        session.add(product)
                        session.flush()
                    elif category_obj and product.category_id != category_obj.id:
                        product.category_id = category_obj.id

                    # Create price record for this run (one row per product per run = time series)
                    price_record = Price(
                        product_id=product.id,
                        category_id=product.category_id,
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
                        match_method=match_method,
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
