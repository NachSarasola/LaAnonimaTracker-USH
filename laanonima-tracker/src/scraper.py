"""Playwright-based scraper for La Anonima supermarket."""

import json
import re
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from time import perf_counter
from typing import Dict, List, Optional, Tuple, Any
from urllib.parse import quote, urljoin, urlsplit, urlunsplit

from loguru import logger
from playwright.sync_api import Error as PlaywrightError, Page, sync_playwright, TimeoutError as PlaywrightTimeout
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from src.basket_planner import build_scrape_plan
from src.config_loader import (
    load_config,
    get_branch_config,
    get_scraping_config,
    resolve_canonical_category,
    get_category_display_names,
)
from src.models import (
    Product, Price, PriceCandidate, ScrapeRun, ScrapeError, Category,
    get_engine, init_db, get_session_factory
)


def _utcnow_naive() -> datetime:
    """Return UTC datetime without tzinfo for DB compatibility."""
    return datetime.utcnow()


class BranchSelectionError(Exception):
    """Raised when branch selection fails."""
    pass


class ProductNotFoundError(Exception):
    """Raised when a product cannot be found."""
    pass


class LaAnonimaScraper:
    """Main scraper class for La Anonima supermarket."""
    
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
        self.navigation_timeout = config.get("website", {}).get("navigation_timeout", self.timeout)
        self.branch_selection_timeout = self.scraping_config.get("branch_selection_timeout", 10000)
        self.retry_attempts = config.get("website", {}).get("retry_attempts", 3)
        candidates_cfg = self.scraping_config.get("candidates", {})
        min_candidates_cfg = candidates_cfg.get(
            "min_candidates_per_product",
            self.scraping_config.get("min_candidates_per_product", 3),
        )
        self.min_candidates_per_product = max(3, int(min_candidates_cfg))
        self.max_results_per_search = max(
            self.min_candidates_per_product,
            int(self.scraping_config.get("max_results_per_search", 12)),
        )
        self.min_match_confidence = float(self.scraping_config.get("min_match_confidence", 0.2))
        self.quick_selector_timeout_ms = int(self.scraping_config.get("quick_selector_timeout_ms", 250))
        perf_cfg = self.scraping_config.get("performance", {})
        if not isinstance(perf_cfg, dict):
            perf_cfg = {}
        self.search_settle_delay_ms = int(
            perf_cfg.get(
                "search_settle_delay_ms",
                self.scraping_config.get("search_settle_delay_ms", 700),
            )
        )
        
        self.headless = headless if headless is not None else self.scraping_config.get("browser", {}).get("headless", True)
        
        # Selectors
        self.selectors = self.scraping_config.get("selectors", {})
        
        # State
        self.page: Optional[Page] = None
        self.playwright = None
        self.browser = None
        self.context = None
        self.current_branch: Optional[str] = None
        self._branch_attempt = 0
        
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
            try:
                self.context.close()
            except Exception:
                pass
        if self.browser:
            try:
                self.browser.close()
            except Exception:
                pass
        if self.playwright:
            try:
                self.playwright.stop()
            except Exception:
                pass
        self.page = None
        self.context = None
        self.browser = None
        self.playwright = None
        logger.info("Browser stopped")

    @staticmethod
    def _is_closed_target_error(exc: Exception) -> bool:
        text = str(exc).lower()
        if isinstance(exc, PlaywrightError) and "closed" in text:
            return True
        return "target page, context or browser has been closed" in text or "target closed" in text

    def _ensure_browser_session(self):
        """Recover page/context when Playwright target got closed between retries."""
        should_restart = self.page is None

        if self.page is not None:
            try:
                should_restart = self.page.is_closed()
            except Exception:
                should_restart = True

        if should_restart:
            logger.warning("Browser page not available; restarting browser session before retry.")
            self.stop()
            self.start()
    
    def __enter__(self):
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
    
    def _get_selector(self, key: str) -> str:
        """Get a CSS selector from config."""
        return self.selectors.get(key, "")

    def _detect_anti_bot_marker(self) -> Optional[str]:
        """Return marker text when landing page appears to be a bot challenge."""
        if not self.page:
            return None
        text_chunks = []
        try:
            text_chunks.append((self.page.title() or "").lower())
        except Exception:
            pass
        try:
            body_text = self.page.locator("body").first.inner_text(timeout=1200)
            text_chunks.append((body_text or "").lower())
        except Exception:
            pass
        combined = " ".join(text_chunks)
        if not combined:
            return None
        markers = [
            "just a moment",
            "verify you are human",
            "checking your browser",
            "attention required",
            "cf-challenge",
            "enable javascript and cookies",
            "captcha",
        ]
        for marker in markers:
            if marker in combined:
                return marker
        return None

    @staticmethod
    def _canonical_product_url(url: Optional[str]) -> str:
        """Canonicalize product URL by dropping query params and fragments."""
        if not url:
            return ""

        parsed = urlsplit(url)
        return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))

    def _is_valid_product_url(self, url: Optional[str]) -> bool:
        """Validate La Anonima product URL format."""
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
        self._branch_attempt += 1
        self._ensure_browser_session()
        if not self.page:
            raise BranchSelectionError("Browser not started")
        
        try:
            # Look for branch indicator in the page
            # Try multiple possible selectors
            selectors_to_try = [
                self._get_selector("current_branch_label"),
                ".sucursal-actual",
                ".sucursal",
                ".branch-name",
                "[data-branch-name]",
                ".sucursal-seleccionada",
                ".header-sucursal",
            ]
            
            for selector in selectors_to_try:
                try:
                    if not selector:
                        continue
                    element = self.page.locator(selector).first
                    if element.is_visible(timeout=2000):
                        text = element.inner_text()
                        if text:
                            logger.debug(f"Found branch indicator: {text}")
                            target_branch = self.branch_config.get("branch_name", "USHUAIA 5")
                            target_postal_code = str(self.branch_config.get("postal_code", "")).strip()
                            target_city = target_branch.split()[0].strip().lower() if target_branch else ""
                            text_lower = text.lower()
                            if (
                                target_branch.lower() in text_lower
                                or (target_city and target_city in text_lower)
                                or (target_postal_code and target_postal_code in text)
                            ):
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
        
        logger.info(
            f"Selecting branch attempt {self._branch_attempt}/3: {branch_name} (CP: {postal_code})"
        )
        
        # Navigate to base URL first (timeout to avoid hanging on slow site)
        logger.info(f"Navigating to {self.base_url}")
        self.page.goto(
            self.base_url,
            wait_until="domcontentloaded",
            timeout=self.navigation_timeout,
        )
        try:
            self.page.wait_for_load_state("networkidle", timeout=1500)
        except Exception:
            pass

        # Dismiss any modal/overlay that blocks clicks (e.g. cookie or promo)
        try:
            overlay = self.page.locator(".reveal-overlay").first
            if overlay.is_visible(timeout=min(1000, self.branch_selection_timeout)):
                overlay.click(force=True)
                self.page.wait_for_timeout(80)
        except Exception:
            pass
        try:
            self.page.keyboard.press("Escape")
            self.page.wait_for_timeout(50)
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
            input_selector = self._get_selector("postal_input") or "#idCodigoPostalUnificado"
            postal_input = self.page.locator(input_selector).first
            quick_timeout = min(self.branch_selection_timeout, max(1200, self.quick_selector_timeout_ms * 6))
            input_visible = False
            input_attached = False

            try:
                input_visible = postal_input.is_visible(timeout=quick_timeout)
            except Exception:
                input_visible = False

            try:
                input_attached = postal_input.count() > 0
            except Exception:
                input_attached = False

            # If input is not already available, try multiple triggers to open the modal.
            if not input_visible and not input_attached:
                trigger_selectors = []
                configured_trigger = self._get_selector("branch_trigger")
                if configured_trigger:
                    trigger_selectors.append(configured_trigger)
                trigger_selectors.extend(
                    [
                        "a[data-toggle='codigo-postal']",
                        "[data-toggle='codigo-postal']",
                        ".seleccionar-sucursal",
                        "[data-target='#codigo-postal']",
                        "a:has-text('Codigo Postal')",
                        "a:has-text('Código Postal')",
                        "a:has-text('Sucursal')",
                        "text=Estás en sucursal",
                        "text=Estas en sucursal",
                    ]
                )
                unique_trigger_selectors = list(dict.fromkeys([s for s in trigger_selectors if s]))
                logger.info(f"Trying branch trigger selectors: {', '.join(unique_trigger_selectors)}")

                trigger_opened = False
                for selector in unique_trigger_selectors:
                    try:
                        candidate = self.page.locator(selector).first
                        if candidate.count() == 0:
                            continue
                        candidate.click(force=True, timeout=quick_timeout)
                        trigger_opened = True
                        logger.info(f"Branch selector clicked via: {selector}")
                        break
                    except Exception:
                        continue

                if not trigger_opened:
                    trigger_opened = self.page.evaluate(
                        """(selectors) => {
                            for (const selector of selectors) {
                                const el = document.querySelector(selector);
                                if (el) {
                                    el.click();
                                    return true;
                                }
                            }
                            const modal = document.querySelector("#codigo-postal");
                            if (modal) {
                                modal.classList.add("is-open");
                                modal.style.display = "block";
                                return true;
                            }
                            return false;
                        }""",
                        unique_trigger_selectors,
                    )
                    if trigger_opened:
                        logger.info("Branch selector opened via JS fallback")

                if trigger_opened:
                    self.page.wait_for_timeout(120)

                postal_input = self.page.locator(input_selector).first
                try:
                    input_attached = postal_input.count() > 0
                except Exception:
                    input_attached = False
                try:
                    input_visible = postal_input.is_visible(timeout=quick_timeout if trigger_opened else 800)
                except Exception:
                    input_visible = False

                if not input_attached:
                    anti_bot_marker = self._detect_anti_bot_marker()
                    if anti_bot_marker:
                        raise BranchSelectionError(
                            f"Landing page appears blocked by anti-bot challenge ({anti_bot_marker})"
                        )
                    raise BranchSelectionError(
                        "Postal input not found after trying all branch trigger selectors"
                    )

            logger.info(f"Filling postal code: {postal_code}")
            if input_visible:
                postal_input.fill(postal_code, timeout=5000)
                postal_input.press("Tab")
            else:
                set_ok = self.page.evaluate(
                    """([selector, value]) => {
                        const input = document.querySelector(selector);
                        if (!input) return false;
                        input.value = value;
                        input.dispatchEvent(new Event("input", { bubbles: true }));
                        input.dispatchEvent(new Event("change", { bubbles: true }));
                        return true;
                    }""",
                    [input_selector, postal_code],
                )
                if not set_ok:
                    raise BranchSelectionError("Postal input not available to set branch")
            self.page.wait_for_timeout(180)

            # Wait for branch options in DOM (may be hidden)
            options_selector = self._get_selector("branch_options") or "#opcionesSucursal"
            radio_selector = self._get_selector("branch_radio") or "input[name='sucursalSuper']"
            self.page.wait_for_selector(
                f"{options_selector} {radio_selector}",
                timeout=self.branch_selection_timeout,
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
            try:
                self.page.wait_for_load_state("networkidle", timeout=3000)
            except Exception:
                self.page.wait_for_timeout(180)

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
            logger.error(f"Branch selection failed (attempt {self._branch_attempt}): {e}")
            # Take screenshot for debugging
            try:
                screenshot_path = f"data/logs/branch_error_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
                self.page.screenshot(path=screenshot_path)
                logger.info(f"Error screenshot saved to {screenshot_path}")
            except:
                pass
            if self._is_closed_target_error(e):
                self._ensure_browser_session()
            raise BranchSelectionError(f"Failed to select branch: {e}")

    def _wait_for_search_settle(self) -> None:
        """Wait briefly for product cards or empty-search markers."""
        if not self.page:
            return
        markers = [
            self._get_selector("product_list"),
            ".producto-item",
            ".listado .producto-item",
            ".producto",
            ".sin-resultado",
            ".resultado-vacio",
            "text=No hay productos",
            "text=No se obtuvieron resultados",
        ]
        selector = ", ".join([m for m in markers if m])
        try:
            self.page.wait_for_selector(
                selector,
                timeout=max(900, self.quick_selector_timeout_ms * 8),
                state="attached",
            )
        except Exception:
            pass
        if self.search_settle_delay_ms > 0:
            self.page.wait_for_timeout(self.search_settle_delay_ms)
    
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
        seen = set()
        
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
                        timeout=self.navigation_timeout,
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
                        timeout=self.navigation_timeout,
                    )

                    search_selector = self._get_selector("search_input") or "#idBuscarProducto"
                    search_input = self.page.locator(search_selector).first
                    search_input.fill("")
                    search_input.fill(keyword)
                    search_input.press("Enter")
                    logger.info(f"Search strategy used: {strategy_used}")
                
                # Wait briefly for either product cards or empty markers.
                self._wait_for_search_settle()
                
                # Parse results with selector fallback (site changed markup over time)
                selector_candidates = [
                    self._get_selector("product_list"),
                    ".producto-item",
                    ".listado .producto-item",
                    ".producto",
                ]
                products = []
                for selector in selector_candidates:
                    if not selector:
                        continue
                    matches = self.page.locator(selector).all()
                    if matches:
                        products = matches
                        break

                logger.info(f"Found {len(products)} products for '{keyword}'")
                
                for product in products[: self.max_results_per_search]:
                    try:
                        product_data = self._parse_product(product)
                        if product_data:
                            key = (product_data.get("url") or "", product_data.get("name") or "")
                            if key in seen:
                                continue
                            seen.add(key)
                            results.append(product_data)
                    except Exception as e:
                        logger.debug(f"Error parsing product: {e}")
                
                if len(results) >= self.min_candidates_per_product:
                    break  # Enough data for tiered selection
                    
            except Exception as e:
                logger.warning(f"Search failed for '{keyword}': {e}")
                continue
        
        return results
    
    def _parse_product(self, product_element) -> Optional[Dict[str, Any]]:
        """Parse a product element into data dictionary."""
        try:
            def _fast_inner_text(selector: str, timeout_ms: Optional[int] = None) -> Optional[str]:
                if not selector:
                    return None
                effective_timeout = timeout_ms or self.quick_selector_timeout_ms
                try:
                    locator = product_element.locator(selector).first
                    try:
                        text = locator.inner_text(timeout=effective_timeout)
                    except TypeError:
                        # Test doubles may not accept timeout kwarg.
                        text = locator.inner_text()
                    text = (text or "").strip()
                    return text or None
                except Exception:
                    return None

            def _fast_attr(selector: str, attr_name: str, timeout_ms: Optional[int] = None) -> Optional[str]:
                if not selector:
                    return None
                effective_timeout = timeout_ms or self.quick_selector_timeout_ms
                try:
                    locator = product_element.locator(selector).first
                    try:
                        value = locator.get_attribute(attr_name, timeout=effective_timeout)
                    except TypeError:
                        value = locator.get_attribute(attr_name)
                    return value
                except Exception:
                    return None

            def _first_text(selectors: List[str]) -> str:
                for selector in selectors:
                    text = _fast_inner_text(selector)
                    if text:
                        return text
                raise ValueError("No selector returned text")

            # Product name
            name = _first_text([
                self._get_selector("product_name"),
                ".titulo",
                "h2",
                ".nombre-producto",
                ".product-name",
            ])
            
            # Price
            price_text = _first_text([
                self._get_selector("product_price"),
                ".precio",
                ".precio.plus",
                ".precio-actual",
                ".price",
            ])
            price = self._parse_price(price_text)
            if price is None:
                try:
                    try:
                        raw_card_text = product_element.inner_text(timeout=self.quick_selector_timeout_ms)
                    except TypeError:
                        raw_card_text = product_element.inner_text()
                    price = self._parse_price(raw_card_text)
                except Exception:
                    pass
            
            # Original price (for discounts)
            old_price_selector = self._get_selector("product_price_old") or ".precio-anterior"
            old_price = None
            old_price_text = _fast_inner_text(old_price_selector, timeout_ms=120)
            old_price = self._parse_price(old_price_text) if old_price_text else None
            
            # Price per unit
            unit_price_selector = self._get_selector("product_unit_price") or ".precio-unitario"
            unit_price = None
            unit_price_text = _fast_inner_text(unit_price_selector, timeout_ms=120)
            unit_price = self._parse_price(unit_price_text) if unit_price_text else None
            
            # URL
            url = ""
            url_valid = False
            for url_selector in [
                self._get_selector("product_url"),
                "a[href*='producto']",
                "a[href*='art_']",
            ]:
                raw_url = _fast_attr(url_selector, "href", timeout_ms=150)
                if raw_url is None:
                    continue
                normalized_url = urljoin("https://www.laanonima.com.ar/", raw_url or "")
                url = self._canonical_product_url(normalized_url)
                url_valid = self._is_valid_product_url(url)
                if url:
                    break
            
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

    def select_tiered_candidates(
        self,
        search_results: List[Dict[str, Any]],
        basket_item: Dict[str, Any],
        min_candidates: Optional[int] = None,
    ) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
        """Select low/mid/high priced candidates for a query.

        Returns:
            A tuple of (selected_candidates, representative_candidate).
            Each candidate contains: product, confidence, tie_break, tier.
        """
        if not search_results:
            return [], None

        target_n = max(3, min_candidates or self.min_candidates_per_product)
        scored: List[Dict[str, Any]] = []
        scored_urls = set()
        for product in search_results:
            score, _breakdown, tie_break = self._score_product_match(product, basket_item)
            price = product.get("price")
            if score <= 0 or price is None:
                continue
            scored_urls.add(product.get("url") or "")
            scored.append(
                {
                    "product": product,
                    "confidence": float(score),
                    "tie_break": tie_break,
                    "fallback": False,
                }
            )

        fallback_pool = [
            p
            for p in search_results
            if p.get("price") is not None and (p.get("url") or "") not in scored_urls
        ]
        if not scored and not fallback_pool:
            return [], None

        if not scored and fallback_pool:
            for product in fallback_pool:
                scored.append(
                    {
                        "product": product,
                        "confidence": float(self.min_match_confidence),
                        "tie_break": (0, 0, 0),
                        "fallback": True,
                    }
                )

        # Prefer ranking by price distribution, tie-breaking on confidence.
        scored.sort(
            key=lambda row: (
                row["product"].get("price"),
                -row["confidence"],
                row["product"].get("name", ""),
            )
        )

        selected_positions = {0, len(scored) // 2, len(scored) - 1}
        selected = [scored[idx].copy() for idx in sorted(selected_positions)]

        max_selectable = min(target_n, len(scored))
        if len(selected) < max_selectable and fallback_pool:
            for product in fallback_pool:
                if any(product is s["product"] for s in selected):
                    continue
                selected.append(
                    {
                        "product": product,
                        "confidence": float(self.min_match_confidence),
                        "tie_break": (0, 0, 0),
                        "fallback": True,
                    }
                )
                if len(selected) >= max_selectable:
                    break

        if len(selected) < max_selectable:
            for row in scored:
                if any(row["product"] is s["product"] for s in selected):
                    continue
                selected.append(row.copy())
                if len(selected) >= max_selectable:
                    break

        selected.sort(key=lambda row: row["product"].get("price"))
        for idx, row in enumerate(selected):
            if len(selected) == 1:
                row["tier"] = "single"
            elif idx == 0:
                row["tier"] = "low"
            elif idx == len(selected) - 1:
                row["tier"] = "high"
            elif idx == len(selected) // 2:
                row["tier"] = "mid"
            else:
                row["tier"] = "mid"

        representative = selected[len(selected) // 2]
        return selected, representative

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
                    self.page.goto(product_url, wait_until="domcontentloaded", timeout=self.navigation_timeout)
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
                    self.page.wait_for_load_state("domcontentloaded", timeout=self.navigation_timeout)
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

        for product, confidence, _breakdown, _tie_break in ranked[:2]:
            detailed_product = _open_and_extract_detail(product)
            if detailed_product:
                return detailed_product, confidence, "detail_verified"

        if list_candidate.get("price") is not None:
            return list_candidate, list_confidence, list_method
        return None, list_confidence, list_method


def _save_candidate_audit_json(run_uuid: str, records: List[Dict[str, Any]]) -> Optional[str]:
    if not records:
        return None
    out_dir = Path("data/analysis/scrape_audits")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"candidates_{run_uuid}.json"
    out_path.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(out_path)


def _to_decimal_or_none(value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (TypeError, ValueError, InvalidOperation):
        return None


def run_scrape(
    config_path: Optional[str] = None,
    basket_type: str = "cba",
    headless: bool = True,
    output_format: str = "sqlite",
    limit: Optional[int] = None,
    profile: str = "balanced",
    runtime_budget_minutes: Optional[int] = None,
    rotation_items: Optional[int] = None,
    sample_random: bool = False,
    dry_plan: bool = False,
    candidate_storage: str = "db",
    observation_policy: str = "single+audit",
    commit_batch_size: Optional[int] = None,
    base_request_delay_ms: Optional[int] = None,
    fail_fast_min_attempts: Optional[int] = None,
    fail_fast_fail_ratio: Optional[float] = None,
) -> Dict[str, Any]:
    """Run a complete scrape operation.

    Args:
        config_path: Path to config file
        basket_type: Type of basket to scrape ('cba', 'extended', 'all')
        headless: Run browser in headless mode
        output_format: Output format ('sqlite', 'postgresql')
        limit: If set, only scrape this many products deterministically
        profile: Planning profile ('balanced', 'full', 'cba_only')
        runtime_budget_minutes: Runtime budget for plan builder
        rotation_items: Max number of rotation items in balanced profile
        sample_random: Enable explicit random sample mode (debug only)
        dry_plan: Return deterministic plan without running browser
        candidate_storage: Candidate persistence mode ('json', 'db', 'off')
        observation_policy: Observation policy ('single', 'single+audit')
        commit_batch_size: Number of processed items per transaction
        base_request_delay_ms: Delay between items in milliseconds
        fail_fast_min_attempts: Min attempts before fail-fast is evaluated
        fail_fast_fail_ratio: Fail ratio threshold for fail-fast [0..1]

    Returns:
        Dictionary with scrape results and statistics
    """
    valid_candidate_storage = {"json", "db", "off"}
    valid_observation_policies = {"single", "single+audit"}
    candidate_storage = (candidate_storage or "json").lower()
    observation_policy = (observation_policy or "single+audit").lower()

    if candidate_storage not in valid_candidate_storage:
        raise ValueError("candidate_storage invalido: use json, db o off")
    if observation_policy not in valid_observation_policies:
        raise ValueError("observation_policy invalido: use single o single+audit")

    config = load_config(config_path)
    scraping_cfg = config.get("scraping", {})
    if not isinstance(scraping_cfg, dict):
        scraping_cfg = {}
    planning_cfg = scraping_cfg.get("planning", {})
    if not isinstance(planning_cfg, dict):
        planning_cfg = {}
    perf_cfg = scraping_cfg.get("performance", {})
    if not isinstance(perf_cfg, dict):
        perf_cfg = {}

    if runtime_budget_minutes is None:
        runtime_budget_minutes = planning_cfg.get("runtime_budget_minutes", 20)

    if rotation_items is None:
        rotation_items = planning_cfg.get("rotation_items_default", 4)

    if commit_batch_size is None:
        commit_batch_size = int(perf_cfg.get("commit_batch_size", 12))
    commit_batch_size = max(1, int(commit_batch_size))

    if base_request_delay_ms is None:
        base_request_delay_ms = int(
            perf_cfg.get("base_request_delay_ms", scraping_cfg.get("request_delay", 1000))
        )
    base_request_delay_ms = max(0, int(base_request_delay_ms))

    if fail_fast_min_attempts is None:
        fail_fast_min_attempts = int(perf_cfg.get("fail_fast_min_attempts", 8))
    fail_fast_min_attempts = max(1, int(fail_fast_min_attempts))

    if fail_fast_fail_ratio is None:
        fail_fast_fail_ratio = float(perf_cfg.get("fail_fast_fail_ratio", 0.85))
    fail_fast_fail_ratio = max(0.0, min(1.0, float(fail_fast_fail_ratio)))

    if candidate_storage == "json":
        candidate_storage = scraping_cfg.get("candidates", {}).get("storage_mode", "json")
        candidate_storage = str(candidate_storage or "json").lower()
        if candidate_storage not in valid_candidate_storage:
            candidate_storage = "json"

    should_audit_candidates = observation_policy == "single+audit" and candidate_storage in {"json", "db"}
    if observation_policy == "single":
        candidate_storage = "off"

    engine = get_engine(config, output_format)
    init_db(engine)
    Session = get_session_factory(engine)
    session = Session()

    scrape_run: Optional[ScrapeRun] = None
    run_uuid: Optional[str] = None

    try:
        plan = build_scrape_plan(
            config=config,
            session=session,
            basket_type=basket_type,
            profile=profile,
            runtime_budget_minutes=runtime_budget_minutes,
            rotation_items=rotation_items,
            limit=limit,
            sample_random=sample_random,
        )
        planned_items = list(plan.planned_items)
        mandatory_ids = set(plan.mandatory_ids)
        plan_summary = dict(plan.plan_summary)
        budget = dict(plan.budget)

        coverage_by_segment: Dict[str, Dict[str, int]] = {}
        for segment, planned_count in plan_summary.get("segments", {}).items():
            coverage_by_segment[segment] = {
                "planned": int(planned_count),
                "scraped": 0,
                "failed": 0,
                "skipped": 0,
            }

        performance_meta = {
            "commit_batch_size": commit_batch_size,
            "base_request_delay_ms": base_request_delay_ms,
            "fail_fast_min_attempts": fail_fast_min_attempts,
            "fail_fast_fail_ratio": fail_fast_fail_ratio,
        }

        if dry_plan:
            return {
                "run_uuid": None,
                "status": "planned",
                "dry_plan": True,
                "started_at": datetime.now(timezone.utc).isoformat(),
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "products_planned": len(planned_items),
                "products_scraped": 0,
                "products_failed": 0,
                "products_skipped": 0,
                "errors": [],
                "plan_summary": plan_summary,
                "coverage_by_segment": coverage_by_segment,
                "budget": {
                    **budget,
                    "actual_seconds": 0,
                    "within_target": bool(budget.get("estimated_within_target", True)),
                },
                "observation_policy": observation_policy,
                "candidate_storage_mode": candidate_storage,
                "candidates_audit_path": None,
                "performance": performance_meta,
            }

        run_uuid = str(uuid.uuid4())
        branch_config = get_branch_config(config)

        scrape_run = ScrapeRun(
            run_uuid=run_uuid,
            branch_id=branch_config.get("branch_id", "75"),
            branch_name=branch_config.get("branch_name", "USHUAIA 5"),
            postal_code=branch_config.get("postal_code", "9410"),
            basket_type=basket_type,
            status="running",
            scraper_version="1.0.0",
            started_at=_utcnow_naive(),
            products_planned=len(planned_items),
        )
        session.add(scrape_run)
        session.commit()

        category_cache: Dict[str, Category] = {}
        for category in session.query(Category).all():
            slug = str(category.slug or "").strip().lower()
            if slug:
                category_cache[slug] = category

        planned_ids = [
            str(item.get("id") or "").strip()
            for item in planned_items
            if str(item.get("id") or "").strip()
        ]
        product_cache: Dict[str, Product] = {}
        if planned_ids:
            for product in session.query(Product).filter(Product.canonical_id.in_(planned_ids)).all():
                product_cache[str(product.canonical_id)] = product

        display_labels = get_category_display_names(config)

        results = {
            "run_uuid": run_uuid,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "products_planned": len(planned_items),
            "products_scraped": 0,
            "products_failed": 0,
            "products_skipped": 0,
            "errors": [],
            "dry_plan": False,
            "plan_summary": plan_summary,
            "coverage_by_segment": coverage_by_segment,
            "budget": {
                **budget,
                "actual_seconds": None,
                "within_target": None,
            },
            "observation_policy": observation_policy,
            "candidate_storage_mode": candidate_storage,
            "candidates_audit_path": None,
            "performance": performance_meta,
            "fail_fast_triggered": False,
        }

        target_seconds = float(results["budget"].get("target_seconds") or 0)
        scrape_started_perf = perf_counter()
        candidate_audit_rows: List[Dict[str, Any]] = []
        processed_attempts = 0
        items_since_commit = 0

        def _commit_if_needed(force: bool = False) -> None:
            nonlocal items_since_commit
            if force or items_since_commit >= commit_batch_size:
                session.commit()
                items_since_commit = 0

        with LaAnonimaScraper(config, headless=headless) as scraper:
            logger.info("Selecting branch...")
            scraper._branch_attempt = 0
            branch_success = scraper.select_branch()

            if not branch_success:
                raise BranchSelectionError("Failed to select target branch")

            logger.info(
                "Scraping {} products from basket '{}' with profile='{}'",
                len(planned_items),
                basket_type,
                profile,
            )

            for item in planned_items:
                item_id = str(item.get("id") or "unknown")
                item_name = str(item.get("name") or "Unknown")
                segment = str(item.get("_plan_segment", "other"))
                segment_metrics = results["coverage_by_segment"].setdefault(
                    segment,
                    {"planned": 0, "scraped": 0, "failed": 0, "skipped": 0},
                )

                elapsed_seconds = perf_counter() - scrape_started_perf
                if target_seconds > 0 and elapsed_seconds > target_seconds and item_id not in mandatory_ids:
                    logger.warning(
                        "Skipping optional item due runtime budget: {} ({}) elapsed={}s target={}s",
                        item_name,
                        item_id,
                        int(elapsed_seconds),
                        int(target_seconds),
                    )
                    results["products_skipped"] += 1
                    scrape_run.products_skipped += 1
                    segment_metrics["skipped"] += 1
                    items_since_commit += 1
                    _commit_if_needed()
                    continue

                try:
                    logger.info(f"Processing: {item_name} ({item_id})")

                    keywords = item.get("keywords", [item_name])
                    search_results = scraper.search_product(keywords)
                    if not search_results:
                        raise ProductNotFoundError(f"No results for {item_name}")

                    tiered_candidates, representative = scraper.select_tiered_candidates(
                        search_results,
                        item,
                        min_candidates=scraper.min_candidates_per_product,
                    )
                    if not representative:
                        raise ProductNotFoundError(f"No acceptable candidates for {item_name}")

                    match = representative["product"]
                    confidence = float(representative["confidence"])
                    if confidence < scraper.min_match_confidence:
                        logger.warning(
                            f"{item_name}: boosting representative confidence "
                            f"{confidence:.2f} -> {scraper.min_match_confidence:.2f} for runnable mode"
                        )
                        confidence = float(scraper.min_match_confidence)
                    match_method = "tiered_listing"

                    if len(tiered_candidates) < scraper.min_candidates_per_product:
                        logger.warning(
                            f"{item_name}: only {len(tiered_candidates)} candidates found "
                            f"(target={scraper.min_candidates_per_product})"
                        )
                    else:
                        tier_summary = ", ".join(
                            f"{cand['tier']}={cand['product'].get('price')}{'*' if cand.get('fallback') else ''}"
                            for cand in tiered_candidates
                        )
                        logger.info(f"{item_name}: tiered candidates selected ({tier_summary})")

                    if should_audit_candidates:
                        for rank, cand in enumerate(tiered_candidates, start=1):
                            cand_product = cand.get("product", {})
                            candidate_url = scraper._canonical_product_url(cand_product.get("url"))
                            selected = bool(cand is representative)
                            row = {
                                "run_uuid": run_uuid,
                                "canonical_id": item_id,
                                "basket_id": item.get("basket_type", "cba"),
                                "product_id": item_id,
                                "product_name": item_name,
                                "tier": cand.get("tier", "mid"),
                                "candidate_rank": rank,
                                "candidate_price": float(cand_product.get("price")) if cand_product.get("price") is not None else None,
                                "candidate_name": cand_product.get("name"),
                                "candidate_url": candidate_url,
                                "confidence_score": float(cand.get("confidence")) if cand.get("confidence") is not None else None,
                                "is_selected": selected,
                                "is_fallback": bool(cand.get("fallback", False)),
                                "scraped_at": _utcnow_naive().isoformat(),
                            }
                            if candidate_storage == "json":
                                candidate_audit_rows.append(row)
                            elif candidate_storage == "db":
                                session.add(
                                    PriceCandidate(
                                        run_id=scrape_run.id,
                                        canonical_id=item_id,
                                        basket_id=item.get("basket_type", "cba"),
                                        product_id=item_id,
                                        product_name=item_name,
                                        tier=str(cand.get("tier", "mid")),
                                        candidate_rank=rank,
                                        candidate_price=_to_decimal_or_none(cand_product.get("price")),
                                        candidate_name=cand_product.get("name"),
                                        candidate_url=candidate_url,
                                        confidence_score=_to_decimal_or_none(cand.get("confidence")),
                                        is_selected=selected,
                                        is_fallback=bool(cand.get("fallback", False)),
                                        scraped_at=_utcnow_naive(),
                                    )
                                )

                    if match:
                        canonical_url = scraper._canonical_product_url(match.get("url"))
                        match["url"] = canonical_url
                        if not scraper._is_valid_product_url(canonical_url):
                            confidence *= 0.3

                    if not match or confidence < scraper.min_match_confidence:
                        raise ProductNotFoundError(
                            f"No acceptable match for {item_name} (confidence={confidence:.2f})"
                        )
                    price_value = match.get("price")
                    if price_value is None:
                        raise ProductNotFoundError(f"No price found for {item_name}")

                    raw_category = item.get("category")
                    canonical_slug = resolve_canonical_category(config, raw_category)
                    category_obj = None
                    if canonical_slug:
                        canonical_slug = str(canonical_slug).strip().lower()
                        category_obj = category_cache.get(canonical_slug)
                        if not category_obj:
                            category_obj = Category(
                                slug=canonical_slug,
                                name=display_labels.get(canonical_slug, canonical_slug.replace("_", " ").title()),
                                description=f"Rubro canonico para '{raw_category}'",
                            )
                            session.add(category_obj)
                            session.flush([category_obj])
                            category_cache[canonical_slug] = category_obj

                    product = product_cache.get(item_id)
                    if not product:
                        product = Product(
                            canonical_id=item_id,
                            basket_id=item.get("basket_type", "cba"),
                            name=item_name,
                            category=raw_category,
                            canonical_category=category_obj,
                            unit=item.get("unit"),
                            quantity=item.get("quantity"),
                            keywords=",".join(keywords),
                            brand_hint=",".join(item.get("brand_hint", [])),
                            matching_rules=item.get("matching", "loose"),
                            signature_name=match.get("name"),
                        )
                        session.add(product)
                        product_cache[item_id] = product
                    elif category_obj:
                        current_slug = (
                            str(product.canonical_category.slug).strip().lower()
                            if getattr(product, "canonical_category", None) is not None
                            else None
                        )
                        if current_slug != canonical_slug:
                            product.canonical_category = category_obj

                    session.add(
                        Price(
                            product=product,
                            canonical_category=category_obj,
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
                            confidence_score=_to_decimal_or_none(confidence),
                            match_method=match_method,
                            scraped_at=_utcnow_naive(),
                        )
                    )

                    results["products_scraped"] += 1
                    scrape_run.products_scraped += 1
                    segment_metrics["scraped"] += 1

                except Exception as e:
                    logger.error(f"Error processing {item_name}: {e}")
                    session.add(
                        ScrapeError(
                            run_id=scrape_run.id,
                            product_id=item_id,
                            product_name=item_name,
                            stage="scraping",
                            error_type=type(e).__name__,
                            error_message=str(e),
                        )
                    )
                    results["products_failed"] += 1
                    results["errors"].append({"product": item_name, "error": str(e)})
                    scrape_run.products_failed += 1
                    segment_metrics["failed"] += 1

                finally:
                    processed_attempts += 1
                    items_since_commit += 1
                    _commit_if_needed()

                    if (
                        processed_attempts >= fail_fast_min_attempts
                        and results["products_scraped"] == 0
                    ):
                        fail_ratio = (
                            float(results["products_failed"]) / float(processed_attempts)
                            if processed_attempts > 0
                            else 0.0
                        )
                        if fail_ratio >= fail_fast_fail_ratio:
                            results["fail_fast_triggered"] = True
                            _commit_if_needed(force=True)
                            raise RuntimeError(
                                "Fail-fast activado: 0 productos scrapeados tras "
                                f"{processed_attempts} intentos (fallo={fail_ratio:.2f})."
                            )

                    if base_request_delay_ms > 0:
                        time.sleep(base_request_delay_ms / 1000.0)

            _commit_if_needed(force=True)
            if candidate_storage == "json" and should_audit_candidates:
                results["candidates_audit_path"] = _save_candidate_audit_json(run_uuid, candidate_audit_rows)

            scrape_run.status = (
                "completed"
                if results["products_failed"] == 0 and results["products_skipped"] == 0
                else "partial"
            )
            scrape_run.completed_at = _utcnow_naive()
            if scrape_run.started_at:
                started_at = scrape_run.started_at
                completed_at = scrape_run.completed_at
                if started_at.tzinfo is not None:
                    started_at = started_at.astimezone(timezone.utc).replace(tzinfo=None)
                if completed_at.tzinfo is not None:
                    completed_at = completed_at.astimezone(timezone.utc).replace(tzinfo=None)
                scrape_run.duration_seconds = int((completed_at - started_at).total_seconds())
            _commit_if_needed(force=True)

            results["status"] = scrape_run.status
            results["completed_at"] = datetime.utcnow().isoformat()
            actual_seconds = int(scrape_run.duration_seconds or 0)
            results["budget"]["actual_seconds"] = actual_seconds
            if target_seconds > 0:
                results["budget"]["within_target"] = actual_seconds <= target_seconds

    except Exception as e:
        logger.error(f"Scrape failed: {e}")
        try:
            session.rollback()
        except Exception:
            pass

        if scrape_run is not None:
            try:
                persisted_run = (
                    session.query(ScrapeRun)
                    .filter(ScrapeRun.run_uuid == scrape_run.run_uuid)
                    .first()
                )
                target_run = persisted_run or scrape_run
                target_run.status = "failed"
                target_run.completed_at = _utcnow_naive()
                session.commit()
            except Exception:
                session.rollback()

        results = locals().get(
            "results",
            {
                "run_uuid": run_uuid,
                "started_at": datetime.now(timezone.utc).isoformat(),
                "products_planned": 0,
                "products_scraped": 0,
                "products_failed": 0,
                "products_skipped": 0,
                "errors": [],
            },
        )
        results["status"] = "failed"
        results["error"] = str(e)
        raise

    finally:
        session.close()
        try:
            engine.dispose()
        except Exception:
            pass

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


