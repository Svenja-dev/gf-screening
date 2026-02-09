"""Selenium-based downloader for Gesellschafterlisten from handelsregister.de.

Architecture:
    This module provides a Selenium-based scraper that navigates handelsregister.de
    to download DK documents (Gesellschafterlisten / shareholder lists) as PDFs.

    Key components:
    - DownloaderConfig: Dataclass holding all configurable parameters (timeouts,
      delays, magic numbers). Replaces scattered magic numbers throughout the code.
    - RateLimiter: Enforces max requests/hour with optional persistent state via JSON.
    - GesellschafterlistenDownloader: Main scraper class orchestrating browser
      automation via Selenium WebDriver.
    - DownloadResult: Typed result container for download outcomes.

    Flow:
    1. Navigate to extended search page
    2. Fill search form (register type, number, court, Bundesland)
    3. Submit search and select correct result row
    4. Click DK link to open document page
    5. Expand document tree and locate Gesellschafterliste
    6. Download PDF (or extract from ZIP), validate magic bytes

    Rate limiting: Max 55 requests/hour (safety margin below the 60/hour limit).
    Anti-detection: Custom user-agent, disabled automation flags, human-like delays.
"""

from __future__ import annotations

import json
import logging
import random
import re
import time
import traceback
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Tuple

from selenium import webdriver
from selenium.common.exceptions import (
    ElementClickInterceptedException,
    NoSuchElementException,
    StaleElementReferenceException,
    TimeoutException,
)
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

try:
    from webdriver_manager.chrome import ChromeDriverManager
    USE_WEBDRIVER_MANAGER = True
except ImportError:
    USE_WEBDRIVER_MANAGER = False

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class DownloaderConfig:
    """Central configuration for all tuneable parameters.

    Replaces magic numbers scattered throughout the codebase with a single,
    documentable, overridable configuration object.
    """

    base_url: str = "https://www.handelsregister.de/rp_web/erweitertesuche/welcome.xhtml"
    rate_limit_per_hour: int = 55
    download_timeout_seconds: int = 45
    page_load_delay: Tuple[float, float] = (4.0, 6.0)
    search_result_delay: Tuple[float, float] = (5.0, 8.0)
    element_interaction_delay: Tuple[float, float] = (1.0, 2.0)
    tree_expansion_delay: Tuple[float, float] = (0.5, 1.0)
    tree_expansion_long_delay: Tuple[float, float] = (2.0, 3.0)
    dk_page_load_delay: Tuple[float, float] = (3.0, 5.0)
    row_selection_delay: Tuple[float, float] = (1.5, 2.5)
    max_tree_iterations: int = 15
    user_agent: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    )
    max_direct_download_wait_seconds: int = 30


# ---------------------------------------------------------------------------
# Result container
# ---------------------------------------------------------------------------

@dataclass
class DownloadResult:
    """Ergebnis eines Download-Versuchs."""

    success: bool
    pdf_path: Optional[Path] = None
    error: Optional[str] = None
    no_gl_available: bool = False  # Keine Gesellschafterliste vorhanden


# ---------------------------------------------------------------------------
# Rate limiter with optional persistence
# ---------------------------------------------------------------------------

class RateLimiter:
    """Rate-Limiter for API calls.

    Optionally persists the last-call timestamp to a JSON file so that the
    rate limit is respected across process restarts.

    Args:
        calls_per_hour: Maximum allowed calls per hour.
        state_file: Optional path to a JSON file for persisting state.
                    When ``None`` (the default), state is kept in-memory only
                    (backward-compatible behaviour).
    """

    def __init__(self, calls_per_hour: int = 55, state_file: Optional[Path] = None) -> None:
        self.min_interval: float = 3600.0 / calls_per_hour
        self.state_file: Optional[Path] = Path(state_file) if state_file else None
        self.last_call: float = self._load_state()

    # -- persistence helpers ------------------------------------------------

    def _load_state(self) -> float:
        """Load last_call timestamp from *state_file*, returning 0.0 on any error."""
        if self.state_file is None:
            return 0.0
        try:
            if self.state_file.exists():
                data = json.loads(self.state_file.read_text(encoding="utf-8"))
                return float(data.get("last_call", 0.0))
        except (json.JSONDecodeError, OSError, ValueError, TypeError) as exc:
            logger.debug(f"[RateLimiter._load_state] Could not load state: {exc}")
        return 0.0

    def _save_state(self) -> None:
        """Persist current *last_call* timestamp to *state_file*."""
        if self.state_file is None:
            return
        try:
            self.state_file.parent.mkdir(parents=True, exist_ok=True)
            self.state_file.write_text(
                json.dumps({"last_call": self.last_call}),
                encoding="utf-8",
            )
        except OSError as exc:
            logger.debug(f"[RateLimiter._save_state] Could not save state: {exc}")

    # -- public API ---------------------------------------------------------

    def wait(self) -> None:
        """Block until the next call is allowed, then record the timestamp."""
        elapsed = time.time() - self.last_call

        if elapsed < self.min_interval:
            sleep_time = self.min_interval - elapsed
            # Add random jitter (1-5 seconds)
            sleep_time += random.uniform(1, 5)
            logger.debug(f"[RateLimiter.wait] Rate-Limit: waiting {sleep_time:.1f}s")
            time.sleep(sleep_time)

        self.last_call = time.time()
        self._save_state()


# ---------------------------------------------------------------------------
# Main downloader
# ---------------------------------------------------------------------------

class GesellschafterlistenDownloader:
    """Selenium-Scraper for handelsregister.de.

    Downloads Gesellschafterlisten (shareholder lists) as PDFs.
    """

    # Mapping of court city names to display names
    COURT_MAPPINGS: dict[str, str] = {
        "berlin": "Berlin (Charlottenburg)",
        "münchen": "München",
        "hamburg": "Hamburg",
        "frankfurt": "Frankfurt am Main",
        "köln": "Köln",
        "düsseldorf": "Düsseldorf",
        "stuttgart": "Stuttgart",
        "hannover": "Hannover",
        "nürnberg": "Nürnberg",
        "dresden": "Dresden",
        "leipzig": "Leipzig",
    }

    # Windows reserved device names that cannot be used as filenames
    WINDOWS_RESERVED_NAMES: frozenset[str] = frozenset([
        "CON", "PRN", "AUX", "NUL",
        "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
        "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
    ])

    def __init__(
        self,
        download_dir: Path,
        headless: bool = True,
        debug: bool = False,
        config: Optional[DownloaderConfig] = None,
    ) -> None:
        """Initialise the downloader.

        Args:
            download_dir: Directory where downloaded PDFs are stored.
            headless: Run the browser without a visible GUI.
            debug: Store debug screenshots during the run.
            config: Optional configuration override. Uses defaults when *None*.
        """
        self.config: DownloaderConfig = config or DownloaderConfig()
        self.download_dir: Path = Path(download_dir)
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.headless: bool = headless
        self.debug: bool = debug
        self.debug_dir: Path = Path(download_dir).parent / "debug"
        if self.debug:
            self.debug_dir.mkdir(parents=True, exist_ok=True)
        self.driver: Optional[webdriver.Chrome] = None
        self.rate_limiter: RateLimiter = RateLimiter(
            calls_per_hour=self.config.rate_limit_per_hour,
        )
        self._debug_counter: int = 0

    # ------------------------------------------------------------------
    # Debug helpers
    # ------------------------------------------------------------------

    def _save_debug_screenshot(self, name: str) -> None:
        """Save a debug screenshot if debug mode is active."""
        if self.debug and self.driver:
            self._debug_counter += 1
            path = self.debug_dir / f"debug_{self._debug_counter:02d}_{name}.png"
            try:
                self.driver.save_screenshot(str(path))
                logger.debug(f"[_save_debug_screenshot] Saved: {path}")
            except TimeoutException as exc:
                logger.debug(f"[_save_debug_screenshot] Timeout saving screenshot: {exc}")
            except OSError as exc:
                logger.debug(f"[_save_debug_screenshot] OS error saving screenshot: {exc}")
            except Exception as exc:
                logger.debug(f"[_save_debug_screenshot] Unexpected error saving screenshot: {exc}")

    # ------------------------------------------------------------------
    # Filename / path safety
    # ------------------------------------------------------------------

    def _sanitize_filename(self, name: str) -> str:
        """Sanitize a string for safe use as a filename.

        Prevents path-traversal attacks (CWE-22).

        Returns:
            Safe filename string, never empty.

        Raises:
            ValueError: If input is empty, contains path-traversal sequences,
                        or resolves outside the download directory.
        """
        if not name or not name.strip():
            raise ValueError("Filename cannot be empty")

        # Explicit path-traversal check (before any transformation)
        if ".." in name or "/" in name or "\\" in name:
            raise ValueError(f"Path traversal detected in filename: {name!r}")

        # Remove ALL non-allowed characters
        safe = re.sub(r"[^\w\s\-]", "", name)
        # Collapse multiple spaces/hyphens/underscores
        safe = re.sub(r"[-\s_]+", "_", safe)
        # Limit length (Windows max: 255, we use 200 for safety margin)
        safe = safe[:200]
        # Strip leading/trailing separators
        safe = safe.strip("_-")

        # Handle edge case: all characters were removed
        if not safe:
            raise ValueError(f"Filename '{name}' contains no valid characters")

        # Block Windows reserved device names
        if safe.upper() in self.WINDOWS_RESERVED_NAMES:
            safe = f"file_{safe}"

        # Final path-traversal guard: resolved path must stay inside download_dir
        final_path = self.download_dir / safe
        if not final_path.resolve().is_relative_to(self.download_dir.resolve()):
            raise ValueError(f"Path traversal detected: resolved path escapes download directory")

        return safe

    # ------------------------------------------------------------------
    # PDF validation
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_downloaded_file(file_path: Path) -> bool:
        """Validate that the downloaded file is actually a PDF (magic-bytes check).

        Returns:
            ``True`` if the file starts with the ``%PDF`` magic bytes.
        """
        try:
            with open(file_path, "rb") as fh:
                header = fh.read(4)
                return header == b"%PDF"
        except OSError:
            return False

    # ------------------------------------------------------------------
    # WebDriver management
    # ------------------------------------------------------------------

    def _setup_driver(self) -> webdriver.Chrome:
        """Configure and return a Chrome WebDriver instance."""
        options = Options()

        prefs = {
            "download.default_directory": str(self.download_dir.absolute()),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "plugins.always_open_pdf_externally": True,
            "safebrowsing.enabled": True,
        }
        options.add_experimental_option("prefs", prefs)

        # Anti-detection
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)

        if self.headless:
            options.add_argument("--headless=new")

        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")
        options.add_argument(f"--user-agent={self.config.user_agent}")

        if USE_WEBDRIVER_MANAGER:
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=options)
        else:
            driver = webdriver.Chrome(options=options)

        # Anti-detection script
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
            """
        })

        return driver

    def start(self) -> None:
        """Start the browser."""
        if self.driver is None:
            logger.info("[start] Starting Chrome browser...")
            self.driver = self._setup_driver()

    def stop(self) -> None:
        """Stop the browser."""
        if self.driver:
            logger.info("[stop] Stopping Chrome browser...")
            self.driver.quit()
            self.driver = None

    # ------------------------------------------------------------------
    # Public download entry point
    # ------------------------------------------------------------------

    def download(self, register_num: str, court: str = "") -> DownloadResult:
        """Download the Gesellschafterliste for a company.

        Args:
            register_num: Full register number (e.g. ``"HRB 12345 B"``).
            court: Register court (optional, for more precise search).

        Returns:
            :class:`DownloadResult` with path to the PDF or an error message.
        """
        # ---- input validation ----
        if not isinstance(register_num, str) or not register_num.strip():
            return DownloadResult(
                success=False,
                error="register_num must be a non-empty string",
            )
        register_num = register_num.strip()
        court = court.strip() if court else ""

        self.start()
        self.rate_limiter.wait()

        try:
            # Parse register number
            reg_type, reg_number, reg_suffix = self._parse_register_num(register_num)

            if not reg_type or not reg_number:
                return DownloadResult(
                    success=False,
                    error=f"Ungueltige Registernummer: {register_num}",
                )

            logger.info(
                f"[download] Suche: {reg_type} {reg_number} "
                f"{reg_suffix or ''} ({court or 'alle Gerichte'})"
            )

            # 1. Navigate to search page
            self.driver.get(self.config.base_url)
            time.sleep(random.uniform(*self.config.page_load_delay))
            self._save_debug_screenshot("01_start_page")

            # 2. Fill search form
            self._fill_search_form(reg_type, reg_number, court)
            self._save_debug_screenshot("02_form_filled")

            # 3. Submit search
            self._submit_search()
            self._save_debug_screenshot("03_after_search")

            # 4. Wait for results and click the correct one
            if not self._click_correct_result(court, reg_type):
                self._save_debug_screenshot("04_no_results")
                return DownloadResult(
                    success=False,
                    error="Keine passenden Suchergebnisse gefunden",
                )
            self._save_debug_screenshot("05_result_clicked")

            # 5. Download DK documents
            pdf_path = self._download_dk_documents(register_num)

            if not pdf_path:
                self._save_debug_screenshot("06_no_download")
                logger.warning(f"[download] Keine Dokumente fuer {register_num} heruntergeladen")
                return DownloadResult(success=True, no_gl_available=True)

            logger.info(f"[download] Erfolgreich heruntergeladen: {pdf_path}")
            return DownloadResult(success=True, pdf_path=pdf_path)

        except TimeoutException as exc:
            logger.error(f"[download] Timeout fuer {register_num}: {exc}")
            return DownloadResult(success=False, error=f"Timeout: {exc}")

        except NoSuchElementException as exc:
            logger.error(f"[download] Element not found for {register_num}: {exc}")
            return DownloadResult(success=False, error=f"Element not found: {exc}")

        except Exception as exc:
            logger.error(f"[download] Unerwarteter Fehler fuer {register_num}: {exc}")
            logger.debug(traceback.format_exc())
            return DownloadResult(success=False, error=str(exc))

    # ------------------------------------------------------------------
    # Register number parsing
    # ------------------------------------------------------------------

    def _parse_register_num(
        self, register_num: str
    ) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """Parse a register number into type, number, and optional suffix.

        Examples::

            "HRB 12345"   -> ("HRB", "12345", None)
            "HRB 12345 B" -> ("HRB", "12345", "B")
            "HRB12345"    -> ("HRB", "12345", None)
        """
        register_num = register_num.strip().upper()
        match = re.match(r"(HRB|HRA|GNR|VR|PR)\s*(\d+)\s*([A-Z])?", register_num)
        if match:
            return match.group(1), match.group(2), match.group(3)
        return None, None, None

    # ------------------------------------------------------------------
    # Search form
    # ------------------------------------------------------------------

    def _fill_search_form(self, reg_type: str, reg_number: str, court: str) -> None:
        """Fill out the search form on handelsregister.de."""
        wait = WebDriverWait(self.driver, 10)

        # Accept cookie banner if present
        try:
            cookie_btn = self.driver.find_element(
                By.XPATH, '//a[contains(text(), "Verstanden")]'
            )
            cookie_btn.click()
            time.sleep(1)
        except NoSuchElementException:
            logger.debug("[_fill_search_form] No cookie banner found")
        except ElementClickInterceptedException as exc:
            logger.debug(f"[_fill_search_form] Cookie banner click intercepted: {exc}")

        # Select Bundeslaender (at least one must be selected)
        self._select_bundeslaender(court)

        # Select register type (PrimeFaces SelectOneMenu)
        try:
            reg_type_dropdown = wait.until(
                EC.element_to_be_clickable((By.ID, "form:registerArt"))
            )
            reg_type_dropdown.click()
            time.sleep(0.5)

            options = self.driver.find_elements(
                By.CSS_SELECTOR, "#form\\:registerArt_panel li"
            )
            for opt in options:
                if opt.text.strip() == reg_type or opt.get_attribute("data-label") == reg_type:
                    opt.click()
                    break
            time.sleep(0.3)
        except TimeoutException as exc:
            logger.debug(f"[_fill_search_form] Registerart dropdown timeout: {exc}")
        except NoSuchElementException as exc:
            logger.debug(f"[_fill_search_form] Registerart dropdown not found: {exc}")
        except Exception as exc:
            logger.debug(f"[_fill_search_form] Registerart selection skipped: {exc}")

        # Enter register number
        try:
            reg_num_field = wait.until(
                EC.presence_of_element_located((By.ID, "form:registerNummer"))
            )
            reg_num_field.clear()
            reg_num_field.send_keys(reg_number)
        except TimeoutException as exc:
            logger.error(f"[_fill_search_form] Registernummer field timeout: {exc}")
        except NoSuchElementException as exc:
            logger.error(f"[_fill_search_form] Registernummer field not found: {exc}")

        # Select court (if specified) via PrimeFaces AutoComplete
        if court:
            try:
                court_input = self.driver.find_element(
                    By.ID, "form:registergericht_input"
                )
                court_input.clear()
                court_input.send_keys(court[:15])
                time.sleep(1.5)

                suggestions = wait.until(
                    EC.presence_of_all_elements_located(
                        (By.CSS_SELECTOR, "#form\\:registergericht_panel li")
                    )
                )
                for suggestion in suggestions:
                    if court.lower() in suggestion.text.lower():
                        suggestion.click()
                        break
                time.sleep(0.3)
            except TimeoutException as exc:
                logger.debug(f"[_fill_search_form] Court autocomplete timeout: {exc}")
            except NoSuchElementException as exc:
                logger.debug(f"[_fill_search_form] Court input not found: {exc}")
            except Exception as exc:
                logger.debug(f"[_fill_search_form] Court selection skipped: {exc}")

    def _select_bundeslaender(self, court: str) -> None:
        """Select the appropriate Bundeslaender based on the court.

        Important: handelsregister.de allows max. 2 Bundeslaender selections.
        """
        # Extended mapping from court cities to Bundeslaender
        CITY_TO_BUNDESLAND: dict[str, str] = {
            # Bayern
            "münchen": "Bayern", "munich": "Bayern", "nürnberg": "Bayern",
            "augsburg": "Bayern", "würzburg": "Bayern", "regensburg": "Bayern",
            "passau": "Bayern", "bayreuth": "Bayern", "ingolstadt": "Bayern",
            "kempten": "Bayern", "landshut": "Bayern", "fürth": "Bayern",
            # Berlin
            "berlin": "Berlin", "charlottenburg": "Berlin",
            # Brandenburg
            "potsdam": "Brandenburg", "cottbus": "Brandenburg",
            "frankfurt (oder)": "Brandenburg",
            # Bremen
            "bremen": "Bremen",
            # Hamburg
            "hamburg": "Hamburg",
            # Hessen
            "frankfurt": "Hessen", "wiesbaden": "Hessen", "darmstadt": "Hessen",
            "kassel": "Hessen", "gießen": "Hessen", "offenbach": "Hessen",
            "fulda": "Hessen", "marburg": "Hessen", "limburg": "Hessen",
            "korbach": "Hessen", "bad homburg": "Hessen", "hanau": "Hessen",
            # Mecklenburg-Vorpommern
            "rostock": "Mecklenburg-Vorpommern", "schwerin": "Mecklenburg-Vorpommern",
            "stralsund": "Mecklenburg-Vorpommern",
            "neubrandenburg": "Mecklenburg-Vorpommern",
            # Niedersachsen
            "hannover": "Niedersachsen", "braunschweig": "Niedersachsen",
            "osnabrück": "Niedersachsen", "oldenburg": "Niedersachsen",
            "göttingen": "Niedersachsen", "hildesheim": "Niedersachsen",
            "wolfsburg": "Niedersachsen", "lüneburg": "Niedersachsen",
            "aurich": "Niedersachsen", "tostedt": "Niedersachsen",
            # Nordrhein-Westfalen
            "köln": "Nordrhein-Westfalen", "düsseldorf": "Nordrhein-Westfalen",
            "dortmund": "Nordrhein-Westfalen", "essen": "Nordrhein-Westfalen",
            "duisburg": "Nordrhein-Westfalen", "bochum": "Nordrhein-Westfalen",
            "wuppertal": "Nordrhein-Westfalen", "bonn": "Nordrhein-Westfalen",
            "bielefeld": "Nordrhein-Westfalen", "münster": "Nordrhein-Westfalen",
            "aachen": "Nordrhein-Westfalen", "siegen": "Nordrhein-Westfalen",
            "paderborn": "Nordrhein-Westfalen", "kleve": "Nordrhein-Westfalen",
            "arnsberg": "Nordrhein-Westfalen", "gütersloh": "Nordrhein-Westfalen",
            "hagen": "Nordrhein-Westfalen", "krefeld": "Nordrhein-Westfalen",
            "siegburg": "Nordrhein-Westfalen",
            # Rheinland-Pfalz
            "mainz": "Rheinland-Pfalz", "koblenz": "Rheinland-Pfalz",
            "trier": "Rheinland-Pfalz", "ludwigshafen": "Rheinland-Pfalz",
            "kaiserslautern": "Rheinland-Pfalz",
            "bad kreuznach": "Rheinland-Pfalz",
            # Saarland
            "saarbrücken": "Saarland",
            # Sachsen
            "dresden": "Sachsen", "leipzig": "Sachsen", "chemnitz": "Sachsen",
            # Sachsen-Anhalt
            "magdeburg": "Sachsen-Anhalt", "halle": "Sachsen-Anhalt",
            "stendal": "Sachsen-Anhalt", "dessau": "Sachsen-Anhalt",
            # Schleswig-Holstein
            "kiel": "Schleswig-Holstein", "lübeck": "Schleswig-Holstein",
            "flensburg": "Schleswig-Holstein", "pinneberg": "Schleswig-Holstein",
            # Thueringen
            "erfurt": "Thüringen", "jena": "Thüringen", "gera": "Thüringen",
            # Baden-Wuerttemberg
            "stuttgart": "Baden-Württemberg", "mannheim": "Baden-Württemberg",
            "karlsruhe": "Baden-Württemberg", "freiburg": "Baden-Württemberg",
            "ulm": "Baden-Württemberg", "heidelberg": "Baden-Württemberg",
            "heilbronn": "Baden-Württemberg", "konstanz": "Baden-Württemberg",
        }

        BUNDESLAND_IDS: dict[str, str] = {
            "Baden-Württemberg": "form:Baden-Württemberg_input",
            "Bayern": "form:Bayern_input",
            "Berlin": "form:Berlin_input",
            "Brandenburg": "form:Brandenburg_input",
            "Bremen": "form:Bremen_input",
            "Hamburg": "form:Hamburg_input",
            "Hessen": "form:Hessen_input",
            "Mecklenburg-Vorpommern": "form:Mecklenburg-Vorpommern_input",
            "Niedersachsen": "form:Niedersachsen_input",
            "Nordrhein-Westfalen": "form:Nordrhein-Westfalen_input",
            "Rheinland-Pfalz": "form:Rheinland-Pfalz_input",
            "Saarland": "form:Saarland_input",
            "Sachsen": "form:Sachsen_input",
            "Sachsen-Anhalt": "form:Sachsen-Anhalt_input",
            "Schleswig-Holstein": "form:Schleswig-Holstein_input",
            "Thüringen": "form:Thüringen_input",
        }

        bundeslaender_to_select: list[str] = []

        # Try to detect Bundesland from court name
        if court:
            court_lower = court.lower()
            for city, bundesland in CITY_TO_BUNDESLAND.items():
                if city in court_lower:
                    bundeslaender_to_select.append(bundesland)
                    break

        # Direct match attempt on Bundesland name in court text
        if not bundeslaender_to_select and court:
            court_lower = court.lower()
            for bundesland in BUNDESLAND_IDS:
                if bundesland.lower() in court_lower:
                    bundeslaender_to_select.append(bundesland)
                    logger.debug(
                        f"[_select_bundeslaender] Bundesland from court text: {bundesland}"
                    )
                    break

        # Fallback
        if not bundeslaender_to_select:
            logger.warning(
                f"[_select_bundeslaender] No Bundesland detected for court '{court}' "
                "- search will likely fail"
            )
            bundeslaender_to_select = ["Bayern", "Niedersachsen"]
            logger.debug("[_select_bundeslaender] Fallback: Bayern and Niedersachsen selected")

        # Max 2 (website limit)
        bundeslaender_to_select = bundeslaender_to_select[:2]

        # Click checkboxes
        selected_count = 0
        for bundesland in bundeslaender_to_select:
            checkbox_id = BUNDESLAND_IDS.get(bundesland)
            if checkbox_id:
                try:
                    checkbox = self.driver.find_element(By.ID, checkbox_id)
                    if not checkbox.is_selected():
                        self.driver.execute_script("arguments[0].click();", checkbox)
                        selected_count += 1
                        logger.debug(f"[_select_bundeslaender] Selected: {bundesland}")
                        time.sleep(0.3)
                except NoSuchElementException as exc:
                    logger.debug(
                        f"[_select_bundeslaender] Checkbox not found for {bundesland}: {exc}"
                    )
                except ElementClickInterceptedException as exc:
                    logger.debug(
                        f"[_select_bundeslaender] Click intercepted for {bundesland}: {exc}"
                    )
                except Exception as exc:
                    logger.debug(
                        f"[_select_bundeslaender] Selection failed for {bundesland}: {exc}"
                    )

        if selected_count == 0:
            logger.warning("[_select_bundeslaender] No Bundesland could be selected!")
        else:
            logger.info(
                f"[_select_bundeslaender] {selected_count} Bundesland(er) selected: "
                f"{bundeslaender_to_select}"
            )

    # ------------------------------------------------------------------
    # Search submission
    # ------------------------------------------------------------------

    def _submit_search(self) -> None:
        """Submit the search form."""
        wait = WebDriverWait(self.driver, 10)

        try:
            submit_btn = wait.until(
                EC.element_to_be_clickable((By.ID, "form:btnSuche"))
            )
            self.driver.execute_script("arguments[0].click();", submit_btn)
            logger.debug("[_submit_search] Search submitted")
        except (TimeoutException, NoSuchElementException) as exc:
            logger.warning(f"[_submit_search] Submit button not found: {exc}")
            # Fallback: press Enter in the register number field
            try:
                reg_field = self.driver.find_element(By.ID, "form:registerNummer")
                reg_field.send_keys(Keys.RETURN)
            except NoSuchElementException as exc2:
                logger.warning(
                    f"[_submit_search] Fallback enter also failed: {exc2}"
                )

        # Wait for AJAX results
        time.sleep(random.uniform(*self.config.search_result_delay))

    # ------------------------------------------------------------------
    # Result selection
    # ------------------------------------------------------------------

    def _click_correct_result(
        self, target_court: str, register_type: str = "HRB"
    ) -> bool:
        """Click the search result matching the desired court and register type.

        Args:
            target_court: Desired court name (e.g. ``"Nuernberg"``).
            register_type: Register type (``HRB``, ``HRA``, etc.).

        Returns:
            ``True`` if a matching result was clicked.
        """
        wait = WebDriverWait(self.driver, 10)

        try:
            result_table = wait.until(
                EC.presence_of_element_located(
                    (By.ID, "ergebnissForm:selectedSuchErgebnisFormTable_data")
                )
            )

            rows = result_table.find_elements(By.CSS_SELECTOR, "tr")
            if not rows:
                logger.warning("[_click_correct_result] No rows in result table")
                return False

            target_court_lower = target_court.lower() if target_court else ""
            register_type_upper = register_type.upper() if register_type else "HRB"
            wrong_types = ["VR ", " VR", "GNR ", " GNR", "PR ", " PR"]

            # Pass 1: exact match (type + court)
            for row in rows:
                try:
                    row_text = row.text.upper()
                    has_correct_type = register_type_upper in row_text
                    is_wrong_type = any(wt in row_text for wt in wrong_types)
                    has_correct_court = (
                        target_court_lower and target_court_lower in row_text.lower()
                    )
                    if has_correct_type and has_correct_court and not is_wrong_type:
                        row.click()
                        logger.info(
                            f"[_click_correct_result] Perfect match: "
                            f"{register_type_upper} in {target_court}"
                        )
                        time.sleep(random.uniform(*self.config.element_interaction_delay))
                        return True
                except StaleElementReferenceException:
                    continue

            # Pass 2: type only
            for row in rows:
                try:
                    row_text = row.text.upper()
                    has_correct_type = register_type_upper in row_text
                    is_wrong_type = any(wt in row_text for wt in wrong_types)
                    if has_correct_type and not is_wrong_type:
                        row.click()
                        logger.info(
                            f"[_click_correct_result] Type match: {register_type_upper} "
                            f"(no court match)"
                        )
                        time.sleep(random.uniform(*self.config.element_interaction_delay))
                        return True
                except StaleElementReferenceException:
                    continue

            # Pass 3: court only
            for row in rows:
                try:
                    row_text = row.text.lower()
                    if target_court_lower and target_court_lower in row_text:
                        row.click()
                        logger.warning(
                            f"[_click_correct_result] Court match only: '{target_court}'"
                        )
                        time.sleep(random.uniform(*self.config.element_interaction_delay))
                        return True
                except StaleElementReferenceException:
                    continue

            # Pass 4: first row that is not VR/GnR/PR
            for row in rows:
                try:
                    row_text = row.text.upper()
                    is_wrong_type = any(wt in row_text for wt in wrong_types)
                    if not is_wrong_type:
                        row.click()
                        logger.warning(
                            "[_click_correct_result] Fallback: first non-VR/GnR/PR row"
                        )
                        time.sleep(random.uniform(*self.config.element_interaction_delay))
                        return True
                except StaleElementReferenceException:
                    continue

            logger.warning("[_click_correct_result] No matching results found")
            return False

        except TimeoutException:
            logger.warning("[_click_correct_result] No search results (timeout)")
            return False

    # ------------------------------------------------------------------
    # Document tree navigation & GL selection
    # ------------------------------------------------------------------

    def _select_and_download_gesellschafterliste(
        self, register_num: str
    ) -> Optional[Path]:
        """Find and download the Gesellschafterliste on the document page.

        This is called after navigating to the "Freigegebene Dokumente" page.
        The document structure on handelsregister.de:
          - "Dokumente zum Rechtstraeger" -> Contains Gesellschafterlisten
          - "Dokumente zur Registernummer" -> Usually only Sammelmappe
        """
        try:
            existing_files = set(self.download_dir.glob("*.*"))
            safe_name = self._sanitize_filename(register_num)

            # 1. Expand document tree
            logger.info("[_select_and_download_gl] Expanding document tree...")
            self._expand_all_tree_nodes()
            time.sleep(random.uniform(*self.config.tree_expansion_long_delay))
            self._save_debug_screenshot("tree_expanded")

            # 2. Find and expand "Liste der Gesellschafter" node
            gl_parent_patterns = [
                "List of shareholders",
                "Liste der Gesellschafter",
                "Gesellschafterliste",
            ]

            gl_found = False
            gl_element = None

            # Step 2a: Find the GL parent node and EXPAND it (do not select)
            gl_node_expanded = False
            for pattern in gl_parent_patterns:
                try:
                    parent_elements = self.driver.find_elements(
                        By.XPATH, f"//*[contains(text(), '{pattern}')]"
                    )

                    for parent_el in parent_elements:
                        if not parent_el.is_displayed():
                            continue

                        parent_text = parent_el.text.strip()
                        has_date = any(
                            x in parent_text.lower()
                            for x in ["entry", "eintrag", "vom ", "/20", "/19"]
                        )

                        if not has_date:
                            logger.info(
                                f"[_select_and_download_gl] GL parent node: '{parent_text}'"
                            )

                            expanded = False

                            # Method 1: Toggler in same container
                            try:
                                container = parent_el.find_element(By.XPATH, "./..")
                                toggler = container.find_element(
                                    By.CSS_SELECTOR,
                                    ".ui-tree-toggler, [class*='toggler'], span[class*='icon']",
                                )
                                if toggler.is_displayed():
                                    self.driver.execute_script(
                                        "arguments[0].click();", toggler
                                    )
                                    expanded = True
                                    logger.info(
                                        "[_select_and_download_gl] GL node expanded via toggler"
                                    )
                            except (NoSuchElementException, StaleElementReferenceException) as exc:
                                logger.debug(
                                    f"[_select_and_download_gl] Toggler method 1 failed: {exc}"
                                )

                            # Method 2: preceding sibling toggler
                            if not expanded:
                                try:
                                    toggler = parent_el.find_element(
                                        By.XPATH,
                                        "./preceding-sibling::*[contains(@class, 'toggler') "
                                        "or contains(@class, 'icon')]",
                                    )
                                    if toggler.is_displayed():
                                        self.driver.execute_script(
                                            "arguments[0].click();", toggler
                                        )
                                        expanded = True
                                        logger.info(
                                            "[_select_and_download_gl] GL node expanded "
                                            "via preceding sibling"
                                        )
                                except (NoSuchElementException, StaleElementReferenceException) as exc:
                                    logger.debug(
                                        f"[_select_and_download_gl] Toggler method 2 failed: {exc}"
                                    )

                            # Method 3: double-click
                            if not expanded:
                                try:
                                    actions = ActionChains(self.driver)
                                    actions.double_click(parent_el).perform()
                                    expanded = True
                                    logger.info(
                                        "[_select_and_download_gl] GL node expanded via double-click"
                                    )
                                except (StaleElementReferenceException, ElementClickInterceptedException) as exc:
                                    logger.debug(
                                        f"[_select_and_download_gl] Double-click expansion failed: {exc}"
                                    )

                            if expanded:
                                gl_node_expanded = True
                                time.sleep(
                                    random.uniform(*self.config.tree_expansion_long_delay)
                                )
                            break

                    if gl_node_expanded:
                        break

                except (NoSuchElementException, StaleElementReferenceException) as exc:
                    logger.debug(
                        f"[_select_and_download_gl] GL parent expansion failed: {exc}"
                    )

            self._save_debug_screenshot("gl_parent_expanded")
            time.sleep(random.uniform(*self.config.tree_expansion_delay))

            # Step 2b: Select the first (newest) entry with a date
            self._save_debug_screenshot("after_gl_expand")

            gl_entry_patterns = [
                "List of shareholders – entry",
                "List of shareholders - entry",
                "Liste der Gesellschafter – Eintrag",
                "Liste der Gesellschafter - Eintrag",
                "Gesellschafterliste vom",
                "Liste der Gesellschafter vom",
                "Gesellschafterliste –",
                "Gesellschafterliste -",
            ]

            for pattern in gl_entry_patterns:
                try:
                    entry_elements = self.driver.find_elements(
                        By.XPATH, f"//*[contains(text(), '{pattern}')]"
                    )

                    visible_entries: list = []
                    for el in entry_elements:
                        if not el.is_displayed():
                            continue
                        el_text = el.text.strip()
                        if re.search(r"\d{2}[./]\d{2}[./]\d{4}", el_text):
                            visible_entries.append(el)
                            logger.debug(
                                f"[_select_and_download_gl] GL entry with date: "
                                f"'{el_text[:50]}'"
                            )

                    if visible_entries:
                        newest_entry = visible_entries[0]
                        entry_text = newest_entry.text.strip()
                        logger.info(
                            f"[_select_and_download_gl] Newest GL: '{entry_text[:60]}'"
                        )

                        # Select the tree node
                        try:
                            treenode = newest_entry.find_element(
                                By.XPATH,
                                "./ancestor::*[contains(@class, 'treenode') "
                                "or contains(@class, 'tree-node')][1]",
                            )
                            content = treenode.find_element(
                                By.CSS_SELECTOR,
                                ".ui-treenode-content, .tree-content, *",
                            )
                            self.driver.execute_script(
                                "arguments[0].click();", content
                            )
                        except (NoSuchElementException, StaleElementReferenceException):
                            self.driver.execute_script(
                                "arguments[0].click();", newest_entry
                            )

                        gl_found = True
                        gl_element = newest_entry
                        time.sleep(
                            random.uniform(*self.config.element_interaction_delay)
                        )
                        break

                except (NoSuchElementException, StaleElementReferenceException) as exc:
                    logger.debug(
                        f"[_select_and_download_gl] GL entry search failed "
                        f"for '{pattern}': {exc}"
                    )
                    continue

            # Additional search: elements with "Gesellschafter" AND a date
            if not gl_found:
                logger.info(
                    "[_select_and_download_gl] Searching for elements with "
                    "'Gesellschafter' and date..."
                )
                try:
                    all_elements = self.driver.find_elements(
                        By.XPATH,
                        "//*[contains(text(), 'Gesellschafter') "
                        "or contains(text(), 'shareholders')]",
                    )

                    for el in all_elements:
                        if not el.is_displayed():
                            continue
                        el_text = el.text.strip()
                        if re.search(r"\d{2}[./]\d{2}[./]\d{4}", el_text):
                            logger.info(
                                f"[_select_and_download_gl] GL with date found: "
                                f"'{el_text[:60]}'"
                            )
                            self.driver.execute_script(
                                "arguments[0].click();", el
                            )
                            gl_found = True
                            gl_element = el
                            time.sleep(
                                random.uniform(*self.config.element_interaction_delay)
                            )
                            break
                except (NoSuchElementException, StaleElementReferenceException) as exc:
                    logger.debug(
                        f"[_select_and_download_gl] Alternative GL search failed: {exc}"
                    )

            # Fallback: first GL match without date
            if not gl_found:
                logger.info(
                    "[_select_and_download_gl] No GL entries with date, trying fallback..."
                )
                for pattern in gl_parent_patterns:
                    try:
                        all_gl = self.driver.find_elements(
                            By.XPATH, f"//*[contains(text(), '{pattern}')]"
                        )
                        visible_gl = [el for el in all_gl if el.is_displayed()]

                        if visible_gl:
                            first_gl = visible_gl[0]
                            logger.info(
                                f"[_select_and_download_gl] Fallback GL: "
                                f"'{first_gl.text[:50]}'"
                            )
                            self.driver.execute_script(
                                "arguments[0].click();", first_gl
                            )
                            gl_found = True
                            gl_element = first_gl
                            time.sleep(
                                random.uniform(*self.config.element_interaction_delay)
                            )
                            break
                    except (NoSuchElementException, StaleElementReferenceException) as exc:
                        logger.debug(
                            f"[_select_and_download_gl] Fallback search failed "
                            f"for '{pattern}': {exc}"
                        )
                        continue

            if not gl_found:
                logger.warning(
                    "[_select_and_download_gl] No GL found in document tree"
                )
                self._save_debug_screenshot("no_gl_in_tree")

                # Last chance: check page source
                page_source = self.driver.page_source
                if "Dokumente zum Rechtsträger" in page_source:
                    if "Liste der Gesellschafter" not in page_source:
                        logger.warning(
                            "[_select_and_download_gl] 'Dokumente zum Rechtstraeger' "
                            "visible but no GL - maybe not expanded?"
                        )
                        self._expand_all_tree_nodes()
                        time.sleep(2)
                        self._save_debug_screenshot("retry_expand")
                    else:
                        logger.info(
                            "[_select_and_download_gl] GL text in page source "
                            "but element not clickable"
                        )
                else:
                    logger.warning(
                        "[_select_and_download_gl] 'Dokumente zum Rechtstraeger' "
                        "not on page - wrong document type?"
                    )

                return None

            # 3. Select PDF format if available
            time.sleep(random.uniform(*self.config.element_interaction_delay))
            self._save_debug_screenshot("gl_selected")

            try:
                pdf_radio_selectors = [
                    "//input[@type='radio' and @value='pdf']",
                    "//input[@type='radio'][following-sibling::*[contains(text(), 'pdf')]]",
                    "//label[contains(text(), 'pdf')]//input",
                    "//label[contains(text(), 'pdf')]/preceding-sibling::input",
                ]
                for selector in pdf_radio_selectors:
                    try:
                        pdf_radios = self.driver.find_elements(By.XPATH, selector)
                        for radio in pdf_radios:
                            if radio.is_displayed() and not radio.is_selected():
                                self.driver.execute_script(
                                    "arguments[0].click();", radio
                                )
                                logger.info(
                                    "[_select_and_download_gl] PDF format selected"
                                )
                                time.sleep(0.5)
                                break
                    except (NoSuchElementException, StaleElementReferenceException) as exc:
                        logger.debug(
                            f"[_select_and_download_gl] PDF radio selector failed: {exc}"
                        )
                        continue
            except Exception as exc:
                logger.debug(
                    f"[_select_and_download_gl] PDF format selection not possible: {exc}"
                )

            # 4. Click download button
            download_selectors = [
                "//button[@id='form:j_id_2h']",
                "//button[contains(@id, 'btnDownload')]",
                "//button[contains(@id, 'Download')]",
                "//input[contains(@id, 'btnDownload')]",
                "//button[normalize-space(text())='Download']",
                "//button[contains(text(), 'Download')]",
                "//input[@value='Download']",
                "//button[span[contains(text(), 'Download')]]",
                "//button[.//span[normalize-space()='Download']]",
                "//button[contains(@class, 'ui-button')]//span[text()='Download']/..",
            ]

            download_clicked = False
            download_btn = None

            for selector in download_selectors:
                try:
                    buttons = self.driver.find_elements(By.XPATH, selector)
                    for btn in buttons:
                        if btn.is_displayed():
                            download_btn = btn
                            logger.info(
                                f"[_select_and_download_gl] Download button found: "
                                f"{selector}"
                            )
                            break
                except (NoSuchElementException, StaleElementReferenceException):
                    continue
                if download_btn:
                    break

            # Fallback: iterate all buttons
            if not download_btn:
                logger.info(
                    "[_select_and_download_gl] Searching download button by text..."
                )
                all_buttons = self.driver.find_elements(By.TAG_NAME, "button")
                for btn in all_buttons:
                    try:
                        if not btn.is_displayed():
                            continue
                        btn_text = (btn.text or "").lower()
                        btn_id = (btn.get_attribute("id") or "").lower()
                        if "download" in btn_text or "download" in btn_id:
                            download_btn = btn
                            logger.info(
                                f"[_select_and_download_gl] Download button via text: "
                                f"'{btn.text}'"
                            )
                            break
                    except StaleElementReferenceException:
                        continue

            if download_btn:
                # Try multiple click methods
                try:
                    self.driver.execute_script(
                        "arguments[0].scrollIntoView("
                        "{behavior: 'smooth', block: 'center'});",
                        download_btn,
                    )
                    time.sleep(0.5)

                    actions = ActionChains(self.driver)
                    actions.move_to_element(download_btn).pause(0.3).click().perform()
                    download_clicked = True
                    logger.info(
                        "[_select_and_download_gl] Download clicked via ActionChains"
                    )
                except (ElementClickInterceptedException, StaleElementReferenceException) as exc:
                    logger.debug(
                        f"[_select_and_download_gl] ActionChains click failed: {exc}"
                    )

                if not download_clicked:
                    try:
                        self.driver.execute_script(
                            "arguments[0].click();", download_btn
                        )
                        download_clicked = True
                        logger.info(
                            "[_select_and_download_gl] Download clicked via JS"
                        )
                    except Exception as exc:
                        logger.debug(
                            f"[_select_and_download_gl] JS click failed: {exc}"
                        )

                if not download_clicked:
                    try:
                        download_btn.click()
                        download_clicked = True
                        logger.info(
                            "[_select_and_download_gl] Download clicked directly"
                        )
                    except (ElementClickInterceptedException, StaleElementReferenceException) as exc:
                        logger.debug(
                            f"[_select_and_download_gl] Direct click failed: {exc}"
                        )
            else:
                logger.warning(
                    "[_select_and_download_gl] No download button found!"
                )

            self._save_debug_screenshot("after_download_click")

            # 5. Wait for download
            logger.info("[_select_and_download_gl] Waiting for download...")
            for i in range(self.config.download_timeout_seconds):
                time.sleep(1)

                new_files = set(self.download_dir.glob("*.*")) - existing_files
                new_files = {
                    f for f in new_files
                    if f.suffix not in (".crdownload", ".tmp", ".part")
                }

                if new_files:
                    newest = max(new_files, key=lambda p: p.stat().st_mtime)
                    logger.info(
                        f"[_select_and_download_gl] Download complete: {newest.name}"
                    )

                    if newest.suffix.lower() == ".zip":
                        return self._extract_pdf_from_zip(newest, safe_name)
                    elif newest.suffix.lower() == ".pdf":
                        new_name = (
                            self.download_dir / f"{safe_name}_gesellschafterliste.pdf"
                        )
                        if new_name.exists():
                            new_name.unlink()
                        newest.rename(new_name)
                        if not self._validate_downloaded_file(new_name):
                            logger.warning(
                                f"[_select_and_download_gl] File {new_name} failed "
                                f"PDF magic-byte validation"
                            )
                        return new_name
                    return newest

                if i > 0 and i % 10 == 0:
                    logger.debug(
                        f"[_select_and_download_gl] Waiting for download... {i}s"
                    )

            logger.warning(
                "[_select_and_download_gl] Download timeout for Gesellschafterliste"
            )
            return None

        except Exception as exc:
            logger.error(
                f"[_select_and_download_gl] Error downloading Gesellschafterliste: {exc}"
            )
            logger.debug(traceback.format_exc())
            return None

    # ------------------------------------------------------------------
    # DK document download
    # ------------------------------------------------------------------

    def _download_dk_documents(self, register_num: str) -> Optional[Path]:
        """Download DK documents by clicking the DK link.

        The DK link on handelsregister.de triggers a direct download
        (PrimeFaces.monitorDownload).

        Returns:
            Path to the downloaded file (PDF/ZIP) or ``None``.
        """
        try:
            existing_files = set(self.download_dir.glob("*.*"))

            # Find DK links
            dk_links = self.driver.find_elements(
                By.XPATH,
                "//a[contains(@class, 'dokumentList') and span[text()='DK']]",
            )

            if not dk_links:
                dk_links = self.driver.find_elements(
                    By.XPATH,
                    "//a[span[text()='DK']] | //a[contains(text(), 'DK')]",
                )

            if not dk_links:
                logger.warning("[_download_dk_documents] No DK links found")
                return None

            logger.info(f"[_download_dk_documents] {len(dk_links)} DK link(s) found")

            for link in dk_links:
                try:
                    if not link.is_displayed():
                        continue

                    # Select the row first (important for PrimeFaces)
                    try:
                        row = link.find_element(By.XPATH, "./ancestor::tr")
                        row.click()
                        time.sleep(
                            random.uniform(*self.config.row_selection_delay)
                        )
                    except NoSuchElementException as exc:
                        logger.debug(
                            f"[_download_dk_documents] Row selection failed: {exc}"
                        )

                    # Scroll to element
                    self.driver.execute_script(
                        "arguments[0].scrollIntoView("
                        "{behavior: 'smooth', block: 'center'});",
                        link,
                    )
                    time.sleep(
                        random.uniform(*self.config.element_interaction_delay)
                    )

                    # Simulate mouse movement and click
                    actions = ActionChains(self.driver)
                    actions.move_to_element(link).pause(
                        random.uniform(*self.config.tree_expansion_delay)
                    ).click().perform()

                    logger.info(
                        "[_download_dk_documents] DK link clicked - waiting for document page..."
                    )
                    time.sleep(random.uniform(*self.config.dk_page_load_delay))

                    # Check if we are on the document page
                    page_source = self.driver.page_source
                    if (
                        "Freigegebene Dokumente" in page_source
                        or "Dokumente zum Rechtsträger" in page_source
                    ):
                        logger.info(
                            "[_download_dk_documents] Document page loaded - "
                            "searching Gesellschafterliste"
                        )
                        self._save_debug_screenshot("dk_documents_page")

                        pdf_path = self._select_and_download_gesellschafterliste(
                            register_num
                        )
                        if pdf_path:
                            return pdf_path
                        else:
                            logger.warning(
                                "[_download_dk_documents] No GL found on document page"
                            )
                            return None

                    # Check for error page
                    if "error" in self.driver.current_url.lower():
                        logger.warning(
                            "[_download_dk_documents] Error page after DK click"
                        )
                        return None

                    # Wait for direct download (if no document tree)
                    for i in range(self.config.max_direct_download_wait_seconds):
                        time.sleep(1)
                        new_files = set(self.download_dir.glob("*.*")) - existing_files
                        new_files = {
                            f for f in new_files
                            if f.suffix not in (".crdownload", ".tmp", ".part")
                        }

                        if new_files:
                            newest = max(
                                new_files, key=lambda p: p.stat().st_mtime
                            )
                            safe_name = self._sanitize_filename(register_num)
                            if newest.suffix.lower() == ".zip":
                                return self._extract_pdf_from_zip(
                                    newest, safe_name
                                )
                            elif newest.suffix.lower() == ".pdf":
                                new_name = (
                                    self.download_dir
                                    / f"{safe_name}_gesellschafterliste.pdf"
                                )
                                if new_name.exists():
                                    new_name.unlink()
                                newest.rename(new_name)
                                if not self._validate_downloaded_file(new_name):
                                    logger.warning(
                                        f"[_download_dk_documents] File {new_name} "
                                        f"failed PDF magic-byte validation"
                                    )
                                return new_name
                            return newest

                    logger.warning(
                        f"[_download_dk_documents] No download after "
                        f"{self.config.max_direct_download_wait_seconds} seconds"
                    )
                    return None

                except (
                    StaleElementReferenceException,
                    ElementClickInterceptedException,
                ) as exc:
                    logger.debug(
                        f"[_download_dk_documents] DK link click failed: {exc}"
                    )
                    continue
                except Exception as exc:
                    logger.debug(
                        f"[_download_dk_documents] DK link click error: {exc}"
                    )
                    continue

            return None

        except Exception as exc:
            logger.error(f"[_download_dk_documents] Error: {exc}")
            logger.debug(traceback.format_exc())
            return None

    # ------------------------------------------------------------------
    # DK tab opening
    # ------------------------------------------------------------------

    def _open_dk_tab(self) -> bool:
        """Open the DK page (document copies).

        On the search result page each row has links like "DK", "HD", etc.
        This clicks the "DK" link in the currently selected row.
        """
        wait = WebDriverWait(self.driver, 15)

        try:
            time.sleep(1)

            # Method 1: DK link in the highlighted / selected row
            selected_row_selectors = [
                "//tr[contains(@class, 'ui-state-highlight')]//a[text()='DK']",
                "//tr[contains(@class, 'selected')]//a[text()='DK']",
                "//tr[@aria-selected='true']//a[text()='DK']",
                "//tr[contains(@class, 'highlight')]//a[text()='DK']",
            ]

            for selector in selected_row_selectors:
                try:
                    dk_link = self.driver.find_element(By.XPATH, selector)
                    if dk_link.is_displayed():
                        self.driver.execute_script(
                            "arguments[0].click();", dk_link
                        )
                        logger.info(
                            "[_open_dk_tab] DK link clicked in selected row"
                        )
                        time.sleep(3)
                        return True
                except NoSuchElementException:
                    continue

            # Method 2: any visible DK link (fallback)
            dk_links = self.driver.find_elements(
                By.XPATH,
                "//a[text()='DK'] | //a[normalize-space(text())='DK'] | "
                "//a[contains(text(), 'DK')] | //a[@title='DK'] | "
                "//span[text()='DK']/.. | //a[contains(@title, 'Dokumentenkopie')]",
            )

            original_window = self.driver.current_window_handle
            original_windows = set(self.driver.window_handles)

            for link in dk_links:
                try:
                    if link.is_displayed():
                        self.driver.execute_script(
                            "arguments[0].scrollIntoView(true);", link
                        )
                        time.sleep(0.5)

                        self.driver.execute_script(
                            "arguments[0].click();", link
                        )
                        logger.info("[_open_dk_tab] DK link clicked (fallback)")
                        time.sleep(3)

                        new_windows = (
                            set(self.driver.window_handles) - original_windows
                        )
                        if new_windows:
                            new_window = new_windows.pop()
                            self.driver.switch_to.window(new_window)
                            logger.info("[_open_dk_tab] Switched to new window")
                            time.sleep(2)

                        return True
                except (
                    StaleElementReferenceException,
                    ElementClickInterceptedException,
                ) as exc:
                    logger.debug(f"[_open_dk_tab] DK link click failed: {exc}")
                    continue

            # Method 3: search through result table rows
            try:
                result_table = self.driver.find_element(
                    By.ID,
                    "ergebnissForm:selectedSuchErgebnisFormTable_data",
                )
                rows = result_table.find_elements(By.TAG_NAME, "tr")

                for row in rows:
                    try:
                        dk_link = row.find_element(
                            By.XPATH, ".//a[text()='DK']"
                        )
                        if dk_link.is_displayed():
                            self.driver.execute_script(
                                "arguments[0].click();", dk_link
                            )
                            logger.info(
                                "[_open_dk_tab] DK link clicked in result row"
                            )
                            time.sleep(3)
                            return True
                    except NoSuchElementException:
                        continue
            except NoSuchElementException:
                logger.debug("[_open_dk_tab] Result table not found")

            logger.warning("[_open_dk_tab] No DK link found")
            return False

        except TimeoutException as exc:
            logger.error(f"[_open_dk_tab] Timeout opening DK page: {exc}")
            return False
        except Exception as exc:
            logger.error(f"[_open_dk_tab] Error opening DK page: {exc}")
            return False

    # ------------------------------------------------------------------
    # Gesellschafterliste finder (alternative path)
    # ------------------------------------------------------------------

    def _find_gesellschafterliste(self) -> bool:
        """Find and click on the Gesellschafterliste in the document tree."""
        wait = WebDriverWait(self.driver, 10)

        try:
            time.sleep(2)

            # Expand all tree nodes
            self._expand_all_tree_nodes()
            time.sleep(1)

            gl_patterns = [
                "Liste der Gesellschafter",
                "Gesellschafterliste",
                "GL ",
            ]

            for pattern in gl_patterns:
                gl_elements = self.driver.find_elements(
                    By.XPATH,
                    f"//span[contains(text(), '{pattern}')] | "
                    f"//td[contains(text(), '{pattern}')]",
                )

                for el in gl_elements:
                    try:
                        treenode = el.find_element(
                            By.XPATH,
                            "./ancestor::li[contains(@class, 'ui-treenode')]",
                        )
                        content = treenode.find_element(
                            By.CSS_SELECTOR, ".ui-treenode-content"
                        )

                        self.driver.execute_script(
                            "arguments[0].click();", content
                        )
                        logger.info(
                            f"[_find_gesellschafterliste] GL found and selected: "
                            f"{pattern}"
                        )
                        time.sleep(1)
                        return True
                    except (NoSuchElementException, StaleElementReferenceException):
                        try:
                            self.driver.execute_script(
                                "arguments[0].click();", el
                            )
                            logger.info(
                                f"[_find_gesellschafterliste] GL clicked directly: "
                                f"{pattern}"
                            )
                            time.sleep(1)
                            return True
                        except Exception as exc2:
                            logger.debug(
                                f"[_find_gesellschafterliste] Click failed for "
                                f"{pattern}: {exc2}"
                            )
                            continue

            logger.warning(
                "[_find_gesellschafterliste] No GL found in document tree"
            )
            return False

        except TimeoutException as exc:
            logger.warning(f"[_find_gesellschafterliste] Timeout: {exc}")
            return False
        except Exception as exc:
            logger.warning(
                f"[_find_gesellschafterliste] GL not found: {exc}"
            )
            return False

    # ------------------------------------------------------------------
    # Tree expansion
    # ------------------------------------------------------------------

    def _expand_all_tree_nodes(self) -> None:
        """Expand all nodes in the PrimeFaces tree and on document pages.

        The handelsregister.de document page has the following structure:
          - "Dokumente zum Rechtstraeger" (contains Gesellschafterlisten!)
          - "Dokumente zur Registernummer" (usually only Sammelmappe)

        Both must be expanded, especially "Dokumente zum Rechtstraeger".
        """
        max_iterations = self.config.max_tree_iterations

        # Explicitly click on "Dokumente zum Rechtstraeger"
        rechtsträger_selectors = [
            "//span[contains(text(), 'Dokumente zum Rechtsträger')]",
            "//a[contains(text(), 'Dokumente zum Rechtsträger')]",
            "//*[contains(text(), 'Rechtsträger')]",
        ]

        for selector in rechtsträger_selectors:
            try:
                elements = self.driver.find_elements(By.XPATH, selector)
                for el in elements:
                    if el.is_displayed():
                        try:
                            parent = el.find_element(By.XPATH, "./..")
                            toggler = parent.find_element(
                                By.CSS_SELECTOR,
                                ".ui-tree-toggler, [class*='toggler'], "
                                "[class*='expand'], span[class*='icon']",
                            )
                            self.driver.execute_script(
                                "arguments[0].click();", toggler
                            )
                            logger.info(
                                "[_expand_all_tree_nodes] Clicked toggler next to "
                                "'Dokumente zum Rechtstraeger'"
                            )
                        except (NoSuchElementException, StaleElementReferenceException):
                            self.driver.execute_script(
                                "arguments[0].click();", el
                            )
                            logger.info(
                                "[_expand_all_tree_nodes] Direct click on "
                                "'Dokumente zum Rechtstraeger'"
                            )
                        time.sleep(
                            random.uniform(*self.config.element_interaction_delay)
                        )
                        break
            except (NoSuchElementException, StaleElementReferenceException) as exc:
                logger.debug(
                    f"[_expand_all_tree_nodes] Rechtstraeger expansion failed: {exc}"
                )

        iteration = 0
        for iteration in range(max_iterations):
            expanded_something = False

            # Method 1: PrimeFaces tree togglers
            togglers = self.driver.find_elements(
                By.CSS_SELECTOR,
                ".ui-tree-toggler, .ui-treetable-toggler, [class*='tree-toggler']",
            )

            for toggler in togglers:
                try:
                    parent_node = toggler.find_element(
                        By.XPATH, "./ancestor::li[1]"
                    )
                    classes = parent_node.get_attribute("class") or ""
                    aria_expanded = parent_node.get_attribute("aria-expanded")

                    is_collapsed = (
                        "collapsed" in classes.lower()
                        or "ui-treenode-collapsed" in classes
                        or aria_expanded == "false"
                    )

                    if is_collapsed:
                        self.driver.execute_script(
                            "arguments[0].click();", toggler
                        )
                        expanded_something = True
                        logger.debug(
                            f"[_expand_all_tree_nodes] Node expanded "
                            f"(iteration {iteration})"
                        )
                        time.sleep(
                            random.uniform(*self.config.tree_expansion_delay)
                        )
                except (
                    NoSuchElementException,
                    StaleElementReferenceException,
                ):
                    continue

            # Method 2: Expand specific document categories
            doc_category_texts = [
                "Dokumente zum Rechtsträger",
                "Dokumente zur Registernummer",
                "Liste der Gesellschafter",
                "Gesellschafterliste",
            ]

            for text in doc_category_texts:
                try:
                    xpath = f"//*[contains(text(), '{text}')]"
                    elements = self.driver.find_elements(By.XPATH, xpath)

                    for el in elements:
                        if not el.is_displayed():
                            continue

                        try:
                            container = el.find_element(
                                By.XPATH,
                                "./ancestor::*[contains(@class, 'node') "
                                "or contains(@class, 'item')][1]",
                            )
                            toggler = container.find_element(
                                By.CSS_SELECTOR,
                                "[class*='toggler'], [class*='expand'], "
                                "[class*='icon-plus'], span[class*='icon']",
                            )
                            if toggler.is_displayed():
                                self.driver.execute_script(
                                    "arguments[0].click();", toggler
                                )
                                expanded_something = True
                                time.sleep(0.5)
                        except (
                            NoSuchElementException,
                            StaleElementReferenceException,
                        ):
                            pass
                except (
                    NoSuchElementException,
                    StaleElementReferenceException,
                ):
                    continue

            # Method 3: All still-collapsed nodes
            collapsed_nodes = self.driver.find_elements(
                By.CSS_SELECTOR,
                "[aria-expanded='false'], .collapsed, .ui-treenode-collapsed",
            )

            for node in collapsed_nodes:
                try:
                    toggler = node.find_element(
                        By.CSS_SELECTOR,
                        ".ui-tree-toggler, [class*='toggler'], span:first-child",
                    )
                    if toggler.is_displayed():
                        self.driver.execute_script(
                            "arguments[0].click();", toggler
                        )
                        expanded_something = True
                        time.sleep(0.3)
                except (
                    NoSuchElementException,
                    StaleElementReferenceException,
                ):
                    continue

            # Method 4: Icons indicating collapsed state
            expand_icons = self.driver.find_elements(
                By.CSS_SELECTOR,
                "[class*='plus'], [class*='right'], [class*='collapsed'] span, "
                ".ui-icon-triangle-1-e, .ui-icon-plusthick",
            )

            for icon in expand_icons:
                try:
                    if icon.is_displayed():
                        self.driver.execute_script(
                            "arguments[0].click();", icon
                        )
                        expanded_something = True
                        time.sleep(0.3)
                except (
                    NoSuchElementException,
                    StaleElementReferenceException,
                    ElementClickInterceptedException,
                ) as exc:
                    logger.debug(
                        f"[_expand_all_tree_nodes] Icon click failed: {exc}"
                    )
                    continue

            if not expanded_something:
                logger.debug(
                    f"[_expand_all_tree_nodes] No more nodes to expand "
                    f"(iteration {iteration})"
                )
                break

            time.sleep(random.uniform(0.3, 0.7))

        logger.debug(
            f"[_expand_all_tree_nodes] Tree expansion complete after "
            f"{iteration + 1} iteration(s)"
        )

    # ------------------------------------------------------------------
    # PDF download (alternative path)
    # ------------------------------------------------------------------

    def _download_pdf(self, register_num: str) -> Optional[Path]:
        """Download the selected PDF/ZIP."""
        try:
            existing_files = set(self.download_dir.glob("*.*"))
            download_success = False

            # Strategy 1: download button in the download panel
            download_selectors = [
                "//button[contains(text(), 'Download')]",
                "//a[contains(text(), 'Download')]",
                "//button[contains(@class, 'download')]",
                "//a[contains(@class, 'download')]",
                "//*[@id='form:downloadButton']",
                "//button[@id='contentForm:btnDownload']",
                "//span[contains(@class, 'ui-button-text') "
                "and contains(text(), 'Download')]/..",
                "//span[contains(@class, 'ui-icon-arrowthickstop-1-s')]/..",
            ]

            for selector in download_selectors:
                try:
                    download_btn = self.driver.find_element(
                        By.XPATH, selector
                    )
                    if download_btn.is_displayed():
                        logger.debug(
                            f"[_download_pdf] Download button found: {selector}"
                        )
                        self.driver.execute_script(
                            "arguments[0].click();", download_btn
                        )
                        download_success = True
                        break
                except (
                    NoSuchElementException,
                    ElementClickInterceptedException,
                ):
                    continue

            if not download_success:
                # Fallback: iterate all visible buttons
                buttons = self.driver.find_elements(By.TAG_NAME, "button")
                for btn in buttons:
                    try:
                        if "download" in btn.text.lower() and btn.is_displayed():
                            self.driver.execute_script(
                                "arguments[0].click();", btn
                            )
                            download_success = True
                            logger.debug(
                                "[_download_pdf] Download button found by text"
                            )
                            break
                    except (
                        StaleElementReferenceException,
                        NoSuchElementException,
                    ) as exc:
                        logger.debug(
                            f"[_download_pdf] Button iteration error: {exc}"
                        )
                        continue

            if not download_success:
                logger.warning("[_download_pdf] No download button found")
                return None

            # Wait for download (PDF or ZIP)
            for i in range(self.config.download_timeout_seconds):
                time.sleep(1)

                new_files = set(self.download_dir.glob("*.*")) - existing_files
                new_files = {
                    f for f in new_files
                    if f.suffix not in (".crdownload", ".tmp", ".part")
                }

                if new_files:
                    newest = max(new_files, key=lambda p: p.stat().st_mtime)
                    logger.info(
                        f"[_download_pdf] Download complete: {newest.name}"
                    )

                    safe_name = self._sanitize_filename(register_num)

                    if newest.suffix.lower() == ".zip":
                        return self._extract_pdf_from_zip(newest, safe_name)

                    elif newest.suffix.lower() == ".pdf":
                        new_name = (
                            self.download_dir
                            / f"{safe_name}_gesellschafterliste.pdf"
                        )
                        if new_name.exists():
                            new_name.unlink()
                        newest.rename(new_name)
                        if not self._validate_downloaded_file(new_name):
                            logger.warning(
                                f"[_download_pdf] File {new_name} failed "
                                f"PDF magic-byte validation"
                            )
                        return new_name

                    else:
                        logger.warning(
                            f"[_download_pdf] Unexpected file format: "
                            f"{newest.suffix}"
                        )
                        return newest

                # Check if download is still in progress
                downloading = list(
                    self.download_dir.glob("*.crdownload")
                ) + list(self.download_dir.glob("*.tmp"))
                if downloading and i < (self.config.download_timeout_seconds - 5):
                    continue

            logger.warning(
                f"[_download_pdf] Download timeout after "
                f"{self.config.download_timeout_seconds} seconds"
            )
            return None

        except Exception as exc:
            logger.error(f"[_download_pdf] Download error: {exc}")
            logger.debug(traceback.format_exc())
            return None

    # ------------------------------------------------------------------
    # ZIP extraction
    # ------------------------------------------------------------------

    def _extract_pdf_from_zip(
        self, zip_path: Path, base_name: str
    ) -> Optional[Path]:
        """Extract PDF or TIF from a ZIP file.

        handelsregister.de delivers older documents as TIF scans instead of PDF.
        """
        extracted_path: Optional[Path] = None

        try:
            with zipfile.ZipFile(zip_path, "r") as zf:
                all_files = zf.namelist()
                pdf_files = [f for f in all_files if f.lower().endswith(".pdf")]
                tif_files = [
                    f
                    for f in all_files
                    if f.lower().endswith((".tif", ".tiff"))
                ]

                target_file: Optional[str] = None
                target_ext: Optional[str] = None

                if pdf_files:
                    target_file = pdf_files[0]
                    target_ext = ".pdf"
                elif tif_files:
                    target_file = tif_files[0]
                    target_ext = ".tif"
                    logger.info(
                        f"[_extract_pdf_from_zip] No PDF in ZIP, "
                        f"but TIF scan found: {target_file}"
                    )

                if not target_file:
                    logger.warning(
                        f"[_extract_pdf_from_zip] No PDF/TIF in ZIP: {zip_path}"
                    )
                    logger.debug(
                        f"[_extract_pdf_from_zip] ZIP contents: {all_files}"
                    )
                    return None

                extracted = zf.extract(target_file, self.download_dir)

                new_name = (
                    self.download_dir
                    / f"{base_name}_gesellschafterliste{target_ext}"
                )
                if new_name.exists():
                    new_name.unlink()

                Path(extracted).rename(new_name)
                extracted_path = new_name

            # Validate PDF magic bytes (only for .pdf files)
            if extracted_path and extracted_path.suffix.lower() == ".pdf":
                if not self._validate_downloaded_file(extracted_path):
                    logger.warning(
                        f"[_extract_pdf_from_zip] File {extracted_path} failed "
                        f"PDF magic-byte validation"
                    )

            # Delete ZIP (outside the with-block so ZIP handle is closed)
            try:
                time.sleep(0.5)  # Brief wait for Windows to release file handle
                zip_path.unlink()
                logger.debug(f"[_extract_pdf_from_zip] ZIP deleted: {zip_path}")
            except OSError as exc:
                logger.debug(
                    f"[_extract_pdf_from_zip] Could not delete ZIP "
                    f"(will be cleaned up later): {exc}"
                )

            logger.info(
                f"[_extract_pdf_from_zip] Document extracted: {extracted_path}"
            )
            return extracted_path

        except zipfile.BadZipFile as exc:
            logger.error(
                f"[_extract_pdf_from_zip] Corrupt ZIP file {zip_path}: {exc}"
            )
            return None
        except Exception as exc:
            logger.error(f"[_extract_pdf_from_zip] ZIP extraction failed: {exc}")
            if extracted_path and extracted_path.exists():
                return extracted_path
            return None

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> GesellschafterlistenDownloader:
        self.start()
        return self

    def __exit__(self, exc_type: type | None, exc_val: BaseException | None, exc_tb: object) -> None:
        self.stop()


# ---------------------------------------------------------------------------
# CLI for single test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    if len(sys.argv) < 2:
        print("Usage: python dk_downloader.py <registernummer> [gericht]")
        print("Beispiel: python dk_downloader.py 'HRB 12345 B' 'Berlin'")
        sys.exit(1)

    register_num = sys.argv[1]
    court = sys.argv[2] if len(sys.argv) > 2 else ""

    with GesellschafterlistenDownloader(
        Path("../pdfs"), headless=False, debug=True
    ) as downloader:
        result = downloader.download(register_num, court)

        print(f"\nErgebnis:")
        print(f"  Erfolgreich: {result.success}")
        print(f"  PDF-Pfad: {result.pdf_path}")
        print(f"  Keine GL: {result.no_gl_available}")
        print(f"  Fehler: {result.error}")
