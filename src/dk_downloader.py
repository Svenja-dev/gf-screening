"""
Selenium-basierter Downloader für Gesellschafterlisten von handelsregister.de

Lädt die DK-Dokumente (Gesellschafterlisten) herunter.
Rate Limit: Max 55 Abrufe/Stunde (Sicherheitsmarge unter 60).
"""

import time
import random
import logging
from pathlib import Path
from typing import Optional, Tuple
from dataclasses import dataclass

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    ElementClickInterceptedException,
    StaleElementReferenceException
)
from selenium.webdriver.common.action_chains import ActionChains

try:
    from webdriver_manager.chrome import ChromeDriverManager
    USE_WEBDRIVER_MANAGER = True
except ImportError:
    USE_WEBDRIVER_MANAGER = False

logger = logging.getLogger(__name__)


@dataclass
class DownloadResult:
    """Ergebnis eines Download-Versuchs."""
    success: bool
    pdf_path: Optional[Path] = None
    error: Optional[str] = None
    no_gl_available: bool = False  # Keine Gesellschafterliste vorhanden


class RateLimiter:
    """Rate-Limiter für API-Aufrufe."""

    def __init__(self, calls_per_hour: int = 55):
        self.min_interval = 3600 / calls_per_hour  # Sekunden zwischen Aufrufen
        self.last_call = 0

    def wait(self):
        """Wartet bis nächster Aufruf erlaubt ist."""
        elapsed = time.time() - self.last_call

        if elapsed < self.min_interval:
            sleep_time = self.min_interval - elapsed
            # Zufällige Variation hinzufügen (1-5 Sekunden)
            sleep_time += random.uniform(1, 5)
            logger.debug(f"Rate-Limit: Warte {sleep_time:.1f}s")
            time.sleep(sleep_time)

        self.last_call = time.time()


class GesellschafterlistenDownloader:
    """
    Selenium-Scraper für handelsregister.de.

    Lädt Gesellschafterlisten als PDF herunter.
    """

    BASE_URL = "https://www.handelsregister.de/rp_web/erweitertesuche/welcome.xhtml"

    def __init__(self, download_dir: Path, headless: bool = True, debug: bool = False):
        """
        Initialisiert den Downloader.

        Args:
            download_dir: Verzeichnis für heruntergeladene PDFs
            headless: Browser ohne GUI starten
            debug: Debug-Screenshots speichern
        """
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.headless = headless
        self.debug = debug
        self.debug_dir = Path(download_dir).parent / "debug"
        if self.debug:
            self.debug_dir.mkdir(parents=True, exist_ok=True)
        self.driver = None
        self.rate_limiter = RateLimiter(calls_per_hour=55)
        self._debug_counter = 0

    def _save_debug_screenshot(self, name: str):
        """Speichert einen Debug-Screenshot."""
        if self.debug and self.driver:
            self._debug_counter += 1
            path = self.debug_dir / f"debug_{self._debug_counter:02d}_{name}.png"
            try:
                self.driver.save_screenshot(str(path))
                logger.debug(f"Debug-Screenshot: {path}")
            except Exception as e:
                logger.debug(f"Screenshot fehlgeschlagen: {e}")

    # Mapping von Bundesland-Abkürzungen zu Gerichtsnamen
    COURT_MAPPINGS = {
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


    def _setup_driver(self) -> webdriver.Chrome:
        """Konfiguriert Chrome WebDriver."""
        options = Options()

        # Download-Einstellungen
        prefs = {
            "download.default_directory": str(self.download_dir.absolute()),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "plugins.always_open_pdf_externally": True,
            "safebrowsing.enabled": True
        }
        options.add_experimental_option("prefs", prefs)

        # Anti-Detection
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)

        if self.headless:
            options.add_argument("--headless=new")

        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

        if USE_WEBDRIVER_MANAGER:
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=options)
        else:
            driver = webdriver.Chrome(options=options)

        # Anti-Detection Script
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
            """
        })

        return driver

    def start(self):
        """Startet den Browser."""
        if self.driver is None:
            logger.info("Starte Chrome Browser...")
            self.driver = self._setup_driver()

    def stop(self):
        """Beendet den Browser."""
        if self.driver:
            logger.info("Beende Chrome Browser...")
            self.driver.quit()
            self.driver = None

    def download(self, register_num: str, court: str = "") -> DownloadResult:
        """
        Lädt Gesellschafterliste für eine Firma herunter.

        Args:
            register_num: Vollständige Registernummer (z.B. "HRB 12345 B")
            court: Registergericht (optional, für genauere Suche)

        Returns:
            DownloadResult mit Pfad zur PDF oder Fehlermeldung
        """
        self.start()
        self.rate_limiter.wait()

        try:
            # Registernummer parsen
            reg_type, reg_number, reg_suffix = self._parse_register_num(register_num)

            if not reg_type or not reg_number:
                return DownloadResult(
                    success=False,
                    error=f"Ungültige Registernummer: {register_num}"
                )

            logger.info(f"Suche: {reg_type} {reg_number} {reg_suffix or ''} ({court or 'alle Gerichte'})")

            # 1. Zur Suchseite navigieren
            self.driver.get(self.BASE_URL)
            # Längere Pause um natürliches Verhalten zu simulieren
            time.sleep(random.uniform(4, 6))
            self._save_debug_screenshot("01_start_page")

            # 2. Suchformular ausfüllen
            self._fill_search_form(reg_type, reg_number, court)
            self._save_debug_screenshot("02_form_filled")

            # 3. Suche absenden
            self._submit_search()
            self._save_debug_screenshot("03_after_search")

            # 4. Auf Ergebnis warten und korrektes anklicken (nach Gericht UND Typ filtern)
            if not self._click_correct_result(court, reg_type):
                self._save_debug_screenshot("04_no_results")
                return DownloadResult(
                    success=False,
                    error="Keine passenden Suchergebnisse gefunden"
                )
            self._save_debug_screenshot("05_result_clicked")

            # 5. DK-Dokumente herunterladen (direkter Download über DK-Link)
            # Die DK-Links triggern direkt einen Download (PrimeFaces.monitorDownload)
            pdf_path = self._download_dk_documents(register_num)

            if not pdf_path:
                self._save_debug_screenshot("06_no_download")
                logger.warning(f"Keine Dokumente für {register_num} heruntergeladen")
                return DownloadResult(
                    success=True,
                    no_gl_available=True
                )

            if pdf_path:
                logger.info(f"Erfolgreich heruntergeladen: {pdf_path}")
                return DownloadResult(success=True, pdf_path=pdf_path)
            else:
                return DownloadResult(
                    success=False,
                    error="Download fehlgeschlagen"
                )

        except TimeoutException as e:
            logger.error(f"Timeout für {register_num}: {e}")
            return DownloadResult(success=False, error=f"Timeout: {e}")

        except Exception as e:
            logger.error(f"Fehler für {register_num}: {e}")
            return DownloadResult(success=False, error=str(e))

    def _parse_register_num(self, register_num: str) -> Tuple[str, str, Optional[str]]:
        """
        Parst Registernummer in Typ, Nummer und Suffix.

        Beispiele:
        - "HRB 12345" -> ("HRB", "12345", None)
        - "HRB 12345 B" -> ("HRB", "12345", "B")
        - "HRB12345" -> ("HRB", "12345", None)
        """
        import re

        # Normalisieren
        register_num = register_num.strip().upper()

        # Pattern: Typ + Nummer + optionales Suffix
        match = re.match(r"(HRB|HRA|GNR|VR|PR)\s*(\d+)\s*([A-Z])?", register_num)

        if match:
            return match.group(1), match.group(2), match.group(3)

        return None, None, None

    def _fill_search_form(self, reg_type: str, reg_number: str, court: str):
        """Füllt das Suchformular aus."""
        wait = WebDriverWait(self.driver, 10)

        # Cookie-Banner akzeptieren falls vorhanden
        try:
            cookie_btn = self.driver.find_element(By.XPATH, '//a[contains(text(), "Verstanden")]')
            cookie_btn.click()
            time.sleep(1)
        except:
            pass

        # WICHTIG: Mindestens ein Bundesland muss ausgewählt werden!
        # Sonst kommt "Registernummer alleine reicht nicht aus"
        self._select_bundeslaender(court)

        # Registerart auswählen (PrimeFaces SelectOneMenu)
        try:
            # Klicke auf das Dropdown um es zu öffnen
            reg_type_dropdown = wait.until(
                EC.element_to_be_clickable((By.ID, "form:registerArt"))
            )
            reg_type_dropdown.click()
            time.sleep(0.5)

            # Wähle die richtige Option aus der Liste
            options = self.driver.find_elements(By.CSS_SELECTOR, "#form\\:registerArt_panel li")
            for opt in options:
                if opt.text.strip() == reg_type or opt.get_attribute("data-label") == reg_type:
                    opt.click()
                    break
            time.sleep(0.3)
        except Exception as e:
            logger.debug(f"Registerart-Auswahl übersprungen: {e}")

        # Registernummer eingeben
        try:
            reg_num_field = wait.until(
                EC.presence_of_element_located((By.ID, "form:registerNummer"))
            )
            reg_num_field.clear()
            reg_num_field.send_keys(reg_number)
        except Exception as e:
            logger.error(f"Registernummer-Feld nicht gefunden: {e}")

        # Gericht auswählen (falls angegeben) - PrimeFaces AutoComplete
        if court:
            try:
                court_input = self.driver.find_element(By.ID, "form:registergericht_input")
                court_input.clear()
                court_input.send_keys(court[:15])
                time.sleep(1.5)

                # Autocomplete-Vorschläge warten und auswählen
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
            except Exception as e:
                logger.debug(f"Gericht-Auswahl übersprungen: {e}")

    def _select_bundeslaender(self, court: str):
        """Wählt passende Bundesländer basierend auf dem Gericht aus.

        WICHTIG: Max. 2 Bundesländer erlaubt auf handelsregister.de!
        """
        # Erweiterte Mapping von Gerichtsstädten zu Bundesländern
        CITY_TO_BUNDESLAND = {
            # Bayern
            "münchen": "Bayern", "munich": "Bayern", "nürnberg": "Bayern",
            "augsburg": "Bayern", "würzburg": "Bayern", "regensburg": "Bayern",
            "passau": "Bayern", "bayreuth": "Bayern", "ingolstadt": "Bayern",
            "kempten": "Bayern", "landshut": "Bayern", "fürth": "Bayern",
            # Berlin
            "berlin": "Berlin", "charlottenburg": "Berlin",
            # Brandenburg
            "potsdam": "Brandenburg", "cottbus": "Brandenburg", "frankfurt (oder)": "Brandenburg",
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
            "stralsund": "Mecklenburg-Vorpommern", "neubrandenburg": "Mecklenburg-Vorpommern",
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
            "siegburg": "Nordrhein-Westfalen", "wuppertal": "Nordrhein-Westfalen",
            "hagen": "Nordrhein-Westfalen", "krefeld": "Nordrhein-Westfalen",
            # Rheinland-Pfalz
            "mainz": "Rheinland-Pfalz", "koblenz": "Rheinland-Pfalz",
            "trier": "Rheinland-Pfalz", "ludwigshafen": "Rheinland-Pfalz",
            "kaiserslautern": "Rheinland-Pfalz", "bad kreuznach": "Rheinland-Pfalz",
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
            # Thüringen
            "erfurt": "Thüringen", "jena": "Thüringen", "gera": "Thüringen",
            # Baden-Württemberg
            "stuttgart": "Baden-Württemberg", "mannheim": "Baden-Württemberg",
            "karlsruhe": "Baden-Württemberg", "freiburg": "Baden-Württemberg",
            "ulm": "Baden-Württemberg", "heidelberg": "Baden-Württemberg",
            "heilbronn": "Baden-Württemberg", "konstanz": "Baden-Württemberg",
        }

        # Checkbox-IDs für die Bundesländer (Format: form:{Bundesland}_input)
        # Hinweis: Umlaute werden direkt verwendet (Baden-Württemberg, Thüringen)
        BUNDESLAND_IDS = {
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

        bundeslaender_to_select = []

        # Wenn ein Gericht angegeben ist, das passende Bundesland finden
        if court:
            court_lower = court.lower()
            for city, bundesland in CITY_TO_BUNDESLAND.items():
                if city in court_lower:
                    bundeslaender_to_select.append(bundesland)
                    break

        # Wenn kein Bundesland erkannt, versuche direkte Zuordnung über Gericht-Text
        if not bundeslaender_to_select and court:
            # Versuche, das Bundesland direkt im Gerichtstext zu finden
            court_lower = court.lower()
            for bundesland in BUNDESLAND_IDS.keys():
                if bundesland.lower() in court_lower:
                    bundeslaender_to_select.append(bundesland)
                    logger.debug(f"Bundesland aus Gerichtstext erkannt: {bundesland}")
                    break

        # Immer noch kein Bundesland? Fehler loggen und leere Liste zurückgeben
        if not bundeslaender_to_select:
            logger.warning(f"Kein Bundesland für Gericht '{court}' erkannt - Suche wird wahrscheinlich fehlschlagen")
            # Als Fallback: Bayern und Niedersachsen (häufigste)
            bundeslaender_to_select = ["Bayern", "Niedersachsen"]
            logger.debug("Fallback: Bayern und Niedersachsen ausgewählt")

        # Maximal 2 Bundesländer (Website-Limit!)
        bundeslaender_to_select = bundeslaender_to_select[:2]

        # Checkboxen anklicken
        selected_count = 0
        for bundesland in bundeslaender_to_select:
            checkbox_id = BUNDESLAND_IDS.get(bundesland)
            if checkbox_id:
                try:
                    checkbox = self.driver.find_element(By.ID, checkbox_id)
                    if not checkbox.is_selected():
                        # JavaScript-Click für versteckte Checkboxen
                        self.driver.execute_script("arguments[0].click();", checkbox)
                        selected_count += 1
                        logger.debug(f"Bundesland ausgewählt: {bundesland}")
                        time.sleep(0.3)  # Kurze Pause zwischen Klicks
                except Exception as e:
                    logger.debug(f"Bundesland-Auswahl fehlgeschlagen: {bundesland}: {e}")

        if selected_count == 0:
            logger.warning("Kein Bundesland konnte ausgewählt werden!")
        else:
            logger.info(f"{selected_count} Bundesland/Bundesländer ausgewählt: {bundeslaender_to_select}")

    def _submit_search(self):
        """Sendet die Suche ab."""
        wait = WebDriverWait(self.driver, 10)

        try:
            # Suchen-Button hat die ID form:btnSuche
            submit_btn = wait.until(
                EC.element_to_be_clickable((By.ID, "form:btnSuche"))
            )
            self.driver.execute_script("arguments[0].click();", submit_btn)
            logger.debug("Suche abgesendet")
        except Exception as e:
            logger.warning(f"Suchen-Button nicht gefunden: {e}")
            # Fallback: Enter im Registernummer-Feld
            try:
                reg_field = self.driver.find_element(By.ID, "form:registerNummer")
                from selenium.webdriver.common.keys import Keys
                reg_field.send_keys(Keys.RETURN)
            except:
                pass

        # Warten auf Ergebnisse (längere Zeit für AJAX-Antwort und natürliches Verhalten)
        time.sleep(random.uniform(5, 8))

    def _click_correct_result(self, target_court: str, register_type: str = "HRB") -> bool:
        """
        Klickt auf das Suchergebnis mit dem passenden Gericht und Registertyp.

        Args:
            target_court: Gesuchtes Registergericht (z.B. "Nürnberg")
            register_type: Registertyp (HRB, HRA, etc.) - wichtig um GnR/VR auszuschließen
        """
        wait = WebDriverWait(self.driver, 10)

        try:
            # Ergebnistabelle finden
            result_table = wait.until(
                EC.presence_of_element_located((By.ID, "ergebnissForm:selectedSuchErgebnisFormTable_data"))
            )

            # Alle Zeilen durchgehen
            rows = result_table.find_elements(By.CSS_SELECTOR, "tr")

            if not rows:
                logger.warning("Keine Suchergebnisse in Tabelle")
                return False

            target_court_lower = target_court.lower() if target_court else ""
            register_type_upper = register_type.upper() if register_type else "HRB"

            # Beste Übereinstimmung suchen: Gericht UND Registertyp
            for row in rows:
                try:
                    row_text = row.text.upper()

                    # Prüfe ob Registertyp in der Zeile vorkommt
                    has_correct_type = register_type_upper in row_text

                    # Prüfe ob es ein "falscher" Registertyp ist (VR, GnR, PR)
                    is_wrong_type = any(wrong in row_text for wrong in ["VR ", " VR", "GNR ", " GNR", "PR ", " PR"])

                    # Prüfe ob Gericht passt
                    has_correct_court = target_court_lower and target_court_lower in row_text.lower()

                    # Beste Übereinstimmung: korrekter Typ UND korrektes Gericht
                    if has_correct_type and has_correct_court and not is_wrong_type:
                        row.click()
                        logger.info(f"Perfekte Übereinstimmung: {register_type_upper} in {target_court}")
                        time.sleep(random.uniform(1, 2))
                        return True

                except StaleElementReferenceException:
                    continue

            # Zweite Runde: Nur Registertyp (ohne Gericht)
            for row in rows:
                try:
                    row_text = row.text.upper()
                    has_correct_type = register_type_upper in row_text
                    is_wrong_type = any(wrong in row_text for wrong in ["VR ", " VR", "GNR ", " GNR", "PR ", " PR"])

                    if has_correct_type and not is_wrong_type:
                        row.click()
                        logger.info(f"Ergebnis mit Registertyp {register_type_upper} gefunden (ohne Gericht-Match)")
                        time.sleep(random.uniform(1, 2))
                        return True

                except StaleElementReferenceException:
                    continue

            # Dritte Runde: Nur Gericht
            for row in rows:
                try:
                    row_text = row.text.lower()
                    if target_court_lower and target_court_lower in row_text:
                        row.click()
                        logger.warning(f"Ergebnis mit Gericht '{target_court}' gefunden (ohne Typ-Match)")
                        time.sleep(random.uniform(1, 2))
                        return True

                except StaleElementReferenceException:
                    continue

            # Fallback: Erste Zeile die NICHT VR/GnR/PR ist
            for row in rows:
                try:
                    row_text = row.text.upper()
                    is_wrong_type = any(wrong in row_text for wrong in ["VR ", " VR", "GNR ", " GNR", "PR ", " PR"])

                    if not is_wrong_type:
                        row.click()
                        logger.warning("Nehme erste Zeile die kein VR/GnR/PR ist")
                        time.sleep(random.uniform(1, 2))
                        return True

                except StaleElementReferenceException:
                    continue

            logger.warning("Keine passenden Suchergebnisse gefunden")
            return False

        except TimeoutException:
            logger.warning("Keine Suchergebnisse gefunden")
            return False

    def _click_first_result(self) -> bool:
        """Veraltet - benutze _click_correct_result stattdessen."""
        return self._click_correct_result("")

    def _select_and_download_gesellschafterliste(self, register_num: str) -> Optional[Path]:
        """
        Findet und lädt die Gesellschafterliste auf der Dokumentenseite herunter.

        Diese Methode wird aufgerufen, nachdem wir auf der "Freigegebene Dokumente"
        Seite gelandet sind.
        """
        try:
            # Aktuelle Dateien merken
            existing_files = set(self.download_dir.glob("*.*"))
            safe_name = register_num.replace(" ", "_").replace("/", "-")

            # 1. Dokumentenbaum expandieren (alle Toggler klicken)
            self._expand_all_tree_nodes()
            time.sleep(1)

            # 2. Nach "Gesellschafterliste" oder "Liste der Gesellschafter" suchen
            gl_patterns = [
                "Liste der Gesellschafter",
                "Gesellschafterliste",
                "Gesellschafter-Liste",
            ]

            gl_found = False
            for pattern in gl_patterns:
                try:
                    # Suche nach dem Text im Dokumentenbaum
                    gl_elements = self.driver.find_elements(
                        By.XPATH,
                        f"//*[contains(text(), '{pattern}')]"
                    )

                    for el in gl_elements:
                        try:
                            if not el.is_displayed():
                                continue

                            # Klicken um das Dokument auszuwählen
                            self.driver.execute_script("arguments[0].click();", el)
                            logger.info(f"Gesellschafterliste gefunden: '{pattern}'")
                            gl_found = True
                            time.sleep(random.uniform(1, 2))
                            break
                        except Exception:
                            continue

                    if gl_found:
                        break
                except Exception:
                    continue

            if not gl_found:
                logger.warning("Keine Gesellschafterliste im Dokumentenbaum gefunden")
                # Screenshot für Debugging
                self._save_debug_screenshot("no_gl_in_tree")
                return None

            # 3. Download-Button klicken
            time.sleep(1)
            self._save_debug_screenshot("gl_selected")

            download_buttons = self.driver.find_elements(
                By.XPATH,
                "//button[contains(text(), 'Download')] | "
                "//a[contains(text(), 'Download')] | "
                "//input[@value='Download'] | "
                "//*[contains(@class, 'download')]"
            )

            for btn in download_buttons:
                try:
                    if btn.is_displayed():
                        self.driver.execute_script("arguments[0].click();", btn)
                        logger.info("Download-Button geklickt")
                        break
                except Exception:
                    continue

            # 4. Warten auf Download
            for i in range(45):
                time.sleep(1)

                new_files = set(self.download_dir.glob("*.*")) - existing_files
                new_files = {f for f in new_files
                            if not f.suffix in ['.crdownload', '.tmp', '.part']}

                if new_files:
                    newest = max(new_files, key=lambda p: p.stat().st_mtime)
                    logger.info(f"Download abgeschlossen: {newest.name}")

                    if newest.suffix.lower() == '.zip':
                        return self._extract_pdf_from_zip(newest, safe_name)
                    elif newest.suffix.lower() == '.pdf':
                        new_name = self.download_dir / f"{safe_name}_gesellschafterliste.pdf"
                        if new_name.exists():
                            new_name.unlink()
                        newest.rename(new_name)
                        return new_name
                    return newest

            logger.warning("Download-Timeout für Gesellschafterliste")
            return None

        except Exception as e:
            logger.error(f"Fehler beim Herunterladen der Gesellschafterliste: {e}")
            return None

    def _download_dk_documents(self, register_num: str) -> Optional[Path]:
        """
        Lädt DK-Dokumente herunter durch Klicken des DK-Links.

        Der DK-Link auf handelsregister.de triggert direkt einen Download
        (PrimeFaces.monitorDownload).

        Returns:
            Pfad zur heruntergeladenen Datei (PDF/ZIP) oder None
        """
        import zipfile

        try:
            # Aktuelle Dateien im Download-Verzeichnis merken
            existing_files = set(self.download_dir.glob("*.*"))

            # DK-Link finden (in der korrekten Zeile)
            # Die Links haben die Klasse 'dokumentList' und enthalten span mit 'DK'
            dk_links = self.driver.find_elements(
                By.XPATH,
                "//a[contains(@class, 'dokumentList') and span[text()='DK']]"
            )

            if not dk_links:
                # Fallback: Alle Links mit DK-Text
                dk_links = self.driver.find_elements(
                    By.XPATH,
                    "//a[span[text()='DK']] | //a[contains(text(), 'DK')]"
                )

            if not dk_links:
                logger.warning("Keine DK-Links gefunden")
                return None

            logger.info(f"{len(dk_links)} DK-Links gefunden")

            # Auf den ersten sichtbaren DK-Link klicken
            for link in dk_links:
                try:
                    if not link.is_displayed():
                        continue

                    # Zuerst die Zeile auswählen (wichtig für PrimeFaces)
                    try:
                        row = link.find_element(By.XPATH, "./ancestor::tr")
                        row.click()
                        time.sleep(random.uniform(1.5, 2.5))  # Längere Pause nach Zeilenauswahl
                    except:
                        pass

                    # Scrollen zum Element (natürliches Verhalten)
                    self.driver.execute_script(
                        "arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});",
                        link
                    )
                    time.sleep(random.uniform(1, 2))  # Pause nach Scrollen

                    # Maus bewegen (simuliert echtes Nutzerverhalten)
                    from selenium.webdriver.common.action_chains import ActionChains
                    actions = ActionChains(self.driver)
                    actions.move_to_element(link).pause(random.uniform(0.5, 1)).click().perform()

                    logger.info("DK-Link geklickt - warte auf Dokumentenseite...")
                    time.sleep(random.uniform(3, 5))

                    # Prüfen ob wir auf der Dokumentenseite sind
                    if "Freigegebene Dokumente" in self.driver.page_source or \
                       "Dokumente zum Rechtsträger" in self.driver.page_source:
                        logger.info("Dokumentenseite geladen - suche Gesellschafterliste")
                        self._save_debug_screenshot("dk_documents_page")

                        # Gesellschafterliste auf der Dokumentenseite finden und herunterladen
                        pdf_path = self._select_and_download_gesellschafterliste(register_num)
                        if pdf_path:
                            return pdf_path
                        else:
                            logger.warning("Keine Gesellschafterliste auf Dokumentenseite gefunden")
                            return None

                    # Prüfen ob wir auf einer Fehlerseite gelandet sind
                    if "error" in self.driver.current_url.lower():
                        logger.warning("Fehlerseite nach DK-Klick")
                        return None

                    # Warten auf direkten Download (falls kein Dokumentenbaum)
                    for i in range(30):
                        time.sleep(1)
                        new_files = set(self.download_dir.glob("*.*")) - existing_files
                        new_files = {f for f in new_files
                                    if not f.suffix in ['.crdownload', '.tmp', '.part']}

                        if new_files:
                            newest = max(new_files, key=lambda p: p.stat().st_mtime)
                            safe_name = register_num.replace(" ", "_").replace("/", "-")
                            if newest.suffix.lower() == '.zip':
                                return self._extract_pdf_from_zip(newest, safe_name)
                            elif newest.suffix.lower() == '.pdf':
                                new_name = self.download_dir / f"{safe_name}_gesellschafterliste.pdf"
                                if new_name.exists():
                                    new_name.unlink()
                                newest.rename(new_name)
                                return new_name
                            return newest

                    logger.warning("Kein Download nach 30 Sekunden")
                    return None

                except Exception as e:
                    logger.debug(f"DK-Link Klick fehlgeschlagen: {e}")
                    continue

            return None

        except Exception as e:
            logger.error(f"Fehler beim DK-Download: {e}")
            return None

    def _open_dk_tab(self) -> bool:
        """
        Öffnet die DK-Seite (Dokumentenkopien).

        Auf der Suchergebnis-Seite gibt es für jede Zeile Links wie "DK", "HD", etc.
        Wir müssen den "DK"-Link in der aktuell ausgewählten Zeile klicken.
        """
        wait = WebDriverWait(self.driver, 15)

        try:
            time.sleep(1)

            # Methode 1: DK-Link in der ausgewählten/markierten Zeile finden
            # Die ausgewählte Zeile hat oft eine besondere CSS-Klasse
            selected_row_selectors = [
                # Ausgewählte Zeile (blau markiert)
                "//tr[contains(@class, 'ui-state-highlight')]//a[text()='DK']",
                "//tr[contains(@class, 'selected')]//a[text()='DK']",
                "//tr[@aria-selected='true']//a[text()='DK']",
                # Letzte Zeile mit Highlight
                "//tr[contains(@class, 'highlight')]//a[text()='DK']",
            ]

            for selector in selected_row_selectors:
                try:
                    dk_link = self.driver.find_element(By.XPATH, selector)
                    if dk_link.is_displayed():
                        self.driver.execute_script("arguments[0].click();", dk_link)
                        logger.info("DK-Link in ausgewählter Zeile geklickt")
                        time.sleep(3)  # Warten auf DK-Seite
                        return True
                except NoSuchElementException:
                    continue

            # Methode 2: Irgendeinen sichtbaren DK-Link klicken (Fallback)
            # Versuche verschiedene Textformate
            dk_links = self.driver.find_elements(
                By.XPATH,
                "//a[text()='DK'] | //a[normalize-space(text())='DK'] | "
                "//a[contains(text(), 'DK')] | //a[@title='DK'] | "
                "//span[text()='DK']/.. | //a[contains(@title, 'Dokumentenkopie')]"
            )

            # Aktuelle Fenster merken
            original_window = self.driver.current_window_handle
            original_windows = set(self.driver.window_handles)

            for link in dk_links:
                try:
                    if link.is_displayed():
                        # Zur Zeile scrollen
                        self.driver.execute_script("arguments[0].scrollIntoView(true);", link)
                        time.sleep(0.5)

                        # Klick ausführen
                        self.driver.execute_script("arguments[0].click();", link)
                        logger.info("DK-Link geklickt (Fallback)")
                        time.sleep(3)

                        # Prüfen ob neues Fenster geöffnet wurde
                        new_windows = set(self.driver.window_handles) - original_windows
                        if new_windows:
                            # Zu neuem Fenster wechseln
                            new_window = new_windows.pop()
                            self.driver.switch_to.window(new_window)
                            logger.info("Zu neuem Fenster gewechselt")
                            time.sleep(2)

                        return True
                except Exception as e:
                    logger.debug(f"DK-Link Klick fehlgeschlagen: {e}")
                    continue

            # Methode 3: Ergebniszeile finden und deren DK-Link klicken
            # Die Tabelle hat ID "ergebnissForm:selectedSuchErgebnisFormTable_data"
            try:
                result_table = self.driver.find_element(
                    By.ID, "ergebnissForm:selectedSuchErgebnisFormTable_data"
                )
                rows = result_table.find_elements(By.TAG_NAME, "tr")

                for row in rows:
                    try:
                        dk_link = row.find_element(By.XPATH, ".//a[text()='DK']")
                        if dk_link.is_displayed():
                            self.driver.execute_script("arguments[0].click();", dk_link)
                            logger.info("DK-Link in Ergebniszeile geklickt")
                            time.sleep(3)
                            return True
                    except NoSuchElementException:
                        continue
            except NoSuchElementException:
                pass

            logger.warning("Kein DK-Link gefunden")
            return False

        except Exception as e:
            logger.error(f"Fehler beim Öffnen der DK-Seite: {e}")
            return False

    def _find_gesellschafterliste(self) -> bool:
        """Findet und klickt auf Gesellschafterliste im Dokumentenbaum."""
        wait = WebDriverWait(self.driver, 10)

        try:
            # Warten bis Dokumentenbaum geladen
            time.sleep(2)

            # PrimeFaces Tree: Zuerst alle Knoten expandieren
            self._expand_all_tree_nodes()

            time.sleep(1)

            # Nach "Liste der Gesellschafter" oder "Gesellschafterliste" suchen
            gl_patterns = [
                "Liste der Gesellschafter",
                "Gesellschafterliste",
                "GL ",  # Abkürzung
            ]

            for pattern in gl_patterns:
                # Suche nach dem Text in Tree-Knoten
                gl_elements = self.driver.find_elements(
                    By.XPATH,
                    f"//span[contains(text(), '{pattern}')] | "
                    f"//td[contains(text(), '{pattern}')]"
                )

                for el in gl_elements:
                    try:
                        # In PrimeFaces Tree muss man auf den treenode-content klicken
                        # Versuche parent treenode zu finden
                        treenode = el.find_element(By.XPATH, "./ancestor::li[contains(@class, 'ui-treenode')]")
                        content = treenode.find_element(By.CSS_SELECTOR, ".ui-treenode-content")

                        # JavaScript click für zuverlässigere Interaktion
                        self.driver.execute_script("arguments[0].click();", content)
                        logger.info(f"Gesellschafterliste gefunden und ausgewählt: {pattern}")
                        time.sleep(1)
                        return True
                    except Exception:
                        try:
                            # Direkter Klick auf das Element
                            self.driver.execute_script("arguments[0].click();", el)
                            logger.info(f"Gesellschafterliste direkt geklickt: {pattern}")
                            time.sleep(1)
                            return True
                        except Exception as e2:
                            logger.debug(f"Klick fehlgeschlagen für {pattern}: {e2}")
                            continue

            logger.warning("Keine Gesellschafterliste im Dokumentenbaum gefunden")
            return False

        except Exception as e:
            logger.warning(f"Gesellschafterliste nicht gefunden: {e}")
            return False

    def _expand_all_tree_nodes(self):
        """Expandiert alle Knoten im PrimeFaces Tree und auf Dokumentenseiten."""
        max_iterations = 10

        for iteration in range(max_iterations):
            expanded_something = False

            # Methode 1: PrimeFaces Tree Toggler
            togglers = self.driver.find_elements(
                By.CSS_SELECTOR,
                ".ui-tree-toggler, .ui-treetable-toggler"
            )

            for toggler in togglers:
                try:
                    parent_node = toggler.find_element(By.XPATH, "./ancestor::li[1]")
                    classes = parent_node.get_attribute("class") or ""
                    if "collapsed" in classes.lower() or "ui-treenode-collapsed" in classes:
                        self.driver.execute_script("arguments[0].click();", toggler)
                        expanded_something = True
                        time.sleep(0.5)
                except Exception:
                    continue

            # Methode 2: Klickbare Kategorien auf Dokumentenseite
            # (z.B. "Dokumente zum Rechtsträger", "Dokumente zur Registernummer")
            category_selectors = [
                "//span[contains(text(), 'Dokumente zum')]",
                "//span[contains(text(), 'Dokumente zur')]",
                "//a[contains(text(), 'Dokumente')]",
                "//*[contains(@class, 'ui-panel-title')]",
                "//*[contains(@class, 'toggleable')]",
            ]

            for selector in category_selectors:
                try:
                    categories = self.driver.find_elements(By.XPATH, selector)
                    for cat in categories:
                        if cat.is_displayed():
                            # Prüfen ob es einen Pfeil/Toggler neben dem Text gibt
                            try:
                                parent = cat.find_element(By.XPATH, "./..")
                                # Klick auf den Parent (könnte ein Panel-Header sein)
                                self.driver.execute_script("arguments[0].click();", parent)
                                expanded_something = True
                                time.sleep(0.5)
                            except:
                                self.driver.execute_script("arguments[0].click();", cat)
                                expanded_something = True
                                time.sleep(0.5)
                except Exception:
                    continue

            # Methode 3: Alle Elemente mit "plus" oder "expand" Icons
            expand_icons = self.driver.find_elements(
                By.CSS_SELECTOR,
                "[class*='expand'], [class*='plus'], [class*='collapsed'] > span"
            )
            for icon in expand_icons:
                try:
                    if icon.is_displayed():
                        self.driver.execute_script("arguments[0].click();", icon)
                        expanded_something = True
                        time.sleep(0.3)
                except:
                    continue

            if not expanded_something:
                break

            time.sleep(0.5)

        logger.debug(f"Tree-Expansion nach {iteration + 1} Iterationen abgeschlossen")

    def _download_pdf(self, register_num: str) -> Optional[Path]:
        """Lädt das ausgewählte PDF/ZIP herunter."""
        import zipfile

        try:
            # Aktuelle Dateien im Download-Verzeichnis merken
            existing_files = set(self.download_dir.glob("*.*"))

            # Verschiedene Download-Strategien versuchen
            download_success = False

            # Strategie 1: Download-Button im Download-Panel
            download_selectors = [
                # PrimeFaces CommandButton
                "//button[contains(text(), 'Download')]",
                "//a[contains(text(), 'Download')]",
                # Button mit Download-Icon
                "//button[contains(@class, 'download')]",
                "//a[contains(@class, 'download')]",
                # CommandLink mit ID
                "//*[@id='form:downloadButton']",
                "//button[@id='contentForm:btnDownload']",
                # Generische Button-Suche
                "//span[contains(@class, 'ui-button-text') and contains(text(), 'Download')]/..",
                # Icon-basierter Download
                "//span[contains(@class, 'ui-icon-arrowthickstop-1-s')]/..",
            ]

            for selector in download_selectors:
                try:
                    download_btn = self.driver.find_element(By.XPATH, selector)
                    if download_btn.is_displayed():
                        logger.debug(f"Download-Button gefunden mit: {selector}")
                        self.driver.execute_script("arguments[0].click();", download_btn)
                        download_success = True
                        break
                except (NoSuchElementException, ElementClickInterceptedException):
                    continue

            if not download_success:
                # Fallback: Alle sichtbaren Buttons durchgehen
                buttons = self.driver.find_elements(By.TAG_NAME, "button")
                for btn in buttons:
                    try:
                        if "download" in btn.text.lower() and btn.is_displayed():
                            self.driver.execute_script("arguments[0].click();", btn)
                            download_success = True
                            logger.debug("Download über Button-Text gefunden")
                            break
                    except:
                        continue

            if not download_success:
                logger.warning("Kein Download-Button gefunden")
                return None

            # Warten auf Download (PDF oder ZIP)
            for i in range(45):  # Max 45 Sekunden
                time.sleep(1)

                # Neue Dateien suchen (PDF oder ZIP)
                new_files = set(self.download_dir.glob("*.*")) - existing_files

                # Temporäre/unvollständige Downloads ignorieren
                new_files = {f for f in new_files
                            if not f.suffix in ['.crdownload', '.tmp', '.part']}

                if new_files:
                    newest = max(new_files, key=lambda p: p.stat().st_mtime)
                    logger.info(f"Download abgeschlossen: {newest.name}")

                    # Safe filename erstellen
                    safe_name = register_num.replace(" ", "_").replace("/", "-")

                    # ZIP entpacken falls nötig
                    if newest.suffix.lower() == '.zip':
                        return self._extract_pdf_from_zip(newest, safe_name)

                    # PDF direkt umbenennen
                    elif newest.suffix.lower() == '.pdf':
                        new_name = self.download_dir / f"{safe_name}_gesellschafterliste.pdf"
                        if new_name.exists():
                            new_name.unlink()
                        newest.rename(new_name)
                        return new_name

                    else:
                        logger.warning(f"Unerwartetes Dateiformat: {newest.suffix}")
                        return newest

                # Prüfen ob Download noch läuft
                downloading = list(self.download_dir.glob("*.crdownload")) + \
                             list(self.download_dir.glob("*.tmp"))
                if downloading and i < 40:
                    continue

            logger.warning("Download-Timeout nach 45 Sekunden")
            return None

        except Exception as e:
            logger.error(f"Download-Fehler: {e}")
            return None

    def _extract_pdf_from_zip(self, zip_path: Path, base_name: str) -> Optional[Path]:
        """Extrahiert PDF aus ZIP-Datei."""
        import zipfile

        try:
            with zipfile.ZipFile(zip_path, 'r') as zf:
                # PDF in ZIP finden
                pdf_files = [f for f in zf.namelist() if f.lower().endswith('.pdf')]

                if not pdf_files:
                    logger.warning(f"Keine PDF in ZIP gefunden: {zip_path}")
                    return None

                # Erste/einzige PDF extrahieren
                pdf_name = pdf_files[0]
                extracted = zf.extract(pdf_name, self.download_dir)

                # Umbenennen
                new_name = self.download_dir / f"{base_name}_gesellschafterliste.pdf"
                if new_name.exists():
                    new_name.unlink()

                Path(extracted).rename(new_name)

                # ZIP löschen
                zip_path.unlink()

                logger.info(f"PDF aus ZIP extrahiert: {new_name}")
                return new_name

        except Exception as e:
            logger.error(f"ZIP-Extraktion fehlgeschlagen: {e}")
            return None

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()


# CLI für Einzeltest
if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    if len(sys.argv) < 2:
        print("Usage: python dk_downloader.py <registernummer> [gericht]")
        print("Beispiel: python dk_downloader.py 'HRB 12345 B' 'Berlin'")
        sys.exit(1)

    register_num = sys.argv[1]
    court = sys.argv[2] if len(sys.argv) > 2 else ""

    with GesellschafterlistenDownloader(Path("../pdfs"), headless=False) as downloader:
        result = downloader.download(register_num, court)

        print(f"\nErgebnis:")
        print(f"  Erfolgreich: {result.success}")
        print(f"  PDF-Pfad: {result.pdf_path}")
        print(f"  Keine GL: {result.no_gl_available}")
        print(f"  Fehler: {result.error}")
