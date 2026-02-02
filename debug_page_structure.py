"""Debug: Untersuche die Seitenstruktur nach Klick auf DK."""

import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

try:
    from webdriver_manager.chrome import ChromeDriverManager
    from selenium.webdriver.chrome.service import Service
    USE_WDM = True
except ImportError:
    USE_WDM = False


def main():
    options = Options()
    options.add_argument("--window-size=1920,1080")

    if USE_WDM:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
    else:
        driver = webdriver.Chrome(options=options)

    try:
        # Zur erweiterten Suche navigieren
        url = "https://www.handelsregister.de/rp_web/erweitertesuche/welcome.xhtml"
        print(f"1. Lade Seite: {url}")
        driver.get(url)
        time.sleep(3)

        # Bayern auswaehlen
        print("2. Bayern auswaehlen")
        bayern_cb = driver.find_element(By.ID, "form:Bayern_input")
        driver.execute_script("arguments[0].click();", bayern_cb)
        time.sleep(0.5)

        # Registernummer eingeben
        print("3. Registernummer eingeben: 12345")
        reg_field = driver.find_element(By.ID, "form:registerNummer")
        reg_field.send_keys("12345")

        # Suchen
        print("4. Suchen")
        search_btn = driver.find_element(By.ID, "form:btnSuche")
        driver.execute_script("arguments[0].click();", search_btn)
        time.sleep(5)

        driver.save_screenshot("debug/structure_01_results.png")
        print(f"   Gefunden: {driver.title}")

        # Alle Links mit "DK" im Text finden
        print("\n5. DK-Links analysieren:")
        dk_elements = driver.find_elements(By.XPATH, "//*[contains(text(), 'DK')]")
        for i, el in enumerate(dk_elements[:10]):
            tag = el.tag_name
            text = el.text[:50] if el.text else "(leer)"
            href = el.get_attribute("href") or "(kein href)"
            onclick = el.get_attribute("onclick") or "(kein onclick)"
            parent = el.find_element(By.XPATH, "./..").tag_name
            print(f"   [{i}] <{tag}> in <{parent}>: '{text}'")
            print(f"       href: {href[:80] if href else href}")
            print(f"       onclick: {onclick[:80] if onclick else onclick}")

        # Erste Ergebniszeile anklicken
        print("\n6. Erste Zeile anklicken")
        try:
            result_table = driver.find_element(By.ID, "ergebnissForm:selectedSuchErgebnisFormTable_data")
            first_row = result_table.find_element(By.CSS_SELECTOR, "tr")
            first_row.click()
            time.sleep(2)
            driver.save_screenshot("debug/structure_02_row_selected.png")
            print("   Zeile ausgewaehlt")
        except Exception as e:
            print(f"   Fehler: {e}")

        # Nach Tabs oder Panels suchen
        print("\n7. Tabs/Panels suchen:")
        tabs = driver.find_elements(By.CSS_SELECTOR, ".ui-tabs-nav li, [role='tab']")
        for tab in tabs:
            print(f"   Tab: '{tab.text}'")

        # Fenster/Frames pruefen
        print(f"\n8. Fenster: {len(driver.window_handles)}")
        print(f"9. Frames: {len(driver.find_elements(By.TAG_NAME, 'iframe'))}")

        # DK-Link direkt anklicken
        print("\n10. DK-Link direkt anklicken")
        dk_links = driver.find_elements(By.XPATH, "//a[contains(text(), 'DK')]")
        if dk_links:
            # Ersten sichtbaren DK-Link klicken
            for link in dk_links:
                if link.is_displayed():
                    print(f"    Klicke: {link.get_attribute('outerHTML')[:100]}")
                    original_windows = set(driver.window_handles)
                    link.click()
                    time.sleep(3)

                    new_windows = set(driver.window_handles) - original_windows
                    print(f"    Neue Fenster: {len(new_windows)}")

                    if new_windows:
                        driver.switch_to.window(list(new_windows)[0])
                        print(f"    Neues Fenster Titel: {driver.title}")

                    driver.save_screenshot("debug/structure_03_after_dk_click.png")
                    print(f"    Nach Klick - URL: {driver.current_url}")
                    break

        input("\nDruecke Enter zum Beenden...")

    finally:
        driver.quit()


if __name__ == "__main__":
    main()
