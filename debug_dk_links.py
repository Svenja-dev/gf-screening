"""Debug: Analysiere DK-Links genauer."""

import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

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
        print(f"Lade: {url}")
        driver.get(url)
        time.sleep(3)

        # Bayern auswaehlen
        bayern_cb = driver.find_element(By.ID, "form:Bayern_input")
        driver.execute_script("arguments[0].click();", bayern_cb)
        time.sleep(0.5)

        # Registernummer eingeben
        reg_field = driver.find_element(By.ID, "form:registerNummer")
        reg_field.send_keys("12345")

        # Suchen
        search_btn = driver.find_element(By.ID, "form:btnSuche")
        driver.execute_script("arguments[0].click();", search_btn)
        time.sleep(5)

        # Alle <a> Elemente mit DK-Span finden
        print("\n=== DK-Links (Parent <a> Analyse) ===")
        dk_anchors = driver.find_elements(By.XPATH, "//a[span[text()='DK']]")
        print(f"Gefunden: {len(dk_anchors)} DK-Links")

        for i, anchor in enumerate(dk_anchors[:3]):
            print(f"\n--- Link {i} ---")
            print(f"ID: {anchor.get_attribute('id')}")
            print(f"Class: {anchor.get_attribute('class')}")
            print(f"href: {anchor.get_attribute('href')}")

            # Alle Attribute ausgeben
            attrs = driver.execute_script('''
                var items = {};
                for (var i = 0; i < arguments[0].attributes.length; i++) {
                    items[arguments[0].attributes[i].name] = arguments[0].attributes[i].value;
                }
                return items;
            ''', anchor)
            print(f"Alle Attribute: {attrs}")

            # OuterHTML (gekuerzt)
            outer = anchor.get_attribute('outerHTML')
            print(f"HTML: {outer[:200]}...")

        # Ersten DK-Link klicken
        if dk_anchors:
            print("\n=== Klicke ersten DK-Link ===")
            first_dk = dk_anchors[0]

            # Screenshot vor Klick
            driver.save_screenshot("debug/dk_before_click.png")

            # Aktuelle URL merken
            url_before = driver.current_url
            print(f"URL vor Klick: {url_before}")

            # Fenster zaehlen
            windows_before = len(driver.window_handles)

            # Klicken
            driver.execute_script("arguments[0].click();", first_dk)
            time.sleep(5)

            # Screenshot nach Klick
            driver.save_screenshot("debug/dk_after_click.png")

            # Pruefen was passiert ist
            url_after = driver.current_url
            windows_after = len(driver.window_handles)

            print(f"URL nach Klick: {url_after}")
            print(f"URL geaendert: {url_before != url_after}")
            print(f"Neue Fenster: {windows_after - windows_before}")

            # Wenn neues Fenster, dahin wechseln
            if windows_after > windows_before:
                all_windows = driver.window_handles
                for w in all_windows:
                    driver.switch_to.window(w)
                    print(f"Fenster: {driver.current_url}")
                    if "DK" in driver.current_url or "dokument" in driver.current_url.lower():
                        driver.save_screenshot("debug/dk_new_window.png")
                        print("DK-Fenster gefunden!")

            # Seitenquelle nach "Gesellschafterliste" durchsuchen
            if "Gesellschafterliste" in driver.page_source:
                print("'Gesellschafterliste' in Seite gefunden!")
            else:
                print("'Gesellschafterliste' NICHT in Seite gefunden")

    finally:
        driver.quit()


if __name__ == "__main__":
    main()
