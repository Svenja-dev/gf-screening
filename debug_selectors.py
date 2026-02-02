"""Debug-Script: Finde die korrekten Selektoren auf handelsregister.de."""

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
        # Seite laden
        url = "https://www.handelsregister.de/rp_web/erweitertesuche/welcome.xhtml"
        print(f"Lade: {url}")
        driver.get(url)
        time.sleep(3)

        # Screenshot der Startseite
        driver.save_screenshot("debug/selector_01_start.png")

        # Alle Checkboxen finden
        print("\n=== ALLE CHECKBOXEN ===")
        checkboxes = driver.find_elements(By.CSS_SELECTOR, "input[type='checkbox']")
        for cb in checkboxes[:20]:  # Erste 20
            cb_id = cb.get_attribute("id")
            cb_name = cb.get_attribute("name")
            cb_value = cb.get_attribute("value")
            # Label finden
            try:
                label = driver.find_element(By.CSS_SELECTOR, f"label[for='{cb_id}']")
                label_text = label.text
            except:
                label_text = "(kein Label)"
            print(f"  ID: {cb_id}")
            print(f"  Name: {cb_name}")
            print(f"  Value: {cb_value}")
            print(f"  Label: {label_text}")
            print()

        # Bundesland-Checkboxen finden
        print("\n=== BUNDESLAND CHECKBOXEN ===")
        bundesland_labels = [
            "Baden-Württemberg", "Bayern", "Berlin", "Brandenburg",
            "Bremen", "Hamburg", "Hessen", "Niedersachsen",
            "Nordrhein-Westfalen", "Sachsen"
        ]

        for bl in bundesland_labels:
            try:
                # Label mit dem Text finden
                label = driver.find_element(By.XPATH, f"//label[contains(text(), '{bl}')]")
                for_attr = label.get_attribute("for")
                print(f"{bl}:")
                print(f"  Label for: {for_attr}")

                # Zugehörige Checkbox finden
                if for_attr:
                    checkbox = driver.find_element(By.ID, for_attr)
                    print(f"  Checkbox ID: {checkbox.get_attribute('id')}")
                    print(f"  Checkbox Name: {checkbox.get_attribute('name')}")
            except Exception as e:
                print(f"{bl}: NICHT GEFUNDEN - {e}")
            print()

        # Formular-Felder finden
        print("\n=== FORMULAR FELDER ===")
        form_fields = {
            "form:registerArt": "Registerart",
            "form:registerNummer": "Registernummer",
            "form:registergericht_input": "Registergericht",
        }

        for field_id, name in form_fields.items():
            try:
                field = driver.find_element(By.ID, field_id)
                print(f"{name}: {field_id} - GEFUNDEN")
            except:
                print(f"{name}: {field_id} - NICHT GEFUNDEN")

        # Suchen-Button finden
        print("\n=== BUTTONS ===")
        buttons = driver.find_elements(By.TAG_NAME, "button")
        for btn in buttons:
            btn_text = btn.text or btn.get_attribute("value") or "(leer)"
            btn_id = btn.get_attribute("id") or "(keine ID)"
            print(f"  Button: '{btn_text}' - ID: {btn_id}")

        # Speichere finalen Screenshot
        driver.save_screenshot("debug/selector_02_final.png")

        input("\nDruecke Enter um Browser zu schliessen...")

    finally:
        driver.quit()


if __name__ == "__main__":
    main()
