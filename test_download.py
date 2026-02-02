"""Pilot-Test: Download von 5 Gesellschafterlisten."""

import sys
import logging
from pathlib import Path

# Pfad für Imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

from models import Database
from dk_downloader import GesellschafterlistenDownloader, DownloadResult

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    db_path = Path(__file__).parent / "data" / "gesellschafter.db"
    pdf_dir = Path(__file__).parent / "pdfs"

    if not db_path.exists():
        print(f"Datenbank nicht gefunden: {db_path}")
        return

    db = Database(str(db_path))
    companies = db.get_pending_downloads()

    print(f"\n{'='*60}")
    print(f"GF-Screening Pilot-Test")
    print(f"{'='*60}")
    print(f"Firmen zum Download: {len(companies)}")
    print(f"Test mit den ersten 5 Firmen:")

    test_companies = companies[:1]  # Nur 1 Firma zum Testen (Rate-Limit beachten!)
    for i, c in enumerate(test_companies, 1):
        print(f"  {i}. {c.name}")
        print(f"     Register: {c.register_num}")
        print(f"     Gericht: {c.court}")

    print(f"\n{'='*60}")
    print("Starte Downloads (headless=False für Debugging)...")
    print(f"{'='*60}\n")

    # Downloader mit sichtbarem Browser und Debug-Screenshots starten
    with GesellschafterlistenDownloader(pdf_dir, headless=False, debug=True) as downloader:
        results = []

        for i, company in enumerate(test_companies, 1):
            print(f"\n[{i}/5] Verarbeite: {company.name}")
            print(f"       Register: {company.register_num}")

            result = downloader.download(
                register_num=company.register_num,
                court=company.court
            )

            results.append((company, result))

            if result.success:
                if result.pdf_path:
                    print(f"       [OK] Erfolgreich: {result.pdf_path}")
                    db.update_download_status(company.id, str(result.pdf_path), True)
                elif result.no_gl_available:
                    print(f"       [WARN] Keine Gesellschafterliste vorhanden")
                    db.update_download_status(company.id, None, True)
            else:
                print(f"       [FEHLER] {result.error}")

    # Zusammenfassung
    print(f"\n{'='*60}")
    print("ZUSAMMENFASSUNG")
    print(f"{'='*60}")

    successful = sum(1 for _, r in results if r.success and r.pdf_path)
    no_gl = sum(1 for _, r in results if r.success and r.no_gl_available)
    failed = sum(1 for _, r in results if not r.success)

    print(f"Erfolgreich heruntergeladen: {successful}")
    print(f"Keine GL vorhanden: {no_gl}")
    print(f"Fehlgeschlagen: {failed}")

    db.close()


if __name__ == "__main__":
    main()
