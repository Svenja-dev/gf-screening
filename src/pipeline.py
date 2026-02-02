"""
GF-Screening Pipeline - Hauptorchiestrierung

Workflow:
1. Dealfront-CSV importieren
2. Gesellschafterlisten von handelsregister.de herunterladen
3. PDFs parsen und Gesellschafter extrahieren
4. Qualifizierte Leads exportieren
"""

import argparse
import logging
import sys
import csv
import re
from pathlib import Path
from datetime import datetime
from typing import Optional

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

from tqdm import tqdm

from models import Database, Company, Shareholder
from dk_downloader import GesellschafterlistenDownloader, DownloadResult
from pdf_parser import GesellschafterlisteParser

# Logging konfigurieren
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class GFScreeningPipeline:
    """
    Hauptpipeline für GF-Screening.

    Orchestriert Import, Download, Parsing und Export.
    """

    def __init__(self, base_dir: Path = None):
        """
        Initialisiert die Pipeline.

        Args:
            base_dir: Basisverzeichnis (default: Elternverzeichnis von src/)
        """
        if base_dir is None:
            base_dir = Path(__file__).parent.parent

        self.base_dir = Path(base_dir)
        self.db = Database(str(self.base_dir / "data" / "gesellschafter.db"))
        self.pdf_dir = self.base_dir / "pdfs"
        self.output_dir = self.base_dir / "output"

        # Verzeichnisse erstellen
        self.pdf_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def import_file(self, file_path: str, delimiter: str = ';'):
        """
        Importiert Dealfront-Export (CSV oder Excel) in die Datenbank.

        Unterstützte Spalten (Dealfront-Format):
        - Company Name: Firmenname
        - Register Number: Registernummer (HRB12345, HRB 12345 B, etc.)
        - District Court: Registergericht
        - Location: Ort
        - Legal Form: Rechtsform (für Filter)
        - ID: Dealfront-ID
        """
        file_path = Path(file_path)

        if not file_path.exists():
            logger.error(f"Datei nicht gefunden: {file_path}")
            return

        logger.info(f"Importiere: {file_path}")

        # Excel oder CSV?
        if file_path.suffix.lower() in ['.xlsx', '.xls']:
            if not HAS_PANDAS:
                logger.error("pandas nicht installiert! pip install pandas openpyxl")
                return
            self._import_excel(file_path)
        else:
            self._import_csv(file_path, delimiter)

    def _import_excel(self, excel_path: Path):
        """Importiert Excel-Datei (Dealfront-Format)."""
        df = pd.read_excel(excel_path)

        logger.info(f"Excel geladen: {len(df)} Zeilen, {len(df.columns)} Spalten")

        imported = 0
        skipped = 0

        for _, row in df.iterrows():
            # Firmenname
            name = str(row.get('Company Name', '')).strip()
            if not name or name == 'nan':
                skipped += 1
                continue

            # Ort
            city = str(row.get('Location', '')).strip()
            if city == 'nan':
                city = ""

            # Registergericht (direkt aus Dealfront)
            court = str(row.get('District Court', '')).strip()
            if court == 'nan':
                court = ""

            # Registernummer parsen
            register_raw = str(row.get('Register Number', '')).strip()
            if register_raw == 'nan':
                register_raw = ""

            reg_type, reg_num, _ = self._parse_register_field(register_raw, city)

            # Dealfront-ID
            dealfront_id = str(row.get('ID', name)).strip()
            if dealfront_id == 'nan':
                dealfront_id = name

            # Rechtsform (optional für Filter)
            legal_form = str(row.get('Legal Form', '')).strip()

            # Nur GmbHs importieren? (Optional)
            # if 'GmbH' not in legal_form:
            #     skipped += 1
            #     continue

            company = Company(
                dealfront_id=dealfront_id,
                name=name,
                city=city,
                court=court,
                register_type=reg_type,
                register_num=f"{reg_type} {reg_num}".strip() if reg_type and reg_num else ""
            )

            self.db.insert_company(company)
            imported += 1

        logger.info(f"Import abgeschlossen: {imported} importiert, {skipped} übersprungen")

        # Statistiken
        stats = self.db.get_stats()
        logger.info(f"Datenbank-Status: {stats['total']} Firmen gesamt")

        missing_register = self.db.conn.execute(
            "SELECT COUNT(*) FROM companies WHERE register_num IS NULL OR register_num = ''"
        ).fetchone()[0]

        if missing_register > 0:
            logger.warning(f"{missing_register} Firmen ohne Registernummer!")

    def _import_csv(self, csv_path: Path, delimiter: str = ';'):
        """Importiert CSV-Datei."""
        imported = 0
        skipped = 0

        with open(csv_path, encoding='utf-8-sig') as f:
            # Delimiter erkennen
            sample = f.read(1024)
            f.seek(0)

            if delimiter not in sample:
                delimiter = ',' if ',' in sample else '\t'

            reader = csv.DictReader(f, delimiter=delimiter)
            fieldnames = {fn.lower().strip(): fn for fn in reader.fieldnames}

            for row in reader:
                name = self._get_field(row, fieldnames, [
                    'firma', 'firmenname', 'name', 'company', 'company name'
                ])

                if not name:
                    skipped += 1
                    continue

                city = self._get_field(row, fieldnames, [
                    'ort', 'stadt', 'city', 'location'
                ]) or ""

                court = self._get_field(row, fieldnames, [
                    'district court', 'registergericht', 'gericht', 'court'
                ]) or ""

                register_raw = self._get_field(row, fieldnames, [
                    'registernummer', 'register number', 'hrb', 'hra'
                ]) or ""

                reg_type, reg_num, parsed_court = self._parse_register_field(register_raw, city)

                if not court:
                    court = parsed_court

                dealfront_id = self._get_field(row, fieldnames, [
                    'id', 'dealfront_id', 'company_id'
                ]) or name

                company = Company(
                    dealfront_id=dealfront_id,
                    name=name.strip(),
                    city=city.strip(),
                    court=court,
                    register_type=reg_type,
                    register_num=f"{reg_type} {reg_num}".strip() if reg_type and reg_num else ""
                )

                self.db.insert_company(company)
                imported += 1

        logger.info(f"Import abgeschlossen: {imported} importiert, {skipped} übersprungen")

    # Alias für Abwärtskompatibilität
    def import_csv(self, csv_path: str, delimiter: str = ';'):
        """Alias für import_file (Abwärtskompatibilität)."""
        self.import_file(csv_path, delimiter)

        # Statistiken anzeigen
        stats = self.db.get_stats()
        logger.info(f"Datenbank-Status: {stats['total']} Firmen gesamt")

        missing_register = self.db.conn.execute(
            "SELECT COUNT(*) FROM companies WHERE register_num IS NULL OR register_num = ''"
        ).fetchone()[0]

        if missing_register > 0:
            logger.warning(f"{missing_register} Firmen ohne Registernummer!")

    def _get_field(self, row: dict, fieldnames: dict, candidates: list) -> Optional[str]:
        """Findet Feldwert basierend auf verschiedenen Spaltennamen."""
        for candidate in candidates:
            if candidate in fieldnames:
                original_name = fieldnames[candidate]
                value = row.get(original_name, "")
                if value:
                    return value.strip()
        return None

    def _parse_register_field(self, register_raw: str, city: str) -> tuple:
        """
        Parst Registernummer aus verschiedenen Formaten.

        Unterstützte Formate:
        - "HRB 12345"
        - "HRB 12345 B"
        - "12345" (nur Nummer, dann HRB annehmen)
        - "Amtsgericht Berlin HRB 12345"
        - "Berlin, HRB 12345"
        """
        if not register_raw:
            return "", "", ""

        register_raw = register_raw.strip().upper()

        # Pattern 1: Vollständig mit Gericht
        match = re.search(
            r"(?:AMTSGERICHT\s+)?(\w+)[\s,]+?(HRB|HRA|GNR|VR|PR)\s*(\d+)\s*([A-Z])?",
            register_raw
        )
        if match:
            court = match.group(1)
            reg_type = match.group(2)
            reg_num = match.group(3)
            suffix = match.group(4) or ""
            return reg_type, f"{reg_num} {suffix}".strip(), court

        # Pattern 2: Typ + Nummer
        match = re.search(r"(HRB|HRA|GNR|VR|PR)\s*(\d+)\s*([A-Z])?", register_raw)
        if match:
            reg_type = match.group(1)
            reg_num = match.group(2)
            suffix = match.group(3) or ""
            # Gericht aus Stadt ableiten
            court = self._city_to_court(city)
            return reg_type, f"{reg_num} {suffix}".strip(), court

        # Pattern 3: Nur Nummer (HRB annehmen)
        match = re.search(r"^(\d+)\s*([A-Z])?$", register_raw)
        if match:
            reg_num = match.group(1)
            suffix = match.group(2) or ""
            court = self._city_to_court(city)
            return "HRB", f"{reg_num} {suffix}".strip(), court

        return "", "", ""

    def _city_to_court(self, city: str) -> str:
        """Leitet Registergericht aus Stadt ab (vereinfacht)."""
        if not city:
            return ""

        city_lower = city.lower()

        # Direkte Mappings
        court_map = {
            "berlin": "Berlin (Charlottenburg)",
            "münchen": "München",
            "munich": "München",
            "hamburg": "Hamburg",
            "frankfurt": "Frankfurt am Main",
            "köln": "Köln",
            "cologne": "Köln",
            "düsseldorf": "Düsseldorf",
            "stuttgart": "Stuttgart",
            "dortmund": "Dortmund",
            "essen": "Essen",
            "bremen": "Bremen",
            "leipzig": "Leipzig",
            "dresden": "Dresden",
            "hannover": "Hannover",
            "nürnberg": "Nürnberg",
            "nuremberg": "Nürnberg",
        }

        for key, value in court_map.items():
            if key in city_lower:
                return value

        return city  # Fallback: Stadt als Gericht verwenden

    def run_downloads(self, limit: int = None, resume: bool = True):
        """
        Lädt Gesellschafterlisten herunter.

        Args:
            limit: Maximale Anzahl Downloads (None = alle)
            resume: Bei vorherigen Downloads fortfahren
        """
        companies = self.db.get_pending_downloads(limit)

        if not companies:
            logger.info("Keine ausstehenden Downloads")
            return

        logger.info(f"Starte Download für {len(companies)} Firmen...")
        logger.info(f"Geschätzte Zeit: {len(companies) * 65 / 3600:.1f} Stunden")

        downloader = GesellschafterlistenDownloader(self.pdf_dir, headless=True)

        try:
            downloader.start()

            with tqdm(companies, desc="Downloads", unit="Firma") as pbar:
                for company in pbar:
                    pbar.set_postfix_str(f"{company.name[:30]}...")

                    result = downloader.download(
                        company.register_num,
                        company.court
                    )

                    if result.success:
                        self.db.update_download_status(
                            company.id,
                            str(result.pdf_path) if result.pdf_path else None,
                            True
                        )

                        if result.no_gl_available:
                            self.db.log_event(
                                company.id, "download", "no_gl",
                                "Keine Gesellschafterliste verfügbar"
                            )
                        else:
                            self.db.log_event(company.id, "download", "success")
                    else:
                        self.db.log_event(
                            company.id, "download", "error",
                            result.error or "Unbekannter Fehler"
                        )
                        # Trotzdem als "versucht" markieren
                        self.db.update_download_status(company.id, None, False)

        finally:
            downloader.stop()

        # Statistiken
        stats = self.db.get_stats()
        logger.info(f"Download-Status: {stats['downloaded']}/{stats['total']} abgeschlossen")
        logger.info(f"Ohne Gesellschafterliste: {stats['no_gl']}")

    def run_parsing(self, limit: int = None):
        """
        Parst heruntergeladene PDFs.

        Args:
            limit: Maximale Anzahl zu parsender PDFs
        """
        companies = self.db.get_pending_parsing(limit)

        if not companies:
            logger.info("Keine ausstehenden PDFs zum Parsen")
            return

        logger.info(f"Parse {len(companies)} PDFs...")

        parser = GesellschafterlisteParser()

        qualified_count = 0
        error_count = 0

        with tqdm(companies, desc="Parsing", unit="PDF") as pbar:
            for company in pbar:
                pbar.set_postfix_str(f"{company.name[:30]}...")

                pdf_path = Path(company.pdf_path)

                if not pdf_path.exists():
                    logger.warning(f"PDF nicht gefunden: {pdf_path}")
                    error_count += 1
                    continue

                try:
                    result = parser.parse(pdf_path)

                    shareholders = [
                        Shareholder(
                            company_id=company.id,
                            name=sh.name,
                            share_percent=sh.share_percent,
                            is_natural_person=sh.is_natural_person,
                            source=sh.source
                        )
                        for sh in result.shareholders
                    ]

                    self.db.update_parsing_result(
                        company.id,
                        result.natural_persons_count,
                        result.legal_entities_count,
                        result.confidence,
                        shareholders
                    )

                    if result.natural_persons_count <= 2 and result.legal_entities_count == 0:
                        qualified_count += 1

                    self.db.log_event(company.id, "parse", "success")

                except Exception as e:
                    logger.error(f"Parsing-Fehler für {company.name}: {e}")
                    self.db.log_event(company.id, "parse", "error", str(e))
                    error_count += 1

        logger.info(f"Parsing abgeschlossen: {qualified_count} qualifiziert, {error_count} Fehler")

    def export(self, output_name: str = None):
        """
        Exportiert qualifizierte Leads als CSV.

        Args:
            output_name: Dateiname (default: qualified_leads_TIMESTAMP.csv)
        """
        if output_name is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_name = f"qualified_leads_{timestamp}.csv"

        output_path = self.output_dir / output_name
        count = self.db.export_qualified(str(output_path))

        logger.info(f"Exportiert: {count} qualifizierte Leads nach {output_path}")
        return output_path

    def show_stats(self):
        """Zeigt aktuelle Pipeline-Statistiken."""
        stats = self.db.get_stats()

        print("\n" + "=" * 50)
        print("GF-Screening Pipeline - Status")
        print("=" * 50)
        print(f"Firmen gesamt:         {stats['total']:>6}")
        print(f"Downloads abgeschlossen: {stats['downloaded']:>6} ({100*stats['downloaded']/max(stats['total'],1):.1f}%)")
        print(f"PDFs geparst:          {stats['parsed']:>6} ({100*stats['parsed']/max(stats['total'],1):.1f}%)")
        print(f"Qualifizierte Leads:   {stats['qualified']:>6} ({100*stats['qualified']/max(stats['parsed'],1):.1f}%)")
        print(f"Ohne Gesellschafterliste: {stats['no_gl']:>6}")
        print("=" * 50)

        # Verbleibende Zeit schätzen
        pending = stats['total'] - stats['downloaded']
        if pending > 0:
            hours = pending * 65 / 3600
            print(f"\nGeschätzte Restzeit Download: {hours:.1f} Stunden ({hours/24:.1f} Tage)")

    def close(self):
        """Schließt Datenbankverbindung."""
        self.db.close()


def main():
    """CLI-Entrypoint."""
    parser = argparse.ArgumentParser(
        description="GF-Screening Pipeline - Gesellschafterstruktur analysieren"
    )

    subparsers = parser.add_subparsers(dest="command", help="Verfügbare Befehle")

    # Import
    import_parser = subparsers.add_parser("import", help="Dealfront-Export importieren (CSV oder Excel)")
    import_parser.add_argument("file_path", help="Pfad zur CSV- oder Excel-Datei")
    import_parser.add_argument("--delimiter", default=";", help="CSV-Delimiter (default: ;)")

    # Download
    download_parser = subparsers.add_parser("download", help="Gesellschafterlisten herunterladen")
    download_parser.add_argument("--limit", type=int, help="Max. Anzahl Downloads")

    # Parse
    parse_parser = subparsers.add_parser("parse", help="PDFs parsen")
    parse_parser.add_argument("--limit", type=int, help="Max. Anzahl zu parsender PDFs")

    # Export
    export_parser = subparsers.add_parser("export", help="Qualifizierte Leads exportieren")
    export_parser.add_argument("--output", help="Output-Dateiname")

    # Stats
    subparsers.add_parser("stats", help="Pipeline-Statistiken anzeigen")

    # Run all
    run_parser = subparsers.add_parser("run", help="Komplette Pipeline ausführen")
    run_parser.add_argument("file_path", help="Pfad zur CSV- oder Excel-Datei")
    run_parser.add_argument("--limit", type=int, help="Max. Anzahl zu verarbeitender Firmen")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    # Pipeline initialisieren
    base_dir = Path(__file__).parent.parent
    pipeline = GFScreeningPipeline(base_dir)

    try:
        if args.command == "import":
            pipeline.import_file(args.file_path, args.delimiter)

        elif args.command == "download":
            pipeline.run_downloads(limit=args.limit)

        elif args.command == "parse":
            pipeline.run_parsing(limit=args.limit)

        elif args.command == "export":
            pipeline.export(args.output)

        elif args.command == "stats":
            pipeline.show_stats()

        elif args.command == "run":
            logger.info("Starte komplette Pipeline...")
            pipeline.import_file(args.file_path)
            pipeline.run_downloads(limit=args.limit)
            pipeline.run_parsing()
            pipeline.export()
            pipeline.show_stats()

    finally:
        pipeline.close()


if __name__ == "__main__":
    main()
