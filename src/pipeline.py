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

import pdfplumber

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
    Hauptpipeline fuer GF-Screening.

    Orchestriert Import, Download, Parsing und Export.
    Verarbeitet Dealfront-Exporte (CSV/Excel) und steuert den gesamten
    Workflow von der Firmenerfassung bis zum qualifizierten Lead-Export.
    """

    def __init__(self, base_dir: Optional[Path] = None) -> None:
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

    def import_file(self, file_path: str, delimiter: str = ';') -> None:
        """
        Importiert Dealfront-Export (CSV oder Excel) in die Datenbank.

        Unterstuetzte Spalten (Dealfront-Format):
        - Company Name: Firmenname
        - Register Number: Registernummer (HRB12345, HRB 12345 B, etc.)
        - District Court: Registergericht
        - Location: Ort
        - Legal Form: Rechtsform (fuer Filter)
        - ID: Dealfront-ID

        Args:
            file_path: Pfad zur Import-Datei (CSV oder Excel).
            delimiter: CSV-Delimiter (default: ';').
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

    def _import_excel(self, excel_path: Path) -> None:
        """
        Importiert Excel-Datei (Dealfront-Format).

        Liest alle Zeilen aus dem Excel-Sheet, parst Firmendaten inkl.
        Registernummer und fuegt sie in die Datenbank ein.

        Args:
            excel_path: Pfad zur Excel-Datei.
        """
        try:
            df = pd.read_excel(excel_path)
        except FileNotFoundError:
            logger.error(f"Excel-Datei nicht gefunden: {excel_path}")
            return
        except ValueError as e:
            logger.error(f"Excel-Datei hat ungueltiges Format: {excel_path} - {type(e).__name__}")
            return
        except PermissionError:
            logger.error(f"Keine Leseberechtigung fuer Excel-Datei: {excel_path}")
            return
        except Exception as e:
            logger.error(f"Fehler beim Lesen der Excel-Datei: {excel_path} - {type(e).__name__}")
            return

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

            # Rechtsform (optional fuer Filter)
            legal_form = str(row.get('Legal Form', '')).strip()

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

        logger.info(f"Import abgeschlossen: {imported} importiert, {skipped} uebersprungen")

        # Statistiken
        stats = self.db.get_stats()
        logger.info(f"Datenbank-Status: {stats['total']} Firmen gesamt")

        missing_register = self.db.conn.execute(
            "SELECT COUNT(*) FROM companies WHERE register_num IS NULL OR register_num = ''"
        ).fetchone()[0]

        if missing_register > 0:
            logger.warning(f"{missing_register} Firmen ohne Registernummer!")

    def _import_csv(self, csv_path: Path, delimiter: str = ';') -> None:
        """
        Importiert CSV-Datei (Dealfront-Format).

        Erkennt den Delimiter automatisch falls der angegebene nicht vorkommt.
        Mappt Spaltennamen case-insensitive auf die erwarteten Felder.

        Args:
            csv_path: Pfad zur CSV-Datei.
            delimiter: CSV-Delimiter (default: ';').
        """
        imported = 0
        skipped = 0

        try:
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

        except FileNotFoundError:
            logger.error(f"CSV-Datei nicht gefunden: {csv_path}")
            return
        except UnicodeDecodeError:
            logger.error(f"CSV-Datei hat falsches Encoding: {csv_path}")
            return
        except PermissionError:
            logger.error(f"Keine Leseberechtigung: {csv_path}")
            return

        logger.info(f"Import abgeschlossen: {imported} importiert, {skipped} uebersprungen")

    def import_csv(self, csv_path: str, delimiter: str = ';') -> None:
        """
        Alias fuer import_file (Abwaertskompatibilitaet).

        Delegiert vollstaendig an import_file. Bestehendes Verhalten
        bleibt erhalten, aber ohne duplizierte Statistik-Logik.

        Args:
            csv_path: Pfad zur CSV-Datei.
            delimiter: CSV-Delimiter (default: ';').
        """
        self.import_file(csv_path, delimiter)

    def _get_field(
        self, row: dict, fieldnames: dict, candidates: list[str]
    ) -> Optional[str]:
        """
        Findet Feldwert basierend auf verschiedenen Spaltennamen.

        Durchsucht die Kandidaten-Liste case-insensitive und gibt den
        ersten gefundenen nicht-leeren Wert zurueck.

        Args:
            row: Zeile als Dictionary (aus csv.DictReader).
            fieldnames: Mapping von lowercase-Feldnamen zu Original-Namen.
            candidates: Liste moeglicher Spaltennamen (lowercase).

        Returns:
            Der gefundene Feldwert oder None.
        """
        if not isinstance(fieldnames, dict) or not isinstance(row, dict):
            return None

        for candidate in candidates:
            if candidate in fieldnames:
                original_name = fieldnames[candidate]
                value = row.get(original_name, "")
                if value:
                    return value.strip()
        return None

    def _parse_register_field(self, register_raw: str, city: str) -> tuple[str, str, str]:
        """
        Parst Registernummer aus verschiedenen Formaten.

        Unterstuetzte Formate:
        - "HRB 12345"
        - "HRB 12345 B"
        - "12345" (nur Nummer, dann HRB annehmen)
        - "Amtsgericht Berlin HRB 12345"
        - "Berlin, HRB 12345"

        Args:
            register_raw: Rohe Registernummer-Zeichenkette.
            city: Stadt fuer Gericht-Fallback.

        Returns:
            Tuple aus (register_type, register_number, court).
        """
        if not register_raw:
            return "", "", ""

        register_raw = register_raw.strip().upper()

        # Pattern 1: Vollstaendig mit Gericht
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
        """
        Leitet Registergericht aus Stadt ab (vereinfacht).

        Verwendet ein statisches Mapping der groessten deutschen Staedte
        zu ihren Registergerichten.

        Args:
            city: Stadtname.

        Returns:
            Name des Registergerichts oder die Stadt selbst als Fallback.
        """
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

    def run_downloads(self, limit: Optional[int] = None, resume: bool = True) -> None:
        """
        Laedt Gesellschafterlisten herunter.

        Holt ausstehende Firmen aus der Datenbank und startet den
        Selenium-basierten Download von handelsregister.de.

        Args:
            limit: Maximale Anzahl Downloads (None = alle).
            resume: Bei vorherigen Downloads fortfahren.
        """
        companies = self.db.get_pending_downloads(limit)

        if not companies:
            logger.info("Keine ausstehenden Downloads")
            return

        logger.info(f"Starte Download fuer {len(companies)} Firmen...")
        logger.info(f"Geschaetzte Zeit: {len(companies) * 65 / 3600:.1f} Stunden")

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
                                "Keine Gesellschafterliste verfuegbar"
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

    def run_parsing(self, limit: Optional[int] = None) -> None:
        """
        Parst heruntergeladene PDFs und extrahiert Gesellschafterstrukturen.

        Unterscheidet zwischen PDF-spezifischen Fehlern (pdfplumber),
        Datei-Fehlern und allgemeinen Fehlern fuer besseres Debugging.

        Args:
            limit: Maximale Anzahl zu parsender PDFs.
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
                    logger.warning(f"PDF nicht gefunden fuer Firma ID {company.id}")
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

                except pdfplumber.pdfminer.pdfparser.PDFSyntaxError as e:
                    logger.error(
                        f"PDF-Syntax-Fehler fuer Firma ID {company.id}: {type(e).__name__}"
                    )
                    self.db.log_event(company.id, "parse", "error", f"PDF-Syntax: {type(e).__name__}")
                    error_count += 1

                except (FileNotFoundError, PermissionError, OSError) as e:
                    logger.error(
                        f"Datei-Fehler fuer Firma ID {company.id}: {type(e).__name__}"
                    )
                    self.db.log_event(company.id, "parse", "error", f"Datei: {type(e).__name__}")
                    error_count += 1

                except Exception as e:
                    logger.error(
                        f"Parsing-Fehler fuer Firma ID {company.id}: {type(e).__name__}"
                    )
                    self.db.log_event(company.id, "parse", "error", type(e).__name__)
                    error_count += 1

        logger.info(f"Parsing abgeschlossen: {qualified_count} qualifiziert, {error_count} Fehler")

    def export(self, output_name: Optional[str] = None) -> Path:
        """
        Exportiert qualifizierte Leads als CSV.

        Args:
            output_name: Dateiname (default: qualified_leads_TIMESTAMP.csv).

        Returns:
            Pfad zur erzeugten CSV-Datei.
        """
        if output_name is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_name = f"qualified_leads_{timestamp}.csv"

        output_path = self.output_dir / output_name
        count = self.db.export_qualified(str(output_path))

        logger.info(f"Exportiert: {count} qualifizierte Leads nach {output_path}")
        return output_path

    def show_stats(self) -> None:
        """Zeigt aktuelle Pipeline-Statistiken auf der Konsole."""
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

        # Verbleibende Zeit schaetzen
        pending = stats['total'] - stats['downloaded']
        if pending > 0:
            hours = pending * 65 / 3600
            print(f"\nGeschaetzte Restzeit Download: {hours:.1f} Stunden ({hours/24:.1f} Tage)")

    def close(self) -> None:
        """Schliesst Datenbankverbindung."""
        self.db.close()


def main() -> None:
    """CLI-Entrypoint fuer die GF-Screening Pipeline."""
    parser = argparse.ArgumentParser(
        description="GF-Screening Pipeline - Gesellschafterstruktur analysieren"
    )

    subparsers = parser.add_subparsers(dest="command", help="Verfuegbare Befehle")

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
    run_parser = subparsers.add_parser("run", help="Komplette Pipeline ausfuehren")
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

    except Exception as e:
        logger.error(f"Pipeline-Fehler: {e}")
        pipeline.db.conn.rollback()
        raise

    finally:
        pipeline.close()


if __name__ == "__main__":
    main()
