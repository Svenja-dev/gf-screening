"""
PDF-Parser für deutsche Gesellschafterlisten.

Unterstützt verschiedene Formate je nach Amtsgericht:
- Tabellen-basierte Extraktion (bevorzugt)
- Regex-basierte Extraktion (Fallback)
"""

import re
import logging
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass

import pdfplumber

from models import Shareholder

logger = logging.getLogger(__name__)


@dataclass
class ParsingResult:
    """Ergebnis des PDF-Parsings."""
    shareholders: List[Shareholder]
    natural_persons_count: int
    legal_entities_count: int
    confidence: float
    raw_text: str = ""


class GesellschafterlisteParser:
    """Parser für deutsche Gesellschafterlisten-PDFs."""

    # Regex-Patterns für verschiedene Formate
    PATTERNS = {
        # Standard: "Mustermann, Max, Berlin, *01.01.1980"
        "standard_birth": re.compile(
            r"([A-ZÄÖÜ][a-zäöüß]+),\s*([A-ZÄÖÜ][a-zäöüß]+(?:\s+[A-ZÄÖÜ][a-zäöüß]+)?),\s*([^,*]+),\s*\*\s*(\d{2}\.\d{2}\.\d{4})",
            re.MULTILINE
        ),

        # "Max Mustermann, Berlin, *01.01.1980"
        "name_first": re.compile(
            r"([A-ZÄÖÜ][a-zäöüß]+(?:\s+[A-ZÄÖÜ][a-zäöüß]+)+),\s*([^,*]+),\s*\*\s*(\d{2}\.\d{2}\.\d{4})",
            re.MULTILINE
        ),

        # "1. Max Mustermann, geb. 01.01.1980"
        "numbered_geb": re.compile(
            r"\d+\.\s*([A-ZÄÖÜ][a-zäöüß]+(?:\s+[A-ZÄÖÜ][a-zäöüß]+)+),?\s*geb\.\s*(\d{2}\.\d{2}\.\d{4})",
            re.MULTILINE
        ),

        # "Max Mustermann 50,00 %" oder "Max Mustermann 50.000,00 EUR"
        "name_share": re.compile(
            r"([A-ZÄÖÜ][a-zäöüß]+(?:\s+[A-ZÄÖÜ][a-zäöüß]+)+)\s+(\d+(?:[.,]\d+)?)\s*(%|EUR|€)",
            re.MULTILINE
        ),

        # Nur vollständige Namen in Zeilen
        "name_only": re.compile(
            r"^([A-ZÄÖÜ][a-zäöüß]+(?:\s+[A-ZÄÖÜ][a-zäöüß]+){1,3})$",
            re.MULTILINE
        ),
    }

    # Juristische Personen erkennen
    LEGAL_ENTITY_MARKERS = [
        "GmbH", "AG", "KG", "SE", "UG", "OHG", "e.V.", "e. V.",
        "Stiftung", "Ltd.", "B.V.", "S.A.", "S.L.", "Inc.", "Corp.",
        "Holding", "Beteiligungs", "Verwaltungs", "& Co.", "mbH",
        "Kommanditgesellschaft", "Aktiengesellschaft", "Genossenschaft",
        "GbR", "PartG", "EWIV", "KGaA", "VVaG"
    ]

    # Wörter die auf keine Person hindeuten
    NON_PERSON_MARKERS = [
        "Geschäftsanteil", "Stammkapital", "Nennbetrag", "laufende",
        "Nummer", "Betrag", "Liste", "Gesellschafter", "insgesamt",
        "Summe", "EUR", "Anteil", "Veränderung"
    ]

    def parse(self, pdf_path: Path) -> ParsingResult:
        """
        Parst Gesellschafterliste und extrahiert Gesellschafter.

        Args:
            pdf_path: Pfad zur PDF-Datei

        Returns:
            ParsingResult mit Gesellschaftern und Statistiken
        """
        if not pdf_path.exists():
            logger.warning(f"PDF nicht gefunden: {pdf_path}")
            return ParsingResult(
                shareholders=[],
                natural_persons_count=0,
                legal_entities_count=0,
                confidence=0.0
            )

        shareholders = []
        full_text = ""

        try:
            with pdfplumber.open(pdf_path) as pdf:
                # Text extrahieren
                full_text = "\n".join(
                    page.extract_text() or "" for page in pdf.pages
                )

                # 1. Versuch: Tabellen-Extraktion
                for page in pdf.pages:
                    tables = page.extract_tables()
                    for table in tables:
                        if table:
                            extracted = self._parse_table(table)
                            shareholders.extend(extracted)

                # 2. Versuch: Regex-Extraktion falls Tabellen leer
                if not shareholders:
                    shareholders = self._parse_with_patterns(full_text)

        except Exception as e:
            logger.error(f"Fehler beim Parsen von {pdf_path}: {e}")
            return ParsingResult(
                shareholders=[],
                natural_persons_count=0,
                legal_entities_count=0,
                confidence=0.0,
                raw_text=full_text
            )

        # Deduplizieren
        shareholders = self._deduplicate(shareholders)

        # Klassifizieren
        for sh in shareholders:
            sh.is_natural_person = self._is_natural_person(sh.name)

        natural = [s for s in shareholders if s.is_natural_person]
        legal = [s for s in shareholders if not s.is_natural_person]

        confidence = self._calculate_confidence(shareholders, full_text)

        logger.info(
            f"Parsed {pdf_path.name}: {len(natural)} natürlich, "
            f"{len(legal)} juristisch, Konfidenz: {confidence:.2f}"
        )

        return ParsingResult(
            shareholders=shareholders,
            natural_persons_count=len(natural),
            legal_entities_count=len(legal),
            confidence=confidence,
            raw_text=full_text
        )

    def _parse_table(self, table: List[List]) -> List[Shareholder]:
        """Extrahiert Gesellschafter aus Tabellen-Struktur."""
        shareholders = []

        if not table or len(table) < 2:
            return shareholders

        headers = [str(h).lower() if h else "" for h in table[0]]

        # Header-Indizes finden
        name_col = self._find_column_index(headers, [
            "name", "gesellschafter", "vor- und nachname", "nachname",
            "inhaber", "anteilsinhaber"
        ])

        share_col = self._find_column_index(headers, [
            "anteil", "%", "geschäftsanteil", "nennbetrag", "betrag", "prozent"
        ])

        # Wenn kein Name-Header gefunden, erste nicht-leere Spalte nehmen
        if name_col is None:
            for i, cell in enumerate(table[0]):
                if cell and not any(m in str(cell).lower() for m in ["nr", "lfd", "nummer"]):
                    name_col = i
                    break

        if name_col is None:
            return shareholders

        # Daten extrahieren
        for row in table[1:]:
            if not row or len(row) <= name_col:
                continue

            name = str(row[name_col]).strip() if row[name_col] else ""

            # Leere oder ungültige Namen überspringen
            if not name or len(name) < 3:
                continue

            # Nicht-Personen-Marker überspringen
            if any(marker.lower() in name.lower() for marker in self.NON_PERSON_MARKERS):
                continue

            share = None
            if share_col is not None and len(row) > share_col and row[share_col]:
                share = self._parse_share(str(row[share_col]))

            shareholders.append(Shareholder(
                name=self._clean_name(name),
                share_percent=share,
                source="table"
            ))

        return shareholders

    def _parse_with_patterns(self, text: str) -> List[Shareholder]:
        """Regex-basierte Extraktion als Fallback."""
        shareholders = []

        for pattern_name, pattern in self.PATTERNS.items():
            matches = pattern.findall(text)

            for match in matches:
                if pattern_name == "standard_birth":
                    # Nachname, Vorname, Ort, Geburtsdatum
                    name = f"{match[1]} {match[0]}"  # Vorname Nachname
                elif pattern_name in ["name_first", "numbered_geb", "name_only"]:
                    name = match[0] if isinstance(match, tuple) else match
                elif pattern_name == "name_share":
                    name = match[0]
                else:
                    name = match[0] if isinstance(match, tuple) else match

                name = self._clean_name(name)

                # Validierung
                if len(name) < 3:
                    continue
                if any(marker.lower() in name.lower() for marker in self.NON_PERSON_MARKERS):
                    continue

                shareholders.append(Shareholder(
                    name=name,
                    source=f"regex:{pattern_name}"
                ))

        return shareholders

    def _is_natural_person(self, name: str) -> bool:
        """Prüft ob Name eine natürliche Person ist."""
        name_upper = name.upper()

        for marker in self.LEGAL_ENTITY_MARKERS:
            if marker.upper() in name_upper:
                return False

        # Zusätzliche Heuristiken
        # Natürliche Personen haben meist 2-4 Wörter
        words = name.split()
        if len(words) > 5:
            return False

        # Enthält Zahlen -> wahrscheinlich Firma
        if re.search(r'\d', name):
            return False

        return True

    def _parse_share(self, share_str: str) -> Optional[float]:
        """Parst Anteil aus String (z.B. '50,00 %' -> 50.0)."""
        if not share_str:
            return None

        # Prozent-Wert suchen
        match = re.search(r"(\d+(?:[.,]\d+)?)\s*%", share_str)
        if match:
            return float(match.group(1).replace(",", "."))

        # EUR-Betrag (für Nennwert) - deutsches Format: 25.000,00 EUR
        match = re.search(r"([\d.]+(?:,\d+)?)\s*(?:EUR|€)", share_str)
        if match:
            # Deutsches Format: Punkt = Tausender, Komma = Dezimal
            amount_str = match.group(1)
            # Tausenderpunkte entfernen, Komma zu Punkt
            amount_str = amount_str.replace(".", "").replace(",", ".")
            return float(amount_str)

        return None

    def _find_column_index(self, headers: List[str], search_terms: List[str]) -> Optional[int]:
        """Findet Spaltenindex basierend auf Header-Namen."""
        for i, header in enumerate(headers):
            if header and any(term in header for term in search_terms):
                return i
        return None

    def _clean_name(self, name: str) -> str:
        """Bereinigt Namen von Sonderzeichen und Whitespace."""
        # Mehrfache Leerzeichen entfernen
        name = re.sub(r'\s+', ' ', name)
        # Führende/trailing Whitespace
        name = name.strip()
        # Kommas am Ende entfernen
        name = name.rstrip(',')
        return name

    def _deduplicate(self, shareholders: List[Shareholder]) -> List[Shareholder]:
        """Entfernt Duplikate basierend auf normalisiertem Namen."""
        seen = set()
        unique = []

        for sh in shareholders:
            normalized = sh.name.lower().strip()
            if normalized not in seen:
                seen.add(normalized)
                unique.append(sh)

        return unique

    def _calculate_confidence(self, shareholders: List[Shareholder], full_text: str) -> float:
        """
        Berechnet Konfidenz des Parsings.
        1.0 = sehr sicher, 0.0 = unsicher
        """
        if not shareholders:
            return 0.0

        score = 0.0

        # Faktor 1: Quelle (Tabelle > Regex)
        table_sources = sum(1 for s in shareholders if s.source == "table")
        if table_sources > 0:
            score += 0.3
        elif any("regex" in s.source for s in shareholders):
            score += 0.15

        # Faktor 2: Anteile gefunden
        has_shares = sum(1 for s in shareholders if s.share_percent is not None)
        if has_shares > 0:
            score += 0.2

        # Faktor 3: Vernünftige Anzahl (1-10 Gesellschafter)
        if 1 <= len(shareholders) <= 10:
            score += 0.2
        elif len(shareholders) <= 20:
            score += 0.1

        # Faktor 4: Geburtsdaten gefunden (starker Indikator für korrekte Extraktion)
        if re.search(r'\*\s*\d{2}\.\d{2}\.\d{4}', full_text):
            score += 0.15

        # Faktor 5: "Gesellschafterliste" im Text
        if "gesellschafterliste" in full_text.lower():
            score += 0.15

        return min(score, 1.0)


# CLI für Einzeltest
if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python pdf_parser.py <pdf_path>")
        sys.exit(1)

    logging.basicConfig(level=logging.INFO)

    parser = GesellschafterlisteParser()
    result = parser.parse(Path(sys.argv[1]))

    print(f"\n{'='*50}")
    print(f"Ergebnis für: {sys.argv[1]}")
    print(f"{'='*50}")
    print(f"Natürliche Personen: {result.natural_persons_count}")
    print(f"Juristische Personen: {result.legal_entities_count}")
    print(f"Konfidenz: {result.confidence:.2f}")
    print(f"\nGesellschafter:")
    for sh in result.shareholders:
        typ = "NAT" if sh.is_natural_person else "JUR"
        share = f"{sh.share_percent}%" if sh.share_percent else "?"
        print(f"  [{typ}] {sh.name} ({share}) - Quelle: {sh.source}")
