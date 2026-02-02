"""
Unit Tests für pdf_parser.py - Gesellschafterlisten-Parser
"""

import pytest
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from pdf_parser import GesellschafterlisteParser


@pytest.fixture
def parser():
    """Parser-Instanz für Tests."""
    return GesellschafterlisteParser()


class TestIsNaturalPerson:
    """Tests für _is_natural_person Methode."""

    def test_natural_person_simple(self, parser):
        """Einfache Namen werden als natürliche Person erkannt."""
        assert parser._is_natural_person("Max Mustermann") is True
        assert parser._is_natural_person("Erika Musterfrau") is True
        assert parser._is_natural_person("Hans-Peter Müller") is True

    def test_gmbh_is_legal_entity(self, parser):
        """GmbH wird als juristische Person erkannt."""
        assert parser._is_natural_person("Holding GmbH") is False
        assert parser._is_natural_person("Muster Verwaltungs GmbH") is False
        assert parser._is_natural_person("ABC GmbH & Co. KG") is False

    def test_ag_is_legal_entity(self, parser):
        """AG wird als juristische Person erkannt."""
        assert parser._is_natural_person("Deutsche Bank AG") is False
        assert parser._is_natural_person("Siemens AG") is False

    def test_other_legal_forms(self, parser):
        """Andere Rechtsformen werden erkannt."""
        assert parser._is_natural_person("XYZ UG") is False
        assert parser._is_natural_person("ABC KG") is False
        assert parser._is_natural_person("Verein e.V.") is False
        assert parser._is_natural_person("Muster Stiftung") is False

    def test_holding_beteiligungs(self, parser):
        """Holding/Beteiligungs-Gesellschaften werden erkannt."""
        assert parser._is_natural_person("Muster Holding") is False
        assert parser._is_natural_person("XYZ Beteiligungs") is False
        assert parser._is_natural_person("ABC Verwaltungs") is False

    def test_foreign_legal_forms(self, parser):
        """Ausländische Rechtsformen werden erkannt."""
        assert parser._is_natural_person("XYZ Ltd.") is False
        assert parser._is_natural_person("ABC B.V.") is False
        assert parser._is_natural_person("Company Inc.") is False


class TestParseShare:
    """Tests für _parse_share Methode."""

    def test_percent_with_comma(self, parser):
        """Prozent mit Komma-Dezimaltrenner."""
        assert parser._parse_share("50,00 %") == 50.0
        assert parser._parse_share("33,33%") == 33.33
        assert parser._parse_share("100,00 %") == 100.0

    def test_percent_with_dot(self, parser):
        """Prozent mit Punkt-Dezimaltrenner."""
        assert parser._parse_share("50.00 %") == 50.0
        assert parser._parse_share("25.5%") == 25.5

    def test_eur_amount(self, parser):
        """EUR-Beträge werden geparst."""
        result = parser._parse_share("25.000,00 EUR")
        assert result == 25000.0

        result = parser._parse_share("12.500 €")
        assert result == 12500.0

    def test_empty_string(self, parser):
        """Leerer String gibt None zurück."""
        assert parser._parse_share("") is None
        assert parser._parse_share(None) is None

    def test_no_number(self, parser):
        """String ohne Zahl gibt None zurück."""
        assert parser._parse_share("keine Angabe") is None


class TestCleanName:
    """Tests für _clean_name Methode."""

    def test_trim_whitespace(self, parser):
        """Whitespace wird entfernt."""
        assert parser._clean_name("  Max Mustermann  ") == "Max Mustermann"

    def test_multiple_spaces(self, parser):
        """Mehrfache Leerzeichen werden reduziert."""
        assert parser._clean_name("Max   Mustermann") == "Max Mustermann"

    def test_trailing_comma(self, parser):
        """Komma am Ende wird entfernt."""
        assert parser._clean_name("Max Mustermann,") == "Max Mustermann"


class TestDeduplicate:
    """Tests für _deduplicate Methode."""

    def test_removes_duplicates(self, parser):
        """Duplikate werden entfernt."""
        from models import Shareholder

        shareholders = [
            Shareholder(name="Max Mustermann"),
            Shareholder(name="Max Mustermann"),
            Shareholder(name="Erika Musterfrau"),
        ]

        result = parser._deduplicate(shareholders)

        assert len(result) == 2
        names = [s.name for s in result]
        assert "Max Mustermann" in names
        assert "Erika Musterfrau" in names

    def test_case_insensitive(self, parser):
        """Deduplizierung ist case-insensitive."""
        from models import Shareholder

        shareholders = [
            Shareholder(name="Max Mustermann"),
            Shareholder(name="max mustermann"),
        ]

        result = parser._deduplicate(shareholders)
        assert len(result) == 1


class TestCalculateConfidence:
    """Tests für _calculate_confidence Methode."""

    def test_empty_list_zero_confidence(self, parser):
        """Leere Liste gibt Konfidenz 0."""
        assert parser._calculate_confidence([], "") == 0.0

    def test_table_source_increases_confidence(self, parser):
        """Tabellen-Quelle erhöht Konfidenz."""
        from models import Shareholder

        shareholders = [
            Shareholder(name="Max", source="table"),
        ]

        confidence = parser._calculate_confidence(shareholders, "gesellschafterliste")
        assert confidence >= 0.3  # Tabellen-Bonus

    def test_shares_increase_confidence(self, parser):
        """Anteile erhöhen Konfidenz."""
        from models import Shareholder

        shareholders = [
            Shareholder(name="Max", share_percent=50.0),
            Shareholder(name="Erika", share_percent=50.0),
        ]

        confidence = parser._calculate_confidence(shareholders, "gesellschafterliste")
        assert confidence >= 0.2  # Anteile-Bonus


class TestRegexPatterns:
    """Tests für Regex-Patterns."""

    def test_standard_birth_pattern(self, parser):
        """Standard-Pattern mit Geburtsdatum."""
        text = "Mustermann, Max, Berlin, *01.01.1980"
        matches = parser.PATTERNS["standard_birth"].findall(text)

        assert len(matches) == 1
        assert matches[0][0] == "Mustermann"
        assert matches[0][1] == "Max"

    def test_numbered_geb_pattern(self, parser):
        """Nummeriertes Pattern mit 'geb.'."""
        text = "1. Max Mustermann, geb. 15.03.1975"
        matches = parser.PATTERNS["numbered_geb"].findall(text)

        assert len(matches) == 1
        assert "Max Mustermann" in matches[0][0]

    def test_name_share_pattern(self, parser):
        """Name mit Anteil Pattern."""
        text = "Max Mustermann 50,00 %"
        matches = parser.PATTERNS["name_share"].findall(text)

        assert len(matches) == 1
        assert "Max Mustermann" in matches[0][0]


class TestParseWithPatterns:
    """Tests für _parse_with_patterns Methode."""

    def test_extracts_shareholders_from_text(self, parser):
        """Extrahiert Gesellschafter aus Text."""
        text = """
        Gesellschafterliste
        1. Max Mustermann, geb. 01.01.1980
        2. Erika Musterfrau, geb. 15.06.1985
        """

        shareholders = parser._parse_with_patterns(text)

        assert len(shareholders) >= 2

    def test_filters_non_person_markers(self, parser):
        """Nicht-Personen-Marker werden gefiltert."""
        text = """
        Geschäftsanteil insgesamt
        Stammkapital 25.000 EUR
        Max Mustermann 50,00 %
        """

        shareholders = parser._parse_with_patterns(text)

        # "Geschäftsanteil" und "Stammkapital" sollten nicht enthalten sein
        names = [s.name for s in shareholders]
        assert not any("Geschäftsanteil" in n for n in names)
        assert not any("Stammkapital" in n for n in names)
