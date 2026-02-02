"""
Unit Tests für pipeline.py - Import und Orchestrierung
"""

import pytest
import tempfile
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from pipeline import GFScreeningPipeline


@pytest.fixture
def temp_pipeline():
    """Erstellt Pipeline mit temporärem Verzeichnis."""
    with tempfile.TemporaryDirectory() as tmpdir:
        pipeline = GFScreeningPipeline(base_dir=Path(tmpdir))
        yield pipeline
        pipeline.close()


class TestParseRegisterField:
    """Tests für _parse_register_field Methode."""

    @pytest.fixture
    def pipeline(self, temp_pipeline):
        return temp_pipeline

    def test_hrb_with_spaces(self, pipeline):
        """HRB mit Leerzeichen."""
        reg_type, reg_num, court = pipeline._parse_register_field("HRB 12345", "Berlin")

        assert reg_type == "HRB"
        assert reg_num == "12345"

    def test_hrb_without_spaces(self, pipeline):
        """HRB ohne Leerzeichen."""
        reg_type, reg_num, court = pipeline._parse_register_field("HRB12345", "München")

        assert reg_type == "HRB"
        assert reg_num == "12345"

    def test_hrb_with_suffix(self, pipeline):
        """HRB mit Suffix (z.B. B für Berlin)."""
        reg_type, reg_num, court = pipeline._parse_register_field("HRB 175642 B", "Berlin")

        assert reg_type == "HRB"
        assert "175642" in reg_num
        assert "B" in reg_num

    def test_hra_register(self, pipeline):
        """HRA (Personengesellschaften)."""
        reg_type, reg_num, court = pipeline._parse_register_field("HRA 7834", "Dortmund")

        assert reg_type == "HRA"
        assert reg_num == "7834"

    def test_only_number(self, pipeline):
        """Nur Nummer ohne Typ."""
        reg_type, reg_num, court = pipeline._parse_register_field("12345", "Hamburg")

        assert reg_type == "HRB"  # Default
        assert reg_num == "12345"

    def test_with_court_prefix(self, pipeline):
        """Mit Amtsgericht-Präfix."""
        reg_type, reg_num, court = pipeline._parse_register_field(
            "Amtsgericht München HRB 12345", ""
        )

        assert reg_type == "HRB"
        assert reg_num == "12345"
        assert "München" in court or court == "MÜNCHEN"

    def test_empty_string(self, pipeline):
        """Leerer String."""
        reg_type, reg_num, court = pipeline._parse_register_field("", "Berlin")

        assert reg_type == ""
        assert reg_num == ""

    def test_lowercase_input(self, pipeline):
        """Kleinschreibung wird normalisiert."""
        reg_type, reg_num, court = pipeline._parse_register_field("hrb 12345", "Berlin")

        assert reg_type == "HRB"
        assert reg_num == "12345"


class TestCityToCourt:
    """Tests für _city_to_court Methode."""

    @pytest.fixture
    def pipeline(self, temp_pipeline):
        return temp_pipeline

    def test_berlin(self, pipeline):
        """Berlin -> Charlottenburg."""
        court = pipeline._city_to_court("Berlin")
        assert "Charlottenburg" in court or "Berlin" in court

    def test_munich(self, pipeline):
        """München."""
        court = pipeline._city_to_court("München")
        assert "München" in court

    def test_munich_english(self, pipeline):
        """Munich (englisch)."""
        court = pipeline._city_to_court("Munich")
        assert "München" in court

    def test_hamburg(self, pipeline):
        """Hamburg."""
        court = pipeline._city_to_court("Hamburg")
        assert "Hamburg" in court

    def test_unknown_city(self, pipeline):
        """Unbekannte Stadt -> Stadt als Fallback."""
        court = pipeline._city_to_court("Kleinkleckersdorf")
        assert court == "Kleinkleckersdorf"

    def test_empty_city(self, pipeline):
        """Leere Stadt."""
        court = pipeline._city_to_court("")
        assert court == ""


class TestImportExcel:
    """Tests für Excel-Import (erfordert Testdaten)."""

    @pytest.fixture
    def pipeline(self, temp_pipeline):
        return temp_pipeline

    def test_import_creates_companies(self, pipeline):
        """Import erstellt Firmen in DB."""
        # Test mit echter Dealfront-Datei wenn vorhanden
        excel_path = Path("C:/Projekte/inqu/New ICP.xlsx")

        if not excel_path.exists():
            pytest.skip("Dealfront-Excel nicht vorhanden")

        pipeline.import_file(str(excel_path))

        stats = pipeline.db.get_stats()
        assert stats['total'] > 0

    def test_stats_after_import(self, pipeline):
        """Stats zeigen korrekte Werte."""
        excel_path = Path("C:/Projekte/inqu/New ICP.xlsx")

        if not excel_path.exists():
            pytest.skip("Dealfront-Excel nicht vorhanden")

        pipeline.import_file(str(excel_path))
        stats = pipeline.db.get_stats()

        assert stats['total'] == 500  # Bekannte Anzahl
        assert stats['downloaded'] == 0
        assert stats['parsed'] == 0


class TestGetField:
    """Tests für _get_field Methode."""

    @pytest.fixture
    def pipeline(self, temp_pipeline):
        return temp_pipeline

    def test_finds_exact_match(self, pipeline):
        """Findet exakten Spaltennamen."""
        row = {'firma': 'Test GmbH', 'ort': 'Berlin'}
        fieldnames = {'firma': 'firma', 'ort': 'ort'}

        result = pipeline._get_field(row, fieldnames, ['firma', 'name'])
        assert result == 'Test GmbH'

    def test_finds_alternative(self, pipeline):
        """Findet alternativen Spaltennamen."""
        row = {'company name': 'Test GmbH'}
        fieldnames = {'company name': 'company name'}

        result = pipeline._get_field(row, fieldnames, ['firma', 'company name'])
        assert result == 'Test GmbH'

    def test_returns_none_if_not_found(self, pipeline):
        """Gibt None zurück wenn nicht gefunden."""
        row = {'andere_spalte': 'Wert'}
        fieldnames = {'andere_spalte': 'andere_spalte'}

        result = pipeline._get_field(row, fieldnames, ['firma', 'name'])
        assert result is None


class TestExport:
    """Tests für Export-Funktion."""

    @pytest.fixture
    def pipeline(self, temp_pipeline):
        return temp_pipeline

    def test_export_creates_file(self, pipeline):
        """Export erstellt CSV-Datei."""
        # Testdaten einfügen
        from models import Company, Shareholder

        company_id = pipeline.db.insert_company(Company(
            name="Test GmbH",
            register_num="HRB 12345"
        ))

        # Als qualifiziert markieren
        pipeline.db.update_parsing_result(
            company_id,
            natural_count=1,
            legal_count=0,
            confidence=0.9,
            shareholders=[
                Shareholder(name="Max Mustermann", is_natural_person=True, source="test")
            ]
        )

        # Export
        output_path = pipeline.export("test_export.csv")

        assert output_path.exists()
        assert output_path.stat().st_size > 0

        # Aufräumen
        output_path.unlink()
