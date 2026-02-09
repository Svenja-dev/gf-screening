"""
Unit Tests for pipeline.py - GF-Screening Pipeline orchestration.

Tests cover: register field parsing, city-to-court mapping, CSV import,
field resolution, export, download orchestration (mocked), parsing
orchestration (mocked), and full pipeline integration (mocked).
"""

import csv
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

from models import Company, Shareholder
from pipeline import GFScreeningPipeline


class TestParseRegisterField:
    """Tests for _parse_register_field method."""

    @pytest.fixture
    def pipeline(self, temp_pipeline):
        return temp_pipeline

    def test_hrb_with_spaces(self, pipeline):
        """'HRB 12345' is parsed into type='HRB', num='12345'."""
        reg_type, reg_num, court = pipeline._parse_register_field("HRB 12345", "Berlin")

        assert reg_type == "HRB"
        assert reg_num == "12345"

    def test_hrb_without_spaces(self, pipeline):
        """'HRB12345' (no space) is correctly parsed."""
        reg_type, reg_num, court = pipeline._parse_register_field("HRB12345", "Berlin")

        assert reg_type == "HRB"
        assert reg_num == "12345"

    def test_hrb_with_suffix(self, pipeline):
        """'HRB 175642 B' includes the suffix in reg_num."""
        reg_type, reg_num, court = pipeline._parse_register_field("HRB 175642 B", "Berlin")

        assert reg_type == "HRB"
        assert "175642" in reg_num
        assert "B" in reg_num

    def test_hra_register(self, pipeline):
        """HRA register type is correctly recognized."""
        reg_type, reg_num, court = pipeline._parse_register_field("HRA 7834", "Dortmund")

        assert reg_type == "HRA"
        assert reg_num == "7834"

    def test_only_number(self, pipeline):
        """Bare number defaults to HRB type."""
        reg_type, reg_num, court = pipeline._parse_register_field("12345", "Hamburg")

        assert reg_type == "HRB"
        assert reg_num == "12345"

    def test_with_court_prefix(self, pipeline):
        """'Amtsgericht Muenchen HRB 12345' extracts court from prefix."""
        reg_type, reg_num, court = pipeline._parse_register_field(
            "Amtsgericht Muenchen HRB 12345", ""
        )

        assert reg_type == "HRB"
        assert reg_num == "12345"
        # The regex captures the word before HRB as court
        assert court == "MUENCHEN"

    def test_empty_string(self, pipeline):
        """Empty input returns empty strings."""
        reg_type, reg_num, court = pipeline._parse_register_field("", "Berlin")

        assert reg_type == ""
        assert reg_num == ""

    def test_lowercase_input(self, pipeline):
        """Lowercase input is normalized to uppercase."""
        reg_type, reg_num, court = pipeline._parse_register_field("hrb 12345", "Berlin")

        assert reg_type == "HRB"
        assert reg_num == "12345"

    def test_vr_register(self, pipeline):
        """VR register type (Vereine) is recognized."""
        reg_type, reg_num, court = pipeline._parse_register_field("VR 5678", "Berlin")

        assert reg_type == "VR"
        assert reg_num == "5678"

    def test_gnr_register(self, pipeline):
        """GNR register type (Genossenschaften) is recognized."""
        reg_type, reg_num, court = pipeline._parse_register_field("GNR 9012", "Berlin")

        assert reg_type == "GNR"
        assert reg_num == "9012"


class TestCityToCourt:
    """Tests for _city_to_court method."""

    @pytest.fixture
    def pipeline(self, temp_pipeline):
        return temp_pipeline

    def test_berlin(self, pipeline):
        """Berlin maps to 'Berlin (Charlottenburg)'."""
        court = pipeline._city_to_court("Berlin")
        assert court == "Berlin (Charlottenburg)"

    def test_munich_german(self, pipeline):
        """'Muenchen' maps to 'Muenchen'."""
        court = pipeline._city_to_court("München")
        assert court == "München"

    def test_munich_english(self, pipeline):
        """'Munich' also maps to 'Muenchen'."""
        court = pipeline._city_to_court("Munich")
        assert court == "München"

    def test_hamburg(self, pipeline):
        """Hamburg maps to 'Hamburg'."""
        court = pipeline._city_to_court("Hamburg")
        assert court == "Hamburg"

    def test_cologne_english(self, pipeline):
        """'Cologne' maps to 'Koeln'."""
        court = pipeline._city_to_court("Cologne")
        assert court == "Köln"

    def test_unknown_city_fallback(self, pipeline):
        """Unknown city returns the city name itself as fallback."""
        court = pipeline._city_to_court("Kleinkleckersdorf")
        assert court == "Kleinkleckersdorf"

    def test_empty_city(self, pipeline):
        """Empty string returns empty string."""
        court = pipeline._city_to_court("")
        assert court == ""


class TestGetField:
    """Tests for _get_field method."""

    @pytest.fixture
    def pipeline(self, temp_pipeline):
        return temp_pipeline

    def test_finds_exact_match(self, pipeline):
        """Finds exact column name from candidates list."""
        row = {"firma": "Test GmbH", "ort": "Berlin"}
        fieldnames = {"firma": "firma", "ort": "ort"}

        result = pipeline._get_field(row, fieldnames, ["firma", "name"])
        assert result == "Test GmbH"

    def test_finds_alternative(self, pipeline):
        """Falls back to alternative column name."""
        row = {"company name": "Test GmbH"}
        fieldnames = {"company name": "company name"}

        result = pipeline._get_field(row, fieldnames, ["firma", "company name"])
        assert result == "Test GmbH"

    def test_returns_none_if_not_found(self, pipeline):
        """Returns None when no candidate column exists."""
        row = {"andere_spalte": "Wert"}
        fieldnames = {"andere_spalte": "andere_spalte"}

        result = pipeline._get_field(row, fieldnames, ["firma", "name"])
        assert result is None

    def test_strips_whitespace(self, pipeline):
        """Field values are stripped of surrounding whitespace."""
        row = {"firma": "  Test GmbH  "}
        fieldnames = {"firma": "firma"}

        result = pipeline._get_field(row, fieldnames, ["firma"])
        assert result == "Test GmbH"

    def test_empty_value_returns_none(self, pipeline):
        """Empty field value returns None (falsy check)."""
        row = {"firma": ""}
        fieldnames = {"firma": "firma"}

        result = pipeline._get_field(row, fieldnames, ["firma"])
        assert result is None


class TestImportCsv:
    """Tests for _import_csv and import_file methods."""

    @pytest.fixture
    def pipeline(self, temp_pipeline):
        return temp_pipeline

    def test_import_sample_csv(self, pipeline, sample_csv):
        """Importing a well-formed CSV creates the expected number of companies."""
        pipeline.import_file(str(sample_csv))

        stats = pipeline.db.get_stats()
        assert stats["total"] == 3

    def test_import_sample_csv_data_correct(self, pipeline, sample_csv):
        """Imported company data matches the CSV content."""
        pipeline.import_file(str(sample_csv))

        rows = pipeline.db.conn.execute(
            "SELECT name, city, register_type FROM companies ORDER BY name"
        ).fetchall()

        names = [r["name"] for r in rows]
        assert "Alpha GmbH" in names
        assert "Beta GmbH" in names
        assert "Gamma GmbH" in names

    def test_import_empty_csv(self, pipeline, empty_csv):
        """Importing CSV with only headers creates no companies."""
        pipeline.import_file(str(empty_csv))

        stats = pipeline.db.get_stats()
        assert stats["total"] == 0

    def test_import_malformed_csv(self, pipeline, malformed_csv):
        """Importing CSV with wrong column names creates no companies (names not recognized)."""
        pipeline.import_file(str(malformed_csv))

        stats = pipeline.db.get_stats()
        assert stats["total"] == 0

    def test_import_nonexistent_file(self, pipeline, tmp_path):
        """Importing a non-existent file logs error but does not crash."""
        pipeline.import_file(str(tmp_path / "nonexistent.csv"))

        stats = pipeline.db.get_stats()
        assert stats["total"] == 0

    def test_import_csv_detects_delimiter(self, pipeline, tmp_path):
        """import_file detects comma delimiter when semicolon is absent."""
        csv_path = tmp_path / "comma.csv"
        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f, delimiter=",")
            writer.writerow(["Firma", "Ort", "Registernummer", "ID"])
            writer.writerow(["Delta GmbH", "Leipzig", "HRB 44444", "DF004"])

        pipeline.import_file(str(csv_path))

        stats = pipeline.db.get_stats()
        assert stats["total"] == 1


class TestExport:
    """Tests for export functionality."""

    @pytest.fixture
    def pipeline(self, temp_pipeline):
        return temp_pipeline

    def test_export_creates_file(self, pipeline):
        """Export creates a CSV file on disk."""
        company_id = pipeline.db.insert_company(
            Company(name="Test GmbH", register_num="HRB 12345")
        )

        pipeline.db.update_parsing_result(
            company_id,
            natural_count=1,
            legal_count=0,
            confidence=0.9,
            shareholders=[
                Shareholder(name="Max Mustermann", is_natural_person=True, source="test")
            ],
        )

        output_path = pipeline.export("test_export.csv")
        assert output_path.exists()
        assert output_path.stat().st_size > 0

    def test_export_csv_content(self, pipeline):
        """Exported CSV has correct headers, delimiter, and data."""
        company_id = pipeline.db.insert_company(
            Company(
                name="Export GmbH",
                city="Dresden",
                court="Dresden",
                register_type="HRB",
                register_num="HRB 55555",
            )
        )

        pipeline.db.update_parsing_result(
            company_id,
            natural_count=1,
            legal_count=0,
            confidence=0.85,
            shareholders=[
                Shareholder(
                    name="Erika Musterfrau",
                    share_percent=100.0,
                    is_natural_person=True,
                    source="table",
                )
            ],
        )

        output_path = pipeline.export("verify_export.csv")

        with open(output_path, encoding="utf-8") as f:
            reader = csv.reader(f, delimiter=";")
            rows = list(reader)

        # Verify header
        assert rows[0][0] == "ID"
        assert rows[0][1] == "Firma"
        assert rows[0][8] == "Gesellschafter"

        # Verify data
        assert len(rows) == 2
        assert rows[1][1] == "Export GmbH"
        assert rows[1][2] == "Dresden"
        assert "Erika Musterfrau" in rows[1][8]

    def test_export_empty_no_data_rows(self, pipeline):
        """Export with no qualified companies produces header-only CSV."""
        output_path = pipeline.export("empty_export.csv")

        with open(output_path, encoding="utf-8") as f:
            lines = f.readlines()
        assert len(lines) == 1  # header only

    def test_export_default_filename(self, pipeline):
        """Export without explicit filename generates timestamped file."""
        company_id = pipeline.db.insert_company(
            Company(name="Auto GmbH", register_num="HRB 77777")
        )
        pipeline.db.update_parsing_result(
            company_id,
            natural_count=1,
            legal_count=0,
            confidence=0.9,
            shareholders=[
                Shareholder(name="Hans", is_natural_person=True, source="test")
            ],
        )

        output_path = pipeline.export()
        assert output_path.exists()
        assert "qualified_leads_" in output_path.name


class TestRunDownloads:
    """Tests for run_downloads with mocked GesellschafterlistenDownloader."""

    @pytest.fixture
    def pipeline(self, temp_pipeline):
        return temp_pipeline

    def test_run_downloads_no_pending(self, pipeline):
        """run_downloads with no pending companies returns immediately."""
        # No companies inserted -- should not crash
        pipeline.run_downloads()

        stats = pipeline.db.get_stats()
        assert stats["downloaded"] == 0

    @patch("pipeline.GesellschafterlistenDownloader")
    def test_run_downloads_success(self, mock_downloader_cls, pipeline):
        """run_downloads processes companies and updates their status."""
        from dk_downloader import DownloadResult

        # Insert a pending company
        pipeline.db.insert_company(
            Company(name="Download GmbH", register_num="HRB 88888", court="Berlin")
        )

        # Configure mock
        mock_instance = MagicMock()
        mock_downloader_cls.return_value = mock_instance

        pdf_path = pipeline.pdf_dir / "test.pdf"
        pdf_path.write_bytes(b"%PDF-1.4 test")

        mock_instance.download.return_value = DownloadResult(
            success=True, pdf_path=pdf_path
        )

        pipeline.run_downloads()

        stats = pipeline.db.get_stats()
        assert stats["downloaded"] == 1
        mock_instance.start.assert_called_once()
        mock_instance.stop.assert_called_once()

    @patch("pipeline.GesellschafterlistenDownloader")
    def test_run_downloads_no_gl(self, mock_downloader_cls, pipeline):
        """run_downloads handles 'no Gesellschafterliste available' result."""
        from dk_downloader import DownloadResult

        pipeline.db.insert_company(
            Company(name="NoGL GmbH", register_num="HRB 11111")
        )

        mock_instance = MagicMock()
        mock_downloader_cls.return_value = mock_instance
        mock_instance.download.return_value = DownloadResult(
            success=True, no_gl_available=True
        )

        pipeline.run_downloads()

        stats = pipeline.db.get_stats()
        assert stats["downloaded"] == 1
        assert stats["no_gl"] == 1

    @patch("pipeline.GesellschafterlistenDownloader")
    def test_run_downloads_error(self, mock_downloader_cls, pipeline):
        """run_downloads handles download errors gracefully."""
        from dk_downloader import DownloadResult

        pipeline.db.insert_company(
            Company(name="Error GmbH", register_num="HRB 22222")
        )

        mock_instance = MagicMock()
        mock_downloader_cls.return_value = mock_instance
        mock_instance.download.return_value = DownloadResult(
            success=False, error="Timeout"
        )

        pipeline.run_downloads()

        # Error is logged, company marked as attempted
        stats = pipeline.db.get_stats()
        assert stats["downloaded"] == 1  # marked as attempted

    @patch("pipeline.GesellschafterlistenDownloader")
    def test_run_downloads_with_limit(self, mock_downloader_cls, pipeline):
        """run_downloads respects the limit parameter."""
        from dk_downloader import DownloadResult

        for i in range(5):
            pipeline.db.insert_company(
                Company(name=f"Firma {i}", register_num=f"HRB {5000 + i}")
            )

        mock_instance = MagicMock()
        mock_downloader_cls.return_value = mock_instance
        mock_instance.download.return_value = DownloadResult(success=True, no_gl_available=True)

        pipeline.run_downloads(limit=2)

        assert mock_instance.download.call_count == 2


class TestRunParsing:
    """Tests for run_parsing with mocked GesellschafterlisteParser."""

    @pytest.fixture
    def pipeline(self, temp_pipeline):
        return temp_pipeline

    def test_run_parsing_no_pending(self, pipeline):
        """run_parsing with no pending PDFs returns immediately."""
        pipeline.run_parsing()

        stats = pipeline.db.get_stats()
        assert stats["parsed"] == 0

    @patch("pipeline.GesellschafterlisteParser")
    def test_run_parsing_success(self, mock_parser_cls, pipeline):
        """run_parsing processes PDFs and stores results."""
        from pdf_parser import ParsingResult

        # Create a company that is downloaded but not parsed
        cid = pipeline.db.insert_company(
            Company(name="Parse GmbH", register_num="HRB 33333")
        )

        # Create a fake PDF file
        pdf_file = pipeline.pdf_dir / "test_parse.pdf"
        pdf_file.write_bytes(b"%PDF-1.4 test")
        pipeline.db.update_download_status(cid, str(pdf_file), True)

        # Configure mock parser
        mock_instance = MagicMock()
        mock_parser_cls.return_value = mock_instance
        mock_instance.parse.return_value = ParsingResult(
            shareholders=[
                Shareholder(name="Max Mustermann", is_natural_person=True, source="table"),
            ],
            natural_persons_count=1,
            legal_entities_count=0,
            confidence=0.9,
        )

        pipeline.run_parsing()

        stats = pipeline.db.get_stats()
        assert stats["parsed"] == 1
        assert stats["qualified"] == 1


class TestPipelineIntegration:
    """Integration test: import -> mock parse -> export -> verify CSV."""

    @pytest.fixture
    def pipeline(self, temp_pipeline):
        return temp_pipeline

    def test_full_pipeline_flow(self, pipeline, sample_csv):
        """Full pipeline: import CSV, simulate download+parse, export, verify output."""
        # Step 1: Import
        pipeline.import_file(str(sample_csv))
        stats = pipeline.db.get_stats()
        assert stats["total"] == 3

        # Step 2: Simulate download for first company
        companies = pipeline.db.get_pending_downloads()
        assert len(companies) == 3

        pdf_file = pipeline.pdf_dir / "alpha.pdf"
        pdf_file.write_bytes(b"%PDF-1.4 fake content")

        pipeline.db.update_download_status(companies[0].id, str(pdf_file), True)
        pipeline.db.log_event(companies[0].id, "download", "success")

        # Mark others as no_gl
        for c in companies[1:]:
            pipeline.db.update_download_status(c.id, None, True)

        # Step 3: Simulate parsing
        pipeline.db.update_parsing_result(
            companies[0].id,
            natural_count=1,
            legal_count=0,
            confidence=0.9,
            shareholders=[
                Shareholder(
                    name="Max Mustermann",
                    share_percent=100.0,
                    is_natural_person=True,
                    source="table",
                )
            ],
        )

        # Step 4: Export
        output_path = pipeline.export("integration_test.csv")
        assert output_path.exists()

        # Step 5: Verify CSV content
        with open(output_path, encoding="utf-8") as f:
            reader = csv.reader(f, delimiter=";")
            rows = list(reader)

        assert len(rows) == 2  # header + 1 qualified company
        assert rows[1][1] == "Alpha GmbH"
        assert "Max Mustermann" in rows[1][8]

        # Step 6: Verify stats
        stats = pipeline.db.get_stats()
        assert stats["total"] == 3
        assert stats["downloaded"] == 3
        assert stats["parsed"] == 1
        assert stats["qualified"] == 1
        assert stats["no_gl"] == 2


class TestShowStats:
    """Tests for show_stats output."""

    @pytest.fixture
    def pipeline(self, temp_pipeline):
        return temp_pipeline

    def test_show_stats_does_not_crash(self, pipeline, capsys):
        """show_stats runs without error even on empty database."""
        pipeline.show_stats()
        captured = capsys.readouterr()
        assert "GF-Screening Pipeline" in captured.out
        assert "Firmen gesamt" in captured.out


class TestClose:
    """Tests for pipeline cleanup."""

    def test_close_shuts_down_db(self, temp_pipeline):
        """close() closes the underlying database connection."""
        temp_pipeline.close()

        with pytest.raises(Exception):
            temp_pipeline.db.conn.execute("SELECT 1")
