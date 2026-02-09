"""
Unit Tests for models.py - Database and data models.

Tests cover: schema creation, CRUD operations, pipeline state queries,
export functionality, and edge cases for the Database class.
"""

import csv
import pytest
from pathlib import Path

from models import Database, Company, Shareholder


class TestDatabase:
    """Tests for Database class."""

    def test_init_creates_tables(self, temp_db):
        """Database initialization creates all required tables."""
        cursor = temp_db.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
        tables = {row[0] for row in cursor.fetchall()}

        assert "companies" in tables
        assert "shareholders" in tables
        assert "pipeline_log" in tables

    def test_init_creates_indexes(self, temp_db):
        """Database initialization creates performance indexes."""
        cursor = temp_db.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
        )
        indexes = {row[0] for row in cursor.fetchall()}

        assert "idx_companies_qualified" in indexes
        assert "idx_companies_pipeline" in indexes
        assert "idx_shareholders_company" in indexes

    def test_insert_company(self, temp_db):
        """Inserting a company returns a positive ID and persists data."""
        company = Company(
            dealfront_id="DF123",
            name="Test GmbH",
            city="Berlin",
            court="Charlottenburg",
            register_type="HRB",
            register_num="HRB 12345",
        )

        company_id = temp_db.insert_company(company)
        assert company_id > 0

        row = temp_db.conn.execute(
            "SELECT name, register_num FROM companies WHERE id = ?",
            (company_id,),
        ).fetchone()

        assert row["name"] == "Test GmbH"
        assert row["register_num"] == "HRB 12345"

    def test_insert_duplicate_ignored(self, temp_db):
        """Duplicate companies (same name + register_num) are silently ignored."""
        company = Company(name="Test GmbH", register_num="HRB 12345")

        id1 = temp_db.insert_company(company)
        id2 = temp_db.insert_company(company)

        assert id1 == id2

        count = temp_db.conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
        assert count == 1

    def test_get_pending_downloads(self, temp_db):
        """get_pending_downloads returns only companies with register_num and not yet downloaded."""
        # Company with register number -- should appear
        temp_db.insert_company(Company(name="Firma A", register_num="HRB 111"))

        # Company without register number -- should NOT appear
        temp_db.insert_company(Company(name="Firma B", register_num=""))

        # Already downloaded -- should NOT appear
        temp_db.insert_company(Company(name="Firma C", register_num="HRB 333"))
        temp_db.conn.execute(
            "UPDATE companies SET dk_downloaded = TRUE WHERE name = 'Firma C'"
        )
        temp_db.conn.commit()

        pending = temp_db.get_pending_downloads()

        assert len(pending) == 1
        assert pending[0].name == "Firma A"

    def test_get_pending_downloads_with_limit(self, temp_db):
        """get_pending_downloads respects the limit parameter."""
        for i in range(5):
            temp_db.insert_company(
                Company(name=f"Firma {i}", register_num=f"HRB {1000 + i}")
            )

        pending = temp_db.get_pending_downloads(limit=2)
        assert len(pending) == 2

    def test_get_pending_parsing(self, temp_db):
        """get_pending_parsing returns only downloaded but unparsed companies with pdf_path."""
        # Downloaded with pdf_path, not parsed -- should appear
        cid = temp_db.insert_company(Company(name="Firma A", register_num="HRB 100"))
        temp_db.update_download_status(cid, "/tmp/a.pdf", True)

        # Downloaded without pdf_path (no_gl) -- should NOT appear
        cid2 = temp_db.insert_company(Company(name="Firma B", register_num="HRB 200"))
        temp_db.update_download_status(cid2, None, True)

        # Not downloaded -- should NOT appear
        temp_db.insert_company(Company(name="Firma C", register_num="HRB 300"))

        pending = temp_db.get_pending_parsing()

        assert len(pending) == 1
        assert pending[0].name == "Firma A"

    def test_get_pending_parsing_with_limit(self, temp_db):
        """get_pending_parsing respects the limit parameter."""
        for i in range(5):
            cid = temp_db.insert_company(
                Company(name=f"Firma {i}", register_num=f"HRB {2000 + i}")
            )
            temp_db.update_download_status(cid, f"/tmp/{i}.pdf", True)

        pending = temp_db.get_pending_parsing(limit=3)
        assert len(pending) == 3

    def test_update_download_status(self, temp_db):
        """update_download_status sets dk_downloaded and pdf_path."""
        company_id = temp_db.insert_company(
            Company(name="Test GmbH", register_num="HRB 12345")
        )

        temp_db.update_download_status(company_id, "/path/to/file.pdf", True)

        row = temp_db.conn.execute(
            "SELECT dk_downloaded, pdf_path FROM companies WHERE id = ?",
            (company_id,),
        ).fetchone()

        assert row["dk_downloaded"] == 1
        assert row["pdf_path"] == "/path/to/file.pdf"

    def test_update_parsing_result(self, temp_db):
        """update_parsing_result persists counts, confidence, qualification, and shareholders."""
        company_id = temp_db.insert_company(
            Company(name="Test GmbH", register_num="HRB 12345")
        )

        shareholders = [
            Shareholder(
                name="Max Mustermann",
                share_percent=50.0,
                is_natural_person=True,
                source="table",
            ),
            Shareholder(
                name="Erika Musterfrau",
                share_percent=50.0,
                is_natural_person=True,
                source="table",
            ),
        ]

        temp_db.update_parsing_result(
            company_id,
            natural_count=2,
            legal_count=0,
            confidence=0.9,
            shareholders=shareholders,
        )

        row = temp_db.conn.execute(
            "SELECT natural_persons_count, legal_entities_count, "
            "parsing_confidence, is_qualified, pdf_parsed FROM companies WHERE id = ?",
            (company_id,),
        ).fetchone()

        assert row["natural_persons_count"] == 2
        assert row["legal_entities_count"] == 0
        assert row["parsing_confidence"] == pytest.approx(0.9)
        assert row["is_qualified"] == 1  # <=2 natural, 0 legal
        assert row["pdf_parsed"] == 1

        sh_count = temp_db.conn.execute(
            "SELECT COUNT(*) FROM shareholders WHERE company_id = ?",
            (company_id,),
        ).fetchone()[0]
        assert sh_count == 2

    def test_update_parsing_result_not_qualified(self, temp_db):
        """Company with legal entities is NOT qualified."""
        company_id = temp_db.insert_company(
            Company(name="Holding Co", register_num="HRB 99999")
        )

        temp_db.update_parsing_result(
            company_id,
            natural_count=1,
            legal_count=1,
            confidence=0.8,
            shareholders=[
                Shareholder(name="Max Mustermann", is_natural_person=True, source="table"),
                Shareholder(name="Alpha Holding GmbH", is_natural_person=False, source="table"),
            ],
        )

        row = temp_db.conn.execute(
            "SELECT is_qualified FROM companies WHERE id = ?", (company_id,)
        ).fetchone()
        assert row["is_qualified"] == 0

    def test_log_event(self, temp_db):
        """log_event persists an event in the pipeline_log table."""
        company_id = temp_db.insert_company(
            Company(name="Test GmbH", register_num="HRB 12345")
        )

        temp_db.log_event(company_id, "download", "success", "Downloaded OK")

        row = temp_db.conn.execute(
            "SELECT company_id, stage, status, message FROM pipeline_log WHERE company_id = ?",
            (company_id,),
        ).fetchone()

        assert row["company_id"] == company_id
        assert row["stage"] == "download"
        assert row["status"] == "success"
        assert row["message"] == "Downloaded OK"

    def test_log_event_multiple_entries(self, temp_db):
        """Multiple log events for the same company are all stored."""
        company_id = temp_db.insert_company(
            Company(name="Test GmbH", register_num="HRB 12345")
        )

        temp_db.log_event(company_id, "download", "error", "Timeout")
        temp_db.log_event(company_id, "download", "success", "Retry OK")

        count = temp_db.conn.execute(
            "SELECT COUNT(*) FROM pipeline_log WHERE company_id = ?",
            (company_id,),
        ).fetchone()[0]
        assert count == 2

    def test_get_stats(self, temp_db):
        """get_stats returns correct aggregate counts."""
        for i in range(3):
            temp_db.insert_company(
                Company(name=f"Firma {i}", register_num=f"HRB {i}")
            )

        temp_db.conn.execute(
            "UPDATE companies SET dk_downloaded = TRUE WHERE name = 'Firma 0'"
        )
        temp_db.conn.commit()

        stats = temp_db.get_stats()

        assert stats["total"] == 3
        assert stats["downloaded"] == 1
        assert stats["parsed"] == 0
        assert stats["qualified"] == 0
        assert stats["no_gl"] == 1  # downloaded but no pdf_path

    def test_export_qualified_csv_content(self, temp_db, tmp_path):
        """export_qualified writes correct CSV with headers, delimiter, and data rows."""
        company_id = temp_db.insert_company(
            Company(
                name="Alpha GmbH",
                city="Berlin",
                court="Charlottenburg",
                register_type="HRB",
                register_num="HRB 12345",
            )
        )

        temp_db.update_parsing_result(
            company_id,
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

        output_path = tmp_path / "qualified.csv"
        count = temp_db.export_qualified(str(output_path))

        assert count == 1
        assert output_path.exists()

        with open(output_path, encoding="utf-8") as f:
            reader = csv.reader(f, delimiter=";")
            rows = list(reader)

        # Header row
        assert rows[0] == [
            "ID", "Firma", "Ort", "Registergericht", "Registerart",
            "Registernummer", "Anzahl Gesellschafter", "Konfidenz", "Gesellschafter",
        ]

        # Data row
        assert len(rows) == 2
        data = rows[1]
        assert data[1] == "Alpha GmbH"
        assert data[2] == "Berlin"
        assert "Max Mustermann" in data[8]

    def test_export_qualified_empty(self, temp_db, tmp_path):
        """export_qualified with no qualified companies writes only the header."""
        output_path = tmp_path / "empty.csv"
        count = temp_db.export_qualified(str(output_path))

        assert count == 0
        assert output_path.exists()

        with open(output_path, encoding="utf-8") as f:
            lines = f.readlines()
        assert len(lines) == 1  # header only

    def test_close(self, temp_db):
        """close() shuts down the database connection without errors."""
        temp_db.close()

        # After close, operations should raise an error
        with pytest.raises(Exception):
            temp_db.conn.execute("SELECT 1")

    # --- _execute_with_limit tests ---

    def test_execute_with_limit_valid(self, temp_db):
        """_execute_with_limit with a valid positive integer limit returns correct rows."""
        for i in range(5):
            temp_db.insert_company(
                Company(name=f"Firma {i}", register_num=f"HRB {3000 + i}")
            )

        rows = temp_db._execute_with_limit("SELECT * FROM companies", limit=3)
        assert len(rows) == 3

    def test_execute_with_limit_none(self, temp_db):
        """_execute_with_limit with limit=None returns all rows."""
        for i in range(3):
            temp_db.insert_company(
                Company(name=f"Firma {i}", register_num=f"HRB {4000 + i}")
            )

        rows = temp_db._execute_with_limit("SELECT * FROM companies")
        assert len(rows) == 3

    def test_execute_with_limit_negative(self, temp_db):
        """_execute_with_limit with negative limit raises ValueError."""
        with pytest.raises(ValueError, match="Invalid limit"):
            temp_db._execute_with_limit("SELECT * FROM companies", limit=-1)

    def test_execute_with_limit_zero(self, temp_db):
        """_execute_with_limit with limit=0 raises ValueError."""
        with pytest.raises(ValueError, match="Invalid limit"):
            temp_db._execute_with_limit("SELECT * FROM companies", limit=0)

    def test_execute_with_limit_string(self, temp_db):
        """_execute_with_limit with string limit raises ValueError."""
        with pytest.raises(ValueError, match="Invalid limit"):
            temp_db._execute_with_limit("SELECT * FROM companies", limit="five")

    def test_execute_with_limit_large_value(self, temp_db):
        """_execute_with_limit with limit > 10000 raises ValueError."""
        with pytest.raises(ValueError, match="exceeds maximum"):
            temp_db._execute_with_limit("SELECT * FROM companies", limit=10001)

    def test_execute_with_limit_at_max(self, temp_db):
        """_execute_with_limit with limit=10000 works without error."""
        temp_db.insert_company(Company(name="Solo", register_num="HRB 9999"))

        rows = temp_db._execute_with_limit(
            "SELECT * FROM companies", limit=10000
        )
        assert len(rows) == 1


class TestCompanyDataclass:
    """Tests for Company dataclass."""

    def test_default_values(self):
        """Default values are correctly set."""
        company = Company()

        assert company.id is None
        assert company.dealfront_id == ""
        assert company.name == ""
        assert company.city == ""
        assert company.court == ""
        assert company.register_type == ""
        assert company.register_num == ""
        assert company.dk_downloaded is False
        assert company.pdf_parsed is False
        assert company.pdf_path is None
        assert company.natural_persons_count is None
        assert company.legal_entities_count is None
        assert company.parsing_confidence is None
        assert company.is_qualified is None
        assert company.created_at is None
        assert company.updated_at is None

    def test_with_values(self):
        """Values are correctly set via constructor."""
        company = Company(
            id=1,
            name="Test GmbH",
            register_num="HRB 12345",
            is_qualified=True,
        )

        assert company.id == 1
        assert company.name == "Test GmbH"
        assert company.is_qualified is True


class TestShareholderDataclass:
    """Tests for Shareholder dataclass."""

    def test_default_values(self):
        """Default values are correctly set."""
        sh = Shareholder()

        assert sh.id is None
        assert sh.company_id == 0
        assert sh.name == ""
        assert sh.share_percent is None
        assert sh.is_natural_person is True
        assert sh.source == ""
        assert sh.created_at is None

    def test_with_values(self):
        """Values are correctly set via constructor."""
        sh = Shareholder(
            name="Max Mustermann",
            share_percent=50.0,
            is_natural_person=True,
            source="table",
        )

        assert sh.name == "Max Mustermann"
        assert sh.share_percent == 50.0
        assert sh.source == "table"
