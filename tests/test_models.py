"""
Unit Tests für models.py - Datenbank und Datenmodelle
"""

import pytest
import tempfile
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from models import Database, Company, Shareholder


@pytest.fixture
def temp_db():
    """Erstellt temporäre Datenbank für Tests."""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        db_path = f.name

    db = Database(db_path)
    yield db

    db.close()
    os.unlink(db_path)


class TestDatabase:
    """Tests für Database-Klasse."""

    def test_init_creates_tables(self, temp_db):
        """Datenbank erstellt alle Tabellen."""
        cursor = temp_db.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
        tables = {row[0] for row in cursor.fetchall()}

        assert 'companies' in tables
        assert 'shareholders' in tables
        assert 'pipeline_log' in tables

    def test_insert_company(self, temp_db):
        """Firma einfügen funktioniert."""
        company = Company(
            dealfront_id="DF123",
            name="Test GmbH",
            city="Berlin",
            court="Charlottenburg",
            register_type="HRB",
            register_num="HRB 12345"
        )

        company_id = temp_db.insert_company(company)

        assert company_id > 0

        # Prüfen ob gespeichert
        row = temp_db.conn.execute(
            "SELECT name, register_num FROM companies WHERE id = ?",
            (company_id,)
        ).fetchone()

        assert row['name'] == "Test GmbH"
        assert row['register_num'] == "HRB 12345"

    def test_insert_duplicate_ignored(self, temp_db):
        """Duplikate werden ignoriert."""
        company = Company(
            name="Test GmbH",
            register_num="HRB 12345"
        )

        id1 = temp_db.insert_company(company)
        id2 = temp_db.insert_company(company)

        # Sollte gleiche ID zurückgeben
        assert id1 == id2

        # Nur ein Eintrag
        count = temp_db.conn.execute(
            "SELECT COUNT(*) FROM companies"
        ).fetchone()[0]
        assert count == 1

    def test_get_pending_downloads(self, temp_db):
        """Pending Downloads werden korrekt gefiltert."""
        # Firma mit Registernummer
        temp_db.insert_company(Company(
            name="Firma A",
            register_num="HRB 111"
        ))

        # Firma ohne Registernummer
        temp_db.insert_company(Company(
            name="Firma B",
            register_num=""
        ))

        # Bereits heruntergeladen
        temp_db.insert_company(Company(
            name="Firma C",
            register_num="HRB 333"
        ))
        temp_db.conn.execute(
            "UPDATE companies SET dk_downloaded = TRUE WHERE name = 'Firma C'"
        )
        temp_db.conn.commit()

        pending = temp_db.get_pending_downloads()

        assert len(pending) == 1
        assert pending[0].name == "Firma A"

    def test_update_download_status(self, temp_db):
        """Download-Status Update funktioniert."""
        company_id = temp_db.insert_company(Company(
            name="Test GmbH",
            register_num="HRB 12345"
        ))

        temp_db.update_download_status(company_id, "/path/to/file.pdf", True)

        row = temp_db.conn.execute(
            "SELECT dk_downloaded, pdf_path FROM companies WHERE id = ?",
            (company_id,)
        ).fetchone()

        assert row['dk_downloaded'] == 1
        assert row['pdf_path'] == "/path/to/file.pdf"

    def test_update_parsing_result(self, temp_db):
        """Parsing-Ergebnis Update funktioniert."""
        company_id = temp_db.insert_company(Company(
            name="Test GmbH",
            register_num="HRB 12345"
        ))

        shareholders = [
            Shareholder(name="Max Mustermann", share_percent=50.0, is_natural_person=True, source="table"),
            Shareholder(name="Erika Musterfrau", share_percent=50.0, is_natural_person=True, source="table"),
        ]

        temp_db.update_parsing_result(
            company_id,
            natural_count=2,
            legal_count=0,
            confidence=0.9,
            shareholders=shareholders
        )

        # Company prüfen
        row = temp_db.conn.execute(
            "SELECT natural_persons_count, is_qualified FROM companies WHERE id = ?",
            (company_id,)
        ).fetchone()

        assert row['natural_persons_count'] == 2
        assert row['is_qualified'] == 1  # <=2 natürliche, 0 juristische

        # Shareholders prüfen
        sh_count = temp_db.conn.execute(
            "SELECT COUNT(*) FROM shareholders WHERE company_id = ?",
            (company_id,)
        ).fetchone()[0]

        assert sh_count == 2

    def test_get_stats(self, temp_db):
        """Statistiken werden korrekt berechnet."""
        # 3 Firmen einfügen
        for i in range(3):
            temp_db.insert_company(Company(
                name=f"Firma {i}",
                register_num=f"HRB {i}"
            ))

        # Eine als heruntergeladen markieren
        temp_db.conn.execute(
            "UPDATE companies SET dk_downloaded = TRUE WHERE name = 'Firma 0'"
        )
        temp_db.conn.commit()

        stats = temp_db.get_stats()

        assert stats['total'] == 3
        assert stats['downloaded'] == 1
        assert stats['parsed'] == 0
        assert stats['qualified'] == 0


class TestCompanyDataclass:
    """Tests für Company Dataclass."""

    def test_default_values(self):
        """Default-Werte sind korrekt."""
        company = Company()

        assert company.id is None
        assert company.name == ""
        assert company.dk_downloaded is False
        assert company.is_qualified is None

    def test_with_values(self):
        """Werte werden korrekt gesetzt."""
        company = Company(
            id=1,
            name="Test GmbH",
            register_num="HRB 12345",
            is_qualified=True
        )

        assert company.id == 1
        assert company.name == "Test GmbH"
        assert company.is_qualified is True
