"""
Shared fixtures for all GF-Screening tests.

Provides reusable test infrastructure: temporary databases,
pipeline instances, parser instances, and test data generators.
"""

import csv
import tempfile
import os
from pathlib import Path

import pytest

from models import Database, Company, Shareholder
from pdf_parser import GesellschafterlisteParser
from pipeline import GFScreeningPipeline


@pytest.fixture
def temp_db():
    """Creates a temporary SQLite database for testing.

    Uses NamedTemporaryFile to get a valid path, then hands it
    to Database. Cleans up after test completion.
    """
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    db = Database(db_path)
    yield db

    db.close()
    try:
        os.unlink(db_path)
    except OSError:
        pass


@pytest.fixture
def temp_pipeline():
    """Creates a pipeline with a temporary working directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        pipeline = GFScreeningPipeline(base_dir=Path(tmpdir))
        yield pipeline
        pipeline.close()


@pytest.fixture
def parser():
    """Creates a GesellschafterlisteParser instance."""
    return GesellschafterlisteParser()


@pytest.fixture
def sample_company():
    """Returns a sample Company for testing."""
    return Company(
        dealfront_id="DF-TEST-001",
        name="Muster GmbH",
        city="Berlin",
        court="Berlin (Charlottenburg)",
        register_type="HRB",
        register_num="HRB 12345",
    )


@pytest.fixture
def sample_shareholders():
    """Returns a list of sample Shareholders for testing."""
    return [
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


@pytest.fixture
def populated_db(temp_db, sample_company, sample_shareholders):
    """Database with one company and shareholders already inserted."""
    company_id = temp_db.insert_company(sample_company)

    temp_db.update_download_status(company_id, "/tmp/test.pdf", True)
    temp_db.update_parsing_result(
        company_id,
        natural_count=2,
        legal_count=0,
        confidence=0.85,
        shareholders=sample_shareholders,
    )

    return temp_db, company_id


@pytest.fixture
def sample_csv(tmp_path):
    """Creates a sample CSV file for import testing."""
    csv_path = tmp_path / "test_import.csv"
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(["Firma", "Ort", "Registernummer", "ID"])
        writer.writerow(["Alpha GmbH", "Berlin", "HRB 11111", "DF001"])
        writer.writerow(["Beta GmbH", "Hamburg", "HRB 22222", "DF002"])
        writer.writerow(["Gamma GmbH", "Dresden", "HRA 33333", "DF003"])
    return csv_path


@pytest.fixture
def empty_csv(tmp_path):
    """Creates an empty CSV file (header only) for edge case testing."""
    csv_path = tmp_path / "empty.csv"
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(["Firma", "Ort", "Registernummer"])
    return csv_path


@pytest.fixture
def malformed_csv(tmp_path):
    """Creates a CSV with wrong column names."""
    csv_path = tmp_path / "malformed.csv"
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(["Spalte1", "Spalte2", "Spalte3"])
        writer.writerow(["Wert1", "Wert2", "Wert3"])
    return csv_path
