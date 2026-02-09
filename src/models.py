"""
Datenmodelle und SQLite-Schema für GF-Screening Pipeline.
"""

import csv
import logging
import sqlite3
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass
from typing import Optional, List

logger = logging.getLogger(__name__)


@dataclass
class Company:
    """Firma aus Dealfront-Import."""
    id: Optional[int] = None
    dealfront_id: str = ""
    name: str = ""
    city: str = ""
    court: str = ""  # Registergericht
    register_type: str = ""  # HRB, HRA
    register_num: str = ""  # Vollständige Nummer

    # Pipeline-Status
    dk_downloaded: bool = False
    pdf_parsed: bool = False
    pdf_path: Optional[str] = None

    # Ergebnis
    natural_persons_count: Optional[int] = None
    legal_entities_count: Optional[int] = None
    parsing_confidence: Optional[float] = None
    is_qualified: Optional[bool] = None

    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass
class Shareholder:
    """Gesellschafter einer Firma."""
    id: Optional[int] = None
    company_id: int = 0
    name: str = ""
    share_percent: Optional[float] = None
    is_natural_person: bool = True
    source: str = ""  # 'table', 'regex:standard', etc.
    created_at: Optional[datetime] = None


class Database:
    """SQLite-Datenbank für Pipeline-State."""

    SCHEMA = """
    CREATE TABLE IF NOT EXISTS companies (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        dealfront_id TEXT,
        name TEXT NOT NULL,
        city TEXT,
        court TEXT,
        register_type TEXT,
        register_num TEXT,

        dk_downloaded BOOLEAN DEFAULT FALSE,
        pdf_parsed BOOLEAN DEFAULT FALSE,
        pdf_path TEXT,

        natural_persons_count INTEGER,
        legal_entities_count INTEGER,
        parsing_confidence REAL,
        is_qualified BOOLEAN,

        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,

        UNIQUE(name, register_num)
    );

    CREATE TABLE IF NOT EXISTS shareholders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_id INTEGER REFERENCES companies(id),
        name TEXT NOT NULL,
        share_percent REAL,
        is_natural_person BOOLEAN,
        source TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS pipeline_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_id INTEGER REFERENCES companies(id),
        stage TEXT,
        status TEXT,
        message TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );

    CREATE INDEX IF NOT EXISTS idx_companies_qualified ON companies(is_qualified);
    CREATE INDEX IF NOT EXISTS idx_companies_pipeline ON companies(dk_downloaded, pdf_parsed);
    CREATE INDEX IF NOT EXISTS idx_shareholders_company ON shareholders(company_id);
    """

    def __init__(self, db_path: str = "data/gesellschafter.db") -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        """Erstellt Tabellen falls nicht vorhanden."""
        self.conn.executescript(self.SCHEMA)
        self.conn.commit()

    def insert_company(self, company: Company) -> int:
        """Fügt Firma ein und gibt ID zurück."""
        cursor = self.conn.execute("""
            INSERT OR IGNORE INTO companies
            (dealfront_id, name, city, court, register_type, register_num)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            company.dealfront_id, company.name, company.city,
            company.court, company.register_type, company.register_num
        ))
        self.conn.commit()

        if cursor.lastrowid == 0:
            # Already exists, get existing ID
            row = self.conn.execute(
                "SELECT id FROM companies WHERE name = ? AND register_num = ?",
                (company.name, company.register_num)
            ).fetchone()
            return row['id'] if row else 0

        return cursor.lastrowid

    def _execute_with_limit(self, base_query: str, limit: Optional[int] = None) -> list:
        """Executes query with optional LIMIT clause using parameterized query.

        Args:
            base_query: SQL-Abfrage ohne LIMIT.
            limit: Optionale maximale Anzahl Ergebnisse (1-10000).

        Returns:
            Liste der Ergebniszeilen.

        Raises:
            ValueError: Wenn limit kein positiver Integer oder > 10000.
        """
        params: list = []
        query: str = base_query

        if limit is not None:
            if not isinstance(limit, int) or limit < 1:
                raise ValueError(f"Invalid limit: {limit}. Must be positive integer.")
            if limit > 10000:
                raise ValueError(f"Limit {limit} exceeds maximum of 10000.")
            query += " LIMIT ?"
            params.append(limit)

        return self.conn.execute(query, params).fetchall()

    def get_pending_downloads(self, limit: Optional[int] = None) -> List[Company]:
        """Holt Firmen die noch heruntergeladen werden müssen."""
        query = """
            SELECT * FROM companies
            WHERE dk_downloaded = FALSE AND register_num IS NOT NULL AND register_num != ''
            ORDER BY id
        """
        rows = self._execute_with_limit(query, limit)
        return [self._row_to_company(row) for row in rows]

    def get_pending_parsing(self, limit: Optional[int] = None) -> List[Company]:
        """Holt Firmen die noch geparst werden müssen."""
        query = """
            SELECT * FROM companies
            WHERE dk_downloaded = TRUE AND pdf_parsed = FALSE AND pdf_path IS NOT NULL
            ORDER BY id
        """
        rows = self._execute_with_limit(query, limit)
        return [self._row_to_company(row) for row in rows]

    def update_download_status(self, company_id: int, pdf_path: Optional[str],
                               success: bool) -> None:
        """Aktualisiert Download-Status."""
        self.conn.execute("""
            UPDATE companies SET
                dk_downloaded = TRUE,
                pdf_path = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (pdf_path, company_id))
        self.conn.commit()

    def update_parsing_result(self, company_id: int, natural_count: int,
                              legal_count: int, confidence: float,
                              shareholders: List[Shareholder]) -> None:
        """Speichert Parsing-Ergebnis."""
        is_qualified: bool = natural_count <= 2 and legal_count == 0

        self.conn.execute("""
            UPDATE companies SET
                pdf_parsed = TRUE,
                natural_persons_count = ?,
                legal_entities_count = ?,
                parsing_confidence = ?,
                is_qualified = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (natural_count, legal_count, confidence, is_qualified, company_id))

        # Gesellschafter einfügen
        for sh in shareholders:
            self.conn.execute("""
                INSERT INTO shareholders (company_id, name, share_percent, is_natural_person, source)
                VALUES (?, ?, ?, ?, ?)
            """, (company_id, sh.name, sh.share_percent, sh.is_natural_person, sh.source))

        self.conn.commit()

    def log_event(self, company_id: int, stage: str, status: str,
                  message: str = "") -> None:
        """Loggt Pipeline-Event."""
        self.conn.execute("""
            INSERT INTO pipeline_log (company_id, stage, status, message)
            VALUES (?, ?, ?, ?)
        """, (company_id, stage, status, message))
        self.conn.commit()

    def get_stats(self) -> dict:
        """Holt Pipeline-Statistiken."""
        stats: dict = {}

        stats['total'] = self.conn.execute(
            "SELECT COUNT(*) FROM companies"
        ).fetchone()[0]

        stats['downloaded'] = self.conn.execute(
            "SELECT COUNT(*) FROM companies WHERE dk_downloaded = TRUE"
        ).fetchone()[0]

        stats['parsed'] = self.conn.execute(
            "SELECT COUNT(*) FROM companies WHERE pdf_parsed = TRUE"
        ).fetchone()[0]

        stats['qualified'] = self.conn.execute(
            "SELECT COUNT(*) FROM companies WHERE is_qualified = TRUE"
        ).fetchone()[0]

        stats['no_gl'] = self.conn.execute(
            "SELECT COUNT(*) FROM companies WHERE dk_downloaded = TRUE AND pdf_path IS NULL"
        ).fetchone()[0]

        return stats

    def export_qualified(self, output_path: str) -> int:
        """Exportiert qualifizierte Leads als CSV.

        Args:
            output_path: Pfad zur Ausgabedatei.

        Returns:
            Anzahl exportierter Zeilen.

        Raises:
            FileNotFoundError: Wenn das Zielverzeichnis nicht existiert.
            PermissionError: Wenn keine Schreibberechtigung besteht.
            OSError: Bei sonstigen Dateisystemfehlern.
        """
        rows = self.conn.execute("""
            SELECT
                c.id, c.name, c.city, c.court, c.register_type, c.register_num,
                c.natural_persons_count, c.parsing_confidence,
                GROUP_CONCAT(s.name, '; ') as shareholders
            FROM companies c
            LEFT JOIN shareholders s ON c.id = s.company_id AND s.is_natural_person = TRUE
            WHERE c.is_qualified = TRUE
            GROUP BY c.id
            ORDER BY c.name
        """).fetchall()

        try:
            with open(output_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f, delimiter=';')
                writer.writerow([
                    'ID', 'Firma', 'Ort', 'Registergericht', 'Registerart',
                    'Registernummer', 'Anzahl Gesellschafter', 'Konfidenz', 'Gesellschafter'
                ])

                for row in rows:
                    writer.writerow(list(row))
        except (FileNotFoundError, PermissionError, OSError) as e:
            logger.error(f"Export fehlgeschlagen: {e}")
            raise

        return len(rows)

    def rollback(self) -> None:
        """Rolls back uncommitted changes."""
        self.conn.rollback()

    def _row_to_company(self, row: sqlite3.Row) -> Company:
        """Konvertiert DB-Row zu Company-Objekt."""
        return Company(
            id=row['id'],
            dealfront_id=row['dealfront_id'] or "",
            name=row['name'],
            city=row['city'] or "",
            court=row['court'] or "",
            register_type=row['register_type'] or "",
            register_num=row['register_num'] or "",
            dk_downloaded=bool(row['dk_downloaded']),
            pdf_parsed=bool(row['pdf_parsed']),
            pdf_path=row['pdf_path'],
            natural_persons_count=row['natural_persons_count'],
            legal_entities_count=row['legal_entities_count'],
            parsing_confidence=row['parsing_confidence'],
            is_qualified=bool(row['is_qualified']) if row['is_qualified'] is not None else None
        )

    def close(self) -> None:
        """Schließt Datenbankverbindung."""
        self.conn.close()
