"""
GF-Screening - Gesellschafterstruktur-Analyse für GmbHs

Module:
- models: Datenmodelle und SQLite-Datenbank
- dk_downloader: Selenium-Scraper für handelsregister.de
- pdf_parser: PDF-Parser für Gesellschafterlisten
- pipeline: Hauptorchiestrierung
"""

from .models import Database, Company, Shareholder
from .pdf_parser import GesellschafterlisteParser
from .dk_downloader import GesellschafterlistenDownloader
from .pipeline import GFScreeningPipeline

__version__ = "1.0.0"
__all__ = [
    "Database",
    "Company",
    "Shareholder",
    "GesellschafterlisteParser",
    "GesellschafterlistenDownloader",
    "GFScreeningPipeline",
]
