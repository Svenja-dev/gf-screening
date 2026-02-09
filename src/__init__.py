"""
GF-Screening - Gesellschafterstruktur-Analyse fuer GmbHs.

Module:
- models: Datenmodelle und SQLite-Datenbank
- dk_downloader: Selenium-Scraper fuer handelsregister.de
- pdf_parser: PDF-Parser fuer Gesellschafterlisten
- pipeline: Hauptorchiestrierung
- retention: DSGVO-konforme Datenbereinigung (Art. 17)
"""

from .models import Database, Company, Shareholder
from .pdf_parser import GesellschafterlisteParser, ParsingResult
from .dk_downloader import (
    GesellschafterlistenDownloader,
    DownloaderConfig,
    DownloadResult,
    RateLimiter,
)
from .pipeline import GFScreeningPipeline
from .retention import run_full_cleanup

__version__ = "1.1.0"
__all__ = [
    "Database",
    "Company",
    "Shareholder",
    "GesellschafterlisteParser",
    "ParsingResult",
    "GesellschafterlistenDownloader",
    "DownloaderConfig",
    "DownloadResult",
    "RateLimiter",
    "GFScreeningPipeline",
    "run_full_cleanup",
]
