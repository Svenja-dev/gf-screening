"""
Data retention policy for GF-Screening (DSGVO Art. 17 compliance).

Gesellschafterlisten contain personal data (names, birth dates) of
company shareholders. This module provides automatic cleanup of
stale data after a configurable retention period.

Usage:
    python -m src.retention --max-age 90
    python -m src.retention --delete-all
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

DEFAULT_RETENTION_DAYS = 90
"""Default retention period in days. After this period, PDFs and
database entries are eligible for deletion."""


def cleanup_old_pdfs(pdf_dir: Path, max_age_days: int = DEFAULT_RETENTION_DAYS) -> int:
    """Deletes PDF and TIF files older than max_age_days.

    Args:
        pdf_dir: Directory containing downloaded PDFs/TIFs.
        max_age_days: Maximum age in days before deletion.

    Returns:
        Number of files deleted.
    """
    if not pdf_dir.exists():
        logger.info(f"PDF-Verzeichnis existiert nicht: {pdf_dir}")
        return 0

    cutoff = time.time() - (max_age_days * 86400)
    deleted = 0

    for pattern in ("*.pdf", "*.tif", "*.tiff"):
        for file_path in pdf_dir.glob(pattern):
            try:
                if file_path.stat().st_mtime < cutoff:
                    file_path.unlink()
                    deleted += 1
                    logger.info(f"Geloescht (>{max_age_days}d alt): {file_path.name}")
            except OSError as e:
                logger.warning(f"Konnte Datei nicht loeschen {file_path.name}: {e}")

    return deleted


def cleanup_old_exports(output_dir: Path, max_age_days: int = DEFAULT_RETENTION_DAYS) -> int:
    """Deletes exported CSV files older than max_age_days.

    Args:
        output_dir: Directory containing exported CSVs.
        max_age_days: Maximum age in days before deletion.

    Returns:
        Number of files deleted.
    """
    if not output_dir.exists():
        return 0

    cutoff = time.time() - (max_age_days * 86400)
    deleted = 0

    for csv_file in output_dir.glob("*.csv"):
        try:
            if csv_file.stat().st_mtime < cutoff:
                csv_file.unlink()
                deleted += 1
                logger.info(f"Export geloescht (>{max_age_days}d alt): {csv_file.name}")
        except OSError as e:
            logger.warning(f"Konnte Export nicht loeschen {csv_file.name}: {e}")

    return deleted


def cleanup_debug_screenshots(debug_dir: Path, max_age_hours: int = 24) -> int:
    """Deletes debug screenshots older than max_age_hours.

    Debug screenshots may contain sensitive company data visible
    in the browser.

    Args:
        debug_dir: Directory containing debug screenshots.
        max_age_hours: Maximum age in hours before deletion.

    Returns:
        Number of files deleted.
    """
    if not debug_dir.exists():
        return 0

    cutoff = time.time() - (max_age_hours * 3600)
    deleted = 0

    for screenshot in debug_dir.glob("debug_*.png"):
        try:
            if screenshot.stat().st_mtime < cutoff:
                screenshot.unlink()
                deleted += 1
        except OSError:
            pass

    if deleted:
        logger.info(f"{deleted} Debug-Screenshots geloescht (>{max_age_hours}h alt)")

    return deleted


def run_full_cleanup(
    base_dir: Path,
    max_age_days: int = DEFAULT_RETENTION_DAYS,
    dry_run: bool = False,
) -> dict:
    """Runs complete data retention cleanup.

    Args:
        base_dir: Project root directory.
        max_age_days: Retention period for PDFs and exports.
        dry_run: If True, only report what would be deleted.

    Returns:
        Dictionary with deletion counts per category.
    """
    pdf_dir = base_dir / "pdfs"
    output_dir = base_dir / "output"
    debug_dir = base_dir / "debug"

    if dry_run:
        logger.info(f"[DRY RUN] Wuerde Dateien aelter als {max_age_days} Tage loeschen")
        # Count without deleting
        results = {"pdfs": 0, "exports": 0, "debug": 0}
        cutoff_pdf = time.time() - (max_age_days * 86400)
        cutoff_debug = time.time() - (24 * 3600)

        if pdf_dir.exists():
            for p in ("*.pdf", "*.tif", "*.tiff"):
                results["pdfs"] += sum(
                    1 for f in pdf_dir.glob(p) if f.stat().st_mtime < cutoff_pdf
                )
        if output_dir.exists():
            results["exports"] = sum(
                1 for f in output_dir.glob("*.csv") if f.stat().st_mtime < cutoff_pdf
            )
        if debug_dir.exists():
            results["debug"] = sum(
                1 for f in debug_dir.glob("debug_*.png")
                if f.stat().st_mtime < cutoff_debug
            )

        logger.info(f"[DRY RUN] Wuerde loeschen: {results}")
        return results

    results = {
        "pdfs": cleanup_old_pdfs(pdf_dir, max_age_days),
        "exports": cleanup_old_exports(output_dir, max_age_days),
        "debug": cleanup_debug_screenshots(debug_dir, max_age_hours=24),
    }

    total = sum(results.values())
    logger.info(
        f"Retention-Cleanup abgeschlossen: {total} Dateien geloescht "
        f"(PDFs: {results['pdfs']}, Exports: {results['exports']}, "
        f"Debug: {results['debug']})"
    )

    return results


if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    arg_parser = argparse.ArgumentParser(
        description="GF-Screening Data Retention - DSGVO Art. 17 Compliance"
    )
    arg_parser.add_argument(
        "--max-age",
        type=int,
        default=DEFAULT_RETENTION_DAYS,
        help=f"Max age in days (default: {DEFAULT_RETENTION_DAYS})",
    )
    arg_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only show what would be deleted",
    )
    arg_parser.add_argument(
        "--delete-all",
        action="store_true",
        help="Delete ALL data files (max-age=0)",
    )

    args = arg_parser.parse_args()

    base_dir = Path(__file__).parent.parent
    max_age = 0 if args.delete_all else args.max_age

    results = run_full_cleanup(base_dir, max_age, dry_run=args.dry_run)

    print(f"\nRetention-Cleanup ({'DRY RUN' if args.dry_run else 'AUSGEFUEHRT'}):")
    print(f"  PDFs geloescht:    {results['pdfs']}")
    print(f"  Exports geloescht: {results['exports']}")
    print(f"  Debug geloescht:   {results['debug']}")
