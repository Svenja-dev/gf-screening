"""
Unit Tests for retention.py - DSGVO Art. 17 data retention cleanup.

Tests cover: PDF cleanup, export cleanup, debug screenshot cleanup,
full cleanup orchestration, dry-run mode, and CLI argument parsing.
"""

import time
from pathlib import Path

import pytest

from retention import (
    DEFAULT_RETENTION_DAYS,
    cleanup_old_pdfs,
    cleanup_old_exports,
    cleanup_debug_screenshots,
    run_full_cleanup,
)


class TestCleanupOldPdfs:
    """Tests for cleanup_old_pdfs function."""

    def test_deletes_old_pdfs(self, tmp_path):
        """PDFs older than max_age_days are deleted."""
        pdf_dir = tmp_path / "pdfs"
        pdf_dir.mkdir()

        old_file = pdf_dir / "old.pdf"
        old_file.write_bytes(b"%PDF-1.4 old")
        # Set mtime to 100 days ago
        old_mtime = time.time() - (100 * 86400)
        import os
        os.utime(old_file, (old_mtime, old_mtime))

        deleted = cleanup_old_pdfs(pdf_dir, max_age_days=90)
        assert deleted == 1
        assert not old_file.exists()

    def test_keeps_recent_pdfs(self, tmp_path):
        """PDFs newer than max_age_days are kept."""
        pdf_dir = tmp_path / "pdfs"
        pdf_dir.mkdir()

        new_file = pdf_dir / "new.pdf"
        new_file.write_bytes(b"%PDF-1.4 new")

        deleted = cleanup_old_pdfs(pdf_dir, max_age_days=90)
        assert deleted == 0
        assert new_file.exists()

    def test_deletes_tif_files(self, tmp_path):
        """TIF and TIFF files are also cleaned up."""
        pdf_dir = tmp_path / "pdfs"
        pdf_dir.mkdir()

        import os
        old_mtime = time.time() - (100 * 86400)

        for ext in ("tif", "tiff"):
            f = pdf_dir / f"scan.{ext}"
            f.write_bytes(b"TIFF data")
            os.utime(f, (old_mtime, old_mtime))

        deleted = cleanup_old_pdfs(pdf_dir, max_age_days=90)
        assert deleted == 2

    def test_nonexistent_dir_returns_zero(self, tmp_path):
        """Non-existent directory returns 0 without error."""
        deleted = cleanup_old_pdfs(tmp_path / "nonexistent", max_age_days=90)
        assert deleted == 0

    def test_ignores_non_matching_files(self, tmp_path):
        """Files with other extensions are not deleted."""
        pdf_dir = tmp_path / "pdfs"
        pdf_dir.mkdir()

        import os
        old_mtime = time.time() - (100 * 86400)

        txt_file = pdf_dir / "notes.txt"
        txt_file.write_text("not a pdf")
        os.utime(txt_file, (old_mtime, old_mtime))

        deleted = cleanup_old_pdfs(pdf_dir, max_age_days=90)
        assert deleted == 0
        assert txt_file.exists()


class TestCleanupOldExports:
    """Tests for cleanup_old_exports function."""

    def test_deletes_old_csvs(self, tmp_path):
        """CSV files older than max_age_days are deleted."""
        import os
        old_mtime = time.time() - (100 * 86400)

        csv_file = tmp_path / "export.csv"
        csv_file.write_text("header\ndata")
        os.utime(csv_file, (old_mtime, old_mtime))

        deleted = cleanup_old_exports(tmp_path, max_age_days=90)
        assert deleted == 1

    def test_keeps_recent_csvs(self, tmp_path):
        """Recent CSV files are kept."""
        csv_file = tmp_path / "export.csv"
        csv_file.write_text("header\ndata")

        deleted = cleanup_old_exports(tmp_path, max_age_days=90)
        assert deleted == 0
        assert csv_file.exists()

    def test_nonexistent_dir(self, tmp_path):
        """Non-existent directory returns 0."""
        deleted = cleanup_old_exports(tmp_path / "nonexistent")
        assert deleted == 0


class TestCleanupDebugScreenshots:
    """Tests for cleanup_debug_screenshots function."""

    def test_deletes_old_screenshots(self, tmp_path):
        """Debug screenshots older than max_age_hours are deleted."""
        import os
        old_mtime = time.time() - (48 * 3600)  # 48 hours ago

        screenshot = tmp_path / "debug_01_search.png"
        screenshot.write_bytes(b"PNG data")
        os.utime(screenshot, (old_mtime, old_mtime))

        deleted = cleanup_debug_screenshots(tmp_path, max_age_hours=24)
        assert deleted == 1

    def test_keeps_recent_screenshots(self, tmp_path):
        """Recent debug screenshots are kept."""
        screenshot = tmp_path / "debug_01_search.png"
        screenshot.write_bytes(b"PNG data")

        deleted = cleanup_debug_screenshots(tmp_path, max_age_hours=24)
        assert deleted == 0
        assert screenshot.exists()

    def test_only_matches_debug_prefix(self, tmp_path):
        """Only files matching debug_*.png pattern are deleted."""
        import os
        old_mtime = time.time() - (48 * 3600)

        # This should NOT be deleted (wrong prefix)
        other = tmp_path / "screenshot.png"
        other.write_bytes(b"PNG data")
        os.utime(other, (old_mtime, old_mtime))

        deleted = cleanup_debug_screenshots(tmp_path, max_age_hours=24)
        assert deleted == 0
        assert other.exists()

    def test_nonexistent_dir(self, tmp_path):
        """Non-existent directory returns 0."""
        deleted = cleanup_debug_screenshots(tmp_path / "nonexistent")
        assert deleted == 0


class TestRunFullCleanup:
    """Tests for run_full_cleanup orchestration."""

    def test_full_cleanup_creates_result_dict(self, tmp_path):
        """run_full_cleanup returns dict with all categories."""
        results = run_full_cleanup(tmp_path, max_age_days=90)

        assert "pdfs" in results
        assert "exports" in results
        assert "debug" in results

    def test_full_cleanup_deletes_across_dirs(self, tmp_path):
        """run_full_cleanup processes all three directories."""
        import os
        old_mtime = time.time() - (100 * 86400)

        # Create directories
        pdf_dir = tmp_path / "pdfs"
        pdf_dir.mkdir()
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        debug_dir = tmp_path / "debug"
        debug_dir.mkdir()

        # Create old files in each
        pdf = pdf_dir / "old.pdf"
        pdf.write_bytes(b"%PDF old")
        os.utime(pdf, (old_mtime, old_mtime))

        csv = output_dir / "old.csv"
        csv.write_text("data")
        os.utime(csv, (old_mtime, old_mtime))

        debug_mtime = time.time() - (48 * 3600)
        screenshot = debug_dir / "debug_01.png"
        screenshot.write_bytes(b"PNG")
        os.utime(screenshot, (debug_mtime, debug_mtime))

        results = run_full_cleanup(tmp_path, max_age_days=90)

        assert results["pdfs"] == 1
        assert results["exports"] == 1
        assert results["debug"] == 1

    def test_dry_run_does_not_delete(self, tmp_path):
        """Dry run counts but does not delete files."""
        import os
        old_mtime = time.time() - (100 * 86400)

        pdf_dir = tmp_path / "pdfs"
        pdf_dir.mkdir()
        pdf = pdf_dir / "old.pdf"
        pdf.write_bytes(b"%PDF old")
        os.utime(pdf, (old_mtime, old_mtime))

        results = run_full_cleanup(tmp_path, max_age_days=90, dry_run=True)

        assert results["pdfs"] == 1
        assert pdf.exists()  # File still there

    def test_empty_dirs_no_errors(self, tmp_path):
        """Empty directories produce zero counts without errors."""
        (tmp_path / "pdfs").mkdir()
        (tmp_path / "output").mkdir()
        (tmp_path / "debug").mkdir()

        results = run_full_cleanup(tmp_path, max_age_days=90)
        assert sum(results.values()) == 0


class TestDefaultRetentionDays:
    """Tests for the default retention constant."""

    def test_default_is_90_days(self):
        """Default retention period is 90 days."""
        assert DEFAULT_RETENTION_DAYS == 90
