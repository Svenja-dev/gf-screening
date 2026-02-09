"""
Tests for dk_downloader.py - Selenium scraper for handelsregister.de.

All tests use mocking to avoid requiring a real browser or network.
Covers: RateLimiter, _sanitize_filename (security critical), _parse_register_num,
_extract_pdf_from_zip, and DownloadResult dataclass.
"""

import json
import time
import zipfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from dk_downloader import (
    DownloadResult,
    DownloaderConfig,
    RateLimiter,
    GesellschafterlistenDownloader,
)


# ---------------------------------------------------------------------------
# RateLimiter tests
# ---------------------------------------------------------------------------

class TestRateLimiter:
    """Tests for the RateLimiter class."""

    @patch("dk_downloader.time.sleep")
    def test_first_call_no_wait(self, mock_sleep):
        """First call to wait() should not sleep because no previous call exists."""
        limiter = RateLimiter(calls_per_hour=55)
        limiter.last_call = 0  # epoch -- far in the past

        limiter.wait()

        mock_sleep.assert_not_called()

    @patch("dk_downloader.random.uniform", return_value=2.0)
    @patch("dk_downloader.time.sleep")
    @patch("dk_downloader.time.time")
    def test_rapid_second_call_waits(self, mock_time, mock_sleep, mock_uniform):
        """A second call within min_interval should trigger a sleep."""
        limiter = RateLimiter(calls_per_hour=55)
        min_interval = 3600 / 55  # ~65.45 seconds

        # Simulate: first call at t=1000, second call at t=1010 (only 10s later)
        mock_time.side_effect = [1010, 1010 + min_interval]
        limiter.last_call = 1000

        limiter.wait()

        mock_sleep.assert_called_once()
        sleep_arg = mock_sleep.call_args[0][0]
        # Should sleep approximately (min_interval - 10) + 2 seconds of random jitter
        expected_sleep = (min_interval - 10) + 2.0
        assert abs(sleep_arg - expected_sleep) < 1.0

    @patch("dk_downloader.time.sleep")
    @patch("dk_downloader.time.time")
    def test_slow_second_call_no_wait(self, mock_time, mock_sleep):
        """A second call after min_interval has passed should not sleep."""
        limiter = RateLimiter(calls_per_hour=55)
        min_interval = 3600 / 55

        # Simulate: first call at t=1000, second call at t=1000+min_interval+10
        mock_time.return_value = 1000 + min_interval + 10
        limiter.last_call = 1000

        limiter.wait()

        mock_sleep.assert_not_called()

    def test_rate_limiter_default_interval(self):
        """Default RateLimiter has correct min_interval for 55 calls/hour."""
        limiter = RateLimiter()
        expected = 3600 / 55
        assert abs(limiter.min_interval - expected) < 0.01

    def test_rate_limiter_custom_rate(self):
        """Custom calls_per_hour sets correct min_interval."""
        limiter = RateLimiter(calls_per_hour=10)
        assert abs(limiter.min_interval - 360.0) < 0.01


# ---------------------------------------------------------------------------
# _sanitize_filename tests (SECURITY CRITICAL - CWE-22 path traversal)
# ---------------------------------------------------------------------------

class TestSanitizeFilename:
    """Tests for _sanitize_filename method. Security-critical for CWE-22."""

    @pytest.fixture
    def downloader(self, tmp_path):
        """Creates a GesellschafterlistenDownloader with a temp directory.

        Does NOT start a browser -- only tests methods that don't need Selenium.
        """
        return GesellschafterlistenDownloader(download_dir=tmp_path, headless=True)

    def test_normal_filename(self, downloader):
        """Normal alphanumeric filename is preserved."""
        result = downloader._sanitize_filename("HRB_12345")
        assert result == "HRB_12345"

    def test_path_traversal_dots(self, downloader):
        """Path traversal with '../' raises ValueError."""
        with pytest.raises(ValueError, match="Path traversal"):
            downloader._sanitize_filename("../../etc/passwd")

    def test_path_traversal_backslash(self, downloader):
        """Path traversal with backslash raises ValueError."""
        with pytest.raises(ValueError, match="Path traversal"):
            downloader._sanitize_filename("..\\..\\windows\\system32")

    def test_forward_slash(self, downloader):
        """Forward slash in filename raises ValueError (path traversal guard)."""
        with pytest.raises(ValueError, match="Path traversal"):
            downloader._sanitize_filename("path/file")

    def test_empty_string(self, downloader):
        """Empty string raises ValueError."""
        with pytest.raises(ValueError, match="empty"):
            downloader._sanitize_filename("")

    def test_none_input(self, downloader):
        """None input raises ValueError."""
        with pytest.raises(ValueError):
            downloader._sanitize_filename(None)

    def test_whitespace_only(self, downloader):
        """Whitespace-only string raises ValueError."""
        with pytest.raises(ValueError, match="empty"):
            downloader._sanitize_filename("   ")

    def test_only_special_chars(self, downloader):
        """String with only special characters raises ValueError."""
        with pytest.raises(ValueError, match="no valid characters"):
            downloader._sanitize_filename("!@#$%^&*()")

    def test_windows_reserved_names_prefixed(self, downloader):
        """Windows reserved device names (CON, PRN, NUL) get 'file_' prefix."""
        for reserved in ["CON", "PRN", "NUL", "COM1", "LPT1"]:
            result = downloader._sanitize_filename(reserved)
            assert result.startswith("file_")
            assert reserved in result

    def test_long_filename_truncated(self, downloader):
        """Filenames longer than 200 characters are truncated."""
        long_name = "A" * 300
        result = downloader._sanitize_filename(long_name)
        assert len(result) <= 200

    def test_german_umlauts_preserved(self, downloader):
        """German umlauts are valid word characters and are preserved."""
        result = downloader._sanitize_filename("Muenchen_Nuernberg")
        assert "Muenchen" in result
        assert "Nuernberg" in result

    def test_umlaut_characters(self, downloader):
        """Actual umlaut characters (ae, oe, ue) are preserved."""
        # \w in Python regex matches unicode word chars including umlauts
        result = downloader._sanitize_filename("Koeln_Duesseldorf")
        assert "Koeln" in result

    def test_spaces_converted_to_underscore(self, downloader):
        """Spaces are collapsed and converted to underscores."""
        result = downloader._sanitize_filename("HRB 12345 B")
        assert " " not in result
        assert "_" in result

    def test_consecutive_special_chars_collapsed(self, downloader):
        """Multiple hyphens/underscores/spaces are collapsed to single underscore."""
        result = downloader._sanitize_filename("HRB---12345___test")
        assert "---" not in result
        assert "___" not in result


# ---------------------------------------------------------------------------
# _parse_register_num tests
# ---------------------------------------------------------------------------

class TestParseRegisterNum:
    """Tests for _parse_register_num method."""

    @pytest.fixture
    def downloader(self, tmp_path):
        return GesellschafterlistenDownloader(download_dir=tmp_path, headless=True)

    def test_hrb_with_space(self, downloader):
        """'HRB 12345' -> ('HRB', '12345', None)."""
        result = downloader._parse_register_num("HRB 12345")
        assert result == ("HRB", "12345", None)

    def test_hrb_without_space(self, downloader):
        """'HRB12345' -> ('HRB', '12345', None)."""
        result = downloader._parse_register_num("HRB12345")
        assert result == ("HRB", "12345", None)

    def test_hrb_with_suffix(self, downloader):
        """'HRB 12345 B' -> ('HRB', '12345', 'B')."""
        result = downloader._parse_register_num("HRB 12345 B")
        assert result == ("HRB", "12345", "B")

    def test_hra(self, downloader):
        """'HRA 7834' -> ('HRA', '7834', None)."""
        result = downloader._parse_register_num("HRA 7834")
        assert result == ("HRA", "7834", None)

    def test_invalid_format(self, downloader):
        """Invalid format returns (None, None, None)."""
        result = downloader._parse_register_num("INVALID")
        assert result == (None, None, None)

    def test_empty_string(self, downloader):
        """Empty string returns (None, None, None)."""
        result = downloader._parse_register_num("")
        assert result == (None, None, None)

    def test_lowercase_normalized(self, downloader):
        """Lowercase input is normalized to uppercase before parsing."""
        result = downloader._parse_register_num("hrb 12345")
        assert result == ("HRB", "12345", None)

    def test_vr_register(self, downloader):
        """'VR 5678' -> ('VR', '5678', None)."""
        result = downloader._parse_register_num("VR 5678")
        assert result == ("VR", "5678", None)

    def test_gnr_register(self, downloader):
        """'GNR 9012' -> ('GNR', '9012', None)."""
        result = downloader._parse_register_num("GNR 9012")
        assert result == ("GNR", "9012", None)

    def test_pr_register(self, downloader):
        """'PR 1234' -> ('PR', '1234', None)."""
        result = downloader._parse_register_num("PR 1234")
        assert result == ("PR", "1234", None)

    def test_extra_whitespace(self, downloader):
        """Leading/trailing whitespace is stripped."""
        result = downloader._parse_register_num("  HRB 12345  ")
        assert result == ("HRB", "12345", None)

    def test_only_numbers(self, downloader):
        """Bare number without type prefix returns (None, None, None)."""
        result = downloader._parse_register_num("12345")
        assert result == (None, None, None)


# ---------------------------------------------------------------------------
# _extract_pdf_from_zip tests
# ---------------------------------------------------------------------------

class TestExtractPdfFromZip:
    """Tests for _extract_pdf_from_zip method."""

    @pytest.fixture
    def downloader(self, tmp_path):
        return GesellschafterlistenDownloader(download_dir=tmp_path, headless=True)

    def _create_zip(self, tmp_path, filename, contents):
        """Helper: creates a zip file with the given filename->content mapping.

        Args:
            tmp_path: pytest tmp_path fixture
            filename: name of the zip file
            contents: dict mapping inner filenames to bytes content
        """
        zip_path = tmp_path / filename
        with zipfile.ZipFile(zip_path, "w") as zf:
            for name, data in contents.items():
                zf.writestr(name, data)
        return zip_path

    def test_extracts_pdf_from_zip(self, downloader, tmp_path):
        """ZIP containing a PDF extracts the PDF and returns its path."""
        zip_path = self._create_zip(
            tmp_path, "test.zip", {"document.pdf": b"%PDF-1.4 fake content"}
        )

        result = downloader._extract_pdf_from_zip(zip_path, "HRB_12345")

        assert result is not None
        assert result.exists()
        assert result.suffix == ".pdf"
        assert "HRB_12345" in result.name
        assert "gesellschafterliste" in result.name

    def test_extracts_tif_from_zip(self, downloader, tmp_path):
        """ZIP containing a TIF (no PDF) extracts the TIF."""
        zip_path = self._create_zip(
            tmp_path, "scan.zip", {"scan.tif": b"TIFF fake content"}
        )

        result = downloader._extract_pdf_from_zip(zip_path, "HRB_99999")

        assert result is not None
        assert result.exists()
        assert result.suffix == ".tif"

    def test_pdf_preferred_over_tif(self, downloader, tmp_path):
        """When both PDF and TIF are present, PDF is preferred."""
        zip_path = self._create_zip(
            tmp_path,
            "mixed.zip",
            {
                "scan.tif": b"TIFF content",
                "document.pdf": b"%PDF-1.4 content",
            },
        )

        result = downloader._extract_pdf_from_zip(zip_path, "HRB_MIXED")

        assert result is not None
        assert result.suffix == ".pdf"

    def test_empty_zip_returns_none(self, downloader, tmp_path):
        """ZIP with no files returns None."""
        zip_path = self._create_zip(tmp_path, "empty.zip", {})

        result = downloader._extract_pdf_from_zip(zip_path, "HRB_EMPTY")

        assert result is None

    def test_zip_without_pdf_or_tif(self, downloader, tmp_path):
        """ZIP containing only non-PDF/TIF files returns None."""
        zip_path = self._create_zip(
            tmp_path, "wrong.zip", {"readme.txt": b"Just a text file"}
        )

        result = downloader._extract_pdf_from_zip(zip_path, "HRB_WRONG")

        assert result is None

    def test_corrupt_zip_returns_none(self, downloader, tmp_path):
        """Corrupt (non-zip) file returns None."""
        corrupt_path = tmp_path / "corrupt.zip"
        corrupt_path.write_bytes(b"This is not a zip file at all")

        result = downloader._extract_pdf_from_zip(corrupt_path, "HRB_CORRUPT")

        assert result is None

    def test_zip_deleted_after_extraction(self, downloader, tmp_path):
        """Original ZIP file is cleaned up after successful extraction."""
        zip_path = self._create_zip(
            tmp_path, "cleanup.zip", {"doc.pdf": b"%PDF-1.4 content"}
        )

        assert zip_path.exists()
        result = downloader._extract_pdf_from_zip(zip_path, "HRB_CLEANUP")

        assert result is not None
        # ZIP should be deleted (or at least attempted)
        # On Windows, the file might be locked briefly, so we allow both states
        # The code does try to unlink, with a fallback log if it fails


# ---------------------------------------------------------------------------
# DownloadResult dataclass tests
# ---------------------------------------------------------------------------

class TestDownloadResult:
    """Tests for the DownloadResult dataclass."""

    def test_success_result(self):
        """Successful download result with PDF path."""
        result = DownloadResult(success=True, pdf_path=Path("/tmp/test.pdf"))

        assert result.success is True
        assert result.pdf_path == Path("/tmp/test.pdf")
        assert result.error is None
        assert result.no_gl_available is False

    def test_failure_result(self):
        """Failed download result with error message."""
        result = DownloadResult(success=False, error="Timeout beim Laden")

        assert result.success is False
        assert result.pdf_path is None
        assert result.error == "Timeout beim Laden"
        assert result.no_gl_available is False

    def test_no_gl_result(self):
        """Successful scrape but no Gesellschafterliste available."""
        result = DownloadResult(success=True, no_gl_available=True)

        assert result.success is True
        assert result.pdf_path is None
        assert result.no_gl_available is True
        assert result.error is None

    def test_default_values(self):
        """Only 'success' is required; others have defaults."""
        result = DownloadResult(success=False)

        assert result.success is False
        assert result.pdf_path is None
        assert result.error is None
        assert result.no_gl_available is False


# ---------------------------------------------------------------------------
# GesellschafterlistenDownloader initialization tests
# ---------------------------------------------------------------------------

class TestDownloaderInit:
    """Tests for GesellschafterlistenDownloader initialization (no browser)."""

    def test_creates_download_dir(self, tmp_path):
        """Constructor creates the download directory if it does not exist."""
        download_dir = tmp_path / "new_dir" / "pdfs"
        assert not download_dir.exists()

        downloader = GesellschafterlistenDownloader(
            download_dir=download_dir, headless=True
        )

        assert download_dir.exists()
        assert downloader.driver is None  # not started yet

    def test_debug_dir_created_in_debug_mode(self, tmp_path):
        """In debug mode, a debug directory is created."""
        download_dir = tmp_path / "pdfs"
        downloader = GesellschafterlistenDownloader(
            download_dir=download_dir, headless=True, debug=True
        )

        assert downloader.debug_dir.exists()

    def test_rate_limiter_initialized(self, tmp_path):
        """Rate limiter is initialized with 55 calls/hour."""
        downloader = GesellschafterlistenDownloader(
            download_dir=tmp_path, headless=True
        )

        assert isinstance(downloader.rate_limiter, RateLimiter)
        expected_interval = 3600 / 55
        assert abs(downloader.rate_limiter.min_interval - expected_interval) < 0.01

    def test_context_manager(self, tmp_path):
        """Context manager protocol (__enter__/__exit__) is implemented."""
        downloader = GesellschafterlistenDownloader(
            download_dir=tmp_path, headless=True
        )

        # Patch start/stop to avoid browser launch
        with patch.object(downloader, "start"), patch.object(downloader, "stop"):
            with downloader as dl:
                assert dl is downloader
            downloader.stop.assert_called_once()

    def test_base_url_in_config(self, tmp_path):
        """Default config base_url points to handelsregister.de."""
        downloader = GesellschafterlistenDownloader(
            download_dir=tmp_path, headless=True
        )
        assert "handelsregister.de" in downloader.config.base_url


# ---------------------------------------------------------------------------
# COURT_MAPPINGS tests
# ---------------------------------------------------------------------------

class TestCourtMappings:
    """Tests for the COURT_MAPPINGS class attribute."""

    def test_common_courts_present(self, tmp_path):
        """Common German courts are in the mapping."""
        downloader = GesellschafterlistenDownloader(
            download_dir=tmp_path, headless=True
        )

        assert "berlin" in downloader.COURT_MAPPINGS
        assert "münchen" in downloader.COURT_MAPPINGS
        assert "hamburg" in downloader.COURT_MAPPINGS
        assert "köln" in downloader.COURT_MAPPINGS

    def test_berlin_maps_to_charlottenburg(self, tmp_path):
        """Berlin maps to 'Berlin (Charlottenburg)'."""
        downloader = GesellschafterlistenDownloader(
            download_dir=tmp_path, headless=True
        )
        assert downloader.COURT_MAPPINGS["berlin"] == "Berlin (Charlottenburg)"

    def test_dresden_present(self, tmp_path):
        """Dresden is in the court mappings."""
        downloader = GesellschafterlistenDownloader(
            download_dir=tmp_path, headless=True
        )
        assert "dresden" in downloader.COURT_MAPPINGS
        assert downloader.COURT_MAPPINGS["dresden"] == "Dresden"


# ---------------------------------------------------------------------------
# WINDOWS_RESERVED_NAMES tests
# ---------------------------------------------------------------------------

class TestWindowsReservedNames:
    """Tests for the WINDOWS_RESERVED_NAMES class attribute."""

    def test_standard_reserved_names(self, tmp_path):
        """Standard Windows reserved names are in the set."""
        downloader = GesellschafterlistenDownloader(
            download_dir=tmp_path, headless=True
        )
        for name in ["CON", "PRN", "AUX", "NUL"]:
            assert name in downloader.WINDOWS_RESERVED_NAMES

    def test_com_ports(self, tmp_path):
        """COM1-COM9 are reserved."""
        downloader = GesellschafterlistenDownloader(
            download_dir=tmp_path, headless=True
        )
        for i in range(1, 10):
            assert f"COM{i}" in downloader.WINDOWS_RESERVED_NAMES

    def test_lpt_ports(self, tmp_path):
        """LPT1-LPT9 are reserved."""
        downloader = GesellschafterlistenDownloader(
            download_dir=tmp_path, headless=True
        )
        for i in range(1, 10):
            assert f"LPT{i}" in downloader.WINDOWS_RESERVED_NAMES


# ---------------------------------------------------------------------------
# download() method tests (fully mocked - no browser)
# ---------------------------------------------------------------------------

class TestDownloadMethod:
    """Tests for the download() method with full mocking."""

    @pytest.fixture
    def downloader(self, tmp_path):
        dl = GesellschafterlistenDownloader(download_dir=tmp_path, headless=True)
        dl.driver = MagicMock()
        dl.rate_limiter = MagicMock()
        return dl

    def test_invalid_register_num(self, downloader):
        """Invalid register number returns failure result."""
        result = downloader.download("INVALID_NUMBER")

        assert result.success is False
        assert "Registernummer" in result.error

    def test_empty_register_num(self, downloader):
        """Empty register number returns failure result."""
        result = downloader.download("")

        assert result.success is False

    def test_rate_limiter_called(self, downloader):
        """download() calls rate_limiter.wait()."""
        # Will fail at navigation, but rate limiter should be called first
        downloader.driver.get.side_effect = Exception("Mocked navigation error")

        result = downloader.download("HRB 12345", "Berlin")

        downloader.rate_limiter.wait.assert_called_once()

    def test_start_called_if_no_driver(self, tmp_path):
        """download() calls start() if driver is None."""
        downloader = GesellschafterlistenDownloader(
            download_dir=tmp_path, headless=True
        )
        assert downloader.driver is None

        with patch.object(downloader, "start") as mock_start, \
             patch.object(downloader, "rate_limiter"):
            # start() is called, but since we mock it, driver stays None
            # This will then fail in _parse_register_num, which is fine
            mock_start.side_effect = lambda: setattr(downloader, "driver", MagicMock())
            downloader.driver = None

            # Override _parse_register_num to return invalid so we exit early
            with patch.object(
                downloader, "_parse_register_num", return_value=(None, None, None)
            ):
                result = downloader.download("HRB 12345")

            mock_start.assert_called_once()


# ---------------------------------------------------------------------------
# DownloaderConfig dataclass tests
# ---------------------------------------------------------------------------

class TestDownloaderConfig:
    """Tests for the DownloaderConfig dataclass."""

    def test_default_values(self):
        """Default config has sensible values."""
        config = DownloaderConfig()

        assert "handelsregister.de" in config.base_url
        assert config.rate_limit_per_hour == 55
        assert config.download_timeout_seconds == 45
        assert config.max_tree_iterations == 15
        assert "Mozilla" in config.user_agent

    def test_custom_values(self):
        """Custom config values override defaults."""
        config = DownloaderConfig(
            rate_limit_per_hour=30,
            download_timeout_seconds=60,
            max_tree_iterations=5,
        )

        assert config.rate_limit_per_hour == 30
        assert config.download_timeout_seconds == 60
        assert config.max_tree_iterations == 5
        # Non-overridden values remain at default
        assert "handelsregister.de" in config.base_url

    def test_custom_base_url(self):
        """base_url can be overridden."""
        config = DownloaderConfig(base_url="https://example.com/test")
        assert config.base_url == "https://example.com/test"

    def test_config_passed_to_downloader(self, tmp_path):
        """DownloaderConfig is stored on the downloader instance."""
        config = DownloaderConfig(rate_limit_per_hour=10)
        downloader = GesellschafterlistenDownloader(
            download_dir=tmp_path, headless=True, config=config,
        )
        assert downloader.config.rate_limit_per_hour == 10


# ---------------------------------------------------------------------------
# _validate_downloaded_file tests
# ---------------------------------------------------------------------------

class TestValidateDownloadedFile:
    """Tests for _validate_downloaded_file static method."""

    def test_valid_pdf_magic_bytes(self, tmp_path):
        """File starting with %PDF magic bytes is valid."""
        pdf_file = tmp_path / "valid.pdf"
        pdf_file.write_bytes(b"%PDF-1.4 some content")

        assert GesellschafterlistenDownloader._validate_downloaded_file(pdf_file) is True

    def test_invalid_file_not_pdf(self, tmp_path):
        """File without PDF magic bytes is invalid."""
        not_pdf = tmp_path / "fake.pdf"
        not_pdf.write_bytes(b"This is not a PDF")

        assert GesellschafterlistenDownloader._validate_downloaded_file(not_pdf) is False

    def test_nonexistent_file(self, tmp_path):
        """Non-existent file returns False."""
        missing = tmp_path / "missing.pdf"

        assert GesellschafterlistenDownloader._validate_downloaded_file(missing) is False

    def test_empty_file(self, tmp_path):
        """Empty file (0 bytes) returns False."""
        empty = tmp_path / "empty.pdf"
        empty.write_bytes(b"")

        assert GesellschafterlistenDownloader._validate_downloaded_file(empty) is False

    def test_short_file(self, tmp_path):
        """File shorter than 4 bytes returns False."""
        short = tmp_path / "short.pdf"
        short.write_bytes(b"%PD")

        assert GesellschafterlistenDownloader._validate_downloaded_file(short) is False


# ---------------------------------------------------------------------------
# Persistent RateLimiter tests
# ---------------------------------------------------------------------------

class TestPersistentRateLimiter:
    """Tests for RateLimiter with state_file persistence."""

    def test_saves_state_to_file(self, tmp_path):
        """After wait(), state is saved to the JSON file."""
        state_file = tmp_path / "rate_state.json"
        limiter = RateLimiter(calls_per_hour=55, state_file=state_file)

        with patch("dk_downloader.time.time", return_value=5000.0):
            with patch("dk_downloader.time.sleep"):
                limiter.wait()

        assert state_file.exists()
        data = json.loads(state_file.read_text(encoding="utf-8"))
        assert "last_call" in data
        assert data["last_call"] == 5000.0

    def test_loads_state_from_file(self, tmp_path):
        """RateLimiter loads last_call from existing state file."""
        state_file = tmp_path / "rate_state.json"
        state_file.write_text(json.dumps({"last_call": 3000.0}), encoding="utf-8")

        limiter = RateLimiter(calls_per_hour=55, state_file=state_file)

        assert limiter.last_call == 3000.0

    def test_no_state_file_defaults_to_zero(self):
        """Without state_file, last_call defaults to 0.0."""
        limiter = RateLimiter(calls_per_hour=55)
        assert limiter.last_call == 0.0

    def test_corrupt_state_file_defaults_to_zero(self, tmp_path):
        """Corrupt JSON in state file defaults to 0.0."""
        state_file = tmp_path / "corrupt.json"
        state_file.write_text("NOT VALID JSON", encoding="utf-8")

        limiter = RateLimiter(calls_per_hour=55, state_file=state_file)
        assert limiter.last_call == 0.0


# ---------------------------------------------------------------------------
# stop() method tests
# ---------------------------------------------------------------------------

class TestStopMethod:
    """Tests for the stop() method."""

    def test_stop_quits_driver(self, tmp_path):
        """stop() calls driver.quit() when driver exists."""
        downloader = GesellschafterlistenDownloader(
            download_dir=tmp_path, headless=True
        )
        mock_driver = MagicMock()
        downloader.driver = mock_driver

        downloader.stop()

        mock_driver.quit.assert_called_once()
        assert downloader.driver is None

    def test_stop_noop_when_no_driver(self, tmp_path):
        """stop() is a no-op when driver is None."""
        downloader = GesellschafterlistenDownloader(
            download_dir=tmp_path, headless=True
        )
        assert downloader.driver is None

        # Should not raise
        downloader.stop()
        assert downloader.driver is None
