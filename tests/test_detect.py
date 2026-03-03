"""Tests for format detection."""

from pathlib import Path

from nrc_event_scraper.parser.detect import detect_format


def test_detect_modern_format(fixtures_dir: Path):
    html = (fixtures_dir / "modern_multi_event.html").read_text()
    assert detect_format(html) == "modern"


def test_detect_legacy_format(fixtures_dir: Path):
    html = (fixtures_dir / "legacy_multi_event.html").read_text()
    assert detect_format(html) == "legacy"


def test_detect_empty_format(fixtures_dir: Path):
    html = (fixtures_dir / "modern_no_events.html").read_text()
    assert detect_format(html) == "empty"


def test_detect_unknown_format():
    assert detect_format("<html><body><p>Random page</p></body></html>") == "empty"


def test_detect_unknown_with_content():
    html = "<html><body>" + "x" * 300 + "</body></html>"
    assert detect_format(html) == "unknown"
