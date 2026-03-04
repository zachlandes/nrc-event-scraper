"""Tests for index page scraper."""

from nrc_event_scraper.config import Settings
from nrc_event_scraper.scraper.index_scraper import (
    extract_daily_page_urls,
    extract_year_urls,
    url_to_report_date,
)


def test_extract_year_urls():
    settings = Settings(start_year=2024, end_year=2026)
    urls = extract_year_urls(settings)
    assert len(urls) == 3
    assert "2024/index.html" in urls[0]
    assert "2026/index.html" in urls[2]


def test_extract_daily_page_urls():
    index_html = """
    <html><body>
    <a href="20260101en">January 01</a>
    <a href="20260102en">January 02</a>
    <a href="20260103en">January 03</a>
    <a href="index.html">Index</a>
    </body></html>
    """
    base = "https://www.nrc.gov/reading-rm/doc-collections/event-status/event"
    urls = extract_daily_page_urls(index_html, base, 2026)

    assert len(urls) == 3
    assert urls[0].endswith("20260101en")
    assert urls[0].startswith("https://")


def test_extract_daily_page_urls_absolute():
    """Handle absolute URLs in the index page."""
    base_url = "https://www.nrc.gov/reading-rm/doc-collections/event-status/event"
    index_html = f"""
    <html><body>
    <a href="{base_url}/2026/20260101en">Jan 01</a>
    </body></html>
    """
    urls = extract_daily_page_urls(index_html, base_url, 2026)
    assert len(urls) == 1


def test_extract_daily_page_urls_filters_non_event_links():
    index_html = """
    <html><body>
    <a href="20260101en">Event</a>
    <a href="index.html">Index</a>
    <a href="../">Parent</a>
    <a href="20250101en">Wrong year</a>
    </body></html>
    """
    base = "https://example.com"
    urls = extract_daily_page_urls(index_html, base, 2026)
    assert len(urls) == 1


def test_extract_daily_page_urls_legacy_html_suffix():
    """Legacy year indexes use ./YYYYMMDDen.html links."""
    index_html = """
    <html><body>
    <a href="./20051230en.html">December 30</a>
    <a href="./20051229en.html">December 29</a>
    <a href="./20051228en.html">December 28</a>
    </body></html>
    """
    base = "https://www.nrc.gov/reading-rm/doc-collections/event-status/event"
    urls = extract_daily_page_urls(index_html, base, 2005)

    assert len(urls) == 3
    # URLs should be normalized without .html
    assert urls[0].endswith("20051228en")
    assert ".html" not in urls[0]
    assert urls[0].startswith("https://")


def test_url_to_report_date():
    assert url_to_report_date("https://example.com/2026/20260303en") == "2026-03-03"
    assert url_to_report_date("https://example.com/2019/20190301en") == "2019-03-01"
    assert url_to_report_date("https://example.com/index.html") is None
