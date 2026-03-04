"""Index page scraper: discovers daily page URLs from year index pages.

URL patterns:
- Year index: https://www.nrc.gov/reading-rm/doc-collections/event-status/event/YYYY/index.html
- Daily page: https://www.nrc.gov/reading-rm/doc-collections/event-status/event/YYYY/YYYYMMDDen
"""

from __future__ import annotations

import re

from bs4 import BeautifulSoup

from nrc_event_scraper.config import Settings


def extract_year_urls(settings: Settings | None = None) -> list[str]:
    """Generate year index URLs for the configured range."""
    s = settings or Settings()
    return [
        f"{s.nrc_base_url}/{year}/index.html" for year in range(s.start_year, s.end_year + 1)
    ]


def extract_daily_page_urls(index_html: str, base_url: str, year: int) -> list[str]:
    """Extract daily page URLs from a year index page HTML.

    Looks for links matching the YYYYMMDDen pattern. Handles two formats:
    - Modern: href="20260303en" or "/path/20260303en"
    - Legacy: href="./20051230en.html" (with .html extension)
    Returns absolute URLs (without .html suffix for consistency).
    """
    soup = BeautifulSoup(index_html, "lxml")
    urls = []
    # Match YYYYMMDDen with optional .html suffix
    pattern = re.compile(rf"^{year}\d{{4}}en(?:\.html)?$")

    for link in soup.find_all("a", href=True):
        href = link["href"]
        # Could be relative or absolute; strip ./ prefix and .html suffix
        filename = href.rstrip("/").split("/")[-1]
        if pattern.match(filename):
            # Normalize: strip .html to get consistent URL format
            canonical = filename.removesuffix(".html")
            if href.startswith("http"):
                urls.append(href.removesuffix(".html"))
            else:
                url = f"{base_url}/{year}/{canonical}"
                urls.append(url)

    return sorted(set(urls))


def url_to_report_date(url: str) -> str | None:
    """Extract YYYY-MM-DD date from a daily page URL like .../20260303en."""
    m = re.search(r"/(\d{4})(\d{2})(\d{2})en", url)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return None
