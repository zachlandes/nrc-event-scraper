"""Format detection: route HTML to the correct parser.

Returns "modern" | "legacy" | "empty" | "unknown" based on DOM structure.
"""

from __future__ import annotations

from bs4 import BeautifulSoup


def detect_format(html: str) -> str:
    """Detect which NRC page format is present.

    - "modern": Drupal 10 with <div class="grid border" id="enXXXXX"> containers (Nov 2020+)
    - "legacy": Table-based with <a name="enXXXXX"> anchors (1999–Oct 2020)
    - "empty": Page with "No events found"
    - "unknown": Unrecognized structure
    """
    soup = BeautifulSoup(html, "lxml")

    # Check for no-events pages first (check ALL <strong> tags — the first one
    # is often the USWDS gov banner "Official websites use .gov")
    for strong in soup.find_all("strong"):
        if "no events found" in strong.get_text(strip=True).lower():
            return "empty"

    # Modern format: div.grid.border with id starting with "en"
    modern_events = soup.select('div.grid.border[id^="en"]')
    if modern_events:
        return "modern"

    # Modern format alternative: div.nrc-event-report-day with grid border divs
    report_day = soup.find("div", class_="nrc-event-report-day")
    if report_day and report_day.select("div.grid.border"):
        return "modern"

    # Legacy format: <a name="enXXXXX"> anchors
    legacy_anchors = soup.find_all("a", attrs={"name": lambda v: v and v.startswith("en")})
    if legacy_anchors:
        return "legacy"

    # Could be an empty page without the "No events found" text
    # (some old pages are just empty shells on the new Drupal site)
    text = soup.get_text(strip=True)
    if len(text) < 200:
        return "empty"

    return "unknown"
