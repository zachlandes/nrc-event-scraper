"""Parser for modern NRC format (Nov 2020–present, Drupal 10).

Modern pages use semantic CSS classes:
- Event containers: <div class="grid border" id="enXXXXX">
- Category: first <div class="th"> child
- Event number: second <div class="th"> child
- Fields: <b>Label:</b> Value patterns in plain <div> children
- Event text: <div class="border"> following the event container
- Reactor unit table: <table class="nrc-report-table"> following the event container
"""

from __future__ import annotations

import logging
import re

from bs4 import BeautifulSoup, Tag

from nrc_event_scraper.models import DailyReport, EventCategory, NRCEvent
from nrc_event_scraper.parser.common import (
    normalize_whitespace,
    parse_category,
    parse_cfr_sections,
    parse_date,
    parse_persons,
    parse_reactor_units_from_rows,
    parse_time_with_tz,
)

logger = logging.getLogger(__name__)


def parse_modern_page(html: str, page_url: str = "") -> DailyReport:
    """Parse a modern-format NRC event notification page.

    Returns a DailyReport with all events found on the page.
    Per-event errors are captured in parse_warnings rather than crashing.
    """
    soup = BeautifulSoup(html, "lxml")
    report = DailyReport(page_url=page_url, html_format="modern")

    # Extract report date from header if present
    header = soup.find("h1")
    if header:
        date_match = re.search(
            r"(\w+ \d{1,2}, \d{4})", header.get_text(strip=True)
        )
        if date_match:
            from datetime import datetime

            try:
                report.report_date = datetime.strptime(
                    date_match.group(1), "%B %d, %Y"
                ).date()
            except ValueError:
                pass

    # Find all event containers
    event_divs = soup.select('div.grid.border[id^="en"]')
    if not event_divs:
        # Try alternate selector
        event_divs = [
            div
            for div in soup.select("div.grid.border")
            if div.get("id", "").startswith("en")
        ]

    for event_div in event_divs:
        try:
            event = _parse_single_event(event_div, page_url, report.report_date)
            report.events.append(event)
        except Exception as e:
            event_id = event_div.get("id", "unknown")
            logger.warning("Failed to parse event %s: %s", event_id, e)
            report.parse_errors.append(f"Event {event_id}: {e}")

    return report


def _parse_single_event(
    event_div: Tag, page_url: str, report_date=None
) -> NRCEvent:
    """Parse a single <div class="grid border" id="enXXXXX"> event container."""
    warnings: list[str] = []

    # Event number from id attribute
    event_id = event_div.get("id", "")
    event_number = int(re.search(r"\d+", event_id).group())  # type: ignore[union-attr]

    # Category and event number from <div class="th"> children
    th_divs = event_div.select("div.th")
    category = EventCategory.UNKNOWN
    if th_divs:
        category = parse_category(th_divs[0].get_text(strip=True))

    # Regular div children (not .th) contain the field data
    field_divs = [
        d for d in event_div.find_all("div", recursive=False) if "th" not in (d.get("class") or [])
    ]

    event = NRCEvent(
        event_number=event_number,
        category=category,
        page_url=page_url,
        report_date=report_date,
        html_format="modern",
    )

    # Parse field divs based on their content
    for div in field_divs:
        _parse_field_div(div, event, warnings)

    # Find event text: the <div class="border"> that follows this event container
    # It's a sibling, not a child
    event_text_div = _find_event_text(event_div)
    if event_text_div:
        event.event_text = _clean_event_text(event_text_div)

    # Find reactor unit table (for power reactor events)
    if event.category == EventCategory.POWER_REACTOR:
        reactor_table = _find_reactor_table(event_div)
        if reactor_table:
            event.reactor_units = _parse_reactor_table(reactor_table)

    event.parse_warnings = warnings
    return event


def _parse_field_div(div: Tag, event: NRCEvent, warnings: list[str]) -> None:
    """Parse a field div by extracting <b>Label:</b> Value pairs."""
    # Get all text, resolving <b> tags
    text = div.get_text(separator="\n")

    # Check what kind of field div this is by looking at labels
    b_tags = div.find_all("b")
    labels = {normalize_whitespace(b.get_text()): b for b in b_tags}

    for label_text, b_tag in labels.items():
        label_clean = label_text.rstrip(":")

        # Get value: text after the <b> tag until next <br> or <b>
        value = _get_value_after_b(b_tag)

        if not value:
            continue

        _assign_field(event, label_clean, value, warnings)

    # Parse CFR sections from the div containing "10 CFR Section:"
    if "10 CFR Section:" in text or "10 CFR Section" in text:
        # Extract everything after "10 CFR Section:" (or similar)
        cfr_text = text.split("10 CFR Section")[-1].lstrip(":").strip()
        event.cfr_sections = parse_cfr_sections(cfr_text)
        # If no CFR sections parsed but category text is present, that's fine
        # (e.g., "Agreement State" is the category, not a CFR section)

    # Parse persons
    if "Person (Organization):" in text or "Person(Organization):" in text:
        persons_text = text.split("Person")[-1]
        if "Organization):" in persons_text:
            persons_text = persons_text.split("Organization):")[-1]
        event.persons_notified = parse_persons(persons_text)


def _get_value_after_b(b_tag: Tag) -> str:
    """Extract the text value that follows a <b> tag until the next structural element."""
    parts = []
    sibling = b_tag.next_sibling
    while sibling:
        if isinstance(sibling, Tag):
            if sibling.name == "b":
                break
            if sibling.name == "br":
                break
            parts.append(sibling.get_text())
        else:
            text = str(sibling).strip()
            if text:
                parts.append(text)
        sibling = sibling.next_sibling
    return normalize_whitespace(" ".join(parts))


def _assign_field(event: NRCEvent, label: str, value: str, warnings: list[str]) -> None:
    """Map a label-value pair to the appropriate NRCEvent field."""
    label_lower = label.lower().strip()

    field_map = {
        "facility": "facility",
        "region": "region",
        "state": "state",
        "unit": "unit",
        "rx type": "rx_type",
        "rep org": "rep_org",
        "licensee": "licensee",
        "license #": "license_number",
        "agreement": "agreement",
        "docket": "docket",
        "county": "county",
        "city": "city",
    }

    if label_lower in field_map:
        setattr(event, field_map[label_lower], value if value else None)
        return

    if label_lower == "notification date":
        event.notification_date = parse_date(value)
    elif label_lower == "notification time":
        time_str, tz = parse_time_with_tz(value)
        event.notification_time = time_str
        event.notification_timezone = tz
    elif label_lower == "event date":
        event.event_date = parse_date(value)
    elif label_lower == "event time":
        time_str, tz = parse_time_with_tz(value)
        event.event_time = time_str
        event.event_timezone = tz
    elif label_lower == "last update date":
        event.last_update_date = parse_date(value)
    elif label_lower == "emergency class":
        event.emergency_class = value
    elif label_lower in ("nrc notified by", "hq ops officer"):
        pass  # Not stored separately; part of metadata
    elif label_lower in ("person (organization)", "person(organization)"):
        pass  # Handled separately in _parse_field_div
    else:
        warnings.append(f"Unexpected field: '{label}' = '{value}'")


def _find_event_text(event_div: Tag) -> Tag | None:
    """Find the <div class="border"> event text div following an event container.

    The DOM structure is: <div class="grid border" id="enXXXXX">, then
    a <b>Event Text</b>, then <div class="border"> (the text content).
    """
    current = event_div.next_sibling
    while current:
        if isinstance(current, Tag):
            classes = current.get("class") or []
            if current.name == "div" and "border" in classes and "grid" not in classes:
                return current
            # Stop if we hit the next event container
            is_next_event = "grid" in classes and current.get("id", "").startswith("en")
            if current.name == "div" and is_next_event:
                return None
        current = current.next_sibling
    return None


def _find_reactor_table(event_div: Tag) -> Tag | None:
    """Find the reactor unit status table following an event container."""
    current = event_div.next_sibling
    while current:
        if isinstance(current, Tag):
            if current.name == "table":
                return current
            # Stop search at next event container or border div (event text)
            if current.name == "div" and "grid" in (current.get("class") or []):
                return None
        current = current.next_sibling
    return None


def _parse_reactor_table(table: Tag) -> list:
    """Parse a <table class="nrc-report-table"> into ReactorUnitStatus objects."""
    rows = []
    for tr in table.find_all("tr"):
        cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
        if cells:
            rows.append(cells)

    # Skip header row (first row)
    if len(rows) > 1:
        return parse_reactor_units_from_rows(rows[1:])
    return []


def _clean_event_text(div: Tag) -> str:
    """Extract and clean event text from a <div class="border">."""
    # Replace <br> with newlines, then get text
    for br in div.find_all("br"):
        br.replace_with("\n")
    text = div.get_text()
    # Clean up multiple newlines but preserve paragraph breaks
    lines = [line.strip() for line in text.split("\n")]
    # Remove leading/trailing empty lines
    while lines and not lines[0]:
        lines.pop(0)
    while lines and not lines[-1]:
        lines.pop()
    return "\n".join(lines)
