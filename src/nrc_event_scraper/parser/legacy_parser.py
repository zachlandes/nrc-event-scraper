"""Parser for legacy NRC format (1999–Oct 2020, table-based).

Legacy pages use nested <table> elements inside <div class="field--name-body">:
- Event anchors: <a name="enXXXXX">
- Event header: first <table> after anchor with category + event number in row 1
- Fields: row 2 has two cells (left: org/licensee info, right: dates/times)
- CFR + persons: row 3 has two cells (left: emergency/CFR, right: persons)
- Optional material warning: separate <table> with colspan="2" cell
- Event text: <table> following <p><b>Event Text</b></p>
- Reactor status: <table width="98%"> with reactor unit data
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


def parse_legacy_page(html: str, page_url: str = "") -> DailyReport:
    """Parse a legacy-format NRC event notification page."""
    soup = BeautifulSoup(html, "lxml")
    report = DailyReport(page_url=page_url, html_format="legacy")

    # Find all event anchors
    anchors = soup.find_all("a", attrs={"name": lambda v: v and v.startswith("en")})

    for anchor in anchors:
        try:
            event = _parse_legacy_event(anchor, page_url, report.report_date)
            report.events.append(event)
        except Exception as e:
            anchor_name = anchor.get("name", "unknown")
            logger.warning("Failed to parse legacy event %s: %s", anchor_name, e)
            report.parse_errors.append(f"Event {anchor_name}: {e}")

    return report


def _parse_legacy_event(anchor: Tag, page_url: str, report_date=None) -> NRCEvent:
    """Parse a single legacy event starting from its <a name="enXXXXX"> anchor."""
    warnings: list[str] = []

    anchor_name = anchor.get("name", "")
    event_number = int(re.search(r"\d+", anchor_name).group())  # type: ignore[union-attr]

    # Find the header table: first <table> after the anchor
    header_table = _find_next_table(anchor)
    if not header_table:
        raise ValueError(f"No header table found for event {event_number}")

    rows = header_table.find_all("tr")
    if not rows:
        raise ValueError(f"Empty header table for event {event_number}")

    # Row 0: category + event number
    cells_0 = rows[0].find_all("td")
    category = EventCategory.UNKNOWN
    if cells_0:
        category = parse_category(cells_0[0].get_text(strip=True))

    event = NRCEvent(
        event_number=event_number,
        category=category,
        page_url=page_url,
        report_date=report_date,
        html_format="legacy",
    )

    # Row 1: field data (left cell = org info, right cell = dates)
    if len(rows) > 1:
        cells_1 = rows[1].find_all("td")
        if cells_1:
            _parse_legacy_fields_cell(cells_1[0], event, warnings)
        if len(cells_1) > 1:
            _parse_legacy_dates_cell(cells_1[1], event, warnings)

    # Row 2: emergency class / CFR (left), persons (right)
    if len(rows) > 2:
        cells_2 = rows[2].find_all("td")
        if cells_2:
            _parse_legacy_cfr_cell(cells_2[0], event, warnings)
        if len(cells_2) > 1:
            _parse_legacy_persons_cell(cells_2[1], event, warnings)

    # Find reactor unit table (if power reactor)
    if event.category == EventCategory.POWER_REACTOR:
        reactor_table = _find_reactor_table(header_table)
        if reactor_table:
            event.reactor_units = _parse_legacy_reactor_table(reactor_table)

    # Find event text
    event_text = _find_event_text(header_table)
    if event_text:
        event.event_text = event_text

    event.parse_warnings = warnings
    return event


def _find_next_table(element: Tag) -> Tag | None:
    """Find the next <table> sibling after an element, skipping non-tag nodes."""
    current = element.next_sibling
    while current:
        if isinstance(current, Tag):
            if current.name == "table":
                return current
            # Skip <br> tags between anchor and table
            if current.name not in ("br",):
                return None
        current = current.next_sibling
    return None


def _parse_legacy_fields_cell(cell: Tag, event: NRCEvent, warnings: list[str]) -> None:
    """Parse the left field cell with org/licensee/facility info.

    Format is lines of 'Label: Value' separated by <br>.
    """
    text = cell.get_text(separator="\n")
    lines = [normalize_whitespace(ln) for ln in text.split("\n") if ln.strip()]

    for line in lines:
        if ":" not in line:
            continue
        label, _, value = line.partition(":")
        label = label.strip()
        value = value.strip()

        label_lower = label.lower()
        if label_lower == "facility":
            event.facility = value or None
        elif label_lower == "region":
            # May include "State:" on same line: "Region: 3     State: IL"
            if "State:" in value:
                parts = value.split("State:")
                event.region = parts[0].strip() or None
                event.state = parts[1].strip() or None
            else:
                event.region = value or None
        elif label_lower == "state":
            event.state = value or None
        elif label_lower == "unit":
            event.unit = value or None
        elif label_lower == "rx type":
            event.rx_type = value or None
        elif label_lower == "rep org":
            event.rep_org = value or None
        elif label_lower == "licensee":
            event.licensee = value or None
        elif label_lower == "license #":
            event.license_number = value or None
        elif label_lower == "agreement":
            event.agreement = value or None
        elif label_lower == "docket":
            event.docket = value or None
        elif label_lower == "county":
            event.county = value or None
        elif label_lower == "city":
            # May include "State:" on same line: "City: WARREN   State: MI"
            if "State:" in value:
                parts = value.split("State:")
                event.city = parts[0].strip() or None
                event.state = parts[1].strip() or None
            else:
                event.city = value or None
        elif label_lower in ("nrc notified by", "hq ops officer"):
            pass  # Not stored as separate fields


def _parse_legacy_dates_cell(cell: Tag, event: NRCEvent, warnings: list[str]) -> None:
    """Parse the right dates cell with notification/event dates and times."""
    text = cell.get_text(separator="\n")
    lines = [normalize_whitespace(ln) for ln in text.split("\n") if ln.strip()]

    for line in lines:
        if ":" not in line:
            continue
        label, _, value = line.partition(":")
        label = label.strip().lower()
        value = value.strip()

        if label == "notification date":
            event.notification_date = parse_date(value)
        elif label == "notification time":
            time_str, tz = parse_time_with_tz(value)
            event.notification_time = time_str
            event.notification_timezone = tz
        elif label == "event date":
            event.event_date = parse_date(value)
        elif label == "event time":
            time_str, tz = parse_time_with_tz(value)
            event.event_time = time_str
            event.event_timezone = tz
        elif label == "last update date":
            event.last_update_date = parse_date(value)


def _parse_legacy_cfr_cell(cell: Tag, event: NRCEvent, warnings: list[str]) -> None:
    """Parse emergency class and CFR sections from the left cell of row 2."""
    text = cell.get_text(separator="\n")
    lines = [normalize_whitespace(ln) for ln in text.split("\n") if ln.strip()]

    for line in lines:
        if line.lower().startswith("emergency class"):
            _, _, value = line.partition(":")
            event.emergency_class = value.strip()
        elif "10 CFR Section" in line:
            continue  # Label line
        elif re.match(r"\d+\.\d+", line):
            # CFR section reference
            event.cfr_sections.extend(parse_cfr_sections(line))
        elif "AGREEMENT STATE" in line.upper():
            pass  # Category indicator in CFR section area


def _parse_legacy_persons_cell(cell: Tag, event: NRCEvent, warnings: list[str]) -> None:
    """Parse person contacts from the right cell of row 2."""
    text = cell.get_text(separator="\n")
    event.persons_notified = parse_persons(text)


def _find_reactor_table(header_table: Tag) -> Tag | None:
    """Find a reactor unit status table after the header table.

    Legacy reactor tables use width="98%" attribute.
    """
    current = header_table.next_sibling
    while current:
        if isinstance(current, Tag):
            if current.name == "table":
                # Check if it's a reactor status table (has "Unit" in first row)
                first_row_text = ""
                first_row = current.find("tr")
                if first_row:
                    first_row_text = first_row.get_text(strip=True)
                if "Unit" in first_row_text and "SCRAM" in first_row_text:
                    return current
                # Check width attribute (legacy indicator)
                if current.get("width", "").startswith("98"):
                    return current
            # Stop if we hit the next anchor
            if current.name == "a" and current.get("name", "").startswith("en"):
                return None
            # Stop at Event Text heading
            if current.name == "p":
                b = current.find("b")
                if b and "Event Text" in b.get_text():
                    return None
        current = current.next_sibling
    return None


def _parse_legacy_reactor_table(table: Tag) -> list:
    """Parse a legacy reactor status table.

    Legacy tables use colspan="2" for each cell, so we extract text from
    each <td> and group by rows.
    """
    rows = []
    for tr in table.find_all("tr"):
        cells = [td.get_text(strip=True) for td in tr.find_all("td")]
        if cells:
            rows.append(cells)

    # Skip header row
    if len(rows) > 1:
        return parse_reactor_units_from_rows(rows[1:])
    return []


def _find_event_text(header_table: Tag) -> str | None:
    """Find and extract event text following 'Event Text' heading.

    The structure is: <p><b>Event Text</b></p><table><tr><td>...text...</td></tr></table>
    """
    current = header_table.next_sibling
    found_event_text_label = False

    while current:
        if isinstance(current, Tag):
            # Look for the "Event Text" label
            if current.name == "p":
                b = current.find("b")
                if b and "Event Text" in b.get_text():
                    found_event_text_label = True

            # After finding the label, the next table contains the text
            if found_event_text_label and current.name == "table":
                td = current.find("td")
                if td:
                    # Replace <br> with newlines
                    for br in td.find_all("br"):
                        br.replace_with("\n")
                    text = td.get_text()
                    lines = [line.strip() for line in text.split("\n")]
                    while lines and not lines[0]:
                        lines.pop(0)
                    while lines and not lines[-1]:
                        lines.pop()
                    return "\n".join(lines)
                return None

            # Stop if we hit the next event anchor
            if current.name == "a" and current.get("name", "").startswith("en"):
                return None

        current = current.next_sibling

    return None
