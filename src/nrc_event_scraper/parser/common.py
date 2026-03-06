"""Shared field normalization used by both modern and legacy parsers.

Handles date/time parsing, CFR section extraction, person parsing,
and whitespace cleanup — all the messy text-to-structured-data conversions.
"""

from __future__ import annotations

import re
from datetime import date

from nrc_event_scraper.models import CFRSection, EventCategory, PersonContact, ReactorUnitStatus


def normalize_whitespace(text: str) -> str:
    """Collapse all whitespace (including &nbsp;) to single spaces, strip edges."""
    text = text.replace("\xa0", " ")  # &nbsp;
    return re.sub(r"\s+", " ", text).strip()


def parse_date(text: str) -> date | None:
    """Parse MM/DD/YYYY date string."""
    text = text.strip()
    m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", text)
    if not m:
        return None
    try:
        return date(int(m.group(3)), int(m.group(1)), int(m.group(2)))
    except ValueError:
        return None


def parse_time_with_tz(text: str) -> tuple[str | None, str]:
    """Parse 'HH:MM [TZ]' string, returning (time_str, timezone).

    Examples:
        '10:36 [ET]' -> ('10:36', 'ET')
        '09:03 [PST]' -> ('09:03', 'PST')
        '10:36' -> ('10:36', '')
    """
    text = text.strip()
    m = re.match(r"(\d{1,2}:\d{2})\s*(?:\[(\w+)\])?", text)
    if not m:
        return None, ""
    return m.group(1), m.group(2) or ""


def parse_cfr_sections(text: str) -> list[CFRSection]:
    """Extract 10 CFR section references from text.

    Handles patterns like:
        '50.72(b)(3)(ii)(A) - Degraded Condition'
        '20.2201(a)(1)(i) - LOST/STOLEN LNM>1000X'
        'AGREEMENT STATE' (treated as category, not CFR)
    """
    sections = []
    text = normalize_whitespace(text)

    # Match patterns like XX.XX(...)... - Description
    pattern = r"(\d+\.\d+(?:\([^)]*\))*(?:\([^)]*\))*)\s*-\s*(.+?)(?=\d+\.\d+\(|$)"
    for m in re.finditer(pattern, text):
        sections.append(CFRSection(code=m.group(1).strip(), description=m.group(2).strip()))

    return sections


def parse_category(text: str) -> EventCategory:
    """Map category text to EventCategory enum."""
    text = normalize_whitespace(text).upper()
    if "POWER REACTOR" in text:
        return EventCategory.POWER_REACTOR
    if "AGREEMENT STATE" in text:
        return EventCategory.AGREEMENT_STATE
    if "FUEL CYCLE" in text:
        return EventCategory.FUEL_CYCLE
    if "MATERIAL" in text or "NON-AGREEMENT" in text:
        return EventCategory.MATERIAL
    if "GENERAL" in text or "INFORMATION" in text:
        return EventCategory.GENERAL
    return EventCategory.UNKNOWN


def parse_persons(text: str) -> list[PersonContact]:
    """Parse person contact lines like 'Name (Org)' separated by line breaks.

    Examples:
        'Seeley, Shawn (R1DO)' -> PersonContact(name='Seeley, Shawn', organization='R1DO')
        'NMSS_EVENTS_NOTIFICATION (EMAIL)' ->
            PersonContact(name='NMSS_EVENTS_NOTIFICATION', organization='EMAIL')
    """
    persons = []
    lines = re.split(r"\n|<br\s*/?>", text)
    for line in lines:
        line = normalize_whitespace(line)
        if not line or line.lower().startswith("person"):
            continue
        # Match 'Name (Org)' pattern
        m = re.match(r"^-?\s*(.+?)\s*\(([^)]+)\)\s*(?:\(EMAIL\))?$", line)
        if m:
            name = m.group(1).strip().rstrip(",")
            org = m.group(2).strip()
            if name:
                persons.append(PersonContact(name=name, organization=org))
        elif line and not line.startswith("-"):
            # Bare name without org
            persons.append(PersonContact(name=line))
    return persons


def parse_reactor_units_from_rows(rows: list[list[str]]) -> list[ReactorUnitStatus]:
    """Parse reactor unit status from table rows (list of cell values).

    First row is headers, remaining are data. Handles both modern <table>
    and legacy <table> with colspan=2 cells.
    """
    units = []
    for row in rows:
        if len(row) < 7:
            continue
        try:
            unit_num = int(row[0].strip())
        except (ValueError, IndexError):
            continue
        units.append(
            ReactorUnitStatus(
                unit=unit_num,
                scram_code=row[1].strip() if len(row) > 1 else "",
                rx_crit=row[2].strip() if len(row) > 2 else "",
                initial_power=_safe_int(row[3]) if len(row) > 3 else None,
                initial_rx_mode=row[4].strip() if len(row) > 4 else "",
                current_power=_safe_int(row[5]) if len(row) > 5 else None,
                current_rx_mode=row[6].strip() if len(row) > 6 else "",
            )
        )
    return units


def extract_field_value(text: str, label: str) -> str:
    """Extract value after a 'Label: Value' pattern from normalized text."""
    pattern = rf"{re.escape(label)}:\s*(.+?)(?=\s+\w[\w\s]*:|$)"
    m = re.search(pattern, text, re.IGNORECASE)
    return m.group(1).strip() if m else ""


def _safe_int(text: str) -> int | None:
    text = text.strip()
    try:
        return int(text)
    except (ValueError, TypeError):
        return None
