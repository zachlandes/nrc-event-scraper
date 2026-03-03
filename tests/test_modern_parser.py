"""Tests for modern parser against real HTML fixtures."""

from pathlib import Path

from nrc_event_scraper.models import EventCategory
from nrc_event_scraper.parser.modern_parser import parse_modern_page


def test_parse_modern_multi_event(fixtures_dir: Path):
    html = (fixtures_dir / "modern_multi_event.html").read_text()
    report = parse_modern_page(html, page_url="https://example.com/20260303en")

    assert report.html_format == "modern"
    assert len(report.events) == 3
    assert len(report.parse_errors) == 0


def test_modern_agreement_state_event(fixtures_dir: Path):
    html = (fixtures_dir / "modern_multi_event.html").read_text()
    report = parse_modern_page(html)

    # First event: Agreement State 58169
    event = report.events[0]
    assert event.event_number == 58169
    assert event.category == EventCategory.AGREEMENT_STATE
    assert event.licensee == "University of Maryland"
    assert event.rep_org == "Maryland Dept of the Environment"
    assert event.region == "1"
    assert event.city == "Baltimore"
    assert event.state == "MD"
    assert event.license_number == "MD-33-004-01"
    assert event.agreement == "Y"


def test_modern_agreement_state_dates(fixtures_dir: Path):
    html = (fixtures_dir / "modern_multi_event.html").read_text()
    report = parse_modern_page(html)
    event = report.events[0]

    assert event.notification_date is not None
    assert event.notification_date.month == 2
    assert event.notification_date.day == 23
    assert event.notification_date.year == 2026
    assert event.notification_time == "10:36"
    assert event.notification_timezone == "ET"
    assert event.event_date is not None
    assert event.event_date.day == 20
    assert event.emergency_class == "Non Emergency"


def test_modern_agreement_state_persons(fixtures_dir: Path):
    html = (fixtures_dir / "modern_multi_event.html").read_text()
    report = parse_modern_page(html)
    event = report.events[0]

    assert len(event.persons_notified) >= 1
    names = [p.name for p in event.persons_notified]
    assert any("Seeley" in n for n in names)


def test_modern_power_reactor_event(fixtures_dir: Path):
    html = (fixtures_dir / "modern_multi_event.html").read_text()
    report = parse_modern_page(html)

    # Last event: Power Reactor 58181
    event = report.events[2]
    assert event.event_number == 58181
    assert event.category == EventCategory.POWER_REACTOR
    assert event.facility == "Brunswick"
    assert event.region == "2"
    assert event.state == "NC"
    assert event.unit == "[1] [] []"
    assert event.rx_type == "[1] GE-4,[2] GE-4"


def test_modern_power_reactor_cfr(fixtures_dir: Path):
    html = (fixtures_dir / "modern_multi_event.html").read_text()
    report = parse_modern_page(html)
    event = report.events[2]

    assert len(event.cfr_sections) == 1
    assert "50.72" in event.cfr_sections[0].code
    assert "Degraded Condition" in event.cfr_sections[0].description


def test_modern_power_reactor_units(fixtures_dir: Path):
    html = (fixtures_dir / "modern_multi_event.html").read_text()
    report = parse_modern_page(html)
    event = report.events[2]

    assert len(event.reactor_units) == 1
    unit = event.reactor_units[0]
    assert unit.unit == 1
    assert unit.scram_code == "N"
    assert unit.rx_crit == "N"
    assert unit.initial_power == 0
    assert unit.initial_rx_mode == "Refueling"


def test_modern_event_text(fixtures_dir: Path):
    html = (fixtures_dir / "modern_multi_event.html").read_text()
    report = parse_modern_page(html)

    # Check event text is extracted
    for event in report.events:
        assert event.event_text, f"Event {event.event_number} has no event text"

    # Power reactor event text
    event = report.events[2]
    assert "DEGRADED CONDITION" in event.event_text
    assert "PRIMARY CONTAINMENT" in event.event_text


def test_modern_no_events(fixtures_dir: Path):
    html = (fixtures_dir / "modern_no_events.html").read_text()
    report = parse_modern_page(html)
    assert len(report.events) == 0


def test_modern_event_html_format(fixtures_dir: Path):
    html = (fixtures_dir / "modern_multi_event.html").read_text()
    report = parse_modern_page(html)
    for event in report.events:
        assert event.html_format == "modern"


def test_modern_licensee_is_null_for_power_reactor(fixtures_dir: Path):
    html = (fixtures_dir / "modern_multi_event.html").read_text()
    report = parse_modern_page(html)
    power_event = report.events[2]
    assert power_event.licensee is None
    assert power_event.facility is not None


def test_modern_facility_is_null_for_material_event(fixtures_dir: Path):
    html = (fixtures_dir / "modern_multi_event.html").read_text()
    report = parse_modern_page(html)
    material_event = report.events[0]
    assert material_event.facility is None
    assert material_event.licensee is not None
