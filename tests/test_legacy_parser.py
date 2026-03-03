"""Tests for legacy parser against real HTML fixtures."""

from pathlib import Path

from nrc_event_scraper.models import EventCategory
from nrc_event_scraper.parser.legacy_parser import parse_legacy_page


def test_parse_legacy_multi_event(fixtures_dir: Path):
    html = (fixtures_dir / "legacy_multi_event.html").read_text()
    report = parse_legacy_page(html, page_url="https://example.com/20190301en")

    assert report.html_format == "legacy"
    assert len(report.events) == 2
    assert len(report.parse_errors) == 0


def test_legacy_agreement_state_event(fixtures_dir: Path):
    html = (fixtures_dir / "legacy_multi_event.html").read_text()
    report = parse_legacy_page(html)

    event = report.events[0]
    assert event.event_number == 53885
    assert event.category == EventCategory.AGREEMENT_STATE
    assert event.licensee == "G3SOILWORKS, INC."
    assert event.rep_org == "CA DEPARTMENT OF PUBLIC HEALTH"
    assert event.region == "4"
    assert event.city == "NEWPORT BEACH"
    assert event.state == "CA"
    assert event.license_number == "6492-30"
    assert event.agreement == "Y"


def test_legacy_agreement_state_dates(fixtures_dir: Path):
    html = (fixtures_dir / "legacy_multi_event.html").read_text()
    report = parse_legacy_page(html)
    event = report.events[0]

    assert event.notification_date is not None
    assert event.notification_date.month == 2
    assert event.notification_date.day == 20
    assert event.notification_date.year == 2019
    assert event.notification_time == "15:15"
    assert event.notification_timezone == "ET"
    assert event.emergency_class == "NON EMERGENCY"


def test_legacy_persons(fixtures_dir: Path):
    html = (fixtures_dir / "legacy_multi_event.html").read_text()
    report = parse_legacy_page(html)
    event = report.events[0]

    assert len(event.persons_notified) >= 1
    names = [p.name for p in event.persons_notified]
    assert any("DEESE" in n.upper() for n in names)


def test_legacy_power_reactor_event(fixtures_dir: Path):
    html = (fixtures_dir / "legacy_multi_event.html").read_text()
    report = parse_legacy_page(html)

    event = report.events[1]
    assert event.event_number == 53903
    assert event.category == EventCategory.POWER_REACTOR
    assert event.facility == "LASALLE"
    assert event.region == "3"
    assert event.state == "IL"
    assert event.unit == "[] [2] []"
    assert event.rx_type == "[1] GE-5,[2] GE-5"


def test_legacy_power_reactor_cfr(fixtures_dir: Path):
    html = (fixtures_dir / "legacy_multi_event.html").read_text()
    report = parse_legacy_page(html)
    event = report.events[1]

    assert len(event.cfr_sections) == 1
    assert "50.72" in event.cfr_sections[0].code
    assert "VALID SPECIF SYS ACTUATION" in event.cfr_sections[0].description


def test_legacy_power_reactor_units(fixtures_dir: Path):
    html = (fixtures_dir / "legacy_multi_event.html").read_text()
    report = parse_legacy_page(html)
    event = report.events[1]

    assert len(event.reactor_units) == 1
    unit = event.reactor_units[0]
    assert unit.unit == 2
    assert unit.scram_code == "N"
    assert unit.rx_crit == "N"
    assert unit.initial_power == 0
    assert unit.initial_rx_mode == "Refueling"


def test_legacy_event_text(fixtures_dir: Path):
    html = (fixtures_dir / "legacy_multi_event.html").read_text()
    report = parse_legacy_page(html)

    for event in report.events:
        assert event.event_text, f"Event {event.event_number} has no event text"

    # Power reactor event text
    event = report.events[1]
    assert "SPECIFIED SYSTEM ACTUATION" in event.event_text


def test_legacy_event_html_format(fixtures_dir: Path):
    html = (fixtures_dir / "legacy_multi_event.html").read_text()
    report = parse_legacy_page(html)
    for event in report.events:
        assert event.html_format == "legacy"
