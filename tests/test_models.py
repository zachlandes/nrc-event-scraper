"""Tests for Pydantic models."""

from datetime import date, datetime, timezone

from nrc_event_scraper.models import (
    CFRSection,
    DailyReport,
    EventCategory,
    NRCEvent,
    PersonContact,
    ReactorUnitStatus,
)


def test_nrc_event_minimal():
    """An event with just an event number is valid."""
    event = NRCEvent(event_number=58181)
    assert event.event_number == 58181
    assert event.category == EventCategory.UNKNOWN
    assert event.cfr_sections == []
    assert event.parse_warnings == []


def test_nrc_event_power_reactor():
    event = NRCEvent(
        event_number=58181,
        category=EventCategory.POWER_REACTOR,
        facility="Brunswick",
        region="2",
        state="NC",
        unit="[1] [] []",
        rx_type="[1] GE-4",
        report_date=date(2026, 3, 3),
        notification_date=date(2026, 3, 2),
        notification_time="07:41",
        emergency_class="Non Emergency",
        cfr_sections=[CFRSection(code="50.72(b)(3)(ii)(A)", description="Degraded Condition")],
        reactor_units=[
            ReactorUnitStatus(unit=1, scram_code="N", rx_crit="Y", initial_power=100)
        ],
    )
    assert event.facility == "Brunswick"
    assert event.licensee is None  # Power reactor doesn't use this
    assert len(event.cfr_sections) == 1
    assert event.reactor_units[0].initial_power == 100


def test_nrc_event_material():
    event = NRCEvent(
        event_number=58180,
        category=EventCategory.MATERIAL,
        licensee="Acme Corp",
        license_number="12-34567-01",
        city="Springfield",
        state="IL",
        county="Sangamon",
    )
    assert event.licensee == "Acme Corp"
    assert event.facility is None
    assert event.reactor_units == []


def test_nrc_event_with_warnings():
    event = NRCEvent(
        event_number=58181,
        parse_warnings=["Unexpected field: 'foo'", "Missing expected region"],
    )
    assert len(event.parse_warnings) == 2


def test_nrc_event_serialization_roundtrip():
    event = NRCEvent(
        event_number=58181,
        category=EventCategory.POWER_REACTOR,
        report_date=date(2026, 3, 3),
        scraped_at=datetime(2026, 3, 3, 17, 10, 22, tzinfo=timezone.utc),
    )
    data = event.model_dump(mode="json")
    restored = NRCEvent.model_validate(data)
    assert restored.event_number == 58181
    assert restored.report_date == date(2026, 3, 3)


def test_daily_report():
    report = DailyReport(
        page_url="https://www.nrc.gov/reading-rm/doc-collections/event-status/event/2026/20260303en",
        report_date=date(2026, 3, 3),
        html_format="modern",
        events=[NRCEvent(event_number=58181), NRCEvent(event_number=58180)],
    )
    assert len(report.events) == 2
    assert not report.is_empty


def test_daily_report_empty():
    report = DailyReport(
        page_url="https://example.com/empty",
        is_empty=True,
    )
    assert report.events == []
    assert report.is_empty


def test_cfr_section():
    cfr = CFRSection(code="50.72(b)(3)(ii)(A)", description="Degraded Condition")
    assert "50.72" in cfr.code


def test_person_contact():
    p = PersonContact(name="John Doe", organization="NRC", phone="301-555-1234")
    assert p.name == "John Doe"


def test_reactor_unit_status():
    u = ReactorUnitStatus(
        unit=1,
        scram_code="N",
        rx_crit="Y",
        initial_power=100,
        initial_rx_mode="Power Operation",
        current_power=100,
        current_rx_mode="Power Operation",
    )
    assert u.initial_power == 100
    assert u.scram_code == "N"
