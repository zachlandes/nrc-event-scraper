"""Pydantic v2 models for NRC Event Notification Reports.

Unified NRCEvent model: Power Reactor events use facility/unit/rx_type/reactor_units,
Material events use licensee/license_number/rep_org/agreement/docket/county/city.
Both use Optional fields — category discriminates which fields are populated.
"""

from __future__ import annotations

import enum
from datetime import date, datetime

from pydantic import BaseModel, Field


class EventCategory(str, enum.Enum):
    POWER_REACTOR = "Power Reactor"
    MATERIAL = "Material"
    FUEL_CYCLE = "Fuel Cycle"
    AGREEMENT_STATE = "Agreement State"
    UNKNOWN = "Unknown"


class CFRSection(BaseModel):
    """A 10 CFR reporting requirement reference."""

    code: str  # e.g. "50.72(b)(3)(ii)(A)"
    description: str = ""  # e.g. "Degraded Condition"


class PersonContact(BaseModel):
    """A contact person listed on an event."""

    name: str
    organization: str = ""
    phone: str = ""


class ReactorUnitStatus(BaseModel):
    """Status row from the reactor status table at the bottom of power reactor events."""

    unit: int
    scram_code: str = ""  # "N", "A/R", "M/R", etc.
    rx_crit: str = ""  # "Y" or "N"
    initial_power: int | None = None
    initial_rx_mode: str = ""
    current_power: int | None = None
    current_rx_mode: str = ""


class NRCEvent(BaseModel):
    """Unified model for a single NRC Event Notification Report entry.

    Fields are Optional where they only apply to certain event categories.
    parse_warnings collects any anomalies encountered during parsing so
    nothing is silently lost.
    """

    event_number: int
    category: EventCategory = EventCategory.UNKNOWN
    page_url: str = ""
    report_date: date | None = None

    # ── Power Reactor fields ────────────────────────────
    facility: str | None = None
    region: str | None = None
    state: str | None = None
    unit: str | None = None  # e.g. "[1] [] []"
    rx_type: str | None = None  # e.g. "[1] GE-4"

    # ── Material / Fuel Cycle / Agreement State fields ──
    licensee: str | None = None
    license_number: str | None = None
    rep_org: str | None = None  # reporting organization
    agreement: str | None = None  # "Y" or "N"
    docket: str | None = None
    county: str | None = None
    city: str | None = None

    # ── Common fields ───────────────────────────────────
    notification_date: date | None = None
    notification_time: str | None = None  # "HH:MM"
    notification_timezone: str = "ET"
    event_date: date | None = None
    event_time: str | None = None
    event_timezone: str = ""
    last_update_date: date | None = None
    emergency_class: str = ""
    cfr_sections: list[CFRSection] = Field(default_factory=list)
    persons_notified: list[PersonContact] = Field(default_factory=list)
    event_text: str = ""
    reactor_units: list[ReactorUnitStatus] = Field(default_factory=list)

    # ── Metadata ────────────────────────────────────────
    scraped_at: datetime | None = None
    html_format: str = ""  # "modern", "legacy"
    parse_warnings: list[str] = Field(default_factory=list)


class DailyReport(BaseModel):
    """A single day's page of events."""

    page_url: str
    report_date: date | None = None
    html_format: str = ""
    events: list[NRCEvent] = Field(default_factory=list)
    is_empty: bool = False  # True if "No events found"
    parse_errors: list[str] = Field(default_factory=list)
