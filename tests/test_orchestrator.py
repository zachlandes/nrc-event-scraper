"""Tests for the orchestrator with mocked HTTP."""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from nrc_event_scraper.config import Settings
from nrc_event_scraper.orchestrator import Orchestrator


@pytest.fixture
def orch_settings(tmp_path: Path) -> Settings:
    return Settings(
        base_dir=tmp_path / "data",
        rate_limit_qps=100.0,
        rate_limit_jitter=0.0,
        max_retries=1,
        retry_backoff_base=0.01,
        start_year=2026,
        end_year=2026,
    )


YEAR_INDEX_HTML = """
<html><body>
<a href="20260101en">January 01</a>
<a href="20260102en">January 02</a>
</body></html>
"""

MODERN_PAGE_HTML = """
<div class="nrc-event-report-day general-content">
  <div class="event-summary text-center">
    <p>U.S. Nuclear Regulatory Commission<br>Operations Center</p>
    <p>EVENT REPORTS FOR<br>01/01/2026 - 01/01/2026</p>
  </div>
  <div class="grid border" id="en99001">
    <div class="th">Power Reactor</div>
    <div class="th">Event Number: 99001</div>
    <div>
      <b>Facility:</b> TestPlant<br>
      <b>Region:</b> 1 &nbsp; &nbsp; <b>State:</b> PA<br>
      <b>Unit:</b> [1] [] []<br>
      <b>RX Type:</b> [1] PWR<br>
      <b>NRC Notified By:</b> Test Person<br>
      <b>HQ OPS Officer:</b> Test Officer
    </div>
    <div>
      <b>Notification Date:</b> 01/01/2026<br>
      <b>Notification Time:</b> 08:00 [ET]<br>
      <b>Event Date:</b> 01/01/2026<br>
      <b>Event Time:</b> 07:00 [EST]<br>
      <b>Last Update Date:</b> 01/01/2026
    </div>
    <div>
      <b>Emergency Class:</b> Non Emergency<br>
      10 CFR Section:<br>
      50.72(b)(2)(i) - Plant S/D Required by TS
    </div>
    <div>
      <b>Person (Organization):</b><br>
      Smith, John (R1DO)<br>
    </div>
  </div>
  <b>Event Text</b>
  <div class="border">
    TEST EVENT - UNIT 1 SHUTDOWN<br>
<br>
Test event description for unit testing purposes.<br>
  </div>
</div>
"""

EMPTY_PAGE_HTML = """
<div class="nrc-event-report-day general-content">
  <strong>No events found</strong>
</div>
"""


def _mock_response(status_code=200, text=""):
    """Create a mock curl_cffi Response."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.text = text
    return resp


def _make_url_router(base_url, routes):
    """Create an async mock that routes URLs to responses.

    routes: dict mapping URL suffix to (status_code, text) tuples.
    """
    async def router(url, **kwargs):
        for suffix, (status, text) in routes.items():
            if url.endswith(suffix):
                return _mock_response(status, text)
        return _mock_response(404, "")
    return router


@pytest.mark.asyncio
async def test_backfill_full_pipeline(orch_settings: Settings):
    """Integration test: backfill discovers, fetches, and parses pages."""
    routes = {
        "/2026/index.html": (200, YEAR_INDEX_HTML),
        "/2026/20260101en": (200, MODERN_PAGE_HTML),
        "/2026/20260102en": (200, EMPTY_PAGE_HTML),
    }

    with patch(
        "nrc_event_scraper.scraper.client.AsyncSession"
    ) as MockSession:
        mock_session = MagicMock()
        mock_session.get = AsyncMock(side_effect=_make_url_router("", routes))
        mock_session.close = AsyncMock()
        mock_session.headers = orch_settings.headers
        mock_session.impersonate = "chrome131"
        MockSession.return_value = mock_session

        orch = Orchestrator(orch_settings)
        stats = await orch.backfill(years=[2026])

    assert stats["pages_discovered"] == 2
    assert stats["pages_fetched"] == 2
    assert stats["pages_parsed"] == 2
    assert stats["events_found"] == 1
    assert stats["errors"] == 0

    # Verify JSONL output
    events = orch.writer.read_events(2026)
    assert len(events) == 1
    assert events[0].event_number == 99001
    assert events[0].facility == "TestPlant"

    # Verify DB state
    db_stats = orch.db.get_stats()
    assert db_stats["total_unique_events"] == 1
    assert db_stats["pages_by_status"]["parsed"] == 2


@pytest.mark.asyncio
async def test_backfill_idempotent(orch_settings: Settings):
    """Running backfill twice should not duplicate events."""
    routes = {
        "/2026/index.html": (200, YEAR_INDEX_HTML),
        "/2026/20260101en": (200, MODERN_PAGE_HTML),
        "/2026/20260102en": (200, EMPTY_PAGE_HTML),
    }

    orch = Orchestrator(orch_settings)

    # First run
    with patch(
        "nrc_event_scraper.scraper.client.AsyncSession"
    ) as MockSession:
        mock_session = MagicMock()
        mock_session.get = AsyncMock(side_effect=_make_url_router("", routes))
        mock_session.close = AsyncMock()
        mock_session.headers = orch_settings.headers
        mock_session.impersonate = "chrome131"
        MockSession.return_value = mock_session

        stats1 = await orch.backfill(years=[2026])
    assert stats1["events_found"] == 1

    # Second run — pages already parsed, should skip
    index_only_routes = {
        "/2026/index.html": (200, YEAR_INDEX_HTML),
    }
    with patch(
        "nrc_event_scraper.scraper.client.AsyncSession"
    ) as MockSession:
        mock_session = MagicMock()
        mock_session.get = AsyncMock(side_effect=_make_url_router("", index_only_routes))
        mock_session.close = AsyncMock()
        mock_session.headers = orch_settings.headers
        mock_session.impersonate = "chrome131"
        MockSession.return_value = mock_session

        stats2 = await orch.backfill(years=[2026])
    assert stats2["pages_fetched"] == 0
    assert stats2["pages_parsed"] == 0

    # Verify no duplicates in JSONL
    events = orch.writer.read_events(2026)
    assert len(events) == 1


@pytest.mark.asyncio
async def test_backfill_with_fetch_error(orch_settings: Settings):
    """Fetch errors should be recorded but not crash the pipeline."""
    routes = {
        "/2026/index.html": (200, YEAR_INDEX_HTML),
        "/2026/20260101en": (200, MODERN_PAGE_HTML),
        "/2026/20260102en": (500, ""),
    }

    with patch(
        "nrc_event_scraper.scraper.client.AsyncSession"
    ) as MockSession:
        mock_session = MagicMock()
        mock_session.get = AsyncMock(side_effect=_make_url_router("", routes))
        mock_session.close = AsyncMock()
        mock_session.headers = orch_settings.headers
        mock_session.impersonate = "chrome131"
        MockSession.return_value = mock_session

        orch = Orchestrator(orch_settings)
        stats = await orch.backfill(years=[2026])

    assert stats["pages_fetched"] == 1
    assert stats["errors"] == 1  # The 500 error page
    assert stats["events_found"] == 1

    # Error page should be marked in DB
    base = orch_settings.nrc_base_url
    error_page = orch.db.get_page(f"{base}/2026/20260102en")
    assert error_page["status"] == "error"


@pytest.mark.asyncio
async def test_backfill_force_refetch(orch_settings: Settings):
    """--force should re-fetch already processed pages."""
    routes = {
        "/2026/index.html": (200, YEAR_INDEX_HTML),
        "/2026/20260101en": (200, MODERN_PAGE_HTML),
        "/2026/20260102en": (200, EMPTY_PAGE_HTML),
    }

    orch = Orchestrator(orch_settings)

    # First run
    with patch(
        "nrc_event_scraper.scraper.client.AsyncSession"
    ) as MockSession:
        mock_session = MagicMock()
        mock_session.get = AsyncMock(side_effect=_make_url_router("", routes))
        mock_session.close = AsyncMock()
        mock_session.headers = orch_settings.headers
        mock_session.impersonate = "chrome131"
        MockSession.return_value = mock_session

        await orch.backfill(years=[2026])

    # Force re-run — should fetch again
    with patch(
        "nrc_event_scraper.scraper.client.AsyncSession"
    ) as MockSession:
        mock_session = MagicMock()
        mock_session.get = AsyncMock(side_effect=_make_url_router("", routes))
        mock_session.close = AsyncMock()
        mock_session.headers = orch_settings.headers
        mock_session.impersonate = "chrome131"
        MockSession.return_value = mock_session

        stats = await orch.backfill(years=[2026], force=True)
    assert stats["pages_fetched"] == 2


@pytest.mark.asyncio
async def test_html_archived_before_parsing(orch_settings: Settings):
    """Raw HTML should be archived before any parsing occurs."""
    routes = {
        "/2026/index.html": (200, YEAR_INDEX_HTML),
        "/2026/20260101en": (200, MODERN_PAGE_HTML),
        "/2026/20260102en": (200, EMPTY_PAGE_HTML),
    }

    with patch(
        "nrc_event_scraper.scraper.client.AsyncSession"
    ) as MockSession:
        mock_session = MagicMock()
        mock_session.get = AsyncMock(side_effect=_make_url_router("", routes))
        mock_session.close = AsyncMock()
        mock_session.headers = orch_settings.headers
        mock_session.impersonate = "chrome131"
        MockSession.return_value = mock_session

        orch = Orchestrator(orch_settings)
        await orch.backfill(years=[2026])

    base = orch_settings.nrc_base_url
    # Check archives exist
    assert orch.archive.exists(f"{base}/2026/20260101en")
    assert orch.archive.exists(f"{base}/2026/20260102en")

    # Verify content is recoverable
    archived = orch.archive.load(f"{base}/2026/20260101en")
    assert "TestPlant" in archived
