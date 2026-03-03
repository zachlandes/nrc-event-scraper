"""Tests for JSONL writer and HTML archive."""

from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from nrc_event_scraper.models import EventCategory, NRCEvent
from nrc_event_scraper.storage.html_archive import HTMLArchive
from nrc_event_scraper.storage.jsonl_writer import JSONLWriter


@pytest.fixture
def events_dir(tmp_path: Path) -> Path:
    d = tmp_path / "events"
    d.mkdir()
    return d


@pytest.fixture
def html_dir(tmp_path: Path) -> Path:
    d = tmp_path / "html"
    d.mkdir()
    return d


def _make_event(num: int, **kwargs) -> NRCEvent:
    return NRCEvent(
        event_number=num,
        category=kwargs.get("category", EventCategory.POWER_REACTOR),
        facility=kwargs.get("facility", "TestPlant"),
        report_date=kwargs.get("report_date", date(2026, 3, 3)),
        scraped_at=datetime.now(timezone.utc),
        **{k: v for k, v in kwargs.items() if k not in ("category", "facility", "report_date")},
    )


class TestJSONLWriter:
    def test_write_events(self, events_dir):
        writer = JSONLWriter(events_dir)
        events = [_make_event(58181), _make_event(58182)]
        written = writer.write_events(events, 2026)

        assert written == 2
        assert (events_dir / "2026.jsonl").exists()

    def test_write_events_dedup(self, events_dir):
        writer = JSONLWriter(events_dir)
        events = [_make_event(58181)]
        writer.write_events(events, 2026)
        # Write same event again
        written = writer.write_events(events, 2026)
        assert written == 0

        # File should have exactly 1 line
        lines = (events_dir / "2026.jsonl").read_text().strip().split("\n")
        assert len(lines) == 1

    def test_write_empty_list(self, events_dir):
        writer = JSONLWriter(events_dir)
        assert writer.write_events([], 2026) == 0

    def test_read_events(self, events_dir):
        writer = JSONLWriter(events_dir)
        events = [_make_event(58181), _make_event(58182)]
        writer.write_events(events, 2026)

        loaded = writer.read_events(2026)
        assert len(loaded) == 2
        assert loaded[0].event_number == 58181
        assert loaded[1].event_number == 58182

    def test_read_nonexistent_year(self, events_dir):
        writer = JSONLWriter(events_dir)
        assert writer.read_events(1999) == []

    def test_append_new_events(self, events_dir):
        writer = JSONLWriter(events_dir)
        writer.write_events([_make_event(58181)], 2026)
        writer.write_events([_make_event(58182)], 2026)

        loaded = writer.read_events(2026)
        assert len(loaded) == 2


class TestHTMLArchive:
    def test_save_and_load(self, html_dir):
        archive = HTMLArchive(html_dir)
        url = "https://www.nrc.gov/reading-rm/doc-collections/event-status/event/2026/20260303en"
        html = "<html><body>Test content</body></html>"

        path, sha256 = archive.save(html, url)
        assert path.exists()
        assert path.suffix == ".gz"
        assert len(sha256) == 64

        loaded = archive.load(url)
        assert loaded == html

    def test_exists(self, html_dir):
        archive = HTMLArchive(html_dir)
        url = "https://www.nrc.gov/reading-rm/doc-collections/event-status/event/2026/20260303en"

        assert not archive.exists(url)
        archive.save("<html>test</html>", url)
        assert archive.exists(url)

    def test_load_nonexistent(self, html_dir):
        archive = HTMLArchive(html_dir)
        url = "https://www.nrc.gov/reading-rm/doc-collections/event-status/event/2026/20260101en"
        assert archive.load(url) is None

    def test_year_partitioned_dirs(self, html_dir):
        archive = HTMLArchive(html_dir)
        url_2026 = "https://www.nrc.gov/reading-rm/doc-collections/event-status/event/2026/20260303en"
        url_2025 = "https://www.nrc.gov/reading-rm/doc-collections/event-status/event/2025/20250101en"

        archive.save("<html>2026</html>", url_2026)
        archive.save("<html>2025</html>", url_2025)

        assert (html_dir / "2026" / "20260303en.html.gz").exists()
        assert (html_dir / "2025" / "20250101en.html.gz").exists()

    def test_sha256_deterministic(self, html_dir):
        archive = HTMLArchive(html_dir)
        url = "https://www.nrc.gov/reading-rm/doc-collections/event-status/event/2026/20260303en"

        _, hash1 = archive.save("same content", url)
        _, hash2 = archive.save("same content", url)
        assert hash1 == hash2
