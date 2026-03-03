"""Tests for SQLite DAL."""

from nrc_event_scraper.db import ScraperDB


def test_schema_creates_tables(db: ScraperDB):
    """Schema init should create all three tables."""
    with db._conn() as conn:
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        names = {r["name"] for r in tables}
    assert "pages" in names
    assert "events" in names
    assert "scrape_runs" in names


def test_upsert_page(db: ScraperDB):
    db.upsert_page("https://example.com/page1", year=2026, report_date="2026-03-03")
    page = db.get_page("https://example.com/page1")
    assert page is not None
    assert page["year"] == 2026
    assert page["status"] == "pending"


def test_upsert_page_idempotent(db: ScraperDB):
    db.upsert_page("https://example.com/page1", year=2026)
    db.upsert_page("https://example.com/page1", year=2026)  # no-op
    pages = db.get_all_pages()
    assert len(pages) == 1


def test_page_lifecycle(db: ScraperDB):
    url = "https://example.com/page1"
    db.upsert_page(url, year=2026)

    # Fetch
    db.mark_page_fetched(url, html_sha256="abc123", html_format="modern")
    page = db.get_page(url)
    assert page["status"] == "fetched"
    assert page["html_sha256"] == "abc123"

    # Parse
    db.mark_page_parsed(url, event_count=3, html_format="modern")
    page = db.get_page(url)
    assert page["status"] == "parsed"
    assert page["event_count"] == 3


def test_mark_page_error(db: ScraperDB):
    url = "https://example.com/page1"
    db.upsert_page(url, year=2026)
    db.mark_page_error(url, "HTTP 500")
    page = db.get_page(url)
    assert page["status"] == "error"
    assert page["error_msg"] == "HTTP 500"


def test_get_pending_pages(db: ScraperDB):
    db.upsert_page("https://example.com/a", year=2025)
    db.upsert_page("https://example.com/b", year=2026)
    db.upsert_page("https://example.com/c", year=2026)
    db.mark_page_fetched("https://example.com/c", html_sha256="x")

    pending_2026 = db.get_pending_pages(year=2026)
    assert len(pending_2026) == 1
    assert pending_2026[0]["url"] == "https://example.com/b"

    all_pending = db.get_pending_pages()
    assert len(all_pending) == 2


def test_get_fetched_unparsed(db: ScraperDB):
    db.upsert_page("https://example.com/a", year=2026)
    db.upsert_page("https://example.com/b", year=2026)
    db.mark_page_fetched("https://example.com/a", html_sha256="x")
    db.mark_page_fetched("https://example.com/b", html_sha256="y")
    db.mark_page_parsed("https://example.com/b", event_count=2, html_format="modern")

    unparsed = db.get_fetched_unparsed()
    assert len(unparsed) == 1
    assert unparsed[0]["url"] == "https://example.com/a"


def test_reset_page(db: ScraperDB):
    url = "https://example.com/page1"
    db.upsert_page(url, year=2026)
    db.mark_page_fetched(url, html_sha256="abc")
    db.reset_page(url)
    page = db.get_page(url)
    assert page["status"] == "pending"
    assert page["html_sha256"] is None


def test_upsert_event(db: ScraperDB):
    url = "https://example.com/page1"
    db.upsert_page(url, year=2026)
    db.upsert_event(58181, url, "Power Reactor")
    db.upsert_event(58180, url, "Material")

    numbers = db.get_event_numbers(url)
    assert numbers == {58181, 58180}


def test_upsert_event_idempotent(db: ScraperDB):
    url = "https://example.com/page1"
    db.upsert_page(url, year=2026)
    db.upsert_event(58181, url, "Power Reactor")
    db.upsert_event(58181, url, "Power Reactor")  # should update, not duplicate

    numbers = db.get_event_numbers(url)
    assert len(numbers) == 1


def test_count_events(db: ScraperDB):
    db.upsert_page("https://example.com/a", year=2026)
    db.upsert_page("https://example.com/b", year=2026)
    db.upsert_event(58181, "https://example.com/a", "Power Reactor")
    db.upsert_event(58181, "https://example.com/b", "Power Reactor")  # same event, diff page
    db.upsert_event(58180, "https://example.com/a", "Material")

    assert db.count_events() == 2  # distinct event numbers


def test_scrape_run_lifecycle(db: ScraperDB):
    run_id = db.start_run("backfill")
    assert run_id is not None

    db.finish_run(run_id, pages_fetched=10, pages_parsed=9, events_found=42, errors=1)
    stats = db.get_stats()
    assert stats["completed_runs"] == 1


def test_get_stats(db: ScraperDB):
    db.upsert_page("https://example.com/a", year=2026)
    db.upsert_page("https://example.com/b", year=2026)
    db.mark_page_fetched("https://example.com/a", html_sha256="x")
    db.mark_page_parsed("https://example.com/b", event_count=2, html_format="modern")
    db.upsert_event(58181, "https://example.com/b", "Power Reactor")

    stats = db.get_stats()
    assert stats["pages_by_status"]["fetched"] == 1
    assert stats["pages_by_status"]["parsed"] == 1
    assert stats["total_unique_events"] == 1
