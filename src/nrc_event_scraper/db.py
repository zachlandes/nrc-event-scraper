"""SQLite schema + data access layer for scraper state tracking.

Three tables:
- pages: tracks fetch/parse status for each daily page URL
- events: tracks extracted event numbers per page (for dedup)
- scrape_runs: audit log of backfill/incremental runs
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

SCHEMA = """
CREATE TABLE IF NOT EXISTS pages (
    url             TEXT PRIMARY KEY,
    year            INTEGER NOT NULL,
    report_date     TEXT,          -- YYYY-MM-DD
    status          TEXT NOT NULL DEFAULT 'pending',  -- pending|fetched|parsed|error
    html_format     TEXT,          -- modern|legacy|empty|unknown
    html_sha256     TEXT,
    fetch_ts        TEXT,
    parse_ts        TEXT,
    error_msg       TEXT,
    event_count     INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS events (
    event_number    INTEGER NOT NULL,
    page_url        TEXT NOT NULL,
    category        TEXT,
    scraped_at      TEXT NOT NULL,
    UNIQUE(event_number, page_url)
);

CREATE TABLE IF NOT EXISTS scrape_runs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    run_type        TEXT NOT NULL,   -- backfill|incremental
    started_at      TEXT NOT NULL,
    finished_at     TEXT,
    pages_fetched   INTEGER DEFAULT 0,
    pages_parsed    INTEGER DEFAULT 0,
    events_found    INTEGER DEFAULT 0,
    errors          INTEGER DEFAULT 0,
    status          TEXT DEFAULT 'running'  -- running|completed|failed
);

CREATE INDEX IF NOT EXISTS idx_pages_status ON pages(status);
CREATE INDEX IF NOT EXISTS idx_pages_year ON pages(year);
CREATE INDEX IF NOT EXISTS idx_events_number ON events(event_number);
"""


class ScraperDB:
    """Synchronous SQLite DAL for scraper state."""

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    @contextmanager
    def _conn(self) -> Iterator[sqlite3.Connection]:
        conn = self._connect()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_schema(self) -> None:
        with self._conn() as conn:
            conn.executescript(SCHEMA)

    # ── Page operations ────────────────────────────────────

    def upsert_page(self, url: str, year: int, report_date: str | None = None) -> None:
        """Insert a page URL if it doesn't exist yet."""
        with self._conn() as conn:
            conn.execute(
                """INSERT INTO pages (url, year, report_date)
                   VALUES (?, ?, ?)
                   ON CONFLICT(url) DO NOTHING""",
                (url, year, report_date),
            )

    def mark_page_fetched(
        self, url: str, html_sha256: str, html_format: str | None = None
    ) -> None:
        with self._conn() as conn:
            conn.execute(
                """UPDATE pages
                   SET status = 'fetched', html_sha256 = ?, html_format = ?,
                       fetch_ts = ?
                   WHERE url = ?""",
                (html_sha256, html_format, _now_iso(), url),
            )

    def mark_page_parsed(self, url: str, event_count: int, html_format: str) -> None:
        with self._conn() as conn:
            conn.execute(
                """UPDATE pages
                   SET status = 'parsed', event_count = ?, html_format = ?,
                       parse_ts = ?
                   WHERE url = ?""",
                (event_count, html_format, _now_iso(), url),
            )

    def mark_page_error(self, url: str, error_msg: str) -> None:
        with self._conn() as conn:
            conn.execute(
                """UPDATE pages SET status = 'error', error_msg = ? WHERE url = ?""",
                (error_msg, url),
            )

    def get_pending_pages(self, year: int | None = None) -> list[dict]:
        """Return pages not yet fetched. Optionally filter by year."""
        with self._conn() as conn:
            if year is not None:
                rows = conn.execute(
                    "SELECT * FROM pages WHERE status = 'pending' AND year = ? ORDER BY url",
                    (year,),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM pages WHERE status = 'pending' ORDER BY url"
                ).fetchall()
            return [dict(r) for r in rows]

    def get_fetched_unparsed(self, year: int | None = None) -> list[dict]:
        """Return pages fetched but not yet parsed."""
        with self._conn() as conn:
            if year is not None:
                rows = conn.execute(
                    "SELECT * FROM pages WHERE status = 'fetched' AND year = ? ORDER BY url",
                    (year,),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM pages WHERE status = 'fetched' ORDER BY url"
                ).fetchall()
            return [dict(r) for r in rows]

    def get_page(self, url: str) -> dict | None:
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM pages WHERE url = ?", (url,)).fetchone()
            return dict(row) if row else None

    def get_all_pages(self, year: int | None = None) -> list[dict]:
        with self._conn() as conn:
            if year is not None:
                rows = conn.execute(
                    "SELECT * FROM pages WHERE year = ? ORDER BY url", (year,)
                ).fetchall()
            else:
                rows = conn.execute("SELECT * FROM pages ORDER BY url").fetchall()
            return [dict(r) for r in rows]

    def reset_page(self, url: str) -> None:
        """Reset a page to pending (for --force re-scrape)."""
        with self._conn() as conn:
            conn.execute(
                """UPDATE pages
                   SET status = 'pending', html_sha256 = NULL, html_format = NULL,
                       fetch_ts = NULL, parse_ts = NULL, error_msg = NULL, event_count = 0
                   WHERE url = ?""",
                (url,),
            )

    # ── Event operations ───────────────────────────────────

    def upsert_event(self, event_number: int, page_url: str, category: str) -> None:
        with self._conn() as conn:
            conn.execute(
                """INSERT INTO events (event_number, page_url, category, scraped_at)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT(event_number, page_url) DO UPDATE
                   SET category = excluded.category, scraped_at = excluded.scraped_at""",
                (event_number, page_url, category, _now_iso()),
            )

    def get_event_numbers(self, page_url: str | None = None) -> set[int]:
        with self._conn() as conn:
            if page_url:
                rows = conn.execute(
                    "SELECT event_number FROM events WHERE page_url = ?", (page_url,)
                ).fetchall()
            else:
                rows = conn.execute("SELECT DISTINCT event_number FROM events").fetchall()
            return {r["event_number"] for r in rows}

    def count_events(self) -> int:
        with self._conn() as conn:
            row = conn.execute("SELECT COUNT(DISTINCT event_number) as cnt FROM events").fetchone()
            return row["cnt"] if row else 0

    # ── Scrape run operations ──────────────────────────────

    def start_run(self, run_type: str) -> int:
        with self._conn() as conn:
            cursor = conn.execute(
                "INSERT INTO scrape_runs (run_type, started_at) VALUES (?, ?)",
                (run_type, _now_iso()),
            )
            return cursor.lastrowid  # type: ignore[return-value]

    def finish_run(
        self,
        run_id: int,
        *,
        pages_fetched: int = 0,
        pages_parsed: int = 0,
        events_found: int = 0,
        errors: int = 0,
        status: str = "completed",
    ) -> None:
        with self._conn() as conn:
            conn.execute(
                """UPDATE scrape_runs
                   SET finished_at = ?, pages_fetched = ?, pages_parsed = ?,
                       events_found = ?, errors = ?, status = ?
                   WHERE id = ?""",
                (_now_iso(), pages_fetched, pages_parsed, events_found, errors, status, run_id),
            )

    def get_stats(self) -> dict:
        """Return summary statistics."""
        with self._conn() as conn:
            pages = conn.execute(
                """SELECT status, COUNT(*) as cnt FROM pages GROUP BY status"""
            ).fetchall()
            total_events = self.count_events()
            runs = conn.execute(
                "SELECT COUNT(*) as cnt FROM scrape_runs WHERE status = 'completed'"
            ).fetchone()
            return {
                "pages_by_status": {r["status"]: r["cnt"] for r in pages},
                "total_unique_events": total_events,
                "completed_runs": runs["cnt"] if runs else 0,
            }


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
