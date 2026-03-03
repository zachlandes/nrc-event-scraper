"""Orchestrator: coordinates fetch → archive → detect → parse → store.

Two modes:
- Backfill: discover all year indexes → find all daily pages → fetch/parse any pending
- Incremental: check current year index → fetch/parse only new pages

Idempotent via SQLite state: pages already fetched/parsed are skipped.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from nrc_event_scraper.config import Settings
from nrc_event_scraper.db import ScraperDB
from nrc_event_scraper.parser.detect import detect_format
from nrc_event_scraper.parser.legacy_parser import parse_legacy_page
from nrc_event_scraper.parser.modern_parser import parse_modern_page
from nrc_event_scraper.scraper.client import NRCClient
from nrc_event_scraper.scraper.index_scraper import (
    extract_daily_page_urls,
    url_to_report_date,
)
from nrc_event_scraper.storage.html_archive import HTMLArchive
from nrc_event_scraper.storage.jsonl_writer import JSONLWriter

logger = logging.getLogger(__name__)


class Orchestrator:
    """Coordinates the full scrape pipeline."""

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or Settings()
        self.db = ScraperDB(self.settings.db_path)
        self.archive = HTMLArchive(self.settings.html_dir)
        self.writer = JSONLWriter(self.settings.events_dir)

    async def backfill(
        self,
        years: list[int] | None = None,
        force: bool = False,
    ) -> dict:
        """Backfill: discover all pages for given years, fetch and parse pending ones.

        Args:
            years: Specific years to backfill. None = all years in config range.
            force: If True, reset already-processed pages and re-fetch.

        Returns summary stats dict.
        """
        if years is None:
            years = list(range(self.settings.start_year, self.settings.end_year + 1))

        run_id = self.db.start_run("backfill")
        stats = {
            "pages_discovered": 0, "pages_fetched": 0,
            "pages_parsed": 0, "events_found": 0, "errors": 0,
        }

        try:
            async with NRCClient(self.settings) as client:
                # Phase 1: Discover daily page URLs from year indexes
                for year in years:
                    logger.info("Discovering pages for year %d", year)
                    discovered = await self._discover_year(client, year)
                    stats["pages_discovered"] += discovered

                    if force:
                        for page in self.db.get_all_pages(year):
                            self.db.reset_page(page["url"])

                # Phase 2: Fetch pending pages
                for year in years:
                    pending = self.db.get_pending_pages(year)
                    logger.info("Year %d: %d pages to fetch", year, len(pending))

                    for page in pending:
                        success = await self._fetch_page(client, page["url"])
                        if success:
                            stats["pages_fetched"] += 1
                        else:
                            stats["errors"] += 1

                # Phase 3: Parse fetched pages
                for year in years:
                    unparsed = self.db.get_fetched_unparsed(year)
                    logger.info("Year %d: %d pages to parse", year, len(unparsed))

                    for page in unparsed:
                        events_count = self._parse_page(page["url"], year)
                        if events_count >= 0:
                            stats["pages_parsed"] += 1
                            stats["events_found"] += events_count
                        else:
                            stats["errors"] += 1

            self.db.finish_run(
                run_id,
                pages_fetched=stats["pages_fetched"],
                pages_parsed=stats["pages_parsed"],
                events_found=stats["events_found"],
                errors=stats["errors"],
            )
        except Exception as e:
            logger.error("Backfill failed: %s", e)
            self.db.finish_run(run_id, errors=stats["errors"], status="failed")
            raise

        return stats

    async def incremental(self) -> dict:
        """Incremental: check current year for new pages, fetch and parse them."""
        current_year = datetime.now(timezone.utc).year
        return await self.backfill(years=[current_year])

    async def _discover_year(self, client: NRCClient, year: int) -> int:
        """Fetch year index and register discovered daily page URLs in the DB."""
        index_url = f"{self.settings.nrc_base_url}/{year}/index.html"

        try:
            html, status, _ = await client.fetch(index_url)
        except Exception as e:
            logger.error("Failed to fetch year index %d: %s", year, e)
            return 0

        if status == 404 or not html:
            logger.warning("Year index %d returned %d", year, status)
            return 0

        urls = extract_daily_page_urls(html, self.settings.nrc_base_url, year)
        for url in urls:
            report_date = url_to_report_date(url)
            self.db.upsert_page(url, year, report_date)

        logger.info("Year %d: discovered %d daily pages", year, len(urls))
        return len(urls)

    async def _fetch_page(self, client: NRCClient, url: str) -> bool:
        """Fetch a single page, archive it, and update DB status."""
        try:
            html, status, sha256 = await client.fetch(url)

            if status == 404 or not html:
                self.db.mark_page_error(url, f"HTTP {status}")
                return False

            # Archive raw HTML before parsing
            self.archive.save(html, url)

            # Detect format
            fmt = detect_format(html)
            self.db.mark_page_fetched(url, sha256, fmt)
            return True

        except Exception as e:
            logger.error("Failed to fetch %s: %s", url, e)
            self.db.mark_page_error(url, str(e))
            return False

    def _parse_page(self, url: str, year: int) -> int:
        """Parse an archived page and store events. Returns event count or -1 on error."""
        html = self.archive.load(url)
        if not html:
            self.db.mark_page_error(url, "Archived HTML not found")
            return -1

        fmt = detect_format(html)

        try:
            if fmt == "modern":
                report = parse_modern_page(html, page_url=url)
            elif fmt == "legacy":
                report = parse_legacy_page(html, page_url=url)
            elif fmt == "empty":
                self.db.mark_page_parsed(url, event_count=0, html_format="empty")
                return 0
            else:
                self.db.mark_page_error(url, f"Unknown format: {fmt}")
                return -1
        except Exception as e:
            logger.error("Parse error for %s: %s", url, e)
            self.db.mark_page_error(url, f"Parse error: {e}")
            return -1

        # Stamp scraped_at on all events
        now = datetime.now(timezone.utc)
        for event in report.events:
            event.scraped_at = now

        # Store events
        written = self.writer.write_events(report.events, year)

        # Register events in DB
        for event in report.events:
            self.db.upsert_event(event.event_number, url, event.category.value)

        self.db.mark_page_parsed(url, event_count=len(report.events), html_format=fmt)

        if report.parse_errors:
            logger.warning("Parse warnings for %s: %s", url, report.parse_errors)

        logger.info(
            "Parsed %s: %d events (%d new written)", url, len(report.events), written
        )
        return len(report.events)
