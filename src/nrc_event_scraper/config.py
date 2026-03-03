"""Configuration via pydantic-settings with NRC_ env prefix."""

from __future__ import annotations

from pathlib import Path

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """NRC scraper configuration.

    All values can be overridden with NRC_ prefixed env vars.
    Example: NRC_RATE_LIMIT_QPS=0.25 lowers the request rate.
    """

    model_config = {"env_prefix": "NRC_"}

    # ── Paths ──────────────────────────────────────────────
    base_dir: Path = Path("data")

    @property
    def html_dir(self) -> Path:
        return self.base_dir / "html"

    @property
    def events_dir(self) -> Path:
        return self.base_dir / "events"

    @property
    def db_path(self) -> Path:
        return self.base_dir / "scraper.db"

    # ── Rate limiting ──────────────────────────────────────
    rate_limit_qps: float = 0.5  # < 1 req/sec hard rule
    rate_limit_jitter: float = 0.5  # seconds of random jitter added
    max_concurrency: int = 3  # asyncio.Semaphore cap

    # ── Retry ──────────────────────────────────────────────
    max_retries: int = 4
    retry_backoff_base: float = 2.0  # exponential base in seconds

    # ── HTTP headers (NEVER change UA mid-session) ─────────
    user_agent: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    )
    accept_encoding: str = "gzip, deflate, br"
    accept_language: str = "en-US,en;q=0.9"
    referer: str = "https://www.nrc.gov/"

    # ── NRC URLs ───────────────────────────────────────────
    nrc_base_url: str = "https://www.nrc.gov/reading-rm/doc-collections/event-status/event"

    # ── Scrape range ───────────────────────────────────────
    start_year: int = 1999
    end_year: int = 2026

    @property
    def headers(self) -> dict[str, str]:
        return {
            "User-Agent": self.user_agent,
            "Accept-Encoding": self.accept_encoding,
            "Accept-Language": self.accept_language,
            "Referer": self.referer,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
