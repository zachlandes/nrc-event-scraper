"""Rate-limited HTTP client for NRC website.

Implements the strict politeness rules required to avoid IP bans:
- < 1 req/sec with jitter (token bucket at 0.5 qps default)
- Fixed browser-like User-Agent (NEVER changes mid-session)
- Exponential backoff retries on 429/5xx
- Concurrency limit via semaphore
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import random
import time

import httpx

from nrc_event_scraper.config import Settings

logger = logging.getLogger(__name__)


class RateLimitError(Exception):
    """Raised when we get a 429 response."""


class ServerError(Exception):
    """Raised on 5xx responses."""


class NRCClient:
    """Async HTTP client with rate limiting and retry for NRC website."""

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or Settings()
        self._semaphore = asyncio.Semaphore(self.settings.max_concurrency)
        self._last_request_time: float = 0.0
        self._lock = asyncio.Lock()

    async def __aenter__(self) -> NRCClient:
        self._http = httpx.AsyncClient(
            headers=self.settings.headers,
            timeout=httpx.Timeout(30.0, connect=10.0),
            follow_redirects=True,
        )
        return self

    async def __aexit__(self, *args) -> None:
        await self._http.aclose()

    async def _wait_for_rate_limit(self) -> None:
        """Token-bucket style rate limiter: wait until enough time has passed."""
        async with self._lock:
            now = time.monotonic()
            min_interval = 1.0 / self.settings.rate_limit_qps
            jitter = random.uniform(0, self.settings.rate_limit_jitter)
            elapsed = now - self._last_request_time
            wait_time = min_interval + jitter - elapsed

            if wait_time > 0:
                logger.debug("Rate limit: waiting %.2fs", wait_time)
                await asyncio.sleep(wait_time)

            self._last_request_time = time.monotonic()

    async def fetch(self, url: str) -> tuple[str, int, str]:
        """Fetch a URL with rate limiting and retry.

        Returns (html_content, status_code, sha256_hash).
        Raises on non-retryable errors.
        """
        async with self._semaphore:
            return await self._fetch_with_retry(url)

    async def _fetch_with_retry(self, url: str) -> tuple[str, int, str]:
        """Fetch with tenacity retry on 429/5xx."""
        last_error: Exception | None = None

        for attempt in range(self.settings.max_retries):
            await self._wait_for_rate_limit()

            try:
                response = await self._http.get(url)

                if response.status_code == 429:
                    wait = (self.settings.retry_backoff_base ** attempt) + random.uniform(0, 1)
                    logger.warning("429 rate limited on %s, backing off %.1fs", url, wait)
                    await asyncio.sleep(wait)
                    continue

                if response.status_code >= 500:
                    wait = (self.settings.retry_backoff_base ** attempt) + random.uniform(0, 1)
                    logger.warning(
                        "%d server error on %s, backing off %.1fs",
                        response.status_code,
                        url,
                        wait,
                    )
                    await asyncio.sleep(wait)
                    continue

                if response.status_code == 404:
                    logger.info("404 not found: %s", url)
                    return "", 404, ""

                response.raise_for_status()

                content = response.text
                sha256 = hashlib.sha256(content.encode()).hexdigest()
                return content, response.status_code, sha256

            except httpx.TimeoutException as e:
                last_error = e
                wait = (self.settings.retry_backoff_base ** attempt) + random.uniform(0, 1)
                logger.warning(
                    "Timeout on %s (attempt %d), backing off %.1fs",
                    url, attempt + 1, wait,
                )
                await asyncio.sleep(wait)
            except httpx.HTTPStatusError:
                raise
            except httpx.HTTPError as e:
                last_error = e
                wait = (self.settings.retry_backoff_base ** attempt) + random.uniform(0, 1)
                logger.warning("HTTP error on %s: %s", url, e)
                await asyncio.sleep(wait)

        raise last_error or RuntimeError(
            f"Failed to fetch {url} after {self.settings.max_retries} attempts"
        )
