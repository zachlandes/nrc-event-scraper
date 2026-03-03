"""Rate-limited HTTP client for NRC website.

Implements the strict politeness rules required to avoid IP bans:
- < 1 req/sec with jitter (token bucket at 0.5 qps default)
- Fixed browser-like User-Agent (NEVER changes mid-session)
- Exponential backoff retries on 429/5xx
- Concurrency limit via semaphore

Uses curl_cffi with Chrome TLS fingerprint impersonation to bypass
Akamai CDN's TLS fingerprinting (which silently drops httpx/urllib connections).
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import random
import time

from curl_cffi import CurlError
from curl_cffi.requests import AsyncSession, Response

from nrc_event_scraper.config import Settings

logger = logging.getLogger(__name__)


class RateLimitError(Exception):
    """Raised when we get a 429 response."""


class ServerError(Exception):
    """Raised on 5xx responses."""


class NRCClient:
    """Async HTTP client with rate limiting and retry for NRC website.

    Uses curl_cffi to impersonate Chrome's TLS fingerprint, which is required
    to connect to NRC's Akamai CDN without being silently dropped.
    """

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or Settings()
        self._semaphore = asyncio.Semaphore(self.settings.max_concurrency)
        self._last_request_time: float = 0.0
        self._lock = asyncio.Lock()

    async def __aenter__(self) -> NRCClient:
        self._session = AsyncSession(
            headers=self.settings.headers,
            impersonate="chrome131",
            timeout=(self.settings.connect_timeout, self.settings.read_timeout),
            allow_redirects=True,
        )
        return self

    async def __aexit__(self, *args) -> None:
        await self._session.close()

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
        """Fetch with retry on 429/5xx/timeout."""
        last_error: Exception | None = None

        for attempt in range(self.settings.max_retries):
            await self._wait_for_rate_limit()

            try:
                response: Response = await self._session.get(url)

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

                if response.status_code >= 400:
                    raise CurlError(
                        f"HTTP {response.status_code} for {url}"
                    )

                content = response.text
                sha256 = hashlib.sha256(content.encode()).hexdigest()
                return content, response.status_code, sha256

            except CurlError as e:
                last_error = e
                wait = (self.settings.retry_backoff_base ** attempt) + random.uniform(0, 1)
                logger.warning(
                    "Request error on %s (attempt %d): %s, backing off %.1fs",
                    url, attempt + 1, e, wait,
                )
                await asyncio.sleep(wait)

        raise last_error or RuntimeError(
            f"Failed to fetch {url} after {self.settings.max_retries} attempts"
        )
