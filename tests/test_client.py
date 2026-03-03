"""Tests for NRC HTTP client."""


import httpx
import pytest
import respx

from nrc_event_scraper.config import Settings
from nrc_event_scraper.scraper.client import NRCClient


@pytest.fixture
def fast_settings(tmp_data_dir):
    """Settings with fast rate limits for testing."""
    return Settings(
        base_dir=tmp_data_dir,
        rate_limit_qps=100.0,  # fast for tests
        rate_limit_jitter=0.0,
        max_retries=2,
        retry_backoff_base=0.01,
    )


@respx.mock
@pytest.mark.asyncio
async def test_fetch_success(fast_settings):
    url = "https://www.nrc.gov/test"
    respx.get(url).respond(200, text="<html>OK</html>")

    async with NRCClient(fast_settings) as client:
        html, status, sha256 = await client.fetch(url)

    assert status == 200
    assert "<html>OK</html>" in html
    assert len(sha256) == 64


@respx.mock
@pytest.mark.asyncio
async def test_fetch_404(fast_settings):
    url = "https://www.nrc.gov/missing"
    respx.get(url).respond(404)

    async with NRCClient(fast_settings) as client:
        html, status, sha256 = await client.fetch(url)

    assert status == 404
    assert html == ""


@respx.mock
@pytest.mark.asyncio
async def test_fetch_retry_on_500(fast_settings):
    url = "https://www.nrc.gov/flaky"
    # First call: 500, second call: 200
    respx.get(url).side_effect = [
        httpx.Response(500),
        httpx.Response(200, text="<html>recovered</html>"),
    ]

    async with NRCClient(fast_settings) as client:
        html, status, sha256 = await client.fetch(url)

    assert status == 200
    assert "recovered" in html


@respx.mock
@pytest.mark.asyncio
async def test_fetch_retry_exhausted(fast_settings):
    url = "https://www.nrc.gov/down"
    respx.get(url).respond(500)

    async with NRCClient(fast_settings) as client:
        with pytest.raises(Exception):
            await client.fetch(url)


@respx.mock
@pytest.mark.asyncio
async def test_browser_headers_set(fast_settings):
    url = "https://www.nrc.gov/test"
    route = respx.get(url).respond(200, text="OK")

    async with NRCClient(fast_settings) as client:
        await client.fetch(url)

    request = route.calls[0].request
    assert "Chrome" in request.headers["user-agent"]
    assert "en-US" in request.headers["accept-language"]


@respx.mock
@pytest.mark.asyncio
async def test_sha256_deterministic(fast_settings):
    url = "https://www.nrc.gov/test"
    respx.get(url).respond(200, text="same content")

    async with NRCClient(fast_settings) as client:
        _, _, hash1 = await client.fetch(url)

    respx.reset()
    respx.get(url).respond(200, text="same content")

    async with NRCClient(fast_settings) as client:
        _, _, hash2 = await client.fetch(url)

    assert hash1 == hash2
