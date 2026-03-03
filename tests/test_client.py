"""Tests for NRC HTTP client (curl_cffi based)."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

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


def _mock_response(status_code=200, text="<html>OK</html>"):
    """Create a mock curl_cffi Response."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.text = text
    return resp


@pytest.mark.asyncio
async def test_fetch_success(fast_settings):
    mock_resp = _mock_response(200, "<html>OK</html>")

    async with NRCClient(fast_settings) as client:
        with patch.object(client._session, "get", new_callable=AsyncMock, return_value=mock_resp):
            html, status, sha256 = await client.fetch("https://www.nrc.gov/test")

    assert status == 200
    assert "<html>OK</html>" in html
    assert len(sha256) == 64


@pytest.mark.asyncio
async def test_fetch_404(fast_settings):
    mock_resp = _mock_response(404, "")

    async with NRCClient(fast_settings) as client:
        with patch.object(client._session, "get", new_callable=AsyncMock, return_value=mock_resp):
            html, status, sha256 = await client.fetch("https://www.nrc.gov/missing")

    assert status == 404
    assert html == ""


@pytest.mark.asyncio
async def test_fetch_retry_on_500(fast_settings):
    resp_500 = _mock_response(500)
    resp_200 = _mock_response(200, "<html>recovered</html>")

    async with NRCClient(fast_settings) as client:
        mock_get = AsyncMock(side_effect=[resp_500, resp_200])
        with patch.object(client._session, "get", mock_get):
            html, status, sha256 = await client.fetch("https://www.nrc.gov/flaky")

    assert status == 200
    assert "recovered" in html


@pytest.mark.asyncio
async def test_fetch_retry_exhausted(fast_settings):
    resp_500 = _mock_response(500)

    async with NRCClient(fast_settings) as client:
        mock_get = AsyncMock(return_value=resp_500)
        with patch.object(client._session, "get", mock_get):
            with pytest.raises(Exception):
                await client.fetch("https://www.nrc.gov/down")


@pytest.mark.asyncio
async def test_session_uses_chrome_impersonation(fast_settings):
    """Verify the session is created with Chrome TLS impersonation."""
    async with NRCClient(fast_settings) as client:
        # curl_cffi AsyncSession stores impersonate as an attribute
        assert client._session.impersonate == "chrome131"


@pytest.mark.asyncio
async def test_sha256_deterministic(fast_settings):
    content = "same content"
    mock_resp = _mock_response(200, content)

    async with NRCClient(fast_settings) as client:
        with patch.object(client._session, "get", new_callable=AsyncMock, return_value=mock_resp):
            _, _, hash1 = await client.fetch("https://www.nrc.gov/test")

    async with NRCClient(fast_settings) as client:
        with patch.object(client._session, "get", new_callable=AsyncMock, return_value=mock_resp):
            _, _, hash2 = await client.fetch("https://www.nrc.gov/test")

    assert hash1 == hash2


@pytest.mark.asyncio
async def test_headers_include_browser_ua(fast_settings):
    """Verify browser-like headers are configured."""
    async with NRCClient(fast_settings) as client:
        headers = dict(client._session.headers)
        # curl_cffi lowercases header keys
        assert "Chrome" in headers.get("user-agent", headers.get("User-Agent", ""))
        assert "en-US" in headers.get("accept-language", headers.get("Accept-Language", ""))
