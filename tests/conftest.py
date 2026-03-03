"""Shared test fixtures."""

from __future__ import annotations

from pathlib import Path

import pytest

from nrc_event_scraper.config import Settings
from nrc_event_scraper.db import ScraperDB


@pytest.fixture
def tmp_data_dir(tmp_path: Path) -> Path:
    return tmp_path / "data"


@pytest.fixture
def settings(tmp_data_dir: Path) -> Settings:
    return Settings(base_dir=tmp_data_dir)


@pytest.fixture
def db(tmp_data_dir: Path) -> ScraperDB:
    tmp_data_dir.mkdir(parents=True, exist_ok=True)
    return ScraperDB(tmp_data_dir / "test.db")


@pytest.fixture
def fixtures_dir() -> Path:
    return Path(__file__).parent / "fixtures"
