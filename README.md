# NRC Event Notification Reports Scraper

Scrapes [NRC Event Notification Reports](https://www.nrc.gov/reading-rm/doc-collections/event-status/event/index) (1999–present) into structured JSONL.

## Quick Start

```bash
# Install
pip install -e ".[dev]"

# Backfill a single year
nrc-scraper backfill --years 2026

# Incremental (current year only)
nrc-scraper incremental

# Check stats
nrc-scraper stats
```

## Output

- **JSONL**: `data/events/YYYY.jsonl` — one JSON object per event, year-partitioned
- **HTML archive**: `data/html/YYYY/YYYYMMDDen.html.gz` — raw HTML for reprocessing
- **SQLite DB**: `data/scraper.db` — operational state (fetch/parse status)

## Architecture

```
cli.py → orchestrator.py → scraper/client.py    (fetch with rate limiting)
                          → storage/html_archive (gzip raw HTML)
                          → parser/detect.py     (modern vs legacy vs empty)
                          → parser/modern_parser  (2020+ Drupal divs)
                          → parser/legacy_parser  (pre-2020 nested tables)
                          → storage/jsonl_writer  (deduped JSONL output)
```

## NRC Politeness Rules

- **< 1 req/sec** with jitter (default 0.5 qps)
- Fixed Chrome User-Agent (never changes mid-session)
- Exponential backoff on 429/5xx (4 retries max)
- Max 3 concurrent requests

## Development

```bash
pip install -e ".[dev]"
pytest -v
ruff check src/ tests/
```

## Configuration

All settings can be overridden via `NRC_`-prefixed environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `NRC_BASE_DIR` | `data` | Base data directory |
| `NRC_RATE_LIMIT_QPS` | `0.5` | Requests per second |
| `NRC_MAX_CONCURRENCY` | `3` | Max parallel requests |
| `NRC_MAX_RETRIES` | `4` | Retry attempts on failure |
| `NRC_START_YEAR` | `1999` | Backfill start year |
| `NRC_END_YEAR` | `2026` | Backfill end year |
