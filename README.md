# NRC Event Notification Reports Scraper

Scrapes [NRC Event Notification Reports](https://www.nrc.gov/reading-rm/doc-collections/event-status/event/index) (1999–present) into structured JSONL.

Pre-scraped data is committed to `data/events/` — you can use it without running the scraper yourself.

## Setup

Requires Python 3.11+ and [uv](https://docs.astral.sh/uv/getting-started/installation/).

```bash
git clone https://github.com/YOUR_USERNAME/nrc-event-scraper.git
cd nrc-event-scraper
uv sync --all-extras
```

## Usage

```bash
# Backfill a single year
uv run nrc-scraper backfill --years 2026

# Backfill all years (1999–2026, takes several hours)
uv run nrc-scraper backfill

# Incremental update (current year only)
uv run nrc-scraper incremental

# Check stats
uv run nrc-scraper stats

# Run tests
uv run pytest -v
```

## Automated Daily Scrape (GitHub Actions)

This repo includes a GitHub Actions workflow that runs the scraper daily and commits new events automatically. To use it:

1. Fork this repo
2. Go to your fork's **Settings > Actions > General** and enable workflows
3. The scraper runs daily at 6:00 AM UTC (1:00 AM ET)
4. You can also trigger it manually from **Actions > NRC Event Scraper > Run workflow**

No API keys, servers, or configuration needed — it runs entirely on GitHub's free CI.

## Output

- **JSONL**: `data/events/YYYY.jsonl` — one JSON object per event, year-partitioned
- **HTML archive**: `data/html/YYYY/YYYYMMDDen.html.gz` — raw HTML for reprocessing (local only, not in git)
- **SQLite DB**: `data/scraper.db` — operational state (fetch/parse status)

### Sample Event (Power Reactor)

```json
{
  "event_number": 58104,
  "category": "Power Reactor",
  "facility": "Hatch",
  "region": "2",
  "state": "GA",
  "unit": "[1] [] []",
  "rx_type": "[1] GE-4,[2] GE-4",
  "notification_date": "2025-12-31",
  "notification_time": "10:49",
  "notification_timezone": "ET",
  "emergency_class": "Non Emergency",
  "cfr_sections": [
    {"code": "50.73(a)(1)", "description": "Invalid Specif System Act"}
  ],
  "event_text": "DEGRADED CONDITION - PRIMARY CONTAINMENT ..."
}
```

## Architecture

```
cli.py → orchestrator.py → scraper/client.py    (fetch with rate limiting + TLS impersonation)
                          → storage/html_archive (gzip raw HTML)
                          → parser/detect.py     (modern vs legacy vs empty)
                          → parser/modern_parser  (2020+ Drupal divs)
                          → parser/legacy_parser  (pre-2020 nested tables)
                          → storage/jsonl_writer  (deduped JSONL output)
```

The HTTP client uses `curl_cffi` with Chrome TLS fingerprint impersonation to bypass NRC's Akamai CDN, which silently drops connections from standard Python HTTP clients (httpx, urllib, requests).

## NRC Politeness Rules

- **< 1 req/sec** with jitter (default 0.5 qps)
- Fixed Chrome User-Agent (never changes mid-session)
- Exponential backoff on 429/5xx (4 retries max)
- Sequential requests (max concurrency: 1)

## Configuration

All settings can be overridden via `NRC_`-prefixed environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `NRC_BASE_DIR` | `data` | Base data directory |
| `NRC_RATE_LIMIT_QPS` | `0.5` | Requests per second |
| `NRC_MAX_CONCURRENCY` | `1` | Max parallel requests |
| `NRC_MAX_RETRIES` | `4` | Retry attempts on failure |
| `NRC_START_YEAR` | `1999` | Backfill start year |
| `NRC_END_YEAR` | `2026` | Backfill end year |
