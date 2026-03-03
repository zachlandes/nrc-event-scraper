"""Click CLI for NRC Event Scraper.

Commands:
- backfill: Discover and scrape all pages for given years
- incremental: Scrape only new pages for the current year
- stats: Show scraper statistics
- validate: Compare scraped events against reconciliation file
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path

import click

from nrc_event_scraper.config import Settings
from nrc_event_scraper.db import ScraperDB
from nrc_event_scraper.orchestrator import Orchestrator


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


@click.group()
@click.option("--data-dir", type=click.Path(), default="data", help="Base data directory")
@click.option("-v", "--verbose", is_flag=True, help="Enable debug logging")
@click.pass_context
def cli(ctx: click.Context, data_dir: str, verbose: bool) -> None:
    """NRC Event Notification Reports Scraper."""
    _setup_logging(verbose)
    ctx.ensure_object(dict)
    ctx.obj["settings"] = Settings(base_dir=Path(data_dir))


@cli.command()
@click.option(
    "--years",
    type=str,
    default=None,
    help="Comma-separated years to backfill (e.g., '2024,2025,2026'). Default: all years.",
)
@click.option("--force", is_flag=True, help="Re-fetch and re-parse already processed pages")
@click.pass_context
def backfill(ctx: click.Context, years: str | None, force: bool) -> None:
    """Discover and scrape all pages for given years."""
    settings: Settings = ctx.obj["settings"]

    year_list: list[int] | None = None
    if years:
        year_list = [int(y.strip()) for y in years.split(",")]

    click.echo(f"Starting backfill (years={year_list or 'all'}, force={force})")
    orch = Orchestrator(settings)
    stats = asyncio.run(orch.backfill(years=year_list, force=force))

    click.echo("\nBackfill complete:")
    click.echo(f"  Pages discovered: {stats['pages_discovered']}")
    click.echo(f"  Pages fetched:    {stats['pages_fetched']}")
    click.echo(f"  Pages parsed:     {stats['pages_parsed']}")
    click.echo(f"  Events found:     {stats['events_found']}")
    click.echo(f"  Errors:           {stats['errors']}")


@cli.command()
@click.pass_context
def incremental(ctx: click.Context) -> None:
    """Scrape only new pages for the current year."""
    settings: Settings = ctx.obj["settings"]
    current_year = datetime.now(timezone.utc).year

    click.echo(f"Starting incremental scrape for {current_year}")
    orch = Orchestrator(settings)
    stats = asyncio.run(orch.incremental())

    click.echo("\nIncremental complete:")
    click.echo(f"  Pages fetched: {stats['pages_fetched']}")
    click.echo(f"  Pages parsed:  {stats['pages_parsed']}")
    click.echo(f"  Events found:  {stats['events_found']}")
    click.echo(f"  Errors:        {stats['errors']}")


@cli.command()
@click.pass_context
def stats(ctx: click.Context) -> None:
    """Show scraper statistics."""
    settings: Settings = ctx.obj["settings"]
    db = ScraperDB(settings.db_path)
    s = db.get_stats()

    click.echo("Scraper Statistics:")
    click.echo(f"  Total unique events: {s['total_unique_events']}")
    click.echo(f"  Completed runs:      {s['completed_runs']}")
    click.echo("  Pages by status:")
    for status, count in sorted(s["pages_by_status"].items()):
        click.echo(f"    {status}: {count}")

    # JSONL file sizes
    click.echo("\n  JSONL files:")
    events_dir = settings.events_dir
    if events_dir.exists():
        for jsonl_file in sorted(events_dir.glob("*.jsonl")):
            line_count = sum(1 for _ in open(jsonl_file))
            size_kb = jsonl_file.stat().st_size / 1024
            click.echo(f"    {jsonl_file.name}: {line_count} events ({size_kb:.1f} KB)")


@cli.command()
@click.argument("reconciliation_file", type=click.Path(exists=True))
@click.pass_context
def validate(ctx: click.Context, reconciliation_file: str) -> None:
    """Compare scraped events against NRC reconciliation file.

    The reconciliation file is a pipe-delimited TXT from NRC's monthly reports.
    """
    settings: Settings = ctx.obj["settings"]
    db = ScraperDB(settings.db_path)

    # Load reconciliation event numbers
    recon_numbers: set[int] = set()
    with open(reconciliation_file) as f:
        for line in f:
            parts = line.strip().split("|")
            if parts and parts[0].strip().isdigit():
                recon_numbers.add(int(parts[0].strip()))

    if not recon_numbers:
        click.echo("No event numbers found in reconciliation file.")
        return

    # Load scraped event numbers
    scraped_numbers = db.get_event_numbers()

    # Compare
    missing = recon_numbers - scraped_numbers
    extra = scraped_numbers - recon_numbers
    matched = recon_numbers & scraped_numbers

    click.echo("Reconciliation results:")
    click.echo(f"  Reconciliation file events: {len(recon_numbers)}")
    click.echo(f"  Scraped events:             {len(scraped_numbers)}")
    click.echo(f"  Matched:                    {len(matched)}")
    click.echo(f"  Missing from scrape:        {len(missing)}")
    click.echo(f"  Extra in scrape:            {len(extra)}")

    if missing:
        click.echo(f"\n  Missing event numbers (first 20): {sorted(missing)[:20]}")
    if extra:
        click.echo(f"\n  Extra event numbers (first 20): {sorted(extra)[:20]}")
