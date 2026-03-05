"""
Fetch full Swiss ISTDATEN daily CSV files from opentransportdata.swiss.

This script downloads the latest N daily files (default: 2) from:
https://data.opentransportdata.swiss/en/dataset/istdaten

Each file is large (~500MB/day). Files are written to `data/raw/full/`.

Usage:
    uv run python fetch_raw.py
    uv run python fetch_raw.py --days 1
"""

import argparse
import logging
import re
import sys
from pathlib import Path
from urllib.parse import urljoin

import requests
from requests import Response

DATASET_PAGE_URL = "https://data.opentransportdata.swiss/en/dataset/istdaten"
DOWNLOAD_LINK_PATTERN = re.compile(
    r'href="(?P<link>[^"]*/download/(?P<date>\d{4}-\d{2}-\d{2})_istdaten\.csv)"',
    re.IGNORECASE,
)

RAW_DIR = Path("data/raw/full")
RAW_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("fetch_raw")


def fetch_dataset_page() -> str:
    """Return the HTML dataset page that lists all daily resources."""
    resp = requests.get(DATASET_PAGE_URL, timeout=60)
    resp.raise_for_status()
    return resp.text


def extract_latest_downloads(html: str, days: int) -> list[tuple[str, str]]:
    """
    Extract download links from HTML and return latest `days` unique dates.

    Returns list of tuples: (date, absolute_download_url)
    """
    matches: dict[str, str] = {}
    for m in DOWNLOAD_LINK_PATTERN.finditer(html):
        date_str = m.group("date")
        rel_or_abs_link = m.group("link")
        matches[date_str] = urljoin(DATASET_PAGE_URL, rel_or_abs_link)

    if not matches:
        raise RuntimeError("Could not find any daily download links on the dataset page.")

    latest_dates = sorted(matches.keys(), reverse=True)[:days]
    return [(d, matches[d]) for d in latest_dates]


def download_stream(resp: Response, destination: Path) -> None:
    """Download response body to destination using chunked streaming."""
    with destination.open("wb") as f:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)


def download_day(date_str: str, download_url: str) -> Path:
    out = RAW_DIR / f"{date_str}_istdaten.csv"
    if out.exists() and out.stat().st_size > 0:
        log.info("Skipping %s (already exists: %s)", date_str, out)
        return out

    log.info("Downloading %s", download_url)
    with requests.get(download_url, stream=True, timeout=120) as resp:
        resp.raise_for_status()
        download_stream(resp, out)

    log.info("Saved %s (%d bytes)", out, out.stat().st_size)
    return out


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download latest full ISTDATEN daily CSV files.")
    parser.add_argument(
        "--days",
        type=int,
        default=2,
        help="How many latest daily files to download (default: 2).",
    )
    parser.add_argument(
        "--list-only",
        action="store_true",
        help="Only print which daily files would be downloaded.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.days < 1:
        log.error("--days must be >= 1")
        sys.exit(1)

    html = fetch_dataset_page()
    targets = extract_latest_downloads(html=html, days=args.days)
    if args.list_only:
        log.info("Available targets:")
        for date_str, url in targets:
            log.info("  %s -> %s", date_str, url)
        return

    saved: list[Path] = []
    for date_str, url in targets:
        try:
            saved.append(download_day(date_str, url))
        except Exception as exc:
            log.error("Failed to download %s: %s", date_str, exc)

    if not saved:
        log.error("No files were downloaded.")
        sys.exit(1)

    log.info("Done. Downloaded %d file(s).", len(saved))


if __name__ == "__main__":
    main()
