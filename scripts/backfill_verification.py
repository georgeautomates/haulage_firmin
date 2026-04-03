"""
One-off script: scrape Proteo for all jobs in Actual Entry that are missing
from the Verification sheet, and write them in.

Usage:
    python scripts/backfill_verification.py
    python scripts/backfill_verification.py --dry-run   # just prints missing jobs
"""
from __future__ import annotations
import argparse
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv()

import gspread
from google.oauth2.service_account import Credentials

from firmin.clients.proteo import ProteoClient
from firmin.clients.sheets import SheetsClient
from firmin.verification import VerificationPipeline
from firmin.utils.logger import get_logger

logger = get_logger(__name__)

SPREADSHEET_ID = "1uEst-r23EiTyfdmL6gx_YQMuzR0s7yyx-qJVLIUyDSI"
ACTUAL_WS = "Actual Entry"
VERIFY_WS = "Verification"


def get_job_numbers(ws: gspread.Worksheet) -> list[str]:
    headers = ws.row_values(1)
    if "delivery_order_number" not in headers:
        raise RuntimeError(f"delivery_order_number column not found in {ws.title}")
    col_idx = headers.index("delivery_order_number")
    values = ws.col_values(col_idx + 1)[1:]  # skip header
    jobs = []
    for v in values:
        v = str(v).strip()
        if v:
            try:
                jobs.append(str(int(float(v))))
            except ValueError:
                pass
    return jobs


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Print missing jobs without scraping")
    args = parser.parse_args()

    sheets = SheetsClient()
    sh = sheets._gc.open_by_key(SPREADSHEET_ID)

    print("Reading Actual Entry...")
    actual_jobs = get_job_numbers(sh.worksheet(ACTUAL_WS))
    print(f"  {len(actual_jobs)} jobs in Actual Entry")

    print("Reading Verification...")
    verify_jobs = set(get_job_numbers(sh.worksheet(VERIFY_WS)))
    print(f"  {len(verify_jobs)} jobs in Verification")

    missing = [j for j in actual_jobs if j not in verify_jobs]
    # Deduplicate preserving order
    seen = set()
    missing_unique = []
    for j in missing:
        if j not in seen:
            seen.add(j)
            missing_unique.append(j)

    print(f"\n  Missing from Verification: {len(missing_unique)} jobs")

    if args.dry_run:
        print("\n--- DRY RUN: jobs that would be scraped ---")
        for j in missing_unique:
            print(f"  {j}")
        return

    if not missing_unique:
        print("Nothing to backfill.")
        return

    confirm = input(f"\nScrape {len(missing_unique)} jobs from Proteo? [y/N]: ").strip().lower()
    if confirm != "y":
        print("Aborted.")
        return

    proteo = ProteoClient()
    verification = VerificationPipeline(proteo=proteo, sheets=sheets)

    print(f"\nBackfilling {len(missing_unique)} jobs...")
    summary = verification.process_jobs(missing_unique)

    print(f"\nDone.")
    print(f"  Written:   {summary['written']}")
    print(f"  Not found: {summary['not_found']}")
    print(f"  Errors:    {summary['errors']}")


if __name__ == "__main__":
    main()
