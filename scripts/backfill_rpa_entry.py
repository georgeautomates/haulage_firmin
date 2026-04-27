"""
Backfill the 'RPA Entry' sheet for historical jobs.

Fetches all rows from 'Actual Entry', skips jobs already in 'RPA Entry',
then processes them in batches with a configurable delay between jobs
to avoid looking suspicious to Proteo.

Usage:
    # First 25 jobs (newest first)
    .venv/bin/python scripts/backfill_rpa_entry.py --limit 25

    # Next 25 (skip already done, newest first)
    .venv/bin/python scripts/backfill_rpa_entry.py --limit 25

    # Oldest first (if you want to go chronologically)
    .venv/bin/python scripts/backfill_rpa_entry.py --limit 25 --oldest-first

    # Dry run — fills form + screenshot but does NOT write to sheet
    .venv/bin/python scripts/backfill_rpa_entry.py --limit 5 --dry-run

    # Custom delay between jobs (default 15s)
    .venv/bin/python scripts/backfill_rpa_entry.py --limit 25 --delay 20
"""
from __future__ import annotations
import argparse
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv()

import gspread
from google.oauth2.service_account import Credentials

from firmin.clients.drive import DriveClient
from firmin.clients.proteo import ProteoClient
from firmin.clients.sheets import SheetsClient
from firmin.verification import RpaEntryPipeline, SPREADSHEET_ID, RPA_ENTRY_WS

ACTUAL_ENTRY_WS = "Actual Entry"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

FIELD_MAP = {
    "delivery_order_number": "delivery_order_number",
    "client_name":           "client_name",
    "business_type":         "business_type",
    "service":               "service",
    "collection_point":      "collection_point",
    "collection_date":       "collection_date",
    "collection_time":       "collection_time",
    "delivery_point":        "delivery_point",
    "delivery_postcode":     "delivery_postcode",
    "delivery_date":         "delivery_date",
    "delivery_time":         "delivery_time",
    "order_number":          "order_number",
    "rate":                  "price",
    "pallets":               "pallets",
    "spaces":                "spaces",
    "booking_window":        "booking_window",
    "traffic_note":          "traffic_note",
    "customer_ref":          "customer_ref",
}


def fetch_all_orders(gc: gspread.Client) -> list[dict]:
    """Fetch all rows from Actual Entry, return as list of order dicts."""
    ws = gc.open_by_key(SPREADSHEET_ID).worksheet(ACTUAL_ENTRY_WS)
    all_values = ws.get_all_values()
    if not all_values:
        return []

    headers = [h.strip().lower() for h in all_values[0]]

    def ci(name: str) -> int:
        return headers.index(name) if name in headers else -1

    job_col = ci("delivery_order_number")
    if job_col < 0:
        print("ERROR: delivery_order_number column not found in Actual Entry")
        return []

    orders = []
    seen_jobs: set[str] = set()
    for row in all_values[1:]:
        if job_col >= len(row):
            continue
        job_number = row[job_col].strip()
        if not job_number or job_number in seen_jobs:
            continue
        seen_jobs.add(job_number)

        order = {"job_number": job_number, "delivery_order_number": job_number}
        for sheet_col, order_key in FIELD_MAP.items():
            idx = ci(sheet_col)
            order[order_key] = row[idx].strip() if idx >= 0 and idx < len(row) else ""
        orders.append(order)

    return orders


def fetch_already_done(gc: gspread.Client) -> set[str]:
    """Return job numbers that succeeded in RPA Entry sheet (success=TRUE)."""
    try:
        ws = gc.open_by_key(SPREADSHEET_ID).worksheet(RPA_ENTRY_WS)
        headers = ws.row_values(1)
        if "job_number" not in headers:
            return set()
        job_col = headers.index("job_number")
        success_col = headers.index("success") if "success" in headers else -1
        all_rows = ws.get_all_values()[1:]
        done = set()
        for row in all_rows:
            job = row[job_col].strip() if job_col < len(row) else ""
            if not job:
                continue
            if success_col >= 0:
                success = row[success_col].strip().upper() if success_col < len(row) else ""
                if success == "TRUE":
                    done.add(job)
            else:
                done.add(job)
        return done
    except Exception:
        return set()


def fetch_failed_jobs(gc: gspread.Client) -> set[str]:
    """Return job numbers that have a failed RPA entry (success=FALSE, most recent row)."""
    try:
        ws = gc.open_by_key(SPREADSHEET_ID).worksheet(RPA_ENTRY_WS)
        headers = ws.row_values(1)
        if "job_number" not in headers or "success" not in headers:
            return set()
        job_col = headers.index("job_number")
        success_col = headers.index("success")
        all_rows = ws.get_all_values()[1:]
        # Track most recent result per job (last row wins)
        latest: dict[str, str] = {}
        for row in all_rows:
            job = row[job_col].strip() if job_col < len(row) else ""
            if not job:
                continue
            success = row[success_col].strip().upper() if success_col < len(row) else ""
            latest[job] = success
        return {job for job, success in latest.items() if success != "TRUE"}
    except Exception:
        return set()


def main():
    parser = argparse.ArgumentParser(description="Backfill RPA Entry for historical jobs")
    parser.add_argument("--limit", type=int, default=25,
                        help="Max jobs to process in this run (default: 25)")
    parser.add_argument("--delay", type=int, default=15,
                        help="Seconds to wait between jobs (default: 15)")
    parser.add_argument("--oldest-first", action="store_true",
                        help="Process oldest jobs first (default: newest first)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Fill form + screenshot but skip sheet write")
    parser.add_argument("--ds-smith-only", action="store_true",
                        help="Skip Unipet and other non-DS-Smith jobs (safe while Unipet AJAX bug is unfixed)")
    parser.add_argument("--retry-failed", action="store_true",
                        help="Only retry jobs that previously failed (success=FALSE in RPA Entry)")
    args = parser.parse_args()

    sa_path = os.getenv("GOOGLE_SERVICE_ACCOUNT_PATH", "config/service_account.json")
    creds = Credentials.from_service_account_file(sa_path, scopes=SCOPES)
    gc = gspread.authorize(creds)

    print("Fetching all orders from Actual Entry...")
    all_orders = fetch_all_orders(gc)
    print(f"  {len(all_orders)} total jobs in Actual Entry")

    print("Fetching already-processed jobs from RPA Entry...")
    already_done = fetch_already_done(gc)
    print(f"  {len(already_done)} jobs successfully done in RPA Entry")

    if args.retry_failed:
        failed_jobs = fetch_failed_jobs(gc)
        print(f"  {len(failed_jobs)} jobs with failed RPA entry — retrying these")
        pending = [o for o in all_orders if o["delivery_order_number"] in failed_jobs]
        print(f"  {len(pending)} pending retry jobs found in Actual Entry")
    else:
        # Filter to jobs not yet successfully done
        pending = [o for o in all_orders if o["delivery_order_number"] not in already_done]
        print(f"  {len(pending)} jobs pending RPA entry")

    # Optionally restrict to DS Smith only (skip Unipet while AJAX bug is unfixed)
    if args.ds_smith_only:
        before = len(pending)
        pending = [o for o in pending if "unipet" not in o.get("client_name", "").lower()]
        skipped_clients = before - len(pending)
        print(f"  --ds-smith-only: skipped {skipped_clients} non-DS-Smith jobs, {len(pending)} remaining")

    if not pending:
        print("Nothing to do.")
        return

    # Newest first by default (all_orders is in sheet order = oldest first, so reverse)
    if not args.oldest_first:
        pending = list(reversed(pending))

    batch = pending[:args.limit]
    print(f"\nWill process {len(batch)} jobs (limit={args.limit}, delay={args.delay}s between each)")
    if args.dry_run:
        print("DRY RUN — sheet write skipped\n")

    print("Initialising clients...")
    try:
        drive = DriveClient()
    except Exception as e:
        print(f"WARNING: Drive unavailable ({e}) — screenshots won't be uploaded")
        drive = None

    proteo = ProteoClient()
    sheets = SheetsClient()
    pipeline = RpaEntryPipeline(proteo=proteo, sheets=sheets, drive_client=drive)
    # Pre-populate seen set so pipeline doesn't re-check the sheet on every job
    pipeline._seen = already_done.copy()

    written = skipped = errors = 0

    for i, order in enumerate(batch, 1):
        job = order["delivery_order_number"]
        print(f"\n[{i}/{len(batch)}] Job {job} — {order.get('collection_point', '?')} → {order.get('delivery_point', '?')}")

        if args.dry_run:
            result = proteo.enter_order(order, drive_client=drive)
            print(f"  agreement: {result.agreement_score}%  success: {result.success}")
            if result.error:
                print(f"  error: {result.error}")
            if result.field_matches:
                failed = [k for k, v in result.field_matches.items() if not v]
                if failed:
                    print(f"  mismatched fields: {failed}")
        else:
            summary = pipeline.process_jobs([order], retry_failed=args.retry_failed)
            written  += summary["written"]
            skipped  += summary["skipped"]
            errors   += summary["errors"]
            status = "✓ written" if summary["written"] else ("skipped" if summary["skipped"] else "✗ error")
            print(f"  {status}")

        if i < len(batch):
            print(f"  waiting {args.delay}s...")
            time.sleep(args.delay)

    if not args.dry_run:
        print(f"\nDone: written={written} skipped={skipped} errors={errors}")
        remaining = len(pending) - len(batch)
        if remaining > 0:
            print(f"{remaining} jobs still pending — run again to continue")
    else:
        print(f"\nDry run complete ({len(batch)} jobs processed)")


if __name__ == "__main__":
    main()
