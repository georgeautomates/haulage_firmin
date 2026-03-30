"""
One-time cleanup: remove duplicate rows from the Actual Entry sheet.
Keeps the latest (most recent processed_at) row per job number.
Deletes all earlier duplicates.

Run locally:
    python scripts/cleanup_duplicate_rows.py

Dry-run by default — pass --execute to actually delete.
"""
import argparse
import os
import sys
from pathlib import Path

# Allow running from repo root
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

import gspread
from google.oauth2.service_account import Credentials

SPREADSHEET_ID = "1uEst-r23EiTyfdmL6gx_YQMuzR0s7yyx-qJVLIUyDSI"
WORKSHEET_NAME = "Actual Entry"
JOB_NUMBER_COL = "delivery_order_number"
TIMESTAMP_COL = "processed_at"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def main(execute: bool):
    sa_path = os.getenv("GOOGLE_SERVICE_ACCOUNT_PATH", "config/service_account.json")
    creds = Credentials.from_service_account_file(sa_path, scopes=SCOPES)
    gc = gspread.authorize(creds)

    sh = gc.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet(WORKSHEET_NAME)

    print(f"Fetching all rows from '{WORKSHEET_NAME}'...")
    all_values = ws.get_all_values()
    headers = all_values[0]
    rows = all_values[1:]  # data rows, 0-indexed here = sheet row 2 onwards

    print(f"Total rows (excluding header): {len(rows)}")

    try:
        job_col = headers.index(JOB_NUMBER_COL)
        ts_col = headers.index(TIMESTAMP_COL)
    except ValueError as e:
        print(f"ERROR: Column not found: {e}")
        sys.exit(1)

    # Build: job_number -> list of (sheet_row_number, timestamp)
    # Sheet row numbers are 1-indexed, header is row 1, data starts at row 2
    job_rows: dict[str, list[tuple[int, str]]] = {}
    for i, row in enumerate(rows):
        sheet_row = i + 2  # +2 because header is row 1
        job_number = row[job_col].strip()
        timestamp = row[ts_col].strip()
        if not job_number:
            continue
        job_rows.setdefault(job_number, []).append((sheet_row, timestamp))

    # For each job with duplicates, keep the latest (highest timestamp), delete the rest
    rows_to_delete = []
    for job_number, entries in job_rows.items():
        if len(entries) <= 1:
            continue
        # Sort by timestamp descending, keep first (latest)
        entries_sorted = sorted(entries, key=lambda x: x[1], reverse=True)
        keep_row = entries_sorted[0]
        delete_rows = [e[0] for e in entries_sorted[1:]]
        rows_to_delete.extend(delete_rows)

    rows_to_delete.sort(reverse=True)  # delete from bottom up to preserve row numbers

    print(f"Duplicate jobs found: {sum(1 for e in job_rows.values() if len(e) > 1)}")
    print(f"Rows to delete: {len(rows_to_delete)}")

    if not rows_to_delete:
        print("Nothing to delete.")
        return

    if not execute:
        print("\nDRY RUN — no changes made. Pass --execute to delete.")
        print(f"Would delete sheet rows: {rows_to_delete[:20]}{'...' if len(rows_to_delete) > 20 else ''}")
        return

    import time
    print(f"\nDeleting {len(rows_to_delete)} rows...")
    for i, row_num in enumerate(rows_to_delete):
        ws.delete_rows(row_num)
        time.sleep(1.2)  # stay under 60 writes/min quota
        if (i + 1) % 20 == 0:
            print(f"  Deleted {i + 1}/{len(rows_to_delete)}...")

    print(f"Done. Deleted {len(rows_to_delete)} duplicate rows.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true", help="Actually delete rows (default is dry run)")
    args = parser.parse_args()
    main(execute=args.execute)
