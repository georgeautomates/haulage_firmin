"""
One-off script: remove junk rows from Verification sheet where order_id is
not a valid number (e.g. 'Orders', 'Runs' — Proteo header rows picked up
by the scraper fallback selector before the fix).

Usage:
    python scripts/cleanup_verification_junk.py
    python scripts/cleanup_verification_junk.py --dry-run
"""
from __future__ import annotations
import argparse
import sys
import os
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv()

from firmin.clients.sheets import SheetsClient

SPREADSHEET_ID = "1uEst-r23EiTyfdmL6gx_YQMuzR0s7yyx-qJVLIUyDSI"
VERIFY_WS = "Verification"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    sheets = SheetsClient()
    ws = sheets._get_worksheet(SPREADSHEET_ID, VERIFY_WS)

    headers = ws.row_values(1)
    if "order_id" not in headers:
        print("order_id column not found — aborting")
        return

    order_id_col = headers.index("order_id")
    all_values = ws.get_all_values()

    # Find rows where order_id is non-numeric (skip header row 0)
    junk_rows = []
    for i, row in enumerate(all_values[1:], start=2):  # 1-indexed, row 1 is header
        order_id = row[order_id_col].strip() if order_id_col < len(row) else ""
        if order_id and not order_id.replace(".", "").isdigit():
            junk_rows.append((i, order_id, row))

    print(f"Total rows: {len(all_values) - 1}")
    print(f"Junk rows found: {len(junk_rows)}")
    print()

    for row_num, order_id, row in junk_rows:
        print(f"  Row {row_num}: order_id={repr(order_id)} | {row[:5]}")

    if not junk_rows:
        print("Nothing to clean.")
        return

    if args.dry_run:
        print("\nDry run — no changes made.")
        return

    confirm = input(f"\nDelete {len(junk_rows)} junk rows? [y/N]: ").strip().lower()
    if confirm != "y":
        print("Aborted.")
        return

    # Delete in reverse order to preserve row indices
    for row_num, order_id, _ in reversed(junk_rows):
        ws.delete_rows(row_num)
        print(f"  Deleted row {row_num} (order_id={repr(order_id)})")
        time.sleep(1.2)  # avoid Sheets rate limit

    print(f"\nDone. Removed {len(junk_rows)} junk rows.")


if __name__ == "__main__":
    main()
