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

    client_name_col = headers.index("client_name") if "client_name" in headers else -1

    VALID_CLIENT_KEYWORDS = ("st regis", "ds smith", "fibre", "reels")

    # Find junk rows — either non-numeric order_id, or wrong client
    junk_rows = []
    for i, row in enumerate(all_values[1:], start=2):  # 1-indexed, row 1 is header
        order_id = row[order_id_col].strip() if order_id_col < len(row) else ""
        if not order_id:
            continue
        # Non-numeric order_id (pagination rows like "30,31,2,3,5")
        if not order_id.replace(".", "").isdigit():
            junk_rows.append((i, order_id, row, "non-numeric order_id"))
            continue
        # Numeric but short (e.g. "30" — pagination summary)
        if len(order_id.replace(".", "")) < 5:
            junk_rows.append((i, order_id, row, "order_id too short"))
            continue
        # Wrong client — result from a different company in Proteo
        if client_name_col >= 0:
            client = row[client_name_col].strip().lower() if client_name_col < len(row) else ""
            if client and not any(kw in client for kw in VALID_CLIENT_KEYWORDS):
                junk_rows.append((i, order_id, row, f"wrong client: {row[client_name_col].strip()}"))
                continue

    print(f"Total rows: {len(all_values) - 1}")
    print(f"Junk rows found: {len(junk_rows)}")
    print()

    for row_num, order_id, row, reason in junk_rows:
        print(f"  Row {row_num}: order_id={repr(order_id)} | reason={reason} | {row[:3]}")

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
    for row_num, order_id, _, reason in reversed(junk_rows):
        ws.delete_rows(row_num)
        print(f"  Deleted row {row_num} (order_id={repr(order_id)}, reason={reason})")
        time.sleep(1.2)  # avoid Sheets rate limit

    print(f"\nDone. Removed {len(junk_rows)} junk rows.")


if __name__ == "__main__":
    main()
