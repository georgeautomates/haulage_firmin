"""
Fix delivery_order_number for the 3 backfilled Community Playthings rows.

The backfill script mistakenly set delivery_order_number = consignment number.
Proteo stores the Your Reference No (e.g. D871E-1) in the Docket column,
which is what run_comparison.py joins on. This script patches those cells.

Usage:
    python scripts/fix_cp_delivery_order_numbers.py           # apply
    python scripts/fix_cp_delivery_order_numbers.py --dry-run # preview only
"""
import sys
import os
from dotenv import load_dotenv
load_dotenv()

import gspread
from google.oauth2.service_account import Credentials

DRY_RUN = "--dry-run" in sys.argv

SPREADSHEET_ID = "1uEst-r23EiTyfdmL6gx_YQMuzR0s7yyx-qJVLIUyDSI"
SERVICE_ACCOUNT_PATH = os.getenv("GOOGLE_SERVICE_ACCOUNT_PATH", "config/service_account.json")

# consignment_number -> correct delivery_order_number (Your Reference No)
FIXES = {
    "9194089": "D871E-1",
    "9194194": "D569E-1",
    # 9211151: order_number is also "9211151", no change needed
}


def main():
    creds = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_PATH,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    gc = gspread.authorize(creds)
    sheet = gc.open_by_key(SPREADSHEET_ID).worksheet("Actual Entry")

    headers = sheet.row_values(1)
    try:
        don_col = headers.index("delivery_order_number") + 1
    except ValueError as e:
        print(f"ERROR: column not found — {e}")
        return

    rows = sheet.get_all_values()
    fixed = 0
    for i, row in enumerate(rows[1:], start=2):  # skip header, 1-indexed
        current = row[don_col - 1].strip() if len(row) >= don_col else ""
        if current in FIXES:
            correct = FIXES[current]
            print(f"  {'WOULD FIX' if DRY_RUN else 'FIXING'}: row {i}: '{current}' -> '{correct}'")
            if not DRY_RUN:
                sheet.update_cell(i, don_col, correct)
            fixed += 1

    print(f"\nDone — {fixed} cells {'would be ' if DRY_RUN else ''}updated")


if __name__ == "__main__":
    print(f"{'DRY RUN — ' if DRY_RUN else ''}Community Playthings delivery_order_number fix")
    main()
