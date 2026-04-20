"""
Backfill booking_window in Actual Entry for rows that have a HHMM-HHMM pattern
sitting in customer_ref (because the field didn't exist when those rows were written).

Also cleans customer_ref by removing the time window from it.

Usage:
    python scripts/backfill_booking_window.py [--dry-run]
"""
import re
import sys
import os
import time

from dotenv import load_dotenv
load_dotenv()

import gspread
from google.oauth2.service_account import Credentials

DRY_RUN = "--dry-run" in sys.argv

SPREADSHEET_ID = "1uEst-r23EiTyfdmL6gx_YQMuzR0s7yyx-qJVLIUyDSI"
SERVICE_ACCOUNT_PATH = os.getenv("GOOGLE_SERVICE_ACCOUNT_PATH", "config/service_account.json")

WINDOW_RE = re.compile(r'\b(\d{4}-\d{4})\b')

creds = Credentials.from_service_account_file(
    SERVICE_ACCOUNT_PATH,
    scopes=["https://www.googleapis.com/auth/spreadsheets"],
)
gc = gspread.authorize(creds)
ss = gc.open_by_key(SPREADSHEET_ID)
ws = ss.worksheet("Actual Entry")

rows = ws.get_all_values()
headers = rows[0]
data = rows[1:]

def col(name):
    try:
        return headers.index(name)
    except ValueError:
        return -1

customer_ref_col  = col("customer_ref")
booking_window_col = col("booking_window")

if customer_ref_col < 0:
    print("ERROR: 'customer_ref' column not found in sheet. Has the column been added?")
    sys.exit(1)
if booking_window_col < 0:
    print("ERROR: 'booking_window' column not found in sheet. Has the column been added?")
    sys.exit(1)

print(f"Scanning {len(data)} rows...")
print(f"customer_ref col:   {customer_ref_col + 1} ({chr(ord('A') + customer_ref_col)})")
print(f"booking_window col: {booking_window_col + 1} ({chr(ord('A') + booking_window_col)})\n")

updates = []  # list of (sheet_row, new_booking_window, new_customer_ref)

for i, row in enumerate(data, start=2):
    current_window = row[booking_window_col].strip() if booking_window_col < len(row) else ""
    current_ref    = row[customer_ref_col].strip()   if customer_ref_col   < len(row) else ""

    # Skip rows that already have a booking_window value
    if current_window:
        continue

    # Check if customer_ref contains a time window pattern
    m = WINDOW_RE.search(current_ref)
    if not m:
        continue

    new_window = m.group(1)
    new_ref = WINDOW_RE.sub("", current_ref).strip(" /").strip()

    updates.append((i, new_window, new_ref, current_ref))

print(f"Found {len(updates)} rows to backfill.")
if updates:
    print("\nSample (first 10):")
    for row_num, window, new_ref, old_ref in updates[:10]:
        print(f"  Row {row_num}: customer_ref '{old_ref}' → booking_window='{window}', customer_ref='{new_ref}'")

if not updates:
    print("Nothing to update.")
    sys.exit(0)

if DRY_RUN:
    print(f"\nDry run — run without --dry-run to apply {len(updates)} updates.")
    sys.exit(0)

# Apply updates in batches using batch_update for speed
bw_col_letter = chr(ord('A') + booking_window_col)
cr_col_letter = chr(ord('A') + customer_ref_col)

print(f"\nWriting {len(updates)} rows (batched)...")

BATCH_SIZE = 50
for batch_start in range(0, len(updates), BATCH_SIZE):
    batch = updates[batch_start:batch_start + BATCH_SIZE]
    cell_updates = []
    for row_num, window, new_ref, _ in batch:
        cell_updates.append({
            "range": f"{bw_col_letter}{row_num}",
            "values": [[window]],
        })
        cell_updates.append({
            "range": f"{cr_col_letter}{row_num}",
            "values": [[new_ref]],
        })
    ws.batch_update(cell_updates)
    print(f"  Wrote rows {updates[batch_start][0]}–{batch[-1][0]}")
    if batch_start + BATCH_SIZE < len(updates):
        time.sleep(1.2)  # stay under Sheets rate limit

print(f"\nDone. {len(updates)} rows backfilled.")
