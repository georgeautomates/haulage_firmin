"""
Backfill client_name in Actual Entry based on collection_point.
Rule: collection_point == "DS SMITH - SITTINGBOURNE" → St Regis Reels
      otherwise (and not Unipet) → St Regis Fibre A/C

Supports --dry-run to preview changes without writing.
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
REELS_COLLECTION = "DS SMITH - SITTINGBOURNE"

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

client_col = headers.index("client_name")
collection_col = headers.index("collection_point")

print(f"Scanning {len(data)} rows...")
print(f"{'DRY RUN — ' if DRY_RUN else ''}Updating client_name based on collection_point\n")

to_reels = []
to_fibre = []

for i, row in enumerate(data, start=2):  # row 2 is first data row in Sheets
    current_client = row[client_col].strip() if client_col < len(row) else ""
    collection = row[collection_col].strip() if collection_col < len(row) else ""

    # Skip Unipet rows
    if "unipet" in current_client.lower():
        continue

    # Determine correct client name
    if collection == REELS_COLLECTION:
        correct = "St Regis Reels"
    else:
        correct = "St Regis Fibre A/C"

    if current_client != correct:
        if correct == "St Regis Reels":
            to_reels.append((i, current_client, collection))
        else:
            to_fibre.append((i, current_client, collection))

print(f"Changes needed:")
print(f"  → St Regis Reels:    {len(to_reels)} rows")
print(f"  → St Regis Fibre A/C: {len(to_fibre)} rows")

if not to_reels and not to_fibre:
    print("Nothing to update.")
    sys.exit(0)

if DRY_RUN:
    print("\nSample Reels changes (first 5):")
    for row_num, old, col in to_reels[:5]:
        print(f"  Row {row_num}: '{old}' → 'St Regis Reels'  (collection: {col})")
    print("\nSample Fibre changes (first 5):")
    for row_num, old, col in to_fibre[:5]:
        print(f"  Row {row_num}: '{old}' → 'St Regis Fibre A/C'  (collection: {col})")
    print("\nRun without --dry-run to apply.")
    sys.exit(0)

# Apply updates
import time
all_changes = [(i, "St Regis Reels") for i, _, _ in to_reels] + \
              [(i, "St Regis Fibre A/C") for i, _, _ in to_fibre]

client_col_letter = chr(ord('A') + client_col)

print(f"\nUpdating {len(all_changes)} rows...")
for row_num, new_client in all_changes:
    cell = f"{client_col_letter}{row_num}"
    ws.update_acell(cell, new_client)
    print(f"  Row {row_num} → {new_client}")
    time.sleep(0.5)  # avoid Sheets rate limit

print(f"\nDone. {len(to_reels)} rows → Reels, {len(to_fibre)} rows → Fibre A/C")
