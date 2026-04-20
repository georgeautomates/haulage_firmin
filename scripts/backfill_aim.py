"""
Backfill AIM (SIG Trading Limited) rows in Actual Entry.

Fixes rows where client_name == "AIM (SIG Trading Limited)":
  1. delivery_point showing raw postcode instead of location name

Usage:
    python scripts/backfill_aim.py           # apply changes
    python scripts/backfill_aim.py --dry-run # preview only
"""
import sys
import os
import re
import time
from dotenv import load_dotenv
load_dotenv()

import gspread
from google.oauth2.service_account import Credentials

DRY_RUN = "--dry-run" in sys.argv

SPREADSHEET_ID = "1uEst-r23EiTyfdmL6gx_YQMuzR0s7yyx-qJVLIUyDSI"
SERVICE_ACCOUNT_PATH = os.getenv("GOOGLE_SERVICE_ACCOUNT_PATH", "config/service_account.json")

KNOWN_LOCATIONS = {
    "RM138HY": "SIG Trading - Rainham",
    "PR77DW":  "Encon Nevill Long - Chorley",
    "BN180BD": "Bewi Insulations & Construction Ltd - Arundel",
    "RM96QJ":  "Minster Insulation & Drylining - Dagenham",
    "LS31LH":  "SCS Building Solutions Ltd - Leeds",
    "NN67GX":  "Direct Building Materials Ltd c/o Winvic Construction - Crick",
    "UB78EQ":  "Minster Insulation & Drylining - West Drayton",
    "SE15SR":  "SIG Trading - London",
    "NN57UW":  "MY-Fab LTD - Northampton",
    "CM133XL": "Hilt Material Supplies Ltd - Brentwood",
    "PO35JT":  "Condor Logistics - Portsmouth",
    "BS494QN": "Smart Systems - Bristol",
    "B692DF":  "Sheffield Insulations - Oldbury",
    "GU146SB": "Farnborough College of Technology - Farnborough",
}


def normalise_postcode(pc: str) -> str:
    return pc.replace(" ", "").upper().strip()


def looks_like_postcode(val: str) -> bool:
    return bool(re.match(r'^[A-Z]{1,2}\d{1,2}[A-Z]?\s?\d[A-Z]{2}$', val.strip().upper()))


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
    return headers.index(name) if name in headers else None


client_col     = col("client_name")
delivery_col   = col("delivery_point")
del_post_col   = col("delivery_postcode")

print(f"Scanning {len(data)} rows...")
print(f"{'DRY RUN — ' if DRY_RUN else ''}Fixing AIM rows\n")

changes = []

for i, row in enumerate(data, start=2):
    def cell(c):
        return row[c].strip() if c is not None and c < len(row) else ""

    if "aim" not in cell(client_col).lower() and "sig trading" not in cell(client_col).lower():
        continue

    dp = cell(delivery_col)
    if looks_like_postcode(dp):
        norm = normalise_postcode(dp)
        replacement = KNOWN_LOCATIONS.get(norm)
        if replacement:
            changes.append((i, delivery_col, dp, replacement, "delivery_point postcode → name"))
        else:
            # Try delivery_postcode column as fallback
            dp2 = cell(del_post_col)
            norm2 = normalise_postcode(dp2) if dp2 else ""
            replacement2 = KNOWN_LOCATIONS.get(norm2) if norm2 else None
            if replacement2:
                changes.append((i, delivery_col, dp, replacement2, "delivery_point postcode → name (via postcode col)"))
            else:
                print(f"  WARNING row {i}: delivery_point='{dp}' — no known override, leaving as-is")

print(f"Changes needed: {len(changes)}")
for row_num, col_idx, old, new, reason in changes:
    print(f"  Row {row_num}: [{reason}] '{old}' → '{new}'")

if not changes:
    print("Nothing to update.")
    sys.exit(0)

if DRY_RUN:
    print("\nRun without --dry-run to apply.")
    sys.exit(0)


def col_letter(idx):
    if idx < 26:
        return chr(ord('A') + idx)
    return chr(ord('A') + idx // 26 - 1) + chr(ord('A') + idx % 26)


print(f"\nApplying {len(changes)} changes...")
for row_num, col_idx, old, new, reason in changes:
    cell_addr = f"{col_letter(col_idx)}{row_num}"
    ws.update_acell(cell_addr, new)
    print(f"  {cell_addr}: '{old}' → '{new}'")
    time.sleep(1.2)

print(f"\nDone. {len(changes)} cells updated.")
