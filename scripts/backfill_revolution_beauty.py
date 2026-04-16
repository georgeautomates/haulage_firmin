"""
Backfill Revolution Beauty rows in Actual Entry.

Fixes two issues in rows where client_name == "Revolution Beauty Ltd":
  1. delivery_point / collection_point showing raw postcode instead of location name
  2. pallets / spaces showing 0 when booking is Full Load (should be 26)

Known location overrides (from revolution_beauty.yaml):
  NG7 2SD  → Boots UK - Nottingham
  WN8 8DY  → Supply Chain Solutions - Skelmersdale
  LU5 4RZ  → SUPERDRUG - DUNSTABLE
  ME11 5JS → Revolution Beauty - Queenborough ME11
  DE11 0BB → Clipper Logistics - Swadlincote (delivery)
           → GXO (Clipper Logistic) - Swadlincote (collection)

Usage:
    python scripts/backfill_revolution_beauty.py           # apply changes
    python scripts/backfill_revolution_beauty.py --dry-run # preview only
"""
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

KNOWN_LOCATIONS = {
    "NG72SD":  "Boots UK - Nottingham",
    "WN88DY":  "Supply Chain Solutions - Skelmersdale",
    "LU54RZ":  "SUPERDRUG - DUNSTABLE",
    "ME115JS": "Revolution Beauty - Queenborough ME11",
    "DE110BB": "Clipper Logistics - Swadlincote",       # delivery direction
    "WA150AF": "The HUT.COM Ltd - Altrincham",
    "DL30PS":  "Steadhouse Savers - Darlington",
}

COLLECTION_KNOWN = {
    "DE110BB": "GXO (Clipper Logistic) - Swadlincote",  # collection direction
    "ME115JS": "Revolution Beauty - Queenborough ME11",
}

FULL_LOAD_PALLETS = 26


def normalise_postcode(pc: str) -> str:
    return pc.replace(" ", "").upper().strip()


def looks_like_postcode(val: str) -> bool:
    """Returns True if the value looks like a raw UK postcode."""
    import re
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

client_col       = col("client_name")
collection_col   = col("collection_point")
delivery_col     = col("delivery_point")
coll_post_col    = col("collection_postcode")
del_post_col     = col("delivery_postcode")
pallets_col      = col("pallets")
spaces_col       = col("spaces")
weight_col       = col("weight")

print(f"Scanning {len(data)} rows...")
print(f"{'DRY RUN — ' if DRY_RUN else ''}Fixing Revolution Beauty rows\n")

changes = []  # list of (row_num, col_index, old_val, new_val, reason)

for i, row in enumerate(data, start=2):
    def cell(c):
        return row[c].strip() if c is not None and c < len(row) else ""

    if "revolution beauty" not in cell(client_col).lower():
        continue

    # --- Fix delivery_point if it looks like a raw postcode ---
    dp = cell(delivery_col)
    if looks_like_postcode(dp):
        norm = normalise_postcode(dp)
        replacement = KNOWN_LOCATIONS.get(norm)
        if replacement:
            changes.append((i, delivery_col, dp, replacement, "delivery_point postcode → name"))
        else:
            print(f"  WARNING row {i}: delivery_point='{dp}' — no known override, leaving as-is")

    # --- Fix collection_point if it looks like a raw postcode ---
    cp = cell(collection_col)
    if looks_like_postcode(cp):
        norm = normalise_postcode(cp)
        replacement = COLLECTION_KNOWN.get(norm) or KNOWN_LOCATIONS.get(norm)
        if replacement:
            changes.append((i, collection_col, cp, replacement, "collection_point postcode → name"))
        else:
            print(f"  WARNING row {i}: collection_point='{cp}' — no known override, leaving as-is")

    # --- Fix pallets / spaces if 0 ---
    if pallets_col is not None:
        p = cell(pallets_col)
        if p in ("0", ""):
            changes.append((i, pallets_col, p, str(FULL_LOAD_PALLETS), "pallets 0 → 26"))

    if spaces_col is not None:
        s = cell(spaces_col)
        if s in ("0", ""):
            changes.append((i, spaces_col, s, str(FULL_LOAD_PALLETS), "spaces 0 → 26"))

    # --- Fix weight if blank ---
    if weight_col is not None:
        w = cell(weight_col)
        if w == "":
            changes.append((i, weight_col, w, "0", "weight blank → 0"))

print(f"Changes needed: {len(changes)}")
for row_num, col_idx, old, new, reason in changes:
    print(f"  Row {row_num}: [{reason}] '{old}' → '{new}'")

if not changes:
    print("Nothing to update.")
    sys.exit(0)

if DRY_RUN:
    print("\nRun without --dry-run to apply.")
    sys.exit(0)

print(f"\nApplying {len(changes)} changes...")

def col_letter(idx):
    # Supports up to column Z (26 cols); extend if needed
    if idx < 26:
        return chr(ord('A') + idx)
    return chr(ord('A') + idx // 26 - 1) + chr(ord('A') + idx % 26)

for row_num, col_idx, old, new, reason in changes:
    cell_addr = f"{col_letter(col_idx)}{row_num}"
    ws.update_acell(cell_addr, new)
    print(f"  {cell_addr}: '{old}' → '{new}'")
    time.sleep(1.2)  # avoid Sheets API rate limit

print(f"\nDone. {len(changes)} cells updated.")
