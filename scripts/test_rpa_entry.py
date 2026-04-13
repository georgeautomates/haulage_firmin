"""
Test the RPA entry pipeline on a single existing job number.

Pulls the extraction row from 'Actual Entry', runs enter_order(),
uploads screenshot to Drive, and writes to 'RPA Entry' sheet.

Usage:
    cd /opt/firmin
    .venv/bin/python scripts/test_rpa_entry.py 2560920
    .venv/bin/python scripts/test_rpa_entry.py 2560920 --dry-run   # skip sheet write
"""
from __future__ import annotations
import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv()

import gspread
from google.oauth2.service_account import Credentials

from firmin.clients.drive import DriveClient
from firmin.clients.proteo import ProteoClient
from firmin.clients.sheets import SheetsClient
from firmin.verification import RpaEntryPipeline

SPREADSHEET_ID = "1uEst-r23EiTyfdmL6gx_YQMuzR0s7yyx-qJVLIUyDSI"
ACTUAL_ENTRY_WS = "Actual Entry"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Columns we need from Actual Entry to reconstruct the order dict
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
    "rate":                  "price",   # rate column → price key
    "pallets":               "pallets",
    "spaces":                "spaces",
}


def fetch_order_from_sheet(job_number: str) -> dict | None:
    sa_path = os.getenv("GOOGLE_SERVICE_ACCOUNT_PATH", "config/service_account.json")
    creds = Credentials.from_service_account_file(sa_path, scopes=SCOPES)
    gc = gspread.authorize(creds)
    ws = gc.open_by_key(SPREADSHEET_ID).worksheet(ACTUAL_ENTRY_WS)

    all_values = ws.get_all_values()
    if not all_values:
        return None

    headers = [h.strip().lower() for h in all_values[0]]

    def ci(name: str) -> int:
        return headers.index(name) if name in headers else -1

    job_col = ci("delivery_order_number")
    if job_col < 0:
        print("ERROR: delivery_order_number column not found")
        return None

    # Find the row — last match wins (most recent)
    found_row = None
    for row in all_values[1:]:
        if job_col < len(row) and row[job_col].strip() == job_number:
            found_row = row

    if not found_row:
        return None

    order = {"job_number": job_number}
    for sheet_col, order_key in FIELD_MAP.items():
        idx = ci(sheet_col)
        order[order_key] = found_row[idx].strip() if idx >= 0 and idx < len(found_row) else ""

    # Ensure delivery_order_number is set
    order["delivery_order_number"] = job_number
    return order


def main():
    parser = argparse.ArgumentParser(description="Test RPA entry on a single job")
    parser.add_argument("job_number", help="Job number from Actual Entry (e.g. 2560920)")
    parser.add_argument("--dry-run", action="store_true", help="Run enter_order but skip sheet write")
    args = parser.parse_args()

    job_number = args.job_number.strip()

    print(f"Fetching extraction data for job {job_number}...")
    order = fetch_order_from_sheet(job_number)
    if not order:
        print(f"ERROR: job {job_number} not found in Actual Entry sheet")
        sys.exit(1)

    print("Order data:")
    for k, v in order.items():
        if v:
            print(f"  {k}: {v}")

    print("\nInitialising Drive client...")
    try:
        drive = DriveClient()
    except RuntimeError as e:
        print(f"WARNING: Drive unavailable ({e}) — screenshot won't be uploaded")
        drive = None

    print("Initialising Proteo client...")
    proteo = ProteoClient()

    if args.dry_run:
        print("\n--- DRY RUN: calling enter_order (screenshot will be saved to /tmp) ---")
        result = proteo.enter_order(order, drive_client=drive)
        print(f"\nResult:")
        print(f"  success:          {result.success}")
        print(f"  agreement_score:  {result.agreement_score}%")
        print(f"  screenshot_url:   {result.screenshot_url or '(not uploaded)'}")
        print(f"  field_matches:    {result.field_matches}")
        if result.error:
            print(f"  error:            {result.error}")
        print("\nTyped values:")
        print(f"  collection_point: {result.typed_collection_point}")
        print(f"  delivery_point:   {result.typed_delivery_point}")
        print(f"  collection_date:  {result.typed_collection_date}")
        print(f"  collection_time:  {result.typed_collection_time}")
        print(f"  delivery_date:    {result.typed_delivery_date}")
        print(f"  delivery_time:    {result.typed_delivery_time}")
        print(f"  order_number:     {result.typed_order_number}")
        print(f"  price:            {result.typed_price}")
    else:
        print("\nRunning RPA entry pipeline (will write to RPA Entry sheet)...")
        sheets = SheetsClient()
        pipeline = RpaEntryPipeline(proteo=proteo, sheets=sheets, drive_client=drive)
        # Bypass seen-check so we can re-run on existing jobs
        pipeline._seen = set()
        summary = pipeline.process_jobs([order])
        print(f"\nDone: {summary}")


if __name__ == "__main__":
    main()
