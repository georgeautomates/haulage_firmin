"""
Backfill Community Playthings individual delivery orders into Actual Entry.

Writes the 3 historical delivery orders that arrived before the client was onboarded.
Order data sourced from PDFs and Proteo screenshots.

Usage:
    python scripts/backfill_community_playthings.py           # apply
    python scripts/backfill_community_playthings.py --dry-run # preview only
"""
import sys
import os
from datetime import datetime, timezone
from dotenv import load_dotenv
load_dotenv()

import gspread
from google.oauth2.service_account import Credentials

DRY_RUN = "--dry-run" in sys.argv

SPREADSHEET_ID = "1uEst-r23EiTyfdmL6gx_YQMuzR0s7yyx-qJVLIUyDSI"
SERVICE_ACCOUNT_PATH = os.getenv("GOOGLE_SERVICE_ACCOUNT_PATH", "config/service_account.json")

ORDERS = [
    {
        "job_number":           "9194089",
        "delivery_order_number":"9194089",
        "order_number":         "D871E-1",
        "po_number":            "D871E-1",
        "customer_ref":         "D871E-1",
        "collection_date":      "09/04/2026",
        "collection_time":      "09:00",
        "delivery_date":        "10/04/2026",
        "delivery_time":        "09:00",
        "delivery_postcode":    "ME15 6BD",
        "delivery_point":       "Maidstone YMCA preschool - Maidstone",
        "collection_point":     "Community Playthings - Sittingbourne",
        "collection_postcode":  "ME10 3RN",
        "pallets":              25,
        "spaces":               25,
        "weight":               "284.75",
        "client_name":          "Community Playthings",
        "business_type":        "Firmin Xpress | Vans",
        "goods_type":           "Palletised",
        "price":                "",
        "rate":                 "",
    },
    {
        "job_number":           "9194194",
        "delivery_order_number":"9194194",
        "order_number":         "D569E-1",
        "po_number":            "D569E-1",
        "customer_ref":         "D569E-1",
        "collection_date":      "10/04/2026",
        "collection_time":      "09:00",
        "delivery_date":        "13/04/2026",
        "delivery_time":        "08:30",
        "delivery_postcode":    "SL6 6AR",
        "delivery_point":       "Fennies Nursery - Maidenhead",
        "collection_point":     "Community Playthings - Sittingbourne",
        "collection_postcode":  "ME10 3RN",
        "pallets":              254,
        "spaces":               254,
        "weight":               "2872.58",
        "client_name":          "Community Playthings",
        "business_type":        "Firmin Xpress | Vans",
        "goods_type":           "Palletised",
        "price":                "",
        "rate":                 "",
    },
    {
        "job_number":           "9211151",
        "delivery_order_number":"9211151",
        "order_number":         "9211151",
        "po_number":            "",
        "customer_ref":         "",
        "collection_date":      "13/04/2026",
        "collection_time":      "09:00",
        "delivery_date":        "14/04/2026",
        "delivery_time":        "09:00",
        "delivery_postcode":    "SY5 8BE",
        "delivery_point":       "Hillside House Nursery - Shrewsbury",
        "collection_point":     "Community Playthings - Sittingbourne",
        "collection_postcode":  "ME10 3RN",
        "pallets":              6,
        "spaces":               6,
        "weight":               "671",
        "client_name":          "Community Playthings",
        "business_type":        "Firmin Xpress | Vans",
        "goods_type":           "Palletised",
        "price":                "",
        "rate":                 "",
    },
]


def get_sheet():
    creds = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_PATH,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    gc = gspread.authorize(creds)
    return gc.open_by_key(SPREADSHEET_ID).worksheet("Actual Entry")


def get_existing_job_numbers(sheet) -> set:
    rows = sheet.get_all_records()
    return {str(r.get("job_number", "")).strip() for r in rows}


def build_row(order: dict, headers: list) -> list:
    now = datetime.now(timezone.utc).isoformat()
    full = {**order, "status": "GREEN", "composite_score": 95,
            "processed_at": now, "message_id": "backfill", "pdf_url": "",
            "email_subject": "backfill", "email_body": ""}
    return [full.get(h, "") for h in headers]


def main():
    print(f"{'DRY RUN — ' if DRY_RUN else ''}Community Playthings backfill")
    sheet = get_sheet()
    headers = sheet.row_values(1)
    existing = get_existing_job_numbers(sheet)

    written = 0
    skipped = 0
    for order in ORDERS:
        jn = order["job_number"]
        if jn in existing:
            print(f"  SKIP (already exists): {jn}")
            skipped += 1
            continue
        row = build_row(order, headers)
        print(f"  {'WOULD WRITE' if DRY_RUN else 'WRITING'}: {jn} "
              f"→ {order['delivery_point']} ({order['delivery_postcode']})")
        if not DRY_RUN:
            sheet.append_row(row, value_input_option="USER_ENTERED")
        written += 1

    print(f"\nDone — {written} written, {skipped} skipped")


if __name__ == "__main__":
    main()
