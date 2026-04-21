"""
Directly backfill the Verification tab for Eurocoils historical orders.

Proteo's search returns Pallet Track results for Eurocoils W/Order numbers
(the numbers clash across clients), so we write Verification rows directly
from the known Proteo data captured in screenshots.

Usage:
    python scripts/backfill_eurocoils_verification.py           # apply
    python scripts/backfill_eurocoils_verification.py --dry-run # preview only
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
VERIFY_WS = "Verification"

# Data sourced from Proteo screenshots
# delivery_order_number = W/Order No (Docket Number) — matches Actual Entry
ORDERS = [
    {
        "order_id":              "1704540",
        "client_name":           "Eurocoils Limited",
        "business_type":         "Pallet Network",
        "pallets":               "1",
        "spaces":                "1.00",
        "weight":                "600",
        "po_number":             "54939",
        "order_number":          "54939",
        "collection_point":      "Eurocoils - Sittingbourne",
        "collection_date":       "01/04/2026",
        "collection_time":       "09:00",
        "delivery_point":        "CWS (NW) Ltd - High Peak",
        "delivery_postcode":     "SK23 0TQ",
        "delivery_date":         "02/04/2026",
        "delivery_time":         "09:00",
        "delivery_order_number": "46417",
        " goods_type":           "Palletised",
    },
    {
        "order_id":              "1704542",
        "client_name":           "Eurocoils Limited",
        "business_type":         "Pallet Network",
        "pallets":               "1",
        "spaces":                "1.00",
        "weight":                "600",
        "po_number":             "54939",
        "order_number":          "54939",
        "collection_point":      "Eurocoils - Sittingbourne",
        "collection_date":       "01/04/2026",
        "collection_time":       "09:00",
        "delivery_point":        "NG Bailey Ltd - Bristol",
        "delivery_postcode":     "BS1 6AZ",
        "delivery_date":         "02/04/2026",
        "delivery_time":         "12:00",
        "delivery_order_number": "46424",
        " goods_type":           "Palletised",
    },
    {
        "order_id":              "1704544",
        "client_name":           "Eurocoils Limited",
        "business_type":         "Pallet Network",
        "pallets":               "1",
        "spaces":                "1.00",
        "weight":                "600",
        "po_number":             "54939",
        "order_number":          "54939",
        "collection_point":      "Eurocoils - Sittingbourne",
        "collection_date":       "01/04/2026",
        "collection_time":       "09:00",
        "delivery_point":        "T Gameson - Cannock",
        "delivery_postcode":     "WS11 0DJ",
        "delivery_date":         "02/04/2026",
        "delivery_time":         "09:00",
        "delivery_order_number": "46476",
        " goods_type":           "Palletised",
    },
    {
        "order_id":              "1704170",
        "client_name":           "Eurocoils Limited",
        "business_type":         "Pallet Network",
        "pallets":               "1",
        "spaces":                "1.00",
        "weight":                "600",
        "po_number":             "54937",
        "order_number":          "54937",
        "collection_point":      "Eurocoils - Sittingbourne",
        "collection_date":       "31/03/2026",
        "collection_time":       "09:00",
        "delivery_point":        "AHS (Specialist Services) - Ringwood",
        "delivery_postcode":     "BH24 3AS",
        "delivery_date":         "01/04/2026",
        "delivery_time":         "09:00",
        "delivery_order_number": "46445",
        " goods_type":           "Palletised",
    },
    {
        "order_id":              "1704966",
        "client_name":           "Eurocoils Limited",
        "business_type":         "Pallet Network",
        "pallets":               "1",
        "spaces":                "1.00",
        "weight":                "600",
        "po_number":             "54941",
        "order_number":          "54941",
        "collection_point":      "Eurocoils - Sittingbourne",
        "collection_date":       "02/04/2026",
        "collection_time":       "09:00",
        "delivery_point":        "Chapman Ventilation Ltd - Welwyn Garden City",
        "delivery_postcode":     "AL7 1JQ",
        "delivery_date":         "07/04/2026",
        "delivery_time":         "09:00",
        "delivery_order_number": "46107",
        " goods_type":           "Palletised",
    },
    {
        "order_id":              "1706460",
        "client_name":           "Eurocoils Limited",
        "business_type":         "Pallet Network",
        "pallets":               "1",
        "spaces":                "1.00",
        "weight":                "600",
        "po_number":             "54956",
        "order_number":          "54956",
        "collection_point":      "Eurocoils - Sittingbourne",
        "collection_date":       "13/04/2026",
        "collection_time":       "09:00",
        "delivery_point":        "Parker Environmental Services - Pontypool",
        "delivery_postcode":     "NP4 0TW",
        "delivery_date":         "14/04/2026",
        "delivery_time":         "09:00",
        "delivery_order_number": "46432",
        " goods_type":           "Palletised",
    },
    {
        "order_id":              "1707291",
        "client_name":           "Eurocoils Limited",
        "business_type":         "Pallet Network",
        "pallets":               "1",
        "spaces":                "1.00",
        "weight":                "600",
        "po_number":             "54964",
        "order_number":          "54964",
        "collection_point":      "Eurocoils - Sittingbourne",
        "collection_date":       "14/04/2026",
        "collection_time":       "09:00",
        "delivery_point":        "HB SWS Ltd - Glossop",
        "delivery_postcode":     "SK13 7AJ",
        "delivery_date":         "15/04/2026",
        "delivery_time":         "09:00",
        "delivery_order_number": "46448",
        " goods_type":           "Palletised",
    },
    {
        "order_id":              "1707294",
        "client_name":           "Eurocoils Limited",
        "business_type":         "Pallet Network",
        "pallets":               "1",
        "spaces":                "1.00",
        "weight":                "600",
        "po_number":             "54964",
        "order_number":          "54964",
        "collection_point":      "Eurocoils - Sittingbourne",
        "collection_date":       "14/04/2026",
        "collection_time":       "09:00",
        "delivery_point":        "Johnson & Smith (Licoln) Ltd - Wellingborough",
        "delivery_postcode":     "NN8 5UB",
        "delivery_date":         "15/04/2026",
        "delivery_time":         "09:00",
        "delivery_order_number": "46477",
        " goods_type":           "Palletised",
    },
    {
        "order_id":              "1708419",
        "client_name":           "Eurocoils Limited",
        "business_type":         "Pallet Network",
        "pallets":               "1",
        "spaces":                "1.00",
        "weight":                "600",
        "po_number":             "54972",
        "order_number":          "54972",
        "collection_point":      "Eurocoils - Sittingbourne",
        "collection_date":       "20/04/2026",
        "collection_time":       "09:00",
        "delivery_point":        "VES Andover Ltd - Eastleigh",
        "delivery_postcode":     "SO53 4NF",
        "delivery_date":         "21/04/2026",
        "delivery_time":         "09:00",
        "delivery_order_number": "46488",
        " goods_type":           "Palletised",
    },
    {
        "order_id":              "1708759",
        "client_name":           "Eurocoils Limited",
        "business_type":         "Pallet Network",
        "pallets":               "1",
        "spaces":                "1.00",
        "weight":                "600",
        "po_number":             "54976",
        "order_number":          "54976",
        "collection_point":      "Eurocoils - Sittingbourne",
        "collection_date":       "20/04/2026",
        "collection_time":       "09:00",
        "delivery_point":        "Air Handling Systems Ltd - Shildon",
        "delivery_postcode":     "DL4 1QB",
        "delivery_date":         "21/04/2026",
        "delivery_time":         "09:00",
        "delivery_order_number": "46442",
        " goods_type":           "Palletised",
    },
]


def get_sheet():
    creds = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_PATH,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    gc = gspread.authorize(creds)
    return gc.open_by_key(SPREADSHEET_ID).worksheet(VERIFY_WS)


def get_existing(sheet) -> set:
    rows = sheet.get_all_records()
    return {str(r.get("delivery_order_number", "")).strip() for r in rows}


def build_row(order: dict, headers: list) -> list:
    now = datetime.now(timezone.utc).isoformat()
    full = {**order, "processed_at": now}
    return [full.get(h, "") for h in headers]


def main():
    print(f"{'DRY RUN — ' if DRY_RUN else ''}Eurocoils Verification backfill")
    sheet = get_sheet()
    headers = sheet.row_values(1)
    existing = get_existing(sheet)

    written = 0
    skipped = 0
    for order in ORDERS:
        don = order["delivery_order_number"]
        if don in existing:
            print(f"  SKIP (already exists): {don}")
            skipped += 1
            continue
        row = build_row(order, headers)
        print(f"  {'WOULD WRITE' if DRY_RUN else 'WRITING'}: {don} → {order['delivery_point']}")
        if not DRY_RUN:
            sheet.append_row(row, value_input_option="USER_ENTERED")
        written += 1

    print(f"\nDone — {written} written, {skipped} skipped")


if __name__ == "__main__":
    main()
