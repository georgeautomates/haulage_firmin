"""
Backfill Eurocoils Limited historical delivery orders into Actual Entry.

Order data sourced from Proteo screenshots and PDFs.
Each order: Load Number = PO number, Docket Number = W/Order No.

Usage:
    python scripts/backfill_eurocoils.py           # apply
    python scripts/backfill_eurocoils.py --dry-run # preview only
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
        "job_number":            "46417",
        "delivery_order_number": "46417",
        "order_number":          "46417",
        "po_number":             "54939",
        "customer_ref":          "46417",
        "collection_date":       "01/04/2026",
        "collection_time":       "09:00",
        "delivery_date":         "02/04/2026",
        "delivery_time":         "09:00",
        "delivery_postcode":     "SK23 0TQ",
        "delivery_point":        "CWS (NW) Ltd - High Peak",
        "collection_point":      "Eurocoils - Sittingbourne",
        "collection_postcode":   "ME10 3RX",
        "pallets":               1,
        "spaces":                1,
        "weight":                "600",
        "client_name":           "Eurocoils Limited",
        "business_type":         "Firmin Xpress | Vans",
        "goods_type":            "Palletised",
        "price":                 "",
        "rate":                  "",
    },
    {
        "job_number":            "46424",
        "delivery_order_number": "46424",
        "order_number":          "46424",
        "po_number":             "54939",
        "customer_ref":          "46424",
        "collection_date":       "01/04/2026",
        "collection_time":       "09:00",
        "delivery_date":         "02/04/2026",
        "delivery_time":         "09:00",
        "delivery_postcode":     "BS1 6AZ",
        "delivery_point":        "NG Bailey Ltd - Bristol",
        "collection_point":      "Eurocoils - Sittingbourne",
        "collection_postcode":   "ME10 3RX",
        "pallets":               1,
        "spaces":                1,
        "weight":                "600",
        "client_name":           "Eurocoils Limited",
        "business_type":         "Firmin Xpress | Vans",
        "goods_type":            "Palletised",
        "price":                 "",
        "rate":                  "",
    },
    {
        "job_number":            "46476",
        "delivery_order_number": "46476",
        "order_number":          "46476",
        "po_number":             "54939",
        "customer_ref":          "46476",
        "collection_date":       "01/04/2026",
        "collection_time":       "09:00",
        "delivery_date":         "02/04/2026",
        "delivery_time":         "09:00",
        "delivery_postcode":     "WS11 0DJ",
        "delivery_point":        "T Gameson - Cannock",
        "collection_point":      "Eurocoils - Sittingbourne",
        "collection_postcode":   "ME10 3RX",
        "pallets":               1,
        "spaces":                1,
        "weight":                "600",
        "client_name":           "Eurocoils Limited",
        "business_type":         "Firmin Xpress | Vans",
        "goods_type":            "Palletised",
        "price":                 "",
        "rate":                  "",
    },
    {
        "job_number":            "46445",
        "delivery_order_number": "46445",
        "order_number":          "46445",
        "po_number":             "54937",
        "customer_ref":          "46445",
        "collection_date":       "31/03/2026",
        "collection_time":       "09:00",
        "delivery_date":         "01/04/2026",
        "delivery_time":         "09:00",
        "delivery_postcode":     "BH24 3AS",
        "delivery_point":        "AHS (Specialist Services) - Ringwood",
        "collection_point":      "Eurocoils - Sittingbourne",
        "collection_postcode":   "ME10 3RX",
        "pallets":               1,
        "spaces":                1,
        "weight":                "600",
        "client_name":           "Eurocoils Limited",
        "business_type":         "Firmin Xpress | Vans",
        "goods_type":            "Palletised",
        "price":                 "",
        "rate":                  "",
    },
    {
        "job_number":            "46107",
        "delivery_order_number": "46107",
        "order_number":          "46107",
        "po_number":             "54941",
        "customer_ref":          "46107",
        "collection_date":       "02/04/2026",
        "collection_time":       "09:00",
        "delivery_date":         "07/04/2026",
        "delivery_time":         "09:00",
        "delivery_postcode":     "AL7 1JQ",
        "delivery_point":        "Chapman Ventilation Ltd - Welwyn Garden City",
        "collection_point":      "Eurocoils - Sittingbourne",
        "collection_postcode":   "ME10 3RX",
        "pallets":               1,
        "spaces":                1,
        "weight":                "600",
        "client_name":           "Eurocoils Limited",
        "business_type":         "Firmin Xpress | Vans",
        "goods_type":            "Palletised",
        "price":                 "",
        "rate":                  "",
    },
    {
        "job_number":            "46432",
        "delivery_order_number": "46432",
        "order_number":          "46432",
        "po_number":             "54956",
        "customer_ref":          "46432",
        "collection_date":       "13/04/2026",
        "collection_time":       "09:00",
        "delivery_date":         "14/04/2026",
        "delivery_time":         "09:00",
        "delivery_postcode":     "NP4 0TW",
        "delivery_point":        "Parker Environmental Services - Pontypool",
        "collection_point":      "Eurocoils - Sittingbourne",
        "collection_postcode":   "ME10 3RX",
        "pallets":               1,
        "spaces":                1,
        "weight":                "600",
        "client_name":           "Eurocoils Limited",
        "business_type":         "Firmin Xpress | Vans",
        "goods_type":            "Palletised",
        "price":                 "",
        "rate":                  "",
    },
    {
        "job_number":            "46442",
        "delivery_order_number": "46442",
        "order_number":          "46442",
        "po_number":             "54976",
        "customer_ref":          "46442",
        "collection_date":       "20/04/2026",
        "collection_time":       "09:00",
        "delivery_date":         "21/04/2026",
        "delivery_time":         "09:00",
        "delivery_postcode":     "DL4 1QB",
        "delivery_point":        "Air Handling Systems Ltd - Shildon",
        "collection_point":      "Eurocoils - Sittingbourne",
        "collection_postcode":   "ME10 3RX",
        "pallets":               1,
        "spaces":                1,
        "weight":                "600",
        "client_name":           "Eurocoils Limited",
        "business_type":         "Firmin Xpress | Vans",
        "goods_type":            "Palletised",
        "price":                 "",
        "rate":                  "",
    },
    {
        "job_number":            "46488",
        "delivery_order_number": "46488",
        "order_number":          "46488",
        "po_number":             "54972",
        "customer_ref":          "46488",
        "collection_date":       "20/04/2026",
        "collection_time":       "09:00",
        "delivery_date":         "21/04/2026",
        "delivery_time":         "09:00",
        "delivery_postcode":     "SO53 4NF",
        "delivery_point":        "VES Andover Ltd - Eastleigh",
        "collection_point":      "Eurocoils - Sittingbourne",
        "collection_postcode":   "ME10 3RX",
        "pallets":               1,
        "spaces":                1,
        "weight":                "600",
        "client_name":           "Eurocoils Limited",
        "business_type":         "Firmin Xpress | Vans",
        "goods_type":            "Palletised",
        "price":                 "",
        "rate":                  "",
    },
    {
        "job_number":            "46448",
        "delivery_order_number": "46448",
        "order_number":          "46448",
        "po_number":             "54964",
        "customer_ref":          "46448",
        "collection_date":       "14/04/2026",
        "collection_time":       "09:00",
        "delivery_date":         "15/04/2026",
        "delivery_time":         "09:00",
        "delivery_postcode":     "SK13 7AJ",
        "delivery_point":        "HB SWS Ltd - Glossop",
        "collection_point":      "Eurocoils - Sittingbourne",
        "collection_postcode":   "ME10 3RX",
        "pallets":               1,
        "spaces":                1,
        "weight":                "600",
        "client_name":           "Eurocoils Limited",
        "business_type":         "Firmin Xpress | Vans",
        "goods_type":            "Palletised",
        "price":                 "",
        "rate":                  "",
    },
    {
        "job_number":            "46477",
        "delivery_order_number": "46477",
        "order_number":          "46477",
        "po_number":             "54964",
        "customer_ref":          "46477",
        "collection_date":       "14/04/2026",
        "collection_time":       "09:00",
        "delivery_date":         "15/04/2026",
        "delivery_time":         "09:00",
        "delivery_postcode":     "NN8 5UB",
        "delivery_point":        "Johnson & Smith (Licoln) Ltd - Wellingborough",
        "collection_point":      "Eurocoils - Sittingbourne",
        "collection_postcode":   "ME10 3RX",
        "pallets":               1,
        "spaces":                1,
        "weight":                "600",
        "client_name":           "Eurocoils Limited",
        "business_type":         "Firmin Xpress | Vans",
        "goods_type":            "Palletised",
        "price":                 "",
        "rate":                  "",
    },
]


def get_sheet():
    creds = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_PATH,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    gc = gspread.authorize(creds)
    return gc.open_by_key(SPREADSHEET_ID).worksheet("Actual Entry")


def get_existing_delivery_order_numbers(sheet) -> set:
    rows = sheet.get_all_records()
    return {str(r.get("delivery_order_number", "")).strip() for r in rows}


def build_row(order: dict, headers: list) -> list:
    now = datetime.now(timezone.utc).isoformat()
    full = {**order, "status": "GREEN", "composite_score": 95,
            "processed_at": now, "message_id": "backfill", "pdf_url": "",
            "email_subject": "backfill", "email_body": ""}
    return [full.get(h, "") for h in headers]


def main():
    print(f"{'DRY RUN — ' if DRY_RUN else ''}Eurocoils backfill")
    sheet = get_sheet()
    headers = sheet.row_values(1)
    existing = get_existing_delivery_order_numbers(sheet)

    written = 0
    skipped = 0
    for order in ORDERS:
        don = order["delivery_order_number"]
        if don in existing:
            print(f"  SKIP (already exists): {don}")
            skipped += 1
            continue
        row = build_row(order, headers)
        print(f"  {'WOULD WRITE' if DRY_RUN else 'WRITING'}: {don} "
              f"→ {order['delivery_point']} ({order['delivery_postcode']})")
        if not DRY_RUN:
            sheet.append_row(row, value_input_option="USER_ENTERED")
        written += 1

    print(f"\nDone — {written} written, {skipped} skipped")


if __name__ == "__main__":
    main()
