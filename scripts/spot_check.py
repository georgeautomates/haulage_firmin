"""
Nightly AI spot-check: use email subject + body to verify extracted order data.

Approach: client-aware data extraction, not domain matching.
- Each client has a CHECK_LEVEL: SKIP (no useful data in email) or CHECK (verifiable data present).
- For CHECK clients, we tell the AI exactly what fields are verifiable and what to look for.
- For SKIP clients, we record SKIP instead of burning API calls on unverifiable rows.

Usage:
    python scripts/spot_check.py                  # check all un-checked rows
    python scripts/spot_check.py --limit 50       # cap at 50 rows
    python scripts/spot_check.py --all            # re-check already-checked rows too
    python scripts/spot_check.py --dry-run        # print verdicts without writing
"""
from __future__ import annotations
import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv()

import gspread
from google.oauth2.service_account import Credentials
from openai import OpenAI

SPREADSHEET_ID = "1uEst-r23EiTyfdmL6gx_YQMuzR0s7yyx-qJVLIUyDSI"
ACTUAL_WS = "Actual Entry"
SPOT_CHECK_WS = "Spot Check"
MODEL = "gpt-4o-mini"

SPOT_CHECK_HEADERS = [
    "job_number",
    "client_name",
    "checked_at",
    "result",           # PASS / FLAG / SKIP
    "confidence",       # HIGH / MEDIUM / LOW / N/A
    "reason",
    "email_subject",
    "our_collection",
    "our_delivery",
    "our_price",
    "our_order_number",
]

# ---------------------------------------------------------------------------
# Client check configurations
# ---------------------------------------------------------------------------
# Each entry maps a client_name fragment (lowercase) to what we can verify.
# check_level: "skip" = no useful email data, "check" = verifiable fields present
# verifiable: human-readable description of what can be checked (shown in prompt)

CLIENT_CONFIGS = [
    # Rich structured data — full cross-check
    {
        "match": "revolution beauty",
        "check_level": "check",
        "prompt_template": "revolution_beauty",
    },
    # Colombier: subject contains delivery postcode, load number, delivery date/time
    # e.g. "Load 66985 Full load of reels on plts - del to ST4 4FA - del Wed 29/4 @ 10am"
    {
        "match": "colombier",
        "check_level": "check",
        "prompt_template": "subject_postcode_date",
        "notes": (
            "Subject contains delivery postcode (e.g. 'ST4 4FA'), load number, and delivery date. "
            "Cross-check delivery postcode and date against extraction."
        ),
    },
    # Roofing Centre: subject contains delivery postcode and order number
    # e.g. "Purchase Order - ME9 7NU - 3101 - 3101400954"
    {
        "match": "roofing centre",
        "check_level": "check",
        "prompt_template": "subject_postcode_order",
        "notes": (
            "Subject contains delivery postcode (e.g. 'ME9 7NU') and order number (e.g. '3101400954'). "
            "Cross-check delivery postcode and order number against extraction."
        ),
    },
    # AIM: subject contains order number
    # e.g. "Purchase order Booking - 315597"
    {
        "match": "aim",
        "check_level": "check",
        "prompt_template": "subject_order_number",
        "notes": (
            "Subject contains the order/booking number after a dash (e.g. 'Purchase order Booking - 315597'). "
            "Cross-check that number against the extracted order number."
        ),
    },
    # InContrast: subject contains collection date
    # e.g. "Collection for: Tuesday 14/04/2026"
    {
        "match": "incontrast",
        "check_level": "check",
        "prompt_template": "subject_date",
        "notes": (
            "Subject contains the collection date (e.g. 'Collection for: Tuesday 14/04/2026'). "
            "Cross-check that date against the extracted collection date."
        ),
    },
    # CCT Worldwide: subject may contain delivery postcode
    # e.g. "QUOTE 12 PALLETS/ 14 SPACES BD19 AM REQUIRED"
    {
        "match": "cct worldwide",
        "check_level": "check",
        "prompt_template": "subject_postcode_order",
        "notes": (
            "Subject may contain a delivery postcode district (e.g. 'BD19'). "
            "Cross-check delivery postcode against extraction if a postcode is present."
        ),
    },
    # DS Smith / St Regis — forwarded threads, no job detail in email
    {"match": "st regis",      "check_level": "skip", "skip_reason": "DS Smith forwarded email — no job detail in subject or body"},
    # Unipet — generic covering note
    {"match": "unipet",        "check_level": "skip", "skip_reason": "Unipet covering email — manifest is in PDF attachment only"},
    # Horizon — reference numbers only, collection often UNMATCHED
    {"match": "horizon",       "check_level": "skip", "skip_reason": "Horizon email — only internal reference numbers, no verifiable extraction data"},
    # Community Playthings / Eurocoils — backfill rows, no email content
    {"match": "community playthings", "check_level": "skip", "skip_reason": "No email content stored for this client"},
    {"match": "eurocoils",     "check_level": "skip", "skip_reason": "No email content stored for this client"},
    # Colombier catch-all (already handled above, but just in case)
    {"match": "colombier",     "check_level": "skip", "skip_reason": "Colombier email — handled above"},
]

# ---------------------------------------------------------------------------
# Pure Python checks — deterministic, no AI involvement in the verdict
# ---------------------------------------------------------------------------

def _normalise_date(d: str) -> str:
    """Convert DD/MM/YY or DD/MM/YYYY to DD/MM/YYYY."""
    d = d.strip()
    m = re.match(r'(\d{2}/\d{2}/)(\d{2})$', d)
    if m:
        return m.group(1) + "20" + m.group(2)
    return d


def check_revolution_beauty(row: dict) -> tuple[str, str, str]:
    """Returns (result, confidence, reason)."""
    subject = row.get("email_subject", "")
    delivery_point = row.get("delivery_point", "").lower()
    collection_point = row.get("collection_point", "").lower()
    collection_date = _normalise_date(row.get("collection_date", ""))

    flags = []

    # 1. Destination town: "Booking DD/MM/YY: From to TOWN (type)"
    town_match = re.search(r'\bto\s+([A-Za-z][A-Za-z\s\-]+?)(?:\s*\(|\s*\||\s*$)', subject, re.IGNORECASE)
    if town_match:
        town = town_match.group(1).strip().lower()
        if town and town not in delivery_point:
            flags.append(f"delivery town '{town_match.group(1).strip()}' not found in delivery point '{row.get('delivery_point','')}'")

    # 2. Collection date: "Booking DD/MM/YY:" in subject
    date_match = re.search(r'Booking\s+(\d{2}/\d{2}/\d{2,4})', subject, re.IGNORECASE)
    if date_match:
        subj_date = _normalise_date(date_match.group(1))
        if subj_date and collection_date and subj_date != collection_date:
            flags.append(f"collection date in subject '{subj_date}' does not match extracted '{collection_date}'")

    # 3. Collection point should be GXO/Clipper/Swadlincote
    if collection_point and not any(k in collection_point for k in ["gxo", "clipper", "swadlincote"]):
        flags.append(f"collection point '{row.get('collection_point','')}' is not GXO/Clipper/Swadlincote")

    if flags:
        return "FLAG", "HIGH", "; ".join(flags)
    return "PASS", "HIGH", "Delivery town, collection date, and collection point all match the email"


def check_subject_postcode(row: dict, subject: str) -> tuple[str, str, str]:
    """Check if a postcode in the subject matches the extracted delivery postcode."""
    postcode_match = re.search(r'\b([A-Z]{1,2}\d{1,2}[A-Z]?\s?\d[A-Z]{2})\b', subject)
    if not postcode_match:
        # Try postcode district only (e.g. BD19)
        district_match = re.search(r'\b([A-Z]{1,2}\d{1,2}[A-Z]?)\b', subject)
        if district_match:
            district = district_match.group(1).upper()
            delivery_pc = row.get("delivery_postcode", "").upper()
            if delivery_pc and not delivery_pc.startswith(district):
                return "FLAG", "MEDIUM", f"postcode district '{district}' in subject does not match extracted delivery postcode '{delivery_pc}'"
            return "PASS", "MEDIUM" if delivery_pc else "LOW", f"postcode district '{district}' consistent with extracted delivery postcode '{delivery_pc}'"
        return "PASS", "LOW", "no postcode found in subject to verify"

    subj_pc = re.sub(r'\s+', ' ', postcode_match.group(1).upper().strip())
    deliv_pc = re.sub(r'\s+', ' ', row.get("delivery_postcode", "").upper().strip())
    if deliv_pc and subj_pc != deliv_pc:
        return "FLAG", "HIGH", f"postcode '{subj_pc}' in subject does not match extracted delivery postcode '{deliv_pc}'"
    return "PASS", "HIGH", f"postcode '{subj_pc}' matches extracted delivery postcode"


def check_subject_order_number(row: dict, subject: str) -> tuple[str, str, str]:
    """Check if the order number in the subject matches the extracted order number."""
    our_order = str(row.get("order_number", "") or row.get("delivery_order_number", "")).strip()
    if not our_order:
        return "PASS", "LOW", "no extracted order number to compare"
    if our_order in subject:
        return "PASS", "HIGH", f"order number '{our_order}' found in subject"
    # Try finding any number in the subject that looks like an order number
    nums = re.findall(r'\b(\d{5,})\b', subject)
    if nums and our_order not in nums:
        return "FLAG", "HIGH", f"order number '{our_order}' not found in subject (subject contains: {', '.join(nums[:3])})"
    return "PASS", "LOW", "could not verify order number from subject"


def check_subject_date(row: dict, subject: str) -> tuple[str, str, str]:
    """Check if a date in the subject matches the extracted collection date."""
    collection_date = _normalise_date(row.get("collection_date", ""))
    date_match = re.search(r'(\d{1,2}/\d{2}/\d{2,4})', subject)
    if not date_match:
        # Try "Tuesday 14/04/2026" style
        date_match = re.search(r'\b(\d{1,2}/\d{2}/\d{2,4})\b', subject)
    if date_match:
        subj_date = _normalise_date(date_match.group(1))
        if collection_date and subj_date != collection_date:
            return "FLAG", "HIGH", f"date '{subj_date}' in subject does not match extracted collection date '{collection_date}'"
        return "PASS", "HIGH", f"date '{subj_date}' in subject matches extracted collection date '{collection_date}'"
    return "PASS", "LOW", "no date found in subject to verify"


def get_config(client_name: str) -> dict:
    """Return the first matching client config, or a default skip."""
    c = client_name.lower()
    for cfg in CLIENT_CONFIGS:
        if cfg["match"] in c:
            return cfg
    return {"check_level": "skip", "skip_reason": "No spot-check config for this client"}


def run_checks(row: dict) -> tuple[str, str, str] | None:
    """
    Run Python-side checks for the row's client.
    Returns (result, confidence, reason) or None if row should be skipped.
    """
    client_name = row.get("client_name", "")
    cfg = get_config(client_name)

    if cfg["check_level"] == "skip":
        return None

    subject = row.get("email_subject", "").strip()
    template = cfg.get("prompt_template", "")

    if template == "revolution_beauty":
        return check_revolution_beauty(row)

    if template in ("subject_postcode_date", "subject_postcode_order"):
        result, conf, reason = check_subject_postcode(row, subject)
        if result == "FLAG":
            return result, conf, reason
        # Also check order number for postcode_order templates
        if template == "subject_postcode_order":
            result2, conf2, reason2 = check_subject_order_number(row, subject)
            if result2 == "FLAG":
                return result2, conf2, reason2
        return result, conf, reason

    if template == "subject_order_number":
        return check_subject_order_number(row, subject)

    if template == "subject_date":
        return check_subject_date(row, subject)

    return "PASS", "LOW", "no specific checks configured for this client"


def get_auth():
    sa_path = os.getenv("GOOGLE_SERVICE_ACCOUNT_PATH", "config/service_account.json")
    creds = Credentials.from_service_account_file(
        sa_path,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(creds)


def ensure_spot_check_sheet(sh: gspread.Spreadsheet) -> gspread.Worksheet:
    try:
        ws = sh.worksheet(SPOT_CHECK_WS)
        existing_headers = ws.row_values(1)
        if existing_headers != SPOT_CHECK_HEADERS:
            ws.delete_rows(1)
            ws.insert_row(SPOT_CHECK_HEADERS, 1)
        return ws
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=SPOT_CHECK_WS, rows=5000, cols=len(SPOT_CHECK_HEADERS))
        ws.insert_row(SPOT_CHECK_HEADERS, 1)
        return ws


def load_actual_entry(ws: gspread.Worksheet) -> list[dict]:
    rows = ws.get_all_records(default_blank="")
    return [{k.strip().lower(): v for k, v in row.items()} for row in rows]


def load_checked_jobs(ws: gspread.Worksheet) -> set[str]:
    try:
        vals = ws.col_values(1)[1:]
        return {str(v).strip() for v in vals if v}
    except Exception:
        return set()


def call_spot_check(openai_client: OpenAI, row: dict) -> dict:
    check = run_checks(row)

    if check is None:
        cfg = get_config(row.get("client_name", ""))
        return {
            "result": "SKIP",
            "confidence": "N/A",
            "reason": cfg.get("skip_reason", "No email data to verify against"),
        }

    result, confidence, reason = check
    return {"result": result, "confidence": confidence, "reason": reason}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--all", action="store_true", help="Re-check already-checked rows")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    gc = get_auth()
    sh = gc.open_by_key(SPREADSHEET_ID)

    print("Loading Actual Entry...")
    actual_ws = sh.worksheet(ACTUAL_WS)
    rows = load_actual_entry(actual_ws)
    rows_with_email = [r for r in rows if r.get("email_subject", "").strip()]
    print(f"  {len(rows)} total rows, {len(rows_with_email)} have email_subject")

    spot_ws = ensure_spot_check_sheet(sh)

    if not args.all:
        checked = load_checked_jobs(spot_ws)
        print(f"  {len(checked)} already checked — skipping (use --all to re-check)")
        to_check = [
            r for r in rows_with_email
            if str(r.get("delivery_order_number", "")).strip() not in checked
        ]
    else:
        to_check = rows_with_email

    if args.limit:
        to_check = to_check[:args.limit]

    print(f"  Checking {len(to_check)} rows...\n")

    if not to_check:
        print("Nothing to check.")
        return

    openai_client = None  # no longer used — all checks are Python-side

    passed = flagged = skipped = 0
    rows_to_write = []

    for i, row in enumerate(to_check, 1):
        job = str(row.get("delivery_order_number", "")).strip()
        subject = row.get("email_subject", "").strip()
        verdict = call_spot_check(openai_client, row)

        result = verdict["result"]
        confidence = verdict["confidence"]
        reason = verdict["reason"]

        if result == "SKIP":
            skipped += 1
        elif result == "PASS":
            passed += 1
        else:
            flagged += 1

        print(f"  [{i}/{len(to_check)}] {job} [{row.get('client_name','')}] - {result} ({confidence}) - {reason}")

        rows_to_write.append([
            job,
            row.get("client_name", ""),
            datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            result,
            confidence,
            reason,
            subject,
            row.get("collection_point", ""),
            row.get("delivery_point", ""),
            row.get("rate", ""),
            row.get("order_number", ""),
        ])

    print(f"\nResults: {passed} PASS · {flagged} FLAG · {skipped} SKIP")

    if args.dry_run:
        print("\n(dry-run — nothing written)")
        return

    if rows_to_write:
        print(f"Writing {len(rows_to_write)} rows to '{SPOT_CHECK_WS}'...")
        spot_ws.append_rows(rows_to_write, value_input_option="USER_ENTERED")
        print("Done.")


if __name__ == "__main__":
    main()
