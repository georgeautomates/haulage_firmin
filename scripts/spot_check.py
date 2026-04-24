"""
Nightly AI spot-check: use the email subject + body to verify that each order's
extraction landed on the correct job / client.

For each recent Actual Entry row that has email_subject populated (and hasn't
been spot-checked yet), ask gpt-4o-mini:
  "Does the extracted data look consistent with what the email says?"

Writes results to a 'Spot Check' sheet tab.

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
    "result",           # PASS or FLAG
    "confidence",       # HIGH / MEDIUM / LOW
    "reason",           # brief explanation — populated on FLAG, short note on PASS
    "email_subject",    # copied for reference
    "our_collection",
    "our_delivery",
    "our_price",
    "our_order_number",
]

# ---------------------------------------------------------------------------
# Client-aware prompts
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Domain → client name mapping (used to build the domain-only prompt)
# One entry per client profile. subject_contains keyword → expected client_name fragment.
# ---------------------------------------------------------------------------
DOMAIN_CLIENT_MAP = {
    # DS Smith sub-clients — both map from the same domain
    "dssmith.com":              ["St Regis Fibre A/C", "St Regis Reels"],
    # Other clients — single mapping
    "unipet.co.uk":             ["Unipet International Ltd"],
    "revolutionbeauty.com":     ["Revolution Beauty Ltd"],
    "cctworldwideltd.com":      ["CCT Worldwide Limited"],
    "colombier.com":            ["Colombier (UK) Ltd"],
    "danxcarousel.com":         ["Community Playthings"],
    "eurocoils.co.uk":          ["Eurocoils Limited"],
    "scangl.com":               ["Horizon International Cargo"],
    # AIM — subject keyword rather than domain
    "FIRMINS BOOKING":          ["AIM (SIG Trading Limited)"],
    "BOOKING FIRMINS":          ["AIM (SIG Trading Limited)"],
    "Purchase order Booking":   ["AIM (SIG Trading Limited)"],
    # InContrast — subject keyword
    "Collection for:":          ["STI Line Ltd T/A InContrast", "STI Line Ltd  T/A InContrast"],
    # Roofing Centre — subject keyword
    "Purchase Order - ME9 7NU": ["Roofing Centre Group Ltd"],
}

# Build a human-readable mapping string for the prompt
_DOMAIN_MAP_TEXT = "\n".join(
    f'- "{k}" → client should be: {" OR ".join(repr(v) for v in vals)}'
    for k, vals in DOMAIN_CLIENT_MAP.items()
)

PROMPT_DOMAIN_ONLY = """\
You are a quality-control assistant for a UK road haulage company.

An automated system matched a booking email to a client and extracted order details.

--- EMAIL ---
Subject: {email_subject}

--- EXTRACTED ORDER ---
Job Number:  {job_number}
Client:      {client_name}

--- PRE-MATCHED SIGNAL ---
Keyword/domain found in subject: {matched_keyword}
Expected clients for that keyword: {expected_clients}

--- TASK ---
The keyword/domain was found in the subject by the system. Your job is to confirm whether
the extracted client name is consistent with the expected clients for that keyword.

If the extracted client matches one of the expected clients → PASS (HIGH confidence).
If the extracted client does NOT match any expected client → FLAG (HIGH confidence).
If no keyword was found (matched_keyword is "none") → PASS (LOW confidence, cannot verify).

Return ONLY this JSON, no markdown:
{{
  "result": "PASS" or "FLAG",
  "confidence": "HIGH", "MEDIUM", or "LOW",
  "reason": "one sentence citing the specific evidence"
}}
"""

# Revolution Beauty: subject and body both contain structured job detail.
# Subject: "Booking DD/MM/YY: [Town] to [Town] (Load Type)"
# Body: "Collect GXO [Town]", "Deliver [Company] [Town]", "Collection DD/MM/YY @ HH:MM"
PROMPT_REVOLUTION_BEAUTY = """\
You are a quality-control assistant for a UK road haulage company.

An automated system received a Revolution Beauty booking email and extracted order details.
The email subject and body contain structured booking information — use them to verify the extraction.

--- EMAIL ---
Subject: {email_subject}
Body:
{email_body}

--- EXTRACTED ORDER ---
Job Number:       {job_number}
Client:           {client_name}
Collection Point: {collection_point}
Delivery Point:   {delivery_point}
Collection Date:  {collection_date}
Order Number:     {order_number}

--- TASK ---
The subject line follows this pattern: "Booking DD/MM/YY: [From Town] to [To Town] (Load Type)"
The body contains: "Collect GXO [Town]", "Deliver [Company] [Town]", "Collection DD/MM/YY @ HH:MM"

Cross-check these specific things:
1. Sender domain must be revolutionbeauty.com — if not, FLAG immediately.
2. The destination town in the subject (after "to") should appear somewhere in the extracted
   delivery point string. E.g. subject "to Nottingham" and delivery "Boots UK - Nottingham" → PASS.
   Only FLAG if the town is completely absent from the delivery point.
3. The delivery company+town in the body ("Deliver [Company] [Town]") — the town should match
   the delivery point. The company name may differ from our internal name, so focus on the town.
4. The collection date in the subject (DD/MM/YY) should match the extracted collection date (DD/MM/YYYY).
5. Collection point should always be GXO / Clipper / Swadlincote — if it's something else, FLAG.

FLAG only on a clear, specific contradiction where the town in the email is completely different
from the town in the extraction. Minor name formatting differences (e.g. "HUT.COM" vs "THG") are fine.

Return ONLY this JSON, no markdown:
{{
  "result": "PASS" or "FLAG",
  "confidence": "HIGH", "MEDIUM", or "LOW",
  "reason": "one sentence citing the specific field and evidence"
}}
"""


def _find_keyword(subject: str) -> tuple[str, list[str]]:
    """Return (matched_keyword, expected_clients) by scanning the subject.
    Returns ('none', []) if no known keyword is present."""
    for keyword, clients in DOMAIN_CLIENT_MAP.items():
        if keyword.lower() in subject.lower():
            return keyword, clients
    return "none", []


def build_prompt(row: dict) -> str:
    client = str(row.get("client_name", "")).lower()
    subject = row.get("email_subject", "").strip() or "(no subject)"
    job = row.get("delivery_order_number", "")
    client_name = row.get("client_name", "")

    # Revolution Beauty has rich structured data in subject + body
    if "revolution" in client:
        return PROMPT_REVOLUTION_BEAUTY.format(
            email_subject=subject,
            email_body=(row.get("email_body", "") or "")[:1500].strip() or "(no body)",
            job_number=job,
            client_name=client_name,
            collection_point=row.get("collection_point", ""),
            delivery_point=row.get("delivery_point", ""),
            collection_date=row.get("collection_date", ""),
            order_number=row.get("order_number", ""),
        )

    # All other clients: Python finds the keyword, AI confirms the match
    matched_keyword, expected_clients = _find_keyword(subject)
    return PROMPT_DOMAIN_ONLY.format(
        email_subject=subject,
        job_number=job,
        client_name=client_name,
        matched_keyword=matched_keyword,
        expected_clients=", ".join(expected_clients) if expected_clients else "unknown",
    )


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
    # Normalise keys (strip whitespace, lowercase)
    return [{k.strip().lower(): v for k, v in row.items()} for row in rows]


def load_checked_jobs(ws: gspread.Worksheet) -> set[str]:
    try:
        vals = ws.col_values(1)[1:]  # skip header
        return {str(v).strip() for v in vals if v}
    except Exception:
        return set()


def call_spot_check(client: OpenAI, row: dict) -> dict:
    prompt = build_prompt(row)

    try:
        response = client.chat.completions.create(
            model=MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
        )
        content = response.choices[0].message.content or ""
        clean = re.sub(r"```json|```", "", content).strip()
        data = json.loads(clean)
        return {
            "result": data.get("result", "FLAG").upper(),
            "confidence": data.get("confidence", "LOW").upper(),
            "reason": data.get("reason", ""),
        }
    except Exception as e:
        return {"result": "FLAG", "confidence": "LOW", "reason": f"Spot-check error: {e}"}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None, help="Max rows to check")
    parser.add_argument("--all", action="store_true", help="Re-check already-checked rows")
    parser.add_argument("--dry-run", action="store_true", help="Print verdicts without writing to sheet")
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

    print(f"  Checking {len(to_check)} rows with {MODEL}...\n")

    if not to_check:
        print("Nothing to check.")
        return

    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    passed = flagged = errors = 0
    rows_to_write = []

    for i, row in enumerate(to_check, 1):
        job = str(row.get("delivery_order_number", "")).strip()
        subject = row.get("email_subject", "").strip()
        verdict = call_spot_check(client, row)

        result = verdict["result"]
        confidence = verdict["confidence"]
        reason = verdict["reason"]

        symbol = "PASS" if result == "PASS" else "FLAG"
        print(f"  [{i}/{len(to_check)}] {job} - {symbol} ({confidence}) - {reason}")

        if result == "PASS":
            passed += 1
        else:
            flagged += 1

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

    print(f"\nResults: {passed} PASS · {flagged} FLAG · {errors} errors")

    if args.dry_run:
        print("\n(dry-run — nothing written)")
        return

    if rows_to_write:
        print(f"Writing {len(rows_to_write)} rows to '{SPOT_CHECK_WS}'...")
        spot_ws.append_rows(rows_to_write, value_input_option="USER_ENTERED")
        print("Done.")


if __name__ == "__main__":
    main()
