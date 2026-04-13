"""
Backfill email_subject and email_body for existing rows in 'Actual Entry'
that have a message_id but no email_subject.

Usage:
    python scripts/backfill_email_subject_body.py --dry-run
    python scripts/backfill_email_subject_body.py
"""
from __future__ import annotations
import argparse
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv()

import gspread
from google.oauth2.service_account import Credentials

from firmin.clients.gmail import GmailClient
from firmin.utils.logger import get_logger

logger = get_logger(__name__)

SPREADSHEET_ID = "1uEst-r23EiTyfdmL6gx_YQMuzR0s7yyx-qJVLIUyDSI"
WORKSHEET = "Actual Entry"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def col_letter(zero_based: int) -> str:
    result = ""
    n = zero_based + 1
    while n:
        n, r = divmod(n - 1, 26)
        result = chr(65 + r) + result
    return result


def get_worksheet() -> gspread.Worksheet:
    sa_path = os.getenv("GOOGLE_SERVICE_ACCOUNT_PATH", "config/service_account.json")
    creds = Credentials.from_service_account_file(sa_path, scopes=SCOPES)
    gc = gspread.authorize(creds)
    return gc.open_by_key(SPREADSHEET_ID).worksheet(WORKSHEET)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    print("Loading sheet...")
    ws = get_worksheet()
    all_values = ws.get_all_values()
    headers = [h.strip().lower() for h in all_values[0]]

    def ci(name: str) -> int:
        return headers.index(name) if name in headers else -1

    msg_col    = ci("message_id")
    subj_col   = ci("email_subject")
    body_col   = ci("email_body")
    job_col    = ci("delivery_order_number")

    if msg_col < 0:
        print("ERROR: message_id column not found.")
        return
    if subj_col < 0 or body_col < 0:
        print("ERROR: email_subject or email_body column not found — add them to the sheet header first.")
        return

    # Collect rows missing email_subject
    to_backfill: list[tuple[int, str, str]] = []  # (sheet_row, message_id, job_number)
    for i, row in enumerate(all_values[1:], start=2):
        msg_id  = row[msg_col].strip()  if msg_col  < len(row) else ""
        subject = row[subj_col].strip() if subj_col < len(row) else ""
        job     = row[job_col].strip()  if job_col  < len(row) else f"row{i}"
        if msg_id and not subject:
            to_backfill.append((i, msg_id, job))

    print(f"Rows to backfill: {len(to_backfill)}")
    if not to_backfill:
        print("Nothing to do.")
        return

    if args.dry_run:
        print("\n--- DRY RUN ---")
        for row_num, msg_id, job in to_backfill[:10]:
            print(f"  Row {row_num} | job {job} | message_id {msg_id}")
        if len(to_backfill) > 10:
            print(f"  ... and {len(to_backfill) - 10} more")
        return

    print("Connecting to Gmail...")
    gmail = GmailClient(
        token_path=os.getenv("GMAIL_TOKEN_PATH", "config/gmail_token.json"),
        credentials_path=os.getenv("GMAIL_CREDENTIALS_PATH", "config/gmail_credentials.json"),
    )
    service = gmail._get_service()

    subj_col_letter = col_letter(subj_col)
    body_col_letter = col_letter(body_col)

    # Cache fetched emails by message_id — many rows share the same email
    cache: dict[str, tuple[str, str]] = {}  # message_id -> (subject, body)

    updated = 0
    failed  = 0

    for row_num, msg_id, job in to_backfill:
        print(f"  Row {row_num} | job {job} ...", end=" ", flush=True)

        if msg_id in cache:
            subject, body = cache[msg_id]
            print("(cached)", end=" ", flush=True)
        else:
            try:
                email = gmail._fetch_message(service, msg_id)
                if not email:
                    print("SKIP — fetch failed")
                    failed += 1
                    continue
                subject = email.subject
                body    = email.body
                cache[msg_id] = (subject, body)
            except Exception as e:
                print(f"ERROR — {e}")
                logger.exception("Failed to fetch message %s", msg_id)
                failed += 1
                time.sleep(2)
                continue

        try:
            ws.batch_update([
                {"range": f"{subj_col_letter}{row_num}", "values": [[subject]]},
                {"range": f"{body_col_letter}{row_num}", "values": [[body]]},
            ])
            print(f"OK — {subject[:60]}")
            updated += 1
            time.sleep(1.2)  # stay under Sheets rate limit
        except Exception as e:
            print(f"SHEET ERROR — {e}")
            logger.exception("Failed to update row %d", row_num)
            failed += 1
            time.sleep(2)

    print(f"\nDone. Updated: {updated} | Failed: {failed}")


if __name__ == "__main__":
    main()
