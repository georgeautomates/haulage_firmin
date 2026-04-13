"""
Backfill pdf_url for existing rows in 'Actual Entry' that have a message_id
but no pdf_url.

For each such row it:
  1. Fetches the original email from Gmail by message_id
  2. Uploads the first PDF attachment to Google Drive
  3. Updates the pdf_url cell in the sheet

Usage:
    cd E:\\Arc Ai\\firmin
    python scripts/backfill_pdf_urls.py
    python scripts/backfill_pdf_urls.py --dry-run   # show what would be updated
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
from firmin.clients.gmail import GmailClient
from firmin.utils.logger import get_logger


def col_letter(zero_based_index: int) -> str:
    """Convert a 0-based column index to a spreadsheet column letter (A, B, ... Z, AA, ...)."""
    result = ""
    n = zero_based_index + 1
    while n:
        n, remainder = divmod(n - 1, 26)
        result = chr(65 + remainder) + result
    return result

logger = get_logger(__name__)

SPREADSHEET_ID = "1uEst-r23EiTyfdmL6gx_YQMuzR0s7yyx-qJVLIUyDSI"
WORKSHEET = "Actual Entry"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def get_worksheet() -> gspread.Worksheet:
    sa_path = os.getenv("GOOGLE_SERVICE_ACCOUNT_PATH", "config/service_account.json")
    creds = Credentials.from_service_account_file(sa_path, scopes=SCOPES)
    gc = gspread.authorize(creds)
    return gc.open_by_key(SPREADSHEET_ID).worksheet(WORKSHEET)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Print rows without updating")
    args = parser.parse_args()

    print("Connecting to sheet...")
    ws = get_worksheet()
    all_values = ws.get_all_values()

    if not all_values:
        print("Sheet is empty.")
        return

    headers = [h.strip().lower() for h in all_values[0]]

    def col(name: str) -> int:
        """Return 0-based column index, or -1 if not found."""
        return headers.index(name) if name in headers else -1

    msg_col = col("message_id")
    pdf_col = col("pdf_url")
    job_col = col("delivery_order_number")

    if msg_col < 0:
        print("ERROR: 'message_id' column not found in sheet.")
        return
    if pdf_col < 0:
        print("ERROR: 'pdf_url' column not found in sheet.")
        return

    # Collect rows that have a message_id but no pdf_url
    to_backfill: list[tuple[int, str, str]] = []  # (sheet_row_1based, message_id, job_number)
    for i, row in enumerate(all_values[1:], start=2):
        message_id = row[msg_col].strip() if msg_col < len(row) else ""
        pdf_url = row[pdf_col].strip() if pdf_col < len(row) else ""
        job_number = row[job_col].strip() if job_col >= 0 and job_col < len(row) else f"row{i}"
        if message_id and not pdf_url:
            to_backfill.append((i, message_id, job_number))

    print(f"Found {len(to_backfill)} rows to backfill.")

    if not to_backfill:
        print("Nothing to do.")
        return

    if args.dry_run:
        print("\n--- DRY RUN ---")
        for row_num, message_id, job_number in to_backfill:
            print(f"  Row {row_num} | job {job_number} | message_id {message_id}")
        return

    print("Connecting to Gmail and Drive...")
    gmail = GmailClient(
        token_path=os.getenv("GMAIL_TOKEN_PATH", "config/gmail_token.json"),
        credentials_path=os.getenv("GMAIL_CREDENTIALS_PATH", "config/gmail_credentials.json"),
    )
    drive = DriveClient()

    pdf_col_letter = col_letter(pdf_col)
    updated = 0
    failed = 0

    for row_num, message_id, job_number in to_backfill:
        print(f"\nProcessing job {job_number} (row {row_num}, message {message_id})...")
        try:
            email = gmail._fetch_message(gmail._get_service(), message_id)
            if not email:
                print(f"  SKIP — could not fetch email")
                failed += 1
                continue

            pdf_attachments = [
                a for a in email.attachments
                if a["filename"].lower().endswith(".pdf") or "pdf" in a.get("mime_type", "").lower()
            ]

            if not pdf_attachments:
                print(f"  SKIP — no PDF attachments found in email")
                failed += 1
                continue

            attachment = pdf_attachments[0]
            pdf_url = drive.upload_pdf(
                pdf_bytes=attachment["data"],
                filename=f"{message_id}.pdf",
            )

            # Update the cell in the sheet
            cell = f"{pdf_col_letter}{row_num}"
            ws.update(cell, [[pdf_url]])
            print(f"  OK — {pdf_url}")
            updated += 1

        except Exception as e:
            print(f"  ERROR — {e}")
            logger.exception("Failed to backfill row %d (job %s)", row_num, job_number)
            failed += 1

    print(f"\nDone. Updated: {updated} | Failed/skipped: {failed}")


if __name__ == "__main__":
    main()
