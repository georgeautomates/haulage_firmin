"""
Backfill message_id and pdf_url for existing rows in 'Actual Entry'.

Scans all Gmail emails with PDF attachments, extracts job numbers from each PDF,
matches them against sheet rows that are missing message_id, then:
  - Updates the message_id column
  - Uploads the PDF to Drive and updates pdf_url

Usage:
    cd E:\\Arc Ai\\firmin
    python scripts/backfill_message_ids.py --dry-run   # show matches without writing
    python scripts/backfill_message_ids.py              # apply updates
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
from firmin.clients.pdf import extract_pdf
from firmin.utils.logger import get_logger

logger = get_logger(__name__)

SPREADSHEET_ID = "1uEst-r23EiTyfdmL6gx_YQMuzR0s7yyx-qJVLIUyDSI"
WORKSHEET = "Actual Entry"
GMAIL_QUERY = "has:attachment"  # broaden if needed

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


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
    parser.add_argument("--max-emails", type=int, default=500, help="Max emails to scan (default 500)")
    args = parser.parse_args()

    # ── Load sheet ──────────────────────────────────────────────────────────
    print("Loading sheet...")
    ws = get_worksheet()
    all_values = ws.get_all_values()
    headers = [h.strip().lower() for h in all_values[0]]

    def ci(name: str) -> int:
        return headers.index(name) if name in headers else -1

    job_col = ci("delivery_order_number")
    msg_col = ci("message_id")
    pdf_col = ci("pdf_url")

    if any(c < 0 for c in [job_col, msg_col, pdf_col]):
        print("ERROR: Missing required columns (delivery_order_number / message_id / pdf_url)")
        return

    # Build lookup: job_number -> sheet row number (1-based)
    # Only include rows that are missing message_id
    job_to_row: dict[str, int] = {}
    for i, row in enumerate(all_values[1:], start=2):
        job = row[job_col].strip() if job_col < len(row) else ""
        msg = row[msg_col].strip() if msg_col < len(row) else ""
        if job and not msg:
            job_to_row[job] = i

    print(f"Sheet rows missing message_id: {len(job_to_row)}")
    if not job_to_row:
        print("Nothing to backfill.")
        return

    # ── Scan Gmail ──────────────────────────────────────────────────────────
    print(f"Connecting to Gmail (scanning up to {args.max_emails} emails)...")
    gmail = GmailClient(
        token_path=os.getenv("GMAIL_TOKEN_PATH", "config/gmail_token.json"),
        credentials_path=os.getenv("GMAIL_CREDENTIALS_PATH", "config/gmail_credentials.json"),
    )
    service = gmail._get_service()

    # Paginate through Gmail results
    message_refs = []
    page_token = None
    while len(message_refs) < args.max_emails:
        kwargs = {"userId": "me", "q": GMAIL_QUERY, "maxResults": min(100, args.max_emails - len(message_refs))}
        if page_token:
            kwargs["pageToken"] = page_token
        resp = service.users().messages().list(**kwargs).execute()
        message_refs.extend(resp.get("messages", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    print(f"Found {len(message_refs)} emails to scan.")

    if not args.dry_run:
        drive = DriveClient()

    # Track matches: job_number -> (message_id, pdf_bytes, filename)
    matches: dict[str, tuple[str, bytes, str]] = {}
    already_matched: set[str] = set()

    for i, ref in enumerate(message_refs):
        remaining = set(job_to_row.keys()) - already_matched
        if not remaining:
            print("All jobs matched — stopping early.")
            break

        print(f"  [{i+1}/{len(message_refs)}] Scanning message {ref['id']}...", end=" ")
        try:
            email = gmail._fetch_message(service, ref["id"])
            if not email:
                print("skip (fetch failed)")
                continue

            pdf_attachments = [
                a for a in email.attachments
                if a["filename"].lower().endswith(".pdf") or "pdf" in a.get("mime_type", "").lower()
            ]
            if not pdf_attachments:
                print("skip (no PDF)")
                continue

            found_jobs = []
            for attachment in pdf_attachments:
                try:
                    result = extract_pdf(attachment["data"])
                    for job in result.job_numbers:
                        if job in remaining and job not in matches:
                            matches[job] = (email.message_id, attachment["data"], attachment["filename"])
                            already_matched.add(job)
                            found_jobs.append(job)
                except Exception as e:
                    logger.warning("PDF extract failed for %s: %s", attachment["filename"], e)

            if found_jobs:
                print(f"matched {len(found_jobs)} job(s): {', '.join(found_jobs)}")
            else:
                print("no matches")

        except Exception as e:
            print(f"error: {e}")
            logger.exception("Failed scanning message %s", ref["id"])

    print(f"\nMatched {len(matches)} / {len(job_to_row)} jobs.")

    if not matches:
        print("No matches found.")
        return

    if args.dry_run:
        print("\n--- DRY RUN: would update ---")
        for job, (msg_id, _, filename) in matches.items():
            row = job_to_row[job]
            print(f"  Row {row} | job {job} | message_id {msg_id} | file {filename}")
        return

    # ── Write updates to sheet ───────────────────────────────────────────────
    import time

    print("\nUploading PDFs and updating sheet...")
    msg_col_letter = col_letter(msg_col)
    pdf_col_letter = col_letter(pdf_col)

    updated = 0
    failed = 0

    for job, (msg_id, pdf_bytes, filename) in matches.items():
        row = job_to_row[job]
        print(f"  Job {job} (row {row})...", end=" ", flush=True)
        try:
            pdf_url = drive.upload_pdf(pdf_bytes=pdf_bytes, filename=f"{msg_id}.pdf")
            # Batch both updates into a single API call to avoid rate limits
            ws.batch_update([
                {"range": f"{msg_col_letter}{row}", "values": [[msg_id]]},
                {"range": f"{pdf_col_letter}{row}", "values": [[pdf_url]]},
            ])
            print("OK")
            updated += 1
            time.sleep(1.2)  # stay under 60 writes/min limit
        except Exception as e:
            print(f"ERROR: {e}")
            logger.exception("Failed to update row %d (job %s)", row, job)
            failed += 1
            time.sleep(2)  # back off a bit on error

    print(f"\nDone. Updated: {updated} | Failed: {failed}")
    unmatched = set(job_to_row.keys()) - already_matched
    if unmatched:
        print(f"Still unmatched ({len(unmatched)}): {', '.join(sorted(unmatched)[:20])}{'...' if len(unmatched) > 20 else ''}")


if __name__ == "__main__":
    main()
