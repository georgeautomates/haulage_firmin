"""
Backfill m2_* columns in 'Actual Entry' for historical DS Smith rows.

For each row that has a pdf_url but no m2_price, re-downloads the PDF,
re-runs extract_job_dual(), and writes the secondary model (gpt-4o-mini)
fields + model_agreement_score + model_agreement_fields back to the sheet.

Skips Unipet rows (no AI extraction).

Usage:
    python scripts/backfill_dual_model.py --dry-run
    python scripts/backfill_dual_model.py
    python scripts/backfill_dual_model.py --limit 20
"""
from __future__ import annotations

import argparse
import re
import sys
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

import gspread
from google.oauth2.service_account import Credentials

from firmin.clients.ai import AiClient
from firmin.clients.pdf import extract_pdf
from firmin.utils.logger import get_logger

logger = get_logger(__name__)

SPREADSHEET_ID = "1uEst-r23EiTyfdmL6gx_YQMuzR0s7yyx-qJVLIUyDSI"
WORKSHEET = "Actual Entry"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

M2_COLUMNS = [
    "m2_collection_org",
    "m2_collection_postcode",
    "m2_collection_date",
    "m2_collection_time",
    "m2_delivery_org",
    "m2_delivery_postcode",
    "m2_delivery_date",
    "m2_delivery_time",
    "m2_price",
    "m2_order_number",
    "m2_work_type",
    "model_agreement_score",
    "model_agreement_fields",
]


def col_letter(zero_based: int) -> str:
    result = ""
    n = zero_based + 1
    while n:
        n, r = divmod(n - 1, 26)
        result = chr(65 + r) + result
    return result


def download_pdf(pdf_url: str) -> bytes | None:
    match = re.search(r'/file/d/([^/]+)', pdf_url)
    if not match:
        return None
    file_id = match.group(1)
    download_url = f"https://drive.google.com/uc?export=download&id={file_id}"
    try:
        req = urllib.request.Request(download_url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.read()
    except Exception as e:
        logger.warning("Failed to download PDF %s: %s", file_id, e)
        return None


def get_worksheet() -> gspread.Worksheet:
    sa_path = __import__("os").getenv("GOOGLE_SERVICE_ACCOUNT_PATH", "config/service_account.json")
    creds = Credentials.from_service_account_file(sa_path, scopes=SCOPES)
    gc = gspread.authorize(creds)
    return gc.open_by_key(SPREADSHEET_ID).worksheet(WORKSHEET)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    print("Loading sheet...")
    ws = get_worksheet()
    all_values = ws.get_all_values()
    headers = [h.strip().lower() for h in all_values[0]]

    def ci(name: str) -> int:
        return headers.index(name) if name in headers else -1

    job_col     = ci("delivery_order_number")
    pdf_col     = ci("pdf_url")
    client_col  = ci("client_name")
    m2_price_col = ci("m2_price")

    missing = [c for c in M2_COLUMNS if ci(c) < 0]
    if missing:
        print(f"ERROR: Missing columns in sheet header: {missing}")
        print("Add them to row 1 of Actual Entry first.")
        return

    if job_col < 0 or pdf_col < 0:
        print("ERROR: Missing delivery_order_number or pdf_url column.")
        return

    # Collect rows to backfill: DS Smith rows with pdf_url but no m2_price
    to_backfill: list[tuple[int, str, str]] = []  # (sheet_row, job_number, pdf_url)
    for i, row in enumerate(all_values[1:], start=2):
        client  = row[client_col].strip().lower() if client_col < len(row) else ""
        job     = row[job_col].strip()            if job_col    < len(row) else ""
        pdf_url = row[pdf_col].strip()            if pdf_col    < len(row) else ""
        m2_price = row[m2_price_col].strip()      if m2_price_col < len(row) else ""

        if "unipet" in client:
            continue  # no AI for Unipet
        if not pdf_url.startswith("http"):
            continue  # no PDF to work with
        if m2_price:
            continue  # already backfilled
        if not job:
            continue

        to_backfill.append((i, job, pdf_url))

    print(f"Rows to backfill: {len(to_backfill)}")
    if not to_backfill:
        print("Nothing to do.")
        return

    if args.limit:
        to_backfill = to_backfill[:args.limit]
        print(f"Limiting to {args.limit} rows")

    if args.dry_run:
        print("\n--- DRY RUN ---")
        for row_num, job, url in to_backfill[:10]:
            print(f"  Row {row_num} | job {job} | pdf {url[:60]}...")
        if len(to_backfill) > 10:
            print(f"  ... and {len(to_backfill) - 10} more")
        return

    ai = AiClient()

    # Cache downloaded PDFs by url — many rows share the same PDF
    pdf_cache: dict[str, bytes] = {}

    # Build column letter map for the m2 columns
    col_letters = {name: col_letter(ci(name)) for name in M2_COLUMNS}

    updated = 0
    failed  = 0

    for row_num, job, pdf_url in to_backfill:
        print(f"  Row {row_num} | job {job} ...", end=" ", flush=True)

        # Download PDF (cached)
        if pdf_url not in pdf_cache:
            pdf_bytes = download_pdf(pdf_url)
            if not pdf_bytes:
                print("SKIP — PDF download failed")
                failed += 1
                continue
            pdf_cache[pdf_url] = pdf_bytes
            print("(downloaded)", end=" ", flush=True)
        else:
            print("(cached)", end=" ", flush=True)

        pdf_bytes = pdf_cache[pdf_url]

        # Extract PDF text
        try:
            pdf_result = extract_pdf(pdf_bytes)
        except Exception as e:
            print(f"SKIP — PDF extract failed: {e}")
            failed += 1
            continue

        # Run dual extraction
        dual = ai.extract_job_dual(pdf_result.raw_text, job)
        if not dual:
            print("SKIP — AI extraction failed")
            failed += 1
            continue

        s = dual.secondary
        disagreed = [f for f, ok in dual.agreement.items() if not ok]

        values = {
            "m2_collection_org":      s.collection_org,
            "m2_collection_postcode": s.collection_postcode,
            "m2_collection_date":     s.collection_date,
            "m2_collection_time":     s.collection_time,
            "m2_delivery_org":        s.delivery_org,
            "m2_delivery_postcode":   s.delivery_postcode,
            "m2_delivery_date":       s.delivery_date,
            "m2_delivery_time":       s.delivery_time,
            "m2_price":               s.price,
            "m2_order_number":        s.order_number,
            "m2_work_type":           s.work_type,
            "model_agreement_score":  str(dual.agreement_score),
            "model_agreement_fields": ", ".join(disagreed) if disagreed else "ALL_MATCH",
        }

        try:
            ws.batch_update([
                {"range": f"{col_letters[col]}{row_num}", "values": [[val]]}
                for col, val in values.items()
            ])
            print(f"OK (agree: {dual.agreement_score}%)")
            updated += 1
            time.sleep(1.2)  # Sheets rate limit
        except Exception as e:
            print(f"SHEET ERROR — {e}")
            logger.exception("Failed to update row %d (job %s)", row_num, job)
            failed += 1
            time.sleep(2)

    print(f"\nDone. Updated: {updated} | Failed: {failed}")


if __name__ == "__main__":
    main()
