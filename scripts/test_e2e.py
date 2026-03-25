"""
End-to-end test: PDF -> AI -> Supabase -> Sheets

Feeds a real PDF directly into the pipeline (bypassing Gmail) and writes
one job row to the Google Sheet.

Usage:
    python scripts/test_e2e.py <pdf_path> [job_number]

If job_number is omitted, the first job in the PDF is used.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from firmin.clients.ai import AiClient
from firmin.clients.pdf import extract_pdf
from firmin.clients.sheets import SheetsClient
from firmin.clients.supabase import SupabaseClient
from firmin.pipeline import Pipeline
from firmin.profiles.loader import load_all_profiles
from firmin.utils.dedup import DedupStore
from firmin.utils.logger import get_logger

logger = get_logger("test_e2e")

SEP = "=" * 60


def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/test_e2e.py <pdf_path> [job_number]")
        sys.exit(1)

    pdf_path = Path(sys.argv[1])
    if not pdf_path.exists():
        print(f"ERROR: File not found: {pdf_path}")
        sys.exit(1)

    target_job = sys.argv[2] if len(sys.argv) > 2 else None

    print(f"\n{SEP}\nEND-TO-END PIPELINE TEST\n{SEP}\n")

    # Load client profile
    profiles = load_all_profiles("config/clients")
    if not profiles:
        print("ERROR: No client profiles found in config/clients/")
        sys.exit(1)
    profile = profiles[0]
    print(f"Profile:    {profile.display_name}")
    print(f"Sheet:      {profile.sheets.spreadsheet_id}")
    print(f"Worksheet:  {profile.sheets.worksheet_name}\n")

    # Extract PDF
    print("Step 1: Extracting PDF...")
    pdf_data = pdf_path.read_bytes()
    pdf_result = extract_pdf(pdf_data)
    print(f"  Jobs found: {len(pdf_result.job_numbers)}")

    job_number = target_job or pdf_result.job_numbers[0]
    print(f"  Processing: {job_number}\n")

    if job_number not in pdf_result.job_numbers:
        print(f"ERROR: Job {job_number} not found in PDF")
        sys.exit(1)

    # Initialise clients
    print("Step 2: Initialising clients...")
    try:
        ai = AiClient()
        print("  AI:       OK")
    except Exception as e:
        print(f"  AI:       FAILED — {e}"); sys.exit(1)

    try:
        supabase = SupabaseClient()
        print("  Supabase: OK")
    except Exception as e:
        print(f"  Supabase: FAILED — {e}"); sys.exit(1)

    try:
        sheets = SheetsClient()
        print("  Sheets:   OK")
    except Exception as e:
        print(f"  Sheets:   FAILED — {e}"); sys.exit(1)

    dedup = DedupStore(":memory:")
    print("  Dedup:    OK (in-memory)\n")

    # AI extraction
    print(f"Step 3: AI extraction for job {job_number}...")
    extracted = ai.extract_job(pdf_result.raw_text, job_number)
    if not extracted:
        print("ERROR: AI extraction failed")
        sys.exit(1)

    print(f"  collection_org:      {extracted.collection_org}")
    print(f"  collection_postcode: {extracted.collection_postcode}")
    print(f"  delivery_org:        {extracted.delivery_org}")
    print(f"  delivery_postcode:   {extracted.delivery_postcode}")
    print(f"  order_number:        {extracted.order_number}")
    print(f"  price:               {extracted.price}")

    # Location lookup
    print("\nStep 4: Location lookup...")
    client_name = profile.defaults.get("client_name", "")
    collection_point = supabase.lookup_location(
        postcode=extracted.collection_postcode,
        org_name=extracted.collection_org,
        search=extracted.collection_search,
        known_locations=profile.known_locations,
        client_name=client_name,
        pdf_address=extracted.collection_search,
    ) or "UNMATCHED"
    delivery_point = supabase.lookup_location(
        postcode=extracted.delivery_postcode,
        org_name=extracted.delivery_org,
        search=extracted.delivery_search,
        known_locations=profile.known_locations,
        client_name=client_name,
        pdf_address=extracted.delivery_search,
    ) or extracted.delivery_org or "UNMATCHED"

    print(f"  collection_point: {collection_point}")
    print(f"  delivery_point:   {delivery_point}")

    # Confirm before writing
    print(f"\n{SEP}")
    confirm = input("Write this row to Google Sheets? [y/N]: ").strip().lower()
    if confirm != "y":
        print("Aborted.")
        sys.exit(0)

    # Run pipeline job directly
    print("\nStep 5: Writing to sheet...")
    pipeline = Pipeline(ai, supabase, sheets, dedup)
    order_result = pipeline._process_job(
        job_number=job_number,
        raw_text=pdf_result.raw_text,
        message_id="test_e2e",
        profile=profile,
    )

    if order_result.error:
        print(f"ERROR: {order_result.error}")
    else:
        print(f"  Written: status={order_result.status}  score={order_result.composite_score}")
        print(f"\nCheck your sheet: https://docs.google.com/spreadsheets/d/{profile.sheets.spreadsheet_id}")

    print(f"\n{SEP}\nDone.\n")


if __name__ == "__main__":
    main()
