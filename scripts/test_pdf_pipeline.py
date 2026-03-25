"""
Smoke test: PDF extraction + AI extraction for a single job.

Usage:
    python scripts/test_pdf_pipeline.py <path_to_pdf> [job_number]

Examples:
    python scripts/test_pdf_pipeline.py "C:/Users/USERAS/Downloads/GRIGGS_Q4995597_0001.pdf"
    python scripts/test_pdf_pipeline.py "C:/Users/USERAS/Downloads/GRIGGS_Q4995597_0001.pdf" 2560920
"""

import json
import sys
from pathlib import Path

# Allow running from project root or scripts/
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from firmin.clients.pdf import extract_pdf
from firmin.clients.ai import AiClient
from firmin.utils.logger import get_logger

logger = get_logger("test_pdf_pipeline")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    pdf_path = Path(sys.argv[1])
    if not pdf_path.exists():
        print(f"ERROR: File not found: {pdf_path}")
        sys.exit(1)

    target_job = sys.argv[2] if len(sys.argv) > 2 else None

    # ── Step 1: PDF extraction ────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print("STEP 1: PDF EXTRACTION")
    print(f"{'='*60}")

    pdf_data = pdf_path.read_bytes()
    result = extract_pdf(pdf_data)

    print(f"Pages:       {result.page_count}")
    print(f"Job numbers: {result.job_numbers}")
    print(f"Postcodes:   {result.postcodes}")
    print(f"Prices:      {result.prices}")
    print(f"Dates:       {result.dates}")
    print(f"PO numbers:  {result.order_numbers}")
    print(f"\nRaw text preview (first 500 chars):\n{result.raw_text[:500]}")

    if not result.job_numbers:
        print("\nERROR: No job numbers found. Check PDF text extraction.")
        sys.exit(1)

    # ── Step 2: Pick a job to test ────────────────────────────────────────────
    if target_job:
        if target_job not in result.job_numbers:
            print(f"\nWARNING: Job {target_job} not found in PDF. Found: {result.job_numbers}")
            print("Using first job instead.")
            target_job = result.job_numbers[0]
    else:
        target_job = result.job_numbers[0]

    print(f"\n{'='*60}")
    print(f"STEP 2: AI EXTRACTION for job {target_job}")
    print(f"{'='*60}")

    ai = AiClient()
    extracted = ai.extract_job(result.raw_text, target_job)

    if not extracted:
        print("ERROR: AI extraction returned None. Check your API key and model.")
        sys.exit(1)

    print(json.dumps({
        "job_number":           extracted.job_number,
        "collection_org":       extracted.collection_org,
        "collection_address":   extracted.collection_address,
        "collection_postcode":  extracted.collection_postcode,
        "collection_date":      extracted.collection_date,
        "collection_time":      extracted.collection_time,
        "delivery_org":         extracted.delivery_org,
        "delivery_address":     extracted.delivery_address,
        "delivery_postcode":    extracted.delivery_postcode,
        "delivery_date":        extracted.delivery_date,
        "delivery_time":        extracted.delivery_time,
        "price":                extracted.price,
        "order_number":         extracted.order_number,
        "customer_ref":         extracted.customer_ref,
        "work_type":            extracted.work_type,
        "collection_search":    extracted.collection_search,
        "delivery_search":      extracted.delivery_search,
    }, indent=2))

    # ── Step 3: Validate key fields ───────────────────────────────────────────
    print(f"\n{'='*60}")
    print("STEP 3: VALIDATION")
    print(f"{'='*60}")

    checks = {
        "job_number matches":      extracted.job_number == target_job,
        "collection_postcode set": bool(extracted.collection_postcode),
        "collection_date format":  _is_date(extracted.collection_date),
        "collection_time format":  bool(extracted.collection_time),
        "delivery_postcode set":   bool(extracted.delivery_postcode),
        "delivery_date format":    _is_date(extracted.delivery_date),
        "price set":               bool(extracted.price),
        "order_number set":        bool(extracted.order_number),
    }

    all_pass = True
    for check, passed in checks.items():
        mark = "PASS" if passed else "FAIL"
        print(f"  [{mark}] {check}")
        if not passed:
            all_pass = False

    print(f"\n{'All checks passed!' if all_pass else 'Some checks failed — review AI output above.'}")

    # ── Step 4: Optionally test all jobs ──────────────────────────────────────
    if len(result.job_numbers) > 1:
        print(f"\n{'='*60}")
        print(f"NOTE: PDF contains {len(result.job_numbers)} jobs total.")
        print("To test all jobs, re-run with each job number, or extend this script.")
        print(f"All jobs: {result.job_numbers}")


def _is_date(s: str) -> bool:
    import re
    return bool(re.match(r'^\d{2}/\d{2}/\d{4}$', s or ""))


if __name__ == "__main__":
    main()
