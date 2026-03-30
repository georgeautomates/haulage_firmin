"""
Comparison script: Actual Entry vs Verification (TMS)

Reads both worksheets from the St Regis Orders spreadsheet, joins on job number
(delivery_order_number), compares key fields, and writes results to the
Comparison tab.

Usage:
    python scripts/run_comparison.py
"""

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from firmin.clients.sheets import SheetsClient
from firmin.profiles.loader import load_all_profiles
from firmin.utils.logger import get_logger

logger = get_logger("run_comparison")

SPREADSHEET_ID = "1uEst-r23EiTyfdmL6gx_YQMuzR0s7yyx-qJVLIUyDSI"
ACTUAL_WS      = "Actual Entry"
VERIFY_WS      = "Verification"
COMPARE_WS     = "Comparison"

# Fields to compare: (label, actual_col, verify_col)
# Fields included in overall_match score
COMPARE_FIELDS = [
    ("collection_point", "collection_point", "collection_point"),
    ("delivery_point",   "delivery_point",   "delivery_point"),
    ("price",            "rate",             "rate"),
    ("order_number",     "order_number",     "order_number"),
]

# Fields shown in output but NOT counted in overall_match (informational only)
INFO_FIELDS = [
    ("collection_date",  "collection_date",  "collection_date"),
    ("delivery_date",    "delivery_date",    "delivery_date"),
]

SEP = "=" * 60


def normalise(val: str, field: str = "") -> str:
    """Lowercase, strip whitespace, normalise date formats, price, and order numbers."""
    v = val.strip().lower()
    # Normalise date: 19/03/26 -> 19/03/2026
    v = re.sub(r'\b(\d{2}/\d{2})/(\d{2})\b', lambda m: m.group(1) + "/20" + m.group(2), v)
    # Normalise time: 8:00 -> 08:00
    v = re.sub(r'\b(\d):', r'0\1:', v)
    # Normalise price: remove £ and .00
    v = re.sub(r'£', '', v)
    v = re.sub(r'\.00$', '', v)
    # Normalise order number: match on PO prefix only (before any / suffix)
    if field == "order_number":
        v = v.split("/")[0].strip()
    # Normalise delivery point: treat Kemsley aliases as equivalent
    if field == "delivery_point":
        kemsley_aliases = {
            "ds smith - sittingbourne",
            "ds smith - kemsley",
            "kemsley depot (dssr) - sittingbourne",
            "d s smith - sittingbourne",
            "ds smith paper ltd - sittingbourne",
        }
        if v in kemsley_aliases:
            v = "kemsley"
        delivery_aliases = {
            # DS Smith Devizes — with/without Ltd/Packaging
            "ds smith packaging ltd - devizes": "ds smith devizes",
            "d s smith - devizes":              "ds smith devizes",
            "ds smith - devizes":               "ds smith devizes",
            # SAICA Newport — various formats
            "newport (saica)":  "saica newport",
            "saica":            "saica newport",
            "saica - newport":  "saica newport",
            # Welton Bibby & Baron — with/without Ltd
            "welton bibby & baron - westbury":     "welton bibby baron westbury",
            "welton bibby & baron ltd - westbury": "welton bibby baron westbury",
        }
        v = delivery_aliases.get(v, v)

    # Normalise collection point: treat known name variants as equivalent
    if field == "collection_point":
        collection_aliases = {
            # Ipswich / Masons Landfill — same site, different name formats
            "ipswich (masons landfill) -": "masons landfill ipswich",
            "mason landfill - ipswich":    "masons landfill ipswich",
            "masons landfill - ipswich":   "masons landfill ipswich",
            # Enva / Envea — same company, spelling variant
            "envea  - nottingham":         "enva nottingham",
            "enva england ltd - nottingham": "enva nottingham",
            "envea - nottingham":          "enva nottingham",
            # Welton Bibby & Baron — with/without Ltd
            "welton bibby & baron - westbury":     "welton bibby baron westbury",
            "welton bibby & baron ltd - westbury": "welton bibby baron westbury",
        }
        v = collection_aliases.get(v, v)
    # Collapse multiple spaces
    v = re.sub(r'\s+', ' ', v)
    return v


def sheet_to_dicts(ws) -> list[dict]:
    rows = ws.get_all_records(numericise_ignore=["all"])
    return rows


def main():
    print(f"\n{SEP}\nCOMPARISON: Actual Entry vs Verification\n{SEP}\n")

    sheets = SheetsClient()

    print("Reading Actual Entry...")
    actual_ws = sheets._get_worksheet(SPREADSHEET_ID, ACTUAL_WS)
    actual_rows = sheet_to_dicts(actual_ws)
    print(f"  {len(actual_rows)} rows")

    print("Reading Verification...")
    verify_ws = sheets._get_worksheet(SPREADSHEET_ID, VERIFY_WS)
    verify_rows = sheet_to_dicts(verify_ws)
    print(f"  {len(verify_rows)} rows")

    def po_key(order_number: str) -> str:
        """Normalise PO number: strip PO- prefix, take part before any / suffix, lowercase."""
        v = order_number.strip().lower()
        v = re.sub(r'^po-', '', v)
        v = v.split("/")[0].strip()
        return v

    # Index actual by job number
    actual_by_job: dict[str, dict] = {}
    for row in actual_rows:
        job = str(row.get("delivery_order_number", "")).strip()
        if job:
            actual_by_job[job] = row

    # Index verification by (job_number, po_key) and also by job_number alone
    verify_by_job_po: dict[tuple, dict] = {}
    verify_by_job: dict[str, dict] = {}
    for row in verify_rows:
        job = str(row.get("delivery_order_number", "")).strip()
        po  = po_key(str(row.get("order_number", "")))
        if job:
            verify_by_job_po[(job, po)] = row
            verify_by_job[job] = row  # last row wins — fallback only

    # Match: prefer job+PO match, fall back to job-only
    matched_jobs = []
    actual_unmatched = []
    for job, a_row in sorted(actual_by_job.items()):
        po = po_key(str(a_row.get("order_number", "")))
        if (job, po) in verify_by_job_po:
            matched_jobs.append((job, verify_by_job_po[(job, po)], "exact"))
        elif job in verify_by_job:
            matched_jobs.append((job, verify_by_job[job], "job_only"))
        else:
            actual_unmatched.append(job)

    only_actual = actual_unmatched
    only_verify = sorted(set(verify_by_job) - set(actual_by_job))

    exact_count    = sum(1 for _, _, m in matched_jobs if m == "exact")
    job_only_count = sum(1 for _, _, m in matched_jobs if m == "job_only")
    print(f"\n  Matched jobs:       {len(matched_jobs)} ({exact_count} exact PO match, {job_only_count} job-number only)")
    print(f"  Only in Actual:     {len(only_actual)}")
    print(f"  Only in Verify:     {len(only_verify)}\n")


    # Build comparison rows
    comparison_rows = []
    stats = {"total": 0, "full_match": 0, "partial": 0, "no_match": 0}

    for job, v, match_type in matched_jobs:
        a = actual_by_job[job]
        stats["total"] += 1

        row = {"job_number": job, "match_type": match_type}
        field_results = []

        # Scored fields — count toward overall_match
        for label, a_col, v_col in COMPARE_FIELDS:
            a_val = str(a.get(a_col, "")).strip()
            v_val = str(v.get(v_col, "")).strip()
            match = normalise(a_val, label) == normalise(v_val, label)
            row[f"{label}_extracted"] = a_val
            row[f"{label}_proteo"]    = v_val
            row[f"{label}_match"]     = "YES" if match else "NO"
            field_results.append(match)

        # Info fields — shown but not counted in overall_match
        for label, a_col, v_col in INFO_FIELDS:
            a_val = str(a.get(a_col, "")).strip()
            v_val = str(v.get(v_col, "")).strip()
            match = normalise(a_val, label) == normalise(v_val, label)
            row[f"{label}_extracted"] = a_val
            row[f"{label}_proteo"]    = v_val
            row[f"{label}_match"]     = "YES" if match else "NO (info only)"

        matched_count = sum(field_results)
        total_fields  = len(field_results)
        if matched_count == total_fields:
            row["overall_match"] = "FULL"
            stats["full_match"] += 1
        elif matched_count == 0:
            row["overall_match"] = "NONE"
            stats["no_match"] += 1
        else:
            row["overall_match"] = f"PARTIAL ({matched_count}/{total_fields})"
            stats["partial"] += 1

        comparison_rows.append(row)

    # Print summary
    print(f"{SEP}")
    print(f"Results: {stats['total']} matched jobs")
    print(f"  Full match:    {stats['full_match']} ({stats['full_match']/max(stats['total'],1)*100:.1f}%)")
    print(f"  Partial match: {stats['partial']}")
    print(f"  No match:      {stats['no_match']}")
    print(f"{SEP}\n")

    if not comparison_rows:
        print("No matched jobs to write.")
        return

    # Write to Comparison tab
    confirm = input(f"Write {len(comparison_rows)} rows to '{COMPARE_WS}' tab? [y/N]: ").strip().lower()
    if confirm != "y":
        print("Aborted.")
        return

    print(f"\nWriting to {COMPARE_WS}...")
    compare_ws = sheets._get_worksheet(SPREADSHEET_ID, COMPARE_WS)

    # Build headers from first row
    headers = list(comparison_rows[0].keys())

    # Clear existing content then write fresh
    compare_ws.clear()
    values = [[str(row.get(h, "")) for h in headers] for row in comparison_rows]
    compare_ws.append_row(headers, value_input_option="USER_ENTERED")
    compare_ws.append_rows(values, value_input_option="USER_ENTERED")
    print(f"  {len(values)} rows written to '{COMPARE_WS}'.")
    print(f"\nDone. Open sheet: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}")


if __name__ == "__main__":
    main()
