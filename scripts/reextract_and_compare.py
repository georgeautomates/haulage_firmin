"""
Re-extraction regression script.

Downloads every PDF from Google Drive (using pdf_url stored in Actual Entry),
re-runs the current extraction pipeline (pdf.py + ai.py + supabase.py) against
each job, and compares the fresh output to Proteo ground truth in the
Verification sheet.

Writes match-rate results to a timestamped row in the 'History' tab so George
can track how code changes improve accuracy over time.

Usage:
    python scripts/reextract_and_compare.py           # full run
    python scripts/reextract_and_compare.py --limit 20  # first N jobs (quick test)
    python scripts/reextract_and_compare.py --dry-run   # no sheet writes
"""

from __future__ import annotations

import argparse
import re
import sys
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from firmin.clients.ai import AiClient, DUAL_MODEL_FIELDS
from firmin.clients.pdf import extract_pdf
from firmin.clients.sheets import SheetsClient
from firmin.clients.supabase import SupabaseClient
from firmin.profiles.loader import load_all_profiles
from firmin.utils.logger import get_logger

logger = get_logger("reextract")

SPREADSHEET_ID = "1uEst-r23EiTyfdmL6gx_YQMuzR0s7yyx-qJVLIUyDSI"
ACTUAL_WS        = "Actual Entry"
VERIFY_WS        = "Verification"
HISTORY_WS       = "History"
REEXTRACT_WS     = "Re-extraction"

# Fields compared against Proteo ground truth
COMPARE_FIELDS = [
    ("collection_point", "collection_point", "collection_point"),
    ("delivery_point",   "delivery_point",   "delivery_point"),
    ("price",            "rate",             "rate"),
    ("order_number",     "order_number",     "order_number"),
]

SEP = "=" * 60


# ---------------------------------------------------------------------------
# Normalisation (mirrors run_comparison.py)
# ---------------------------------------------------------------------------

def normalise(val: str, field: str = "") -> str:
    v = val.strip().lower()
    v = re.sub(r'\b(\d{2}/\d{2})/(\d{2})\b', lambda m: m.group(1) + "/20" + m.group(2), v)
    v = re.sub(r'£', '', v)
    v = re.sub(r'\.00$', '', v)
    if field == "order_number":
        v = v.split("/")[0].strip()
    if field == "delivery_point":
        kemsley_aliases = {
            "ds smith - sittingbourne", "ds smith - kemsley",
            "kemsley depot (dssr) - sittingbourne", "kemsley depot",
            "d s smith - sittingbourne", "ds smith paper ltd - sittingbourne",
        }
        if v in kemsley_aliases:
            v = "kemsley"
        delivery_aliases = {
            "ds smith packaging ltd - devizes": "ds smith devizes",
            "d s smith - devizes":              "ds smith devizes",
            "ds smith - devizes":               "ds smith devizes",
            "newport (saica)":  "saica newport",
            "saica":            "saica newport",
            "saica - newport":  "saica newport",
            "welton bibby & baron - westbury":     "welton bibby baron westbury",
            "welton bibby & baron ltd - westbury": "welton bibby baron westbury",
            "vpk - banbury":                       "vpk encase banbury",
            "encase - banbury":                    "vpk encase banbury",
            "banbury (vpk - encase) - banbury":    "vpk encase banbury",
            "majestic corrugated cases ltd - wolverhampton": "wolverhampton corrugated",
            "onboard - wolverhampton":                       "wolverhampton corrugated",
            "cepac ltd - rotherham": "cepac rotherham",
            "cepac ltd":             "cepac rotherham",
            "itw angleboard - dudley":    "angleboard dudley",
            "angleboard uk ltd - dudley": "angleboard dudley",
        }
        v = delivery_aliases.get(v, v)
    if field == "collection_point":
        collection_aliases = {
            "ipswich (masons landfill) -": "masons landfill ipswich",
            "mason landfill - ipswich":    "masons landfill ipswich",
            "masons landfill - ipswich":   "masons landfill ipswich",
            "envea  - nottingham":           "enva nottingham",
            "enva england ltd - nottingham": "enva nottingham",
            "envea - nottingham":            "enva nottingham",
            "welton bibby & baron - westbury":     "welton bibby baron westbury",
            "welton bibby & baron ltd - westbury": "welton bibby baron westbury",
            "suez - huddersfield  hd1": "suez huddersfield",
            "suez. - huddersfield":     "suez huddersfield",
            "suez - huddersfield":      "suez huddersfield",
            "rcp procurement - deeside": "shotton mill deeside",
            "shotton mill site":         "shotton mill deeside",
        }
        v = collection_aliases.get(v, v)
    v = re.sub(r'\s+', ' ', v)
    return v


def po_key(order_number: str) -> str:
    v = order_number.strip().lower()
    v = re.sub(r'^po-', '', v)
    return v.split("/")[0].strip()


# ---------------------------------------------------------------------------
# PDF download from Drive
# ---------------------------------------------------------------------------

def download_pdf(pdf_url: str) -> bytes | None:
    """Download PDF bytes from a Google Drive view URL."""
    # Convert view URL to direct download URL
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


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Re-extract PDFs and compare to Proteo ground truth")
    parser.add_argument("--limit", type=int, default=0, help="Only process first N jobs (0 = all)")
    parser.add_argument("--dry-run", action="store_true", help="Do not write to History sheet")
    args = parser.parse_args()

    print(f"\n{SEP}\nRE-EXTRACTION REGRESSION TEST\n{SEP}\n")

    # Load client profile (St Regis Fibre)
    profiles = load_all_profiles()
    if not profiles:
        print("ERROR: no client profiles found in config/clients/")
        sys.exit(1)
    profile = profiles[0]
    client_name = profile.defaults.get("client_name", "")
    conditional_locations = getattr(profile, "conditional_locations", {})

    # Init clients
    sheets  = SheetsClient()
    ai      = AiClient()
    supa    = SupabaseClient()

    # Read Actual Entry
    print("Reading Actual Entry sheet...")
    actual_ws   = sheets._get_worksheet(SPREADSHEET_ID, ACTUAL_WS)
    actual_rows = actual_ws.get_all_records(numericise_ignore=["all"])
    print(f"  {len(actual_rows)} rows found")

    # Filter to rows that have a pdf_url
    rows_with_pdf = [r for r in actual_rows if str(r.get("pdf_url", "")).startswith("http")]
    print(f"  {len(rows_with_pdf)} rows have a pdf_url")

    if args.limit:
        rows_with_pdf = rows_with_pdf[:args.limit]
        print(f"  Limiting to {args.limit} jobs")

    # Read Verification sheet (Proteo ground truth)
    print("\nReading Verification sheet...")
    verify_ws   = sheets._get_worksheet(SPREADSHEET_ID, VERIFY_WS)
    verify_rows = verify_ws.get_all_records(numericise_ignore=["all"])
    print(f"  {len(verify_rows)} rows found")

    # Index verification by (job, po_key) and job alone
    verify_by_job_po: dict[tuple, dict] = {}
    verify_by_job: dict[str, dict] = {}
    for row in verify_rows:
        job = str(row.get("delivery_order_number", "")).strip()
        po  = po_key(str(row.get("order_number", "")))
        if job:
            verify_by_job_po[(job, po)] = row
            verify_by_job[job] = row

    # Group rows_with_pdf by pdf_url so we download each PDF once
    by_pdf_url: dict[str, list[dict]] = {}
    for row in rows_with_pdf:
        url = str(row["pdf_url"]).strip()
        by_pdf_url.setdefault(url, []).append(row)

    print(f"\n  {len(by_pdf_url)} unique PDFs to download\n")

    # --- Run re-extraction ---
    results = []  # list of (job, field, fresh_val, fresh_val2, proteo_val, match: bool, match2: bool)
    agreement_scores: dict[str, int] = {}  # job -> inter-model agreement score
    stats = {"processed": 0, "no_pdf": 0, "ai_fail": 0, "no_proteo": 0}

    for pdf_idx, (pdf_url, pdf_rows) in enumerate(by_pdf_url.items(), 1):
        print(f"[{pdf_idx}/{len(by_pdf_url)}] Downloading PDF for {len(pdf_rows)} job(s)...", end=" ", flush=True)

        pdf_bytes = download_pdf(pdf_url)
        if not pdf_bytes:
            print("FAILED")
            for row in pdf_rows:
                stats["no_pdf"] += 1
            continue

        pdf_result = extract_pdf(pdf_bytes)
        print(f"OK ({len(pdf_result.raw_text)} chars)")

        for row in pdf_rows:
            job = str(row.get("delivery_order_number", "")).strip()
            if not job:
                continue

            # Find Proteo row
            po  = po_key(str(row.get("order_number", "")))
            proteo = verify_by_job_po.get((job, po)) or verify_by_job.get(job)
            if not proteo:
                stats["no_proteo"] += 1
                continue

            # Re-run AI extraction — dual model
            dual = ai.extract_job_dual(pdf_result.raw_text, job)
            if not dual:
                print(f"  Job {job}: AI extraction FAILED")
                stats["ai_fail"] += 1
                continue

            # Primary (gpt-4o) location lookup
            extracted = dual.primary
            collection_point = (
                supa.lookup_location(
                    postcode=extracted.collection_postcode,
                    org_name=extracted.collection_org,
                    search=extracted.collection_search,
                    known_locations=profile.known_locations,
                    conditional_locations=conditional_locations,
                    client_name=client_name,
                    pdf_address=extracted.collection_search,
                ) or "UNMATCHED"
            )
            delivery_point = (
                supa.lookup_location(
                    postcode=extracted.delivery_postcode,
                    org_name=extracted.delivery_org,
                    search=extracted.delivery_search,
                    known_locations=profile.known_locations,
                    conditional_locations=conditional_locations,
                    client_name=client_name,
                    pdf_address=extracted.delivery_search,
                ) or extracted.delivery_org or "UNMATCHED"
            )

            # Secondary (gpt-4o-mini) location lookup
            extracted2 = dual.secondary
            collection_point2 = (
                supa.lookup_location(
                    postcode=extracted2.collection_postcode,
                    org_name=extracted2.collection_org,
                    search=extracted2.collection_search,
                    known_locations=profile.known_locations,
                    conditional_locations=conditional_locations,
                    client_name=client_name,
                    pdf_address=extracted2.collection_search,
                ) or "UNMATCHED"
            )
            delivery_point2 = (
                supa.lookup_location(
                    postcode=extracted2.delivery_postcode,
                    org_name=extracted2.delivery_org,
                    search=extracted2.delivery_search,
                    known_locations=profile.known_locations,
                    conditional_locations=conditional_locations,
                    client_name=client_name,
                    pdf_address=extracted2.delivery_search,
                ) or extracted2.delivery_org or "UNMATCHED"
            )

            fresh = {
                "collection_point": collection_point,
                "delivery_point":   delivery_point,
                "rate":             extracted.price,
                "order_number":     extracted.order_number,
            }
            fresh2 = {
                "collection_point": collection_point2,
                "delivery_point":   delivery_point2,
                "rate":             extracted2.price,
                "order_number":     extracted2.order_number,
            }

            for label, fresh_col, proteo_col in COMPARE_FIELDS:
                fv  = fresh.get(fresh_col, "")
                fv2 = fresh2.get(fresh_col, "")
                pv  = str(proteo.get(proteo_col, "")).strip()
                match  = normalise(fv,  label) == normalise(pv, label)
                match2 = normalise(fv2, label) == normalise(pv, label)
                results.append((job, label, fv, fv2, pv, match, match2))

            # Store agreement score for this job
            agreement_scores[job] = dual.agreement_score

            stats["processed"] += 1

    # --- Aggregate results ---
    print(f"\n{SEP}")
    print(f"RESULTS  (processed: {stats['processed']} | no_pdf: {stats['no_pdf']} | ai_fail: {stats['ai_fail']} | no_proteo: {stats['no_proteo']})")
    print(SEP)

    field_stats:  dict[str, dict] = {label: {"match": 0, "match2": 0, "total": 0} for label, _, _ in COMPARE_FIELDS}
    mismatches:   dict[str, list[tuple]] = {label: [] for label, _, _ in COMPARE_FIELDS}
    mismatches2:  dict[str, list[tuple]] = {label: [] for label, _, _ in COMPARE_FIELDS}

    for job, label, fv, fv2, pv, match, match2 in results:
        field_stats[label]["total"] += 1
        if match:
            field_stats[label]["match"] += 1
        else:
            mismatches[label].append((job, fv, pv))
        if match2:
            field_stats[label]["match2"] += 1
        else:
            mismatches2[label].append((job, fv2, pv))

    total_jobs    = stats["processed"]
    full_matches  = 0
    full_matches2 = 0

    job_field_results:  dict[str, list[bool]] = {}
    job_field_results2: dict[str, list[bool]] = {}
    for job, label, fv, fv2, pv, match, match2 in results:
        job_field_results.setdefault(job,  []).append(match)
        job_field_results2.setdefault(job, []).append(match2)
    for job, fr in job_field_results.items():
        if all(fr):
            full_matches += 1
    for job, fr in job_field_results2.items():
        if all(fr):
            full_matches2 += 1

    avg_agreement = (
        round(sum(agreement_scores.values()) / len(agreement_scores))
        if agreement_scores else 0
    )

    print(f"\nJobs processed:       {total_jobs}")
    print(f"Full match (gpt-4o):      {full_matches}  ({full_matches/max(total_jobs,1)*100:.1f}%)")
    print(f"Full match (gpt-4o-mini): {full_matches2} ({full_matches2/max(total_jobs,1)*100:.1f}%)")
    print(f"Avg model agreement:      {avg_agreement}%\n")

    field_rates  = {}
    field_rates2 = {}
    print(f"  {'Field':<25} {'gpt-4o':>10}   {'gpt-4o-mini':>12}")
    print(f"  {'-'*25} {'-'*10}   {'-'*12}")
    for label, _, _ in COMPARE_FIELDS:
        s = field_stats[label]
        pct  = s["match"]  / max(s["total"], 1) * 100
        pct2 = s["match2"] / max(s["total"], 1) * 100
        field_rates[label]  = pct
        field_rates2[label] = pct2
        print(f"  {label:<25} {s['match']:>4}/{s['total']:<4} {pct:>5.1f}%   {s['match2']:>4}/{s['total']:<4} {pct2:>5.1f}%")

    # Show up to 3 mismatch examples per field (primary model only)
    print(f"\n{SEP}\nMISMATCH EXAMPLES (gpt-4o)\n{SEP}")
    for label, _, _ in COMPARE_FIELDS:
        mm = mismatches[label]
        if not mm:
            print(f"\n{label}: no mismatches")
            continue
        print(f"\n{label} — {len(mm)} mismatches (showing up to 3):")
        for job, fv, pv in mm[:3]:
            print(f"  Job {job}")
            print(f"    Extracted: {fv or '(empty)'}")
            print(f"    Proteo:    {pv or '(empty)'}")

    # --- Write to History tab ---
    if args.dry_run:
        print(f"\n{SEP}\nDry run — skipping History tab write.\n")
        return

    run_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    history_row = {
        "run_at":                    run_ts,
        "jobs_processed":            total_jobs,
        "full_match_pct":            f"{full_matches/max(total_jobs,1)*100:.1f}",
        "collection_point_pct":      f"{field_rates.get('collection_point', 0):.1f}",
        "delivery_point_pct":        f"{field_rates.get('delivery_point', 0):.1f}",
        "price_pct":                 f"{field_rates.get('price', 0):.1f}",
        "order_number_pct":          f"{field_rates.get('order_number', 0):.1f}",
        # Secondary model (gpt-4o-mini)
        "m2_full_match_pct":         f"{full_matches2/max(total_jobs,1)*100:.1f}",
        "m2_collection_point_pct":   f"{field_rates2.get('collection_point', 0):.1f}",
        "m2_delivery_point_pct":     f"{field_rates2.get('delivery_point', 0):.1f}",
        "m2_price_pct":              f"{field_rates2.get('price', 0):.1f}",
        "m2_order_number_pct":       f"{field_rates2.get('order_number', 0):.1f}",
        "model_agreement_pct":       f"{avg_agreement}",
    }

    try:
        history_ws = sheets._get_worksheet(SPREADSHEET_ID, HISTORY_WS)
    except Exception:
        # Tab doesn't exist yet — create it
        print("\nCreating History tab...")
        sh = sheets._gc.open_by_key(SPREADSHEET_ID)
        history_ws = sh.add_worksheet(title=HISTORY_WS, rows=500, cols=10)
        history_ws.append_row(list(history_row.keys()), value_input_option="USER_ENTERED")
        # Cache it
        sheets._worksheets[f"{SPREADSHEET_ID}:{HISTORY_WS}"] = history_ws

    history_ws.append_row(list(history_row.values()), value_input_option="USER_ENTERED")
    print(f"\n{SEP}\nRun logged to '{HISTORY_WS}' tab at {run_ts}\n")

    # --- Write per-job detail to Re-extraction tab ---
    # Build per-job summary: one row per job with both model results
    job_data: dict[str, dict] = {}
    for job, label, fv, fv2, pv, match, match2 in results:
        if job not in job_data:
            job_data[job] = {}
        job_data[job][label] = (fv, fv2, pv, match, match2)

    reextract_headers = [
        "run_at", "job_number", "full_match", "m2_full_match", "model_agreement_score",
        "collection_point_extracted", "m2_collection_point_extracted", "collection_point_proteo", "collection_point_match", "m2_collection_point_match",
        "delivery_point_extracted",   "m2_delivery_point_extracted",   "delivery_point_proteo",   "delivery_point_match",   "m2_delivery_point_match",
        "price_extracted",            "m2_price_extracted",            "price_proteo",            "price_match",            "m2_price_match",
        "order_number_extracted",     "m2_order_number_extracted",     "order_number_proteo",     "order_number_match",     "m2_order_number_match",
    ]

    reextract_rows = []
    for job, fields in job_data.items():
        cp = fields.get("collection_point", ("", "", "", False, False))
        dp = fields.get("delivery_point",   ("", "", "", False, False))
        pr = fields.get("price",            ("", "", "", False, False))
        on = fields.get("order_number",     ("", "", "", False, False))
        all_match  = all([cp[3], dp[3], pr[3], on[3]])
        all_match2 = all([cp[4], dp[4], pr[4], on[4]])
        reextract_rows.append([
            run_ts, job,
            "TRUE" if all_match  else "FALSE",
            "TRUE" if all_match2 else "FALSE",
            str(agreement_scores.get(job, "")),
            cp[0], cp[1], cp[2], "TRUE" if cp[3] else "FALSE", "TRUE" if cp[4] else "FALSE",
            dp[0], dp[1], dp[2], "TRUE" if dp[3] else "FALSE", "TRUE" if dp[4] else "FALSE",
            pr[0], pr[1], pr[2], "TRUE" if pr[3] else "FALSE", "TRUE" if pr[4] else "FALSE",
            on[0], on[1], on[2], "TRUE" if on[3] else "FALSE", "TRUE" if on[4] else "FALSE",
        ])

    try:
        reextract_ws = sheets._get_worksheet(SPREADSHEET_ID, REEXTRACT_WS)
        # Clear existing data (full overwrite each run)
        reextract_ws.clear()
    except Exception:
        print(f"Creating '{REEXTRACT_WS}' tab...")
        sh = sheets._gc.open_by_key(SPREADSHEET_ID)
        reextract_ws = sh.add_worksheet(title=REEXTRACT_WS, rows=1000, cols=20)
        sheets._worksheets[f"{SPREADSHEET_ID}:{REEXTRACT_WS}"] = reextract_ws

    reextract_ws.append_row(reextract_headers, value_input_option="USER_ENTERED")
    if reextract_rows:
        reextract_ws.append_rows(reextract_rows, value_input_option="USER_ENTERED")
    print(f"Per-job detail written to '{REEXTRACT_WS}' tab ({len(reextract_rows)} rows)\n")


if __name__ == "__main__":
    main()
