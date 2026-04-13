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
    # Normalise price: remove £, commas, and trailing .00 / .0
    if field == "price":
        v = re.sub(r'[£,]', '', v)        # strip £ and thousand-separator commas
        v = re.sub(r'\.00$', '', v)        # £300.00 -> 300
        v = re.sub(r'\.0$', '', v)         # £300.0  -> 300
        v = v.strip()
    else:
        v = re.sub(r'£', '', v)
        v = re.sub(r'\.00$', '', v)
    # Normalise order number: strip PO- prefix, take part before any / suffix, strip spaces
    if field == "order_number":
        v = re.sub(r'^po-', '', v)
        v = v.split("/")[0].strip()
    # Collapse multiple spaces early so alias lookups match regardless of spacing
    v = re.sub(r'\s+', ' ', v).strip()
    # Normalise delivery point: treat Kemsley aliases as equivalent
    if field == "delivery_point":
        kemsley_aliases = {
            "ds smith - sittingbourne",
            "ds smith - kemsley",
            "kemsley depot (dssr) - sittingbourne",
            "kemsley depot",
            "d s smith - sittingbourne",
            "ds smith paper ltd - sittingbourne",
            "ds smith - kemsley mill",
            "kemsley mill - sittingbourne",
        }
        if v in kemsley_aliases:
            v = "kemsley"
        delivery_aliases = {
            # DS Smith Devizes — with/without Ltd/Packaging
            "ds smith packaging ltd - devizes": "ds smith devizes",
            "d s smith - devizes":              "ds smith devizes",
            "ds smith - devizes":               "ds smith devizes",
            "ds smith packaging - devizes":     "ds smith devizes",
            # SAICA Newport — various formats
            "newport (saica)":   "saica newport",
            "saica":             "saica newport",
            "saica - newport":   "saica newport",
            "saica pack newport": "saica newport",
            # Welton Bibby & Baron — with/without Ltd
            "welton bibby & baron - westbury":     "welton bibby baron westbury",
            "welton bibby & baron ltd - westbury": "welton bibby baron westbury",
            "welton bibby baron - westbury":       "welton bibby baron westbury",
            # VPK / Encase Banbury — rebranded, Proteo uses old or new name
            "vpk - banbury":                       "vpk encase banbury",
            "encase - banbury":                    "vpk encase banbury",
            "banbury (vpk - encase) - banbury":    "vpk encase banbury",
            "vpk encase - banbury":                "vpk encase banbury",
            # Majestic / Onboard Wolverhampton — same site
            "majestic corrugated cases ltd - wolverhampton": "wolverhampton corrugated",
            "onboard - wolverhampton":                       "wolverhampton corrugated",
            "majestic - wolverhampton":                      "wolverhampton corrugated",
            # Cepac Rotherham
            "cepac ltd - rotherham": "cepac rotherham",
            "cepac ltd":             "cepac rotherham",
            "cepac - rotherham":     "cepac rotherham",
            # Angleboard Dudley
            "itw angleboard - dudley":    "angleboard dudley",
            "angleboard uk ltd - dudley": "angleboard dudley",
            "angleboard - dudley":        "angleboard dudley",
            # St Regis / Mondi — same mill, rebranded
            "st regis - bristol":  "mondi bristol",
            "mondi - bristol":     "mondi bristol",
            "st regis bristol":    "mondi bristol",
            # Smurfit Kappa variants
            "smurfit kappa - wrexham": "smurfit kappa wrexham",
            "sk - wrexham":            "smurfit kappa wrexham",
            # Smurfit Kappa Abercarn
            "abercarn (kappa)":          "smurfit kappa abercarn",
            "abercarn (smurfit kappa)":  "smurfit kappa abercarn",
            "smurfit kappa - abercarn":  "smurfit kappa abercarn",
            "kappa - abercarn":          "smurfit kappa abercarn",
            # Smurfit Kappa Yate
            "yate (smurfit kappa)":      "smurfit kappa yate",
            "smurfit kappa yate":        "smurfit kappa yate",
            "smurfit kappa - yate":      "smurfit kappa yate",
            # Kappa / Smurfit Kappa Northampton
            "northampton (kappa)":       "smurfit kappa northampton",
            "kappa northampton":         "smurfit kappa northampton",
            "smurfit kappa - northampton": "smurfit kappa northampton",
            # VPK Selby
            "selby (vpk)":               "vpk selby",
            "vpk - selby":               "vpk selby",
            # VPK Wellington
            "wellington (vpk)":          "vpk wellington",
            "vpk - wellington":          "vpk wellington",
            # SAICA / SCA Thatcham
            "thatcham (saica)":                    "saica thatcham",
            "sca packaging uk ltd - thatcham":     "saica thatcham",
            "saica pack - thatcham":               "saica thatcham",
            "saica - thatcham":                    "saica thatcham",
            # SAV Eco / Hamilton Leicester
            "leicester (sav eco hamilton)":        "sav eco leicester",
            "sav eco packaging - leicester":       "sav eco leicester",
            "sav eco - leicester":                 "sav eco leicester",
            # Board24 Coalville
            "coalville (board24 ltd)":             "board24 coalville",
            "board24 ltd - coalville":             "board24 coalville",
            "board24 - coalville":                 "board24 coalville",
            # Mondi Birmingham
            "birmingham (mondi group)":            "mondi birmingham",
            "mondi group - birmingham":            "mondi birmingham",
            "mondi - birmingham":                  "mondi birmingham",
            # Siniat Bristol
            "siniat - bristol":                    "siniat bristol",
            "siniat ltd - bristol":                "siniat bristol",
            # Cullompton (Higher Kings Mill)
            "higher kings mill - cullompton":      "cullompton",
            "higher kings mill":                   "cullompton",
        }
        v = delivery_aliases.get(v, v)

    # Normalise collection point: treat known name variants as equivalent
    if field == "collection_point":
        collection_aliases = {
            # Ipswich / Masons Landfill — same site, different name formats
            "ipswich (masons landfill) -": "masons landfill ipswich",
            "mason landfill - ipswich":    "masons landfill ipswich",
            "masons landfill - ipswich":   "masons landfill ipswich",
            "masons - ipswich":            "masons landfill ipswich",
            # Enva / Envea / Enva Recycling — same company, spelling variants
            "envea - nottingham":            "enva nottingham",
            "enva england ltd - nottingham": "enva nottingham",
            "enva - nottingham":             "enva nottingham",
            "enva recycling - nottingham":   "enva nottingham",
            # Welton Bibby & Baron — with/without Ltd
            "welton bibby & baron - westbury":     "welton bibby baron westbury",
            "welton bibby & baron ltd - westbury": "welton bibby baron westbury",
            "welton bibby baron - westbury":       "welton bibby baron westbury",
            # Suez Huddersfield — punctuation/spacing variants
            "suez - huddersfield  hd1": "suez huddersfield",
            "suez. - huddersfield":     "suez huddersfield",
            "suez - huddersfield":      "suez huddersfield",
            "suez huddersfield hd1":    "suez huddersfield",
            # RCP Procurement / Shotton Mill — same site, different name in Proteo
            "rcp procurement - deeside": "shotton mill deeside",
            "shotton mill site":         "shotton mill deeside",
            "shotton mill - deeside":    "shotton mill deeside",
            # DS Smith / Kemsley as collection point
            "ds smith - kemsley":              "kemsley",
            "ds smith - sittingbourne":        "kemsley",
            "kemsley depot":                   "kemsley",
            "kemsley mill":                    "kemsley",
            "kemsley mill (km)":               "kemsley",
            "kemsley mill (a)":                "kemsley",
            "kemsley mill (d)":                "kemsley",
            # Biffa Ford / Arundel — same site
            "ford (biffa)":             "biffa arundel",
            "biffa - arundel":          "biffa arundel",
            "biffa ford - arundel":     "biffa arundel",
            # Lidl Dunstable / Luton — same site
            "luton (lidl)":                                   "lidl dunstable",
            "lidl regional distribution centre - dunstable":  "lidl dunstable",
            "lidl - dunstable":                               "lidl dunstable",
            # Tesco Magor / Trunking Station
            "tesco (magor) - magor":            "magor trunking station",
            "magor - trunking station - magor": "magor trunking station",
            # Morrison / Swan Valley Northampton — same site
            "morrison - northampton":           "swan valley northampton",
            "swan valley site 3 - northampton": "swan valley northampton",
            # Smurfit Kappa variants
            "smurfit kappa - wrexham": "smurfit kappa wrexham",
            "sk - wrexham":            "smurfit kappa wrexham",
            # Veolia Liverpool / Gillmoss — same site
            "veolia - liverpool":          "gillmoss veolia liverpool",
            "gillmoss (veolia) - liverpool": "gillmoss veolia liverpool",
            "gillmoss veolia - liverpool":   "gillmoss veolia liverpool",
            # Woodgreen / Chas Storer Potters Bar — same site
            "woodgreen timber company ltd - potters bar": "chas storer potters bar",
            "chas storer - potters bar":                  "chas storer potters bar",
            # Additional Kemsley Mill variants (sub-bays)
            "kemsley mill (a c)":      "kemsley",
            "kemsley mill (b d)":      "kemsley",
            "kemsley mill (c)":        "kemsley",
            "kemsley mill (d: pm6)":   "kemsley",
            "kemsley mill (e)":        "kemsley",
            "kemsley mill (f)":        "kemsley",
            # Suez / Kirklees Huddersfield — same site
            "kirklees (suez)":               "suez huddersfield",
            "suez - kirklees":               "suez huddersfield",
            # Veolia collection depots
            "amersham (veolia)":             "veolia amersham",
            "oldham (veolia)":               "veolia oldham",
            "portsmouth (veolia)":           "veolia portsmouth",
            "mansfield (veolia brentwood)":  "veolia mansfield",
            "mansfield (veolia)":            "veolia mansfield",
            # Tesco collection depots
            "avonmouth (tesco)":             "tesco avonmouth",
            "tesco - avonmouth":             "tesco avonmouth",
            "reading (tesco)":               "tesco reading",
            "tesco - reading":               "tesco reading",
            "goole (tesco)":                 "tesco goole",
            "tesco - goole":                 "tesco goole",
            # Biffa Irlam (Manchester)
            "irlam (biffa)":                 "biffa irlam",
            "biffa - irlam":                 "biffa irlam",
            # AM Recycling / Deeside
            "deeside (am recycling)":        "am recycling deeside",
            "am recycling - deeside":        "am recycling deeside",
            # Boxes & Pack Bristol
            "bristol (boxes & pack)":        "boxes and pack bristol",
            "boxes & pack - bristol":        "boxes and pack bristol",
            # Green Recycling Maldon
            "maldon (green recycling)":      "green recycling maldon",
            "green recycling - maldon":      "green recycling maldon",
            # WH Smith Swindon
            "swindon (wh smith plc)":        "wh smith swindon",
            "wh smith plc - swindon":        "wh smith swindon",
            "wh smith - swindon":            "wh smith swindon",
        }
        v = collection_aliases.get(v, v)
    return v


def normalise_order(val: str) -> str:
    """Normalise a single order number token: lowercase, strip PO- prefix and junk suffixes."""
    v = val.strip().lower()
    v = re.sub(r'\*+.*$', '', v)      # strip **DEMURRAGE and similar suffixes
    v = re.sub(r'^po-0*', '', v)      # strip PO- prefix and leading zeros: PO-0804282 -> 804282
    v = re.sub(r'^0+', '', v)         # strip leading zeros from plain numbers: 01480107 -> 1480107
    return v.strip()


def fields_match(a_val: str, v_val: str, field: str) -> bool:
    """
    Compare two field values, returning True if they match after normalisation.

    For order_number, Proteo often stores a compound reference like
    'INTERNAL_ID/CUSTOMER_PO'. We check whether the extracted value matches
    EITHER part of the compound, so '1480107' matches '1842622/1480107'.
    """
    if field != "order_number":
        return normalise(a_val, field) == normalise(v_val, field)

    # Normalise extracted value (single token)
    a_norm = normalise_order(a_val)
    if not a_norm:
        return False

    # Proteo value may be compound — check every part
    v_parts = [normalise_order(p) for p in v_val.split("/")]
    v_parts = [p for p in v_parts if p]  # drop empty parts

    return a_norm in v_parts


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

    # Index actual by delivery_order_number — this is unique per job across all clients
    # (Unipet stores Delivery Note here; DS Smith stores the job/docket number)
    actual_by_job: dict[str, dict] = {}
    for row in actual_rows:
        job = str(row.get("delivery_order_number", "")).strip()
        if job and job not in actual_by_job:
            actual_by_job[job] = row

    # Index verification by (job, client_type) and (job, po, client_type)
    def client_type(name: str) -> str:
        n = name.lower()
        if "unipet" in n:
            return "unipet"
        return "dssmith"

    verify_by_job_po: dict[tuple, dict] = {}
    verify_by_job: dict[tuple, dict] = {}
    for row in verify_rows:
        ct = client_type(str(row.get("client_name", "")))
        if ct == "unipet":
            # Unipet: Actual Entry delivery_order_number = Proteo Load Number (e.g. 36259)
            # Verification order_number = getText(13) = Load Number — same value, correct join key
            job = str(row.get("order_number", "")).strip()
        else:
            job = str(row.get("delivery_order_number", "")).strip()
        po = po_key(str(row.get("order_number", "")))
        if job:
            verify_by_job_po[(job, po, ct)] = row
            verify_by_job[(job, ct)] = row  # last row wins — fallback only

    # Match: prefer job+PO match, fall back to job-only — same client type only
    matched_jobs = []
    actual_unmatched = []
    for job, a_row in sorted(actual_by_job.items()):
        po = po_key(str(a_row.get("order_number", "")))
        ct = client_type(str(a_row.get("client_name", "")))
        if (job, po, ct) in verify_by_job_po:
            matched_jobs.append((job, verify_by_job_po[(job, po, ct)], "exact"))
        elif (job, ct) in verify_by_job:
            matched_jobs.append((job, verify_by_job[(job, ct)], "job_only"))
        else:
            actual_unmatched.append(job)

    only_actual = actual_unmatched
    only_verify = sorted({job for job, ct in verify_by_job} - set(actual_by_job))

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
            match = fields_match(a_val, v_val, label)
            row[f"{label}_extracted"] = a_val
            row[f"{label}_proteo"]    = v_val
            row[f"{label}_match"]     = "YES" if match else "NO"
            field_results.append(match)

        # Info fields — shown but not counted in overall_match
        for label, a_col, v_col in INFO_FIELDS:
            a_val = str(a.get(a_col, "")).strip()
            v_val = str(v.get(v_col, "")).strip()
            match = fields_match(a_val, v_val, label)
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
