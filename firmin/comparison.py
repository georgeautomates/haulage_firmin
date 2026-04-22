"""
Automated comparison: Actual Entry vs Verification (TMS).

Reads both worksheets, joins on delivery_order_number, compares key fields,
and writes results to the Comparison tab without any user interaction.
"""
from __future__ import annotations
import re

from firmin.utils.logger import get_logger

logger = get_logger(__name__)

SPREADSHEET_ID = "1uEst-r23EiTyfdmL6gx_YQMuzR0s7yyx-qJVLIUyDSI"
ACTUAL_WS  = "Actual Entry"
VERIFY_WS  = "Verification"
COMPARE_WS = "Comparison"

COMPARE_FIELDS = [
    ("collection_point", "collection_point", "collection_point"),
    ("delivery_point",   "delivery_point",   "delivery_point"),
    ("price",            "rate",             "rate"),
    ("order_number",     "order_number",     "order_number"),
]

INFO_FIELDS = [
    ("collection_date", "collection_date", "collection_date"),
    ("delivery_date",   "delivery_date",   "delivery_date"),
]


def normalise(val: str, field: str = "") -> str:
    v = val.strip().lower()
    v = re.sub(r'\b(\d{2}/\d{2})/(\d{2})\b', lambda m: m.group(1) + "/20" + m.group(2), v)
    v = re.sub(r'\b(\d):', r'0\1:', v)
    if field == "price":
        v = re.sub(r'[£,]', '', v)
        v = re.sub(r'\.00$', '', v)
        v = re.sub(r'\.0$', '', v)
        v = v.strip()
    else:
        v = re.sub(r'£', '', v)
        v = re.sub(r'\.00$', '', v)
    if field == "order_number":
        v = re.sub(r'^po-', '', v)
        v = re.sub(r'^[st]o-rbl-', '', v)
        v = v.split("/")[0].strip()
    v = re.sub(r'\s+', ' ', v).strip()
    if field == "delivery_point":
        kemsley_aliases = {
            "ds smith - sittingbourne", "ds smith - kemsley",
            "kemsley depot (dssr) - sittingbourne", "kemsley depot",
            "d s smith - sittingbourne", "ds smith paper ltd - sittingbourne",
            "ds smith - kemsley mill", "kemsley mill - sittingbourne",
            "kevin kemsley - sittingbourne",
        }
        if v in kemsley_aliases:
            v = "kemsley"
        delivery_aliases = {
            "ds smith packaging ltd - devizes": "ds smith devizes",
            "d s smith - devizes":              "ds smith devizes",
            "ds smith - devizes":               "ds smith devizes",
            "ds smith packaging - devizes":     "ds smith devizes",
            "newport (saica)":   "saica newport",
            "saica":             "saica newport",
            "saica - newport":   "saica newport",
            "saica pack newport": "saica newport",
            "welton bibby & baron - westbury":     "welton bibby baron westbury",
            "welton bibby & baron ltd - westbury": "welton bibby baron westbury",
            "welton bibby baron - westbury":       "welton bibby baron westbury",
            "vpk - banbury":                       "vpk encase banbury",
            "encase - banbury":                    "vpk encase banbury",
            "banbury (vpk - encase) - banbury":    "vpk encase banbury",
            "vpk encase - banbury":                "vpk encase banbury",
            "majestic corrugated cases ltd - wolverhampton": "wolverhampton corrugated",
            "onboard - wolverhampton":                       "wolverhampton corrugated",
            "majestic - wolverhampton":                      "wolverhampton corrugated",
            "cepac ltd - rotherham": "cepac rotherham",
            "cepac ltd":             "cepac rotherham",
            "cepac - rotherham":     "cepac rotherham",
            "itw angleboard - dudley":    "angleboard dudley",
            "angleboard uk ltd - dudley": "angleboard dudley",
            "angleboard - dudley":        "angleboard dudley",
            "st regis - bristol":  "mondi bristol",
            "mondi - bristol":     "mondi bristol",
            "st regis bristol":    "mondi bristol",
            "smurfit kappa - wrexham": "smurfit kappa wrexham",
            "sk - wrexham":            "smurfit kappa wrexham",
            "abercarn (kappa)":          "smurfit kappa abercarn",
            "abercarn (smurfit kappa)":  "smurfit kappa abercarn",
            "smurfit kappa - abercarn":  "smurfit kappa abercarn",
            "kappa - abercarn":          "smurfit kappa abercarn",
            "yate (smurfit kappa)":      "smurfit kappa yate",
            "smurfit kappa yate":        "smurfit kappa yate",
            "smurfit kappa - yate":      "smurfit kappa yate",
            "northampton (kappa)":       "smurfit kappa northampton",
            "kappa northampton":         "smurfit kappa northampton",
            "smurfit kappa - northampton": "smurfit kappa northampton",
            "selby (vpk)":               "vpk selby",
            "vpk - selby":               "vpk selby",
            "vpk packaging - selby":     "vpk selby",
            "wellington (vpk)":          "vpk wellington",
            "vpk - wellington":          "vpk wellington",
            "thatcham (saica)":                    "saica thatcham",
            "sca packaging uk ltd - thatcham":     "saica thatcham",
            "saica pack - thatcham":               "saica thatcham",
            "saica - thatcham":                    "saica thatcham",
            "leicester (sav eco hamilton)":        "sav eco leicester",
            "sav eco packaging - leicester":       "sav eco leicester",
            "sav eco - leicester":                 "sav eco leicester",
            "coalville (board24 ltd)":             "board24 coalville",
            "board24 ltd - coalville":             "board24 coalville",
            "board24 - coalville":                 "board24 coalville",
            "birmingham (mondi group)":            "mondi birmingham",
            "mondi group - birmingham":            "mondi birmingham",
            "mondi - birmingham":                  "mondi birmingham",
            "siniat - bristol":                    "siniat bristol",
            "siniat ltd - bristol":                "siniat bristol",
            "higher kings mill - cullompton":      "cullompton",
            "higher kings mill":                   "cullompton",
            "gxo (clipper logistic) - swadlincote": "clipper swadlincote",
            "clipper logistics - swadlincote":       "clipper swadlincote",
            "superdrug - dunstable":               "superdrug dunstable",
            "superdrug - dunstable (beds)":        "superdrug dunstable",
        }
        v = delivery_aliases.get(v, v)
    if field == "collection_point":
        collection_aliases = {
            "ipswich (masons landfill) -": "masons landfill ipswich",
            "mason landfill - ipswich":    "masons landfill ipswich",
            "masons landfill - ipswich":   "masons landfill ipswich",
            "masons - ipswich":            "masons landfill ipswich",
            "envea - nottingham":            "enva nottingham",
            "enva england ltd - nottingham": "enva nottingham",
            "enva - nottingham":             "enva nottingham",
            "enva recycling - nottingham":   "enva nottingham",
            "welton bibby & baron - westbury":     "welton bibby baron westbury",
            "welton bibby & baron ltd - westbury": "welton bibby baron westbury",
            "welton bibby baron - westbury":       "welton bibby baron westbury",
            "suez - huddersfield  hd1": "suez huddersfield",
            "suez. - huddersfield":     "suez huddersfield",
            "suez - huddersfield":      "suez huddersfield",
            "suez huddersfield hd1":    "suez huddersfield",
            "rcp procurement - deeside": "shotton mill deeside",
            "shotton mill site":         "shotton mill deeside",
            "shotton mill - deeside":    "shotton mill deeside",
            "ds smith - kemsley":              "kemsley",
            "ds smith - sittingbourne":        "kemsley",
            "kemsley depot":                   "kemsley",
            "kemsley mill":                    "kemsley",
            "kemsley mill (km)":               "kemsley",
            "kemsley mill (a)":                "kemsley",
            "kemsley mill (d)":                "kemsley",
            "ford (biffa)":             "biffa arundel",
            "biffa - arundel":          "biffa arundel",
            "biffa ford - arundel":     "biffa arundel",
            "luton (lidl)":                                   "lidl dunstable",
            "lidl regional distribution centre - dunstable":  "lidl dunstable",
            "lidl - dunstable":                               "lidl dunstable",
            "tesco (magor) - magor":            "magor trunking station",
            "magor - trunking station - magor": "magor trunking station",
            "morrison - northampton":           "swan valley northampton",
            "swan valley site 3 - northampton": "swan valley northampton",
            "smurfit kappa - wrexham": "smurfit kappa wrexham",
            "sk - wrexham":            "smurfit kappa wrexham",
            "veolia - liverpool":          "gillmoss veolia liverpool",
            "gillmoss (veolia) - liverpool": "gillmoss veolia liverpool",
            "gillmoss veolia - liverpool":   "gillmoss veolia liverpool",
            "woodgreen timber company ltd - potters bar": "chas storer potters bar",
            "chas storer - potters bar":                  "chas storer potters bar",
            "kemsley mill (a c)":      "kemsley",
            "kemsley mill (b d)":      "kemsley",
            "kemsley mill (c)":        "kemsley",
            "kemsley mill (d: pm6)":   "kemsley",
            "kemsley mill (e)":        "kemsley",
            "kemsley mill (f)":        "kemsley",
            "kirklees (suez)":               "suez huddersfield",
            "suez - kirklees":               "suez huddersfield",
            "amersham (veolia)":             "veolia amersham",
            "oldham (veolia)":               "veolia oldham",
            "portsmouth (veolia)":           "veolia portsmouth",
            "mansfield (veolia brentwood)":  "veolia mansfield",
            "mansfield (veolia)":            "veolia mansfield",
            "avonmouth (tesco)":             "tesco avonmouth",
            "tesco - avonmouth":             "tesco avonmouth",
            "reading (tesco)":               "tesco reading",
            "tesco - reading":               "tesco reading",
            "goole (tesco)":                 "tesco goole",
            "tesco - goole":                 "tesco goole",
            "irlam (biffa)":                 "biffa irlam",
            "biffa - irlam":                 "biffa irlam",
            "deeside (am recycling)":        "am recycling deeside",
            "am recycling - deeside":        "am recycling deeside",
            "bristol (boxes & pack)":        "boxes and pack bristol",
            "boxes & pack - bristol":        "boxes and pack bristol",
            "gxo (clipper logistic) - swadlincote": "clipper swadlincote",
            "clipper logistics - swadlincote":       "clipper swadlincote",
            "maldon (green recycling)":      "green recycling maldon",
            "green recycling - maldon":      "green recycling maldon",
            "swindon (wh smith plc)":        "wh smith swindon",
            "wh smith plc - swindon":        "wh smith swindon",
            "wh smith - swindon":            "wh smith swindon",
        }
        v = collection_aliases.get(v, v)
    return v


def _normalise_order(val: str) -> str:
    v = val.strip().lower()
    v = re.sub(r'\*+.*$', '', v)
    v = re.sub(r'^po-0*', '', v)
    v = re.sub(r'^[st]o-rbl-', '', v)
    v = re.sub(r'^0+', '', v)
    return v.strip()


def _po_key(order_number: str) -> str:
    v = order_number.strip().lower()
    v = re.sub(r'^po-', '', v)
    v = v.split("/")[0].strip()
    return v


def _fields_match(a_val: str, v_val: str, field: str) -> bool:
    if field != "order_number":
        return normalise(a_val, field) == normalise(v_val, field)
    a_norm = _normalise_order(a_val)
    if not a_norm:
        return False
    v_parts = [_normalise_order(p) for p in v_val.split("/")]
    v_parts = [p for p in v_parts if p]
    return a_norm in v_parts


def _client_type(name: str) -> str:
    n = name.lower()
    if "unipet" in n:
        return "unipet"
    if "revolution beauty" in n:
        return "revolution_beauty"
    if "aim" in n or "sig trading" in n:
        return "aim"
    if "community playthings" in n:
        return "community_playthings"
    if "eurocoils" in n:
        return "eurocoils"
    if "incontrast" in n or "sti line" in n:
        return "incontrast"
    return "dssmith"


def run_comparison(sheets_client) -> None:
    """
    Read Actual Entry + Verification tabs, join on delivery_order_number,
    and overwrite the Comparison tab with fresh results.
    """
    logger.info("Comparison: reading Actual Entry...")
    actual_ws = sheets_client._get_worksheet(SPREADSHEET_ID, ACTUAL_WS)
    actual_rows = actual_ws.get_all_records(numericise_ignore=["all"])
    logger.info("Comparison: %d actual rows", len(actual_rows))

    logger.info("Comparison: reading Verification...")
    verify_ws = sheets_client._get_worksheet(SPREADSHEET_ID, VERIFY_WS)
    verify_rows = verify_ws.get_all_records(numericise_ignore=["all"])
    logger.info("Comparison: %d verification rows", len(verify_rows))

    # Index actual by delivery_order_number (first occurrence wins)
    actual_by_job: dict[str, dict] = {}
    for row in actual_rows:
        job = str(row.get("delivery_order_number", "")).strip()
        if job and job not in actual_by_job:
            actual_by_job[job] = row

    # Index verification by (job, po, client_type) and (job, client_type)
    verify_by_job_po: dict[tuple, dict] = {}
    verify_by_job: dict[tuple, dict] = {}
    for row in verify_rows:
        ct = _client_type(str(row.get("client_name", "")))
        if ct == "unipet":
            job = str(row.get("order_number", "")).strip()
        else:
            job = str(row.get("delivery_order_number", "")).strip()
        po = _po_key(str(row.get("order_number", "")))
        if job:
            verify_by_job_po[(job, po, ct)] = row
            verify_by_job[(job, ct)] = row

    matched_jobs = []
    for job, a_row in sorted(actual_by_job.items()):
        po = _po_key(str(a_row.get("order_number", "")))
        ct = _client_type(str(a_row.get("client_name", "")))
        if (job, po, ct) in verify_by_job_po:
            matched_jobs.append((job, verify_by_job_po[(job, po, ct)], "exact"))
        elif (job, ct) in verify_by_job:
            matched_jobs.append((job, verify_by_job[(job, ct)], "job_only"))

    logger.info("Comparison: %d matched jobs", len(matched_jobs))

    if not matched_jobs:
        logger.info("Comparison: nothing to write")
        return

    comparison_rows = []
    for job, v, match_type in matched_jobs:
        a = actual_by_job[job]
        row: dict = {"job_number": job, "match_type": match_type}

        for label, a_col, v_col in COMPARE_FIELDS:
            a_val = str(a.get(a_col, "")).strip()
            v_val = str(v.get(v_col, "")).strip()
            match = _fields_match(a_val, v_val, label)
            row[f"{label}_extracted"] = a_val
            row[f"{label}_proteo"]    = v_val
            row[f"{label}_match"]     = "YES" if match else "NO"

        for label, a_col, v_col in INFO_FIELDS:
            a_val = str(a.get(a_col, "")).strip()
            v_val = str(v.get(v_col, "")).strip()
            match = _fields_match(a_val, v_val, label)
            row[f"{label}_extracted"] = a_val
            row[f"{label}_proteo"]    = v_val
            row[f"{label}_match"]     = "YES" if match else "NO (info only)"

        field_results = [row[f"{label}_match"] == "YES" for label, _, _ in COMPARE_FIELDS]
        matched_count = sum(field_results)
        total_fields  = len(field_results)
        if matched_count == total_fields:
            row["overall_match"] = "FULL"
        elif matched_count == 0:
            row["overall_match"] = "NONE"
        else:
            row["overall_match"] = f"PARTIAL ({matched_count}/{total_fields})"

        comparison_rows.append(row)

    compare_ws = sheets_client._get_worksheet(SPREADSHEET_ID, COMPARE_WS)
    headers = list(comparison_rows[0].keys())
    values = [[str(row.get(h, "")) for h in headers] for row in comparison_rows]

    compare_ws.clear()
    compare_ws.append_row(headers, value_input_option="USER_ENTERED")
    compare_ws.append_rows(values, value_input_option="USER_ENTERED")
    logger.info("Comparison: wrote %d rows to '%s'", len(values), COMPARE_WS)
