"""
Post a Firmin vs Proteo comparison report to Slack.

Reads Actual Entry + Verification from Google Sheets, runs the same
comparison logic as run_comparison.py, and posts a summary to Slack.

Usage:
    python scripts/slack_comparison_report.py
"""

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from firmin.clients.sheets import SheetsClient
from firmin.clients.slack import SlackClient
from firmin.utils.logger import get_logger

logger = get_logger("slack_comparison_report")

SPREADSHEET_ID  = "1uEst-r23EiTyfdmL6gx_YQMuzR0s7yyx-qJVLIUyDSI"
SPREADSHEET_URL = f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}"
ACTUAL_WS       = "Actual Entry"
VERIFY_WS       = "Verification"

COMPARE_FIELDS = [
    ("collection_point", "collection_point", "collection_point"),
    ("delivery_point",   "delivery_point",   "delivery_point"),
    ("price",            "rate",             "rate"),
    ("order_number",     "order_number",     "order_number"),
]


def normalise(val: str, field: str = "") -> str:
    v = val.strip().lower()
    v = re.sub(r'\b(\d{2}/\d{2})/(\d{2})\b', lambda m: m.group(1) + "/20" + m.group(2), v)
    v = re.sub(r'\b(\d):', r'0\1:', v)
    v = re.sub(r'£', '', v)
    v = re.sub(r'\.00$', '', v)
    if field == "order_number":
        v = v.split("/")[0].strip()
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
    v = re.sub(r'\s+', ' ', v)
    return v


def main():
    sheets = SheetsClient()
    slack  = SlackClient()

    if not slack.webhook_url:
        print("ERROR: SLACK_WEBHOOK_URL not set in environment.")
        sys.exit(1)

    print("Reading Actual Entry...")
    actual_rows = sheets._get_worksheet(SPREADSHEET_ID, ACTUAL_WS).get_all_records(numericise_ignore=["all"])
    print(f"  {len(actual_rows)} rows")

    print("Reading Verification...")
    verify_rows = sheets._get_worksheet(SPREADSHEET_ID, VERIFY_WS).get_all_records(numericise_ignore=["all"])
    print(f"  {len(verify_rows)} rows")

    actual_by_job = {str(r.get("delivery_order_number", "")).strip(): r for r in actual_rows if r.get("delivery_order_number")}
    verify_by_job = {str(r.get("delivery_order_number", "")).strip(): r for r in verify_rows if r.get("delivery_order_number")}

    matched_jobs = sorted(set(actual_by_job) & set(verify_by_job))
    only_actual  = sorted(set(actual_by_job) - set(verify_by_job))
    only_verify  = sorted(set(verify_by_job) - set(actual_by_job))

    stats = {"total": 0, "full_match": 0, "partial": 0, "no_match": 0}
    field_stats = {label: {"match": 0, "total": 0} for label, _, _ in COMPARE_FIELDS}
    # Track mismatch examples per field: {label: [(job, ours, proteo), ...]}
    mismatch_examples: dict[str, list] = {label: [] for label, _, _ in COMPARE_FIELDS}

    for job in matched_jobs:
        a = actual_by_job[job]
        v = verify_by_job[job]
        stats["total"] += 1

        field_results = []
        for label, a_col, v_col in COMPARE_FIELDS:
            a_val = str(a.get(a_col, "")).strip()
            v_val = str(v.get(v_col, "")).strip()
            match = normalise(a_val, label) == normalise(v_val, label)
            field_stats[label]["total"] += 1
            if match:
                field_stats[label]["match"] += 1
            else:
                if len(mismatch_examples[label]) < 3:
                    mismatch_examples[label].append((job, a_val, v_val))
            field_results.append(match)

        matched_count = sum(field_results)
        if matched_count == len(field_results):
            stats["full_match"] += 1
        elif matched_count == 0:
            stats["no_match"] += 1
        else:
            stats["partial"] += 1

    print(f"\nMatched: {stats['total']}  Full: {stats['full_match']}  Partial: {stats['partial']}  None: {stats['no_match']}")

    ok = slack.post_comparison_report(
        total_matched=stats["total"],
        full_match=stats["full_match"],
        partial=stats["partial"],
        no_match=stats["no_match"],
        only_actual=len(only_actual),
        only_verify=len(only_verify),
        field_stats=field_stats,
        mismatch_examples=mismatch_examples,
        spreadsheet_url=SPREADSHEET_URL,
    )

    print("Slack message sent." if ok else "Slack message FAILED.")


if __name__ == "__main__":
    main()
