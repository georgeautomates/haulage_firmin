"""
Check order number formats used by St Regis Reels jobs.

Reads the Actual Entry sheet and prints all distinct order_number values
for rows classified as 'St Regis Reels', so we can understand the format
and improve classification + extraction logic.

Usage:
    python scripts/check_reels_order_numbers.py
"""

import sys
from pathlib import Path
from collections import Counter

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from firmin.clients.sheets import SheetsClient

SPREADSHEET_ID = "1uEst-r23EiTyfdmL6gx_YQMuzR0s7yyx-qJVLIUyDSI"
ACTUAL_WS = "Actual Entry"
SEP = "=" * 60


def classify_format(order_number: str) -> str:
    v = order_number.strip().upper()
    if v.startswith("PO-"):
        return "PO- prefix"
    if v.startswith("SO-"):
        return "SO- prefix"
    if v.isdigit():
        return "plain number"
    if "/" in v:
        return "compound (with /)"
    if not v:
        return "EMPTY"
    return f"other: {v[:20]}"


def main():
    sheets = SheetsClient()
    ws = sheets._get_worksheet(SPREADSHEET_ID, ACTUAL_WS)
    rows = ws.get_all_records(numericise_ignore=["all"])

    reels_rows = [r for r in rows if "reel" in str(r.get("client_name", "")).lower()]
    fibre_rows = [r for r in rows if "fibre" in str(r.get("client_name", "")).lower()]
    unclassified = [r for r in rows if r not in reels_rows and r not in fibre_rows]

    print(f"\n{SEP}")
    print(f"Actual Entry: {len(rows)} total rows")
    print(f"  St Regis Reels:    {len(reels_rows)}")
    print(f"  St Regis Fibre A/C: {len(fibre_rows)}")
    print(f"  Other/unclassified: {len(unclassified)}")
    print(SEP)

    # Show Reels order number formats
    print(f"\n--- St Regis REELS order numbers ({len(reels_rows)} jobs) ---\n")
    reels_formats = Counter()
    reels_samples: dict[str, list[str]] = {}
    for row in reels_rows:
        on = str(row.get("order_number", "")).strip()
        fmt = classify_format(on)
        reels_formats[fmt] += 1
        if fmt not in reels_samples:
            reels_samples[fmt] = []
        if len(reels_samples[fmt]) < 5:
            reels_samples[fmt].append(on)

    for fmt, count in reels_formats.most_common():
        print(f"  [{fmt}] — {count} jobs")
        for s in reels_samples[fmt]:
            print(f"    e.g. '{s}'")
        print()

    # Show Fibre order number formats for comparison
    print(f"\n--- St Regis FIBRE A/C order numbers ({len(fibre_rows)} jobs) ---\n")
    fibre_formats = Counter()
    fibre_samples: dict[str, list[str]] = {}
    for row in fibre_rows:
        on = str(row.get("order_number", "")).strip()
        fmt = classify_format(on)
        fibre_formats[fmt] += 1
        if fmt not in fibre_samples:
            fibre_samples[fmt] = []
        if len(fibre_samples[fmt]) < 5:
            fibre_samples[fmt].append(on)

    for fmt, count in fibre_formats.most_common():
        print(f"  [{fmt}] — {count} jobs")
        for s in fibre_samples[fmt]:
            print(f"    e.g. '{s}'")
        print()

    print(SEP)


if __name__ == "__main__":
    main()
