"""
Find missing aliases by reading all PARTIAL/NONE rows from the Comparison sheet.

Prints every mismatched field pair so you can add them to normalise() in run_comparison.py.

Usage:
    python scripts/find_missing_aliases.py
"""

import sys
from pathlib import Path
from collections import defaultdict

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from firmin.clients.sheets import SheetsClient

SPREADSHEET_ID = "1uEst-r23EiTyfdmL6gx_YQMuzR0s7yyx-qJVLIUyDSI"
COMPARE_WS = "Comparison"
SCORED_FIELDS = ["collection_point", "delivery_point", "price", "order_number"]

SEP = "=" * 60


def main():
    sheets = SheetsClient()
    ws = sheets._get_worksheet(SPREADSHEET_ID, COMPARE_WS)
    rows = ws.get_all_records(numericise_ignore=["all"])

    mismatches: dict[str, list[tuple[str, str]]] = defaultdict(list)
    total = 0
    failing = 0

    for row in rows:
        overall = str(row.get("overall_match", "")).strip()
        total += 1
        if overall == "FULL":
            continue
        failing += 1
        for field in SCORED_FIELDS:
            if str(row.get(f"{field}_match", "")).strip().upper() == "NO":
                extracted = str(row.get(f"{field}_extracted", "")).strip()
                proteo = str(row.get(f"{field}_proteo", "")).strip()
                if extracted or proteo:
                    mismatches[field].append((extracted, proteo))

    print(f"\n{SEP}")
    print(f"Comparison sheet: {total} rows, {failing} not FULL match")
    print(SEP)

    if not mismatches:
        print("\nNo mismatches found — you're at 100%!")
        return

    for field, pairs in mismatches.items():
        # Deduplicate while preserving order
        seen = set()
        unique_pairs = []
        for p in pairs:
            key = (p[0].lower(), p[1].lower())
            if key not in seen:
                seen.add(key)
                unique_pairs.append(p)

        print(f"\n[{field}] — {len(unique_pairs)} unique mismatch(es):")
        for extracted, proteo in sorted(unique_pairs):
            print(f'  extracted: "{extracted}"')
            print(f'  proteo:    "{proteo}"')
            print()

    print(SEP)
    print("Add these pairs as aliases in scripts/run_comparison.py → normalise()")
    print(SEP)


if __name__ == "__main__":
    main()
