"""
Smoke test: Supabase location point lookup.

Tests a few known postcodes from the DS Smith PDF against the Location Points table.

Usage:
    python scripts/test_supabase.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from firmin.clients.supabase import SupabaseClient
from firmin.utils.logger import get_logger

logger = get_logger("test_supabase")

# Known postcodes + search strings from the PDF
TEST_CASES = [
    {
        "label": "Dartford (Data Solutions)",
        "postcode": "DA1 4QX",
        "search": "DATA SOLUTIONS UNIT 4 OPTIMA PARK DARTFORD DA1 4QX",
        "type": "collection",
    },
    {
        "label": "Kemsley Mill",
        "postcode": "ME10 2TD",
        "search": "KEMSLEY KEMSLEY MILL NR SITTINGBOURNE ME10 2TD",
        "type": "delivery",
    },
    {
        "label": "Southwark (Veolia)",
        "postcode": "SE15 1AL",
        "search": "VEOLIA BRENTWOOD 43 DEVON STREET SOUTHWARK LONDON SE15 1AL",
        "type": "collection",
    },
    {
        "label": "Avonmouth (Suez)",
        "postcode": "BS11 8AQ",
        "search": "SUEZ AVONMOUTH MRF MEREBANK ROAD BRISTOL BS11 8AQ",
        "type": "collection",
    },
    {
        "label": "Luton (Lidl)",
        "postcode": "LU5 5AY",
        "search": "LIDL WOODSIDE LINK ROAD HOUGHTON REGIS DUNSTABLE LUTON LU5 5AY",
        "type": "collection",
    },
]


def main():
    print(f"\n{'='*60}")
    print("SUPABASE LOCATION POINT LOOKUP TEST")
    print(f"{'='*60}\n")

    try:
        client = SupabaseClient()
        print("Connected to Supabase OK\n")
    except Exception as e:
        print(f"ERROR: Could not connect — {e}")
        sys.exit(1)

    all_pass = True
    for tc in TEST_CASES:
        if tc["type"] == "collection":
            result = client.lookup_collection_point(tc["postcode"], tc["search"])
        else:
            result = client.lookup_delivery_point(tc["postcode"], tc["search"])

        matched = result is not None
        mark = "MATCH" if matched else "UNMATCHED"
        print(f"  [{mark}] {tc['label']} ({tc['postcode']})")
        if matched:
            print(f"           → {result}")
        else:
            all_pass = False

    print(f"\n{'All locations matched!' if all_pass else 'Some locations unmatched — may need data in Location Points table.'}")


if __name__ == "__main__":
    main()
