"""
Unit tests for the normalise() function in run_comparison.py.
No credentials or external services required.

Usage:
    python scripts/test_normalise.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

# Import normalise and fields_match directly — no network calls involved
from scripts.run_comparison import normalise, fields_match

PASS = "\033[32mPASS\033[0m"
FAIL = "\033[31mFAIL\033[0m"


def check(description: str, got: str, expected: str):
    ok = got == expected
    status = PASS if ok else FAIL
    print(f"  [{status}] {description}")
    if not ok:
        print(f"          got:      {repr(got)}")
        print(f"          expected: {repr(expected)}")
    return ok


def run_suite(suite_name: str, cases: list[tuple]) -> tuple[int, int]:
    print(f"\n{suite_name}")
    print("-" * len(suite_name))
    passed = 0
    for args in cases:
        desc, val, field, expected = args
        got = normalise(val, field)
        if check(desc, got, expected):
            passed += 1
    return passed, len(cases)


# ---------------------------------------------------------------------------
# Price normalisation
# ---------------------------------------------------------------------------
price_cases = [
    ("strips £ symbol",            "£300.00",    "price", "300"),
    ("strips .00 suffix",          "£490.00",    "price", "490"),
    ("strips .0 suffix",           "£490.0",     "price", "490"),
    ("strips thousand comma",      "£1,200.00",  "price", "1200"),
    ("strips comma + .00",         "£2,500.00",  "price", "2500"),
    ("no trailing zeros",          "£750",       "price", "750"),
    ("handles leading space",      " £300.00",   "price", "300"),
]

# ---------------------------------------------------------------------------
# Order number normalisation
# ---------------------------------------------------------------------------
order_cases = [
    ("strips PO- prefix",          "PO-0804230", "order_number", "0804230"),
    ("lowercase po- prefix",       "po-0804230", "order_number", "0804230"),
    ("plain numeric PO",           "1838735",    "order_number", "1838735"),
    ("strips /suffix and PO-",     "PO-12345/A", "order_number", "12345"),
    ("no change needed",           "0804230",    "order_number", "0804230"),
]

# ---------------------------------------------------------------------------
# Delivery point aliases
# ---------------------------------------------------------------------------
delivery_cases = [
    # Kemsley group
    ("kemsley: ds smith sittingbourne",    "DS Smith - Sittingbourne",             "delivery_point", "kemsley"),
    ("kemsley: ds smith kemsley",          "DS Smith - Kemsley",                   "delivery_point", "kemsley"),
    ("kemsley: kemsley depot dssr",        "Kemsley Depot (DSSR) - Sittingbourne", "delivery_point", "kemsley"),
    ("kemsley: kemsley depot",             "Kemsley Depot",                        "delivery_point", "kemsley"),
    ("kemsley: d s smith sittingbourne",   "D S Smith - Sittingbourne",            "delivery_point", "kemsley"),
    ("kemsley: ds smith paper ltd",        "DS Smith Paper Ltd - Sittingbourne",   "delivery_point", "kemsley"),
    ("kemsley: kemsley mill",              "DS Smith - Kemsley Mill",              "delivery_point", "kemsley"),
    # DS Smith Devizes
    ("devizes: packaging ltd",             "DS Smith Packaging Ltd - Devizes",     "delivery_point", "ds smith devizes"),
    ("devizes: d s smith",                 "D S Smith - Devizes",                  "delivery_point", "ds smith devizes"),
    ("devizes: ds smith",                  "DS Smith - Devizes",                   "delivery_point", "ds smith devizes"),
    # SAICA Newport
    ("saica: newport (saica)",             "Newport (SAICA)",                      "delivery_point", "saica newport"),
    ("saica: plain",                       "SAICA",                                "delivery_point", "saica newport"),
    ("saica: saica - newport",             "SAICA - Newport",                      "delivery_point", "saica newport"),
    # Welton Bibby
    ("welton: without ltd",                "Welton Bibby & Baron - Westbury",      "delivery_point", "welton bibby baron westbury"),
    ("welton: with ltd",                   "Welton Bibby & Baron Ltd - Westbury",  "delivery_point", "welton bibby baron westbury"),
    # VPK / Encase
    ("vpk: vpk banbury",                   "VPK - Banbury",                        "delivery_point", "vpk encase banbury"),
    ("vpk: encase banbury",                "Encase - Banbury",                     "delivery_point", "vpk encase banbury"),
    ("vpk: full name",                     "Banbury (VPK - Encase) - Banbury",     "delivery_point", "vpk encase banbury"),
    # Cepac
    ("cepac: with ltd",                    "Cepac Ltd - Rotherham",                "delivery_point", "cepac rotherham"),
    ("cepac: plain",                       "Cepac Ltd",                            "delivery_point", "cepac rotherham"),
    # Angleboard
    ("angleboard: itw",                    "ITW Angleboard - Dudley",              "delivery_point", "angleboard dudley"),
    ("angleboard: uk ltd",                 "Angleboard UK Ltd - Dudley",           "delivery_point", "angleboard dudley"),
]

# ---------------------------------------------------------------------------
# Collection point aliases
# ---------------------------------------------------------------------------
collection_cases = [
    # Masons Landfill
    ("masons: ipswich brackets",           "Ipswich (Masons Landfill) -",          "collection_point", "masons landfill ipswich"),
    ("masons: mason landfill",             "Mason Landfill - Ipswich",             "collection_point", "masons landfill ipswich"),
    ("masons: masons landfill",            "Masons Landfill - Ipswich",            "collection_point", "masons landfill ipswich"),
    # Enva / Envea
    ("enva: double space",                 "Envea  - Nottingham",                  "collection_point", "enva nottingham"),
    ("enva: england ltd",                  "Enva England Ltd - Nottingham",        "collection_point", "enva nottingham"),
    ("enva: plain envea",                  "Envea - Nottingham",                   "collection_point", "enva nottingham"),
    # Suez
    ("suez: trailing postcode",            "Suez - Huddersfield  HD1",             "collection_point", "suez huddersfield"),
    ("suez: with dot",                     "Suez. - Huddersfield",                 "collection_point", "suez huddersfield"),
    ("suez: plain",                        "Suez - Huddersfield",                  "collection_point", "suez huddersfield"),
    # Shotton Mill / RCP
    ("shotton: rcp procurement",           "RCP Procurement - Deeside",            "collection_point", "shotton mill deeside"),
    ("shotton: shotton mill site",         "Shotton Mill Site",                    "collection_point", "shotton mill deeside"),
    # Kemsley as collection
    ("kemsley coll: ds smith kemsley",     "DS Smith - Kemsley",                   "collection_point", "kemsley"),
    ("kemsley coll: kemsley depot",        "Kemsley Depot",                        "collection_point", "kemsley"),
]

# ---------------------------------------------------------------------------
# Date normalisation
# ---------------------------------------------------------------------------
date_cases = [
    ("expands 2-digit year",               "19/03/26",   "", "19/03/2026"),
    ("leaves 4-digit year alone",          "19/03/2026", "", "19/03/2026"),
    ("pads single-digit hour",             "8:00",       "", "08:00"),
    ("leaves padded hour alone",           "08:00",      "", "08:00"),
]


# ---------------------------------------------------------------------------
# Order number compound matching (fields_match)
# ---------------------------------------------------------------------------

def check_match(description: str, a: str, v: str, expected: bool):
    got = fields_match(a, v, "order_number")
    ok = got == expected
    status = PASS if ok else FAIL
    print(f"  [{status}] {description}")
    if not ok:
        print(f"          extracted: {repr(a)}")
        print(f"          proteo:    {repr(v)}")
        print(f"          expected match={expected}, got match={got}")
    return ok


def run_match_suite(suite_name: str, cases: list[tuple]) -> tuple[int, int]:
    print(f"\n{suite_name}")
    print("-" * len(suite_name))
    passed = 0
    for desc, a, v, expected in cases:
        if check_match(desc, a, v, expected):
            passed += 1
    return passed, len(cases)


order_match_cases = [
    # Customer PO is second part of Proteo compound number
    ("PO in second part",          "1480107",      "1842622/1480107",      True),
    ("PO in second part (2)",      "1479977",      "140484/1479977",       True),
    ("PO in first part",           "1842622",      "1842622/1480107",      True),
    # PO- prefix stripped before matching
    ("PO- prefix stripped",        "PO-0804282",   "1840282/1478631",      False),  # genuinely different
    ("PO- prefix same number",     "PO-0804538",   "PO-0804538**DEMURRAGE", True),  # DEMURRAGE suffix stripped
    # Leading zero stripping
    ("leading zeros stripped",     "804282",       "1840282/1478631",      False),  # still different
    # Empty extracted = no match
    ("empty extracted",            "",             "PO-0810504/SKM-S17461", False),
    # Exact match
    ("exact match",                "PO-0804269",   "PO-080269",            False),  # genuinely different truncation
    ("exact same",                 "1838735",      "1838735",              True),
    # Compound on both sides
    ("both compound",              "1480107",      "1842622/1480107",      True),
]


def main():
    total_pass = 0
    total_all = 0

    for suite, cases in [
        ("PRICE NORMALISATION", price_cases),
        ("ORDER NUMBER NORMALISATION", order_cases),
        ("DELIVERY POINT ALIASES", delivery_cases),
        ("COLLECTION POINT ALIASES", collection_cases),
        ("DATE/TIME NORMALISATION", date_cases),
    ]:
        p, a = run_suite(suite, cases)
        total_pass += p
        total_all += a

    for suite, cases in [
        ("ORDER NUMBER COMPOUND MATCHING", order_match_cases),
    ]:
        p, a = run_match_suite(suite, cases)
        total_pass += p
        total_all += a

    print(f"\n{'='*50}")
    print(f"RESULT: {total_pass}/{total_all} tests passed", end="  ")
    if total_pass == total_all:
        print("\033[32m ALL PASS \033[0m")
    else:
        print(f"\033[31m {total_all - total_pass} FAILED \033[0m")
    print("=" * 50)

    sys.exit(0 if total_pass == total_all else 1)


if __name__ == "__main__":
    main()
