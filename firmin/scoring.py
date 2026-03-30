from __future__ import annotations
from dataclasses import dataclass, field


@dataclass
class ScoredOrder:
    signal1_customer: int
    signal2_completeness: int
    signal3_location: int
    signal4_price: int
    signal5_dates: int
    composite_score: int
    status: str  # GREEN / YELLOW / RED
    failure_reasons: list[str] = field(default_factory=list)


REQUIRED_FIELDS = [
    "job_number",
    "collection_point",
    "collection_date",
    "collection_time",
    "delivery_point",
    "delivery_date",
    "delivery_time",
    "price",
    "order_number",
]

KNOWN_CLIENTS = ["St Regis", "DS Smith"]


def score_order(order: dict) -> ScoredOrder:
    # Signal 1: known client (15%)
    client_name = order.get("client_name", "")
    signal1 = 100 if any(c in client_name for c in KNOWN_CLIENTS) else 50

    # Signal 2: field completeness (30%)
    present = sum(1 for f in REQUIRED_FIELDS if str(order.get(f, "")).strip())
    signal2 = round((present / len(REQUIRED_FIELDS)) * 100)

    # Signal 3: collection point matched (25%)
    signal3 = 0 if order.get("collection_point") == "UNMATCHED" else 100

    # Guard: collection same as delivery is likely an extraction error
    collection_same_as_delivery = (
        order.get("collection_point")
        and order.get("collection_point") == order.get("delivery_point")
    )

    # Signal 4: price validity (20%)
    price_str = str(order.get("price") or order.get("rate") or "0")
    price_clean = "".join(c for c in price_str if c.isdigit() or c == ".")
    try:
        price = float(price_clean)
    except ValueError:
        price = 0.0
    signal4 = 100 if 50 < price < 10000 else 0

    # Signal 5: date format validity (10%)
    import re
    date_re = re.compile(r'^\d{2}/\d{2}/\d{4}$')
    col_date_ok = bool(date_re.match(str(order.get("collection_date", ""))))
    del_date_ok = bool(date_re.match(str(order.get("delivery_date", ""))))
    signal5 = 100 if (col_date_ok and del_date_ok) else 50

    composite = round(
        signal1 * 0.15
        + signal2 * 0.30
        + signal3 * 0.25
        + signal4 * 0.20
        + signal5 * 0.10
    )

    if composite >= 90 and not collection_same_as_delivery:
        status = "GREEN"
    elif composite >= 70 or (composite >= 90 and collection_same_as_delivery):
        status = "YELLOW"
    else:
        status = "RED"

    failure_reasons = []
    if signal1 < 100:
        failure_reasons.append("unknown client name")
    if signal2 < 100:
        missing = [f for f in REQUIRED_FIELDS if not str(order.get(f, "")).strip()]
        failure_reasons.append(f"missing fields: {', '.join(missing)}")
    if signal3 == 0:
        failure_reasons.append("collection point unmatched")
    if signal4 == 0:
        failure_reasons.append(f"price out of range ({price_str})")
    if signal5 < 100:
        if not col_date_ok:
            failure_reasons.append(f"invalid collection date ({order.get('collection_date', '')})")
        if not del_date_ok:
            failure_reasons.append(f"invalid delivery date ({order.get('delivery_date', '')})")
    if collection_same_as_delivery:
        failure_reasons.append("collection same as delivery — possible extraction error")

    return ScoredOrder(
        signal1_customer=signal1,
        signal2_completeness=signal2,
        signal3_location=signal3,
        signal4_price=signal4,
        signal5_dates=signal5,
        composite_score=composite,
        status=status,
        failure_reasons=failure_reasons,
    )
