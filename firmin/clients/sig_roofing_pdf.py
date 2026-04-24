"""
Roofing Centre Group Ltd (SIG Roofing) — PDF parser.

Document: "HAULIER ORDER" from SIG Trading Limited.
Email subject: "Purchase Order - ME9 7NU - 3101 - XXXXXXXXXX | From: ..."
Emails are forwarded internally — filter on subject_contains.

Fields:
  Order Number  = dedup key (e.g. 3101400951)
  Order Date    = collection date (when order placed)
  Expected date = delivery date (from product table)
  Total Value   = price
  Collection    = free-text "Please collect from" block
  Delivery      = free-text "deliver back to" / "deliver to" block
  Pallets       = from free text ("X pallet")
  REF           = customer reference
"""
from __future__ import annotations
import re
from dataclasses import dataclass
from typing import Optional

from firmin.utils.logger import get_logger

logger = get_logger(__name__)

_DOC_NO_RE        = re.compile(r'Document\s+Number\s+(\d+/\d+)', re.IGNORECASE)
_ORDER_NO_RE      = re.compile(r'Order\s+Number\s+(\d{7,12})\b', re.IGNORECASE)
_ORDER_DATE_RE    = re.compile(r'Order\s+Date:\s+(\d{2}/\d{2}/\d{4})', re.IGNORECASE)
_DELIVERY_DATE_RE = re.compile(r'(\d{2}/\d{2}/\d{4})\s+[\d.]+\s+EA\b', re.IGNORECASE)
_TOTAL_RE         = re.compile(r'Total\s+Value:\s*([\d.]+)', re.IGNORECASE)
_REF_RE           = re.compile(r'\bREF\s+(\S+)', re.IGNORECASE)
_PALLETS_RE       = re.compile(r'(\d+)\s+pallet', re.IGNORECASE)
_POSTCODE_RE      = re.compile(r'\b([A-Z]{1,2}\d{1,2}[A-Z]?\s?\d[A-Z]{2})\b')

_COLLECT_FROM_RE = re.compile(
    r'(?:Please\s+collect\s+from|PLEASE\s+COLLECT\s+FROM)\s*\n(.*?)'
    r'(?=\n\s*REF\b|\n\s*Please\s+deliv|\n\s*PLEASE\s+DELIV|\n\s*Deliver\s+to|\n\s*DELIVER)',
    re.IGNORECASE | re.DOTALL,
)
_DELIVER_TO_RE = re.compile(
    r'(?:Please\s+deliver(?:\s+back)?\s+to|PLEASE\s+DELIVER(?:\s+BACK)?\s+TO)\s*\n(.*?)'
    r'(?=\n\s*Please\s+can|\n\s*Can\s+you|\n\s*PLEASE\s+CAN|\n\s*CAN\s+YOU'
    r'|\n\s*Many\s+Thanks|\n\s*MANY\s+THANKS|\Z)',
    re.IGNORECASE | re.DOTALL,
)


@dataclass
class SigRoofingBooking:
    order_number: str       # 3101400951 — dedup key
    order_date: str         # DD/MM/YYYY — collection date
    delivery_date: str      # DD/MM/YYYY
    price: str              # e.g. "78.10"
    customer_ref: str       # e.g. "3121/306492"
    collection_address: str # raw address block
    collection_postcode: str
    delivery_address: str
    delivery_postcode: str
    pallets: int


def _extract_postcode(text: str) -> str:
    matches = _POSTCODE_RE.findall(text.upper())
    return matches[-1].strip() if matches else ""


def _first_line(text: str) -> str:
    for line in text.strip().splitlines():
        line = line.strip()
        if line:
            return line
    return ""


def parse_sig_roofing_pdf(raw_text: str) -> Optional[SigRoofingBooking]:
    """
    Parse a SIG Roofing Haulier Order PDF.
    Returns None if Order Number cannot be found.
    """
    text = re.sub(r'\r\n|\r', '\n', raw_text)

    # Prefer Document Number (e.g. 3101/00400951) — matches Proteo Load Number
    m = _DOC_NO_RE.search(text)
    if m:
        order_number = m.group(1)
    else:
        m = _ORDER_NO_RE.search(text)
        if not m:
            logger.warning("SIG Roofing: Order Number not found in PDF")
            return None
        order_number = m.group(1)

    order_date = ""
    m = _ORDER_DATE_RE.search(text)
    if m:
        order_date = m.group(1)

    delivery_date = ""
    m = _DELIVERY_DATE_RE.search(text)
    if m:
        delivery_date = m.group(1)

    price = ""
    m = _TOTAL_RE.search(text)
    if m:
        price = m.group(1)

    customer_ref = ""
    m = _REF_RE.search(text)
    if m:
        customer_ref = m.group(1).strip()

    pallets = 1
    m = _PALLETS_RE.search(text)
    if m:
        try:
            pallets = int(m.group(1))
        except ValueError:
            pass

    collection_address = ""
    collection_postcode = ""
    m = _COLLECT_FROM_RE.search(text)
    if m:
        collection_address = m.group(1).strip()
        collection_postcode = _extract_postcode(collection_address)

    delivery_address = ""
    delivery_postcode = ""
    m = _DELIVER_TO_RE.search(text)
    if m:
        delivery_address = m.group(1).strip()
        delivery_postcode = _extract_postcode(delivery_address)

    logger.info(
        "SIG Roofing order parsed: %s collect=%s deliver=%s date=%s price=%s",
        order_number, collection_postcode, delivery_postcode, delivery_date, price,
    )
    return SigRoofingBooking(
        order_number=order_number,
        order_date=order_date,
        delivery_date=delivery_date,
        price=f"£{price}" if price else "",
        customer_ref=customer_ref,
        collection_address=collection_address,
        collection_postcode=collection_postcode,
        delivery_address=delivery_address,
        delivery_postcode=delivery_postcode,
        pallets=pallets,
    )
