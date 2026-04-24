"""
Colombier (UK) Ltd — PDF parser.

Document: "Routing Report"
Email subject: "Load XXXXX - ... | From: tim.donnellan@colombier.com"
Emails are forwarded internally — filter on subject_contains.

Fixed collection: Colombier (UK) Ltd - Castle Road, Sittingbourne
Carrier is always Alan Firmin Ltd (stated in header).
Delivery address from consignee block (BOL# entry).
"""
from __future__ import annotations
import re
from dataclasses import dataclass
from typing import Optional

from firmin.utils.logger import get_logger

logger = get_logger(__name__)

_LOAD_RE        = re.compile(r'Load\s+#\s+(\d+)')
_SHIP_DATE_RE   = re.compile(r'Ship\s+Date:\s+(\d{2}/\d{2}/\d{2})')
_PO_RE          = re.compile(r'PO#\s+(.+)')
_FREIGHT_RE     = re.compile(r'Total\s+Freight\s+=\s+([\d.]+)')
_POSTCODE_RE    = re.compile(r'\b([A-Z]{1,2}\d{1,2}[A-Z]?\s?\d[A-Z]{2})\b')

# Consignee block: BOL# followed by company + address
_CONSIGNEE_RE   = re.compile(
    r'^\s*(\d{5})\s+(.+?)(?=\n\s*=====|\nFREIGHT|\Z)',
    re.MULTILINE | re.DOTALL,
)

# Delivery date from bottom freetext note
# e.g. "del Mon 20/4 @ 8am", "del Tues 21/4 @ 11:15am"
_DEL_DATE_RE    = re.compile(
    r'del\w*\s+(?:Mon|Tues?|Wed|Thur?|Fri|Sat|Sun)?\s*(\d{1,2}/\d{1,2})\s*@\s*(\S+)',
    re.IGNORECASE,
)


@dataclass
class ColombierBooking:
    load_number: str       # 66934 — dedup key
    ship_date: str         # DD/MM/YYYY — collection date
    delivery_date: str     # DD/MM/YYYY
    delivery_time: str     # HH:MM
    delivery_company: str
    delivery_postcode: str
    po_number: str
    price: str             # Total Freight
    gross_weight: str


def _convert_date_short(date_str: str) -> str:
    """Convert DD/MM/YY → DD/MM/YYYY."""
    if re.match(r'\d{2}/\d{2}/\d{2}$', date_str):
        d, m, y = date_str.split("/")
        return f"{d}/{m}/20{y}"
    return date_str


def _convert_date_partial(date_str: str) -> str:
    """Convert D/M or DD/M → DD/MM/YYYY (current year 2026)."""
    parts = date_str.split("/")
    if len(parts) == 2:
        d = parts[0].zfill(2)
        m = parts[1].zfill(2)
        return f"{d}/{m}/2026"
    return date_str


def _normalise_time(raw: str) -> str:
    raw = raw.strip().lower()
    if not raw:
        return ""
    # "8am" → "08:00", "11:15am" → "11:15"
    m = re.match(r'(\d{1,2}):(\d{2})', raw)
    if m:
        return f"{int(m.group(1)):02d}:{m.group(2)}"
    m = re.match(r'(\d{1,2})am', raw)
    if m:
        return f"{int(m.group(1)):02d}:00"
    m = re.match(r'(\d{1,2})pm', raw)
    if m:
        h = int(m.group(1))
        return f"{h + 12 if h < 12 else h:02d}:00"
    return ""


def parse_colombier_pdf(raw_text: str) -> Optional[ColombierBooking]:
    """
    Parse a Colombier Routing Report PDF.
    Returns None if Load # not found.
    """
    text = re.sub(r'\r\n|\r', '\n', raw_text)

    m = _LOAD_RE.search(text)
    if not m:
        logger.warning("Colombier: Load # not found in PDF")
        return None
    load_number = m.group(1)

    ship_date = ""
    m = _SHIP_DATE_RE.search(text)
    if m:
        ship_date = _convert_date_short(m.group(1))

    # Delivery date from freetext note at bottom
    delivery_date = ship_date  # default
    delivery_time = ""
    m = _DEL_DATE_RE.search(text)
    if m:
        delivery_date = _convert_date_partial(m.group(1))
        delivery_time = _normalise_time(m.group(2))

    # Consignee block — first BOL# entry
    delivery_company = ""
    delivery_postcode = ""
    po_number = ""
    gross_weight = ""

    m = _CONSIGNEE_RE.search(text)
    if m:
        block = m.group(2).strip()
        lines = [l.strip() for l in block.splitlines() if l.strip()]
        if lines:
            delivery_company = lines[0]
        # Postcode
        pcs = _POSTCODE_RE.findall(block.upper())
        if pcs:
            delivery_postcode = pcs[-1].strip()
        # PO#
        po_m = _PO_RE.search(block)
        if po_m:
            po_number = po_m.group(1).strip()

    # Gross weight from TOTAL line
    weight_m = re.search(r'TOTAL\s+\d+\s+(\d+)\s+\d+', text)
    if weight_m:
        gross_weight = weight_m.group(1)

    # Price
    price = ""
    m = _FREIGHT_RE.search(text)
    if m and m.group(1) != "0":
        price = f"£{m.group(1)}"

    logger.info(
        "Colombier parsed: load=%s deliver=%s postcode=%s date=%s price=%s",
        load_number, delivery_company[:30] if delivery_company else "", delivery_postcode, delivery_date, price,
    )
    return ColombierBooking(
        load_number=load_number,
        ship_date=ship_date,
        delivery_date=delivery_date,
        delivery_time=delivery_time,
        delivery_company=delivery_company,
        delivery_postcode=delivery_postcode,
        po_number=po_number,
        price=price,
        gross_weight=gross_weight,
    )
