"""
AIM (SIG Trading Limited) — Purchase Order parser.

Each PDF is a single booking with:
  - Purchase order no: XXXXXX  (AIM PO number — used as job reference)
  - Product description line: {N}NO ... PALLETS / {N}KGS? TOTAL
  - Date line: READY TO COLLECT DD.MM.YYYY
              READY FROM [HH.MM[pm]] DD.MM.YYYY
              DELIVERY AM / DEL AM DD.MM.YYYY
  - Net Price (ex VAT) — the rate
  - Deliver to: company name + address + postcode

Collection is always from AIM's Crawley depot (RH10 9NH).
"""
from __future__ import annotations
import re
from dataclasses import dataclass
from typing import Optional

from firmin.utils.logger import get_logger

logger = get_logger(__name__)

# AIM Purchase Order number
_PO_RE = re.compile(r'Purchase order no[:\s]+(\d+)', re.IGNORECASE)

# Pallets: "18NO", "40NO", "12NO" etc.
_PALLETS_RE = re.compile(r'(\d+)\s*NO\b', re.IGNORECASE)

# Weight: "8640KG TOTAL" or "9600KGS TOTAL"
_WEIGHT_RE = re.compile(r'(\d+)\s*KGS?\s+TOTAL', re.IGNORECASE)

# Net Price (first occurrence in the price column area)
_NET_PRICE_RE = re.compile(r'(\d+\.\d{2})\s+\d+\.\d{2}\s+\d+\.\d{2}')

# Collection date/time variations:
#   READY TO COLLECT 20.04.2026
#   READY FROM 14.00 23.04.2026
#   READY FROM 15.00pm 08/05/2026
#   READY FROM 3PM 24.04.2026
_COLLECT_RE = re.compile(
    r'READY\s+(?:TO\s+COLLECT|FROM)\s+'
    r'(?:(\d{1,2}[.:]\d{2}(?:pm|am)?|\d{1,2}(?:PM|AM))\s+)?'
    r'(\d{2}[./]\d{2}[./]\d{4})',
    re.IGNORECASE,
)

# Delivery date variations:
#   DELIVERY AM 21.04.2026
#   DEL AM 24.04.2026
#   DELIVERY AM 11/05/2026
_DELIVERY_RE = re.compile(
    r'(?:DELIVERY|DEL)\s+AM\s+(\d{2}[./]\d{2}[./]\d{4})',
    re.IGNORECASE,
)

# UK postcode
_POSTCODE_RE = re.compile(r'\b([A-Z]{1,2}\d{1,2}[A-Z]?\s?\d[A-Z]{2})\b')


def _normalise_date(raw: str) -> str:
    """Convert DD.MM.YYYY or DD/MM/YYYY to DD/MM/YYYY."""
    return raw.replace('.', '/')


def _parse_time(raw: Optional[str]) -> str:
    """Convert various time formats to HH:MM. Returns 09:00 if absent."""
    if not raw:
        return "09:00"
    raw = raw.strip().upper().rstrip('PM').rstrip('AM').strip()
    raw = raw.replace(':', '.').replace('.', ':')
    parts = raw.split(':')
    try:
        h = int(parts[0])
        m = int(parts[1]) if len(parts) > 1 else 0
        return f"{h:02d}:{m:02d}"
    except (ValueError, IndexError):
        return "09:00"


def _extract_delivery_postcode(text: str) -> str:
    """
    Extract the delivery postcode from the Deliver to: block.
    AIM's own Crawley postcode (RH10 9NH) is in the header — skip it.
    Alan Firmin's postcode (ME17 4BB) is in the address block — skip it.
    Return the first postcode that is neither of those.
    """
    skip = {"RH109NH", "ME174BB"}
    postcodes = _POSTCODE_RE.findall(text.upper())
    for pc in postcodes:
        norm = pc.replace(" ", "")
        if norm not in skip:
            return pc.strip()
    return ""


def _extract_delivery_company(text: str) -> str:
    """Extract company name from the 'Deliver to:' block."""
    m = re.search(r'Deliver\s+to\s*:\s*\n(.+)', text, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return ""


@dataclass
class AimBooking:
    job_number: str          # AIM PO number (dedup key)
    order_number: str        # AIM PO number
    collection_date: str     # DD/MM/YYYY
    collection_time: str     # HH:MM
    delivery_date: str       # DD/MM/YYYY
    delivery_postcode: str
    delivery_company: str    # raw company name from PDF (for location lookup hint)
    pallets: int
    weight: int              # KG
    price: str               # Net price ex VAT as string e.g. "575.00"


def parse_aim_booking(raw_text: str) -> Optional[AimBooking]:
    """Parse an AIM Purchase Order PDF into a structured booking."""
    text = re.sub(r'\r\n|\r', '\n', raw_text)

    # --- PO number ---
    po_match = _PO_RE.search(text)
    if not po_match:
        logger.warning("AIM parser: no Purchase order no found")
        return None
    job_number = po_match.group(1).strip()

    # --- Pallets ---
    pallets = 0
    pallets_match = _PALLETS_RE.search(text)
    if pallets_match:
        pallets = int(pallets_match.group(1))

    # --- Weight ---
    weight = 0
    weight_match = _WEIGHT_RE.search(text)
    if weight_match:
        weight = int(weight_match.group(1))

    # --- Net price ---
    price = ""
    price_match = _NET_PRICE_RE.search(text)
    if price_match:
        price = price_match.group(1)

    # --- Collection date/time ---
    collection_date = ""
    collection_time = "09:00"
    collect_match = _COLLECT_RE.search(text)
    if collect_match:
        time_raw = collect_match.group(1)
        date_raw = collect_match.group(2)
        collection_date = _normalise_date(date_raw)
        collection_time = _parse_time(time_raw)

    # --- Delivery date ---
    delivery_date = ""
    delivery_match = _DELIVERY_RE.search(text)
    if delivery_match:
        delivery_date = _normalise_date(delivery_match.group(1))

    # --- Delivery postcode ---
    delivery_postcode = _extract_delivery_postcode(text)

    # --- Delivery company name (hint for Supabase lookup) ---
    delivery_company = _extract_delivery_company(text)

    if not delivery_postcode:
        logger.warning("AIM parser: no delivery postcode found for PO %s", job_number)

    booking = AimBooking(
        job_number=job_number,
        order_number=job_number,
        collection_date=collection_date,
        collection_time=collection_time,
        delivery_date=delivery_date,
        delivery_postcode=delivery_postcode,
        delivery_company=delivery_company,
        pallets=pallets,
        weight=weight,
        price=price,
    )

    logger.info(
        "AIM booking parsed: PO=%s deliver=%s pallets=%d weight=%d price=%s collect=%s deliver=%s",
        booking.job_number, booking.delivery_postcode, booking.pallets,
        booking.weight, booking.price, booking.collection_date, booking.delivery_date,
    )
    return booking
