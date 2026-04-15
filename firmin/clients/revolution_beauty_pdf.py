"""
Revolution Beauty Ltd — Haulier Booking Sheet parser.

Each PDF is a single booking with:
  - Collection Address (left column) + Delivery Address (right column)
  - Collection Date & Time / Delivery Date & Time
  - Number of Pallets: "Full Load" or an integer
  - Revolution Order(s): SO-RBL-XXXXXXX or TO-RBL-XXXXX
  - GXO ID(s): S4XXXXXXX or T4XXXXXXX
  - Customer Order Reference (optional)

PyMuPDF flattens the 2-column layout so we extract fields by regex,
not by column position.
"""
from __future__ import annotations
import re
from dataclasses import dataclass, field
from typing import Optional

from firmin.utils.logger import get_logger

logger = get_logger(__name__)

# UK postcode
_POSTCODE_RE = re.compile(r'\b([A-Z]{1,2}\d{1,2}[A-Z]?\s?\d[A-Z]{2})\b')

# Revolution Order: SO-RBL-XXXXXXX or TO-RBL-XXXXX
_REVOLUTION_ORDER_RE = re.compile(r'\b([ST]O-RBL-[\w-]+)\b')

# GXO ID: S4XXXXXXX or T4XXXXXXX (8-9 digit IDs starting with S or T)
_GXO_ID_RE = re.compile(r'\b([ST]\d{7,9})\b')

# Date + optional time: 13/04/26 @ 11:00  or  13/04/26
_DATE_TIME_RE = re.compile(r'(\d{2}/\d{2}/\d{2})(?:\s*@\s*(\d{1,2}:\d{2}))?')

# Number of pallets line
_PALLETS_RE = re.compile(r'Number of Pallets[:\s]+(Full Load|\d+)', re.IGNORECASE)

# Proteo point names for DE11 0BB — direction-dependent
_SWADLINCOTE_COLLECTION = "GXO (Clipper Logistic) - Swadlincote"
_SWADLINCOTE_DELIVERY   = "Clipper Logistics - Swadlincote"
_SWADLINCOTE_POSTCODE   = "DE11 0BB"


def _fmt_date(ddmmyy: str) -> str:
    """Convert DD/MM/YY to DD/MM/YYYY."""
    parts = ddmmyy.split('/')
    if len(parts) == 3 and len(parts[2]) == 2:
        return f"{parts[0]}/{parts[1]}/20{parts[2]}"
    return ddmmyy


def _fmt_time(t: Optional[str]) -> str:
    """Ensure HH:MM format, default to 00:00 if missing."""
    if not t:
        return "00:00"
    parts = t.split(':')
    return f"{int(parts[0]):02d}:{parts[1]}"


@dataclass
class RevolutionBeautyBooking:
    job_number: str           # first Revolution Order (dedup key)
    order_number: str         # first Revolution Order
    gxo_id: str              # first GXO ID
    customer_ref: str         # Customer Order Reference (if present)
    collection_postcode: str
    delivery_postcode: str
    collection_date: str      # DD/MM/YYYY
    collection_time: str      # HH:MM
    delivery_date: str        # DD/MM/YYYY
    delivery_time: str        # HH:MM
    pallets_raw: str          # "Full Load" or "1", "2" etc.
    business_type: str        # derived from pallets
    all_revolution_orders: list[str] = field(default_factory=list)
    all_gxo_ids: list[str] = field(default_factory=list)


def _derive_business_type(pallets_raw: str) -> str:
    """
    Full Load → General | Part/Full Load
    Small pallet count (1-5) → Firmin Xpress | Vans
    """
    if pallets_raw.strip().lower() == "full load":
        return "General | Part/Full Load"
    try:
        count = int(pallets_raw.strip())
        if count <= 5:
            return "Firmin Xpress | Vans"
        return "General | Part/Full Load"
    except ValueError:
        return "General | Part/Full Load"


def collection_point_for(postcode: str) -> Optional[str]:
    """Return hardcoded Proteo name if this is a known collection postcode."""
    if postcode.replace(" ", "").upper() == _SWADLINCOTE_POSTCODE.replace(" ", ""):
        return _SWADLINCOTE_COLLECTION
    return None


def delivery_point_for(postcode: str) -> Optional[str]:
    """Return hardcoded Proteo name if this is a known delivery postcode."""
    if postcode.replace(" ", "").upper() == _SWADLINCOTE_POSTCODE.replace(" ", ""):
        return _SWADLINCOTE_DELIVERY
    return None


def parse_revolution_beauty_booking(raw_text: str) -> Optional[RevolutionBeautyBooking]:
    """
    Parse a Revolution Beauty Haulier Booking Sheet PDF into a structured booking.

    Returns None if the minimum required fields cannot be extracted.
    """
    # Normalise whitespace
    text = re.sub(r'\r\n|\r', '\n', raw_text)
    text_upper = text.upper()

    # --- Postcodes ---
    # First postcode = collection, second = delivery
    postcodes = _POSTCODE_RE.findall(text_upper)
    # Normalise: remove internal spaces
    postcodes = [p.replace(" ", " ").strip() for p in postcodes]
    # Deduplicate while preserving order
    seen = set()
    unique_postcodes = []
    for p in postcodes:
        normalised = p.replace(" ", "")
        if normalised not in seen:
            seen.add(normalised)
            unique_postcodes.append(p)

    if len(unique_postcodes) < 2:
        logger.warning("Revolution Beauty parser: fewer than 2 postcodes found — got %s", unique_postcodes)
        # Fall back: try to use what we have
        collection_postcode = unique_postcodes[0] if unique_postcodes else ""
        delivery_postcode = unique_postcodes[1] if len(unique_postcodes) > 1 else ""
    else:
        collection_postcode = unique_postcodes[0]
        delivery_postcode = unique_postcodes[1]

    # --- Revolution Order numbers ---
    revolution_orders = _REVOLUTION_ORDER_RE.findall(text)
    gxo_ids = _GXO_ID_RE.findall(text)

    # Use first Revolution Order as job reference
    # Fall back to first GXO ID if no standard order format
    if revolution_orders:
        job_number = revolution_orders[0]
        order_number = revolution_orders[0]
    elif gxo_ids:
        job_number = gxo_ids[0]
        order_number = gxo_ids[0]
    else:
        logger.warning("Revolution Beauty parser: no order reference found in PDF")
        job_number = ""
        order_number = ""

    # --- GXO ID ---
    gxo_id = gxo_ids[0] if gxo_ids else ""

    # --- Customer Order Reference ---
    # Appears after "Customer Order Reference:" label
    customer_ref = ""
    cref_match = re.search(r'Customer Order Reference[:\s]+([^\n]+)', text, re.IGNORECASE)
    if cref_match:
        val = cref_match.group(1).strip()
        # Exclude if it's a column header or blank
        if val and val.lower() not in ("pallet dimensions", ""):
            customer_ref = val

    # --- Dates and times ---
    # Find all date+time pairs in the text
    date_times = _DATE_TIME_RE.findall(text)
    # date_times is list of (date, time) tuples — time may be empty string

    collection_date = ""
    collection_time = "00:00"
    delivery_date = ""
    delivery_time = "00:00"

    if len(date_times) >= 1:
        collection_date = _fmt_date(date_times[0][0])
        collection_time = _fmt_time(date_times[0][1] if date_times[0][1] else None)
    if len(date_times) >= 2:
        delivery_date = _fmt_date(date_times[1][0])
        delivery_time = _fmt_time(date_times[1][1] if date_times[1][1] else None)

    # --- Number of pallets ---
    pallets_raw = "Full Load"
    pallets_match = _PALLETS_RE.search(text)
    if pallets_match:
        pallets_raw = pallets_match.group(1).strip()

    business_type = _derive_business_type(pallets_raw)

    booking = RevolutionBeautyBooking(
        job_number=job_number,
        order_number=order_number,
        gxo_id=gxo_id,
        customer_ref=customer_ref,
        collection_postcode=collection_postcode,
        delivery_postcode=delivery_postcode,
        collection_date=collection_date,
        collection_time=collection_time,
        delivery_date=delivery_date,
        delivery_time=delivery_time,
        pallets_raw=pallets_raw,
        business_type=business_type,
        all_revolution_orders=revolution_orders,
        all_gxo_ids=gxo_ids,
    )

    logger.info(
        "Revolution Beauty booking parsed: job=%s collect=%s deliver=%s pallets=%s date=%s",
        booking.job_number, booking.collection_postcode, booking.delivery_postcode,
        booking.pallets_raw, booking.collection_date,
    )
    return booking
