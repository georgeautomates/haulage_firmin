"""
Community Playthings — PDF parser.

Handles two booking types from danxcarousel.com emails:

1. INDIVIDUAL DELIVERY NOTE (Carousel Logistics)
   - Consignment number: *9204860* (job_number / load_number)
   - Your Reference No: D203G-1 (docket / order_number)
   - Collect From: Community Playthings / Carousel, Sittingbourne, ME10 3RN (fixed)
   - Deliver To: varies — company + postcode
   - Despatch Date: collection date
   - Delivery date/time: parsed from Special Instructions ("DELIVER 14th April at 8am")
   - Weight / packages / volume also extracted

2. TURN SCHEDULE (Round Robin — weekly interplant run)
   - PDF title: "Turn Schedule"
   - 5 daily runs Mon-Fri, always same route
   - Collection: Community Playthings - Sittingbourne
   - Delivery: Round Robin - Sittingbourne
   - Load number: "[Day] Round Robin"
   - Pallets: 26
"""
from __future__ import annotations
import re
from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Optional

from firmin.utils.logger import get_logger

logger = get_logger(__name__)

# Consignment number: *9204860*
_CONSIGNMENT_RE = re.compile(r'\*(\d{7,})\*')

# Your Reference No: D203G-1
_REF_RE = re.compile(r'Your Reference No[:\s]+([A-Z]\d{3,}[A-Z]-\d+)', re.IGNORECASE)

# Despatch Date: Monday, 13/04/2026
_DESPATCH_RE = re.compile(r'Despatch Date[:\s]+\w+,?\s*(\d{2}/\d{2}/\d{4})', re.IGNORECASE)

# Weight: Weight (kgs): 1320.15
_WEIGHT_RE = re.compile(r'Weight\s*\(kgs\)[:\s]+([\d.]+)', re.IGNORECASE)

# No Packages
_PACKAGES_RE = re.compile(r'No Packages[:\s]+(\d+)', re.IGNORECASE)

# UK postcode — for delivery address extraction
_POSTCODE_RE = re.compile(r'\b([A-Z]{1,2}\d{1,2}[A-Z]?\s?\d[A-Z]{2})\b')

# Turn Schedule date lines: "20-Apr-2026 Monday"
_TURN_DATE_RE = re.compile(r'(\d{2}-[A-Za-z]+-\d{4})\s+(Monday|Tuesday|Wednesday|Thursday|Friday)', re.IGNORECASE)

# Delivery date from Special Instructions
# e.g. "DELIVER 14th April at 8am"
# e.g. "DELIVER 23rd March between 09:00-13:00"
# e.g. "DELIVER bank holiday 3rd April"
_DELIVER_RE = re.compile(
    r'DELIVER\b[^,\n]*?'
    r'(\d{1,2})(?:st|nd|rd|th)?\s+'
    r'(January|February|March|April|May|June|July|August|September|October|November|December)'
    r'(?:[^,\n]*?(?:at\s+|from\s+)?(\d{1,2}(?::\d{2})?\s*(?:am|pm)))?',
    re.IGNORECASE,
)

_MONTH_MAP = {
    'january': 1, 'february': 2, 'march': 3, 'april': 4,
    'may': 5, 'june': 6, 'july': 7, 'august': 8,
    'september': 9, 'october': 10, 'november': 11, 'december': 12,
}


def _parse_delivery_datetime(special_instructions: str, reference_year: int) -> tuple[str, str]:
    """
    Extract delivery date and time from Special Instructions text.
    Returns (DD/MM/YYYY, HH:MM) or ("", "09:00") if not found.
    """
    m = _DELIVER_RE.search(special_instructions)
    if not m:
        return "", "09:00"

    day = int(m.group(1))
    month = _MONTH_MAP.get(m.group(2).lower(), 0)
    time_raw = m.group(3) or ""

    if not month:
        return "", "09:00"

    delivery_date = f"{day:02d}/{month:02d}/{reference_year}"

    # Parse time
    delivery_time = "09:00"
    if time_raw:
        time_clean = time_raw.strip().lower()
        is_pm = 'pm' in time_clean
        time_clean = time_clean.replace('am', '').replace('pm', '').strip()
        if ':' in time_clean:
            h, mi = time_clean.split(':', 1)
        else:
            h, mi = time_clean, '0'
        try:
            h = int(h)
            mi = int(mi)
            if is_pm and h != 12:
                h += 12
            elif not is_pm and h == 12:
                h = 0
            delivery_time = f"{h:02d}:{mi:02d}"
        except ValueError:
            pass

    return delivery_date, delivery_time


def _extract_delivery_postcode(text: str) -> str:
    """
    Find the delivery postcode from the Deliver To block.
    Skip ME10 3RN (collection) and ME19 4UA (Carousel registered office).
    """
    skip = {"ME103RN", "ME194UA", "ME103RF"}
    postcodes = _POSTCODE_RE.findall(text.upper())
    for pc in postcodes:
        if pc.replace(" ", "") not in skip:
            return pc.strip()
    return ""


def _extract_delivery_company(text: str) -> str:
    """Extract first company line after 'Deliver To:' label."""
    m = re.search(r'Deliver\s+To[:\s]*\n\s*(.+)', text, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return ""


@dataclass
class CommunityPlaythingsDelivery:
    """A single Carousel delivery note booking."""
    job_number: str          # consignment number (e.g. 9204860)
    order_number: str        # Your Reference No (e.g. D203G-1)
    collection_date: str     # DD/MM/YYYY
    collection_time: str     # HH:MM
    delivery_date: str       # DD/MM/YYYY
    delivery_time: str       # HH:MM
    delivery_postcode: str
    delivery_company: str
    weight: str              # kg as string
    packages: int
    booking_type: str = "delivery"


@dataclass
class CommunityPlaythingsRoundRobin:
    """A single daily entry from a Turn Schedule."""
    job_number: str      # e.g. "2026-04-21-round-robin"
    date: str            # DD/MM/YYYY
    day_name: str        # Monday, Tuesday etc.
    booking_type: str = "round_robin"


def parse_community_playthings_pdf(raw_text: str) -> list:
    """
    Parse a Community Playthings PDF.
    Returns a list of CommunityPlaythingsDelivery or CommunityPlaythingsRoundRobin objects.
    """
    text = re.sub(r'\r\n|\r', '\n', raw_text)

    # Detect booking type
    if "turn schedule" in text.lower():
        return _parse_turn_schedule(text)
    else:
        result = _parse_delivery_note(text)
        return [result] if result else []


def _parse_delivery_note(text: str) -> Optional[CommunityPlaythingsDelivery]:
    """Parse a Carousel delivery note PDF."""
    # Consignment number
    consignment_match = _CONSIGNMENT_RE.search(text)
    if not consignment_match:
        logger.warning("Community Playthings: no consignment number found")
        return None
    job_number = consignment_match.group(1)

    # Your Reference No
    ref_match = _REF_RE.search(text)
    order_number = ref_match.group(1) if ref_match else job_number

    # Despatch Date (collection date)
    despatch_match = _DESPATCH_RE.search(text)
    collection_date = despatch_match.group(1) if despatch_match else ""
    reference_year = int(collection_date.split('/')[-1]) if collection_date else datetime.now().year

    # Special Instructions — delivery date/time
    si_match = re.search(r'Special Instructions[:\s]*\n(.+?)(?:\n\n|\nDriver Obligations)', text, re.IGNORECASE | re.DOTALL)
    special_instructions = si_match.group(1).strip() if si_match else ""
    delivery_date, delivery_time = _parse_delivery_datetime(special_instructions, reference_year)

    # Delivery postcode and company
    delivery_postcode = _extract_delivery_postcode(text)
    delivery_company = _extract_delivery_company(text)

    # Weight
    weight_match = _WEIGHT_RE.search(text)
    weight = weight_match.group(1) if weight_match else ""

    # Packages
    pkg_match = _PACKAGES_RE.search(text)
    packages = int(pkg_match.group(1)) if pkg_match else 0

    booking = CommunityPlaythingsDelivery(
        job_number=job_number,
        order_number=order_number,
        collection_date=collection_date,
        collection_time="09:00",
        delivery_date=delivery_date,
        delivery_time=delivery_time,
        delivery_postcode=delivery_postcode,
        delivery_company=delivery_company,
        weight=weight,
        packages=packages,
    )

    logger.info(
        "Community Playthings delivery parsed: consignment=%s ref=%s deliver=%s date=%s",
        booking.job_number, booking.order_number, booking.delivery_postcode, booking.delivery_date,
    )
    return booking


def _parse_turn_schedule(text: str) -> list[CommunityPlaythingsRoundRobin]:
    """Parse a Turn Schedule PDF — returns one entry per day."""
    entries = []
    for m in _TURN_DATE_RE.finditer(text):
        date_str = m.group(1)   # "20-Apr-2026"
        day_name = m.group(2).capitalize()  # "Monday"
        try:
            dt = datetime.strptime(date_str, "%d-%b-%Y")
            formatted_date = dt.strftime("%d/%m/%Y")
            job_number = f"{dt.strftime('%Y-%m-%d')}-round-robin"
        except ValueError:
            logger.warning("Community Playthings: could not parse turn date: %s", date_str)
            continue

        entries.append(CommunityPlaythingsRoundRobin(
            job_number=job_number,
            date=formatted_date,
            day_name=day_name,
        ))
        logger.info("Community Playthings Round Robin: %s %s", day_name, formatted_date)

    return entries
