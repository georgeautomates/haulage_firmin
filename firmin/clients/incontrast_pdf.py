"""
InContrast (STI Line Ltd T/A InContrast) — PDF parser.

Email: subject "Collection for: {Day} {DD/MM/YYYY}" from Josh.Thorpe@sti-group.com
PDF: Productio Smart Transport Sheet — one PDF per day, multiple jobs per page.

Each job block contains:
  JOB No.       = order_number (not unique — same job can appear twice with different SDNs)
  SDN           = job_number / delivery_order_number (unique per booking)
  DESPATCH DATE = collection_date
  Booked Date   = delivery_date
  Booked Time   = delivery_time
  TOTAL PALLETS = pallets
  TOTAL SPACES  = spaces
  Delivery Address = delivery company + postcode

Collection point is always: INCONTRAST - GILLINGHAM (ME8 0SA, fixed)
"""
from __future__ import annotations
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from firmin.utils.logger import get_logger

logger = get_logger(__name__)

# Header date: "ASSEMBLY DEPT COLLECTIONS FOR: Thursday 26 March 2026"
_HEADER_DATE_RE = re.compile(
    r'ASSEMBLY\s+DEPT\s+COLLECTIONS\s+FOR[:\s]+\w+\s+(\d{1,2}\s+\w+\s+\d{4})',
    re.IGNORECASE,
)

# Split into job blocks on "JOB No." header (may or may not be followed by "Details")
_JOB_SPLIT_RE = re.compile(r'JOB\s+No\.?', re.IGNORECASE)

# JOB No value: first numeric line after the block starts
_JOB_NO_RE = re.compile(r'^(\d{5,})\s*$', re.MULTILINE)

# SDN
_SDN_RE = re.compile(r'\bSDN\b\s+(\d+)', re.IGNORECASE)

# Dates
_DESPATCH_RE = re.compile(r'DESPATCH\s+DATE\s+(\d{2}/\d{2}/\d{4})', re.IGNORECASE)
_BOOKED_DATE_RE = re.compile(r'Booked\s+Date\s+(\d{2}/\d{2}/\d{4})', re.IGNORECASE)
_BOOKED_TIME_RE = re.compile(r'Booked\s+Time\s+(\d{2}:\d{2})', re.IGNORECASE)

# Pallets and spaces — value may be on same line as label or preceded by "0 0"
_PALLETS_RE = re.compile(r'TOTAL\s+PALLETS[\s\d]*?(\d+)\s*$', re.IGNORECASE | re.MULTILINE)
_SPACES_RE = re.compile(r'TOTAL\s+SPACES\s+(\d+)', re.IGNORECASE)

# Delivery Address block
_DELIVERY_ADDR_RE = re.compile(r'Delivery\s+Address\s*\n(.+?)(?:\n0 0|\nSlot|\Z)', re.IGNORECASE | re.DOTALL)

# UK postcode
_POSTCODE_RE = re.compile(r'\b([A-Z]{1,2}\d{1,2}[A-Z]?\s?\d[A-Z]{2})\b')

# Skip InContrast's own postcode
_SKIP_POSTCODES = {"ME80SA"}


@dataclass
class InContrastBooking:
    job_number: str       # SDN — unique dedup key
    order_number: str     # JOB No.
    collection_date: str  # DD/MM/YYYY
    collection_time: str  # HH:MM
    delivery_date: str    # DD/MM/YYYY
    delivery_time: str    # HH:MM
    delivery_postcode: str
    delivery_company: str
    pallets: int
    spaces: int


def _parse_date(raw: str) -> str:
    """Convert '26 March 2026' to '26/03/2026', pass through DD/MM/YYYY."""
    raw = raw.strip()
    if re.match(r'\d{2}/\d{2}/\d{4}', raw):
        return raw
    try:
        return datetime.strptime(raw, "%d %B %Y").strftime("%d/%m/%Y")
    except ValueError:
        return raw


def _extract_postcode(text: str) -> str:
    for pc in _POSTCODE_RE.findall(text.upper()):
        if pc.replace(" ", "") not in _SKIP_POSTCODES:
            return pc.strip()
    return ""


def _extract_delivery_company(addr_text: str) -> str:
    lines = [l.strip() for l in addr_text.strip().splitlines() if l.strip() and l.strip() != "0 0"]
    return lines[0] if lines else ""


def _parse_job_block(block: str, header_date: str) -> Optional[InContrastBooking]:
    # JOB No.
    job_no_m = _JOB_NO_RE.search(block)
    order_number = job_no_m.group(1) if job_no_m else ""

    # SDN
    sdn_m = _SDN_RE.search(block)
    if not sdn_m:
        logger.warning("InContrast: no SDN found in job block (JOB No. %s)", order_number)
        return None
    job_number = sdn_m.group(1)

    # Collection date
    despatch_m = _DESPATCH_RE.search(block)
    collection_date = despatch_m.group(1) if despatch_m else header_date

    # Delivery date + time
    booked_date_m = _BOOKED_DATE_RE.search(block)
    delivery_date = booked_date_m.group(1) if booked_date_m else ""

    booked_time_m = _BOOKED_TIME_RE.search(block)
    delivery_time = booked_time_m.group(1) if booked_time_m else "09:00"

    # Pallets + spaces
    pallets = 0
    pallets_m = _PALLETS_RE.search(block)
    if pallets_m:
        try:
            pallets = int(pallets_m.group(1))
        except ValueError:
            pass

    spaces = pallets  # default to pallets if not found separately
    spaces_m = _SPACES_RE.search(block)
    if spaces_m:
        try:
            spaces = int(spaces_m.group(1))
        except ValueError:
            pass

    # Delivery address
    delivery_company = ""
    delivery_postcode = ""
    addr_m = _DELIVERY_ADDR_RE.search(block)
    if addr_m:
        addr_text = addr_m.group(1)
        delivery_company = _extract_delivery_company(addr_text)
        delivery_postcode = _extract_postcode(addr_text)

    if not delivery_postcode:
        delivery_postcode = _extract_postcode(block)

    return InContrastBooking(
        job_number=job_number,
        order_number=order_number,
        collection_date=collection_date,
        collection_time="09:00",
        delivery_date=delivery_date,
        delivery_time=delivery_time,
        delivery_postcode=delivery_postcode,
        delivery_company=delivery_company,
        pallets=pallets,
        spaces=spaces,
    )


def parse_incontrast_pdf(raw_text: str) -> list[InContrastBooking]:
    """
    Parse an InContrast Transport Sheet PDF.
    Returns one InContrastBooking per SDN (job slot).
    """
    text = re.sub(r'\r\n|\r', '\n', raw_text)

    # Header collection date
    header_date = ""
    header_m = _HEADER_DATE_RE.search(text)
    if header_m:
        header_date = _parse_date(header_m.group(1))

    # Split into job blocks
    parts = _JOB_SPLIT_RE.split(text)
    if len(parts) <= 1:
        logger.warning("InContrast: no job blocks found in PDF — raw text sample: %r", text[:300])
        return []

    results = []
    for part in parts[1:]:  # skip preamble before first job
        booking = _parse_job_block(part, header_date)
        if booking:
            results.append(booking)
            logger.info(
                "InContrast booking parsed: SDN=%s JOB=%s postcode=%s date=%s pallets=%d",
                booking.job_number, booking.order_number,
                booking.delivery_postcode, booking.delivery_date, booking.pallets,
            )

    if not results:
        logger.warning("InContrast: no bookings parsed from PDF")

    return results
