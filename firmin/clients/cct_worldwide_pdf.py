"""
CCT Worldwide Limited — PDF parser.

Document: "COLLECTION / DELIVERY NOTE"
Email subject: "Booking : SIGRPJ... | From: warehouse@cctworldwideltd.com"
Emails are forwarded internally — filter on subject_contains.

Fixed collection: CCT Worldwide Ltd - Gravesend, DA12 2PL
Delivery varies.
Haulier check: skip if not ALAN FIRMIN LTD.
One email may contain multiple PDFs; some for other hauliers (skip those).
"""
from __future__ import annotations
import re
from dataclasses import dataclass
from typing import Optional

from firmin.utils.logger import get_logger

logger = get_logger(__name__)

_OUR_REF_RE       = re.compile(r'(SIGRPJ\d+)')
_HAULIER_RE       = re.compile(r'Haulier\s*\n\s*(.+)', re.IGNORECASE)
_DOC_DATE_RE      = re.compile(r'Date\s+(\d{2}/\d{2}/\d{2})\s+\d{2}:\d{2}', re.IGNORECASE)
_DELIVERY_DATE_RE = re.compile(r'Delivery\s+Date:\s+(?:\w+\s+)?(\d{2}/\d{2}/\d{4})', re.IGNORECASE)
_DELIVERY_TIME_RE = re.compile(r'Delivery\s+Time:\s*(\S+)', re.IGNORECASE)
_CUSTOMER_REF_RE  = re.compile(r'Customer\s+Reference\s*\n\s*(.+)', re.IGNORECASE)
_COLLECTION_REF_RE = re.compile(r'Collection\s+Ref:\s*(\S+)', re.IGNORECASE)
_PKGS_RE          = re.compile(r'^(\d+)\s+(?:BOXES?|ROLLS?|CARTONS?|PALLETS?|PACKAGES?|BAGS?)\b', re.IGNORECASE | re.MULTILINE)
_POSTCODE_RE      = re.compile(r'\b([A-Z]{1,2}\d{1,2}[A-Z]?\s?\d[A-Z]{2})\b')
_DELIVERY_BLOCK_RE = re.compile(
    r'Delivery\s+Details\s*\n(.+?)(?=Delivery\s+Date:|Collection\s+Details)',
    re.IGNORECASE | re.DOTALL,
)

_SKIP_POSTCODES = {"DA122PL"}  # CCT Gravesend — collection point, skip when finding delivery


@dataclass
class CctWorldwideBooking:
    our_reference: str      # SIGRPJ144716 — dedup key
    haulier: str
    collection_date: str    # DD/MM/YYYY (document date)
    delivery_date: str      # DD/MM/YYYY
    delivery_time: str      # HH:MM or AM/PM
    delivery_company: str
    delivery_postcode: str
    customer_ref: str
    collection_ref: str
    packages: int


def _convert_date(date_str: str) -> str:
    """Convert DD/MM/YY to DD/MM/YYYY."""
    if re.match(r'\d{2}/\d{2}/\d{2}$', date_str):
        d, m, y = date_str.split("/")
        return f"{d}/{m}/20{y}"
    return date_str


def _extract_delivery_postcode(text: str) -> str:
    for pc in _POSTCODE_RE.findall(text.upper()):
        if pc.replace(" ", "") not in _SKIP_POSTCODES:
            return pc.strip()
    return ""


def _normalise_time(raw: str) -> str:
    raw = raw.strip().upper()
    if not raw:
        return ""
    if raw == "AM":
        return "09:00"
    if raw == "PM":
        return "12:00"
    return raw


def parse_cct_worldwide_pdf(raw_text: str, filename: str = "") -> Optional[CctWorldwideBooking]:
    """
    Parse a CCT Worldwide Collection/Delivery Note PDF.
    Returns None if Our Reference not found or haulier is not Alan Firmin.
    """
    text = re.sub(r'\r\n|\r', '\n', raw_text)

    # Our Reference — from text or filename
    our_reference = ""
    m = _OUR_REF_RE.search(text)
    if m:
        our_reference = m.group(1)
    if not our_reference and filename:
        m = _OUR_REF_RE.search(filename)
        if m:
            our_reference = m.group(1)

    if not our_reference:
        logger.warning("CCT Worldwide: Our Reference not found in '%s'", filename)
        return None

    # Haulier check
    haulier = ""
    m = _HAULIER_RE.search(text)
    if m:
        haulier = m.group(1).strip()
    if haulier and "alan firmin" not in haulier.lower():
        logger.info("CCT Worldwide: skipping %s — haulier is '%s'", our_reference, haulier)
        return None

    # Collection date (document date — Collection Date field is blank)
    collection_date = ""
    m = _DOC_DATE_RE.search(text)
    if m:
        collection_date = _convert_date(m.group(1))

    # Delivery date + time
    delivery_date = ""
    m = _DELIVERY_DATE_RE.search(text)
    if m:
        delivery_date = m.group(1)

    delivery_time = ""
    m = _DELIVERY_TIME_RE.search(text)
    if m:
        delivery_time = _normalise_time(m.group(1))

    # Delivery company + postcode
    delivery_company = ""
    delivery_postcode = ""
    m = _DELIVERY_BLOCK_RE.search(text)
    if m:
        block = m.group(1).strip()
        lines = [l.strip() for l in block.splitlines() if l.strip()]
        if lines:
            delivery_company = lines[0]
        delivery_postcode = _extract_delivery_postcode(block)
    if not delivery_postcode:
        delivery_postcode = _extract_delivery_postcode(text)

    # Customer reference
    customer_ref = ""
    m = _CUSTOMER_REF_RE.search(text)
    if m:
        val = m.group(1).strip()
        if val:
            customer_ref = val

    # Collection ref
    collection_ref = ""
    m = _COLLECTION_REF_RE.search(text)
    if m:
        collection_ref = m.group(1).strip()

    # Packages
    packages = 0
    m = _PKGS_RE.search(text)
    if m:
        try:
            packages = int(m.group(1))
        except ValueError:
            pass

    logger.info(
        "CCT Worldwide parsed: ref=%s deliver=%s postcode=%s date=%s",
        our_reference, delivery_company[:30] if delivery_company else "", delivery_postcode, delivery_date,
    )
    return CctWorldwideBooking(
        our_reference=our_reference,
        haulier=haulier,
        collection_date=collection_date,
        delivery_date=delivery_date,
        delivery_time=delivery_time,
        delivery_company=delivery_company,
        delivery_postcode=delivery_postcode,
        customer_ref=customer_ref,
        collection_ref=collection_ref,
        packages=packages,
    )
