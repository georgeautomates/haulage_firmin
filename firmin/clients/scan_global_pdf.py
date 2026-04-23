"""
Scan Global Logistics (UK) Ltd — Transport Instructions parser.
Client: Horizon International Cargo

PDF: "Transport Instructions" — one job per PDF.
  Job Reference  = SD/SI + 6 digits (order reference)
  Serial Number  = 6-digit number (unique per PDF — dedup key)
  Haulier        = ALAN FIRMIN LTD (skip if other haulier)
  Collection and delivery addresses vary — handled by AI extraction.
  NOP            = number of pallets (sometimes placeholder 1 for full loads)
"""
from __future__ import annotations
import re
from dataclasses import dataclass
from typing import Optional

from firmin.utils.logger import get_logger

logger = get_logger(__name__)

_FILENAME_RE = re.compile(r'^([A-Z]{2}\d{6})-(\d{5,7})_', re.IGNORECASE)
_JOB_REF_RE  = re.compile(r'\b((?:SD|SI)\d{6})\b')
_SERIAL_RE   = re.compile(r'Serial\s+No[:\s]+(\d{5,7})', re.IGNORECASE)
_HAULIER_RE  = re.compile(r'Haulier[:\s]+(.+)', re.IGNORECASE)
_NOP_RE      = re.compile(r'NOP\s+Weight\s+Cube\s*\n\s*(\d+)', re.IGNORECASE)
_WEIGHT_RE   = re.compile(r'NOP\s+Weight\s+Cube\s*\n\s*\d+\s+(\d+)', re.IGNORECASE)


@dataclass
class ScanGlobalBooking:
    job_reference: str  # SD718565 — order number / Proteo search key
    serial_number: str  # 598072 — unique per PDF, used as dedup key
    haulier: str
    pallets: int
    weight: str


def parse_scan_global_header(raw_text: str, filename: str = "") -> Optional[ScanGlobalBooking]:
    """
    Extract the structured header fields from a Scan Global Transport Instructions PDF.
    Tries the filename first (most reliable), falls back to PDF text.
    Returns None if Job Reference or Serial Number cannot be found.
    """
    text = re.sub(r'\r\n|\r', '\n', raw_text)

    # Primary: extract from filename — SD718565-598663_CollDel Report....pdf
    job_reference = ""
    serial_number = ""
    if filename:
        m = _FILENAME_RE.match(filename)
        if m:
            job_reference = m.group(1).upper()
            serial_number = m.group(2)

    # Fallback: scan PDF text
    if not job_reference:
        m = _JOB_REF_RE.search(text)
        job_reference = m.group(1) if m else ""

    if not serial_number:
        m = _SERIAL_RE.search(text)
        serial_number = m.group(1) if m else ""

    if not job_reference or not serial_number:
        logger.warning("Scan Global: could not extract job reference or serial number from '%s'", filename)
        return None

    haulier = ""
    m = _HAULIER_RE.search(text)
    if m:
        haulier = m.group(1).strip()

    pallets = 1
    m = _NOP_RE.search(text)
    if m:
        try:
            pallets = int(m.group(1))
        except ValueError:
            pass

    weight = ""
    m = _WEIGHT_RE.search(text)
    if m:
        weight = m.group(1).strip()

    logger.info(
        "Scan Global header parsed: ref=%s serial=%s haulier=%s pallets=%s",
        job_reference, serial_number, haulier, pallets,
    )
    return ScanGlobalBooking(
        job_reference=job_reference,
        serial_number=serial_number,
        haulier=haulier,
        pallets=pallets,
        weight=weight,
    )
