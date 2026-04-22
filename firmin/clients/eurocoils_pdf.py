"""
Eurocoils Limited — PDF parser.

Email subject: "PO - XXXXX" from jack@eurocoils.co.uk
PDF structure (multi-page):
  - Page 1: Official Order (handwritten body, printed PO number) — PO number extracted only
  - Pages 2+: Delivery Notes (fully printed) — one booking per page

Proteo mapping:
  Load Number   = Eurocoils PO number (from Official Order page / email subject)
  Docket Number = W/Order No from Delivery Note
  Business Type = Firmin Xpress | Vans
  Goods Type    = Palletised
  Collect From  = Eurocoils - Sittingbourne (ME10 3RX, fixed)
  Deliver To    = company + postcode from Delivery Note
"""
from __future__ import annotations
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

from firmin.utils.logger import get_logger

logger = get_logger(__name__)

# PO number: 5-digit number immediately after "OFFICIAL ORDER" header
_PO_HEADER_RE = re.compile(r'OFFICIAL\s+ORDER\s*\n+\s*(\d{5})', re.IGNORECASE)
_PO_DIGIT_RE = re.compile(r'\b(5\d{4})\b')  # fallback: 5-digit starting with 5

# W/Order No in item description header row of Delivery Note
# Also handles "Works O/No 46417/1" variant (top-left client ref box)
_WORDER_RE = re.compile(r'(?:W/Order\s+No|Works\s+O/No)\s+(\d+)', re.IGNORECASE)

# YOUR ORDER + DATE — may be on same line or split across lines
_YOUR_ORDER_DATE_RE = re.compile(
    r'YOUR\s+ORDER\s+(.+?)\s+DATE\s+(\d{2}/\d{2}/\d{4})',
    re.IGNORECASE,
)
_DATE_RE = re.compile(r'\bDATE\s+(\d{2}/\d{2}/\d{4})', re.IGNORECASE)
_YOUR_ORDER_RE = re.compile(r'YOUR\s+ORDER\s*\n\s*(.+)', re.IGNORECASE)

# Delivery To: first company line
_DELIVERY_TO_RE = re.compile(r'DELIVERY\s+TO\s*\n\s*(.+)', re.IGNORECASE)

# UK postcode
_POSTCODE_RE = re.compile(r'\b([A-Z]{1,2}\d{1,2}[A-Z]?\s?\d[A-Z]{2})\b')

# QTY.ORD / QTY.DEL column values
_QTY_RE = re.compile(r'QTY\.\s*\n?\s*(?:ORD|DEL)\.?\s*\n?\s*(\d+)', re.IGNORECASE)

# Eurocoils Sittingbourne postcodes to skip when finding delivery postcode
_SKIP_POSTCODES = {"ME103RX", "ME103RY"}


@dataclass
class EurocoilsDelivery:
    job_number: str        # W/Order No (dedup key / Proteo Docket Number)
    po_number: str         # Eurocoils PO number (Proteo Load Number)
    customer_ref: str      # YOUR ORDER reference
    collection_date: str   # DD/MM/YYYY
    delivery_date: str     # DD/MM/YYYY (collection + 1 calendar day)
    delivery_postcode: str
    delivery_company: str  # raw company name (hint for location lookup)
    pallets: int


def _next_day(date_str: str) -> str:
    """Return DD/MM/YYYY + 1 calendar day."""
    try:
        return (datetime.strptime(date_str, "%d/%m/%Y") + timedelta(days=1)).strftime("%d/%m/%Y")
    except ValueError:
        return ""


def _extract_delivery_postcode(text: str) -> str:
    for pc in _POSTCODE_RE.findall(text.upper()):
        if pc.replace(" ", "") not in _SKIP_POSTCODES:
            return pc.strip()
    return ""


def _split_pages(text: str) -> list[str]:
    """Split PDF text on form-feed characters (PyMuPDF page separator)."""
    pages = text.split('\f')
    return [p.strip() for p in pages if p.strip()]


def _extract_po_from_page(page: str) -> str:
    """Extract the printed PO number from an Official Order page."""
    m = _PO_HEADER_RE.search(page)
    if m:
        return m.group(1)
    # Fallback: any 5-digit number starting with 5 on this page
    m = _PO_DIGIT_RE.search(page)
    if m:
        return m.group(1)
    return ""


def _parse_delivery_note(page: str, po_number: str) -> Optional[EurocoilsDelivery]:
    """Parse a single Delivery Note page. Returns None if W/Order No not found."""
    worder_m = _WORDER_RE.search(page)
    if not worder_m:
        return None
    job_number = worder_m.group(1)

    # YOUR ORDER + DATE
    customer_ref = ""
    collection_date = ""
    combined = _YOUR_ORDER_DATE_RE.search(page)
    if combined:
        customer_ref = combined.group(1).strip()
        collection_date = combined.group(2)
    else:
        date_m = _DATE_RE.search(page)
        if date_m:
            collection_date = date_m.group(1)
        your_order_m = _YOUR_ORDER_RE.search(page)
        if your_order_m:
            customer_ref = your_order_m.group(1).strip()

    # Delivery company (first line of DELIVERY TO block)
    delivery_company = ""
    dt_m = _DELIVERY_TO_RE.search(page)
    if dt_m:
        delivery_company = dt_m.group(1).strip()

    # Delivery postcode
    delivery_postcode = _extract_delivery_postcode(page)

    # Pallets — QTY column, default 1
    pallets = 1
    qty_m = _QTY_RE.search(page)
    if qty_m:
        try:
            pallets = int(qty_m.group(1))
        except ValueError:
            pass

    delivery_date = _next_day(collection_date)

    return EurocoilsDelivery(
        job_number=job_number,
        po_number=po_number,
        customer_ref=customer_ref,
        collection_date=collection_date,
        delivery_date=delivery_date,
        delivery_postcode=delivery_postcode,
        delivery_company=delivery_company,
        pallets=pallets,
    )


def parse_eurocoils_pdf(raw_text: str, email_subject: str = "") -> list[EurocoilsDelivery]:
    """
    Parse a Eurocoils PDF. Returns one EurocoilsDelivery per Delivery Note page.
    """
    text = re.sub(r'\r\n|\r', '\n', raw_text)
    pages = _split_pages(text)

    # Extract PO number from Official Order page
    po_number = ""
    for page in pages:
        if "OFFICIAL ORDER" in page.upper():
            po_number = _extract_po_from_page(page)
            if po_number:
                break

    # Fallback: parse from email subject "PO - 54976"
    if not po_number and email_subject:
        subj_m = re.search(r'PO\s*[-–]\s*(\d{5})', email_subject, re.IGNORECASE)
        if subj_m:
            po_number = subj_m.group(1)

    results = []
    for page in pages:
        if "DELIVERY NOTE" not in page.upper():
            continue
        delivery = _parse_delivery_note(page, po_number)
        if delivery:
            results.append(delivery)
            logger.info(
                "Eurocoils delivery parsed: PO=%s W/Order=%s postcode=%s date=%s pallets=%d",
                delivery.po_number, delivery.job_number,
                delivery.delivery_postcode, delivery.collection_date, delivery.pallets,
            )
        else:
            logger.warning("Eurocoils: could not parse delivery note page (no W/Order No)")

    if not results:
        logger.warning("Eurocoils PDF: no delivery notes found (subject=%s)", email_subject)

    return results


def parse_eurocoils_pdf_vision(pdf_bytes: bytes, ai_client, email_subject: str = "") -> list[EurocoilsDelivery]:
    """
    Fallback parser for scanned Eurocoils PDFs — uses GPT-4o vision.
    Returns the same EurocoilsDelivery list as parse_eurocoils_pdf.
    """
    raw_items = ai_client.extract_eurocoils_scanned(pdf_bytes)
    if not raw_items:
        logger.warning("Eurocoils vision: no deliveries extracted (subject=%s)", email_subject)
        return []

    # Fallback PO from subject if vision missed it
    subject_po = ""
    if email_subject:
        subj_m = re.search(r'PO\s*[-–]\s*(\d{5})', email_subject, re.IGNORECASE)
        if subj_m:
            subject_po = subj_m.group(1)

    results = []
    for item in raw_items:
        po_number = str(item.get("po_number", "") or subject_po).strip()
        job_number = str(item.get("job_number", "")).strip()
        if not job_number:
            continue
        collection_date = str(item.get("collection_date", "")).strip()
        delivery = EurocoilsDelivery(
            job_number=job_number,
            po_number=po_number,
            customer_ref="",
            collection_date=collection_date,
            delivery_date=_next_day(collection_date),
            delivery_postcode=str(item.get("delivery_postcode", "")).strip(),
            delivery_company=str(item.get("delivery_company", "")).strip(),
            pallets=int(item.get("pallets", 1) or 1),
        )
        results.append(delivery)
        logger.info(
            "Eurocoils vision delivery: PO=%s W/Order=%s postcode=%s date=%s pallets=%d",
            delivery.po_number, delivery.job_number,
            delivery.delivery_postcode, delivery.collection_date, delivery.pallets,
        )

    return results
