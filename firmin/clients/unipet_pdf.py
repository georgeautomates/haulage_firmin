from __future__ import annotations
import re
from dataclasses import dataclass, field
from typing import Optional

from firmin.utils.logger import get_logger

logger = get_logger(__name__)

# Matches UK postcodes
_POSTCODE_RE = re.compile(r'\b([A-Z]{1,2}\d{1,2}[A-Z]?\s?\d[A-Z]{2})\b')

# Delivery note number: 4-6 digit standalone number
_DELIVERY_NOTE_RE = re.compile(r'^\d{4,6}$')

# Collection date from header: DATE: DD.MM.YY
_DATE_RE = re.compile(r'DATE:\s*(\d{2}\.\d{2}\.\d{2})')

# Prebooked date/time patterns:
#   Booked In: 13.04.26 @ 08.00
#   Booked In: 14.04.26 after 10.00
#   Booked In: 14.04.26 Between 10.00 - 15.00
_PREBOOKED_RE = re.compile(
    r'Booked In:\s*(\d{2}\.\d{2}\.\d{2})'
    r'\s*(?:@|after|Between)?\s*'
    r'(\d{2}[:.]\d{2})',
    re.IGNORECASE,
)


@dataclass
class UnipetRow:
    delivery_note: str          # Delivery Note Number (or Customer Order if Awaiting Paperwork)
    customer_order: str         # Customer Order number
    customer_name: str
    postcode: str
    pallets: int
    delivery_date: str          # DD/MM/YYYY
    delivery_time: str          # HH:MM
    collection_date: str        # from PDF header


@dataclass
class UnipetManifestResult:
    collection_date: str
    rows: list[UnipetRow] = field(default_factory=list)


def _fmt_date(ddmmyy: str) -> str:
    """Convert DD.MM.YY to DD/MM/YYYY."""
    parts = ddmmyy.split('.')
    if len(parts) == 3:
        return f"{parts[0]}/{parts[1]}/20{parts[2]}"
    return ddmmyy


def _fmt_time(t: str) -> str:
    """Normalise HH.MM or HH:MM to HH:MM."""
    return t.replace('.', ':')


def parse_unipet_manifest(raw_text: str) -> UnipetManifestResult:
    """
    Parse a Unipet Collection Manifest PDF text into structured rows.

    The PDF text (extracted by PyMuPDF) comes out as one token per line.
    Structure per delivery row:
      <delivery_note | "Awaiting" + "Paperwork">
      <customer_order>
      <customer_name>
      <postcode>
      <pallets>
      <"Yes" | blank>          # Paperwork sent
      <prebooked date/time>    # one or two lines
      <"N/A">                  # Booking required
      <"Please use ...">       # Special requirements
      <phone>
    """
    lines = [l.strip() for l in raw_text.splitlines() if l.strip()]

    # Extract collection date from header — "DATE:" and the date may be on separate lines
    collection_date = ""
    header_text = " ".join(lines[:10])
    m = _DATE_RE.search(header_text)
    if m:
        collection_date = _fmt_date(m.group(1))

    result = UnipetManifestResult(collection_date=collection_date)

    i = 0
    while i < len(lines):
        line = lines[i]

        # Detect start of a delivery row
        is_awaiting = line == "Awaiting" and i + 1 < len(lines) and lines[i + 1] == "Paperwork"
        is_note = bool(_DELIVERY_NOTE_RE.match(line))

        if not is_note and not is_awaiting:
            i += 1
            continue

        # Consume the delivery note field
        if is_awaiting:
            delivery_note_raw = "Awaiting Paperwork"
            i += 2  # skip "Awaiting" and "Paperwork"
        else:
            delivery_note_raw = line
            i += 1

        # Next token: customer order number
        if i >= len(lines):
            break
        customer_order = lines[i]
        i += 1

        # Next: customer name — keep consuming until we hit a postcode
        name_parts = []
        while i < len(lines) and not _POSTCODE_RE.match(lines[i]):
            # Stop if we've hit something that looks like a number (next row starting)
            if _DELIVERY_NOTE_RE.match(lines[i]) or lines[i] in ("Awaiting", "Total Pallets"):
                break
            name_parts.append(lines[i])
            i += 1
        customer_name = " ".join(name_parts).strip()

        # Next: postcode
        if i >= len(lines) or not _POSTCODE_RE.match(lines[i]):
            logger.warning("Unipet parser: no postcode after customer name '%s'", customer_name)
            continue
        postcode = lines[i]
        i += 1

        # Next: pallets (integer)
        if i >= len(lines):
            break
        try:
            pallets = int(lines[i])
            i += 1
        except ValueError:
            logger.warning("Unipet parser: expected pallets int, got '%s'", lines[i])
            pallets = 0
            i += 1

        # Optional "Yes" / blank for paperwork sent
        if i < len(lines) and lines[i] in ("Yes", "No"):
            i += 1

        # Prebooked date/time — scan next few lines for the pattern
        delivery_date = ""
        delivery_time = ""
        prebooked_text = ""
        for lookahead in range(min(4, len(lines) - i)):
            chunk = " ".join(lines[i: i + lookahead + 1])
            m = _PREBOOKED_RE.search(chunk)
            if m:
                delivery_date = _fmt_date(m.group(1))
                delivery_time = _fmt_time(m.group(2))
                prebooked_text = chunk
                i += lookahead + 1
                break

        # Use customer order as delivery_note if awaiting paperwork
        delivery_note = customer_order if delivery_note_raw == "Awaiting Paperwork" else delivery_note_raw

        row = UnipetRow(
            delivery_note=delivery_note,
            customer_order=customer_order,
            customer_name=customer_name,
            postcode=postcode,
            pallets=pallets,
            delivery_date=delivery_date,
            delivery_time=delivery_time,
            collection_date=collection_date,
        )
        result.rows.append(row)
        logger.debug(
            "Unipet row: note=%s order=%s customer=%s postcode=%s pallets=%d del=%s %s",
            row.delivery_note, row.customer_order, row.customer_name,
            row.postcode, row.pallets, row.delivery_date, row.delivery_time,
        )

    logger.info("Unipet manifest parsed: %d rows, collection_date=%s", len(result.rows), collection_date)
    return result
