from __future__ import annotations
import json
import os
import re
from dataclasses import dataclass
from typing import Optional

from openai import OpenAI

from firmin.utils.logger import get_logger

logger = get_logger(__name__)

# Job number pattern: 7-digit numbers starting with 25 or 26
_JOB_RE = re.compile(r'\b(2[56]\d{5})\b')


def _slice_job_text(raw_text: str, job_number: str) -> str:
    """
    Return only the portion of raw_text that belongs to job_number.

    PyMuPDF flattens multi-column PDFs so rows from adjacent jobs bleed
    together. By slicing between consecutive job number occurrences we give
    the AI a clean window containing only the target job's data.

    Falls back to the full text if the job number isn't found.
    """
    # Find all job number positions in order of appearance
    matches = [(m.group(1), m.start()) for m in _JOB_RE.finditer(raw_text)]

    # Find the position of our target job number
    target_pos = None
    target_idx = None
    for i, (jn, pos) in enumerate(matches):
        if jn == job_number:
            target_pos = pos
            target_idx = i
            break

    if target_pos is None:
        logger.warning("Job %s not found in raw text — using full text", job_number)
        return raw_text

    # Slice starts at the previous different job number so we don't bleed in
    # data (especially PO numbers) from the job immediately before ours.
    start = 0
    for i in range(target_idx - 1, -1, -1):
        prev_jn, prev_pos = matches[i]
        if prev_jn != job_number:
            start = prev_pos
            break

    # Slice ends at the start of the next different job number
    end = len(raw_text)
    for i in range(target_idx + 1, len(matches)):
        next_jn, next_pos = matches[i]
        if next_jn != job_number:
            end = next_pos
            break

    sliced = raw_text[start:end]
    logger.debug("Sliced text for job %s: %d chars (from %d to %d)", job_number, len(sliced), start, end)
    return sliced


EXTRACTION_PROMPT = """\
You are extracting order details from a UK haulage booking form PDF.

The PDF text has a multi-column table layout that may cause collection and delivery \
address text to run together. Each row has these columns in order:
Job Number | Collection Date&Time | Collection Address | Delivery Date&Time | Delivery Address | Price/Order/Ref | Work Type

The collection address first line often has format: LOCATION NAME (ORG NAME) e.g. "DARTFORD (DATA SOLUTIONS)" or "AVONMOUTH (SUEZ)".
The delivery address is separate from the collection address — do not mix them.
Work type is a short code at the end of the row: X, MIS, KWH, PLA etc.

Here is the full booking form text:
{raw_text}

Extract the details for job number: {job_number}

Rules:
- collection_org: the organisation name in brackets on the collection address first line, or the location name if no brackets. \
  e.g. "DARTFORD (DATA SOLUTIONS)" → "DATA SOLUTIONS"; "KEMSLEY" → "KEMSLEY"; "KEMSLEY MILL (KM)" → "KEMSLEY MILL". \
  IMPORTANT: Kemsley/DS Smith can appear as EITHER the collection OR the delivery point depending on the job — \
  always check the column position, never assume Kemsley is always the delivery.
- delivery_org: the organisation/location name on the delivery address first line. \
  e.g. "KEMSLEY" → "KEMSLEY"; "BRISTOL (SINIAT)" → "SINIAT". \
  IMPORTANT: some jobs collect FROM Kemsley and deliver TO another site — read the column headings carefully.
- collection_address: street address lines only for the COLLECTION address, NOT including the org name or postcode. Do not mix in delivery address lines.
- delivery_address: street address lines only for the DELIVERY address, NOT including the org name or postcode. Do not mix in collection address lines.
- collection_postcode: the postcode belonging to the collection address (appears at the end of the collection address block). \
  Must be a valid UK postcode format e.g. "ME10 2XF", "TN9 1RA". Never copy the delivery postcode here.
- delivery_postcode: the postcode belonging to the delivery address (appears at the end of the delivery address block). \
  Must be a valid UK postcode format. Never copy the collection postcode here.
- price: the value starting with £ in the Price/Order/Ref column (e.g. £300.00, £490.00, £1,200.00). \
  Always include the £ symbol and preserve the exact amount. Never use a plain number as the price.
- order_number: extract as follows depending on format: \
  (a) If a PO-prefixed reference exists (e.g. PO-0808360), always use that — it is the DS Smith PO number. \
  (b) If there is no PO- prefix and the field contains two numbers separated by "/" (e.g. "1841694 / 1479265"), \
      use the FIRST number only. \
  Never use a work type code, time value, or SKM reference as the order_number. \
  Do NOT include customer_ref or work_type here.
- customer_ref: the reference after the order_number — e.g. the second number in "PO-0808360 / 1773780", \
  or a SKM code like "SKM-S17211". Do NOT include time windows (like "0700-1300") here — those go in booking_window. Empty string if not present.
- booking_window: a delivery time window in the format HHMM-HHMM (e.g. "0700-1300", "0600-1800"). \
  Often appears on the same line or below the order_number/customer_ref. Empty string if not present.
- traffic_note: any free-text note or instruction for the driver/traffic team — e.g. "HIAB REQUIRED", \
  "TAIL LIFT", "CALL BEFORE DELIVERY", "NO EARLY DELIVERIES". Empty string if not present.
- work_type: the short code that appears on the same line as the £ price (X, MIS, KWH, PLA, KFL, HYP etc). Empty string if not present.
- collection_date and delivery_date: format as DD/MM/YYYY (e.g. 14/04/2026). Never swap collection and delivery dates.
- collection_time and delivery_time: format as HH:MM using 24-hour time (e.g. 08:00, 13:30).
- All fields must be filled with actual values from the text, never with placeholder descriptions.
- Double-check: collection_postcode and delivery_postcode must be DIFFERENT postcodes belonging to their respective addresses.

--- EXAMPLES ---

Example 1 — standard row with bracketed org name:
  PDF text (abbreviated): "12345  14/04/2026 08:00  DARTFORD (DATA SOLUTIONS)  Manor Road  DA1 1AB  15/04/2026 10:00  KEMSLEY  Milton Creek Road  ME10 2XF  PO-0804230  £300.00  SKM-S17211  X"
  Output:
  {{
    "job_number": "12345",
    "collection_org": "DATA SOLUTIONS",
    "collection_address": "Manor Road",
    "collection_postcode": "DA1 1AB",
    "collection_date": "14/04/2026",
    "collection_time": "08:00",
    "delivery_org": "KEMSLEY",
    "delivery_address": "Milton Creek Road",
    "delivery_postcode": "ME10 2XF",
    "delivery_date": "15/04/2026",
    "delivery_time": "10:00",
    "price": "£300.00",
    "order_number": "PO-0804230",
    "customer_ref": "SKM-S17211",
    "booking_window": "",
    "traffic_note": "",
    "work_type": "X"
  }}

Example 2 — plain location name, numeric PO, no customer ref:
  PDF text (abbreviated): "67890  20/04/2026 07:00  AVONMOUTH (SUEZ)  Kings Weston Lane  BS11 0YA  20/04/2026 14:00  DEVIZES (DS SMITH)  Hopton Road  SN10 2EY  1838735  £490.00  MIS"
  Output:
  {{
    "job_number": "67890",
    "collection_org": "SUEZ",
    "collection_address": "Kings Weston Lane",
    "collection_postcode": "BS11 0YA",
    "collection_date": "20/04/2026",
    "collection_time": "07:00",
    "delivery_org": "DS SMITH",
    "delivery_address": "Hopton Road",
    "delivery_postcode": "SN10 2EY",
    "delivery_date": "20/04/2026",
    "delivery_time": "14:00",
    "price": "£490.00",
    "order_number": "1838735",
    "customer_ref": "",
    "booking_window": "",
    "traffic_note": "",
    "work_type": "MIS"
  }}

Example 3 — booking window in customer ref area:
  PDF text (abbreviated): "99001  17/04/2026 06:00  BRISTOL (SEVERNSIDE)  Harbour Road  BS11 0NX  17/04/2026 14:00  KEMSLEY  Milton Creek Road  ME10 2XF  PO-0809100  1774500  0700-1300  £350.00  X"
  Output:
  {{
    "job_number": "99001",
    "collection_org": "SEVERNSIDE",
    "collection_address": "Harbour Road",
    "collection_postcode": "BS11 0NX",
    "collection_date": "17/04/2026",
    "collection_time": "06:00",
    "delivery_org": "KEMSLEY",
    "delivery_address": "Milton Creek Road",
    "delivery_postcode": "ME10 2XF",
    "delivery_date": "17/04/2026",
    "delivery_time": "14:00",
    "price": "£350.00",
    "order_number": "PO-0809100",
    "customer_ref": "1774500",
    "booking_window": "0700-1300",
    "traffic_note": "",
    "work_type": "X"
  }}

--- END EXAMPLES ---

Now extract the details for job number {job_number} from the booking form text above.

Return ONLY this JSON with no markdown, no backticks, no explanation:
{{
  "job_number": "{job_number}",
  "collection_org": "",
  "collection_address": "",
  "collection_postcode": "",
  "collection_date": "",
  "collection_time": "",
  "delivery_org": "",
  "delivery_address": "",
  "delivery_postcode": "",
  "delivery_date": "",
  "delivery_time": "",
  "price": "",
  "order_number": "",
  "customer_ref": "",
  "booking_window": "",
  "traffic_note": "",
  "work_type": ""
}}
"""


@dataclass
class AiExtractionResult:
    job_number: str
    collection_org: str
    collection_address: str
    collection_postcode: str
    collection_date: str
    collection_time: str
    delivery_org: str
    delivery_address: str
    delivery_postcode: str
    delivery_date: str
    delivery_time: str
    price: str
    order_number: str
    customer_ref: str
    booking_window: str
    traffic_note: str
    work_type: str

    @property
    def collection_search(self) -> str:
        return (
            self.collection_org + " "
            + self.collection_address.replace(",", " ") + " "
            + self.collection_postcode
        )

    @property
    def delivery_search(self) -> str:
        return (
            self.delivery_org + " "
            + self.delivery_address.replace(",", " ") + " "
            + self.delivery_postcode
        )


DUAL_MODEL_FIELDS = [
    "collection_org", "collection_postcode", "collection_date", "collection_time",
    "delivery_org", "delivery_postcode", "delivery_date", "delivery_time",
    "price", "order_number", "booking_window", "work_type",
]


@dataclass
class DualExtractionResult:
    primary: AiExtractionResult          # gpt-4o
    secondary: AiExtractionResult        # gpt-4o-mini
    agreement: dict[str, bool]           # field -> True if both models agree
    agreement_score: int                 # 0-100 percentage of fields that agree


_EUROCOILS_VISION_PROMPT = """\
You are extracting delivery information from scanned Eurocoils PDF documents.

The PDF has multiple pages:
- Page 1: Official Order — contains the PO number printed large at the top right (e.g. 54984), \
  and a handwritten list of delivery destinations.
- Pages 2+: Delivery Notes — each has a "DELIVERY NOTE" header, \
  and a table row with W/Order No, delivery address, date, and quantity.

For each Delivery Note page extract:
- po_number: the 5-digit PO number from the Official Order page (top-right corner of page 1)
- job_number: the W/Order No value from the QTY.ORD column (e.g. "46455") — this is italic/handwritten
- delivery_company: the company name from the DELIVERY TO section (top-right block)
- delivery_postcode: the UK postcode from the DELIVERY TO section (e.g. "GL2 4NZ")
- collection_date: the DATE field on the delivery note in DD/MM/YYYY format
- pallets: the integer quantity from QTY.DEL column (default 1 if unclear)

Return ONLY a JSON array, one object per Delivery Note page, no markdown, no explanation:
[
  {
    "po_number": "54984",
    "job_number": "46455",
    "delivery_company": "Neptune Building Services",
    "delivery_postcode": "GL2 4NZ",
    "collection_date": "21/04/2026",
    "pallets": 1
  }
]
"""


class AiClient:
    PRIMARY_MODEL = "gpt-4o"
    SECONDARY_MODEL = "gpt-4o-mini"

    def __init__(self):
        if os.getenv("OPENROUTER_API_KEY"):
            api_key = os.getenv("OPENROUTER_API_KEY")
            base_url = "https://openrouter.ai/api/v1"
            self.primary_model = "openai/gpt-4o"
            self.secondary_model = "openai/gpt-4o-mini"
        else:
            api_key = os.getenv("OPENAI_API_KEY")
            base_url = None
            self.primary_model = os.getenv("AI_EXTRACTION_MODEL", self.PRIMARY_MODEL)
            self.secondary_model = os.getenv("AI_EXTRACTION_MODEL_2", self.SECONDARY_MODEL)

        self.client = OpenAI(api_key=api_key, base_url=base_url)

    def extract_job(self, raw_text: str, job_number: str) -> Optional[AiExtractionResult]:
        """Single-model extraction using the primary model."""
        return self._run_extraction(raw_text, job_number, self.primary_model)

    def extract_job_dual(self, raw_text: str, job_number: str) -> Optional[DualExtractionResult]:
        """Run both models and return both results with per-field agreement."""
        primary = self._run_extraction(raw_text, job_number, self.primary_model)
        if not primary:
            return None
        secondary = self._run_extraction(raw_text, job_number, self.secondary_model)
        if not secondary:
            return None

        agreement = {}
        for f in DUAL_MODEL_FIELDS:
            v1 = getattr(primary, f, "").strip().lower()
            v2 = getattr(secondary, f, "").strip().lower()
            agreement[f] = v1 == v2

        agreed = sum(1 for v in agreement.values() if v)
        agreement_score = round(agreed / len(DUAL_MODEL_FIELDS) * 100)

        return DualExtractionResult(
            primary=primary,
            secondary=secondary,
            agreement=agreement,
            agreement_score=agreement_score,
        )

    def _run_extraction(self, raw_text: str, job_number: str, model: str) -> Optional[AiExtractionResult]:
        job_text = _slice_job_text(raw_text, job_number)
        prompt = EXTRACTION_PROMPT.format(raw_text=job_text, job_number=job_number)

        try:
            response = self.client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0,
            )
            content = response.choices[0].message.content or ""
            return _parse_response(content, job_number)
        except Exception as e:
            logger.error("AI extraction failed for job %s (model %s): %s", job_number, model, e)
            return None

    def extract_eurocoils_scanned(self, pdf_bytes: bytes) -> list[dict]:
        """
        Extract Eurocoils delivery data from a scanned PDF using GPT-4o vision.
        Returns a list of dicts with keys: po_number, job_number, delivery_company,
        delivery_postcode, collection_date, pallets.
        """
        import base64
        import fitz

        try:
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            images_b64 = []
            for i in range(len(doc)):
                pix = doc[i].get_pixmap(matrix=fitz.Matrix(2, 2))
                images_b64.append(base64.b64encode(pix.tobytes("png")).decode())
            doc.close()
        except Exception as e:
            logger.error("Eurocoils vision: failed to render PDF pages: %s", e)
            return []

        content = [{"type": "text", "text": _EUROCOILS_VISION_PROMPT}]
        for img_b64 in images_b64:
            content.append({
                "type": "image_url",
                "image_url": {"url": f"data:image/png;base64,{img_b64}", "detail": "high"},
            })

        try:
            response = self.client.chat.completions.create(
                model=self.primary_model,
                messages=[{"role": "user", "content": content}],
                temperature=0,
            )
            raw = response.choices[0].message.content or ""
            clean = re.sub(r"```json|```", "", raw).strip()
            data = json.loads(clean)
            if isinstance(data, dict):
                data = [data]
            logger.info("Eurocoils vision extracted %d delivery note(s)", len(data))
            return data
        except Exception as e:
            logger.error("Eurocoils vision extraction failed: %s", e)
            return []


def _parse_response(content: str, job_number: str) -> Optional[AiExtractionResult]:
    try:
        clean = re.sub(r"```json|```", "", content).strip()
        data = json.loads(clean)
    except Exception as e:
        logger.error("Failed to parse AI output for job %s: %s\nOutput: %s", job_number, e, content)
        return None

    # Post-process: if booking_window is empty, try to extract HHMM-HHMM from customer_ref
    booking_window = data.get("booking_window", "")
    customer_ref = data.get("customer_ref", "")
    if not booking_window and customer_ref:
        m = re.search(r'\b(\d{4}-\d{4})\b', customer_ref)
        if m:
            booking_window = m.group(1)
            customer_ref = customer_ref.replace(m.group(1), "").strip(" /").strip()

    return AiExtractionResult(
        job_number=data.get("job_number", job_number),
        collection_org=data.get("collection_org", ""),
        collection_address=data.get("collection_address", ""),
        collection_postcode=data.get("collection_postcode", ""),
        collection_date=data.get("collection_date", ""),
        collection_time=data.get("collection_time", ""),
        delivery_org=data.get("delivery_org", ""),
        delivery_address=data.get("delivery_address", ""),
        delivery_postcode=data.get("delivery_postcode", ""),
        delivery_date=data.get("delivery_date", ""),
        delivery_time=data.get("delivery_time", ""),
        price=data.get("price", ""),
        order_number=data.get("order_number", ""),
        customer_ref=customer_ref,
        booking_window=booking_window,
        traffic_note=data.get("traffic_note", ""),
        work_type=data.get("work_type", ""),
    )
