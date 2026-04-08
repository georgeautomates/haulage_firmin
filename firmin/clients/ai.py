from __future__ import annotations
import json
import os
import re
from dataclasses import dataclass
from typing import Optional

from openai import OpenAI

from firmin.utils.logger import get_logger

logger = get_logger(__name__)

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
  or a SKM code like "SKM-S17211", or a time window like "0700-1300". Empty string if not present.
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
    "work_type": "MIS"
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


class AiClient:
    def __init__(self):
        if os.getenv("OPENROUTER_API_KEY"):
            api_key = os.getenv("OPENROUTER_API_KEY")
            base_url = "https://openrouter.ai/api/v1"
            default_model = "openai/gpt-4o"
        else:
            api_key = os.getenv("OPENAI_API_KEY")
            base_url = None  # use OpenAI directly
            default_model = "gpt-4o"

        self.model = os.getenv("AI_EXTRACTION_MODEL", default_model)
        self.client = OpenAI(api_key=api_key, base_url=base_url)

    def extract_job(self, raw_text: str, job_number: str) -> Optional[AiExtractionResult]:
        prompt = EXTRACTION_PROMPT.format(raw_text=raw_text, job_number=job_number)

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0,
            )
            content = response.choices[0].message.content or ""
            return _parse_response(content, job_number)
        except Exception as e:
            logger.error("AI extraction failed for job %s: %s", job_number, e)
            return None


def _parse_response(content: str, job_number: str) -> Optional[AiExtractionResult]:
    try:
        clean = re.sub(r"```json|```", "", content).strip()
        data = json.loads(clean)
    except Exception as e:
        logger.error("Failed to parse AI output for job %s: %s\nOutput: %s", job_number, e, content)
        return None

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
        customer_ref=data.get("customer_ref", ""),
        work_type=data.get("work_type", ""),
    )
