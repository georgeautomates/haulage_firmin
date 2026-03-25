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
- collection_org: the organisation name in brackets on the collection address first line, or the location name if no brackets
- delivery_org: the organisation/location name on the delivery address first line. If it is just a place name like "KEMSLEY" use that.
- collection_address: street address lines only for the COLLECTION address, NOT including the org name or postcode. Do not mix in delivery address lines.
- delivery_address: street address lines only for the DELIVERY address, NOT including the org name or postcode. Do not mix in collection address lines.
- collection_postcode: the postcode belonging to the collection address (appears at the end of the collection address block)
- delivery_postcode: the postcode belonging to the delivery address (appears at the end of the delivery address block)
- order_number: the PO number starting with "PO-" (e.g. PO-0804230). This is the purchase order number.
- customer_ref: any reference number on the line AFTER the PO number (e.g. SKM-S17211, 0700-1300, 1478638). Empty string if not present.
- work_type: the short code after the price (X, MIS, KWH, PLA etc). Empty string if not present.
- All fields must be filled with actual values from the text, never with placeholder descriptions.

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
            default_model = "openai/gpt-4o-mini"
        else:
            api_key = os.getenv("OPENAI_API_KEY")
            base_url = None  # use OpenAI directly
            default_model = "gpt-4o-mini"

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
