from __future__ import annotations
import io
import re
from dataclasses import dataclass, field
from typing import Optional

import pdfplumber
import fitz  # PyMuPDF

from firmin.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class PdfExtractResult:
    raw_text: str
    job_numbers: list[str]
    postcodes: list[str]
    prices: list[str]
    dates: list[str]
    order_numbers: list[str]
    page_count: int


def extract_pdf(data: bytes) -> PdfExtractResult:
    """Extract text from PDF bytes. Tries pdfplumber first, falls back to PyMuPDF."""
    text = _extract_with_pdfplumber(data)
    if not text or len(text.strip()) < 50:
        logger.warning("pdfplumber returned little text, trying PyMuPDF")
        text = _extract_with_pymupdf(data)

    page_count = _count_pages(data)
    job_numbers = list(dict.fromkeys(re.findall(r'\b(2[56]\d{5})\b', text)))
    postcodes = list(dict.fromkeys(re.findall(r'[A-Z]{1,2}\d{1,2}[A-Z]?\s?\d[A-Z]{2}', text)))
    prices = re.findall(r'£[\d,]+\.?\d*', text)
    dates = list(dict.fromkeys(re.findall(r'\d{2}/\d{2}/\d{4}', text)))
    order_numbers = list(dict.fromkeys(re.findall(r'PO-[\w/]+', text)))

    logger.info(
        "PDF extracted: %d pages, %d jobs, %d postcodes",
        page_count, len(job_numbers), len(postcodes),
    )

    return PdfExtractResult(
        raw_text=text,
        job_numbers=job_numbers,
        postcodes=postcodes,
        prices=prices,
        dates=dates,
        order_numbers=order_numbers,
        page_count=page_count,
    )


def _extract_with_pdfplumber(data: bytes) -> str:
    try:
        with pdfplumber.open(io.BytesIO(data)) as pdf:
            pages = []
            for page in pdf.pages:
                page_text = page.extract_text() or ""
                pages.append(page_text)
            return "\n".join(pages)
    except Exception as e:
        logger.warning("pdfplumber failed: %s", e)
        return ""


def _extract_with_pymupdf(data: bytes) -> str:
    try:
        doc = fitz.open(stream=data, filetype="pdf")
        pages = [doc[i].get_text() for i in range(len(doc))]
        doc.close()
        return "\n".join(pages)
    except Exception as e:
        logger.error("PyMuPDF failed: %s", e)
        return ""


def _count_pages(data: bytes) -> int:
    try:
        with pdfplumber.open(io.BytesIO(data)) as pdf:
            return len(pdf.pages)
    except Exception:
        return 0
