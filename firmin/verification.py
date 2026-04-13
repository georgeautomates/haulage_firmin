from __future__ import annotations
import json
from datetime import datetime, timezone

from firmin.clients.proteo import ProteoClient
from firmin.clients.sheets import SheetsClient
from firmin.utils.logger import get_logger

logger = get_logger(__name__)

SPREADSHEET_ID = "1uEst-r23EiTyfdmL6gx_YQMuzR0s7yyx-qJVLIUyDSI"
VERIFICATION_WS = "Verification"
RPA_ENTRY_WS    = "RPA Entry"

RPA_ENTRY_HEADERS = [
    "job_number",
    "processed_at",
    "success",
    "screenshot_url",
    "agreement_score",
    "field_matches",
    "error",
    "typed_client",
    "typed_collection_point",
    "typed_delivery_point",
    "typed_collection_date",
    "typed_collection_time",
    "typed_delivery_date",
    "typed_delivery_time",
    "typed_order_number",
    "typed_price",
    "planned_collection_point",
    "planned_delivery_point",
    "planned_collection_date",
    "planned_collection_time",
    "planned_delivery_date",
    "planned_delivery_time",
    "planned_order_number",
    "planned_price",
]


class VerificationPipeline:
    def __init__(self, proteo: ProteoClient, sheets: SheetsClient):
        self.proteo = proteo
        self.sheets = sheets
        self._seen: set[str] = set()

    def _load_seen(self) -> set[str]:
        """Load job numbers already in the Verification sheet to avoid duplicates."""
        try:
            ws = self.sheets._get_worksheet(SPREADSHEET_ID, VERIFICATION_WS)
            headers = ws.row_values(1)
            if "delivery_order_number" not in headers:
                return set()
            col_idx = headers.index("delivery_order_number")
            values = ws.col_values(col_idx + 1)[1:]  # skip header
            return {str(v).strip() for v in values if v}
        except Exception as e:
            logger.error("Could not load Verification sheet existing jobs: %s", e)
            return set()

    def process_jobs(self, job_numbers: list[str]) -> dict:
        """
        Scrape Proteo for each job number and write to Verification sheet.
        Skips jobs already present in the sheet.
        Returns summary counts.
        """
        if not self._seen:
            self._seen = self._load_seen()

        written = 0
        skipped = 0
        not_found = 0
        errors = 0

        for job_number in job_numbers:
            if job_number in self._seen:
                logger.debug("Verification: skipping already-seen job %s", job_number)
                skipped += 1
                continue

            try:
                row = self.proteo.scrape_job(job_number)
            except Exception as e:
                logger.error("Verification: scrape error for job %s: %s", job_number, e)
                errors += 1
                continue

            if not row:
                logger.info("Verification: job %s not found in Proteo", job_number)
                not_found += 1
                continue

            try:
                # Map goods_type to the leading-space column name the sheet expects
                row[" goods_type"] = row.pop("goods_type", "")
                self.sheets.append_row(SPREADSHEET_ID, VERIFICATION_WS, row)
                self._seen.add(job_number)
                written += 1
            except Exception as e:
                logger.error("Verification: sheet write failed for job %s: %s", job_number, e)
                errors += 1

        logger.info(
            "Verification complete — written=%d skipped=%d not_found=%d errors=%d",
            written, skipped, not_found, errors,
        )
        return {"written": written, "skipped": skipped, "not_found": not_found, "errors": errors}


class RpaEntryPipeline:
    """
    Fills the Proteo AddOrder form for each job using extracted data,
    takes a screenshot, and writes results to the 'RPA Entry' sheet.
    Does NOT save the order — dry-run only.
    """

    def __init__(self, proteo: ProteoClient, sheets: SheetsClient, drive_client=None):
        self.proteo = proteo
        self.sheets = sheets
        self.drive = drive_client
        self._seen: set[str] = set()

    def _ensure_rpa_sheet(self):
        """Create the RPA Entry worksheet with headers if it doesn't exist."""
        try:
            sh = self.sheets._gc.open_by_key(SPREADSHEET_ID)
            existing = [ws.title for ws in sh.worksheets()]
            if RPA_ENTRY_WS not in existing:
                ws = sh.add_worksheet(title=RPA_ENTRY_WS, rows=1000, cols=len(RPA_ENTRY_HEADERS))
                ws.append_row(RPA_ENTRY_HEADERS, value_input_option="USER_ENTERED")
                logger.info("Created '%s' worksheet with headers", RPA_ENTRY_WS)
            else:
                # Ensure headers exist in row 1
                ws = sh.worksheet(RPA_ENTRY_WS)
                if not ws.row_values(1):
                    ws.append_row(RPA_ENTRY_HEADERS, value_input_option="USER_ENTERED")
                    logger.info("Added headers to existing '%s' worksheet", RPA_ENTRY_WS)
            # Cache the worksheet
            key = f"{SPREADSHEET_ID}:{RPA_ENTRY_WS}"
            self.sheets._worksheets[key] = sh.worksheet(RPA_ENTRY_WS)
        except Exception as e:
            logger.warning("Could not ensure RPA Entry sheet: %s", e)

    def _load_seen(self) -> set[str]:
        try:
            self._ensure_rpa_sheet()
            ws = self.sheets._get_worksheet(SPREADSHEET_ID, RPA_ENTRY_WS)
            headers = ws.row_values(1)
            if "job_number" not in headers:
                return set()
            col_idx = headers.index("job_number")
            values = ws.col_values(col_idx + 1)[1:]
            return {str(v).strip() for v in values if v}
        except Exception as e:
            logger.warning("Could not load RPA Entry sheet: %s", e)
            return set()

    def process_jobs(self, job_orders: list[dict]) -> dict:
        """
        Run RPA entry for each order dict. job_orders is a list of order dicts
        with all extraction fields (collection_point, delivery_point, dates, etc.).

        Returns summary counts.
        """
        if not self._seen:
            self._seen = self._load_seen()

        written = 0
        skipped = 0
        errors = 0

        for order in job_orders:
            job_number = order.get("delivery_order_number") or order.get("job_number", "")
            if not job_number:
                continue

            if job_number in self._seen:
                logger.debug("RPA: skipping already-processed job %s", job_number)
                skipped += 1
                continue

            try:
                rpa = self.proteo.enter_order(order, drive_client=self.drive)
            except Exception as e:
                logger.error("RPA: enter_order failed for job %s: %s", job_number, e)
                errors += 1
                continue

            now = datetime.now(timezone.utc).isoformat()
            row = {
                "job_number":             job_number,
                "processed_at":           now,
                "success":                str(rpa.success),
                "screenshot_url":         rpa.screenshot_url,
                "typed_client":           rpa.typed_client,
                "typed_collection_point": rpa.typed_collection_point,
                "typed_delivery_point":   rpa.typed_delivery_point,
                "typed_collection_date":  rpa.typed_collection_date,
                "typed_collection_time":  rpa.typed_collection_time,
                "typed_delivery_date":    rpa.typed_delivery_date,
                "typed_delivery_time":    rpa.typed_delivery_time,
                "typed_order_number":     rpa.typed_order_number,
                "typed_price":            rpa.typed_price,
                "agreement_score":        rpa.agreement_score,
                "field_matches":          json.dumps(rpa.field_matches),
                "error":                  rpa.error,
                # Planned (extraction) values for comparison
                "planned_collection_point": order.get("collection_point", ""),
                "planned_delivery_point":   order.get("delivery_point", ""),
                "planned_collection_date":  order.get("collection_date", ""),
                "planned_collection_time":  order.get("collection_time", ""),
                "planned_delivery_date":    order.get("delivery_date", ""),
                "planned_delivery_time":    order.get("delivery_time", ""),
                "planned_order_number":     order.get("order_number", ""),
                "planned_price":            order.get("price", "") or order.get("rate", ""),
            }

            try:
                self.sheets.append_row(SPREADSHEET_ID, RPA_ENTRY_WS, row)
                self._seen.add(job_number)
                written += 1
            except Exception as e:
                logger.error("RPA: sheet write failed for job %s: %s", job_number, e)
                errors += 1

        logger.info(
            "RPA entry complete — written=%d skipped=%d errors=%d",
            written, skipped, errors,
        )
        return {"written": written, "skipped": skipped, "errors": errors}
