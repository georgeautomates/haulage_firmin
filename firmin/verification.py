from __future__ import annotations

from firmin.clients.proteo import ProteoClient
from firmin.clients.sheets import SheetsClient
from firmin.utils.logger import get_logger

logger = get_logger(__name__)

SPREADSHEET_ID = "1uEst-r23EiTyfdmL6gx_YQMuzR0s7yyx-qJVLIUyDSI"
VERIFICATION_WS = "Verification"


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
