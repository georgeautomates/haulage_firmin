from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from firmin.clients.ai import AiClient, AiExtractionResult
from firmin.clients.pdf import extract_pdf
from firmin.clients.sheets import SheetsClient
from firmin.clients.slack import SlackClient
from firmin.clients.supabase import SupabaseClient
from firmin.clients.gmail import EmailMessage
from firmin.profiles.loader import ClientProfile
from firmin.scoring import score_order
from firmin.utils.dedup import DedupStore
from firmin.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class OrderResult:
    job_number: str
    status: str  # GREEN / YELLOW / RED
    composite_score: int
    written_to_sheet: bool
    skipped_duplicate: bool
    error: Optional[str] = None
    collection_point: str = "—"
    delivery_point: str = "—"
    price: str = "—"
    failure_reasons: list = None

    def __post_init__(self):
        if self.failure_reasons is None:
            self.failure_reasons = []


@dataclass
class PipelineResult:
    message_id: str
    total_jobs: int
    orders: list[OrderResult] = field(default_factory=list)

    @property
    def written(self) -> int:
        return sum(1 for o in self.orders if o.written_to_sheet)

    @property
    def skipped(self) -> int:
        return sum(1 for o in self.orders if o.skipped_duplicate)

    @property
    def errors(self) -> int:
        return sum(1 for o in self.orders if o.error)


class Pipeline:
    def __init__(
        self,
        ai_client: AiClient,
        supabase_client: SupabaseClient,
        sheets_client: SheetsClient,
        dedup_store: DedupStore,
        slack_client: SlackClient | None = None,
    ):
        self.ai = ai_client
        self.supabase = supabase_client
        self.sheets = sheets_client
        self.dedup = dedup_store
        self.slack = slack_client

    def process_email(self, email: EmailMessage, profile: ClientProfile) -> PipelineResult:
        result = PipelineResult(message_id=email.message_id, total_jobs=0)

        # Find PDF attachments
        pdf_attachments = [
            a for a in email.attachments
            if a["filename"].lower().endswith(".pdf")
            or "pdf" in a.get("mime_type", "").lower()
        ]

        if not pdf_attachments:
            logger.warning("No PDF attachments in email %s", email.message_id)
            return result

        for attachment in pdf_attachments:
            logger.info("Processing attachment: %s", attachment["filename"])
            pdf_result = extract_pdf(attachment["data"])

            if not pdf_result.job_numbers:
                logger.warning("No job numbers found in %s", attachment["filename"])
                continue

            logger.info(
                "Found %d job numbers in %s",
                len(pdf_result.job_numbers), attachment["filename"]
            )
            result.total_jobs += len(pdf_result.job_numbers)

            for job_number in pdf_result.job_numbers:
                order_result = self._process_job(
                    job_number=job_number,
                    raw_text=pdf_result.raw_text,
                    message_id=email.message_id,
                    profile=profile,
                )
                result.orders.append(order_result)

        if result.total_jobs > 0 and self.slack:
            slack_orders = [
                {
                    "job_number": o.job_number,
                    "status": o.status,
                    "composite_score": o.composite_score,
                    "collection_point": getattr(o, "collection_point", "—"),
                    "delivery_point": getattr(o, "delivery_point", "—"),
                    "price": getattr(o, "price", "—"),
                    "failure_reasons": getattr(o, "failure_reasons", []),
                }
                for o in result.orders
            ]
            self.slack.post_batch_summary(
                email_subject=email.subject,
                total_jobs=result.total_jobs,
                written=result.written,
                skipped=result.skipped,
                errors=result.errors,
                orders=slack_orders,
            )

        return result

    def _process_job(
        self,
        job_number: str,
        raw_text: str,
        message_id: str,
        profile: ClientProfile,
    ) -> OrderResult:
        # Dedup check
        if self.dedup.order_seen(job_number):
            logger.info("Skipping duplicate job: %s", job_number)
            return OrderResult(
                job_number=job_number,
                status="SKIPPED",
                composite_score=0,
                written_to_sheet=False,
                skipped_duplicate=True,
            )

        # AI extraction
        extracted = self.ai.extract_job(raw_text, job_number)
        if not extracted:
            return OrderResult(
                job_number=job_number,
                status="ERROR",
                composite_score=0,
                written_to_sheet=False,
                skipped_duplicate=False,
                error="AI extraction failed",
            )

        # Location lookup
        client_name = profile.defaults.get("client_name", "")
        conditional_locations = getattr(profile, "conditional_locations", {})
        collection_point = (
            self.supabase.lookup_location(
                postcode=extracted.collection_postcode,
                org_name=extracted.collection_org,
                search=extracted.collection_search,
                known_locations=profile.known_locations,
                conditional_locations=conditional_locations,
                client_name=client_name,
                pdf_address=extracted.collection_search,
            ) or "UNMATCHED"
        )
        delivery_point = (
            self.supabase.lookup_location(
                postcode=extracted.delivery_postcode,
                org_name=extracted.delivery_org,
                search=extracted.delivery_search,
                known_locations=profile.known_locations,
                conditional_locations=conditional_locations,
                client_name=client_name,
                pdf_address=extracted.delivery_search,
            ) or extracted.delivery_org or "UNMATCHED"
        )

        # Build order row
        now = datetime.now(timezone.utc).isoformat()
        order = {
            # Hardcoded defaults from profile
            **profile.defaults,
            # AI extracted
            "job_number": job_number,
            "price": extracted.price,
            "order_number": extracted.order_number,
            "po_number": extracted.order_number,
            "customer_ref": extracted.customer_ref,
            "work_type": extracted.work_type,
            "collection_postcode": extracted.collection_postcode,
            "collection_date": extracted.collection_date,
            "collection_time": extracted.collection_time,
            "delivery_postcode": extracted.delivery_postcode,
            "delivery_date": extracted.delivery_date,
            "delivery_time": extracted.delivery_time,
            # Location matched
            "collection_point": collection_point,
            "delivery_point": delivery_point,
            # Metadata
            "delivery_order_number": job_number,
            "processed_at": now,
            # Sheets column name mapping
            "rate": extracted.price,
            " goods_type": profile.defaults.get("goods_type", ""),
        }

        # Confidence scoring
        scored = score_order(order)
        order["composite_score"] = scored.composite_score
        order["Composite_score"] = scored.composite_score
        order["status"] = scored.status
        order["Status"] = scored.status

        # Mark as seen before writing — prevents reprocessing on crash/retry
        self.dedup.mark_order_seen(job_number, message_id)

        # Write to sheet
        try:
            self.sheets.append_row(
                profile.sheets.spreadsheet_id,
                profile.sheets.worksheet_name,
                order,
            )
            logger.info(
                "Job %s written — %s (score: %d)",
                job_number, scored.status, scored.composite_score,
            )
            return OrderResult(
                job_number=job_number,
                status=scored.status,
                composite_score=scored.composite_score,
                written_to_sheet=True,
                skipped_duplicate=False,
                collection_point=collection_point,
                delivery_point=delivery_point,
                price=extracted.price or "—",
                failure_reasons=scored.failure_reasons,
            )
        except Exception as e:
            logger.error("SHEET WRITE FAILED for job %s (already marked seen — manual recovery needed): %s", job_number, e)
            return OrderResult(
                job_number=job_number,
                status=scored.status,
                composite_score=scored.composite_score,
                written_to_sheet=False,
                skipped_duplicate=False,
                error=str(e),
                collection_point=collection_point,
                delivery_point=delivery_point,
                price=extracted.price or "—",
                failure_reasons=scored.failure_reasons,
            )
