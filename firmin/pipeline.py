from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from firmin.clients.ai import AiClient, AiExtractionResult, DualExtractionResult
from firmin.clients.drive import DriveClient
from firmin.clients.pdf import extract_pdf
from firmin.clients.sheets import SheetsClient
from firmin.clients.slack import SlackClient
from firmin.clients.supabase import SupabaseClient
from firmin.clients.unipet_pdf import parse_unipet_manifest
from firmin.clients.revolution_beauty_pdf import parse_revolution_beauty_booking, collection_point_for, delivery_point_for
from firmin.clients.aim_pdf import parse_aim_booking
from firmin.clients.community_playthings_pdf import parse_community_playthings_pdf, CommunityPlaythingsDelivery, CommunityPlaythingsRoundRobin
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
    _order_dict: dict = field(default_factory=dict)

    def __post_init__(self):
        if self.failure_reasons is None:
            self.failure_reasons = []


@dataclass
class PipelineResult:
    message_id: str
    total_jobs: int
    orders: list[OrderResult] = field(default_factory=list)
    # Full order dicts (extraction + location lookup) — used by RPA entry pipeline
    _order_dicts: list[dict] = field(default_factory=list)

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
        drive_client: DriveClient | None = None,
    ):
        self.ai = ai_client
        self.supabase = supabase_client
        self.sheets = sheets_client
        self.dedup = dedup_store
        self.slack = slack_client
        self.drive = drive_client

    def process_email(self, email: EmailMessage, profile: ClientProfile, drive_client: DriveClient | None = None) -> PipelineResult:
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

            custom_parser = profile.parser in ("unipet_manifest", "revolution_beauty", "aim", "community_playthings")
            if not pdf_result.job_numbers and not custom_parser:
                logger.warning("No job numbers found in %s", attachment["filename"])
                continue

            logger.info(
                "Found %d job numbers in %s",
                len(pdf_result.job_numbers), attachment["filename"]
            )
            if not custom_parser:
                result.total_jobs += len(pdf_result.job_numbers)

            # Upload PDF to Drive (once per attachment)
            pdf_url = ""
            _drive = drive_client or self.drive
            if _drive and attachment.get("data"):
                try:
                    pdf_url = _drive.upload_pdf(
                        pdf_bytes=attachment["data"],
                        filename=f"{email.message_id}.pdf",
                    )
                except Exception as e:
                    logger.warning("Drive upload failed for %s: %s", attachment["filename"], e)

            if profile.parser == "unipet_manifest":
                manifest = parse_unipet_manifest(pdf_result.raw_text)
                result.total_jobs += len(manifest.rows)
                for row in manifest.rows:
                    order_result = self._process_unipet_row(
                        row=row,
                        message_id=email.message_id,
                        profile=profile,
                        pdf_url=pdf_url,
                        email_subject=email.subject,
                        email_body=email.body,
                    )
                    result.orders.append(order_result)
            elif profile.parser == "revolution_beauty":
                booking = parse_revolution_beauty_booking(pdf_result.raw_text)
                if booking:
                    result.total_jobs += 1
                    order_result = self._process_revolution_beauty_booking(
                        booking=booking,
                        message_id=email.message_id,
                        profile=profile,
                        pdf_url=pdf_url,
                        email_subject=email.subject,
                        email_body=email.body,
                    )
                    result.orders.append(order_result)
                else:
                    logger.warning("Revolution Beauty parser returned nothing for %s", attachment["filename"])
            elif profile.parser == "aim":
                booking = parse_aim_booking(pdf_result.raw_text)
                if booking:
                    result.total_jobs += 1
                    order_result = self._process_aim_booking(
                        booking=booking,
                        message_id=email.message_id,
                        profile=profile,
                        pdf_url=pdf_url,
                        email_subject=email.subject,
                        email_body=email.body,
                    )
                    result.orders.append(order_result)
                else:
                    logger.warning("AIM parser returned nothing for %s", attachment["filename"])
            elif profile.parser == "community_playthings":
                bookings = parse_community_playthings_pdf(pdf_result.raw_text)
                if bookings:
                    result.total_jobs += len(bookings)
                    for booking in bookings:
                        order_result = self._process_community_playthings_booking(
                            booking=booking,
                            message_id=email.message_id,
                            profile=profile,
                            pdf_url=pdf_url,
                            email_subject=email.subject,
                            email_body=email.body,
                        )
                        result.orders.append(order_result)
                else:
                    logger.warning("Community Playthings parser returned nothing for %s", attachment["filename"])
            else:
                for job_number in pdf_result.job_numbers:
                    order_result = self._process_job(
                        job_number=job_number,
                        raw_text=pdf_result.raw_text,
                        message_id=email.message_id,
                        profile=profile,
                        pdf_url=pdf_url,
                        email_subject=email.subject,
                        email_body=email.body,
                    )
                    result.orders.append(order_result)

        # Collect full order dicts for RPA entry pipeline
        result._order_dicts = [
            o._order_dict for o in result.orders
            if o._order_dict and not o.skipped_duplicate and not o.error
        ]

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

    def _process_unipet_row(self, row, message_id: str, profile: ClientProfile, pdf_url: str = "", email_subject: str = "", email_body: str = "") -> OrderResult:
        from firmin.clients.unipet_pdf import UnipetRow
        # Customer Order number is the Proteo-searchable unique identifier (Order No)
        # Delivery Note is Proteo's internal Delivery Order Number
        job_number = row.customer_order

        if self.dedup.order_seen(job_number):
            logger.info("Skipping duplicate Unipet job: %s", job_number)
            return OrderResult(
                job_number=job_number,
                status="SKIPPED",
                composite_score=0,
                written_to_sheet=False,
                skipped_duplicate=True,
            )

        # Delivery location lookup by postcode + customer name
        client_name = profile.defaults.get("client_name", "")
        conditional_locations = getattr(profile, "conditional_locations", {})
        delivery_point = (
            self.supabase.lookup_location(
                postcode=row.postcode,
                org_name=row.customer_name,
                search=row.customer_name,
                known_locations=profile.known_locations,
                conditional_locations=conditional_locations,
                client_name=client_name,
                pdf_address=row.customer_name,
            ) or row.customer_name
        )

        now = __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat()
        order = {
            **profile.defaults,
            "job_number": job_number,
            "delivery_order_number": row.delivery_note,
            "order_number": job_number,
            "po_number": job_number,
            "customer_ref": job_number,
            "pallets": row.pallets,
            "spaces": row.pallets,
            "collection_date": row.collection_date,
            "collection_time": "09:00",
            "delivery_point": delivery_point,
            "delivery_postcode": row.postcode,
            "delivery_date": row.delivery_date,
            "delivery_time": row.delivery_time,
            "price": "",
            "rate": "",
            "work_type": "",
            "processed_at": now,
            "message_id": message_id,
            "pdf_url": pdf_url,
            "email_subject": email_subject,
            "email_body": email_body,
            " goods_type": profile.defaults.get("goods_type", ""),
        }

        scored = score_order(order)
        order["composite_score"] = scored.composite_score
        order["Composite_score"] = scored.composite_score
        order["status"] = scored.status
        order["Status"] = scored.status

        self.dedup.mark_order_seen(job_number, message_id)

        try:
            self.sheets.append_row(
                profile.sheets.spreadsheet_id,
                profile.sheets.worksheet_name,
                order,
            )
            logger.info("Unipet job %s written — %s (score: %d)", job_number, scored.status, scored.composite_score)
            return OrderResult(
                job_number=job_number,
                status=scored.status,
                composite_score=scored.composite_score,
                written_to_sheet=True,
                skipped_duplicate=False,
                collection_point=profile.defaults.get("collection_point", "—"),
                delivery_point=delivery_point,
                price="—",
                failure_reasons=scored.failure_reasons,
            )
        except Exception as e:
            logger.error("SHEET WRITE FAILED for Unipet job %s (already marked seen — manual recovery needed): %s", job_number, e)
            return OrderResult(
                job_number=job_number,
                status=scored.status,
                composite_score=scored.composite_score,
                written_to_sheet=False,
                skipped_duplicate=False,
                error=str(e),
                collection_point=profile.defaults.get("collection_point", "—"),
                delivery_point=delivery_point,
                price="—",
                failure_reasons=scored.failure_reasons,
            )

    def _process_revolution_beauty_booking(self, booking, message_id: str, profile: ClientProfile, pdf_url: str = "", email_subject: str = "", email_body: str = "") -> OrderResult:
        from firmin.clients.revolution_beauty_pdf import RevolutionBeautyBooking

        job_number = booking.job_number
        if not job_number:
            job_number = message_id  # fallback dedup key

        if self.dedup.order_seen(job_number):
            logger.info("Skipping duplicate Revolution Beauty job: %s", job_number)
            return OrderResult(
                job_number=job_number,
                status="SKIPPED",
                composite_score=0,
                written_to_sheet=False,
                skipped_duplicate=True,
            )

        # Direction-aware DE11 0BB handling
        collection_point = collection_point_for(booking.collection_postcode)
        delivery_point = delivery_point_for(booking.delivery_postcode)

        # For other postcodes — look up via known_locations then Supabase
        if not collection_point:
            collection_point = profile.known_locations.get(booking.collection_postcode, "")
            if not collection_point:
                collection_point = self.supabase.lookup_location(
                    postcode=booking.collection_postcode,
                    org_name="",
                    search="",
                    known_locations=profile.known_locations,
                    conditional_locations=getattr(profile, "conditional_locations", {}),
                    client_name=profile.defaults.get("client_name", ""),
                    pdf_address="",
                ) or booking.collection_postcode

        if not delivery_point:
            delivery_point = profile.known_locations.get(booking.delivery_postcode, "")
            if not delivery_point:
                delivery_point = self.supabase.lookup_location(
                    postcode=booking.delivery_postcode,
                    org_name="",
                    search="",
                    known_locations=profile.known_locations,
                    conditional_locations=getattr(profile, "conditional_locations", {}),
                    client_name=profile.defaults.get("client_name", ""),
                    pdf_address="",
                ) or booking.delivery_postcode

        # Pallets — Full Load = 26, otherwise use the integer from the PDF
        try:
            pallets = int(booking.pallets_raw) if booking.pallets_raw.strip().lower() != "full load" else 26
        except ValueError:
            pallets = 26

        # Weight — Full Load = blank, Firmin Xpress = pallets × 50
        is_full_load = booking.pallets_raw.strip().lower() == "full load" or pallets == 26
        weight = "" if is_full_load else pallets * 50

        now = __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat()
        order = {
            **profile.defaults,
            "client_name": profile.defaults.get("client_name", "Revolution Beauty Ltd"),
            "business_type": booking.business_type,
            "job_number": job_number,
            "delivery_order_number": job_number,
            "order_number": booking.order_number,
            "po_number": booking.order_number,
            "customer_ref": booking.customer_ref,
            "collection_point": collection_point,
            "collection_postcode": booking.collection_postcode,
            "collection_date": booking.collection_date,
            "collection_time": booking.collection_time,
            "delivery_point": delivery_point,
            "delivery_postcode": booking.delivery_postcode,
            "delivery_date": booking.delivery_date,
            "delivery_time": booking.delivery_time,
            "pallets": pallets,
            "spaces": pallets,
            "weight": weight,
            "price": "",
            "rate": "",
            "work_type": "",
            "processed_at": now,
            "message_id": message_id,
            "pdf_url": pdf_url,
            "email_subject": email_subject,
            "email_body": email_body,
        }

        scored = score_order(order)
        order["composite_score"] = scored.composite_score
        order["Composite_score"] = scored.composite_score
        order["status"] = scored.status
        order["Status"] = scored.status

        self.dedup.mark_order_seen(job_number, message_id)

        try:
            self.sheets.append_row(
                profile.sheets.spreadsheet_id,
                profile.sheets.worksheet_name,
                order,
            )
            logger.info("Revolution Beauty job %s written — %s (score: %d)", job_number, scored.status, scored.composite_score)
            return OrderResult(
                job_number=job_number,
                status=scored.status,
                composite_score=scored.composite_score,
                written_to_sheet=True,
                skipped_duplicate=False,
                collection_point=collection_point,
                delivery_point=delivery_point,
                price="—",
                failure_reasons=scored.failure_reasons,
            )
        except Exception as e:
            logger.error("SHEET WRITE FAILED for Revolution Beauty job %s: %s", job_number, e)
            return OrderResult(
                job_number=job_number,
                status=scored.status,
                composite_score=scored.composite_score,
                written_to_sheet=False,
                skipped_duplicate=False,
                error=str(e),
                collection_point=collection_point,
                delivery_point=delivery_point,
                price="—",
                failure_reasons=scored.failure_reasons,
            )

    def _process_aim_booking(self, booking, message_id: str, profile: ClientProfile, pdf_url: str = "", email_subject: str = "", email_body: str = "") -> OrderResult:
        job_number = booking.job_number
        if not job_number:
            job_number = message_id

        if self.dedup.order_seen(job_number):
            logger.info("Skipping duplicate AIM job: %s", job_number)
            return OrderResult(
                job_number=job_number,
                status="SKIPPED",
                composite_score=0,
                written_to_sheet=False,
                skipped_duplicate=True,
            )

        # Delivery location lookup — postcode + company name hint
        delivery_point = profile.known_locations.get(booking.delivery_postcode, "")
        if not delivery_point:
            delivery_point = self.supabase.lookup_location(
                postcode=booking.delivery_postcode,
                org_name=booking.delivery_company,
                search=booking.delivery_company,
                known_locations=profile.known_locations,
                conditional_locations=getattr(profile, "conditional_locations", {}),
                client_name=profile.defaults.get("client_name", ""),
                pdf_address=booking.delivery_company,
            ) or booking.delivery_postcode

        collection_point = profile.defaults.get("collection_point", "AIM Ltd - Crawley")

        now = __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat()
        order = {
            **profile.defaults,
            "client_name": profile.defaults.get("client_name", "AIM (SIG Trading Limited)"),
            "job_number": job_number,
            "delivery_order_number": job_number,
            "order_number": booking.order_number,
            "po_number": booking.order_number,
            "customer_ref": booking.order_number,
            "collection_point": collection_point,
            "collection_postcode": profile.defaults.get("collection_postcode", "RH10 9NH"),
            "collection_date": booking.collection_date,
            "collection_time": booking.collection_time,
            "delivery_point": delivery_point,
            "delivery_postcode": booking.delivery_postcode,
            "delivery_date": booking.delivery_date,
            "delivery_time": "09:00",
            "pallets": booking.pallets,
            "spaces": booking.pallets,
            "weight": booking.weight,
            "price": booking.price,
            "rate": booking.price,
            "work_type": "",
            "processed_at": now,
            "message_id": message_id,
            "pdf_url": pdf_url,
            "email_subject": email_subject,
            "email_body": email_body,
        }

        scored = score_order(order)
        order["composite_score"] = scored.composite_score
        order["Composite_score"] = scored.composite_score
        order["status"] = scored.status
        order["Status"] = scored.status

        self.dedup.mark_order_seen(job_number, message_id)

        try:
            self.sheets.append_row(
                profile.sheets.spreadsheet_id,
                profile.sheets.worksheet_name,
                order,
            )
            logger.info("AIM job %s written — %s (score: %d)", job_number, scored.status, scored.composite_score)
            return OrderResult(
                job_number=job_number,
                status=scored.status,
                composite_score=scored.composite_score,
                written_to_sheet=True,
                skipped_duplicate=False,
                collection_point=collection_point,
                delivery_point=delivery_point,
                price=booking.price or "—",
                failure_reasons=scored.failure_reasons,
                _order_dict=order,
            )
        except Exception as e:
            logger.error("SHEET WRITE FAILED for AIM job %s: %s", job_number, e)
            return OrderResult(
                job_number=job_number,
                status=scored.status,
                composite_score=scored.composite_score,
                written_to_sheet=False,
                skipped_duplicate=False,
                error=str(e),
                collection_point=collection_point,
                delivery_point=delivery_point,
                price=booking.price or "—",
                failure_reasons=scored.failure_reasons,
                _order_dict=order,
            )

    def _process_community_playthings_booking(self, booking, message_id: str, profile: ClientProfile, pdf_url: str = "", email_subject: str = "", email_body: str = "") -> OrderResult:
        if isinstance(booking, CommunityPlaythingsRoundRobin):
            job_number = booking.job_number
            if self.dedup.order_seen(job_number):
                logger.info("Skipping duplicate CP Round Robin: %s", job_number)
                return OrderResult(job_number=job_number, status="SKIPPED", composite_score=0, written_to_sheet=False, skipped_duplicate=True)

            collection_point = "Community Playthings - Sittingbourne"
            delivery_point = "Round Robin - Sittingbourne"
            now = __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat()
            order = {
                **profile.defaults,
                "client_name": profile.defaults.get("client_name", "Community Playthings"),
                "business_type": "General | Part/Full Load",
                "service": "Full Load",
                "job_number": job_number,
                "delivery_order_number": f"{booking.day_name} Round Robin",
                "order_number": f"{booking.day_name} Round Robin",
                "po_number": "",
                "customer_ref": "",
                "collection_point": collection_point,
                "collection_postcode": "ME10 3RN",
                "collection_date": booking.date,
                "collection_time": "09:00",
                "delivery_point": delivery_point,
                "delivery_postcode": "ME10 3RN",
                "delivery_date": booking.date,
                "delivery_time": "09:00",
                "pallets": 26,
                "spaces": 26,
                "weight": "",
                "price": "",
                "rate": "",
                "work_type": "",
                "processed_at": now,
                "message_id": message_id,
                "pdf_url": pdf_url,
                "email_subject": email_subject,
                "email_body": email_body,
            }
        else:
            # CommunityPlaythingsDelivery
            job_number = booking.job_number
            if self.dedup.order_seen(job_number):
                logger.info("Skipping duplicate CP delivery: %s", job_number)
                return OrderResult(job_number=job_number, status="SKIPPED", composite_score=0, written_to_sheet=False, skipped_duplicate=True)

            delivery_point = profile.known_locations.get(booking.delivery_postcode, "")
            if not delivery_point:
                delivery_point = self.supabase.lookup_location(
                    postcode=booking.delivery_postcode,
                    org_name=booking.delivery_company,
                    search=booking.delivery_company,
                    known_locations=profile.known_locations,
                    conditional_locations=getattr(profile, "conditional_locations", {}),
                    client_name=profile.defaults.get("client_name", ""),
                    pdf_address=booking.delivery_company,
                ) or booking.delivery_postcode

            collection_point = "COMMUNITY PLAYTHINGS - SITTINGBOURNE"
            now = __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat()
            order = {
                **profile.defaults,
                "client_name": profile.defaults.get("client_name", "Community Playthings"),
                "business_type": "Firmin Xpress | Vans",
                "job_number": job_number,
                "delivery_order_number": job_number,
                "order_number": booking.order_number,
                "po_number": booking.order_number,
                "customer_ref": booking.order_number,
                "collection_point": collection_point,
                "collection_postcode": "ME10 3RN",
                "collection_date": booking.collection_date,
                "collection_time": booking.collection_time,
                "delivery_point": delivery_point,
                "delivery_postcode": booking.delivery_postcode,
                "delivery_date": booking.delivery_date,
                "delivery_time": booking.delivery_time,
                "pallets": booking.packages,
                "spaces": booking.packages,
                "weight": booking.weight,
                "price": "",
                "rate": "",
                "work_type": "",
                "processed_at": now,
                "message_id": message_id,
                "pdf_url": pdf_url,
                "email_subject": email_subject,
                "email_body": email_body,
            }

        scored = score_order(order)
        order["composite_score"] = scored.composite_score
        order["Composite_score"] = scored.composite_score
        order["status"] = scored.status
        order["Status"] = scored.status

        self.dedup.mark_order_seen(job_number, message_id)

        try:
            self.sheets.append_row(profile.sheets.spreadsheet_id, profile.sheets.worksheet_name, order)
            logger.info("Community Playthings job %s written — %s (score: %d)", job_number, scored.status, scored.composite_score)
            return OrderResult(
                job_number=job_number, status=scored.status, composite_score=scored.composite_score,
                written_to_sheet=True, skipped_duplicate=False,
                collection_point=collection_point, delivery_point=delivery_point,
                price="—", failure_reasons=scored.failure_reasons,
            )
        except Exception as e:
            logger.error("SHEET WRITE FAILED for Community Playthings job %s: %s", job_number, e)
            return OrderResult(
                job_number=job_number, status=scored.status, composite_score=scored.composite_score,
                written_to_sheet=False, skipped_duplicate=False, error=str(e),
                collection_point=collection_point, delivery_point=delivery_point,
                price="—", failure_reasons=scored.failure_reasons,
            )

    def _process_job(
        self,
        job_number: str,
        raw_text: str,
        message_id: str,
        profile: ClientProfile,
        pdf_url: str = "",
        email_subject: str = "",
        email_body: str = "",
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

        # AI extraction — dual model (gpt-4o primary, gpt-4o-mini secondary)
        dual = self.ai.extract_job_dual(raw_text, job_number)
        if not dual:
            return OrderResult(
                job_number=job_number,
                status="ERROR",
                composite_score=0,
                written_to_sheet=False,
                skipped_duplicate=False,
                error="AI extraction failed",
            )
        extracted = dual.primary

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

        # Classify St Regis sub-client:
        # Primary: subject line contains "reels"
        # Fallback: collection point is DS Smith mill (outbound Full Load jobs)
        client_name = profile.defaults.get("client_name", "")
        if "st regis" in client_name.lower() or "ds smith" in client_name.lower():
            if "reel" in email_subject.lower() or collection_point == "DS SMITH - SITTINGBOURNE":
                client_name = "St Regis Reels"
            else:
                client_name = "St Regis Fibre A/C"

        # Build order row
        now = datetime.now(timezone.utc).isoformat()
        order = {
            # Hardcoded defaults from profile
            **profile.defaults,
            "client_name": client_name,
            # AI extracted
            "job_number": job_number,
            "price": extracted.price,
            "order_number": extracted.order_number,
            "po_number": extracted.order_number,
            "customer_ref": extracted.customer_ref,
            "booking_window": extracted.booking_window,
            "traffic_note": extracted.traffic_note,
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
            "message_id": message_id,
            "pdf_url": pdf_url,
            "email_subject": email_subject,
            "email_body": email_body,
            # Dual model — secondary (gpt-4o-mini) extraction
            "m2_collection_org": dual.secondary.collection_org,
            "m2_collection_postcode": dual.secondary.collection_postcode,
            "m2_collection_date": dual.secondary.collection_date,
            "m2_collection_time": dual.secondary.collection_time,
            "m2_delivery_org": dual.secondary.delivery_org,
            "m2_delivery_postcode": dual.secondary.delivery_postcode,
            "m2_delivery_date": dual.secondary.delivery_date,
            "m2_delivery_time": dual.secondary.delivery_time,
            "m2_price": dual.secondary.price,
            "m2_order_number": dual.secondary.order_number,
            "m2_work_type": dual.secondary.work_type,
            "model_agreement_score": dual.agreement_score,
            "model_agreement_fields": ", ".join(
                f for f, ok in dual.agreement.items() if not ok
            ) or "ALL_MATCH",
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
                _order_dict=order,
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
                _order_dict=order,
            )
