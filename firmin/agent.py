from __future__ import annotations
import os
import time

from dotenv import load_dotenv

load_dotenv()

from firmin.clients.ai import AiClient
from firmin.clients.drive import DriveClient
from firmin.clients.gmail import GmailClient
from firmin.clients.proteo import ProteoClient
from firmin.clients.sheets import SheetsClient
from firmin.clients.slack import SlackClient
from firmin.clients.supabase import SupabaseClient
from firmin.pipeline import Pipeline
from firmin.profiles.loader import load_all_profiles, match_profile
from firmin.utils.dedup import DedupStore
from firmin.utils.logger import get_logger
from firmin.verification import VerificationPipeline, RpaEntryPipeline

logger = get_logger(__name__)


def run():
    poll_interval = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))
    dedup_db = os.getenv("DEDUP_DB_PATH", "firmin.db")
    clients_dir = os.getenv("CLIENTS_DIR", "config/clients")
    gmail_query = os.getenv("GMAIL_QUERY", "")

    logger.info("Firmin agent starting — poll interval: %ds", poll_interval)

    # Initialise clients (fail fast if misconfigured)
    gmail = GmailClient(
        token_path=os.getenv("GMAIL_TOKEN_PATH", "config/gmail_token.json"),
        credentials_path=os.getenv("GMAIL_CREDENTIALS_PATH", "config/gmail_credentials.json"),
    )
    ai = AiClient()
    supabase = SupabaseClient()
    sheets = SheetsClient()
    slack = SlackClient()
    dedup = DedupStore(dedup_db)

    profiles = load_all_profiles(clients_dir)
    logger.info("Loaded %d client profile(s)", len(profiles))

    # Build Gmail query from all profile subject_contains + sender_contains filters (OR'd together)
    if not gmail_query:
        terms = []
        for p in profiles:
            for kw in p.email_filters.subject_contains:
                terms.append(f'subject:"{kw}"')
            for kw in p.email_filters.sender_contains:
                terms.append(f"from:{kw}")
        gmail_query = "(" + " OR ".join(terms) + ") is:unread has:attachment"
    logger.info("Gmail query: %s", gmail_query)

    try:
        drive = DriveClient()
        logger.info("Drive client initialised — PDFs will be uploaded to Drive")
    except RuntimeError as e:
        logger.warning("Drive upload disabled: %s", e)
        drive = None

    pipeline = Pipeline(
        ai_client=ai,
        supabase_client=supabase,
        sheets_client=sheets,
        dedup_store=dedup,
        slack_client=slack,
        drive_client=drive,
    )

    try:
        proteo = ProteoClient()
        verification = VerificationPipeline(proteo=proteo, sheets=sheets)
        rpa_entry = RpaEntryPipeline(proteo=proteo, sheets=sheets, drive_client=drive)
        logger.info("Proteo verification + RPA entry pipeline initialised")
    except RuntimeError as e:
        logger.warning("Proteo verification disabled: %s", e)
        verification = None
        rpa_entry = None

    while True:
        try:
            _poll(gmail, pipeline, verification, profiles, dedup, gmail_query, drive, rpa_entry)
        except Exception as e:
            logger.error("Poll cycle error: %s", e, exc_info=True)

        logger.debug("Sleeping %ds until next poll", poll_interval)
        time.sleep(poll_interval)


def _poll(
    gmail: GmailClient,
    pipeline: Pipeline,
    verification: VerificationPipeline | None,
    profiles,
    dedup: DedupStore,
    query: str,
    drive: DriveClient | None = None,
    rpa_entry: RpaEntryPipeline | None = None,
):
    emails = gmail.fetch_unread(query=query)

    if not emails:
        logger.debug("No new emails")
        return

    for email in emails:
        if dedup.email_seen(email.message_id):
            logger.debug("Email already processed: %s", email.message_id)
            gmail.mark_as_read(email.message_id)
            continue

        profile = match_profile(
            subject=email.subject,
            has_attachment=bool(email.attachments),
            profiles=profiles,
            sender=email.sender,
        )

        if not profile:
            logger.info("No profile matched for email: %s (subject: %s)", email.message_id, email.subject)
            dedup.mark_email_seen(email.message_id)
            gmail.mark_as_read(email.message_id)
            continue

        logger.info(
            "Processing email %s with profile: %s",
            email.message_id, profile.display_name,
        )

        result = pipeline.process_email(email, profile, drive_client=drive)
        dedup.mark_email_seen(email.message_id)
        gmail.mark_as_read(email.message_id)

        logger.info(
            "Email %s done — %d jobs total, %d written, %d skipped, %d errors",
            email.message_id,
            result.total_jobs,
            result.written,
            result.skipped,
            result.errors,
        )

        active_orders = [o for o in result.orders if not o.skipped_duplicate and not o.error]
        job_numbers = [o.job_number for o in active_orders]
        po_numbers = {o.job_number: o.po_number for o in active_orders if o.po_number}
        search_terms = {o.job_number: o.order_number for o in active_orders if o.order_number}

        # Verification: scrape Proteo Find Order for each processed job
        if verification and job_numbers:
            logger.info("Running Proteo verification for %d jobs", len(job_numbers))
            try:
                verification.process_jobs(job_numbers, po_numbers=po_numbers or None, search_terms=search_terms or None)
            except Exception as e:
                logger.error("Verification pipeline error: %s", e, exc_info=True)

        # RPA entry: fill AddOrder form, screenshot, compare vs extraction
        if rpa_entry and job_numbers and hasattr(result, "_order_dicts"):
            logger.info("Running RPA entry for %d jobs", len(job_numbers))
            try:
                rpa_entry.process_jobs(result._order_dicts)
            except Exception as e:
                logger.error("RPA entry pipeline error: %s", e, exc_info=True)


if __name__ == "__main__":
    run()
