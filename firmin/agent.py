from __future__ import annotations
import os
import time

from dotenv import load_dotenv

load_dotenv()

from firmin.clients.ai import AiClient
from firmin.clients.gmail import GmailClient
from firmin.clients.sheets import SheetsClient
from firmin.clients.slack import SlackClient
from firmin.clients.supabase import SupabaseClient
from firmin.pipeline import Pipeline
from firmin.profiles.loader import load_all_profiles, match_profile
from firmin.utils.dedup import DedupStore
from firmin.utils.logger import get_logger

logger = get_logger(__name__)


def run():
    poll_interval = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))
    dedup_db = os.getenv("DEDUP_DB_PATH", "firmin.db")
    clients_dir = os.getenv("CLIENTS_DIR", "config/clients")
    gmail_query = os.getenv("GMAIL_QUERY", "subject:@dssmith.com is:unread has:attachment")

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

    pipeline = Pipeline(
        ai_client=ai,
        supabase_client=supabase,
        sheets_client=sheets,
        dedup_store=dedup,
        slack_client=slack,
    )

    while True:
        try:
            _poll(gmail, pipeline, profiles, dedup, gmail_query)
        except Exception as e:
            logger.error("Poll cycle error: %s", e, exc_info=True)

        logger.debug("Sleeping %ds until next poll", poll_interval)
        time.sleep(poll_interval)


def _poll(gmail: GmailClient, pipeline: Pipeline, profiles, dedup: DedupStore, query: str):
    emails = gmail.fetch_unread(query=query)

    if not emails:
        logger.debug("No new emails")
        return

    for email in emails:
        if dedup.email_seen(email.message_id):
            logger.debug("Email already processed: %s", email.message_id)
            continue

        profile = match_profile(
            subject=email.subject,
            has_attachment=bool(email.attachments),
            profiles=profiles,
        )

        if not profile:
            logger.info("No profile matched for email: %s (subject: %s)", email.message_id, email.subject)
            dedup.mark_email_seen(email.message_id)
            continue

        logger.info(
            "Processing email %s with profile: %s",
            email.message_id, profile.display_name,
        )

        result = pipeline.process_email(email, profile)
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


if __name__ == "__main__":
    run()
