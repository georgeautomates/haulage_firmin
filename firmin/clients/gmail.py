from __future__ import annotations
import base64
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from firmin.utils.logger import get_logger

logger = get_logger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/gmail.modify",
    "https://www.googleapis.com/auth/drive.file",
]


@dataclass
class EmailMessage:
    message_id: str
    subject: str
    sender: str
    body: str = ""
    attachments: list[dict] = field(default_factory=list)
    # each attachment: {"filename": str, "data": bytes, "mime_type": str}


class GmailClient:
    def __init__(
        self,
        token_path: str = "config/gmail_token.json",
        credentials_path: str = "config/gmail_credentials.json",
    ):
        self.token_path = token_path
        self.credentials_path = credentials_path
        self._service = None

    def _get_service(self):
        if self._service:
            return self._service

        creds = None
        if Path(self.token_path).exists():
            creds = Credentials.from_authorized_user_file(self.token_path, SCOPES)

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
                with open(self.token_path, "w") as f:
                    f.write(creds.to_json())
            else:
                raise RuntimeError(
                    f"Gmail token not found or invalid at {self.token_path}. "
                    "Run scripts/setup_gmail_oauth.py first."
                )

        self._service = build("gmail", "v1", credentials=creds)
        return self._service

    def fetch_unread(self, query: str = "is:unread has:attachment") -> list[EmailMessage]:
        service = self._get_service()
        results = service.users().messages().list(userId="me", q=query).execute()
        messages = results.get("messages", [])
        logger.info("Found %d unread messages", len(messages))

        emails = []
        for msg_ref in messages:
            email = self._fetch_message(service, msg_ref["id"])
            if email:
                emails.append(email)
        return emails

    def mark_as_read(self, message_id: str):
        service = self._get_service()
        try:
            service.users().messages().modify(
                userId="me",
                id=message_id,
                body={"removeLabelIds": ["UNREAD"]},
            ).execute()
            logger.debug("Marked message %s as read", message_id)
        except Exception as e:
            logger.warning("Failed to mark message %s as read: %s", message_id, e)

    def _fetch_message(self, service, message_id: str) -> Optional[EmailMessage]:
        try:
            msg = service.users().messages().get(
                userId="me", id=message_id, format="full"
            ).execute()

            headers = {h["name"].lower(): h["value"] for h in msg["payload"].get("headers", [])}
            subject = headers.get("subject", "")
            sender = headers.get("from", "")

            body = self._extract_body(msg["payload"])
            attachments = []
            self._extract_attachments(service, msg["payload"], message_id, attachments)

            return EmailMessage(
                message_id=message_id,
                subject=subject,
                sender=sender,
                body=body,
                attachments=attachments,
            )
        except Exception as e:
            logger.error("Failed to fetch message %s: %s", message_id, e)
            return None

    def _extract_body(self, payload: dict) -> str:
        """Extract plain-text body from a Gmail message payload."""
        # Walk the MIME tree looking for text/plain parts
        def _walk(part: dict) -> str:
            mime = part.get("mimeType", "")
            if mime == "text/plain":
                data = part.get("body", {}).get("data", "")
                if data:
                    return base64.urlsafe_b64decode(data).decode("utf-8", errors="replace")
            for sub in part.get("parts", []):
                result = _walk(sub)
                if result:
                    return result
            return ""

        return _walk(payload).strip()

    def _extract_attachments(self, service, payload: dict, message_id: str, attachments: list):
        if "parts" in payload:
            for part in payload["parts"]:
                self._extract_attachments(service, part, message_id, attachments)
        else:
            mime_type = payload.get("mimeType", "")
            filename = payload.get("filename", "")
            body = payload.get("body", {})

            if filename and body.get("attachmentId"):
                attachment = service.users().messages().attachments().get(
                    userId="me", messageId=message_id, id=body["attachmentId"]
                ).execute()
                data = base64.urlsafe_b64decode(attachment["data"])
                attachments.append({
                    "filename": filename,
                    "data": data,
                    "mime_type": mime_type,
                })
                logger.debug("Fetched attachment: %s (%d bytes)", filename, len(data))
