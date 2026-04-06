from __future__ import annotations
import io
import os
from pathlib import Path

from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

from firmin.utils.logger import get_logger

logger = get_logger(__name__)

SERVICE_ACCOUNT_SCOPES = ["https://www.googleapis.com/auth/drive.file"]
OAUTH_SCOPES = [
    "https://www.googleapis.com/auth/gmail.modify",
    "https://www.googleapis.com/auth/drive.file",
]


class DriveClient:
    def __init__(self, service_account_path: str | None = None, folder_id: str | None = None):
        self.folder_id = folder_id or os.getenv("DRIVE_FOLDER_ID", "")
        if not self.folder_id:
            raise RuntimeError("DRIVE_FOLDER_ID not set — cannot upload PDFs to Drive")

        # Prefer OAuth user credentials (uploads count against user quota, not service account)
        oauth_token_path = os.getenv("GMAIL_TOKEN_PATH", "config/gmail_token.json")
        if Path(oauth_token_path).exists():
            creds = Credentials.from_authorized_user_file(oauth_token_path, OAUTH_SCOPES)
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            if creds and creds.valid:
                self._service = build("drive", "v3", credentials=creds)
                logger.debug("DriveClient using OAuth user credentials")
                return

        # Fall back to service account
        sa_path = service_account_path or os.getenv(
            "GOOGLE_SERVICE_ACCOUNT_PATH", "config/service_account.json"
        )
        creds = service_account.Credentials.from_service_account_file(
            sa_path, scopes=SERVICE_ACCOUNT_SCOPES
        )
        self._service = build("drive", "v3", credentials=creds)
        logger.debug("DriveClient using service account credentials")

    def upload_pdf(self, pdf_bytes: bytes, filename: str) -> str:
        """Upload a PDF to the configured Drive folder. Returns a direct view URL.

        If a file with the same name already exists in the folder, returns its
        existing URL without re-uploading (idempotent for same message_id).
        """
        existing_url = self._find_existing(filename)
        if existing_url:
            logger.debug("PDF already in Drive: %s", filename)
            return existing_url

        file_metadata = {
            "name": filename,
            "parents": [self.folder_id],
        }
        media = MediaIoBaseUpload(
            io.BytesIO(pdf_bytes),
            mimetype="application/pdf",
            resumable=False,
        )
        file = (
            self._service.files()
            .create(body=file_metadata, media_body=media, fields="id", supportsAllDrives=True)
            .execute()
        )
        file_id = file.get("id")

        # Make the file readable by anyone with the link
        self._service.permissions().create(
            fileId=file_id,
            body={"type": "anyone", "role": "reader"},
            supportsAllDrives=True,
        ).execute()

        url = f"https://drive.google.com/file/d/{file_id}/view"
        logger.info("Uploaded PDF to Drive: %s → %s", filename, url)
        return url

    def _find_existing(self, filename: str) -> str | None:
        """Return the view URL if a file with this name already exists in the folder."""
        query = (
            f"name='{filename}' and '{self.folder_id}' in parents and trashed=false"
        )
        results = (
            self._service.files()
            .list(q=query, fields="files(id)", pageSize=1, supportsAllDrives=True, includeItemsFromAllDrives=True)
            .execute()
        )
        files = results.get("files", [])
        if files:
            file_id = files[0]["id"]
            return f"https://drive.google.com/file/d/{file_id}/view"
        return None
