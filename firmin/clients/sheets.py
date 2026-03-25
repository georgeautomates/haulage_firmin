from __future__ import annotations
import os
from datetime import datetime
from typing import Any

import gspread
from google.oauth2.service_account import Credentials

from firmin.utils.logger import get_logger

logger = get_logger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


class SheetsClient:
    def __init__(self):
        sa_path = os.getenv("GOOGLE_SERVICE_ACCOUNT_PATH", "config/service_account.json")
        creds = Credentials.from_service_account_file(sa_path, scopes=SCOPES)
        self._gc = gspread.authorize(creds)
        self._worksheets: dict[str, gspread.Worksheet] = {}

    def _get_worksheet(self, spreadsheet_id: str, worksheet_name: str) -> gspread.Worksheet:
        key = f"{spreadsheet_id}:{worksheet_name}"
        if key not in self._worksheets:
            sh = self._gc.open_by_key(spreadsheet_id)
            self._worksheets[key] = sh.worksheet(worksheet_name)
        return self._worksheets[key]

    def append_row(self, spreadsheet_id: str, worksheet_name: str, row: dict) -> None:
        ws = self._get_worksheet(spreadsheet_id, worksheet_name)

        # Get headers from row 1 to ensure correct column ordering
        headers = ws.row_values(1)
        values = [str(row.get(h, "")) for h in headers]

        ws.append_row(values, value_input_option="USER_ENTERED")
        logger.info(
            "Appended row for job %s to %s / %s",
            row.get("delivery_order_number", "?"),
            spreadsheet_id,
            worksheet_name,
        )
