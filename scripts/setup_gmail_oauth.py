"""
One-time Gmail OAuth2 setup script.

Run this once to generate config/gmail_token.json.
After that, the agent uses the token automatically (auto-refreshes).

Usage:
    python scripts/setup_gmail_oauth.py

Prerequisites:
    1. Go to https://console.cloud.google.com/
    2. Create a project → Enable the Gmail API
    3. Create OAuth 2.0 credentials (Desktop app type)
    4. Download the credentials JSON and save as config/gmail_credentials.json
    5. Run this script — it will open a browser for you to authorise
"""

import os
import sys
from pathlib import Path

# Allow running from project root
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
import json

SCOPES = [
    "https://www.googleapis.com/auth/gmail.modify",
    "https://www.googleapis.com/auth/drive.file",
]

CREDENTIALS_PATH = os.getenv("GMAIL_CREDENTIALS_PATH", "config/gmail_credentials.json")
TOKEN_PATH = os.getenv("GMAIL_TOKEN_PATH", "config/gmail_token.json")


def main():
    creds = None

    if Path(TOKEN_PATH).exists():
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            print("Refreshing existing token...")
            creds.refresh(Request())
        else:
            if not Path(CREDENTIALS_PATH).exists():
                print(f"ERROR: Credentials file not found at {CREDENTIALS_PATH}")
                print("Download it from Google Cloud Console → APIs & Services → Credentials")
                sys.exit(1)

            print("Opening browser for Gmail authorisation...")
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_PATH, SCOPES)
            creds = flow.run_local_server(port=0, open_browser=False)

        Path(TOKEN_PATH).parent.mkdir(parents=True, exist_ok=True)
        with open(TOKEN_PATH, "w") as f:
            f.write(creds.to_json())

        print(f"Token saved to {TOKEN_PATH}")
        print("Gmail OAuth setup complete. You can now run the agent.")
    else:
        print("Token already valid. No action needed.")


if __name__ == "__main__":
    main()
