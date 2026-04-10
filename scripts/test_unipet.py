"""
Test Unipet manifest parsing and location lookup against real emails.

Usage:
    cd E:/Arc Ai/firmin
    python scripts/test_unipet.py
"""
from __future__ import annotations
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv()

from firmin.clients.gmail import GmailClient
from firmin.clients.pdf import extract_pdf
from firmin.clients.unipet_pdf import parse_unipet_manifest
from firmin.clients.supabase import SupabaseClient
from firmin.profiles.loader import load_all_profiles

profiles = load_all_profiles(os.getenv("CLIENTS_DIR", "config/clients"))
profile = next((p for p in profiles if p.client_id == "unipet"), None)
if not profile:
    print("ERROR: unipet profile not found in config/clients/")
    sys.exit(1)

gmail = GmailClient(
    token_path=os.getenv("GMAIL_TOKEN_PATH", "config/gmail_token.json"),
    credentials_path=os.getenv("GMAIL_CREDENTIALS_PATH", "config/gmail_credentials.json"),
)
service = gmail._get_service()

# Fetch the most recent Unipet email
resp = service.users().messages().list(userId="me", q="unipet", maxResults=1).execute()
if not resp.get("messages"):
    print("No Unipet emails found.")
    sys.exit(0)

email = gmail._fetch_message(service, resp["messages"][0]["id"])
print(f"Email: {email.subject}")
print(f"Attachment: {email.attachments[0]['filename']}")
print()

raw_text = extract_pdf(email.attachments[0]["data"]).raw_text
result = parse_unipet_manifest(raw_text)

print(f"Collection date: {result.collection_date}")
print(f"Rows parsed: {len(result.rows)}")
print()

supabase = SupabaseClient()

print(f"{'Delivery Note':<15} {'Customer':<35} {'Postcode':<10} {'Pallets':<8} {'Delivery':<22} {'Location Match'}")
print("-" * 120)
for row in result.rows:
    point = supabase.lookup_location(
        postcode=row.postcode,
        org_name=row.customer_name,
        search=row.customer_name,
        known_locations=profile.known_locations,
        conditional_locations=profile.conditional_locations,
        client_name=profile.defaults.get("client_name", ""),
        pdf_address=row.customer_name,
    )
    match = point or "UNMATCHED"
    print(f"{row.delivery_note:<15} {row.customer_name:<35} {row.postcode:<10} {row.pallets:<8} {row.delivery_date + ' ' + row.delivery_time:<22} {match}")
