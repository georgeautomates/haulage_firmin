"""Mark all Unipet emails as read (cleanup — no-attachment emails get stuck unread)."""
from dotenv import load_dotenv
load_dotenv()

from firmin.clients.gmail import GmailClient

g = GmailClient()
svc = g._get_service()

result = svc.users().messages().list(userId="me", q="subject:unipet.co.uk is:unread").execute()
msgs = result.get("messages", [])
print(f"Found {len(msgs)} unread Unipet emails")

for m in msgs:
    svc.users().messages().modify(
        userId="me",
        id=m["id"],
        body={"removeLabelIds": ["UNREAD"]},
    ).execute()
    print(f"Marked read: {m['id']}")

print("Done.")
