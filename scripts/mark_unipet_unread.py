"""Mark all Unipet emails as unread so the agent reprocesses them."""
from dotenv import load_dotenv
load_dotenv()

from firmin.clients.gmail import GmailClient

g = GmailClient()
svc = g._get_service()

result = svc.users().messages().list(userId="me", q="subject:unipet.co.uk has:attachment").execute()
msgs = result.get("messages", [])
print(f"Found {len(msgs)} Unipet emails")

for m in msgs:
    svc.users().messages().modify(
        userId="me",
        id=m["id"],
        body={"addLabelIds": ["UNREAD"]},
    ).execute()
    print(f"Marked unread: {m['id']}")

print("Done.")
