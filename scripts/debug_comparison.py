"""Debug: check job number overlap between both sheets."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from dotenv import load_dotenv
load_dotenv()
from firmin.clients.sheets import SheetsClient

SPREADSHEET_ID = "1uEst-r23EiTyfdmL6gx_YQMuzR0s7yyx-qJVLIUyDSI"

sheets = SheetsClient()

actual_ws = sheets._get_worksheet(SPREADSHEET_ID, "Actual Entry")
verify_ws = sheets._get_worksheet(SPREADSHEET_ID, "Verification")

actual_jobs = set(str(r.get("delivery_order_number","")).strip()
                  for r in actual_ws.get_all_records(numericise_ignore=["all"]))
verify_jobs = set(str(r.get("delivery_order_number","")).strip()
                  for r in verify_ws.get_all_records(numericise_ignore=["all"]))

matched = actual_jobs & verify_jobs
only_actual = actual_jobs - verify_jobs
only_verify = verify_jobs - actual_jobs

print(f"Actual jobs:  {len(actual_jobs)}")
print(f"Verify jobs:  {len(verify_jobs)}")
print(f"Matched:      {len(matched)}")
print(f"Only actual:  {len(only_actual)}")
print(f"Only verify:  {len(only_verify)}")
print(f"\nSample matched: {sorted(matched)[:10]}")
print(f"Sample only actual: {sorted(only_actual)[:5]}")
print(f"Sample only verify: {sorted(only_verify)[:5]}")
