"""
Analyse Reels vs Fibre A/C jobs by joining Actual Entry with Verification.
Shows patterns in collection_point, delivery_point, work_type etc.
"""
from dotenv import load_dotenv
load_dotenv()

import os
import gspread
from google.oauth2.service_account import Credentials
from collections import defaultdict

SPREADSHEET_ID = "1uEst-r23EiTyfdmL6gx_YQMuzR0s7yyx-qJVLIUyDSI"
SERVICE_ACCOUNT_PATH = os.getenv("GOOGLE_SERVICE_ACCOUNT_PATH", "config/service_account.json")

creds = Credentials.from_service_account_file(
    SERVICE_ACCOUNT_PATH,
    scopes=["https://www.googleapis.com/auth/spreadsheets"],
)
gc = gspread.authorize(creds)
ss = gc.open_by_key(SPREADSHEET_ID)

# Load Actual Entry
ae = ss.worksheet("Actual Entry")
ae_rows = ae.get_all_records()
print(f"Actual Entry: {len(ae_rows)} rows")

# Load Verification
vf = ss.worksheet("Verification")
vf_rows = vf.get_all_records()
print(f"Verification: {len(vf_rows)} rows")

# Debug: show column names
print("Verification columns:", list(vf_rows[0].keys()) if vf_rows else "empty")
print("Actual Entry columns:", list(ae_rows[0].keys()) if ae_rows else "empty")
print("Sample Verification row:", {k: v for k, v in list(vf_rows[0].items())[:6]} if vf_rows else "")

# Build verification lookup by job number
vf_by_job = {}
for row in vf_rows:
    jn = str(row.get("job_number", "") or row.get("Job Number", "")).strip()
    if jn:
        vf_by_job[jn] = row

print(f"Verification jobs indexed: {len(vf_by_job)}, sample keys: {list(vf_by_job.keys())[:5]}")

# Join and classify
reels_jobs = []
fibre_jobs = []
unmatched = []

for row in ae_rows:
    jn = str(row.get("job_number", "") or row.get("delivery_order_number", "")).strip()
    client = str(row.get("client_name", "")).strip()
    if "unipet" in client.lower():
        continue  # skip Unipet

    vf = vf_by_job.get(jn)
    if not vf:
        unmatched.append(row)
        continue

    vf_client = str(vf.get("client_name", "") or vf.get("Client Name", "")).strip().lower()
    if "reel" in vf_client:
        reels_jobs.append((row, vf))
    else:
        fibre_jobs.append((row, vf))

print(f"\nMatched: {len(reels_jobs)} Reels, {len(fibre_jobs)} Fibre, {len(unmatched)} unmatched")

# Analyse patterns in Reels jobs
print("\n" + "="*60)
print(f"REELS JOBS ({len(reels_jobs)} total) — pattern analysis")
print("="*60)

reels_collection = defaultdict(int)
reels_delivery = defaultdict(int)
reels_worktype = defaultdict(int)

for row, vf in reels_jobs:
    reels_collection[row.get("collection_point", "").strip()] += 1
    reels_delivery[row.get("delivery_point", "").strip()] += 1
    reels_worktype[row.get("work_type", "").strip()] += 1

print("\nTop collection points:")
for k, v in sorted(reels_collection.items(), key=lambda x: -x[1])[:10]:
    print(f"  {v:3d}x  {k}")

print("\nTop delivery points:")
for k, v in sorted(reels_delivery.items(), key=lambda x: -x[1])[:10]:
    print(f"  {v:3d}x  {k}")

print("\nWork types:")
for k, v in sorted(reels_worktype.items(), key=lambda x: -x[1]):
    print(f"  {v:3d}x  {k or '(blank)'}")

# Analyse patterns in Fibre jobs
print("\n" + "="*60)
print(f"FIBRE JOBS ({len(fibre_jobs)} total) — pattern analysis")
print("="*60)

fibre_collection = defaultdict(int)
fibre_delivery = defaultdict(int)
fibre_worktype = defaultdict(int)

for row, vf in fibre_jobs:
    fibre_collection[row.get("collection_point", "").strip()] += 1
    fibre_delivery[row.get("delivery_point", "").strip()] += 1
    fibre_worktype[row.get("work_type", "").strip()] += 1

print("\nTop collection points:")
for k, v in sorted(fibre_collection.items(), key=lambda x: -x[1])[:10]:
    print(f"  {v:3d}x  {k}")

print("\nTop delivery points:")
for k, v in sorted(fibre_delivery.items(), key=lambda x: -x[1])[:10]:
    print(f"  {v:3d}x  {k}")

print("\nWork types:")
for k, v in sorted(fibre_worktype.items(), key=lambda x: -x[1]):
    print(f"  {v:3d}x  {k or '(blank)'}")

# Show overlap — collection/delivery combos that appear in BOTH
print("\n" + "="*60)
print("OVERLAP CHECK — collection points appearing in both")
print("="*60)
reels_c = set(reels_collection.keys())
fibre_c = set(fibre_collection.keys())
overlap = reels_c & fibre_c
if overlap:
    for k in sorted(overlap):
        print(f"  R:{reels_collection[k]}  F:{fibre_collection[k]}  {k}")
else:
    print("  No overlap — collection points are fully distinct!")
