# Firmin — New Client Onboarding Guide

**Who this is for:** Anyone adding a new client — Ayon, George, or any team member.
**Goal:** By the end of this guide you will have a new client live — emails arriving, being parsed, written to Google Sheets, verified in Proteo, and visible on the dashboard.

---

## How the system works (read this first)

Firmin runs 24/7 on a server (VPS). Every minute it checks the Gmail inbox for unread emails. When it finds one it recognises, it:

1. Downloads the PDF attachment
2. Extracts the booking data (AI or custom parser)
3. Looks up the collection/delivery point names in the database
4. Writes a row to Google Sheets
5. Verifies the order exists in Proteo TMS
6. Sends a Slack notification

**Adding a new client means teaching the system:**
- Which emails to pick up (subject line filter)
- How to read the PDF (AI extraction or custom parser)
- What default values to fill in (service type, goods type, etc.)
- What location names to use

Almost everything is controlled by a single YAML config file — no code changes needed for standard clients.

---

## Project folder structure

```
firmin/
├── config/
│   └── clients/              ← ONE YAML FILE PER CLIENT — this is where you work
│       ├── st_regis_fibre.yaml
│       └── unipet.yaml
├── firmin/                   ← Python source code (don't touch unless Omi says so)
├── scripts/                  ← Utility scripts (testing, backfill, etc.)
└── docs/                     ← This guide lives here
```

The VPS (server) runs the code from `/opt/firmin`. Changes are deployed by pushing to GitHub and pulling on the server.

---

## Important: Forwarded emails

DS Smith and most clients don't email Alan Firmin directly — the emails are **forwarded** by Alan Firmin's team. This means:

- The **From** field shows an internal forwarder address — ignore it
- The **subject line** contains the original sender's domain (e.g. `@dssmith.com`)
- The subject line filter works on the subject, not the From field

Always check the subject line, not the From address, when setting up the email filter.

---

## Step 1: Gather information before you start

You need these things **before creating any files**. Ask Alan Firmin's team or George.

### 1a. Get sample emails (minimum 3)

- Ask Alan Firmin's team to forward 3 recent booking emails from the client
- Save the PDF attachments separately
- You need to see: what the email subject looks like, what the PDF looks like

### 1b. Fill in this checklist

Print this out or copy it into a doc. Fill every field before moving to Step 2.

```
CLIENT NAME (as it appears in Proteo):     ___________________________
DISPLAY NAME (short label for dashboard):  ___________________________

EMAIL SUBJECT — what unique word/phrase appears in every subject?
  ___________________________
  (e.g. "@dssmith.com", "unipet.co.uk", "revolutionbeauty.com")
  TIP: Open 3 sample emails and compare subjects — what's always there?

PDF TYPE:
  [ ] DS Smith format — has 7-digit job numbers (e.g. 2560920), tabular layout
  [ ] Manifest / delivery list — rows of deliveries, no job numbers (like Unipet)
  [ ] Other (describe): ___________________________

COLLECTION POINT: Is it always the same place?
  [ ] Yes, fixed → exact name: ___________________________
  [ ] No, varies per booking (AI will extract it)

DELIVERY POINT: Fixed or varies?
  [ ] Fixed → exact name: ___________________________
  [ ] Varies (AI will extract it)

PROTEO DEFAULTS — confirm these with George before filling in:
  Business type:  ___________________________
                  (e.g. "General | Part/Full Load", "Artic Reloads")
  Service:        ___________________________
                  (e.g. "Palletised", "Baled Waste/Recycling", "Full Load")
  Goods type:     ___________________________
                  (e.g. "Palletised", "Baled Waste")
  Pallets:        ___________________________  (number, or blank if varies)
  Weight:         ___________________________  (usually 0)
```

### 1c. Read the PDF carefully

Open one sample PDF and answer these before writing the YAML:

- Are there **job numbers**? What do they look like? (e.g. `2560920` — 7 digits starting with 25 or 26)
- Does the **collection address** change per booking, or is it always the same depot?
- Are **delivery addresses** structured with a postcode on the last line?
- Is there a **price** on the PDF, or is pricing handled separately?
- Are there **PO numbers or order references**? What format? (e.g. `PO-0804230`)

---

## Step 2: Create the YAML profile

Create a new file: `config/clients/your_client_id.yaml`

The `client_id` must be lowercase, no spaces (use underscores). Example: `revolution_beauty`.

Use the right template below:

---

### Template A: Standard client (AI reads the PDF, job numbers present)

Use this when the PDF has 7-digit job numbers and a structured layout.

```yaml
client_id: your_client_id          # lowercase_underscores, unique
display_name: "Your Client Name"   # shown in dashboard and Slack

# Email matching — the system checks every unread email against this.
# ALL conditions must pass for this profile to be used.
email_filters:
  subject_contains:
    - "keyword_in_every_subject"   # e.g. "@dssmith.com"
  has_attachment: true
  attachment_type: "pdf"

# Job number pattern — the regex used to find job numbers in the PDF text.
# This default matches DS Smith format (25XXXXX or 26XXXXX).
# Change only if the client uses a different format.
job_number_patterns:
  - "\\b2[56]\\d{5}\\b"

# Hardcoded defaults — written to every row, never extracted from PDF.
# Confirm business_type, service, goods_type with George before going live.
defaults:
  client_name: "Your Client Name"
  business_type: "Artic Reloads"
  service: "Baled Waste/Recycling"
  pallets: 26
  spaces: 26
  weight: 0
  goods_type: "Baled Waste"

# Google Sheets — same spreadsheet and tab for all clients, don't change these.
sheets:
  spreadsheet_id: "1uEst-r23EiTyfdmL6gx_YQMuzR0s7yyx-qJVLIUyDSI"
  worksheet_name: "Actual Entry"

# Known location overrides.
# If a postcode always maps to one specific Proteo point name, add it here.
# This skips the fuzzy database lookup entirely — faster and more accurate.
# Format: "POSTCODE": "Exact name from Proteo"
# Leave as {} if you don't need any overrides yet.
known_locations: {}

# Conditional overrides — for postcodes shared by two different sites.
# See Step 5 for when and how to use this.
conditional_locations: {}

confidence_thresholds:
  green: 80
  yellow: 50    # below 50 = RED
```

---

### Template B: Manifest client (structured delivery list, no job numbers)

Use this when the PDF is a list of deliveries like Unipet — no 7-digit job numbers.

```yaml
client_id: your_client_id
display_name: "Your Client Name"

email_filters:
  subject_contains:
    - "keyword_in_every_subject"
  has_attachment: true
  attachment_type: "pdf"

# This tells the system to use a custom parser instead of AI.
# The parser must be written in Python — ask Omi.
parser: "your_manifest_parser_name"

defaults:
  client_name: "Your Client Name"
  business_type: "General | Part/Full Load"
  service: "Palletised"
  goods_type: "Palletised"
  collection_point: "Fixed Collection Name - Town"   # hardcode if always the same

sheets:
  spreadsheet_id: "1uEst-r23EiTyfdmL6gx_YQMuzR0s7yyx-qJVLIUyDSI"
  worksheet_name: "Actual Entry"

known_locations: {}

confidence_thresholds:
  green: 80
  yellow: 50
```

> **Note:** Template B also needs a Python parser written by Omi. Don't use this template without talking to Omi first.

---

### Things NOT to touch in the YAML

- `sheets.spreadsheet_id` and `sheets.worksheet_name` — same for all clients, never change
- The `rate` and ` goods_type` column mappings are handled automatically by the pipeline — don't add them to `defaults`
- Don't add `delivery_order_number` or `processed_at` to defaults — these are set automatically

---

## Step 3: Verify the profile loads correctly

Before anything else, confirm the system can read your new YAML without errors.

**SSH to the VPS:**
```bash
ssh root@72.61.202.184
cd /opt/firmin
git pull   # get your latest changes
```

**Run this check:**
```bash
.venv/bin/python -c "
from firmin.profiles.loader import load_all_profiles
profiles = load_all_profiles()
for p in profiles:
    print(p.client_id, '->', p.email_filters.subject_contains)
"
```

You should see your new client in the list. If it's missing, there's a YAML syntax error — most commonly wrong indentation or missing quotes around a value.

---

## Step 4: Test PDF extraction

Test that the AI can read one of the sample PDFs correctly before going live.

```bash
# Copy your sample PDF to the VPS first, then:
.venv/bin/python scripts/test_pdf_pipeline.py /path/to/sample.pdf
```

Check the output for each extracted job:
- Job numbers found? (correct count?)
- Collection org and postcode — do they look right?
- Delivery org and postcode — correct?
- Price — right format?
- Order/PO number — present?

If something looks wrong, it's usually a location issue (Step 5) not an AI issue. The AI is reliable — location matching is where most problems happen.

---

## Step 5: Fix location mismatches

The most common problem when adding a new client. The system looks up collection and delivery point names in a database (Supabase) by postcode. If the postcode isn't there, or maps to the wrong name, you'll see `UNMATCHED` in the sheet.

### How to find the correct Proteo point name

1. Log into Proteo
2. Search for a recent completed job from this client
3. Open the job and look at the **Collection Point** or **Delivery Point** field
4. Copy the name exactly — spelling and capitalisation matter

### Option A: Known locations (one postcode = one place, always)

Add to your YAML:

```yaml
known_locations:
  "DE11 0BB": "GXO - Swadlincote"    # exact name from Proteo
  "AB12 3CD": "Another Site - Town"
```

Use this for fixed collection points and any delivery locations that always map to the same Proteo name.

### Option B: Conditional locations (one postcode, two different places)

Some postcodes are shared by multiple sites. Use this when the correct name depends on what's written in the PDF:

```yaml
conditional_locations:
  "ME10 2TD":
    - keyword: "KEMSLEY DEPOT"    # if this appears in the org name → use this result
      result: "Kemsley Depot (DSSR) - Sittingbourne"
    - keyword: ""                 # empty string = fallback, catches everything else
      result: "DS SMITH - SITTINGBOURNE"
```

The rules are checked in order — first match wins.

### What if the postcode isn't in Supabase at all?

Tell Omi — the location may need to be added to the database, or a `known_locations` override added. Don't try to edit Supabase directly.

---

## Step 6: Understanding confidence scores (GREEN / YELLOW / RED)

Every processed order gets a confidence score (0–100) and a status:

| Status | Score | Meaning | Action needed? |
|--------|-------|---------|----------------|
| GREEN | 80–100 | All key fields extracted cleanly | None — all good |
| YELLOW | 50–79 | Some fields missing or uncertain | Review in dashboard, may need location fix |
| RED | 0–49 | Significant extraction failure | Investigate — check the PDF, check location matching |

**You'll see these in Slack notifications.** YELLOW and RED jobs show the reason inline (e.g. "collection point UNMATCHED", "price missing").

**When a new client first goes live, expect some YELLOWs** — usually location overrides haven't been added yet. This is normal. Add the missing `known_locations` entries and reprocess.

**RED almost always means one of:**
- Collection point UNMATCHED (postcode not in database)
- PDF format is different from expected (tell Omi)
- Email was processed with wrong client profile (subject filter too broad)

---

## Step 7: Add comparison aliases (if needed)

The comparison script checks our extraction against Proteo. Sometimes the same place has a slightly different name in each system. Aliases tell the comparison "these two names mean the same thing."

You only need this step if the dashboard is showing MISMATCH on collection or delivery point even though they're clearly the same place.

**Add to `scripts/run_comparison.py`** in the `normalise()` function:

```python
# In delivery_aliases dict — if it's a delivery point:
"name as it appears in our sheet":  "shared canonical name",
"name as it appears in proteo":     "shared canonical name",

# In collection_aliases dict — if it's a collection point:
"our version":    "shared canonical name",
"proteo version": "shared canonical name",
```

**Then mirror the same entries in `lib/aliases.ts`** (dashboard repo) so the Alias Mappings tab stays in sync. Both files must always match.

---

## Step 8: Deploy and go live

### On your machine:

```bash
git add config/clients/your_client.yaml
git commit -m "Add YOUR CLIENT NAME client profile"
git push
```

### On the VPS:

```bash
ssh root@72.61.202.184
cd /opt/firmin
git pull
systemctl restart firmin

# Watch the logs to confirm it started cleanly:
journalctl -u firmin -f
```

You should see a line like:
```
Loaded client profile: Your Client Name
```

If you see an error, check the YAML syntax.

### Trigger a test

Forward one of the sample emails to the Gmail inbox (`george.automates.ai@gmail.com`). Within 1–2 minutes the system will pick it up.

Watch the logs:
```bash
journalctl -u firmin -f
```

---

## Step 9: Verify everything worked

After a test email is processed, check all four places:

1. **Google Sheets → Actual Entry tab** — new row at the bottom? Check collection point, delivery point, price, order number look correct
2. **Dashboard** — does the order appear? What's the match status?
3. **Slack** — was a batch notification sent? GREEN/YELLOW/RED?
4. **Proteo → Verification tab in the sheet** — did the verification run? Does it match?

If the row appears but some fields are wrong → go back to Step 5 (location overrides).
If no row appears → check the logs for errors.

---

## About the dedup system

The system remembers every job it has processed (in a local database). If the same email arrives twice, or you forward a test email more than once, the second run will be silently skipped — this is intentional.

**To force reprocess a job** (e.g. during testing):
```bash
ssh root@72.61.202.184
sqlite3 /opt/firmin/firmin.db "DELETE FROM processed_orders WHERE order_id = '2560920';"
```

Replace `2560920` with the actual job number.

**To wipe all history and reprocess everything** (only for major testing — use with caution):
```bash
sqlite3 /opt/firmin/firmin.db "DELETE FROM processed_emails; DELETE FROM processed_orders;"
```

---

## Common issues and fixes

| Problem | Likely cause | Fix |
|---------|-------------|-----|
| No emails picked up | Subject filter doesn't match | Check `subject_contains` against real subject line |
| No job numbers found | Wrong regex pattern | Update `job_number_patterns` in YAML |
| Collection point = UNMATCHED | Postcode not in Supabase | Add to `known_locations` |
| Wrong delivery point name | Postcode matches wrong site | Use `conditional_locations` |
| Same email processed twice | Shouldn't happen — dedup prevents it | If it does, tell Omi |
| Email stays unread forever | No profile matched it | Check YAML loaded (Step 3), check subject filter |
| Dashboard shows MISMATCH on location | Name variant not aliased | Add alias to `run_comparison.py` + `lib/aliases.ts` |
| RED confidence score | UNMATCHED location or missing price | Check logs for `failure_reasons` |
| YAML won't load | Syntax error | Check indentation (2 spaces), check quotes around values with special characters |

---

## Checklist: Ready to go live?

- [ ] 3+ sample emails and PDFs obtained
- [ ] Information checklist (Step 1b) fully filled in
- [ ] Proteo defaults confirmed with George
- [ ] PDF read and understood — job numbers, collection/delivery structure, price, order ref
- [ ] YAML file created in `config/clients/`
- [ ] Profile loads correctly on VPS (Step 3 check passes)
- [ ] PDF extraction tested — key fields look correct
- [ ] Location overrides added for known postcodes
- [ ] Deployed to VPS (`git push` → `git pull` → `systemctl restart firmin`)
- [ ] Test email forwarded and processed
- [ ] Row appears in Google Sheets ✓
- [ ] Confidence status is GREEN or expected YELLOW ✓
- [ ] Row appears on dashboard ✓
- [ ] Slack notification received ✓
- [ ] Comparison aliases added if needed ✓
