# Firmin — New Client Onboarding Guide

**Who this is for:** Ayon (and any team member) adding a new client to the Firmin automated booking pipeline.  
**Goal:** By the end of this guide you will have a new client live — emails arriving, being parsed, written to Google Sheets, verified in Proteo, and visible on the dashboard.

---

## Overview: What "adding a client" means

Firmin is an email → PDF → Google Sheets pipeline. For each client it needs to know:

1. **Which emails to pick up** (subject line filter)
2. **How to read the PDF** (DS Smith-style AI extraction, or a custom manifest parser)
3. **What default values to fill in** (client name, service type, goods type, etc.)
4. **What location names to use** (Supabase lookup + overrides)
5. **How the comparison script treats name variants** (aliases)

Everything is configured through a single YAML file in `config/clients/`. No code changes are needed for most clients — only the YAML file.

---

## Step 1: Gather information before you start

You need these things **before writing any code**. Ask George or get them from a sample email/PDF.

### 1a. Get sample emails (minimum 3)

- Ask Alan Firmin's team to forward 3 recent booking emails from the client
- Save the PDF attachments too
- You need to see: what the email subject looks like, what the PDF looks like

### 1b. Fill in this information checklist

```
CLIENT NAME (as it appears in Proteo):     ___________________________
DISPLAY NAME (short label for dashboard):  ___________________________

EMAIL SUBJECT contains (unique keyword):   ___________________________
  (Look at the subject line — what's unique? Usually an email domain or company name)
  (e.g. "@dssmith.com", "unipet.co.uk", "revolutionbeauty.com")

PDF TYPE:
  [ ] DS Smith format (job numbers like 2560920, tabular layout)
  [ ] Manifest / delivery list (rows of orders, no job numbers)
  [ ] Other (describe): ___________________________

COLLECTION POINT: Is it always fixed?
  [ ] Yes, always the same place → write it here: ___________________________
  [ ] No, varies per booking → will be extracted from PDF

DELIVERY POINT: Fixed or varies?
  [ ] Fixed: ___________________________
  [ ] Varies (AI will extract)

GOOGLE SHEET:
  Spreadsheet ID:   1uEst-r23EiTyfdmL6gx_YQMuzR0s7yyx-qJVLIUyDSI  (same for all clients)
  Worksheet tab:    Actual Entry  (same for all clients)

PROTEO DEFAULTS (ask George/Alan Firmin):
  Business type:    ___________________________  (e.g. "General | Part/Full Load", "Artic Reloads")
  Service:          ___________________________  (e.g. "Palletised", "Baled Waste/Recycling")
  Goods type:       ___________________________  (e.g. "Palletised", "Baled Waste")
  Pallets:          ___________________________  (number, or leave blank if varies)
  Weight:           ___________________________  (usually 0)
```

### 1c. Read the PDF carefully

Open one of the sample PDFs and answer these:

- Are there **job numbers** (7-digit codes)? What format? (e.g. `25XXXXX` or `26XXXXX`)
- Is the collection address always the same site? Or does it change per booking?
- Are delivery addresses structured (postcode on last line)? Or free-form?
- Is there a **price** visible? Or is pricing handled separately?
- Are there **PO numbers / order references**? What format?

---

## Step 2: Create the YAML profile

Create a new file: `config/clients/YOUR_CLIENT_ID.yaml`

Use one of these templates:

---

### Template A: DS Smith-style client (AI extraction, job numbers in PDF)

Use this when: the PDF has 7-digit job numbers and a tabular layout.

```yaml
client_id: your_client_id          # snake_case, no spaces, unique
display_name: "Your Client Name"   # shown in dashboard and Slack

# Email matching — ALL conditions must pass
email_filters:
  subject_contains:
    - "keyword_that_appears_in_every_subject"   # e.g. "@dssmith.com"
  has_attachment: true
  attachment_type: "pdf"

# Job number regex — adjust if format differs from DS Smith
job_number_patterns:
  - "\\b2[56]\\d{5}\\b"   # matches 25XXXXX and 26XXXXX

# Hardcoded values — filled in for every order, never extracted from PDF
defaults:
  client_name: "Your Client Name"
  business_type: "Artic Reloads"        # confirm with George
  service: "Baled Waste/Recycling"      # confirm with George
  pallets: 26                           # confirm with George
  spaces: 26                            # usually same as pallets
  weight: 0
  goods_type: "Baled Waste"             # confirm with George

# Google Sheets output
sheets:
  spreadsheet_id: "1uEst-r23EiTyfdmL6gx_YQMuzR0s7yyx-qJVLIUyDSI"
  worksheet_name: "Actual Entry"

# Known location overrides — if a postcode always maps to one point name,
# add it here to skip fuzzy matching entirely.
# Format: "POSTCODE": "Exact Description from Supabase Location Points"
known_locations: {}

# Conditional overrides — same postcode, different point name depending on org name in PDF
# Example: ME10 2TD can be Kemsley Mill OR Kemsley Depot depending on who's there
conditional_locations: {}

confidence_thresholds:
  green: 80
  yellow: 50    # below 50 = RED
```

---

### Template B: Manifest / delivery list client (no AI, fixed fields)

Use this when: the PDF is a structured list (like Unipet) — no job numbers, rows of deliveries.

```yaml
client_id: your_client_id
display_name: "Your Client Name"

email_filters:
  subject_contains:
    - "keyword_in_every_email_subject"
  has_attachment: true
  attachment_type: "pdf"

# Tell the pipeline to use a custom manifest parser instead of AI
parser: "your_manifest_parser_name"

defaults:
  client_name: "Your Client Name"
  business_type: "General | Part/Full Load"
  service: "Palletised"
  goods_type: "Palletised"
  collection_point: "Your Client Name - Town"   # hardcoded if fixed

sheets:
  spreadsheet_id: "1uEst-r23EiTyfdmL6gx_YQMuzR0s7yyx-qJVLIUyDSI"
  worksheet_name: "Actual Entry"

known_locations: {}

confidence_thresholds:
  green: 80
  yellow: 50
```

> **Note:** Template B also requires writing a Python manifest parser (like `firmin/clients/unipet_pdf.py`).  
> This is a more complex case — see Omi for help with the parser.

---

## Step 3: Test the email filter

Before running the full pipeline, verify the email filter works.

Open `agent.py` and look at the Gmail query it builds. With your new YAML in place:

```bash
# On the VPS:
cd /opt/firmin
.venv/bin/python -c "
from firmin.profiles.loader import load_all_profiles
profiles = load_all_profiles()
for p in profiles:
    print(p.client_id, '->', p.email_filters.subject_contains)
"
```

You should see your new client listed. If not, check the YAML syntax (indentation, quotes).

---

## Step 4: Test PDF extraction on a sample

Before sending live to production, test on a sample PDF.

```bash
# On the VPS (or locally):
.venv/bin/python scripts/test_pdf_pipeline.py path/to/sample.pdf
```

This will:
- Extract raw text from the PDF
- Find job numbers (regex)
- Run AI extraction on the first job
- Print the extracted fields

Check the output:
- Are job numbers found correctly?
- Are collection/delivery addresses extracted correctly?
- Is the price format right?

If extraction looks wrong, you may need to adjust the AI prompt or add `known_locations` overrides.

---

## Step 5: Handle location mismatches

The most common issue is location names not matching Supabase. Two ways to fix:

### Option A: Known locations (exact postcode → exact Description)

If a postcode always maps to one Proteo point name:

```yaml
known_locations:
  "AB12 3CD": "Exact Name From Proteo - Town"
```

**How to find the exact Proteo name:** Log into Proteo, search for a recent job from that client, look at the Collection/Delivery Point field. Copy it exactly (case-sensitive in Supabase).

### Option B: Conditional locations (postcode shared by multiple sites)

If two different places share a postcode:

```yaml
conditional_locations:
  "ME10 2TD":
    - keyword: "KEMSLEY DEPOT"      # if org name contains this
      result: "Kemsley Depot (DSSR) - Sittingbourne"
    - keyword: ""                   # fallback — empty string = catch all
      result: "DS SMITH - SITTINGBOURNE"
```

---

## Step 6: Add comparison aliases (if needed)

If the client's delivery or collection point names appear differently in our extraction vs Proteo, add aliases to `scripts/run_comparison.py`.

Open the `normalise()` function and add to the appropriate dict:

```python
# In delivery_aliases dict:
"our extracted name - town":  "canonical name",
"proteo name variant":        "canonical name",

# In collection_aliases dict:
"our extracted name - town":  "canonical name",
```

Then mirror the same entries in `lib/aliases.ts` in the dashboard so the Alias Mappings tab stays in sync.

---

## Step 7: Deploy and test

### Local test (recommended first):

```bash
# Force-process a specific test email
.venv/bin/python scripts/test_e2e.py
```

### Deploy to VPS:

```bash
# On your machine:
git add config/clients/your_client.yaml
git commit -m "Add YOUR_CLIENT_NAME client profile"
git push

# On the VPS:
cd /opt/firmin
git pull
systemctl restart firmin

# Watch the logs:
journalctl -u firmin -f
```

Send a test email (or forward an old one to the Gmail inbox) and watch the logs.

---

## Step 8: Verify the output

After a test email is processed:

1. **Check Google Sheets** → Actual Entry tab: is a new row there?
2. **Check the dashboard** → does the order appear?
3. **Check Slack** → was a batch notification sent?
4. **Check Proteo** → does the Verification tab populate correctly?

---

## Common issues and fixes

| Problem | Likely cause | Fix |
|---------|-------------|-----|
| No emails picked up | Subject filter doesn't match | Check `subject_contains` — must appear in the real subject |
| No job numbers found | Wrong regex pattern | Update `job_number_patterns` in YAML |
| Collection point = UNMATCHED | Postcode not in Supabase | Add to `known_locations` |
| Wrong delivery point | Postcode matches wrong site | Use `conditional_locations` |
| Comparison shows MISMATCH on location | Name variant not aliased | Add to `normalise()` + `lib/aliases.ts` |
| Email stays unread | No client profile matched | Check YAML loaded correctly (see Step 3) |
| Sheet write fails | Column name mismatch | Check `sheets.worksheet_name` is exact match |

---

## Checklist: Ready to go live?

- [ ] Sample emails obtained (minimum 3)
- [ ] PDF read and understood — collection/delivery/price/order number fields identified
- [ ] Defaults confirmed with George (business_type, service, goods_type)
- [ ] YAML file created in `config/clients/`
- [ ] Email filter tested — profile loads correctly
- [ ] PDF extraction tested — key fields extracting correctly
- [ ] Location overrides added for any known postcodes
- [ ] Comparison aliases added if name variants differ between our output and Proteo
- [ ] Deployed to VPS (`git push` → `git pull` → `systemctl restart firmin`)
- [ ] Test email processed successfully
- [ ] Row appears in Google Sheets ✓
- [ ] Row appears on dashboard ✓
- [ ] Slack notification received ✓
