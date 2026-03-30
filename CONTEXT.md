# Firmin — Session Context
_Last updated: 2026-03-30 (session 4 — end of day)_

## What this project is

Python agent for Alan Firmin Ltd (haulage). Polls Gmail for DS Smith booking form emails (PDF attachments), extracts structured order data per job using AI, looks up location point names from Supabase, scores confidence, and writes rows to Google Sheets. Runs in shadow mode alongside the existing n8n workflow for validation.

This is a **multi-client system** — built to scale to other clients who send orders as images, Excel files, or plain email body text. DS Smith (St Regis Fibre A/C) is the first and only client implemented so far.

Reference n8n workflow: `C:\Users\USERAS\Downloads\St Regis Fiber - Shadow Mode.json`

---

## Architecture

```
firmin/
├── agent.py              — main poll loop (entry point: python -m firmin.agent)
│                           marks emails as read after processing (gmail.modify scope)
├── pipeline.py           — per-email orchestrator
├── scoring.py            — confidence scoring (GREEN/YELLOW/RED)
├── clients/
│   ├── gmail.py          — Gmail OAuth2 polling
│   ├── pdf.py            — PDF text extraction (pdfplumber + PyMuPDF fallback)
│   ├── ai.py             — AI extraction per job (OpenAI direct)
│   ├── supabase.py       — 3-tier location lookup (override → cache → fuzzy)
│   ├── sheets.py         — Google Sheets writer (gspread)
│   └── slack.py          — Slack webhook client (batch summary + comparison report)
├── profiles/
│   └── loader.py         — YAML client profile loader + email matcher
└── utils/
    ├── dedup.py           — SQLite dedup (email + job number)
    └── logger.py          — logging utility

config/
├── settings.yaml
└── clients/
    └── st_regis_fibre.yaml  — St Regis Fibre A/C client profile

scripts/
├── setup_gmail_oauth.py        — one-time Gmail OAuth setup (scope: gmail.modify)
├── test_pdf_pipeline.py        — smoke test: PDF + AI extraction
├── test_supabase.py            — smoke test: Supabase location lookup
├── test_e2e.py                 — end-to-end test: PDF → pipeline → Sheets (single job)
├── run_comparison.py           — comparison: Actual Entry vs Verification tab (interactive)
├── slack_comparison_report.py  — posts comparison report to Slack (on-demand)
└── debug_comparison.py         — debug: prints job number overlap between sheets

deploy/
├── firmin.service   — systemd service unit file
└── setup_vps.sh     — one-time VPS setup script (clone, venv, service install)
```

---

## Current status — PRODUCTION RUNNING ON VPS

The agent is deployed on Hostinger VPS (`72.61.202.184`, Ubuntu 24.04) and running as a systemd service. It starts on boot and restarts on failure.

### What's been completed this session (2026-03-30, session 4)

#### Dedup race condition fixed (pipeline.py)
- `mark_order_seen` now fires **before** the sheet write, not after
- Previously: crash/retry between write and mark caused duplicate rows
- Sheet write failure now logs "SHEET WRITE FAILED — manual recovery needed"

#### 276 duplicate sheet rows cleaned (scripts/cleanup_duplicate_rows.py)
- One-time script written to remove legacy duplicates from local test runs
- Keeps latest `processed_at` row per job number, deletes all earlier ones
- 519 rows → 243 rows (one per unique job)
- Script throttles at 1.2s per deletion to avoid Google Sheets rate limit

#### Comparison script overhauled (scripts/run_comparison.py)
- **Clears tab before writing** — no more row accumulation across runs
- **Joins on job_number + PO number** — prevents false mismatches when two
  different orders share the same DS Smith job number
- Shows "exact PO match" vs "job-only match" count in summary
- **Name variant normalisation** added for collection and delivery points:
  - Collection: Masons Landfill / Ipswich variants, Enva/Envea, Welton Bibby
  - Delivery: DS Smith Devizes variants, SAICA Newport variants, Welton Bibby

#### Location cache corrections (Supabase location_mappings)
- Fixed 6 wrong cache entries, marked as verified (Tier 2 — bypasses fuzzy):
  - `EN6 4NE` Woodgreen Timber → `Chas Storer - Potters Bar`
  - `NN4 9BX` Morrison → `Swan Valley Site 3 - Northampton`
  - `CH5 2LL` RCP Procurement → `Shotton Mill Site`

#### Scoring guard: collection same as delivery (scoring.py)
- When `collection_point == delivery_point`, status capped at YELLOW
- Failure reason: `"collection same as delivery — possible extraction error"`
- Catches cases where AI extracts delivery postcode for collection (PDF column bleed)

#### Comparison results after all fixes (183 matched jobs, exact PO matches only):
| Field | Match rate |
|-------|-----------|
| collection_point | 93.0% |
| delivery_point | 99.2% |
| price | 97.7% |
| order_number | 97.7% |

### What's been completed this session (2026-03-26, session 3)

#### Slack failure reasons (scoring.py, pipeline.py, slack.py)
- `ScoredOrder` now has `failure_reasons: list[str]` — populated for any signal that fails
- Plain-English reasons: `"collection point unmatched"`, `"missing fields: X"`, `"price out of range"`, `"invalid collection/delivery date"`
- `OrderResult` carries `failure_reasons` through from scoring
- Batch Slack notification: YELLOW/RED job lines now show `⚠️ reason · reason` underneath
- Comparison report: now accepts `mismatch_examples` — shows up to 3 real job-level examples per failing field (ours vs Proteo), only for fields below 90% match rate

#### PDF extractor fix (pdf.py)
- Switched primary extractor from pdfplumber → PyMuPDF
- pdfplumber was mangling multi-column price fields — e.g. `£490.00` → `£4` for job `2562808`
- PyMuPDF extracts the same PDF cleanly with price on its own line
- pdfplumber retained as fallback if PyMuPDF returns < 50 chars

#### AI prompt fixes (ai.py)
- `price`: now explicitly instructed to look for `£` symbol — never use a plain number as price
- `order_number`: now accepts numbers without `PO-` prefix (e.g. `1838735`)
- `work_type`: clarified it appears on the same line as the `£` price (catches codes like `TE3`)
- `customer_ref`: clarified it appears after the order number line

#### Duplicate sheet rows — diagnosed
- Jobs like `2561003` had 3 rows in the sheet but only 1 entry in `firmin.db`
- Root cause: rows were written locally before VPS deployment; VPS started with empty DB and re-processed
- Dedup is working correctly — this is a one-time legacy issue, not a recurring bug
- Cleanup of duplicate rows still pending

#### Staff communication drafted
- Message drafted for DS Smith booking form staff explaining the location matching problem in plain English
- Focuses on Kemsley/Sittingbourne area — multiple sites share ME10 2TD postcode
- Asks staff for examples: "when form says X, Proteo name is Y"
- Boss confirmed: Paul approved speaking to anyone on the team

### What's been completed this session (2026-03-25, session 2)

#### Slack notifications
- `firmin/clients/slack.py` — webhook client using `urllib` (no extra dependencies)
- Batch summary posted after each email: total jobs, GREEN/YELLOW/RED breakdown, per-job line with collection → delivery, price, score
- Comparison report posted on demand via `scripts/slack_comparison_report.py`
- `SLACK_WEBHOOK_URL` added to `.env` and `.env.example`
- Webhook: `https://hooks.slack.com/services/T0ADXEXQP8W/B0APKMRK8JU/...` (Order Test app)

#### VPS deployment (Hostinger, Ubuntu 24.04)
- Repo pushed to `https://github.com/georgeautomates/haulage_firmin`
- Deployed to `/opt/firmin` via `git clone` + `deploy/setup_vps.sh`
- Python 3.12.3 venv at `/opt/firmin/.venv`
- systemd service `firmin.service` — enabled, starts on boot
- Credentials uploaded via SCP: `.env`, `gmail_token.json`, `gmail_credentials.json`, `service_account.json`

#### Gmail scope fix
- Changed from `gmail.readonly` → `gmail.modify` in both `gmail.py` and `setup_gmail_oauth.py`
- `mark_as_read()` method added to `GmailClient` — called after each email is processed
- Token regenerated with new scope (old token revoked at myaccount.google.com/permissions first)

#### Comparison timer
- systemd timer: `firmin-comparison.timer` + `firmin-comparison.service`
- Runs daily at 8am UK time (BST/GMT auto-handled via `TimeZone=Europe/London`)
- Posts comparison report to Slack
- First run: 2026-03-26 08:00 UTC

#### Deployment workflow (for future updates)
1. Make changes locally
2. `git add . && git commit -m "..." && git push`
3. On VPS: `cd /opt/firmin && git pull && systemctl restart firmin`

---

### What's been completed in session 1 (2026-03-25)

#### Infrastructure
- Fixed Supabase DSN — old hostname `db.ttwyttggzmgnkgcmrebq.supabase.co` replaced with pooler `aws-1-eu-west-2.pooler.supabase.com`, username format changed to `postgres.ttwyttggzmgnkgcmrebq`
- Google Cloud project created, Sheets API + Gmail API enabled
- Service account created → `config/service_account.json` → shared with spreadsheet as Editor
- Gmail OAuth credentials → `config/gmail_credentials.json` → token generated at `config/gmail_token.json`
- Gmail OAuth setup script modified to use `open_browser=False` (user pastes URL into correct browser profile)

#### Location matching — 3-tier system (supabase.py)
- **Tier 1 — Known overrides**: postcode → exact Description, defined per client in YAML profile. Bypasses all DB lookups. `ME10 2TD → DS SMITH - SITTINGBOURNE`
- **Tier 2 — Verified cache**: queries `location_mappings` table for previously verified matches (`verified=true`)
- **Tier 3 — Fuzzy search**: `similarity(OrganisationName, org_name) * 0.6 + similarity(full_address, search) * 0.4` — weighted toward org name vs old full_address-only approach
- Cache write: after Tier 3 match, writes unverified row to `location_mappings` for future human review

#### AI prompt fixes (ai.py)
- Added explicit `order_number` rule: "the PO number starting with PO-"
- Added `customer_ref` clarification: "line AFTER the PO number"
- Strengthened address separation rules to reduce collection/delivery text bleed

#### Client profile fixes (st_regis_fibre.yaml)
- `spaces` corrected from `0` to `26` (matches Proteo/n8n)
- `known_locations`: `ME10 2TD → DS SMITH - SITTINGBOURNE` (confirmed as most common Proteo delivery point, 173/459 entries)

#### Dedup fix (dedup.py)
- Fixed in-memory SQLite mode (`:memory:`) — now holds a persistent connection so tables survive between calls

#### End-to-end validated
- Single job `2560920` written to sheet matches Proteo TMS entries exactly:
  - collection_point: `Data Solutions Ltd - Dartford` ✓
  - delivery_point: `DS SMITH - SITTINGBOURNE` ✓
  - rate: `£300.00` ✓, pallets: `26` ✓, spaces: `26` ✓
  - score: `100 GREEN` ✓

#### Agent run
- `python -m firmin.agent` run successfully — processed all unread DS Smith emails in inbox
- SQLite dedup (`firmin.db`) prevents reprocessing on subsequent runs

#### Comparison script (run_comparison.py)
- Reads Actual Entry + Verification tabs, joins on `delivery_order_number`
- Compares: `collection_point`, `delivery_point`, `price`, `order_number` (scored)
- Also shows: `collection_date`, `delivery_date` (info only — PDF dates ≠ actual execution dates)
- Normalisation handles: date format `19/03/26` → `19/03/2026`, price `£300.00` → `300`, order number PO prefix before `/`, case, whitespace
- Kemsley delivery point aliases normalised to single value for matching
- Results written to Comparison tab

#### Comparison results (136 matched jobs)
| Field | Match rate |
|-------|-----------|
| collection_point | 85.3% |
| price | 91.2% |
| delivery_date | 80.9% (info only) |
| collection_date | 77.9% (info only) |
| order_number | 72.8% |
| delivery_point | 73.5% |
| **Overall FULL match** | **40.4%** |

Known mismatches:
- `delivery_point`: 20x `DS SMITH - SITTINGBOURNE` vs `Kemsley Depot (DSSR) - Sittingbourne` — Proteo staff use both interchangeably, normalised as aliases in comparison
- `collection_point`: some wrong fuzzy matches (Woodgreen→Chas Storer, Morrison→Swan Valley) — location DB has duplicate/variant entries
- `order_number`: some Proteo entries use internal reference format `1840071/1478384` instead of PO number — not fixable from PDF data
- Dates off by 1 day — PDF = requested date, Proteo = actual execution date — structural, not a bug

---

## Supabase

**Connection:** `postgresql://postgres.ttwyttggzmgnkgcmrebq:***@aws-1-eu-west-2.pooler.supabase.com:5432/postgres`

**Tables used:**
- `"Location Points"` — 159,421 rows. Columns: `"Description"`, `"OrganisationName"`, `"PostCode"`, `full_address`. Used for location matching.
- `location_mappings` — learned cache. Columns: `id`, `pdf_address`, `postcode`, `matched_description`, `verified`, `client_name`, `created_at`. No unique constraint — insert uses NOT EXISTS guard.
- `customer_profiles` — not yet used in code

**Key Supabase findings:**
- `ME10 2TD` (Kemsley Mill) has hundreds of rows — unusable with postcode-only filter
- `OrganisationName` column always populated — better similarity target than `full_address`
- `pg_trgm` extension enabled (required for `similarity()`)

---

## Google Sheets

**Spreadsheet ID:** `1uEst-r23EiTyfdmL6gx_YQMuzR0s7yyx-qJVLIUyDSI`

**Tabs:**
- `Actual Entry` — agent output (243 rows as of 2026-03-30, duplicates cleaned)
- `Verification` — Proteo TMS actual entries scraped by Playwright RPA (649 rows as of 2026-03-30)
- `Comparison` — comparison results written by run_comparison.py

**Column notes:**
- ` goods_type` has a **leading space** in the column name — must match exactly
- `delivery_order_number` = job number — join key for comparison

---

## .env

```
OPENAI_API_KEY=set ✓
AI_EXTRACTION_MODEL=gpt-4o-mini
SUPABASE_POSTGRES_DSN=set ✓ (pooler endpoint)
GMAIL_TOKEN_PATH=config/gmail_token.json ✓
GMAIL_CREDENTIALS_PATH=config/gmail_credentials.json ✓
GOOGLE_SERVICE_ACCOUNT_PATH=config/service_account.json ✓
SLACK_WEBHOOK_URL=set ✓
POLL_INTERVAL_SECONDS=60
LOG_LEVEL=INFO
```

---

## Key facts

- PDF: 3 jobs per page, up to 48 jobs per email, one AI call per job
- Gmail filter: `subject:@dssmith.com is:unread has:attachment`
- Dedup: SQLite `firmin.db` — job marked seen BEFORE sheet write (fixed 2026-03-30)
- Confidence scoring: GREEN ≥90 (and collection≠delivery), YELLOW ≥70 or collection=delivery, RED <70
- AI model: direct OpenAI (`OPENAI_API_KEY`), model `gpt-4o-mini`
- Collection times in PDF = requested time, not actual — will always differ from Proteo
- Dates in PDF = booking date, not execution date — typically 1 day earlier than Proteo

---

## What's NOT done yet

- **Production scheduling** — ✅ DONE. Running as systemd service on Hostinger VPS.
- **Slack notifications** — ✅ DONE. Batch summary per email + on-demand comparison report.
- **Comparison scheduling** — ✅ DONE. systemd timer runs daily at 8am UK time (BST), posts to Slack.
- **Duplicate sheet row cleanup** — ✅ DONE. 276 legacy rows removed, 243 unique jobs remain.
- **Kemsley location mapping** — awaiting response from DS Smith staff member re: correct Proteo names
- **Multi-client expansion** — image, Excel, email body input types not built
- **Playwright RPA auto-entry** — GREEN orders not yet auto-submitted to Proteo TMS
- **Verification scrape** — Playwright scrape of Proteo back into Verification tab (currently manual/separate process)
- **`customer_profiles` Supabase table** — defined in spec but not used in code yet
- **`location_mappings` human review UI** — unverified cache entries accumulate but no workflow to review/verify them

---

## Known challenges & gotchas

### VPS deployment
- `.env` must be uploaded manually via SCP — it is gitignored and never in the repo
- Gmail token (`gmail_token.json`) must also be uploaded manually — it is gitignored
- If the Gmail token expires or is revoked: delete it locally, run `python scripts/setup_gmail_oauth.py` (paste URL into correct browser profile), re-upload via SCP, restart service
- Gmail scope must be `gmail.modify` (not `readonly`) — token must be regenerated if scope changes
- VPS `.env` previously got overwritten with `.env.example` template during SCP — always verify with `grep OPENAI /opt/firmin/.env` after uploading

### Dedup / reprocessing
- If the agent fails mid-run (e.g. 401 AI errors), emails get marked as seen in dedup but nothing is written to the sheet
- To reprocess: `sqlite3 /opt/firmin/firmin.db "DELETE FROM processed_emails; DELETE FROM processed_orders;"`
- Gmail backlog: 50 emails were processed with errors before the API key was correctly set — these are now marked as read in Gmail and in dedup, and are gone from the backlog

### Gmail query
- Query: `subject:@dssmith.com is:unread has:attachment`
- DS Smith emails are **forwarded**, so the sender domain `@dssmith.com` appears in the subject line, not the From field
- PDF filenames are `GRIGGS_Q...` — this is Alan Firmin's internal reference name for DS Smith jobs, not a different client

### Slack
- Webhook URL is in `.env` as `SLACK_WEBHOOK_URL`
- If not set, Slack notifications are silently skipped (no error)
- Comparison report requires Verification tab to have data — if empty, reports 0 matched jobs

### Kemsley Depot collection issue (open)
- When PDF has `KEMSLEY DEPOT / DS SMITH RECYCLING / ME10 2TD` as the **collection** address, it matches `DS SMITH - SITTINGBOURNE` — wrong when Kemsley is collection not delivery
- Needs discussion with George to determine correct behaviour
- Comparison report data will help quantify the impact
