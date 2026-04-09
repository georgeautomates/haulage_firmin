# Firmin — Session Context
_Last updated: 2026-04-06 (session 6 — end of day)_

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
├── verification.py       — VerificationPipeline: scrapes Proteo, writes to Verification sheet
├── clients/
│   ├── gmail.py          — Gmail OAuth2 polling
│   ├── pdf.py            — PDF text extraction (PyMuPDF primary, pdfplumber fallback)
│   ├── ai.py             — AI extraction per job (OpenAI direct)
│   ├── supabase.py       — 3-tier location lookup (override → cache → fuzzy)
│   ├── sheets.py         — Google Sheets writer (gspread)
│   ├── slack.py          — Slack webhook client (batch summary + comparison report)
│   ├── proteo.py         — Playwright headless scraper for Proteo TMS
│   └── drive.py          — Google Drive PDF uploader (for dashboard PDF links)
├── profiles/
│   └── loader.py         — YAML client profile loader + email matcher
└── utils/
    ├── dedup.py           — SQLite dedup (email + job number)
    └── logger.py          — logging utility

config/
├── settings.yaml          — includes drive_folder_id config
└── clients/
    └── st_regis_fibre.yaml  — St Regis Fibre A/C client profile

scripts/
├── setup_gmail_oauth.py        — one-time Gmail OAuth setup (scopes: gmail.modify + drive.file)
├── test_pdf_pipeline.py        — smoke test: PDF + AI extraction
├── test_supabase.py            — smoke test: Supabase location lookup
├── test_e2e.py                 — end-to-end test: PDF → pipeline → Sheets (single job)
├── run_comparison.py           — comparison: Actual Entry vs Verification tab (interactive)
├── slack_comparison_report.py  — posts comparison report to Slack (on-demand)
├── debug_comparison.py         — debug: prints job number overlap between sheets
├── backfill_verification.py    — scrape Proteo for all Actual Entry jobs missing from Verification
│                                 supports --dry-run and --yes (for automated timer runs)
├── cleanup_verification_junk.py — remove junk rows from Verification sheet
│                                  detects: non-numeric order_id, order_id < 5 digits, wrong client
└── cleanup_duplicate_rows.py   — one-off: remove duplicate Actual Entry rows (already run)

deploy/
├── firmin.service              — main agent systemd service
├── firmin-comparison.service   — daily comparison report service
├── firmin-comparison.timer     — runs at 08:00 UK time daily
├── firmin-verification.service — verification backfill service
├── firmin-verification.timer   — runs at 14:00 + 22:00 UK time daily
└── setup_vps.sh                — one-time VPS setup script
```

## Dashboard (separate repo — E:\Arc Ai\firmin-dashboard)

Next.js app (deployed separately) for reviewing processed orders.

- **Stack:** Next.js, TypeScript, Tailwind, dark theme
- **Data source:** reads directly from Google Sheets (Comparison + Actual Entry tabs) via service account
- **Order list page (`/`):** stats bar (total, match rate, mismatched, partial), filter tabs (ALL/MATCH/PARTIAL/MISMATCH), date range filter, search by job number/delivery
- **Order detail page (`/orders/[id]`):** 3-column layout:
  - Left: booking form PDF embedded via Google Drive iframe
  - Middle: our extraction (with mismatch highlights)
  - Right: Proteo actual values
- **PDF links:** `pdf_url` + `message_id` stored in Actual Entry sheet, joined at read time
- **Env vars needed:** `GOOGLE_SERVICE_ACCOUNT_JSON`, `SPREADSHEET_ID`
- **PDF backlog:** all historical PDFs manually uploaded to Drive with `pdf_url` + `message_id` backfilled into Actual Entry

---

## Current status — PRODUCTION RUNNING ON VPS

The agent is deployed on Hostinger VPS (`72.61.202.184`, Ubuntu 24.04) and running as a systemd service. It starts on boot and restarts on failure.

### What's been completed this session (2026-04-09, session 8)

#### Re-extraction regression script — full run (343 jobs)
- Script updated to also write per-job detail to `Re-extraction` sheet tab (one row per job, all field-level match booleans)
- `Re-extraction` tab overwrites on each run — always reflects latest run
- History tab filters out test runs (<100 jobs) in the dashboard chart

#### Column bleed fix (firmin/clients/ai.py)
- Root cause: PyMuPDF flattens multi-column PDF → adjacent job data bleeds together
- Fix: `_slice_job_text()` — slices raw text from previous job number to next job number before passing to AI
- First slice attempt used 200-char lookback — still bled PO numbers from previous job
- Final fix: slice starts at previous job number boundary, ends at next job number boundary
- Results with gpt-4o-mini + slicing (343 jobs):
  - collection_point: 89.5% → **98.8%**
  - delivery_point: 94.2% → **99.4%**
  - price: 90.1% → **98.5%**
  - order_number: 85.7% → **87.5%**
  - **Full match: 83.4% → 85.7%**

#### George's prompt + model upgrade (pulled from remote)
- AI prompt significantly expanded: better rules, two worked examples, Kemsley collection/delivery disambiguation
- Model upgraded from `gpt-4o-mini` → `gpt-4o`
- Combined run (gpt-4o + slicing): 81.3% full match — slightly lower than gpt-4o-mini + slicing (85.7%)
- **Open question: gpt-4o underperforming gpt-4o-mini on this task — needs another run to confirm**

#### Dashboard — History trend chart
- Collapsible "Re-extraction History" panel between stats bar and filter tabs
- SVG line chart (no library) — 5 lines: full match, collection, delivery, price, order number
- Filters to runs with ≥100 jobs to exclude test runs
- Shows latest run summary inline when collapsed
- API route: `/api/history`

#### Dashboard — per-job re-extraction diff panel
- "Re-extraction Check" section at bottom of middle panel on order detail page
- Green "stable" badge if all re-extracted values match stored values
- Orange "N fields drifted" badge + stored vs re-extracted comparison for drifted fields
- Catches column bleed / wrong-job extractions at the per-job level
- API route: `/api/reextraction?job=<job_number>`

#### Dashboard deployment fix
- Vercel hobby plan only deploys commits authored by `georgeautomates`
- Fix: set `git config user.name/email` to `georgeautomates` in firmin-dashboard repo
- All future commits from that repo will author as `georgeautomates` → auto-deploys work

#### Remaining known mismatches (unfixable or deferred)
- `PO-0804269` vs `PO-080269` — AI adding extra digit, possible PDF character issue
- Proteo internal ref format (`1841694/1479265`) — unfixable from PDF
- `**LOST LOAD**` suffix — staff annotation in Proteo, not in PDF
- `Veolia - Southwark` UNMATCHED — needs postcode override in st_regis_fibre.yaml
- `VPK - Selby` vs `VPK Packaging - Selby` — alias missing from normalisation

#### Next session priorities (session 9):
1. **Confirm gpt-4o vs gpt-4o-mini** — run regression again, determine which model performs better for this task
2. **Multi-client expansion** — George wants the top 5 clients onboarded (St Regis is #1). Need client names + sample PDFs/emails for each. **Ask George for the list of top 5 clients and sample booking forms.**
3. **Daily stats tracking** — agent writes a `Daily Stats` sheet tab per day (jobs processed, GREEN/YELLOW/RED, match rate). Dashboard reads it for a day-by-day accuracy chart of live orders.
4. **Fix remaining location mismatches** — Veolia Southwark postcode override, VPK Selby alias, SCA/SAICA Newport alias
5. **Order list drift badge** — show re-extraction drift indicator on order list rows (not just detail page)

### What's been completed this session (2026-04-06, session 6)

#### Order review dashboard (firmin-dashboard repo)
- Next.js app built at `E:\Arc Ai\firmin-dashboard`
- Reads Comparison + Actual Entry sheets, joins on job number
- Order list with stats, filter tabs, date range, search
- Order detail: 3-column layout — PDF iframe | our extraction | Proteo actual
- PDF links via Google Drive: `drive.py` uploads PDF per email using service account, stores URL in Actual Entry sheet
- `message_id` + `pdf_url` columns added to Actual Entry row data
- Historical PDFs manually backlogged into Drive with URLs written to sheet
- `DRIVE_FOLDER_ID=1bM-ksJynQjABdLazYAHshvH8_xie5urP` set in VPS `.env` — live and uploading
- DriveClient uses service account only (OAuth fallback removed — caused invalid_scope crash on VPS)
- Dashboard not yet deployed publicly — running locally

#### Verification sheet fixes
- **Root cause of junk rows:** Proteo search is global across all clients — wrong-client orders (Pallet Track, ECS Container Services, Carousel Logistics etc.) were being written when their job numbers matched DS Smith job numbers
- **Fix 1:** `proteo.py` now validates `client_name` contains "st regis", "ds smith", "fibre", or "reels" — rejects wrong-client results
- **Fix 2:** `order_id` must be ≥ 5 digits — rejects pagination summary rows (e.g. `30`)
- **Cleanup:** `cleanup_verification_junk.py` updated to also detect wrong-client rows — removed 14 junk rows total
- **Gap root cause:** agent crashed with read timeout on Apr 4, down ~37 hours — jobs processed during downtime were never verified
- **Backfill:** ran `backfill_verification.py` — 17 written, 25 not found (either not in Proteo yet or wrong-client rejections)
- **Fix agent scope bug:** `drive` variable was referenced inside `_poll()` but not passed as parameter — would have caused `NameError` on first email

#### Twice-daily verification backfill timer
- New systemd timer: `firmin-verification.timer` + `firmin-verification.service`
- Runs at 14:00 + 22:00 UK time — catches jobs entered into Proteo hours after the email arrives
- `backfill_verification.py` updated with `--yes` flag to skip interactive prompt
- Installed and active on VPS

### What's been completed this session (2026-04-03, session 5)

#### Location matching overhaul

##### Conditional overrides (supabase.py, profiles/loader.py, st_regis_fibre.yaml)
- New `conditional_locations` field in ClientProfile — postcode maps to a list of `{keyword, result}` rules
- First keyword match in `org_name` wins; empty-keyword rule acts as fallback
- ME10 2TD now correctly resolves to two different Proteo names:
  - `KEMSLEY DEPOT` / `DS SMITH RECYCLING` in org → `Kemsley Depot (DSSR) - Sittingbourne`
  - Everything else (KEMSLEY MILL, KEMSLEY, KM bay codes) → `DS SMITH - SITTINGBOURNE`
- `pipeline.py` passes `conditional_locations` through to both collection and delivery lookups

##### Known location overrides added (st_regis_fibre.yaml)
- `OX16 1RE` → `VPK - Banbury` (Encase rebranded to VPK — Supabase stale)
- `RG19 4NH` → `Saica Packaging   - Thatcham` (Smurfit Kappa site → Saica — Supabase stale)
- `S63 5JD` → `Cepac Ltd - Rotherham` (Supabase has `S63 5DJ` — 1 char off)
- `BN18 0FL` → `Biffa - Arundel` (correct Arundel postcode confirmed from cache)

##### Key findings from PDF analysis
- PDF delivery address label distinguishes Kemsley types:
  - `KEMSLEY` / `KEMSLEY MILL` = standard inbound baled waste → DS SMITH - SITTINGBOURNE
  - `KEMSLEY DEPOT` / `DS SMITH RECYCLING` = Lidl jobs → Kemsley Depot (DSSR)
  - `KEMSLEY MILL (B/D/KM)` = Full Load outbound from DS Smith mill → DS SMITH - SITTINGBOURNE
- Full Load jobs: DS Smith is the COLLECTION point, delivery is to other paper mills
- Service type = `Full Load` (not `Baled Waste/Recycling`) for these outbound jobs

#### Proteo verification rebuilt in Python (replaces n8n/SSH/JS)

##### Root cause of Verification gap
- n8n Google Sheets OAuth credential expired 2026-03-27 — `redirect_uri_mismatch` error
- Verification tab frozen at 2026-03-26, 178 jobs uncompared since then
- Decision: replace n8n workflow entirely with Python — removes OAuth dependency

##### New files
- `firmin/clients/proteo.py` — Playwright (headless Chromium) scraper:
  - Logs into `firmin.proteoenterprise.co.uk`, navigates to Find Order, searches by job number
  - Extracts same table columns as JS script (cell indices 0-21)
  - `headless=True`, single browser per `scrape_job()` call
  - Requires `PROTEO_USERNAME` + `PROTEO_PASSWORD` in `.env`
- `firmin/verification.py` — VerificationPipeline:
  - Loads existing Verification sheet jobs on first run to avoid duplicates
  - Writes via SheetsClient (same service account as Actual Entry)
  - Returns written/skipped/not_found/errors summary
- `scripts/backfill_verification.py` — one-off backfill script:
  - Finds all Actual Entry jobs missing from Verification
  - Supports `--dry-run` to preview
  - Runs sequentially through Playwright (178 jobs ≈ 20-30 min)

##### agent.py changes
- Imports and initialises `ProteoClient` + `VerificationPipeline` on startup
- After each email is processed, runs verification for all newly written jobs
- Gracefully disabled if `PROTEO_PASSWORD` not set

##### VPS deployment
- `playwright` installed in `.venv` (v1.58.0)
- `playwright install chromium --with-deps` run — all deps already present
- `PROTEO_USERNAME` + `PROTEO_PASSWORD` added to `/opt/firmin/.env`
- Backfill of 178 gap jobs started (in progress as of end of session)

#### Verification gap root cause analysis
- 178 jobs in Actual Entry with no Proteo match
- Confirmed: not missing from Proteo — Verification tab just stale since 2026-03-26
- 3 non-numeric job numbers in Verification (`ecbu...`) — Playwright scrape noise, ignorable

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

#### Comparison results after session 5 fixes (324 matched jobs):
| Field | Match rate |
|-------|-----------|
| collection_point | 87.0% |
| delivery_point | 93.8% |
| price | 91.7% |
| order_number | 77.8% |
| **Full match** | **71.9%** |

Note: order_number low due to Proteo using internal ref format (e.g. `1840071/1478384`) instead of PO — not fixable from PDF data.

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
PROTEO_USERNAME=George ✓
PROTEO_PASSWORD=set ✓
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
- **Kemsley location mapping** — RESOLVED via conditional_locations (session 5). No staff contact needed.
- **Proteo scraper junk rows** — RESOLVED (session 6 full fix). Now validates order_id ≥ 5 digits AND client_name matches St Regis/DS Smith. 14 junk rows cleaned (wrong-client + pagination rows).
- **Comparison normalisations** — added: VPK/Encase Banbury, Majestic/Onboard Wolverhampton, Cepac Rotherham, Angleboard Dudley, Suez Huddersfield variants, RCP Procurement/Shotton Mill.
- **Multi-client expansion** — image, Excel, email body input types not built
- **Playwright RPA auto-entry** — GREEN orders not yet auto-submitted to Proteo TMS
- **Verification scrape** — ✅ DONE (session 5). Python Playwright replaces n8n/SSH/JS. Runs automatically after each email.
- **`customer_profiles` Supabase table** — defined in spec but not used in code yet
- **`location_mappings` human review UI** — unverified cache entries accumulate but no workflow to review/verify them
- **Verification retry tracking** — "not found" jobs are re-attempted by the twice-daily timer indefinitely; no cutoff for permanently cancelled/amended jobs yet (low priority)
- **Dashboard deployment** — firmin-dashboard is running locally; not yet deployed to a public URL
- **Drive folder ID on VPS** — ✅ DONE. `DRIVE_FOLDER_ID=1bM-ksJynQjABdLazYAHshvH8_xie5urP` set in `/opt/firmin/.env`

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
