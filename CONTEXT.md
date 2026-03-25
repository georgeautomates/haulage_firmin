# Firmin — Session Context
_Last updated: 2026-03-25_

## What this project is

Python agent for Alan Firmin Ltd (haulage). Polls Gmail for DS Smith booking form emails (PDF attachments), extracts structured order data per job using AI, looks up location point names from Supabase, scores confidence, and writes rows to Google Sheets. Runs in shadow mode alongside the existing n8n workflow for validation.

This is a **multi-client system** — built to scale to other clients who send orders as images, Excel files, or plain email body text. DS Smith (St Regis Fibre A/C) is the first and only client implemented so far.

Reference n8n workflow: `C:\Users\USERAS\Downloads\St Regis Fiber - Shadow Mode.json`

---

## Architecture

```
firmin/
├── agent.py              — main poll loop (entry point: python -m firmin.agent)
├── pipeline.py           — per-email orchestrator
├── scoring.py            — confidence scoring (GREEN/YELLOW/RED)
├── clients/
│   ├── gmail.py          — Gmail OAuth2 polling
│   ├── pdf.py            — PDF text extraction (pdfplumber + PyMuPDF fallback)
│   ├── ai.py             — AI extraction per job (OpenAI direct)
│   ├── supabase.py       — 3-tier location lookup (override → cache → fuzzy)
│   └── sheets.py         — Google Sheets writer (gspread)
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
├── setup_gmail_oauth.py     — one-time Gmail OAuth setup
├── test_pdf_pipeline.py     — smoke test: PDF + AI extraction
├── test_supabase.py         — smoke test: Supabase location lookup
├── test_e2e.py              — end-to-end test: PDF → pipeline → Sheets (single job)
├── run_comparison.py        — comparison: Actual Entry vs Verification tab
└── debug_comparison.py      — debug: prints job number overlap between sheets
```

---

## Current status — PRODUCTION RUNNING

The agent is running and writing to Google Sheets. All components are working.

### What's been completed this session (2026-03-25)

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
- `Actual Entry` — agent output (428+ rows as of 2026-03-25)
- `Verification` — Proteo TMS actual entries scraped by Playwright RPA (459 rows)
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
POLL_INTERVAL_SECONDS=60
LOG_LEVEL=INFO
```

---

## Key facts

- PDF: 3 jobs per page, up to 48 jobs per email, one AI call per job
- Gmail filter: `subject:@dssmith.com is:unread has:attachment`
- Dedup: SQLite `firmin.db` — tracks processed email message_ids and job numbers
- Confidence scoring: GREEN ≥80, YELLOW ≥50, RED <50 (per st_regis_fibre.yaml thresholds)
- AI model: direct OpenAI (`OPENAI_API_KEY`), model `gpt-4o-mini`
- Collection times in PDF = requested time, not actual — will always differ from Proteo
- Dates in PDF = booking date, not execution date — typically 1 day earlier than Proteo

---

## What's NOT done yet

- **Production scheduling** — agent not yet set up as a Windows service or Task Scheduler job. Currently run manually.
- **Comparison scheduling** — run_comparison.py is manual only, not automated
- **Multi-client expansion** — image, Excel, email body input types not built
- **Playwright RPA auto-entry** — GREEN orders not yet auto-submitted to Proteo TMS
- **Verification scrape** — Playwright scrape of Proteo back into Verification tab (currently manual/separate process)
- **`customer_profiles` Supabase table** — defined in spec but not used in code yet
- **`location_mappings` human review UI** — unverified cache entries accumulate but no workflow to review/verify them
