# Security Mapper

An Azure Function App that resolves raw portfolio input strings (company name + security description) to standardized master securities stored in Elasticsearch. It handles abbreviations, alias/rename chains, and fuzzy name variants, with a fast bypass for previously confirmed human-reviewed mappings.

> For full API request/response schemas and integration examples see [API_INTEGRATION.md](API_INTEGRATION.md).

---

## Table of Contents

1. [Tech Stack](#tech-stack)
2. [Project Structure](#project-structure)
3. [End-to-End Flow](#end-to-end-flow)
4. [Step 1 — Normalization](#step-1--normalization)
5. [Step 2 — Alias Bypass Lookup](#step-2--alias-bypass-lookup)
6. [Step 3 — Phase 1: Family Retrieval](#step-3--phase-1-family-retrieval)
7. [Step 4 — Phase 2: Security Retrieval](#step-4--phase-2-security-retrieval)
8. [Step 5 — Result Assembly and Re-anchoring](#step-5--result-assembly-and-re-anchoring)
9. [Score Scaling](#score-scaling)
10. [Boost Configuration](#boost-configuration)
11. [Elasticsearch Index Design](#elasticsearch-index-design)
12. [Mapping Management — How Alias Docs Are Built](#mapping-management--how-alias-docs-are-built)
13. [API Endpoints Summary](#api-endpoints-summary)
14. [Local Development Setup](#local-development-setup)
15. [Environment Variables](#environment-variables)
16. [Testing](#testing)
17. [Utility Scripts](#utility-scripts)

---

## Tech Stack

| Component | Technology |
|---|---|
| Runtime | Python 3.10+ |
| Hosting | Azure Functions v2 (HTTP trigger) |
| Search engine | Elasticsearch 8.x |
| Abbreviation expansion | PostgreSQL (`abbreviation_map` table, 5-min cached) |
| Local runner | Azure Functions Core Tools (`func start`) |

---

## Project Structure

```
security-mapper/
│
├── function_app.py             # All 6 Azure Function endpoints + core retrieval logic
├── normalization.py            # Text normalization pipeline + abbreviation expansion
├── config.py                   # ES query boost weights used by function_app.py
│
├── es_index_mapping.json       # Elasticsearch index schema (field mappings + settings)
├── ingest_to_es.py             # Bulk ingest master securities into ES
├── store_manual_mapping.py     # One-off: manually store a single confirmed alias mapping
├── scratch_store_mappings.py   # Batch: store reviewed mappings from Excel sheet
├── scratch_update_mapping.py   # Update an existing alias mapping document
│
├── test.http                   # VS Code REST Client requests for all 6 endpoints
├── test_all.py                 # Batch evaluation against Security_Mapping_TestCases.xlsx
├── test_batch_securities.py    # Concurrent batch evaluation (10 parallel workers)
├── test_mappings.py            # Round-trip integration test for mapping CRUD endpoints
├── test_es_connection.py       # Quick ES connectivity check
│
├── requirements.txt
├── host.json
├── local.settings.json         # Local env variables — not committed
└── API_INTEGRATION.md          # Full API reference for frontend/backend integrators
```

---

## End-to-End Flow

Every call to `POST /api/security-mapping` goes through `_resolve_security_mapping()` in `function_app.py`. The five steps are:

```
Raw (company_input, security_input)
            │
            ▼
  ┌─────────────────────────────────────────┐
  │  Step 1: Normalization                  │
  │  normalize_input() → family_query       │
  │                    → security_query     │
  └────────────────────┬────────────────────┘
                       │
                       ▼
  ┌─────────────────────────────────────────┐
  │  Step 2: Alias Bypass Lookup            │
  │  Single ES search — exact keyword match │
  │  Hit found  → return immediately        │
  │              (is_mapped: true, conf 1.0)│
  │  No hit     → continue to Phase 1      │
  └────────────────────┬────────────────────┘
                       │ no hit
                       ▼
  ┌─────────────────────────────────────────┐
  │  Step 3: Phase 1 — Family Retrieval     │
  │  msearch (2 queries in parallel)        │
  │  Q1: master docs — match on family/soi  │
  │  Q2: alias docs  — phrase on company    │
  │  Merge → top K candidate families       │
  └────────────────────┬────────────────────┘
                       │
                       ▼
  ┌─────────────────────────────────────────┐
  │  Step 4: Phase 2 — Security Retrieval   │
  │  msearch (2 queries in parallel)        │
  │  Q1: master docs in candidate families  │
  │  Q2: alias docs  in candidate families  │
  │  Merge → top 20 ranked unique securities│
  └────────────────────┬────────────────────┘
                       │
                       ▼
  ┌─────────────────────────────────────────┐
  │  Step 5: Result Assembly                │
  │  Re-anchor best_family if needed        │
  │  Compute confidence scores              │
  │  Return match + candidates              │
  └─────────────────────────────────────────┘
```

---

## Step 1 — Normalization

**Where:** `normalization.py` → `normalize(text, conn_string)`, called via `normalize_input()`

Both `company_input` and `security_input` are normalized independently, producing `family_query` and `security_query`. These normalized strings are what every subsequent ES query runs against — the raw strings are never sent to ES directly.

**Pipeline (in order):**

| # | Operation | What it does | Example |
|---|---|---|---|
| 1 | Lowercase | Entire string to lowercase | `"Events Buyer, LLC"` → `"events buyer, llc"` |
| 2 | Strip parentheses | Removes `(` and `)` | `"AFC-Dell (12/23)"` → `"AFC-Dell 12/23"` |
| 3 | Separator normalization | Replaces `/`, `.`, `-` with a space — done **before** tokenization so compound tokens split correctly | `"T/L"` → `"T L"`, `"AFC-Dell"` → `"AFC Dell"` |
| 4 | Tokenize | Split on whitespace | `["afc", "dell", "12", "23"]` |
| 5 | Abbreviation expansion | Looks up each token (and adjacent token pairs) in PostgreSQL `abbreviation_map`. Combines adjacent tokens before lookup too (`t` + `l` → `tl` → `term loan`). Phrase matches take priority (`revolving loan` → `revolver`) | `"tl"` → `"term loan"`, `"dd"` → `"delayed draw"`, `"rc"` → `"revolver"` |
| 6 | Ordinal conversion | `num2words` converts ordinal suffixes to full words. Cardinals are left unchanged | `"4th"` → `"fourth"`, `"1st"` → `"first"`, `"2023"` unchanged |
| 7 | Symbol stripping | Removes remaining non-alphanumeric chars `[^\w\s]` | — |
| 8 | Stopword removal | Strips corporate entity suffixes | `ltd`, `inc`, `corp`, `llc`, `lp`, `plc`, `company`, `co`, `limited`, `pvt` |

**Abbreviation map caching:** the PostgreSQL `abbreviation_map` table is loaded once at startup and cached in memory for 300 seconds. On the next call after expiry it reloads. This avoids a DB hit per request while keeping expansions up to date.

---

## Step 2 — Alias Bypass Lookup

**Where:** inline in `_resolve_security_mapping()`, before any Phase 1/2 logic

This is the fastest path. Before running any fuzzy ES search, the code checks whether this exact normalized input pair has already been confirmed by a human reviewer and stored as an alias document. It fires a **single ES search** with two `should` branches:

**Branch A — Alias doc lookup (boost 2.0, higher priority):**
```
is_alias: true
AND normalized_company_name.keyword = family_query    ← exact keyword match
AND normalized_security_name.keyword = security_query ← exact keyword match
```
This finds alias docs where the raw input company name (after normalization) matches a previously stored mapping. The boost 2.0 means if both branches match, the alias doc wins.

**Branch B — Direct master doc lookup:**
```
is_alias: false
AND normalized_family_name.keyword = family_query     ← exact keyword match
AND normalized_security_name.keyword = security_query ← exact keyword match
```
This catches cases where the normalized input happens to exactly match a master security's own normalized family + security name — a zero-ambiguity direct hit against the canonical record.

**What happens on a hit:**

- If the hit is an alias doc (`is_alias: true`), the code reads `master_security_details` (the master record snapshot embedded in the alias) and determines the match type:
  - `"historical"` — `stored_normalized_security_name == security_query` (exact same security string confirmed before)
  - `"indirect"` — they differ (e.g. the security was renamed since the alias was created, but the company+security pair still points to the right master)
- Returns immediately with `family_confidence: 1.0`, `security_confidence: 1.0`, `is_mapped: true`.

- If **no hit**, falls through to Phase 1. The bypass failure is silent — `is_mapped` stays `False` and the full search runs.

---

## Step 3 — Phase 1: Family Retrieval

**Where:** `search_family_matches(family_query)` in `function_app.py`

The goal is to find the best matching **company family** (issuer). The raw `family_query` is cleaned further before the ES query runs.

### Broad Retrieval Cleaning

`clean_query_for_broad_retrieval(family_query)` strips asset-class noise from the normalized company query so the ES search focuses on the company name only:

**Phrases removed first** (substring replacement with space):
`"first lien"`, `"second lien"`, `"common equity"`, `"preferred equity"`, `"delayed draw term loan"`, `"term loan"`

**Then token-level stopwords removed** (`GENERIC_RETRIEVAL_STOPWORDS` — much broader than normalization stopwords):
`holdings`, `holding`, `holdco`, `group`, `trust`, `the`, `and`, `of`, `a`, `an`, `common`, `preferred`, `equity`, `first`, `second`, `lien`, `amendment`, `amend`, `initial`, `closing`, `date`, `new`, `money`, `class`, `series`, `unit`, `units`, `unfunded`, `funded`, `priority`, `fourth`, `out`, `incremental`, `roll`, `rollup`, `restated`, `restatement`, plus all corporate stopwords.

The result (`cleaned_family_query`) is a lean company-name-only string. For example `"events buyer term loan"` → `"events buyer"`. If cleaning produces an empty string (edge case for very generic inputs), the original `family_query` is used as a fallback.

### Two ES Queries via `msearch`

Both queries fire in a single `msearch` call — one round trip to ES.

**Query 1 — Master docs** (`is_alias: false`, collapsed on `family_name.keyword`):

Searches `normalized_family_name`, `normalized_soi_name`, and `normalized_security_name` with a layered `should` clause. The `cleaned_family_query` (lean company name) is used for family/SOI fields; the original `family_query` (may include security type words) is used for the security name fallback tier. One hit per family is returned via collapse.

**Query 2 — Alias docs** (`is_alias: true`, collapsed on `master_normalized_family_name.keyword`):

Searches `normalized_company_name^50` and `normalized_security_name^30` with a `multi_match phrase` query using `cleaned_family_query`. The high `^50` weight on company name means the alias company name is the primary signal. One hit per master family is returned via collapse.

### Merge Logic

Results from both queries are merged into one ranked list:
- For each alias hit, extract `master_normalized_family_name` (or fall back to `master_security_details.normalized_family_name`)
- If that family is already in the master results list, keep whichever has the **higher scaled score**
- If it's a new family not found in master results, add it to the list

The final merged list is sorted by scaled score descending. Only the top `MATCH_TOP_K` (default 5) families are kept.

---

## Step 4 — Phase 2: Security Retrieval

**Where:** `search_securities_es(security_query, candidate_families, family_query)` in `function_app.py`

Takes the candidate families from Phase 1 and finds the best matching **security** within them. Again two parallel queries via a single `msearch` call.

**Query 1 — Master docs:**

Hard-filtered to `normalized_family_name.keyword IN [candidate family names]` and `is_alias: false`. The `security_query` (full normalized security description) is matched against:
- `normalized_security_name` via `match_phrase` (highest boost — exact phrase)
- `normalized_security_name` via `match OR` with 50% minimum (partial token overlap)
- `normalized_security_name` + `normalized_soi_name` via `cross_fields` multi_match at 50% min (tokens can be spread across both fields)
- Same `cross_fields` but at 10% min (broad catch-all for weak partial matches)

Collapsed on `security_name.keyword` → up to 20 unique securities.

**Query 2 — Alias docs:**

Hard-filtered to `master_normalized_family_name.keyword IN [candidate family names]` and `is_alias: true`. Matches `normalized_company_name^30` + `normalized_security_name^50` with a `multi_match phrase` query against `security_query`. The higher `^50` on security name means the stored alias security name is the dominant signal here.

Collapsed on `master_normalized_security_name.keyword`.

### Merge and Dedup

Both result sets are merged. Deduplication is by `security_name` — alias hits that surface a master security already found in Query 1 are skipped. The combined list is sorted by raw ES score descending.

---

## Step 5 — Result Assembly and Re-anchoring

**Where:** end of `_resolve_security_mapping()`

After Phase 2 returns the ranked securities, the top result (`best_sec`) is taken. A subtle issue can arise: Phase 1 found multiple candidate families, and Phase 2 searched across all of them. The winning security may not belong to `best_family` (Phase 1's top result) — it may belong to a lower-ranked candidate family that turned out to have a better security match.

**Re-anchoring:** if `best_sec["normalized_family_name"] != best_family["normalized_family_name"]`, the code scans the full `family_matches` list to find whichever candidate family actually owns `best_sec` and replaces `best_family` with it. This ensures the final response never has mismatched family and security fields from two different companies.

**Confidence scores:**
- `family_confidence` = the scaled score of `best_family` from Phase 1
- `security_confidence` = `_es_scaled(best_sec["score"])` — Phase 2 raw score scaled to `[0, 1]`

**Response shape (search path, `is_mapped: false`):**
- `match.top_security` — the winning security name
- `match.family_confidence` / `match.security_confidence` — scaled scores
- `match.matched` — `True` if any security score > 0
- `match.match_type` — `"direct"` if matched, `"unmatched"` if not
- `candidates.top_families` — all candidate families considered
- `candidates.ranked_securities` — all securities evaluated

> `master_data` is currently commented out of the search-path response. The resolved master record is present in `candidates.ranked_securities[0]` instead.

---

## Score Scaling

Raw Elasticsearch scores are mapped to `[0.0, 1.0]` via:

```
scaled = log(1 + raw_score) / log(1 + cap)
clamped to [0.0, 1.0]
```

`cap` is set by `ES_SCORE_LOG_CAP` (default `600.0`). A score of `1.0` only comes from a stored alias bypass — it is never reachable via fuzzy search.

**Practical thresholds for the search path:**

| Condition | Action |
|---|---|
| Both `family_confidence` and `security_confidence` ≥ 0.4 | Accept the match |
| Either score < 0.4 | Route to human review |
| `matched: false` | Escalate / manual lookup |

---

## Boost Configuration

All values live in `config.py` and are imported into `function_app.py` via `FAMILY_RETRIEVAL_CONFIG`, `SECURITY_RETRIEVAL_CONFIG`, `MAPPED_FAMILY_RETRIEVAL_CONFIG`, `MAPPED_SECURITY_RETRIEVAL_CONFIG`.

**Phase 1 — Master doc query:**

| Field | Query type | Boost | min_should_match |
|---|---|---|---|
| `normalized_family_name` | `match_phrase` | 30 | — |
| `normalized_family_name` | `match OR` | 15 | 50% |
| `normalized_soi_name` | `match_phrase` | 25 | — |
| `normalized_soi_name` | `match OR` | 20 | 50% |
| `normalized_security_name` | `match_phrase` | 10 | — |
| `normalized_security_name` | `match OR` | 8 | — |

**Phase 1 — Alias doc query:**

| Fields | Query type | Boost |
|---|---|---|
| `normalized_company_name^50` + `normalized_security_name^30` | `multi_match phrase` | 30 |

**Phase 2 — Master doc query:**

| Field | Query type | Boost | min_should_match |
|---|---|---|---|
| `normalized_security_name` | `match_phrase` | 30 | — |
| `normalized_security_name` | `match OR` | 10 | 50% |
| `normalized_security_name` + `normalized_soi_name` | `cross_fields` | 25 | 50% |
| `normalized_security_name` + `normalized_soi_name` | `cross_fields` (broad) | 10 | 10% |

**Phase 2 — Alias doc query:**

| Fields | Query type | Boost |
|---|---|---|
| `normalized_company_name^30` + `normalized_security_name^50` | `multi_match phrase` | 30 |

---

## Elasticsearch Index Design

Index: `security_master_v4` (overridable via `ES_INDEX`). Schema in `es_index_mapping.json`. Two logical document types live in the same index, distinguished by `is_alias`.

### Master security (`is_alias: false`)

The canonical record. Ingested via `ingest_to_es.py`.

| Field | Type | Purpose |
|---|---|---|
| `family_name` | text + keyword | Full legal entity name |
| `normalized_family_name` | text + keyword | Normalized — Phase 1 query target + bypass branch B |
| `soi_name` | text + keyword | Schedule of Investments short name |
| `normalized_soi_name` | text + keyword | Normalized — Phase 1 + Phase 2 query target |
| `security_name` | text + keyword | Full canonical security name |
| `normalized_security_name` | text + keyword | Normalized — Phase 1 fallback + Phase 2 primary query target |
| `security_type` | keyword | `"Term Loan"`, `"Revolver"`, `"Common Equity"`, etc. |

### Alias mapping (`is_alias: true`)

Written by `POST /api/store-mappings`, `store_manual_mapping.py`, and `scratch_store_mappings.py`. Links a raw input pair to a master security.

| Field | Type | Purpose |
|---|---|---|
| `company_name` | text + keyword | Raw input company name as received |
| `normalized_company_name` | text + keyword | Normalized — bypass branch A + Phase 1/2 alias queries |
| `security_name` | text + keyword | Raw input security name as received |
| `normalized_security_name` | text + keyword | Normalized — bypass branch A + Phase 2 alias query |
| `master_security_details` | object (not indexed) | Full snapshot of the master record at time of mapping — read by the bypass |
| `master_family_name` | text + keyword | Flat searchable mirror of `master_security_details.family_name` |
| `master_normalized_family_name` | text + keyword | Flat mirror — Phase 1 alias collapse field + Phase 2 alias filter |
| `master_security_name` | text + keyword | Flat mirror |
| `master_normalized_security_name` | text + keyword | Flat mirror — Phase 2 alias collapse field |
| `master_soi_name` / `master_normalized_soi_name` | text + keyword | Flat mirror |
| `master_security_type` | keyword | Flat mirror |
| `filetype` | keyword | Source file type label |
| `loan_type` | keyword | Loan/security type |
| `metadata` | object (dynamic) | Arbitrary key-value store |
| `ingested_at` | date | ISO timestamp |

**Why flat mirror fields?** `master_security_details` is stored as a non-indexed blob (fast writes, no mapping explosion). But Phase 1 and Phase 2 need to query and collapse on family/security names from alias docs. The flat `master_*` fields are the indexed, searchable copies of those values.

**Alias doc ID** is deterministic:
```
id = sha256( normalized_company_name + "|" + normalized_security_name )
```
The same input always maps to the same document ID. Creating a mapping is idempotent by identity — a second call with the same normalized pair returns `409 Conflict` rather than creating a duplicate.

---

## Mapping Management — How Alias Docs Are Built

When `POST /api/store-mappings` is called with a confirmed (company, security, target_security_name):

1. Both `company_input` and `security_input` are normalized via `normalize_input()` → same pipeline as Step 1
2. The SHA-256 id is computed from the normalized pair
3. ES is checked for an existing doc with that id — if found, `409` is returned
4. `_fetch_master_security_doc()` fetches the full master record by exact `security_name.keyword` match (only non-alias docs)
5. `_build_mapping_doc()` assembles the alias document: raw inputs, normalized inputs, the full master doc as `master_security_details`, all the flat `master_*` mirrors, plus `filetype`, `loan_type`, `metadata`, `ingested_at`
6. `es.index()` writes it with the deterministic id

**Effect on future requests:** the next call to `/api/security-mapping` with the same `company_input` + `security_input` will hit the alias bypass (Step 2) and return instantly with `is_mapped: true`, `confidence: 1.0`, without running any fuzzy search.

**Updating a mapping** (`PUT /api/update-mappings/{id}`):
- If `target_security_name` is provided in the body, `_fetch_master_security_doc()` re-fetches the new master record and all `master_*` flat fields are replaced
- `file_type`, `loan_type`, `metadata` are updated if provided; unset fields retain their existing values
- The document is re-indexed with the same id (overwrite)
- The normalized company/security identity does not change — the id stays the same

**Deleting a mapping** (`DELETE /api/delete-mappings/{id}`):
- `es.delete()` removes the alias doc
- The next call for the same input falls through to Phase 1/2 search again

---

## API Endpoints Summary

| # | Method | Route | Purpose |
|---|---|---|---|
| 1 | `POST` | `/api/security-mapping` | Resolve raw inputs to master securities (batch array) |
| 2 | `POST` | `/api/store-mappings` | Store a user-confirmed mapping as an alias doc |
| 3 | `GET` | `/api/view-mappings` | Browse / filter by company or security / download CSV |
| 4 | `PUT` | `/api/update-mappings/{id}` | Update a stored mapping |
| 5 | `DELETE` | `/api/delete-mappings/{id}` | Remove a stored mapping |
| 6 | `POST` | `/api/store-master-security` | Add a new master security record to the index |

Full schemas, response shapes, error codes, and JavaScript/Python/cURL examples: [API_INTEGRATION.md](API_INTEGRATION.md).

---

## Local Development Setup

### Prerequisites

- Python 3.10+
- [Azure Functions Core Tools v4](https://learn.microsoft.com/en-us/azure/azure-functions/functions-run-local)
- Elasticsearch 8.x with `security_master_v4` index (create using `es_index_mapping.json`)
- PostgreSQL with `abbreviation_map` table

### Install

```bash
python -m venv .venv
.venv\Scripts\activate        # Windows
pip install -r requirements.txt
```

### Configure `local.settings.json`

```json
{
  "IsEncrypted": false,
  "Values": {
    "AzureWebJobsStorage": "UseDevelopmentStorage=true",
    "FUNCTIONS_WORKER_RUNTIME": "python",
    "ES_URL": "https://<es-host>:9200",
    "ES_USERNAME": "<username>",
    "ES_PASSWORD": "<password>",
    "ES_VERIFY_CERTS": "false",
    "ES_INDEX": "security_master_v4",
    "POSTGRES_CONN": "host=<host> dbname=<db> user=<user> password=<pass>",
    "MATCH_TOP_K": "5",
    "ES_SCORE_LOG_CAP": "600.0"
  }
}
```

### Run

```bash
func start
```

---

## Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `ES_URL` | Yes | — | Elasticsearch base URL |
| `ES_USERNAME` | No | — | Basic auth username (used if no `ES_API_KEY`) |
| `ES_PASSWORD` | No | — | Basic auth password |
| `ES_API_KEY` | No | — | API key auth — takes priority over basic auth |
| `ES_VERIFY_CERTS` | No | `true` | Set `false` for self-signed certs |
| `ES_INDEX` | No | `security_master_v4` | Elasticsearch index name |
| `POSTGRES_CONN` | Yes | — | PostgreSQL connection string |
| `MATCH_TOP_K` | No | `5` | Max candidate families returned in Phase 1 |
| `ES_SCORE_LOG_CAP` | No | `600.0` | Cap for logarithmic score scaling |

---

## Testing

### Round-trip integration test

```bash
func start                  # terminal 1
python test_mappings.py     # terminal 2
```

Tests create → view → bypass verify → update → delete → 404 confirm. Set `COMPANY_INPUT`/`SECURITY_INPUT` at the top to a pair that resolves in your index.

### Batch evaluation

```bash
python test_all.py              # sequential, writes evaluation_results.xlsx
python test_batch_securities.py # concurrent (10 workers), writes batch_test_results.xlsx
```

Both read from `Security_Mapping_TestCases.xlsx`.

### Other

```bash
python test_es_connection.py    # ES connectivity
```

Open `test.http` in VS Code with the REST Client extension for per-endpoint interactive testing.

---

## Utility Scripts

| Script | Purpose |
|---|---|
| `ingest_to_es.py` | Bulk-ingest master security records from source data |
| `store_manual_mapping.py` | Store a single hand-crafted alias mapping |
| `scratch_store_mappings.py` | Batch-store reviewed mappings from the test Excel sheet |
| `scratch_update_mapping.py` | Update fields on an existing alias mapping doc |
| `run_normalization.py` | Test the normalization pipeline interactively on a raw string |
