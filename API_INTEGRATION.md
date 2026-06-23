# Security Mapping API — Integration Guide

This document covers everything needed to call the `map-security` endpoint from an external system.

---

## Overview

The Security Mapping API resolves raw portfolio input strings (company name + security description) to standardized master securities stored in Elasticsearch. It handles typos, abbreviations (e.g. `T/L` → `term loan`), alias/rename chains, and fuzzy name variants.

---

## Base URL

| Environment | URL |
|---|---|
| Local development | `http://localhost:7071/api` |
| Azure (deployed) | `https://<function-app-name>.azurewebsites.net/api` |

---

## Authentication

Azure Function Apps support function-level API keys. Include the key in the request header or query string:

```
x-functions-key: <your-function-key>
```

or as a query parameter:

```
POST /api/map-security?code=<your-function-key>
```

For local development, no key is required.

---

## Endpoint

### `POST /api/map-security`

Maps one or more (company, security) pairs to master securities.

**Headers**

```
Content-Type: application/json
x-functions-key: <key>          # required on deployed instances
```

---

## Request Schema

### Single item

```json
{
  "input": "Events Buyer, LLC",
  "security_input": "Events buyer Term loan"
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `input` | string | Yes | Raw company / issuer name |
| `security_input` | string | No | Raw security description. Defaults to `input` if omitted. |

### Batch (array of items)

Send a JSON array to resolve multiple pairs in one call:

```json
[
  {
    "input": "Events Buyer, LLC",
    "security_input": "Events buyer Term loan"
  },
  {
    "input": "AFC-Dell Holding Corp.",
    "security_input": "AFC-Dell Holding DD T/L (12/23)"
  }
]
```

The response is a JSON array with results in the same order as the request. Items that fail individually return `{"error": "<message>"}` without failing the whole batch.

---

## Response Schema

The response shape differs slightly depending on how the match was resolved. There are two top-level shapes: **mapped match** (alias/historical lookup) and **search match** (Elasticsearch retrieval).

### Common fields (always present)

```json
{
  "input": {
    "company_input": "Events Buyer, LLC",
    "security_input": "Events buyer Term loan",
    "company_query": "events buyer",
    "security_query": "events buyer term loan"
  },
  "is_mapped": false,
  "match": {
    "top_security": "Events Buyer, LLC Initial Term Loan",
    "family_confidence": 0.582,
    "security_confidence": 0.374,
    "matched": true,
    "match_type": "direct"
  }
}
```

| Field | Type | Description |
|---|---|---|
| `input.company_input` | string | Original company string as sent |
| `input.security_input` | string | Original security string as sent |
| `input.company_query` | string | Normalized form used for search |
| `input.security_query` | string | Normalized form used for search |
| `is_mapped` | bool | `true` if resolved via a pre-stored alias/mapping record |
| `match.top_security` | string | Best matching security name |
| `match.family_confidence` | float 0–1 | Confidence in the company/family match |
| `match.security_confidence` | float 0–1 | Confidence in the security match |
| `match.matched` | bool | `true` if a security was found |
| `match.match_type` | string | See [Match Types](#match-types) below |

---

### Shape A — Search match (`is_mapped: false`)

Returned when the match is resolved through Elasticsearch retrieval (no pre-stored alias record exists).

```json
{
  "input": { ... },
  "is_mapped": false,
  "match": {
    "top_security": "Events Buyer, LLC Initial Term Loan",
    "family_confidence": 0.582,
    "security_confidence": 0.374,
    "matched": true,
    "match_type": "direct"
  },
  "master_data": {
    "family_name": "Events Buyer, LLC",
    "normalized_family_name": "events buyer",
    "soi_name": "Events Buyer",
    "security_name": "Events Buyer, LLC Initial Term Loan",
    "normalized_security_name": "events buyer initial term loan",
    "security_type": "Term Loan"
  },
  "candidates": {
    "top_families": [
      { "family_name": "Events Buyer, LLC" }
    ],
    "ranked_securities": [
      {
        "security_name": "Events Buyer, LLC Initial Term Loan",
        "security_type": "Term Loan",
        "normalized_soi_name": "events buyer",
        "normalized_security_name": "events buyer initial term loan",
        "normalized_family_name": "events buyer",
        "score": 55.0
      }
    ]
  }
}
```

| Field | Description |
|---|---|
| `master_data` | The resolved master security record. `null` when `matched: false`. |
| `candidates.top_families` | Up to 5 candidate families considered (ordered by score) |
| `candidates.ranked_securities` | All securities evaluated within the candidate families |

---

### Shape B — Mapped match (`is_mapped: true`)

Returned when the input pair resolves directly to a pre-stored alias record (historical rename, indirect name, or manually mapped entry).

```json
{
  "input": { ... },
  "is_mapped": true,
  "mapped": {
    "mapped_family": "Events Buyer, LLC",
    "mapped_security": "Events Buyer, LLC Initial Term Loan",
    "filetype": "portfolio_upload",
    "mapped_at": "2024-11-15T10:30:00Z",
    "master_security_details": {
      "family_name": "Events Buyer, LLC",
      "normalized_family_name": "events buyer",
      "soi_name": "Events Buyer",
      "security_name": "Events Buyer, LLC Initial Term Loan",
      "normalized_security_name": "events buyer initial term loan",
      "security_type": "Term Loan"
    }
  },
  "match": {
    "top_security": "Events Buyer, LLC Initial Term Loan",
    "family_confidence": 1.0,
    "security_confidence": 1.0,
    "matched": true,
    "match_type": "historical"
  }
}
```

| Field | Description |
|---|---|
| `mapped.mapped_family` | Master family name from the alias record |
| `mapped.mapped_security` | Master security name from the alias record |
| `mapped.filetype` | Source file type that created the alias record |
| `mapped.mapped_at` | ISO timestamp when the alias was ingested |
| `mapped.master_security_details` | Full master record fields |

---

## Match Types

| `match_type` | `is_mapped` | Description |
|---|---|---|
| `historical` | `true` | Exact match against a stored alias — same normalized security name |
| `indirect` | `true` | Stored alias matched but normalized security name differs (e.g. renamed security) |
| `direct` | `false` | Resolved through Elasticsearch fuzzy/phrase search |
| `unmatched` | `false` | No security found; `matched: false` |

---

## Confidence Scores

Both `family_confidence` and `security_confidence` are floats in `[0.0, 1.0]`.

- Scores are derived from raw Elasticsearch scores via logarithmic scaling:  
  `scaled = log(1 + raw_score) / log(1 + cap)` where `cap` defaults to `600`.
- `1.0` means the match came from a pre-stored alias record (certain).
- For search-based matches, a practical threshold for "high confidence" is **≥ 0.4** on both fields.

**Suggested decision logic:**

```
match_type == "historical" or "indirect"  →  accept (is_mapped = true, confidence 1.0)
match_type == "direct" AND family_confidence >= 0.4 AND security_confidence >= 0.4  →  accept
match_type == "direct" AND (family_confidence < 0.4 OR security_confidence < 0.4)   →  review
match_type == "unmatched"  →  escalate / manual review
```

---

## Code Examples

### Python

```python
import requests

BASE_URL = "https://<function-app-name>.azurewebsites.net/api"
API_KEY  = "<your-function-key>"

# Single item
response = requests.post(
    f"{BASE_URL}/map-security",
    headers={"x-functions-key": API_KEY, "Content-Type": "application/json"},
    json={
        "input": "Events Buyer, LLC",
        "security_input": "Events buyer Term loan"
    }
)
result = response.json()
print(result["match"]["match_type"], result["match"]["top_security"])

# Batch
items = [
    {"input": "Events Buyer, LLC",     "security_input": "Events buyer Term loan"},
    {"input": "AFC-Dell Holding Corp.", "security_input": "AFC-Dell Holding DD T/L (12/23)"},
]
batch_response = requests.post(
    f"{BASE_URL}/map-security",
    headers={"x-functions-key": API_KEY, "Content-Type": "application/json"},
    json=items
)
for item in batch_response.json():
    if "error" in item:
        print("ERROR:", item["error"])
    else:
        print(item["match"]["match_type"], item["match"]["top_security"])
```

### JavaScript / Node.js

```js
const BASE_URL = "https://<function-app-name>.azurewebsites.net/api";
const API_KEY  = "<your-function-key>";

async function mapSecurity(companyInput, securityInput) {
  const res = await fetch(`${BASE_URL}/map-security`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-functions-key": API_KEY,
    },
    body: JSON.stringify({ input: companyInput, security_input: securityInput }),
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

// Batch
async function mapSecuritiesBatch(items) {
  const res = await fetch(`${BASE_URL}/map-security`, {
    method: "POST",
    headers: { "Content-Type": "application/json", "x-functions-key": API_KEY },
    body: JSON.stringify(items),
  });
  return res.json();
}
```

### cURL

```bash
# Single item
curl -X POST "https://<function-app-name>.azurewebsites.net/api/map-security" \
  -H "Content-Type: application/json" \
  -H "x-functions-key: <your-function-key>" \
  -d '{"input": "Events Buyer, LLC", "security_input": "Events buyer Term loan"}'

# Batch
curl -X POST "https://<function-app-name>.azurewebsites.net/api/map-security" \
  -H "Content-Type: application/json" \
  -H "x-functions-key: <your-function-key>" \
  -d '[
    {"input": "Events Buyer, LLC", "security_input": "Events buyer Term loan"},
    {"input": "AFC-Dell Holding Corp.", "security_input": "AFC-Dell Holding DD T/L"}
  ]'
```

---

## Error Handling

| HTTP Status | Meaning |
|---|---|
| `200` | Success — check `match.matched` and `match.match_type` for result quality |
| `400` | Bad request — `input` field missing |
| `500` | Server error — Elasticsearch or PostgreSQL connection failure |

In batch mode the outer response is always `200`. Individual failed items carry an `"error"` key:

```json
[
  { "match": { ... } },
  { "error": "Input string is required" }
]
```

Always check for the `"error"` key per item when processing batch results.

---

## Tips for Integration

- **Abbreviations are handled automatically.** Send raw strings as they appear in source files (e.g. `T/L`, `DD T/L`, `R/C`). The API expands them before matching.
- **Legal suffixes can be included or omitted.** `Inc`, `LLC`, `Corp`, etc. are stripped during normalization.
- **Use batch mode for bulk processing.** A single batch call is more efficient than sequential single calls. There is no documented hard limit on batch size, but keep batches under 500 items per call for stable latency.
- **Cache `historical`/`indirect` results.** A `match_type` of `historical` or `indirect` means the exact input pair is stored in the alias index. These results are stable and can be cached without re-querying.
- **Log `company_query` and `security_query`** from the response `input` block for debugging — these are the normalized strings actually used for search.
