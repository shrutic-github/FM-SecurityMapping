# Security Mapping API — Integration Guide

> Structured for conversion to OpenAPI / Apidog format.

---

## Overview

The Security Mapping API resolves raw portfolio input strings (company name + security description) to standardized master securities stored in Elasticsearch. It handles abbreviations (e.g. `T/L` → `term loan`), alias/rename chains, and fuzzy name variants.

---

## Base URL

| Environment | Base URL |
|---|---|
| Local development | `http://localhost:7071/api` |
| Azure (deployed) | `https://<function-app-name>.azurewebsites.net/api` |

---

## Authentication

Azure Function Apps support function-level keys.

**Header (recommended)**
```
x-functions-key: <your-function-key>
```

**Query string**
```
?code=<your-function-key>
```

Local development requires no key.

---

## Endpoints Summary

| # | Method | Route | Purpose |
|---|---|---|---|
| 1 | `POST` | `/api/security-mapping` | Resolve input strings to matching securities |
| 2 | `POST` | `/api/store-mappings` | Store a user-confirmed mapping |
| 3 | `GET` | `/api/view-mappings` | Filter mappings by company / security, list all, or download CSV |
| 4 | `PUT` | `/api/update-mappings/{id}` | Update a stored mapping |
| 5 | `DELETE` | `/api/delete-mappings/{id}` | Delete a stored mapping |
| 6 | `POST` | `/api/store-master-security` | Add a new master security record to the index |

---

## Endpoint 1 — Security Retrieval

### `POST /api/security-mapping`

Resolves one or more (company, security) input pairs to their best matching master securities. Always accepts a **JSON array** even for a single item.

**Headers**
```
Content-Type: application/json
x-functions-key: <key>
```

### Request Body

Array of objects:

```json
[
  {
    "company_input": "Events Buyer, LLC",
    "security_input": "Events buyer Term loan",
    "file_type": "us_bank_cashfile"
  },
  {
    "company_input": "AFC-Dell Holding Corp.",
    "security_input": "AFC-Dell Holding DD T/L (12/23)"
  }
]
```

| Field | Type | Required | Description |
|---|---|---|---|
| `company_input` | string | Yes | Raw company / issuer name |
| `security_input` | string | No | Raw security description. Defaults to `company_input` if omitted. |
| `file_type` | string | No | Source file type label (e.g. `"us_bank_cashfile"`, `"trade_blotter"`). Echoed back in the response `input` block. Pass the same value as `file_type` when calling `POST /api/store-mappings`. |

### Response — `200 OK`

Array of result objects in the same order as the request. A failed individual item returns `{"error": "<message>"}` without failing the whole batch.

**Shape A — Search match** (`is_mapped: false`)

Returned when the match is resolved through Elasticsearch fuzzy/phrase search.

```json
[
  {
    "input": {
      "company_input": "Events Buyer, LLC",
      "security_input": "Events buyer Term loan",
      "company_query": "events buyer",
      "security_query": "events buyer term loan",
      "file_type": "us_bank_cashfile"
    },
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
]
```

**Shape B — Mapped match** (`is_mapped: true`)

Returned when the input pair resolves directly to a pre-stored alias record (user-confirmed or historical mapping). Confidence is always `1.0`.

```json
[
  {
    "input": {
      "company_input": "Events Buyer, LLC",
      "security_input": "Events buyer Term loan",
      "company_query": "events buyer",
      "security_query": "events buyer term loan",
      "file_type": "us_bank_cashfile"
    },
    "is_mapped": true,
    "mapped": {
      "mapped_family": "Events Buyer, LLC",
      "mapped_security": "Events Buyer, LLC Initial Term Loan",
      "filetype": "us_bank_cashfile",
      "mapped_at": "2024-11-15T10:30:00+00:00",
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
]
```

### Response Fields

| Field | Type | Description |
|---|---|---|
| `input.company_input` | string | Original company string as sent |
| `input.security_input` | string | Original security string as sent |
| `input.company_query` | string | Normalized form used for ES search |
| `input.security_query` | string | Normalized form used for ES search |
| `input.file_type` | string | Echoed from the request item. Only present if `file_type` was supplied. Pass the same value as `file_type` when calling `POST /api/store-mappings`. |
| `is_mapped` | boolean | `true` if hit came from a pre-stored alias record |
| `match.top_security` | string | Best matching security name |
| `match.family_confidence` | float 0–1 | Confidence in the company/family match |
| `match.security_confidence` | float 0–1 | Confidence in the security match |
| `match.matched` | boolean | `true` if a security was found |
| `match.match_type` | string | `historical` \| `indirect` \| `direct` \| `unmatched` |
| `master_data` | object \| null | Resolved master record. Present only when `is_mapped: false`. `null` when unmatched. |
| `mapped` | object | Present only when `is_mapped: true`. Contains alias record details. |
| `candidates.top_families` | array | Up to 5 candidate families considered |
| `candidates.ranked_securities` | array | All securities evaluated within candidate families |

### Match Types

| `match_type` | `is_mapped` | Description |
|---|---|---|
| `historical` | `true` | Exact match on a stored alias — normalized security name matches exactly |
| `indirect` | `true` | Stored alias matched but normalized security name differs (e.g. renamed security) |
| `direct` | `false` | Resolved through Elasticsearch fuzzy/phrase search |
| `unmatched` | `false` | No security found; `matched: false` |

### Confidence Score Guidance

Scores are logarithmically scaled from raw Elasticsearch scores: `scaled = log(1 + raw) / log(1 + 600)`.

| Condition | Recommended action |
|---|---|
| `match_type` is `historical` or `indirect` | Accept — pre-confirmed, confidence = 1.0 |
| `direct` AND both confidences ≥ 0.4 | Accept |
| `direct` AND either confidence < 0.4 | Route to human review |
| `unmatched` | Escalate / manual lookup |

### Error Responses

| Status | Body | Cause |
|---|---|---|
| `400` | `{"error": "Payload must be a JSON array"}` | Request body is not an array |
| `500` | `{"error": "<message>"}` | ES or Postgres connection failure |

---

## Endpoint 2 — Store Confirmed Mapping

### `POST /api/store-mappings`

Stores a user-confirmed (company, security) → master security mapping in Elasticsearch. Future calls to `/api/security-mapping` with the same input will resolve instantly via the alias bypass without a full ES search.

**Headers**
```
Content-Type: application/json
x-functions-key: <key>
```

### Request Body

```json
{
  "company_input": "Events Buyer, LLC",
  "security_input": "Events buyer Term loan",
  "target_security_name": "Events Buyer, LLC Initial Term Loan",
  "file_type": "us_bank_cashfile",
  "loan_type": "Term Loan",
  "metadata": {}
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `company_input` | string | Yes | Raw company name as it appears in the source system |
| `security_input` | string | Yes | Raw security name as it appears in the source system |
| `target_security_name` | string | Yes | Exact master security name to map to (use `match.top_security` from retrieval response) |
| `file_type` | string | No | Source file type label (e.g. `"us_bank_cashfile"`). Use the value from `input.file_type` in the Endpoint 1 response. |
| `loan_type` | string | No | Loan/security type override. Defaults to master record's `security_type`. |
| `metadata` | object | No | Arbitrary key-value metadata to attach to the mapping record |

### Response — `201 Created`

```json
{
  "id": "a3f2c1d4e5b6...",
  "mapping": {
    "is_alias": true,
    "company_name": "Events Buyer, LLC",
    "normalized_company_name": "events buyer",
    "security_name": "Events buyer Term loan",
    "normalized_security_name": "events buyer term loan",
    "filetype": "us_bank_cashfile",
    "loan_type": "Term Loan",
    "master_family_name": "Events Buyer, LLC",
    "master_normalized_family_name": "events buyer",
    "master_security_name": "Events Buyer, LLC Initial Term Loan",
    "master_normalized_security_name": "events buyer initial term loan",
    "master_soi_name": "Events Buyer",
    "master_security_type": "Term Loan",
    "master_security_details": { },
    "metadata": {},
    "ingested_at": "2024-11-15T10:30:00+00:00"
  }
}
```

| Field | Description |
|---|---|
| `id` | SHA-256 hash of `normalized_company\|normalized_security`. Use this for GET / PUT / DELETE calls. |
| `mapping` | Full alias document written to Elasticsearch |

### Error Responses

| Status | Body | Cause |
|---|---|---|
| `400` | `{"error": "company_input, security_input, and target_security_name are required"}` | Missing required field |
| `409` | `{"error": "Mapping already exists", "id": "...", "existing_mapping": {...}}` | A mapping for this normalized pair already exists |
| `500` | `{"error": "<message>"}` | ES or Postgres connection failure |

---

## Endpoint 3 — View / Download Mappings

### `GET /api/view-mappings`

Three modes depending on which query parameters are supplied:

**Headers**
```
x-functions-key: <key>
```

### Query Parameters

| Parameter | Type | Description |
|---|---|---|
| `company_input` | string | Filter by company name (normalized before matching). Returns all stored mappings for that company. |
| `security_input` | string | Filter by security name (normalized before matching). Returns all stored mappings for that security. |
| `page` | integer | Page number for browsing all mappings (default `1`). Ignored when `company_input` or `security_input` is present. |
| `limit` | integer | Results per page (default `100`, max `1000`). Ignored when `company_input` or `security_input` is present. |
| `format` | string | Pass `csv` to download **all** mappings as a CSV file. Cannot be combined with `company_input`/`security_input`. |

### Behaviour summary

| Query params | Behaviour |
|---|---|
| `?company_input=...` | Returns mappings for that company only |
| `?security_input=...` | Returns mappings for that security only |
| `?company_input=...&security_input=...` | Returns mappings matching both (AND) |
| _(none)_ | Returns all mappings, paginated |
| `?format=csv` | Downloads all mappings as `all_mappings.csv` |

### Response — `200 OK` (filtered by company / security)

```json
{
  "count": 3,
  "results": [
    {
      "id": "a3f2c1d4e5b6...",
      "company_name": "Events Buyer, LLC",
      "security_name": "Events buyer Term loan",
      "master_family_name": "Events Buyer, LLC",
      "master_security_name": "Events Buyer, LLC Initial Term Loan",
      "master_security_type": "Term Loan",
      "filetype": "us_bank_cashfile",
      "loan_type": "Term Loan",
      "ingested_at": "2024-11-15T10:30:00+00:00"
    }
  ]
}
```

### Response — `200 OK` (all mappings, paginated)

```json
{
  "page": 1,
  "limit": 100,
  "total": 342,
  "count": 100,
  "results": [ { "id": "...", "company_name": "...", "..." : "..." } ]
}
```

| Field | Description |
|---|---|
| `total` | Total number of stored mappings in the index (only in paginated mode) |
| `count` | Number of results returned |
| `results` | Array of mapping records |

### Response — `200 OK` (CSV, `?format=csv`)

Returns a `text/csv` file attachment named `all_mappings.csv`.

```
Content-Type: text/csv
Content-Disposition: attachment; filename=all_mappings.csv
```

Columns: `id`, `company_name`, `security_name`, `master_family_name`, `master_security_name`, `master_security_type`, `filetype`, `loan_type`, `ingested_at`.

### Error Responses

| Status | Body | Cause |
|---|---|---|
| `404` | `{"error": "No mappings found for the given input"}` | Filter returned no results |
| `500` | `{"error": "<message>"}` | ES or Postgres connection failure |

---

## Endpoint 4 — Update Mapping

### `PUT /api/update-mappings/{id}`

Updates an existing mapping record. Use this to re-point a stored mapping to a different master security or update metadata. The `id` is returned by `POST /api/store-mappings`.

**Headers**
```
Content-Type: application/json
x-functions-key: <key>
```

### Path Parameters

| Parameter | Type | Description |
|---|---|---|
| `id` | string | SHA-256 mapping ID returned by the store endpoint |

### Request Body

All fields are optional. Omitted fields retain their existing values.

```json
{
  "target_security_name": "Events Buyer, LLC Amended Term Loan",
  "file_type": "manual_review",
  "loan_type": "Term Loan",
  "metadata": { "reviewed_by": "analyst_1" }
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `target_security_name` | string | No | New master security to point to. If omitted, existing master details are kept. |
| `file_type` | string | No | Updated file type label |
| `loan_type` | string | No | Updated loan type |
| `metadata` | object | No | Replaces existing metadata entirely |

### Response — `200 OK`

```json
{
  "id": "a3f2c1d4e5b6...",
  "mapping": {
    "company_name": "Events Buyer, LLC",
    "security_name": "Events buyer Term loan",
    "master_security_name": "Events Buyer, LLC Amended Term Loan",
    "ingested_at": "2024-11-16T08:00:00+00:00"
  }
}
```

### Error Responses

| Status | Body | Cause |
|---|---|---|
| `404` | `{"error": "Mapping not found"}` | No document with that ID exists |
| `500` | `{"error": "<message>"}` | ES connection failure |

---

## Endpoint 5 — Delete Mapping

### `DELETE /api/delete-mappings/{id}`

Permanently removes a stored mapping from Elasticsearch. After deletion, future retrieval calls for the same input will fall back to the full ES search.

**Headers**
```
x-functions-key: <key>
```

### Path Parameters

| Parameter | Type | Description |
|---|---|---|
| `id` | string | SHA-256 mapping ID returned by the store endpoint |

### Response — `200 OK`

```json
{
  "deleted": true,
  "id": "a3f2c1d4e5b6..."
}
```

### Error Responses

| Status | Body | Cause |
|---|---|---|
| `404` | `{"error": "Mapping not found"}` | No document with that ID exists |
| `500` | `{"error": "<message>"}` | ES connection failure |

---

## Endpoint 6 — Store Master Security

### `POST /api/store-master-security`

Adds a new master (non-alias) security record to the Elasticsearch index. Use this when a security does not yet exist in the index and you want to make it available for future matching.

**Headers**
```
Content-Type: application/json
x-functions-key: <key>
```

### Request Body

```json
{
  "soi_name": "Events Buyer",
  "family_name": "Events Buyer, LLC",
  "master_comp_security_name": "Events Buyer, LLC Initial Term Loan",
  "security_type": "Term Loan"
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `soi_name` | string | Yes | Schedule of Investments name (short company name) |
| `family_name` | string | Yes | Full legal entity / family name |
| `master_comp_security_name` | string | Yes | Full master security name to index |
| `security_type` | string | Yes | Type of security (e.g. `"Term Loan"`, `"Revolver"`, `"Common Equity"`) |

### Response — `201 Created`

```json
{
  "message": "Master security stored successfully",
  "id": "<es-auto-generated-id>",
  "document": {
    "soi_name": "Events Buyer",
    "family_name": "Events Buyer, LLC",
    "security_name": "Events Buyer, LLC Initial Term Loan",
    "security_type": "Term Loan",
    "normalized_security_name": "events buyer initial term loan",
    "normalized_soi_name": "events buyer",
    "normalized_family_name": "events buyer",
    "ingested_at": "2024-11-15T10:30:00+00:00"
  }
}
```

> **Note:** The `id` returned here is an Elasticsearch auto-generated ID, not a SHA-256 hash. It is not used by the other endpoints.

### Error Responses

| Status | Body | Cause |
|---|---|---|
| `400` | `{"error": "soi_name, family_name, master_comp_security_name and security_type are required"}` | Missing required field |
| `409` | `{"error": "Master security already exists", "id": "...", "existing_document": {...}}` | A master record with the same normalized security + family name already exists |
| `500` | `{"error": "<message>"}` | ES or Postgres connection failure |

---

## Code Examples

### JavaScript / React — Full workflow

```js
const BASE = "https://<function-app-name>.azurewebsites.net/api";
const HEADERS = {
  "Content-Type": "application/json",
  "x-functions-key": "<your-function-key>",
};

// 1. Resolve a (company, security) pair
const searchRes = await fetch(`${BASE}/security-mapping`, {
  method: "POST",
  headers: HEADERS,
  body: JSON.stringify([
    { company_input: "Events Buyer, LLC", security_input: "Events buyer Term loan" }
  ]),
});
const [result] = await searchRes.json();
const mappingId = null; // populated after store

// 2. User confirms the match — store it
if (result.match.matched) {
  const storeRes = await fetch(`${BASE}/store-mappings`, {
    method: "POST",
    headers: HEADERS,
    body: JSON.stringify({
      company_input: result.input.company_input,
      security_input: result.input.security_input,
      target_security_name: result.match.top_security,
      file_type: result.input.file_type ?? "",
    }),
  });
  if (storeRes.status === 409) {
    const conflict = await storeRes.json();
    console.log("Already mapped:", conflict.existing_mapping);
  } else {
    const stored = await storeRes.json();
    mappingId = stored.id; // store this for update / delete
  }
}

// 3a. View all mappings for a company
const byCompany = await fetch(
  `${BASE}/view-mappings?company_input=Events+Buyer+LLC`,
  { headers: HEADERS }
);
const { count, results } = await byCompany.json();

// 3b. View all mappings for a security name
const bySecurity = await fetch(
  `${BASE}/view-mappings?security_input=Events+buyer+Term+loan`,
  { headers: HEADERS }
);

// 3c. Browse all mappings paginated
const allMappings = await fetch(
  `${BASE}/view-mappings?page=1&limit=50`,
  { headers: HEADERS }
);

// 3d. Download all mappings as CSV
const csvRes = await fetch(`${BASE}/view-mappings?format=csv`, { headers: HEADERS });
const blob = await csvRes.blob();
const url = URL.createObjectURL(blob);
const a = document.createElement("a");
a.href = url;
a.download = "all_mappings.csv";
a.click();

// 4. Update a mapping (re-point to a different master security)
await fetch(`${BASE}/update-mappings/${mappingId}`, {
  method: "PUT",
  headers: HEADERS,
  body: JSON.stringify({ target_security_name: "Events Buyer, LLC Amended Term Loan" }),
});

// 5. Delete a mapping
await fetch(`${BASE}/delete-mappings/${mappingId}`, {
  method: "DELETE",
  headers: HEADERS,
});
```

### Python — Full workflow

```python
import requests

BASE_URL = "https://<function-app-name>.azurewebsites.net/api"
HEADERS  = {"x-functions-key": "<key>", "Content-Type": "application/json"}

# 1. Retrieve matching securities (pass file_type per item if available)
res = requests.post(
    f"{BASE_URL}/security-mapping",
    headers=HEADERS,
    json=[
        {
            "company_input":  "Events Buyer, LLC",
            "security_input": "Events buyer Term loan",
            "file_type":      "us_bank_cashfile",   # optional; echoed back in response
        }
    ]
)
result = res.json()[0]
print(result["match"]["match_type"], result["match"]["top_security"])

# 2. User accepts the match — store it
store_res = requests.post(
    f"{BASE_URL}/store-mappings",
    headers=HEADERS,
    json={
        "company_input":        result["input"]["company_input"],
        "security_input":       result["input"]["security_input"],
        "target_security_name": result["match"]["top_security"],
        "file_type":            result["input"].get("file_type", ""),
    }
)
mapping_id = store_res.json()["id"]   # use this for update / delete

# 3a. Filter by company name
company_res = requests.get(f"{BASE_URL}/view-mappings", headers=HEADERS,
                           params={"company_input": "Events Buyer, LLC"})
print(company_res.json()["count"], "mappings for this company")

# 3b. Filter by security name
sec_res = requests.get(f"{BASE_URL}/view-mappings", headers=HEADERS,
                       params={"security_input": "Events buyer Term loan"})

# 3c. All mappings paginated
view_res = requests.get(f"{BASE_URL}/view-mappings", headers=HEADERS, params={"page": 1, "limit": 50})
print(view_res.json()["total"], "total mappings")

# 3d. Download as CSV (always all mappings)
csv_res = requests.get(f"{BASE_URL}/view-mappings", headers=HEADERS, params={"format": "csv"})
with open("all_mappings.csv", "w") as f:
    f.write(csv_res.text)

# 5. Update a mapping
requests.put(
    f"{BASE_URL}/update-mappings/{mapping_id}",
    headers=HEADERS,
    json={"target_security_name": "Events Buyer, LLC Amended Term Loan"}
)

# 6. Delete a mapping
requests.delete(f"{BASE_URL}/delete-mappings/{mapping_id}", headers=HEADERS)
```

### cURL

```bash
BASE="https://<function-app-name>.azurewebsites.net/api"
KEY="<your-function-key>"

# 1. Retrieve (file_type is optional per item)
curl -X POST "$BASE/security-mapping" \
  -H "Content-Type: application/json" \
  -H "x-functions-key: $KEY" \
  -d '[{"company_input": "Events Buyer, LLC", "security_input": "Events buyer Term loan", "file_type": "us_bank_cashfile"}]'

# 2. Store confirmed mapping
curl -X POST "$BASE/store-mappings" \
  -H "Content-Type: application/json" \
  -H "x-functions-key: $KEY" \
  -d '{
    "company_input": "Events Buyer, LLC",
    "security_input": "Events buyer Term loan",
    "target_security_name": "Events Buyer, LLC Initial Term Loan"
  }'

# 3a. Filter by company
curl "$BASE/view-mappings?company_input=Events+Buyer+LLC" -H "x-functions-key: $KEY"

# 3b. Filter by security
curl "$BASE/view-mappings?security_input=Events+buyer+Term+loan" -H "x-functions-key: $KEY"

# 3c. All mappings paginated
curl "$BASE/view-mappings?page=1&limit=50" -H "x-functions-key: $KEY"

# 3d. Download all as CSV
curl "$BASE/view-mappings?format=csv" -H "x-functions-key: $KEY" -o all_mappings.csv

# 6. Store a new master security
curl -X POST "$BASE/store-master-security" \
  -H "Content-Type: application/json" \
  -H "x-functions-key: $KEY" \
  -d '{
    "soi_name": "Events Buyer",
    "family_name": "Events Buyer, LLC",
    "master_comp_security_name": "Events Buyer, LLC Initial Term Loan",
    "security_type": "Term Loan"
  }'

# 5. Update
curl -X PUT "$BASE/update-mappings/<id>" \
  -H "Content-Type: application/json" \
  -H "x-functions-key: $KEY" \
  -d '{"target_security_name": "Events Buyer, LLC Amended Term Loan"}'

# 6. Delete
curl -X DELETE "$BASE/delete-mappings/<id>" -H "x-functions-key: $KEY"
```

---

## Global Error Reference

| Status | Meaning |
|---|---|
| `200` | Success |
| `201` | Resource created |
| `400` | Bad request — missing or invalid fields |
| `404` | Resource not found |
| `409` | Conflict — duplicate mapping |
| `500` | Server error — check ES / Postgres connectivity |

---

## Notes for OpenAPI Conversion

When importing this document into Apidog / Swagger:

- **Server**: set `url` to the Base URL table above (use variables for environment).
- **Security scheme**: `apiKey` in header, name `x-functions-key`.
- **`id` path parameter**: type `string`, format `hex`, description "SHA-256 of normalized_company|normalized_security".
- **Endpoint 1 request**: schema type `array`, items are the per-item object schema.
- **Endpoint 1 response**: schema type `array`, items use `oneOf` with Shape A and Shape B discriminated by `is_mapped`.
- **Endpoint 3 `format=csv`**: model as a separate response with `content: text/csv`.
- **`match_type` enum**: `["historical", "indirect", "direct", "unmatched"]`.
- **`family_confidence` / `security_confidence`**: type `number`, format `float`, minimum `0`, maximum `1`.
- **`file_type` vs `filetype`**: All request bodies use `file_type` (with underscore). Responses that return stored ES documents (store, update, view) return the field as `filetype` (no underscore) because that is the index field name. When reading a mapping response and re-submitting it, map `response.filetype` → `file_type` in the request.
