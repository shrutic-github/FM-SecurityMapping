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
| 3 | `GET` | `/api/view-mappings` | List all stored mappings / download as CSV |
| 4 | `PUT` | `/api/update-mappings/{id}` | Update a stored mapping |
| 5 | `DELETE` | `/api/delete-mappings/{id}` | Delete a stored mapping |

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
| `file_type` | string | No | Source file type label (e.g. `"us_bank_cashfile"`, `"trade_blotter"`). Echoed back in the response `input` block. Pass this as `filetype` when calling `POST /api/store-mappings`. |

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
| `input.file_type` | string | Echoed from the request item. Only present if `file_type` was supplied. Pass as `filetype` when calling `POST /api/store-mappings`. |
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
  "filetype": "us_bank_cashfile",
  "loan_type": "Term Loan",
  "metadata": {}
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `company_input` | string | Yes | Raw company name as it appears in the source system |
| `security_input` | string | Yes | Raw security name as it appears in the source system |
| `target_security_name` | string | Yes | Exact master security name to map to (use `match.top_security` from retrieval response) |
| `filetype` | string | No | Source file type label (e.g. `"us_bank_cashfile"`). Use the value from `input.file_type` in the retrieval response. Note the field is named `file_type` in Endpoint 1 input / response, but `filetype` here and in stored ES documents. |
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

## Endpoint 3 — List / Download All Mappings

### `GET /api/view-mappings`

Returns all stored (alias) mapping records from Elasticsearch. Supports pagination and CSV download.

**Headers**
```
x-functions-key: <key>
```

### Query Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `page` | integer | `1` | Page number (1-based) |
| `limit` | integer | `100` | Results per page. Maximum `1000`. |
| `format` | string | `json` | `json` for paginated JSON response; `csv` to download as a CSV file |

### Response — `200 OK` (JSON)

```json
{
  "page": 1,
  "limit": 100,
  "total": 342,
  "count": 100,
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

| Field | Description |
|---|---|
| `total` | Total number of stored mappings in the index |
| `count` | Number of results in this page |
| `results` | Array of mapping records |

### Response — `200 OK` (CSV, `?format=csv`)

Returns a `text/csv` file attachment named `mappings.csv`.

```
Content-Type: text/csv
Content-Disposition: attachment; filename=mappings.csv
```

Columns match the JSON result fields: `id`, `company_name`, `security_name`, `master_family_name`, `master_security_name`, `master_security_type`, `filetype`, `loan_type`, `ingested_at`.

### Error Responses

| Status | Body | Cause |
|---|---|---|
| `500` | `{"error": "<message>"}` | ES connection failure |

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
  "filetype": "manual_review",
  "loan_type": "Term Loan",
  "metadata": { "reviewed_by": "analyst_1" }
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `target_security_name` | string | No | New master security to point to. If omitted, existing master details are kept. |
| `filetype` | string | No | Updated file type label |
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

## Code Examples

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
# file_type from Endpoint 1 becomes filetype in Endpoint 2
store_res = requests.post(
    f"{BASE_URL}/store-mappings",
    headers=HEADERS,
    json={
        "company_input":        result["input"]["company_input"],
        "security_input":       result["input"]["security_input"],
        "target_security_name": result["match"]["top_security"],
        "filetype":             result["input"].get("file_type", ""),
    }
)
mapping_id = store_res.json()["id"]   # use this for update / delete

# 3. List all mappings (JSON)
view_res = requests.get(f"{BASE_URL}/view-mappings", headers=HEADERS, params={"page": 1, "limit": 50})
print(view_res.json()["total"], "total mappings")

# 4. Download as CSV
csv_res = requests.get(f"{BASE_URL}/view-mappings", headers=HEADERS, params={"format": "csv"})
with open("mappings.csv", "w") as f:
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

# 3. List mappings
curl "$BASE/view-mappings?page=1&limit=50" -H "x-functions-key: $KEY"

# 4. Download CSV
curl "$BASE/view-mappings?format=csv" -H "x-functions-key: $KEY" -o mappings.csv

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
- **`file_type` vs `filetype`**: Endpoint 1 request and response use `file_type` (underscore). Endpoint 2 request, Endpoint 4 request, and all stored ES documents use `filetype` (no underscore). Model them as separate properties in OpenAPI with a description cross-referencing each other.
