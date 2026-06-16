import pandas as pd
import json
import hashlib
from datetime import datetime, timezone
from elasticsearch import Elasticsearch
from normalization import normalize, resolve_conn_string

INPUT_FILE = "Security_Mapping_TestCases.xlsx"

# --------------------------------------------------
# Config
# --------------------------------------------------
with open("local.settings.json") as f:
    config_data = json.load(f)["Values"]

ES_CLIENT = Elasticsearch(
    config_data.get("ES_URL"),
    basic_auth=(
        config_data.get("ES_USERNAME"),
        config_data.get("ES_PASSWORD")
    ),
    verify_certs=(
        config_data.get(
            "ES_VERIFY_CERTS",
            "true"
        ).lower() == "true"
    )
)

try:
    POSTGRES_CONN = resolve_conn_string()
except Exception:
    POSTGRES_CONN = config_data.get("POSTGRES_CONN")

ES_INDEX = config_data.get(
    "ES_INDEX",
    "security_master_v4"
)

print(
    f"Connected to ES: "
    f"{config_data.get('ES_URL')} "
    f"| Index={ES_INDEX}"
)

# --------------------------------------------------
# Load sheet and locate header row
# --------------------------------------------------
sheet_name = "Test Results 9June"

raw_df = pd.read_excel(
    INPUT_FILE,
    sheet_name=sheet_name,
    header=None
)

header_row = None

for i in range(len(raw_df)):
    vals = [
        str(v).strip().lower()
        for v in raw_df.iloc[i].tolist()
        if pd.notna(v)
    ]

    if (
        "expected_security" in vals
        and "predicted_security" in vals
    ):
        header_row = i
        break

if header_row is None:
    raise Exception(
        "Could not locate header row."
    )

print(
    f"Found header row at Excel row "
    f"{header_row + 1}"
)

df = pd.read_excel(
    INPUT_FILE,
    sheet_name=sheet_name,
    header=header_row
)

df.columns = [
    str(c).strip()
    for c in df.columns
]

print(
    f"Loaded {len(df)} records"
)

# --------------------------------------------------
# Helper
# --------------------------------------------------
def clean(v):
    if pd.isna(v):
        return ""

    s = str(v).strip()

    if s.lower() in (
        "nan",
        "none",
        "null"
    ):
        return ""

    return s


def build_inputs(row):
    family_input = clean(
        row.get("input")
    )

    security_input = clean(
        row.get("security_input")
    )

    if not family_input:
        family_input = clean(
            row.get(
                "Borrower/Company/Issuer Name"
            )
        )

    if not security_input:
        security_input = clean(
            row.get("Security Name")
        )

    return (
        family_input,
        security_input
    )


# --------------------------------------------------
# Processing
# --------------------------------------------------
stored_count = 0
skipped_count = 0

for idx, row in df.iterrows():

    expected_security = clean(
        row.get("expected_security")
    )

    predicted_security = clean(
        row.get("predicted_security")
    )

    expected_family = clean(
        row.get("expected_family")
    )

    matched_flag = clean(
        row.get("matched_flag")
    ).upper()

    security_correct = clean(
        row.get("security_correct")
    ).upper()

    # ------------------------------------------
    # Store ONLY correct mappings
    # ------------------------------------------
    if (
        expected_security.lower()
        != predicted_security.lower()
    ):
        skipped_count += 1
        continue

    if not expected_security:
        skipped_count += 1
        continue

    family_input, security_input = (
        build_inputs(row)
    )

    if (
        not family_input
        or not security_input
    ):
        skipped_count += 1
        continue

    try:

        norm_fam = normalize(
            family_input,
            POSTGRES_CONN
        )

        norm_sec = normalize(
            security_input,
            POSTGRES_CONN
        )

        # --------------------------------------
        # Fetch master security
        # --------------------------------------
        master_res = ES_CLIENT.search(
            index=ES_INDEX,
            body={
                "size": 1,
                "query": {
                    "term": {
                        "security_name.keyword":
                        expected_security
                    }
                }
            }
        )

        hits = (
            master_res
            .get("hits", {})
            .get("hits", [])
        )

        master_doc = (
            hits[0]["_source"]
            if hits
            else None
        )

        if not master_doc:

            print(
                f"[{idx+1}] "
                f"Master security not found: "
                f"{expected_security}"
            )

            master_doc = {
                "family_name":
                    expected_family,

                "security_name":
                    expected_security,

                "normalized_family_name":
                    norm_fam,

                "normalized_security_name":
                    norm_sec,
            }

        # --------------------------------------
        # Dynamic metadata
        # --------------------------------------
        metadata = {}

        for col in row.index:

            col_name = str(col)

            if any(
                k in col_name.lower()
                for k in [
                    "id",
                    "loanxid",
                    "identifier",
                    "cusip",
                    "isin",
                    "ticker",
                    "ref"
                ]
            ):
                value = row[col]

                if pd.notna(value):
                    metadata[col_name] = str(
                        value
                    ).strip()

        # --------------------------------------
        # Historical mapping document
        # --------------------------------------
        mapping_doc = {

            "is_alias": True,

            "company_name":
                family_input,

            "normalized_company_name":
                norm_fam,

            "security_name":
                security_input,

            "normalized_security_name":
                norm_sec,

            "filetype":
                clean(
                    row.get(
                        "Source file type"
                    )
                ),

            "loan_type":
                clean(
                    row.get(
                        "Loan/Asset Type"
                    )
                ),

            "master_security_details":
                master_doc,

            "metadata":
                metadata,

            "ingested_at":
                datetime.now(
                    timezone.utc
                ).isoformat()
        }

        doc_id = hashlib.sha256(
            (
                norm_fam
                + "|"
                + norm_sec
            ).encode("utf-8")
        ).hexdigest()

        ES_CLIENT.index(
            index=ES_INDEX,
            id=doc_id,
            body=mapping_doc
        )

        stored_count += 1

        print(
            f"[{idx+1}] Stored -> "
            f"{family_input} | "
            f"{security_input}"
        )

    except Exception as e:

        print(
            f"[{idx+1}] ERROR: "
            f"{str(e)}"
        )

print()
print(
    f"Stored: {stored_count}"
)
print(
    f"Skipped: {skipped_count}"
)
