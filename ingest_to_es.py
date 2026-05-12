from elasticsearch import Elasticsearch, helpers
import csv
import json
import os
import uuid
from datetime import datetime, timezone


INDEX_NAME = os.environ.get("ES_INDEX", "security_master_v4")
INPUT_FILE = os.environ.get(
    "ES_INPUT",
    "pflt_security_mapping_unique_normalized.csv"
)

MAPPING_FILE = "es_index_mapping.json"


# ─────────────────────────────────────────────
# CONNECT ELASTICSEARCH
# ─────────────────────────────────────────────
def get_es():
    path = os.path.join(os.path.dirname(__file__), "local.settings.json")

    if not os.path.isfile(path):
        raise FileNotFoundError(f"Missing {path}")

    with open(path, encoding="utf-8") as f:
        config = json.load(f)["Values"]

    url = config.get("ES_URL")
    user = config.get("ES_USERNAME")
    pwd = config.get("ES_PASSWORD")

    if not url or not user or not pwd:
        raise ValueError(
            "Missing ES_URL / ES_USERNAME / ES_PASSWORD"
        )

    verify = (
        str(config.get("ES_VERIFY_CERTS", "true")).lower()
        == "true"
    )

    es = Elasticsearch(
        url,
        basic_auth=(user, pwd),
        verify_certs=verify
    )

    if not es.ping():
        raise ConnectionError(
            "Elasticsearch ping failed"
        )

    print("Connected to Elasticsearch")
    return es


# ─────────────────────────────────────────────
# CREATE INDEX
# ─────────────────────────────────────────────
def create_index(es):

    if es.indices.exists(index=INDEX_NAME):
        print(f"Index '{INDEX_NAME}' already exists")
        return

    with open(MAPPING_FILE, encoding="utf-8") as f:
        mapping = json.load(f)

    es.indices.create(
        index=INDEX_NAME,
        body=mapping
    )

    print(f"Index '{INDEX_NAME}' created")


# ─────────────────────────────────────────────
# SAFE COLUMN ACCESS
# ─────────────────────────────────────────────
def get_value(row, *keys):

    for key in keys:
        if key in row:
            return (row.get(key) or "").strip()

    return ""


# ─────────────────────────────────────────────
# GENERATE BULK ACTIONS
# ─────────────────────────────────────────────
def generate_actions():

    with open(
        INPUT_FILE,
        encoding="utf-8-sig",
        newline=""
    ) as f:

        reader = csv.DictReader(f)

        if not reader.fieldnames:
            raise ValueError("CSV missing header row")

        print("CSV Columns:", reader.fieldnames)

        # detect normalized column
        if "normalized_security_name" in reader.fieldnames:
            norm_col = "normalized_security_name"

        elif "normalized_name" in reader.fieldnames:
            norm_col = "normalized_name"

        else:
            raise ValueError(
                f"No normalized column found.\n"
                f"Available columns: {reader.fieldnames}"
            )

        soi_norm_col = (
            "normalized_soi_name"
            if "normalized_soi_name" in reader.fieldnames
            else None
        )

        for row in reader:

            # flexible security name matching
            sec = get_value(
                row,
                "Security Name",
                "security_name",
                "master_comp_security_name"
            )

            norm = (row.get(norm_col) or "").strip()

            if not sec or not norm or len(norm) < 3:
                continue

            doc = {

                "soi_name": get_value(
                    row,
                    "SOI Name",
                    "soi_name"
                ),

                "security_name": sec,

                "family_name": get_value(
                    row,
                    "Family Name",
                    "family_name"
                ),

                "security_type": get_value(
                    row,
                    "Security Type",
                    "security_type"
                ),

                # 🔥 CRITICAL FOR SEARCH (normalized_security_name used by multi_match)
                "normalized_name": norm,
                "normalized_security_name": norm,

                "normalized_soi_name": (
                    (row.get(soi_norm_col) or "").strip()
                    if soi_norm_col else ""
                ),

                "ingested_at": datetime.now(
                    timezone.utc
                ).isoformat()
            }

            # 🔥 UNIQUE ID
            # keeps ALL records
            doc_id = str(uuid.uuid4())

            yield {
                "_index": INDEX_NAME,
                "_id": doc_id,
                "_source": doc
            }


# ─────────────────────────────────────────────
# INGEST DATA
# ─────────────────────────────────────────────
def ingest(es):

    success = 0
    failed = 0

    for ok, result in helpers.streaming_bulk(
        es,
        generate_actions(),
        chunk_size=1000,
        raise_on_error=False
    ):

        if ok:
            success += 1

        else:
            failed += 1

            if failed <= 5:
                print("FAILED DOC:", result)

        # progress logging
        if (success + failed) % 1000 == 0:
            print(
                f"Processed: {success + failed} | "
                f"Success: {success} | "
                f"Failed: {failed}"
            )

    print(
        f"\nFINAL: {success} indexed, "
        f"{failed} failed"
    )

    return failed


# ─────────────────────────────────────────────
# VERIFY COUNT
# ─────────────────────────────────────────────
def verify_count(es):

    es.indices.refresh(index=INDEX_NAME)

    count = es.count(index=INDEX_NAME)["count"]

    print(
        f"Total documents in "
        f"'{INDEX_NAME}': {count}"
    )


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
if __name__ == "__main__":

    es = get_es()

    create_index(es)

    failed = ingest(es)

    verify_count(es)

    if failed:
        exit(1)