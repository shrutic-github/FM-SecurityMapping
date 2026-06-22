import json
import hashlib
from datetime import datetime, timezone
from elasticsearch import Elasticsearch
from normalization import normalize

# 1. Load config settings from local.settings.json
with open("local.settings.json") as f:
    config_data = json.load(f)["Values"]

ES_CLIENT = Elasticsearch(
    config_data.get("ES_URL"),
    basic_auth=(config_data.get("ES_USERNAME"), config_data.get("ES_PASSWORD")),
    verify_certs=config_data.get("ES_VERIFY_CERTS", "true").lower() == "true"
)
POSTGRES_CONN = config_data.get("POSTGRES_CONN")
ES_INDEX = config_data.get("ES_INDEX", "security_master_v4")

def store_manual_mapping(source_family, source_security, target_security_name, filetype="us bank cashfile", loan_type="", metadata=None):
    print(f"\nProcessing manual mapping for: '{source_security}' -> '{target_security_name}'")
    
    # 2. Normalize raw inputs
    norm_fam = normalize(source_family, POSTGRES_CONN)
    norm_sec = normalize(source_security, POSTGRES_CONN)
    
    # 3. Retrieve target master record to populate nested master_security_details
    master_res = ES_CLIENT.search(
        index=ES_INDEX,
        body={
            "size": 1,
            "query": {
                "bool": {
                    "must": [
                        { "term": { "security_name.keyword": target_security_name } }
                    ],
                    "must_not": [
                        { "term": { "is_alias": True } }  # Ensure we fetch primary master record, not an alias
                    ]
                }
            }
        }
    )
    
    master_hits = master_res.get("hits", {}).get("hits", [])
    master_doc = master_hits[0]["_source"] if master_hits else None
    
    if not master_doc:
        print(f"   [WARNING] Target master security '{target_security_name}' not found in index '{ES_INDEX}'. Storing with blank master fields.")
        master_doc = {}
    
    # 4. Construct the mapping document to match the updated schema
    mapping_doc = {
        "is_alias": True,
        "company_name": source_family,
        "normalized_company_name": norm_fam,
        "security_name": source_security,
        "normalized_security_name": norm_sec,
        "filetype": filetype,
        "loan_type": loan_type or (master_doc.get("security_type", "") if master_doc else ""),
        "master_security_details": master_doc,
        # flat searchable mirror of master_security_details
        "master_family_name":              master_doc.get("family_name", ""),
        "master_normalized_family_name":   master_doc.get("normalized_family_name", ""),
        "master_security_name":            master_doc.get("security_name", ""),
        "master_normalized_security_name": master_doc.get("normalized_security_name", ""),
        "master_soi_name":                 master_doc.get("soi_name", ""),
        "master_normalized_soi_name":      master_doc.get("normalized_soi_name", ""),
        "master_security_type":            master_doc.get("security_type", ""),
        "metadata": metadata or {},
        "ingested_at": datetime.now(timezone.utc).isoformat()
    }
    
    # 5. Generate deterministic ID
    doc_id = hashlib.sha256(
        (norm_fam + "|" + norm_sec).encode("utf-8")
    ).hexdigest()
    
    # 6. Index into Elasticsearch
    try:
        ES_CLIENT.index(
            index=ES_INDEX,
            id=doc_id,
            body=mapping_doc
        )
        print(f"   [SUCCESS] Document stored with ID: {doc_id}")
    except Exception as e:
        print(f"   [ERROR] Failed to index manual mapping: {e}")

# ==========================================
# Run manual mappings here
# ==========================================
if __name__ == "__main__":
    # Example usage:
    # Change these values to insert your manual overrides
    store_manual_mapping(
        source_family="PlayPower, Inc.",
        source_security="PlayPower T/L (5/19)",
        target_security_name="Planview (TL)",
        filetype="us bank cashfile",
        loan_type="Term Loan",
        metadata={
            "loanxid": "LX179874",
            "Identifier": "",
            "source_file_id": "",
        }
    )
