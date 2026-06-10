import pandas as pd
import json
import hashlib
from datetime import datetime, timezone
from elasticsearch import Elasticsearch
from normalization import normalize

INPUT_FILE = "Security_Mapping_TestCases.xlsx"

# 1. Load configuration and initialize ES Client
with open("local.settings.json") as f:
    config_data = json.load(f)["Values"]

ES_CLIENT = Elasticsearch(
    config_data.get("ES_URL"),
    basic_auth=(config_data.get("ES_USERNAME"), config_data.get("ES_PASSWORD")),
    verify_certs=config_data.get("ES_VERIFY_CERTS", "true").lower() == "true"
)
POSTGRES_CONN = config_data.get("POSTGRES_CONN")
ES_INDEX = config_data.get("ES_INDEX", "security_master_v4")

print(f"Connecting to ES: {config_data.get('ES_URL')} | Index: {ES_INDEX}")

# 2. Build input helper (reused from test_all.py)
def build_input(row):
    borrower = str(row.get("Borrower/Company/Issuer Name", "")).strip()
    security_name = str(row.get("Security Name", "")).strip()
    loan_asset_type = str(row.get("Loan/Asset Type", "")).strip()
    
    borrower = "" if borrower.lower() == "nan" else borrower
    security_name = "" if security_name.lower() == "nan" else security_name
    loan_asset_type = "" if loan_asset_type.lower() == "nan" else loan_asset_type
    
    if borrower:
        family_input = borrower
        if security_name:
            security_input = security_name
        else:
            security_input = " ".join([borrower, loan_asset_type]).strip() if loan_asset_type else borrower
        return family_input, security_input
    elif security_name:
        return security_name, security_name
    return "", ""

# 3. Read the excel sheet
df = pd.read_excel(INPUT_FILE)
print(f"Loaded {len(df)} test cases from {INPUT_FILE}.")

stored_count = 0
for idx, row in df.iterrows():
    if stored_count >= 5:
        break
        
    family_input, security_input = build_input(row)
    expected_family = str(row.get("Mastercomp Family Name", "")).strip()
    expected_security = str(row.get("Mastercomp Security", "")).strip()
    source_file_type = str(row.get("Source file type", "")).strip()
    source_file_type = "" if source_file_type.lower() == "nan" else source_file_type
    source_file_type = source_file_type or "Unknown"
    
    if not expected_security or expected_security.lower() == "nan":
        print(f"[{idx+1}] Skipping: no expected security name")
        continue

    print(f"\n[{idx+1}] Processing mapping for raw input:")
    print(f"   Family Input:  '{family_input}'")
    print(f"   Security Input: '{security_input}'")
    print(f"   Target Security: '{expected_security}'")
    
    try:
        # Find original master document
        master_res = ES_CLIENT.search(
            index=ES_INDEX,
            body={
                "size": 1,
                "query": {
                    "term": {
                        "security_name.keyword": expected_security
                    }
                }
            }
        )
        master_hits = master_res.get("hits", {}).get("hits", [])
        master_doc = master_hits[0]["_source"] if master_hits else None
        
        if not master_doc:
            print(f"   [WARNING] Target master security '{expected_security}' not found in master data index!")
        
        # Normalize raw inputs
        norm_fam = normalize(family_input, POSTGRES_CONN)
        norm_sec = normalize(security_input, POSTGRES_CONN)
        
        # Build mapping document as per user's strict structure
        mapping_doc = {
            "is_alias": True,
            "normalized_security_name": norm_sec,
            "normalized_family_name": norm_fam,
            
            "filetype": source_file_type,
            "company_name": family_input,             # raw input company
            "security_name": security_input,           # raw input security description
            "ids": str(idx),
            "loan_type": str(row.get("Loan/Asset Type") or ""),
            "mapped_security_name": expected_security, # target mapped security
            "mapped_family_name": expected_family,     # target mapped family
            "master_security_details": master_doc,     # nested master record copy
            "ingested_at": datetime.now(timezone.utc).isoformat()
        }
        
        # Generate deterministic ID
        doc_id = hashlib.sha256(f"{norm_fam}|{norm_sec}".encode("utf-8")).hexdigest()
        
        # Index document
        print(f"   Indexing mapping document with ID '{doc_id}'...")
        ES_CLIENT.index(
            index=ES_INDEX,
            id=doc_id,
            body=mapping_doc
        )
        stored_count += 1
        print(f"   Successfully stored mapped security #{stored_count}")
        
    except Exception as e:
        print(f"   [ERROR] Failed to save mapping: {e}")

print(f"\nDone! Stored {stored_count} mapped records directly in Elasticsearch.")
