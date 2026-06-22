import pandas as pd
import requests
import concurrent.futures
from datetime import datetime

API_URL = "http://localhost:7071/api/map-security"
INPUT_FILE = "Security_Mapping_TestCases.xlsx"
OUTPUT_FILE = "batch_test_results.xlsx"
MAX_WORKERS = 10


# -----------------------------
# BUILD INPUT (same logic as test_all.py)
# -----------------------------
def build_input(row):

    borrower = str(
        row.get("Borrower/Company/Issuer Name", "")
    ).strip()

    security_name = str(
        row.get("Security Name", "")
    ).strip()

    loan_asset_type = str(
        row.get("Loan/Asset Type", "")
    ).strip()

    borrower = "" if borrower.lower() == "nan" else borrower

    security_name = (
        ""
        if security_name.lower() == "nan"
        else security_name
    )

    loan_asset_type = (
        ""
        if loan_asset_type.lower() == "nan"
        else loan_asset_type
    )

    if borrower:
        family_input = borrower

        if security_name:
            security_input = security_name
        else:
            security_input = (
                " ".join([borrower, loan_asset_type]).strip()
                if loan_asset_type
                else borrower
            )

        return family_input, security_input

    elif security_name:
        return security_name, security_name

    return "", ""


# -----------------------------
# API CALL (same logic as test_all.py)
# -----------------------------
def call_api(family_input, security_input):

    try:
        response = requests.post(
            API_URL,
            json={
                "input": family_input,
                "security_input": security_input
            },
            timeout=10
        )

        if response.status_code == 200:
            return response.json()

        return {"error": f"HTTP {response.status_code}"}

    except Exception as e:
        return {"error": str(e)}


# -----------------------------
# PROCESS A SINGLE ROW
# -----------------------------
def process_row(row):

    text_case_category = row.get("TestCase Catagory")

    family_input, security_input = build_input(row)
    input_text = family_input

    source_file_type = str(row.get("Source file type", "")).strip()
    source_file_type = (
        "" if source_file_type.lower() == "nan" else source_file_type
    ) or "Unknown"

    expected_family = str(row.get("Mastercomp Family Name", "")).strip()
    expected_security = str(row.get("Mastercomp Security", "")).strip()

    api_result = call_api(family_input, security_input)

    if "error" in api_result:
        return {
            "input": input_text,
            "source_file_type": source_file_type,
            "text_case_category": text_case_category,
            "expected_family": expected_family,
            "expected_security": expected_security,
            "predicted_family": None,
            "predicted_security": None,
            "match_type": None,
            "matched_flag": False,
            "status": "ERROR",
            "error": api_result["error"],
        }

    match_info = api_result.get("match") or {}
    match_type = match_info.get("match_type")

    # Historical/indirect (bypass) responses nest the master record
    # under mapped.master_security_details instead of master_data.
    if match_type in ("historical", "indirect"):
        master_data = (api_result.get("mapped") or {}).get(
            "master_security_details"
        ) or {}
    else:
        master_data = api_result.get("master_data") or {}

    predicted_family = (
        master_data.get("family_name")
        or master_data.get("normalized_family_name")
    )
    predicted_security = master_data.get("security_name")

    return {
        "input": input_text,
        "source_file_type": source_file_type,
        "text_case_category": text_case_category,
        "expected_family": expected_family,
        "predicted_family": predicted_family,
        "family_correct": predicted_family == expected_family,
        "expected_security": expected_security,
        "predicted_security": predicted_security,
        "security_correct": predicted_security == expected_security,
        "match_type": match_type,
        "matched_flag": match_info.get("matched"),
        "status": "OK",
        "error": None,
    }


# -----------------------------
# RUN BATCH CONCURRENTLY
# -----------------------------
def run_batch():

    df = pd.read_excel(INPUT_FILE)

    print(
        f"Loaded {len(df)} rows. "
        f"Sending requests concurrently (max_workers={MAX_WORKERS})..."
    )

    start_time = datetime.now()
    results = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

        futures = {
            executor.submit(process_row, row): idx
            for idx, row in df.iterrows()
        }

        for future in concurrent.futures.as_completed(futures):
            idx = futures[future]
            try:
                result = future.result()
                results.append(result)
                print(
                    f"[{idx + 1}] {result['input']} -> "
                    f"{result.get('predicted_family')} | "
                    f"{result.get('predicted_security')}"
                )
            except Exception as e:
                print(f"[{idx + 1}] ERROR: {e}")

    elapsed = datetime.now() - start_time
    print(f"\nCompleted {len(results)} requests in {elapsed}")

    result_df = pd.DataFrame(results)
    result_df.to_excel(OUTPUT_FILE, index=False)
    print(f"[OK] Results saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    run_batch()
