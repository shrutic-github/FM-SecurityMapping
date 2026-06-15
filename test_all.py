import pandas as pd
import requests
import time
from datetime import datetime
# -----------------------------
# CONFIG
# -----------------------------
API_URL = "http://localhost:7071/api/map-security"
INPUT_FILE = "Security_Mapping_TestCases.xlsx"
OUTPUT_FILE = "evaluation_results.xlsx"
TOP_K = 5
 
# -----------------------------
# BUILD INPUT
# borrower + asset/loan type
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
 
    # -----------------------------
    # Handle NaN
    # -----------------------------
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
 
    # =====================================================
    # CASE 1: Borrower exists
    # family_input  = borrower name only (clean for family retrieval)
    # security_input = security name (if present) or borrower + loan/asset type
    # =====================================================
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
 
    # =====================================================
    # CASE 2: Borrower missing — use security name for both
    # =====================================================
    elif security_name:
 
        return security_name, security_name
 
    return "", ""
 
 
 
# -----------------------------
# API CALL
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
 
        return {
            "error": f"HTTP {response.status_code}"
        }
 
    except Exception as e:
 
        return {
            "error": str(e)
        }
 
 
# -----------------------------
# MAIN EVALUATION
# -----------------------------
def run_evaluation():
 
    df = pd.read_excel(INPUT_FILE)
 
    results = []
 
    # -----------------------------
    # METRICS
    # -----------------------------
    family_top1_correct = 0
    family_topk_correct = 0
 
    security_top1_correct = 0
    security_topk_correct = 0
 
    # -----------------------------
    # LOOP
    # -----------------------------
    start_time = datetime.now()
    end_time = None
    for idx, row in df.iterrows():
        text_case_category = row["TestCase Catagory"]
        # if text_case_category != "Security Match":
        #     print(f"skipping {idx}")
        #     continue
 
        end_time = datetime.now()
        print(end_time - start_time)
 
        family_input, security_input = build_input(row)
 
        input_text = family_input
 
        source_file_type = str(
            row.get("Source file type", "")
        ).strip()
 
        source_file_type = (
            "" if source_file_type.lower() == "nan"
            else source_file_type
        ) or "Unknown"
 
        expected_family = str(
            row.get("Mastercomp Family Name", "")
        ).strip()
 
        expected_security = str(
            row.get("Mastercomp Security", "")
        ).strip()
 
 
        print(f"[{idx+1}] Testing: {input_text}")
        start_time = datetime.now()
 
        api_result = call_api(family_input, security_input)
 
        # -----------------------------
        # ERROR CASE
        # -----------------------------
        if "error" in api_result:
 
            results.append({
 
                "input": input_text,
                "source_file_type": source_file_type,
                "text_case_category": text_case_category,
 
                "expected_family": expected_family,
                "expected_security": expected_security,
 
                "predicted_family": None,
                "predicted_security": None,
 
                "family_correct": False,
                "family_topk_correct": False,
 
                "security_correct": False,
                "security_topk_correct": False,
 
                "family_rank": None,
                "security_rank": None,
 
                "status": "ERROR",
 
                "error": api_result["error"]
            })
 
            continue
 
        # =====================================================
        # FAMILY EVALUATION
        # =====================================================
 
        best_family = api_result.get(
            "best_family_match"
        ) or {}
 
        predicted_family = (
            best_family.get("family_name")
            or best_family.get("normalized_family_name")
        )
 
        family_correct = (
            predicted_family == expected_family
        )
 
        if family_correct:
            family_top1_correct += 1
 
        top_families = api_result.get(
            "top_matching_families",
            []
        )
 
        top_family_names = [
            x.get("family_name") or x.get("normalized_family_name")
            for x in top_families
        ]
 
        family_topk = (
            expected_family in top_family_names
        )
 
        if family_topk:
            family_topk_correct += 1
 
        family_rank = None
 
        if expected_family in top_family_names:
 
            family_rank = (
                top_family_names.index(expected_family) + 1
            )
 
        # =====================================================
        # SECURITY EVALUATION
        # =====================================================
 
        ranked_securities = api_result.get(
            "ranked_family_securities",
            []
        )
 
        predicted_security = (
         best_family.get("top_security")
         if best_family
        else None
    )
 
 
        security_correct = (
            predicted_security == expected_security
        )
 
        if security_correct:
            security_top1_correct += 1
 
        ranked_security_names = [
            s.get("security_name")
            for s in ranked_securities
        ]
 
        security_topk = (
            expected_security in ranked_security_names[:TOP_K]
        )
 
        if security_topk:
            security_topk_correct += 1
 
        security_rank = None
 
        if expected_security in ranked_security_names:
 
            security_rank = (
                ranked_security_names.index(
                    expected_security
                ) + 1
            )
 
        # =====================================================
        # STORE RESULT
        # =====================================================
 
        results.append({
 
            # -----------------------------
            # INPUT
            # -----------------------------
            "input": input_text,
            "source_file_type": source_file_type,
            "text_case_category": text_case_category,
            "security_input":api_result.get("security_input"),
            "family_query_to_es": api_result.get("normalized_input"),
 
            "expected_family": expected_family,
            "predicted_family": predicted_family,
            "family_correct": family_correct,
            "family_topk_correct": family_topk,
            "family_rank": family_rank,
 
            "expected_security": expected_security,
            "predicted_security": predicted_security,
            "security_correct": security_correct,
            "security_topk_correct": security_topk,
            "security_rank": security_rank,
 
           
 
            # -----------------------------
            # SCORES
            # -----------------------------
            "family_score": best_family.get("score"),
            "security_score": best_family.get("security_score"),
 
            # -----------------------------
            # DEBUG
            # -----------------------------
            "top_family_matches": " | ".join(
                [str(x) for x in top_family_names]
            ),
 
            "top_security_matches": " | ".join(
                [str(x) for x in ranked_security_names[:TOP_K]]
            ),
 
            "matched_flag": api_result.get(
                "matched"
            )
        })
 
        time.sleep(0.05)
 
    # =====================================================
    # FINAL METRICS
    # =====================================================
 
    total = len(results)
 
    family_top1_acc = (
        (family_top1_correct / total) * 100
        if total > 0 else 0
    )
 
    family_topk_acc = (
        (family_topk_correct / total) * 100
        if total > 0 else 0
    )
 
    security_top1_acc = (
        (security_top1_correct / total) * 100
        if total > 0 else 0
    )
 
    security_topk_acc = (
        (security_topk_correct / total) * 100
        if total > 0 else 0
    )
 
    # =====================================================
    # PRINT OVERALL METRICS
    # =====================================================
 
    print("\n=========================")
    print(f"Total Cases: {total}")
 
    print("\n----- FAMILY -----")
    print(f"Family Top-1 Accuracy: {family_top1_acc:.2f}%")
    print(f"Family Top-{TOP_K} Accuracy: {family_topk_acc:.2f}%")
 
    print("\n----- SECURITY -----")
    print(f"Security Top-1 Accuracy: {security_top1_acc:.2f}%")
    print(f"Security Top-{TOP_K} Accuracy: {security_topk_acc:.2f}%")
 
    print("=========================\n")
 
    # =====================================================
    # PER FILE TYPE METRICS
    # =====================================================
 
    result_df = pd.DataFrame(results)
 
    #result_df = result_df[result_df["text_case_category"] == "Security Match"]
 
    file_type_rows = []
 
    for file_type, group in result_df.groupby("source_file_type"):
 
        n = int(len(group))
 
        fam_top1_pass = int(group["family_correct"].sum())
        fam_topk_pass = int(group["family_topk_correct"].sum())
        sec_top1_pass = int(group["security_correct"].sum())
        sec_topk_pass = int(group["security_topk_correct"].sum())
 
        file_type_rows.append({
            "Source File Type":          file_type,
            "Total":                     n,
            "Fam Top-1 Pass":            fam_top1_pass,
            "Fam Top-1 Fail":            n - fam_top1_pass,
            "Fam Top-1 %":               f"{fam_top1_pass/n*100:.1f}%",
            f"Fam Top-{TOP_K} Pass":     fam_topk_pass,
            f"Fam Top-{TOP_K} Fail":     n - fam_topk_pass,
            f"Fam Top-{TOP_K} %":        f"{fam_topk_pass/n*100:.1f}%",
            "Sec Top-1 Pass":            sec_top1_pass,
            "Sec Top-1 Fail":            n - sec_top1_pass,
            "Sec Top-1 %":               f"{sec_top1_pass/n*100:.1f}%",
            f"Sec Top-{TOP_K} Pass":     sec_topk_pass,
            f"Sec Top-{TOP_K} Fail":     n - sec_topk_pass,
            f"Sec Top-{TOP_K} %":        f"{sec_topk_pass/n*100:.1f}%",
        })
 
    summary_df = pd.DataFrame(file_type_rows)
 
    print("\n--- Accuracy by File Type ---")
    print(summary_df.to_string(index=False))
    print()
 
    # =====================================================
    # SAVE OUTPUT
    # =====================================================
 
    with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
 
        result_df.to_excel(
            writer,
            sheet_name="Results",
            index=False
        )
 
        summary_df.to_excel(
            writer,
            sheet_name="By File Type",
            index=False
        )
 
    print(f"[OK] Results saved to {OUTPUT_FILE}")
 
 
# -----------------------------
# RUN
# -----------------------------
if __name__ == "__main__":
    run_evaluation()
 
 
 
 
 
 