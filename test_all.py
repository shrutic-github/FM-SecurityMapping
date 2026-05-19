import pandas as pd
import requests
import time
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
    # CASE 1:
    # Borrower exists
    # Build:
    # borrower + loan/asset type
    # =====================================================
    if borrower:

        instrument = loan_asset_type

        parts = [borrower]

        if instrument:
            parts.append(instrument)

        return " ".join(parts).strip()

    # =====================================================
    # CASE 2:
    # Borrower missing
    # Use full security name
    # =====================================================
    elif security_name:

        return security_name

    return ""


# -----------------------------
# API CALL
# -----------------------------
def call_api(input_text):

    try:

        response = requests.post(
            API_URL,
            json={"input": input_text},
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
    for idx, row in df.iterrows():

        input_text = build_input(row)

        expected_family = str(
            row.get("Mastercomp Family Name", "")
        ).strip()

        expected_security = str(
            row.get("Mastercomp Security", "")
        ).strip()

        print(f"[{idx+1}] Testing: {input_text}")

        api_result = call_api(input_text)

        # -----------------------------
        # ERROR CASE
        # -----------------------------
        if "error" in api_result:

            results.append({

                "input": input_text,

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

        predicted_family = best_family.get(
            "family_name"
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
            x.get("family_name")
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
                top_family_names.index(
                    expected_family
                ) + 1
            )

        # =====================================================
        # SECURITY EVALUATION
        # =====================================================

        ranked_securities = api_result.get(
            "ranked_family_securities",
            []
        )

        predicted_security = None

        if ranked_securities:
            predicted_security = ranked_securities[0].get(
                "security_name"
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

            # -----------------------------
            # EXPECTED
            # -----------------------------
            "expected_family": expected_family,
            "expected_security": expected_security,

            # -----------------------------
            # PREDICTED
            # -----------------------------
            "predicted_family": predicted_family,
            "predicted_security": predicted_security,

            # -----------------------------
            # FAMILY METRICS
            # -----------------------------
            "family_correct": family_correct,

            "family_topk_correct": family_topk,

            "family_rank": family_rank,

            # -----------------------------
            # SECURITY METRICS
            # -----------------------------
            "security_correct": security_correct,

            "security_topk_correct": security_topk,

            "security_rank": security_rank,

            # -----------------------------
            # SCORES
            # -----------------------------
            "family_score": best_family.get("score"),

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
    # PRINT METRICS
    # =====================================================

    print("\n=========================")

    print(f"Total Cases: {total}")

    print("\n----- FAMILY -----")

    print(
        f"Family Top-1 Accuracy: "
        f"{family_top1_acc:.2f}%"
    )

    print(
        f"Family Top-{TOP_K} Accuracy: "
        f"{family_topk_acc:.2f}%"
    )

    print("\n----- SECURITY -----")

    print(
        f"Security Top-1 Accuracy: "
        f"{security_top1_acc:.2f}%"
    )

    print(
        f"Security Top-{TOP_K} Accuracy: "
        f"{security_topk_acc:.2f}%"
    )

    print("=========================\n")

    # =====================================================
    # SAVE OUTPUT
    # =====================================================

    result_df = pd.DataFrame(results)

    result_df.to_excel(
        OUTPUT_FILE,
        index=False
    )

    print(
        f"✅ Results saved to {OUTPUT_FILE}"
    )


# -----------------------------
# RUN
# -----------------------------
if __name__ == "__main__":
    run_evaluation()

