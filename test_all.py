
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
# BUILD INPUT LOGIC
# -----------------------------
def build_input(row):

    borrower = str(
        row.get("Borrower/Company/Issuer Name", "")
    ).strip()

    security_name = str(
        row.get("Security Name", "")
    ).strip()

    # Handle NaN values
    borrower = "" if borrower.lower() == "nan" else borrower
    security_name = "" if security_name.lower() == "nan" else security_name

    if borrower:
        return borrower

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
        else:
            return {"error": f"HTTP {response.status_code}"}

    except Exception as e:
        return {"error": str(e)}


# -----------------------------
# MAIN TEST RUN
# -----------------------------
def run_evaluation():

    df = pd.read_excel(INPUT_FILE)

    results = []

    family_correct_count = 0
    family_topk_correct_count = 0

    for idx, row in df.iterrows():

        input_text = build_input(row)

        # -----------------------------
        # EXPECTED FAMILY
        # -----------------------------
        expected_family = str(
            row.get("Mastercomp Family Name", "")
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
                "predicted_family": None,

                "family_correct": False,
                "family_topk_correct": False,

                "family_rank": None,

                "score": None,
                "raw_es_score": None,

                "status": "ERROR",
                "error": api_result["error"]
            })

            continue

        # -----------------------------
        # BEST MATCH
        # -----------------------------
        best_match = api_result.get("best_match") or {}

        predicted_family = (
            best_match.get("family_name")
        )

        # -----------------------------
        # FAMILY ACCURACY
        # -----------------------------
        family_correct = (
            predicted_family == expected_family
        )

        if family_correct:
            family_correct_count += 1

        # -----------------------------
        # TOP MATCHES
        # -----------------------------
        top_matches = api_result.get("matches", [])

        top_family_names = []

        for m in top_matches:

            top_family_names.append(
                m.get("family_name")
            )

        # -----------------------------
        # FAMILY TOP-K
        # -----------------------------
        family_topk_correct = (
            expected_family in top_family_names
        )

        if family_topk_correct:
            family_topk_correct_count += 1

        if expected_family in top_family_names:
            family_rank = (
                top_family_names.index(expected_family) + 1
            )
        else:
            family_rank = None

        # -----------------------------
        # STORE RESULT
        # -----------------------------
        results.append({

            "input": input_text,

            # -----------------------------
            # FAMILY
            # -----------------------------
            "expected_family": expected_family,
            "predicted_family": predicted_family,
            "family_correct": family_correct,

            # -----------------------------
            # SCORES
            # -----------------------------
            "score": best_match.get("score"),
            "raw_es_score": best_match.get("raw_es_score"),

            # -----------------------------
            # TOP-K FAMILY
            # -----------------------------
            "family_top_matches": " | ".join(
                [str(x) for x in top_family_names]
            ),

            "family_topk_correct": family_topk_correct,

            "family_rank": family_rank,

            # -----------------------------
            # MATCH INFO
            # -----------------------------
            "matched_flag": api_result.get("matched")
        })

        time.sleep(0.05)

    # -----------------------------
    # FINAL METRICS
    # -----------------------------
    total = len(results)

    family_accuracy = (
        (family_correct_count / total) * 100
        if total > 0 else 0
    )

    family_topk_accuracy = (
        (family_topk_correct_count / total) * 100
        if total > 0 else 0
    )

    # -----------------------------
    # PRINT METRICS
    # -----------------------------
    print("\n=========================")

    print(f"Total Cases: {total}")

    print(f"Family Top-1 Accuracy: {family_accuracy:.2f}%")

    print(f"Family Top-{TOP_K} Accuracy: {family_topk_accuracy:.2f}%")

    print("=========================\n")

    # -----------------------------
    # SAVE OUTPUT
    # -----------------------------
    result_df = pd.DataFrame(results)

    result_df.to_excel(OUTPUT_FILE, index=False)

    print(f"✅ Results saved to {OUTPUT_FILE}")


# -----------------------------
# RUN
# -----------------------------
if __name__ == "__main__":
    run_evaluation()