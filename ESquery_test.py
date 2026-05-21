import pandas as pd
import requests
import time
import math

# ==========================================
# INPUT FILE
# ==========================================
 
INPUT_FILE = "Security_Mapping_TestCases.xlsx"
OUTPUT_FILE = "es_security_testing.xlsx"

# ==========================================
# BUILD INPUT HELPER
# ==========================================
def build_input(row):
    borrower = str(row.get("Borrower/Company/Issuer Name", "")).strip()
    security_name = str(row.get("Security Name", "")).strip()
    loan_asset_type = str(row.get("Loan/Asset Type", "")).strip()

    # Handle NaN values
    borrower = "" if borrower.lower() == "nan" else borrower
    security_name = "" if security_name.lower() == "nan" else security_name
    loan_asset_type = "" if loan_asset_type.lower() == "nan" else loan_asset_type

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

# ==========================================
# READ TEST CASE FILE
# ==========================================
 
df = pd.read_excel(INPUT_FILE)

results = []

# ==========================================
# CREATE INPUT QUERY COLUMN
# ==========================================
 
# Use borrower/issuer name if present
# Otherwise use security name
 
df["Input Query"] = df["Borrower/Company/Issuer Name"].fillna("").astype(str)

# Replace empty strings with security name
df.loc[
    df["Input Query"].str.strip() == "",
    "Input Query"
] = df["Security Name"]
 
# ==========================================
# CREATE OUTPUT TEMPLATE
# ==========================================
 
output_df = pd.DataFrame({
 
    "Input": df["Input Query"],
 
    "Expected Family": df["Mastercomp Family Name"],
    "Predicted Family": "",
    "Expected Security": df["Mastercomp Security"],
    "Predicted Security": "",
    "Security rank": "",
    "Score": "",
    "family Query To ES": "",
    "Security Query To ES": ""
})

# ==========================================
# SAVE TO EXCEL
# ==========================================
output_df.to_excel(OUTPUT_FILE, index=False)
print(f"Excel file created successfully: {OUTPUT_FILE}")