import requests

# --------------------------------------------------
# CONFIG
# --------------------------------------------------
BASE_URL = "http://localhost:7071/api"

SECURITY_MAPPING_URL = f"{BASE_URL}/security-mapping"
STORE_MAPPING_URL = f"{BASE_URL}/store-mappings"
VIEW_MAPPINGS_URL = f"{BASE_URL}/view-mappings"
UPDATE_MAPPING_URL = f"{BASE_URL}/update-mappings"
DELETE_MAPPING_URL = f"{BASE_URL}/delete-mappings"

COMPANY_INPUT = "Events Buyer, LLC"
SECURITY_INPUT = "Events buyer Term loan 12"


# --------------------------------------------------
# STEP 1: Discover Target Security
# --------------------------------------------------
def discover_target_security():

    resp = requests.post(
        SECURITY_MAPPING_URL,
        json=[
            {
                "company_input": COMPANY_INPUT,
                "security_input": SECURITY_INPUT,
            }
        ],
        timeout=15,
    )

    resp.raise_for_status()

    results = resp.json()

    if not results:
        raise Exception("No response returned")

    result = results[0]

    if "error" in result:
        raise Exception(result["error"])

    is_mapped = result.get("is_mapped", False)

    if is_mapped:
        master_data = (
            result.get("mapped", {})
            .get("master_security_details", {})
        )
    else:
        master_data = result.get("master_data", {})

    target_security_name = master_data.get("security_name")

    if not target_security_name:
        raise Exception(
            "No target security discovered from security-mapping"
        )

    print(
        f"[OK] Target Security Found: "
        f"{target_security_name}"
    )

    return target_security_name


# --------------------------------------------------
# STEP 2: Create Mapping
# --------------------------------------------------
def create_mapping(target_security_name):

    payload = {
        "company_input": COMPANY_INPUT,
        "security_input": SECURITY_INPUT,
        "target_security_name": target_security_name,
        "filetype": "test_api",
        "loan_type": "Term Loan",
        "metadata": {
            "source": "integration_test"
        }
    }

    resp = requests.post(
        STORE_MAPPING_URL,
        json=payload,
        timeout=15,
    )

    if resp.status_code == 409:
        body = resp.json()

        print(
            f"[INFO] Mapping already exists: "
            f"{body['id']}"
        )

        return body["id"]

    assert resp.status_code == 201, (
        f"Create failed: {resp.status_code} {resp.text}"
    )

    body = resp.json()

    print(
        f"[OK] Mapping Created: "
        f"{body['id']}"
    )

    return body["id"]


# --------------------------------------------------
# STEP 3: View Mappings
# --------------------------------------------------
def view_mappings():

    resp = requests.get(
        VIEW_MAPPINGS_URL,
        timeout=15,
    )

    assert resp.status_code == 200, (
        f"View failed: {resp.status_code}"
    )

    body = resp.json()

    print(
        f"[OK] Total mappings returned: "
        f"{body['count']}"
    )

    return body


# --------------------------------------------------
# STEP 4: Download CSV
# --------------------------------------------------
def download_csv():

    resp = requests.get(
        f"{VIEW_MAPPINGS_URL}?format=csv",
        timeout=15,
    )

    assert resp.status_code == 200

    assert (
        "text/csv"
        in resp.headers.get("Content-Type", "")
    )

    print(
        "[OK] CSV download endpoint working"
    )


# --------------------------------------------------
# STEP 5: Verify Bypass
# --------------------------------------------------
def verify_bypass_lookup():

    resp = requests.post(
        SECURITY_MAPPING_URL,
        json=[
            {
                "company_input": COMPANY_INPUT,
                "security_input": SECURITY_INPUT,
            }
        ],
        timeout=15,
    )

    resp.raise_for_status()

    result = resp.json()[0]

    is_mapped = result.get("is_mapped")

    assert is_mapped is True, (
        f"Expected is_mapped=True, got {is_mapped}"
    )

    print(
        "[OK] Historical mapping bypass working"
    )


# --------------------------------------------------
# STEP 6: Update Mapping
# --------------------------------------------------
def update_mapping(mapping_id):

    payload = {
        "filetype": "updated_test_api",
        "loan_type": "Revolver",
    }

    resp = requests.put(
        f"{UPDATE_MAPPING_URL}/{mapping_id}",
        json=payload,
        timeout=15,
    )

    assert resp.status_code == 200, (
        f"Update failed: {resp.status_code}"
    )

    mapping = resp.json()["mapping"]

    print(
        "[OK] Mapping updated"
    )

    print(
        f"     filetype={mapping['filetype']}"
    )

    print(
        f"     loan_type={mapping['loan_type']}"
    )


# --------------------------------------------------
# STEP 7: Delete Mapping
# --------------------------------------------------
def delete_mapping(mapping_id):

    resp = requests.delete(
        f"{DELETE_MAPPING_URL}/{mapping_id}",
        timeout=15,
    )

    assert resp.status_code == 200, (
        f"Delete failed: {resp.status_code}"
    )

    body = resp.json()

    assert body["deleted"] is True

    print(
        f"[OK] Deleted mapping: {mapping_id}"
    )


# --------------------------------------------------
# RUN FULL TEST
# --------------------------------------------------
def run():

    print("\n===== START TEST =====\n")

    target_security_name = (
        discover_target_security()
    )

    mapping_id = create_mapping(
        target_security_name
    )

    view_mappings()

    verify_bypass_lookup()

    update_mapping(mapping_id)

    download_csv()

    delete_mapping(mapping_id)

    print(
        "\n===== ALL TESTS PASSED ====="
    )


if __name__ == "__main__":
    run()