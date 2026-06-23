import requests

# -----------------------------
# CONFIG
# -----------------------------
BASE_URL = "http://localhost:7071/api"
MAP_SECURITY_URL = f"{BASE_URL}/map-security"
MAPPINGS_URL = f"{BASE_URL}/mappings"

# Pick any (company, security) pair that your /map-security endpoint can
# already resolve via search - its predicted master security is reused
# as the target for the new mapping, so the test doesn't need a
# hardcoded security name from your index.
COMPANY_INPUT = "Events Buyer, LLC"
SECURITY_INPUT = "Events buyer Term loan"


# -----------------------------
# STEP 1: Discover a real target security via /map-security
# -----------------------------
def discover_target_security():

    resp = requests.post(
        MAP_SECURITY_URL,
        json=[{"company_input": COMPANY_INPUT, "security_input": SECURITY_INPUT}],
        timeout=15
    )
    resp.raise_for_status()

    result = resp.json()[0]

    if "error" in result:
        raise Exception(f"map-security failed: {result['error']}")

    match_type = (result.get("match") or {}).get("match_type")

    if match_type in ("historical", "indirect"):
        master_data = (result.get("mapped") or {}).get("master_security_details") or {}
    else:
        master_data = result.get("master_data") or {}

    target_security_name = master_data.get("security_name")

    if not target_security_name:
        raise Exception(
            "Could not discover a target security - "
            "/map-security returned no match for the configured test input. "
            "Update COMPANY_INPUT/SECURITY_INPUT to a pair that resolves."
        )

    print(f"[OK] Discovered target security: {target_security_name}")
    return target_security_name


# -----------------------------
# STEP 2: Create mapping
# -----------------------------
def create_mapping(target_security_name):

    resp = requests.post(
        MAPPINGS_URL,
        json={
            "company_input": COMPANY_INPUT,
            "security_input": SECURITY_INPUT,
            "target_security_name": target_security_name,
            "filetype": "test_mappings.py",
            "loan_type": "Term Loan",
        },
        timeout=15
    )

    assert resp.status_code == 201, f"Create failed: {resp.status_code} {resp.text}"

    body = resp.json()
    print(f"[OK] Created mapping id={body['id']}")
    return body["id"]


# -----------------------------
# STEP 3: Get mapping
# -----------------------------
def get_mapping(mapping_id, expect_status=200):

    resp = requests.get(f"{MAPPINGS_URL}/{mapping_id}", timeout=15)

    assert resp.status_code == expect_status, (
        f"Get failed: expected {expect_status}, got {resp.status_code} {resp.text}"
    )

    print(f"[OK] GET mapping returned {resp.status_code}")
    return resp.json() if resp.status_code == 200 else None


# -----------------------------
# STEP 4: Confirm bypass lookup picks up the new mapping
# -----------------------------
def verify_bypass_lookup():

    resp = requests.post(
        MAP_SECURITY_URL,
        json=[{"company_input": COMPANY_INPUT, "security_input": SECURITY_INPUT}],
        timeout=15
    )
    resp.raise_for_status()

    result = resp.json()[0]

    is_mapped = result.get("is_mapped")
    match_type = (result.get("match") or {}).get("match_type")

    assert is_mapped is True, f"Expected is_mapped=True, got {is_mapped}"
    assert match_type in ("historical", "indirect"), f"Unexpected match_type={match_type}"

    print(f"[OK] /map-security now returns is_mapped=True (match_type={match_type})")


# -----------------------------
# STEP 5: Update mapping
# -----------------------------
def update_mapping(mapping_id):

    resp = requests.put(
        f"{MAPPINGS_URL}/{mapping_id}",
        json={
            "filetype": "test_mappings.py (updated)",
            "loan_type": "Revolver",
        },
        timeout=15
    )

    assert resp.status_code == 200, f"Update failed: {resp.status_code} {resp.text}"

    body = resp.json()["mapping"]
    assert body["filetype"] == "test_mappings.py (updated)"
    assert body["loan_type"] == "Revolver"

    print("[OK] Updated mapping filetype/loan_type")


# -----------------------------
# STEP 6: Delete mapping
# -----------------------------
def delete_mapping(mapping_id):

    resp = requests.delete(f"{MAPPINGS_URL}/{mapping_id}", timeout=15)

    assert resp.status_code == 200, f"Delete failed: {resp.status_code} {resp.text}"
    assert resp.json().get("deleted") is True

    print(f"[OK] Deleted mapping id={mapping_id}")


# -----------------------------
# RUN FULL ROUND TRIP
# -----------------------------
def run():

    target_security_name = discover_target_security()

    mapping_id = create_mapping(target_security_name)

    fetched = get_mapping(mapping_id)
    assert fetched["mapping"]["normalized_company_name"], "Missing normalized_company_name"

    verify_bypass_lookup()

    update_mapping(mapping_id)

    delete_mapping(mapping_id)

    get_mapping(mapping_id, expect_status=404)

    print("\nAll mapping endpoint tests passed.")


if __name__ == "__main__":
    run()
