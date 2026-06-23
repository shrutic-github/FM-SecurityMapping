# -----------------------------
# Phase 1: Family Retrieval — Direct (non-alias) query
# -----------------------------
FAMILY_RETRIEVAL_CONFIG = {
    # Family name matching
    "match_phrase_family_boost": 30,
    "match_family_boost": 15,
    "match_family_min_should_match": "50%",

    # SOI name matching
    "match_phrase_soi_boost": 25,
    "match_soi_boost": 20,
    "match_soi_min_should_match": "50%",

    # Security name matching (fallback signal)
    "match_phrase_security_boost": 10,
    "match_security_boost": 8,
}

# -----------------------------
# Phase 1: Family Retrieval — Alias query
# -----------------------------
MAPPED_FAMILY_RETRIEVAL_CONFIG = {
    # Company name phrase match (highest priority — exact stored alias)
    # Fields: normalized_company_name^50, normalized_security_name^30
    "multi_match_phrase_company_boost": 30,

}

# -----------------------------
# Phase 2: Security Retrieval — Direct (non-alias) query
# -----------------------------
SECURITY_RETRIEVAL_CONFIG = {
    # Exact phrase match on normalized security name
    "match_phrase_sec_boost": 30,

    # OR match on normalized security name
    "match_sec_or_boost": 10,
    "match_sec_or_min_should_match": "50%",

    # Cross-fields match across security name + soi name (precise)
    "multi_match_security_soi_boost": 25,
    "multi_match_security_soi_min_should_match": "50%",

    # Cross-fields match across security name + soi name (broad retrieval)
    "multi_match_sec_soi_fields_broad_boost": 10,
    "multi_match_sec_soi_fields_broad_boost_min_should_match": "10%",
}

# -----------------------------
# Phase 2: Security Retrieval — Alias query
# -----------------------------
MAPPED_SECURITY_RETRIEVAL_CONFIG = {
    # Phrase match on alias's own normalized_security_name
    "multi_match_company_security_boost": 30,
}

