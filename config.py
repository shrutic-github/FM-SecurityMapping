# -----------------------------
# Phase 1: Family Retrieval Configuration
# -----------------------------
FAMILY_RETRIEVAL_CONFIG = {
    # Family name matching
    "match_phrase_family_boost": 30,
    "match_family_boost": 15,
    "match_family_min_should_match": "50%",
    
    # SOI (Statement of Intent) name matching
    "match_phrase_soi_boost": 25,
    "match_soi_boost": 20,
    "match_soi_min_should_match": "50%",
    
    # Security name matching
    "match_phrase_security_boost": 10,
    "match_security_boost": 8
}

# -----------------------------
# Post-ES Asset Type Boosting Config
# -----------------------------
ASSET_TYPE_BOOST_CONFIG = {
    "ddtl": {
        "match_token": "delayed draw",
        "boost": 0.3,
        "penalty_token": "term loan",
        "penalty": -0.1
    },
    "tl": {
        "match_token": "term loan",
        "boost": 0.2,
        "penalty_token": "revolver",
        "penalty": -0.1
    },
    "rev": {
        "match_token": "revolver",
        "boost": 0.2,
        "penalty": -0.1
    },
    "equity": {
        "match_token": "equity",
        "boost": 0.2,
        "penalty": -0.1
    }
}

# -----------------------------
# Phase 2: Security Retrieval Configuration
# -----------------------------
SECURITY_RETRIEVAL_CONFIG = {
    # Normalized security name matches
    "match_phrase_sec_boost": 30,
    
    # Combination of match(or) on security name and match(or) on soi name (cross_fields)
    "match_sec_cross_fields_boost": 25,
    "match_sec_cross_fields_min_should_match": "50%",
    
    # Or-match on normalized_security_name
    "match_sec_or_boost": 10,
    "match_sec_or_min_should_match": "50%",

    # Combination of match(or) on security name and match(or) on soi name (cross_fields, broad match)
    "match_sec_cross_fields_broad_boost": 10,
    "match_sec_cross_fields_broad_min_should_match": "10%",
    
    # Inactive/Commented-out tiers in function_app.py Phase 2 query:
    # "match_family_or_boost": 15,
    # "match_family_or_min_should_match": "35%",
    # "match_sec_type_or_boost": 20,
    # "match_sec_type_or_min_should_match": "20%"
}
