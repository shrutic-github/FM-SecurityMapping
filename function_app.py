import logging
import os
import azure.functions as func
import psycopg2
import json
import math
import hashlib
import csv
import io
from datetime import datetime, timezone
from elasticsearch import Elasticsearch, NotFoundError
from normalization import normalize_input
from config import FAMILY_RETRIEVAL_CONFIG, SECURITY_RETRIEVAL_CONFIG, MAPPED_FAMILY_RETRIEVAL_CONFIG,MAPPED_SECURITY_RETRIEVAL_CONFIG
 
app = func.FunctionApp()
ES_CLIENT = None
 
 
# -----------------------------
# Elasticsearch Client
# -----------------------------
def get_es_client() -> Elasticsearch:
    global ES_CLIENT
    if ES_CLIENT is not None:
        return ES_CLIENT
 
    es_url = os.environ.get("ES_URL")
    if not es_url:
        raise ValueError("ES_URL environment variable not found")
 
    verify_certs = (
        os.environ.get("ES_VERIFY_CERTS", "true").lower() == "true"
    )
 
    username = os.environ.get("ES_USERNAME")
    password = os.environ.get("ES_PASSWORD")
    api_key = os.environ.get("ES_API_KEY")
 
    kwargs = {
        "hosts": [es_url],
        "verify_certs": verify_certs,
        "request_timeout": 15,
    }
 
    if api_key:
        kwargs["api_key"] = api_key
    elif username and password:
        kwargs["basic_auth"] = (username, password)
 
    ES_CLIENT = Elasticsearch(**kwargs)
    return ES_CLIENT
 
 
# -----------------------------
# ES Score Scaling
# -----------------------------
def _es_scaled(raw_es_score: float) -> float:
    cap = float(os.environ.get("ES_SCORE_LOG_CAP", "600.0"))
    if raw_es_score <= 0 or cap <= 0:
        return 0.0
 
    scaled = math.log1p(raw_es_score) / math.log1p(cap)
    return max(0.0, min(scaled, 1.0))
 
 
# -----------------------------
# Generic Cleanup
# -----------------------------
GENERIC_RETRIEVAL_STOPWORDS = frozenset(
    {
        "ltd",
        "inc",
        "corp",
        "corporation",
        "llc",
        "lp",
        "plc",
        "company",
        "topco",
        "co",
        "limited",
        "pvt",
        "holdings",
        "holding",
        "holdco",
        "group",
        "trust",
        "the",
        "and",
        "of",
        "a",
        "an",
        "common",
        "preferred",
        "equity",
        "first",
        "second",
        "lien",
        "liens",
        "amendment",
        "amend",
        "initial",
        "closing",
        "date",
        "new",
        "money",
        "class",
        "series",
        "unit",
        "units",
        "unfunded",
        "funded",
        "priority",
        "fourth",
        "out",
        "incremental",
        "roll",
        "rollup",
        "restated",
        "restatement",
    }
)
 
GENERIC_RETRIEVAL_PHRASES = (
    "first lien",
    "second lien",
    "common equity",
    "preferred equity",
    "delayed draw term loan",
    "term loan"
    
)
 
 
def clean_query_for_broad_retrieval(family_query: str) -> str:
 
    t = (family_query or "").strip().lower()
    for phrase in GENERIC_RETRIEVAL_PHRASES:
        t = t.replace(phrase, " ")
 
    tokens = [
        w for w in t.split()
        if w and w not in GENERIC_RETRIEVAL_STOPWORDS
    ]
 
    cleaned_family_query = " ".join(tokens)
 
    return cleaned_family_query if cleaned_family_query else (
        family_query or ""
    ).strip().lower()
 
 
# -----------------------------
# Extract Security Type
# -----------------------------
# def extract_type(q):
#     q = q.lower()
#     if "delayed draw" in q:
#         return "ddtl"
#     elif "term loan" in q:
#         return "tl"
#     elif "revolver" in q:
#         return "rev"
#     elif "equity" in q:
#         return "equity"
#     return None
 
 
# -----------------------------
# Family-Level Type Boost
# -----------------------------
# def boost_by_type(matches, family_query):
#     input_type = extract_type(family_query)
#     if not input_type:
#         return matches
 
#     for m in matches:
#         name = (
#             (m.get("normalized_security_name") or "")
#             + " "
#             + (m.get("top_security") or "")
#         ).lower()
 
#         score = m["score"]
 
#         if input_type == "ddtl":
#             if "delayed draw" in name:
#                 score += 0.3
#             elif "term loan" in name:
#                 score -= 0.1
 
#         elif input_type == "tl":
#             if "term loan" in name:
#                 score += 0.2
#             elif "revolver" in name:
#                 score -= 0.1
 
#         elif input_type == "rev":
#             if "revolver" in name:
#                 score += 0.2
#             else:
#                 score -= 0.1
 
#         elif input_type == "equity":
#             if "equity" in name:
#                 score += 0.2
#             else:
#                 score -= 0.1
 
#         m["score"] = round(score, 4)
 
#     matches.sort(key=lambda x: x["score"], reverse=True)
#     return matches
 
 
# -----------------------------
# Retrieve Best Families
# -----------------------------
def search_family_matches(family_query: str) -> list[dict]:
 
    es = get_es_client()
 
    index_name = os.environ.get(
        "ES_INDEX",
        "security_master_v4"
    )
 
    top_k = int(os.environ.get("MATCH_TOP_K", "5"))
 
    cleaned_family_query = clean_query_for_broad_retrieval(family_query)
 
    _tokens = (cleaned_family_query or family_query or "").split()
    primary_token = next((t for t in _tokens if len(t) >= 3), _tokens[0] if _tokens else "")
 
    # -----------------------------
    # Family Retrieval Logic
    # -----------------------------
    should_clauses = [
 
 
        # # ---- Family tier ----
        {
            "match_phrase": {
                "normalized_family_name": {
                    "query": cleaned_family_query,
                    "boost": FAMILY_RETRIEVAL_CONFIG["match_phrase_family_boost"],
                }
            }
        },
        {
            "match": {
                "normalized_family_name": {
                    "query": cleaned_family_query,
                    "operator": "or",
                    "minimum_should_match": FAMILY_RETRIEVAL_CONFIG["match_family_min_should_match"],
                    "boost":  FAMILY_RETRIEVAL_CONFIG["match_family_boost"],
                }
            }
        },
 
        # # ---- SOI tier ----
        {
            "match_phrase": {
                "normalized_soi_name": {
                    "query": cleaned_family_query,
                    "boost": FAMILY_RETRIEVAL_CONFIG["match_phrase_soi_boost"],
                }
            }
        },
        {
            "match": {
                "normalized_soi_name": {
                    "query": cleaned_family_query,
                    "operator": "or",
                    "minimum_should_match": FAMILY_RETRIEVAL_CONFIG["match_soi_min_should_match"],
                    "boost": FAMILY_RETRIEVAL_CONFIG["match_soi_boost"],
                }
            }
        },
        # # ---- Security Name tier ----
        {
            "match_phrase": {
                "normalized_security_name": {
                    "query": family_query,
                    "boost": FAMILY_RETRIEVAL_CONFIG["match_phrase_security_boost"],
                }
            }
        },
        {
            "match": {
                "normalized_security_name": {
                    "query": family_query,
                    "operator": "or",
                    "boost": FAMILY_RETRIEVAL_CONFIG["match_security_boost"],
                }
            }
        },
    ]
 
    body = {
        "size": top_k,
        "query": {
            "bool": {
                "must_not": [
                    { "term": { "is_alias": True } }
                ],
                "should": should_clauses,
                "minimum_should_match": 1,
            }
        },

        # IMPORTANT
        # one representative hit per family
        "collapse": {
            "field": "family_name.keyword"
        }
    }

    alias_family_body = {
        "size": top_k,
        "query": {
            "bool": {
                "filter": [{"term": {"is_alias": True}}],
                "should": [

                    {"multi_match": {
                             "query": cleaned_family_query,
                           "fields": ["normalized_company_name^50", "normalized_security_name^30"],
                             "type": "phrase",
                             "boost": MAPPED_FAMILY_RETRIEVAL_CONFIG["multi_match_phrase_company_boost"],
                        }},

                    
                ],
                "minimum_should_match": 1,
            }
        },
        "collapse": {"field": "master_normalized_family_name.keyword"},
    }

    # Both queries are independent (alias vs non-alias docs) - batch them
    # into one msearch call instead of two sequential round trips.
    msearch_resp = es.msearch(
        index=index_name,
        searches=[
            {}, body,
            {}, alias_family_body,
        ],
    )
    responses = msearch_resp.get("responses", [])

    hits = responses[0].get("hits", {}).get("hits", []) if responses else []

    matches = []
    for hit in hits:
        source = hit.get("_source", {})
 
        raw_es_score = float(
            hit.get("_score", 0.0)
        )
 
        es_scaled = _es_scaled(raw_es_score)
 
        norm_sec = source.get("normalized_security_name", "")
 
        matches.append(
            {
                "family_name": source.get("family_name",""),
                "normalized_family_name": source.get("normalized_family_name","" ),
                "top_security": source.get("security_name",""),
                "soi_name": source.get("soi_name",""),
                "security_type": source.get("security_type",""),
                "normalized_security_name": norm_sec,"score": round(es_scaled, 4),
                "raw_es_score": round(raw_es_score,4),
            }
        )
 
    matches.sort(
        key=lambda x: x["score"],
        reverse=True
    )

    # ---- Alias search: Phase 1 addition ----
    family_index_by_name = {
        m["normalized_family_name"]: i for i, m in enumerate(matches)
    }

    alias_family_hits = (
        responses[1].get("hits", {}).get("hits", [])
        if len(responses) > 1
        else []
    )

    for hit in alias_family_hits:
        source = hit.get("_source", {})
        msd = source.get("master_security_details") or {}

        norm_fam = source.get("master_normalized_family_name", "") or msd.get("normalized_family_name", "")
        if not norm_fam:
            continue

        raw_es_score = float(hit.get("_score", 0.0))
        candidate = {
            "family_name": source.get("master_family_name", "") or msd.get("family_name", ""),
            "normalized_family_name": norm_fam,
            "top_security": source.get("master_security_name", "") or msd.get("security_name", ""),
            "soi_name": source.get("master_soi_name", "") or msd.get("soi_name", ""),
            "security_type": source.get("master_security_type", "") or msd.get("security_type", ""),
            "normalized_security_name": source.get("master_normalized_security_name", "") or msd.get("normalized_security_name", ""),
            "score": round(_es_scaled(raw_es_score), 4),
            "raw_es_score": round(raw_es_score, 4),
        }

        existing_idx = family_index_by_name.get(norm_fam)
        if existing_idx is None:
            matches.append(candidate)
            family_index_by_name[norm_fam] = len(matches) - 1
        elif candidate["score"] > matches[existing_idx]["score"]:
            # Alias mapping is a more authoritative signal than a weak
            # incidental token-overlap match, so let it win on the same family.
            matches[existing_idx] = candidate

    matches.sort(key=lambda x: x["score"], reverse=True)

    return {
        "matches": matches[:top_k],
        "primary_token": primary_token,
        "cleaned_family_query": cleaned_family_query,
    }

# -----------------------------
# ES-Based Security Search
# -----------------------------
def search_securities_es(
    security_query: str,
    family_matches: list,
    family_query: str = ""
) -> list:
 
    es = get_es_client()
 
    index_name = os.environ.get(
        "ES_INDEX",
        "security_master_v4"
    )
 
    family_weight = float(
        os.environ.get("FAMILY_WEIGHT", "0.5")
    )
 
    normalized_family_names = [
        f["normalized_family_name"] for f in family_matches
    ]
 
    # ---- Scoring clauses for normalized fields and security type ----
    should_clauses = [
        # ---- Normalized Security Name Tier ----
        {
            "match_phrase": {
                "normalized_security_name": {
                    "query": security_query,
                    "boost": SECURITY_RETRIEVAL_CONFIG["match_phrase_sec_boost"],
                }
            }
        },
        {
            "match": {
                "normalized_security_name": {
                    "query": security_query,
                    "operator": "or",
                    "minimum_should_match": SECURITY_RETRIEVAL_CONFIG["match_sec_or_min_should_match"],
                    "boost": SECURITY_RETRIEVAL_CONFIG["match_sec_or_boost"],
                }
            }
        },
        # ---- Combination of match(or) on security name and match(or) on soi name ----
        {
            "multi_match": {
                "query": security_query,
                "fields": ["normalized_security_name", "normalized_soi_name"],
                "type": "cross_fields",
                "operator": "or",
                "minimum_should_match": SECURITY_RETRIEVAL_CONFIG["multi_match_security_soi_min_should_match"],
                "boost": SECURITY_RETRIEVAL_CONFIG["multi_match_security_soi_boost"],
            }
        },

        {
            "multi_match": {
                "query": security_query,
                "fields": ["normalized_security_name", "normalized_soi_name"],
                "type": "cross_fields",
                "operator": "or",
                "minimum_should_match": SECURITY_RETRIEVAL_CONFIG["multi_match_sec_soi_fields_broad_boost_min_should_match"],
                "boost": SECURITY_RETRIEVAL_CONFIG["multi_match_sec_soi_fields_broad_boost"],
            }
        },
 
    #    # ---- Security Type Tier ----
        # {
        #     "match":{
        #         "security_type":{
        #             "query":security_query,
        #             "operator":"or",
        #             "minimum_should_match": "20%",
        #             "boost": 20,
        #         }
        #     }
        # }
     ]
 
    body = {
        "size": 20,
        "query": {
            "function_score": {
                "query": {
                    "bool": {
                        "filter": [
                            {
                                "terms": {
                                    "normalized_family_name.keyword": normalized_family_names
                                }
                            }
                        ],
                        "must_not": [
                            {
                                "term": {
                                    "is_alias": True
                                }
                            }
                        ],
                        "should": should_clauses,
                        "minimum_should_match": 0,
                    }
                },
                #"functions": functions,
                "score_mode": "sum",
                "boost_mode": "sum",
            }
        },
        "_source": [
            "security_name",
            "security_type",
            "normalized_soi_name",
            "normalized_security_name",
            "normalized_family_name",
        ],
        "collapse": {
            "field": "security_name.keyword"
        },
    }
 
    # ---- Alias search: Phase 2 addition ----
    alias_sec_body = None
    if family_query:
        alias_sec_body = {
            "size": 20,
            "query": {
                "bool": {
                    "filter": [
                        {"term": {"is_alias": True}},
                        {"terms": {"master_normalized_family_name.keyword": normalized_family_names}},
                    ],
                    "should": [
                        {"multi_match": {
                            "query": security_query,
                            "fields": ["normalized_company_name^30", "normalized_security_name^50"],
                            "type": "phrase",
                            "boost": MAPPED_SECURITY_RETRIEVAL_CONFIG["multi_match_company_security_boost"],
                        }},
                        # {"match_phrase": {"normalized_security_name":
                        #                    {"query": security_query, 
                        #                     "boost": MAPPED_SECURITY_RETRIEVAL_CONFIG["match_phrase_alias_sec_boost"]
                        #                     }}},
                      

                        
                    ],
                    "minimum_should_match": 1,
                }
            },
            "_source": [
                "master_security_details",
                "master_family_name",
                "master_normalized_family_name",
                "master_security_name",
                "master_normalized_security_name",
                "master_soi_name",
                "master_normalized_soi_name",
                "master_security_type",
            ],
            "collapse": {"field": "master_normalized_security_name.keyword"},
        }

    # Both queries are independent (alias vs non-alias docs) - batch them
    # into one msearch call instead of two sequential round trips.
    searches = [{}, body]
    if alias_sec_body is not None:
        searches.extend([{}, alias_sec_body])

    responses = es.msearch(
        index=index_name,
        searches=searches,
    ).get("responses", [])

    hits = responses[0].get("hits", {}).get("hits", []) if responses else []

    securities = []
    seen_security_names = set()

    for hit in hits:
        src = hit.get("_source", {})
        sec_name = src.get("security_name", "")

        if not sec_name.strip():
            continue

        securities.append({
            "security_name": sec_name,
            "security_type": src.get("security_type", ""),
            "normalized_soi_name": src.get("normalized_soi_name", ""),
            "normalized_security_name": src.get("normalized_security_name", ""),
            "normalized_family_name": src.get("normalized_family_name", ""),
            "score": round(float(hit.get("_score", 0.0)), 4),
        })
        seen_security_names.add(sec_name)

    if alias_sec_body is not None:
        alias_sec_hits = (
            responses[1].get("hits", {}).get("hits", [])
            if len(responses) > 1
            else []
        )

        for hit in alias_sec_hits:
            src = hit.get("_source", {})
            msd = src.get("master_security_details") or {}

            sec_name = src.get("master_security_name", "") or msd.get("security_name", "")
            if not sec_name.strip() or sec_name in seen_security_names:
                continue

            securities.append({
                "security_name": sec_name,
                "security_type": src.get("master_security_type", "") or msd.get("security_type", ""),
                "normalized_soi_name": src.get("master_normalized_soi_name", "") or msd.get("normalized_soi_name", ""),
                "normalized_security_name": src.get("master_normalized_security_name", "") or msd.get("normalized_security_name", ""),
                "normalized_family_name": src.get("master_normalized_family_name", "") or msd.get("normalized_family_name", ""),
                "score": round(float(hit.get("_score", 0.0)), 4),
            })
            seen_security_names.add(sec_name)

    securities.sort(key=lambda x: x["score"], reverse=True)

    return securities
 
def _resolve_security_mapping(
    family_string: str,
    security_string: str,
    conn_string: str
) -> dict:

    logging.info(
        f"Input received: {family_string} | security_input: {security_string}"
    )

    # -----------------------------
    # Normalize
    # -----------------------------

    normalized_result = normalize_input(
        family_string,
        conn_string
    )

    family_query = normalized_result[
        "normalized_query"
    ]

    security_query = normalize_input(
        security_string,
        conn_string
    )["normalized_query"]

    # -----------------------------
    # Check for Mapped Security Bypass (Indirect Match)
    # -----------------------------
    is_mapped=False
    try:
            es_client = get_es_client()
            index_name = os.environ.get("ES_INDEX", "security_master_v4")
            
            bypass_res = es_client.search(
                index=index_name,
                body={
                    "size": 1,
                    "query": {
                        "bool": {
                            "should": [
                                {
                                    "bool": {
                                        "boost": 2.0,
                                        "must": [
                                            { "term": { "is_alias": True } },
                                            { "term": { "normalized_company_name.keyword": family_query } },
                                            { "term": { "normalized_security_name.keyword": security_query } }
                                        ]
                                    }
                                },
                                {
                                    "bool": {
                                        "must": [
                                            { "term": { "normalized_family_name.keyword": family_query } },
                                            { "term": { "normalized_security_name.keyword": security_query } }
                                        ],
                                        "must_not": [
                                            { "term": { "is_alias": True } }
                                        ]
                                    }
                                }
                            ],
                            "minimum_should_match": 1
                        }
                    }
                }
            )
            
            bypass_hits = bypass_res.get("hits", {}).get("hits", [])
            if bypass_hits:
                bypass_source = bypass_hits[0]["_source"]
                is_alias = bypass_source.get("is_alias", False)
               
                if is_alias:
                    is_mapped = True
                
                if is_alias:
                    # Check match type (exact historical vs indirect)
                    stored_normalized_sec = bypass_source.get("normalized_security_name", "")
                    is_exact = (stored_normalized_sec == security_query)
                    match_type = "historical" if is_exact else "indirect"
                    
                    master_details = bypass_source.get("master_security_details", {}) or {}
                    metadata = bypass_source.get("metadata", {}) or {}
                else:
                    master_details = bypass_source
                    metadata = {}
                    match_type = "direct"
                
                if master_details is None:
                    master_details = {}
                if metadata is None:
                    metadata = {}
                
                sec_type = master_details.get("security_type", "") or metadata.get("loan_type", "")

                result = {
                    "input": {
                        "company_input": family_string,
                        "security_input": security_string,
                        "company_query": family_query,
                        "security_query": security_query, 
                    },
                    "is_mapped":is_mapped,
                    "mapped": {
                        "mapped_family": master_details.get("family_name", "") if is_alias else None,
                        "mapped_security": master_details.get("security_name", "") if is_alias else None,
                        "filetype": bypass_source.get("filetype", "") if is_alias else None,
                        "mapped_at": bypass_source.get("ingested_at", "") if is_alias else None,
                        "master_security_details": {
                            "family_name": master_details.get("family_name", ""),
                            "normalized_family_name": master_details.get("normalized_family_name", ""),
                            "soi_name": master_details.get("soi_name", ""),
                            "security_name": master_details.get("security_name", ""),
                            "normalized_security_name": master_details.get("normalized_security_name", ""),
                            "security_type": sec_type,
                        },
                    },
                    "match": {
                        "top_security": master_details.get("security_name", ""),
                        "family_confidence": 1.0,
                        "security_confidence": 1.0,
                        "matched": True,
                        "match_type": match_type,
                    },
                }
                logging.info(f"Historical Match ({match_type}) Found for raw inputs: {family_string} | {security_string}")
                return result
    except Exception as e:
        logging.error(f"Bypass lookup failed: {e}")

    # -----------------------------
    # Retrieve Families
    # -----------------------------

    family_result = search_family_matches(family_query)
    family_matches = family_result[  "matches" ]
    primary_token = family_result["primary_token"]
    cleaned_family_query = family_result["cleaned_family_query"]

    # family_matches = boost_by_type(
    #     family_matches,
    #     family_query
    # )

    best_family = (family_matches[0]
        if family_matches
        else None
    )

    matched = False
    reranked_securities = []

    # -----------------------------
    # ES Security Search
    # -----------------------------
    if best_family:

        # A score of 1.0 means the family was resolved via an
        # mapped-confirmed match, not a fuzzy token-overlap guess -
        # trust it and don't let a weaker cross-family security
        # match override it.
        is_high_confidence_family = best_family.get("score", 0.0) >= 1.0

        # candidate_families = (
        #     [best_family] if is_high_confidence_family else family_matches
        # )
        candidate_families = family_matches
        # Search securities within the resolved family, or across all
        # candidate families when family confidence is still low.
        reranked_securities = search_securities_es(
            security_query,
            candidate_families,
            family_query=family_query
        )

        if reranked_securities and reranked_securities[0]["score"] > 0.0:
            matched = True
            best_sec = reranked_securities[0]

            # When family confidence is low, search_securities_es searches
            # across all candidate families at once, so the winning
            # security may not belong to best_family. Re-anchor to
            # whichever family actually owns it, so master_data's
            # family and security fields never end up from two
            # different families.
            if best_sec["normalized_family_name"] != best_family.get("normalized_family_name"):
                matching_family = next(
                    (
                        f for f in family_matches
                        if f["normalized_family_name"] == best_sec["normalized_family_name"]
                    ),
                    None
                )
                if matching_family:
                    best_family = matching_family

            best_family["top_security"] = best_sec["security_name"]
            best_family["normalized_security_name"] = best_sec["normalized_security_name"]
            best_family["security_type"] = best_sec["security_type"]
            best_family["security_score"] = best_sec["score"]
            security_confidence = _es_scaled(best_sec["score"])
        else:
            matched = False
            best_family["top_security"] = None
            best_family["security_score"] = 0.0
            security_confidence = 0.0
    else:
        security_confidence = 0.0

    # -----------------------------
    # Final Response
    # -----------------------------
    result = {
        "input": {
            "company_input": family_string,
            "security_input": security_string,
            "company_query": family_query,
            "security_query": security_query,
        },
        "is_mapped":is_mapped,
        
        "match": {
            "top_security": best_family.get("top_security") if best_family else None,
            "family_confidence": best_family.get("score", 0.0) if best_family else 0.0,
            "security_confidence": security_confidence,
            "matched": matched,
            "match_type": "direct" if matched else "unmatched",
        },
        # "master_data": (
        #     {
        #         "family_name": best_family.get("family_name"),
        #         "normalized_family_name": best_family.get("normalized_family_name"),
        #         "soi_name": best_family.get("soi_name"),
        #         "security_name": best_family.get("top_security"),
        #         "normalized_security_name": best_family.get("normalized_security_name"),
        #         "security_type": best_family.get("security_type"),
        #     }
        #     if best_family
        #     else None
        # ),

        "candidates": {
            "top_families": [
                {
                    "family_name": f["family_name"],
                }
                for f in family_matches
            ],
            "ranked_securities": reranked_securities,
        },
    }

    return result


# -----------------------------
# Mapping Storage Helpers
# -----------------------------
def _mapping_doc_id(normalized_company: str, normalized_security: str) -> str:
    return hashlib.sha256(
        f"{normalized_company}|{normalized_security}".encode("utf-8")
    ).hexdigest()


def _fetch_master_security_doc(es: Elasticsearch, index_name: str, security_name: str) -> dict:
    if not security_name:
        return {}
    res = es.search(
        index=index_name,
        body={
            "size": 1,
            "query": {
                "bool": {
                    "must": [{"term": {"security_name.keyword": security_name}}],
                    "must_not": [{"term": {"is_alias": True}}],
                }
            },
        },
    )
    hits = res.get("hits", {}).get("hits", [])
    return hits[0]["_source"] if hits else {}


def _build_mapping_doc(
    company_input: str,
    security_input: str,
    normalized_company: str,
    normalized_security: str,
    master_doc: dict,
    filetype: str = "",
    loan_type: str = "",
    metadata: dict = None,
) -> dict:
    return {
        "is_alias": True,
        "company_name": company_input,
        "normalized_company_name": normalized_company,
        "security_name": security_input,
        "normalized_security_name": normalized_security,
        "filetype": filetype or "",
        "loan_type": loan_type or master_doc.get("security_type", ""),
        "master_security_details": master_doc,
        "master_family_name": master_doc.get("family_name", ""),
        "master_normalized_family_name": master_doc.get("normalized_family_name", ""),
        "master_security_name": master_doc.get("security_name", ""),
        "master_normalized_security_name": master_doc.get("normalized_security_name", ""),
        "master_soi_name": master_doc.get("soi_name", ""),
        "master_normalized_soi_name": master_doc.get("normalized_soi_name", ""),
        "master_security_type": master_doc.get("security_type", ""),
        "metadata": metadata or {},
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


# ─────────────────────────────────────────────────────────────────
# Endpoint 1: Security Retrieval  POST /api/security-mapping 
# ─────────────────────────────────────────────────────────────────
@app.route(route="security-mapping", methods=["POST"])
def security_mapping_api(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Received mapping request")
    try:
        body = req.get_json()
        if not isinstance(body, list):
            return func.HttpResponse(
                json.dumps({"error": "Payload must be a JSON array"}),
                status_code=400,
                mimetype="application/json",
            )

        conn_string = os.environ.get("POSTGRES_CONN")
        if not conn_string:
            raise Exception("POSTGRES_CONN not found")

        results = []
        for item in body:
            company  = item.get("company_input")
            security = item.get("security_input") or company
            filetype = item.get("file_type")
            if not company:
                results.append({"error": "company_input is required"})
                continue
            try:
                result = _resolve_security_mapping(company, security, conn_string)
                if filetype:
                    result["input"]["file_type"] = filetype
                results.append(result)
            except Exception as e:
                logging.error(f"Error mapping item ({company}): {e}")
                results.append({"error": str(e)})

        return func.HttpResponse(
            json.dumps(results), status_code=200, mimetype="application/json"
        )

    except Exception as e:
        logging.error(f"Error: {e}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}), status_code=500, mimetype="application/json"
        )


# ─────────────────────────────────────────────────────────────────
# Endpoint 2: Store Confirmed Mapping  POST /api/mappings
# ─────────────────────────────────────────────────────────────────
@app.route(route="store-mappings", methods=["POST"])
def store_mapping_api(req: func.HttpRequest) -> func.HttpResponse:
    try:
        body = req.get_json()

        company_input        = body.get("company_input")
        security_input       = body.get("security_input")
        target_security_name = body.get("target_security_name")

        if not company_input or not security_input or not target_security_name:
            return func.HttpResponse(
                json.dumps({"error": "company_input, security_input, and target_security_name are required"}),
                status_code=400,
                mimetype="application/json",
            )

        conn_string = os.environ.get("POSTGRES_CONN")
        if not conn_string:
            raise Exception("POSTGRES_CONN not found")

        norm_company  = normalize_input(company_input,  conn_string)["normalized_query"]
        norm_security = normalize_input(security_input, conn_string)["normalized_query"]

        es         = get_es_client()
        index_name = os.environ.get("ES_INDEX", "security_master_v4")
        doc_id     = _mapping_doc_id(norm_company, norm_security)

        if es.exists(index=index_name, id=doc_id):
            existing = es.get(index=index_name, id=doc_id)["_source"]
            return func.HttpResponse(
                json.dumps({"error": "Mapping already exists", "id": doc_id, "existing_mapping": existing}),
                status_code=409,
                mimetype="application/json",
            )

        master_doc  = _fetch_master_security_doc(es, index_name, target_security_name)
        mapping_doc = _build_mapping_doc(
            company_input, security_input, norm_company, norm_security, master_doc,
            filetype=body.get("filetype", ""),
            loan_type=body.get("loan_type", ""),
            metadata=body.get("metadata"),
        )

        es.index(index=index_name, id=doc_id, document=mapping_doc)

        return func.HttpResponse(
            json.dumps({"id": doc_id, "mapping": mapping_doc}),
            status_code=201,
            mimetype="application/json",
        )

    except Exception as e:
        logging.error(f"Error creating mapping: {e}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}), status_code=500, mimetype="application/json"
        )


# ─────────────────────────────────────────────────────────────────
# Endpoint 3: List / Download All Mappings  GET /api/mappings
#   ?page=1&limit=100   → paginated JSON list
#   ?format=csv         → CSV file download
# ─────────────────────────────────────────────────────────────────
@app.route(route="view-mappings", methods=["GET"])
def view_mappings_api(req: func.HttpRequest) -> func.HttpResponse:
    try:
        es         = get_es_client()
        index_name = os.environ.get("ES_INDEX", "security_master_v4")

        page  = int(req.params.get("page",  "1"))
        limit = min(int(req.params.get("limit", "100")), 1000)
        fmt   = req.params.get("format", "json").lower()

        res = es.search(
            index=index_name,
            body={
                "from": (page - 1) * limit,
                "size": limit,
                "query": {"term": {"is_alias": True}},
                "sort": [{"ingested_at": {"order": "desc"}}],
                "_source": [
                    "company_name", "security_name",
                    "master_family_name", "master_security_name",
                    "master_security_type", "filetype", "loan_type",
                    "ingested_at",
                ],
            },
        )

        total = res.get("hits", {}).get("total", {}).get("value", 0)
        hits  = res.get("hits", {}).get("hits", [])
        rows  = [{"id": h["_id"], **h["_source"]} for h in hits]

        if fmt == "csv":
            if not rows:
                csv_body = ""
            else:
                buf    = io.StringIO()
                writer = csv.DictWriter(buf, fieldnames=rows[0].keys())
                writer.writeheader()
                writer.writerows(rows)
                csv_body = buf.getvalue()

            return func.HttpResponse(
                csv_body,
                status_code=200,
                mimetype="text/csv",
                headers={"Content-Disposition": "attachment; filename=mappings.csv"},
            )

        return func.HttpResponse(
            json.dumps({"page": page, "limit": limit, "total": total, "count": len(rows), "results": rows}),
            status_code=200,
            mimetype="application/json",
        )

    except Exception as e:
        logging.error(f"Error listing mappings: {e}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}), status_code=500, mimetype="application/json"
        )


# ─────────────────────────────────────────────────────────────────
# Endpoint 4: Update Mapping  PUT /api/mappings/{id}
# ─────────────────────────────────────────────────────────────────
@app.route(route="update-mappings/{id}", methods=["PUT"])
def update_mapping_api(req: func.HttpRequest) -> func.HttpResponse:
    doc_id = req.route_params.get("id")
    try:
        es         = get_es_client()
        index_name = os.environ.get("ES_INDEX", "security_master_v4")

        try:
            existing = es.get(index=index_name, id=doc_id)["_source"]
        except NotFoundError:
            return func.HttpResponse(
                json.dumps({"error": "Mapping not found"}),
                status_code=404,
                mimetype="application/json",
            )

        body                 = req.get_json()
        target_security_name = body.get("target_security_name")
        master_doc = (
            _fetch_master_security_doc(es, index_name, target_security_name)
            if target_security_name
            else (existing.get("master_security_details") or {})
        )

        mapping_doc = _build_mapping_doc(
            existing.get("company_name", ""),
            existing.get("security_name", ""),
            existing.get("normalized_company_name", ""),
            existing.get("normalized_security_name", ""),
            master_doc,
            filetype=body.get("filetype",  existing.get("filetype",  "")),
            loan_type=body.get("loan_type", existing.get("loan_type", "")),
            metadata=body.get("metadata",   existing.get("metadata",  {})),
        )

        es.index(index=index_name, id=doc_id, document=mapping_doc)

        return func.HttpResponse(
            json.dumps({"id": doc_id, "mapping": mapping_doc}),
            status_code=200,
            mimetype="application/json",
        )

    except Exception as e:
        logging.error(f"Error updating mapping {doc_id}: {e}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}), status_code=500, mimetype="application/json"
        )


# ─────────────────────────────────────────────────────────────────
# Endpoint 5: Delete Mapping  DELETE /api/mappings/{id}
# ─────────────────────────────────────────────────────────────────
@app.route(route="delete-mappings/{id}", methods=["DELETE"])
def delete_mapping_api(req: func.HttpRequest) -> func.HttpResponse:
    doc_id = req.route_params.get("id")
    try:
        es         = get_es_client()
        index_name = os.environ.get("ES_INDEX", "security_master_v4")

        try:
            es.delete(index=index_name, id=doc_id)
        except NotFoundError:
            return func.HttpResponse(
                json.dumps({"error": "Mapping not found"}),
                status_code=404,
                mimetype="application/json",
            )

        return func.HttpResponse(
            json.dumps({"deleted": True, "id": doc_id}),
            status_code=200,
            mimetype="application/json",
        )

    except Exception as e:
        logging.error(f"Error deleting mapping {doc_id}: {e}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}), status_code=500, mimetype="application/json"
        )
# ─────────────────────────────────────────────────────────────────
# Endpoint 6: Store Master Security
# POST /api/store-master-security
# ─────────────────────────────────────────────────────────────────
@app.route(route="store-master-security", methods=["POST"])
def store_master_security_api(req: func.HttpRequest) -> func.HttpResponse:
    try:
        body = req.get_json()

        soi_name = body.get("soi_name")
        family_name = body.get("family_name")
        security_name = body.get("master_comp_security_name")
        security_type = body.get("security_type")

        if not all([soi_name, family_name, security_name, security_type]):
            return func.HttpResponse(
                json.dumps({
                    "error": "soi_name, family_name, master_comp_security_name and security_type are required"
                }),
                status_code=400,
                mimetype="application/json",
            )

        conn_string = os.environ.get("POSTGRES_CONN")
        if not conn_string:
            raise Exception("POSTGRES_CONN not found")

        # Normalize input
        norm_soi = normalize_input(
            soi_name,
            conn_string
        )["normalized_query"]

        norm_family = normalize_input(
            family_name,
            conn_string
        )["normalized_query"]

        norm_security = normalize_input(
            security_name,
            conn_string
        )["normalized_query"]

        es = get_es_client()
        index_name = os.environ.get("ES_INDEX", "security_master_v4")

        # Duplicate check
        duplicate_query = {
            "size": 1,
            "query": {
                "bool": {
                    "must": [
                        {
                            "term": {
                                "normalized_security_name.keyword": norm_security
                            }
                        },
                        {
                            "term": {
                                "normalized_family_name.keyword": norm_family
                            }
                        }
                    ]
                }
            }
        }

        existing = es.search(
            index=index_name,
            body=duplicate_query
        )

        if existing["hits"]["total"]["value"] > 0:
            existing_hit = existing["hits"]["hits"][0]
            return func.HttpResponse(
                json.dumps({
                    "error": "Master security already exists",
                    "id": existing_hit["_id"],
                    "existing_document": existing_hit["_source"],
                }),
                status_code=409,
                mimetype="application/json",
            )

        master_doc = {
            "soi_name": soi_name,
            "security_name": security_name,
            "family_name": family_name,
            "security_type": security_type,

            "normalized_name": norm_security,
            "normalized_security_name": norm_security,
            "normalized_soi_name": norm_soi,
            "normalized_family_name": norm_family,

            "ingested_at": datetime.now(timezone.utc).isoformat()
        }

        response = es.index(
            index=index_name,
            document=master_doc,
            refresh=True
        )

        return func.HttpResponse(
            json.dumps({
                "message": "Master security stored successfully",
                "id": response["_id"],
                "document": master_doc
            }),
            status_code=201,
            mimetype="application/json",
        )

    except Exception as e:
        logging.error(f"Error storing master security: {e}")

        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json",
        )