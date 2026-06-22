import logging
import os
import azure.functions as func
import psycopg2
import json
import math
from elasticsearch import Elasticsearch
 
from normalization import normalize_input
from config import FAMILY_RETRIEVAL_CONFIG, ASSET_TYPE_BOOST_CONFIG, SECURITY_RETRIEVAL_CONFIG
 
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
                    "boost": 30,
                }
            }
        },
        {
            "match": {
                "normalized_family_name": {
                    "query": cleaned_family_query,
                    "operator": "or",
                    "minimum_should_match": "50%",
                    "boost": 15,
                }
            }
        },
 
        # # ---- SOI tier ----
        {
            "match_phrase": {
                "normalized_soi_name": {
                    "query": cleaned_family_query,
                    "boost": 25,
                }
            }
        },
        {
            "match": {
                "normalized_soi_name": {
                    "query": cleaned_family_query,
                    "operator": "or",
                    "minimum_should_match": "50%",
                    "boost": 20,
                }
            }
        },
        # # ---- Security Name tier ----
        {
            "match_phrase": {
                "normalized_security_name": {
                    "query": family_query,
                    "boost": 10,
                }
            }
        },
        {
            "match": {
                "normalized_security_name": {
                    "query": family_query,
                    "operator": "or",
                    "boost": 8,
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
                             "boost": 30,
                        }},

                     {"match_phrase": {"master_normalized_family_name": {"query": cleaned_family_query, "boost": 30}}},
                     {"match": {"master_normalized_family_name": {"query": cleaned_family_query, "operator": "or", "minimum_should_match": "70%", "boost": 15}}},
                     {"match_phrase": {"master_normalized_soi_name": {"query": cleaned_family_query, "boost": 25}}},
                     {"match": {"master_normalized_soi_name": {"query": cleaned_family_query, "operator": "or", "minimum_should_match": "70%", "boost": 20}}},
                     {"match_phrase": {"master_normalized_security_name": {"query": cleaned_family_query, "boost": 10}}},
                     {"match": {"master_normalized_security_name": {"query": cleaned_family_query, "operator": "or", "minimum_should_match": "70%", "boost": 8}}},
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
                    "boost": 30,
                }
            }
        },
        {
            "match": {
                "normalized_security_name": {
                    "query": security_query,
                    "operator": "or",
                    "minimum_should_match": "50%",
                    "boost": 10,
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
                "minimum_should_match": "50%",
                "boost": 25,
            }
        },

        {
            "multi_match": {
                "query": security_query,
                "fields": ["normalized_security_name", "normalized_soi_name"],
                "type": "cross_fields",
                "operator": "or",
                "minimum_should_match": "10%",
                "boost": 10,
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
                        # {"multi_match": {
                        #     "query": security_query,
                        #     "fields": ["normalized_company_name^30", "normalized_security_name^50"],
                        #     "type": "phrase",
                        #     "boost": 30,
                        # }},
                        {"match_phrase": {"normalized_security_name": {"query": security_query, "boost": 30}}},
                        {"match_phrase": {"master_normalized_security_name": {"query": security_query, "boost": 30}}},
                        {"match": {"master_normalized_security_name": {"query": security_query, "operator": "or", "minimum_should_match": "70%", "boost": 10}}},
                
                      
                        {"multi_match": {
                            "query": security_query,
                            "fields": ["master_normalized_security_name", "master_normalized_soi_name"],
                            "type": "cross_fields",
                            "operator": "or",
                            "minimum_should_match": "50%",
                            "boost": 25,
                        }},
                        {"multi_match": {
                            "query": security_query,
                            "fields": ["master_normalized_security_name", "master_normalized_security_type"],
                            "type": "cross_fields",
                            "operator": "or",
                            "minimum_should_match": "70%",
                            "boost": 20,
                        }},

                        
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
        # alias-confirmed match, not a fuzzy token-overlap guess -
        # trust it and don't let a weaker cross-family security
        # match override it.
        is_high_confidence_family = best_family.get("score", 0.0) >= 1.0

        candidate_families = (
            [best_family] if is_high_confidence_family else family_matches
        )

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
        "mapped": {
            "mapped_family": None,
            "mapped_security": None,
            "filetype": None,
            "mapped_at": None,
        },
        "match": {
            "top_security": best_family.get("top_security") if best_family else None,
            "family_confidence": best_family.get("score", 0.0) if best_family else 0.0,
            "security_confidence": security_confidence,
            "matched": matched,
            "match_type": "direct" if matched else "unmatched",
        },
        "master_data": (
            {
                "family_name": best_family.get("family_name"),
                "normalized_family_name": best_family.get("normalized_family_name"),
                "soi_name": best_family.get("soi_name"),
                "security_name": best_family.get("top_security"),
                "normalized_security_name": best_family.get("normalized_security_name"),
                "security_type": best_family.get("security_type"),
            }
            if best_family
            else None
        ),

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


@app.route(
    route="map-security",
    methods=["POST"]
)
def map_security_api(
    req: func.HttpRequest
) -> func.HttpResponse:

    logging.info(
        "Received mapping request"
    )

    try:
        body = req.get_json()

        conn_string = os.environ.get("POSTGRES_CONN")
        if not conn_string:
            raise Exception("POSTGRES_CONN not found")

        # Batch mode: a bare JSON array of {input, security_input} objects.
        if isinstance(body, list):
            results = []

            for item in body:
                item_family_string = item.get("input")
                item_security_string = item.get("security_input") or item_family_string

                if not item_family_string:
                    results.append({"error": "Input string is required"})
                    continue

                try:
                    results.append(
                        _resolve_security_mapping(
                            item_family_string,
                            item_security_string,
                            conn_string
                        )
                    )
                except Exception as e:
                    logging.error(f"Error mapping item ({item_family_string}): {e}")
                    results.append({"error": str(e)})

            return func.HttpResponse(
                json.dumps(results),
                status_code=200,
                mimetype="application/json"
            )

        # Single-item mode
        family_string = body.get("input")
        security_string = body.get("security_input") or family_string

        if not family_string:
            return func.HttpResponse(
                json.dumps({
                    "error": "Input string is required"
                }),
                status_code=400,
                mimetype="application/json"
            )

        result = _resolve_security_mapping(family_string, security_string, conn_string)

        return func.HttpResponse(
            json.dumps(result),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as e:

        logging.error(
            f"Error: {str(e)}"
        )

        return func.HttpResponse(
            json.dumps({
                "error": str(e)
            }),

            status_code=500,
            mimetype="application/json"
        )
