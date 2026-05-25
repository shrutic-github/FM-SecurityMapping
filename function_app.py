import logging
import os
import azure.functions as func
import psycopg2
import json
import math
from elasticsearch import Elasticsearch
 
from normalization import normalize_input
 
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
        "co",
        "limited",
        "pvt",
        "holdings",
        "holding",
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
def extract_type(q):
    q = q.lower()
    if "delayed draw" in q:
        return "ddtl"
    elif "term loan" in q:
        return "tl"
    elif "revolver" in q:
        return "rev"
    elif "equity" in q:
        return "equity"
    return None
 
 
# -----------------------------
# Family-Level Type Boost
# -----------------------------
def boost_by_type(matches, family_query):
    input_type = extract_type(family_query)
    if not input_type:
        return matches
 
    for m in matches:
        name = (
            (m.get("normalized_name") or "")
            + " "
            + (m.get("top_security") or "")
        ).lower()
 
        score = m["score"]
 
        if input_type == "ddtl":
            if "delayed draw" in name:
                score += 0.3
            elif "term loan" in name:
                score -= 0.1
 
        elif input_type == "tl":
            if "term loan" in name:
                score += 0.2
            elif "revolver" in name:
                score -= 0.1
 
        elif input_type == "rev":
            if "revolver" in name:
                score += 0.2
            else:
                score -= 0.1
 
        elif input_type == "equity":
            if "equity" in name:
                score += 0.2
            else:
                score -= 0.1
 
        m["score"] = round(score, 4)
 
    matches.sort(key=lambda x: x["score"], reverse=True)
    return matches
 
 
# -----------------------------
# Retrieve Best Families
# -----------------------------
def search_family_matches(family_query: str) -> list[dict]:
 
    es = get_es_client()
 
    index_name = os.environ.get(
        "ES_INDEX",
        "security_master_v1"
    )
 
    top_k = int(os.environ.get("MATCH_TOP_K", "5"))
 
    cleaned_family_query = clean_query_for_broad_retrieval(family_query)

    _tokens = (cleaned_family_query or family_query or "").split()
    primary_token = next((t for t in _tokens if len(t) >= 3), _tokens[0] if _tokens else "")
 
 
    issuer_mm = os.environ.get("ISSUER_GATE_MIN_SHOULD_MATCH","40%")
 
    # -----------------------------
    # Issuer Gate
    # -----------------------------
    issuer_gate = {
        "bool": {
            "should": [
                {"match": {"normalized_family_name": {"query": cleaned_family_query,"operator": "or","minimum_should_match": issuer_mm,}}},
                {
                    "match": {
                        "normalized_soi_name": {
                            "query": cleaned_family_query,
                            "operator": "or",
                            "minimum_should_match": issuer_mm,
                        }
                    }
                },
                {
                    "match_phrase": {
                        "normalized_family_name": {
                            "query": cleaned_family_query
                        }
                    }
                },
            ],
            "minimum_should_match": 1,
        }
    }
    # -----------------------------
    # Family Retrieval Logic
    # -----------------------------
    should_clauses = [
 
        # ---- Anchor tier ----
        {
            "term": {
                "normalized_family_name.keyword": {
                    "value": primary_token,
                    "case_insensitive": True,
                    "boost": 25,
                }
            }
        },
        {
            "match": {
                "normalized_soi_name": {
                    "query": primary_token,
                    "boost": 25,
                }
            }
        },
        {
            "match": {
            "normalized_family_name": {
            "query": primary_token,   # "osg"
            "boost": 25,
        }
    }
},
 
        # ---- Security tier ----
        {
            "term": {
                "normalized_security_name.keyword": {
                    "value": family_query,
                    "boost": 20,
                }
            }
        },
        {
            "match_phrase": {
                "normalized_security_name": {
                    "query": family_query,
                    "boost": 20,
                }
            }
        },
        {
            "match": {
                "normalized_security_name": {
                    "query": family_query,
                    "operator": "and",
                    "boost": 20,
                }
            }
        },
 
        # ---- SOI tier ----
        {
            "match_phrase": {
                "normalized_soi_name": {
                    "query": cleaned_family_query,
                    "boost": 12,
                }
            }
        },
        {
            "match": {
                "normalized_soi_name": {
                    "query": cleaned_family_query,
                    "operator": "or",
                    "minimum_should_match": "50%",
                    "boost": 12,
                }
            }
        },
 
        # ---- Family tier ----
        {
            "match_phrase": {
                "normalized_family_name": {
                    "query": cleaned_family_query,
                    "boost": 12,
                }
            }
        },
        {
            "match": {
                "normalized_family_name": {
                    "query": cleaned_family_query,
                    "operator": "or",
                    "minimum_should_match": "50%",
                    "boost": 12,
                }
            }
        },
    ]
 
    body = {
        "size": top_k,
        "query": {
            "bool": {
                "must": [issuer_gate],
                "should": should_clauses,
                "minimum_should_match": 0,
            }
        },
 
        # IMPORTANT
        # one representative hit per family
        "collapse": {
            "field": "family_name.keyword"
        }
    }
 
    response = es.search(
        index=index_name,
        body=body
    )
 
    hits = response.get("hits", {}).get("hits", [])
 
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
                "family_name": source.get(
                    "family_name",
                    ""
                ),

                "normalized_family_name": source.get(
                    "normalized_family_name",
                    ""
                ),

                "top_security": source.get(
                    "security_name",
                    ""
                ),
 
                "soi_name": source.get(
                    "soi_name",
                    ""
                ),
 
                "security_type": source.get(
                    "security_type",
                    ""
                ),
 
                "normalized_security_name": norm_sec,
 
                "score": round(es_scaled, 4),
 
                "raw_es_score": round(
                    raw_es_score,
                    4
                ),
            }
        )
 
    matches.sort(
        key=lambda x: x["score"],
        reverse=True
    )
 
    return matches
 
 
# -----------------------------
# Fetch Securities Of Family
# -----------------------------
def fetch_securities_by_family(
    normalized_family_name: str
) -> list:
 
    es = get_es_client()
 
    index_name = os.environ.get(
        "ES_INDEX",
        "security_master_v1"
    )
 
    body = {
        "size": 100,
        "query": {
            "term": {
                "normalized_family_name.keyword": normalized_family_name
            }
        },
 
        "_source": [
            "security_name",
            "security_type",
            "normalized_soi_name",
            "normalized_security_name",
            "normalized_family_name",
        ],
    }
 
    response = es.search(
        index=index_name,
        body=body
    )
 
    hits = response.get("hits",{}).get("hits",[])
 
    securities = []
 
    for hit in hits:
 
        src = hit.get("_source", {})
 
        securities.append({
            "security_name": src.get(
                "security_name",
                ""
            ),
 
            "security_type": src.get(
                "security_type",
                ""
            ),
 
            "normalized_soi_name": src.get(
                "normalized_soi_name",
                ""
            ),
 
            "normalized_security_name": src.get(
                "normalized_security_name",
                ""
            ),
 
            "normalized_family_name": src.get(
                "normalized_family_name",
                ""
            ),
 
            "score": 0.0
        })
 
    return securities
 
 
# -----------------------------
# ES-Based Security Search
# -----------------------------
def search_securities_es(
    security_query: str,
    family_matches: list
) -> list:
 
    es = get_es_client()
 
    index_name = os.environ.get(
        "ES_INDEX",
        "security_master_v1"
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
            "term": {
                "normalized_security_name.keyword": {
                    "value": security_query,
                    "boost": 25,
                    
                }
            }
        },
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
                    "operator": "and",
                    "boost": 25,
                }
            }
        },
        {
            "match": {
                "normalized_security_name": {
                    "query": security_query,
                    "operator": "or",
                    "minimum_should_match": "50%",
                    "boost": 30,
                }
            }
        },

        # ---- Normalized SOI Name Tier ----
        {
            "match_phrase": {
                "normalized_soi_name": {
                    "query": security_query,
                    "boost": 15,
                }
            }
        },
        {
            "match": {
                "normalized_soi_name": {
                    "query": security_query,
                    "operator": "or",
                    "minimum_should_match": "50%",
                    "boost": 10,
                }
            }
        },

        # ---- Security Type Tier ----
        {
            "term": {
                "security_type": {
                    "value": security_query,
                    "case_insensitive": True,
                    "boost": 15,
                }
            }
        },
        {
            "match_phrase": {
                "security_type": {
                    "query": security_query,
                    "boost": 15,
                }
            }
        }
    ]
 
    # ---- Family score weighting via function_score ----
    functions = [
        {
            "filter": {
                "term": {
                    "normalized_family_name.keyword": fam["normalized_family_name"]
                }
            },
            "weight": fam["score"] * family_weight,
        }
        for fam in family_matches
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
                        "should": should_clauses,
                        "minimum_should_match": 0,
                    }
                },
                "functions": functions,
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
 
    hits = es.search(
        index=index_name,
        body=body
    ).get("hits", {}).get("hits", [])
 
    securities = []
 
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
 
    return securities
 
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
 
        logging.info(
            f"Input received: {family_string} | security_input: {security_string}"
        )
 
        # -----------------------------
        # PostgreSQL Validation
        # -----------------------------
        try:
 
            conn_string = os.environ.get(
                "POSTGRES_CONN"
            )
 
            if not conn_string:
                raise Exception(
                    "POSTGRES_CONN not found"
                )
 
            conn = psycopg2.connect(
                conn_string
            )
 
            conn.close()
 
        except Exception as e:
 
            logging.error(
                f"PostgreSQL connection failed: {e}"
            )
 
        # -----------------------------
        # Normalize
        # -----------------------------
        conn_string = os.environ.get(
            "POSTGRES_CONN"
        )
 
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
        # Retrieve Families
        # -----------------------------
        family_matches = search_family_matches(
            family_query
        )
 
        family_matches = boost_by_type(
            family_matches,
            family_query
        )
 
        best_family = (
            family_matches[0]
            if family_matches
            else None
        )
 
        matched = False
        reranked_securities = []
 
        # -----------------------------
        # ES Security Search
        # -----------------------------
        if best_family:
 
            reranked_securities = search_securities_es(
                security_query,
                family_matches
            )
 
            if reranked_securities:
                matched = True
                best_family["top_security"] = (
                    reranked_securities[0]["security_name"]
                )
 
        # -----------------------------
        # Final Response
        # -----------------------------
        result = {
            "input": family_string,
 
            "security_input": security_string,
 
            "normalized": family_query,
 
            "security_normalized": security_query,
 
            "matched": matched,
 
            "best_family_match": best_family,
 
            "top_matching_families": [
                {
                    "family_name": f["family_name"],
                    "normalized_family_name": f["normalized_family_name"]
                }
                for f in family_matches
            ],
 
            "ranked_family_securities":
                reranked_securities,
        }
 
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
 
 