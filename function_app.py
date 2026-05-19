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
)


def clean_query_for_broad_retrieval(normalized_query: str) -> str:

    t = (normalized_query or "").strip().lower()
    for phrase in GENERIC_RETRIEVAL_PHRASES:
        t = t.replace(phrase, " ")

    tokens = [
        w for w in t.split()
        if w and w not in GENERIC_RETRIEVAL_STOPWORDS
    ]

    cleaned = " ".join(tokens)

    return cleaned if cleaned else (
        normalized_query or ""
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
# Retrieve Best Families
# -----------------------------
def search_family_matches(normalized_query: str) -> list[dict]:

    es = get_es_client()

    index_name = os.environ.get(
        "ES_INDEX",
        "security_master_v4"
    )

    top_k = int(os.environ.get("MATCH_TOP_K", "5"))

    cleaned = clean_query_for_broad_retrieval(
        normalized_query
    )

    primary_token = (
        cleaned.split()[0]
        if cleaned
        else (
            normalized_query.split()[0]
            if normalized_query
            else ""
        )
    )

    issuer_mm = os.environ.get(
        "ISSUER_GATE_MIN_SHOULD_MATCH",
        "40%"
    )

    # -----------------------------
    # Issuer Gate
    # -----------------------------
    issuer_gate = {
        "bool": {
            "should": [
                {
                    "match": {
                        "family_name": {
                            "query": cleaned,
                            "operator": "or",
                            "minimum_should_match": issuer_mm,
                        }
                    }
                },
                {
                    "match": {
                        "normalized_soi_name": {
                            "query": cleaned,
                            "operator": "or",
                            "minimum_should_match": issuer_mm,
                        }
                    }
                },
                {
                    "match_phrase": {
                        "family_name": {
                            "query": cleaned
                        }
                    }
                },
                {
                    "match_phrase": {
                        "normalized_soi_name": {
                            "query": cleaned
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

        # strong family anchor
        {
            "term": {
                "family_name.keyword": {
                    "value": primary_token,
                    "case_insensitive": True,
                    "boost": 30,
                }
            }
        },

        # family
        {
            "match_phrase": {
                "family_name": {
                    "query": cleaned,
                    "boost": 12,
                }
            }
        },
        {
            "match": {
                "family_name": {
                    "query": cleaned,
                    "operator": "or",
                    "minimum_should_match": "50%",
                    "boost": 8,
                }
            }
        },

        # SOI
        {
            "match_phrase": {
                "normalized_soi_name": {
                    "query": cleaned,
                    "boost": 10,
                }
            }
        },
        {
            "match": {
                "normalized_soi_name": {
                    "query": cleaned,
                    "operator": "or",
                    "minimum_should_match": "50%",
                    "boost": 5,
                }
            }
        },

        # normalized security
        {
            "match_phrase": {
                "normalized_name": {
                    "query": normalized_query,
                    "boost": 8,
                }
            }
        },

        {
            "match": {
                "normalized_name": {
                    "query": normalized_query,
                    "operator": "and",
                    "boost": 5,
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

        matches.append(
            {
                "family_name": source.get(
                    "family_name",
                    ""
                ),

                "top_security": source.get(
                    "security_name",
                    ""
                ),

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
    family_name: str
) -> list:

    es = get_es_client()

    index_name = os.environ.get(
        "ES_INDEX",
        "security_master_v4"
    )

    body = {
        "size": 100,
        "query": {
            "term": {
                "family_name.keyword": family_name
            }
        },

        "_source": [
            "security_name",
            "security_type",
            "normalized_soi_name",
            "normalized_security_name",
            "normalized_name",
            "family_name",
        ],
    }

    response = es.search(
        index=index_name,
        body=body
    )

    hits = response.get(
        "hits",
        {}
    ).get(
        "hits",
        []
    )

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

            "normalized_name": (
                src.get(
                    "normalized_security_name"
                )
                or src.get(
                    "normalized_name",
                    ""
                )
            ),

            "family_name": src.get(
                "family_name",
                ""
            ),

            "score": 0.0
        })

    return securities


# -----------------------------
# Rerank Securities INSIDE FAMILY
# -----------------------------
def rerank_family_securities(
    securities,
    normalized_query
):

    input_type = extract_type(
        normalized_query
    )

    q = normalized_query.lower()

    for s in securities:

        score = 0.0

        name = (
            (
                s.get("normalized_name", "")
                + " "
                + s.get("security_name", "")
            ).lower()
        )

        # -----------------------------
        # Exact Phrase
        # -----------------------------
        if q in name:
            score += 1.0

        # -----------------------------
        # Token Overlap
        # -----------------------------
        q_tokens = set(q.split())
        n_tokens = set(name.split())

        overlap = len(
            q_tokens.intersection(n_tokens)
        )

        score += overlap * 0.1

        # -----------------------------
        # Type Boosting
        # -----------------------------
        if input_type == "ddtl":

            if "delayed draw" in name:
                score += 0.5

            elif "term loan" in name:
                score -= 0.2

        elif input_type == "tl":

            if "term loan" in name:
                score += 0.4

            elif "revolver" in name:
                score -= 0.2

        elif input_type == "rev":

            if "revolver" in name:
                score += 0.4

            else:
                score -= 0.2

        elif input_type == "equity":

            if "equity" in name:
                score += 0.4

            else:
                score -= 0.2

        s["score"] = round(score, 4)

    securities.sort(
        key=lambda x: x["score"],
        reverse=True
    )

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
        input_string = body.get("input")

        if not input_string:
            return func.HttpResponse(
                json.dumps({
                    "error": "Input string is required"
                }),

                status_code=400,
                mimetype="application/json"
            )

        logging.info(
            f"Input received: {input_string}"
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
            input_string,
            conn_string
        )

        normalized_query = normalized_result[
            "normalized_query"
        ]

        # -----------------------------
        # Retrieve Families
        # -----------------------------
        family_matches = search_family_matches(
            normalized_query
        )

        best_family = (
            family_matches[0]
            if family_matches
            else None
        )

        matched = best_family is not None

        reranked_securities = []

        # -----------------------------
        # Fetch + Rerank Securities
        # -----------------------------
        if matched:

            family_securities = (
                fetch_securities_by_family(
                    best_family["family_name"]
                )
            )

            reranked_securities = (
                rerank_family_securities(
                    family_securities,
                    normalized_query
                )
            )

            if reranked_securities:
                best_family["top_security"] = (
                    reranked_securities[0]["security_name"]
                )

        # -----------------------------
        # Final Response
        # -----------------------------
        result = {
            "input": input_string,

            "normalized": normalized_query,

            "matched": matched,

            "best_family_match": best_family,

            "top_matching_families": [
                {k: v for k, v in f.items() if k != "top_security"}
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