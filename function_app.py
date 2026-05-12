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

    verify_certs = os.environ.get("ES_VERIFY_CERTS", "true").lower() == "true"
    username = os.environ.get("ES_USERNAME")
    password = os.environ.get("ES_PASSWORD")
    api_key = os.environ.get("ES_API_KEY")

    kwargs = {"hosts": [es_url], "verify_certs": verify_certs, "request_timeout": 15}

    if api_key:
        kwargs["api_key"] = api_key
    elif username and password:
        kwargs["basic_auth"] = (username, password)

    ES_CLIENT = Elasticsearch(**kwargs)
    return ES_CLIENT


# -----------------------------
# ES Score Scaling (IMPORTANT)
# -----------------------------
def _es_scaled(raw_es_score: float) -> float:
    cap = float(os.environ.get("ES_SCORE_LOG_CAP", "600.0"))
    if raw_es_score <= 0 or cap <= 0:
        return 0.0

    scaled = math.log1p(raw_es_score) / math.log1p(cap)
    return max(0.0, min(scaled, 1.0))

#-------------------------------
# Reranking Boost by Type
#-------------------------------
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


def boost_by_type(matches, normalized_query):
    input_type = extract_type(normalized_query)

    if not input_type:
        return matches

    for m in matches:
        name = (
            (m.get("normalized_name") or "") + " " +
            (m.get("security_name") or "")
        ).lower()

        score = m["score"]

        # -----------------------------
        # STRONG TYPE MATCH LOGIC
        # -----------------------------

        if input_type == "ddtl":
            if "delayed draw" in name:
                score += 0.3   # strong boost
            elif "term loan" in name:
                score -= 0.1   # penalty

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

        m["score"] = score

    matches.sort(key=lambda x: x["score"], reverse=True)
    return matches
# -----------------------------
# Search Function (ES ONLY)
# -----------------------------
def search_matches(normalized_query: str) -> list[dict]:
    es = get_es_client()
    index_name = os.environ.get("ES_INDEX", "security_master_v4")
    top_k = int(os.environ.get("MATCH_TOP_K", "20"))

    primary_token = normalized_query.split()[0] if normalized_query else ""

    # Family-first anchor + weighted Mastercomp security, SOI, security_type (no multi_match).
    body = {
        "size": top_k,
        "query": {
            "bool": {
                "should": [
                    # ---- Anchor (family + token hits on core fields) ----
                    {
                        "term": {
                            "family_name.keyword": {
                                "value": primary_token,
                                "case_insensitive": True,
                                "boost": 30,
                            }
                        }
                    },
                    {
                        "match": {
                            "security_name": {
                                "query": primary_token,
                                "operator": "and",
                                "boost": 14,
                            }
                        }
                    },
                    {
                        "match": {
                            "normalized_name": {
                                "query": primary_token,
                                "boost": 10,
                            }
                        }
                    },
                    {
                        "match": {
                            "soi_name": {
                                "query": primary_token,
                                "boost": 10,
                            }
                        }
                    },

                    # ---- Security tier (Mastercomp normalized + display name) ----
                    {
                        "term": {
                            "normalized_name.keyword": {
                                "value": normalized_query,
                                "boost": 15,
                            }
                        }
                    },
                    {
                        "match_phrase": {
                            "normalized_name": {
                                "query": normalized_query,
                                "boost": 10,
                            }
                        }
                    },
                    {
                        "match": {
                            "normalized_name": {
                                "query": normalized_query,
                                "operator": "and",
                                "boost": 7,
                            }
                        }
                    },
                    {
                        "match_phrase": {
                            "security_name": {
                                "query": normalized_query,
                                "boost": 8,
                            }
                        }
                    },
                    {
                        "match": {
                            "security_name": {
                                "query": normalized_query,
                                "operator": "and",
                                "boost": 5,
                            }
                        }
                    },

                    # ---- SOI tier ----
                    {
                        "match_phrase": {
                            "normalized_soi_name": {
                                "query": normalized_query,
                                "boost": 6,
                            }
                        }
                    },
                    {
                        "match": {
                            "normalized_soi_name": {
                                "query": normalized_query,
                                "operator": "or",
                                "minimum_should_match": "50%",
                                "boost": 4,
                            }
                        }
                    },
                    {
                        "match_phrase": {
                            "soi_name": {
                                "query": normalized_query,
                                "boost": 5,
                            }
                        }
                    },
                    {
                        "match": {
                            "soi_name": {
                                "query": normalized_query,
                                "operator": "or",
                                "boost": 3,
                            }
                        }
                    },

                    # ---- Family tier ----
                    {
                        "match_phrase": {
                            "family_name": {
                                "query": normalized_query,
                                "boost": 2,
                            }
                        }
                    },
                    {
                        "match": {
                            "family_name": {
                                "query": normalized_query,
                                "operator": "or",
                                "minimum_should_match": "50%",
                                "boost": 1,
                            }
                        }
                    },

                    # ---- Security type (refinement) ----
                    {
                        "match_phrase": {
                            "security_type": {
                                "query": normalized_query,
                                "boost": 10,
                            }
                        }
                    },
                    {
                        "match": {
                            "security_type": {
                                "query": normalized_query,
                                "operator": "or",
                                "boost": 3,
                            }
                        }
                    },
                ],
                "minimum_should_match": 1,
            }
        },
        "collapse": {"field": "family_name.keyword"},
    }

    response = es.search(index=index_name, body=body)
    hits = response.get("hits", {}).get("hits", [])

    matches = []
    for hit in hits:
        source = hit.get("_source", {})
        raw_es_score = float(hit.get("_score", 0.0))
        es_scaled = _es_scaled(raw_es_score)
        norm_sec = source.get("normalized_security_name") or source.get(
            "normalized_name", ""
        )

        matches.append(
            {
                "security_name": source.get("security_name", ""),
                "score": round(es_scaled, 4),
                "raw_es_score": round(raw_es_score, 4),
                "soi_name": source.get("soi_name", ""),
                "family_name": source.get("family_name", ""),
                "security_type": source.get("security_type", ""),
                "normalized_name": norm_sec,
            }
        )

    matches.sort(key=lambda x: x["score"], reverse=True)
    return matches


# -----------------------------
# API ENTRY POINT
# -----------------------------
@app.route(route="map-security", methods=["POST"])
def map_security_api(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Received mapping request")

    try:
        body = req.get_json()
        input_string = body.get("input")

        if not input_string:
            return func.HttpResponse(
                json.dumps({"error": "Input string is required"}),
                status_code=400,
                mimetype="application/json"
            )

        logging.info(f"Input received: {input_string}")

        # PostgreSQL connection (for normalization)
        try:
            conn_string = os.environ.get('POSTGRES_CONN')
            if not conn_string:
                raise Exception("POSTGRES_CONN not found")

            conn = psycopg2.connect(conn_string)
            conn.close()

        except Exception as e:
            logging.error(f"PostgreSQL connection failed: {e}")

        # NORMALIZATION
        conn_string = os.environ.get('POSTGRES_CONN')
        normalized_result = normalize_input(input_string, conn_string)

        matches = search_matches(normalized_result["normalized_query"])

        matches = boost_by_type(matches, normalized_result["normalized_query"])

        threshold = float(os.environ.get("MATCH_SCORE_THRESHOLD", "0.7"))
        top_score = matches[0]["score"] if matches else 0.0
        matched = bool(matches) and top_score >= threshold

        result = {
            "input": input_string,
            "normalized": normalized_result["normalized_query"],
            "matched": matched,
            "best_match": matches[0] if matched else None,
            "matches": matches
        }

        return func.HttpResponse(
            json.dumps(result),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as e:
        logging.error(f"Error: {str(e)}")

        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )
