import re
import time
import psycopg2
from num2words import num2words
import json
import os
from pathlib import Path


# ─────────────────────────────────────────────
# STOPWORDS
# ─────────────────────────────────────────────
STOPWORDS = {
    "ltd", "inc", "corp", "llc", "lp", "plc",
    "company", "co", "limited", "pvt"
}


# ─────────────────────────────────────────────
# CACHE
# ─────────────────────────────────────────────
ABBREVIATION_MAP = None
LAST_FETCH = 0
CACHE_TTL = 300


# ─────────────────────────────────────────────
# DB LOAD
# ─────────────────────────────────────────────
def load_abbreviations(conn_string):
    try:
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT LOWER(acronym), LOWER(expansion)
            FROM abbreviation_map
            WHERE is_active = TRUE
        """)

        rows = cursor.fetchall()

        cursor.close()
        conn.close()

        print(f"Loaded {len(rows)} abbreviations from DB")
        return {row[0]: row[1] for row in rows}

    except Exception as e:
        print(f"DB LOAD ERROR: {e}")
        return {}


def get_abbreviation_map(conn_string):
    global ABBREVIATION_MAP, LAST_FETCH

    if ABBREVIATION_MAP is None or (time.time() - LAST_FETCH > CACHE_TTL):
        ABBREVIATION_MAP = load_abbreviations(conn_string)
        LAST_FETCH = time.time()

    return ABBREVIATION_MAP


# ─────────────────────────────────────────────
# TOKEN STANDARDIZATION (CANONICAL FORM)
# ─────────────────────────────────────────────
def standardize_token(token: str) -> str:
    token = token.lower()

    # remove separators like / . -
    token = re.sub(r"[./-]", "", token)

    return token


# ─────────────────────────────────────────────
# NUMBER NORMALIZATION
# ─────────────────────────────────────────────
def normalize_numbers_tokens(tokens):
    result = []

    for token in tokens:
        match = re.match(r"^(\d+)(st|nd|rd|th)$", token)

        if match:
            num = int(match.group(1))
            try:
                word = num2words(num, to="ordinal")
                result.extend(word.split())
            except:
                result.append(token)
        else:
            # 🔥 keep cardinal numbers unchanged
            result.append(token)

    return result
# ─────────────────────────────────────────────
# ABBREVIATION EXPANSION (SMART)
# ─────────────────────────────────────────────
def expand_tokens(tokens, conn_string):
    abbrev_map = get_abbreviation_map(conn_string)
    expanded = []

    i = 0
    while i < len(tokens):

        # 🔥 Combine tokens (t + l → tl)
        if i < len(tokens) - 1:
            combined = standardize_token(tokens[i] + tokens[i + 1])

            if combined in abbrev_map:
                expanded.append(abbrev_map[combined])
                i += 2
                continue

        # single token
        clean = standardize_token(tokens[i])

        if clean in abbrev_map:
            expanded.append(abbrev_map[clean])
        else:
            expanded.append(clean)

        i += 1

    return expanded


# ─────────────────────────────────────────────
# CORE NORMALIZATION
# ─────────────────────────────────────────────
def normalize(text: str, conn_string: str) -> str:
    if not text:
        return ""

    # 1. lowercase
    text = text.lower()

    # 2. remove brackets only
    text = re.sub(r"[()]", "", text)

    # 3. normalize separators BEFORE tokenization
    text = re.sub(r"[./-]", " ", text)   # T/L → T L

    # 4. tokenize
    tokens = text.split()

    # 5. expand abbreviations FIRST
    tokens = expand_tokens(tokens, conn_string)

    # 6. normalize numbers
    tokens = normalize_numbers_tokens(tokens)

    # 7. clean tokens
    tokens = [re.sub(r"[^\w\s]", "", t) for t in tokens]

    # 8. remove stopwords
    tokens = [t for t in tokens if t and t not in STOPWORDS]

    return " ".join(tokens)


# ─────────────────────────────────────────────
# INPUT NORMALIZATION
# ─────────────────────────────────────────────
def normalize_input(input_text: str, conn_string: str) -> dict:
    return {
        "raw_input": input_text,
        "normalized_query": normalize(input_text, conn_string)
    }


# ─────────────────────────────────────────────
# MASTER NORMALIZATION
# ─────────────────────────────────────────────
def normalize_master_record(record: dict, conn_string: str) -> dict:
    raw_name = record.get("security_name", "")

    return {
        "raw_name": raw_name,
        "normalized_name": normalize(raw_name, conn_string)
    }


# ─────────────────────────────────────────────
# CONNECTION RESOLVER
# ─────────────────────────────────────────────
def resolve_conn_string() -> str:
    conn = os.environ.get("POSTGRES_CONN")
    if conn:
        return conn

    possible_paths = [
        Path.cwd() / "local.settings.json",
        Path(__file__).parent / "local.settings.json"
    ]

    for path in possible_paths:
        if path.exists():
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
                conn = data.get("Values", {}).get("POSTGRES_CONN")
                if conn:
                    return conn

    raise Exception("POSTGRES_CONN not found")


# ─────────────────────────────────────────────
# CSV NORMALIZATION
# ─────────────────────────────────────────────
def normalize_csv_security_names(
    input_csv,
    output_csv,
    security_name_header="Security Name",
    soi_name_header="SOI Name"
):

    import csv

    conn_string = resolve_conn_string()

    with open(input_csv, encoding="utf-8-sig") as infile, \
         open(output_csv, "w", encoding="utf-8", newline="") as outfile:

        reader = csv.DictReader(infile)

        fieldnames = reader.fieldnames + [
            "normalized_security_name",
            "normalized_soi_name"
        ]

        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            sec = row.get(security_name_header, "")
            soi = row.get(soi_name_header, "")

            row["normalized_security_name"] = normalize(sec, conn_string)
            row["normalized_soi_name"] = normalize(soi, conn_string)

            writer.writerow(row)

    print("CSV normalization complete")