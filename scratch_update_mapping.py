import json
from elasticsearch import Elasticsearch

# Connection settings matching local.settings.json
ES_URL = "https://135.119.88.170:9200"
ES_USER = "elastic"
ES_PASS = "YJF=5hPRz+IJgPoMRUSk"
INDEX_NAME = "security_master_v4"

es = Elasticsearch(
    ES_URL,
    basic_auth=(ES_USER, ES_PASS),
    verify_certs=False
)

# Load the new properties from es_index_mapping.json
with open("es_index_mapping.json", "r") as f:
    mapping_data = json.load(f)

# Extract only the properties mapping to put
properties = mapping_data["mappings"]["properties"]

print("Updating index properties on live index:", INDEX_NAME)
try:
    response = es.indices.put_mapping(
        index=INDEX_NAME,
        body={"properties": properties}
    )
    print("Success:", response)
except Exception as e:
    print("Failed to update mapping:", e)
