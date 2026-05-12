from elasticsearch import Elasticsearch

es = Elasticsearch(
    "https://135.119.88.170:9200",
    basic_auth=("elastic", "YJF=5hPRz+IJgPoMRUSk"),
    verify_certs=False
)

print("Ping:", es.ping())

try:
    info = es.info()
    print("Connected to ES:")
    print(info)
except Exception as e:
    print("Connection failed:", e)