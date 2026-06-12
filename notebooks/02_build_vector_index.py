# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Build Vector Search index over the address reference table
# MAGIC
# MAGIC We embed `search_text` (normalized delivery line + city/state/zip) with the
# MAGIC Databricks-managed `databricks-gte-large-en` endpoint. At runtime we query
# MAGIC this index for top-K candidates when an exact hash-join misses.

# COMMAND ----------

dbutils.widgets.text("catalog", "address_reference")
dbutils.widgets.text("schema", "us")
dbutils.widgets.text("endpoint", "address_verify_vs")
dbutils.widgets.text("index_name", "address_reference_vs_idx")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
ENDPOINT = dbutils.widgets.get("endpoint")
INDEX = f"{CATALOG}.{SCHEMA}.{dbutils.widgets.get('index_name')}"
SOURCE = f"{CATALOG}.{SCHEMA}.address_reference"

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient

vsc = VectorSearchClient(disable_notice=True)

existing = {e["name"] for e in vsc.list_endpoints().get("endpoints", [])}
if ENDPOINT not in existing:
    vsc.create_endpoint(name=ENDPOINT, endpoint_type="STANDARD")

# COMMAND ----------

# MAGIC %md ### Create or sync the index
# MAGIC
# MAGIC Delta Sync Index: Databricks keeps the index in sync with the source table
# MAGIC automatically. We embed `search_text` with the managed GTE endpoint.

index_exists = any(
    i["name"] == INDEX for i in vsc.list_indexes(ENDPOINT).get("vector_indexes", [])
)

if not index_exists:
    vsc.create_delta_sync_index(
        endpoint_name=ENDPOINT,
        index_name=INDEX,
        source_table_name=SOURCE,
        pipeline_type="TRIGGERED",
        primary_key="oa_hash",
        embedding_source_column="search_text",
        embedding_model_endpoint_name="databricks-gte-large-en",
    )
else:
    vsc.get_index(ENDPOINT, INDEX).sync()

# COMMAND ----------

# MAGIC %md ### Smoke test: query the index

idx = vsc.get_index(ENDPOINT, INDEX)
result = idx.similarity_search(
    query_text="1600 PENNSYLVANIA AVE NW WASHINGTON DC 20500",
    columns=["delivery_line", "city", "state", "zipcode", "latitude", "longitude"],
    num_results=5,
)
display(result)
