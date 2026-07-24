# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Load US address reference data into Unity Catalog
# MAGIC
# MAGIC Replaces Informatica's proprietary reference databases with free US sources:
# MAGIC
# MAGIC | Source | Purpose | Refresh |
# MAGIC |---|---|---|
# MAGIC | OpenAddresses (US) | ~200M addresses with rooftop lat/lon | Quarterly |
# MAGIC | Census ZCTA / TIGER | ZIP code tabulation area polygons | Annual |
# MAGIC | Census ACS 5-year | Demographic enrichment (replaces CAMEO) | Annual |
# MAGIC | USPS Pub 28 | Suffix/unit abbreviations | Static (in code) |
# MAGIC
# MAGIC Output: `${catalog}.${schema}.*` Delta tables, clustered for join performance.

# COMMAND ----------

# MAGIC %md ## Install the local `address_verify` package
# MAGIC
# MAGIC Self-locating: we derive the repo root from this notebook's own path, so it works
# MAGIC regardless of which user or Workspace folder the repo lives in — no hardcoded name.
# MAGIC We use `%pip install -e` (not a `sys.path` append) because the standardizer runs
# MAGIC inside a Spark UDF on the executors, and notebook-scoped `%pip` distributes the
# MAGIC package to them. This cell triggers a Python restart, so it must run first.
# MAGIC
# MAGIC The `%pip` magic can't read a Python variable, so we invoke it via
# MAGIC `run_line_magic` with the computed path — this keeps the install notebook-scoped.

# COMMAND ----------

# Derive the repo root (the folder containing pyproject.toml) from this notebook's path:
# /Workspace/<...>/addressdoctor/notebooks/01_... -> /Workspace/<...>/addressdoctor
_nb_path = (
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)
repo_root = "/Workspace" + "/".join(_nb_path.split("/")[:-2])
print(f"Installing address_verify (editable) from: {repo_root}")

get_ipython().run_line_magic("pip", f"install -q -e {repo_root}")

# COMMAND ----------

# MAGIC %md Restart Python so the freshly installed package is importable, then continue.

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "address_reference")
dbutils.widgets.text("schema", "us")
dbutils.widgets.text("openaddresses_volume", "/Volumes/address_reference/us/openaddresses")

# COMMAND ----------

# MAGIC %md Read the widget values. Adjust the widgets above if yours differ from the
# MAGIC defaults, then re-run this cell to pick up the new values.

# COMMAND ----------

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
OA_VOL = dbutils.widgets.get("openaddresses_volume")

# COMMAND ----------

# MAGIC %md Set the session context. The catalog and schema are expected to already
# MAGIC exist.

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

# MAGIC %md ## OpenAddresses
# MAGIC
# MAGIC Download the OpenAddresses US address extracts from
# MAGIC https://batch.openaddresses.io/data (US collection) to the Unity Catalog
# MAGIC volume, preserving the per-state subdirectory layout
# MAGIC (`{volume}/<state>/<file>-addresses-<scope>.geojson`), then land as Delta.
# MAGIC Schema reference:
# MAGIC https://github.com/openaddresses/openaddresses/blob/master/CONTRIBUTING.md
# MAGIC
# MAGIC The extracts ship as newline-delimited GeoJSON (one `Feature` per line), which
# MAGIC `spark.read.json` reads natively. We ingest every `*-addresses-*.geojson`
# MAGIC (statewide + county + city layers) and de-duplicate on the OpenAddresses `hash`
# MAGIC so rows that appear in both a statewide file and a county/city file collapse.
# MAGIC Non-address layers (`-parcels-`, `-buildings-`, `-centerlines-`) are excluded by
# MAGIC the glob.

# COMMAND ----------

from pyspark.sql import functions as F

oa_raw = (
    spark.read.json(f"{OA_VOL}/*/*-addresses-*.geojson")
    .select(
        F.col("geometry.coordinates")[0].cast("double").alias("longitude"),  # [lon, lat]
        F.col("geometry.coordinates")[1].cast("double").alias("latitude"),
        F.col("properties.number").alias("primary_number"),
        F.col("properties.street").alias("street_raw"),
        F.col("properties.unit").alias("secondary_raw"),
        F.col("properties.city").alias("city"),
        F.col("properties.region").alias("state"),
        F.col("properties.postcode").alias("zipcode"),
        F.col("properties.hash").alias("oa_hash"),
    )
    .filter(F.col("state").isNotNull() & F.col("zipcode").isNotNull())
    .dropDuplicates(["oa_hash"])  # dedup statewide vs county/city overlap
)

(
    oa_raw.write.mode("overwrite")
    .option("delta.columnMapping.mode", "name")
    .clusterBy("state", "zipcode")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.openaddresses_us")
)

# COMMAND ----------

# MAGIC %md ## Normalize OpenAddresses through our Pub 28 standardizer
# MAGIC
# MAGIC This produces the reference table we'll build the Vector Search index over.
# MAGIC Every reference row gets split into the same AddressSchema fields and
# MAGIC standardized identically to runtime inputs, so exact-match joins are possible.

# COMMAND ----------

from pyspark.sql.types import StringType, StructField, StructType
from address_verify.schemas import AddressSchema
from address_verify.standardize import standardize_address, format_delivery_line


REF_SCHEMA = StructType([
    StructField("primary_number", StringType(), True),
    StructField("street_predirection", StringType(), True),
    StructField("street_name", StringType(), True),
    StructField("street_suffix", StringType(), True),
    StructField("street_postdirection", StringType(), True),
    StructField("secondary_designator", StringType(), True),
    StructField("secondary_number", StringType(), True),
    StructField("normalized_line", StringType(), True),
])


@F.udf(returnType=REF_SCHEMA)
def _split_street_udf(primary_number: str, street_raw: str, secondary_raw: str):
    """Rough splitter for OpenAddresses STREET free-text. TODO: switch to libpostal
    when the native Spark wrapper is installed on the cluster."""
    toks = (street_raw or "").upper().split()
    pre = toks[0] if toks and toks[0] in {"N", "S", "E", "W", "NE", "NW", "SE", "SW"} else None
    if pre:
        toks = toks[1:]
    post = toks[-1] if toks and toks[-1] in {"N", "S", "E", "W", "NE", "NW", "SE", "SW"} else None
    if post:
        toks = toks[:-1]
    suffix = toks[-1] if toks else None
    name = " ".join(toks[:-1]) if toks else None

    parsed = AddressSchema(
        primary_number=primary_number, street_predirection=pre,
        street_name=name, street_suffix=suffix, street_postdirection=post,
        secondary_designator=None, secondary_number=secondary_raw,
        city=None, state=None, zipcode=None, zip_plus_4=None, confidence=1.0,
    )
    std = standardize_address(parsed)
    return (
        std.primary_number, std.street_predirection, std.street_name,
        std.street_suffix, std.street_postdirection, std.secondary_designator,
        std.secondary_number, format_delivery_line(std),
    )


ref = (
    spark.table(f"{CATALOG}.{SCHEMA}.openaddresses_us")
    .withColumn("_s", _split_street_udf("primary_number", "street_raw", "secondary_raw"))
    .select(
        "latitude", "longitude",
        F.col("_s.primary_number").alias("primary_number"),
        F.col("_s.street_predirection").alias("street_predirection"),
        F.col("_s.street_name").alias("street_name"),
        F.col("_s.street_suffix").alias("street_suffix"),
        F.col("_s.street_postdirection").alias("street_postdirection"),
        F.col("_s.secondary_designator").alias("secondary_designator"),
        F.col("_s.secondary_number").alias("secondary_number"),
        F.upper(F.col("city")).alias("city"),
        F.upper(F.col("state")).alias("state"),
        F.substring(F.regexp_replace("zipcode", r"\D", ""), 1, 5).alias("zipcode"),
        F.col("_s.normalized_line").alias("delivery_line"),
        F.concat_ws(" ",
            F.col("_s.normalized_line"),
            F.upper(F.col("city")),
            F.upper(F.col("state")),
            F.col("zipcode"),
        ).alias("search_text"),
        F.col("oa_hash"),
    )
)

(
    ref.write.mode("overwrite")
    .clusterBy("state", "zipcode")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.address_reference")
)

spark.sql(f"""
    ALTER TABLE {CATALOG}.{SCHEMA}.address_reference
    SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# COMMAND ----------

# MAGIC %md ## Census ACS demographics (ZCTA-level) — enrichment join table
# MAGIC
# MAGIC Pull ACS 5-year estimates by ZCTA from https://api.census.gov/data. This is
# MAGIC our CAMEO-equivalent: median income, household size, population density, etc.

# COMMAND ----------

import urllib.request, json

ACS_VARS = ["B01003_001E", "B19013_001E", "B25010_001E", "B02001_002E"]
year = 2022
url = (
    f"https://api.census.gov/data/{year}/acs/acs5?"
    f"get=NAME,{','.join(ACS_VARS)}&for=zip%20code%20tabulation%20area:*"
)
with urllib.request.urlopen(url, timeout=60) as r:
    rows = json.load(r)

header, *data = rows
acs_df = spark.createDataFrame(data, schema=header).select(
    F.col("zip code tabulation area").alias("zcta"),
    F.col("B01003_001E").cast("long").alias("population"),
    F.col("B19013_001E").cast("long").alias("median_household_income"),
    F.col("B25010_001E").cast("double").alias("avg_household_size"),
    F.col("B02001_002E").cast("long").alias("white_alone"),
)

(
    acs_df.write.mode("overwrite")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.acs_demographics")
)

# COMMAND ----------

# MAGIC %md ## Sanity check

# COMMAND ----------

display(spark.sql(f"""
    SELECT state, COUNT(*) AS rows
    FROM {CATALOG}.{SCHEMA}.address_reference
    GROUP BY state ORDER BY rows DESC LIMIT 10
"""))
