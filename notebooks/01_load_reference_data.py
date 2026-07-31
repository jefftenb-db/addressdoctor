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
# MAGIC We use a notebook-scoped `%pip install` (not a `sys.path` append) because the
# MAGIC standardizer runs inside a Spark UDF on the executors, and `%pip` distributes the
# MAGIC package to them; a driver-only path change would leave the UDF failing.
# MAGIC
# MAGIC Note: we install **non-editable** (no `-e`). This project uses a `src/` layout, and
# MAGIC an editable install's finder does not reliably resolve `address_verify` in the
# MAGIC Databricks kernel (`find_spec` returns `None` even though `pip show` succeeds). A
# MAGIC plain install builds the package and copies it into site-packages, so it imports
# MAGIC normally. The `%pip` magic can't read a Python variable, so we invoke it via
# MAGIC `run_line_magic` with the computed path. This cell triggers a Python restart.

# COMMAND ----------

# Derive the repo root (the folder containing pyproject.toml) from this notebook's path:
# /Workspace/<...>/addressdoctor/notebooks/01_... -> /Workspace/<...>/addressdoctor
_nb_path = (
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)
repo_root = "/Workspace" + "/".join(_nb_path.split("/")[:-2])
print(f"Installing address_verify from: {repo_root}")

get_ipython().run_line_magic("pip", f"install -q {repo_root}")

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

# State is taken from the <state> directory in the file path, which is authoritative.
# OpenAddresses' properties.region is populated inconsistently across source files
# (frequently ""), which previously left ~1/3 of rows with a blank state. Files land
# under {OA_VOL}/<state>/..., so the parent directory of each file is its state.
_state_from_path = F.upper(
    F.regexp_extract(F.col("_metadata.file_path"), r"/([^/]+)/[^/]+\.geojson$", 1)
)

oa_raw = (
    spark.read.json(f"{OA_VOL}/*/*-addresses-*.geojson")
    .select(
        F.col("geometry.coordinates")[0].cast("double").alias("longitude"),  # [lon, lat]
        F.col("geometry.coordinates")[1].cast("double").alias("latitude"),
        F.col("properties.number").alias("primary_number"),
        F.col("properties.street").alias("street_raw"),
        F.col("properties.unit").alias("secondary_raw"),
        F.col("properties.city").alias("city"),
        F.coalesce(
            F.when(F.trim(_state_from_path) != "", _state_from_path),
            F.when(F.trim(F.upper(F.col("properties.region"))) != "",
                   F.upper(F.col("properties.region"))),
        ).alias("state"),
        F.col("properties.postcode").alias("zipcode"),
        F.col("properties.hash").alias("oa_hash"),
    )
    # Require a non-empty state (isNotNull alone leaked empty strings, so trim != "").
    # We intentionally do NOT require zipcode: some sources (notably all of New
    # Hampshire) have no postcode upstream. Those rows keep a null/empty zip here and
    # get a spatially-derived ZIP backfilled from Census ZCTA polygons further below.
    .filter(F.col("state").isNotNull() & (F.trim("state") != ""))
    .dropDuplicates(["oa_hash"])  # dedup statewide vs county/city overlap
)

(
    oa_raw.write.mode("overwrite")
    .option("delta.columnMapping.mode", "name")
    .clusterBy("state", "zipcode")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.openaddresses_us")
)

# COMMAND ----------

# MAGIC %md ## Load Census ZCTA boundaries (for ZIP backfill)
# MAGIC
# MAGIC Some OpenAddresses sources have no `postcode` upstream — most notably **all of
# MAGIC New Hampshire**, whose source GIS layers don't map a ZIP. Every row still has
# MAGIC rooftop lat/lon, so we assign those points to a **Census ZCTA** polygon and use
# MAGIC the ZCTA5 code as an (estimated) ZIP.
# MAGIC
# MAGIC ZCTAs are decennial, so the boundary file lives under Census GENZ2020:
# MAGIC `cb_2020_us_zcta520_500k.zip` (~33K polygons — small enough to read on the driver
# MAGIC with geopandas). We store each polygon's H3 cover cells so the point→polygon join
# MAGIC is an H3 equijoin (fast) rather than an N×M spatial cross join.

# COMMAND ----------

# MAGIC %pip install -q geopandas

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Re-establish config after the restart (widgets persist; Python state does not).
from pyspark.sql import functions as F

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# H3 resolution for the ZCTA cover / point lookup. Res 7 (~5 km² cells) keeps the
# per-polygon cover-cell count modest while still pruning the join hard.
H3_RES = 7

# COMMAND ----------

import os
import urllib.request
import zipfile

import geopandas as gpd

_ZCTA_URL = "https://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_zcta520_500k.zip"
_stage = "/local_disk0/zcta"
os.makedirs(_stage, exist_ok=True)
_zip_path = os.path.join(_stage, "zcta.zip")

urllib.request.urlretrieve(_ZCTA_URL, _zip_path)
with zipfile.ZipFile(_zip_path) as zf:
    zf.extractall(_stage)

gdf = gpd.read_file(_stage)  # reads the .shp in the dir
gdf = gdf.to_crs(4326)  # ensure WGS84 lon/lat
# ZCTA5CE20 is the 5-digit ZCTA code column in the 2020 cartographic file.
zcta_pdf = gdf[["ZCTA5CE20", "geometry"]].rename(columns={"ZCTA5CE20": "zcta"})
zcta_pdf["geojson"] = zcta_pdf["geometry"].apply(lambda g: g.__geo_interface__).apply(
    __import__("json").dumps
)
zcta_pdf = zcta_pdf[["zcta", "geojson"]]
print(f"ZCTA polygons: {len(zcta_pdf):,}")

# COMMAND ----------

# Land the polygons as Delta with a WKB geometry column, then explode each polygon's
# H3 cover cells into (zcta, h3_cell) for the equijoin pre-filter.
zcta_sdf = (
    spark.createDataFrame(zcta_pdf)
    .withColumn("geom", F.expr("ST_SetSRID(ST_GeomFromGeoJSON(geojson), 4326)"))
    .withColumn("geom_wkb", F.expr("ST_AsBinary(geom)"))
    .select("zcta", "geom_wkb")
)
zcta_sdf.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.zcta_boundaries")

zcta_h3 = (
    spark.table(f"{CATALOG}.{SCHEMA}.zcta_boundaries")
    .withColumn("h3_cell", F.explode(F.expr(f"h3_coverash3(geom_wkb, {H3_RES})")))
    .select("zcta", "h3_cell", "geom_wkb")
)
zcta_h3.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.zcta_h3")

# COMMAND ----------

# MAGIC %md ## Backfill missing ZIPs from ZCTA polygons
# MAGIC
# MAGIC For rows whose zip is null/empty we compute the point's H3 cell, equijoin to the
# MAGIC ZCTA cover cells (fast prune), then confirm with `ST_Contains` for the exact
# MAGIC polygon (a point's cell can overlap more than one ZCTA at boundaries). Matched
# MAGIC rows get `zipcode = zcta` and `zip_is_estimated = true`. Rows that already have a
# MAGIC zip are untouched (`zip_is_estimated = false`).

# COMMAND ----------

_oa = spark.table(f"{CATALOG}.{SCHEMA}.openaddresses_us")
_oa_cols = _oa.columns  # original column list, before the join adds zcta/h3_cell/geom_wkb
_has_zip = _oa.filter(F.col("zipcode").isNotNull() & (F.trim("zipcode") != ""))
_no_zip = _oa.filter(F.col("zipcode").isNull() | (F.trim("zipcode") == ""))

_pt_cell = F.expr(f"h3_longlatash3(longitude, latitude, {H3_RES})")
# WKB carries no SRID, so re-tag it 4326 to match the point (else ST_Contains errors
# with ST_DIFFERENT_SRID_VALUES).
_contains = F.expr(
    "ST_Contains(ST_SetSRID(ST_GeomFromWKB(geom_wkb), 4326), "
    "ST_SetSRID(ST_Point(longitude, latitude), 4326))"
)

_zcta_h3 = spark.table(f"{CATALOG}.{SCHEMA}.zcta_h3")
_backfilled = (
    _no_zip.withColumn("_h3", _pt_cell)
    .join(_zcta_h3, F.col("_h3") == F.col("h3_cell"), "left")
    .filter(F.col("zcta").isNull() | _contains)  # keep exact containment (or no match)
    # A point may hit multiple candidate cells; keep one containing ZCTA per row.
    .dropDuplicates(["oa_hash"])
    .withColumn("zip_is_estimated", F.col("zcta").isNotNull())
    .withColumn("zipcode", F.coalesce(F.col("zcta"), F.col("zipcode")))
    .select(*_oa_cols, "zip_is_estimated")
)

_openaddresses_final = (
    _has_zip.withColumn("zip_is_estimated", F.lit(False))
    .select(*_oa_cols, "zip_is_estimated")
    .unionByName(_backfilled)
)

# Write to a distinct table rather than overwriting openaddresses_us in place — reading
# and overwriting the same table in one operation is unsafe in Spark. Downstream reads
# this geocoded table.
_openaddresses_final.write.mode("overwrite").option(
    "delta.columnMapping.mode", "name"
).clusterBy("state", "zipcode").saveAsTable(f"{CATALOG}.{SCHEMA}.openaddresses_us_geocoded")

# COMMAND ----------

# MAGIC %md ## Normalize OpenAddresses through our Pub 28 standardizer
# MAGIC
# MAGIC This produces the reference table we'll build the Vector Search index over.
# MAGIC Every reference row gets split into the same AddressSchema fields and
# MAGIC standardized identically to runtime inputs, so exact-match joins are possible.

# COMMAND ----------

from pyspark.sql import functions as F
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
    spark.table(f"{CATALOG}.{SCHEMA}.openaddresses_us_geocoded")
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
        F.col("zip_is_estimated"),  # true for spatially-derived (ZCTA) ZIPs, e.g. NH
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

from pyspark.sql import functions as F

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
