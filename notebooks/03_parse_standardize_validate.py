# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Batch verification pipeline (parse → standardize → validate → geocode)
# MAGIC
# MAGIC DLT / Lakeflow pipeline. For each raw input we:
# MAGIC 1. Parse with `ai_query` + strict JSON response format.
# MAGIC 2. Standardize to USPS Pub 28 abbreviations.
# MAGIC 3. Exact hash-join to the reference table, OR fall back to Vector Search top-5.
# MAGIC 4. LLM-judge re-rank to pick the best candidate and compute correction flag.
# MAGIC 5. Join geocode + ACS demographics.
# MAGIC 6. Emit `av_status` + `match_score`.

# COMMAND ----------

import json
import dlt  # type: ignore[import-not-found]
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, BooleanType

from address_verify.schemas import ADDRESS_JSON_SCHEMA, PARSE_PROMPT, AddressSchema
from address_verify.standardize import (
    DIRECTIONAL, SECONDARY_DESIGNATOR, STREET_SUFFIX,
    format_delivery_line, format_last_line, standardize_address,
)
from address_verify.scoring import AVStatus, MatchInputs, compute_av_status, compute_match_score

CATALOG = spark.conf.get("bundle.catalog", "address_verify")
REF_CATALOG = spark.conf.get("bundle.ref_catalog", "address_reference")
REF_SCHEMA = spark.conf.get("bundle.ref_schema", "us")
VS_ENDPOINT = spark.conf.get("bundle.vs_endpoint", "address_verify_vs")
VS_INDEX = spark.conf.get("bundle.vs_index", f"{REF_CATALOG}.{REF_SCHEMA}.address_reference_vs_idx")

PARSE_MODEL = "databricks-claude-sonnet-4-6"
JUDGE_MODEL = "databricks-claude-sonnet-4-6"

# COMMAND ----------

@dlt.table(name="bronze_addresses_raw", comment="Raw free-text addresses landed from source systems.")
def bronze_addresses_raw():
    return (
        spark.readStream.table(f"{CATALOG}.ingest.addresses_landing")
        .withColumn("raw_input_hash", F.sha2("raw_input", 256))
    )

# COMMAND ----------

# MAGIC %md ## Parse with ai_query (structured JSON output)

SCHEMA_JSON = json.dumps(ADDRESS_JSON_SCHEMA)

@dlt.table(name="silver_addresses_parsed")
def silver_addresses_parsed():
    df = dlt.read_stream("bronze_addresses_raw")
    return df.withColumn(
        "_parsed_json",
        F.expr(f"""
            ai_query(
                '{PARSE_MODEL}',
                concat(
                    '{PARSE_PROMPT.replace("'", "''")}\\n\\nInput: ',
                    raw_input
                ),
                responseFormat => '{SCHEMA_JSON.replace("'", "''")}'
            )
        """),
    ).select(
        "raw_input", "raw_input_hash",
        F.from_json("_parsed_json", schema=_parsed_spark_schema()).alias("parsed"),
    )


def _parsed_spark_schema() -> StructType:
    return StructType([
        StructField("primary_number", StringType(), True),
        StructField("street_predirection", StringType(), True),
        StructField("street_name", StringType(), True),
        StructField("street_suffix", StringType(), True),
        StructField("street_postdirection", StringType(), True),
        StructField("secondary_designator", StringType(), True),
        StructField("secondary_number", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("zipcode", StringType(), True),
        StructField("zip_plus_4", StringType(), True),
        StructField("confidence", DoubleType(), True),
    ])

# COMMAND ----------

# MAGIC %md ## Standardize using the USPS Pub 28 tables from the shared package

_suffix_map_bc = spark.sparkContext.broadcast(STREET_SUFFIX)
_unit_map_bc = spark.sparkContext.broadcast(SECONDARY_DESIGNATOR)
_dir_map_bc = spark.sparkContext.broadcast(DIRECTIONAL)


@F.udf(returnType=_parsed_spark_schema())
def _standardize_udf(p):
    if p is None:
        return None
    addr = AddressSchema(**p.asDict())
    s = standardize_address(addr)
    return s.model_dump()


@dlt.table(name="silver_addresses_standardized")
def silver_addresses_standardized():
    return (
        dlt.read_stream("silver_addresses_parsed")
        .withColumn("std", _standardize_udf("parsed"))
    )

# COMMAND ----------

# MAGIC %md ## Exact hash-match against reference

@dlt.table(name="silver_addresses_exact_matched")
def silver_addresses_exact_matched():
    parsed = dlt.read_stream("silver_addresses_standardized")
    ref = spark.read.table(f"{REF_CATALOG}.{REF_SCHEMA}.address_reference")

    join_keys = ["primary_number", "street_name", "street_suffix", "city", "state", "zipcode"]
    return (
        parsed.alias("p")
        .join(
            ref.alias("r"),
            on=[F.col(f"p.std.{k}") == F.col(f"r.{k}") for k in join_keys],
            how="left",
        )
        .withColumn("exact_hit", F.col("r.oa_hash").isNotNull())
    )

# COMMAND ----------

# MAGIC %md ## Vector Search fallback for non-exact matches
# MAGIC
# MAGIC For rows where `exact_hit = false` we call the index; top-5 candidates are
# MAGIC passed to the LLM judge. Uses pandas UDF so each batch queries once.

from pyspark.sql.functions import pandas_udf
import pandas as pd
from databricks.vector_search.client import VectorSearchClient

_CANDIDATES_SCHEMA = "array<struct<delivery_line:string,city:string,state:string,zipcode:string,latitude:double,longitude:double,score:double>>"


@pandas_udf(_CANDIDATES_SCHEMA)
def _vector_candidates(search_text: pd.Series) -> pd.Series:
    vsc = VectorSearchClient(disable_notice=True)
    idx = vsc.get_index(VS_ENDPOINT, VS_INDEX)
    out = []
    for q in search_text:
        if not q:
            out.append([])
            continue
        res = idx.similarity_search(
            query_text=q,
            columns=["delivery_line", "city", "state", "zipcode", "latitude", "longitude"],
            num_results=5,
        )
        rows = res.get("result", {}).get("data_array", [])
        out.append([
            {
                "delivery_line": r[0], "city": r[1], "state": r[2], "zipcode": r[3],
                "latitude": float(r[4]) if r[4] is not None else None,
                "longitude": float(r[5]) if r[5] is not None else None,
                "score": float(r[-1]),
            }
            for r in rows
        ])
    return pd.Series(out)


@dlt.table(name="silver_addresses_candidates")
def silver_addresses_candidates():
    df = dlt.read_stream("silver_addresses_exact_matched")
    query = F.concat_ws(" ",
        F.col("std.primary_number"), F.col("std.street_predirection"),
        F.col("std.street_name"), F.col("std.street_suffix"),
        F.col("std.street_postdirection"), F.col("std.city"),
        F.col("std.state"), F.col("std.zipcode"),
    )
    return df.withColumn(
        "candidates",
        F.when(F.col("exact_hit"), F.lit(None)).otherwise(_vector_candidates(query)),
    )

# COMMAND ----------

# MAGIC %md ## LLM judge: pick the correct candidate, emit confidence

JUDGE_PROMPT = """Given a standardized input address and a list of candidate matches from
our US address reference database, pick the single best match or reply with index -1 if
none is correct. Also return your confidence from 0 to 1. Respond in JSON:
{"index": <int>, "confidence": <float>}."""

JUDGE_SCHEMA_JSON = json.dumps({
    "type": "json_schema",
    "json_schema": {
        "name": "judgment",
        "schema": {
            "type": "object",
            "properties": {
                "index": {"type": "integer"},
                "confidence": {"type": "number", "minimum": 0, "maximum": 1},
            },
            "required": ["index", "confidence"],
            "additionalProperties": False,
        },
        "strict": True,
    },
})


@dlt.table(name="silver_addresses_judged")
def silver_addresses_judged():
    df = dlt.read_stream("silver_addresses_candidates")
    prompt = F.concat(
        F.lit(JUDGE_PROMPT),
        F.lit("\n\nInput: "),
        F.to_json("std"),
        F.lit("\n\nCandidates: "),
        F.to_json("candidates"),
    )
    return df.withColumn(
        "_judgment_json",
        F.when(
            F.col("exact_hit"),
            F.to_json(F.struct(F.lit(0).alias("index"), F.lit(1.0).alias("confidence"))),
        ).otherwise(F.expr(f"""
            ai_query('{JUDGE_MODEL}', {_col_to_sql('prompt')},
                responseFormat => '{JUDGE_SCHEMA_JSON.replace("'", "''")}')
        """)),
    ).withColumn(
        "judgment",
        F.from_json(
            "_judgment_json",
            schema=StructType([
                StructField("index", IntegerType(), True),
                StructField("confidence", DoubleType(), True),
            ]),
        ),
    )


def _col_to_sql(name: str) -> str:
    return f"`{name}`"

# COMMAND ----------

# MAGIC %md ## Final gold table — score, av_status, geocode, enrichment

@dlt.table(name="gold_addresses_verified")
def gold_addresses_verified():
    df = dlt.read_stream("silver_addresses_judged")
    acs = spark.read.table(f"{REF_CATALOG}.{REF_SCHEMA}.acs_demographics")

    picked = F.when(
        F.col("exact_hit"),
        F.struct(
            F.col("r.delivery_line"), F.col("r.city"), F.col("r.state"),
            F.col("r.zipcode"), F.col("r.latitude"), F.col("r.longitude"),
            F.lit(0.0).alias("score"),
        ),
    ).otherwise(
        F.element_at("candidates", F.col("judgment.index") + 1)
    )

    out = (
        df.withColumn("chosen", picked)
          .withColumn("latitude", F.col("chosen.latitude"))
          .withColumn("longitude", F.col("chosen.longitude"))
          .withColumn("delivery_line", F.col("chosen.delivery_line"))
          .withColumn("last_line", F.concat_ws(" ",
              F.concat_ws(", ", F.col("chosen.city"), F.col("chosen.state")),
              F.col("chosen.zipcode"),
          ))
    )

    return (
        out.join(acs, out.zipcode == acs.zcta, how="left")
           .withColumn("av_status", _status_udf(
               F.col("exact_hit"), F.col("judgment.confidence"), F.col("chosen.score"),
               F.col("std.primary_number"), F.col("std.street_name"),
               F.col("std.city"), F.col("std.state"), F.col("std.zipcode"),
               F.size("candidates"),
           ))
    )


@F.udf(returnType=StringType())
def _status_udf(exact, conf, dist, pn, street, city, state, zipc, cand_count):
    m = MatchInputs(
        exact_ref_hit=bool(exact),
        vector_distance=float(dist or 2.0),
        llm_judge_confidence=float(conf or 0.0),
        was_corrected=False,
        has_primary_number=bool(pn),
        has_street=bool(street),
        has_city=bool(city),
        has_state=bool(state),
        has_zip=bool(zipc),
        candidate_count=int(cand_count or 0),
    )
    return compute_av_status(m).value
