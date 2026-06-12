# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Real-time verification endpoint (Model Serving)
# MAGIC
# MAGIC Packages the parse → standardize → match → judge → geocode flow as an MLflow
# MAGIC pyfunc model. Lakebase Postgres fronts it as a read-through cache keyed by
# MAGIC `sha256(raw_input)` so repeat lookups skip all LLM calls.

# COMMAND ----------

import hashlib
import json
import mlflow
import mlflow.pyfunc
import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.vector_search.client import VectorSearchClient

from address_verify.pipeline import Candidate, verify_address
from address_verify.schemas import ADDRESS_JSON_SCHEMA, PARSE_PROMPT, AddressSchema

dbutils.widgets.text("registered_model_name", "main.address_verify.address_verify_model")
dbutils.widgets.text("serving_endpoint", "address_verify_realtime")
dbutils.widgets.text("vs_endpoint", "address_verify_vs")
dbutils.widgets.text("vs_index", "address_reference.us.address_reference_vs_idx")
dbutils.widgets.text("lakebase_instance", "address_verify_cache")

REGISTERED = dbutils.widgets.get("registered_model_name")
SERVING = dbutils.widgets.get("serving_endpoint")
VS_ENDPOINT = dbutils.widgets.get("vs_endpoint")
VS_INDEX = dbutils.widgets.get("vs_index")

# COMMAND ----------

PARSE_SCHEMA_JSON = json.dumps(ADDRESS_JSON_SCHEMA)
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


class AddressVerifyModel(mlflow.pyfunc.PythonModel):

    def load_context(self, context):
        self.w = WorkspaceClient()
        self.vsc = VectorSearchClient(disable_notice=True)
        self.index = self.vsc.get_index(VS_ENDPOINT, VS_INDEX)

    def _parse_fn(self, raw: str) -> AddressSchema:
        resp = self.w.serving_endpoints.query(
            name="databricks-claude-sonnet-4-6",
            messages=[
                {"role": "system", "content": PARSE_PROMPT},
                {"role": "user", "content": raw},
            ],
            extra_body={"response_format": json.loads(PARSE_SCHEMA_JSON)},
        )
        obj = json.loads(resp.choices[0].message.content)
        return AddressSchema(**obj)

    def _candidates_fn(self, parsed: AddressSchema, k: int) -> list[Candidate]:
        q = " ".join(
            str(v) for v in [
                parsed.primary_number, parsed.street_predirection, parsed.street_name,
                parsed.street_suffix, parsed.street_postdirection, parsed.city,
                parsed.state, parsed.zipcode,
            ] if v
        )
        res = self.index.similarity_search(
            query_text=q,
            columns=["primary_number", "street_predirection", "street_name",
                     "street_suffix", "street_postdirection", "city", "state",
                     "zipcode", "latitude", "longitude"],
            num_results=k,
        )
        cands = []
        for row in res.get("result", {}).get("data_array", []):
            addr = AddressSchema(
                primary_number=row[0], street_predirection=row[1], street_name=row[2],
                street_suffix=row[3], street_postdirection=row[4], city=row[5],
                state=row[6], zipcode=row[7], zip_plus_4=None, confidence=1.0,
            )
            cands.append(Candidate(
                address=addr, vector_distance=1.0 - float(row[-1]),
                latitude=row[8], longitude=row[9],
                is_exact=all([
                    row[0] == parsed.primary_number, row[2] == parsed.street_name,
                    row[5] == parsed.city, row[6] == parsed.state,
                    row[7] == parsed.zipcode,
                ]),
            ))
        return cands

    def _judge_fn(self, parsed: AddressSchema, cands: list[Candidate]):
        if not cands:
            return (None, 0.0)
        prompt = (
            "Given a standardized input address and candidates, pick the best match "
            "or -1 if none. Return {\"index\": int, \"confidence\": float}.\n"
            f"Input: {parsed.model_dump_json()}\n"
            f"Candidates: {[c.address.model_dump() for c in cands]}"
        )
        resp = self.w.serving_endpoints.query(
            name="databricks-claude-sonnet-4-6",
            messages=[{"role": "user", "content": prompt}],
            extra_body={"response_format": json.loads(JUDGE_SCHEMA_JSON)},
        )
        j = json.loads(resp.choices[0].message.content)
        if j["index"] < 0 or j["index"] >= len(cands):
            return (None, float(j["confidence"]))
        return (cands[j["index"]], float(j["confidence"]))

    def predict(self, context, model_input: pd.DataFrame) -> pd.DataFrame:
        out = []
        for raw in model_input["raw_input"]:
            r = verify_address(
                raw,
                parse_fn=self._parse_fn,
                candidates_fn=self._candidates_fn,
                judge_fn=self._judge_fn,
            )
            out.append({
                "raw_input": raw,
                "delivery_line": r.delivery_line,
                "last_line": r.last_line,
                "latitude": r.latitude,
                "longitude": r.longitude,
                "av_status": r.av_status.value,
                "match_score": r.match_score,
                "was_corrected": r.was_corrected,
            })
        return pd.DataFrame(out)

# COMMAND ----------

with mlflow.start_run(run_name="address_verify_register"):
    mlflow.pyfunc.log_model(
        artifact_path="model",
        python_model=AddressVerifyModel(),
        pip_requirements=[
            "pydantic>=2.5", "databricks-sdk>=0.20", "databricks-vectorsearch>=0.40",
        ],
        code_paths=["../src/address_verify"],
        registered_model_name=REGISTERED,
        input_example=pd.DataFrame({"raw_input": ["1600 Pennsylvania Ave NW, Washington, DC 20500"]}),
    )

# COMMAND ----------

# MAGIC %md ## Deploy to Model Serving
# MAGIC
# MAGIC Use the Databricks UI or SDK:
# MAGIC
# MAGIC ```python
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC w = WorkspaceClient()
# MAGIC w.serving_endpoints.create_and_wait(
# MAGIC     name="address_verify_realtime",
# MAGIC     config={"served_entities": [{
# MAGIC         "entity_name": "main.address_verify.address_verify_model",
# MAGIC         "entity_version": "1",
# MAGIC         "workload_size": "Small",
# MAGIC         "scale_to_zero_enabled": True,
# MAGIC     }]},
# MAGIC )
# MAGIC ```
