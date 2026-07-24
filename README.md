# address-verify — Databricks-native replacement for Informatica Address Doctor (US)

A starter kit for alternative to Informatica Address Doctor with a
Databricks + Gen AI pipeline. US scope, no USPS CASS certification, batch + real-time.

## Layout

```
src/address_verify/      # Pure-Python core (unit-tested locally)
  schemas.py             # AddressSchema + JSON Schema for ai_query responseFormat
  standardize.py         # USPS Pub 28 abbreviations, suffix/unit/directional tables
  scoring.py             # AVStatus (V4/V3/V2/C4/C3/I4/I2) + match_score
  pipeline.py            # Orchestration — parse/std/match/judge/geocode
  eval/                  # Golden dataset + eval harness
tests/                   # pytest — runs offline, no Databricks required
notebooks/               # Databricks notebooks (Jobs/DLT/Model Serving)
  00_download_openaddresses.py
  01_load_reference_data.py
  02_build_vector_index.py
  03_parse_standardize_validate.py
  04_serve_realtime_endpoint.py
```

## Local dev loop

```bash
python3 -m venv .venv
.venv/bin/pip install -e ".[test]"
.venv/bin/pytest -q                # 22 tests, ~0.2s
```

## Deploy to a Databricks workspace

Order matters — each notebook depends on the artifacts of the previous one.

1. **Push this repo** to a Databricks Git folder (Workspace → Repos → Add Repo).
2. **Create a Unity Catalog + volume** for reference data:
   ```sql
   CREATE CATALOG address_reference;
   CREATE SCHEMA address_reference.us;
   CREATE VOLUME address_reference.us.openaddresses;
   ```
3. **Fetch OpenAddresses data into the volume.** Store your
   [batch.openaddresses.io](https://batch.openaddresses.io/docs) API token in a secret,
   then run `notebooks/00_download_openaddresses.py`:
   ```bash
   databricks secrets create-scope openaddresses
   databricks secrets put-secret openaddresses api_token   # paste your token
   ```
   Set the `collections` widget (default `us-northeast`; also `us-south`, `us-west`,
   `us-midwest`) and point `secret_scope`/`secret_key` at that secret. The notebook
   downloads each collection zip and extracts only `*-addresses-*.geojson` (statewide +
   county + city) straight into `/Volumes/address_reference/us/openaddresses/<state>/`
   — no laptop round-trip. _Fallback:_ you can instead manually download the GeoJSON
   extracts and upload them, preserving that same per-state layout. The loader reads the
   newline-delimited GeoJSON directly and de-duplicates on the OpenAddresses `hash`.
4. **Run `notebooks/01_load_reference_data.py`** as a job. Lands ~200M rows into
   `address_reference.us.address_reference` + ACS demographics.
5. **Run `notebooks/02_build_vector_index.py`**. Creates the Vector Search endpoint
   and the Delta Sync index over `search_text`.
6. **Deploy `notebooks/03_parse_standardize_validate.py`** as a DLT pipeline. Point
   its `bundle.*` configs at your catalogs. Output: `gold_addresses_verified`.
7. **Run `notebooks/04_serve_realtime_endpoint.py`** to register the pyfunc model
   and deploy a Model Serving endpoint for real-time intake forms.

## Verify parity with Address Doctor

```python
# From a Databricks notebook pointed at the deployed endpoint:
from address_verify.eval.run_eval import run_eval
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

def verify(raw: str):
    resp = w.serving_endpoints.query(
        name="address_verify_realtime",
        dataframe_records=[{"raw_input": raw}],
    )
    # wrap back into VerifiedAddress for the harness...

summary = run_eval(verify, mlflow_experiment="/Users/you@co.com/address_verify_eval")
```

**Targets for sign-off with customer:**

| Metric | Target |
|---|---|
| Parse accuracy (field-level, clean inputs) | ≥ 95% |
| V4 on 1k clean canonical addresses | ≥ 98% |
| Correction (typo/abbr/wrong ZIP) → C3/C4 with right output | ≥ 85% |
| Geocode median error vs. Census Geocoder | < 20 m |
| Real-time p95 latency, cache hit | < 400 ms |
| Real-time p95 latency, cold | < 1.5 s |
| Address Doctor vs. ours, agreement rate on customer sample | ≥ 95% |

## What's deliberately NOT in this repo

- **USPS CASS certification.** If the customer needs presorted-mail postage
  discounts, pair this pipeline with a certified vendor (Melissa, Smarty, Precisely)
  only for the final CASS stamp. Everything else stays in Databricks.
- **Non-US addresses.** Architecture extends globally by adding country-specific
  reference data and standardization rules.
- **Secret management.** The Databricks SDK uses the notebook's workspace identity;
  adapt for service principal / OBO auth when deploying.

## Key design decisions

| Decision | Why |
|---|---|
| LLM does parse + correction; exact-match + Vector Search do validation | LLMs hallucinate ZIP codes; reference data is authoritative |
| Structured-output `responseFormat` on every LLM call | Eliminates JSON-parsing flakiness that kills production pipelines |
| Pub 28 standardization in pure Python, not LLM | Cheap, deterministic, auditable, testable |
| Lakebase cache by `sha256(raw_input)` | ~10× cost reduction when same addresses flow through repeatedly (very common) |
| MLflow eval harness + golden dataset committed to repo | Lets the SA diff runs across model versions and show the customer the parity story |
