# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Download OpenAddresses collections into the Unity Catalog volume
# MAGIC
# MAGIC Pulls OpenAddresses data straight from the batch API into the volume that
# MAGIC `01_load_reference_data.py` reads — no laptop download/upload round-trip.
# MAGIC Run this **before** notebook 01.
# MAGIC
# MAGIC The batch API serves data as whole-collection zips. There is no single "US"
# MAGIC collection; the US is split into four regions:
# MAGIC
# MAGIC | id | collection   | approx zip size |
# MAGIC |----|--------------|-----------------|
# MAGIC | 2  | us-northeast | ~2.2 GB         |
# MAGIC | 3  | us-south     | ~13 GB          |
# MAGIC | 4  | us-west      | ~10 GB          |
# MAGIC | 5  | us-midwest   | ~5.4 GB         |
# MAGIC
# MAGIC Each zip contains every layer (addresses, parcels, buildings, centerlines).
# MAGIC We extract **only `*-addresses-*.geojson`** — the layers notebook 01 reads —
# MAGIC which drops ~80–90% of the bytes. Files land at
# MAGIC `{openaddresses_volume}/<state>/<file>-addresses-<scope>.geojson`, exactly where
# MAGIC notebook 01 globs (`{OA_VOL}/*/*-addresses-*.geojson`).
# MAGIC
# MAGIC **Prerequisite** — store your OpenAddresses API token in a Databricks secret:
# MAGIC ```
# MAGIC databricks secrets create-scope openaddresses
# MAGIC databricks secrets put-secret openaddresses api_token
# MAGIC ```
# MAGIC API reference: https://batch.openaddresses.io/docs

# COMMAND ----------

dbutils.widgets.text("openaddresses_volume", "/Volumes/address_reference/us/openaddresses")
dbutils.widgets.text("collections", "us-northeast")
dbutils.widgets.text("secret_scope", "openaddresses")
dbutils.widgets.text("secret_key", "api_token")
dbutils.widgets.text("staging_dir", "/tmp/oa_zips")
dbutils.widgets.dropdown("keep_zip", "false", ["true", "false"])

OA_VOL = dbutils.widgets.get("openaddresses_volume")
COLLECTIONS = [c.strip() for c in dbutils.widgets.get("collections").split(",") if c.strip()]
SECRET_SCOPE = dbutils.widgets.get("secret_scope")
SECRET_KEY = dbutils.widgets.get("secret_key")
STAGING_DIR = dbutils.widgets.get("staging_dir")
KEEP_ZIP = dbutils.widgets.get("keep_zip") == "true"

API_BASE = "https://batch.openaddresses.io/api"

# Token comes from a secret so it never appears in code or notebook state. The value
# is redacted in any downstream print/log output by the Databricks secret guard.
TOKEN = dbutils.secrets.get(SECRET_SCOPE, SECRET_KEY)
AUTH = {"Authorization": f"Bearer {TOKEN}"}

print(f"Collections requested: {COLLECTIONS}")
print(f"Volume target:         {OA_VOL}")
print(f"Staging dir:           {STAGING_DIR}")

# COMMAND ----------

# MAGIC %pip install -q "requests>=2.31"

# COMMAND ----------

# MAGIC %md ## Resolve collection names → ids
# MAGIC
# MAGIC `GET /api/collections` lists every collection. We map the requested names to
# MAGIC their numeric ids and fail early (listing valid names) if one is unknown.

# COMMAND ----------

import requests

resp = requests.get(f"{API_BASE}/collections", headers=AUTH, timeout=60)
resp.raise_for_status()
catalog = {c["name"]: c for c in resp.json()}

print("Available collections:")
for name, c in sorted(catalog.items()):
    gb = c.get("size", 0) / 1e9
    print(f"  {c['id']:>3}  {name:<14} ~{gb:5.1f} GB")

unknown = [name for name in COLLECTIONS if name not in catalog]
if unknown:
    raise ValueError(
        f"Unknown collection(s): {unknown}. Valid names: {sorted(catalog)}"
    )

targets = [(name, catalog[name]["id"]) for name in COLLECTIONS]
print(f"\nWill download: {targets}")

# COMMAND ----------

# MAGIC %md ## Download + selective extract
# MAGIC
# MAGIC For each collection we stream the zip to fast driver-local disk, then extract
# MAGIC only the `*-addresses-*.geojson` members to `{OA_VOL}/<state>/`. Streaming (never
# MAGIC loading the whole zip into memory) keeps multi-GB regions safe. `requests` follows
# MAGIC the API's 302 to presigned S3 and strips the `Authorization` header cross-host, so
# MAGIC the token is not leaked to S3.

# COMMAND ----------

import os
import fnmatch
import shutil
import zipfile

CHUNK = 1024 * 1024  # 1 MB


def _download_zip(collection_id: int, dest_path: str) -> None:
    """Stream a collection archive to local disk, following the S3 redirect."""
    url = f"{API_BASE}/collections/{collection_id}/data"
    with requests.get(url, headers=AUTH, stream=True, allow_redirects=True, timeout=120) as r:
        r.raise_for_status()
        total = int(r.headers.get("Content-Length", 0))
        done = 0
        next_log = 100 * CHUNK  # log every ~100 MB
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(CHUNK):
                if not chunk:
                    continue
                f.write(chunk)
                done += len(chunk)
                if done >= next_log:
                    pct = f" ({done / total:.0%})" if total else ""
                    print(f"    {done / 1e6:,.0f} MB{pct}")
                    next_log += 100 * CHUNK
    print(f"    downloaded {done / 1e6:,.0f} MB -> {dest_path}")


def _extract_addresses(zip_path: str, out_root: str) -> tuple[int, int]:
    """Extract only *-addresses-*.geojson members into out_root/<state>/.

    Returns (files_written, bytes_written). The state dir is the member's immediate
    parent directory, which handles both `us/<state>/file` and `<state>/file` layouts.
    """
    files = 0
    written = 0
    with zipfile.ZipFile(zip_path) as zf:
        for info in zf.infolist():
            if info.is_dir():
                continue
            base = os.path.basename(info.filename)
            if not fnmatch.fnmatch(base, "*-addresses-*.geojson"):
                continue  # skip parcels/buildings/centerlines/.meta
            state = os.path.basename(os.path.dirname(info.filename)) or "unknown"
            out_dir = os.path.join(out_root, state)
            os.makedirs(out_dir, exist_ok=True)
            out_path = os.path.join(out_dir, base)
            with zf.open(info) as src, open(out_path, "wb") as dst:
                shutil.copyfileobj(src, dst, CHUNK)  # overwrite -> idempotent re-runs
            files += 1
            written += info.file_size
    return files, written


os.makedirs(STAGING_DIR, exist_ok=True)
os.makedirs(OA_VOL, exist_ok=True)

for name, cid in targets:
    print(f"\n=== {name} (id {cid}) ===")
    zip_path = os.path.join(STAGING_DIR, f"{name}.zip")
    _download_zip(cid, zip_path)
    files, written = _extract_addresses(zip_path, OA_VOL)
    print(f"    extracted {files} address files ({written / 1e6:,.0f} MB) into {OA_VOL}")
    if not KEEP_ZIP:
        os.remove(zip_path)
        print(f"    removed staged zip {zip_path}")

# COMMAND ----------

# MAGIC %md ## Sanity check — what landed in the volume
# MAGIC
# MAGIC Counts `*-addresses-*.geojson` files and total bytes per state directory, so you
# MAGIC can confirm coverage before running `01_load_reference_data.py`.

# COMMAND ----------

rows = []
for state in sorted(os.listdir(OA_VOL)):
    state_dir = os.path.join(OA_VOL, state)
    if not os.path.isdir(state_dir):
        continue
    geojsons = [f for f in os.listdir(state_dir) if fnmatch.fnmatch(f, "*-addresses-*.geojson")]
    if not geojsons:
        continue
    mb = sum(os.path.getsize(os.path.join(state_dir, f)) for f in geojsons) / 1e6
    rows.append((state, len(geojsons), round(mb, 1)))

summary = spark.createDataFrame(rows, ["state", "address_files", "size_mb"])
print(f"{len(rows)} state dirs populated, "
      f"{sum(r[1] for r in rows)} address files, "
      f"{sum(r[2] for r in rows):,.0f} MB total")
display(summary.orderBy("state"))
