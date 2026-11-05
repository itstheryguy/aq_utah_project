# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup
# MAGIC
# MAGIC Creates the Unity Catalog objects the rest of the project uses:
# MAGIC
# MAGIC | Object | Purpose |
# MAGIC |---|---|
# MAGIC | `<catalog>.aq` schema | holds bronze, silver, and gold tables |
# MAGIC | `<catalog>.aq.landing` volume | raw files from the extractor (`raw/`) and the historical backfill |
# MAGIC | `<catalog>.aq.checkpoints` volume | streaming checkpoint storage |
# MAGIC | `<catalog>.aq.exports` volume | files for use outside the workspace, e.g. the gold snapshot for local model development |
# MAGIC
# MAGIC The `catalog` and `schema` widgets default to `workspace.aq`. When the notebook runs as part of the
# MAGIC `aq_hourly` job, the job parameters set them (see `databricks.yml`).

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "aq", "Schema")
dbutils.widgets.text("extractor_principal", "", "Extractor service principal (application ID)")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
extractor = dbutils.widgets.get("extractor_principal")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.landing")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.checkpoints")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.exports")

# Auto Loader needs the folders it watches to exist before the first file arrives.
for folder in ["raw", "epa"]:
    dbutils.fs.mkdirs(f"/Volumes/{catalog}/{schema}/landing/{folder}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extractor access
# MAGIC The GitHub Actions extractor authenticates as a service principal that can only read and write the
# MAGIC landing volume. It can't query or change any table. GRANT is idempotent, so this runs every time.

# COMMAND ----------

if extractor:
    spark.sql(f"GRANT USE CATALOG ON CATALOG {catalog} TO `{extractor}`")
    spark.sql(f"GRANT USE SCHEMA ON SCHEMA {catalog}.{schema} TO `{extractor}`")
    spark.sql(f"GRANT READ VOLUME, WRITE VOLUME ON VOLUME {catalog}.{schema}.landing TO `{extractor}`")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tables shared by every task
# MAGIC
# MAGIC | Table | One row per | Written by |
# MAGIC |---|---|---|
# MAGIC | `bronze_raw` | API call, body stored exactly as received | `01_bronze_ingest` (Auto Loader on `landing/raw/`) |
# MAGIC | `ops_metrics` | check per task per run (row counts, HTTP status, latency, …) | every task |
# MAGIC
# MAGIC Liquid clustering instead of partitioning: the tables are too small for partitions to help, and
# MAGIC partitioning hourly writes would only create many tiny files.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.bronze_raw (
    source      STRING    COMMENT 'daq_xml, daq_forecast, openmeteo_forecast, openaq_locations, openaq_sensor_hours',
    request     STRING    COMMENT 'URL with query string. API keys go in headers, so none appear here.',
    fetched_at  TIMESTAMP COMMENT 'UTC time the response was received',
    http_status INT       COMMENT 'NULL if the request never got a response',
    body        STRING    COMMENT 'Raw response body, unparsed',
    error       STRING    COMMENT 'Exception message if the request failed',
    latency_ms  DOUBLE    COMMENT 'Request time including retries',
    run_id      STRING    COMMENT 'Extractor run: gha-<GitHub run ID>, or local',
    source_file STRING    COMMENT 'Landing file this row was loaded from',
    ingested_at TIMESTAMP COMMENT 'When Auto Loader loaded the row'
)
CLUSTER BY (source, fetched_at)
COMMENT 'Raw API responses. Append-only; silver is rebuilt from here.'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.ops_metrics (
    run_id      STRING,
    ts          TIMESTAMP,
    task        STRING,
    source      STRING,
    metric      STRING,
    value       DOUBLE
)
CLUSTER BY (task, ts)
COMMENT 'Per-run pipeline checks: row counts, HTTP status, latency, NULL rates, freshness.'
""")

display(spark.sql(f"SHOW TABLES IN {catalog}.{schema}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check that secrets exist
# MAGIC
# MAGIC The export step reads the Hugging Face token from a secret scope named `aq`. The OpenAQ key is also kept
# MAGIC there, though extraction now runs in GitHub Actions with its own copy. See the README for the `databricks secrets` CLI commands.

# COMMAND ----------

try:
    keys = [s.key for s in dbutils.secrets.list("aq")]
    print("Found secrets in scope 'aq':", keys)
    missing = {"openaq-api-key", "hf-token"} - set(keys)
    if missing:
        print("Missing:", missing)
except Exception as e:
    print("Secret scope 'aq' not found yet. See README → 'Credentials'.")
    print(e)
