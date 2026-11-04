# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup
# MAGIC
# MAGIC Creates the Unity Catalog objects the rest of the project uses:
# MAGIC
# MAGIC | Object | Purpose |
# MAGIC |---|---|
# MAGIC | `<catalog>.aq` schema | holds bronze, silver, and gold tables |
# MAGIC | `<catalog>.aq.landing` volume | bulk files for the historical backfill (EPA AirData) |
# MAGIC | `<catalog>.aq.checkpoints` volume | streaming checkpoint storage |
# MAGIC
# MAGIC The `catalog` and `schema` widgets default to `workspace.aq`. When the notebook runs as part of the
# MAGIC `aq_hourly` job, the job parameters set them (see `databricks.yml`).

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "aq", "Schema")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.landing")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.checkpoints")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tables shared by every task
# MAGIC
# MAGIC | Table | One row per | Written by |
# MAGIC |---|---|---|
# MAGIC | `bronze_raw` | API call, body stored exactly as received | `01_bronze_ingest` |
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
    run_id      STRING    COMMENT 'Databricks job run ID, or manual'
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
# MAGIC Ingestion reads the OpenAQ key, and the export step reads the Hugging Face token, from a secret scope
# MAGIC named `aq`. Utah DAQ and Open-Meteo need no key. See the README for the `databricks secrets` CLI commands.

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
