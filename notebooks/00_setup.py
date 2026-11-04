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
