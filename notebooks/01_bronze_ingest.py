# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze: load landed files with Auto Loader
# MAGIC
# MAGIC ```
# MAGIC GitHub Actions extractor ──hourly──► landing/raw/<date>/<ts>_<run>.jsonl.gz ──Auto Loader──► bronze_raw
# MAGIC ```
# MAGIC
# MAGIC Extraction runs outside Databricks because serverless compute on this workspace can't reach
# MAGIC `air.utah.gov` or `api.openaq.org` (see design doc §5). Each extractor run lands one gzipped JSON Lines
# MAGIC file with one line per HTTP request, failures included. That's the same shape as `bronze_raw`, so this
# MAGIC notebook only adds load metadata.
# MAGIC
# MAGIC | `source` | What it is | Lines per file |
# MAGIC |---|---|---|
# MAGIC | `daq_xml` | Utah DAQ, all 28 sites, ~10 days of hourly history | 1 |
# MAGIC | `daq_forecast` | Utah DAQ 3-day category + action per area | 1 |
# MAGIC | `openmeteo_forecast` | Hourly forecast, next 4 days, every station + the ridge reference point | 1 |
# MAGIC | `openaq_locations` | Utah locations, used to discover sensors | 1 |
# MAGIC | `openaq_sensor_hours` | Last 24 hours for each active low-cost PM2.5 sensor | ~40 |
# MAGIC
# MAGIC **Trigger:** `availableNow` loads every file that arrived since the last checkpoint, then stops. The job
# MAGIC runs when a new file lands (file arrival trigger), so normally that's one file.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "aq", "Schema")
dbutils.widgets.text("run_id", "manual", "Run ID")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
run_id = dbutils.widgets.get("run_id")

landing = f"/Volumes/{catalog}/{schema}/landing/raw"
checkpoint = f"/Volumes/{catalog}/{schema}/checkpoints/bronze_raw"
spark.sql(f"USE {catalog}.{schema}")


def fq(table):
    # foreachBatch may run in a separate session that doesn't inherit USE, so name tables fully there.
    return f"{catalog}.{schema}.{table}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load
# MAGIC The schema is declared rather than inferred, so a malformed file can't silently change column types.
# MAGIC Fields that don't fit the schema land in `_rescued_data`, which is counted below.
# MAGIC
# MAGIC Each micro-batch writes `bronze_raw` and `ops_metrics` together. The Delta options `txnAppId` +
# MAGIC `txnVersion` make each write idempotent: if a batch is retried after a failure, Delta skips the version it
# MAGIC already committed instead of appending the rows twice.

# COMMAND ----------

from datetime import datetime, timezone

from pyspark.sql import functions as F

LANDED_SCHEMA = (
    "source STRING, request STRING, fetched_at STRING, http_status INT, body STRING, "
    "error STRING, latency_ms DOUBLE, run_id STRING"
)


def write_batch(batch, batch_id):
    batch = batch.cache()
    (
        batch.drop("_rescued_data")
        .write.format("delta").mode("append")
        .option("txnAppId", "bronze_raw").option("txnVersion", batch_id)
        .saveAsTable(fq("bronze_raw"))
    )

    ok = F.col("http_status").between(200, 299)
    per_source = batch.groupBy("source").agg(
        F.count("*").alias("requests"),
        F.sum((~ok | F.col("http_status").isNull()).cast("int")).alias("requests_failed"),
        F.sum(F.length("body")).alias("bytes"),
        F.avg("latency_ms").alias("latency_ms"),
        F.count("_rescued_data").alias("rescued_rows"),
        F.countDistinct("source_file").alias("files"),
    )
    metric_cols = [c for c in per_source.columns if c != "source"]
    (
        per_source
        .select("source", F.explode(F.map_from_arrays(
            F.array(*[F.lit(c) for c in metric_cols]),
            F.array(*[F.col(c).cast("double") for c in metric_cols]),
        )).alias("metric", "value"))
        .select(F.lit(run_id).alias("run_id"), F.current_timestamp().alias("ts"), F.lit("bronze").alias("task"),
                "source", "metric", "value")
        .write.format("delta").mode("append")
        .option("txnAppId", "bronze_ops_metrics").option("txnVersion", batch_id)
        .saveAsTable(fq("ops_metrics"))
    )
    batch.unpersist()


query = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .schema(LANDED_SCHEMA)
    .load(landing)
    .select(
        "source", "request",
        F.to_timestamp("fetched_at").alias("fetched_at"),
        "http_status", "body", "error", "latency_ms", "run_id",
        F.col("_metadata.file_path").alias("source_file"),
        F.current_timestamp().alias("ingested_at"),
        "_rescued_data",
    )
    .writeStream
    .foreachBatch(write_batch)
    .option("checkpointLocation", checkpoint)
    .trigger(availableNow=True)
    .start()
)
query.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT source, COUNT(*) AS requests, COUNT_IF(http_status BETWEEN 200 AND 299) AS ok,
# MAGIC        MAX(fetched_at) AS latest, ROUND(SUM(LENGTH(body)) / 1e6, 1) AS mb
# MAGIC FROM bronze_raw
# MAGIC WHERE fetched_at > current_timestamp() - INTERVAL 1 DAY
# MAGIC GROUP BY source ORDER BY source

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hard check: DAQ freshness
# MAGIC DAQ is the primary source: without it there's nothing to forecast. If the newest successful DAQ pull is more
# MAGIC than 3 hours old, the run fails and the job sends its failure email. That covers both a failing source
# MAGIC and a stalled extractor. Other sources failing only shows up in `ops_metrics`, since the next pull's
# MAGIC overlapping window fills the gap.

# COMMAND ----------

latest_daq = (
    spark.table("bronze_raw")
    .where("source = 'daq_xml' AND http_status = 200 AND body LIKE '%<site>%'")
    .agg(F.max("fetched_at")).first()[0]
)
age_hours = (datetime.now(timezone.utc) - latest_daq.replace(tzinfo=timezone.utc)).total_seconds() / 3600 if latest_daq else None
print(f"Newest successful DAQ pull: {latest_daq} UTC ({age_hours:.1f} h ago)" if latest_daq else "No successful DAQ pull yet")
if age_hours is None or age_hours > 3:
    raise RuntimeError("No successful DAQ pull in the last 3 hours. Check the extractor workflow and bronze_raw.")
