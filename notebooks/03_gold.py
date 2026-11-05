# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold: features and labels, one row per station-hour
# MAGIC
# MAGIC ```
# MAGIC silver_airnow_history ─┐  AirNow first, DAQ fills the hours after it
# MAGIC silver_readings (DAQ) ─┼──────────────────────────────────────────────► silver_readings_best   (station × hour)
# MAGIC silver_epa_readings ───┘  hours EPA's quality review removed → flagged          │
# MAGIC                                                                                 ▼
# MAGIC silver_weather_archive ─┐  forecast as issued 1 / 2 / 3 days ahead     lags, rolling stats,
# MAGIC silver_weather_forecast ┴─ + valley-minus-ridge inversion ─────────────► regional mean, calendar ──► gold_features
# MAGIC ```
# MAGIC
# MAGIC A row `(station_id, hour_utc)` describes what is known once the reading for `hour_utc` is in: that reading,
# MAGIC the hours before it, and the weather forecast for the next 72 hours. The labels `pm25_t24`, `pm25_t48`,
# MAGIC `pm25_t72` are the readings 24, 48 and 72 hours later. They stay NULL until those hours arrive.
# MAGIC
# MAGIC **Point in time.** Every feature uses only data that existed at `hour_utc`. The weather columns are
# MAGIC forecasts *as issued* ~1, 2 or 3 days before the hour they describe, never the observed weather. Training
# MAGIC and serving read the same columns from the same table, so there's one feature definition, not two.
# MAGIC
# MAGIC **Recompute window.** Each hourly run recomputes the last 14 days and MERGEs them. That's enough for the
# MAGIC 72-hour labels to fill in and for DAQ's revisions (up to ~10 days back) to reach the features. `mode = full`
# MAGIC recomputes everything from 2024-01-22, e.g. after adding a feature. MERGE only rewrites rows whose values
# MAGIC changed, so both modes give the same table.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "aq", "Schema")
dbutils.widgets.text("run_id", "manual", "Run ID")
dbutils.widgets.dropdown("mode", "incremental", ["incremental", "full"], "Mode")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
run_id = dbutils.widgets.get("run_id")
mode = dbutils.widgets.get("mode")

spark.sql(f"USE {catalog}.{schema}")
spark.conf.set("spark.sql.session.timeZone", "UTC")


def fq(table):
    return f"{catalog}.{schema}.{table}"

# COMMAND ----------

from datetime import datetime, timedelta

from pyspark.sql import functions as F
from pyspark.sql.window import Window

TRAIN_START = datetime(2024, 1, 22)  # first day with forecasts issued 3 days ahead (design doc §8)
RECOMPUTE_DAYS = 14
LOOKBACK_HOURS = 168                 # longest trailing feature
HORIZONS = [24, 48, 72]
RIDGE = "ref_hidden_peak"
KEYS = ["station_id", "hour_utc"]

end_hour = (
    spark.table("silver_airnow_history").select("hour_utc")
    .unionByName(spark.table("silver_readings").select("hour_utc"))
    .agg(F.max("hour_utc")).first()[0]
)
start = TRAIN_START if mode == "full" else end_hour - timedelta(days=RECOMPUTE_DAYS)
read_from = start - timedelta(hours=LOOKBACK_HOURS)
print(f"mode={mode}: rows {start} → {end_hour}, reading from {read_from}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shared MERGE logic
# MAGIC Like silver, a row is rewritten only when its values changed (`row_hash`). The table is created on the
# MAGIC first run with a primary key on `(station_id, hour_utc TIMESERIES)`, which makes it a Unity Catalog feature
# MAGIC table that supports point-in-time lookups. `WITH SCHEMA EVOLUTION` adds new feature columns automatically.
# MAGIC A `full` run then fills them in for past rows.

# COMMAND ----------

metrics = []


def merge_into(df, target):
    value_cols = [c for c in df.columns if c not in KEYS]
    src = (
        df.withColumn("row_hash", F.sha2(F.to_json(F.struct(*value_cols)), 256))
        .withColumn("updated_at", F.current_timestamp())
    )
    if not spark.catalog.tableExists(fq(target)):
        src.limit(0).write.saveAsTable(fq(target))
        for k in KEYS:
            spark.sql(f"ALTER TABLE {fq(target)} ALTER COLUMN {k} SET NOT NULL")
        spark.sql(f"ALTER TABLE {fq(target)} ADD CONSTRAINT {target}_pk PRIMARY KEY (station_id, hour_utc TIMESERIES)")
        spark.sql(f"ALTER TABLE {fq(target)} CLUSTER BY (station_id, hour_utc)")
    src.createOrReplaceTempView(f"src_{target}")
    result = spark.sql(f"""
        MERGE WITH SCHEMA EVOLUTION INTO {fq(target)} t
        USING src_{target} s
        ON t.station_id = s.station_id AND t.hour_utc = s.hour_utc
        WHEN MATCHED AND NOT (t.row_hash <=> s.row_hash) THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """).first()
    metrics.extend([(target, "inserted", result["num_inserted_rows"]), (target, "updated", result["num_updated_rows"])])
    print(f"{target}: {result['num_inserted_rows']:,} inserted, {result['num_updated_rows']:,} updated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver_readings_best: one PM2.5 and NO₂ series per station
# MAGIC
# MAGIC **AirNow history first, DAQ after it.** Both are the same primary-instrument series: fresh DAQ values
# MAGIC match AirNow to within rounding (design doc §8). AirNow covers 2024-01-15 to the backfill date, DAQ's live
# MAGIC feed everything since. Where both exist, AirNow wins, because it holds the values as first published and
# MAGIC DAQ revises some stations' values after about a week.
# MAGIC
# MAGIC **EPA marks the hours quality review removed.** For each station, the EPA instrument (POC) that matches
# MAGIC AirNow most often is the primary one. Inside EPA's coverage, an hour that AirNow reported but the primary
# MAGIC POC doesn't have was invalidated, so `pm25_qa_removed` is set. Labels skip those hours. Features keep them,
# MAGIC because at serving time nobody has reviewed the latest hours yet either.

# COMMAND ----------

airnow = (
    spark.table("silver_airnow_history").where(F.col("hour_utc") >= read_from)
    .groupBy(*KEYS).pivot("parameter", ["pm25", "no2"]).agg(F.first("value"))
)
daq = spark.table("silver_readings").where((F.col("source") == "daq") & (F.col("hour_utc") >= read_from))
best = airnow.join(daq.select(*KEYS, "pm25_ugm3", "no2_ppb"), KEYS, "full").select(
    *KEYS,
    F.coalesce("pm25", "pm25_ugm3").alias("pm25_ugm3"),
    F.coalesce("no2", "no2_ppb").alias("no2_ppb"),
    F.when(F.col("pm25").isNotNull(), "airnow").when(F.col("pm25_ugm3").isNotNull(), "daq").alias("pm25_source"),
)

epa = spark.table("silver_epa_readings").where("parameter = 'pm25'")
epa_end = epa.agg(F.max("hour_utc")).first()[0]
if epa_end and read_from <= epa_end:
    pm25_airnow = spark.table("silver_airnow_history").where("parameter = 'pm25'").select(*KEYS, F.col("value").alias("airnow"))
    poc_match = (
        epa.join(pm25_airnow, KEYS)
        .groupBy("station_id", "poc").agg(F.avg((F.abs(F.col("value") - F.col("airnow")) < 0.05).cast("double")).alias("match_share"))
        .withColumn("_rank", F.row_number().over(Window.partitionBy("station_id").orderBy(F.col("match_share").desc())))
    )
    primary = poc_match.where("_rank = 1").select("station_id", "poc", "match_share")
    print("Primary EPA instrument per station:", {r.station_id: (r.poc, round(r.match_share, 3)) for r in primary.collect()})
    epa_primary = epa.join(primary.select("station_id", "poc"), ["station_id", "poc"])
    coverage = epa_primary.groupBy("station_id").agg(F.min("hour_utc").alias("epa_from"), F.max("hour_utc").alias("epa_to"))
    epa_valid = epa_primary.where("value IS NOT NULL").select(*KEYS, F.lit(True).alias("epa_valid"))
    best = (
        best.join(coverage, "station_id", "left").join(epa_valid, KEYS, "left")
        .withColumn("pm25_qa_removed", F.coalesce(
            F.col("pm25_ugm3").isNotNull() & F.col("hour_utc").between(F.col("epa_from"), F.col("epa_to")) & F.col("epa_valid").isNull(),
            F.lit(False),
        ))
        .drop("epa_from", "epa_to", "epa_valid")
    )
else:
    best = best.withColumn("pm25_qa_removed", F.lit(False))  # the window is past EPA's coverage

merge_into(best, "silver_readings_best")
best = spark.table("silver_readings_best").where(F.col("hour_utc") >= read_from)
metrics.append(("silver_readings_best", "qa_removed_hours", best.where("pm25_qa_removed AND hour_utc >= '%s'" % start).count()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Readings features
# MAGIC Every PM2.5 station gets a row for every hour, missing readings included, so `lag(n)` is exactly *n* hours
# MAGIC and a trailing window of *n* rows is exactly *n* hours. Averages ignore missing hours, and `pm25_obs_24h`
# MAGIC says how many of the last 24 were present.
# MAGIC
# MAGIC **Regional mean:** the average over Wasatch Front stations in the same hour. An inversion traps the whole
# MAGIC valley, so the regional level often says more about tomorrow than one station's noise.
# MAGIC
# MAGIC **Calendar:** local time is fixed MST (UTC−7), the same convention as DAQ, so hour of day never jumps at a
# MAGIC DST change. `fireworks_t*` marks the target day falling on a Utah fireworks night: around July 4,
# MAGIC Pioneer Day (July 24), and New Year. Those nights produce the sharpest PM2.5 spikes of the summer.

# COMMAND ----------

LAGS = [1, 2, 3, 6, 12, 24, 48, 72]
MEANS = [3, 6, 12, 24, 72, 168]
FIREWORKS = ["07-04", "07-05", "07-24", "07-25", "12-31", "01-01"]

stations = spark.table("silver_station")
hours = spark.sql(f"SELECT explode(sequence(TIMESTAMP '{read_from}', TIMESTAMP '{end_hour}', INTERVAL 1 HOUR)) AS hour_utc")
grid = (
    stations.where("has_pm25").select("station_id", "latitude", "longitude", "wasatch_front")
    .crossJoin(hours)
    .join(best.select(*KEYS, "pm25_ugm3", "no2_ppb", "pm25_qa_removed"), KEYS, "left")
    .withColumn("pm25_label", F.when(~F.coalesce("pm25_qa_removed", F.lit(False)), F.col("pm25_ugm3")))
)

by_hour = Window.orderBy("hour_utc")
region = (
    grid.where("wasatch_front").groupBy("hour_utc").agg(F.avg("pm25_ugm3").alias("region_pm25"))
    .withColumn("region_pm25_mean_24h", F.avg("region_pm25").over(by_hour.rowsBetween(-23, 0)))
)

w = Window.partitionBy("station_id").orderBy("hour_utc")


def trailing(n):
    return w.rowsBetween(-(n - 1), 0)


pm = F.col("pm25_ugm3")
local = F.col("hour_utc") - F.expr("INTERVAL 7 HOURS")
features = grid.join(region, "hour_utc", "left").select(
    *KEYS, "latitude", "longitude", "wasatch_front", "pm25_ugm3", "no2_ppb",
    *[F.lag(pm, n).over(w).alias(f"pm25_lag_{n}h") for n in LAGS],
    *[F.avg(pm).over(trailing(n)).alias(f"pm25_mean_{n}h") for n in MEANS],
    F.max(pm).over(trailing(24)).alias("pm25_max_24h"),
    F.stddev(pm).over(trailing(24)).alias("pm25_std_24h"),
    F.count(pm).over(trailing(24)).alias("pm25_obs_24h"),
    (pm - F.lag(pm, 24).over(w)).alias("pm25_change_24h"),
    F.avg("no2_ppb").over(trailing(24)).alias("no2_mean_24h"),
    "region_pm25", "region_pm25_mean_24h",
    F.hour(local).alias("hour_local"),
    F.dayofweek(local).alias("dow_local"),
    F.dayofyear(local).alias("doy_local"),
    *[F.date_format(local + F.expr(f"INTERVAL {h} HOURS"), "MM-dd").isin(FIREWORKS).alias(f"fireworks_t{h}") for h in HORIZONS],
    *[F.lead("pm25_label", h).over(w).alias(f"pm25_t{h}") for h in HORIZONS],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Weather as issued
# MAGIC For horizon *h* (1, 2 or 3 days), the features describe the 24 hours ending at `hour_utc + h`, from a
# MAGIC forecast issued about *h* before. Those hours all have forecasts issued at or before `hour_utc`.
# MAGIC
# MAGIC | | Training history | Since live extraction started |
# MAGIC |---|---|---|
# MAGIC | Source | `silver_weather_archive` (Previous Runs), `lead_days` 1–3 | `silver_weather_forecast`, issued ≥ 24·d hours before the target |
# MAGIC | Priority | 2 | 1 (what serving sees) |
# MAGIC
# MAGIC Both are `gfs_seamless`, so the two sources are the same forecast model. The archive has surface variables
# MAGIC only, so there's no boundary layer height. **`inversion_c` = ridge temperature − station temperature**, using
# MAGIC Hidden Peak (3,350 m) as the ridge. On a normal day the ridge is ~13 °C colder. Under an inversion the gap
# MAGIC shrinks or turns positive. **`pressure_change_24h_hpa`** catches the cold fronts that break inversions.

# COMMAND ----------

WX = ["temp_c", "rh_pct", "wind_speed_ms", "wind_dir_deg", "pressure_msl_hpa", "precip_mm", "cloud_cover_pct", "shortwave_wm2"]
WX_FEATURES = [
    "temp_c", "rh_pct", "wind_speed_ms", "wind_dir_deg", "cloud_cover_pct", "shortwave_wm2", "inversion_c",
    "wind_mean_24h", "wind_max_24h", "precip_24h_mm", "inversion_mean_24h", "pressure_change_24h_hpa",
]
wx_from = start + timedelta(hours=min(HORIZONS) - 48)  # 24 h window + 24 h pressure change before the first target

archive = (
    spark.table("silver_weather_archive").where(F.col("lead_days").between(1, 3) & (F.col("target_hour_utc") >= wx_from))
    .select("station_id", "target_hour_utc", "lead_days", *WX, F.lit(2).alias("priority"))
)
live = (
    spark.table("silver_weather_forecast").where(F.col("target_hour_utc") >= wx_from)
    .withColumn("lead_days", F.explode(F.array(*[F.lit(h // 24) for h in HORIZONS])))
    # The newest forecast issued at least d days ahead. Up to 23 hours older still counts, so missed hourly
    # runs leave the features slightly stale rather than missing.
    .where(F.col("lead_hours").between(F.col("lead_days") * 24, F.col("lead_days") * 24 + 23))
    .withColumn("_rank", F.row_number().over(Window.partitionBy("station_id", "target_hour_utc", "lead_days").orderBy("lead_hours")))
    .where("_rank = 1")
    .select("station_id", "target_hour_utc", "lead_days", *WX, F.lit(1).alias("priority"))
)
wx = (
    archive.unionByName(live)
    .withColumn("_rank", F.row_number().over(Window.partitionBy("station_id", "target_hour_utc", "lead_days").orderBy("priority")))
    .where("_rank = 1").drop("_rank", "priority")
)
ridge = wx.where(F.col("station_id") == RIDGE).select("target_hour_utc", "lead_days", F.col("temp_c").alias("ridge_temp_c"))
wx = (
    wx.where(F.col("station_id") != RIDGE)
    .join(ridge, ["target_hour_utc", "lead_days"], "left")
    .withColumn("inversion_c", F.col("ridge_temp_c") - F.col("temp_c"))
)

ww = Window.partitionBy("station_id", "lead_days").orderBy(F.unix_timestamp("target_hour_utc"))
day = ww.rangeBetween(-23 * 3600, 0)
wx = wx.select(
    "station_id", "target_hour_utc", "lead_days",
    "temp_c", "rh_pct", "wind_speed_ms", "wind_dir_deg", "cloud_cover_pct", "shortwave_wm2", "inversion_c",
    F.avg("wind_speed_ms").over(day).alias("wind_mean_24h"),
    F.max("wind_speed_ms").over(day).alias("wind_max_24h"),
    F.sum("precip_mm").over(day).alias("precip_24h_mm"),
    F.avg("inversion_c").over(day).alias("inversion_mean_24h"),
    (F.col("pressure_msl_hpa") - F.first("pressure_msl_hpa").over(ww.rangeBetween(-24 * 3600, -24 * 3600))).alias("pressure_change_24h_hpa"),
)

for h in HORIZONS:
    features = features.join(
        wx.where(F.col("lead_days") == h // 24).select(
            "station_id",
            (F.col("target_hour_utc") - F.expr(f"INTERVAL {h} HOURS")).alias("hour_utc"),
            *[F.col(c).alias(f"wx{h}_{c}") for c in WX_FEATURES],
        ),
        KEYS, "left",
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write
# MAGIC Columns are ordered keys → station → readings → calendar → weather → labels, so the table reads left to
# MAGIC right in the order a forecast is built.

# COMMAND ----------

label_cols = [f"pm25_t{h}" for h in HORIZONS]
features = features.where(F.col("hour_utc") >= start)
features = features.select(*[c for c in features.columns if c not in label_cols], *label_cols)
merge_into(features, "gold_features")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checks
# MAGIC Soft checks only, recorded in `ops_metrics`. The share of label-eligible rows that have a label shows
# MAGIC whether readings arrive. The share with weather features shows whether forecasts arrive, which depends on
# MAGIC the hourly extractor running.

# COMMAND ----------

gold = spark.table("gold_features").where(F.col("hour_utc") >= start)
checks = gold.agg(
    F.count("*").alias("rows"),
    F.avg(F.col("pm25_ugm3").isNotNull().cast("double")).alias("share_with_pm25"),
    *[F.avg(F.when(F.col("hour_utc") <= F.lit(end_hour) - F.expr(f"INTERVAL {h} HOURS"), F.col(f"pm25_t{h}").isNotNull().cast("double")))
      .alias(f"share_with_label_t{h}") for h in HORIZONS],
    *[F.avg(F.col(f"wx{h}_temp_c").isNotNull().cast("double")).alias(f"share_with_weather_t{h}") for h in HORIZONS],
    F.max(F.when(F.col("pm25_ugm3").isNotNull(), F.col("hour_utc"))).alias("newest_hour"),
).first().asDict()
newest = checks.pop("newest_hour")
checks["newest_hour_age_h"] = (datetime.utcnow() - newest).total_seconds() / 3600 if newest else None
metrics.extend(("gold_features", k, v) for k, v in checks.items())

spark.createDataFrame(
    [(run_id, source, m, float(v)) for source, m, v in metrics if v is not None],
    "run_id STRING, source STRING, metric STRING, value DOUBLE",
).select("run_id", F.current_timestamp().alias("ts"), F.lit("gold").alias("task"), "source", "metric", "value") \
    .write.mode("append").saveAsTable(fq("ops_metrics"))
display(spark.table("ops_metrics").where(f"task = 'gold' AND run_id = '{run_id}'").select("source", "metric", "value"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT s.name, g.hour_utc, g.pm25_ugm3, g.pm25_mean_24h, g.region_pm25, g.wx24_inversion_c, g.wx24_wind_mean_24h, g.pm25_t24
# MAGIC FROM gold_features g JOIN silver_station s USING (station_id)
# MAGIC WHERE s.wasatch_front AND g.pm25_ugm3 IS NOT NULL
# MAGIC QUALIFY row_number() OVER (PARTITION BY g.station_id ORDER BY g.hour_utc DESC) = 1
# MAGIC ORDER BY s.name
