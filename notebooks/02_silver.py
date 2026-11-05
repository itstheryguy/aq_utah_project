# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver: parse, convert units, reconcile overlapping pulls
# MAGIC
# MAGIC ```
# MAGIC reference/stations.csv ───────────────────────────────overwrite──► silver_station
# MAGIC bronze_raw ─┬─ daq_xml ─────────── parse + units + ranges ─MERGE──► silver_readings          (station × hour × source)
# MAGIC             ├─ daq_forecast ────── parse CSV ──────────────MERGE──► silver_daq_forecast      (area × day × issue date)
# MAGIC             ├─ openmeteo_forecast ─ zip with request coords ─MERGE─► silver_weather_forecast  (station × issue hour × target hour)
# MAGIC             ├─ openaq_locations ── low-cost PM2.5 sensors ──MERGE──► silver_lowcost_sensors   (sensor)
# MAGIC             └─ openaq_sensor_hours ─────────────────────────MERGE──► silver_lowcost_readings  (sensor × hour)
# MAGIC ```
# MAGIC
# MAGIC Each stream reads only the bronze rows that are new since its checkpoint, and runs with `availableNow`.
# MAGIC
# MAGIC **Why MERGE:** every DAQ pull repeats the last ~10 days, and every OpenAQ pull the last 24 hours. Each
# MAGIC batch is first reduced to the newest value per key, then MERGEd. A row is updated only if the fetch is newer
# MAGIC **and** the values changed (compared by `row_hash`). `revisions` counts how often a source changed a value
# MAGIC it had already published. DAQ's live data isn't quality-assured, so this measures how much it moves.
# MAGIC
# MAGIC All timestamps are UTC. Column names carry their units (`temp_c`, `no2_ppb`).

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "aq", "Schema")
dbutils.widgets.text("run_id", "manual", "Run ID")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
run_id = dbutils.widgets.get("run_id")

checkpoint_root = f"/Volumes/{catalog}/{schema}/checkpoints"
spark.sql(f"USE {catalog}.{schema}")
spark.conf.set("spark.sql.session.timeZone", "UTC")


def fq(table):
    # foreachBatch may run in a separate session that doesn't inherit USE, so name tables fully there.
    return f"{catalog}.{schema}.{table}"

# COMMAND ----------

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.window import Window

stations_pd = pd.read_csv("../reference/stations.csv", dtype={
    "openaq_location_id": "Int64", "openaq_pm25_sensor_id": "Int64", "openaq_no2_sensor_id": "Int64", "epa_site_id": "string",
})
overrides_pd = pd.read_csv("../reference/daq_unit_overrides.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver_station
# MAGIC Reference data kept in git, so the table is simply overwritten from the CSV each run.

# COMMAND ----------

stations = spark.createDataFrame(stations_pd)
stations.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_station")
stations = spark.table("silver_station")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shared MERGE logic
# MAGIC `transform(batch)` returns the parsed rows plus any extra metrics. `merge_stream` then keeps the newest row
# MAGIC per key, MERGEs it, and records what happened in `ops_metrics`.

# COMMAND ----------

def merge_stream(name, sources, transform, target, keys, ddl, table="bronze_raw"):
    """Stream `table` (bronze_raw rows from `sources` by default) through `transform` and MERGE into `target`."""
    spark.sql(f"CREATE TABLE IF NOT EXISTS {fq(target)} ({ddl}) CLUSTER BY ({', '.join(keys)})")

    def write_batch(batch, batch_id):
        batch = batch.cache()
        rows, extra_metrics = transform(batch)
        value_cols = [c for c in rows.columns if c not in keys and c != "fetched_at"]

        newest = Window.partitionBy(*keys).orderBy(F.col("fetched_at").desc())
        updates = (
            rows.withColumn("_rank", F.row_number().over(newest)).where("_rank = 1").drop("_rank")
            .withColumn("row_hash", F.sha2(F.to_json(F.struct(*value_cols)), 256))
        )
        updates.createOrReplaceTempView(f"updates_{name}")

        on = " AND ".join(f"t.{k} <=> s.{k}" for k in keys)
        insert_cols = keys + value_cols + ["row_hash", "fetched_at"]
        result = updates.sparkSession.sql(f"""
            MERGE INTO {fq(target)} t
            USING updates_{name} s
            ON {on}
            WHEN MATCHED AND s.fetched_at > t.fetched_at AND s.row_hash <> t.row_hash THEN UPDATE SET
                {', '.join(f't.{c} = s.{c}' for c in value_cols)},
                t.row_hash = s.row_hash, t.fetched_at = s.fetched_at, t.revisions = t.revisions + 1
            WHEN NOT MATCHED THEN INSERT ({', '.join(insert_cols)}, first_seen_at, revisions)
                VALUES ({', '.join(f's.{c}' for c in insert_cols)}, s.fetched_at, 0)
        """).first()

        metrics = [
            ("bronze_rows", batch.count()),
            ("rows", updates.count()),
            ("inserted", result["num_inserted_rows"]),
            ("updated", result["num_updated_rows"]),
        ] + extra_metrics
        updates.sparkSession.createDataFrame(
            [(run_id, name, m, float(v)) for m, v in metrics if v is not None],
            "run_id STRING, source STRING, metric STRING, value DOUBLE",
        ).select(
            "run_id", F.current_timestamp().alias("ts"), F.lit("silver").alias("task"), "source", "metric", "value"
        ).write.mode("append").saveAsTable(fq("ops_metrics"))
        batch.unpersist()

    # Bound each micro-batch: a backfill can put ~1 GB of raw JSON into bronze at once.
    stream = spark.readStream.option("maxBytesPerTrigger", "256m").table(table)
    if table == "bronze_raw":
        stream = stream.where(F.col("source").isin(sources) & F.col("http_status").between(200, 299))
    return (
        stream.writeStream
        .foreachBatch(write_batch)
        .option("checkpointLocation", f"{checkpoint_root}/silver_{name}")
        .trigger(availableNow=True)
        .start()
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver_readings: Utah DAQ
# MAGIC
# MAGIC **Timestamps:** DAQ reports Mountain *Standard* Time all year, labelled with the start of the hour. Adding
# MAGIC a fixed 7 hours gives UTC. Converting from `America/Denver` would be wrong for half the year and create a
# MAGIC duplicate or missing hour at each DST change.
# MAGIC
# MAGIC **Units vary by site within the same feed.** Most sites report gases in ppm and pressure in mmHg, but
# MAGIC Washakie reports NO₂/NOx in ppb, Magna reports SO₂ in ppb, and Redwash and Ouray report pressure in
# MAGIC hPa. `reference/daq_unit_overrides.csv` lists these cases, with the evidence for each. A value that is
# MAGIC still outside a plausible range after conversion becomes NULL and is counted as `out_of_range_<column>`.
# MAGIC
# MAGIC DAQ's own rolling averages (`ozone_8hr_avg`, `pm25_24hr_avg`) are skipped. Gold computes its own rolling
# MAGIC features from the hourly values.

# COMMAND ----------

# daq_field: (silver column, DAQ default unit, silver unit, plausible min, plausible max)
DAQ_FIELDS = {
    "pm25":              ("pm25_ugm3",     "ug/m3", "ug/m3", -5, 1000),
    "pm10":              ("pm10_ugm3",     "ug/m3", "ug/m3", -5, 3000),
    "ozone":             ("o3_ppb",        "ppm",   "ppb",   -5, 300),
    "no2":               ("no2_ppb",       "ppm",   "ppb",   -5, 500),
    "nox":               ("nox_ppb",       "ppm",   "ppb",   -5, 1500),
    "noy":               ("noy_ppb",       "ppb",   "ppb",   -5, 1500),
    "co":                ("co_ppm",        "ppm",   "ppm",   -1, 50),
    "so2":               ("so2_ppb",       "ppm",   "ppb",   -5, 500),
    "temperature":       ("temp_c",        "F",     "C",     -45, 50),
    "relative_humidity": ("rh_pct",        "%",     "%",     0, 100.5),
    "wind_speed":        ("wind_speed_ms", "mph",   "m/s",   0, 60),  # mph inferred, see design doc §4
    "wind_direction":    ("wind_dir_deg",  "deg",   "deg",   0, 360),
    "solar_radiation":   ("solar_wm2",     "W/m2",  "W/m2",  -10, 1500),
    "bp":                ("pressure_hpa",  "mmHg",  "hPa",   500, 1100),
}
CONVERT = {
    ("ppm", "ppb"): lambda x: x * 1000,
    ("F", "C"): lambda x: (x - 32) * 5 / 9,
    ("mph", "m/s"): lambda x: x * 0.44704,
    ("mmHg", "hPa"): lambda x: x * 1.333224,
}
DAQ_KNOWN_TAGS = set(DAQ_FIELDS) | {"air_quality_data", "state", "site", "name", "data", "date", "ozone_8hr_avg", "pm25_24hr_avg"}

daq_xml_schema = (
    "STRUCT<site: ARRAY<STRUCT<name: STRING, data: ARRAY<STRUCT<date: STRING, "
    + ", ".join(f"{f}: STRING" for f in DAQ_FIELDS)
    + ">>>>>"
)
field_spec = spark.createDataFrame(
    [(f, *spec) for f, spec in DAQ_FIELDS.items()],
    "daq_field STRING, column STRING, default_unit STRING, silver_unit STRING, lo DOUBLE, hi DOUBLE",
)
unit_overrides = spark.createDataFrame(overrides_pd[["station_id", "daq_field", "unit"]])
daq_columns = [spec[0] for spec in DAQ_FIELDS.values()]


def convert(value, unit, silver_unit):
    out = F.when(F.col(unit) == F.col(silver_unit), value)
    for (src, dst), fn in CONVERT.items():
        out = out.when((F.col(unit) == src) & (F.col(silver_unit) == dst), fn(value))
    return out  # any other combination, including unit = 'invalid', gives NULL


def transform_daq(batch):
    sites = (
        batch.select("fetched_at", F.explode(F.from_xml("body", daq_xml_schema)["site"]).alias("s"))
        .select("fetched_at", F.col("s.name").alias("daq_site_name"), F.explode("s.data").alias("d"))
    )
    long = sites.select(
        "fetched_at", "daq_site_name",
        (F.to_timestamp("d.date", "MM/dd/yyyy HH:mm:ss") + F.expr("INTERVAL 7 HOURS")).alias("hour_utc"),
        F.explode(F.map_from_arrays(
            F.array(*[F.lit(f) for f in DAQ_FIELDS]),
            F.array(*[F.col(f"d.{f}") for f in DAQ_FIELDS]),
        )).alias("daq_field", "raw"),
    )
    raw = F.expr("try_cast(nullif(trim(raw), '') AS DOUBLE)")
    converted = (
        long.join(F.broadcast(stations.select("station_id", "daq_site_name")), "daq_site_name", "left")
        .join(F.broadcast(field_spec), "daq_field")
        .join(F.broadcast(unit_overrides), ["station_id", "daq_field"], "left")
        .withColumn("unit", F.coalesce("unit", "default_unit"))
        .withColumn("raw", F.when(raw != -9999, raw))
        .withColumn("converted", convert(F.col("raw"), "unit", "silver_unit"))
        .withColumn("out_of_range", F.col("converted").isNotNull() & ~F.col("converted").between(F.col("lo"), F.col("hi")))
        .withColumn("value", F.when(~F.col("out_of_range"), F.col("converted")))
    )

    unknown_sites = converted.where("station_id IS NULL").select("daq_site_name").distinct().count()
    out_of_range = {r["column"]: r["n"] for r in converted.where("out_of_range").groupBy("column").agg(F.count("*").alias("n")).collect()}
    tags = batch.select(F.explode(F.regexp_extract_all("body", F.lit(r"<([a-z0-9_]+)>"), 1)).alias("t")).distinct()
    unknown_tags = [r.t for r in tags.collect() if r.t not in DAQ_KNOWN_TAGS]
    if unknown_tags:
        print("DAQ XML has tags the parser doesn't know:", unknown_tags)

    rows = (
        converted.where("station_id IS NOT NULL")
        .groupBy("station_id", "hour_utc", "fetched_at").pivot("column", daq_columns).agg(F.first("value"))
        .withColumn("source", F.lit("daq"))
    )
    null_share = rows.select([F.avg(F.col(c).isNull().cast("double")).alias(c) for c in daq_columns]).first().asDict()
    newest_hour = rows.agg(F.max("hour_utc")).first()[0]

    metrics = (
        [("unknown_sites", unknown_sites), ("unknown_xml_tags", len(unknown_tags))]
        + [(f"null_share_{c}", v) for c, v in null_share.items()]
        + [(f"out_of_range_{c}", n) for c, n in out_of_range.items()]
        + [("newest_hour_age_h", (pd.Timestamp.utcnow().tz_localize(None) - pd.Timestamp(newest_hour)).total_seconds() / 3600 if newest_hour else None)]
    )
    return rows, metrics


daq_ddl = (
    "station_id STRING, hour_utc TIMESTAMP, source STRING, "
    + ", ".join(f"{c} DOUBLE" for c in daq_columns)
    + ", row_hash STRING, fetched_at TIMESTAMP, first_seen_at TIMESTAMP, revisions INT"
)
merge_stream("daq", ["daq_xml"], transform_daq, "silver_readings", ["station_id", "hour_utc", "source"], daq_ddl).awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver_daq_forecast
# MAGIC DAQ's own forecast: a category (`good`, `moderate`, …) and a burn-restriction action per forecast area, for
# MAGIC today and the next 2 days. It's re-fetched every hour, and DAQ can revise it during the day, so the key
# MAGIC includes the local date it was issued. The newest version per issue date wins.

# COMMAND ----------

def transform_daq_forecast(batch):
    lines = batch.select("fetched_at", F.posexplode(F.split("body", r"\r?\n")).alias("i", "line")).where("i > 0 AND trim(line) <> ''")
    parsed = lines.select(
        "fetched_at",
        F.from_csv("line", "day STRING, area STRING, severity STRING, action STRING, message STRING").alias("r"),
    )
    rows = parsed.select(
        F.col("r.area").alias("daq_forecast_area"),
        F.to_date("r.day").alias("forecast_date"),
        F.to_date(F.from_utc_timestamp("fetched_at", "America/Denver")).alias("issued_date"),
        F.lower("r.severity").alias("severity"),
        F.lower("r.action").alias("action"),
        F.nullif(F.trim("r.message"), F.lit("")).alias("message"),
        "fetched_at",
    ).withColumn("days_ahead", F.datediff("forecast_date", "issued_date"))
    return rows, [("unparsed_lines", rows.where("forecast_date IS NULL OR daq_forecast_area IS NULL").count())]


merge_stream(
    "daq_forecast", ["daq_forecast"], transform_daq_forecast, "silver_daq_forecast",
    ["daq_forecast_area", "forecast_date", "issued_date"],
    "daq_forecast_area STRING, forecast_date DATE, issued_date DATE, severity STRING, action STRING, message STRING, "
    "days_ahead INT, row_hash STRING, fetched_at TIMESTAMP, first_seen_at TIMESTAMP, revisions INT",
).awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver_weather_forecast: Open-Meteo
# MAGIC One response holds every location, in request order. Open-Meteo snaps coordinates to its grid
# MAGIC (40.7335 comes back as 40.72379), so the response coordinates can't be matched to stations. Instead,
# MAGIC each result is paired with the coordinates at the same position in the request URL.
# MAGIC
# MAGIC `issued_hour_utc` is the hour the forecast was fetched, and `lead_hours` = target − issued. Gold uses
# MAGIC `lead_hours` 24 / 48 / 72 to line up with the Previous Runs archive used for training.

# COMMAND ----------

OM_FIELDS = {  # Open-Meteo variable: (silver column, conversion)
    "temperature_2m": ("temp_c", None),
    "relative_humidity_2m": ("rh_pct", None),
    "dew_point_2m": ("dew_point_c", None),
    "wind_speed_10m": ("wind_speed_ms", lambda x: x / 3.6),  # km/h → m/s
    "wind_direction_10m": ("wind_dir_deg", None),
    "pressure_msl": ("pressure_msl_hpa", None),
    "surface_pressure": ("surface_pressure_hpa", None),
    "precipitation": ("precip_mm", None),
    "cloud_cover": ("cloud_cover_pct", None),
    "shortwave_radiation": ("shortwave_wm2", None),
    "boundary_layer_height": ("blh_m", None),
}
om_schema = (
    "ARRAY<STRUCT<elevation: DOUBLE, hourly: STRUCT<time: ARRAY<STRING>, "
    + ", ".join(f"{v}: ARRAY<DOUBLE>" for v in OM_FIELDS)
    + ">>>"
)
station_coords = stations.select(
    "station_id", F.round("latitude", 5).alias("lat"), F.round("longitude", 5).alias("lon")
)


def transform_openmeteo(batch):
    located = batch.select(
        "fetched_at",
        F.split(F.expr("parse_url(request, 'QUERY', 'latitude')"), ",").alias("lats"),
        F.split(F.expr("parse_url(request, 'QUERY', 'longitude')"), ",").alias("lons"),
        F.posexplode(F.from_json("body", om_schema)).alias("i", "loc"),
    ).select(
        "fetched_at", "loc",
        F.round(F.element_at("lats", F.col("i") + 1).cast("double"), 5).alias("lat"),
        F.round(F.element_at("lons", F.col("i") + 1).cast("double"), 5).alias("lon"),
    ).join(F.broadcast(station_coords), ["lat", "lon"], "left")

    unmatched = located.where("station_id IS NULL").count()
    hours = located.where("station_id IS NOT NULL").select(
        "station_id", "fetched_at",
        F.col("loc.elevation").alias("model_elevation_m"),
        F.explode(F.arrays_zip(*[F.col(f"loc.hourly.{v}").alias(v) for v in ["time", *OM_FIELDS]])).alias("h"),
    )
    issued = F.date_trunc("hour", "fetched_at")
    target = F.to_timestamp("h.time", "yyyy-MM-dd'T'HH:mm")
    rows = hours.select(
        "station_id",
        issued.alias("issued_hour_utc"),
        target.alias("target_hour_utc"),
        ((F.unix_timestamp(target) - F.unix_timestamp(issued)) / 3600).cast("int").alias("lead_hours"),
        *[(fn(F.col(f"h.{v}")) if fn else F.col(f"h.{v}")).alias(col) for v, (col, fn) in OM_FIELDS.items()],
        "model_elevation_m",
        "fetched_at",
    )
    return rows, [("unmatched_locations", unmatched)]


om_columns = [col for col, _ in OM_FIELDS.values()]
merge_stream(
    "openmeteo", ["openmeteo_forecast"], transform_openmeteo, "silver_weather_forecast",
    ["station_id", "issued_hour_utc", "target_hour_utc"],
    "station_id STRING, issued_hour_utc TIMESTAMP, target_hour_utc TIMESTAMP, lead_hours INT, "
    + ", ".join(f"{c} DOUBLE" for c in om_columns)
    + ", model_elevation_m DOUBLE, row_hash STRING, fetched_at TIMESTAMP, first_seen_at TIMESTAMP, revisions INT",
).awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Low-cost sensors: OpenAQ
# MAGIC AirGradient and Clarity sensors aren't regulatory monitors. They're less accurate and sit at different
# MAGIC places from the DAQ stations. So they get their own tables rather than rows in `silver_readings`. Gold uses
# MAGIC them as neighborhood context around each station, not as forecast targets.

# COMMAND ----------

def transform_openaq_locations(batch):
    locations = batch.select(
        "fetched_at",
        F.explode(F.from_json("body", (
            "STRUCT<results: ARRAY<STRUCT<id: BIGINT, name: STRING, provider: STRUCT<name: STRING>, "
            "coordinates: STRUCT<latitude: DOUBLE, longitude: DOUBLE>, datetimeLast: STRUCT<utc: STRING>, "
            "sensors: ARRAY<STRUCT<id: BIGINT, parameter: STRUCT<name: STRING>>>>>>"
        ))["results"]).alias("l"),
    ).where("l.provider.name <> 'AirNow'")
    rows = locations.select("fetched_at", "l", F.explode("l.sensors").alias("s")).where("s.parameter.name = 'pm25'").select(
        F.col("s.id").alias("sensor_id"),
        F.col("l.id").alias("location_id"),
        F.col("l.name").alias("name"),
        F.col("l.provider.name").alias("provider"),
        F.col("l.coordinates.latitude").alias("latitude"),
        F.col("l.coordinates.longitude").alias("longitude"),
        F.to_timestamp("l.datetimeLast.utc").alias("last_reported_utc"),
        "fetched_at",
    )
    return rows, []


merge_stream(
    "openaq_sensors", ["openaq_locations"], transform_openaq_locations, "silver_lowcost_sensors", ["sensor_id"],
    "sensor_id BIGINT, location_id BIGINT, name STRING, provider STRING, latitude DOUBLE, longitude DOUBLE, "
    "last_reported_utc TIMESTAMP, row_hash STRING, fetched_at TIMESTAMP, first_seen_at TIMESTAMP, revisions INT",
).awaitTermination()

# COMMAND ----------

def transform_openaq_hours(batch):
    rows = batch.select(
        "fetched_at",
        F.regexp_extract("request", r"/sensors/(\d+)/hours", 1).cast("bigint").alias("sensor_id"),
        F.explode(F.from_json("body", (
            "STRUCT<results: ARRAY<STRUCT<value: DOUBLE, flagInfo: STRUCT<hasFlags: BOOLEAN>, "
            "period: STRUCT<datetimeFrom: STRUCT<utc: STRING>>, coverage: STRUCT<percentComplete: DOUBLE>>>>"
        ))["results"]).alias("r"),
    ).select(
        "sensor_id",
        F.to_timestamp("r.period.datetimeFrom.utc").alias("hour_utc"),  # start of the hour, like DAQ
        F.col("r.value").alias("raw"),
        F.col("r.coverage.percentComplete").alias("coverage_pct"),
        F.col("r.flagInfo.hasFlags").alias("has_flags"),
        "fetched_at",
    )
    out_of_range = rows.where("raw IS NOT NULL AND NOT raw BETWEEN -5 AND 1000").count()
    rows = rows.withColumn("pm25_ugm3", F.when(F.col("raw").between(-5, 1000), F.col("raw"))).drop("raw")
    return rows, [("out_of_range_pm25_ugm3", out_of_range)]


merge_stream(
    "openaq_hours", ["openaq_sensor_hours"], transform_openaq_hours, "silver_lowcost_readings", ["sensor_id", "hour_utc"],
    "sensor_id BIGINT, hour_utc TIMESTAMP, coverage_pct DOUBLE, has_flags BOOLEAN, pm25_ugm3 DOUBLE, "
    "row_hash STRING, fetched_at TIMESTAMP, first_seen_at TIMESTAMP, revisions INT",
).awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## History (backfill)
# MAGIC
# MAGIC | Table | From | Covers | Role |
# MAGIC |---|---|---|---|
# MAGIC | `silver_airnow_history` | OpenAQ `/sensors/{id}/hours` for each DAQ monitor | 2024-01-15 → backfill date | Training history: the same primary-instrument series DAQ publishes live |
# MAGIC | `silver_epa_readings` | EPA AirData pre-generated files | 2024-01-01 → 2026-06-01 | Quality-assured check: every collocated instrument (POC), with qualifiers |
# MAGIC | `silver_weather_archive` | Open-Meteo Previous Runs | 2024-01-15 → backfill date | Forecasts as issued 0–3 days ahead, for training |
# MAGIC
# MAGIC These are long tables (one row per parameter or lead time) so that values for one key arriving in different
# MAGIC files can't overwrite each other.

# COMMAND ----------

HISTORY_RANGES = {"pm25": (-5, 1000), "no2": (-5, 500)}
sensor_map = stations.select(
    "station_id", F.explode(F.array(
        F.struct(F.lit("pm25").alias("parameter"), F.col("openaq_pm25_sensor_id").alias("sensor_id")),
        F.struct(F.lit("no2").alias("parameter"), F.col("openaq_no2_sensor_id").alias("sensor_id")),
    )).alias("m"),
).select("station_id", "m.parameter", "m.sensor_id").where("sensor_id IS NOT NULL")


def in_range(parameter, value):
    out = F.lit(None).cast("double")
    for p, (lo, hi) in HISTORY_RANGES.items():
        out = F.when((parameter == p) & value.between(lo, hi), value).otherwise(out)
    return out


def transform_airnow_history(batch):
    rows = batch.select(
        "fetched_at",
        F.regexp_extract("request", r"/sensors/(\d+)/hours", 1).cast("bigint").alias("sensor_id"),
        F.explode(F.from_json("body", (
            "STRUCT<results: ARRAY<STRUCT<value: DOUBLE, parameter: STRUCT<units: STRING>, "
            "period: STRUCT<datetimeFrom: STRUCT<utc: STRING>>, coverage: STRUCT<percentComplete: DOUBLE>>>>"
        ))["results"]).alias("r"),
    ).join(F.broadcast(sensor_map), "sensor_id")
    # OpenAQ reports AirNow gases in ppm; silver uses ppb.
    value = F.when(F.col("r.parameter.units") == "ppm", F.col("r.value") * 1000).otherwise(F.col("r.value"))
    rows = rows.select(
        "station_id", "parameter",
        F.to_timestamp("r.period.datetimeFrom.utc").alias("hour_utc"),
        value.alias("raw"),
        F.col("r.coverage.percentComplete").alias("coverage_pct"),
        "fetched_at",
    )
    out_of_range = rows.where(F.col("raw").isNotNull() & in_range(F.col("parameter"), F.col("raw")).isNull()).count()
    rows = rows.withColumn("value", in_range(F.col("parameter"), F.col("raw"))).drop("raw")
    return rows, [("out_of_range", out_of_range)]


merge_stream(
    "airnow_history", ["openaq_history"], transform_airnow_history, "silver_airnow_history",
    ["station_id", "parameter", "hour_utc"],
    "station_id STRING, parameter STRING, hour_utc TIMESTAMP, coverage_pct DOUBLE, value DOUBLE, "
    "row_hash STRING, fetched_at TIMESTAMP, first_seen_at TIMESTAMP, revisions INT",
).awaitTermination()

# COMMAND ----------

EPA_PARAMETERS = {"88101": "pm25", "42602": "no2"}
epa_sites = stations.where("epa_site_id IS NOT NULL").select("station_id", "epa_site_id")


def transform_epa(batch):
    rows = batch.select(
        F.concat_ws("-", "state_code", "county_code", "site_num").alias("epa_site_id"),
        F.element_at(F.create_map(*[F.lit(x) for kv in EPA_PARAMETERS.items() for x in kv]), F.col("parameter_code")).alias("parameter"),
        F.col("poc").cast("int").alias("poc"),
        # EPA files carry both local and GMT times; GMT is already UTC and has no DST ambiguity.
        F.to_timestamp(F.concat_ws(" ", "date_gmt", "time_gmt"), "yyyy-MM-dd HH:mm").alias("hour_utc"),
        F.expr("try_cast(sample_measurement AS DOUBLE)").alias("raw"),
        F.col("units_of_measure").alias("units"),
        F.nullif("qualifier", F.lit("")).alias("qualifier"),
        F.col("method_code"),
        F.to_date("date_of_last_change").alias("date_of_last_change"),
        F.col("ingested_at").alias("fetched_at"),
    ).join(F.broadcast(epa_sites), "epa_site_id").drop("epa_site_id").where("parameter IS NOT NULL")
    out_of_range = rows.where(F.col("raw").isNotNull() & in_range(F.col("parameter"), F.col("raw")).isNull()).count()
    rows = rows.withColumn("value", in_range(F.col("parameter"), F.col("raw"))).drop("raw")
    units = [r.units for r in rows.select("parameter", "units").distinct().collect()]
    print("EPA units seen:", units)  # pm25 µg/m³ and no2 ppb, already silver units
    return rows, [("out_of_range", out_of_range)]


merge_stream(
    "epa", None, transform_epa, "silver_epa_readings", ["station_id", "parameter", "hour_utc", "poc"],
    "station_id STRING, parameter STRING, poc INT, hour_utc TIMESTAMP, units STRING, qualifier STRING, "
    "method_code STRING, date_of_last_change DATE, value DOUBLE, "
    "row_hash STRING, fetched_at TIMESTAMP, first_seen_at TIMESTAMP, revisions INT",
    table="bronze_epa_hourly",
).awaitTermination()

# COMMAND ----------

ARCHIVE_FIELDS = {  # Open-Meteo variable: (silver column, conversion), same columns as silver_weather_forecast
    "temperature_2m": ("temp_c", None),
    "relative_humidity_2m": ("rh_pct", None),
    "wind_speed_10m": ("wind_speed_ms", lambda x: x / 3.6),
    "wind_direction_10m": ("wind_dir_deg", None),
    "pressure_msl": ("pressure_msl_hpa", None),
    "precipitation": ("precip_mm", None),
    "cloud_cover": ("cloud_cover_pct", None),
    "shortwave_radiation": ("shortwave_wm2", None),
}
ARCHIVE_LEADS = [0, 1, 2, 3]


def archive_var(v, lead):
    return v if lead == 0 else f"{v}_previous_day{lead}"


archive_schema = "STRUCT<hourly: STRUCT<time: ARRAY<STRING>, " + ", ".join(
    f"{archive_var(v, d)}: ARRAY<DOUBLE>" for d in ARCHIVE_LEADS for v in ARCHIVE_FIELDS
) + ">>"


def transform_weather_archive(batch):
    located = batch.select(
        "fetched_at",
        F.round(F.expr("parse_url(request, 'QUERY', 'latitude')").cast("double"), 5).alias("lat"),
        F.round(F.expr("parse_url(request, 'QUERY', 'longitude')").cast("double"), 5).alias("lon"),
        F.from_json("body", archive_schema)["hourly"].alias("h"),
    ).join(F.broadcast(station_coords), ["lat", "lon"], "left")
    unmatched = located.where("station_id IS NULL").count()

    cols = ["time"] + [archive_var(v, d) for d in ARCHIVE_LEADS for v in ARCHIVE_FIELDS]
    hours = located.where("station_id IS NOT NULL").select(
        "station_id", "fetched_at", F.explode(F.arrays_zip(*[F.col(f"h.{c}").alias(c) for c in cols])).alias("z")
    )
    by_lead = hours.select(
        "station_id", "fetched_at",
        F.to_timestamp("z.time", "yyyy-MM-dd'T'HH:mm").alias("target_hour_utc"),
        F.explode(F.array(*[
            F.struct(F.lit(d).alias("lead_days"), *[
                (fn(F.col(f"z.{archive_var(v, d)}")) if fn else F.col(f"z.{archive_var(v, d)}")).alias(col)
                for v, (col, fn) in ARCHIVE_FIELDS.items()
            ])
            for d in ARCHIVE_LEADS
        ])).alias("x"),
    )
    rows = by_lead.select("station_id", "target_hour_utc", "x.*", "fetched_at").where(
        " OR ".join(f"{col} IS NOT NULL" for col, _ in ARCHIVE_FIELDS.values())
    )
    return rows, [("unmatched_locations", unmatched)]


merge_stream(
    "weather_archive", ["openmeteo_archive"], transform_weather_archive, "silver_weather_archive",
    ["station_id", "target_hour_utc", "lead_days"],
    "station_id STRING, target_hour_utc TIMESTAMP, lead_days INT, "
    + ", ".join(f"{col} DOUBLE" for col, _ in ARCHIVE_FIELDS.values())
    + ", row_hash STRING, fetched_at TIMESTAMP, first_seen_at TIMESTAMP, revisions INT",
).awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## This run

# COMMAND ----------

display(
    spark.table("ops_metrics").where(f"task = 'silver' AND run_id = '{run_id}'")
    .where("metric IN ('bronze_rows', 'rows', 'inserted', 'updated', 'unknown_sites', 'unmatched_locations') OR metric LIKE 'out_of_range%'")
    .groupBy("source").pivot("metric").agg(F.sum("value"))
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT s.name, r.hour_utc, r.pm25_ugm3, r.no2_ppb, r.temp_c, r.wind_speed_ms, r.pressure_hpa, r.revisions
# MAGIC FROM silver_readings r JOIN silver_station s USING (station_id)
# MAGIC WHERE s.wasatch_front
# MAGIC QUALIFY row_number() OVER (PARTITION BY r.station_id ORDER BY r.hour_utc DESC) = 1
# MAGIC ORDER BY s.name
