# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze: pull the live sources
# MAGIC
# MAGIC | Source | Request | Calls per run | `source` value |
# MAGIC |---|---|---|---|
# MAGIC | Utah DAQ | `xmlFeed.php`: all 28 sites, ~10 days of hourly history | 1 | `daq_xml` |
# MAGIC | Utah DAQ | `csvFeed.php?id=forecast`: 3-day category + action per area | 1 | `daq_forecast` |
# MAGIC | Open-Meteo | Hourly forecast, next 4 days, every station + the ridge reference point in one call | 1 | `openmeteo_forecast` |
# MAGIC | OpenAQ | Utah locations (to discover sensors) | 1 | `openaq_locations` |
# MAGIC | OpenAQ | Last 24 hours for each active low-cost PM2.5 sensor | ~40 | `openaq_sensor_hours` |
# MAGIC
# MAGIC Every response is appended to `bronze_raw` exactly as received, failures included, so a bad run is visible
# MAGIC in the table rather than missing from it. Parsing happens in silver.
# MAGIC
# MAGIC **Overlapping windows make missed runs harmless.** DAQ returns ~10 days per call and OpenAQ is asked for 24
# MAGIC hours, so if a run fails, the next successful run picks up the gap. Silver MERGEs on (station, hour), so
# MAGIC the overlap never creates duplicates.
# MAGIC
# MAGIC **OpenAQ only covers low-cost sensors (AirGradient, Clarity).** OpenAQ also republishes the DAQ monitors
# MAGIC through AirNow, but DAQ is pulled directly, with one less hop.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "aq", "Schema")
dbutils.widgets.text("run_id", "manual", "Run ID")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
run_id = dbutils.widgets.get("run_id")

spark.sql(f"USE {catalog}.{schema}")

# COMMAND ----------

import json
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Retry transient failures (rate limits, 5xx) with backoff: 2s, 4s, 8s.
session = requests.Session()
session.mount(
    "https://",
    HTTPAdapter(max_retries=Retry(total=3, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])),
)

records = []  # rows for bronze_raw
metrics = []  # rows for ops_metrics


def fetch(source, url, params=None, headers=None):
    """GET url and record the response in `records`, success or not. Returns the response, or None on failure."""
    started = time.monotonic()
    resp, error = None, None
    try:
        resp = session.get(url, params=params, headers=headers, timeout=60)
    except requests.RequestException as e:
        error = f"{type(e).__name__}: {e}"
    records.append({
        "source": source,
        "request": resp.url if resp is not None else requests.Request("GET", url, params=params).prepare().url,
        "fetched_at": datetime.now(timezone.utc),
        "http_status": resp.status_code if resp is not None else None,
        "body": resp.text if resp is not None else None,
        "error": error,
        "run_id": run_id,
    })
    metrics.append((source, "latency_ms", (time.monotonic() - started) * 1000))
    return resp if resp is not None and resp.ok else None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Station reference
# MAGIC `reference/stations.csv` maps each DAQ site name to its OpenAQ location and coordinates (the DAQ feed has
# MAGIC no coordinates). The mapping was checked by comparing 48 hours of PM2.5 from both sources: every match
# MAGIC agreed within ~0.05 µg/m³ on average, against >1 µg/m³ for the next-closest station.
# MAGIC
# MAGIC `ref_hidden_peak` is a ridge-top point, not a monitor. Valley temperature minus ridge-top temperature
# MAGIC measures inversion strength, using only surface forecasts, which Open-Meteo archives (see design doc §4).

# COMMAND ----------

stations = pd.read_csv("../reference/stations.csv")
print(f"{len(stations)} rows: {(stations.kind == 'monitor').sum()} monitors, {(stations.kind == 'weather_ref').sum()} weather reference points")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Utah DAQ

# COMMAND ----------

daq = fetch("daq_xml", "https://air.utah.gov/xmlFeed.php")
fetch("daq_forecast", "https://air.utah.gov/csvFeed.php", params={"id": "forecast"})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Open-Meteo
# MAGIC One call covers every location: Open-Meteo accepts comma-separated coordinate lists and returns one result per
# MAGIC location, in the same order.
# MAGIC
# MAGIC `models=gfs_seamless` is set explicitly so the live forecast comes from the same weather model as the
# MAGIC archived forecasts used for training. The default ("best match") can change which model it uses.
# MAGIC `boundary_layer_height` isn't in that archive, so it can't be a training feature yet. It's pulled anyway,
# MAGIC so this table builds its own archive of it from today on.

# COMMAND ----------

OPENMETEO_HOURLY = [
    "temperature_2m", "relative_humidity_2m", "dew_point_2m", "wind_speed_10m", "wind_direction_10m",
    "pressure_msl", "surface_pressure", "precipitation", "cloud_cover", "shortwave_radiation",
    "boundary_layer_height",
]

fetch(
    "openmeteo_forecast",
    "https://api.open-meteo.com/v1/forecast",
    params={
        "latitude": ",".join(stations.latitude.astype(str)),
        "longitude": ",".join(stations.longitude.astype(str)),
        "hourly": ",".join(OPENMETEO_HOURLY),
        "models": "gfs_seamless",
        "forecast_days": 4,
        "timezone": "UTC",
    },
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## OpenAQ
# MAGIC Sensors are discovered each run from the Utah locations list, so new low-cost sensors get picked up without a
# MAGIC code change. A sensor counts as active if it reported in the last 7 days.
# MAGIC
# MAGIC The rate limit is 60 requests per minute. One run makes ~40, so they can go out back to back.

# COMMAND ----------

OPENAQ = "https://api.openaq.org/v3"
UTAH_BBOX = "-114.05,37.0,-109.04,42.0"
openaq_headers = {"X-API-Key": dbutils.secrets.get("aq", "openaq-api-key")}

now = datetime.now(timezone.utc)
locations = fetch("openaq_locations", f"{OPENAQ}/locations", params={"bbox": UTAH_BBOX, "limit": 1000}, headers=openaq_headers)

low_cost_sensors = []
if locations is not None:
    active_since = (now - timedelta(days=7)).isoformat()
    for loc in locations.json()["results"]:
        last = (loc.get("datetimeLast") or {}).get("utc")
        if loc["provider"]["name"] == "AirNow" or not last or last < active_since:
            continue
        low_cost_sensors += [s["id"] for s in loc["sensors"] if s["parameter"]["name"] == "pm25"]

since = (now - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")
for sensor_id in low_cost_sensors:
    fetch(
        "openaq_sensor_hours",
        f"{OPENAQ}/sensors/{sensor_id}/hours",
        params={"datetime_from": since, "limit": 100},
        headers=openaq_headers,
    )

print(f"{len(low_cost_sensors)} active low-cost PM2.5 sensors")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write bronze and run metrics
# MAGIC Both tables are written before the hard checks run, so a failed run still leaves a record of what it got.

# COMMAND ----------

from pyspark.sql import functions as F

bronze_schema = spark.table("bronze_raw").schema
spark.createDataFrame(records, bronze_schema).write.mode("append").saveAsTable("bronze_raw")

by_source = pd.DataFrame(records).groupby("source")
for source, group in by_source:
    ok = group.http_status.between(200, 299)
    metrics += [
        (source, "requests", len(group)),
        (source, "requests_failed", int((~ok).sum())),
        (source, "bytes", float(group.body.fillna("").str.len().sum())),
    ]

ts = datetime.now(timezone.utc)
spark.createDataFrame(
    [(run_id, ts, "bronze", source, metric, float(value)) for source, metric, value in metrics],
    spark.table("ops_metrics").schema,
).write.mode("append").saveAsTable("ops_metrics")

display(
    spark.table("ops_metrics")
    .where(F.col("run_id") == run_id)
    .where(F.col("ts") == ts)
    .groupBy("source").pivot("metric").agg(F.round(F.sum("value"), 1))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hard checks
# MAGIC DAQ is the primary source: without it there's nothing to forecast, so the run fails and the job sends its
# MAGIC failure email. Other sources failing only shows up in `ops_metrics`, since the next run's overlapping window
# MAGIC fills the gap.

# COMMAND ----------

if daq is None or "<site>" not in daq.text:
    raise RuntimeError("DAQ XML pull failed or returned no <site> elements. See bronze_raw for the response.")
