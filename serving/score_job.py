"""Batch inference and export: the last task of each pipeline run.

    score_job.py --catalog workspace --schema aq --run-id <job run id> --hf-dataset itstheryguy/aq-utah-forecasts

1. Load `@champion` of each model family from Unity Catalog, and score each station's newest hour in
   `gold_features`. Persistence is scored alongside, as the baseline the app shows next to the models.
2. MERGE the forecasts into `predictions`, one row per (station, as-of hour, model, horizon).
3. Live accuracy: join past predictions with the readings that have since arrived.
4. Export small CSV files for the app (forecasts, recent history, accuracy, stations, DAQ's own forecast)
   and push them to the public Hugging Face dataset as one commit. The Space reads them from there.

Batch, not online: every possible request is "a station's next 72 hours", so scoring every station each run
covers them all (design doc §9). The app needs no model and no ML libraries.
"""

import argparse
import json
import os
import platform
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path


def _repo_root():
    # Serverless runs the script through exec(), where __file__ isn't defined; argv[0] is the script path.
    script = globals().get("__file__") or (sys.argv[0] if sys.argv[0].endswith(".py") else None)
    return Path(script).resolve().parents[1] if script else Path(os.getcwd()).resolve().parent


ROOT = _repo_root()
sys.path.insert(0, str(ROOT))
print(f"Python {platform.python_version()} on {platform.machine()}, repo root {ROOT}")

import lightgbm  # noqa: E402,F401  (before torch; see training/backtest.py)
import mlflow  # noqa: E402
import pandas as pd  # noqa: E402
from mlflow import MlflowClient  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

from aq_models.common import HORIZONS, KEYS, PREDS  # noqa: E402

FAMILIES = ["lightgbm", "gru", "linear"]
CONTEXT_HOURS = 80   # the GRU needs 72 consecutive hours before the hour it scores
HISTORY_DAYS = 7     # PM2.5 history shown in the app
STALE_HOURS = 6      # a station whose newest reading is older than this is still scored, but flagged


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--catalog", default="workspace")
    parser.add_argument("--schema", default="aq")
    parser.add_argument("--run-id", default="manual")
    parser.add_argument("--hf-dataset", default="", help="Hugging Face dataset repo to export to; empty = skip")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    fq = lambda t: f"{args.catalog}.{args.schema}.{t}"  # noqa: E731
    mlflow.set_registry_uri("databricks-uc")
    client = MlflowClient()
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    # --- inputs: each station's newest hour with a reading, plus the hours before it ---------------------------
    gold = spark.table(fq("gold_features")).drop("row_hash", "updated_at")
    newest = gold.where("pm25_ugm3 IS NOT NULL").groupBy("station_id").agg({"hour_utc": "max"}).withColumnRenamed("max(hour_utc)", "as_of")
    window = gold.join(newest, "station_id").where(f"hour_utc > as_of - INTERVAL {CONTEXT_HOURS} HOURS AND hour_utc <= as_of")
    pdf = window.toPandas()
    pdf.attrs = {}
    latest = pdf[pdf.hour_utc == pdf.as_of].drop(columns="as_of").set_index(KEYS)
    print(f"Scoring {len(latest)} stations, as-of hours {latest.index.get_level_values(1).min()} → {latest.index.get_level_values(1).max()}")

    # --- score ----------------------------------------------------------------------------------------------
    forecasts = [pd.DataFrame({"model": "persistence", "model_version": "0",
                               **{p: latest["pm25_ugm3"] for p in PREDS}}, index=latest.index)]
    versions = {}
    for family in FAMILIES:
        name = fq(f"pm25_{family}")
        try:
            version = client.get_model_version_by_alias(name, "champion").version
        except mlflow.exceptions.MlflowException:
            print(f"{name} has no @champion yet, skipping")
            continue
        model = mlflow.pyfunc.load_model(f"models:/{name}@champion")
        cols = model.metadata.get_input_schema().input_names()  # what this version was trained on
        out = model.predict(pdf[cols]).set_index(KEYS).reindex(latest.index)
        forecasts.append(out.assign(model=family, model_version=str(version)))
        versions[family] = version

    wide = pd.concat(forecasts).reset_index()
    long = wide.melt(id_vars=KEYS + ["model", "model_version"], value_vars=PREDS, var_name="pred", value_name="predicted_pm25")
    long["horizon_h"] = long.pop("pred").str.removeprefix("pred_t").astype(int)
    long["target_hour_utc"] = long["hour_utc"] + pd.to_timedelta(long["horizon_h"], unit="h")
    long["predicted_pm25"] = long["predicted_pm25"].clip(lower=0)
    long["input_age_h"] = (now - long["hour_utc"]).dt.total_seconds() / 3600
    long["scored_at"] = now
    long["run_id"] = args.run_id

    # Hard check: every model must forecast every station with a fresh reading.
    fresh = long[long.input_age_h <= STALE_HOURS]
    missing = fresh[fresh.predicted_pm25.isna()]
    if len(missing):
        raise RuntimeError(f"{len(missing)} forecasts missing for fresh stations:\n{missing[['station_id', 'model', 'horizon_h']].head(20)}")

    # --- predictions table ----------------------------------------------------------------------------------
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {fq('predictions')} (
            station_id STRING, hour_utc TIMESTAMP, model STRING, horizon_h INT, model_version STRING,
            target_hour_utc TIMESTAMP, predicted_pm25 DOUBLE, input_age_h DOUBLE, scored_at TIMESTAMP, run_id STRING
        ) CLUSTER BY (station_id, hour_utc)
        COMMENT 'One forecast per station, as-of hour (newest reading), model, and horizon. Scored by serving/score_job.py.'
    """)
    spark.createDataFrame(long[["station_id", "hour_utc", "model", "horizon_h", "model_version", "target_hour_utc",
                                "predicted_pm25", "input_age_h", "scored_at", "run_id"]]).selectExpr(
        "station_id", "CAST(hour_utc AS TIMESTAMP) AS hour_utc", "model", "CAST(horizon_h AS INT) AS horizon_h",
        "model_version", "CAST(target_hour_utc AS TIMESTAMP) AS target_hour_utc", "CAST(predicted_pm25 AS DOUBLE) AS predicted_pm25",
        "input_age_h", "CAST(scored_at AS TIMESTAMP) AS scored_at", "run_id",
    ).createOrReplaceTempView("new_predictions")
    # A rerun for the same as-of hour replaces the forecast only if a model or its version changed it.
    result = spark.sql(f"""
        MERGE INTO {fq('predictions')} t USING new_predictions s
        ON t.station_id = s.station_id AND t.hour_utc = s.hour_utc AND t.model = s.model AND t.horizon_h = s.horizon_h
        WHEN MATCHED AND (t.model_version <> s.model_version OR NOT (t.predicted_pm25 <=> s.predicted_pm25)) THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """).first()
    print(f"predictions: {result['num_inserted_rows']} inserted, {result['num_updated_rows']} updated")

    # --- live accuracy --------------------------------------------------------------------------------------
    accuracy = spark.sql(f"""
        WITH scored AS (
            SELECT p.model, p.horizon_h, s.wasatch_front, p.target_hour_utc,
                   abs(p.predicted_pm25 - r.pm25_ugm3) AS abs_err, p.predicted_pm25 - r.pm25_ugm3 AS err
            FROM {fq('predictions')} p
            JOIN {fq('silver_readings_best')} r ON r.station_id = p.station_id AND r.hour_utc = p.target_hour_utc
            JOIN {fq('silver_station')} s ON s.station_id = p.station_id
            WHERE r.pm25_ugm3 IS NOT NULL AND NOT r.pm25_qa_removed AND p.input_age_h <= {STALE_HOURS}
        )
        SELECT model, horizon_h, w.days AS window_days, region,
               count(*) AS n, round(avg(abs_err), 3) AS mae, round(avg(err), 3) AS bias
        FROM scored
        CROSS JOIN (SELECT explode(array(7, 30)) AS days) w
        CROSS JOIN (SELECT explode(array('all', 'wasatch_front')) AS region) g
        WHERE (g.region = 'all' OR wasatch_front)
          AND target_hour_utc >= current_timestamp() - make_interval(0, 0, 0, w.days)
        GROUP BY model, horizon_h, w.days, g.region
        ORDER BY region, window_days, horizon_h, model
    """).toPandas()
    print(accuracy.to_string() if len(accuracy) else "No live accuracy yet: no target hour has passed.")

    metrics = [("stations_scored", len(latest)), ("stations_stale", int((latest.index.get_level_values(1) < now - pd.Timedelta(hours=STALE_HOURS)).sum())),
               ("forecasts", len(long)), *[(f"champion_{f}", float(v)) for f, v in versions.items()]]
    metrics += [(f"live_mae_7d_t{r.horizon_h}_{r.model}", r.mae) for r in accuracy.itertuples() if r.window_days == 7 and r.region == "all"]
    spark.createDataFrame([(args.run_id, "score", m, float(v)) for m, v in metrics],
                          "run_id STRING, source STRING, metric STRING, value DOUBLE") \
        .selectExpr("run_id", "current_timestamp() AS ts", "'serve' AS task", "source", "metric", "value") \
        .write.mode("append").saveAsTable(fq("ops_metrics"))

    if not args.hf_dataset:
        print("No --hf-dataset, skipping export")
        return
    export(spark, fq, args, long, accuracy, versions, now)


def export(spark, fq, args, forecasts, accuracy, versions, now):
    """Small CSV files: the app runs in the browser (stlite), where CSV needs no extra packages to read."""
    stations = spark.table(fq("silver_station")).where("has_pm25").select(
        "station_id", "name", "latitude", "longitude", "county", "daq_forecast_area", "wasatch_front").toPandas()
    history = spark.sql(f"""
        SELECT station_id, hour_utc, round(pm25_ugm3, 1) AS pm25_ugm3
        FROM {fq('silver_readings_best')}
        WHERE hour_utc >= current_timestamp() - INTERVAL {HISTORY_DAYS} DAYS AND pm25_ugm3 IS NOT NULL
        ORDER BY station_id, hour_utc
    """).toPandas()
    # DAQ's own forecast, newest issue per area and day: shown next to the models for comparison.
    daq = spark.sql(f"""
        SELECT daq_forecast_area, forecast_date, issued_date, severity, action
        FROM {fq('silver_daq_forecast')}
        WHERE forecast_date >= current_date() - INTERVAL 1 DAY
        QUALIFY row_number() OVER (PARTITION BY daq_forecast_area, forecast_date ORDER BY issued_date DESC) = 1
        ORDER BY daq_forecast_area, forecast_date
    """).toPandas()
    out = forecasts[["station_id", "hour_utc", "model", "model_version", "horizon_h", "target_hour_utc", "predicted_pm25", "input_age_h"]]
    out = out.assign(predicted_pm25=out.predicted_pm25.round(1), input_age_h=out.input_age_h.round(1))
    meta = {
        "as_of_utc": str(forecasts.hour_utc.max()), "generated_at_utc": now.isoformat(timespec="seconds"),
        "run_id": args.run_id, "champions": {f: str(v) for f, v in versions.items()},
        "units": "PM2.5 in µg/m³, hourly averages. Times in UTC; hour_utc is the start of the hour.",
    }

    from huggingface_hub import HfApi

    try:
        from databricks.sdk.runtime import dbutils
        token = dbutils.secrets.get("aq", "hf-token")
    except Exception:  # outside a notebook-like runtime, read it through the SDK instead
        import base64
        from databricks.sdk import WorkspaceClient
        token = base64.b64decode(WorkspaceClient().secrets.get_secret("aq", "hf-token").value).decode()

    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        for name, df in {"forecasts": out, "history": history, "accuracy": accuracy, "stations": stations, "daq_forecast": daq}.items():
            df.to_csv(tmp / f"{name}.csv", index=False)
        (tmp / "meta.json").write_text(json.dumps(meta, indent=2))
        api = HfApi(token=token)
        commit = api.upload_folder(repo_id=args.hf_dataset, repo_type="dataset", folder_path=str(tmp), path_in_repo="latest",
                                   commit_message=f"Forecasts as of {meta['as_of_utc']} UTC (run {args.run_id})")
        print("Exported to", commit.commit_url)


if __name__ == "__main__":
    main()
