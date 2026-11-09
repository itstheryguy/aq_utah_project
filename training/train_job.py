"""Weekly training: train one model family, register it in Unity Catalog, and promote it if it passes the gate.

Runs as a serverless Python script task in the `aq_weekly_train` job (databricks.yml), once per family:

    train_job.py --model lightgbm --catalog workspace --schema aq --run-id <job run id>

1. Read `gold_features`. Hold out the last 28 days that have labels (the gate window), and train on
   everything before, with the same 72-hour purge gap as the backtest.
2. Log the model to MLflow and register a new version of `<catalog>.<schema>.pm25_<model>`.
3. Reload that version from the registry and score it on the gate window, next to the current `@champion`
   and persistence. Scoring the packaged model, not the object in memory, catches packaging bugs here.
4. Promote (move the `champion` alias) only if the new version's mean MAE beats the champion's **and** it
   beats persistence at every horizon. A version that fails stays registered, tagged with the reason.

Last week's champion was trained a week earlier, so its training data ended before this gate window too.
Both models are scored on data neither has seen.
"""

import argparse
import json
import sys
import tempfile
import time
from pathlib import Path

import os  # noqa: E402
import platform  # noqa: E402


def _repo_root():
    # Serverless runs the script through exec(), where __file__ isn't defined; argv[0] is the script path.
    script = globals().get("__file__") or (sys.argv[0] if sys.argv[0].endswith(".py") else None)
    return Path(script).resolve().parents[1] if script else Path(os.getcwd()).resolve().parent


ROOT = _repo_root()
sys.path.insert(0, str(ROOT))
print(f"Python {platform.python_version()} on {platform.machine()}, repo root {ROOT}")

import mlflow  # noqa: E402
import pandas as pd  # noqa: E402
from mlflow import MlflowClient  # noqa: E402
from mlflow.models import infer_signature  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

from aq_models.common import HORIZONS, KEYS, MAX_HORIZON, PREDS, baselines, evaluate, feature_columns, time_split  # noqa: E402
from aq_models.pyfunc import Forecaster  # noqa: E402

GATE_DAYS = 28
PIP = {
    "lightgbm": ["lightgbm==4.7.0", "pandas", "numpy"],
    "gru": ["torch==2.15.0", "pandas", "numpy"],
    "linear": ["scikit-learn==1.9.1", "pandas", "numpy"],
}


def score(model, df, gate, inputs):
    """Predictions from a pyfunc model, aligned to `gate`. Context starts 72 h early for the GRU's history."""
    context = df.loc[df.hour_utc >= gate.hour_utc.min() - MAX_HORIZON, inputs]
    out = gate[KEYS].merge(model.predict(context), on=KEYS, how="left")
    out.index = gate.index
    return out[PREDS]


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--model", choices=["lightgbm", "gru", "linear"], required=True)
    parser.add_argument("--catalog", default="workspace")
    parser.add_argument("--schema", default="aq")
    parser.add_argument("--run-id", default="manual")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    fq = lambda t: f"{args.catalog}.{args.schema}.{t}"  # noqa: E731
    name = fq(f"pm25_{args.model}")
    user = spark.sql("SELECT current_user()").first()[0]
    mlflow.set_registry_uri("databricks-uc")
    mlflow.set_experiment(f"/Users/{user}/aq_{args.schema}_training")
    client = MlflowClient()

    df = spark.table(fq("gold_features")).drop("row_hash", "updated_at").toPandas()
    df.attrs = {}
    inputs = KEYS + feature_columns(df)
    end = df.hour_utc.max()
    gate_start = end - pd.Timedelta(days=GATE_DAYS) - MAX_HORIZON  # 28 days of rows whose labels are all in
    train, gate = time_split(df, gate_start)
    print(f"{args.model}: train {train.hour_utc.min()} → {train.hour_utc.max()} ({len(train):,} rows), "
          f"gate {gate.hour_utc.min()} → {end} ({len(gate):,} rows)")

    started = time.time()
    if args.model == "lightgbm":
        from aq_models.lgbm import LightGBMForecaster as cls
    elif args.model == "linear":
        from aq_models.linear import LinearForecaster as cls
    else:
        from aq_models.gru import GRUForecaster as cls
    model = cls().fit(train)
    train_seconds = time.time() - started

    with mlflow.start_run(run_name=f"{args.model}-{args.run_id}") as run, tempfile.TemporaryDirectory() as tmp:
        # A named folder, not the temp dir itself: MLflow keeps the folder name inside the logged model.
        model_dir = Path(tmp) / args.model
        model.save(model_dir)
        mlflow.log_params({
            "model": args.model, "train_from": str(train.hour_utc.min()), "train_to": str(train.hour_utc.max()),
            "gate_from": str(gate.hour_utc.min()), "gate_to": str(end), "train_rows": len(train),
            "job_run_id": args.run_id,
            **{f"cfg_{k}": v for k, v in (getattr(model, "params", None) or getattr(model, "cfg", {})).items()},
        })
        mlflow.log_metric("train_seconds", train_seconds)
        if args.model == "lightgbm":
            mlflow.log_dict(model.feature_importance(top=40).round(4).to_dict(), "feature_importance.json")
        if args.model == "linear":
            mlflow.log_dict(model.coefficients().round(4).to_dict(), "coefficients.json")

        sample = df.loc[df.hour_utc >= end - MAX_HORIZON, inputs]
        info = mlflow.pyfunc.log_model(
            name="model",
            python_model=Forecaster(args.model),
            artifacts={"model": str(model_dir)},
            code_paths=[str(ROOT / "aq_models")],
            signature=infer_signature(sample, pd.DataFrame({**{k: sample[k] for k in KEYS}, **{p: 0.0 for p in PREDS}})),
            pip_requirements=PIP[args.model],
            registered_model_name=name,
        )
        version = info.registered_model_version

        predictions = {"challenger": score(mlflow.pyfunc.load_model(f"models:/{name}/{version}"), df, gate, inputs),
                       "persistence": baselines(gate)["persistence"]}
        try:
            champion = client.get_model_version_by_alias(name, "champion")
            predictions["champion"] = score(mlflow.pyfunc.load_model(f"models:/{name}@champion"), df, gate, inputs)
        except mlflow.exceptions.MlflowException:
            champion = None

        # Hard check: the packaged model must predict nearly every gate row that has a current reading.
        has_reading = gate["pm25_ugm3"].notna()
        coverage = predictions["challenger"].loc[has_reading, PREDS[0]].notna().mean()
        if coverage < 0.95:
            raise RuntimeError(f"Packaged {args.model} predicted only {coverage:.1%} of gate rows; not promoting.")

        metrics = evaluate(gate, predictions)
        metrics.to_csv(Path(tmp) / "gate_metrics.csv", index=False)
        mlflow.log_artifact(str(Path(tmp) / "gate_metrics.csv"))
        overall = metrics[metrics.subset == "all"].pivot(index="model", columns="horizon_h", values="mae")
        for m, row in overall.iterrows():
            for h in HORIZONS:
                mlflow.log_metric(f"gate_mae_t{h}_{m}", row[h])
        print(overall.round(3).to_string())

        mean = overall.mean(axis=1)
        beats_persistence = all(overall.loc["challenger", h] < overall.loc["persistence", h] for h in HORIZONS)
        beats_champion = champion is None or mean["challenger"] < mean["champion"]
        promote = bool(beats_persistence and beats_champion)  # numpy.bool_ when a champion exists; json.dumps rejects it
        reason = ("first version" if champion is None else
                  f"mean MAE {mean['challenger']:.3f} vs champion v{champion.version} {mean['champion']:.3f}")
        if not beats_persistence:
            reason = "does not beat persistence at every horizon"
        client.set_model_version_tag(name, version, "gate", "passed" if promote else "failed")
        client.set_model_version_tag(name, version, "gate_reason", reason)
        client.set_model_version_tag(name, version, "gate_window", f"{gate.hour_utc.min()} → {end}")
        if promote:
            client.set_registered_model_alias(name, "champion", version)
        mlflow.set_tags({"gate": "passed" if promote else "failed", "registered_version": version})
        print(f"{name} v{version}: {'PROMOTED to @champion' if promote else 'not promoted'} ({reason})")

        ops = [("train_seconds", train_seconds), ("version", float(version)), ("promoted", float(promote)),
               ("gate_coverage", coverage)]
        ops += [(f"gate_mae_t{h}_{m}", float(row[h])) for m, row in overall.iterrows() for h in HORIZONS]
        spark.createDataFrame(
            [(args.run_id, args.model, k, float(v)) for k, v in ops],
            "run_id STRING, source STRING, metric STRING, value DOUBLE",
        ).selectExpr("run_id", "current_timestamp() AS ts", "'train' AS task", "source", "metric", "value") \
            .write.mode("append").saveAsTable(fq("ops_metrics"))
        print(json.dumps({"run": run.info.run_id, "version": version, "promoted": promote}))


if __name__ == "__main__":
    main()
