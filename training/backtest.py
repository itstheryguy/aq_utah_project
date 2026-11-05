"""Backtest on a gold snapshot: train on everything before a cutoff, then score the year after it.

    databricks fs cp dbfs:/Volumes/workspace/aq_dev/exports/gold_features.parquet data/
    .venv/bin/python -m training.backtest lightgbm
    .venv/bin/python -m training.backtest gru
    .venv/bin/python -m training.backtest score       # compare every model trained so far, plus baselines

A full year of test data covers one winter of inversions and one summer of smoke, so the numbers are
reported by season, not just overall. Each model's test predictions are saved to
data/backtest/<cutoff>/preds_<model>.parquet, so models train one at a time and are scored together.

One model per process, on purpose: on macOS, LightGBM and PyTorch each load their own OpenMP runtime, and
running torch CPU ops with both loaded segfaults. (Linux, where the Databricks job runs, isn't affected.)
"""

import argparse
import time
from pathlib import Path

import pandas as pd

from aq_models.common import KEYS, baselines, evaluate, time_split


def train(name, df, train_df, test_df, out):
    started = time.time()
    if name == "lightgbm":
        from aq_models.lgbm import LightGBMForecaster
        model = LightGBMForecaster().fit(train_df)
        print(f"lightgbm: {time.time() - started:.0f}s, rounds per horizon {model.best_iterations}")
        print(model.feature_importance().round(3).to_string())
        preds = model.predict(test_df)
    elif name == "gru":
        from aq_models.gru import GRUForecaster
        model = GRUForecaster().fit(train_df)
        print(f"gru: {time.time() - started:.0f}s, best epoch {model.epochs_trained}")
        # The GRU needs the 72 hours before each test row, so it also gets the end of the training period.
        context = df[(df.hour_utc >= test_df.hour_utc.min() - pd.Timedelta(hours=72)) & (df.hour_utc <= test_df.hour_utc.max())]
        preds = model.predict(context).loc[test_df.index]
    else:
        raise SystemExit(f"unknown model {name}")
    model.save(out / name)
    pd.concat([test_df[KEYS], preds], axis=1).to_parquet(out / f"preds_{name}.parquet", index=False)


def score(test_df, out):
    predictions = baselines(test_df)
    for path in sorted(out.glob("preds_*.parquet")):
        saved = test_df[KEYS].merge(pd.read_parquet(path), on=KEYS, how="left")
        saved.index = test_df.index
        predictions[path.stem.removeprefix("preds_")] = saved.drop(columns=KEYS)
    metrics = evaluate(test_df, predictions)
    metrics.to_csv(out / "metrics.csv", index=False)
    for subset in metrics.subset.unique():
        rows = metrics[metrics.subset == subset]
        print(f"\nMAE (µg/m³), {subset}, n per horizon {rows.groupby('horizon_h').n.max().to_dict()}")
        print(rows.pivot(index="model", columns="horizon_h", values="mae").round(2).to_string())


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("step", choices=["lightgbm", "gru", "score"])
    parser.add_argument("--data", default="data/gold_features.parquet")
    parser.add_argument("--cutoff", default="2025-11-01")
    parser.add_argument("--test-days", type=int, default=365)
    args = parser.parse_args()

    df = pd.read_parquet(args.data)
    train_df, test_df = time_split(df, args.cutoff, args.test_days)
    print(f"train {train_df.hour_utc.min():%Y-%m-%d} → {train_df.hour_utc.max():%Y-%m-%d} ({len(train_df):,} rows), "
          f"test {test_df.hour_utc.min():%Y-%m-%d} → {test_df.hour_utc.max():%Y-%m-%d} ({len(test_df):,} rows)")
    out = Path("data/backtest") / args.cutoff
    out.mkdir(parents=True, exist_ok=True)
    if args.step == "score":
        score(test_df, out)
    else:
        train(args.step, df, train_df, test_df, out)


if __name__ == "__main__":
    main()
