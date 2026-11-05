"""What every model shares: which gold columns are features, how to split by time, baselines, and metrics.

Each model takes a pandas DataFrame of `gold_features` rows and returns one prediction column per horizon,
`pred_t24`, `pred_t48`, `pred_t72`, aligned to the input rows.
"""

import numpy as np
import pandas as pd

HORIZONS = [24, 48, 72]
LABELS = [f"pm25_t{h}" for h in HORIZONS]
PREDS = [f"pred_t{h}" for h in HORIZONS]
KEYS = ["station_id", "hour_utc"]
MAX_HORIZON = pd.Timedelta(hours=max(HORIZONS))

# AQI breakpoints for PM2.5 (EPA, 2024). Applied here to hourly values, so only a rough guide to the daily index.
UNHEALTHY_SENSITIVE = 35.5


def feature_columns(df):
    """Every gold column except the keys and labels. `station_id` goes back in as a categorical feature."""
    return [c for c in df.columns if c not in KEYS and c not in LABELS]


def time_split(df, cutoff, test_days=None):
    """Train on rows whose labels all fall before `cutoff`, test on rows from `cutoff` on.

    A row at hour t has labels up to t + 72 h. Training rows within 72 hours of the cutoff would have labels
    inside the test period, so they're dropped (a purge gap). A random split would leak far worse: neighbouring
    hours are nearly identical, so the test set would be mostly copies of training rows.
    """
    cutoff = pd.Timestamp(cutoff)
    train = df[df["hour_utc"] < cutoff - MAX_HORIZON]
    test = df[df["hour_utc"] >= cutoff]
    if test_days:
        test = test[test["hour_utc"] < cutoff + pd.Timedelta(days=test_days)]
    return train, test


def baselines(df):
    """Persistence (the latest reading, for every horizon) and the trailing 24-hour mean."""
    return {
        "persistence": pd.DataFrame({p: df["pm25_ugm3"] for p in PREDS}, index=df.index),
        "mean_24h": pd.DataFrame({p: df["pm25_mean_24h"] for p in PREDS}, index=df.index),
    }


def season(ts):
    return pd.Series(np.select(
        [ts.dt.month.isin([12, 1, 2]), ts.dt.month.isin([6, 7, 8])], ["winter", "summer"], "spring_fall"
    ), index=ts.index)


def evaluate(df, predictions):
    """MAE / RMSE per model, horizon and subset.

    Every model is scored on the same rows: label present and a current reading present (so persistence is
    defined). Subsets: all stations, Wasatch Front, by season, and hours at or above the "unhealthy for
    sensitive groups" level, where a forecast matters most.
    """
    rows = []
    seasons = season(df["hour_utc"])
    for h, label, pred in zip(HORIZONS, LABELS, PREDS):
        base = df[label].notna() & df["pm25_ugm3"].notna()
        subsets = {
            "all": base,
            "wasatch_front": base & df["wasatch_front"].astype(bool),
            "winter": base & (seasons == "winter"),
            "summer": base & (seasons == "summer"),
            f">= {UNHEALTHY_SENSITIVE}": base & (df[label] >= UNHEALTHY_SENSITIVE),
        }
        for model, p in predictions.items():
            for subset, mask in subsets.items():
                mask = mask & p[pred].notna()
                err = p.loc[mask, pred] - df.loc[mask, label]
                rows.append({"model": model, "horizon_h": h, "subset": subset, "n": int(mask.sum()),
                             "mae": float(err.abs().mean()), "rmse": float(np.sqrt((err ** 2).mean())),
                             "bias": float(err.mean())})
    return pd.DataFrame(rows)
