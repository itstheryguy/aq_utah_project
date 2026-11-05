"""LightGBM: one gradient-boosted tree model per horizon, on the gold features as they are.

Trees handle NULLs, mixed scales, and interactions (an inversion matters more in January than in July)
without any preprocessing, so the features go in unchanged. `station_id` is a categorical feature with a
fixed category list, so codes are the same at training and serving time.
"""

import json
from pathlib import Path

import lightgbm as lgb
import pandas as pd

from .common import HORIZONS, LABELS, PREDS, feature_columns

# Tweedie suits a skewed, non-negative target. In the 2025-11 → 2026-10 backtest it beat squared error
# (lower MAE at every horizon, bias near zero) and Huber at 48/72 h and in winter. No objective fixed the
# spikes: every variant underpredicts hours ≥ 35.5 µg/m³ by ~33 (see docs/design.md §11).
PARAMS = {
    "objective": "tweedie",
    "tweedie_variance_power": 1.3,
    "learning_rate": 0.05,
    "num_leaves": 63,
    "min_data_in_leaf": 100,
    "feature_fraction": 0.8,
    "bagging_fraction": 0.8,
    "bagging_freq": 1,
    "lambda_l2": 1.0,
    "verbose": -1,
}
VALID_DAYS = 28


class LightGBMForecaster:
    def __init__(self, params=None):
        self.params = {**PARAMS, **(params or {})}
        self.boosters = {}
        self.features = None
        self.stations = None
        self.best_iterations = {}

    def _matrix(self, df):
        X = df[self.features].copy()
        X["station_id"] = pd.Categorical(X["station_id"], categories=self.stations)
        for c in X.columns:
            if X[c].dtype == bool or str(X[c].dtype) == "boolean":
                X[c] = X[c].astype("float32")
        return X

    def fit(self, train):
        """Early-stop on the last 28 days of the training period, then refit on all of it with that many rounds.

        The validation window comes from the end of training, never from the test period, so the test set is
        only ever used once, for the final score.
        """
        self.features = ["station_id"] + feature_columns(train)
        self.stations = sorted(train["station_id"].unique())
        split = train["hour_utc"].max() - pd.Timedelta(days=VALID_DAYS)
        for h, label in zip(HORIZONS, LABELS):
            # Readings can be slightly negative (instrument noise near zero); Tweedie needs labels ≥ 0.
            rows = train[train[label].notna()].assign(**{label: lambda d: d[label].clip(lower=0)})
            fit, valid = rows[rows["hour_utc"] < split], rows[rows["hour_utc"] >= split]
            probe = lgb.train(
                self.params, lgb.Dataset(self._matrix(fit), fit[label]), num_boost_round=3000,
                valid_sets=[lgb.Dataset(self._matrix(valid), valid[label])],
                callbacks=[lgb.early_stopping(100, verbose=False)],
            )
            self.best_iterations[h] = probe.best_iteration
            self.boosters[h] = lgb.train(self.params, lgb.Dataset(self._matrix(rows), rows[label]),
                                         num_boost_round=probe.best_iteration)
        return self

    def predict(self, df):
        X = self._matrix(df)
        return pd.DataFrame({p: self.boosters[h].predict(X) for h, p in zip(HORIZONS, PREDS)}, index=df.index)

    def feature_importance(self, top=15):
        gain = pd.DataFrame({h: b.feature_importance("gain") for h, b in self.boosters.items()}, index=self.features)
        return (gain / gain.sum()).sort_values(HORIZONS[0], ascending=False).head(top)

    def save(self, path):
        path = Path(path)
        path.mkdir(parents=True, exist_ok=True)
        for h, b in self.boosters.items():
            b.save_model(str(path / f"t{h}.txt"))
        (path / "meta.json").write_text(json.dumps({
            "params": self.params, "features": self.features, "stations": self.stations,
            "best_iterations": self.best_iterations,
        }, indent=2))

    @classmethod
    def load(cls, path):
        path = Path(path)
        meta = json.loads((path / "meta.json").read_text())
        m = cls(meta["params"])
        m.features, m.stations, m.best_iterations = meta["features"], meta["stations"], {int(k): v for k, v in meta["best_iterations"].items()}
        m.boosters = {h: lgb.Booster(model_file=str(path / f"t{h}.txt")) for h in HORIZONS}
        return m
