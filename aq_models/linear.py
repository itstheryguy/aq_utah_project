"""Ridge regression: one linear model per horizon, the simplest model that uses weather forecasts.

Two variants, to measure what the extra features buy:

- `small`: five inputs per horizon. The current reading and the 24-hour mean (the persistence signal), plus
  the forecast inversion strength, wind, and precipitation for the 24 hours ending at the target hour.
- `full`: every gold feature, with the station one-hot encoded and hour, day of year, and wind direction as
  sin/cos pairs (a linear model can't use an angle or a station code directly).

PM2.5 is skewed, so the target and the PM2.5 inputs are log1p-transformed: the model is linear in log space,
which makes a weather effect multiplicative (an inversion doubles a high reading's growth, not a low one's).
Missing inputs are filled with the training median, plus an indicator column. Features are standardized, so
the coefficients are comparable: `coefficients()` shows them for the write-up.
"""

import json
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.linear_model import Ridge
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from .common import HORIZONS, LABELS, PREDS, feature_columns

ALPHA = 1.0
SMALL = ["pm25_ugm3", "pm25_mean_24h", "wx{h}_inversion_mean_24h", "wx{h}_wind_mean_24h", "wx{h}_precip_24h_mm"]
CYCLIC = {"hour_local": 24, "doy_local": 366}


def _is_pm(c):
    return c.startswith(("pm25_", "region_pm25")) and c not in ("pm25_obs_24h", "pm25_change_24h", "pm25_std_24h")


class LinearForecaster:
    def __init__(self, variant="small", alpha=ALPHA):
        self.cfg = {"variant": variant, "alpha": alpha}
        self.models = {}
        self.features = {}
        self.stations = None

    def _matrix(self, df, h):
        """The design matrix for horizon h, in a fixed column order."""
        if self.cfg["variant"] == "small":
            cols = [c.format(h=h) for c in SMALL]
            X = df[cols].astype("float64")
        else:
            # A horizon's model sees only its own weather block (wx24_* for t+24), like the small variant.
            cols = [c for c in feature_columns(df)
                    if c != "station_id" and (not c.startswith("wx") or c.startswith(f"wx{h}_"))
                    and (not c.startswith("fireworks_") or c == f"fireworks_t{h}")]
            X = df[cols].astype("float64")
            for c, period in CYCLIC.items():
                X[f"{c}_sin"], X[f"{c}_cos"] = np.sin(2 * np.pi * X[c] / period), np.cos(2 * np.pi * X[c] / period)
            rad = np.deg2rad(X[f"wx{h}_wind_dir_deg"])
            X[f"wx{h}_wind_dir_sin"], X[f"wx{h}_wind_dir_cos"] = np.sin(rad), np.cos(rad)
            X = X.drop(columns=[*CYCLIC, f"wx{h}_wind_dir_deg"])
            stations = pd.Categorical(df["station_id"], categories=self.stations)
            X = pd.concat([X, pd.get_dummies(stations, prefix="station", dtype="float64").set_index(X.index)], axis=1)
        for c in X.columns:
            if _is_pm(c):
                X[c] = np.log1p(X[c].clip(lower=0))
        return X

    def fit(self, train):
        self.stations = sorted(train["station_id"].unique())
        for h, label in zip(HORIZONS, LABELS):
            rows = train[train[label].notna()]
            X = self._matrix(rows, h)
            self.features[h] = list(X.columns)
            model = make_pipeline(SimpleImputer(strategy="median", add_indicator=True, keep_empty_features=True),
                                  StandardScaler(), Ridge(alpha=self.cfg["alpha"]))
            self.models[h] = model.fit(X.to_numpy(), np.log1p(rows[label].clip(lower=0)).to_numpy())
        return self

    def predict(self, df):
        return pd.DataFrame({p: np.expm1(self.models[h].predict(self._matrix(df, h).to_numpy())).clip(min=0)
                             for h, p in zip(HORIZONS, PREDS)}, index=df.index)

    def coefficients(self):
        """Standardized coefficients per horizon (log-space effect of a one-standard-deviation change)."""
        out = {}
        for h, model in self.models.items():
            imputer, ridge = model.named_steps["simpleimputer"], model.named_steps["ridge"]
            names = self.features[h] + [f"{self.features[h][i]}_missing" for i in imputer.indicator_.features_]
            out[h] = pd.Series(ridge.coef_, index=[n.replace(f"wx{h}_", "wx_") for n in names])
        return pd.DataFrame(out).sort_values(HORIZONS[0], key=abs, ascending=False)

    def save(self, path):
        path = Path(path)
        path.mkdir(parents=True, exist_ok=True)
        for h, model in self.models.items():
            joblib.dump(model, path / f"t{h}.joblib")
        (path / "meta.json").write_text(json.dumps({
            "cfg": self.cfg, "stations": self.stations, "features": {str(h): f for h, f in self.features.items()},
        }, indent=2))

    @classmethod
    def load(cls, path):
        path = Path(path)
        meta = json.loads((path / "meta.json").read_text())
        m = cls(**meta["cfg"])
        m.stations, m.features = meta["stations"], {int(h): f for h, f in meta["features"].items()}
        m.models = {h: joblib.load(path / f"t{h}.joblib") for h in HORIZONS}
        return m
