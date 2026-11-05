"""MLflow wrapper, so every model family registers and loads the same way.

Input: `gold_features` rows (keys + features). Output: `station_id`, `hour_utc`, `pred_t24/48/72` for every
input row. The GRU returns NaN for rows without 72 consecutive hours of history in the input, so batch
inference passes it the last 72 hours per station.

The model files are logged as an artifact and this package as code (`code_paths`), so a registered version
loads anywhere with `mlflow.pyfunc.load_model("models:/<name>@champion")`.
"""

import mlflow
import pandas as pd

from .common import KEYS


class Forecaster(mlflow.pyfunc.PythonModel):
    def __init__(self, kind):
        self.kind = kind

    def load_context(self, context):
        if self.kind == "lightgbm":
            from .lgbm import LightGBMForecaster as cls
        else:
            from .gru import GRUForecaster as cls
        self.model = cls.load(context.artifacts["model"])

    def predict(self, context, model_input, params=None):
        return pd.concat([model_input[KEYS].reset_index(drop=True),
                          self.model.predict(model_input).reset_index(drop=True)], axis=1)
