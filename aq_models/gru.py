"""GRU, trained from scratch: the last 72 hours as a sequence, plus the forecast weather, with three outputs.

    last 72 h of [PM2.5, missing flag, NO₂, regional PM2.5, hour of day] ──► GRU ──┐
    weather forecasts for each horizon, calendar, rolling stats (standardized) ────┼──► MLP ──► Δ for t+24/48/72
    station embedding ─────────────────────────────────────────────────────────────┘

The network predicts the *change* from the current reading, not the level. Persistence is then the starting
point, and the network only has to learn when PM2.5 will move away from it.

The sequence is read from consecutive gold rows. Gold has a row for every station-hour, so 72 rows are
exactly 72 hours. A row gets a prediction only if the 71 rows before it are the same station's previous hours.
"""

import json
from pathlib import Path

import numpy as np
import pandas as pd
import torch
from torch import nn

from .common import HORIZONS, KEYS, LABELS, PREDS, feature_columns

SEQ_LEN = 72
SEQ_COLS = ["pm25_ugm3", "no2_ppb", "region_pm25"]
SCALE = 10.0  # µg/m³ per unit of network output
VALID_DAYS = 28
CONFIG = {"hidden": 64, "head": 128, "dropout": 0.1, "embedding": 8, "lr": 2e-3, "batch": 1024,
          "max_epochs": 20, "patience": 3, "seed": 7}


class Net(nn.Module):
    def __init__(self, n_seq, n_static, n_stations, cfg):
        super().__init__()
        self.embedding = nn.Embedding(n_stations + 1, cfg["embedding"])  # last index: a station not seen in training
        self.gru = nn.GRU(n_seq, cfg["hidden"], batch_first=True)
        self.head = nn.Sequential(
            nn.Linear(cfg["hidden"] + n_static + cfg["embedding"], cfg["head"]), nn.ReLU(), nn.Dropout(cfg["dropout"]),
            nn.Linear(cfg["head"], len(HORIZONS)),
        )

    def forward(self, seq, static, station):
        _, h = self.gru(seq)
        return self.head(torch.cat([h[-1], static, self.embedding(station)], dim=1))


def _device():
    return torch.device("mps" if torch.backends.mps.is_available() else "cuda" if torch.cuda.is_available() else "cpu")


class GRUForecaster:
    def __init__(self, cfg=None):
        self.cfg = {**CONFIG, **(cfg or {})}
        self.net = None
        self.static_cols = None
        self.mean = self.std = None
        self.stations = None
        self.epochs_trained = 0

    # --- inputs -------------------------------------------------------------------------------------------------

    def _arrays(self, df):
        """Sort by station and hour, then build the per-hour sequence channels, static features, and anchors."""
        df = df.sort_values(KEYS)
        log = lambda c: np.log1p(df[c].clip(lower=0).to_numpy(dtype="float32"))  # noqa: E731
        pm25 = df["pm25_ugm3"].to_numpy(dtype="float32")
        hour = df["hour_local"].to_numpy(dtype="float32") * (2 * np.pi / 24)
        seq = np.stack([log("pm25_ugm3"), np.isnan(pm25).astype("float32"), log("no2_ppb"), log("region_pm25"),
                        np.sin(hour), np.cos(hour)], axis=1)
        seq = np.nan_to_num(seq)

        static = df[self.static_cols].astype("float32").to_numpy()
        static = np.nan_to_num((static - self.mean) / self.std)  # missing → the training mean
        index = {s: i for i, s in enumerate(self.stations)}
        station = df["station_id"].map(index).fillna(len(self.stations)).to_numpy(dtype="int64")

        anchor = df["pm25_ugm3"].fillna(df["pm25_mean_24h"]).to_numpy(dtype="float32") / SCALE
        start = df.groupby("station_id")["hour_utc"].shift(SEQ_LEN - 1)
        usable = ((df["hour_utc"] - start) == pd.Timedelta(hours=SEQ_LEN - 1)).to_numpy() & ~np.isnan(anchor)
        return df, seq, static, station, anchor, usable

    def _static_columns(self, df):
        cols = [c for c in feature_columns(df) if c not in ["station_id", *SEQ_COLS]]
        return [c for c in cols if pd.api.types.is_numeric_dtype(df[c]) or pd.api.types.is_bool_dtype(df[c])]

    @staticmethod
    def _windows(seq, idx):
        return seq[idx[:, None] + np.arange(-(SEQ_LEN - 1), 1)]

    def _batches(self, arrays, idx, targets=None, shuffle=False):
        _, seq, static, station, anchor, _ = arrays
        if shuffle:
            idx = np.random.permutation(idx)
        dev = _device()
        for i in range(0, len(idx), self.cfg["batch"]):
            b = idx[i:i + self.cfg["batch"]]
            out = [torch.from_numpy(self._windows(seq, b)).to(dev), torch.from_numpy(static[b]).to(dev),
                   torch.from_numpy(station[b]).to(dev), torch.from_numpy(anchor[b]).to(dev)]
            if targets is not None:
                out.append(torch.from_numpy(targets[b]).to(dev))
            yield out

    def _forward(self, seq, static, station, anchor):
        return anchor[:, None] + self.net(seq, static, station)

    # --- training -----------------------------------------------------------------------------------------------

    def fit(self, train, log=print):
        torch.manual_seed(self.cfg["seed"])
        np.random.seed(self.cfg["seed"])
        self.stations = sorted(train["station_id"].unique())
        self.static_cols = self._static_columns(train)
        values = train[self.static_cols].astype("float32")
        self.mean = values.mean().to_numpy(dtype="float32")
        self.std = values.std().replace(0, 1).fillna(1).to_numpy(dtype="float32")

        arrays = self._arrays(train)
        df, usable = arrays[0], arrays[-1]
        targets = (df[LABELS].to_numpy(dtype="float32") / SCALE)
        has_label = ~np.isnan(targets).all(axis=1)
        split = df["hour_utc"].max() - pd.Timedelta(days=VALID_DAYS)
        late = (df["hour_utc"] >= split).to_numpy()
        fit_idx = np.flatnonzero(usable & has_label & ~late)
        valid_idx = np.flatnonzero(usable & has_label & late)

        dev = _device()
        self.net = Net(arrays[1].shape[1], arrays[2].shape[1], len(self.stations), self.cfg).to(dev)
        opt = torch.optim.AdamW(self.net.parameters(), lr=self.cfg["lr"], weight_decay=1e-4)
        best, best_state, waited = np.inf, None, 0
        log(f"GRU: {len(fit_idx):,} training sequences, {len(valid_idx):,} validation, device {dev}")
        for epoch in range(1, self.cfg["max_epochs"] + 1):
            self.net.train()
            total = 0.0
            for seq, static, station, anchor, y in self._batches(arrays, fit_idx, targets, shuffle=True):
                pred = self._forward(seq, static, station, anchor)
                mask = ~torch.isnan(y)
                loss = nn.functional.smooth_l1_loss(pred[mask], y[mask], beta=0.5)
                opt.zero_grad()
                loss.backward()
                nn.utils.clip_grad_norm_(self.net.parameters(), 1.0)
                opt.step()
                total += loss.item() * len(y)
            mae = self._mae(arrays, valid_idx, targets)
            log(f"  epoch {epoch:2d}: train loss {total / len(fit_idx):.4f}, validation MAE {mae:.3f} µg/m³")
            if mae < best - 1e-3:
                best, waited = mae, 0
                best_state = {k: v.detach().cpu().clone() for k, v in self.net.state_dict().items()}
                self.epochs_trained = epoch
            else:
                waited += 1
                if waited >= self.cfg["patience"]:
                    break
        self.net.load_state_dict(best_state)
        return self

    @torch.no_grad()
    def _mae(self, arrays, idx, targets):
        self.net.eval()
        errs = []
        for seq, static, station, anchor, y in self._batches(arrays, idx, targets):
            e = (self._forward(seq, static, station, anchor) - y) * SCALE
            errs.append(e[~torch.isnan(e)].abs().cpu())
        return torch.cat(errs).mean().item()

    # --- inference ----------------------------------------------------------------------------------------------

    @torch.no_grad()
    def predict(self, df):
        """Predictions aligned to `df`'s index. NaN for rows without 72 consecutive hours of history in `df`."""
        self.net.eval()
        arrays = self._arrays(df)
        sorted_df, usable = arrays[0], arrays[-1]
        idx = np.flatnonzero(usable)
        out = np.full((len(sorted_df), len(HORIZONS)), np.nan, dtype="float32")
        preds = [self._forward(*b).cpu().numpy() for b in self._batches(arrays, idx)]
        if preds:
            out[idx] = np.concatenate(preds) * SCALE
        return pd.DataFrame(out, index=sorted_df.index, columns=PREDS).reindex(df.index)

    def save(self, path):
        path = Path(path)
        path.mkdir(parents=True, exist_ok=True)
        torch.save(self.net.state_dict(), path / "net.pt")
        (path / "meta.json").write_text(json.dumps({
            "cfg": self.cfg, "static_cols": self.static_cols, "mean": self.mean.tolist(), "std": self.std.tolist(),
            "stations": self.stations, "epochs_trained": self.epochs_trained, "n_seq": 6,
        }, indent=2))

    @classmethod
    def load(cls, path):
        path = Path(path)
        meta = json.loads((path / "meta.json").read_text())
        m = cls(meta["cfg"])
        m.static_cols, m.stations, m.epochs_trained = meta["static_cols"], meta["stations"], meta["epochs_trained"]
        m.mean, m.std = np.array(meta["mean"], dtype="float32"), np.array(meta["std"], dtype="float32")
        m.net = Net(meta["n_seq"], len(m.static_cols), len(m.stations), m.cfg).to(_device())
        m.net.load_state_dict(torch.load(path / "net.pt", map_location=_device()))
        return m
