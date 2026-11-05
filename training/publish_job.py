"""Publish each family's `@champion` to the Hugging Face model repo, with a generated model card.

The last task of `aq_weekly_train`, after both training tasks. A family is uploaded only when its champion
version differs from the one already published (`versions.json` in the repo), so a week with no promotion
makes no commit. The card is regenerated whenever anything is published.

    publish_job.py --catalog workspace --schema aq --hf-repo itstheryguy/aq-utah-models
"""

import argparse
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import mlflow
from mlflow import MlflowClient

FAMILIES = {"lightgbm": "LightGBM, one gradient-boosted model per horizon (Tweedie objective)",
            "gru": "GRU trained from scratch in PyTorch: the last 72 h as a sequence, plus forecast weather, three outputs"}
HORIZONS = [24, 48, 72]


def hf_token():
    try:
        from databricks.sdk.runtime import dbutils
        return dbutils.secrets.get("aq", "hf-token")
    except Exception:  # outside a notebook-like runtime, read it through the SDK instead
        import base64
        from databricks.sdk import WorkspaceClient
        return base64.b64decode(WorkspaceClient().secrets.get_secret("aq", "hf-token").value).decode()


def card(champions):
    rows = "\n".join(
        f"| {f} | v{c['version']} | {c['trained_to'][:10]} | "
        + " | ".join(f"{c['gate'].get(f'gate_mae_t{h}_challenger', float('nan')):.2f}" for h in HORIZONS)
        + " | " + " | ".join(f"{c['gate'].get(f'gate_mae_t{h}_persistence', float('nan')):.2f}" for h in HORIZONS) + " |"
        for f, c in champions.items()
    )
    return f"""---
license: mit
tags:
  - air-quality
  - pm2.5
  - time-series-forecasting
  - lightgbm
  - pytorch
---

# Utah PM2.5 forecasting models

Hourly PM2.5 (µg/m³) forecasts 24, 48, and 72 hours ahead for Utah's regulatory air quality monitors. These
are the current champions of a weekly retraining job with a promotion gate. They're used in batch every few
hours, and the forecasts are published to
[itstheryguy/aq-utah-forecasts](https://huggingface.co/datasets/itstheryguy/aq-utah-forecasts) and shown in
the [Utah PM2.5 Forecast Space](https://huggingface.co/spaces/itstheryguy/aq-utah).
Code and design: [github.com/itstheryguy/aq_utah_project](https://github.com/itstheryguy/aq_utah_project).

| Folder | Model |
|---|---|
""" + "\n".join(f"| `{f}/` | {d} |" for f, d in FAMILIES.items() if f in champions) + f"""

Each folder is an MLflow pyfunc model (`MLmodel`, model files under `artifacts/`, model code under `code/`):

```python
import mlflow
model = mlflow.pyfunc.load_model("lightgbm")   # a local copy of the folder
forecasts = model.predict(gold_rows)           # station_id, hour_utc, pred_t24, pred_t48, pred_t72
```

The input is rows of the project's `gold_features` table, one per station-hour: recent PM2.5 and NO₂, rolling
statistics, a regional mean, calendar features, and weather forecasts *as issued* 1–3 days ahead (Open-Meteo,
`gfs_seamless`), including a valley-minus-ridge temperature inversion proxy.

## Evaluation

**Promotion gate** for the current versions: mean absolute error (µg/m³) on the 28 most recent days with labels,
which neither the new version nor the previous champion was trained on.

| Model | Version | Trained through | 24 h | 48 h | 72 h | Persistence 24 h | 48 h | 72 h |
|---|---|---|---|---|---|---|---|---|
{rows}

**Backtest** (trained through Oct 2025, scored on Nov 2025 → Oct 2026), MAE in µg/m³, all stations:

| Model | 24 h | 48 h | 72 h | Winter 72 h | Summer 72 h |
|---|---|---|---|---|---|
| Persistence | 3.30 | 3.80 | 4.07 | 5.20 | 4.89 |
| LightGBM | 2.50 | 2.66 | 2.80 | 3.19 | 3.74 |
| GRU | 2.57 | 2.67 | 2.78 | 3.05 | 3.70 |

## Training data

2024-01-22 onward: AirNow PM2.5 and NO₂ history via OpenAQ, with hours removed by EPA quality review (EPA AirData)
excluded from the labels; Utah DAQ's live feed after that; Open-Meteo forecasts as issued (Previous Runs API). 26
stations, ~635k station-hours.

## Limitations

- **Spikes aren't predicted.** On hours ≥ 35.5 µg/m³, every model (and persistence) is off by ~30–35, almost all
  underprediction. Fireworks and wildfire smoke from outside the region aren't visible in the features.
- Labels from the last few months are provisional: EPA's quality review lags by about half a year.
- Trained on Utah's monitors only. Not an official forecast.

_Published {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC from Unity Catalog `@champion` aliases._
"""


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--catalog", default="workspace")
    parser.add_argument("--schema", default="aq")
    parser.add_argument("--hf-repo", default="", help="Hugging Face model repo; empty = skip")
    args = parser.parse_args()
    if not args.hf_repo:
        print("No --hf-repo, skipping publish")
        return

    from huggingface_hub import CommitOperationAdd, HfApi, hf_hub_download

    mlflow.set_registry_uri("databricks-uc")
    client = MlflowClient()
    api = HfApi(token=hf_token())
    try:
        published = json.loads(Path(hf_hub_download(args.hf_repo, "versions.json", token=api.token)).read_text())
    except Exception:  # first publish
        published = {}

    champions = {}
    for family in FAMILIES:
        name = f"{args.catalog}.{args.schema}.pm25_{family}"
        try:
            mv = client.get_model_version_by_alias(name, "champion")
        except mlflow.exceptions.MlflowException:
            print(f"{name} has no @champion, skipping")
            continue
        run = client.get_run(mv.run_id)
        champions[family] = {"version": str(mv.version), "uc_model": name, "run_id": mv.run_id,
                             "trained_to": run.data.params.get("train_to", ""), "gate": run.data.metrics}

    changed = [f for f, c in champions.items() if published.get(f, {}).get("version") != c["version"]]
    if not changed:
        print("Champions already published:", {f: c["version"] for f, c in champions.items()})
        return

    with tempfile.TemporaryDirectory() as tmp:
        for family in changed:
            c = champions[family]
            local = mlflow.artifacts.download_artifacts(f"models:/{c['uc_model']}/{c['version']}", dst_path=os.path.join(tmp, family))
            # Replace the family's folder entirely, so files from an older version don't linger.
            api.upload_folder(repo_id=args.hf_repo, folder_path=local, path_in_repo=family, delete_patterns="*",
                              commit_message=f"{family} v{c['version']} (Unity Catalog {c['uc_model']})")
            print(f"Published {family} v{c['version']}")
        versions = {f: {k: c[k] for k in ("version", "uc_model", "run_id", "trained_to")} for f, c in champions.items()}
        api.create_commit(repo_id=args.hf_repo, commit_message="Model card and versions", operations=[
            CommitOperationAdd("README.md", card(champions).encode()),
            CommitOperationAdd("versions.json", json.dumps(versions, indent=2).encode()),
        ])
    print("Model card updated:", {f: c["version"] for f, c in champions.items()})


if __name__ == "__main__":
    main()
