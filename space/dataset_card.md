---
license: cc-by-4.0
pretty_name: Utah PM2.5 forecasts
tags:
  - air-quality
  - pm2.5
  - forecasting
  - utah
---

# Utah PM2.5 forecasts

Batch forecasts of hourly PM2.5 (µg/m³) 24, 48, and 72 hours ahead for Utah's regulatory air quality monitors,
refreshed every few hours by a Databricks pipeline. The
[Utah PM2.5 Forecast Space](https://huggingface.co/spaces/itstheryguy/aq-utah) reads these files.
Code and design: [github.com/itstheryguy/aq_utah_project](https://github.com/itstheryguy/aq_utah_project).

`latest/` is overwritten on each run. The repo's commit history keeps every earlier version.

| File | One row per | Columns |
|---|---|---|
| `latest/forecasts.csv` | station × model × horizon | `station_id`, `hour_utc` (newest reading the forecast starts from), `model` (`lightgbm`, `gru`, `persistence`), `model_version`, `horizon_h`, `target_hour_utc`, `predicted_pm25`, `input_age_h` |
| `latest/history.csv` | station × hour, last 7 days | `station_id`, `hour_utc`, `pm25_ugm3` |
| `latest/accuracy.csv` | model × horizon × window × region | Live MAE and bias of past forecasts against the readings that later arrived. `window_days` is 7 or 30. |
| `latest/stations.csv` | station | `station_id`, `name`, `latitude`, `longitude`, `county`, `daq_forecast_area`, `wasatch_front` |
| `latest/daq_forecast.csv` | forecast area × day | Utah DAQ's own forecast category and burn-restriction action, for comparison |
| `latest/meta.json` | | `as_of_utc`, `generated_at_utc`, champion model versions |

Times are UTC, and `hour_utc` is the start of the hour. Readings are provisional (not yet quality-assured).

**Sources:** Utah Division of Air Quality, AirNow (via OpenAQ), EPA AirData, Open-Meteo. Forecasts are model
output and aren't an official forecast.
