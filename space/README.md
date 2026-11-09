---
title: Utah PM2.5 Forecast
emoji: 🌫️
colorFrom: purple
colorTo: purple
sdk: static
pinned: false
license: mit
short_description: 1–3 day PM2.5 forecasts for Utah, retrained weekly
models:
  - itstheryguy/aq-utah-models
datasets:
  - itstheryguy/aq-utah-forecasts
---

# Utah PM2.5 forecast

Fine-particle pollution (PM2.5) forecasts 24, 48, and 72 hours ahead for Utah's air quality monitors, from
LightGBM, a GRU trained from scratch, and ridge regression, next to a persistence baseline. In a year-long
backtest, LightGBM and the GRU cut persistence's error by 24–32%. The app also shows each model's live accuracy.

The app runs entirely in your browser ([stlite](https://github.com/whitphx/stlite)). A Databricks pipeline
computes forecasts in batch every few hours and publishes them to the dataset
[itstheryguy/aq-utah-forecasts](https://huggingface.co/datasets/itstheryguy/aq-utah-forecasts), which this
page reads. Weekly retraining publishes the champion models to
[itstheryguy/aq-utah-models](https://huggingface.co/itstheryguy/aq-utah-models).

Code and design: [github.com/itstheryguy/aq_utah_project](https://github.com/itstheryguy/aq_utah_project).
Zip code locations: U.S. Census Bureau ZCTA Gazetteer (2024), public domain.

Not an official forecast. During wildfire smoke or fireworks, check [AirNow](https://www.airnow.gov).
