---
title: Utah PM2.5 Forecast
emoji: 🌫️
colorFrom: purple
colorTo: purple
sdk: static
pinned: false
license: mit
short_description: 'Air quality predictions for Utah. '
---

# Utah PM2.5 forecast

Fine-particle pollution (PM2.5) forecasts 24, 48, and 72 hours ahead for Utah's air quality monitors, from a
LightGBM model and a GRU trained from scratch, next to a persistence baseline.

The app runs entirely in your browser ([stlite](https://github.com/whitphx/stlite)). Forecasts are computed in
batch on Databricks and published to the dataset
[itstheryguy/aq-utah-forecasts](https://huggingface.co/datasets/itstheryguy/aq-utah-forecasts), which this
page reads.

Code and design: [github.com/itstheryguy/aq_utah_project](https://github.com/itstheryguy/aq_utah_project).
Zip code locations: U.S. Census Bureau ZCTA Gazetteer (2024), public domain.

Not an official forecast. During wildfire smoke or fireworks, check [AirNow](https://www.airnow.gov).
