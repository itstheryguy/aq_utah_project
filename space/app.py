"""Utah PM2.5 forecast: the Hugging Face Space.

Runs in the visitor's browser with stlite (Streamlit on WebAssembly), so the Space needs no server, and it
doesn't sleep. The app runs no model. Databricks scores every station in batch and exports small CSV files
to the public dataset this app reads (docs/design.md §7). The same file also runs as a normal Streamlit
app: `streamlit run space/app.py`.
"""

import io
import json
import math

import altair as alt
import pandas as pd
import streamlit as st

DATASET = "https://huggingface.co/datasets/itstheryguy/aq-utah-forecasts/resolve/main/latest/"
REPO = "https://github.com/itstheryguy/aq_utah_project"
MAX_DISTANCE_KM = 25
MODELS = {"lightgbm": "LightGBM", "gru": "GRU (PyTorch)", "linear": "Ridge regression", "persistence": "Persistence (baseline)"}
# EPA AQI breakpoints for PM2.5 (2024). Officially for 24-hour averages; applied here to hourly forecasts.
AQI = [(9.0, "Good", "#00e400"), (35.4, "Moderate", "#ffff00"), (55.4, "Unhealthy for sensitive groups", "#ff7e00"),
       (125.4, "Unhealthy", "#ff0000"), (225.4, "Very unhealthy", "#8f3f97"), (math.inf, "Hazardous", "#7e0023")]
# Backtest on 2025-11 → 2026-10, MAE in µg/m³ (docs/design.md §11).
BACKTEST = pd.DataFrame({"24 h": [3.30, 2.78, 2.50, 2.57], "48 h": [3.80, 3.02, 2.66, 2.67], "72 h": [4.07, 3.11, 2.80, 2.78]},
                        index=["Persistence", "Ridge regression (5 inputs)", "LightGBM", "GRU"])


def fetch(url):
    """Text from a URL, in the browser (stlite/Pyodide) or on a server."""
    try:
        from pyodide.http import open_url  # only exists under stlite
        return open_url(url).read()
    except ImportError:
        import urllib.request
        with urllib.request.urlopen(url, timeout=30) as resp:
            return resp.read().decode()


@st.cache_data(ttl=600, show_spinner="Loading the latest forecasts…")
def load():
    csv = lambda name, **kw: pd.read_csv(io.StringIO(fetch(DATASET + name)), **kw)  # noqa: E731
    times = ["hour_utc", "target_hour_utc"]
    data = {
        "forecasts": csv("forecasts.csv", parse_dates=times),
        "history": csv("history.csv", parse_dates=["hour_utc"]),
        "accuracy": csv("accuracy.csv"),
        "stations": csv("stations.csv", dtype={"wasatch_front": bool}),
        "daq": csv("daq_forecast.csv"),
        "meta": json.loads(fetch(DATASET + "meta.json")),
    }
    data["zips"] = pd.read_csv("utah_zips.csv", dtype={"zip": str})
    return data


def category(value):
    for upper, name, color in AQI:
        if value <= upper:
            return name, color


def distance_km(lat1, lon1, lat2, lon2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    a = math.sin((p2 - p1) / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lon2 - lon1) / 2) ** 2
    return 6371 * 2 * math.asin(math.sqrt(a))


def local(ts):
    """Display times in Mountain Time (the data is UTC)."""
    return pd.Timestamp(ts).tz_localize("UTC").tz_convert("America/Denver")


def when(ts, date=False):
    """'Thu 5 PM' or 'Thu Nov 5, 5 PM'. Built by hand: strftime's %-I isn't portable to WebAssembly."""
    t = local(ts)
    hour = f"{t.hour % 12 or 12} {'AM' if t.hour < 12 else 'PM'}"
    return f"{t:%a %b} {t.day}, {hour}" if date else f"{t:%a} {hour}"


st.set_page_config(page_title="Utah PM2.5 forecast", page_icon="🌫️", layout="centered")
st.title("Utah PM2.5 forecast")
st.caption("Fine-particle pollution forecasts 1–3 days ahead for Utah's air quality monitors, "
           f"from models trained on 2+ years of data. [How it works]({REPO})")

try:
    data = load()
except Exception as e:  # noqa: BLE001
    st.error(f"Couldn't load the forecast data ({e}). Try again in a few minutes.")
    st.stop()
stations, forecasts, meta = data["stations"], data["forecasts"], data["meta"]

# --- where -------------------------------------------------------------------------------------------------
left, right = st.columns([1, 2])
zip_code = left.text_input("Zip code", value="84102", max_chars=5)
match = data["zips"][data["zips"].zip == zip_code.strip()]
if match.empty:
    right.warning("Enter a 5-digit Utah zip code, or pick a station below.")
    nearest, dist = stations.sort_values("name").iloc[0], None
else:
    z = match.iloc[0]
    stations = stations.assign(km=[distance_km(z.latitude, z.longitude, s.latitude, s.longitude) for s in stations.itertuples()])
    nearest = stations.sort_values("km").iloc[0]
    dist = nearest.km
names = stations.sort_values("name")["name"].tolist()
picked = right.selectbox("Monitoring station", names, index=names.index(nearest["name"]))
station = stations[stations.name == picked].iloc[0]
if dist is not None and picked == nearest["name"]:
    if dist > MAX_DISTANCE_KM:
        st.warning(f"The nearest monitor, {picked}, is {dist:.0f} km from {zip_code}. Its forecast may not reflect "
                   "air quality where you are.")
    else:
        st.caption(f"Nearest monitor to {zip_code}: **{picked}**, {dist:.1f} km away.")

model = st.radio("Model", list(MODELS), format_func=MODELS.get, horizontal=True)

# --- forecast ----------------------------------------------------------------------------------------------
f = forecasts[(forecasts.station_id == station.station_id) & (forecasts.model == model)].sort_values("horizon_h")
history = data["history"][data["history"].station_id == station.station_id]
if f.empty or f.predicted_pm25.isna().all():
    st.info("No forecast for this station right now.")
else:
    as_of = f.hour_utc.iloc[0]
    current = history.pm25_ugm3.iloc[-1] if len(history) else None
    cols = st.columns(4)
    if current is not None:
        name, _ = category(current)
        cols[0].metric(f"Now ({when(as_of)})", f"{current:.1f}", help="Latest hourly reading, µg/m³")
        cols[0].caption(name)
    for col, row in zip(cols[1:], f.itertuples()):
        name, _ = category(row.predicted_pm25)
        delta = row.predicted_pm25 - current if current is not None else None
        col.metric(when(row.target_hour_utc), f"{row.predicted_pm25:.1f}",
                   delta=f"{delta:+.1f}" if delta is not None else None, delta_color="inverse",
                   help=f"{row.horizon_h} hours ahead, µg/m³")
        col.caption(name)
    if f.input_age_h.iloc[0] > 6:
        st.warning(f"This station's latest reading is {f.input_age_h.iloc[0]:.0f} hours old, so the forecast is too.")

    # History + every model's forecast, so the chosen model can be compared with the others.
    hist = history.assign(series="Measured", time=history.hour_utc.map(local))[["time", "pm25_ugm3", "series"]]
    every = forecasts[forecasts.station_id == station.station_id]
    fc = every.assign(series=every.model.map(MODELS), time=every.target_hour_utc.map(local), pm25_ugm3=every.predicted_pm25)
    anchor = fc.drop_duplicates("model").assign(time=local(as_of), pm25_ugm3=current) if current is not None else fc.iloc[:0]
    fc = pd.concat([anchor, fc])[["time", "pm25_ugm3", "series"]]
    chart = alt.Chart(pd.concat([hist, fc])).mark_line(point=alt.OverlayMarkDef(size=20)).encode(
        x=alt.X("time:T", title=None),
        y=alt.Y("pm25_ugm3:Q", title="PM2.5 (µg/m³)"),
        color=alt.Color("series:N", title=None, legend=alt.Legend(orient="bottom")),
        strokeDash=alt.condition(alt.datum.series == "Measured", alt.value([1, 0]), alt.value([4, 3])),
        tooltip=[alt.Tooltip("time:T", format="%a %b %-d, %-I %p"), "series:N", alt.Tooltip("pm25_ugm3:Q", format=".1f")],
    )
    st.altair_chart(chart, use_container_width=True)

# DAQ's own forecast for the area, for comparison.
daq = data["daq"][data["daq"].daq_forecast_area == station.daq_forecast_area]
if len(daq):
    st.caption("Utah DAQ's official forecast for this area: " + " · ".join(
        f"**{pd.Timestamp(r.forecast_date):%a}** {r.severity}" + (f" ({r.action})" if isinstance(r.action, str) and r.action else "")
        for r in daq.itertuples()))

# --- how good is it ----------------------------------------------------------------------------------------
with st.expander("How accurate are these forecasts?"):
    st.markdown("**Backtest:** trained on data up to Oct 2025, then scored on the following year, which the "
                "models never saw. Mean absolute error in µg/m³, all stations (lower is better):")
    st.dataframe(BACKTEST, use_container_width=True)
    acc = data["accuracy"]
    live = acc[(acc.region == "all") & (acc.window_days == 30)] if len(acc) else acc
    if len(live):
        st.markdown("**Live:** forecasts made by this site, scored once the hour they predict has passed (last 30 days):")
        st.dataframe(live.pivot(index="model", columns="horizon_h", values="mae").rename(index=MODELS)
                     .rename(columns=lambda h: f"{h} h"), use_container_width=True)
    else:
        st.markdown("**Live:** scoring starts once the first forecasts' target hours have passed.")
    st.markdown("- LightGBM and the GRU cut persistence's error by roughly a quarter to a third, most of all in "
                "winter inversions, when forecast weather matters most.\n"
                "- Ridge regression on five inputs gets about 60% of that gain and trains in seconds. It falls "
                "behind most in winter, when an inversion's effect builds up nonlinearly over days.\n"
                "- **They don't predict sudden spikes** from fireworks or wildfire smoke, which come from outside "
                "what the models see. Check [AirNow](https://www.airnow.gov) during smoke events.\n"
                "- Categories use EPA's PM2.5 breakpoints, which are defined for 24-hour averages. These are "
                "hourly values, so treat the category as a guide.")

st.caption(f"Forecasts as of {when(meta['as_of_utc'], date=True)} MT · models: LightGBM v{meta['champions'].get('lightgbm', '–')}, "
           f"GRU v{meta['champions'].get('gru', '–')}, ridge v{meta['champions'].get('linear', '–')} · data: Utah DAQ, AirNow via OpenAQ, EPA AirData, Open-Meteo. "
           "Not an official forecast.")
