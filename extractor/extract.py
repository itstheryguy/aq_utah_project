"""Fetch the live sources and land the raw responses in a Unity Catalog volume.

Runs hourly from GitHub Actions (.github/workflows/extract.yml). Databricks serverless can't reach
air.utah.gov or api.openaq.org on this workspace (see docs/design.md §5), so extraction runs here and
Databricks picks the files up with Auto Loader (notebooks/01_bronze_ingest.py).

Each run writes ONE file: gzipped JSON Lines, one line per HTTP request, failures included:
    <volume>/raw/<YYYY-MM-DD>/<YYYYMMDDTHHMMSSZ>_<run_id>.jsonl.gz

Standard library only, so the workflow needs no pip install.

    python extractor/extract.py --local-dir data/landing          # dry run, writes the file locally
    python extractor/extract.py --volume /Volumes/workspace/aq_dev/landing

Auth for uploads, from the environment:
    DATABRICKS_HOST plus either DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET (service principal,
    OAuth machine-to-machine) or DATABRICKS_TOKEN (e.g. `databricks auth token` for local testing).
    OPENAQ_API_KEY for OpenAQ.
"""

import argparse
import base64
import csv
import gzip
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
USER_AGENT = "aq_utah_project (+https://github.com/itstheryguy/aq_utah_project)"
RETRY_STATUS = {429, 500, 502, 503, 504}

OPENAQ = "https://api.openaq.org/v3"
UTAH_BBOX = "-114.05,37.0,-109.04,42.0"

# boundary_layer_height isn't in Open-Meteo's archive of past forecasts, so it can't be a training feature
# yet. It's pulled anyway to build our own archive of it. See docs/design.md §4.
OPENMETEO_HOURLY = [
    "temperature_2m", "relative_humidity_2m", "dew_point_2m", "wind_speed_10m", "wind_direction_10m",
    "pressure_msl", "surface_pressure", "precipitation", "cloud_cover", "shortwave_radiation",
    "boundary_layer_height",
]

records = []


def fetch(source, url, params=None, headers=None, retries=3):
    """GET url with retries, and append the outcome to `records` whether or not it succeeded.

    Returns the body on a 2xx response, otherwise None.
    """
    # Open-Meteo and OpenAQ take comma-separated lists, so commas stay unescaped.
    full_url = url + ("?" + urllib.parse.urlencode(params, safe=",") if params else "")
    request = urllib.request.Request(full_url, headers={"User-Agent": USER_AGENT, **(headers or {})})
    started = time.monotonic()
    status = body = error = None
    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(request, timeout=60) as resp:
                status, body, error = resp.status, resp.read().decode("utf-8", "replace"), None
            break
        except urllib.error.HTTPError as e:
            status, body, error = e.code, e.read().decode("utf-8", "replace"), f"HTTPError: {e.code} {e.reason}"
            if e.code not in RETRY_STATUS:
                break
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            status, body, error = None, None, f"{type(e).__name__}: {e}"
        if attempt < retries:
            time.sleep(2 ** (attempt + 1))  # 2s, 4s, 8s
    records.append({
        "source": source,
        "request": full_url,  # API keys go in headers, so none appear here
        "fetched_at": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "http_status": status,
        "body": body,
        "error": error,
        "latency_ms": round((time.monotonic() - started) * 1000, 1),
    })
    return body if status and 200 <= status < 300 else None


def extract():
    with open(REPO / "reference" / "stations.csv") as f:
        stations = list(csv.DictReader(f))

    daq = fetch("daq_xml", "https://air.utah.gov/xmlFeed.php")
    fetch("daq_forecast", "https://air.utah.gov/csvFeed.php", params={"id": "forecast"})

    # One call for every station plus the ridge reference point. gfs_seamless is pinned so the live forecast
    # comes from the same weather model as the archived forecasts used for training.
    fetch("openmeteo_forecast", "https://api.open-meteo.com/v1/forecast", params={
        "latitude": ",".join(s["latitude"] for s in stations),
        "longitude": ",".join(s["longitude"] for s in stations),
        "hourly": ",".join(OPENMETEO_HOURLY),
        "models": "gfs_seamless",
        "forecast_days": 4,
        "timezone": "UTC",
    })

    # OpenAQ: discover active low-cost PM2.5 sensors, then pull each one's last 24 hours. DAQ's own monitors
    # also appear in OpenAQ (via AirNow) but are skipped, since DAQ is pulled directly.
    openaq_headers = {"X-API-Key": os.environ["OPENAQ_API_KEY"]}
    now = datetime.now(timezone.utc)
    locations = fetch("openaq_locations", f"{OPENAQ}/locations", params={"bbox": UTAH_BBOX, "limit": 1000},
                      headers=openaq_headers)
    sensors = []
    if locations:
        active_since = (now - timedelta(days=7)).isoformat()
        for loc in json.loads(locations)["results"]:
            last = (loc.get("datetimeLast") or {}).get("utc")
            if loc["provider"]["name"] == "AirNow" or not last or last < active_since:
                continue
            sensors += [s["id"] for s in loc["sensors"] if s["parameter"]["name"] == "pm25"]
    since = (now - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")
    for sensor_id in sensors:  # ~40 calls, under the 60/minute rate limit
        fetch("openaq_sensor_hours", f"{OPENAQ}/sensors/{sensor_id}/hours",
              params={"datetime_from": since, "limit": 100}, headers=openaq_headers)

    return daq is not None and "<site>" in daq


def databricks_call(request, timeout):
    """urlopen for Databricks APIs. On an HTTP error, raise with the response body, which says why."""
    request.add_header("User-Agent", USER_AGENT)
    try:
        return urllib.request.urlopen(request, timeout=timeout)
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"{request.get_method()} {request.full_url.split('?')[0]} → {e.code}: "
                           f"{e.read().decode('utf-8', 'replace')[:500]}") from None


def databricks_token(host):
    if os.environ.get("DATABRICKS_TOKEN"):
        return os.environ["DATABRICKS_TOKEN"]
    creds = f"{os.environ['DATABRICKS_CLIENT_ID']}:{os.environ['DATABRICKS_CLIENT_SECRET']}"
    # The scope requested must be one the service principal's secret was created with.
    scope = os.environ.get("DATABRICKS_OAUTH_SCOPE") or "all-apis"
    request = urllib.request.Request(
        f"{host}/oidc/v1/token",
        data=urllib.parse.urlencode({"grant_type": "client_credentials", "scope": scope}).encode(),
        headers={"Authorization": "Basic " + base64.b64encode(creds.encode()).decode()},
    )
    with databricks_call(request, timeout=30) as resp:
        return json.load(resp)["access_token"]


def upload(volume_path, data):
    """PUT a file into a Unity Catalog volume with the Files API."""
    host = os.environ["DATABRICKS_HOST"].rstrip("/")
    request = urllib.request.Request(
        f"{host}/api/2.0/fs/files{urllib.parse.quote(volume_path)}?overwrite=false",
        data=data,
        method="PUT",
        headers={"Authorization": f"Bearer {databricks_token(host)}", "Content-Type": "application/octet-stream"},
    )
    databricks_call(request, timeout=120).close()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--volume", help="Volume root, e.g. /Volumes/workspace/aq_dev/landing")
    target.add_argument("--local-dir", type=Path, help="Write the file locally instead of uploading")
    args = parser.parse_args()

    run_id = f"gha-{os.environ['GITHUB_RUN_ID']}" if os.environ.get("GITHUB_RUN_ID") else "local"
    daq_ok = extract()
    for r in records:
        r["run_id"] = run_id

    started = datetime.strptime(records[0]["fetched_at"][:19], "%Y-%m-%dT%H:%M:%S")
    name = f"raw/{started:%Y-%m-%d}/{started:%Y%m%dT%H%M%S}Z_{run_id}.jsonl.gz"
    data = gzip.compress("".join(json.dumps(r) + "\n" for r in records).encode())

    if args.local_dir:
        path = args.local_dir / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
    else:
        path = f"{args.volume.rstrip('/')}/{name}"
        upload(path, data)

    failed = [r for r in records if not (r["http_status"] and 200 <= r["http_status"] < 300)]
    print(f"{len(records)} requests, {len(failed)} failed, {len(data) / 1024:.0f} KB → {path}")
    for r in failed:
        print(f"  FAILED {r['source']}: {r['error']}")

    # The file is written first, so a failed DAQ pull is still recorded. Then a non-zero exit fails the
    # workflow, and GitHub emails the failure.
    if not daq_ok:
        sys.exit("DAQ XML pull failed or returned no <site> elements")


if __name__ == "__main__":
    main()
