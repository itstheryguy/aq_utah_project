"""One-time history backfill, saved locally first and then uploaded to the landing volume.

    python extractor/backfill.py openaq      # AirNow history (PM2.5 + NO2) for every DAQ monitor, via OpenAQ
    python extractor/backfill.py openmeteo   # Open-Meteo forecasts as issued 0-3 days ahead, every station
    python extractor/backfill.py epa         # EPA AirData hourly PM2.5 + NO2, filtered to Utah
    python extractor/backfill.py repair      # re-request anything that failed (e.g. 429s), rewrite those files
    python extractor/backfill.py upload --volume /Volumes/workspace/aq_dev/landing

Each download step skips files that already exist under data/backfill/, so an interrupted run resumes where it
stopped. OpenAQ and Open-Meteo files use the same JSON Lines envelope as the hourly extractor (one line per
request), so Auto Loader loads them into bronze_raw like any other landed file. EPA files stay CSV and load
into their own bronze table. See docs/design.md §8.

Why AirNow (via OpenAQ) is the main history and EPA is only a check: AirNow is the same primary-instrument
series DAQ publishes live, so models train on what they're scored on. EPA's quality-assured files only reach
2026-06-01 and include every collocated instrument (POC) at a site.
"""

import argparse
import csv
import gzip
import io
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import extract  # noqa: E402  (shares fetch/upload with the hourly extractor)

REPO = extract.REPO
OUT = REPO / "data" / "backfill"
# Open-Meteo's archive of forecasts issued 3 days ahead starts ~2024-01-22. A week earlier gives the rolling
# PM2.5 features some history on the first training day.
START = date(2024, 1, 15)
OPENMETEO_VARS = [
    "temperature_2m", "relative_humidity_2m", "wind_speed_10m", "wind_direction_10m",
    "pressure_msl", "precipitation", "cloud_cover", "shortwave_radiation",
]
OPENMETEO_LEAD_DAYS = [1, 2, 3]  # plus the base variable, which is the latest run (lead 0)
EPA_PARAMETERS = {"88101": "pm25", "42602": "no2"}
EPA_YEARS = [2024, 2025, 2026]


def stations():
    with open(REPO / "reference" / "stations.csv") as f:
        return list(csv.DictReader(f))


def write_envelopes(path, source):
    """Write the records collected by extract.fetch as one JSON Lines file, then reset them."""
    for r in extract.records:
        r["source"] = source
        r["run_id"] = "backfill"
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_bytes(gzip.compress("".join(json.dumps(r) + "\n" for r in extract.records).encode()))
    tmp.rename(path)  # only complete files count as done
    failed = sum(1 for r in extract.records if not (r["http_status"] and 200 <= r["http_status"] < 300))
    extract.records.clear()
    return failed


def date_chunks(start, end, days):
    while start <= end:
        yield start, min(start + timedelta(days=days - 1), end)
        start += timedelta(days=days)


def backfill_openaq(shard=0, shards=1):
    """Every DAQ monitor's PM2.5 and NO2 sensor, from START to now, one file per sensor.

    Chunks of 40 days are under 1,000 hours, so each request is a single page: ~1,400 requests in total.
    OpenAQ takes ~6 s to answer each one, so `--shards 5` runs five processes on separate sensors. That's
    still under the 60/minute rate limit.
    """
    headers = {"X-API-Key": os.environ["OPENAQ_API_KEY"]}
    sensors = [(s["station_id"], p, s[f"openaq_{p}_sensor_id"]) for s in stations() for p in ("pm25", "no2")
               if s["kind"] == "monitor" and s[f"openaq_{p}_sensor_id"]]
    today = datetime.now(timezone.utc).date()
    for i, (station, parameter, sensor) in enumerate(sensors, 1):
        if i % shards != shard:
            continue
        path = OUT / "raw" / "openaq_history" / f"{station}_{parameter}_{sensor}.jsonl.gz"
        if path.exists():
            continue
        for frm, to in date_chunks(START, today, 40):
            extract.fetch("openaq_history", f"{extract.OPENAQ}/sensors/{sensor}/hours", headers=headers, params={
                "datetime_from": f"{frm}T00:00:00Z", "datetime_to": f"{to + timedelta(days=1)}T00:00:00Z", "limit": 1000,
            })
            time.sleep(1.05)
        failed = write_envelopes(path, "openaq_history")
        print(f"[{i}/{len(sensors)}] {station} {parameter}: {failed} failed requests")


OPENMETEO_ARCHIVE = "https://previous-runs-api.open-meteo.com/v1/forecast"


def openmeteo_params(station, frm, to):
    hourly = OPENMETEO_VARS + [f"{v}_previous_day{d}" for d in OPENMETEO_LEAD_DAYS for v in OPENMETEO_VARS]
    return {"latitude": station["latitude"], "longitude": station["longitude"], "hourly": ",".join(hourly),
            "models": "gfs_seamless", "start_date": frm, "end_date": to, "timezone": "UTC"}


def backfill_openmeteo():
    """Forecasts as issued for every station, quarter by quarter, one file per station.

    Each request asks for 8 variables × 4 lead times. Open-Meteo counts large requests as several calls
    against its free limits (5,000/hour, 10,000/day), so a 429 means wait and retry rather than fail.
    """
    today = datetime.now(timezone.utc).date()
    sites = stations()
    for i, s in enumerate(sites, 1):
        path = OUT / "raw" / "openmeteo_archive" / f"{s['station_id']}.jsonl.gz"
        if path.exists():
            continue
        for frm, to in date_chunks(START, today, 92):
            for attempt, wait in enumerate([0, 60, 300, 900, 1800]):
                if attempt:
                    print(f"  rate limited, waiting {wait}s: {(extract.records[-1]['body'] or '')[:100]}")
                    extract.records.pop()  # a retry replaces the 429; the final attempt is always kept
                time.sleep(wait)
                body = extract.fetch("openmeteo_archive", OPENMETEO_ARCHIVE, params=openmeteo_params(s, frm, to), retries=1)
                if body is not None or extract.records[-1]["http_status"] != 429:
                    break
        failed = write_envelopes(path, "openmeteo_archive")
        print(f"[{i}/{len(sites)}] {s['station_id']}: {failed} failed requests")


def backfill_epa():
    """EPA AirData pre-generated hourly files: download the national zip, keep Utah rows (state code 49)."""
    for code, parameter in EPA_PARAMETERS.items():
        for year in EPA_YEARS:
            path = OUT / "epa" / f"hourly_{code}_{year}_utah.csv.gz"
            if path.exists():
                continue
            url = f"https://aqs.epa.gov/aqsweb/airdata/hourly_{code}_{year}.zip"
            request = urllib.request.Request(url, headers={"User-Agent": extract.USER_AGENT})
            with urllib.request.urlopen(request, timeout=600) as resp:
                archive = zipfile.ZipFile(io.BytesIO(resp.read()))
            path.parent.mkdir(parents=True, exist_ok=True)
            kept = 0
            with archive.open(archive.namelist()[0]) as src, gzip.open(path.with_suffix(".tmp"), "wt", newline="") as dst:
                reader = csv.reader(io.TextIOWrapper(src, encoding="utf-8"))
                writer = csv.writer(dst)
                header = next(reader)
                writer.writerow(header)
                state = header.index("State Code")
                for row in reader:
                    if row[state] == "49":
                        writer.writerow(row)
                        kept += 1
            path.with_suffix(".tmp").rename(path)
            print(f"{parameter} {year}: {kept:,} Utah rows → {path.relative_to(REPO)}")


def repair():
    """Re-request every failed request in the saved API files, one at a time, and rewrite those files.

    The parallel OpenAQ shards sometimes add up to more than the shared 60/minute limit, which leaves 429s in
    the files. Run this until it reports nothing left to repair.
    """
    for path in sorted((OUT / "raw").rglob("*.jsonl.gz")):
        records = [json.loads(line) for line in gzip.open(path)]
        if records[0]["source"] == "openmeteo_archive":
            # An older version of the retry loop could drop a chunk entirely. Re-add any missing chunk as a
            # placeholder failure so it gets requested below.
            station = next(s for s in stations() if s["station_id"] == path.name.split(".")[0])
            have = {urllib.parse.parse_qs(urllib.parse.urlparse(r["request"]).query)["start_date"][0] for r in records}
            for frm, to in date_chunks(START, date.fromisoformat(records[-1]["fetched_at"][:10]), 92):
                if str(frm) not in have:
                    url = OPENMETEO_ARCHIVE + "?" + urllib.parse.urlencode(openmeteo_params(station, frm, to), safe=",")
                    records.append({"source": "openmeteo_archive", "request": url, "http_status": None})
        failed = [i for i, r in enumerate(records) if not (r["http_status"] and 200 <= r["http_status"] < 300)]
        if not failed:
            continue
        headers = {"X-API-Key": os.environ["OPENAQ_API_KEY"]} if "openaq" in records[0]["source"] else None
        for i in failed:
            time.sleep(1.5)
            extract.fetch(records[i]["source"], records[i]["request"], headers=headers)
            records[i] = {**extract.records.pop(), "run_id": "backfill"}
        extract.records.extend(records)
        still_failed = write_envelopes(path, records[0]["source"])
        print(f"{path.name}: repaired {len(failed) - still_failed} of {len(failed)}")


def upload_all(volume):
    """Upload everything under data/backfill/. Existing remote files are skipped, except EPA files: EPA
    republishes them twice a year, so those overwrite."""
    for path in sorted(OUT.rglob("*.gz")):
        rel = path.relative_to(OUT).as_posix()
        remote = f"{volume.rstrip('/')}/{rel if rel.startswith('epa/') else 'raw/backfill/' + rel.removeprefix('raw/')}"
        try:
            extract.upload(remote, path.read_bytes(), overwrite=rel.startswith("epa/"))
            print(f"uploaded {rel} ({path.stat().st_size / 1e6:.1f} MB) → {remote}")
        except RuntimeError as e:
            if "409" in str(e) or "ALREADY_EXISTS" in str(e):
                print(f"exists, skipped {rel}")
            else:
                raise


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("step", choices=["openaq", "openmeteo", "epa", "repair", "upload"])
    parser.add_argument("--volume", help="Volume root for upload, e.g. /Volumes/workspace/aq_dev/landing")
    parser.add_argument("--shard", type=int, default=0, help="openaq only: which shard this process runs")
    parser.add_argument("--shards", type=int, default=1, help="openaq only: total number of shards")
    args = parser.parse_args()
    if args.step == "upload":
        if not args.volume:
            parser.error("upload needs --volume")
        upload_all(args.volume)
    else:
        if args.step == "openaq":
            backfill_openaq(args.shard, args.shards)
        else:
            {"openmeteo": backfill_openmeteo, "epa": backfill_epa, "repair": repair}[args.step]()


if __name__ == "__main__":
    main()
