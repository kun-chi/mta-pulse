# MTA Pulse 🚇

A fully automated pipeline that snapshots NYC subway real-time data every 15 minutes using GitHub Actions — no servers, no API keys, no cost.

## What it collects

Every 15 minutes, `collect_mta.py`:

1. Fetches all 8 NYC subway [GTFS-Realtime](https://api.mta.info/) feeds
2. Records each active train's next predicted stop and arrival time
3. Records all active service alerts (delays, reroutes, planned work)
4. Appends everything to gzipped daily CSVs in `data/`

## Data schema

**`data/trains_YYYY-MM-DD.csv.gz`**

| column | meaning |
|---|---|
| `snapshot_utc` | when the snapshot was taken |
| `feed` | feed group (`ace`, `bdfm`, `main`, ...) |
| `route_id` | subway line (`A`, `6`, `L`, ...) |
| `trip_id` | unique trip identifier |
| `next_stop_id` | GTFS id of the train's next stop |
| `predicted_arrival_utc` | predicted arrival at that stop |
| `feed_timestamp_utc` | when MTA generated the feed |

**`data/alerts_YYYY-MM-DD.csv.gz`**

| column | meaning |
|---|---|
| `snapshot_utc` | when the snapshot was taken |
| `alert_id` | MTA alert id |
| `routes` | affected routes, pipe-separated |
| `header` | alert text |
| `active_start_utc` / `active_end_utc` | alert active window |

## How it runs

GitHub Actions (`.github/workflows/collect.yml`) triggers on a `*/15` cron,
runs the collector, and commits any new data back to the repo. The full
collection history lives in git.

## Run it locally

```bash
pip install -r requirements.txt
python collect_mta.py
```

## Reading the data

```python
import pandas as pd
df = pd.read_csv("data/trains_2026-07-29.csv.gz")
```

---

*Built as the data pipeline behind my NYC subway analysis series.*
