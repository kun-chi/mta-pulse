"""
MTA GTFS-Realtime snapshot collector.

Every run:
  1. Fetches all NYC subway real-time feeds (no API key needed).
  2. For each active train, records its next predicted stop + arrival time.
  3. Fetches the service-alerts feed and records active alerts.
  4. Appends rows to gzipped daily CSV files under data/.

Designed to be run every ~15 minutes by GitHub Actions.
"""

import csv
import gzip
import io
import os
from datetime import datetime, timezone

import requests
from google.transit import gtfs_realtime_pb2

BASE = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds"

# All NYC subway trip-update feeds (keyless since 2023)
FEEDS = {
    "main": f"{BASE}/nyct%2Fgtfs",        # 1 2 3 4 5 6 7 S
    "ace":  f"{BASE}/nyct%2Fgtfs-ace",    # A C E
    "bdfm": f"{BASE}/nyct%2Fgtfs-bdfm",   # B D F M
    "g":    f"{BASE}/nyct%2Fgtfs-g",      # G
    "jz":   f"{BASE}/nyct%2Fgtfs-jz",     # J Z
    "nqrw": f"{BASE}/nyct%2Fgtfs-nqrw",   # N Q R W
    "l":    f"{BASE}/nyct%2Fgtfs-l",      # L
    "si":   f"{BASE}/nyct%2Fgtfs-si",     # Staten Island Railway
}

ALERTS_FEED = f"{BASE}/camsys%2Fsubway-alerts"

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

TRAIN_COLUMNS = [
    "snapshot_utc",      # when this script ran
    "feed",              # which feed group (ace, bdfm, ...)
    "route_id",          # e.g. "A", "6"
    "trip_id",           # unique trip identifier
    "next_stop_id",      # GTFS stop id of the next predicted stop
    "predicted_arrival_utc",  # predicted arrival time at that stop
    "feed_timestamp_utc",     # when MTA generated the feed
]

ALERT_COLUMNS = [
    "snapshot_utc",
    "alert_id",
    "routes",            # pipe-separated affected routes, e.g. "A|C"
    "header",            # short human-readable alert text
    "active_start_utc",
    "active_end_utc",
]


def utc_iso(ts: int | None) -> str:
    """Unix timestamp -> ISO string, empty if missing/zero."""
    if not ts:
        return ""
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(timespec="seconds")


def fetch_feed(url: str) -> gtfs_realtime_pb2.FeedMessage | None:
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(resp.content)
        return feed
    except Exception as exc:  # noqa: BLE001 - log and keep going
        print(f"[warn] failed to fetch {url}: {exc}")
        return None


def collect_trains(snapshot_utc: str) -> list[list[str]]:
    rows = []
    for name, url in FEEDS.items():
        feed = fetch_feed(url)
        if feed is None:
            continue
        feed_ts = utc_iso(feed.header.timestamp)
        for entity in feed.entity:
            if not entity.HasField("trip_update"):
                continue
            tu = entity.trip_update
            stus = tu.stop_time_update
            if not stus:
                continue
            # First stop_time_update = the train's next stop
            nxt = stus[0]
            arrival = nxt.arrival.time if nxt.HasField("arrival") else 0
            rows.append([
                snapshot_utc,
                name,
                tu.trip.route_id,
                tu.trip.trip_id,
                nxt.stop_id,
                utc_iso(arrival),
                feed_ts,
            ])
        print(f"[ok] {name}: {len(feed.entity)} entities")
    return rows


def collect_alerts(snapshot_utc: str) -> list[list[str]]:
    rows = []
    feed = fetch_feed(ALERTS_FEED)
    if feed is None:
        return rows
    for entity in feed.entity:
        if not entity.HasField("alert"):
            continue
        alert = entity.alert
        routes = sorted(
            {ie.route_id for ie in alert.informed_entity if ie.route_id}
        )
        header = ""
        if alert.header_text.translation:
            header = alert.header_text.translation[0].text.replace("\n", " ")
        start, end = "", ""
        if alert.active_period:
            start = utc_iso(alert.active_period[0].start)
            end = utc_iso(alert.active_period[0].end)
        rows.append([
            snapshot_utc,
            entity.id,
            "|".join(routes),
            header,
            start,
            end,
        ])
    print(f"[ok] alerts: {len(rows)} active")
    return rows


def append_gzip_csv(path: str, columns: list[str], rows: list[list[str]]) -> None:
    """Append rows to a gzipped CSV, writing a header if the file is new."""
    if not rows:
        return
    is_new = not os.path.exists(path)
    # gzip files can be appended by concatenating members - readers handle it fine
    with gzip.open(path, "at", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        if is_new:
            writer.writerow(columns)
        writer.writerows(rows)


def main() -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    now = datetime.now(timezone.utc)
    snapshot_utc = now.isoformat(timespec="seconds")
    day = now.strftime("%Y-%m-%d")

    train_rows = collect_trains(snapshot_utc)
    alert_rows = collect_alerts(snapshot_utc)

    append_gzip_csv(
        os.path.join(DATA_DIR, f"trains_{day}.csv.gz"), TRAIN_COLUMNS, train_rows
    )
    append_gzip_csv(
        os.path.join(DATA_DIR, f"alerts_{day}.csv.gz"), ALERT_COLUMNS, alert_rows
    )

    print(f"[done] {len(train_rows)} train rows, {len(alert_rows)} alert rows")


if __name__ == "__main__":
    main()
