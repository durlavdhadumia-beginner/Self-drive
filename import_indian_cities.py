"""Populate the cities table with Indian towns using multiple open datasets.

Datasets used:
1. Countries-States-Cities project
   https://github.com/dr5hn/countries-states-cities-database
   (provides larger cities and coordinates)
2. India Post PIN code directory
   https://github.com/sanand0/pincode
   (covers small branch offices mapped to PIN codes)

Run:
    python import_indian_cities.py

It downloads both CSVs, merges the entries, and writes them into the `cities`
table of `car_rental.db`. Town names are deduplicated by (name, state).
PIN codes from India Post are stored to enable postcode-based search later.
"""

from __future__ import annotations

import csv
import io
import sqlite3
import sys
import urllib.request
from pathlib import Path
from typing import Iterable, Tuple

PRIMARY_DATA_URL = (
    "https://raw.githubusercontent.com/dr5hn/countries-states-cities-database/master/csv/cities.csv"
)
PINCODE_DATA_URL = "https://raw.githubusercontent.com/sanand0/pincode/master/data/IN.csv"
DB_PATH = Path(__file__).with_name("car_rental.db")


def _download(url: str) -> str:
    with urllib.request.urlopen(url) as response:  # type: ignore[call-arg]
        if response.status != 200:
            raise RuntimeError(f"Failed to download {url} (status {response.status})")
        return response.read().decode("utf-8")


def download_primary_dataset() -> str:
    print("Downloading primary city dataset …", flush=True)
    return _download(PRIMARY_DATA_URL)


def download_pincode_dataset() -> str:
    print("Downloading pin code dataset …", flush=True)
    return _download(PINCODE_DATA_URL)


def transform_rows(
    primary_csv: str, pincode_csv: str
) -> Iterable[Tuple[int, str, str, float | None, float | None, str | None]]:
    seen: set[Tuple[str, str]] = set()

    # Primary dataset with coordinates
    primary_reader = csv.DictReader(io.StringIO(primary_csv))
    for row in primary_reader:
        if row.get("country_code") != "IN":
            continue
        try:
            city_id = int(row["id"])
        except (TypeError, ValueError):
            continue
        name = (row.get("name") or "").strip()
        if not name:
            continue
        state = (row.get("state_name") or "").strip()
        key = (name.lower(), state.lower())
        if key in seen:
            continue
        try:
            latitude = float(row["latitude"]) if row.get("latitude") else None
        except ValueError:
            latitude = None
        try:
            longitude = float(row["longitude"]) if row.get("longitude") else None
        except ValueError:
            longitude = None
        yield city_id, name, state, latitude, longitude, None
        seen.add(key)

    # India Post dataset – includes smaller towns/offices
    pincode_reader = csv.DictReader(io.StringIO(pincode_csv))
    for raw_row in pincode_reader:
        row = {k.strip().lower(): (v.strip() if isinstance(v, str) else v) for k, v in raw_row.items()}
        name = row.get("office name") or row.get("office_name") or ""
        state = row.get("state name") or row.get("state_name") or ""
        pincode = row.get("pincode")
        if not name or not state or not pincode:
            continue
        key = (name.lower(), state.lower())
        if key in seen:
            continue
        try:
            pin_int = int(pincode)
        except (TypeError, ValueError):
            continue
        city_id = 100_000_000 + pin_int  # Offset to avoid clashing with primary IDs
        yield city_id, name, state, None, None, pincode
        seen.add(key)


def ensure_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS cities (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            state TEXT,
            latitude REAL,
            longitude REAL,
            pincode TEXT
        )
        """
    )
    try:
        conn.execute("ALTER TABLE cities ADD COLUMN pincode TEXT")
    except sqlite3.OperationalError:
        pass
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cities_name ON cities(name COLLATE NOCASE)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cities_pincode ON cities(pincode)")


def main() -> None:
    try:
        primary_csv = download_primary_dataset()
        pincode_csv = download_pincode_dataset()
    except Exception as exc:
        print(f"Download failed: {exc}", file=sys.stderr)
        sys.exit(1)

    rows = list(transform_rows(primary_csv, pincode_csv))
    if not rows:
        print("No rows were generated from the datasets.", file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)
    try:
        ensure_table(conn)
        with conn:
            conn.execute("DELETE FROM cities")
            conn.executemany(
                """
                INSERT OR REPLACE INTO cities (id, name, state, latitude, longitude, pincode)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
    finally:
        conn.close()

    print(f"Imported {len(rows)} Indian cities/towns into {DB_PATH.name}.")


if __name__ == "__main__":
    main()
