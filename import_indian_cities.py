"""Populate the cities table with Indian towns using the countries-states-cities dataset.

Run this script whenever you want to refresh the list of cities:

    python import_indian_cities.py

It downloads the latest CSV from
https://github.com/dr5hn/countries-states-cities-database
and imports rows where ``country_code == 'IN'``.
"""

from __future__ import annotations

import csv
import io
import sqlite3
import sys
import urllib.request
from pathlib import Path
from typing import Iterable, Tuple


DATA_URL = "https://raw.githubusercontent.com/dr5hn/countries-states-cities-database/master/csv/cities.csv"
DB_PATH = Path(__file__).with_name("car_rental.db")


def download_dataset() -> str:
    print("Downloading city dataset …", flush=True)
    with urllib.request.urlopen(DATA_URL) as response:  # type: ignore[call-arg]
        if response.status != 200:
            raise RuntimeError(f"Failed to download dataset (status {response.status})")
        data = response.read().decode("utf-8")
    return data


def transform_rows(csv_text: str) -> Iterable[Tuple[int, str, str, float | None, float | None]]:
    reader = csv.DictReader(io.StringIO(csv_text))
    for row in reader:
        if row.get("country_code") != "IN":
            continue
        try:
            city_id = int(row["id"])
        except (TypeError, ValueError):
            continue
        name = row.get("name", "").strip()
        if not name:
            continue
        state_name = (row.get("state_name") or "").strip()
        try:
            latitude = float(row["latitude"]) if row.get("latitude") else None
        except ValueError:
            latitude = None
        try:
            longitude = float(row["longitude"]) if row.get("longitude") else None
        except ValueError:
            longitude = None
        yield city_id, name, state_name, latitude, longitude


def ensure_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS cities (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            state TEXT,
            latitude REAL,
            longitude REAL
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_cities_name ON cities(name COLLATE NOCASE)"
    )


def main() -> None:
    csv_text = download_dataset()
    rows = list(transform_rows(csv_text))
    if not rows:
        print("No Indian cities were found in the dataset!", file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)
    try:
        ensure_table(conn)
        with conn:
            conn.execute("DELETE FROM cities")
            conn.executemany(
                "INSERT OR REPLACE INTO cities (id, name, state, latitude, longitude) VALUES (?, ?, ?, ?, ?)",
                rows,
            )
    finally:
        conn.close()

    print(f"Imported {len(rows)} Indian cities into {DB_PATH.name}.")


if __name__ == "__main__":
    main()
