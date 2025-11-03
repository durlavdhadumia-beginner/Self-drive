"""Populate the cities table with the countries-states-cities dataset.

Run this script whenever you want to refresh the list of Indian cities:

    python import_indian_cities.py
"""

from __future__ import annotations

import csv
import io
import os
import sqlite3
import sys
import urllib.request
from pathlib import Path
from typing import Iterable, Tuple

PRIMARY_DATA_URL = (
    "https://raw.githubusercontent.com/dr5hn/countries-states-cities-database/master/csv/cities.csv"
)
APP_ROOT = Path(__file__).resolve().parent
DATA_ROOT = Path(os.environ.get("CARRENTAL_DATA_DIR") or APP_ROOT.joinpath("data"))
DB_PATH = Path(os.environ.get("CARRENTAL_DB_PATH") or DATA_ROOT.joinpath("car_rental.db"))
DB_PATH.parent.mkdir(parents=True, exist_ok=True)


def download_primary_dataset() -> str:
    print("Downloading primary city dataset …", flush=True)
    with urllib.request.urlopen(PRIMARY_DATA_URL) as response:  # type: ignore[call-arg]
        if response.status != 200:
            raise RuntimeError(f"Failed to download dataset (status {response.status})")
        return response.read().decode("utf-8")


def transform_rows(primary_csv: str) -> Iterable[Tuple[int, str, str, float | None, float | None, str | None]]:
    reader = csv.DictReader(io.StringIO(primary_csv))
    for row in reader:
        if row.get("country_code") != "IN":
            continue
        try:
            city_id = int(row["id"])
        except (TypeError, ValueError):
            continue
        name = (row.get("name") or "").strip()
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
        yield city_id, name, state_name, latitude, longitude, None


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
    except Exception as exc:
        print(f"Download failed: {exc}", file=sys.stderr)
        sys.exit(1)

    rows = list(transform_rows(primary_csv))
    if not rows:
        print("No Indian cities were found in the dataset!", file=sys.stderr)
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

    print(f"Imported {len(rows)} Indian cities into {DB_PATH.name}.")


if __name__ == "__main__":
    main()
