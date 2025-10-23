"""Self-drive car rental platform with geolocation search and owner management."""

from __future__ import annotations

import json
import os
import re
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from math import asin, ceil, cos, radians, sin, sqrt
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

from flask import (
    Flask,
    abort,
    g,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    send_from_directory,
    url_for,
)
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.utils import secure_filename


DATABASE = Path(__file__).with_name("car_rental.db")
UPLOAD_ROOT = Path(__file__).with_name("static").joinpath("uploads")
USER_DOC_ROOT = UPLOAD_ROOT.joinpath("user_docs")
ALLOWED_EXTENSIONS = {"png", "jpg", "jpeg", "gif", "webp"}
DOCUMENT_EXTENSIONS = {"png", "jpg", "jpeg", "pdf"}
PROMO_CODES: Dict[str, float] = {
    "ZOOM10": 0.10,
    "WEEKEND15": 0.15,
    "FIRSTDRIVE": 0.20,
}
COMPANY_COMMISSION_RATE = 0.10
OWNER_INITIAL_PAYOUT_RATE = 0.10
OWNER_FINAL_PAYOUT_RATE = 0.90
HOST_SERVICE_FEE_RATE = 0.40
MAX_CAR_IMAGES = 8
POPULAR_VEHICLE_TYPES: List[str] = [
    "SUV",
    "Sedan",
    "Compact",
    "Hatchback",
    "Motorcycle",
    "Scooter",
    "Pickup",
    "Electric",
]
DEFAULT_FUEL_TYPES: List[str] = [
    "Petrol",
    "Diesel",
    "Electric",
    "Hybrid",
    "CNG",
    "LPG",
]
DELIVERY_DISTANCE_CHOICES: List[int] = [25, 50, 100, 200]

UPLOAD_ROOT.mkdir(parents=True, exist_ok=True)
USER_DOC_ROOT.mkdir(parents=True, exist_ok=True)

COMPANY_SUPPORT_EMAIL = "support@carrentalntravel.com"
COMPANY_SUPPORT_PHONE = "+91 98540 50567"
COMPANY_SUPPORT_WHATSAPP = "+919854050567"
COMPANY_UPI_ID = "carrentalntravel@ybl"
COMPANY_BANK_DETAILS: Dict[str, str] = {
    "bank_name": "HDFC Bank, Guwahati Branch",
    "account_name": "CarRentalNTravel Pvt Ltd",
    "account_number": "50100234567890",
    "ifsc": "HDFC0001234",
    "branch_address": "Zoo Tiniali, Guwahati, Assam 781003",
}

app = Flask(__name__)
app.config.update(
    SECRET_KEY="replace-with-a-secure-random-value",
    UPLOAD_FOLDER=str(UPLOAD_ROOT),
    MAX_CONTENT_LENGTH=32 * 1024 * 1024,  # 32 MB per request
)
app.logger.setLevel("INFO")


def naive_utcnow() -> datetime:
    """Return a timezone-naive UTC timestamp compatible with legacy data."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def naive_utcnow_iso() -> str:
    return naive_utcnow().isoformat()


@dataclass
class Car:
    id: int
    name: str
    latitude: float
    longitude: float
    distance_km: Optional[float]
    status: str
    rate_per_hour: float
    daily_rate: float
    seats: int
    owner_username: str
    owner_public_name: str
    city: str
    image_url: str
    vehicle_type: str
    size_category: str
    has_gps: bool
    fuel_type: str
    transmission: str
    rating: float
    description: str
    images: List[str] = field(default_factory=list)
    delivery_options: Dict[int, float] = field(default_factory=dict)


def get_db() -> sqlite3.Connection:
    if "db" not in g:
        g.db = sqlite3.connect(DATABASE)
        g.db.row_factory = sqlite3.Row
    return g.db


@app.teardown_appcontext
def close_db(_: BaseException | None) -> None:
    db = g.pop("db", None)
    if db is not None:
        db.close()


def normalize_contact(username: str) -> Tuple[str, str]:
    """Return contact type ('email' or 'phone') and normalized value."""
    value = username.strip()
    if not value:
        return ("", "")
    if "@" in value:
        return ("email", value.lower())
    digits = re.sub(r"\D", "", value)
    if digits.startswith("91") and len(digits) == 12:
        normalized = digits
    elif len(digits) == 10:
        normalized = f"91{digits}"
    else:
        return ("", "")
    if not re.fullmatch(r"91[6-9]\d{9}", normalized):
        return ("", "")
    return ("phone", normalized)


def first_non_empty(*values: Optional[str]) -> str:
    """Return the first truthy string value (trimmed) from the inputs."""
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def build_public_label(
    raw_value: Optional[str],
    *,
    fallback_prefix: str,
    fallback_id: Optional[int] = None,
) -> str:
    """Return a safe, non-contact label for displaying user names to other users."""
    text = (raw_value or "").strip()
    if text:
        contact_type, _ = normalize_contact(text)
        digits = re.sub(r"\D", "", text)
        # Treat long digit sequences as potential contact numbers.
        if not contact_type and len(digits) < 6 and "@" not in text:
            return text
    suffix = f" #{fallback_id}" if fallback_id else ""
    return f"{fallback_prefix}{suffix}".strip()


def _format_city_label(name: str, state: str) -> str:
    name_clean = (name or "").strip()
    state_clean = (state or "").strip()
    return f"{name_clean}, {state_clean}" if state_clean else name_clean


def load_city_entries(include_coordinates: bool = False) -> List[Dict[str, object]]:
    """Return city dictionaries with optional coordinates for dropdowns and maps."""
    db = get_db()
    rows = db.execute(
        "SELECT name, state, latitude, longitude FROM cities ORDER BY name"
    ).fetchall()
    entries: List[Dict[str, object]] = []
    seen: set[str] = set()

    def _append(name: str, state: str, latitude: Optional[float], longitude: Optional[float]) -> None:
        label = _format_city_label(name, state)
        key = label.lower()
        if not label or key in seen:
            return
        seen.add(key)
        entry: Dict[str, object] = {
            "name": name.strip(), "state": state.strip()}
        if include_coordinates:
            entry["latitude"] = float(
                latitude) if latitude is not None else None
            entry["longitude"] = float(
                longitude) if longitude is not None else None
        entries.append(entry)

    if rows:
        for row in rows:
            _append(row["name"] or "", row["state"] or "",
                    row["latitude"], row["longitude"])
    else:
        fallback_rows = db.execute(
            """
            SELECT city, latitude, longitude
            FROM cars
            WHERE city <> ''
            ORDER BY city
            """
        ).fetchall()
        for row in fallback_rows:
            _append(row["city"] or "", "", row["latitude"], row["longitude"])
    return entries


def load_vehicle_type_options() -> List[str]:
    db = get_db()
    rows = db.execute(
        "SELECT DISTINCT vehicle_type FROM cars WHERE vehicle_type <> '' ORDER BY vehicle_type"
    ).fetchall()
    return [row[0] for row in rows if row[0]]


def load_fuel_type_options() -> List[str]:
    db = get_db()
    rows = db.execute(
        "SELECT DISTINCT fuel_type FROM cars WHERE fuel_type <> '' ORDER BY fuel_type"
    ).fetchall()
    return [row[0] for row in rows if row[0]]


def build_fuel_type_list() -> List[str]:
    values = load_fuel_type_options()
    fuel_types: List[str] = []
    seen: set[str] = set()
    for value in [*DEFAULT_FUEL_TYPES, *values]:
        normalized = (value or "").strip()
        if not normalized:
            continue
        key = normalized.lower()
        if key in seen:
            continue
        seen.add(key)
        fuel_types.append(normalized)
    return fuel_types


def display_name(user: dict | sqlite3.Row | None) -> str:
    """Return a friendly display name for a user record."""
    if user is None:
        return ""
    if isinstance(user, dict):
        account = user.get("account_name") or ""
        username = user.get("username") or ""
    else:
        account = getattr(user, "account_name", "") or ""
        username = getattr(user, "username", "") or ""
    return account or username


def init_db() -> None:
    db = get_db()
    db.executescript(
        """
        PRAGMA foreign_keys = ON;

        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL CHECK(role IN ('owner', 'renter', 'both')),
            is_admin INTEGER NOT NULL DEFAULT 0,
            account_name TEXT NOT NULL DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS user_profiles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL UNIQUE,
            full_name TEXT NOT NULL DEFAULT '',
            date_of_birth TEXT,
            phone TEXT,
            govt_id_type TEXT,
            govt_id_number TEXT,
            driver_license TEXT,
            additional_id TEXT,
            address TEXT,
            vehicle_registration TEXT,
            gps_tracking INTEGER NOT NULL DEFAULT 1,
            profile_completed INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            profile_verified_at TEXT,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS user_documents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            doc_type TEXT DEFAULT '',
            filename TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS cities (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            state TEXT,
            latitude REAL,
            longitude REAL
        );

        CREATE TABLE IF NOT EXISTS user_payout_details (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL UNIQUE,
            account_holder TEXT DEFAULT '',
            account_number TEXT DEFAULT '',
            ifsc_code TEXT DEFAULT '',
            upi_id TEXT DEFAULT '',
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS company_payout_config (
            id INTEGER PRIMARY KEY CHECK(id = 1),
            account_holder TEXT DEFAULT '',
            account_number TEXT DEFAULT '',
            ifsc_code TEXT DEFAULT '',
            upi_id TEXT DEFAULT '',
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS cars (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            owner_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            brand TEXT NOT NULL,
            model TEXT NOT NULL,
            licence_plate TEXT NOT NULL,
            seats INTEGER NOT NULL DEFAULT 4,
            rate_per_hour REAL NOT NULL DEFAULT 0,
            daily_rate REAL NOT NULL DEFAULT 0,
            vehicle_type TEXT DEFAULT '',
            size_category TEXT DEFAULT '',
            has_gps INTEGER NOT NULL DEFAULT 1,
            latitude REAL NOT NULL,
            longitude REAL NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1,
            is_available INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            city TEXT DEFAULT '',
            image_url TEXT DEFAULT '',
            fuel_type TEXT DEFAULT '',
            transmission TEXT DEFAULT '',
            rating REAL DEFAULT 4.5,
            description TEXT DEFAULT '',
            FOREIGN KEY (owner_id) REFERENCES users(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS rentals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            car_id INTEGER NOT NULL,
            renter_id INTEGER NOT NULL,
            status TEXT NOT NULL CHECK(status IN ('booked', 'active', 'completed', 'cancelled')),
            start_time TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            end_time TEXT,
            promo_code TEXT DEFAULT '',
            discount_amount REAL NOT NULL DEFAULT 0,
            rental_amount REAL NOT NULL DEFAULT 0,
            delivery_type TEXT NOT NULL DEFAULT 'pickup',
            delivery_fee REAL NOT NULL DEFAULT 0,
            delivery_distance_km REAL DEFAULT 0,
            delivery_latitude REAL,
            delivery_longitude REAL,
            delivery_address TEXT DEFAULT '',
            trip_destinations TEXT NOT NULL DEFAULT '[]',
            total_amount REAL NOT NULL DEFAULT 0,
            owner_response TEXT NOT NULL DEFAULT 'pending',
            owner_response_at TEXT,
            counter_amount REAL,
            counter_comment TEXT DEFAULT '',
            counter_used INTEGER NOT NULL DEFAULT 0,
            payment_status TEXT NOT NULL DEFAULT 'pending',
            payment_due_at TEXT,
            payment_confirmed_at TEXT,
            payment_channel TEXT DEFAULT 'manual',
            payment_gateway TEXT DEFAULT '',
            payment_reference TEXT DEFAULT '',
            payment_order_id TEXT DEFAULT '',
            company_commission_amount REAL NOT NULL DEFAULT 0,
            owner_payout_amount REAL NOT NULL DEFAULT 0,
            owner_payout_status TEXT NOT NULL DEFAULT 'pending',
            owner_payout_released_at TEXT,
            owner_initial_payout_amount REAL NOT NULL DEFAULT 0,
            owner_initial_payout_status TEXT NOT NULL DEFAULT 'pending',
            owner_initial_payout_released_at TEXT,
            owner_final_payout_amount REAL NOT NULL DEFAULT 0,
            owner_final_payout_status TEXT NOT NULL DEFAULT 'pending',
            owner_final_payout_released_at TEXT,
            renter_response TEXT DEFAULT '',
            renter_response_at TEXT,
            owner_started_at TEXT,
            completed_at TEXT,
            cancel_reason TEXT DEFAULT '',
            FOREIGN KEY (car_id) REFERENCES cars(id) ON DELETE CASCADE,
            FOREIGN KEY (renter_id) REFERENCES users(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS car_images (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            car_id INTEGER NOT NULL,
            filename TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (car_id) REFERENCES cars(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS car_delivery_options (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            car_id INTEGER NOT NULL,
            distance_km INTEGER NOT NULL,
            price REAL NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (car_id) REFERENCES cars(id) ON DELETE CASCADE,
            UNIQUE(car_id, distance_km)
        );

        CREATE TABLE IF NOT EXISTS notifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            message TEXT NOT NULL,
            link TEXT DEFAULT '',
            is_read INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS complaints (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rental_id INTEGER NOT NULL,
            submitted_by INTEGER NOT NULL,
            target_user_id INTEGER NOT NULL,
            role TEXT NOT NULL CHECK(role IN ('renter', 'owner')),
            category TEXT NOT NULL,
            description TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'open',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            resolved_at TEXT,
            resolution TEXT DEFAULT '',
            FOREIGN KEY (rental_id) REFERENCES rentals(id) ON DELETE CASCADE,
            FOREIGN KEY (submitted_by) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY (target_user_id) REFERENCES users(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS support_feedback (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            role TEXT NOT NULL CHECK(role IN ('renter', 'owner', 'both')),
            category TEXT NOT NULL,
            description TEXT NOT NULL,
            rental_id INTEGER,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY (rental_id) REFERENCES rentals(id) ON DELETE SET NULL
        );

        CREATE TABLE IF NOT EXISTS reviews (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rental_id INTEGER NOT NULL,
            reviewer_id INTEGER NOT NULL,
            target_user_id INTEGER NOT NULL,
            reviewer_role TEXT NOT NULL,
            target_role TEXT NOT NULL,
            trip_rating INTEGER,
            car_rating INTEGER,
            owner_rating INTEGER,
            passenger_rating INTEGER,
            comment TEXT DEFAULT '',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (rental_id) REFERENCES rentals(id) ON DELETE CASCADE,
            FOREIGN KEY (reviewer_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY (target_user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        """
    )

    alter_statements = [
        "ALTER TABLE users ADD COLUMN is_admin INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE users ADD COLUMN account_name TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE cars ADD COLUMN city TEXT DEFAULT ''",
        "ALTER TABLE cars ADD COLUMN image_url TEXT DEFAULT ''",
        "ALTER TABLE cars ADD COLUMN fuel_type TEXT DEFAULT ''",
        "ALTER TABLE cars ADD COLUMN transmission TEXT DEFAULT ''",
        "ALTER TABLE cars ADD COLUMN rating REAL DEFAULT 4.5",
        "ALTER TABLE cars ADD COLUMN description TEXT DEFAULT ''",
        "ALTER TABLE cars ADD COLUMN daily_rate REAL DEFAULT 0",
        "ALTER TABLE cars ADD COLUMN vehicle_type TEXT DEFAULT ''",
        "ALTER TABLE cars ADD COLUMN size_category TEXT DEFAULT ''",
        "ALTER TABLE cars ADD COLUMN has_gps INTEGER NOT NULL DEFAULT 1",
        "ALTER TABLE rentals ADD COLUMN promo_code TEXT DEFAULT ''",
        "ALTER TABLE rentals ADD COLUMN discount_amount REAL NOT NULL DEFAULT 0",
        "ALTER TABLE rentals ADD COLUMN rental_amount REAL NOT NULL DEFAULT 0",
        "ALTER TABLE rentals ADD COLUMN delivery_type TEXT NOT NULL DEFAULT 'pickup'",
        "ALTER TABLE rentals ADD COLUMN delivery_fee REAL NOT NULL DEFAULT 0",
        "ALTER TABLE rentals ADD COLUMN delivery_distance_km REAL DEFAULT 0",
        "ALTER TABLE rentals ADD COLUMN delivery_latitude REAL",
        "ALTER TABLE rentals ADD COLUMN delivery_longitude REAL",
        "ALTER TABLE rentals ADD COLUMN delivery_address TEXT DEFAULT ''",
        "ALTER TABLE rentals ADD COLUMN trip_destinations TEXT NOT NULL DEFAULT '[]'",
        "ALTER TABLE rentals ADD COLUMN total_amount REAL NOT NULL DEFAULT 0",
        "ALTER TABLE rentals ADD COLUMN owner_response TEXT NOT NULL DEFAULT 'pending'",
        "ALTER TABLE rentals ADD COLUMN owner_response_at TEXT",
        "ALTER TABLE rentals ADD COLUMN counter_amount REAL",
        "ALTER TABLE rentals ADD COLUMN counter_comment TEXT DEFAULT ''",
        "ALTER TABLE rentals ADD COLUMN counter_used INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE rentals ADD COLUMN payment_status TEXT NOT NULL DEFAULT 'pending'",
        "ALTER TABLE rentals ADD COLUMN payment_due_at TEXT",
        "ALTER TABLE rentals ADD COLUMN payment_confirmed_at TEXT",
        "ALTER TABLE rentals ADD COLUMN payment_channel TEXT DEFAULT 'manual'",
        "ALTER TABLE rentals ADD COLUMN payment_gateway TEXT DEFAULT ''",
        "ALTER TABLE rentals ADD COLUMN payment_reference TEXT DEFAULT ''",
        "ALTER TABLE rentals ADD COLUMN payment_order_id TEXT DEFAULT ''",
        "ALTER TABLE rentals ADD COLUMN company_commission_amount REAL NOT NULL DEFAULT 0",
        "ALTER TABLE rentals ADD COLUMN owner_payout_amount REAL NOT NULL DEFAULT 0",
        "ALTER TABLE rentals ADD COLUMN owner_payout_status TEXT NOT NULL DEFAULT 'pending'",
        "ALTER TABLE rentals ADD COLUMN owner_payout_released_at TEXT",
        "ALTER TABLE rentals ADD COLUMN owner_initial_payout_amount REAL NOT NULL DEFAULT 0",
        "ALTER TABLE rentals ADD COLUMN owner_initial_payout_status TEXT NOT NULL DEFAULT 'pending'",
        "ALTER TABLE rentals ADD COLUMN owner_initial_payout_released_at TEXT",
        "ALTER TABLE rentals ADD COLUMN owner_final_payout_amount REAL NOT NULL DEFAULT 0",
        "ALTER TABLE rentals ADD COLUMN owner_final_payout_status TEXT NOT NULL DEFAULT 'pending'",
        "ALTER TABLE rentals ADD COLUMN owner_final_payout_released_at TEXT",
        "ALTER TABLE rentals ADD COLUMN renter_response TEXT DEFAULT ''",
        "ALTER TABLE rentals ADD COLUMN renter_response_at TEXT",
        "ALTER TABLE rentals ADD COLUMN owner_started_at TEXT",
        "ALTER TABLE rentals ADD COLUMN completed_at TEXT",
        "ALTER TABLE rentals ADD COLUMN cancel_reason TEXT DEFAULT ''",
        "ALTER TABLE user_profiles ADD COLUMN profile_verified_at TEXT",
        "ALTER TABLE user_documents ADD COLUMN doc_type TEXT DEFAULT ''"
    ]
    for statement in alter_statements:
        try:
            db.execute(statement)
        except sqlite3.OperationalError:
            pass
    db.commit()
    try:
        db.execute(
            "UPDATE users SET account_name = username WHERE account_name IS NULL OR account_name = ''"
        )
    except sqlite3.OperationalError:
        pass
    try:
        db.execute(
            "UPDATE car_images SET filename = substr(filename, 9) WHERE filename LIKE 'uploads/%'"
        )
    except sqlite3.OperationalError:
        pass
    db.commit()
    db.execute(
        "CREATE INDEX IF NOT EXISTS idx_cities_name ON cities(name COLLATE NOCASE)"
    )
    db.commit()
    seed_cities_if_needed(db)
    db.execute(
        "INSERT OR IGNORE INTO company_payout_config (id, updated_at) VALUES (1, ?)",
        (naive_utcnow_iso(),),
    )
    db.commit()


def allowed_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def save_car_images(car_id: int, files: List, limit: Optional[int] = None) -> List[str]:
    if not files:
        return []
    if limit is None:
        limit = MAX_CAR_IMAGES
    if limit <= 0:
        return []
    saved_paths: List[str] = []
    target_dir = UPLOAD_ROOT.joinpath(str(car_id))
    target_dir.mkdir(parents=True, exist_ok=True)
    db = get_db()
    for upload in files[:limit]:
        if not upload or not upload.filename:
            continue
        if not allowed_file(upload.filename):
            continue
        safe_name = secure_filename(upload.filename)
        timestamp = naive_utcnow().strftime("%Y%m%d%H%M%S%f")
        filename = f"{timestamp}_{safe_name}"
        filepath = target_dir.joinpath(filename)
        upload.save(filepath)
        relative_path = f"{car_id}/{filename}"
        db.execute(
            "INSERT INTO car_images (car_id, filename) VALUES (?, ?)",
            (car_id, relative_path),
        )
        saved_paths.append(relative_path)
    db.commit()
    return saved_paths


def save_user_documents(user_id: int, files: List, types: List[str]) -> List[str]:
    saved = []
    if not files:
        return saved
    target = USER_DOC_ROOT.joinpath(str(user_id))
    target.mkdir(parents=True, exist_ok=True)
    db = get_db()
    for index, upload in enumerate(files):
        if not upload or not upload.filename:
            continue
        filename = upload.filename
        ext = filename.rsplit('.', 1)[-1].lower() if '.' in filename else ''
        if ext not in DOCUMENT_EXTENSIONS:
            continue
        safe_name = secure_filename(filename)
        timestamp = naive_utcnow().strftime("%Y%m%d%H%M%S%f")
        final_name = f"{timestamp}_{index}_{safe_name}"
        path = target.joinpath(final_name)
        upload.save(path)
        doc_type = ''
        if index < len(types):
            doc_type = types[index].strip()[:80]
        db.execute(
            "INSERT INTO user_documents (user_id, doc_type, filename) VALUES (?, ?, ?)",
            (user_id, doc_type, f"user_docs/{user_id}/{final_name}"),
        )
        saved.append(final_name)
    db.commit()
    return saved


def fetch_user_documents(user_id: int) -> List[sqlite3.Row]:
    db = get_db()
    return db.execute(
        "SELECT * FROM user_documents WHERE user_id = ? ORDER BY created_at",
        (user_id,),
    ).fetchall()


def fetch_car_images(car_ids: List[int]) -> Dict[int, List[str]]:
    if not car_ids:
        return {}
    placeholders = ",".join("?" for _ in car_ids)
    db = get_db()
    rows = db.execute(
        f"SELECT car_id, filename FROM car_images WHERE car_id IN ({placeholders}) ORDER BY created_at",
        car_ids,
    ).fetchall()
    images: Dict[int, List[str]] = {}
    for row in rows:
        images.setdefault(row["car_id"], []).append(row["filename"])
    return images


def fetch_car_delivery_options(car_ids: List[int]) -> Dict[int, Dict[int, float]]:
    if not car_ids:
        return {}
    placeholders = ",".join("?" for _ in car_ids)
    db = get_db()
    rows = db.execute(
        f"SELECT car_id, distance_km, price FROM car_delivery_options WHERE car_id IN ({placeholders})",
        car_ids,
    ).fetchall()
    options: Dict[int, Dict[int, float]] = {}
    for row in rows:
        options.setdefault(row["car_id"], {})[
            row["distance_km"]] = row["price"]
    return options


def save_car_delivery_options(car_id: int, options: Dict[int, float]) -> None:
    db = get_db()
    db.execute("DELETE FROM car_delivery_options WHERE car_id = ?", (car_id,))
    now_iso = naive_utcnow_iso()
    for distance, price in options.items():
        db.execute(
            """
            INSERT INTO car_delivery_options (car_id, distance_km, price, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (car_id, distance, price, now_iso, now_iso),
        )


def ensure_user_profile(user_id: int) -> dict:
    db = get_db()
    profile = db.execute(
        "SELECT * FROM user_profiles WHERE user_id = ?",
        (user_id,),
    ).fetchone()
    if profile is None:
        db.execute("INSERT INTO user_profiles (user_id) VALUES (?)", (user_id,))
        db.commit()
        profile = db.execute(
            "SELECT * FROM user_profiles WHERE user_id = ?",
            (user_id,),
        ).fetchone()
    return dict(profile)


def ensure_user_payout(user_id: int) -> dict:
    db = get_db()
    payout = db.execute(
        "SELECT * FROM user_payout_details WHERE user_id = ?",
        (user_id,),
    ).fetchone()
    if payout is None:
        db.execute(
            "INSERT INTO user_payout_details (user_id) VALUES (?)", (user_id,))
        db.commit()
        payout = db.execute(
            "SELECT * FROM user_payout_details WHERE user_id = ?",
            (user_id,),
        ).fetchone()
    return dict(payout)


def get_company_payout_details() -> dict:
    db = get_db()
    row = db.execute(
        "SELECT * FROM company_payout_config WHERE id = 1",
    ).fetchone()
    if row is None:
        db.execute(
            "INSERT INTO company_payout_config (id, updated_at) VALUES (1, ?)",
            (naive_utcnow_iso(),),
        )
        db.commit()
        row = db.execute(
            "SELECT * FROM company_payout_config WHERE id = 1").fetchone()
    return dict(row)


def get_primary_admin_payout_details() -> dict:
    """Return payout preferences configured on the primary admin profile."""
    db = get_db()
    row = db.execute(
        """
        SELECT
            users.id AS user_id,
            COALESCE(
                NULLIF(upd.account_holder, ''),
                NULLIF(users.account_name, ''),
                NULLIF(profile.full_name, ''),
                users.username
            ) AS account_holder,
            COALESCE(upd.account_number, '') AS account_number,
            COALESCE(upd.ifsc_code, '') AS ifsc_code,
            COALESCE(upd.upi_id, '') AS upi_id,
            upd.updated_at AS updated_at
        FROM users
        LEFT JOIN user_payout_details AS upd ON upd.user_id = users.id
        LEFT JOIN user_profiles AS profile ON profile.user_id = users.id
        WHERE users.is_admin = 1
        ORDER BY
            CASE
                WHEN COALESCE(upd.account_number, '') != '' AND COALESCE(upd.ifsc_code, '') != '' THEN 0
                ELSE 1
            END,
            CASE
                WHEN COALESCE(upd.upi_id, '') != '' THEN 0
                ELSE 1
            END,
            COALESCE(upd.updated_at, '') DESC,
            users.id ASC
        LIMIT 1
        """,
    ).fetchone()
    if row is None:
        return {}
    data = dict(row)
    data["account_holder"] = (data.get("account_holder") or "").strip()
    data["account_number"] = (data.get("account_number") or "").strip()
    data["ifsc_code"] = (data.get("ifsc_code") or "").strip().upper()
    data["upi_id"] = (data.get("upi_id") or "").strip()
    return data


def seed_cities_if_needed(conn: sqlite3.Connection) -> None:
    try:
        existing = conn.execute("SELECT COUNT(*) FROM cities").fetchone()[0]
    except sqlite3.OperationalError:
        return
    if existing:
        return
    try:
        from import_indian_cities import (
            download_primary_dataset,
            transform_rows,
        )
    except Exception as exc:  # pragma: no cover
        print(f"City import skipped (module unavailable): {exc}")
        return
    try:
        primary_csv = download_primary_dataset()
        rows = list(transform_rows(primary_csv))
    except Exception as exc:  # pragma: no cover
        print(f"City import skipped (download failed): {exc}")
        return
    if not rows:
        return
    conn.execute("DELETE FROM cities")
    conn.executemany(
        """
        INSERT OR REPLACE INTO cities (id, name, state, latitude, longitude, pincode)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()
    print(f"Seeded {len(rows)} Indian cities.")


def profile_is_complete(profile: dict | None) -> bool:
    if profile is None:
        return False

    def _value(source, key):
        if isinstance(source, dict):
            return source.get(key)
        return getattr(source, key, None)

    required_fields = [
        _value(profile, "full_name"),
        _value(profile, "phone"),
        _value(profile, "email_contact"),
    ]
    docs_required = 0
    if getattr(g, "user", None) and has_role("owner"):
        docs_required = 3
    if getattr(g, "user", None) and has_role("renter") and not has_role("owner"):
        docs_required = 3
    if docs_required:
        db = get_db()
        count = db.execute(
            "SELECT COUNT(*) FROM user_documents WHERE user_id = ?",
            (g.user["id"],),
        ).fetchone()[0]
        if count < docs_required:
            return False
    return all(field and str(field).strip() for field in required_fields)


def create_notification(user_id: int, message: str, link: str = "") -> None:
    db = get_db()
    db.execute(
        "INSERT INTO notifications (user_id, message, link) VALUES (?, ?, ?)",
        (user_id, message, link),
    )
    db.commit()


def mark_notifications_read(user_id: int) -> None:
    db = get_db()
    db.execute(
        "UPDATE notifications SET is_read = 1 WHERE user_id = ?", (user_id,))
    db.commit()


def login_required(view: Callable) -> Callable:
    def wrapped(*args, **kwargs):
        if g.user is None:
            return redirect(url_for("login"))
        return view(*args, **kwargs)

    wrapped.__name__ = view.__name__
    return wrapped


def role_required(*required_roles: str) -> Callable:
    allowed = {role for role in required_roles if role}

    def decorator(view: Callable) -> Callable:
        def wrapped(*args, **kwargs):
            if g.user is None:
                return redirect(url_for("login"))
            if isinstance(g.user, dict):
                is_admin_flag = bool(g.user.get("is_admin"))
                user_role = g.user.get("role")
            else:
                is_admin_flag = bool(g.user["is_admin"])
                user_role = g.user["role"]
            if is_admin_flag:
                return view(*args, **kwargs)
            if not allowed:
                return view(*args, **kwargs)
            if user_role == "both":
                return view(*args, **kwargs)
            if user_role not in allowed:
                abort(403)
            return view(*args, **kwargs)

        wrapped.__name__ = view.__name__
        return wrapped

    return decorator


def admin_required(view: Callable) -> Callable:
    def wrapped(*args, **kwargs):
        if g.user is None or not g.user.get("is_admin"):
            abort(403)
        return view(*args, **kwargs)

    wrapped.__name__ = view.__name__
    return wrapped


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius = 6371.0
    lat1_rad, lon1_rad, lat2_rad, lon2_rad = map(
        radians, [lat1, lon1, lat2, lon2])
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    a = sin(dlat / 2) ** 2 + cos(lat1_rad) * cos(lat2_rad) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    return radius * c


def reverse_geocode_city(latitude: float, longitude: float) -> Optional[str]:
    try:
        import reverse_geocoder as rg  # type: ignore
    except ImportError:
        return None
    try:
        result = rg.search((latitude, longitude), mode=1)
    except Exception:
        return None
    if result:
        return result[0].get("name")
    return None


def lookup_city_coordinates(city_name: str) -> Optional[Tuple[float, float]]:
    """Return latitude/longitude for a city, tolerating state or region suffixes."""
    cleaned = (city_name or "").strip()
    if not cleaned:
        return None
    db = get_db()

    def _query(value: str, comparator: str = "=") -> Optional[Tuple[float, float]]:
        row = db.execute(
            f"""
            SELECT latitude, longitude
            FROM cities
            WHERE LOWER(name) {comparator} LOWER(?)
              AND latitude IS NOT NULL
              AND longitude IS NOT NULL
            ORDER BY (pincode IS NULL), pincode
            LIMIT 1
            """,
            (value,),
        ).fetchone()
        if row and row["latitude"] is not None and row["longitude"] is not None:
            return float(row["latitude"]), float(row["longitude"])
        return None

    lowered = cleaned.lower()

    exact = _query(lowered)
    if exact:
        return exact

    tokens = [part.strip()
              for part in re.split(r",|\s+", lowered) if part.strip()]
    for size in range(len(tokens), 0, -1):
        attempt = " ".join(tokens[:size])
        result = _query(attempt)
        if result:
            return result

    prefix = _query(lowered + "%", comparator="LIKE")
    if prefix:
        return prefix

    substring = _query("%" + lowered + "%", comparator="LIKE")
    if substring:
        return substring

    return None


def fetch_available_cars(
    *,
    latitude: float,
    longitude: float,
    radius_km: float,
    city: Optional[str] = None,
    price_min: Optional[float] = None,
    price_max: Optional[float] = None,
    vehicle_types: Optional[List[str]] = None,
    seat_min: Optional[int] = None,
    seat_max: Optional[int] = None,
    require_gps: bool = False,
    fuel_types: Optional[List[str]] = None,
) -> List[Car]:
    db = get_db()
    params: List[object] = []
    predicates: List[str] = ["cars.is_active = 1"]

    if city:
        predicates.append("LOWER(cars.city) = LOWER(?)")
        params.append(city)
    if vehicle_types:
        placeholders = ",".join("?" for _ in vehicle_types)
        predicates.append(f"LOWER(cars.vehicle_type) IN ({placeholders})")
        params.extend([vt.lower() for vt in vehicle_types])
    if require_gps:
        predicates.append("cars.has_gps = 1")
    if seat_min is not None:
        predicates.append("cars.seats >= ?")
        params.append(seat_min)
    if seat_max is not None:
        predicates.append("cars.seats <= ?")
        params.append(seat_max)
    if price_min is not None:
        predicates.append("cars.rate_per_hour >= ?")
        params.append(price_min)
    if price_max is not None:
        predicates.append("cars.rate_per_hour <= ?")
        params.append(price_max)
    if fuel_types:
        placeholders = ",".join("?" for _ in fuel_types)
        predicates.append(f"LOWER(cars.fuel_type) IN ({placeholders})")
        params.extend([ft.lower() for ft in fuel_types])

    where_clause = " AND ".join(predicates)

    rows = db.execute(
        f"""
        SELECT cars.*,
               users.username AS owner_username,
               COALESCE(users.account_name, '') AS owner_account_name,
               EXISTS(
                   SELECT 1 FROM rentals
                   WHERE rentals.car_id = cars.id
                     AND rentals.status IN ('booked', 'active')
               ) AS has_active_rental
        FROM cars
        JOIN users ON users.id = cars.owner_id
        WHERE {where_clause}
        """,
        params,
    ).fetchall()

    car_ids = [row["id"] for row in rows]
    images_by_car = fetch_car_images(car_ids)
    delivery_by_car = fetch_car_delivery_options(car_ids)

    cars: List[Car] = []
    for row in rows:
        distance = haversine_km(latitude, longitude,
                                row["latitude"], row["longitude"])
        if radius_km and distance > radius_km:
            continue
        is_available = row["is_available"] and not row["has_active_rental"]
        owner_label = build_public_label(
            row["owner_account_name"] or row["owner_username"],
            fallback_prefix="Host",
            fallback_id=row["owner_id"],
        )
        status = "Available" if is_available else "Unavailable"
        cars.append(
            Car(
                id=row["id"],
                name=row["name"] or f"{row['brand']} {row['model']}",
                latitude=row["latitude"],
                longitude=row["longitude"],
                distance_km=round(distance, 2),
                status=status,
                rate_per_hour=row["rate_per_hour"],
                daily_rate=row["daily_rate"] or row["rate_per_hour"] * 24,
                seats=row["seats"],
                owner_username=row["owner_username"],
                owner_public_name=owner_label,
                city=row["city"] or "",
                image_url=row["image_url"] or "",
                vehicle_type=row["vehicle_type"] or "",
                size_category=row["size_category"] or "",
                has_gps=bool(row["has_gps"]),
                fuel_type=row["fuel_type"] or "",
                transmission=row["transmission"] or "",
                rating=row["rating"] or 4.5,
                description=row["description"] or "",
                images=images_by_car.get(row["id"], []),
                delivery_options=delivery_by_car.get(row["id"], {}),
            )
        )
    cars.sort(key=lambda car: car.distance_km or 0)
    return cars


def has_role(role: str) -> bool:
    if g.user is None:
        return False
    user_role = g.user["role"]
    if role == "owner":
        return user_role in ("owner", "both")
    if role == "renter":
        return user_role in ("renter", "both")
    return False


app.jinja_env.globals.update(
    has_role=has_role,
    now=naive_utcnow,
    promo_codes=PROMO_CODES,
    profile_is_complete=profile_is_complete,
    support_phone=COMPANY_SUPPORT_PHONE,
    support_email=COMPANY_SUPPORT_EMAIL,
    support_whatsapp=COMPANY_SUPPORT_WHATSAPP,
    company_upi=COMPANY_UPI_ID,
    company_bank=COMPANY_BANK_DETAILS,
)


def parse_datetime(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed.isoformat()


def parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def parse_float(value: Optional[str], default: Optional[float] = None) -> Optional[float]:
    if value in (None, ""):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def parse_int(value: Optional[str]) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def calculate_pricing(
    rate_per_hour: float,
    daily_rate: float,
    start: datetime,
    end: datetime,
    promo_code: Optional[str] = None,
) -> Dict[str, object]:
    if end <= start:
        end = start + timedelta(hours=4)
    hours = (end - start).total_seconds() / 3600
    hours = max(1.0, hours)

    effective_daily = daily_rate or rate_per_hour * 24
    full_days = int(hours // 24)
    remaining_hours = hours - (full_days * 24)
    remaining_hours = ceil(remaining_hours) if remaining_hours > 0 else 0

    base_total = full_days * effective_daily
    if remaining_hours:
        base_total += min(effective_daily, remaining_hours * rate_per_hour)

    packages = []
    for package_hours in (4, 8, 24):
        package_price = min(effective_daily, rate_per_hour * package_hours)
        packages.append(
            {
                "label": f"{package_hours}-hour pack" if package_hours != 24 else "Full day",
                "hours": package_hours,
                "price": round(package_price, 2),
            }
        )

    discount = 0.0
    applied = None
    if promo_code:
        code = promo_code.upper().strip()
        if code in PROMO_CODES:
            discount = round(base_total * PROMO_CODES[code], 2)
            applied = code

    total = max(0.0, round(base_total - discount, 2))
    return {
        "hours": round(hours, 2),
        "base_amount": round(base_total, 2),
        "discount": discount,
        "total": total,
        "promo_applied": applied,
        "packages": packages,
    }


@app.before_request
def load_logged_in_user() -> None:
    user_id = session.get("user_id")
    if user_id is None:
        g.user = None
        g.profile = None
        g.unread_notifications = 0
        g.profile_complete = False
        return
    db = get_db()
    user_row = db.execute(
        "SELECT id, username, role, is_admin, account_name FROM users WHERE id = ?",
        (user_id,),
    ).fetchone()
    if user_row is None:
        g.user = None
        g.profile = None
        g.unread_notifications = 0
        g.profile_complete = False
        return
    g.user = dict(user_row)
    if not g.user.get("account_name"):
        g.user["account_name"] = g.user.get("username", "")
    g.user["display_name"] = g.user.get(
        "account_name") or g.user.get("username", "")
    profile_row = db.execute(
        "SELECT * FROM user_profiles WHERE user_id = ?",
        (user_id,),
    ).fetchone()
    if profile_row is None:
        profile_row = ensure_user_profile(user_id)
    g.profile = dict(profile_row)
    g.payout_details = ensure_user_payout(user_id)
    g.unread_notifications = db.execute(
        "SELECT COUNT(*) FROM notifications WHERE user_id = ? AND is_read = 0",
        (user_id,),
    ).fetchone()[0]
    g.profile_complete = profile_is_complete(g.profile)


@app.route("/profile", methods=["GET", "POST"])
@login_required
def profile() -> str:
    db = get_db()
    try:
        db.execute(
            "ALTER TABLE user_profiles ADD COLUMN email_contact TEXT DEFAULT ''")
        db.commit()
    except sqlite3.OperationalError:
        pass
    profile_row = ensure_user_profile(g.user["id"])
    payout_row = ensure_user_payout(g.user["id"])
    fallback_contact = g.user.get("username") if g.user else ""
    if not profile_row.get("email_contact"):
        profile_row["email_contact"] = fallback_contact or profile_row.get(
            "phone", "")
    g.profile = profile_row
    message = request.args.get("message")
    error = None
    documents = fetch_user_documents(g.user["id"])
    missing_fields: List[str] = []
    missing_keys: List[str] = []
    if request.method == "POST":
        form = request.form
        full_name = form.get("full_name", "").strip()
        date_of_birth = form.get("date_of_birth", "").strip()
        email_contact = form.get("email_contact", "").strip()
        phone = form.get("phone", "").strip()
        address = form.get("address", "").strip()
        vehicle_registration_raw = form.get("vehicle_registration")
        if vehicle_registration_raw is None:
            vehicle_registration = (profile_row.get("vehicle_registration") or "").strip()
        else:
            vehicle_registration = vehicle_registration_raw.strip()
        gps_tracking = 1 if form.get("gps_tracking") else 0
        account_holder = form.get("account_holder", "").strip()
        account_number = form.get("account_number", "").strip()
        ifsc_code = form.get("ifsc_code", "").strip().upper()
        upi_id = form.get("upi_id", "").strip()
        doc_types = form.getlist("doc_type")
        doc_files = request.files.getlist("doc_files")
        if not full_name:
            missing_fields.append("full name")
            missing_keys.append("full_name")
        if not phone:
            missing_fields.append("mobile number")
            missing_keys.append("phone")
        if not email_contact:
            missing_fields.append("email ID or account contact")
            missing_keys.append("email_contact")
        if not ((account_number and ifsc_code) or upi_id):
            missing_fields.append(
                "payout details (bank account + IFSC or UPI ID)")
            missing_keys.extend(["account_number", "ifsc_code", "upi_id"])
        db.execute(
            """
            UPDATE user_profiles
            SET full_name = ?, date_of_birth = ?, phone = ?, address = ?,
                vehicle_registration = ?, gps_tracking = ?, profile_completed = ?,
                updated_at = ?, email_contact = ?
            WHERE user_id = ?
            """,
            (
                full_name,
                date_of_birth,
                phone,
                address,
                vehicle_registration,
                gps_tracking,
                0 if missing_fields else 1,
                naive_utcnow_iso(),
                email_contact,
                g.user["id"],
            ),
        )
        db.commit()
        db.execute(
            """
            UPDATE user_payout_details
            SET account_holder = ?, account_number = ?, ifsc_code = ?, upi_id = ?, updated_at = ?
            WHERE user_id = ?
            """,
            (
                account_holder or full_name,
                account_number,
                ifsc_code,
                upi_id,
                naive_utcnow_iso(),
                g.user["id"],
            ),
        )
        db.commit()
        if any(getattr(f, "filename", "") for f in doc_files):
            save_user_documents(g.user["id"], doc_files, doc_types)
        profile_row = ensure_user_profile(g.user["id"])
        if not profile_row.get("email_contact"):
            profile_row["email_contact"] = email_contact or (
                g.user.get("username") if g.user else "")
        payout_row = ensure_user_payout(g.user["id"])
        g.profile = profile_row
        g.profile_complete = profile_is_complete(profile_row)
        documents = fetch_user_documents(g.user["id"])
        if missing_fields:
            if len(missing_fields) == 1:
                missing_text = missing_fields[0]
            else:
                missing_text = ", ".join(
                    missing_fields[:-1]) + f" and {missing_fields[-1]}"
            error = f"Please add the following to finish your profile: {missing_text}."
            message = None
        else:
            message = "Profile updated successfully."
    return render_template(
        "profile.html",
        profile=profile_row,
        payout=payout_row,
        message=message,
        error=error,
        is_owner=has_role("owner"),
        documents=documents,
        missing_fields=missing_fields,
        missing_keys=missing_keys,
    )


@app.post("/profile/documents/<int:doc_id>/delete")
@login_required
def profile_delete_document(doc_id: int) -> str:
    db = get_db()
    doc = db.execute(
        "SELECT * FROM user_documents WHERE id = ? AND user_id = ?",
        (doc_id, g.user["id"]),
    ).fetchone()
    if doc is None:
        abort(404)
    stored_path = UPLOAD_ROOT.joinpath(doc["filename"])
    try:
        stored_path.unlink(missing_ok=True)  # type: ignore[attr-defined]
    except FileNotFoundError:
        pass
    db.execute("DELETE FROM user_documents WHERE id = ?", (doc_id,))
    db.commit()
    g.profile = ensure_user_profile(g.user["id"])
    g.profile_complete = profile_is_complete(g.profile)
    return redirect(url_for("profile"))


@app.route("/notifications", methods=["GET", "POST"])
@login_required
def notifications() -> str:
    db = get_db()
    if request.method == "POST":
        mark_notifications_read(g.user["id"])
        g.unread_notifications = 0
        return redirect(url_for("notifications"))
    rows = db.execute(
        "SELECT id, message, link, is_read, created_at FROM notifications WHERE user_id = ? ORDER BY created_at DESC",
        (g.user["id"],),
    ).fetchall()
    return render_template("notifications.html", notifications=rows)


def build_admin_dashboard_context() -> dict:
    db = get_db()
    metrics = {
        "users": db.execute("SELECT COUNT(*) FROM users").fetchone()[0],
        "owners": db.execute("SELECT COUNT(*) FROM users WHERE role IN ('owner', 'both')").fetchone()[0],
        "renters": db.execute("SELECT COUNT(*) FROM users WHERE role IN ('renter', 'both')").fetchone()[0],
        "cars": db.execute("SELECT COUNT(*) FROM cars").fetchone()[0],
        "active_rentals": db.execute("SELECT COUNT(*) FROM rentals WHERE status = 'active'").fetchone()[0],
        "pending_requests": db.execute("SELECT COUNT(*) FROM rentals WHERE status = 'booked' AND owner_response = 'pending'").fetchone()[0],
        "open_complaints": db.execute("SELECT COUNT(*) FROM complaints WHERE status = 'open'").fetchone()[0],
        "feedback_total": db.execute("SELECT COUNT(*) FROM support_feedback").fetchone()[0],
    }
    company_payout = get_company_payout_details()
    pending_payments = db.execute(
        """
        SELECT rentals.id, rentals.total_amount, rentals.payment_due_at, renters.username AS renter_username, cars.name AS car_name
        FROM rentals
        JOIN users AS renters ON renters.id = rentals.renter_id
        JOIN cars ON cars.id = rentals.car_id
        WHERE rentals.payment_status = 'awaiting_payment'
        ORDER BY rentals.payment_due_at
        """
    ).fetchall()
    open_complaints = db.execute(
        """
        SELECT complaints.*, submit.username AS submitter, target.username AS target_username, cars.name AS car_name
        FROM complaints
        JOIN users AS submit ON submit.id = complaints.submitted_by
        JOIN users AS target ON target.id = complaints.target_user_id
        LEFT JOIN rentals ON rentals.id = complaints.rental_id
        LEFT JOIN cars ON cars.id = rentals.car_id
        WHERE complaints.status = 'open'
        ORDER BY complaints.created_at DESC
        """
    ).fetchall()
    recent_rentals = db.execute(
        """
        SELECT rentals.id, rentals.status, rentals.start_time, rentals.end_time, rentals.payment_status,
               renters.username AS renter_username, owners.username AS owner_username, cars.name AS car_name
        FROM rentals
        JOIN users AS renters ON renters.id = rentals.renter_id
        JOIN cars ON cars.id = rentals.car_id
        JOIN users AS owners ON owners.id = cars.owner_id
        ORDER BY rentals.start_time DESC
        LIMIT 8
        """
    ).fetchall()
    recent_users = db.execute(
        "SELECT id, username, role, is_admin FROM users ORDER BY id DESC LIMIT 8").fetchall()
    recent_feedback = db.execute(
        """
        SELECT support_feedback.*, users.username,
               rentals.id AS rental_id, rentals.start_time, rentals.end_time,
               cars.name AS car_name, cars.brand, cars.model
        FROM support_feedback
        JOIN users ON users.id = support_feedback.user_id
        LEFT JOIN rentals ON rentals.id = support_feedback.rental_id
        LEFT JOIN cars ON cars.id = rentals.car_id
        ORDER BY support_feedback.created_at DESC
        LIMIT 6
        """
    ).fetchall()
    return {
        "metrics": metrics,
        "pending_payments": pending_payments,
        "open_complaints": open_complaints,
        "recent_rentals": recent_rentals,
        "recent_users": recent_users,
        "recent_feedback": recent_feedback,
        "company_payout": company_payout,
        "commission_rate": int(COMPANY_COMMISSION_RATE * 100),
    }


@app.route("/admin")
@login_required
@admin_required
def admin_dashboard() -> str:
    context = build_admin_dashboard_context()
    return render_template("admin_dashboard.html", **context)


@app.route("/admin/complaints/<int:complaint_id>/resolve", methods=["POST"])
@login_required
@admin_required
def resolve_complaint(complaint_id: int) -> str:
    db = get_db()
    complaint = db.execute(
        "SELECT submitted_by, role FROM complaints WHERE id = ?", (complaint_id,)).fetchone()
    if complaint is None:
        abort(404)
    resolution = request.form.get("resolution", "").strip()
    now_iso = naive_utcnow_iso()
    db.execute(
        "UPDATE complaints SET status = 'resolved', resolved_at = ?, resolution = ? WHERE id = ?",
        (now_iso, resolution, complaint_id),
    )
    db.commit()
    redirect_target = url_for(
        "rentals") if complaint["role"] == 'renter' else url_for("owner_cars")
    create_notification(
        complaint["submitted_by"], "Your complaint has been resolved by the admin team.", redirect_target)
    return redirect(url_for("admin_dashboard"))


@app.route("/admin/users")
@login_required
@admin_required
def admin_users() -> str:
    db = get_db()
    rows = db.execute(
        """
        SELECT
            users.id, users.username, users.account_name, users.role, users.is_admin,
            IFNULL(profiles.full_name, '') AS full_name,
            IFNULL(profiles.phone, '') AS phone,
            profiles.profile_completed,
            profiles.profile_verified_at,
            (SELECT COUNT(*) FROM user_documents WHERE user_documents.user_id = users.id) AS doc_count,
            (SELECT COUNT(*) FROM cars WHERE cars.owner_id = users.id) AS vehicle_count,
            (SELECT COUNT(*) FROM rentals WHERE rentals.renter_id = users.id) AS trip_count
        FROM users
        LEFT JOIN user_profiles AS profiles ON profiles.user_id = users.id
        ORDER BY users.username COLLATE NOCASE
        """
    ).fetchall()
    users = []
    for row in rows:
        record = dict(row)
        record.setdefault("account_name", "")
        record["display_name"] = record.get(
            "account_name") or record.get("username", "")
        users.append(record)
    return render_template("admin_users.html", users=users)


@app.route("/admin/users/<int:user_id>")
@login_required
@admin_required
def admin_user_detail(user_id: int) -> str:
    db = get_db()
    account_row = db.execute(
        "SELECT id, username, role, is_admin FROM users WHERE id = ?",
        (user_id,),
    ).fetchone()
    if account_row is None:
        abort(404)
    account = dict(account_row)
    account.setdefault("account_name", "")
    account["display_name"] = account.get(
        "account_name") or account.get("username", "")
    profile = ensure_user_profile(user_id)
    documents = [dict(doc) for doc in fetch_user_documents(user_id)]
    stats = {
        "vehicle_count": db.execute("SELECT COUNT(*) FROM cars WHERE owner_id = ?", (user_id,)).fetchone()[0],
        "trip_count": db.execute("SELECT COUNT(*) FROM rentals WHERE renter_id = ?", (user_id,)).fetchone()[0],
    }
    car_rows = db.execute(
        "SELECT * FROM cars WHERE owner_id = ? ORDER BY created_at DESC",
        (user_id,),
    ).fetchall()
    car_images = fetch_car_images([row["id"] for row in car_rows])
    cars: List[dict] = []
    for row in car_rows:
        car = dict(row)
        car["images"] = car_images.get(row["id"], [])
        cars.append(car)
    city_entries = load_city_entries(include_coordinates=True)
    payout_row = db.execute(
        "SELECT account_holder, account_number, ifsc_code, upi_id, updated_at FROM user_payout_details WHERE user_id = ?",
        (user_id,),
    ).fetchone()
    payout = dict(payout_row) if payout_row else {}
    return render_template(
        "admin_user_detail.html",
        user=account,
        profile=profile,
        documents=documents,
        payout=payout,
        stats=stats,
        cars=cars,
        city_entries=city_entries,
        delivery_choices=DELIVERY_DISTANCE_CHOICES,
    )


@app.route("/admin/users/<int:user_id>/documents/<int:doc_id>")
@login_required
@admin_required
def admin_download_user_document(user_id: int, doc_id: int):
    db = get_db()
    doc = db.execute(
        "SELECT filename FROM user_documents WHERE id = ? AND user_id = ?",
        (doc_id, user_id),
    ).fetchone()
    if doc is None:
        abort(404)
    stored_path = UPLOAD_ROOT.joinpath(doc["filename"])
    try:
        resolved = stored_path.resolve(strict=True)
    except FileNotFoundError:
        abort(404)
    upload_root = UPLOAD_ROOT.resolve()
    if not str(resolved).startswith(str(upload_root)):
        abort(403)
    return send_from_directory(resolved.parent, resolved.name, as_attachment=True)


@app.post("/admin/users/<int:user_id>/verify")
@login_required
@admin_required
def admin_verify_user(user_id: int) -> str:
    db = get_db()
    db.execute(
        "UPDATE user_profiles SET profile_verified_at = ? WHERE user_id = ?",
        (naive_utcnow_iso(), user_id),
    )
    db.commit()
    return redirect(url_for("admin_users"))


@app.route("/admin/rentals")
@login_required
@admin_required
def admin_rentals() -> str:
    db = get_db()
    rows = db.execute(
        """
        SELECT rentals.*,
               cars.name AS car_name, cars.brand, cars.model,
               COALESCE(renters.account_name, renters.username) AS renter_display_name,
               COALESCE(owners.account_name, owners.username) AS owner_display_name,
               renters.username AS renter_username,
               owners.username AS owner_username,
               cars.id AS car_id
        FROM rentals
        JOIN cars ON cars.id = rentals.car_id
        JOIN users AS renters ON renters.id = rentals.renter_id
        JOIN users AS owners ON owners.id = cars.owner_id
        ORDER BY rentals.start_time DESC
        """
    ).fetchall()
    rentals = [dict(row) for row in rows]
    car_images = fetch_car_images([row["car_id"] for row in rentals])
    for rental in rentals:
        rental["images"] = car_images.get(rental["car_id"], [])
    return render_template(
        "admin_rentals.html",
        rentals=rentals,
        commission_rate=int(COMPANY_COMMISSION_RATE * 100),
    )


@app.post("/admin/rentals/<int:rental_id>/payment")
@login_required
@admin_required
def admin_update_payment(rental_id: int) -> str:
    new_status = request.form.get("payment_status", "").lower()
    if new_status not in {"pending", "awaiting_payment", "paid"}:
        abort(400)
    db = get_db()
    rental = db.execute(
        """
        SELECT total_amount,
               company_commission_amount,
               owner_payout_amount,
               owner_payout_status,
               owner_payout_released_at,
               owner_initial_payout_amount,
               owner_initial_payout_status,
               owner_initial_payout_released_at,
               owner_final_payout_amount,
               owner_final_payout_status,
               owner_final_payout_released_at
        FROM rentals
        WHERE id = ?
        """,
        (rental_id,),
    ).fetchone()
    if rental is None:
        abort(404)
    commission = float(rental["company_commission_amount"] or 0)
    owner_payout = float(rental["owner_payout_amount"] or 0)
    owner_status = rental["owner_payout_status"] or "pending"
    released_at = rental["owner_payout_released_at"]
    initial_amount = float(rental["owner_initial_payout_amount"] or 0)
    initial_status = rental["owner_initial_payout_status"] or "pending"
    initial_released = rental["owner_initial_payout_released_at"]
    final_amount = float(rental["owner_final_payout_amount"] or 0)
    final_status = rental["owner_final_payout_status"] or owner_status
    final_released = rental["owner_final_payout_released_at"]
    now_iso = naive_utcnow_iso()
    if new_status == "paid":
        total_amount = float(rental["total_amount"] or 0)
        commission = round(total_amount * COMPANY_COMMISSION_RATE, 2)
        owner_payout = max(0.0, round(total_amount - commission, 2))
        initial_amount = round(owner_payout * OWNER_INITIAL_PAYOUT_RATE, 2)
        if owner_payout <= 0:
            initial_amount = 0.0
        final_amount = max(0.0, round(owner_payout - initial_amount, 2))
        owner_status = "pending" if final_amount > 0 else "not_required"
        released_at = None
        initial_status = "paid" if initial_amount > 0 else "not_required"
        initial_released = now_iso if initial_amount > 0 else None
        final_status = owner_status
        final_released = None
    else:
        commission = 0
        owner_payout = 0
        owner_status = "pending"
        released_at = None
        initial_amount = 0
        initial_status = "pending"
        initial_released = None
        final_amount = 0
        final_status = "pending"
        final_released = None
    db.execute(
        """
        UPDATE rentals
        SET payment_status = ?,
            company_commission_amount = ?,
            owner_payout_amount = ?,
            owner_payout_status = ?,
            owner_payout_released_at = ?,
            owner_initial_payout_amount = ?,
            owner_initial_payout_status = ?,
            owner_initial_payout_released_at = ?,
            owner_final_payout_amount = ?,
            owner_final_payout_status = ?,
            owner_final_payout_released_at = ?
        WHERE id = ?
        """,
        (
            new_status,
            commission,
            owner_payout,
            owner_status,
            released_at,
            initial_amount,
            initial_status,
            initial_released,
            final_amount,
            final_status,
            final_released,
            rental_id,
        ),
    )
    db.commit()
    return redirect(url_for("admin_rentals"))


@app.post("/admin/rentals/<int:rental_id>/payout")
@login_required
@admin_required
def admin_release_owner_payout(rental_id: int) -> str:
    db = get_db()
    rental = db.execute(
        "SELECT payment_status, owner_payout_status FROM rentals WHERE id = ?",
        (rental_id,),
    ).fetchone()
    if rental is None:
        abort(404)
    if rental["payment_status"] != "paid":
        return redirect(url_for("admin_rentals"))
    if rental["owner_payout_status"] == "paid":
        return redirect(url_for("admin_rentals"))
    now_iso = naive_utcnow_iso()
    db.execute(
        """
        UPDATE rentals
        SET owner_payout_status = 'paid',
            owner_payout_released_at = ?,
            owner_final_payout_status = 'paid',
            owner_final_payout_released_at = ?
        WHERE id = ?
        """,
        (now_iso, now_iso, rental_id),
    )
    db.commit()
    return redirect(url_for("admin_rentals"))


@app.route("/admin/map")
@login_required
@admin_required
def admin_map() -> str:
    db = get_db()
    vehicle_rows = db.execute(
        """
        SELECT cars.id, cars.name, cars.vehicle_type, cars.latitude, cars.longitude, cars.city, users.username AS owner_username
        FROM cars
        JOIN users ON users.id = cars.owner_id
        WHERE cars.latitude IS NOT NULL AND cars.longitude IS NOT NULL
        """
    ).fetchall()
    active_ids = [row["car_id"] for row in db.execute(
        "SELECT DISTINCT car_id FROM rentals WHERE status = 'active'"
    ).fetchall()]
    vehicles = [dict(row) for row in vehicle_rows]
    for vehicle in vehicles:
        try:
            vehicle["latitude"] = float(
                vehicle["latitude"]) if vehicle["latitude"] is not None else None
            vehicle["longitude"] = float(
                vehicle["longitude"]) if vehicle["longitude"] is not None else None
        except (TypeError, ValueError):
            vehicle["latitude"] = vehicle["longitude"] = None
    return render_template("admin_map.html", vehicles=vehicles, active_vehicle_ids=active_ids)


@app.route("/admin/feedback")
@login_required
@admin_required
def admin_feedback_list() -> str:
    db = get_db()
    entries = db.execute(
        """
        SELECT support_feedback.*, users.username,
               rentals.start_time, rentals.end_time,
               cars.name AS car_name, cars.brand, cars.model
        FROM support_feedback
        JOIN users ON users.id = support_feedback.user_id
        LEFT JOIN rentals ON rentals.id = support_feedback.rental_id
        LEFT JOIN cars ON cars.id = rentals.car_id
        ORDER BY support_feedback.created_at DESC
        """
    ).fetchall()
    return render_template("admin_feedback.html", feedback_entries=entries)


@app.route("/admin/feedback/<int:feedback_id>")
@login_required
@admin_required
def admin_feedback_detail(feedback_id: int) -> str:
    db = get_db()
    entry = db.execute(
        """
        SELECT support_feedback.*, users.username,
               rentals.start_time, rentals.end_time,
               cars.name AS car_name, cars.brand, cars.model,
               cars.licence_plate
        FROM support_feedback
        JOIN users ON users.id = support_feedback.user_id
        LEFT JOIN rentals ON rentals.id = support_feedback.rental_id
        LEFT JOIN cars ON cars.id = rentals.car_id
        WHERE support_feedback.id = ?
        """,
        (feedback_id,),
    ).fetchone()
    if entry is None:
        abort(404)
    return render_template("admin_feedback_detail.html", feedback=entry)


@app.route("/admin/payment-settings", methods=["GET", "POST"])
@login_required
@admin_required
def admin_payment_settings() -> str:
    db = get_db()
    config = get_company_payout_details()
    message = None
    error = None
    if request.method == "POST":
        account_holder = request.form.get("account_holder", "").strip()
        account_number = request.form.get("account_number", "").strip()
        ifsc_code = request.form.get("ifsc_code", "").strip().upper()
        upi_id = request.form.get("upi_id", "").strip()
        if not (account_number and ifsc_code) and not upi_id:
            error = "Please provide bank account details (account number + IFSC) or a UPI ID."
        if error is None:
            db.execute(
                """
                UPDATE company_payout_config
                SET account_holder = ?,
                    account_number = ?,
                    ifsc_code = ?,
                    upi_id = ?,
                    updated_at = ?
                WHERE id = 1
                """,
                (account_holder, account_number, ifsc_code,
                 upi_id, naive_utcnow_iso()),
            )
            db.commit()
            config = get_company_payout_details()
            message = "Company payout details updated."
    return render_template(
        "admin_payment_settings.html",
        config=config,
        message=message,
        error=error,
        commission_rate=int(COMPANY_COMMISSION_RATE * 100),
    )


@app.route("/")
def home() -> str:
    if g.user and g.user.get("is_admin"):
        context = build_admin_dashboard_context()
        return render_template("admin_dashboard.html", **context)
    db = get_db()
    cities = load_city_entries()
    vehicle_type_values = load_vehicle_type_options()
    additional_vehicle_types = [
        vt for vt in vehicle_type_values if vt not in POPULAR_VEHICLE_TYPES
    ]
    fuel_types = build_fuel_type_list()
    requested_destinations = [
        value.strip()
        for value in request.args.getlist("destinations")
        if value and value.strip()
    ]
    owner_stats = None
    commission_rate_pct = int(COMPANY_COMMISSION_RATE * 100)
    if g.user and has_role("owner"):
        stats_row = db.execute(
            """
            SELECT
                SUM(CASE WHEN rentals.status = 'active' THEN 1 ELSE 0 END) AS active_trips,
                SUM(CASE WHEN rentals.payment_status = 'awaiting_payment' THEN 1 ELSE 0 END) AS awaiting_payouts,
                SUM(CASE WHEN rentals.payment_status = 'awaiting_payment' THEN rentals.total_amount ELSE 0 END) AS awaiting_total
            FROM rentals
            JOIN cars ON cars.id = rentals.car_id
            WHERE cars.owner_id = ?
            """,
            (g.user["id"],),
        ).fetchone()
        awaiting_total = stats_row["awaiting_total"] or 0
        owner_stats = {
            "active_trips": stats_row["active_trips"] or 0,
            "awaiting_payouts": stats_row["awaiting_payouts"] or 0,
            "awaiting_total": round(awaiting_total, 2),
            "take_home_estimate": round(awaiting_total * (1 - COMPANY_COMMISSION_RATE), 2),
        }
    return render_template(
        "home.html",
        cities=cities,
        vehicle_types=additional_vehicle_types,
        popular_vehicle_types=POPULAR_VEHICLE_TYPES,
        fuel_types=fuel_types,
        requested_destinations=requested_destinations,
        owner_stats=owner_stats,
        commission_rate=commission_rate_pct,
    )


@app.route("/search", methods=["GET", "POST"])
def search() -> str:
    profile_warning = None
    user = getattr(g, 'user', None)
    if user is not None and not getattr(g, 'profile_complete', False):
        profile_warning = "You can browse vehicles, but please complete your profile before confirming a booking."
    db = get_db()
    city_entries = load_city_entries(include_coordinates=True)
    city_options = [
        _format_city_label(entry["name"], entry["state"])
        for entry in city_entries
    ]

    cars: List[Car] = []
    cars_payload: List[dict] = []
    pricing_map: Dict[int, Dict[str, object]] = {}
    destinations: List[str] = []
    latitude = longitude = None
    radius = None
    city_raw = request.form.get(
        "city") if request.method == "POST" else request.args.get("city")
    city_display = (city_raw or "").strip()
    city = city_display.split(",")[0].strip() if city_display else None
    detected_city = None
    start_time_raw = end_time_raw = None
    error = request.args.get("error")

    available_vehicle_types = load_vehicle_type_options()
    fuel_types = build_fuel_type_list()

    filters = {
        "vehicle_types": [],
        "price_min": None,
        "price_max": None,
        "seat_min": None,
        "seat_max": None,
        "require_gps": False,
        "price_unit": "hour",
        "fuel_type": None,
        "destinations": [],
    }

    def normalize_unit(raw: str | None) -> str:
        unit = (raw or "hour").lower()
        return unit if unit in {"hour", "day"} else "hour"

    def convert_price(value: Optional[float], unit: str) -> Optional[float]:
        if value is None:
            return None
        if unit == "day":
            return value / 24
        return value

    if request.method == "POST":
        start_time_raw = request.form.get("start_time")
        end_time_raw = request.form.get("end_time")
        filters["vehicle_types"] = [
            v for v in request.form.getlist("vehicle_types") if v]
        filters["price_unit"] = normalize_unit(request.form.get("price_unit"))
        filters["price_min"] = parse_float(request.form.get("price_min"))
        filters["price_max"] = parse_float(request.form.get("price_max"))
        filters["seat_min"] = parse_int(request.form.get("seat_min"))
        filters["seat_max"] = parse_int(request.form.get("seat_max"))
        filters["require_gps"] = request.form.get("require_gps") == "on"
        fuel_type_raw = request.form.get("fuel_type", "").strip()
        filters["fuel_type"] = fuel_type_raw or None
        destinations = [value.strip() for value in request.form.getlist(
            "destinations") if value and value.strip()]
        filters["destinations"] = destinations
        price_min_hours = convert_price(
            filters["price_min"], filters["price_unit"])
        price_max_hours = convert_price(
            filters["price_max"], filters["price_unit"])
        lat_raw = request.form.get("latitude", "").strip()
        lon_raw = request.form.get("longitude", "").strip()
        radius_raw = request.form.get("radius", "").strip()
        latitude = parse_float(lat_raw)
        longitude = parse_float(lon_raw)
        radius = parse_float(radius_raw)
        radius = radius if radius is not None else 10.0
        if latitude is not None and longitude is not None:
            if not city:
                detected_city = reverse_geocode_city(latitude, longitude)
                city = detected_city or city
                if detected_city and not city_display:
                    city_display = detected_city
            city_filter = city if latitude is None or longitude is None else None
            cars = fetch_available_cars(
                latitude=latitude,
                longitude=longitude,
                radius_km=radius,
                city=city_filter,
                price_min=price_min_hours,
                price_max=price_max_hours,
                vehicle_types=filters["vehicle_types"],
                seat_min=filters["seat_min"],
                seat_max=filters["seat_max"],
                require_gps=filters["require_gps"],
                fuel_types=[filters["fuel_type"]
                            ] if filters["fuel_type"] else None,
            )
            if not cars:
                error = "No cars found within the selected filters."
        else:
            latitude = longitude = None
    else:
        start_time_raw = request.args.get("start_time")
        end_time_raw = request.args.get("end_time")
        filters["vehicle_types"] = [
            v for v in request.args.getlist("vehicle_types") if v]
        filters["price_unit"] = normalize_unit(request.args.get("price_unit"))
        filters["price_min"] = parse_float(request.args.get("price_min"))
        filters["price_max"] = parse_float(request.args.get("price_max"))
        filters["seat_min"] = parse_int(request.args.get("seat_min"))
        filters["seat_max"] = parse_int(request.args.get("seat_max"))
        filters["require_gps"] = request.args.get(
            "require_gps") in {"1", "true", "on"}
        fuel_type_raw = request.args.get("fuel_type", "")
        filters["fuel_type"] = fuel_type_raw.strip() or None
        destinations = [value.strip() for value in request.args.getlist(
            "destinations") if value and value.strip()]
        filters["destinations"] = destinations
        price_min_hours = convert_price(
            filters["price_min"], filters["price_unit"])
        price_max_hours = convert_price(
            filters["price_max"], filters["price_unit"])
        try:
            lat_query = request.args.get("latitude")
            lon_query = request.args.get("longitude")
            radius_query = request.args.get("radius")
            latitude = float(lat_query) if lat_query else None
            longitude = float(lon_query) if lon_query else None
            radius = float(radius_query) if radius_query else None
            if latitude is not None and longitude is not None:
                lookup_radius = radius if radius is not None else 10.0
                city_filter = city if latitude is None or longitude is None else None
                cars = fetch_available_cars(
                    latitude=latitude,
                    longitude=longitude,
                    radius_km=lookup_radius,
                    city=city_filter,
                    price_min=price_min_hours,
                    price_max=price_max_hours,
                    vehicle_types=filters["vehicle_types"],
                    seat_min=filters["seat_min"],
                    seat_max=filters["seat_max"],
                    require_gps=filters["require_gps"],
                    fuel_types=[filters["fuel_type"]
                                ] if filters["fuel_type"] else None,
                )
                radius = lookup_radius
        except (TypeError, ValueError):
            latitude = longitude = None
            radius = None

    if (latitude is None or longitude is None) and city:
        coords = lookup_city_coordinates(city)
        if coords:
            latitude, longitude = coords
            if radius is None:
                radius = 10.0
            if not city_display:
                city_display = city

    if not cars and latitude is not None and longitude is not None:
        lookup_radius = radius if radius is not None else 10.0
        city_filter = city if latitude is None or longitude is None else None
        cars = fetch_available_cars(
            latitude=latitude,
            longitude=longitude,
            radius_km=lookup_radius,
            city=city_filter,
            price_min=price_min_hours,
            price_max=price_max_hours,
            vehicle_types=filters["vehicle_types"],
            seat_min=filters["seat_min"],
            seat_max=filters["seat_max"],
            require_gps=filters["require_gps"],
            fuel_types=[filters["fuel_type"]
                        ] if filters["fuel_type"] else None,
        )
        radius = lookup_radius

    start_dt = parse_iso(parse_datetime(start_time_raw))
    end_dt = parse_iso(parse_datetime(end_time_raw))

    app.logger.info(
        "search results count=%s filters=%s lat=%s lng=%s radius=%s destinations=%s user_id=%s",
        len(cars),
        {
            "vehicle_types": filters["vehicle_types"],
            "seat_min": filters["seat_min"],
            "seat_max": filters["seat_max"],
            "price_min": filters["price_min"],
            "price_max": filters["price_max"],
            "require_gps": filters["require_gps"],
            "fuel_type": filters["fuel_type"],
        },
        latitude,
        longitude,
        radius,
        destinations,
        user["id"] if user else None,
    )
    if cars:
        for car in cars:
            cars_payload.append(
                {
                    "id": car.id,
                    "name": car.name,
                    "latitude": car.latitude,
                    "longitude": car.longitude,
                    "distance_km": car.distance_km,
                    "status": car.status,
                    "rate_per_hour": car.rate_per_hour,
                    "daily_rate": car.daily_rate,
                    "seats": car.seats,
                    "owner_public_name": car.owner_public_name,
                    "city": car.city,
                    "image_url": car.image_url,
                    "vehicle_type": car.vehicle_type,
                    "size_category": car.size_category,
                    "has_gps": car.has_gps,
                    "fuel_type": car.fuel_type,
                    "transmission": car.transmission,
                    "rating": car.rating,
                    "description": car.description,
                    "images": car.images,
                    "delivery_options": car.delivery_options,
                }
            )
        if start_dt and end_dt:
            for car in cars:
                pricing_map[car.id] = calculate_pricing(
                    car.rate_per_hour,
                    car.daily_rate,
                    start_dt,
                    end_dt,
                )

    start_display = format_trip_datetime(start_time_raw) if start_time_raw else "-"
    end_display = format_trip_datetime(end_time_raw) if end_time_raw else "-"

    return render_template(
        "search.html",
        cars=cars,
        cars_payload=cars_payload,
        pricing=pricing_map,
        latitude=latitude,
        longitude=longitude,
        radius=radius,
        city=city,
        city_display=city_display,
        detected_city=detected_city,
        start_time=start_time_raw,
        end_time=end_time_raw,
        start_time_display=start_display,
        end_time_display=end_display,
        error=error,
        filters=filters,
        available_vehicle_types=available_vehicle_types,
        fuel_types=fuel_types,
        destinations=destinations,
        profile_warning=profile_warning,
        city_options=city_options,
        city_entries=city_entries,
        popular_vehicle_types=POPULAR_VEHICLE_TYPES,
    )


@app.route("/rentals")
@login_required
@role_required("renter", "owner")
def rentals() -> str:
    db = get_db()
    rows = db.execute(
        """
        SELECT rentals.*, cars.brand, cars.model, cars.licence_plate, cars.image_url, cars.name AS car_name,
               cars.owner_id AS owner_id, cars.has_gps, cars.latitude AS car_latitude, cars.longitude AS car_longitude,
               owners.username AS owner_username,
               COALESCE(owners.account_name, '') AS owner_account_name
        FROM rentals
        JOIN cars ON cars.id = rentals.car_id
        JOIN users AS owners ON owners.id = cars.owner_id
        WHERE rentals.renter_id = ?
        ORDER BY rentals.id DESC
        """,
        (g.user["id"],),
    ).fetchall()
    car_ids = [row["car_id"] for row in rows]
    image_map = fetch_car_images(car_ids)
    rentals_list: List[Dict[str, object]] = []
    for row in rows:
        rental = dict(row)
        owner_label = build_public_label(
            rental.get("owner_account_name") or rental.get("owner_username"),
            fallback_prefix="Host",
            fallback_id=rental.get("owner_id"),
        )
        rental["owner_public_name"] = owner_label
        rental["owner_username"] = owner_label
        rental.pop("owner_account_name", None)
        rental["has_gps"] = bool(rental.get("has_gps"))
        if rental.get("status") == "cancelled":
            renter_flag = (rental.get("renter_response") or "").lower()
            owner_flag = (rental.get("owner_response") or "").lower()
            if renter_flag in {"cancelled_by_renter", "cancelled", "declined"}:
                rental["cancelled_by_label"] = "Cancelled by you"
            elif owner_flag in {"rejected", "cancelled"}:
                rental["cancelled_by_label"] = "Cancelled by host"
            else:
                rental["cancelled_by_label"] = "Cancelled"
        if rental.get("car_latitude") is not None:
            try:
                rental["car_latitude"] = float(rental["car_latitude"])
            except (TypeError, ValueError):
                rental["car_latitude"] = None
        if rental.get("car_longitude") is not None:
            try:
                rental["car_longitude"] = float(rental["car_longitude"])
            except (TypeError, ValueError):
                rental["car_longitude"] = None
        primary_image: Optional[str] = None
        car_images = image_map.get(row["car_id"], [])
        if car_images:
            primary_image = url_for("serve_upload", filename=car_images[0])
        else:
            raw_image = rental.get("image_url") or ""
            if raw_image and isinstance(raw_image, str):
                if raw_image.startswith(("http://", "https://")):
                    primary_image = raw_image
                else:
                    primary_image = url_for(
                        "serve_upload", filename=raw_image.lstrip("/")
                    )
        rental["image_url"] = primary_image
        try:
            rental["trip_destinations_list"] = json.loads(
                rental.get("trip_destinations") or "[]"
            )
        except (TypeError, json.JSONDecodeError):
            rental["trip_destinations_list"] = []
        rentals_list.append(rental)

    rental_ids = [rental["id"] for rental in rentals_list if rental.get("id") is not None]
    if rental_ids:
        placeholders = ",".join("?" for _ in rental_ids)
        owner_reviews = db.execute(
            f"""
            SELECT rental_id, passenger_rating, comment, created_at
            FROM reviews
            WHERE rental_id IN ({placeholders})
              AND reviewer_role = 'owner'
              AND target_user_id = ?
            """,
            (*rental_ids, g.user["id"]),
        ).fetchall()
        review_lookup = {
            row["rental_id"]: {
                "passenger_rating": row["passenger_rating"],
                "comment": (row["comment"] or "").strip(),
                "created_at": row["created_at"],
            }
            for row in owner_reviews
        }
        for rental in rentals_list:
            rental["host_review"] = review_lookup.get(rental["id"])
    else:
        for rental in rentals_list:
            rental["host_review"] = None
    awaiting_payment = [
        rental
        for rental in rentals_list
        if rental.get("payment_status") == "awaiting_payment"
        and rental.get("owner_response") == "accepted"
    ]
    pending_host_action = [
        rental
        for rental in rentals_list
        if rental.get("owner_response") in ("pending", "counter")
        and rental.get("status") == "booked"
    ]
    return render_template(
        "rentals.html",
        rentals=rentals_list,
        awaiting_payment=awaiting_payment,
        pending_host_action=pending_host_action,
    )


@app.route("/rent/<int:car_id>", methods=["POST"])
@login_required
@role_required("renter", "owner")
def rent_car(car_id: int) -> str:
    if not g.profile_complete:
        return redirect(url_for("profile", message="Complete your profile before booking."))
    db = get_db()
    car = db.execute(
        "SELECT id, owner_id, is_available, rate_per_hour, daily_rate, name, brand, model FROM cars WHERE id = ?",
        (car_id,),
    ).fetchone()
    if car is None or not car["is_available"]:
        return redirect(url_for("search", error="Car is no longer available."))

    start_raw = request.form.get("start_time")
    end_raw = request.form.get("end_time")
    promo_code = request.form.get("promo_code", "")

    start_iso = parse_datetime(start_raw) or naive_utcnow_iso()
    end_iso = parse_datetime(end_raw)
    start_dt = parse_iso(start_iso) or naive_utcnow()
    end_dt = parse_iso(end_iso) if end_iso else start_dt + timedelta(hours=4)

    delivery_type_raw = (request.form.get("delivery_type", "pickup") or "pickup").strip().lower()
    delivery_type = "delivery" if delivery_type_raw == "delivery" else "pickup"
    delivery_fee_value = parse_float(request.form.get("delivery_fee", "0"))
    delivery_fee = round(delivery_fee_value, 2) if delivery_fee_value is not None else 0.0
    if delivery_fee < 0:
        delivery_fee = 0.0
    delivery_distance_val = parse_float(request.form.get("delivery_distance", ""))
    delivery_distance_km = round(delivery_distance_val,
                                 2) if delivery_distance_val is not None else None
    delivery_lat = parse_float(request.form.get("delivery_latitude", ""))
    delivery_lng = parse_float(request.form.get("delivery_longitude", ""))
    delivery_address = (request.form.get("delivery_address") or "").strip()
    if len(delivery_address) > 240:
        delivery_address = delivery_address[:240]

    if delivery_type == "delivery":
        if delivery_lat is None or delivery_lng is None or delivery_distance_km is None:
            return redirect(url_for("search", error="Please share a delivery pin before requesting delivery."))
        if delivery_distance_km < 0:
            delivery_distance_km = 0.0
    else:
        delivery_type = "pickup"
        delivery_fee = 0.0
        delivery_distance_km = None
        delivery_lat = None
        delivery_lng = None
        delivery_address = ""

    destinations_list = [
        value.strip()
        for value in request.form.getlist("destinations")
        if value and value.strip()
    ]
    destinations_json = json.dumps(destinations_list)

    pricing = calculate_pricing(
        rate_per_hour=car["rate_per_hour"],
        daily_rate=car["daily_rate"] or car["rate_per_hour"] * 24,
        start=start_dt,
        end=end_dt,
        promo_code=promo_code,
    )

    rental_amount = float(pricing["total"] or 0.0)
    total_amount = round(rental_amount + delivery_fee, 2)
    discount_amount = float(pricing["discount"] or 0.0)
    promo_applied = pricing["promo_applied"] or ""

    cursor = db.execute(
        """
        INSERT INTO rentals (
            car_id,
            renter_id,
            status,
            start_time,
            end_time,
            promo_code,
            discount_amount,
            rental_amount,
            delivery_type,
            delivery_fee,
            delivery_distance_km,
            delivery_latitude,
            delivery_longitude,
            delivery_address,
            trip_destinations,
            total_amount
        )
        VALUES (?, ?, 'booked', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            car_id,
            g.user["id"],
            start_iso,
            end_dt.isoformat(),
            promo_applied,
            discount_amount,
            rental_amount,
            delivery_type,
            delivery_fee,
            delivery_distance_km,
            delivery_lat,
            delivery_lng,
            delivery_address,
            destinations_json,
            total_amount,
        ),
    )
    rental_id = cursor.lastrowid
    db.execute(
        "UPDATE cars SET is_available = 0, updated_at = ? WHERE id = ?",
        (naive_utcnow_iso(), car_id),
    )
    db.commit()

    car_name = car["name"] or f"{car['brand']} {car['model']}"
    actor_name = display_name(g.user)
    total_label = f"Rs {round(total_amount):.0f}"
    rental_label = f"Rs {round(rental_amount):.0f}"
    if delivery_type == "delivery":
        delivery_label = f"delivery fee Rs {round(delivery_fee):.0f}"
        location_label = delivery_address or "Pinned delivery location"
        distance_label = f"{delivery_distance_km:.1f} km" if delivery_distance_km is not None else "distance pending"
        owner_message = (
            f"{actor_name} requested to book your {car_name}. "
            f"Total {total_label} (rental {rental_label} + {delivery_label}). "
            f"Delivery requested to {location_label} (~{distance_label})."
        )
    else:
        owner_message = (
            f"{actor_name} requested to book your {car_name}. "
            f"Total {total_label} (rental {rental_label}). Pickup at your location."
        )
    renter_message = (
        f"Booking request sent for {car_name}. We'll notify you once the host responds."
    )
    create_notification(car["owner_id"], owner_message, url_for("owner_cars"))
    create_notification(g.user["id"], renter_message, url_for("rentals"))
    return redirect(url_for("rentals"))


def _process_rental_complaint(rental_id: int, category: str, description: str) -> str:
    category = category.strip() or "General"
    description = description.strip()
    db = get_db()
    rental = db.execute(
        """
        SELECT rentals.*, cars.owner_id, cars.name AS car_name, cars.brand, cars.model
        FROM rentals
        JOIN cars ON cars.id = rentals.car_id
        WHERE rentals.id = ?
        """,
        (rental_id,),
    ).fetchone()
    if rental is None:
        abort(404)
    role = None
    target_user_id = None
    redirect_target = url_for("rentals")
    actor = display_name(g.user)
    if rental["renter_id"] == g.user["id"]:
        role = "renter"
        target_user_id = rental["owner_id"]
    elif rental["owner_id"] == g.user["id"]:
        role = "owner"
        target_user_id = rental["renter_id"]
        redirect_target = url_for("owner_cars")
    else:
        abort(403)
    if not description:
        return redirect_target
    db.execute(
        "INSERT INTO complaints (rental_id, submitted_by, target_user_id, role, category, description, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (rental_id, g.user["id"], target_user_id, role,
         category, description, naive_utcnow_iso()),
    )
    db.commit()
    car_label = rental["car_name"] or f"{rental['brand']} {rental['model']}"
    create_notification(
        target_user_id,
        f"{actor} filed a complaint about the trip with {car_label}.",
        redirect_target,
    )
    return redirect_target


@app.route("/rentals/<int:rental_id>/complaint", methods=["POST"])
@login_required
def submit_complaint(rental_id: int) -> str:
    category = request.form.get("category", "General")
    description = request.form.get("description", "")
    redirect_target = _process_rental_complaint(
        rental_id, category, description)
    return redirect(redirect_target)


@app.route("/support/feedback", methods=["POST"])
@login_required
def submit_feedback() -> str:
    category = request.form.get(
        "category", "Platform issue").strip() or "Platform issue"
    description = request.form.get("description", "").strip()
    rental_id_value = request.form.get("rental_id", "").strip()
    redirect_endpoint = request.form.get("redirect_endpoint", "").strip()
    feedback_role = request.form.get("feedback_role", "").strip().lower()

    if redirect_endpoint not in {"rentals", "owner_cars"}:
        redirect_endpoint = "rentals" if has_role("renter") else "owner_cars"
    redirect_target = url_for(redirect_endpoint)

    if not description:
        return redirect(redirect_target)

    rental_id: Optional[int] = None
    if rental_id_value:
        try:
            rental_id = int(rental_id_value)
        except ValueError:
            rental_id = None

    if rental_id:
        redirect_url = _process_rental_complaint(
            rental_id, category, description)
        return redirect(redirect_url)

    if feedback_role not in {"renter", "owner", "both"}:
        if has_role("owner") and has_role("renter"):
            feedback_role = "both"
        elif has_role("owner"):
            feedback_role = "owner"
        else:
            feedback_role = "renter"

    db = get_db()
    cursor = db.execute(
        "INSERT INTO support_feedback (user_id, role, category, description, rental_id, created_at) VALUES (?, ?, ?, ?, ?, ?)",
        (g.user["id"], feedback_role, category,
         description, None, naive_utcnow_iso()),
    )
    db.commit()
    feedback_id = cursor.lastrowid
    feedback_link = url_for("admin_feedback_detail", feedback_id=feedback_id)
    admin_rows = db.execute(
        "SELECT id FROM users WHERE is_admin = 1").fetchall()
    for row in admin_rows:
        create_notification(
            row["id"],
            f"{display_name(g.user)} shared feedback: {category}.",
            feedback_link,
        )

    create_notification(
        g.user["id"],
        "Thanks for sharing your feedback. Our team will review it shortly.",
        redirect_target,
    )
    return redirect(redirect_target)


@app.route("/rentals/<int:rental_id>/review", methods=["POST"])
@login_required
@role_required("renter", "owner")
def renter_review(rental_id: int) -> str:
    db = get_db()
    rental = db.execute(
        """
        SELECT rentals.*, cars.owner_id, cars.name AS car_name, cars.brand, cars.model
        FROM rentals
        JOIN cars ON cars.id = rentals.car_id
        WHERE rentals.id = ? AND rentals.renter_id = ? AND rentals.status = 'completed'
        """,
        (rental_id, g.user["id"]),
    ).fetchone()
    if rental is None:
        abort(404)
    try:
        trip_rating = int(request.form.get("trip_rating", "0"))
        car_rating = int(request.form.get("car_rating", "0"))
        owner_rating = int(request.form.get("owner_rating", "0"))
    except (TypeError, ValueError):
        return redirect(url_for("rentals"))
    if not all(1 <= rating <= 5 for rating in (trip_rating, car_rating, owner_rating)):
        return redirect(url_for("rentals"))
    comment = request.form.get("comment", "").strip()
    existing = db.execute(
        "SELECT id FROM reviews WHERE rental_id = ? AND reviewer_id = ?",
        (rental_id, g.user["id"]),
    ).fetchone()
    now_iso = naive_utcnow_iso()
    if existing:
        db.execute(
            "UPDATE reviews SET trip_rating = ?, car_rating = ?, owner_rating = ?, passenger_rating = NULL, comment = ?, created_at = ? WHERE id = ?",
            (trip_rating, car_rating, owner_rating,
             comment, now_iso, existing["id"]),
        )
    else:
        db.execute(
            """
            INSERT INTO reviews (rental_id, reviewer_id, target_user_id, reviewer_role, target_role, trip_rating, car_rating, owner_rating, passenger_rating, comment, created_at)
            VALUES (?, ?, ?, 'renter', 'owner', ?, ?, ?, NULL, ?, ?)
            """,
            (rental_id, g.user["id"], rental["owner_id"],
             trip_rating, car_rating, owner_rating, comment, now_iso),
        )
    db.commit()
    car_label = rental["car_name"] or f"{rental['brand']} {rental['model']}"
    create_notification(
        rental["owner_id"],
        f"{g.user['username']} left a review for {car_label}.",
        url_for("owner_cars"),
    )
    return redirect(url_for("rentals"))


@app.route("/rentals/<int:rental_id>/respond", methods=["POST"])
@login_required
@role_required("renter", "owner")
def renter_respond_rental(rental_id: int) -> str:
    db = get_db()
    action = request.form.get("action")
    rental = db.execute(
        """
        SELECT rentals.*, cars.name AS car_name, cars.brand, cars.model, cars.owner_id, owners.username AS owner_username
        FROM rentals
        JOIN cars ON cars.id = rentals.car_id
        JOIN users AS owners ON owners.id = cars.owner_id
        WHERE rentals.id = ? AND rentals.renter_id = ?
        """,
        (rental_id, g.user["id"]),
    ).fetchone()
    if rental is None:
        abort(404)
    now = naive_utcnow()
    car_label = rental["car_name"] or f"{rental['brand']} {rental['model']}"
    if action == "accept_counter" and rental["owner_response"] == "counter":
        counter_amount = rental["counter_amount"] or rental["total_amount"]
        new_total = round(counter_amount, 2)
        delivery_fee_existing = float(rental["delivery_fee"] or 0.0)
        base_rental_updated = max(0.0, round(new_total - delivery_fee_existing, 2))
        db.execute(
            """
            UPDATE rentals
            SET owner_response = 'accepted',
                renter_response = 'accepted',
                renter_response_at = ?,
                total_amount = ?,
                rental_amount = ?,
                delivery_fee = ?,
                payment_status = 'awaiting_payment',
                payment_due_at = ?,
                payment_channel = 'manual',
                company_commission_amount = 0,
                owner_payout_amount = 0,
                owner_payout_status = 'pending',
                owner_payout_released_at = NULL
            WHERE id = ?
            """,
            (now.isoformat(), new_total,
             base_rental_updated,
             delivery_fee_existing,
             (now + timedelta(hours=1)).isoformat(), rental_id),
        )
        db.commit()
        create_notification(
            rental["owner_id"],
            f"{g.user['username']} accepted your revised price for {car_label}.",
            url_for("owner_cars"),
        )
    elif action == "decline_counter" and rental["owner_response"] == "counter":
        db.execute(
            "UPDATE rentals SET renter_response = 'declined', renter_response_at = ?, status = 'cancelled', cancel_reason = 'Counter offer declined' WHERE id = ?",
            (now.isoformat(), rental_id),
        )
        db.execute(
            "UPDATE cars SET is_available = 1, updated_at = ? WHERE id = ?",
            (now.isoformat(), rental["car_id"]),
        )
        db.commit()
        create_notification(
            rental["owner_id"],
            f"{g.user['username']} declined your price suggestion for {car_label}.",
            url_for("owner_cars"),
        )
    else:
        return redirect(url_for("rentals"))
    return redirect(url_for("rentals"))


def finalize_rental_payment(
    rental: sqlite3.Row,
    *,
    payment_channel: str = "manual",
    payment_gateway: str = "manual",
    payment_reference: str = "",
    payment_order_id: Optional[str] = None,
) -> Dict[str, float]:
    """Mark the rental as paid and return payout breakdown."""
    db = get_db()
    total_amount = float(rental["total_amount"] or 0)
    commission = round(total_amount * COMPANY_COMMISSION_RATE, 2)
    owner_net = max(0.0, round(total_amount - commission, 2))
    initial_payout = round(owner_net * OWNER_INITIAL_PAYOUT_RATE, 2)
    if owner_net <= 0:
        initial_payout = 0.0
    final_payout = max(0.0, round(owner_net - initial_payout, 2))
    if round(initial_payout + final_payout, 2) != round(owner_net, 2):
        final_payout = max(0.0, round(owner_net - initial_payout, 2))
    now_iso = naive_utcnow_iso()
    stored_order_id = payment_order_id or rental["payment_order_id"] or ""
    initial_status = "paid" if initial_payout > 0 else "not_required"
    initial_released_at = now_iso if initial_payout > 0 else None
    final_status = "pending" if final_payout > 0 else "not_required"
    db.execute(
        """
        UPDATE rentals
        SET payment_status = 'paid',
            payment_confirmed_at = ?,
            payment_channel = ?,
            payment_gateway = ?,
            payment_reference = ?,
            payment_order_id = ?,
            company_commission_amount = ?,
            owner_payout_amount = ?,
            owner_payout_status = ?,
            owner_payout_released_at = NULL,
            owner_initial_payout_amount = ?,
            owner_initial_payout_status = ?,
            owner_initial_payout_released_at = ?,
            owner_final_payout_amount = ?,
            owner_final_payout_status = ?,
            owner_final_payout_released_at = NULL,
            payment_due_at = NULL,
            renter_response = CASE WHEN renter_response = '' THEN 'paid' ELSE renter_response END
        WHERE id = ?
        """,
        (
            now_iso,
            payment_channel,
            payment_gateway,
            payment_reference,
            stored_order_id,
            commission,
            owner_net,
            final_status,
            initial_payout,
            initial_status,
            initial_released_at,
            final_payout,
            final_status,
            rental["id"],
        ),
    )
    db.commit()
    return {
        "commission": commission,
        "owner_net": owner_net,
        "initial_payout": initial_payout,
        "final_payout": final_payout,
    }


@app.route("/rentals/<int:rental_id>/pay", methods=["GET"])
@login_required
@role_required("renter", "owner")
def renter_payment_page(rental_id: int) -> str:
    db = get_db()
    rental = db.execute(
        """
        SELECT rentals.*,
               cars.name AS car_name,
               cars.brand,
               cars.model,
               cars.licence_plate,
               cars.owner_id AS owner_id,
               COALESCE(owners.account_name, owners.username) AS owner_display_name
        FROM rentals
        JOIN cars ON cars.id = rentals.car_id
        JOIN users AS owners ON owners.id = cars.owner_id
        WHERE rentals.id = ? AND rentals.renter_id = ?
        """,
        (rental_id, g.user["id"]),
    ).fetchone()
    if rental is None:
        abort(404)
    if rental["owner_response"] != "accepted":
        return redirect(url_for("rentals"))
    if rental["payment_status"] not in {"awaiting_payment", "pending"}:
        return redirect(url_for("rentals"))
    rental_dict, payment_summary = build_renter_payment_context(rental)
    return render_template(
        "renter_payment.html",
        rental=rental_dict,
        payment_summary=payment_summary,
    )


def build_renter_payment_context(rental_row: sqlite3.Row) -> Tuple[Dict[str, object], Dict[str, float]]:
    rental_dict = dict(rental_row)
    car_label = rental_dict.get("car_name") or f"{rental_dict.get('brand', '')} {rental_dict.get('model', '')}".strip()
    rental_dict["car_label"] = car_label.strip() or "Vehicle"
    rental_dict["owner_public_name"] = build_public_label(
        rental_dict.get("owner_display_name"),
        fallback_prefix="Host",
        fallback_id=rental_dict.get("owner_id"),
    )
    total_amount = float(rental_dict.get("total_amount") or 0.0)
    delivery_fee = float(rental_dict.get("delivery_fee") or 0.0)
    if delivery_fee < 0:
        delivery_fee = 0.0
    rental_amount = float(rental_dict.get("rental_amount") or 0.0)
    if total_amount <= 0:
        total_amount = round(rental_amount + delivery_fee, 2)
    base_from_total = max(0.0, round(total_amount - delivery_fee, 2))
    if rental_amount <= 0 or abs((rental_amount + delivery_fee) - total_amount) > 0.01:
        rental_amount = base_from_total
    else:
        rental_amount = round(rental_amount, 2)
    total_amount = round(rental_amount + delivery_fee, 2)
    commission_amount = round(total_amount * COMPANY_COMMISSION_RATE, 2)
    owner_net = max(0.0, round(total_amount - commission_amount, 2))
    initial_payout = round(owner_net * OWNER_INITIAL_PAYOUT_RATE, 2)
    if owner_net <= 0:
        initial_payout = 0.0
    final_payout = max(0.0, round(owner_net - initial_payout, 2))
    rental_dict["rental_amount"] = rental_amount
    rental_dict["delivery_fee"] = delivery_fee
    rental_dict["total_amount"] = total_amount
    payment_summary = {
        "total_amount": round(total_amount, 2),
        "rental_amount": round(rental_amount, 2),
        "delivery_fee": round(delivery_fee, 2),
        "commission_amount": commission_amount,
        "owner_net": owner_net,
        "initial_payout": initial_payout,
        "final_payout": final_payout,
        "commission_rate": int(COMPANY_COMMISSION_RATE * 100),
        "initial_rate": int(OWNER_INITIAL_PAYOUT_RATE * 100),
        "final_rate": int(OWNER_FINAL_PAYOUT_RATE * 100),
    }
    return rental_dict, payment_summary


@app.route("/rentals/<int:rental_id>/payment-instructions", methods=["POST", "GET"])
@login_required
@role_required("renter", "owner")
def renter_payment_instructions(rental_id: int) -> str:
    payment_channel = request.values.get("payment_channel", "upi/netbanking")
    db = get_db()
    rental = db.execute(
        """
        SELECT rentals.*,
               cars.name AS car_name,
               cars.brand,
               cars.model,
               cars.licence_plate,
               cars.owner_id AS owner_id,
               COALESCE(owners.account_name, owners.username) AS owner_display_name
        FROM rentals
        JOIN cars ON cars.id = rentals.car_id
        JOIN users AS owners ON owners.id = cars.owner_id
        WHERE rentals.id = ? AND rentals.renter_id = ?
        """,
        (rental_id, g.user["id"]),
    ).fetchone()
    if rental is None:
        abort(404)
    if rental["owner_response"] != "accepted":
        return redirect(url_for("rentals"))
    if rental["payment_status"] not in {"awaiting_payment", "pending"}:
        return redirect(url_for("rentals"))
    rental_dict, payment_summary = build_renter_payment_context(rental)
    admin_payout = get_primary_admin_payout_details()
    company_payout = get_company_payout_details()
    payment_account = {
        "account_holder": first_non_empty(
            admin_payout.get("account_holder"),
            company_payout.get("account_holder"),
            COMPANY_BANK_DETAILS.get("account_name"),
        ),
        "account_number": first_non_empty(
            admin_payout.get("account_number"),
            company_payout.get("account_number"),
            COMPANY_BANK_DETAILS.get("account_number"),
        ),
        "ifsc_code": first_non_empty(
            admin_payout.get("ifsc_code"),
            company_payout.get("ifsc_code"),
            COMPANY_BANK_DETAILS.get("ifsc"),
        ),
        "bank_name": first_non_empty(
            admin_payout.get("bank_name"),
            company_payout.get("bank_name"),
        ),
        "branch_address": first_non_empty(
            admin_payout.get("branch_address"),
            company_payout.get("branch_address"),
        ),
    }
    payment_account["ifsc_code"] = payment_account["ifsc_code"].upper()
    upi_id = first_non_empty(
        admin_payout.get("upi_id"),
        company_payout.get("upi_id"),
        COMPANY_UPI_ID,
    )
    whatsapp_link = f"https://wa.me/{COMPANY_SUPPORT_WHATSAPP.lstrip('+')}"
    return render_template(
        "renter_payment_instructions.html",
        rental=rental_dict,
        payment_summary=payment_summary,
        payment_channel=payment_channel,
        whatsapp_link=whatsapp_link,
        payment_account=payment_account,
        payment_upi=upi_id,
    )


@app.route("/rentals/<int:rental_id>/confirm-payment", methods=["POST"])
@login_required
@role_required("renter", "owner")
def renter_confirm_payment(rental_id: int) -> str:
    db = get_db()
    rental = db.execute(
        """
        SELECT rentals.*, cars.owner_id, cars.name AS car_name, cars.brand, cars.model
        FROM rentals
        JOIN cars ON cars.id = rentals.car_id
        WHERE rentals.id = ? AND rentals.renter_id = ? AND rentals.owner_response = 'accepted'
        """,
        (rental_id, g.user["id"]),
    ).fetchone()
    if rental is None:
        abort(404)
    payment_channel = request.form.get(
        "payment_channel", "upi/netbanking").strip().lower() or "upi/netbanking"
    payout_breakdown = finalize_rental_payment(
        rental,
        payment_channel=payment_channel,
        payment_gateway="manual",
        payment_reference="manual-confirmation",
    )
    car_label = rental["car_name"] or f"{rental['brand']} {rental['model']}"
    create_notification(
        rental["owner_id"],
        f"{g.user['username']} confirmed payment for {car_label}. 10% host payout released now; remaining 90% reserved until the trip completes.",
        url_for("owner_cars"),
    )
    return redirect(url_for("rentals"))


@app.route("/rentals/<int:rental_id>/cancel", methods=["POST"])
@login_required
@role_required("renter", "owner")
def cancel_rental(rental_id: int) -> str:
    db = get_db()
    rental = db.execute(
        "SELECT id, car_id, status FROM rentals WHERE id = ? AND renter_id = ?",
        (rental_id, g.user["id"]),
    ).fetchone()
    if rental is None or rental["status"] not in ("booked", "active"):
        abort(404)
    cancelled_at = naive_utcnow_iso()
    db.execute(
        "UPDATE rentals SET status = 'cancelled', end_time = ?, renter_response = 'cancelled_by_renter', renter_response_at = ? WHERE id = ?",
        (cancelled_at, cancelled_at, rental_id),
    )
    db.execute(
        "UPDATE cars SET is_available = 1, updated_at = ? WHERE id = ?",
        (naive_utcnow_iso(), rental["car_id"]),
    )
    db.commit()
    return redirect(url_for("rentals"))


@app.route("/owner/cars")
@login_required
@role_required("owner")
def owner_cars() -> str:
    context = build_owner_dashboard_context(g.user["id"])
    return render_template("owner_cars.html", **context)


def build_owner_dashboard_context(owner_id: int) -> Dict[str, object]:
    db = get_db()
    car_rows = db.execute(
        "SELECT * FROM cars WHERE owner_id = ? ORDER BY created_at DESC",
        (owner_id,),
    ).fetchall()
    car_dicts = []
    car_ids = [row["id"] for row in car_rows]
    images = fetch_car_images(car_ids)
    delivery_map = fetch_car_delivery_options(car_ids)
    for row in car_rows:
        car = dict(row)
        car["images"] = images.get(row["id"], [])
        car["delivery_options"] = delivery_map.get(row["id"], {})
        car_dicts.append(car)
    cars_by_id = {car["id"]: car for car in car_dicts}
    rental_rows = db.execute(
        """
        SELECT rentals.*, users.username AS renter_username,
               COALESCE(users.account_name, '') AS renter_account_name,
               cars.brand,
               cars.model,
               cars.name AS car_name,
               cars.latitude AS car_latitude,
               cars.longitude AS car_longitude,
               cars.city AS car_city,
               cars.image_url AS car_image_url,
               cars.has_gps AS car_has_gps,
               cars.vehicle_type AS car_vehicle_type,
               cars.seats AS car_seats
        FROM rentals
        JOIN cars ON cars.id = rentals.car_id
        JOIN users ON users.id = rentals.renter_id
        WHERE cars.owner_id = ? AND rentals.status IN ('booked', 'active', 'completed')
        ORDER BY CASE
            WHEN rentals.owner_response = 'pending' THEN 0
            WHEN rentals.owner_response = 'counter' THEN 1
            WHEN rentals.status = 'active' THEN 2
            WHEN rentals.status = 'booked' THEN 3
            ELSE 4
        END, rentals.start_time DESC
        """,
        (owner_id,),
    ).fetchall()
    city_entries = load_city_entries(include_coordinates=True)
    label_lookup: Dict[str, Tuple[float, float]] = {}
    for entry in city_entries:
        try:
            lat_val = entry.get("latitude")
            lng_val = entry.get("longitude")
            if lat_val is None or lng_val is None:
                continue
            label_lookup[_format_city_label(entry["name"], entry["state"]).strip().lower()] = (
                float(lat_val),
                float(lng_val),
            )
        except (ValueError, TypeError):
            continue
    rentals_data: List[Dict[str, object]] = []
    for row in rental_rows:
        rental = dict(row)
        rental["has_gps"] = bool(rental.pop("car_has_gps", rental.get("car_has_gps", 0)))
        rental["car_image_url"] = (rental.get("car_image_url") or "").strip()
        rental["car_vehicle_type"] = (rental.get("car_vehicle_type") or "").strip()
        try:
            rental["car_seats"] = int(rental.get("car_seats") or 0)
        except (TypeError, ValueError):
            rental["car_seats"] = 0
        renter_label = build_public_label(
            rental.get("renter_account_name") or rental.get("renter_username"),
            fallback_prefix="Guest",
            fallback_id=rental.get("renter_id"),
        )
        rental["renter_public_name"] = renter_label
        rental["renter_username"] = renter_label
        rental.pop("renter_account_name", None)
        try:
            rental["trip_destinations_list"] = json.loads(
                rental.get("trip_destinations") or "[]"
            )
        except (TypeError, json.JSONDecodeError):
            rental["trip_destinations_list"] = []
        total_amount = float(rental.get("total_amount") or 0.0)
        delivery_fee_value = float(rental.get("delivery_fee") or 0.0)
        base_rental_amount = float(rental.get("rental_amount") or 0.0)
        if base_rental_amount <= 0 or abs((base_rental_amount + delivery_fee_value) - total_amount) > 0.01:
            base_rental_amount = max(0.0, round(total_amount - delivery_fee_value, 2))
        else:
            base_rental_amount = round(base_rental_amount, 2)
        rental["host_rental_amount"] = base_rental_amount
        service_fee_amount = round(total_amount * HOST_SERVICE_FEE_RATE, 2)
        rental["host_service_fee"] = service_fee_amount
        rental["host_service_fee_rate"] = int(HOST_SERVICE_FEE_RATE * 100)
        rental["host_net_take_home"] = max(0.0, round(total_amount - service_fee_amount, 2))
        initial_status = (rental.get("owner_initial_payout_status") or "").lower()
        final_status = (rental.get("owner_final_payout_status") or "").lower()
        owner_status = (rental.get("owner_payout_status") or "").lower()
        initial_paid = float(rental.get("owner_initial_payout_amount") or 0.0) if initial_status == "paid" else 0.0
        final_paid = float(rental.get("owner_final_payout_amount") or 0.0) if final_status == "paid" else 0.0
        owner_net = float(rental.get("host_net_take_home") or 0.0)
        owner_total_paid = initial_paid + final_paid
        if owner_total_paid <= 0 and owner_status == "paid":
            owner_total_paid = float(rental.get("owner_payout_amount") or owner_net)
        owner_total_paid = min(owner_net, round(owner_total_paid, 2)) if owner_net else round(owner_total_paid, 2)
        rental["host_amount_received"] = owner_total_paid
        rental["host_amount_balance"] = max(0.0, round(owner_net - owner_total_paid, 2))
        delivery_lat = rental.get("delivery_latitude")
        delivery_lng = rental.get("delivery_longitude")
        rental["delivery_latitude"] = float(delivery_lat) if delivery_lat is not None else None
        rental["delivery_longitude"] = float(delivery_lng) if delivery_lng is not None else None
        car_lat = rental.get("car_latitude")
        car_lng = rental.get("car_longitude")
        rental["car_latitude"] = float(car_lat) if car_lat is not None else None
        rental["car_longitude"] = float(car_lng) if car_lng is not None else None
        destinations_with_coords: List[Dict[str, object]] = []
        for destination in rental["trip_destinations_list"]:
            entry: Dict[str, object] = {"name": destination}
            coords = lookup_city_coordinates(destination)
            if not coords:
                lookup_key = destination.strip().lower()
                coords = label_lookup.get(lookup_key)
            if not coords and "," in destination:
                first_part = destination.split(",", 1)[0].strip().lower()
                coords = label_lookup.get(first_part)
            if coords:
                entry["latitude"], entry["longitude"] = coords
            destinations_with_coords.append(entry)
        rental["trip_destinations_map"] = destinations_with_coords
        rentals_data.append(rental)
    rental_ids = [int(r.get("id")) for r in rentals_data if r.get("id") is not None]
    renter_reviews_by_rental: Dict[int, Dict[str, object]] = {}
    owner_reviews_by_rental: Dict[int, Dict[str, object]] = {}
    if rental_ids:
        placeholders = ",".join("?" for _ in rental_ids)
        review_rows = db.execute(
            f"""
            SELECT rental_id, trip_rating, car_rating, owner_rating, comment, created_at
            FROM reviews
            WHERE rental_id IN ({placeholders})
              AND reviewer_role = 'renter'
              AND target_user_id = ?
            """,
            (*rental_ids, owner_id),
        ).fetchall()
        for review_row in review_rows:
            review_dict = {
                "trip_rating": review_row["trip_rating"],
                "car_rating": review_row["car_rating"],
                "owner_rating": review_row["owner_rating"],
                "comment": (review_row["comment"] or "").strip(),
                "created_at": review_row["created_at"],
            }
            renter_reviews_by_rental[int(review_row["rental_id"])] = review_dict
        owner_review_rows = db.execute(
            f"""
            SELECT rental_id, passenger_rating, comment, created_at
            FROM reviews
            WHERE rental_id IN ({placeholders})
              AND reviewer_role = 'owner'
              AND reviewer_id = ?
            """,
            (*rental_ids, owner_id),
        ).fetchall()
        for review_row in owner_review_rows:
            owner_reviews_by_rental[int(review_row["rental_id"])] = {
                "passenger_rating": review_row["passenger_rating"],
                "comment": (review_row["comment"] or "").strip(),
                "created_at": review_row["created_at"],
            }
    for rental in rentals_data:
        rental_id_raw = rental.get("id")
        try:
            sort_key = int(rental_id_raw)
        except (TypeError, ValueError):
            sort_key = 0
        rental["_booking_sort_key"] = sort_key
        rental["renter_review"] = renter_reviews_by_rental.get(sort_key)
        rental["host_review"] = owner_reviews_by_rental.get(sort_key)
        car = cars_by_id.get(rental.get("car_id"))
        images_for_car = []
        if car:
            images_for_car = car.get("images") or []
        primary_image = ""
        if images_for_car:
            primary_image = url_for("serve_upload", filename=images_for_car[0])
        elif car and car.get("image_url"):
            raw_image = str(car.get("image_url")).strip()
            if raw_image.startswith(("http://", "https://")):
                primary_image = raw_image
            else:
                primary_image = url_for("serve_upload", filename=raw_image.lstrip("/"))
        elif rental.get("car_image_url"):
            raw_image = rental["car_image_url"]
            if raw_image.startswith(("http://", "https://")):
                primary_image = raw_image
            else:
                primary_image = url_for("serve_upload", filename=raw_image.lstrip("/"))
        rental["primary_image_url"] = primary_image
        brand = (rental.get("brand") or "").strip()
        model = (rental.get("model") or "").strip()
        fallback_name = " ".join(part for part in (brand, model) if part)
        display_name = first_non_empty(rental.get("car_name"), fallback_name)
        rental["car_display_name"] = display_name or "Vehicle"
    vehicle_type_values = load_vehicle_type_options()
    other_vehicle_types = [
        vt for vt in vehicle_type_values if vt not in POPULAR_VEHICLE_TYPES]
    fuel_types = build_fuel_type_list()
    pending_requests = [r for r in rentals_data if r.get("owner_response") == "pending"]
    counter_requests = [r for r in rentals_data if r.get("owner_response") == "counter"]
    awaiting_payments = [r for r in rentals_data if r.get("payment_status") == "awaiting_payment"]
    ready_to_start = [
        r
        for r in rentals_data
        if r.get("status") == "booked"
        and r.get("owner_response") == "accepted"
        and r.get("payment_status") == "paid"
    ]
    active_trips = [r for r in rentals_data if r.get("status") == "active"]
    completed_trips = [
        r
        for r in rentals_data
        if r.get("status") == "completed" and r.get("id") not in owner_reviews_by_rental
    ]
    active_trip_car_ids = {r["car_id"] for r in active_trips}
    in_progress_car_ids = active_trip_car_ids | {r["car_id"] for r in ready_to_start}
    trip_history = sorted(rentals_data, key=lambda r: r.get("_booking_sort_key", 0), reverse=True)
    for rental in rentals_data:
        rental.pop("_booking_sort_key", None)
    for car in car_dicts:
        car_id = car["id"]
        car["has_active_trip"] = car_id in active_trip_car_ids
        car["has_trip_in_progress"] = car_id in in_progress_car_ids
    pending_total_amount = sum(
        float(r.get("total_amount") or 0) for r in awaiting_payments
    )
    return {
        "cars": car_dicts,
        "rentals": rentals_data,
        "pending_payments": awaiting_payments,
        "pending_total_amount": pending_total_amount,
        "pending_requests": pending_requests,
        "counter_requests": counter_requests,
        "awaiting_payments": awaiting_payments,
        "ready_to_start": ready_to_start,
        "active_trips": active_trips,
        "completed_trips": completed_trips,
        "trip_history": trip_history,
        "commission_rate": int(COMPANY_COMMISSION_RATE * 100),
        "city_entries": city_entries,
        "popular_vehicle_types": POPULAR_VEHICLE_TYPES,
        "other_vehicle_types": other_vehicle_types,
        "fuel_types": fuel_types,
        "delivery_choices": DELIVERY_DISTANCE_CHOICES,
        "cars_by_id": cars_by_id,
    }


@app.route("/owner/trips")
@login_required
@role_required("owner")
def owner_trip_history() -> str:
    context = build_owner_dashboard_context(g.user["id"])
    trips = context.get("trip_history", [])
    cars_by_id = context.get("cars_by_id", {})
    return render_template(
        "owner_trip_history.html",
        trips=trips,
        cars_by_id=cars_by_id,
        service_fee_rate=int(HOST_SERVICE_FEE_RATE * 100),
    )


@app.route("/owner/trips/<string:category>")
@login_required
@role_required("owner")
def owner_trip_list(category: str) -> str:
    context = build_owner_dashboard_context(g.user["id"])
    category = category.lower()
    trip_map = {
        "active": context["active_trips"],
        "pending": context["pending_requests"] + context["counter_requests"],
        "in-progress": context["ready_to_start"] + context["active_trips"],
    }
    if category not in trip_map:
        abort(404)
    trips = []
    for rental in trip_map[category]:
        trip = dict(rental)
        amount_received = float(trip.get("owner_initial_payout_amount") or 0.0)
        trip["host_amount_received"] = round(amount_received, 2)
        trip["host_amount_balance"] = max(
            0.0, round(float(trip.get("host_net_take_home") or 0.0) - amount_received, 2)
        )
        trips.append(trip)
    title_lookup = {
        "active": "Trips currently in progress",
        "pending": "Pending booking requests",
        "in-progress": "Trips in progress",
    }
    status_note = {
        "active": "These trips are currently marked as active.",
        "pending": "Bookings awaiting your response are listed below.",
        "in-progress": "Trips that are scheduled to start soon or already active are shown below.",
    }
    cars_by_id = {car["id"]: car for car in context["cars"]}
    return render_template(
        "owner_trip_list.html",
        trips=trips,
        cars_by_id=cars_by_id,
        category=category,
        title=title_lookup.get(category, "Trips"),
        description=status_note.get(category, ""),
        service_fee_rate=int(HOST_SERVICE_FEE_RATE * 100),
    )


@app.route("/owner/cars/add", methods=["POST"])
@login_required
@role_required("owner")
def owner_add_car() -> str:
    form = request.form
    if not g.profile_complete:
        return redirect(url_for("profile", message="Complete your host profile before listing cars."))
    try:
        seats = int(form.get("seats", 4))
    except (TypeError, ValueError):
        return redirect(url_for("owner_cars"))
    rate_unit = (form.get("rate_unit", "hour") or "hour").strip().lower()
    rate_amount_raw = form.get("rate_amount", "")
    try:
        rate_amount = float(rate_amount_raw) if rate_amount_raw not in (
            None, "") else 0.0
    except (TypeError, ValueError):
        rate_amount = 0.0
    if rate_amount < 0:
        rate_amount = 0.0
    if rate_unit not in {"hour", "day"}:
        rate_unit = "hour"
    if rate_unit == "day":
        daily_rate = rate_amount
        rate_per_hour = round(rate_amount / 24, 2) if rate_amount else 0.0
    else:
        rate_per_hour = rate_amount
        daily_rate = round(rate_amount * 24, 2)
    try:
        latitude = float(form.get("latitude"))
        longitude = float(form.get("longitude"))
    except (TypeError, ValueError):
        return redirect(url_for("owner_cars"))
    vehicle_type = form.get("vehicle_type", "").strip() or "car"
    size_category = form.get("size_category", "").strip()
    has_gps = 1 if form.get("has_gps") else 0
    city_raw = (form.get("city") or "").strip()
    city_value = city_raw.split(",")[0].strip() if city_raw else ""
    db = get_db()
    cursor = db.execute(
        """
        INSERT INTO cars (owner_id, name, brand, model, licence_plate, seats, rate_per_hour, daily_rate,
                          vehicle_type, size_category, has_gps, latitude, longitude, city, image_url, fuel_type,
                          transmission, description)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            g.user["id"],
            (form.get("name")
             or f"{form.get('brand', '')} {form.get('model', '')}").strip(),
            form.get("brand", "Unknown"),
            form.get("model", "Unknown"),
            form.get("licence_plate", ""),
            seats,
            rate_per_hour,
            daily_rate,
            vehicle_type,
            size_category,
            has_gps,
            latitude,
            longitude,
            city_value,
            form.get("image_url", ""),
            form.get("fuel_type", ""),
            form.get("transmission", ""),
            form.get("description", ""),
        ),
    )
    car_id = cursor.lastrowid
    save_car_images(car_id, request.files.getlist("photos"))
    enabled_delivery = {
        int(value)
        for value in request.form.getlist("delivery_options")
        if value and value.isdigit()
    }
    delivery_values: Dict[int, float] = {}
    for distance in DELIVERY_DISTANCE_CHOICES:
        if distance not in enabled_delivery:
            continue
        price_raw = request.form.get(f"delivery_price_{distance}", "").strip()
        price_value = parse_float(price_raw, 0.0) or 0.0
        if price_value < 0:
            price_value = 0.0
        delivery_values[distance] = float(price_value)
    save_car_delivery_options(car_id, delivery_values)
    db.commit()
    return redirect(url_for("owner_cars"))


@app.route("/owner/cars/<int:car_id>/availability", methods=["POST"])
@login_required
@role_required("owner")
def owner_toggle_availability(car_id: int) -> str:
    db = get_db()
    car = db.execute(
        "SELECT id, is_available FROM cars WHERE id = ? AND owner_id = ?",
        (car_id, g.user["id"]),
    ).fetchone()
    if car is None:
        abort(404)
    new_state = 0 if car["is_available"] else 1
    db.execute(
        "UPDATE cars SET is_available = ?, updated_at = ? WHERE id = ?",
        (new_state, naive_utcnow_iso(), car_id),
    )
    db.commit()
    return redirect(url_for("owner_cars"))


@app.route("/owner/cars/<int:car_id>/location", methods=["POST"])
@login_required
@role_required("owner")
def owner_update_location(car_id: int) -> str:
    try:
        latitude = float(request.form.get("latitude"))
        longitude = float(request.form.get("longitude"))
    except (TypeError, ValueError):
        return redirect(url_for("owner_cars"))
    db = get_db()
    updated = db.execute(
        "UPDATE cars SET latitude = ?, longitude = ?, updated_at = ? WHERE id = ? AND owner_id = ?",
        (latitude, longitude, naive_utcnow_iso(), car_id, g.user["id"]),
    )
    db.commit()
    if updated.rowcount == 0:
        abort(404)
    return redirect(url_for("owner_cars"))


@app.route("/owner/cars/<int:car_id>/data")
@login_required
@role_required("owner", "admin")
def owner_get_car_data(car_id: int):
    db = get_db()
    is_admin = bool(g.user and g.user.get("is_admin"))
    if is_admin:
        row = db.execute("SELECT * FROM cars WHERE id = ?", (car_id,)).fetchone()
    else:
        row = db.execute(
            "SELECT * FROM cars WHERE id = ? AND owner_id = ?",
            (car_id, g.user["id"]),
        ).fetchone()
    if row is None:
        abort(404)
    car = dict(row)
    image_rows = db.execute(
        "SELECT id, filename FROM car_images WHERE car_id = ? ORDER BY created_at",
        (car_id,),
    ).fetchall()
    delivery_options = fetch_car_delivery_options([car_id]).get(car_id, {})
    images = [
        {
            "id": image_row["id"],
            "filename": image_row["filename"],
            "url": url_for("serve_upload", filename=image_row["filename"]),
        }
        for image_row in image_rows
    ]
    display_name = (car.get("name") or "").strip()
    if not display_name:
        brand = (car.get("brand") or "").strip()
        model = (car.get("model") or "").strip()
        display_name = f"{brand} {model}".strip()
    payload = {
        "id": car["id"],
        "name": car.get("name") or "",
        "brand": car.get("brand") or "",
        "model": car.get("model") or "",
        "licence_plate": car.get("licence_plate") or "",
        "vehicle_type": car.get("vehicle_type") or "",
        "size_category": car.get("size_category") or "",
        "fuel_type": car.get("fuel_type") or "",
        "transmission": car.get("transmission") or "",
        "city": car.get("city") or "",
        "seats": car.get("seats") or 0,
        "rate_per_hour": car.get("rate_per_hour") or 0,
        "daily_rate": car.get("daily_rate") or 0,
        "has_gps": bool(car.get("has_gps")),
        "image_url": car.get("image_url") or "",
        "description": car.get("description") or "",
        "latitude": car.get("latitude"),
        "longitude": car.get("longitude"),
        "images": images,
        "is_available": bool(car.get("is_available")),
        "display_name": display_name,
        "detail_url": url_for("owner_get_car_data", car_id=car_id),
        "update_url": url_for("owner_update_car", car_id=car_id),
        "delivery_options": delivery_options,
    }
    return jsonify({"car": payload})


@app.route("/owner/cars/<int:car_id>/update", methods=["POST"])
@login_required
@role_required("owner", "admin")
def owner_update_car(car_id: int):
    db = get_db()
    is_admin = bool(g.user and g.user.get("is_admin"))
    if is_admin:
        existing = db.execute("SELECT * FROM cars WHERE id = ?", (car_id,)).fetchone()
    else:
        existing = db.execute(
            "SELECT * FROM cars WHERE id = ? AND owner_id = ?",
            (car_id, g.user["id"]),
        ).fetchone()
    if existing is None:
        abort(404)
    existing_car = dict(existing)
    form = request.form

    def parse_int(value: Optional[str], default: int) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    def parse_float(value: Optional[str], default: float) -> float:
        try:
            if value is None or value == "":
                return default
            return float(value)
        except (TypeError, ValueError):
            return default

    name = (form.get("name") or "").strip()
    brand = (form.get("brand") or existing_car.get("brand") or "").strip()
    model = (form.get("model") or existing_car.get("model") or "").strip()
    if not brand:
        brand = "Unknown"
    if not model:
        model = "Unknown"
    if not name:
        name = f"{brand} {model}".strip()
    licence_plate = (form.get("licence_plate")
                     or existing_car.get("licence_plate") or "").strip()
    vehicle_type = (form.get("vehicle_type")
                    or existing_car.get("vehicle_type") or "").strip()
    size_category = (form.get("size_category")
                     or existing_car.get("size_category") or "").strip()
    fuel_type = (form.get("fuel_type") or existing_car.get(
        "fuel_type") or "").strip()
    transmission = (form.get("transmission")
                    or existing_car.get("transmission") or "").strip()
    description = (form.get("description")
                   or existing_car.get("description") or "").strip()
    seats = parse_int(form.get("seats"), existing_car.get("seats") or 4)
    rate_per_hour = parse_float(
        form.get("rate_per_hour"), existing_car.get("rate_per_hour") or 0.0)
    daily_rate = parse_float(form.get("daily_rate"),
                             existing_car.get("daily_rate") or 0.0)
    image_url = (form.get("image_url") or existing_car.get(
        "image_url") or "").strip()
    latitude = parse_float(form.get("latitude"),
                           existing_car.get("latitude") or 0.0)
    longitude = parse_float(form.get("longitude"),
                            existing_car.get("longitude") or 0.0)
    has_gps = 1 if form.get("has_gps") in {"on", "1", "true", "yes"} else 0
    city_raw = (form.get("city") or existing_car.get("city") or "").strip()
    city_value = city_raw.split(",")[0].strip() if city_raw else ""

    update_params: List[object] = [
        name,
        brand,
        model,
        licence_plate,
        seats,
        rate_per_hour,
        daily_rate,
        vehicle_type,
        size_category,
        has_gps,
        fuel_type,
        transmission,
        city_value,
        image_url,
        description,
        latitude,
        longitude,
        naive_utcnow_iso(),
        car_id,
    ]
    update_query = """
        UPDATE cars
        SET name = ?, brand = ?, model = ?, licence_plate = ?, seats = ?, rate_per_hour = ?, daily_rate = ?,
            vehicle_type = ?, size_category = ?, has_gps = ?, fuel_type = ?, transmission = ?, city = ?,
            image_url = ?, description = ?, latitude = ?, longitude = ?, updated_at = ?
        WHERE id = ?
    """
    if not is_admin:
        update_query += " AND owner_id = ?"
        update_params.append(g.user["id"])
    db.execute(update_query, update_params)

    image_rows = db.execute(
        "SELECT id, filename FROM car_images WHERE car_id = ? ORDER BY created_at",
        (car_id,),
    ).fetchall()
    delete_ids = {
        int(value)
        for value in form.getlist("delete_images")
        if value and value.isdigit()
    }
    if delete_ids:
        rows_to_delete = [row for row in image_rows if row["id"] in delete_ids]
        if rows_to_delete:
            placeholders = ",".join("?" for _ in rows_to_delete)
            params = [car_id] + [row["id"] for row in rows_to_delete]
            db.execute(
                f"DELETE FROM car_images WHERE car_id = ? AND id IN ({placeholders})",
                params,
            )
            for row in rows_to_delete:
                file_path = UPLOAD_ROOT.joinpath(row["filename"])
                try:
                    file_path.unlink()
                except FileNotFoundError:
                    pass
        image_rows = [row for row in image_rows if row["id"] not in delete_ids]

    remaining_count = len(image_rows)
    new_files = request.files.getlist("new_photos")
    available_slots = max(0, MAX_CAR_IMAGES - remaining_count)
    if available_slots and new_files:
        save_car_images(car_id, new_files, limit=available_slots)

    enabled_distances = {
        int(value)
        for value in form.getlist("delivery_options")
        if value and value.isdigit()
    }
    delivery_updates: Dict[int, float] = {}
    for distance in DELIVERY_DISTANCE_CHOICES:
        if distance not in enabled_distances:
            continue
        price_raw = form.get(f"delivery_price_{distance}", "").strip()
        price_value = parse_float(price_raw, 0.0) or 0.0
        if price_value < 0:
            price_value = 0.0
        delivery_updates[distance] = float(price_value)
    save_car_delivery_options(car_id, delivery_updates)

    db.commit()

    if is_admin:
        updated = db.execute("SELECT * FROM cars WHERE id = ?", (car_id,)).fetchone()
    else:
        updated = db.execute(
            "SELECT * FROM cars WHERE id = ? AND owner_id = ?",
            (car_id, g.user["id"]),
        ).fetchone()
    image_rows = db.execute(
        "SELECT id, filename FROM car_images WHERE car_id = ? ORDER BY created_at",
        (car_id,),
    ).fetchall()
    image_payload = [
        {
            "id": row["id"],
            "filename": row["filename"],
            "url": url_for("serve_upload", filename=row["filename"]),
        }
        for row in image_rows
    ]
    delivery_payload = fetch_car_delivery_options([car_id]).get(car_id, {})
    updated_car = dict(updated)
    display_name = (updated_car.get("name") or "").strip()
    if not display_name:
        brand = (updated_car.get("brand") or "").strip()
        model = (updated_car.get("model") or "").strip()
        display_name = f"{brand} {model}".strip()
    response = {
        "id": updated_car["id"],
        "name": updated_car.get("name") or "",
        "brand": updated_car.get("brand") or "",
        "model": updated_car.get("model") or "",
        "licence_plate": updated_car.get("licence_plate") or "",
        "vehicle_type": updated_car.get("vehicle_type") or "",
        "size_category": updated_car.get("size_category") or "",
        "fuel_type": updated_car.get("fuel_type") or "",
        "transmission": updated_car.get("transmission") or "",
        "city": updated_car.get("city") or "",
        "seats": updated_car.get("seats") or 0,
        "rate_per_hour": updated_car.get("rate_per_hour") or 0,
        "daily_rate": updated_car.get("daily_rate") or 0,
        "has_gps": bool(updated_car.get("has_gps")),
        "image_url": updated_car.get("image_url") or "",
        "description": updated_car.get("description") or "",
        "latitude": updated_car.get("latitude"),
        "longitude": updated_car.get("longitude"),
        "images": image_payload,
        "is_available": bool(updated_car.get("is_available")),
        "display_name": display_name,
        "detail_url": url_for("owner_get_car_data", car_id=car_id),
        "update_url": url_for("owner_update_car", car_id=car_id),
        "delivery_options": delivery_payload,
    }
    return jsonify({"success": True, "car": response})


@app.route("/owner/rentals/<int:rental_id>/start", methods=["POST"])
@login_required
@role_required("owner")
def owner_start_rental(rental_id: int) -> str:
    db = get_db()
    rental = db.execute(
        """
        SELECT rentals.*, cars.name AS car_name, cars.brand, cars.model
        FROM rentals
        JOIN cars ON cars.id = rentals.car_id
        WHERE rentals.id = ? AND cars.owner_id = ? AND rentals.status = 'booked' AND rentals.owner_response = 'accepted'
        """,
        (rental_id, g.user["id"]),
    ).fetchone()
    if rental is None or rental["payment_status"] != "paid":
        abort(404)
    now_iso = naive_utcnow_iso()
    db.execute(
        "UPDATE rentals SET status = 'active', owner_started_at = ? WHERE id = ?",
        (now_iso, rental_id),
    )
    db.commit()
    car_label = rental["car_name"] or f"{rental['brand']} {rental['model']}"
    create_notification(
        rental["renter_id"],
        f"{g.user['username']} marked your trip with {car_label} as started.",
        url_for("rentals"),
    )
    return redirect(url_for("owner_cars"))


@app.route("/owner/rentals/<int:rental_id>/extend", methods=["POST"])
@login_required
@role_required("owner")
def owner_extend_rental(rental_id: int) -> str:
    db = get_db()
    try:
        extra_hours = int(request.form.get("extra_hours", "0"))
    except (TypeError, ValueError):
        extra_hours = 0
    if extra_hours <= 0:
        return redirect(url_for("owner_cars"))
    rental = db.execute(
        """
        SELECT rentals.*, cars.rate_per_hour, cars.name AS car_name, cars.brand, cars.model
        FROM rentals
        JOIN cars ON cars.id = rentals.car_id
        WHERE rentals.id = ? AND cars.owner_id = ? AND rentals.status IN ('booked', 'active')
        """,
        (rental_id, g.user["id"]),
    ).fetchone()
    if rental is None:
        abort(404)
    end_dt = parse_iso(rental["end_time"]) or naive_utcnow()
    new_end = end_dt + timedelta(hours=extra_hours)
    additional_amount = round(rental["rate_per_hour"] * extra_hours, 2)
    base_rental_amount = float(rental["rental_amount"] or 0)
    delivery_fee = float(rental["delivery_fee"] or 0)
    current_total = float(rental["total_amount"] or 0)
    new_rental_amount = round(base_rental_amount + additional_amount, 2)
    new_total = round(new_rental_amount + delivery_fee, 2)
    new_status = rental["payment_status"]
    new_due = rental["payment_due_at"]
    if rental["payment_status"] == "paid":
        new_status = "awaiting_payment"
        new_due = (naive_utcnow() + timedelta(hours=1)).isoformat()
    commission = float(rental["company_commission_amount"] or 0)
    owner_payout = float(rental["owner_payout_amount"] or 0)
    owner_payout_status = rental["owner_payout_status"] or "pending"
    owner_payout_released_at = rental["owner_payout_released_at"]
    owner_initial_amount = float(rental["owner_initial_payout_amount"] or 0)
    owner_initial_status = rental["owner_initial_payout_status"] or "pending"
    owner_initial_released = rental["owner_initial_payout_released_at"]
    owner_final_amount = float(rental["owner_final_payout_amount"] or 0)
    owner_final_status = rental["owner_final_payout_status"] or owner_payout_status
    owner_final_released = rental["owner_final_payout_released_at"]
    if new_status == "awaiting_payment":
        commission = 0
        owner_payout = 0
        owner_payout_status = "pending"
        owner_payout_released_at = None
        owner_initial_amount = 0
        owner_initial_status = "pending"
        owner_initial_released = None
        owner_final_amount = 0
        owner_final_status = "pending"
        owner_final_released = None
    db.execute(
        """
        UPDATE rentals
        SET end_time = ?,
            rental_amount = ?,
            total_amount = ?,
            payment_status = ?,
            payment_due_at = ?,
            payment_channel = CASE WHEN ? = 'awaiting_payment' THEN 'manual' ELSE payment_channel END,
            company_commission_amount = ?,
            owner_payout_amount = ?,
            owner_payout_status = ?,
            owner_payout_released_at = ?,
            owner_initial_payout_amount = ?,
            owner_initial_payout_status = ?,
            owner_initial_payout_released_at = ?,
            owner_final_payout_amount = ?,
            owner_final_payout_status = ?,
            owner_final_payout_released_at = ?
        WHERE id = ?
        """,
        (
            new_end.isoformat(),
            new_rental_amount,
            new_total,
            new_status,
            new_due,
            new_status,
            commission,
            owner_payout,
            owner_payout_status,
            owner_payout_released_at,
            owner_initial_amount,
            owner_initial_status,
            owner_initial_released,
            owner_final_amount,
            owner_final_status,
            owner_final_released,
            rental_id,
        ),
    )
    db.commit()
    car_label = rental["car_name"] or f"{rental['brand']} {rental['model']}"
    create_notification(
        rental["renter_id"],
        f"Your trip with {car_label} was extended by {extra_hours} hours. Additional amount: Rs {additional_amount:.0f}. New total: Rs {new_total:.0f}.",
        url_for("rentals"),
    )
    return redirect(url_for("owner_cars"))


@app.route("/owner/rentals/<int:rental_id>/complete", methods=["POST"])
@login_required
@role_required("owner")
def owner_complete_rental(rental_id: int) -> str:
    db = get_db()
    rental = db.execute(
        """
        SELECT rentals.id,
               rentals.car_id,
               rentals.renter_id,
               rentals.payment_status,
               rentals.total_amount,
               rentals.company_commission_amount,
               rentals.owner_payout_amount,
               rentals.owner_payout_status,
               rentals.owner_payout_released_at,
               rentals.owner_initial_payout_amount,
               rentals.owner_initial_payout_status,
               rentals.owner_initial_payout_released_at,
               rentals.owner_final_payout_amount,
               rentals.owner_final_payout_status,
               rentals.owner_final_payout_released_at,
               cars.name AS car_name,
               cars.brand,
               cars.model
        FROM rentals
        JOIN cars ON cars.id = rentals.car_id
        WHERE rentals.id = ? AND cars.owner_id = ? AND rentals.status IN ('booked', 'active')
        """,
        (rental_id, g.user["id"]),
    ).fetchone()
    if rental is None:
        abort(404)
    now_iso = naive_utcnow_iso()
    commission = float(rental["company_commission_amount"] or 0)
    owner_net = float(rental["owner_payout_amount"] or 0)
    owner_status = rental["owner_payout_status"] or "pending"
    payout_released_at = rental["owner_payout_released_at"]
    initial_payout = float(rental["owner_initial_payout_amount"] or 0)
    initial_status = rental["owner_initial_payout_status"] or "pending"
    initial_released = rental["owner_initial_payout_released_at"]
    final_payout = float(
        rental["owner_final_payout_amount"]
        if rental["owner_final_payout_amount"] is not None
        else max(0.0, owner_net - initial_payout)
    )
    final_status = rental["owner_final_payout_status"] or owner_status
    final_released = rental["owner_final_payout_released_at"]
    payment_status_after = rental["payment_status"]

    if payment_status_after != "paid":
        total_amount = float(rental["total_amount"] or 0)
        commission = round(total_amount * COMPANY_COMMISSION_RATE, 2)
        owner_net = max(0.0, round(total_amount - commission, 2))
        initial_payout = round(owner_net * OWNER_INITIAL_PAYOUT_RATE, 2)
        if owner_net <= 0:
            initial_payout = 0.0
        final_payout = max(0.0, round(owner_net - initial_payout, 2))
        payment_status_after = "paid"
        initial_status = "paid" if initial_payout > 0 else "not_required"
        initial_released = now_iso if initial_payout > 0 else None
        final_status = "paid" if final_payout == 0 else "pending"
        final_released = now_iso if final_status == "paid" else None
        owner_status = final_status
        payout_released_at = final_released

    if final_payout > 0:
        final_status = "paid"
        final_released = now_iso
        owner_status = "paid"
        payout_released_at = now_iso
    else:
        final_status = "not_required"
        final_released = final_released or now_iso
        owner_status = "not_required"
        payout_released_at = payout_released_at or now_iso

    db.execute(
        """
        UPDATE rentals
        SET status = 'completed',
            end_time = ?,
            completed_at = ?,
            payment_status = ?,
            company_commission_amount = ?,
            owner_payout_amount = ?,
            owner_payout_status = ?,
            owner_payout_released_at = ?,
            owner_initial_payout_amount = ?,
            owner_initial_payout_status = ?,
            owner_initial_payout_released_at = ?,
            owner_final_payout_amount = ?,
            owner_final_payout_status = ?,
            owner_final_payout_released_at = ?
        WHERE id = ?
        """,
        (
            now_iso,
            now_iso,
            payment_status_after,
            commission,
            owner_net,
            owner_status,
            payout_released_at,
            initial_payout,
            initial_status,
            initial_released,
            final_payout,
            final_status,
            final_released,
            rental_id,
        ),
    )
    db.execute(
        "UPDATE cars SET is_available = 1, updated_at = ? WHERE id = ?",
        (now_iso, rental["car_id"]),
    )
    db.commit()
    car_label = rental["car_name"] or f"{rental['brand']} {rental['model']}"
    create_notification(
        rental["renter_id"],
        f"Your trip with {car_label} is marked complete. Share your review to help the community.",
        url_for("rentals"),
    )
    return redirect(url_for("owner_cars"))


@app.route("/owner/rentals/<int:rental_id>/review", methods=["POST"])
@login_required
@role_required("owner")
def owner_review_rental(rental_id: int) -> str:
    db = get_db()
    rental = db.execute(
        """
        SELECT rentals.*, cars.name AS car_name, cars.brand, cars.model, rentals.renter_id, renters.username AS renter_username
        FROM rentals
        JOIN cars ON cars.id = rentals.car_id
        JOIN users AS renters ON renters.id = rentals.renter_id
        WHERE rentals.id = ? AND cars.owner_id = ? AND rentals.status IN ('booked', 'active', 'completed')
        """,
        (rental_id, g.user["id"]),
    ).fetchone()
    if rental is None:
        abort(404)
    try:
        passenger_rating = int(request.form.get("passenger_rating", "0"))
    except (TypeError, ValueError):
        return redirect(url_for("owner_cars"))
    if passenger_rating < 1 or passenger_rating > 5:
        return redirect(url_for("owner_cars"))
    comment = request.form.get("comment", "").strip()
    existing = db.execute(
        "SELECT id FROM reviews WHERE rental_id = ? AND reviewer_id = ?",
        (rental_id, g.user["id"]),
    ).fetchone()
    now_iso = naive_utcnow_iso()
    if existing:
        db.execute(
            "UPDATE reviews SET passenger_rating = ?, trip_rating = NULL, car_rating = NULL, owner_rating = NULL, comment = ?, created_at = ? WHERE id = ?",
            (passenger_rating, comment, now_iso, existing["id"]),
        )
    else:
        db.execute(
            """
            INSERT INTO reviews (rental_id, reviewer_id, target_user_id, reviewer_role, target_role, passenger_rating, trip_rating, car_rating, owner_rating, comment, created_at)
            VALUES (?, ?, ?, 'owner', 'renter', ?, NULL, NULL, NULL, ?, ?)
            """,
            (rental_id, g.user["id"], rental['renter_id'],
             passenger_rating, comment, now_iso),
        )
    db.commit()
    car_label = rental["car_name"] or f"{rental['brand']} {rental['model']}"
    create_notification(
        rental['renter_id'],
        f"{g.user['username']} rated you after the trip with {car_label}.",
        url_for("rentals"),
    )
    return redirect(url_for("owner_cars"))


@app.route("/owner/rentals/<int:rental_id>/respond", methods=["POST"])
@login_required
@role_required("owner")
def owner_respond_rental(rental_id: int) -> str:
    db = get_db()
    action = request.form.get("action")
    rental = db.execute(
        """
        SELECT rentals.*, cars.name AS car_name, cars.brand, cars.model, cars.owner_id, users.username AS renter_username
        FROM rentals
        JOIN cars ON cars.id = rentals.car_id
        JOIN users ON users.id = rentals.renter_id
        WHERE rentals.id = ? AND cars.owner_id = ?
        """,
        (rental_id, g.user["id"]),
    ).fetchone()
    if rental is None:
        abort(404)
    now = naive_utcnow()
    car_label = rental["car_name"] or f"{rental['brand']} {rental['model']}"
    if action == "accept" and rental["owner_response"] in ("pending", "counter"):
        payment_due = (now + timedelta(hours=1)).isoformat()
        db.execute(
            """
            UPDATE rentals
            SET owner_response = 'accepted',
                owner_response_at = ?,
                payment_status = 'awaiting_payment',
                payment_due_at = ?,
                payment_channel = 'manual',
                company_commission_amount = 0,
                owner_payout_amount = 0,
                owner_payout_status = 'pending',
                owner_payout_released_at = NULL,
                owner_initial_payout_amount = 0,
                owner_initial_payout_status = 'pending',
                owner_initial_payout_released_at = NULL,
                owner_final_payout_amount = 0,
                owner_final_payout_status = 'pending',
                owner_final_payout_released_at = NULL
            WHERE id = ?
            """,
            (now.isoformat(), payment_due, rental_id),
        )
        db.commit()
        create_notification(
            rental["renter_id"],
            f"{g.user['username']} accepted your booking request for {car_label}. Complete payment within the next hour.",
            url_for("renter_payment_page", rental_id=rental_id),
        )
    elif action == "reject" and rental["owner_response"] == "pending":
        reason = request.form.get("reason", "").strip()
        db.execute(
            "UPDATE rentals SET owner_response = 'rejected', owner_response_at = ?, status = 'cancelled', cancel_reason = ? WHERE id = ?",
            (now.isoformat(), reason, rental_id),
        )
        db.execute(
            "UPDATE cars SET is_available = 1, updated_at = ? WHERE id = ?",
            (now.isoformat(), rental["car_id"]),
        )
        db.commit()
        create_notification(
            rental["renter_id"],
            f"{g.user['username']} declined your booking for {car_label}.",
            url_for("rentals"),
        )
    elif action == "counter" and rental["owner_response"] == "pending" and rental["counter_used"] == 0:
        try:
            counter_amount = float(request.form.get("counter_amount", "0"))
        except ValueError:
            return redirect(url_for("owner_cars"))
        note = request.form.get("counter_comment", "").strip()
        db.execute(
            "UPDATE rentals SET owner_response = 'counter', owner_response_at = ?, counter_amount = ?, counter_comment = ?, counter_used = 1 WHERE id = ?",
            (now.isoformat(), counter_amount, note, rental_id),
        )
        db.commit()
        create_notification(
            rental["renter_id"],
            f"{g.user['username']} suggested a new price of Rs {counter_amount:.0f} for {car_label}. Review and confirm.",
            url_for("rentals"),
        )
    else:
        return redirect(url_for("owner_cars"))
    return redirect(url_for("owner_cars"))


@app.route("/forgot-password", methods=["GET", "POST"])
def forgot_password() -> str:
    if g.user is not None:
        return redirect(url_for("home"))
    error = None
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        phone = request.form.get("phone", "").strip()
        new_password = request.form.get("new_password", "")
        confirm_password = request.form.get("confirm_password", "")
        if not username or not phone or not new_password:
            error = "All fields are required."
        elif new_password != confirm_password:
            error = "Passwords do not match."
        else:
            db = get_db()
            user = db.execute(
                """
                SELECT users.id
                FROM users
                JOIN user_profiles ON user_profiles.user_id = users.id
                WHERE users.username = ? AND user_profiles.phone = ?
                """,
                (username, phone),
            ).fetchone()
            if user is None:
                error = "We could not verify your details."
            else:
                db.execute(
                    "UPDATE users SET password_hash = ? WHERE id = ?",
                    (generate_password_hash(new_password), user["id"]),
                )
                db.commit()
                create_notification(
                    user["id"], "Your password was reset successfully.")
                return redirect(url_for("login", message="Password reset. Please sign in."))
    return render_template("forgot_password.html", error=error)


@app.route("/register", methods=["GET", "POST"])
def register() -> str:
    if g.user is not None:
        return redirect(url_for("home"))
    error = None
    message = request.args.get("message")
    form_data = {
        "username": "",
        "account_name": "",
        "role": "renter",
        "admin_request": False,
    }
    if request.method == "POST":
        username_input = request.form.get("username", "").strip()
        account_name = request.form.get("account_name", "").strip()
        password = request.form.get("password", "")
        confirm_password = request.form.get("confirm_password", "")
        selected_role = request.form.get("role", "renter").lower()
        admin_request = request.form.get("admin_request")
        admin_code = request.form.get("admin_code", "").strip()
        if account_name:
            account_name = account_name[:80]
        form_data["username"] = username_input
        form_data["account_name"] = account_name
        form_data["role"] = selected_role
        form_data["admin_request"] = bool(
            admin_request) or selected_role == "admin"

        contact_type, normalized_value = normalize_contact(username_input)
        if not username_input:
            error = "Email or mobile number is required."
        elif not contact_type:
            error = "Enter a valid email address or Indian mobile number."
        if error is None and not account_name:
            error = "Account name is required."
        if error is None and len(account_name) < 2:
            error = "Account name must be at least 2 characters."
        if error is None and len(account_name) > 60:
            error = "Account name must be under 60 characters."
        if error is None:
            if password != confirm_password:
                error = "Passwords do not match."
            elif len(password) < 8 or not re.search(r"[A-Za-z]", password) or not re.search(r"[0-9]", password) or not re.search(r"[^A-Za-z0-9]", password):
                error = "Password must include letters, numbers, and a special character."
        is_admin = 0
        admin_required = form_data["admin_request"]
        if error is None and admin_required:
            secret = app.config.get("ADMIN_SETUP_SECRET", "DRIVENOW-ADMIN")
            if not admin_code or admin_code != secret:
                error = "Invalid admin invite code."
            else:
                is_admin = 1
        role_for_db = selected_role if selected_role in {
            "owner", "renter", "both"} else "renter"
        if selected_role == "admin":
            role_for_db = "renter"
        username_to_store = normalized_value if contact_type else username_input
        if error is None:
            db = get_db()
            existing = db.execute(
                "SELECT 1 FROM users WHERE username = ?", (username_to_store,)).fetchone()
            if existing:
                error = "Username is already taken."
        if error is None:
            db = get_db()
            try:
                db.execute(
                    "INSERT INTO users (username, password_hash, role, is_admin, account_name) VALUES (?, ?, ?, ?, ?)",
                    (username_to_store, generate_password_hash(
                        password), role_for_db, is_admin, account_name),
                )
                db.commit()
            except sqlite3.IntegrityError:
                error = "Username is already taken."
            else:
                return redirect(url_for("login", message="Account created. Please sign in."))
    return render_template(
        "register.html",
        error=error,
        message=message,
        form_data=form_data,
    )


@app.route("/login", methods=["GET", "POST"])
def login() -> str:
    if g.user is not None:
        return redirect(url_for("home"))
    error = None
    message = request.args.get("message")
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        db = get_db()
        user = db.execute(
            "SELECT id, username, password_hash, role FROM users WHERE username = ?",
            (username,),
        ).fetchone()
        if user is None or not check_password_hash(user["password_hash"], password):
            error = "Invalid username or password."
        else:
            session.clear()
            session["user_id"] = user["id"]
            return redirect(url_for("home"))
    return render_template("login.html", error=error, message=message)


@app.route("/logout")
def logout() -> str:
    session.clear()
    return redirect(url_for("login", message="You have been signed out."))


with app.app_context():
    init_db()


@app.route("/uploads/<path:filename>")
def serve_upload(filename: str):
    return send_from_directory(UPLOAD_ROOT, filename)


@app.route("/contact")
def contact() -> str:
    return render_template("contact.html")


@app.route("/shipping-policy")
def shipping_policy() -> str:
    return render_template("shipping_policy.html")


@app.route("/terms-and-conditions")
def terms_and_conditions() -> str:
    return render_template("terms_and_conditions.html")


@app.route("/cancellations-and-refunds")
def cancellations_and_refunds() -> str:
    return render_template("cancellations_and_refunds.html")


def ordinal_number(value: int) -> str:
    suffix = "th"
    if value % 100 not in (11, 12, 13):
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(value % 10, "th")
    return f"{value}{suffix}"


@app.template_filter("trip_datetime")
def format_trip_datetime(value: str | datetime | None) -> str:
    if value is None:
        return "-"
    if isinstance(value, datetime):
        date_value = value
    else:
        parsed = parse_iso(value)
        if not parsed:
            return value
        date_value = parsed
    hour = date_value.strftime("%I").lstrip("0") or "0"
    minute = date_value.strftime("%M")
    period = date_value.strftime("%p")
    time_part = f"{hour}{period}" if minute == "00" else f"{hour}:{minute}{period}"
    day_part = ordinal_number(date_value.day)
    month_part = date_value.strftime("%b")
    year_part = date_value.strftime("%Y")
    return f"{time_part}, {day_part} {month_part}, {year_part}"


def main() -> None:
    app.run(debug=True, port=5000)


if __name__ == "__main__":
    main()
