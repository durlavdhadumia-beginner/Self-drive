"""Self-drive car rental platform with geolocation search and owner management."""

from __future__ import annotations

import os
import re
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from math import asin, ceil, cos, radians, sin, sqrt
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

from flask import (
    Flask,
    abort,
    g,
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
COMPANY_COMMISSION_RATE = 0.05

UPLOAD_ROOT.mkdir(parents=True, exist_ok=True)
USER_DOC_ROOT.mkdir(parents=True, exist_ok=True)

app = Flask(__name__)
app.config.update(
    SECRET_KEY="replace-with-a-secure-random-value",
    UPLOAD_FOLDER=str(UPLOAD_ROOT),
    MAX_CONTENT_LENGTH=16 * 1024 * 1024,  # 16 MB per request
)
app.logger.setLevel("INFO")


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
            is_admin INTEGER NOT NULL DEFAULT 0
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
            company_commission_amount REAL NOT NULL DEFAULT 0,
            owner_payout_amount REAL NOT NULL DEFAULT 0,
            owner_payout_status TEXT NOT NULL DEFAULT 'pending',
            owner_payout_released_at TEXT,
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
        "ALTER TABLE rentals ADD COLUMN company_commission_amount REAL NOT NULL DEFAULT 0",
        "ALTER TABLE rentals ADD COLUMN owner_payout_amount REAL NOT NULL DEFAULT 0",
        "ALTER TABLE rentals ADD COLUMN owner_payout_status TEXT NOT NULL DEFAULT 'pending'",
        "ALTER TABLE rentals ADD COLUMN owner_payout_released_at TEXT",
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
    db.execute(
        "CREATE INDEX IF NOT EXISTS idx_cities_name ON cities(name COLLATE NOCASE)"
    )
    db.commit()
    seed_cities_if_needed(db)
    db.execute(
        "INSERT OR IGNORE INTO company_payout_config (id, updated_at) VALUES (1, ?)",
        (datetime.utcnow().isoformat(),),
    )
    db.commit()


def allowed_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def save_car_images(car_id: int, files: List) -> List[str]:
    if not files:
        return []
    saved_paths: List[str] = []
    target_dir = UPLOAD_ROOT.joinpath(str(car_id))
    target_dir.mkdir(parents=True, exist_ok=True)
    db = get_db()
    for upload in files[:8]:
        if not upload or not upload.filename:
            continue
        if not allowed_file(upload.filename):
            continue
        safe_name = secure_filename(upload.filename)
        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
        filename = f"{timestamp}_{safe_name}"
        filepath = target_dir.joinpath(filename)
        upload.save(filepath)
        relative_path = f"uploads/{car_id}/{filename}"
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
        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
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
        db.execute("INSERT INTO user_payout_details (user_id) VALUES (?)", (user_id,))
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
            (datetime.utcnow().isoformat(),),
        )
        db.commit()
        row = db.execute("SELECT * FROM company_payout_config WHERE id = 1").fetchone()
    return dict(row)


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
        required_fields.append(_value(profile, "vehicle_registration"))
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
    db.execute("UPDATE notifications SET is_read = 1 WHERE user_id = ?", (user_id,))
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
            user_role = g.user.get("role") if isinstance(g.user, dict) else g.user["role"]
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
    """Return latitude and longitude for a given city name from the local cities table."""
    cleaned = (city_name or "").strip()
    if not cleaned:
        return None
    db = get_db()
    matches = db.execute(
        """
        SELECT latitude, longitude
        FROM cities
        WHERE LOWER(name) LIKE LOWER(?) || '%'
          AND latitude IS NOT NULL
          AND longitude IS NOT NULL
        ORDER BY LENGTH(name), (pincode IS NULL), pincode
        LIMIT 1
        """,
        (cleaned,),
    ).fetchone()
    if matches and matches["latitude"] is not None and matches["longitude"] is not None:
        return float(matches["latitude"]), float(matches["longitude"])
    tokens = cleaned.lower().split()
    for size in range(len(tokens), 0, -1):
        attempt = " ".join(tokens[:size])
        row = db.execute(
            """
            SELECT latitude, longitude
            FROM cities
            WHERE LOWER(name) = LOWER(?)
              AND latitude IS NOT NULL
              AND longitude IS NOT NULL
            ORDER BY (pincode IS NULL), pincode
            LIMIT 1
            """,
            (attempt,),
        ).fetchone()
        if row and row["latitude"] is not None and row["longitude"] is not None:
            return float(row["latitude"]), float(row["longitude"])
    row = db.execute(
        """
        SELECT latitude, longitude
        FROM cities
        WHERE LOWER(name) LIKE '%' || LOWER(?) || '%'
          AND latitude IS NOT NULL
          AND longitude IS NOT NULL
        ORDER BY LENGTH(name), (pincode IS NULL), pincode
        LIMIT 1
        """,
        (cleaned,),
    ).fetchone()
    if row and row["latitude"] is not None and row["longitude"] is not None:
        return float(row["latitude"]), float(row["longitude"])
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

    where_clause = " AND ".join(predicates)

    rows = db.execute(
        f"""
        SELECT cars.*, users.username AS owner_username,
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

    cars: List[Car] = []
    for row in rows:
        distance = haversine_km(latitude, longitude, row["latitude"], row["longitude"])
        if radius_km and distance > radius_km:
            continue
        is_available = row["is_available"] and not row["has_active_rental"]
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
    now=datetime.utcnow,
    promo_codes=PROMO_CODES,
    profile_is_complete=profile_is_complete,
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


def parse_float(value: Optional[str]) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None



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
        "SELECT id, username, role, is_admin FROM users WHERE id = ?",
        (user_id,),
    ).fetchone()
    if user_row is None:
        g.user = None
        g.profile = None
        g.unread_notifications = 0
        g.profile_complete = False
        return
    g.user = dict(user_row)
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
        db.execute("ALTER TABLE user_profiles ADD COLUMN email_contact TEXT DEFAULT ''")
        db.commit()
    except sqlite3.OperationalError:
        pass
    profile_row = ensure_user_profile(g.user["id"])
    payout_row = ensure_user_payout(g.user["id"])
    fallback_contact = g.user.get("username") if g.user else ""
    if not profile_row.get("email_contact"):
        profile_row["email_contact"] = fallback_contact or profile_row.get("phone", "")
    g.profile = profile_row
    message = request.args.get("message")
    error = None
    documents = fetch_user_documents(g.user["id"])
    if request.method == "POST":
        form = request.form
        full_name = form.get("full_name", "").strip()
        date_of_birth = form.get("date_of_birth", "").strip()
        email_contact = form.get("email_contact", "").strip()
        phone = form.get("phone", "").strip()
        address = form.get("address", "").strip()
        vehicle_registration = form.get("vehicle_registration", "").strip()
        gps_tracking = 1 if form.get("gps_tracking") else 0
        account_holder = form.get("account_holder", "").strip()
        account_number = form.get("account_number", "").strip()
        ifsc_code = form.get("ifsc_code", "").strip().upper()
        upi_id = form.get("upi_id", "").strip()
        doc_types = form.getlist("doc_type")
        doc_files = request.files.getlist("doc_files")
        missing_fields: List[str] = []
        if not full_name:
            missing_fields.append("full name")
        if not phone:
            missing_fields.append("mobile number")
        if not email_contact:
            missing_fields.append("email ID or account contact")
        if has_role("owner") and not vehicle_registration:
            missing_fields.append("vehicle registration details")
        if not ((account_number and ifsc_code) or upi_id):
            missing_fields.append("payout details (bank account + IFSC or UPI ID)")
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
                datetime.utcnow().isoformat(),
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
                datetime.utcnow().isoformat(),
                g.user["id"],
            ),
        )
        db.commit()
        if any(getattr(f, "filename", "") for f in doc_files):
            save_user_documents(g.user["id"], doc_files, doc_types)
        profile_row = ensure_user_profile(g.user["id"])
        if not profile_row.get("email_contact"):
            profile_row["email_contact"] = email_contact or (g.user.get("username") if g.user else "")
        payout_row = ensure_user_payout(g.user["id"])
        g.profile = profile_row
        g.profile_complete = profile_is_complete(profile_row)
        documents = fetch_user_documents(g.user["id"])
        if missing_fields:
            if len(missing_fields) == 1:
                missing_text = missing_fields[0]
            else:
                missing_text = ", ".join(missing_fields[:-1]) + f" and {missing_fields[-1]}"
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
    recent_users = db.execute("SELECT id, username, role, is_admin FROM users ORDER BY id DESC LIMIT 8").fetchall()
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
    complaint = db.execute("SELECT submitted_by, role FROM complaints WHERE id = ?", (complaint_id,)).fetchone()
    if complaint is None:
        abort(404)
    resolution = request.form.get("resolution", "").strip()
    now_iso = datetime.utcnow().isoformat()
    db.execute(
        "UPDATE complaints SET status = 'resolved', resolved_at = ?, resolution = ? WHERE id = ?",
        (now_iso, resolution, complaint_id),
    )
    db.commit()
    redirect_target = url_for("rentals") if complaint["role"] == 'renter' else url_for("owner_cars")
    create_notification(complaint["submitted_by"], "Your complaint has been resolved by the admin team.", redirect_target)
    return redirect(url_for("admin_dashboard"))


@app.route("/admin/users")
@login_required
@admin_required
def admin_users() -> str:
    db = get_db()
    rows = db.execute(
        """
        SELECT
            users.id, users.username, users.role, users.is_admin,
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
    users = [dict(row) for row in rows]
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
    profile = ensure_user_profile(user_id)
    documents = [dict(doc) for doc in fetch_user_documents(user_id)]
    stats = {
        "vehicle_count": db.execute("SELECT COUNT(*) FROM cars WHERE owner_id = ?", (user_id,)).fetchone()[0],
        "trip_count": db.execute("SELECT COUNT(*) FROM rentals WHERE renter_id = ?", (user_id,)).fetchone()[0],
    }
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
        (datetime.utcnow().isoformat(), user_id),
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
               renters.username AS renter_username, owners.username AS owner_username
        FROM rentals
        JOIN cars ON cars.id = rentals.car_id
        JOIN users AS renters ON renters.id = rentals.renter_id
        JOIN users AS owners ON owners.id = cars.owner_id
        ORDER BY rentals.start_time DESC
        """
    ).fetchall()
    rentals = [dict(row) for row in rows]
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
        "SELECT total_amount, company_commission_amount, owner_payout_amount, owner_payout_status, owner_payout_released_at FROM rentals WHERE id = ?",
        (rental_id,),
    ).fetchone()
    if rental is None:
        abort(404)
    commission = rental["company_commission_amount"] or 0
    owner_payout = rental["owner_payout_amount"] or 0
    owner_status = rental["owner_payout_status"] or "pending"
    released_at = rental["owner_payout_released_at"]
    if new_status == "paid":
        total_amount = float(rental["total_amount"] or 0)
        commission = round(total_amount * COMPANY_COMMISSION_RATE, 2)
        owner_payout = max(0.0, round(total_amount - commission, 2))
        if owner_status != "paid":
            owner_status = "pending"
            released_at = None
    else:
        commission = 0
        owner_payout = 0
        owner_status = "pending"
        released_at = None
    db.execute(
        """
        UPDATE rentals
        SET payment_status = ?,
            company_commission_amount = ?,
            owner_payout_amount = ?,
            owner_payout_status = ?,
            owner_payout_released_at = ?
        WHERE id = ?
        """,
        (new_status, commission, owner_payout, owner_status, released_at, rental_id),
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
    db.execute(
        "UPDATE rentals SET owner_payout_status = 'paid', owner_payout_released_at = ? WHERE id = ?",
        (datetime.utcnow().isoformat(), rental_id),
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
            vehicle["latitude"] = float(vehicle["latitude"]) if vehicle["latitude"] is not None else None
            vehicle["longitude"] = float(vehicle["longitude"]) if vehicle["longitude"] is not None else None
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
                (account_holder, account_number, ifsc_code, upi_id, datetime.utcnow().isoformat()),
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
    city_rows = db.execute(
        "SELECT name, state FROM cities ORDER BY name"
    ).fetchall()
    if city_rows:
        cities = [
            {"name": row["name"], "state": row["state"] or ""}
            for row in city_rows
        ]
    else:
        fallback_rows = db.execute(
            "SELECT DISTINCT city FROM cars WHERE city <> '' ORDER BY city"
        ).fetchall()
        cities = [{"name": row[0], "state": ""} for row in fallback_rows]
    vehicle_types = [row[0] for row in db.execute(
        "SELECT DISTINCT vehicle_type FROM cars WHERE vehicle_type <> '' ORDER BY vehicle_type").fetchall()]
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
        vehicle_types=vehicle_types,
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
    cars: List[Car] = []
    cars_payload: List[dict] = []
    pricing_map: Dict[int, Dict[str, object]] = {}
    latitude = longitude = None
    radius = None
    city = request.form.get("city") if request.method == "POST" else request.args.get("city")
    detected_city = None
    start_time_raw = end_time_raw = None
    error = request.args.get("error")

    vehicle_type_rows = db.execute(
        "SELECT DISTINCT vehicle_type FROM cars WHERE vehicle_type <> '' ORDER BY vehicle_type"
    ).fetchall()
    available_vehicle_types = [row[0] for row in vehicle_type_rows]

    filters = {
        "vehicle_types": [],
        "price_min": None,
        "price_max": None,
        "seat_min": None,
        "seat_max": None,
        "require_gps": False,
        "price_unit": "hour",
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
        filters["vehicle_types"] = [v for v in request.form.getlist("vehicle_types") if v]
        filters["price_unit"] = normalize_unit(request.form.get("price_unit"))
        filters["price_min"] = parse_float(request.form.get("price_min"))
        filters["price_max"] = parse_float(request.form.get("price_max"))
        filters["seat_min"] = parse_int(request.form.get("seat_min"))
        filters["seat_max"] = parse_int(request.form.get("seat_max"))
        filters["require_gps"] = request.form.get("require_gps") == "on"
        price_min_hours = convert_price(filters["price_min"], filters["price_unit"])
        price_max_hours = convert_price(filters["price_max"], filters["price_unit"])
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
            cars = fetch_available_cars(
                latitude=latitude,
                longitude=longitude,
                radius_km=radius,
                city=city,
                price_min=price_min_hours,
                price_max=price_max_hours,
                vehicle_types=filters["vehicle_types"],
                seat_min=filters["seat_min"],
                seat_max=filters["seat_max"],
                require_gps=filters["require_gps"],
            )
            if not cars:
                error = "No cars found within the selected filters."
        else:
            latitude = longitude = None
    else:
        start_time_raw = request.args.get("start_time")
        end_time_raw = request.args.get("end_time")
        filters["vehicle_types"] = [v for v in request.args.getlist("vehicle_types") if v]
        filters["price_unit"] = normalize_unit(request.args.get("price_unit"))
        filters["price_min"] = parse_float(request.args.get("price_min"))
        filters["price_max"] = parse_float(request.args.get("price_max"))
        filters["seat_min"] = parse_int(request.args.get("seat_min"))
        filters["seat_max"] = parse_int(request.args.get("seat_max"))
        filters["require_gps"] = request.args.get("require_gps") in {"1", "true", "on"}
        price_min_hours = convert_price(filters["price_min"], filters["price_unit"])
        price_max_hours = convert_price(filters["price_max"], filters["price_unit"])
        try:
            lat_query = request.args.get("latitude")
            lon_query = request.args.get("longitude")
            radius_query = request.args.get("radius")
            latitude = float(lat_query) if lat_query else None
            longitude = float(lon_query) if lon_query else None
            radius = float(radius_query) if radius_query else None
            if latitude is not None and longitude is not None:
                lookup_radius = radius if radius is not None else 10.0
                cars = fetch_available_cars(
                    latitude=latitude,
                    longitude=longitude,
                    radius_km=lookup_radius,
                    city=city,
                    price_min=price_min_hours,
                    price_max=price_max_hours,
                    vehicle_types=filters["vehicle_types"],
                    seat_min=filters["seat_min"],
                    seat_max=filters["seat_max"],
                    require_gps=filters["require_gps"],
                )
                radius = lookup_radius
        except (TypeError, ValueError):
            latitude = longitude = None
            radius = None

    if (latitude is None or longitude is None) and city:
        coords = lookup_city_coordinates(city.split(",")[0].strip())
        if coords:
            latitude, longitude = coords
            if radius is None:
                radius = 10.0

    if not cars and latitude is not None and longitude is not None:
        lookup_radius = radius if radius is not None else 10.0
        cars = fetch_available_cars(
            latitude=latitude,
            longitude=longitude,
            radius_km=lookup_radius,
            city=city,
            price_min=price_min_hours,
            price_max=price_max_hours,
            vehicle_types=filters["vehicle_types"],
            seat_min=filters["seat_min"],
            seat_max=filters["seat_max"],
            require_gps=filters["require_gps"],
        )
        radius = lookup_radius

    start_dt = parse_iso(parse_datetime(start_time_raw))
    end_dt = parse_iso(parse_datetime(end_time_raw))

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
                    "owner_username": car.owner_username,
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

    city_rows = db.execute(
        "SELECT name FROM cities ORDER BY name COLLATE NOCASE LIMIT 25"
    ).fetchall()
    city_options = [row["name"] for row in city_rows]

    return render_template(
        "search.html",
        cars=cars,
        cars_payload=cars_payload,
        pricing=pricing_map,
        latitude=latitude,
        longitude=longitude,
        radius=radius,
        city=city,
        detected_city=detected_city,
        start_time=start_time_raw,
        end_time=end_time_raw,
        error=error,
        filters=filters,
        available_vehicle_types=available_vehicle_types,
        profile_warning=profile_warning,
        city_options=city_options,
    )


@app.route("/rentals")
@login_required
@role_required("renter", "owner")
def rentals() -> str:
    db = get_db()
    rows = db.execute(
        """
        SELECT rentals.*, cars.brand, cars.model, cars.licence_plate, cars.image_url, cars.name AS car_name,
               owners.username AS owner_username
        FROM rentals
        JOIN cars ON cars.id = rentals.car_id
        JOIN users AS owners ON owners.id = cars.owner_id
        WHERE rentals.renter_id = ?
        ORDER BY rentals.start_time DESC
        """,
        (g.user["id"],),
    ).fetchall()
    awaiting_payment = [row for row in rows if row["payment_status"] == 'awaiting_payment' and row["owner_response"] == 'accepted']
    pending_host_action = [row for row in rows if row["owner_response"] in ('pending', 'counter') and row["status"] == 'booked']
    return render_template("rentals.html", rentals=rows, awaiting_payment=awaiting_payment, pending_host_action=pending_host_action)


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

    start_iso = parse_datetime(start_raw) or datetime.utcnow().isoformat()
    end_iso = parse_datetime(end_raw)

    start_dt = parse_iso(start_iso) or datetime.utcnow()
    end_dt = parse_iso(end_iso) if end_iso else start_dt + timedelta(hours=4)

    pricing = calculate_pricing(
        rate_per_hour=car["rate_per_hour"],
        daily_rate=car["daily_rate"] or car["rate_per_hour"] * 24,
        start=start_dt,
        end=end_dt,
        promo_code=promo_code,
    )

    cursor = db.execute(
        """
        INSERT INTO rentals (car_id, renter_id, status, start_time, end_time, promo_code, discount_amount, total_amount)
        VALUES (?, ?, 'booked', ?, ?, ?, ?, ?)
        """,
        (
            car_id,
            g.user["id"],
            start_iso,
            end_dt.isoformat(),
            pricing["promo_applied"] or "",
            pricing["discount"],
            pricing["total"],
        ),
    )
    rental_id = cursor.lastrowid
    db.execute(
        "UPDATE cars SET is_available = 0, updated_at = ? WHERE id = ?",
        (datetime.utcnow().isoformat(), car_id),
    )
    db.commit()

    car_name = car["name"] or f"{car['brand']} {car['model']}"
    owner_message = f"{g.user['username']} requested to book your {car_name}."
    renter_message = f"Booking request sent for {car_name}. We'll notify you once the host responds."
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
        (rental_id, g.user["id"], target_user_id, role, category, description, datetime.utcnow().isoformat()),
    )
    db.commit()
    car_label = rental["car_name"] or f"{rental['brand']} {rental['model']}"
    create_notification(
        target_user_id,
        f"{g.user['username']} filed a complaint about the trip with {car_label}.",
        redirect_target,
    )
    return redirect_target


@app.route("/rentals/<int:rental_id>/complaint", methods=["POST"])
@login_required
def submit_complaint(rental_id: int) -> str:
    category = request.form.get("category", "General")
    description = request.form.get("description", "")
    redirect_target = _process_rental_complaint(rental_id, category, description)
    return redirect(redirect_target)


@app.route("/support/feedback", methods=["POST"])
@login_required
def submit_feedback() -> str:
    category = request.form.get("category", "Platform issue").strip() or "Platform issue"
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
        redirect_url = _process_rental_complaint(rental_id, category, description)
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
        (g.user["id"], feedback_role, category, description, None, datetime.utcnow().isoformat()),
    )
    db.commit()
    feedback_id = cursor.lastrowid
    feedback_link = url_for("admin_feedback_detail", feedback_id=feedback_id)
    admin_rows = db.execute("SELECT id FROM users WHERE is_admin = 1").fetchall()
    for row in admin_rows:
        create_notification(
            row["id"],
            f"{g.user['username']} shared feedback: {category}.",
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
    now_iso = datetime.utcnow().isoformat()
    if existing:
        db.execute(
            "UPDATE reviews SET trip_rating = ?, car_rating = ?, owner_rating = ?, passenger_rating = NULL, comment = ?, created_at = ? WHERE id = ?",
            (trip_rating, car_rating, owner_rating, comment, now_iso, existing["id"]),
        )
    else:
        db.execute(
            """
            INSERT INTO reviews (rental_id, reviewer_id, target_user_id, reviewer_role, target_role, trip_rating, car_rating, owner_rating, passenger_rating, comment, created_at)
            VALUES (?, ?, ?, 'renter', 'owner', ?, ?, ?, NULL, ?, ?)
            """,
            (rental_id, g.user["id"], rental["owner_id"], trip_rating, car_rating, owner_rating, comment, now_iso),
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
    now = datetime.utcnow()
    car_label = rental["car_name"] or f"{rental['brand']} {rental['model']}"
    if action == "accept_counter" and rental["owner_response"] == "counter":
        counter_amount = rental["counter_amount"] or rental["total_amount"]
        new_total = round(counter_amount, 2)
        db.execute(
            """
            UPDATE rentals
            SET owner_response = 'accepted',
                renter_response = 'accepted',
                renter_response_at = ?,
                total_amount = ?,
                payment_status = 'awaiting_payment',
                payment_due_at = ?,
                payment_channel = 'manual',
                company_commission_amount = 0,
                owner_payout_amount = 0,
                owner_payout_status = 'pending',
                owner_payout_released_at = NULL
            WHERE id = ?
            """,
            (now.isoformat(), new_total, (now + timedelta(hours=1)).isoformat(), rental_id),
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
    now_iso = datetime.utcnow().isoformat()
    total_amount = float(rental["total_amount"] or 0)
    commission = round(total_amount * COMPANY_COMMISSION_RATE, 2)
    owner_payout = max(0.0, round(total_amount - commission, 2))
    payment_channel = request.form.get("payment_channel", "upi/netbanking").strip().lower() or "upi/netbanking"
    db.execute(
        """
        UPDATE rentals
        SET payment_status = 'paid',
            payment_confirmed_at = ?,
            payment_channel = ?,
            company_commission_amount = ?,
            owner_payout_amount = ?,
            owner_payout_status = 'pending',
            owner_payout_released_at = NULL,
            renter_response = CASE WHEN renter_response = '' THEN 'paid' ELSE renter_response END
        WHERE id = ?
        """,
        (now_iso, payment_channel, commission, owner_payout, rental_id),
    )
    db.commit()
    car_label = rental["car_name"] or f"{rental['brand']} {rental['model']}"
    create_notification(
        rental["owner_id"],
        f"{g.user['username']} confirmed payment for {car_label}.",
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
    db.execute(
        "UPDATE rentals SET status = 'cancelled', end_time = ? WHERE id = ?",
        (datetime.utcnow().isoformat(), rental_id),
    )
    db.execute(
        "UPDATE cars SET is_available = 1, updated_at = ? WHERE id = ?",
        (datetime.utcnow().isoformat(), rental["car_id"]),
    )
    db.commit()
    return redirect(url_for("rentals"))


@app.route("/owner/cars")
@login_required
@role_required("owner")
def owner_cars() -> str:
    db = get_db()
    car_rows = db.execute(
        "SELECT * FROM cars WHERE owner_id = ? ORDER BY created_at DESC",
        (g.user["id"],),
    ).fetchall()
    car_dicts = []
    images = fetch_car_images([row["id"] for row in car_rows])
    for row in car_rows:
        car = dict(row)
        car["images"] = images.get(row["id"], [])
        car_dicts.append(car)
    rental_rows = db.execute(
        """
        SELECT rentals.*, users.username AS renter_username, cars.brand, cars.model, cars.name AS car_name
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
        (g.user["id"],),
    ).fetchall()
    pending_payments = [row for row in rental_rows if row["payment_status"] == 'awaiting_payment']
    return render_template(
        "owner_cars.html",
        cars=car_dicts,
        rentals=rental_rows,
        pending_payments=pending_payments,
        commission_rate=int(COMPANY_COMMISSION_RATE * 100),
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
        rate = float(form.get("rate_per_hour", 0))
        daily_rate_input = form.get("daily_rate")
        daily_rate = float(daily_rate_input) if daily_rate_input else rate * 24
        latitude = float(form.get("latitude"))
        longitude = float(form.get("longitude"))
    except (TypeError, ValueError):
        return redirect(url_for("owner_cars"))
    vehicle_type = form.get("vehicle_type", "").strip() or "car"
    size_category = form.get("size_category", "").strip()
    has_gps = 1 if form.get("has_gps") else 0
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
            (form.get("name") or f"{form.get('brand', '')} {form.get('model', '')}").strip(),
            form.get("brand", "Unknown"),
            form.get("model", "Unknown"),
            form.get("licence_plate", ""),
            seats,
            rate,
            daily_rate,
            vehicle_type,
            size_category,
            has_gps,
            latitude,
            longitude,
            form.get("city", ""),
            form.get("image_url", ""),
            form.get("fuel_type", ""),
            form.get("transmission", ""),
            form.get("description", ""),
        ),
    )
    car_id = cursor.lastrowid
    save_car_images(car_id, request.files.getlist("photos"))
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
        (new_state, datetime.utcnow().isoformat(), car_id),
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
        (latitude, longitude, datetime.utcnow(
        ).isoformat(), car_id, g.user["id"]),
    )
    db.commit()
    if updated.rowcount == 0:
        abort(404)
    return redirect(url_for("owner_cars"))


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
    now_iso = datetime.utcnow().isoformat()
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
    end_dt = parse_iso(rental["end_time"]) or datetime.utcnow()
    new_end = end_dt + timedelta(hours=extra_hours)
    additional_amount = round(rental["rate_per_hour"] * extra_hours, 2)
    current_total = rental["total_amount"] or 0
    new_total = round(current_total + additional_amount, 2)
    new_status = rental["payment_status"]
    new_due = rental["payment_due_at"]
    if rental["payment_status"] == "paid":
        new_status = "awaiting_payment"
        new_due = (datetime.utcnow() + timedelta(hours=1)).isoformat()
    commission = rental["company_commission_amount"] or 0
    owner_payout = rental["owner_payout_amount"] or 0
    owner_payout_status = rental["owner_payout_status"] or "pending"
    owner_payout_released_at = rental["owner_payout_released_at"]
    if new_status == "awaiting_payment":
        commission = 0
        owner_payout = 0
        owner_payout_status = "pending"
        owner_payout_released_at = None
    db.execute(
        """
        UPDATE rentals
        SET end_time = ?,
            total_amount = ?,
            payment_status = ?,
            payment_due_at = ?,
            payment_channel = CASE WHEN ? = 'awaiting_payment' THEN 'manual' ELSE payment_channel END,
            company_commission_amount = ?,
            owner_payout_amount = ?,
            owner_payout_status = ?,
            owner_payout_released_at = ?
        WHERE id = ?
        """,
        (new_end.isoformat(), new_total, new_status, new_due, new_status, commission, owner_payout, owner_payout_status, owner_payout_released_at, rental_id),
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
    now_iso = datetime.utcnow().isoformat()
    commission = rental["company_commission_amount"] or 0
    owner_payout = rental["owner_payout_amount"] or 0
    owner_status = rental["owner_payout_status"] or "pending"
    if rental["payment_status"] != "paid":
        total_amount = float(rental["total_amount"] or 0)
        commission = round(total_amount * COMPANY_COMMISSION_RATE, 2)
        owner_payout = max(0.0, round(total_amount - commission, 2))
        owner_status = "pending"
    db.execute(
        """
        UPDATE rentals
        SET status = 'completed',
            end_time = ?,
            completed_at = ?,
            payment_status = CASE WHEN payment_status = 'paid' THEN payment_status ELSE 'paid' END,
            company_commission_amount = ?,
            owner_payout_amount = ?,
            owner_payout_status = ?
        WHERE id = ?
        """,
        (now_iso, now_iso, commission, owner_payout, owner_status, rental_id),
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
    now_iso = datetime.utcnow().isoformat()
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
            (rental_id, g.user["id"], rental['renter_id'], passenger_rating, comment, now_iso),
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
    now = datetime.utcnow()
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
                owner_payout_released_at = NULL
            WHERE id = ?
            """,
            (now.isoformat(), payment_due, rental_id),
        )
        db.commit()
        create_notification(
            rental["renter_id"],
            f"{g.user['username']} accepted your booking request for {car_label}. Complete payment within the next hour.",
            url_for("rentals"),
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
                create_notification(user["id"], "Your password was reset successfully.")
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
        "role": "renter",
        "admin_request": False,
    }
    if request.method == "POST":
        username_input = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        confirm_password = request.form.get("confirm_password", "")
        selected_role = request.form.get("role", "renter").lower()
        admin_request = request.form.get("admin_request")
        admin_code = request.form.get("admin_code", "").strip()
        form_data["username"] = username_input
        form_data["role"] = selected_role
        form_data["admin_request"] = bool(admin_request) or selected_role == "admin"

        contact_type, normalized_value = normalize_contact(username_input)
        if not username_input:
            error = "Email or mobile number is required."
        elif not contact_type:
            error = "Enter a valid email address or Indian mobile number."
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
        role_for_db = selected_role if selected_role in {"owner", "renter", "both"} else "renter"
        if selected_role == "admin":
            role_for_db = "renter"
        username_to_store = normalized_value if contact_type else username_input
        if error is None:
            db = get_db()
            existing = db.execute("SELECT 1 FROM users WHERE username = ?", (username_to_store,)).fetchone()
            if existing:
                error = "Username is already taken."
        if error is None:
            db = get_db()
            try:
                db.execute(
                    "INSERT INTO users (username, password_hash, role, is_admin) VALUES (?, ?, ?, ?)",
                    (username_to_store, generate_password_hash(password), role_for_db, is_admin),
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


def main() -> None:
    with app.app_context():
        init_db()
    app.run(debug=True, port=5000)


if __name__ == "__main__":
    main()


with app.app_context():
    init_db()


@app.route("/uploads/<path:filename>")
def serve_upload(filename: str):
    return send_from_directory(UPLOAD_ROOT, filename)
