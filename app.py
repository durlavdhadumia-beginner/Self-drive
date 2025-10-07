"""Self-drive car rental platform with geolocation search and owner management."""

from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from math import asin, ceil, cos, radians, sin, sqrt
from pathlib import Path
from typing import Callable, Dict, List, Optional

from flask import (
    Flask,
    abort,
    g,
    redirect,
    render_template,
    request,
    session,
    url_for,
)
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.utils import secure_filename


DATABASE = Path(__file__).with_name("car_rental.db")
UPLOAD_ROOT = Path(__file__).with_name("static").joinpath("uploads")
ALLOWED_EXTENSIONS = {"png", "jpg", "jpeg", "gif", "webp"}
PROMO_CODES: Dict[str, float] = {
    "ZOOM10": 0.10,
    "WEEKEND15": 0.15,
    "FIRSTDRIVE": 0.20,
}

UPLOAD_ROOT.mkdir(parents=True, exist_ok=True)

app = Flask(__name__)
app.config.update(
    SECRET_KEY="replace-with-a-secure-random-value",
    UPLOAD_FOLDER=str(UPLOAD_ROOT),
    MAX_CONTENT_LENGTH=16 * 1024 * 1024,  # 16 MB per request
)


@dataclass
class Car:
    id: int
    name: str
    latitude: float
    longitude: float
    distance_km: Optional[float]
    status: str
    rate_per_hour: float
    seats: int
    owner_username: str
    city: str
    image_url: str
    fuel_type: str
    transmission: str
    rating: float
    description: str
    daily_rate: float
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


def init_db() -> None:
    db = get_db()
    db.executescript(
        """
        PRAGMA foreign_keys = ON;

        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL CHECK(role IN ('owner', 'renter', 'both'))
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
        """
    )

    alter_statements = [
        "ALTER TABLE cars ADD COLUMN city TEXT DEFAULT ''",
        "ALTER TABLE cars ADD COLUMN image_url TEXT DEFAULT ''",
        "ALTER TABLE cars ADD COLUMN fuel_type TEXT DEFAULT ''",
        "ALTER TABLE cars ADD COLUMN transmission TEXT DEFAULT ''",
        "ALTER TABLE cars ADD COLUMN rating REAL DEFAULT 4.5",
        "ALTER TABLE cars ADD COLUMN description TEXT DEFAULT ''",
        "ALTER TABLE cars ADD COLUMN daily_rate REAL DEFAULT 0",
        "ALTER TABLE rentals ADD COLUMN promo_code TEXT DEFAULT ''",
        "ALTER TABLE rentals ADD COLUMN discount_amount REAL NOT NULL DEFAULT 0",
        "ALTER TABLE rentals ADD COLUMN total_amount REAL NOT NULL DEFAULT 0",
    ]
    for statement in alter_statements:
        try:
            db.execute(statement)
        except sqlite3.OperationalError:
            pass
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
    for upload in files:
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


def login_required(view: Callable) -> Callable:
    def wrapped(*args, **kwargs):
        if g.user is None:
            return redirect(url_for("login"))
        return view(*args, **kwargs)

    wrapped.__name__ = view.__name__
    return wrapped


def role_required(required_role: str) -> Callable:
    def decorator(view: Callable) -> Callable:
        def wrapped(*args, **kwargs):
            if g.user is None:
                return redirect(url_for("login"))
            user_role = g.user["role"]
            if required_role == "owner" and user_role not in ("owner", "both"):
                abort(403)
            if required_role == "renter" and user_role not in ("renter", "both"):
                abort(403)
            return view(*args, **kwargs)

        wrapped.__name__ = view.__name__
        return wrapped

    return decorator


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


def fetch_available_cars(
    *,
    latitude: float,
    longitude: float,
    radius_km: float,
    city: Optional[str] = None,
) -> List[Car]:
    db = get_db()
    params: List[object] = []
    city_clause = ""
    if city:
        city_clause = " AND LOWER(cars.city) = LOWER(?)"
        params.append(city)

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
        WHERE cars.is_active = 1{city_clause}
        """,
        params,
    ).fetchall()

    car_ids = [row["id"] for row in rows]
    images_by_car = fetch_car_images(car_ids)

    cars: List[Car] = []
    for row in rows:
        distance = haversine_km(latitude, longitude,
                                row["latitude"], row["longitude"])
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
                seats=row["seats"],
                owner_username=row["owner_username"],
                city=row["city"] or "",
                image_url=row["image_url"] or "",
                fuel_type=row["fuel_type"] or "",
                transmission=row["transmission"] or "",
                rating=row["rating"] or 4.5,
                description=row["description"] or "",
                daily_rate=row["daily_rate"] or row["rate_per_hour"] * 24,
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
    has_role=has_role, now=datetime.utcnow, promo_codes=PROMO_CODES)


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
        return
    db = get_db()
    g.user = db.execute(
        "SELECT id, username, role FROM users WHERE id = ?",
        (user_id,),
    ).fetchone()


@app.route("/")
def home() -> str:
    db = get_db()
    cities = [row[0] for row in db.execute(
        "SELECT DISTINCT city FROM cars WHERE city <> '' ORDER BY city").fetchall()]
    return render_template("home.html", cities=cities)


@app.route("/search", methods=["GET", "POST"])
@login_required
@role_required("renter")
def search() -> str:
    cars: List[Car] = []
    cars_payload: List[dict] = []
    pricing_map: Dict[int, Dict[str, object]] = {}
    latitude = longitude = radius = None
    city = request.form.get(
        "city") if request.method == "POST" else request.args.get("city")
    detected_city = None
    start_time_raw = end_time_raw = None
    error = request.args.get("error")

    if request.method == "POST":
        start_time_raw = request.form.get("start_time")
        end_time_raw = request.form.get("end_time")
        try:
            latitude = float(request.form.get("latitude", ""))
            longitude = float(request.form.get("longitude", ""))
            radius = float(request.form.get("radius", "10"))
            if not city:
                detected_city = reverse_geocode_city(latitude, longitude)
                city = detected_city or city
            cars = fetch_available_cars(
                latitude=latitude,
                longitude=longitude,
                radius_km=radius,
                city=city,
            )
            if not cars:
                error = "No cars found within the selected radius."
        except ValueError:
            error = "Latitude, longitude, and radius must be numeric values."
    else:
        start_time_raw = request.args.get("start_time")
        end_time_raw = request.args.get("end_time")
        try:
            radius_query = request.args.get("radius")
            radius = float(radius_query) if radius_query else None
        except (TypeError, ValueError):
            radius = None
        try:
            lat_query = request.args.get("latitude")
            lon_query = request.args.get("longitude")
            latitude = float(lat_query) if lat_query else None
            longitude = float(lon_query) if lon_query else None
        except (TypeError, ValueError):
            latitude = longitude = None

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
                    "seats": car.seats,
                    "owner_username": car.owner_username,
                    "city": car.city,
                    "image_url": car.image_url,
                    "fuel_type": car.fuel_type,
                    "transmission": car.transmission,
                    "rating": car.rating,
                    "description": car.description,
                    "daily_rate": car.daily_rate,
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
    )


@app.route("/rentals")
@login_required
@role_required("renter")
def rentals() -> str:
    db = get_db()
    rows = db.execute(
        """
        SELECT rentals.*, cars.brand, cars.model, cars.licence_plate, cars.image_url
        FROM rentals
        JOIN cars ON cars.id = rentals.car_id
        WHERE rentals.renter_id = ?
        ORDER BY rentals.start_time DESC
        """,
        (g.user["id"],),
    ).fetchall()
    return render_template("rentals.html", rentals=rows)


@app.route("/rent/<int:car_id>", methods=["POST"])
@login_required
@role_required("renter")
def rent_car(car_id: int) -> str:
    db = get_db()
    car = db.execute(
        "SELECT id, is_available, rate_per_hour, daily_rate FROM cars WHERE id = ?",
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

    db.execute(
        "INSERT INTO rentals (car_id, renter_id, status, start_time, end_time, promo_code, discount_amount, total_amount)"
        " VALUES (?, ?, 'booked', ?, ?, ?, ?, ?)",
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
    db.execute(
        "UPDATE cars SET is_available = 0, updated_at = ? WHERE id = ?",
        (datetime.utcnow().isoformat(), car_id),
    )
    db.commit()
    return redirect(url_for("rentals"))


@app.route("/rentals/<int:rental_id>/cancel", methods=["POST"])
@login_required
@role_required("renter")
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
    active_rentals = db.execute(
        """
        SELECT rentals.*, users.username AS renter_username, cars.brand, cars.model
        FROM rentals
        JOIN cars ON cars.id = rentals.car_id
        JOIN users ON users.id = rentals.renter_id
        WHERE cars.owner_id = ? AND rentals.status IN ('booked', 'active')
        ORDER BY rentals.start_time DESC
        """,
        (g.user["id"],),
    ).fetchall()
    return render_template("owner_cars.html", cars=car_dicts, rentals=active_rentals)


@app.route("/owner/cars/add", methods=["POST"])
@login_required
@role_required("owner")
def owner_add_car() -> str:
    form = request.form
    try:
        seats = int(form.get("seats", 4))
        rate = float(form.get("rate_per_hour", 0))
        daily_rate_input = form.get("daily_rate")
        daily_rate = float(daily_rate_input) if daily_rate_input else rate * 24
        latitude = float(form.get("latitude"))
        longitude = float(form.get("longitude"))
    except (TypeError, ValueError):
        return redirect(url_for("owner_cars"))
    db = get_db()
    cursor = db.execute(
        """
        INSERT INTO cars (owner_id, name, brand, model, licence_plate, seats, rate_per_hour, daily_rate,
                          latitude, longitude, city, image_url, fuel_type, transmission, description)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            g.user["id"],
            form.get(
                "name") or f"{form.get('brand', '')} {form.get('model', '')}",
            form.get("brand", "Unknown"),
            form.get("model", "Unknown"),
            form.get("licence_plate", ""),
            seats,
            rate,
            daily_rate,
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


@app.route("/owner/rentals/<int:rental_id>/complete", methods=["POST"])
@login_required
@role_required("owner")
def owner_complete_rental(rental_id: int) -> str:
    db = get_db()
    rental = db.execute(
        """
        SELECT rentals.id, rentals.car_id
        FROM rentals
        JOIN cars ON cars.id = rentals.car_id
        WHERE rentals.id = ? AND cars.owner_id = ? AND rentals.status IN ('booked', 'active')
        """,
        (rental_id, g.user["id"]),
    ).fetchone()
    if rental is None:
        abort(404)
    db.execute(
        "UPDATE rentals SET status = 'completed', end_time = ? WHERE id = ?",
        (datetime.utcnow().isoformat(), rental_id),
    )
    db.execute(
        "UPDATE cars SET is_available = 1, updated_at = ? WHERE id = ?",
        (datetime.utcnow().isoformat(), rental["car_id"]),
    )
    db.commit()
    return redirect(url_for("owner_cars"))


@app.route("/register", methods=["GET", "POST"])
def register() -> str:
    if g.user is not None:
        return redirect(url_for("home"))
    error = None
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        role = request.form.get("role", "renter")
        if role not in {"owner", "renter", "both"}:
            role = "renter"
        if not username or not password:
            error = "Username and password are required."
        else:
            db = get_db()
            try:
                db.execute(
                    "INSERT INTO users (username, password_hash, role) VALUES (?, ?, ?)",
                    (username, generate_password_hash(password), role),
                )
                db.commit()
            except sqlite3.IntegrityError:
                error = "Username is already taken."
            else:
                return redirect(url_for("login", message="Account created. Please sign in."))
    return render_template("register.html", error=error)


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
