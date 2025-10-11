from pathlib import Path
import textwrap

path = Path("app.py")
text = path.read_text()
marker = "@app.route(\"/owner/rentals/<int:rental_id>/respond\", methods=[\"POST\"])\n"
if marker not in text:
    raise SystemExit("owner respond marker not found")
block = textwrap.dedent("""@app.route(\"/owner/rentals/<int:rental_id>/review\", methods=[\"POST\"])
@login_required
@role_required(\"owner\")
def owner_review_rental(rental_id: int) -> str:
    db = get_db()
    rental = db.execute(
        \"\"\"
        SELECT rentals.*, cars.name AS car_name, cars.brand, cars.model, rentals.renter_id, renters.username AS renter_username
        FROM rentals
        JOIN cars ON cars.id = rentals.car_id
        JOIN users AS renters ON renters.id = rentals.renter_id
        WHERE rentals.id = ? AND cars.owner_id = ? AND rentals.status = 'completed'
        \"\"\",
        (rental_id, g.user[\"id\"]),
    ).fetchone()
    if rental is None:
        abort(404)
    try:
        passenger_rating = int(request.form.get(\"passenger_rating\", \"0\"))
    except (TypeError, ValueError):
        return redirect(url_for(\"owner_cars\"))
    if passenger_rating < 1 or passenger_rating > 5:
        return redirect(url_for(\"owner_cars\"))
    comment = request.form.get(\"comment\", \"\").strip()
    existing = db.execute(
        \"SELECT id FROM reviews WHERE rental_id = ? AND reviewer_id = ?\",
        (rental_id, g.user[\"id\"]),
    ).fetchone()
    now_iso = datetime.utcnow().isoformat()
    if existing:
        db.execute(
            \"UPDATE reviews SET passenger_rating = ?, trip_rating = NULL, car_rating = NULL, owner_rating = NULL, comment = ?, created_at = ? WHERE id = ?\",
            (passenger_rating, comment, now_iso, existing[\"id\"]),
        )
    else:
        db.execute(
            \"\"\"
            INSERT INTO reviews (rental_id, reviewer_id, target_user_id, reviewer_role, target_role, passenger_rating, trip_rating, car_rating, owner_rating, comment, created_at)
            VALUES (?, ?, ?, 'owner', 'renter', ?, NULL, NULL, NULL, ?, ?)
            \"\"\",
            (rental_id, g.user[\"id\"], rental['renter_id'], passenger_rating, comment, now_iso),
        )
    db.commit()
    car_label = rental[\"car_name\"] or f\"{rental['brand']} {rental['model']}\"
    create_notification(
        rental['renter_id'],
        f\"{g.user['username']} rated you after the trip with {car_label}.\",
        url_for(\"rentals\"),
    )
    return redirect(url_for(\"owner_cars\"))\n\n\n""")
text = text.replace(marker, block + marker)
path.write_text(text)
