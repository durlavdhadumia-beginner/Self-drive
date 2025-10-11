from pathlib import Path

path = Path("app.py")
text = path.read_text()
old1 = "        db.execute(\n            \"INSERT INTO reviews (rental_id, reviewer_id, target_user_id, reviewer_role, target_role, trip_rating, car_rating, owner_rating, passenger_rating, comment, created_at)\n             VALUES (?, ?, ?, 'renter', 'owner', ?, ?, ?, NULL, ?, ?)\",\n            (rental_id, g.user[\"id\"], rental[\"owner_id\"], trip_rating, car_rating, owner_rating, comment, now_iso),\n        )\n"
new1 = "        db.execute(\n            \"\"\"\n            INSERT INTO reviews (rental_id, reviewer_id, target_user_id, reviewer_role, target_role, trip_rating, car_rating, owner_rating, passenger_rating, comment, created_at)\n            VALUES (?, ?, ?, 'renter', 'owner', ?, ?, ?, NULL, ?, ?)\n            \"\"\",\n            (rental_id, g.user[\"id\"], rental[\"owner_id\"], trip_rating, car_rating, owner_rating, comment, now_iso),\n        )\n"
if old1 not in text:
    raise SystemExit("renter review insert not found")
text = text.replace(old1, new1)
old2 = "        db.execute(\n            \"INSERT INTO reviews (rental_id, reviewer_id, target_user_id, reviewer_role, target_role, passenger_rating, trip_rating, car_rating, owner_rating, comment, created_at)\n             VALUES (?, ?, ?, 'owner', 'renter', ?, NULL, NULL, NULL, ?, ?)\",\n            (rental_id, g.user[\"id\"], rental["renter_id"], passenger_rating, comment, now_iso),\n        )\n"
new2 = "        db.execute(\n            \"\"\"\n            INSERT INTO reviews (rental_id, reviewer_id, target_user_id, reviewer_role, target_role, passenger_rating, trip_rating, car_rating, owner_rating, comment, created_at)\n            VALUES (?, ?, ?, 'owner', 'renter', ?, NULL, NULL, NULL, ?, ?)\n            \"\"\",\n            (rental_id, g.user[\"id\"], rental["renter_id"], passenger_rating, comment, now_iso),\n        )\n"
if old2 not in text:
    raise SystemExit("owner review insert not found")
path.write_text(text.replace(old2, new2))
