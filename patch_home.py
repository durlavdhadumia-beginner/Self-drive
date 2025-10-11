from pathlib import Path
import textwrap

path = Path("app.py")
text = path.read_text()
old = textwrap.dedent("""
@app.route("/")
def home() -> str:
    db = get_db()
    cities = [row[0] for row in db.execute(
        "SELECT DISTINCT city FROM cars WHERE city <> '' ORDER BY city").fetchall()]
    return render_template("home.html", cities=cities)
""")
new = textwrap.dedent("""
@app.route("/")
def home() -> str:
    db = get_db()
    cities = [row[0] for row in db.execute(
        "SELECT DISTINCT city FROM cars WHERE city <> '' ORDER BY city").fetchall()]
    vehicle_types = [row[0] for row in db.execute(
        "SELECT DISTINCT vehicle_type FROM cars WHERE vehicle_type <> '' ORDER BY vehicle_type").fetchall()]
    return render_template("home.html", cities=cities, vehicle_types=vehicle_types)
""")
if old not in text:
    raise SystemExit("home function block not found")
path.write_text(text.replace(old, new))
