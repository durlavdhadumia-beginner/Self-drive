from pathlib import Path
path = Path("app.py")
text = path.read_text()
old = "app.config.update(\n    SECRET_KEY=\"replace-with-a-secure-random-value\",\n    UPLOAD_FOLDER=str(UPLOAD_ROOT),\n    MAX_CONTENT_LENGTH=16 * 1024 * 1024,  # 16 MB per request\n)\n"
new = "app.config.update(\n    SECRET_KEY=\"replace-with-a-secure-random-value\",\n    UPLOAD_FOLDER=str(UPLOAD_ROOT),\n    MAX_CONTENT_LENGTH=16 * 1024 * 1024,  # 16 MB per request\n    ADMIN_SETUP_SECRET=os.environ.get(\"ADMIN_SETUP_SECRET\", \"DRIVENOW-ADMIN\"),\n)\n"
if old not in text:
    raise SystemExit("config block not found")
path.write_text(text.replace(old, new))
