from flask import Flask, send_from_directory
from flask_cors import CORS
from pathlib import Path
from routes.communities import communities_bp
from routes.time_data import time_data_bp
from routes.debug import debug_bp
from routes.health import health_bp
from routes.performance import performance_bp
from utils.db import check_database


# --- Paths ---
BASE_DIR = Path(__file__).parent
DIST_FOLDER = BASE_DIR / "../frontend/dist"  # Vite build output

# --- Flask app ---
app = Flask(__name__, static_folder=DIST_FOLDER, static_url_path="/")
CORS(app)

# --- Register blueprints ---
app.register_blueprint(communities_bp, url_prefix='/api')
app.register_blueprint(time_data_bp, url_prefix='/api')
app.register_blueprint(debug_bp, url_prefix='/api')
app.register_blueprint(health_bp, url_prefix='/api')
app.register_blueprint(performance_bp, url_prefix='/api')


# --- Serve frontend SPA ---
@app.route("/", defaults={"path": ""})
@app.route("/<path:path>")
def serve(path):
    requested_file = DIST_FOLDER / path
    if requested_file.exists() and requested_file.is_file():
        return send_from_directory(DIST_FOLDER, path)
    # Fallback to index.html for SPA routes
    return send_from_directory(DIST_FOLDER, "index.html")

if __name__ == "__main__":
    db_ok, message = check_database()
    print(f"🗄️ Database status: {message}")

    if not db_ok:
        print("❌ Database not ready. Run:")
        print("   python scripts/csv_migrate_to_sqlite.py")
    else:
        print("✅ Database ready, starting server...")
        app.run(debug=True, port=5001, threaded=True)
