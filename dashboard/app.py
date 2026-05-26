# ══════════════════════════════════════════════════════════════════════════════
# NewsPulse — Dashboard Flask
# GET /          → render index.html
# GET /api/data  → return JSON hasil olahan Spark
# ══════════════════════════════════════════════════════════════════════════════

import json
import os

from flask import Flask, render_template, jsonify

app = Flask(__name__)

# Path ke file data
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
LAKEHOUSE_PAYLOAD_PATH = os.path.join(ROOT_DIR, "lakehouse", "dashboard_data_from_delta.json")


def load_json(filename, default=None):
    """Load JSON file, return default jika tidak ada."""
    filepath = os.path.join(DATA_DIR, filename)
    if os.path.exists(filepath):
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            pass
    return default if default is not None else {}


def load_json_path(filepath, default=None):
    """Load JSON file by absolute path, return default jika tidak ada."""
    if os.path.exists(filepath):
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            pass
    return default if default is not None else {}


def normalize_lakehouse_to_spark(lakehouse_payload):
    """Map payload Lakehouse ke format yang dipakai frontend dashboard saat ini."""
    if not isinstance(lakehouse_payload, dict):
        return None

    kata_trending = lakehouse_payload.get("kata_trending", [])
    distribusi_sumber = lakehouse_payload.get("distribusi_sumber", [])
    volume_per_jam = lakehouse_payload.get("volume_per_jam")

    if not kata_trending and not distribusi_sumber:
        return None

    if not isinstance(volume_per_jam, list) or len(volume_per_jam) == 0:
        volume_per_jam = [
            {"jam": hour_index, "jumlah_berita": 0}
            for hour_index in range(24)
        ]

    return {
        "generated_at": lakehouse_payload.get("generated_at"),
        "kata_trending": kata_trending,
        "distribusi_sumber": distribusi_sumber,
        "volume_per_jam": volume_per_jam,
    }


@app.route("/")
def index():
    """Render halaman dashboard utama."""
    return render_template("index.html")


@app.route("/monitoring")
def monitoring():
    """Render halaman monitoring workflow."""
    return render_template("monitoring.html")


@app.route("/api/data")
def api_data():
    """Return JSON hasil olahan Spark untuk dashboard."""
    spark_results = load_json("spark_results.json", {
        "kata_trending": [],
        "distribusi_sumber": [],
        "volume_per_jam": [],
        "live_news": [],
    })
    lakehouse_payload = load_json_path(LAKEHOUSE_PAYLOAD_PATH, {})
    lakehouse_as_spark = normalize_lakehouse_to_spark(lakehouse_payload)
    if lakehouse_as_spark is not None:
        spark_results = lakehouse_as_spark

    live_api = load_json("live_api.json", [])
    live_rss = load_json("live_rss.json", [])
    total_api = spark_results.get("total_api", len(live_api))
    total_rss = spark_results.get("total_rss", len(live_rss))

    return jsonify({
        "spark": spark_results,
        "live_news": live_api + live_rss,
        "total_api": total_api,
        "total_rss": total_rss,
    })


@app.route("/api/status")
def api_status():
    """Return basic pipeline status written by the lakehouse runner."""
    status_path = os.path.join(DATA_DIR, "pipeline_status.json")
    status = load_json("pipeline_status.json", None)
    if not status:
        return jsonify({"phase": "unknown", "last_run": None, "last_success": False})
    return jsonify(status)


if __name__ == "__main__":
    # Pastikan folder data ada
    os.makedirs(DATA_DIR, exist_ok=True)
    print("[NewsPulse] Dashboard ready at http://localhost:5000")
    debug_mode = os.getenv("FLASK_DEBUG", "0") == "1"
    app.run(debug=debug_mode, host="0.0.0.0", port=5000)
