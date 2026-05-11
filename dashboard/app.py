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


@app.route("/")
def index():
    """Render halaman dashboard utama."""
    return render_template("index.html")


@app.route("/api/data")
def api_data():
    """Return JSON hasil olahan Spark untuk dashboard."""
    spark_results = load_json("spark_results.json", {
        "kata_trending": [],
        "distribusi_sumber": [],
        "volume_per_jam": [],
        "live_news": [],
    })

    live_api = load_json("live_api.json", [])
    live_rss = load_json("live_rss.json", [])
    
    return jsonify({
        "spark": spark_results,
        "live_news": live_api + live_rss,
        "total_api": spark_results.get("total_api", 0),
        "total_rss": spark_results.get("total_rss", 0),
    })


if __name__ == "__main__":
    # Pastikan folder data ada
    os.makedirs(DATA_DIR, exist_ok=True)
    print("[NewsPulse] Dashboard ready at http://localhost:5000")
    app.run(debug=True, host="0.0.0.0", port=5000)
