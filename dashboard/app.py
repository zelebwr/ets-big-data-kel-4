# ══════════════════════════════════════════════════════════════════════════════
# NewsPulse — Dashboard Flask
# GET /          → render index.html
# GET /api/data  → return JSON gabungan spark + live data
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
    """Return JSON gabungan semua data untuk dashboard."""
    spark_results = load_json("spark_results.json", {
        "kata_trending": [],
        "distribusi_sumber": [],
        "volume_per_jam": [],
    })
    live_api = load_json("live_api.json", [])
    live_rss = load_json("live_rss.json", [])

    # Gabung semua berita terbaru
    all_news = live_api + live_rss
    # Sort by timestamp descending
    all_news.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
    # Max 50 berita terbaru
    all_news = all_news[:50]

    return jsonify({
        "spark": spark_results,
        "live_news": all_news,
        "total_api": len(live_api),
        "total_rss": len(live_rss),
    })


if __name__ == "__main__":
    # Pastikan folder data ada
    os.makedirs(DATA_DIR, exist_ok=True)
    print("[NewsPulse] Dashboard ready at http://localhost:5000")
    app.run(debug=True, host="0.0.0.0", port=5000)
