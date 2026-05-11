# ══════════════════════════════════════════════════════════════════════════════
# NewsPulse — Producer API (GNews)
# Mengambil top headlines Indonesia dari GNews API setiap 10 menit
# Topic Kafka: news-api | Key: kategori berita
# ══════════════════════════════════════════════════════════════════════════════

import json
import time
import hashlib
import requests
import os
from datetime import datetime
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

# ── Konfigurasi ──────────────────────────────────────────────────────────────
# Daftar gratis di https://gnews.io → dapatkan API key
GNEWS_API_KEY   = os.getenv("GNEWS_API_KEY", "YOUR_GNEWS_API_KEY_HERE")
GNEWS_URL       = "https://gnews.io/api/v4/top-headlines"

POLL_INTERVAL   = int(os.getenv("API_POLL_INTERVAL_SECONDS", "60"))
KAFKA_BROKER    = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC     = "news-api"
LIVE_OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "..", "dashboard", "data", "live_api.json")

# Set untuk menyimpan URL yang sudah dikirim (hindari duplikat)
sent_urls = set()
# ─────────────────────────────────────────────────────────────────────────────


def create_producer() -> KafkaProducer:
    """Inisialisasi Kafka Producer dengan idempotence & acks=all."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        enable_idempotence=True,        # exactly-once per partisi
        acks="all",                     # tunggu semua ISR acknowledge
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        retries=5,
    )


def hash_url(url: str) -> str:
    """Hash URL untuk deduplication."""
    return hashlib.md5(url.encode()).hexdigest()[:8]


def fetch_news() -> list:
    """Ambil top headlines Indonesia dari GNews API."""
    params = {
        "country": "id",
        "lang":    "id",
        "max":     10,
        "token":   GNEWS_API_KEY,
    }
    try:
        resp = requests.get(GNEWS_URL, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        articles = []
        for item in data.get("articles", []):
            image_url = (
                item.get("image")
                or item.get("thumbnail")
                or item.get("urlToImage")
                or ""
            )
            # Format JSON konsisten sesuai spesifikasi ETS
            event = {
                "judul":        item.get("title", ""),
                "sumber":       item.get("source", {}).get("name", "GNews"),
                "url":          item.get("url", ""),
                "kategori":     "nasional",             # default kategori
                "deskripsi":    item.get("description", ""),
                "image":        image_url,              # thumbnail dari GNews
                "thumbnail":    image_url,
                "waktu_terbit": item.get("publishedAt", datetime.now().isoformat()),
                "timestamp":    datetime.now().isoformat(),
            }
            articles.append(event)
        return articles

    except requests.exceptions.RequestException as e:
        print(f"  [ERROR] Gagal fetch GNews API: {e}")
        return []


def save_live_snapshot(events: list):
    """Simpan snapshot terbaru ke file lokal (untuk dashboard)."""
    os.makedirs(os.path.dirname(LIVE_OUTPUT_PATH), exist_ok=True)

    # Baca data lama jika ada, append data baru
    existing = []
    if os.path.exists(LIVE_OUTPUT_PATH):
        try:
            with open(LIVE_OUTPUT_PATH, "r") as f:
                existing = json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            existing = []

    # Gabung, deduplicate by URL, simpan max 100 terbaru
    all_events = events + existing
    seen = set()
    unique = []
    for e in all_events:
        url_hash = hash_url(e.get("url", ""))
        if url_hash not in seen:
            seen.add(url_hash)
            unique.append(e)
    unique = unique[:100]  # simpan max 100

    with open(LIVE_OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(unique, f, indent=2, ensure_ascii=False)
    print(f"  [SNAPSHOT] live_api.json diperbarui -- {len(events)} berita diproses, total {len(unique)}")


def main():
    print("=" * 60)
    print("  NewsPulse -- Producer API (GNews)")
    print("=" * 60)
    print(f"  Broker  : {KAFKA_BROKER}")
    print(f"  Topic   : {KAFKA_TOPIC}")
    print(f"  Interval: {POLL_INTERVAL} detik")
    print(f"  API     : GNews (top headlines Indonesia)")
    print("=" * 60 + "\n")

    if GNEWS_API_KEY == "YOUR_GNEWS_API_KEY_HERE":
        print("  [WARNING] API key belum diatur! Set GNEWS_API_KEY di environment/.env")
        print("  [WARNING] Daftar gratis di https://gnews.io\n")

    producer = create_producer()
    print("[INIT] Berhasil terhubung ke Kafka broker.\n")

    cycle = 0
    while True:
        cycle += 1
        print(f"--- Cycle {cycle} | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")

        articles = fetch_news()
        sent_this_cycle = []

        for article in articles:
            url_hash = hash_url(article["url"])

            # Lewati jika sudah pernah dikirim
            if url_hash in sent_urls:
                continue

            future = producer.send(
                KAFKA_TOPIC,
                key=article["kategori"],     # key = kategori berita
                value=article,
            )
            future.get(timeout=10)  # tunggu ACK

            sent_urls.add(url_hash)
            sent_this_cycle.append(article)
            print(
                f"  [SENT] {article['sumber']:15s} | "
                f"{article['judul'][:60]}..."
            )

        producer.flush()
        print(f"  [FLUSH] {len(sent_this_cycle)} berita baru dikirim ke Kafka")

        if articles:
            save_live_snapshot(articles)

        print(f"  [SLEEP] Menunggu {POLL_INTERVAL} detik...\n")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
