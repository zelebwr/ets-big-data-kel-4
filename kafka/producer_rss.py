# ══════════════════════════════════════════════════════════════════════════════
# NewsPulse — Producer RSS (Kompas & Tempo Nasional)
# Polling 2 RSS feed sekaligus dalam 1 producer, setiap 5 menit
# Topic Kafka: news-rss | Key: hash 8 char dari URL
# ══════════════════════════════════════════════════════════════════════════════

import json
import time
import hashlib
import os
import re
import feedparser
from datetime import datetime
from kafka import KafkaProducer

# ── Konfigurasi ──────────────────────────────────────────────────────────────
RSS_FEEDS = [
    {
        "url": "https://rss.kompas.com/feed/kompas.com/nasional",
        "sumber": "Kompas",
    },
    {
        "url": "https://rss.tempo.co/nasional",
        "sumber": "Tempo",
    },
]

POLL_INTERVAL_SECONDS = 300  # 5 menit sesuai spesifikasi ETS
POLL_INTERVAL_SECONDS = int(os.getenv("RSS_POLL_INTERVAL_SECONDS", str(POLL_INTERVAL_SECONDS)))

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC  = "news-rss"

LIVE_OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "..", "dashboard", "data", "live_rss.json")
# ─────────────────────────────────────────────────────────────────────────────

# Set global untuk menghindari duplikat antar kedua RSS
sent_ids = set()


def hash_url(url: str) -> str:
    """Buat key 8 karakter dari URL — untuk menghindari duplikat."""
    return hashlib.md5(url.encode()).hexdigest()[:8]


def create_producer() -> KafkaProducer:
    """Inisialisasi Kafka Producer dengan idempotence & acks=all."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        enable_idempotence=True,
        acks="all",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        retries=5,
    )


def extract_entry_image(entry: dict) -> str:
    """Ambil thumbnail dari RSS jika feed menyediakannya."""
    media_content = entry.get("media_content") or []
    if media_content and media_content[0].get("url"):
        return media_content[0]["url"]

    media_thumbnail = entry.get("media_thumbnail") or []
    if media_thumbnail and media_thumbnail[0].get("url"):
        return media_thumbnail[0]["url"]

    for link in entry.get("links", []):
        if link.get("rel") == "enclosure" and str(link.get("type", "")).startswith("image/"):
            return link.get("href", "")

    summary = entry.get("summary", "") or entry.get("description", "")
    match = re.search(r'<img[^>]+src=["\']([^"\']+)["\']', summary, re.IGNORECASE)
    return match.group(1) if match else ""


def fetch_rss(feed_config: dict) -> list:
    """Parse satu RSS feed, kembalikan list artikel."""
    url    = feed_config["url"]
    sumber = feed_config["sumber"]

    try:
        feed = feedparser.parse(url)
        articles = []

        for entry in feed.entries:
            # Ekstrak waktu terbit
            waktu = entry.get("published", "")
            if not waktu:
                waktu = entry.get("updated", datetime.now().isoformat())

            article = {
                "judul":        entry.get("title", ""),
                "sumber":       sumber,
                "url":          entry.get("link", ""),
                "kategori":     "nasional",
                "deskripsi":    entry.get("summary", ""),
                "image":        extract_entry_image(entry),
                "waktu_terbit": waktu,
                "timestamp":    datetime.now().isoformat(),
            }
            articles.append(article)

        return articles

    except Exception as e:
        print(f"  [ERROR] Gagal fetch RSS {url}: {e}")
        return []


def save_live_snapshot(articles: list):
    """Simpan snapshot terbaru ke file lokal (untuk dashboard)."""
    os.makedirs(os.path.dirname(LIVE_OUTPUT_PATH), exist_ok=True)

    # Baca data lama jika ada
    existing = []
    if os.path.exists(LIVE_OUTPUT_PATH):
        try:
            with open(LIVE_OUTPUT_PATH, "r") as f:
                existing = json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            existing = []

    # Gabung, deduplicate, max 100
    all_articles = articles + existing
    seen = set()
    unique = []
    for a in all_articles:
        url_hash = hash_url(a.get("url", ""))
        if url_hash not in seen:
            seen.add(url_hash)
            unique.append(a)
    unique = unique[:100]

    with open(LIVE_OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(unique, f, indent=2, ensure_ascii=False)
    print(f"  [SNAPSHOT] live_rss.json updated -- {len(articles)} artikel diproses, total {len(unique)}")


def main():
    print("=" * 60)
    print("  NewsPulse -- Producer RSS (Kompas & Tempo Nasional)")
    print("=" * 60)
    print(f"  Broker  : {KAFKA_BROKER}")
    print(f"  Topic   : {KAFKA_TOPIC}")
    print(f"  Interval: {POLL_INTERVAL_SECONDS // 60} menit")
    print(f"  Feeds   : {[f['sumber'] for f in RSS_FEEDS]}")
    print("=" * 60 + "\n")

    producer = create_producer()
    print("[INIT] Berhasil terhubung ke Kafka broker.\n")

    cycle = 0
    while True:
        cycle += 1
        print(f"--- Cycle {cycle} | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")

        all_new_articles = []
        all_seen_articles = []

        for feed_config in RSS_FEEDS:
            articles = fetch_rss(feed_config)
            all_seen_articles.extend(articles)
            new_count = 0

            for article in articles:
                url_key = hash_url(article["url"])

                # Lewati jika sudah pernah dikirim (hindari duplikat antar RSS)
                if url_key in sent_ids:
                    continue

                producer.send(
                    KAFKA_TOPIC,
                    key=url_key,           # key = hash 8 char URL
                    value=article,
                )
                sent_ids.add(url_key)
                all_new_articles.append(article)
                new_count += 1

            print(f"  [RSS] {feed_config['sumber']:10s} → {new_count} artikel baru dikirim (total parsed: {len(articles)})")

        producer.flush()
        print(f"  [FLUSH] Total artikel baru cycle ini: {len(all_new_articles)}")

        if all_seen_articles:
            save_live_snapshot(all_seen_articles)

        print(f"  [SLEEP] Menunggu {POLL_INTERVAL_SECONDS // 60} menit...\n")
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
