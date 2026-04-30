# [Muhammad Fatihul Qolbi]: producer_rss.py — polling RSS cuaca, kirim ke weather-rss topic

import json
import time
import hashlib
import feedparser
from datetime import datetime
from kafka import KafkaProducer

# ── Konfigurasi ──────────────────────────────────────────────────────────────
RSS_URLS = [
    "https://rss.tempo.co/tag/cuaca",
    "https://rss.kompas.com/feed/kompas.com/sains/environment",  # backup
]

POLL_INTERVAL_SECONDS = 300  # 5 menit sesuai spesifikasi ETS

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC  = "weather-rss"

LIVE_OUTPUT_PATH = "../dashboard/data/live_rss.json"
# ─────────────────────────────────────────────────────────────────────────────


def hash_url(url: str) -> str:
    """Buat key 8 karakter dari URL — untuk menghindari duplikat."""
    return hashlib.md5(url.encode()).hexdigest()[:8]


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        enable_idempotence=True,
        acks="all",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )


def fetch_rss(url: str) -> list:
    """Parse satu RSS feed, kembalikan list artikel."""
    try:
        feed = feedparser.parse(url)
        articles = []
        for entry in feed.entries:
            article = {
                "judul":      entry.get("title", ""),
                "link":       entry.get("link", ""),
                "ringkasan":  entry.get("summary", ""),
                "sumber":     feed.feed.get("title", url),
                "waktu_terbit": entry.get("published", datetime.now().isoformat()),
                "timestamp":  datetime.now().isoformat(),
            }
            articles.append(article)
        return articles
    except Exception as e:
        print(f"[ERROR] Gagal fetch RSS {url}: {e}")
        return []


def save_live_snapshot(articles: list):
    import os
    os.makedirs(os.path.dirname(LIVE_OUTPUT_PATH), exist_ok=True)
    with open(LIVE_OUTPUT_PATH, "w") as f:
        json.dump(articles, f, indent=2, ensure_ascii=False)
    print(f"[SNAPSHOT] live_rss.json updated — {len(articles)} artikel")


def main():
    print("[INIT] Menghubungkan ke Kafka broker...")
    producer = create_producer()
    print(f"[INIT] Connected. Topic target: {KAFKA_TOPIC}")
    print(f"[INIT] Polling interval: {POLL_INTERVAL_SECONDS // 60} menit\n")

    sent_ids = set()  # simpan URL yang sudah pernah dikirim — hindari duplikat

    cycle = 0
    while True:
        cycle += 1
        print(f"--- Cycle {cycle} | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")

        all_articles = []

        for rss_url in RSS_URLS:
            articles = fetch_rss(rss_url)
            new_count = 0

            for article in articles:
                url_key = hash_url(article["link"])

                # Lewati jika sudah pernah dikirim (hindari duplikat)
                if url_key in sent_ids:
                    continue

                producer.send(
                    KAFKA_TOPIC,
                    key=url_key,
                    value=article,
                )
                sent_ids.add(url_key)
                all_articles.append(article)
                new_count += 1

            print(f"  [RSS] {rss_url[:50]}... → {new_count} artikel baru dikirim")

        producer.flush()
        print(f"  [FLUSH] Total artikel baru cycle ini: {len(all_articles)}")

        if all_articles:
            save_live_snapshot(all_articles)

        print(f"  [SLEEP] Menunggu {POLL_INTERVAL_SECONDS // 60} menit...\n")
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()