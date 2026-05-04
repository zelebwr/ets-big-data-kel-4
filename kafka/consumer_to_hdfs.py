# ══════════════════════════════════════════════════════════════════════════════
# NewsPulse — Consumer to HDFS
# Subscribe ke news-api dan news-rss, buffer lalu flush ke HDFS
# Bonus: menggunakan hdfs Python library langsung (+2 poin)
# ══════════════════════════════════════════════════════════════════════════════

import json
import hashlib
import threading
import os
import time
import subprocess
from datetime import datetime
from kafka import KafkaConsumer

# ── Konfigurasi ──────────────────────────────────────────────────────────────
KAFKA_BROKER    = "localhost:9092"
TOPIC_API       = "news-api"
TOPIC_RSS       = "news-rss"
GROUP_ID        = "news-consumer"

HDFS_PATH_API   = "/data/news/api"
HDFS_PATH_RSS   = "/data/news/rss"

# Folder sementara sebelum upload ke HDFS
LOCAL_TEMP_DIR  = os.path.join(os.path.dirname(__file__), "..", "temp_buffer")

# Juga update file dashboard
LIVE_API_PATH   = os.path.join(os.path.dirname(__file__), "..", "dashboard", "data", "live_api.json")
LIVE_RSS_PATH   = os.path.join(os.path.dirname(__file__), "..", "dashboard", "data", "live_rss.json")

FLUSH_INTERVAL  = 120   # flush ke HDFS setiap 2 menit
# ─────────────────────────────────────────────────────────────────────────────

# Buffer per topic (diakses dari 2 thread, pakai lock)
buffer_api = []
buffer_rss = []
lock_api   = threading.Lock()
lock_rss   = threading.Lock()


# ══════════════════════════════════════════════════════════════════════════════
# HDFS Upload — menggunakan hdfs Python library (bonus +2 poin)
# Fallback ke subprocess jika library tidak tersedia
# ══════════════════════════════════════════════════════════════════════════════

USE_HDFS_LIBRARY = False
hdfs_client = None

try:
    from hdfs import InsecureClient
    hdfs_client = InsecureClient("http://localhost:9870", user="root")
    # Test koneksi — cek apakah bisa resolve hostname datanode
    hdfs_client.status("/", strict=False)
    # Test write kecil untuk memastikan redirect ke datanode bisa jalan
    hdfs_client.write("/tmp/_test_conn", b"ok", overwrite=True)
    USE_HDFS_LIBRARY = True
    print("[INIT] Menggunakan hdfs Python library (bonus +2)")
except Exception as e:
    print(f"[INIT] hdfs library tidak tersedia ({e}), menggunakan subprocess fallback")
    USE_HDFS_LIBRARY = False


def put_to_hdfs_library(data: list, hdfs_path: str, filename: str):
    """Upload data langsung ke HDFS menggunakan hdfs Python library."""
    full_path = f"{hdfs_path}/{filename}"
    content = json.dumps(data, indent=2, ensure_ascii=False)
    try:
        hdfs_client.write(full_path, content.encode("utf-8"), overwrite=True)
        print(f"  [HDFS-LIB] Tersimpan: {full_path}")
    except Exception as e:
        print(f"  [HDFS-LIB] Gagal: {e}")
        # Fallback ke subprocess
        put_to_hdfs_subprocess(data, hdfs_path, filename)


def put_to_hdfs_subprocess(data: list, hdfs_path: str, filename: str):
    """Upload file ke HDFS menggunakan docker exec + hdfs dfs -put (fallback)."""
    os.makedirs(LOCAL_TEMP_DIR, exist_ok=True)
    local_path = os.path.join(LOCAL_TEMP_DIR, filename)

    with open(local_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    # Step 1: Salin file ke dalam container namenode
    copy_cmd = ["docker", "cp", local_path, f"hadoop-namenode:/tmp/{filename}"]
    result_cp = subprocess.run(copy_cmd, capture_output=True, text=True)
    if result_cp.returncode != 0:
        print(f"  [ERROR] docker cp gagal: {result_cp.stderr}")
        return

    # Step 2: Pastikan direktori HDFS ada (auto-create)
    mkdir_cmd = [
        "docker", "exec", "hadoop-namenode",
        "hdfs", "dfs", "-mkdir", "-p", hdfs_path
    ]
    subprocess.run(mkdir_cmd, capture_output=True, text=True)

    # Step 3: Put dari container ke HDFS
    put_cmd = [
        "docker", "exec", "hadoop-namenode",
        "hdfs", "dfs", "-put", "-f",
        f"/tmp/{filename}", hdfs_path
    ]
    result_put = subprocess.run(put_cmd, capture_output=True, text=True)
    if result_put.returncode != 0:
        print(f"  [ERROR] hdfs put gagal: {result_put.stderr}")
    else:
        print(f"  [HDFS-SUB] Tersimpan: {hdfs_path}/{filename}")


def put_to_hdfs(data: list, hdfs_path: str, filename: str):
    """Upload ke HDFS — pilih metode terbaik yang tersedia."""
    if USE_HDFS_LIBRARY and hdfs_client:
        put_to_hdfs_library(data, hdfs_path, filename)
    else:
        put_to_hdfs_subprocess(data, hdfs_path, filename)


def update_dashboard_file(data: list, filepath: str):
    """Update file JSON untuk dashboard."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    existing = []
    if os.path.exists(filepath):
        try:
            with open(filepath, "r") as f:
                existing = json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            existing = []

    # Gabung data baru + lama, deduplicate by URL, max 100 entries
    combined = data + existing
    seen = set()
    unique = []
    for item in combined:
        url = item.get("url", "")
        key = hashlib.md5(url.encode()).hexdigest()[:8] if url else item.get("judul", "")
        if key in seen:
            continue
        seen.add(key)
        unique.append(item)
    combined = unique[:100]

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(combined, f, indent=2, ensure_ascii=False)


def flush_buffer(buffer: list, lock: threading.Lock, hdfs_path: str,
                 dashboard_path: str, label: str):
    """Ambil isi buffer, upload ke HDFS, update dashboard file."""
    with lock:
        if not buffer:
            return
        data_to_flush = buffer.copy()
        buffer.clear()

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename  = f"{label}_{timestamp}.json"

    print(f"  [FLUSH] {label.upper()}: {len(data_to_flush)} event → {hdfs_path}/{filename}")
    put_to_hdfs(data_to_flush, hdfs_path, filename)
    update_dashboard_file(data_to_flush, dashboard_path)


def consume_topic(topic: str, buffer: list, lock: threading.Lock, label: str):
    """Thread: terus-menerus baca dari Kafka topic, simpan ke buffer."""
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        consumer_timeout_ms=5000,   # timeout 5 detik kalau tidak ada pesan baru
    )

    print(f"  [CONSUMER] Thread {label} aktif — membaca topic: {topic}")

    try:
        while True:
            for msg in consumer:
                with lock:
                    buffer.append(msg.value)
            # Kalau timeout (tidak ada pesan), tunggu sebentar lalu lanjut
            time.sleep(2)
    except Exception as e:
        print(f"  [ERROR] Thread {label}: {e}")
    finally:
        consumer.close()


def flush_loop():
    """Thread: setiap FLUSH_INTERVAL detik, flush kedua buffer ke HDFS."""
    while True:
        time.sleep(FLUSH_INTERVAL)
        now = datetime.now().strftime("%H:%M:%S")
        print(f"\n[SCHEDULER] {now} — Flushing ke HDFS...")
        flush_buffer(buffer_api, lock_api, HDFS_PATH_API, LIVE_API_PATH, "api")
        flush_buffer(buffer_rss, lock_rss, HDFS_PATH_RSS, LIVE_RSS_PATH, "rss")


def main():
    print("=" * 60)
    print("  NewsPulse -- Consumer to HDFS")
    print("=" * 60)
    print(f"  Membaca : {TOPIC_API} dan {TOPIC_RSS}")
    print(f"  HDFS    : {HDFS_PATH_API} dan {HDFS_PATH_RSS}")
    print(f"  Flush   : setiap {FLUSH_INTERVAL // 60} menit")
    print(f"  Method  : {'hdfs library (bonus +2)' if USE_HDFS_LIBRARY else 'subprocess (docker exec)'}")
    print("=" * 60 + "\n")

    # Jalankan 3 thread: 2 consumer + 1 scheduler flush
    t_api   = threading.Thread(
        target=consume_topic,
        args=(TOPIC_API, buffer_api, lock_api, "API"),
        daemon=True
    )
    t_rss   = threading.Thread(
        target=consume_topic,
        args=(TOPIC_RSS, buffer_rss, lock_rss, "RSS"),
        daemon=True
    )
    t_flush = threading.Thread(target=flush_loop, daemon=True)

    t_api.start()
    t_rss.start()
    t_flush.start()

    print("[MAIN] Semua thread aktif. Tekan Ctrl+C untuk berhenti.\n")

    try:
        # Flush pertama setelah consumer sempat baca beberapa pesan
        time.sleep(15)
        print("[MAIN] Initial flush...")
        flush_buffer(buffer_api, lock_api, HDFS_PATH_API, LIVE_API_PATH, "api")
        flush_buffer(buffer_rss, lock_rss, HDFS_PATH_RSS, LIVE_RSS_PATH, "rss")

        # Keep main thread alive
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        print("\n[STOP] Consumer dihentikan. Flush buffer terakhir...")
        flush_buffer(buffer_api, lock_api, HDFS_PATH_API, LIVE_API_PATH, "api")
        flush_buffer(buffer_rss, lock_rss, HDFS_PATH_RSS, LIVE_RSS_PATH, "rss")
        print("[STOP] Selesai.")


if __name__ == "__main__":
    main()
