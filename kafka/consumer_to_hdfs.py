# [Muhammad Fatihul Qolbi]: consumer_to_hdfs.py — baca dari 2 topic Kafka, simpan ke HDFS

import json
import subprocess
import threading
import os
from datetime import datetime
from kafka import KafkaConsumer

# ── Konfigurasi ──────────────────────────────────────────────────────────────
KAFKA_BROKER   = "localhost:9092"
TOPIC_API      = "weather-api"
TOPIC_RSS      = "weather-rss"
GROUP_ID       = "weather-hdfs-consumer"

HDFS_PATH_API  = "/data/weather/api"
HDFS_PATH_RSS  = "/data/weather/rss"

LOCAL_TEMP_DIR = "/tmp/weather_buffer"   # folder sementara sebelum ke HDFS

FLUSH_INTERVAL = 120   # simpan ke HDFS setiap 2 menit
# ─────────────────────────────────────────────────────────────────────────────

# Buffer per topic (diakses dari 2 thread, pakai lock)
buffer_api = []
buffer_rss = []
lock_api   = threading.Lock()
lock_rss   = threading.Lock()


def put_to_hdfs(local_file: str, hdfs_path: str):
    """Upload file lokal ke HDFS menggunakan subprocess hdfs dfs -put."""
    cmd = ["docker", "exec", "hadoop-namenode",
           "hdfs", "dfs", "-put", "-f",
           f"/tmp/{os.path.basename(local_file)}",
           hdfs_path]

    # Salin file ke dalam container namenode dulu
    copy_cmd = ["docker", "cp", local_file, f"hadoop-namenode:/tmp/{os.path.basename(local_file)}"]
    result_cp = subprocess.run(copy_cmd, capture_output=True, text=True)
    if result_cp.returncode != 0:
        print(f"[ERROR] docker cp gagal: {result_cp.stderr}")
        return

    # Lalu put dari container ke HDFS
    result_put = subprocess.run(cmd, capture_output=True, text=True)
    if result_put.returncode != 0:
        print(f"[ERROR] hdfs put gagal: {result_put.stderr}")
    else:
        print(f"[HDFS] ✅ Tersimpan: {hdfs_path}/{os.path.basename(local_file)}")


def flush_buffer(buffer: list, lock: threading.Lock, hdfs_path: str, label: str):
    """Ambil isi buffer, simpan ke file lokal, lalu upload ke HDFS."""
    with lock:
        if not buffer:
            return
        data_to_flush = buffer.copy()
        buffer.clear()

    os.makedirs(LOCAL_TEMP_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename  = f"{label}_{timestamp}.json"
    local_path = os.path.join(LOCAL_TEMP_DIR, filename)

    with open(local_path, "w") as f:
        json.dump(data_to_flush, f, indent=2, ensure_ascii=False)

    print(f"[FLUSH] {label}: {len(data_to_flush)} event → {local_path}")
    put_to_hdfs(local_path, hdfs_path)


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

    print(f"[CONSUMER] Thread {label} aktif — membaca topic: {topic}")

    try:
        while True:
            for msg in consumer:
                with lock:
                    buffer.append(msg.value)
                # tidak print tiap event agar terminal tidak banjir
    except Exception as e:
        print(f"[ERROR] Thread {label}: {e}")
    finally:
        consumer.close()


def flush_loop():
    """Thread: setiap FLUSH_INTERVAL detik, flush kedua buffer ke HDFS."""
    import time
    while True:
        time.sleep(FLUSH_INTERVAL)
        print(f"\n[SCHEDULER] {datetime.now().strftime('%H:%M:%S')} — Flushing ke HDFS...")
        flush_buffer(buffer_api, lock_api, HDFS_PATH_API, "api")
        flush_buffer(buffer_rss, lock_rss, HDFS_PATH_RSS, "rss")


def main():
    print("=" * 55)
    print("  Weather Consumer → HDFS")
    print(f"  Membaca: {TOPIC_API} dan {TOPIC_RSS}")
    print(f"  Flush ke HDFS setiap {FLUSH_INTERVAL // 60} menit")
    print("=" * 55 + "\n")

    # Jalankan 3 thread: 2 consumer + 1 scheduler flush
    t_api   = threading.Thread(target=consume_topic,
                               args=(TOPIC_API, buffer_api, lock_api, "API"),
                               daemon=True)
    t_rss   = threading.Thread(target=consume_topic,
                               args=(TOPIC_RSS, buffer_rss, lock_rss, "RSS"),
                               daemon=True)
    t_flush = threading.Thread(target=flush_loop, daemon=True)

    t_api.start()
    t_rss.start()
    t_flush.start()

    print("[MAIN] Semua thread aktif. Tekan Ctrl+C untuk berhenti.\n")

    try:
        # Flush pertama langsung saat start (baca data yang sudah ada di Kafka)
        import time
        time.sleep(15)  # tunggu consumer sempat baca beberapa pesan dulu
        flush_buffer(buffer_api, lock_api, HDFS_PATH_API, "api")
        flush_buffer(buffer_rss, lock_rss, HDFS_PATH_RSS, "rss")

        # Keep main thread hidup
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        print("\n[STOP] Consumer dihentikan. Flush buffer terakhir...")
        flush_buffer(buffer_api, lock_api, HDFS_PATH_API, "api")
        flush_buffer(buffer_rss, lock_rss, HDFS_PATH_RSS, "rss")
        print("[STOP] Selesai.")


if __name__ == "__main__":
    main()