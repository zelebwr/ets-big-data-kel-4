# [NamaAnggota2]: producer_api.py — polling Open-Meteo API, kirim ke weather-api topic

import json
import time
import requests
from datetime import datetime
from kafka import KafkaProducer

# ---------------------------------------------------------------------------
# [NamaAnggota2]: konfigurasi 6 kota target sesuai spesifikasi ETS
# key   = kode kota (dipakai sebagai Kafka message key)
# lat/lon = koordinat untuk Open-Meteo API parameter
# ---------------------------------------------------------------------------
CITIES = [
    {"kode": "JKT", "nama": "Jakarta",   "lat": -6.21,  "lon": 106.85},
    {"kode": "SBY", "nama": "Surabaya",  "lat": -7.25,  "lon": 112.75},
    {"kode": "SMG", "nama": "Semarang",  "lat": -6.99,  "lon": 110.42},
    {"kode": "MDN", "nama": "Medan",     "lat": -3.59,  "lon": 98.67},
    {"kode": "MKS", "nama": "Makassar",  "lat": -5.14,  "lon": 119.41},
    {"kode": "DPS", "nama": "Denpasar",  "lat": -8.67,  "lon": 115.21},
]

# ---------------------------------------------------------------------------
# [NamaAnggota2]: Open-Meteo endpoint — gratis, tanpa API key
# current fields yang diminta:
#   temperature_2m    → suhu udara 2m dari permukaan (°C)
#   relative_humidity_2m → kelembaban relatif (%)
#   wind_speed_10m    → kecepatan angin 10m dari permukaan (km/h)
#   weather_code      → WMO weather interpretation code
# ---------------------------------------------------------------------------
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

POLL_INTERVAL_SECONDS = 600  # 10 menit sesuai spesifikasi ETS

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC  = "weather-api"

LIVE_OUTPUT_PATH = "../dashboard/data/live_api.json"


# ---------------------------------------------------------------------------
# [NamaAnggota2]: inisialisasi KafkaProducer
# enable_idempotence=True  → garantee exactly-once delivery per session
# acks="all"               → broker konfirmasi setelah semua in-sync replica terima
# value_serializer         → encode dict Python ke bytes JSON UTF-8
# key_serializer           → encode string kode kota ke bytes UTF-8
# ---------------------------------------------------------------------------
def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        enable_idempotence=True,
        acks="all",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )


# ---------------------------------------------------------------------------
# [NamaAnggota2]: fetch data cuaca 1 kota dari Open-Meteo
# return: dict event atau None jika request gagal
# ---------------------------------------------------------------------------
def fetch_weather(city: dict) -> dict | None:
    params = {
        "latitude":  city["lat"],
        "longitude": city["lon"],
        "current":   "temperature_2m,relative_humidity_2m,wind_speed_10m,weather_code",
        "timezone":  "Asia/Jakarta",
    }
    try:
        resp = requests.get(OPEN_METEO_URL, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        current = data["current"]

        # [NamaAnggota2]: struktur JSON konsisten — semua field selalu ada
        event = {
            "kode_kota":    city["kode"],
            "nama_kota":    city["nama"],
            "temperature":  current["temperature_2m"],
            "humidity":     current["relative_humidity_2m"],
            "wind_speed":   current["wind_speed_10m"],
            "weather_code": current["weather_code"],
            "timestamp":    current["time"],          # ISO8601 dari API
            "fetched_at":   datetime.now().isoformat(), # waktu fetch lokal
        }
        return event

    except requests.exceptions.RequestException as e:
        print(f"[ERROR] fetch gagal untuk {city['kode']}: {e}")
        return None


# ---------------------------------------------------------------------------
# [NamaAnggota2]: simpan snapshot terbaru semua kota ke file lokal
# dashboard/app.py membaca file ini untuk panel live data
# overwrite setiap polling cycle — hanya butuh data terkini
# ---------------------------------------------------------------------------
def save_live_snapshot(events: list):
    import os
    os.makedirs(os.path.dirname(LIVE_OUTPUT_PATH), exist_ok=True)
    with open(LIVE_OUTPUT_PATH, "w") as f:
        json.dump(events, f, indent=2)
    print(f"[SNAPSHOT] live_api.json updated — {len(events)} kota")


# ---------------------------------------------------------------------------
# [NamaAnggota2]: main polling loop
# flow per cycle:
#   fetch 6 kota → kirim ke Kafka (key=kode_kota) → simpan snapshot lokal
#   → tunggu 10 menit → ulangi
# ---------------------------------------------------------------------------
def main():
    print("[INIT] Menghubungkan ke Kafka broker...")
    producer = create_producer()
    print(f"[INIT] Connected. Topic target: {KAFKA_TOPIC}")
    print(f"[INIT] Polling interval: {POLL_INTERVAL_SECONDS // 60} menit\n")

    cycle = 0
    while True:
        cycle += 1
        print(f"--- Cycle {cycle} | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")

        successful_events = []

        for city in CITIES:
            event = fetch_weather(city)
            if event is None:
                # [NamaAnggota2]: skip kota jika fetch gagal, lanjut kota berikutnya
                continue

            # [NamaAnggota2]: kirim ke Kafka
            # key=kode_kota → garantee semua event kota yang sama masuk partisi yang sama
            producer.send(
                KAFKA_TOPIC,
                key=event["kode_kota"],
                value=event,
            )
            successful_events.append(event)
            print(f"  [SENT] {event['kode_kota']} | {event['temperature']}°C | "
                  f"Humidity: {event['humidity']}% | Wind: {event['wind_speed']} km/h")

        # [NamaAnggota2]: flush garantee semua pesan terkirim sebelum sleep
        producer.flush()
        print(f"  [FLUSH] {len(successful_events)}/6 kota terkirim ke Kafka")

        # [NamaAnggota2]: simpan snapshot untuk dashboard
        if successful_events:
            save_live_snapshot(successful_events)

        print(f"  [SLEEP] Menunggu {POLL_INTERVAL_SECONDS // 60} menit...\n")
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
