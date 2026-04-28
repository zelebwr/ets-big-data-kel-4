# [NamaAnggota2]: producer_api.py — polling Open-Meteo API, kirim ke weather-api topic

import json
import time
import requests
from datetime import datetime
from kafka import KafkaProducer

CITIES = [
    {"kode": "JKT", "nama": "Jakarta",   "lat": -6.21,  "lon": 106.85},
    {"kode": "SBY", "nama": "Surabaya",  "lat": -7.25,  "lon": 112.75},
    {"kode": "SMG", "nama": "Semarang",  "lat": -6.99,  "lon": 110.42},
    {"kode": "MDN", "nama": "Medan",     "lat": -3.59,  "lon": 98.67},
    {"kode": "MKS", "nama": "Makassar",  "lat": -5.14,  "lon": 119.41},
    {"kode": "DPS", "nama": "Denpasar",  "lat": -8.67,  "lon": 115.21},
]


OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

POLL_INTERVAL_SECONDS = 600  # 10 menit sesuai spesifikasi ETS

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC  = "weather-api"

LIVE_OUTPUT_PATH = "../dashboard/data/live_api.json"


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        enable_idempotence=True,
        acks="all",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )


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



def save_live_snapshot(events: list):
    import os
    os.makedirs(os.path.dirname(LIVE_OUTPUT_PATH), exist_ok=True)
    with open(LIVE_OUTPUT_PATH, "w") as f:
        json.dump(events, f, indent=2)
    print(f"[SNAPSHOT] live_api.json updated — {len(events)} kota")


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
                continue

            producer.send(
                KAFKA_TOPIC,
                key=event["kode_kota"],
                value=event,
            )
            successful_events.append(event)
            print(f"  [SENT] {event['kode_kota']} | {event['temperature']}°C | "
                  f"Humidity: {event['humidity']}% | Wind: {event['wind_speed']} km/h")

        producer.flush()
        print(f"  [FLUSH] {len(successful_events)}/6 kota terkirim ke Kafka")

        if successful_events:
            save_live_snapshot(successful_events)

        print(f"  [SLEEP] Menunggu {POLL_INTERVAL_SECONDS // 60} menit...\n")
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
