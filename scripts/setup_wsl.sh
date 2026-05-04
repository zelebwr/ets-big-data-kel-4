#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if ! command -v docker >/dev/null 2>&1; then
  echo "[ERROR] Docker tidak ditemukan di WSL. Install Docker Desktop + aktifkan WSL integration."
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  echo "[ERROR] Docker daemon belum aktif. Jalankan Docker Desktop dulu."
  exit 1
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "[ERROR] python3 tidak ditemukan. Install Python 3 di WSL."
  exit 1
fi

if [ ! -d ".venv" ]; then
  echo "[STEP] Membuat virtual environment .venv"
  python3 -m venv .venv
fi

echo "[STEP] Mengaktifkan virtual environment"
# shellcheck disable=SC1091
source .venv/bin/activate

echo "[STEP] Upgrade pip dan install dependencies"
python -m pip install --upgrade pip
pip install -r requirements.txt

if [ -f ".env" ]; then
  echo "[STEP] Memuat environment dari .env"
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

echo "[STEP] Menjalankan Kafka dan Hadoop"
docker compose -f docker-compose-kafka.yml up -d
docker compose -f docker-compose-hadoop.yml up -d

echo "[STEP] Menunggu service siap"
sleep 12

echo "[STEP] Membuat topic Kafka (idempotent)"
docker exec kafka-broker kafka-topics --create --if-not-exists --topic news-api --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker exec kafka-broker kafka-topics --create --if-not-exists --topic news-rss --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

echo "[STEP] Membuat direktori HDFS"
docker exec hadoop-namenode hdfs dfs -mkdir -p /data/news/api
docker exec hadoop-namenode hdfs dfs -mkdir -p /data/news/rss
docker exec hadoop-namenode hdfs dfs -mkdir -p /data/news/hasil

echo "[DONE] Setup WSL selesai. Jalankan pipeline dengan scripts/run_wsl.sh"
