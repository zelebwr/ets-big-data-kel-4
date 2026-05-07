#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [ ! -d ".venv" ]; then
  echo "[ERROR] Virtual env belum ada. Jalankan scripts/setup_wsl.sh dulu."
  exit 1
fi

# shellcheck disable=SC1091
source .venv/bin/activate

if [ -f ".env" ]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

mkdir -p logs dashboard/data

if [ -z "${GNEWS_API_KEY:-}" ] || [ "${GNEWS_API_KEY:-}" = "YOUR_GNEWS_API_KEY_HERE" ]; then
  echo "[WARN] GNEWS_API_KEY belum diset. producer_api.py kemungkinan gagal fetch API."
fi

echo "[STEP] Menjalankan producer API"
nohup python kafka/producer_api.py > logs/producer_api.log 2>&1 &
PID_API=$!

echo "[STEP] Menjalankan producer RSS"
nohup python kafka/producer_rss.py > logs/producer_rss.log 2>&1 &
PID_RSS=$!

echo "[STEP] Menjalankan consumer ke HDFS"
nohup python kafka/consumer_to_hdfs.py > logs/consumer_to_hdfs.log 2>&1 &
PID_CONSUMER=$!

echo "[STEP] Menjalankan analisis Spark berkala"
nohup python scripts/run_analysis.py --watch --interval 120 > logs/spark_analysis.log 2>&1 &
PID_SPARK=$!

echo "[STEP] Menjalankan dashboard Flask"
nohup python dashboard/app.py > logs/dashboard.log 2>&1 &
PID_DASHBOARD=$!

echo "$PID_API" > logs/producer_api.pid
echo "$PID_RSS" > logs/producer_rss.pid
echo "$PID_CONSUMER" > logs/consumer_to_hdfs.pid
echo "$PID_SPARK" > logs/spark_analysis.pid
echo "$PID_DASHBOARD" > logs/dashboard.pid

echo "[DONE] Semua proses jalan di background."
echo "       Dashboard: http://localhost:5000"
echo "       Spark log: tail -f logs/spark_analysis.log"
echo "       Cek log   : tail -f logs/dashboard.log"
