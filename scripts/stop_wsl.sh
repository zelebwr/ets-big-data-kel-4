#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

stop_by_pid_file() {
  local pid_file="$1"
  if [ -f "$pid_file" ]; then
    local pid
    pid="$(cat "$pid_file")"
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill "$pid"
      echo "[STOP] PID $pid dihentikan ($pid_file)"
    fi
    rm -f "$pid_file"
  fi
}

stop_by_pid_file logs/producer_api.pid
stop_by_pid_file logs/producer_rss.pid
stop_by_pid_file logs/consumer_to_hdfs.pid
stop_by_pid_file logs/spark_analysis.pid
stop_by_pid_file logs/dashboard.pid

echo "[STEP] Menghentikan container Kafka/Hadoop"
docker compose -f docker-compose-kafka.yml down
docker compose -f docker-compose-hadoop.yml down

echo "[DONE] Semua service dihentikan."
