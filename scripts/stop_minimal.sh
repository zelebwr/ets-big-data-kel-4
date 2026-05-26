#!/usr/bin/env bash
# Stop minimal lakehouse processes
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "[STOP] Stopping minimal lakehouse..."

for pidfile in logs/lakehouse.pid logs/dashboard.pid; do
  if [ -f "$pidfile" ]; then
    pid=$(cat "$pidfile")
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      echo "[STOP] Killed PID $pid"
    fi
    rm -f "$pidfile"
  fi
done

# Also kill any python processes running our scripts
pkill -f "run_lakehouse_continuous.py" 2>/dev/null || true
pkill -f "dashboard/app.py" 2>/dev/null || true

echo "[DONE] All minimal processes stopped"
