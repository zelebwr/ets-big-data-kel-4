#!/usr/bin/env bash
# Minimal resource lakehouse runner - background mode with auto-refresh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

mkdir -p logs dashboard/data local_input/api local_input/rss

echo "=== MINIMAL LAKEHOUSE (BACKGROUND MODE) ==="

if [ ! -d ".venv" ]; then
  python3 -m venv .venv
fi

source .venv/bin/activate
pip install -q pyspark delta-spark flask requests feedparser python-dotenv

if [ -f ".env" ]; then
  set -a
  source .env
  set +a
fi

# Create sample data if needed
[ -z "$(ls -A local_input/api 2>/dev/null)" ] && echo '[{"judul": "Sample API News", "sumber": "gnews", "url": "http://sample.com"}]' > local_input/api/sample.json
[ -z "$(ls -A local_input/rss 2>/dev/null)" ] && echo '[{"judul": "Sample RSS News", "sumber": "kompas", "url": "http://rss.com"}]' > local_input/rss/sample.json

# Run lakehouse in background (refresh every 2 minutes)
echo "[LAKEHOUSE] Starting continuous runner (2 min interval)..."
nohup python scripts/run_lakehouse_continuous.py --use-local --hdfs-base ./local_input --interval 120 --log-level ERROR > logs/lakehouse_minimal.log 2>&1 &
echo $! > logs/lakehouse.pid

# Run dashboard
echo "[DASHBOARD] Starting on http://localhost:5000"
nohup python dashboard/app.py > logs/dashboard.log 2>&1 &
echo $! > logs/dashboard.pid

echo ""
echo "[DONE] Running in background:"
echo "  Dashboard: http://localhost:5000"
echo "  Lakehouse: tail -f logs/lakehouse_minimal.log"
echo "  Stop: ./scripts/stop_minimal.sh"
