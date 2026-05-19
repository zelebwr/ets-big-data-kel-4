#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="$(command -v python3 || command -v python || true)"

if [ -z "${PYTHON_BIN:-}" ]; then
  echo "[ERROR] python3/python tidak ditemukan di PATH."
  exit 1
fi

mkdir -p "$ROOT_DIR/logs"

MARKER="# newspulse-run-analysis"
LINE="*/2 * * * * cd \"$ROOT_DIR\" && \"$PYTHON_BIN\" scripts/run_analysis.py >> \"$ROOT_DIR/logs/cron_analysis.log\" 2>&1 $MARKER"

TMP_FILE="$(mktemp)"
trap 'rm -f "$TMP_FILE"' EXIT

crontab -l 2>/dev/null | grep -vF "$MARKER" > "$TMP_FILE" || true
printf '%s\n' "$LINE" >> "$TMP_FILE"
crontab "$TMP_FILE"

echo "[DONE] Cron terpasang: run_analysis.py akan jalan tiap 2 menit."
echo "       Log: $ROOT_DIR/logs/cron_analysis.log"
echo "       Cek: crontab -l"
