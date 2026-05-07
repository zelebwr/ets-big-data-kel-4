#!/usr/bin/env bash
set -euo pipefail

MARKER="# newspulse-run-analysis"
TMP_FILE="$(mktemp)"
trap 'rm -f "$TMP_FILE"' EXIT

crontab -l 2>/dev/null | grep -vF "$MARKER" > "$TMP_FILE" || true
crontab "$TMP_FILE"

echo "[DONE] Cron newspulse dihapus."
