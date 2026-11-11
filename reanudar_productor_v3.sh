#!/usr/bin/env bash
set -Eeuo pipefail
cd "$(dirname "$0")"
R="$PWD/.atemporales_v3"
[[ -f "$R/dashboard.pid" ]] || { echo 'Inicia primero v3'; exit 1; }
if [[ -f "$R/producer.pid" ]] && kill -0 "$(cat "$R/producer.pid")" 2>/dev/null; then
  echo 'El productor ya está funcionando'; exit 0
fi
.venv/bin/python -u motor_v3.py producer > "$R/logs/producer.log" 2>&1 &
echo $! > "$R/producer.pid"
echo '[OK] Productor continuo reanudado'
