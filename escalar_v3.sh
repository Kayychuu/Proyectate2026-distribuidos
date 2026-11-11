#!/usr/bin/env bash
set -Eeuo pipefail
cd "$(dirname "$0")"
RUN="$PWD/.atemporales_v3"
N="${1:-1}"
[[ "$N" =~ ^[123]$ ]] || { echo 'Uso: bash escalar_v3.sh 1|2|3'; exit 2; }
[[ -f "$RUN/dashboard.pid" ]] && kill -0 "$(cat "$RUN/dashboard.pid")" 2>/dev/null || { echo 'Inicia primero bash iniciar_v3.sh'; exit 1; }
for d in produccion rrhh inversion; do
  for i in 2 3; do
    f="$RUN/worker_${d}_${i}.pid"
    if (( i<=N )); then
      if [[ ! -f "$f" ]] || ! kill -0 "$(cat "$f")" 2>/dev/null; then
        .venv/bin/python -u motor_v3.py worker --dept "$d" --id "$i" > "$RUN/logs/worker_${d}_${i}.log" 2>&1 &
        echo $! > "$f"
      fi
    else
      if [[ -f "$f" ]]; then kill "$(cat "$f")" 2>/dev/null || true; rm -f "$f"; fi
      rm -f "$RUN/worker_${d}_${i}.json"
    fi
  done
done
echo "[OK] $N workers por departamento ($((N*3)) en total). Rebalanceo Kafka en curso."
