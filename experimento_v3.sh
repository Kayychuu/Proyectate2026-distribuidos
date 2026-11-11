#!/usr/bin/env bash
set -Eeuo pipefail
cd "$(dirname "$0")"
RUN="$PWD/.atemporales_v3"
[[ -f "$RUN/dashboard.pid" ]] || { echo 'Inicia primero bash iniciar_v3.sh'; exit 1; }
# Suspender el productor continuo para tener una carga fija reproducible.
if [[ -f "$RUN/producer.pid" ]]; then
    kill "$(cat "$RUN/producer.pid")" 2>/dev/null || true
    rm -f "$RUN/producer.pid"
fi
for n in 1 2 3; do
    bash escalar_v3.sh "$n"
    sleep 12  # permitir rebalanceo y confirmación de asignaciones
    echo "[ENSAYO] $n worker(s) por tópico; 1200 eventos; objetivo 300 ev/s"
    .venv/bin/python motor_v3.py bench --cantidad 1200 --tasa 300 --timeout 180
done
echo "[OK] Comparativa almacenada en $RUN/benchmarks.csv"
echo "Para reactivar productor continuo: bash reanudar_productor_v3.sh"
