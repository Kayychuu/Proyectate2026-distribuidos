#!/usr/bin/env bash
set -Eeuo pipefail
cd "$(dirname "$0")"
R="$PWD/.atemporales_v3"
export KAFKA_HOME="${KAFKA_HOME:-$HOME/kafka}"
export KAFKA_HEAP_OPTS="${KAFKA_HEAP_OPTS:--Xms256m -Xmx512m}"
export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}"
export PATH="$JAVA_HOME/bin:$PATH"
ACT="${1:-status}"; ID="${2:-2}"
[[ "$ID" == 2 || "$ID" == 3 ]] || { echo 'Broker válido: 2 o 3'; exit 2; }
[[ -f "$R/propietario" && "$(cat "$R/propietario")" == 1 ]] || { echo 'Solo se pueden gestionar brokers creados por iniciar_v3.sh'; exit 1; }
P="$R/broker${ID}.pid"; PORT=$((9092+(ID-2)*2))
case "$ACT" in
  down)
    [[ -f "$P" ]] && kill "$(cat "$P")" 2>/dev/null || true
    rm -f "$P"
    echo "[OK] Broker $ID detenido; verifica cambio de líder e ISR" ;;
  up)
    if [[ -f "$P" ]] && kill -0 "$(cat "$P")" 2>/dev/null; then echo "Broker $ID ya funciona"; exit 0; fi
    "$KAFKA_HOME/bin/kafka-server-start.sh" "$R/broker${ID}.properties" > "$R/logs/broker${ID}.log" 2>&1 &
    echo $! > "$P"
    echo "[OK] Broker $ID reiniciado; espera la resincronización ISR" ;;
  status)
    if [[ -f "$P" ]] && kill -0 "$(cat "$P")" 2>/dev/null; then echo "Broker $ID, pid=$(cat "$P"), puerto=$PORT"; else echo "Broker $ID detenido"; fi ;;
  *) echo 'Uso: bash broker_v3.sh down|up|status 2|3'; exit 2 ;;
esac
