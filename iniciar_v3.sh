#!/usr/bin/env bash
set -Eeuo pipefail
cd "$(dirname "$0")"
ROOT="$PWD"
RUN="$ROOT/.atemporales_v3"
LOG="$RUN/logs"
DATA="$RUN/data"
mkdir -p "$LOG" "$DATA"
export KAFKA_HOME="${KAFKA_HOME:-$HOME/kafka}"
export KAFKA_HEAP_OPTS="${KAFKA_HEAP_OPTS:--Xms256m -Xmx512m}"
[[ -x "$KAFKA_HOME/bin/kafka-server-start.sh" ]] || { echo 'ERROR: Kafka no está en ~/kafka'; exit 1; }
[[ -x .venv/bin/python ]] || { echo 'ERROR: no existe .venv (entorno virtual)'; exit 1; }
PY="$ROOT/.venv/bin/python"
if "$PY" - <<'PYCHK'
import socket
s=socket.socket()
r=s.connect_ex(('127.0.0.1',8501))
s.close()
raise SystemExit(0 if r != 0 else 1)
PYCHK
then :; else
  echo '[ERROR] Streamlit ocupa el puerto 8501. Detén el anterior con Ctrl+C.'
  exit 1
fi
export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}"
export PATH="$JAVA_HOME/bin:$PATH"

# Si ya existe un clúster completo en los puertos, reutilizarlo sin borrarlo.
ocupados=$($PY - <<'PY'
import socket
print(''.join('1' if socket.socket().connect_ex(('127.0.0.1', p))==0 else '0' for p in (9092,9093,9094)))
PY
)
OWN=0
if [[ "$ocupados" == "111" ]]; then
    echo '[OK] Reutilizando clúster Kafka activo en 9092,9093,9094'
elif [[ "$ocupados" != "000" ]]; then
    echo "ERROR: puertos parcialmente ocupados ($ocupados). Revisa Kafka antes de arrancar."; exit 1
else
    OWN=1
    mkdir -p "$DATA/controller" "$DATA/broker2" "$DATA/broker3"
    cat > "$RUN/controller.properties" <<EOF
process.roles=controller
node.id=1
controller.quorum.voters=1@localhost:9093
listeners=CONTROLLER://localhost:9093
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
log.dirs=$DATA/controller
EOF
    for especificacion in '2 9092' '3 9094'; do
        read -r id puerto <<< "$especificacion"
        cat > "$RUN/broker${id}.properties" <<EOF
process.roles=broker
node.id=$id
controller.quorum.voters=1@localhost:9093
listeners=PLAINTEXT://localhost:$puerto
advertised.listeners=PLAINTEXT://localhost:$puerto
inter.broker.listener.name=PLAINTEXT
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
log.dirs=$DATA/broker$id
num.partitions=3
default.replication.factor=2
min.insync.replicas=1
offsets.topic.replication.factor=2
transaction.state.log.replication.factor=2
transaction.state.log.min.isr=1
EOF
    done
    [[ -s "$RUN/cluster.id" ]] || "$KAFKA_HOME/bin/kafka-storage.sh" random-uuid > "$RUN/cluster.id"
    ID="$(cat "$RUN/cluster.id")"
    for c in controller broker2 broker3; do
        "$KAFKA_HOME/bin/kafka-storage.sh" format -t "$ID" -c "$RUN/$c.properties" --ignore-formatted >/dev/null
        "$KAFKA_HOME/bin/kafka-server-start.sh" "$RUN/$c.properties" > "$LOG/$c.log" 2>&1 &
        echo $! > "$RUN/$c.pid"
        [[ "$c" == "controller" ]] && sleep 3
    done
    echo '[OK] Controller y dos brokers iniciados'
fi

echo "$OWN" > "$RUN/propietario"
# Create only after broker responds (RF=2).
for intento in {1..30}; do
    if "$KAFKA_HOME/bin/kafka-topics.sh" --list --bootstrap-server localhost:9092 >/dev/null 2>&1; then break; fi
    sleep 2
done
for topic in empresa.produccion empresa.rrhh empresa.inversion empresa.resultados; do
    "$KAFKA_HOME/bin/kafka-topics.sh" --create --if-not-exists \
       --bootstrap-server localhost:9092 --topic "$topic" \
       --partitions 3 --replication-factor 2
    "$KAFKA_HOME/bin/kafka-topics.sh" --describe --bootstrap-server localhost:9092 --topic "$topic"
done

# No permitir dos orquestadores sobre la misma configuración.
if [[ -f "$RUN/dashboard.pid" ]] && kill -0 "$(cat "$RUN/dashboard.pid")" 2>/dev/null; then
    echo 'ERROR: hay una instancia v3 activa'; exit 1
fi

detener() {
    echo '[*] Deteniendo servicios v3...'
    for f in "$RUN"/worker_*.pid "$RUN"/producer.pid "$RUN"/sink.pid "$RUN"/exporter.pid; do
        [[ -e "$f" ]] || continue
        kill "$(cat "$f")" 2>/dev/null || true
        rm -f "$f"
    done
    if [[ "$(cat "$RUN/propietario")" == 1 ]]; then
        for c in controller broker2 broker3; do
            if [[ -f "$RUN/$c.pid" ]]; then
                kill "$(cat "$RUN/$c.pid")" 2>/dev/null || true
                rm -f "$RUN/$c.pid"
            fi
        done
    fi
    rm -f "$RUN/dashboard.pid"
}
trap detener EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

for d in produccion rrhh inversion; do
    "$PY" -u motor_v3.py worker --dept "$d" --id 1 > "$LOG/worker_${d}_1.log" 2>&1 &
    echo $! > "$RUN/worker_${d}_1.pid"
done
"$PY" -u motor_v3.py sink > "$LOG/sink.log" 2>&1 &
echo $! > "$RUN/sink.pid"
"$PY" -u motor_v3.py producer > "$LOG/producer.log" 2>&1 &
echo $! > "$RUN/producer.pid"
"$PY" -u prom_v3.py > "$LOG/prometheus_exporter.log" 2>&1 &
echo $! > "$RUN/exporter.pid"
[[ -f "$RUN/control.json" ]] || echo '{"tasa":30,"prob":0.04,"ruido":2.5,"modo":"mezcla_selectiva"}' > "$RUN/control.json"
echo "[OK] Dashboard: http://localhost:8501  |  Exporter: http://localhost:9108/metrics"
echo '[INFO] Para detener: Ctrl+C. Para escalar: bash escalar_v3.sh 2 (o 3).'
# Bloquear otros inicios con pid del proceso que administra Streamlit.
echo $$ > "$RUN/dashboard.pid"
"$PY" -m streamlit run app_v3.py --server.address 127.0.0.1 --server.port 8501
