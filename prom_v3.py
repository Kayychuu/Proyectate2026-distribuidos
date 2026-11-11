#!/usr/bin/env python3
"""Métricas observadas para Prometheus; no sustituye a Kafka JMX Exporter."""
import time
from prometheus_client import Gauge, start_http_server
from motor_v3 import DEPTS, BASE, leer_json, metadatos

BROKER = Gauge("atemporales_broker_registered", "Broker registrado en Kafka", ["node"])
ISRS = Gauge("atemporales_under_replicated_partitions", "Particiones con ISR menor que replicas")
WORKERS = Gauge("atemporales_active_workers", "Workers con heartbeat reciente", ["area"])
TOTAL = Gauge("atemporales_worker_events_total_observed", "Contador de eventos por worker", ["area", "worker"])
P95 = Gauge("atemporales_worker_latency_p95_ms", "Latencia extremo a extremo (p95) ms", ["area", "worker"])

start_http_server(9108, addr="127.0.0.1")
while True:
    try:
        meta = metadatos()
        ids = {b.get("node_id") for b in meta.get("brokers", [])}
        for b in (2, 3):
            BROKER.labels(str(b)).set(int(b in ids))
        under = 0
        for t in meta.get("topics", []):
            for p in t.get("partitions", []):
                under += int(len(p.get("isr", [])) < len(p.get("replicas", [])))
        ISRS.set(under)
    except Exception as exc:
        print("[Prometheus] Kafka no responde:", exc, flush=True)
        for b in (2, 3): BROKER.labels(str(b)).set(0)
    for d in DEPTS:
        vivos = 0
        for i in range(1, 4):
            info = leer_json(BASE / f"worker_{d}_{i}.json")
            if not info: continue
            from datetime import datetime, timezone
            try:
                edad = (datetime.now(timezone.utc)-datetime.fromisoformat(info["actualizado"])).total_seconds()
            except (ValueError, KeyError): edad = 999
            if edad > 8: continue
            vivos += 1
            TOTAL.labels(d, str(i)).set(info.get("total", 0))
            if info.get("latencia_p95_ms") is not None:
                P95.labels(d, str(i)).set(info["latencia_p95_ms"])
        WORKERS.labels(d).set(vivos)
    time.sleep(5)
